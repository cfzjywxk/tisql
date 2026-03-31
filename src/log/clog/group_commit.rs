// Copyright 2024 TiSQL Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Group commit writer for batching durability sync operations via io_uring.
//!
//! Multiple concurrent callers submit serialized records via `submit()`.
//! A writer task/thread batches records, writes via `IoService::write_at()`,
//! and issues a single durability sync (`fdatasync`) for the batch.
//!
//! ## Design
//!
//! ```text
//! Caller threads              Writer task (io_uring)
//! ─────────────              ──────────────────────────
//! submit(record1) ────→     recv batch (record1, record2, record3)
//! submit(record2) ────→       io.write_at(batch_data)
//! submit(record3) ────→       io.sync()
//!                   ←──── notify(ok1), notify(ok2), notify(ok3)
//! ```
//!
//! ## 3-State Machine
//!
//! `WriterInner` transitions:
//! - `Active`: normal operation, `submit()` sends to channel
//! - `Draining`: `stop_and_drain()` called, `submit()` waits on Condvar
//! - `Failed`: unrecoverable error, `submit()` returns error immediately

use std::sync::Arc;
use std::time::Duration;

use crate::io::{AlignedBuf, DmaFile, IoService, IoTraceTag, DMA_ALIGNMENT};

use super::GroupCommitTuning;

/// A request submitted to the group commit writer.
struct GroupCommitRequest {
    /// Serialized record data to write.
    data: Vec<u8>,
    /// Whether this request requires durability sync.
    sync: bool,
    /// Channel to notify the caller when the write (+ optional sync) is complete.
    reply: tokio::sync::oneshot::Sender<Result<(), String>>,
}

/// Writer task/thread handle for draining.
enum DrainHandle {
    Tokio(tokio::task::JoinHandle<()>),
    Thread(Option<std::thread::JoinHandle<()>>),
}

/// Internal writer state when active.
struct WriterState {
    tx: tokio::sync::mpsc::UnboundedSender<GroupCommitRequest>,
    drain_handle: DrainHandle,
}

/// 3-state machine for the writer lifecycle.
enum WriterInner {
    /// Normal operation: submit() sends to channel.
    Active(WriterState),
    /// stop_and_drain() in progress: submit() waits on Condvar.
    Draining,
    /// Unrecoverable error or shutdown: submit() returns error.
    Failed(String),
}

/// Group commit writer that batches durability sync operations via io_uring.
///
/// Callers submit serialized records via `submit()` which returns a
/// `tokio::sync::oneshot::Receiver`. The writer task batches records,
/// writes via `IoService::write_at()`, and syncs via IoService sync APIs.
///
/// Supports `stop_and_drain()` for truncation (async drain of in-flight
/// writes) and `restart()` to resume with a new file.
pub(crate) struct GroupCommitWriter {
    inner: parking_lot::Mutex<WriterInner>,
    available: parking_lot::Condvar,
    io_trace: IoTraceTag,
    group_commit_delay: Duration,
    group_commit_no_delay_count: usize,
}

impl GroupCommitWriter {
    /// Create a new group commit writer with an explicit I/O trace tag.
    pub(crate) fn new_with_trace(
        file: DmaFile,
        io: Arc<IoService>,
        handle: &tokio::runtime::Handle,
        io_trace: IoTraceTag,
    ) -> Self {
        Self::new_with_config_with_trace(file, io, handle, io_trace, GroupCommitTuning::default())
    }

    /// Create a new group commit writer with explicit batching knobs.
    pub(crate) fn new_with_config_with_trace(
        file: DmaFile,
        io: Arc<IoService>,
        handle: &tokio::runtime::Handle,
        io_trace: IoTraceTag,
        group_commit: GroupCommitTuning,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let io_clone = Arc::clone(&io);
        let group_commit_delay = group_commit.delay;
        let group_commit_no_delay_count = group_commit.no_delay_count.max(1);
        let jh = handle.spawn(async move {
            async_writer_loop(
                file,
                io_clone,
                rx,
                io_trace,
                group_commit_delay,
                group_commit_no_delay_count,
            )
            .await;
        });

        Self {
            inner: parking_lot::Mutex::new(WriterInner::Active(WriterState {
                tx,
                drain_handle: DrainHandle::Tokio(jh),
            })),
            available: parking_lot::Condvar::new(),
            io_trace,
            group_commit_delay,
            group_commit_no_delay_count,
        }
    }

    /// Create a GroupCommitWriter backed by a plain `std::thread`.
    ///
    /// Uses `blocking_recv()` and `.wait()` for synchronous I/O.
    /// Use this in test code or when no tokio runtime handle is available.
    pub(crate) fn new_with_thread_with_trace(
        file: DmaFile,
        io: Arc<IoService>,
        io_trace: IoTraceTag,
    ) -> Self {
        Self::new_with_thread_config_with_trace(file, io, io_trace, GroupCommitTuning::default())
    }

    /// Create a thread-backed group commit writer with explicit delay knobs.
    pub(crate) fn new_with_thread_config_with_trace(
        file: DmaFile,
        io: Arc<IoService>,
        io_trace: IoTraceTag,
        group_commit: GroupCommitTuning,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let io_clone = Arc::clone(&io);
        let group_commit_delay = group_commit.delay;
        let group_commit_no_delay_count = group_commit.no_delay_count.max(1);
        let jh = std::thread::spawn(move || {
            thread_writer_loop(
                file,
                io_clone,
                rx,
                io_trace,
                group_commit_delay,
                group_commit_no_delay_count,
            );
        });

        Self {
            inner: parking_lot::Mutex::new(WriterInner::Active(WriterState {
                tx,
                drain_handle: DrainHandle::Thread(Some(jh)),
            })),
            available: parking_lot::Condvar::new(),
            io_trace,
            group_commit_delay,
            group_commit_no_delay_count,
        }
    }

    /// Submit a serialized record for writing, optionally with durability sync.
    ///
    /// Returns a `tokio::sync::oneshot::Receiver` that resolves when the write
    /// (+ optional sync) is complete.
    ///
    /// If the writer is `Draining`, blocks on a Condvar until it becomes
    /// `Active` (after `restart()`) or `Failed`.
    pub(crate) fn submit(
        &self,
        data: Vec<u8>,
        sync: bool,
    ) -> std::result::Result<tokio::sync::oneshot::Receiver<Result<(), String>>, String> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();

        let request = GroupCommitRequest {
            data,
            sync,
            reply: reply_tx,
        };

        let mut guard = self.inner.lock();
        loop {
            match &*guard {
                WriterInner::Active(state) => {
                    state
                        .tx
                        .send(request)
                        .map_err(|_| "Group commit writer shut down".to_string())?;
                    return Ok(reply_rx);
                }
                WriterInner::Draining => {
                    self.available.wait(&mut guard);
                    // Re-check state after wake
                }
                WriterInner::Failed(msg) => {
                    return Err(msg.clone());
                }
            }
        }
    }

    /// Stop the writer and drain all in-flight requests.
    ///
    /// Sets state to `Draining`, closes the channel, and awaits the
    /// writer task/thread to complete. Callers of `submit()` will block
    /// on a Condvar until `restart()` or `set_failed()` is called.
    pub(crate) async fn stop_and_drain(&self) -> Result<(), String> {
        let drain_handle = {
            let mut guard = self.inner.lock();
            if matches!(&*guard, WriterInner::Draining) {
                return Err("Already draining".to_string());
            }

            if let WriterInner::Failed(msg) = &*guard {
                return Err(msg.clone());
            }

            let WriterInner::Active(state) = std::mem::replace(&mut *guard, WriterInner::Draining)
            else {
                unreachable!("writer state checked above")
            };

            // Drop tx to close channel — writer loop exits
            drop(state.tx);
            state.drain_handle
        };
        // Lock released — await without holding it

        let drain_result: Result<(), String> = match drain_handle {
            DrainHandle::Tokio(handle) => handle
                .await
                .map_err(|e| format!("Writer task panicked: {e}")),
            DrainHandle::Thread(Some(handle)) => {
                match tokio::task::spawn_blocking(move || handle.join()).await {
                    Ok(join_result) => {
                        join_result.map_err(|_| "Writer thread panicked".to_string())
                    }
                    Err(e) => Err(format!("spawn_blocking failed: {e}")),
                }
            }
            DrainHandle::Thread(None) => Err("Thread handle already taken".to_string()),
        };

        if let Err(msg) = &drain_result {
            let mut guard = self.inner.lock();
            if matches!(&*guard, WriterInner::Draining) {
                *guard = WriterInner::Failed(msg.clone());
                self.available.notify_all();
            }
        }

        drain_result
    }

    /// Set the writer to `Failed` state and wake all waiters.
    pub(crate) fn set_failed(&self, msg: String) {
        let mut guard = self.inner.lock();
        *guard = WriterInner::Failed(msg);
        self.available.notify_all();
    }

    /// Restart the writer with a new file after truncation.
    ///
    /// Transitions from `Draining` (or `Failed`) to `Active` and wakes
    /// all waiters blocked in `submit()`.
    ///
    /// If `handle` is `Some`, spawns an async tokio task. Otherwise spawns
    /// a plain `std::thread`.
    pub(crate) fn restart(
        &self,
        file: DmaFile,
        io: Arc<IoService>,
        handle: Option<&tokio::runtime::Handle>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let drain_handle = if let Some(h) = handle {
            let io_clone = Arc::clone(&io);
            let io_trace = self.io_trace;
            let group_commit_delay = self.group_commit_delay;
            let group_commit_no_delay_count = self.group_commit_no_delay_count;
            let jh = h.spawn(async move {
                async_writer_loop(
                    file,
                    io_clone,
                    rx,
                    io_trace,
                    group_commit_delay,
                    group_commit_no_delay_count,
                )
                .await;
            });
            DrainHandle::Tokio(jh)
        } else {
            let io_clone = Arc::clone(&io);
            let io_trace = self.io_trace;
            let group_commit_delay = self.group_commit_delay;
            let group_commit_no_delay_count = self.group_commit_no_delay_count;
            let jh = std::thread::spawn(move || {
                thread_writer_loop(
                    file,
                    io_clone,
                    rx,
                    io_trace,
                    group_commit_delay,
                    group_commit_no_delay_count,
                );
            });
            DrainHandle::Thread(Some(jh))
        };

        let mut guard = self.inner.lock();
        *guard = WriterInner::Active(WriterState { tx, drain_handle });
        self.available.notify_all();
    }

    /// Close the channel, signalling the writer loop to exit.
    ///
    /// Fire-and-forget: does NOT await the writer task/thread.
    /// The JoinHandle is dropped, so the task/thread detaches.
    pub(crate) fn shutdown(&self) {
        let mut guard = self.inner.lock();
        *guard = WriterInner::Failed("shutdown".to_string());
        self.available.notify_all();
    }
}

impl Drop for GroupCommitWriter {
    fn drop(&mut self) {
        // Close the channel by replacing with Failed state.
        // Writer task/thread exits when recv returns None/Err.
        let guard = self.inner.get_mut();
        *guard = WriterInner::Failed("dropped".to_string());
    }
}

// ============================================================================
// Writer Loops
// ============================================================================

/// Async writer loop — runs as a tokio task.
///
/// Batches requests, writes via `io.write_at()`, then issues a sync barrier.
/// Exits on channel close (all senders dropped) or I/O error (fail-stop).
async fn async_writer_loop(
    file: DmaFile,
    io: Arc<IoService>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<GroupCommitRequest>,
    io_trace: IoTraceTag,
    group_commit_delay: Duration,
    group_commit_no_delay_count: usize,
) {
    let mut offset = file.file_size();
    let mut current_batch: Vec<GroupCommitRequest> = Vec::with_capacity(64);
    let mut next_batch: Vec<GroupCommitRequest> = Vec::with_capacity(64);

    // Bootstrap the first batch. Subsequent batches are collected while the
    // previous batch's I/O is in-flight to overlap queueing with sync latency.
    let mut channel_closed = collect_batch_with_delay_async(
        &mut rx,
        &mut current_batch,
        group_commit_delay,
        group_commit_no_delay_count,
    )
    .await;
    if current_batch.is_empty() {
        return;
    }

    loop {
        #[cfg(feature = "failpoints")]
        if let Some(dur) = fail::eval("clog_writer_loop_pause", |_| {
            std::time::Duration::from_millis(100)
        }) {
            tokio::time::sleep(dur).await;
        }

        let write_error = {
            let write_future = write_batch_async(&file, &io, &mut offset, &current_batch, io_trace);
            tokio::pin!(write_future);

            loop {
                if channel_closed {
                    break write_future.await;
                }

                tokio::select! {
                    write_error = &mut write_future => break write_error,
                    req = rx.recv() => match req {
                        Some(req) => {
                            next_batch.push(req);
                            drain_ready_requests(&mut rx, &mut next_batch);
                        }
                        None => {
                            channel_closed = true;
                        }
                    }
                }
            }
        };

        match write_error {
            Some(err) => {
                reply_batch(&mut current_batch, Err(err.clone()));
                reply_batch(&mut next_batch, Err(err));
                return;
            }
            None => {
                reply_batch(&mut current_batch, Ok(()));
            }
        }

        if next_batch.is_empty() {
            if channel_closed {
                return;
            }

            channel_closed = collect_batch_with_delay_async(
                &mut rx,
                &mut next_batch,
                group_commit_delay,
                group_commit_no_delay_count,
            )
            .await;
            if next_batch.is_empty() {
                return;
            }
        }

        std::mem::swap(&mut current_batch, &mut next_batch);
    }
}

/// Thread-based writer loop — runs on a plain std::thread.
///
/// Same logic as `async_writer_loop` but uses `blocking_recv()` and `.wait()`.
fn thread_writer_loop(
    file: DmaFile,
    io: Arc<IoService>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<GroupCommitRequest>,
    io_trace: IoTraceTag,
    group_commit_delay: Duration,
    group_commit_no_delay_count: usize,
) {
    let mut offset = file.file_size();
    let mut batch: Vec<GroupCommitRequest> = Vec::with_capacity(64);

    loop {
        match rx.blocking_recv() {
            Some(first) => batch.push(first),
            None => return,
        }

        drain_ready_requests(&mut rx, &mut batch);
        if group_commit_delay > Duration::ZERO && batch.len() < group_commit_no_delay_count {
            std::thread::sleep(group_commit_delay);
            drain_ready_requests(&mut rx, &mut batch);
        }

        #[cfg(feature = "failpoints")]
        fail::fail_point!("clog_writer_loop_pause");

        let write_error = write_batch_sync(&file, &io, &mut offset, &batch, io_trace);

        let result = match &write_error {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        };
        for req in batch.drain(..) {
            let _ = req.reply.send(result.clone());
        }

        if write_error.is_some() {
            return;
        }
    }
}

fn drain_ready_requests(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<GroupCommitRequest>,
    batch: &mut Vec<GroupCommitRequest>,
) {
    while let Ok(req) = rx.try_recv() {
        batch.push(req);
    }
}

fn reply_batch(batch: &mut Vec<GroupCommitRequest>, result: Result<(), String>) {
    for req in batch.drain(..) {
        let _ = req.reply.send(result.clone());
    }
}

async fn collect_batch_with_delay_async(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<GroupCommitRequest>,
    batch: &mut Vec<GroupCommitRequest>,
    delay: Duration,
    no_delay_count: usize,
) -> bool {
    // Wait for first request (or channel close).
    match rx.recv().await {
        Some(first) => batch.push(first),
        None => return true,
    }
    drain_ready_requests(rx, batch);

    // Delay window: collect more requests until timeout, saturation, or close.
    if delay > Duration::ZERO && batch.len() < no_delay_count {
        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                _ = &mut sleep => break,
                req = rx.recv() => match req {
                    Some(req) => {
                        batch.push(req);
                        drain_ready_requests(rx, batch);
                        if batch.len() >= no_delay_count {
                            break;
                        }
                    }
                    None => return true,
                }
            }
        }
    }

    false
}

// ============================================================================
// Batch Write Helpers
// ============================================================================

/// Write a batch of requests via io_uring (async).
async fn write_batch_async(
    file: &DmaFile,
    io: &IoService,
    offset: &mut u64,
    batch: &[GroupCommitRequest],
    io_trace: IoTraceTag,
) -> Option<String> {
    // Combine data from all requests
    let total_len: usize = batch.iter().map(|r| r.data.len()).sum();
    if total_len > 0 {
        let mut combined = Vec::with_capacity(total_len);
        for req in batch {
            combined.extend_from_slice(&req.data);
        }

        let buf = AlignedBuf::from_slice(&combined, DMA_ALIGNMENT);
        match io.write_at_with_trace(file, *offset, buf, io_trace).await {
            Ok(n) => {
                if n != total_len {
                    return Some(format!("Short write: expected {total_len}, got {n}"));
                }
                *offset += n as u64;
            }
            Err(e) => return Some(format!("Write error: {e}")),
        }
    }

    // Sync once if any request in the batch requires it.
    let needs_sync = batch.iter().any(|r| r.sync);
    if needs_sync {
        let sync_result = io.datasync_with_trace(file, io_trace).await;
        if let Err(e) = sync_result {
            return Some(format!("Sync error: {e}"));
        }
    }

    None
}

/// Write a batch of requests via io_uring (sync `.wait()`).
fn write_batch_sync(
    file: &DmaFile,
    io: &IoService,
    offset: &mut u64,
    batch: &[GroupCommitRequest],
    io_trace: IoTraceTag,
) -> Option<String> {
    let total_len: usize = batch.iter().map(|r| r.data.len()).sum();
    if total_len > 0 {
        let mut combined = Vec::with_capacity(total_len);
        for req in batch {
            combined.extend_from_slice(&req.data);
        }

        let buf = AlignedBuf::from_slice(&combined, DMA_ALIGNMENT);
        match io.write_at_with_trace(file, *offset, buf, io_trace).wait() {
            Ok(n) => {
                if n != total_len {
                    return Some(format!("Short write: expected {total_len}, got {n}"));
                }
                *offset += n as u64;
            }
            Err(e) => return Some(format!("Write error: {e}")),
        }
    }

    let needs_sync = batch.iter().any(|r| r.sync);
    if needs_sync {
        let sync_result = io.datasync_with_trace(file, io_trace).wait();
        if let Err(e) = sync_result {
            return Some(format!("Sync error: {e}"));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn make_writer() -> (DmaFile, Arc<IoService>, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let file = DmaFile::open_append_buffered(tmp.path()).unwrap();
        let io = IoService::new(32).unwrap();
        (file, io, tmp)
    }

    #[tokio::test]
    async fn test_group_commit_single_write() {
        let (file, io, tmp) = make_writer();
        let gc = GroupCommitWriter::new_with_thread_with_trace(file, io, IoTraceTag::default());

        let rx = gc.submit(b"hello".to_vec(), true).unwrap();
        rx.await.unwrap().unwrap();
        drop(gc);

        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&content, b"hello");
    }

    #[tokio::test]
    async fn test_group_commit_multiple_sequential() {
        let (file, io, tmp) = make_writer();
        let gc = GroupCommitWriter::new_with_thread_with_trace(file, io, IoTraceTag::default());

        for data in [b"aaa".as_slice(), b"bbb", b"ccc"] {
            let rx = gc.submit(data.to_vec(), true).unwrap();
            rx.await.unwrap().unwrap();
        }
        drop(gc);

        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&content, b"aaabbbccc");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_group_commit_concurrent() {
        let (file, io, tmp) = make_writer();
        let gc = Arc::new(GroupCommitWriter::new_with_thread_with_trace(
            file,
            io,
            IoTraceTag::default(),
        ));

        let mut handles = vec![];
        for i in 0..10 {
            let w = Arc::clone(&gc);
            handles.push(tokio::spawn(async move {
                let data = format!("{i:02}");
                let rx = w.submit(data.into_bytes(), true).unwrap();
                rx.await.unwrap().unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        drop(gc);

        let content = std::fs::read(tmp.path()).unwrap();
        // Each write is 2 bytes, total should be 20
        assert_eq!(content.len(), 20);

        // All numbers 00-09 should appear
        let text = String::from_utf8(content).unwrap();
        for i in 0..10 {
            assert!(text.contains(&format!("{i:02}")));
        }
    }

    #[tokio::test]
    async fn test_group_commit_no_sync() {
        let (file, io, tmp) = make_writer();
        let gc = GroupCommitWriter::new_with_thread_with_trace(file, io, IoTraceTag::default());

        let rx = gc.submit(b"data".to_vec(), false).unwrap();
        rx.await.unwrap().unwrap();
        drop(gc);

        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&content, b"data");
    }

    #[tokio::test]
    async fn test_group_commit_submit_async() {
        let (file, io, tmp) = make_writer();
        let gc = GroupCommitWriter::new_with_thread_with_trace(file, io, IoTraceTag::default());

        // submit returns a future
        let rx = gc.submit(b"async_data".to_vec(), true).unwrap();

        // Await the future — resolves when sync completes
        rx.await.unwrap().unwrap();
        drop(gc);

        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&content, b"async_data");
    }

    #[tokio::test]
    async fn test_group_commit_async_runtime_round_trip() {
        let (file, io, tmp) = make_writer();
        let gc = GroupCommitWriter::new_with_trace(
            file,
            io,
            &tokio::runtime::Handle::current(),
            IoTraceTag::default(),
        );

        let rx = gc.submit(b"async_runtime".to_vec(), true).unwrap();
        rx.await.unwrap().unwrap();

        gc.stop_and_drain().await.unwrap();
        gc.shutdown();

        let submit_err = gc.submit(b"after_shutdown".to_vec(), true).unwrap_err();
        assert_eq!(submit_err, "shutdown");

        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&content, b"async_runtime");
    }

    #[tokio::test]
    async fn test_group_commit_mixed_async() {
        let (file, io, tmp) = make_writer();
        let gc = Arc::new(GroupCommitWriter::new_with_thread_with_trace(
            file,
            io,
            IoTraceTag::default(),
        ));

        // First async write via .await
        let rx = gc.submit(b"sync".to_vec(), true).unwrap();
        rx.await.unwrap().unwrap();

        // Second async write via .await
        let rx = gc.submit(b"_async".to_vec(), true).unwrap();
        rx.await.unwrap().unwrap();

        drop(gc);

        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&content, b"sync_async");
    }

    #[tokio::test]
    async fn test_stop_and_drain_keeps_failed_state() {
        let (file, io, _tmp) = make_writer();
        let gc = GroupCommitWriter::new_with_thread_with_trace(file, io, IoTraceTag::default());

        gc.set_failed("writer failed".to_string());

        let err1 = gc.stop_and_drain().await.unwrap_err();
        let err2 = gc.stop_and_drain().await.unwrap_err();

        assert_eq!(err1, "writer failed");
        assert_eq!(err2, "writer failed");
    }

    #[tokio::test]
    async fn test_stop_and_drain_error_transitions_to_failed() {
        let (file, io, _tmp) = make_writer();
        let gc = GroupCommitWriter::new_with_thread_with_trace(file, io, IoTraceTag::default());

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        {
            let mut guard = gc.inner.lock();
            *guard = WriterInner::Active(WriterState {
                tx,
                drain_handle: DrainHandle::Thread(None),
            });
        }

        let err1 = gc.stop_and_drain().await.unwrap_err();
        let err2 = gc.stop_and_drain().await.unwrap_err();
        let submit_err = gc.submit(b"x".to_vec(), true).unwrap_err();

        assert_eq!(err1, "Thread handle already taken");
        assert_eq!(err2, "Thread handle already taken");
        assert_eq!(submit_err, "Thread handle already taken");
    }

    #[tokio::test]
    async fn test_group_commit_no_delay_count_short_circuits_delay() {
        let (file, io, _tmp) = make_writer();
        let gc = GroupCommitWriter::new_with_config_with_trace(
            file,
            io,
            &tokio::runtime::Handle::current(),
            IoTraceTag::default(),
            GroupCommitTuning {
                delay: Duration::from_secs(10),
                no_delay_count: 1,
            },
        );

        let rx = gc.submit(b"x".to_vec(), true).unwrap();
        // no_delay_count=1 should skip the long delay and finish quickly.
        tokio::time::timeout(Duration::from_secs(1), rx)
            .await
            .expect("group commit delay should be skipped at no_delay_count")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_stop_and_drain_interrupts_delay_window() {
        let (file, io, _tmp) = make_writer();
        let gc = GroupCommitWriter::new_with_config_with_trace(
            file,
            io,
            &tokio::runtime::Handle::current(),
            IoTraceTag::default(),
            GroupCommitTuning {
                delay: Duration::from_secs(10),
                no_delay_count: usize::MAX,
            },
        );

        let rx = gc.submit(b"delay".to_vec(), true).unwrap();

        // With a large delay and very high no_delay_count, this should still be pending.
        assert!(tokio::time::timeout(Duration::from_millis(50), rx)
            .await
            .is_err());

        // stop_and_drain should not block for the full delay window.
        tokio::time::timeout(Duration::from_secs(1), gc.stop_and_drain())
            .await
            .expect("stop_and_drain should interrupt delay window")
            .unwrap();
    }

    #[tokio::test]
    async fn test_group_commit_async_restart_accepts_new_writes() {
        let (file, io, tmp) = make_writer();
        let gc = GroupCommitWriter::new_with_trace(
            file,
            Arc::clone(&io),
            &tokio::runtime::Handle::current(),
            IoTraceTag::default(),
        );

        let rx = gc.submit(b"before".to_vec(), true).unwrap();
        rx.await.unwrap().unwrap();
        gc.stop_and_drain().await.unwrap();

        let reopened = DmaFile::open_append_buffered(tmp.path()).unwrap();
        gc.restart(
            reopened,
            Arc::clone(&io),
            Some(&tokio::runtime::Handle::current()),
        );

        let rx = gc.submit(b"_after".to_vec(), true).unwrap();
        rx.await.unwrap().unwrap();
        gc.stop_and_drain().await.unwrap();

        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&content, b"before_after");
    }

    #[tokio::test]
    async fn test_group_commit_async_delay_collects_follow_up_request() {
        let (file, io, tmp) = make_writer();
        let gc = GroupCommitWriter::new_with_config_with_trace(
            file,
            io,
            &tokio::runtime::Handle::current(),
            IoTraceTag::default(),
            GroupCommitTuning {
                delay: Duration::from_millis(200),
                no_delay_count: 2,
            },
        );

        let rx1 = gc.submit(b"first".to_vec(), true).unwrap();
        let rx2 = gc.submit(b"second".to_vec(), true).unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            rx1.await.unwrap().unwrap();
            rx2.await.unwrap().unwrap();
        })
        .await
        .expect("delay window should admit the second request");

        gc.stop_and_drain().await.unwrap();

        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&content, b"firstsecond");
    }
}
