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

//! Group commit writer for batching fsync operations.
//!
//! Multiple concurrent callers submit serialized records via a channel.
//! A blocking task (via `spawn_blocking`) collects a batch, writes, and
//! issues a single fsync for the entire batch — amortizing the cost
//! of durable writes across many transactions.
//!
//! ## Design
//!
//! ```text
//! Caller threads          Group Commit (spawn_blocking)
//! ─────────────          ──────────────────────────────
//! tx.send(record1) ────→ recv batch (record1, record2, record3)
//! tx.send(record2) ────→   write_all(record1..3)
//! tx.send(record3) ────→   flush() + fsync()
//!                   ←──── notify(ok1), notify(ok2), notify(ok3)
//! ```

use std::fs::File;
use std::io::{BufWriter, Write};

/// A request submitted to the group commit writer.
struct GroupCommitRequest {
    /// Serialized record data to write.
    data: Vec<u8>,
    /// Whether this request requires fsync.
    sync: bool,
    /// Channel to notify the caller when the write (+ optional fsync) is complete.
    reply: tokio::sync::oneshot::Sender<Result<(), String>>,
}

/// Group commit writer that batches fsync operations.
///
/// Callers submit serialized records via `submit()` which returns a
/// `tokio::sync::oneshot::Receiver`. The caller either `.await`s it (async)
/// or uses `block_on_sync()` (sync). A blocking task on the I/O runtime's
/// thread pool collects pending records, writes them in one batch, then
/// notifies all callers.
pub struct GroupCommitWriter {
    /// Send end of the crossbeam channel. Wrapped in Option so Drop can close
    /// it to signal shutdown.
    sender: parking_lot::Mutex<Option<crossbeam_channel::Sender<GroupCommitRequest>>>,
}

impl GroupCommitWriter {
    /// Create a new group commit writer with the given buffered file writer.
    ///
    /// Spawns a blocking task on `handle` that processes write requests.
    /// The blocking task runs on the runtime's blocking thread pool,
    /// separate from async worker threads — preventing deadlocks with
    /// sync callers.
    pub fn new(writer: BufWriter<File>, handle: &tokio::runtime::Handle) -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();

        handle.spawn_blocking(move || {
            Self::writer_loop(writer, rx);
        });

        Self {
            sender: parking_lot::Mutex::new(Some(tx)),
        }
    }

    /// Create a GroupCommitWriter backed by a plain `std::thread`.
    ///
    /// Unlike `new()` which uses `spawn_blocking` on a tokio runtime,
    /// this spawns a regular OS thread. Use this in test code or when
    /// no tokio runtime handle is available.
    pub fn new_with_thread(writer: BufWriter<File>) -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();

        std::thread::spawn(move || {
            Self::writer_loop(writer, rx);
        });

        Self {
            sender: parking_lot::Mutex::new(Some(tx)),
        }
    }

    /// Submit a serialized record for writing, optionally with fsync.
    ///
    /// Returns a `tokio::sync::oneshot::Receiver` that resolves when the write
    /// (+ optional fsync) is complete. The caller `.await`s or uses `block_on_sync()`
    /// to wait for the result.
    pub fn submit(
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

        self.send_request(request)?;
        Ok(reply_rx)
    }

    /// Send a request to the writer task via the crossbeam channel.
    fn send_request(&self, request: GroupCommitRequest) -> Result<(), String> {
        let guard = self.sender.lock();
        let sender = guard
            .as_ref()
            .ok_or_else(|| "Group commit writer shut down".to_string())?;
        sender
            .send(request)
            .map_err(|_| "Group commit writer shut down".to_string())
    }

    /// The writer loop — runs on a blocking thread (via `spawn_blocking`).
    ///
    /// Waits for requests on the crossbeam channel, batches them, writes,
    /// then notifies callers. The loop exits when the channel is closed.
    fn writer_loop(
        mut writer: BufWriter<File>,
        rx: crossbeam_channel::Receiver<GroupCommitRequest>,
    ) {
        let mut batch: Vec<GroupCommitRequest> = Vec::with_capacity(64);

        loop {
            // Block waiting for first request
            match rx.recv() {
                Ok(first) => batch.push(first),
                Err(_) => return, // Channel closed — shutdown
            }

            // Drain additional queued requests (non-blocking)
            while let Ok(req) = rx.try_recv() {
                batch.push(req);
            }

            // Write the batch
            let write_error = Self::write_batch(&mut writer, &batch);

            // Notify all callers
            let result = match &write_error {
                Some(e) => Err(e.clone()),
                None => Ok(()),
            };
            for req in batch.drain(..) {
                let _ = req.reply.send(result.clone());
            }
        }
    }

    /// Write all records in the batch, flush, and optionally fsync.
    fn write_batch(writer: &mut BufWriter<File>, batch: &[GroupCommitRequest]) -> Option<String> {
        // Write all records
        for req in batch {
            if let Err(e) = writer.write_all(&req.data) {
                return Some(format!("Write error: {e}"));
            }
        }

        // Flush once for the entire batch
        if let Err(e) = writer.flush() {
            return Some(format!("Flush error: {e}"));
        }

        // Fsync once if any request in the batch requires it
        let needs_sync = batch.iter().any(|req| req.sync);
        if needs_sync {
            if let Err(e) = writer.get_ref().sync_data() {
                return Some(format!("Sync error: {e}"));
            }
        }

        None
    }

    /// Close the channel, signalling the writer loop to exit.
    ///
    /// Call this before dropping the runtime that hosts the writer task,
    /// otherwise the runtime drop will block waiting for the writer loop
    /// which is stuck on `rx.recv()` with the sender still alive.
    pub fn shutdown(&self) {
        self.sender.lock().take();
    }
}

impl Drop for GroupCommitWriter {
    fn drop(&mut self) {
        // Close the channel — the writer loop will exit on next recv().
        // For spawn_blocking: the runtime's shutdown waits for blocking tasks.
        // For std::thread (test): the thread exits when recv returns Err.
        self.sender.lock().take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    fn make_writer() -> (BufWriter<File>, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let file = tmp.reopen().unwrap();
        (BufWriter::new(file), tmp)
    }

    #[tokio::test]
    async fn test_group_commit_single_write() {
        let (writer, tmp) = make_writer();
        let gc = GroupCommitWriter::new_with_thread(writer);

        let rx = gc.submit(b"hello".to_vec(), true).unwrap();
        crate::io::block_on_sync(rx).unwrap().unwrap();
        drop(gc);

        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&content, b"hello");
    }

    #[tokio::test]
    async fn test_group_commit_multiple_sequential() {
        let (writer, tmp) = make_writer();
        let gc = GroupCommitWriter::new_with_thread(writer);

        for data in [b"aaa".as_slice(), b"bbb", b"ccc"] {
            let rx = gc.submit(data.to_vec(), true).unwrap();
            crate::io::block_on_sync(rx).unwrap().unwrap();
        }
        drop(gc);

        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&content, b"aaabbbccc");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_group_commit_concurrent() {
        let (writer, tmp) = make_writer();
        let gc = Arc::new(GroupCommitWriter::new_with_thread(writer));

        let mut handles = vec![];
        for i in 0..10 {
            let w = Arc::clone(&gc);
            handles.push(tokio::task::spawn_blocking(move || {
                let data = format!("{i:02}");
                let rx = w.submit(data.into_bytes(), true).unwrap();
                crate::io::block_on_sync(rx).unwrap().unwrap();
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
        let (writer, tmp) = make_writer();
        let gc = GroupCommitWriter::new_with_thread(writer);

        let rx = gc.submit(b"data".to_vec(), false).unwrap();
        crate::io::block_on_sync(rx).unwrap().unwrap();
        drop(gc);

        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&content, b"data");
    }

    #[tokio::test]
    async fn test_group_commit_submit_async() {
        let (writer, tmp) = make_writer();
        let gc = GroupCommitWriter::new_with_thread(writer);

        // submit returns a future
        let rx = gc.submit(b"async_data".to_vec(), true).unwrap();

        // Await the future — resolves when fsync completes
        rx.await.unwrap().unwrap();
        drop(gc);

        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&content, b"async_data");
    }

    #[tokio::test]
    async fn test_group_commit_mixed_sync_async() {
        let (writer, tmp) = make_writer();
        let gc = Arc::new(GroupCommitWriter::new_with_thread(writer));

        // Sync-style write via block_on_sync
        let rx = gc.submit(b"sync".to_vec(), true).unwrap();
        crate::io::block_on_sync(rx).unwrap().unwrap();

        // Async-style write via .await
        let rx = gc.submit(b"_async".to_vec(), true).unwrap();
        rx.await.unwrap().unwrap();

        drop(gc);

        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&content, b"sync_async");
    }
}
