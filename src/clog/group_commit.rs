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
//! A dedicated writer thread collects a batch, writes all records, and
//! issues a single fsync for the entire batch — amortizing the cost of
//! durable writes across many transactions.
//!
//! ## Design
//!
//! ```text
//! Caller threads          Group Commit Writer Thread
//! ─────────────          ───────────────────────────
//! tx.send(record1) ────→ recv batch (record1, record2, record3)
//! tx.send(record2) ────→   write_all(record1)
//! tx.send(record3) ────→   write_all(record2)
//!                          write_all(record3)
//!                          single flush() + fsync()
//!                   ←──── notify(ok1), notify(ok2), notify(ok3)
//! ```

use std::fs::File;
use std::io::{BufWriter, Write};
use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Sender};

/// Reply channel supporting both synchronous (crossbeam) and asynchronous (tokio oneshot) callers.
///
/// The writer thread sends the result through whichever variant was provided.
/// `tokio::sync::oneshot::Sender::send()` does NOT require a tokio runtime,
/// so it's safe to call from the std::thread writer loop.
enum ReplyChannel {
    Sync(crossbeam_channel::Sender<Result<(), String>>),
    Async(tokio::sync::oneshot::Sender<Result<(), String>>),
}

/// A request submitted to the group commit writer.
struct GroupCommitRequest {
    /// Serialized record data to write.
    data: Vec<u8>,
    /// Whether this request requires fsync.
    sync: bool,
    /// Channel to notify the caller when the write (+ optional fsync) is complete.
    reply: ReplyChannel,
}

/// Group commit writer that batches fsync operations.
///
/// Callers submit serialized records via `submit()`. A background thread
/// collects pending records, writes them all, then issues a single fsync
/// and notifies all callers.
pub struct GroupCommitWriter {
    /// Send end of the channel. Wrapped in Option so Drop can take it
    /// before joining the writer thread (otherwise the join deadlocks
    /// because the thread is blocked on recv).
    sender: parking_lot::Mutex<Option<Sender<GroupCommitRequest>>>,
    writer_handle: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

impl GroupCommitWriter {
    /// Create a new group commit writer with the given buffered file writer.
    ///
    /// Spawns a background thread that processes write requests.
    pub fn new(writer: BufWriter<File>) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded();

        let handle = thread::Builder::new()
            .name("group-commit".into())
            .spawn(move || {
                Self::writer_loop(writer, receiver);
            })
            .expect("Failed to spawn group commit writer thread");

        Self {
            sender: parking_lot::Mutex::new(Some(sender)),
            writer_handle: parking_lot::Mutex::new(Some(handle)),
        }
    }

    /// Submit a serialized record for writing with fsync.
    ///
    /// Blocks until the record has been written and fsync'd (as part of a batch).
    #[cfg(test)]
    pub fn submit(&self, data: Vec<u8>) -> Result<(), String> {
        self.submit_inner(data, true)
    }

    /// Submit a serialized record for writing, optionally with fsync.
    pub fn submit_with_sync(&self, data: Vec<u8>, sync: bool) -> Result<(), String> {
        self.submit_inner(data, sync)
    }

    fn submit_inner(&self, data: Vec<u8>, sync: bool) -> Result<(), String> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);

        let request = GroupCommitRequest {
            data,
            sync,
            reply: ReplyChannel::Sync(reply_tx),
        };

        self.send_request(request)?;

        // Block until the write is complete
        reply_rx
            .recv()
            .map_err(|_| "Group commit writer dropped reply channel".to_string())?
    }

    /// Submit a serialized record for writing (async variant).
    ///
    /// Returns a `tokio::sync::oneshot::Receiver` that resolves when the write
    /// (+ optional fsync) is complete. The caller `.await`s this receiver,
    /// yielding the thread while the writer thread performs I/O.
    pub fn submit_async(
        &self,
        data: Vec<u8>,
        sync: bool,
    ) -> std::result::Result<tokio::sync::oneshot::Receiver<Result<(), String>>, String> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();

        let request = GroupCommitRequest {
            data,
            sync,
            reply: ReplyChannel::Async(reply_tx),
        };

        self.send_request(request)?;
        Ok(reply_rx)
    }

    /// Send a request to the writer thread via the channel.
    fn send_request(&self, request: GroupCommitRequest) -> Result<(), String> {
        let guard = self.sender.lock();
        let sender = guard
            .as_ref()
            .ok_or_else(|| "Group commit writer shut down".to_string())?;
        sender
            .send(request)
            .map_err(|_| "Group commit writer shut down".to_string())
    }

    /// The writer thread's main loop.
    fn writer_loop(mut writer: BufWriter<File>, receiver: Receiver<GroupCommitRequest>) {
        let mut batch: Vec<GroupCommitRequest> = Vec::with_capacity(64);

        loop {
            // Wait for the first request (blocking)
            match receiver.recv() {
                Ok(first) => batch.push(first),
                Err(_) => return, // Channel disconnected - shut down
            }

            // Drain any additional queued requests (non-blocking)
            while let Ok(req) = receiver.try_recv() {
                batch.push(req);
            }

            // Write all records in the batch
            let mut write_error: Option<String> = None;
            for req in &batch {
                if write_error.is_none() {
                    if let Err(e) = writer.write_all(&req.data) {
                        write_error = Some(format!("Write error: {e}"));
                    }
                }
            }

            // Check if any request in the batch requires sync
            let needs_sync = batch.iter().any(|req| req.sync);

            // Flush + fsync once for the entire batch
            if write_error.is_none() {
                if let Err(e) = writer.flush() {
                    write_error = Some(format!("Flush error: {e}"));
                }
            }

            if write_error.is_none() && needs_sync {
                if let Err(e) = writer.get_ref().sync_data() {
                    write_error = Some(format!("Sync error: {e}"));
                }
            }

            // Notify all callers
            let result = match &write_error {
                Some(e) => Err(e.clone()),
                None => Ok(()),
            };

            for req in batch.drain(..) {
                match req.reply {
                    ReplyChannel::Sync(tx) => {
                        let _ = tx.send(result.clone());
                    }
                    ReplyChannel::Async(tx) => {
                        let _ = tx.send(result.clone());
                    }
                }
            }
        }
    }
}

impl Drop for GroupCommitWriter {
    fn drop(&mut self) {
        // Drop the sender first to disconnect the channel,
        // which unblocks the writer thread's recv() call.
        self.sender.lock().take();

        // Now join the writer thread.
        let mut handle = self.writer_handle.lock();
        if let Some(h) = handle.take() {
            let _ = h.join();
        }
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

    #[test]
    fn test_group_commit_single_write() {
        let (writer, tmp) = make_writer();
        let gc = GroupCommitWriter::new(writer);

        gc.submit(b"hello".to_vec()).unwrap();
        drop(gc);

        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&content, b"hello");
    }

    #[test]
    fn test_group_commit_multiple_sequential() {
        let (writer, tmp) = make_writer();
        let gc = GroupCommitWriter::new(writer);

        gc.submit(b"aaa".to_vec()).unwrap();
        gc.submit(b"bbb".to_vec()).unwrap();
        gc.submit(b"ccc".to_vec()).unwrap();
        drop(gc);

        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&content, b"aaabbbccc");
    }

    #[test]
    fn test_group_commit_concurrent() {
        let (writer, tmp) = make_writer();
        let gc = Arc::new(GroupCommitWriter::new(writer));

        let mut handles = vec![];
        for i in 0..10 {
            let w = Arc::clone(&gc);
            handles.push(thread::spawn(move || {
                let data = format!("{i:02}");
                w.submit(data.into_bytes()).unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
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

    #[test]
    fn test_group_commit_no_sync() {
        let (writer, tmp) = make_writer();
        let gc = GroupCommitWriter::new(writer);

        gc.submit_with_sync(b"data".to_vec(), false).unwrap();
        drop(gc);

        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&content, b"data");
    }

    #[tokio::test]
    async fn test_group_commit_submit_async() {
        let (writer, tmp) = make_writer();
        let gc = GroupCommitWriter::new(writer);

        // submit_async returns immediately with a future
        let rx = gc.submit_async(b"async_data".to_vec(), true).unwrap();

        // Await the future — resolves when fsync completes
        rx.await.unwrap().unwrap();
        drop(gc);

        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&content, b"async_data");
    }

    #[tokio::test]
    async fn test_group_commit_mixed_sync_async() {
        let (writer, tmp) = make_writer();
        let gc = Arc::new(GroupCommitWriter::new(writer));

        // Sync write
        gc.submit(b"sync".to_vec()).unwrap();

        // Async write
        let rx = gc.submit_async(b"_async".to_vec(), true).unwrap();
        rx.await.unwrap().unwrap();

        drop(gc);

        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&content, b"sync_async");
    }
}
