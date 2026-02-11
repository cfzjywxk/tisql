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

//! io_uring-based I/O service with dedicated event loop thread.
//!
//! ## Design
//!
//! A single dedicated thread owns the io_uring ring. Callers submit requests
//! via a crossbeam channel and receive results through `IoFuture<T>`:
//!
//! - **Async callers**: `.await` the IoFuture in tokio tasks
//! - **Sync callers**: `.wait()` using `blocking_recv()` (for iterators,
//!   flush/compaction on spawn_blocking threads)
//!
//! ## O_DIRECT Alignment
//!
//! All reads/writes are internally aligned to `DMA_ALIGNMENT` (4096 bytes).
//! For reads, the caller gets back an `AlignedBuf` containing the exact bytes
//! requested (the service reads a larger aligned range and slices the result).

use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::aligned_buf::{align_down, align_up, AlignedBuf, DMA_ALIGNMENT};
use super::dma_file::DmaFile;

/// Result type for IO operations — contains either data or an error message.
type IoResult<T> = Result<T, String>;

/// A future representing a pending I/O operation.
///
/// Supports both async (`.await`) and sync (`.wait()`) consumption,
/// following the same dual-mode pattern as `ClogFsyncFuture`.
pub struct IoFuture<T: Send + 'static> {
    rx: tokio::sync::oneshot::Receiver<IoResult<T>>,
}

impl<T: Send + 'static> IoFuture<T> {
    /// Block until the I/O operation completes (for sync callers).
    ///
    /// Use this in `spawn_blocking` threads, dedicated I/O threads,
    /// or iterator chains that haven't been converted to async yet.
    ///
    /// When called from within a tokio runtime, uses `block_in_place`
    /// to avoid the "cannot block from within a runtime" panic.
    pub fn wait(self) -> IoResult<T> {
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| {
                self.rx
                    .blocking_recv()
                    .map_err(|_| "IoService reply channel closed".to_string())?
            })
        } else {
            self.rx
                .blocking_recv()
                .map_err(|_| "IoService reply channel closed".to_string())?
        }
    }
}

impl<T: Send + 'static> std::future::Future for IoFuture<T> {
    type Output = IoResult<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(Err("IoService reply channel closed".to_string())),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// An IO operation to submit to the ring.
enum IoOp {
    ReadAt {
        fd: RawFd,
        offset: u64,
        len: usize,
        reply: tokio::sync::oneshot::Sender<IoResult<AlignedBuf>>,
    },
    WriteAt {
        fd: RawFd,
        offset: u64,
        buf: AlignedBuf,
        reply: tokio::sync::oneshot::Sender<IoResult<usize>>,
    },
    Fsync {
        fd: RawFd,
        reply: tokio::sync::oneshot::Sender<IoResult<()>>,
    },
}

/// io_uring-backed I/O service.
///
/// Spawns a dedicated thread with an io_uring ring. All SST file I/O
/// (reads, writes, fsyncs) goes through this service.
pub struct IoService {
    tx: crossbeam_channel::Sender<IoOp>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl std::fmt::Debug for IoService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("IoService")
    }
}

impl IoService {
    /// Create a new IoService with the given ring size.
    ///
    /// `ring_size` determines the io_uring submission queue depth (e.g., 256).
    pub fn new(ring_size: u32) -> Result<Arc<Self>, String> {
        let (tx, rx) = crossbeam_channel::unbounded::<IoOp>();

        let thread = std::thread::Builder::new()
            .name("tisql-io-uring".into())
            .spawn(move || {
                if let Err(e) = io_thread_main(rx, ring_size) {
                    tracing::error!("io_uring thread failed: {e}");
                }
            })
            .map_err(|e| format!("failed to spawn io thread: {e}"))?;

        Ok(Arc::new(Self {
            tx,
            thread: Some(thread),
        }))
    }

    /// Read `len` bytes at `offset` from the file.
    ///
    /// Returns an `IoFuture` that can be `.await`ed or `.wait()`ed.
    pub fn read_at(&self, file: &DmaFile, offset: u64, len: usize) -> IoFuture<AlignedBuf> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        if self
            .tx
            .send(IoOp::ReadAt {
                fd: file.fd(),
                offset,
                len,
                reply: reply_tx,
            })
            .is_err()
        {
            let (err_tx, err_rx) = tokio::sync::oneshot::channel();
            let _ = err_tx.send(Err("IoService shut down".into()));
            return IoFuture { rx: err_rx };
        }
        IoFuture { rx: reply_rx }
    }

    /// Write `buf` at `offset` to the file.
    ///
    /// Returns an `IoFuture` with the number of bytes written.
    pub fn write_at(&self, file: &DmaFile, offset: u64, buf: AlignedBuf) -> IoFuture<usize> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        if self
            .tx
            .send(IoOp::WriteAt {
                fd: file.fd(),
                offset,
                buf,
                reply: reply_tx,
            })
            .is_err()
        {
            let (err_tx, err_rx) = tokio::sync::oneshot::channel();
            let _ = err_tx.send(Err("IoService shut down".into()));
            return IoFuture { rx: err_rx };
        }
        IoFuture { rx: reply_rx }
    }

    /// Fsync the file.
    ///
    /// Returns an `IoFuture` that completes when the fsync is done.
    pub fn fsync(&self, file: &DmaFile) -> IoFuture<()> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        if self
            .tx
            .send(IoOp::Fsync {
                fd: file.fd(),
                reply: reply_tx,
            })
            .is_err()
        {
            let (err_tx, err_rx) = tokio::sync::oneshot::channel();
            let _ = err_tx.send(Err("IoService shut down".into()));
            return IoFuture { rx: err_rx };
        }
        IoFuture { rx: reply_rx }
    }
}

impl Drop for IoService {
    fn drop(&mut self) {
        // Dropping tx causes recv() in the IO thread to return Err, which exits the loop.
        // We need to drop tx explicitly first by replacing the sender.
        // Since we can't easily drop tx without consuming self, we just join the thread.
        // The thread will exit when all senders are dropped (which happens when IoService is dropped).
        if let Some(thread) = self.thread.take() {
            // tx will be dropped after this Drop impl returns, so we need to
            // not join here — the thread will exit asynchronously.
            // But for clean shutdown we should wait. Let's use a timeout approach:
            // drop the sender first by forgetting about it, then join.
            drop(self.tx.clone()); // no-op, original tx still alive
                                   // The thread will exit when all Senders are dropped. Since we're in Drop,
                                   // self.tx will be dropped after this method returns. We can't easily
                                   // synchronize that. Just detach the thread.
            let _ = thread; // Thread is detached.
        }
    }
}

// ============================================================================
// IO Thread Main Loop
// ============================================================================

/// Main loop for the dedicated io_uring thread.
///
/// Receives IoOp requests from the channel, submits them to io_uring,
/// reaps completions, and notifies callers via oneshot reply channels.
fn io_thread_main(rx: crossbeam_channel::Receiver<IoOp>, ring_size: u32) -> Result<(), String> {
    let mut ring = io_uring::IoUring::new(ring_size)
        .map_err(|e| format!("io_uring::IoUring::new failed: {e}"))?;

    // Pending operations: maps user_data -> completion handler
    let mut pending: Vec<Option<PendingOp>> = Vec::new();
    let mut next_id: u64 = 0;

    loop {
        // Wait for first request (blocking)
        let first = match rx.recv() {
            Ok(op) => op,
            Err(_) => break, // Channel closed — shut down
        };

        // Collect batch: drain all pending requests
        let mut batch = vec![first];
        while let Ok(op) = rx.try_recv() {
            batch.push(op);
        }

        // Submit each operation to io_uring
        for op in batch {
            let id = next_id;
            next_id += 1;

            match op {
                IoOp::ReadAt {
                    fd,
                    offset,
                    len,
                    reply,
                } => {
                    // Align offset and length for O_DIRECT
                    let aligned_offset = align_down(offset, DMA_ALIGNMENT as u64);
                    let aligned_end = align_up(offset + len as u64, DMA_ALIGNMENT as u64);
                    let aligned_len = (aligned_end - aligned_offset) as usize;

                    let buf = AlignedBuf::zeroed(aligned_len, DMA_ALIGNMENT);
                    let buf_ptr = buf.as_ptr() as *mut u8;

                    let sqe = io_uring::opcode::Read::new(
                        io_uring::types::Fd(fd),
                        buf_ptr,
                        aligned_len as u32,
                    )
                    .offset(aligned_offset)
                    .build()
                    .user_data(id);

                    // Ensure pending vec is large enough
                    while pending.len() <= id as usize {
                        pending.push(None);
                    }
                    pending[id as usize] = Some(PendingOp::Read {
                        buf,
                        requested_offset: offset,
                        aligned_offset,
                        requested_len: len,
                        reply,
                    });

                    // SAFETY: buf stays alive because it's stored in pending[id].
                    // The io_uring read writes into the buffer pointer we provided.
                    // We don't touch buf until after reaping the completion.
                    unsafe {
                        ring.submission()
                            .push(&sqe)
                            .map_err(|e| format!("SQ full: {e}"))?;
                    }
                }
                IoOp::WriteAt {
                    fd,
                    offset,
                    buf,
                    reply,
                } => {
                    let buf_ptr = buf.as_ptr();
                    let buf_len = buf.len();

                    let sqe = io_uring::opcode::Write::new(
                        io_uring::types::Fd(fd),
                        buf_ptr,
                        buf_len as u32,
                    )
                    .offset(offset)
                    .build()
                    .user_data(id);

                    while pending.len() <= id as usize {
                        pending.push(None);
                    }
                    pending[id as usize] = Some(PendingOp::Write {
                        _buf: buf, // Keep alive until completion
                        reply,
                    });

                    unsafe {
                        ring.submission()
                            .push(&sqe)
                            .map_err(|e| format!("SQ full: {e}"))?;
                    }
                }
                IoOp::Fsync { fd, reply } => {
                    let sqe = io_uring::opcode::Fsync::new(io_uring::types::Fd(fd))
                        .build()
                        .user_data(id);

                    while pending.len() <= id as usize {
                        pending.push(None);
                    }
                    pending[id as usize] = Some(PendingOp::Fsync { reply });

                    unsafe {
                        ring.submission()
                            .push(&sqe)
                            .map_err(|e| format!("SQ full: {e}"))?;
                    }
                }
            }
        }

        // Submit all and wait for at least 1 completion
        ring.submit_and_wait(1)
            .map_err(|e| format!("submit_and_wait failed: {e}"))?;

        // Reap all available completions
        for cqe in ring.completion() {
            let id = cqe.user_data() as usize;
            let result = cqe.result();

            if let Some(op) = pending.get_mut(id).and_then(|slot| slot.take()) {
                match op {
                    PendingOp::Read {
                        buf,
                        requested_offset,
                        aligned_offset,
                        requested_len,
                        reply,
                    } => {
                        if result < 0 {
                            let _ = reply.send(Err(format!(
                                "io_uring read failed: {}",
                                std::io::Error::from_raw_os_error(-result)
                            )));
                        } else {
                            // Extract the exact range the caller requested
                            let skip = (requested_offset - aligned_offset) as usize;
                            let end = skip + requested_len;
                            if end <= buf.len() {
                                let result_buf =
                                    AlignedBuf::from_slice(&buf[skip..end], DMA_ALIGNMENT);
                                let _ = reply.send(Ok(result_buf));
                            } else {
                                // Short read — return what we got
                                let available = buf.len().saturating_sub(skip);
                                let actual_len = requested_len.min(available);
                                let result_buf = AlignedBuf::from_slice(
                                    &buf[skip..skip + actual_len],
                                    DMA_ALIGNMENT,
                                );
                                let _ = reply.send(Ok(result_buf));
                            }
                        }
                    }
                    PendingOp::Write { _buf, reply } => {
                        if result < 0 {
                            let _ = reply.send(Err(format!(
                                "io_uring write failed: {}",
                                std::io::Error::from_raw_os_error(-result)
                            )));
                        } else {
                            let _ = reply.send(Ok(result as usize));
                        }
                    }
                    PendingOp::Fsync { reply } => {
                        if result < 0 {
                            let _ = reply.send(Err(format!(
                                "io_uring fsync failed: {}",
                                std::io::Error::from_raw_os_error(-result)
                            )));
                        } else {
                            let _ = reply.send(Ok(()));
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

/// A pending IO operation awaiting completion from io_uring.
enum PendingOp {
    Read {
        buf: AlignedBuf,
        requested_offset: u64,
        aligned_offset: u64,
        requested_len: usize,
        reply: tokio::sync::oneshot::Sender<IoResult<AlignedBuf>>,
    },
    Write {
        _buf: AlignedBuf, // Keep alive until io_uring completes
        reply: tokio::sync::oneshot::Sender<IoResult<usize>>,
    },
    Fsync {
        reply: tokio::sync::oneshot::Sender<IoResult<()>>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::DmaFile;
    use tempfile::tempdir;

    #[test]
    fn test_io_service_create() {
        let service = IoService::new(32);
        assert!(service.is_ok(), "IoService creation should succeed");
    }

    #[test]
    fn test_io_service_write_and_read() {
        let dir = tempdir().unwrap();
        let io = IoService::new(32).unwrap();
        let path = dir.path().join("test_io.dat");

        // Write aligned data
        let file = DmaFile::open_write(&path).unwrap();
        let data = AlignedBuf::from_slice(&[0xAA; 4096], DMA_ALIGNMENT);
        io.write_at(&file, 0, data).wait().unwrap();
        io.fsync(&file).wait().unwrap();
        drop(file);

        // Read it back
        let file = DmaFile::open_read(&path).unwrap();
        let buf = io.read_at(&file, 0, 4096).wait().unwrap();
        assert_eq!(buf.len(), 4096);
        assert!(buf.iter().all(|&b| b == 0xAA));
    }

    #[test]
    fn test_io_service_unaligned_read() {
        let dir = tempdir().unwrap();
        let io = IoService::new(32).unwrap();
        let path = dir.path().join("test_unaligned.dat");

        // Write 8192 bytes of known data
        let file = DmaFile::open_write(&path).unwrap();
        let mut data = AlignedBuf::zeroed(8192, DMA_ALIGNMENT);
        for (i, byte) in data.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
        io.write_at(&file, 0, data).wait().unwrap();
        io.fsync(&file).wait().unwrap();
        drop(file);

        // Read 100 bytes starting at offset 1000 (unaligned)
        let file = DmaFile::open_read(&path).unwrap();
        let buf = io.read_at(&file, 1000, 100).wait().unwrap();
        assert_eq!(buf.len(), 100);
        for (i, &byte) in buf.iter().enumerate() {
            assert_eq!(byte, ((1000 + i) % 256) as u8);
        }
    }

    #[test]
    fn test_io_service_multiple_writes() {
        let dir = tempdir().unwrap();
        let io = IoService::new(32).unwrap();
        let path = dir.path().join("test_multi_write.dat");

        // Write two aligned blocks
        let file = DmaFile::open_write(&path).unwrap();
        let block1 = AlignedBuf::from_slice(&[0x11; 4096], DMA_ALIGNMENT);
        let block2 = AlignedBuf::from_slice(&[0x22; 4096], DMA_ALIGNMENT);
        io.write_at(&file, 0, block1).wait().unwrap();
        io.write_at(&file, 4096, block2).wait().unwrap();
        io.fsync(&file).wait().unwrap();
        drop(file);

        // Read both back
        let file = DmaFile::open_read(&path).unwrap();
        let buf1 = io.read_at(&file, 0, 4096).wait().unwrap();
        let buf2 = io.read_at(&file, 4096, 4096).wait().unwrap();
        assert!(buf1.iter().all(|&b| b == 0x11));
        assert!(buf2.iter().all(|&b| b == 0x22));
    }
}
