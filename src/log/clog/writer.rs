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

//! Dedicated clog writer backed by `pwrite` on an `O_DIRECT|O_SYNC` fd.

use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::util::io::{AlignedBuf, DMA_ALIGNMENT};
use crate::{log_error, log_warn};

use super::group_buffer::{ClogGroupBuffer, FlushSlot};

#[inline]
fn align_up_u64(value: u64, alignment: u64) -> u64 {
    (value + alignment - 1) & !(alignment - 1)
}

pub(crate) type WalFatalAction = Arc<dyn Fn(String) + Send + Sync + 'static>;

enum WriterState {
    Active(tokio::task::JoinHandle<()>),
    Stopped,
    Failed(String),
}

pub(crate) struct ClogWriter {
    path: PathBuf,
    group_buffer: Arc<ClogGroupBuffer>,
    runtime: tokio::runtime::Handle,
    fatal_action: WalFatalAction,
    staging_size: usize,
    state: Mutex<WriterState>,
}

impl ClogWriter {
    pub(crate) fn new(
        path: PathBuf,
        group_buffer: Arc<ClogGroupBuffer>,
        runtime: &tokio::runtime::Handle,
        start_offset: u64,
        staging_size: usize,
        fatal_action: WalFatalAction,
    ) -> Result<Self, String> {
        let writer = Self {
            path,
            group_buffer,
            runtime: runtime.clone(),
            fatal_action,
            staging_size,
            state: Mutex::new(WriterState::Stopped),
        };
        writer.restart(start_offset)?;
        Ok(writer)
    }

    pub(crate) async fn stop_and_drain(&self, err: &str) -> Result<(), String> {
        self.group_buffer.set_shutdown();

        let handle = {
            let mut guard = self.state.lock();
            match std::mem::replace(&mut *guard, WriterState::Stopped) {
                WriterState::Active(handle) => handle,
                WriterState::Stopped => {
                    self.group_buffer.fail_all_pending(err);
                    return Ok(());
                }
                WriterState::Failed(msg) => {
                    return Err(msg);
                }
            }
        };

        if let Err(join_err) = handle.await {
            let msg = format!("clog writer task join error: {join_err}");
            *self.state.lock() = WriterState::Failed(msg.clone());
            self.group_buffer.fail_all_pending(&msg);
            return Err(msg);
        }

        self.group_buffer.fail_all_pending(err);
        *self.state.lock() = WriterState::Stopped;
        Ok(())
    }

    pub(crate) fn restart(&self, start_offset: u64) -> Result<(), String> {
        {
            let guard = self.state.lock();
            if matches!(&*guard, WriterState::Active(_)) {
                return Err("clog writer is already active".to_string());
            }
        }

        let file = open_writer_file(&self.path)
            .map_err(|e| format!("failed to open clog writer fd: {e}"))?;
        self.group_buffer.reset_for_restart();

        let group_buffer = Arc::clone(&self.group_buffer);
        let fatal_action = Arc::clone(&self.fatal_action);
        let staging_size = self.staging_size;
        let handle = self.runtime.spawn_blocking(move || {
            writer_loop(file, start_offset, group_buffer, staging_size, fatal_action)
        });

        *self.state.lock() = WriterState::Active(handle);
        Ok(())
    }

    pub(crate) fn shutdown(&self) {
        self.group_buffer.set_shutdown();
    }
}

impl Drop for ClogWriter {
    fn drop(&mut self) {
        // `close()`/`stop_and_drain()` is the strong cleanup path. Drop must stay
        // non-blocking to avoid deadlocks when called from within tokio test/runtime
        // contexts.
        self.group_buffer.fail_all_pending("clog writer dropped");

        let state = self.state.get_mut();
        if let WriterState::Active(handle) = std::mem::replace(state, WriterState::Stopped) {
            // Best effort: wake cancellation. For spawn_blocking tasks this does
            // not preempt active syscalls, but shutdown has already been signaled.
            handle.abort();
        }
    }
}

fn open_writer_file(path: &Path) -> std::io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    #[cfg(target_os = "linux")]
    let flags = libc::O_DIRECT | libc::O_SYNC;
    #[cfg(not(target_os = "linux"))]
    let flags = libc::O_SYNC;

    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .custom_flags(flags)
        .open(path)
}

fn writer_loop(
    file: File,
    mut file_offset: u64,
    group_buffer: Arc<ClogGroupBuffer>,
    staging_size: usize,
    fatal_action: WalFatalAction,
) {
    let mut staging = AlignedBuf::zeroed(staging_size, DMA_ALIGNMENT);
    let mut batch: Vec<FlushSlot> = Vec::with_capacity(group_buffer.max_slots());

    loop {
        if group_buffer.is_shutdown() {
            return;
        }

        if group_buffer.drain_cancelled_head("clog reservation cancelled") {
            continue;
        }

        let Some(meta) = group_buffer.collect_filled_batch(&mut batch) else {
            group_buffer.wait_for_data_or_shutdown();
            continue;
        };

        let copy_size = group_buffer.copy_to_staging(&batch, &mut staging);
        if copy_size != meta.data_size {
            let err = format!(
                "clog writer batch copy mismatch: expected {}, got {}",
                meta.data_size, copy_size
            );
            group_buffer.complete_batch(&batch, Err(err.clone()));
            group_buffer.fail_all_pending(&err);
            (fatal_action)(err);
            return;
        }

        if copy_size > 0 {
            let padded = align_up_u64(copy_size as u64, DMA_ALIGNMENT as u64) as usize;
            staging[copy_size..padded].fill(0);

            match pwrite_all(&file, &staging[..padded], file_offset) {
                Ok(written) if written == padded => {
                    file_offset += padded as u64;
                }
                Ok(written) => {
                    let err =
                        format!("clog writer short write: expected {padded}, wrote {written}");
                    log_error!("{err}");
                    group_buffer.complete_batch(&batch, Err(err.clone()));
                    group_buffer.fail_all_pending(&err);
                    (fatal_action)(err);
                    return;
                }
                Err(e) => {
                    let err = format!("clog writer pwrite error: {e}");
                    log_error!("{err}");
                    group_buffer.complete_batch(&batch, Err(err.clone()));
                    group_buffer.fail_all_pending(&err);
                    (fatal_action)(err);
                    return;
                }
            }
        }

        group_buffer.complete_batch(&batch, Ok(()));
    }
}

fn pwrite_all(file: &File, mut buf: &[u8], mut offset: u64) -> std::io::Result<usize> {
    let mut written = 0usize;
    let fd = file.as_raw_fd();

    while !buf.is_empty() {
        // SAFETY: `fd` is a valid opened file descriptor. `buf` pointer remains
        // valid for `buf.len()` bytes for the duration of the call.
        let n = unsafe { libc::pwrite(fd, buf.as_ptr().cast(), buf.len(), offset as libc::off_t) };

        if n < 0 {
            return Err(std::io::Error::last_os_error());
        }
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "pwrite returned 0",
            ));
        }

        let n = n as usize;
        written += n;
        offset += n as u64;
        buf = &buf[n..];
    }

    Ok(written)
}

pub(crate) fn default_fatal_action() -> WalFatalAction {
    Arc::new(|err| {
        log_warn!("fatal clog writer error: {err}");
        std::process::abort();
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_writer_start_stop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.clog");
        let gb = ClogGroupBuffer::new(4096 * 4, 64);
        let runtime = tokio::runtime::Handle::current();

        let writer = ClogWriter::new(path, gb, &runtime, 0, 4096 * 4, Arc::new(|_| {})).unwrap();

        writer
            .stop_and_drain("test stop")
            .await
            .expect("stop should succeed");
    }
}
