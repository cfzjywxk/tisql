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
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::util::io::{AlignedBuf, DMA_ALIGNMENT};
use crate::{log_debug, log_error, log_warn};

use super::group_buffer::{ClogGroupBuffer, FlushSlot};

#[inline]
fn align_up_u64(value: u64, alignment: u64) -> u64 {
    (value + alignment - 1) & !(alignment - 1)
}

pub(crate) type WalFatalAction = Arc<dyn Fn(String) + Send + Sync + 'static>;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ClogWriterTuning {
    pub spin_before_park_iters: u32,
    pub micro_batch_delay: Duration,
    pub micro_batch_no_delay_count: usize,
    pub stats_log_interval: Duration,
    #[cfg(test)]
    pub hook_delay_wait_start: Option<fn()>,
    #[cfg(test)]
    pub hook_batch_collected: Option<fn(usize)>,
}

impl Default for ClogWriterTuning {
    fn default() -> Self {
        Self {
            spin_before_park_iters: 0,
            micro_batch_delay: Duration::ZERO,
            micro_batch_no_delay_count: 16,
            stats_log_interval: Duration::ZERO,
            #[cfg(test)]
            hook_delay_wait_start: None,
            #[cfg(test)]
            hook_batch_collected: None,
        }
    }
}

#[derive(Debug)]
struct WriterPerfStats {
    window_batches: u64,
    window_slots: u64,
    window_data_bytes: u64,
    window_padded_bytes: u64,
    empty_collects: u64,
    park_count: u64,
    park_wait_ns: u128,
    spin_count: u64,
    spin_hits: u64,
    spin_wait_ns: u128,
    delay_count: u64,
    delay_wait_ns: u128,
    delay_slots_gained: u64,
    collect_ns: u128,
    copy_ns: u128,
    pwrite_ns: u128,
    complete_ns: u128,
    last_log_at: Instant,
}

impl WriterPerfStats {
    fn new(now: Instant) -> Self {
        Self {
            window_batches: 0,
            window_slots: 0,
            window_data_bytes: 0,
            window_padded_bytes: 0,
            empty_collects: 0,
            park_count: 0,
            park_wait_ns: 0,
            spin_count: 0,
            spin_hits: 0,
            spin_wait_ns: 0,
            delay_count: 0,
            delay_wait_ns: 0,
            delay_slots_gained: 0,
            collect_ns: 0,
            copy_ns: 0,
            pwrite_ns: 0,
            complete_ns: 0,
            last_log_at: now,
        }
    }

    fn maybe_log_and_reset(&mut self, interval: Duration) {
        if interval.is_zero() {
            return;
        }

        let now = Instant::now();
        if now.duration_since(self.last_log_at) < interval {
            return;
        }

        let batches = self.window_batches.max(1);
        let avg_batch_slots = self.window_slots as f64 / batches as f64;
        let avg_collect_us = self.collect_ns as f64 / batches as f64 / 1_000.0;
        let avg_copy_us = self.copy_ns as f64 / batches as f64 / 1_000.0;
        let avg_pwrite_us = self.pwrite_ns as f64 / batches as f64 / 1_000.0;
        let avg_complete_us = self.complete_ns as f64 / batches as f64 / 1_000.0;
        let padded_ratio = if self.window_data_bytes == 0 {
            1.0
        } else {
            self.window_padded_bytes as f64 / self.window_data_bytes as f64
        };
        let spin_hit_rate_pct = if self.spin_count == 0 {
            0.0
        } else {
            (self.spin_hits as f64 * 100.0) / self.spin_count as f64
        };
        let avg_park_wait_us = if self.park_count == 0 {
            0.0
        } else {
            self.park_wait_ns as f64 / self.park_count as f64 / 1_000.0
        };
        let avg_spin_wait_us = if self.spin_count == 0 {
            0.0
        } else {
            self.spin_wait_ns as f64 / self.spin_count as f64 / 1_000.0
        };
        let avg_delay_wait_us = if self.delay_count == 0 {
            0.0
        } else {
            self.delay_wait_ns as f64 / self.delay_count as f64 / 1_000.0
        };
        let avg_delay_slots_gained = if self.delay_count == 0 {
            0.0
        } else {
            self.delay_slots_gained as f64 / self.delay_count as f64
        };

        log_debug!(
            "clog writer perf window: batches={} avg_batch_slots={:.2} data_bytes={} padded_bytes={} padded_ratio={:.2} empty_collects={} park_count={} avg_park_wait_us={:.2} spin_count={} spin_hit_rate_pct={:.2} avg_spin_wait_us={:.2} delay_count={} avg_delay_wait_us={:.2} avg_delay_slots_gained={:.2} avg_collect_us={:.2} avg_copy_us={:.2} avg_pwrite_us={:.2} avg_complete_us={:.2}",
            self.window_batches,
            avg_batch_slots,
            self.window_data_bytes,
            self.window_padded_bytes,
            padded_ratio,
            self.empty_collects,
            self.park_count,
            avg_park_wait_us,
            self.spin_count,
            spin_hit_rate_pct,
            avg_spin_wait_us,
            self.delay_count,
            avg_delay_wait_us,
            avg_delay_slots_gained,
            avg_collect_us,
            avg_copy_us,
            avg_pwrite_us,
            avg_complete_us
        );

        *self = Self::new(now);
    }
}

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
    tuning: ClogWriterTuning,
    state: Mutex<WriterState>,
}

impl ClogWriter {
    pub(crate) fn new(
        path: PathBuf,
        group_buffer: Arc<ClogGroupBuffer>,
        runtime: &tokio::runtime::Handle,
        start_offset: u64,
        staging_size: usize,
        tuning: ClogWriterTuning,
        fatal_action: WalFatalAction,
    ) -> Result<Self, String> {
        let writer = Self {
            path,
            group_buffer,
            runtime: runtime.clone(),
            fatal_action,
            staging_size,
            tuning,
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
        let tuning = self.tuning;
        let handle = self.runtime.spawn_blocking(move || {
            writer_loop(
                file,
                start_offset,
                group_buffer,
                staging_size,
                tuning,
                fatal_action,
            )
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
    tuning: ClogWriterTuning,
    fatal_action: WalFatalAction,
) {
    let mut staging = AlignedBuf::zeroed(staging_size, DMA_ALIGNMENT);
    let mut batch: Vec<FlushSlot> = Vec::with_capacity(group_buffer.max_slots());
    let mut stats = WriterPerfStats::new(Instant::now());

    loop {
        if group_buffer.is_shutdown() {
            return;
        }

        if group_buffer.drain_cancelled_head("clog reservation cancelled") {
            continue;
        }

        let mut delayed_before_slots: Option<usize> = None;
        if tuning.micro_batch_delay > Duration::ZERO {
            let ready =
                group_buffer.count_contiguous_filled_from_head(tuning.micro_batch_no_delay_count);
            if ready > 0 && ready < tuning.micro_batch_no_delay_count {
                #[cfg(test)]
                if let Some(hook) = tuning.hook_delay_wait_start {
                    hook();
                }
                let delay_start = Instant::now();
                let _ready = group_buffer.wait_for_min_filled_or_shutdown(
                    tuning.micro_batch_no_delay_count,
                    tuning.micro_batch_delay,
                    tuning.spin_before_park_iters,
                );
                stats.delay_count += 1;
                stats.delay_wait_ns += delay_start.elapsed().as_nanos();
                delayed_before_slots = Some(ready);
            }
        }

        let collect_start = Instant::now();
        let Some(meta) = group_buffer.collect_filled_batch(&mut batch) else {
            stats.collect_ns += collect_start.elapsed().as_nanos();
            stats.empty_collects += 1;
            if tuning.spin_before_park_iters > 0 {
                let spin_start = Instant::now();
                let spin_ready =
                    group_buffer.spin_wait_for_data_or_shutdown(tuning.spin_before_park_iters);
                stats.spin_count += 1;
                stats.spin_wait_ns += spin_start.elapsed().as_nanos();
                if spin_ready {
                    stats.spin_hits += 1;
                } else {
                    let park_start = Instant::now();
                    group_buffer.wait_for_data_or_shutdown();
                    stats.park_count += 1;
                    stats.park_wait_ns += park_start.elapsed().as_nanos();
                }
            } else {
                let park_start = Instant::now();
                group_buffer.wait_for_data_or_shutdown();
                stats.park_count += 1;
                stats.park_wait_ns += park_start.elapsed().as_nanos();
            }
            stats.maybe_log_and_reset(tuning.stats_log_interval);
            continue;
        };
        stats.collect_ns += collect_start.elapsed().as_nanos();
        #[cfg(test)]
        if let Some(hook) = tuning.hook_batch_collected {
            hook(batch.len());
        }
        if let Some(before) = delayed_before_slots {
            stats.delay_slots_gained += batch.len().saturating_sub(before) as u64;
        }

        let copy_start = Instant::now();
        let copy_size = group_buffer.copy_to_staging(&batch, &mut staging);
        stats.copy_ns += copy_start.elapsed().as_nanos();
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

            let write_start = Instant::now();
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
            stats.pwrite_ns += write_start.elapsed().as_nanos();
            stats.window_padded_bytes += padded as u64;
        }

        let complete_start = Instant::now();
        group_buffer.complete_batch(&batch, Ok(()));
        stats.complete_ns += complete_start.elapsed().as_nanos();
        stats.window_batches += 1;
        stats.window_slots += batch.len() as u64;
        stats.window_data_bytes += copy_size as u64;
        stats.maybe_log_and_reset(tuning.stats_log_interval);
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
    use std::sync::mpsc;
    use std::sync::{Mutex as StdMutex, OnceLock};

    #[derive(Default)]
    struct WriterTestHookState {
        delay_wait_start_tx: Option<mpsc::Sender<()>>,
        batch_collected_tx: Option<mpsc::Sender<usize>>,
    }

    fn hook_state() -> &'static StdMutex<WriterTestHookState> {
        static HOOK_STATE: OnceLock<StdMutex<WriterTestHookState>> = OnceLock::new();
        HOOK_STATE.get_or_init(|| StdMutex::new(WriterTestHookState::default()))
    }

    fn install_test_hooks(
        delay_wait_start_tx: mpsc::Sender<()>,
        batch_collected_tx: mpsc::Sender<usize>,
    ) {
        let mut guard = hook_state()
            .lock()
            .expect("writer test hook mutex poisoned");
        guard.delay_wait_start_tx = Some(delay_wait_start_tx);
        guard.batch_collected_tx = Some(batch_collected_tx);
    }

    fn clear_test_hooks() {
        let mut guard = hook_state()
            .lock()
            .expect("writer test hook mutex poisoned");
        guard.delay_wait_start_tx = None;
        guard.batch_collected_tx = None;
    }

    fn test_hook_delay_wait_start() {
        let guard = hook_state()
            .lock()
            .expect("writer test hook mutex poisoned");
        if let Some(tx) = guard.delay_wait_start_tx.as_ref() {
            let _ = tx.send(());
        }
    }

    fn test_hook_batch_collected(slots: usize) {
        let guard = hook_state()
            .lock()
            .expect("writer test hook mutex poisoned");
        if let Some(tx) = guard.batch_collected_tx.as_ref() {
            let _ = tx.send(slots);
        }
    }

    struct TestHookGuard;

    impl Drop for TestHookGuard {
        fn drop(&mut self) {
            clear_test_hooks();
        }
    }

    #[tokio::test]
    async fn test_writer_start_stop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.clog");
        let gb = ClogGroupBuffer::new(4096 * 4, 64);
        let runtime = tokio::runtime::Handle::current();

        let writer = ClogWriter::new(
            path,
            gb,
            &runtime,
            0,
            4096 * 4,
            ClogWriterTuning::default(),
            Arc::new(|_| {}),
        )
        .unwrap();

        writer
            .stop_and_drain("test stop")
            .await
            .expect("stop should succeed");
    }

    #[tokio::test]
    async fn test_writer_micro_batch_delay_coalesces_following_slot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_delay.clog");
        let gb = ClogGroupBuffer::new(4096 * 4, 64);
        let runtime = tokio::runtime::Handle::current();
        let (delay_tx, delay_rx) = mpsc::channel();
        let (batch_tx, batch_rx) = mpsc::channel();
        install_test_hooks(delay_tx, batch_tx);
        let _hook_guard = TestHookGuard;

        let tuning = ClogWriterTuning {
            micro_batch_delay: Duration::from_millis(200),
            micro_batch_no_delay_count: 2,
            stats_log_interval: Duration::ZERO,
            hook_delay_wait_start: Some(test_hook_delay_wait_start),
            hook_batch_collected: Some(test_hook_batch_collected),
            ..Default::default()
        };

        let writer = ClogWriter::new(
            path.clone(),
            Arc::clone(&gb),
            &runtime,
            0,
            4096 * 4,
            tuning,
            Arc::new(|_| {}),
        )
        .unwrap();

        let mut r1 = gb.alloc(8).unwrap();
        r1.as_mut_slice().copy_from_slice(b"12345678");
        let rx1 = r1.commit();

        delay_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("writer should enter micro-batch delay window");

        let mut r2 = gb.alloc(8).unwrap();
        r2.as_mut_slice().copy_from_slice(b"abcdefgh");
        let rx2 = r2.commit();

        rx1.await
            .expect("first reservation waiter dropped")
            .expect("first reservation write failed");
        rx2.await
            .expect("second reservation waiter dropped")
            .expect("second reservation write failed");

        let slots = batch_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("writer should report first collected batch");
        assert_eq!(slots, 2);

        writer
            .stop_and_drain("test stop")
            .await
            .expect("stop should succeed");

        assert_eq!(std::fs::metadata(path).unwrap().len(), 4096);
    }
}
