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

//! Background compaction scheduler for LSM storage engine.
//!
//! This module provides a background worker that automatically compacts
//! SST files when level sizes exceed thresholds.
//!
//! ## Design
//!
//! The `CompactionScheduler` runs a tokio task that:
//! 1. Waits for notification (from flush) or poll timeout
//! 2. Calls `engine.do_compaction()` to perform one round
//! 3. Loops on success for cascading compaction (L0→L1 then L1→L2)
//! 4. Handles clean shutdown
//!
//! ## Usage
//!
//! ```ignore
//! let engine = Arc::new(LsmEngine::open(config)?);
//! let scheduler = CompactionScheduler::new(Arc::clone(&engine));
//! scheduler.start();
//!
//! // Wire flush → compaction notification
//! engine.set_compaction_notify(scheduler.notifier());
//!
//! // Clean shutdown
//! scheduler.stop();
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::Notify;

use super::lsm::LsmEngine;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CompactionSchedulerStatus {
    pub in_progress: u64,
    pub completed: u64,
    pub failed: u64,
    pub last_error_ts_ms: Option<u64>,
}

/// Background compaction scheduler for LSM storage engine.
///
/// Runs a tokio task on the provided runtime handle that compacts SST files
/// when level thresholds are met.
pub struct CompactionScheduler {
    /// Shared state between caller and worker task.
    inner: Arc<CompactionSchedulerInner>,

    /// Worker handle (None if not started or already stopped).
    worker_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// Shared state for the compaction scheduler.
struct CompactionSchedulerInner {
    /// The LSM engine to compact.
    engine: Arc<LsmEngine>,

    /// Shutdown signal.
    shutdown: AtomicBool,

    /// Async notification for waking the worker.
    notify: Notify,

    /// Number of compactions completed (for testing/monitoring).
    compaction_count: AtomicU64,
    /// Number of compaction jobs currently running.
    in_progress: AtomicU64,
    /// Number of compaction jobs completed.
    completed: AtomicU64,
    /// Number of compaction jobs failed.
    failed: AtomicU64,
    /// Last compaction error timestamp in unix millis.
    last_error_ts_ms: AtomicU64,
}

impl CompactionScheduler {
    /// Create a new compaction scheduler for the given engine.
    ///
    /// The scheduler is not started until `start()` is called.
    pub fn new(engine: Arc<LsmEngine>) -> Self {
        Self {
            inner: Arc::new(CompactionSchedulerInner {
                engine,
                shutdown: AtomicBool::new(false),
                notify: Notify::new(),
                compaction_count: AtomicU64::new(0),
                in_progress: AtomicU64::new(0),
                completed: AtomicU64::new(0),
                failed: AtomicU64::new(0),
                last_error_ts_ms: AtomicU64::new(0),
            }),
            worker_handle: parking_lot::Mutex::new(None),
        }
    }

    /// Start the background compaction worker on the given runtime.
    ///
    /// Does nothing if already started.
    pub fn start(&self, handle: &tokio::runtime::Handle) {
        let mut worker = self.worker_handle.lock();
        if worker.is_some() {
            return; // Already started
        }

        let inner = Arc::clone(&self.inner);
        *worker = Some(handle.spawn(async move {
            inner.compaction_worker_loop().await;
        }));
    }

    /// Stop the background compaction worker.
    ///
    /// Signals the worker to stop. Compaction is not drained on shutdown
    /// since it can safely resume on next startup.
    pub fn stop(&self) {
        // Signal shutdown
        self.inner.shutdown.store(true, Ordering::SeqCst);
        self.inner.notify.notify_one();

        // Take the handle and abort
        let mut handle = self.worker_handle.lock();
        if let Some(jh) = handle.take() {
            jh.abort();
        }
    }

    /// Notify the scheduler that new L0 SSTs are available.
    ///
    /// Call this after flushing memtables to wake the compaction worker.
    pub fn notify(&self) {
        self.inner.notify.notify_one();
    }

    /// Get a notifier callback for wiring to `LsmEngine::set_compaction_notify`.
    ///
    /// Returns an `Arc<dyn Fn() + Send + Sync>` that calls `notify()` when invoked.
    pub fn notifier(&self) -> Arc<dyn Fn() + Send + Sync> {
        let inner = Arc::clone(&self.inner);
        Arc::new(move || {
            inner.notify.notify_one();
        })
    }

    /// Check if the scheduler is running.
    pub fn is_running(&self) -> bool {
        let handle = self.worker_handle.lock();
        handle.is_some() && !self.inner.shutdown.load(Ordering::Relaxed)
    }

    /// Get the number of compactions completed.
    pub fn compaction_count(&self) -> u64 {
        self.inner.compaction_count.load(Ordering::Relaxed)
    }

    pub fn status(&self) -> CompactionSchedulerStatus {
        let last = self.inner.last_error_ts_ms.load(Ordering::Relaxed);
        CompactionSchedulerStatus {
            in_progress: self.inner.in_progress.load(Ordering::Relaxed),
            completed: self.inner.completed.load(Ordering::Relaxed),
            failed: self.inner.failed.load(Ordering::Relaxed),
            last_error_ts_ms: (last != 0).then_some(last),
        }
    }

    /// Wait for at least `count` compactions to complete.
    ///
    /// Used for testing. Returns false if timeout occurs.
    pub fn wait_for_compaction_count(&self, count: u64, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while self.compaction_count() < count {
            if start.elapsed() > timeout {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        true
    }
}

impl Drop for CompactionScheduler {
    fn drop(&mut self) {
        self.stop();
    }
}

impl CompactionSchedulerInner {
    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Main loop for the compaction worker (async — tokio task).
    async fn compaction_worker_loop(&self) {
        tracing::info!("Compaction worker started (async)");

        while !self.shutdown.load(Ordering::Relaxed) {
            if !self.engine.has_pending_compaction() {
                tokio::select! {
                    _ = self.notify.notified() => {}
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
                continue;
            }

            self.in_progress.fetch_add(1, Ordering::Relaxed);
            let start = Instant::now();
            tracing::info!(
                "[engine-event] op=compact phase=begin tablet_ns={} l0_files={}",
                self.engine.tablet_cache_ns(),
                self.engine.current_version().level_size(0)
            );

            // Advance GC safe point before each compaction attempt
            self.engine.update_gc_safe_point();

            match self.engine.do_compaction().await {
                Ok(true) => {
                    self.compaction_count.fetch_add(1, Ordering::Relaxed);
                    self.completed.fetch_add(1, Ordering::Relaxed);
                    let stats = self.engine.stats();
                    tracing::info!(
                        "[engine-event] op=compact phase=finish tablet_ns={} l0_files={} total_sst={} bytes={} elapsed_ms={}",
                        self.engine.tablet_cache_ns(),
                        stats.l0_sst_count,
                        stats.total_sst_count,
                        stats.total_sst_bytes,
                        start.elapsed().as_millis()
                    );
                    tracing::debug!(
                        "Compaction completed, total: {}",
                        self.compaction_count.load(Ordering::Relaxed)
                    );
                    self.in_progress.fetch_sub(1, Ordering::Relaxed);
                    continue;
                }
                Ok(false) => {
                    self.in_progress.fetch_sub(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(e) => {
                    self.failed.fetch_add(1, Ordering::Relaxed);
                    self.last_error_ts_ms
                        .store(Self::now_ms(), Ordering::Relaxed);
                    tracing::warn!(
                        "[engine-event] op=compact phase=abort tablet_ns={} reason=\"{}\" elapsed_ms={}",
                        self.engine.tablet_cache_ns(),
                        e,
                        start.elapsed().as_millis()
                    );
                    tracing::error!("Compaction failed: {e}");
                    self.in_progress.fetch_sub(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }

        tracing::info!("Compaction worker stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tablet::{LsmConfig, StorageEngine, WriteBatch};
    use tempfile::TempDir;

    fn test_config(dir: &std::path::Path) -> LsmConfig {
        LsmConfig::builder(dir)
            .memtable_size(200) // Small to trigger rotation quickly
            .max_frozen_memtables(4)
            .l0_compaction_trigger(4)
            .target_file_size(512)
            .l1_max_size(2048)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_compaction_scheduler_new() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        let scheduler = CompactionScheduler::new(engine);
        assert!(!scheduler.is_running());
        assert_eq!(scheduler.compaction_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_compaction_scheduler_start_stop() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        let scheduler = CompactionScheduler::new(engine);

        // Start
        scheduler.start(&tokio::runtime::Handle::current());
        assert!(scheduler.is_running());

        // Stop
        scheduler.stop();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!scheduler.is_running());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_compaction_scheduler_double_start() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        let scheduler = CompactionScheduler::new(engine);
        let handle = tokio::runtime::Handle::current();

        // Start twice - should be safe
        scheduler.start(&handle);
        scheduler.start(&handle);
        assert!(scheduler.is_running());

        scheduler.stop();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_compaction_scheduler_drop_stops_worker() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        {
            let scheduler = CompactionScheduler::new(engine);
            scheduler.start(&tokio::runtime::Handle::current());
            assert!(scheduler.is_running());
            // scheduler dropped here
        }

        // Worker should have been stopped
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_compaction_scheduler_triggers_on_l0() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100) // Very small
            .max_frozen_memtables(32) // High to avoid write stall
            .l0_compaction_trigger(4)
            .l0_slowdown_trigger(100)
            .l0_stop_trigger(200)
            .target_file_size(512)
            .l1_max_size(4096)
            .build()
            .unwrap();
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        let scheduler = CompactionScheduler::new(Arc::clone(&engine));
        scheduler.start(&tokio::runtime::Handle::current());

        // Write and flush enough data to create L0 files above compaction trigger
        for i in 0..20 {
            let mut batch = WriteBatch::new();
            batch.set_commit_ts((i + 1) as u64);
            let key = format!("key_{i:04}");
            let value = vec![b'x'; 50];
            batch.put(key.into_bytes(), value);
            engine.write_batch(batch).unwrap();
        }

        // Flush all to create L0 files
        engine.flush_all_with_active().unwrap();

        // Notify compaction scheduler
        scheduler.notify();

        // Wait for at least one compaction
        let compacted = scheduler.wait_for_compaction_count(1, Duration::from_secs(10));
        assert!(compacted, "Should have compacted at least once");

        scheduler.stop();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_compaction_scheduler_polling_updates_gc_and_filters_old_versions() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(120)
            .max_frozen_memtables(16)
            .l0_compaction_trigger(3)
            .l0_slowdown_trigger(100)
            .l0_stop_trigger(200)
            .target_file_size(256)
            .l1_max_size(4096)
            .build()
            .unwrap();
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        // Scheduler should advance this safe point before compaction.
        engine.set_gc_safe_point_updater(Arc::new(|| 25));

        let scheduler = CompactionScheduler::new(Arc::clone(&engine));
        scheduler.start(&tokio::runtime::Handle::current());

        // Build three versions for the same key in separate L0 files.
        for (ts, value) in [(10u64, b"v10"), (20u64, b"v20"), (30u64, b"v30")] {
            let mut batch = WriteBatch::new();
            batch.set_commit_ts(ts);
            batch.put(b"gc_key".to_vec(), value.to_vec());
            engine.write_batch(batch).unwrap();
            engine.flush_all_with_active().unwrap();
        }

        // Add extra files so L0 compaction score definitely crosses threshold.
        for i in 0..3u64 {
            let mut batch = WriteBatch::new();
            batch.set_commit_ts(100 + i);
            batch.put(format!("filler_{i:03}").into_bytes(), vec![b'f'; 32]);
            engine.write_batch(batch).unwrap();
            engine.flush_all_with_active().unwrap();
        }

        // No explicit notify(): scheduler should compact via polling.
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if scheduler.compaction_count() >= 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("compaction scheduler did not compact via polling");

        assert_eq!(
            engine.gc_safe_point(),
            25,
            "GC safe point should be advanced by updater callback"
        );

        // gc_safe_point=25 keeps barrier ts=20 and drops ts=10 at bottommost level.
        assert_eq!(engine.get_at(b"gc_key", 15).await.unwrap(), None);
        assert_eq!(
            engine.get_at(b"gc_key", 20).await.unwrap(),
            Some(b"v20".to_vec())
        );
        assert_eq!(engine.get(b"gc_key").await.unwrap(), Some(b"v30".to_vec()));

        scheduler.stop();
    }
}
