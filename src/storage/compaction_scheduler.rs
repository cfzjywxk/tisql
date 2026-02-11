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
use std::time::Duration;

use tokio::sync::Notify;

use super::lsm::LsmEngine;

/// Handle for the worker — either a tokio task or a std thread.
enum WorkerHandle {
    Tokio(tokio::task::JoinHandle<()>),
    Thread(std::thread::JoinHandle<()>),
}

/// Background compaction scheduler for LSM storage engine.
///
/// Runs a tokio task (preferred) or std::thread (fallback when no runtime)
/// that compacts SST files when level thresholds are met.
pub struct CompactionScheduler {
    /// Shared state between caller and worker task.
    inner: Arc<CompactionSchedulerInner>,

    /// Worker handle (None if not started or already stopped).
    worker_handle: parking_lot::Mutex<Option<WorkerHandle>>,
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
            }),
            worker_handle: parking_lot::Mutex::new(None),
        }
    }

    /// Start the background compaction worker.
    ///
    /// Uses `tokio::spawn` if a tokio runtime is available, otherwise falls
    /// back to `std::thread::spawn`. Does nothing if already started.
    pub fn start(&self) {
        let mut handle = self.worker_handle.lock();
        if handle.is_some() {
            return; // Already started
        }

        let inner = Arc::clone(&self.inner);
        if let Ok(rt_handle) = tokio::runtime::Handle::try_current() {
            *handle = Some(WorkerHandle::Tokio(rt_handle.spawn(async move {
                inner.compaction_worker_loop().await;
            })));
        } else {
            *handle = Some(WorkerHandle::Thread(std::thread::spawn(move || {
                inner.compaction_worker_loop_sync();
            })));
        }
    }

    /// Stop the background compaction worker.
    ///
    /// Signals the worker to stop and waits for it to finish.
    /// Unlike `FlushScheduler`, compaction is not drained on shutdown
    /// since it can safely resume on next startup.
    pub fn stop(&self) {
        // Signal shutdown
        self.inner.shutdown.store(true, Ordering::SeqCst);
        self.inner.notify.notify_one();

        // Take the handle and abort/join
        let mut handle = self.worker_handle.lock();
        if let Some(h) = handle.take() {
            match h {
                WorkerHandle::Tokio(jh) => jh.abort(),
                WorkerHandle::Thread(jh) => {
                    if jh.join().is_err() {
                        tracing::warn!("Compaction worker thread panicked");
                    }
                }
            }
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
    /// Main loop for the compaction worker (async — tokio task).
    async fn compaction_worker_loop(&self) {
        tracing::info!("Compaction worker started (async)");

        while !self.shutdown.load(Ordering::Relaxed) {
            match self.engine.do_compaction().await {
                Ok(true) => {
                    self.compaction_count.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(
                        "Compaction completed, total: {}",
                        self.compaction_count.load(Ordering::Relaxed)
                    );
                    continue;
                }
                Ok(false) => {
                    tokio::select! {
                        _ = self.notify.notified() => {}
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
                Err(e) => {
                    tracing::error!("Compaction failed: {e}");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }

        tracing::info!("Compaction worker stopped");
    }

    /// Main loop for the compaction worker (sync — std::thread fallback).
    fn compaction_worker_loop_sync(&self) {
        tracing::info!("Compaction worker started (sync fallback)");

        while !self.shutdown.load(Ordering::Relaxed) {
            match crate::io::block_on_sync(self.engine.do_compaction()) {
                Ok(true) => {
                    self.compaction_count.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Ok(false) => {
                    std::thread::sleep(Duration::from_secs(1));
                }
                Err(e) => {
                    tracing::error!("Compaction failed: {e}");
                    std::thread::sleep(Duration::from_millis(500));
                }
            }
        }

        tracing::info!("Compaction worker stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{LsmConfig, StorageEngine, WriteBatch};
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

    #[test]
    fn test_compaction_scheduler_new() {
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
        scheduler.start();
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

        // Start twice - should be safe
        scheduler.start();
        scheduler.start();
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
            scheduler.start();
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
        scheduler.start();

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
}
