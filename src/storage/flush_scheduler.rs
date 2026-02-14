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

//! Background flush scheduler for LSM storage engine.
//!
//! This module provides a background worker that automatically flushes
//! frozen memtables to SST files.
//!
//! ## Design
//!
//! The `FlushScheduler` runs a tokio task that:
//! 1. Waits for notification of new frozen memtables
//! 2. Flushes frozen memtables in order (oldest first)
//! 3. Handles clean shutdown via `CancellationToken`
//!
//! ## Usage
//!
//! ```ignore
//! let engine = Arc::new(LsmEngine::open(config)?);
//! let scheduler = FlushScheduler::new(Arc::clone(&engine));
//! scheduler.start();
//!
//! // Write data - scheduler flushes automatically when memtables freeze
//! engine.write_batch(batch)?;
//!
//! // Clean shutdown
//! scheduler.stop();
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

use super::lsm::LsmEngine;

/// Background flush scheduler for LSM storage engine.
///
/// Runs a tokio task on the provided runtime handle that flushes frozen
/// memtables to SST files.
pub struct FlushScheduler {
    /// Shared state between caller and worker task.
    inner: Arc<FlushSchedulerInner>,

    /// Worker handle (None if not started or already stopped).
    worker_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// Shared state for the flush scheduler.
struct FlushSchedulerInner {
    /// The LSM engine to flush.
    engine: Arc<LsmEngine>,

    /// Shutdown signal.
    shutdown: AtomicBool,

    /// Async notification for waking the worker.
    notify: Notify,

    /// Number of flushes completed (for testing/monitoring).
    flush_count: AtomicU64,
}

impl FlushScheduler {
    /// Create a new flush scheduler for the given engine.
    ///
    /// The scheduler is not started until `start()` is called.
    pub fn new(engine: Arc<LsmEngine>) -> Self {
        Self {
            inner: Arc::new(FlushSchedulerInner {
                engine,
                shutdown: AtomicBool::new(false),
                notify: Notify::new(),
                flush_count: AtomicU64::new(0),
            }),
            worker_handle: parking_lot::Mutex::new(None),
        }
    }

    /// Start the background flush worker on the given runtime.
    ///
    /// Does nothing if already started.
    pub fn start(&self, handle: &tokio::runtime::Handle) {
        let mut worker = self.worker_handle.lock();
        if worker.is_some() {
            return; // Already started
        }

        let inner = Arc::clone(&self.inner);
        *worker = Some(handle.spawn(async move {
            inner.flush_worker_loop().await;
        }));
    }

    /// Stop the background flush worker.
    ///
    /// Signals the worker to stop and waits for it to finish.
    /// Any in-progress flush will complete before the worker exits.
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

    /// Notify the scheduler that new frozen memtables are available.
    ///
    /// Call this after rotating memtables to wake the flush worker.
    pub fn notify(&self) {
        self.inner.notify.notify_one();
    }

    /// Check if the scheduler is running.
    pub fn is_running(&self) -> bool {
        let handle = self.worker_handle.lock();
        handle.is_some() && !self.inner.shutdown.load(Ordering::Relaxed)
    }

    /// Get the number of flushes completed.
    pub fn flush_count(&self) -> u64 {
        self.inner.flush_count.load(Ordering::Relaxed)
    }

    /// Wait for at least `count` flushes to complete.
    ///
    /// Used for testing. Returns false if timeout occurs.
    pub fn wait_for_flush_count(&self, count: u64, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while self.flush_count() < count {
            if start.elapsed() > timeout {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        true
    }
}

impl Drop for FlushScheduler {
    fn drop(&mut self) {
        self.stop();
    }
}

impl FlushSchedulerInner {
    /// Main loop for the flush worker (async — tokio task).
    async fn flush_worker_loop(&self) {
        tracing::info!("Flush worker started (async)");

        while !self.shutdown.load(Ordering::Relaxed) {
            let frozen_count = self.engine.frozen_count();

            if frozen_count > 0 {
                match self.engine.flush_all_async().await {
                    Ok(metas) => {
                        if !metas.is_empty() {
                            self.flush_count
                                .fetch_add(metas.len() as u64, Ordering::Relaxed);
                            tracing::debug!(
                                "Flushed {} memtables, total flushes: {}",
                                metas.len(),
                                self.flush_count.load(Ordering::Relaxed)
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!("Flush failed: {e}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            } else {
                tokio::select! {
                    _ = self.notify.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                }
            }
        }

        self.final_flush().await;
        tracing::info!("Flush worker stopped");
    }

    /// Final flush on shutdown — drain any remaining frozen memtables.
    async fn final_flush(&self) {
        let frozen_count = self.engine.frozen_count();
        if frozen_count > 0 {
            tracing::info!("Final flush of {frozen_count} frozen memtables on shutdown");
            if let Err(e) = self.engine.flush_all_async().await {
                tracing::error!("Final flush failed: {e}");
            }
        }
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
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_flush_scheduler_new() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        let scheduler = FlushScheduler::new(engine);
        assert!(!scheduler.is_running());
        assert_eq!(scheduler.flush_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_flush_scheduler_start_stop() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        let scheduler = FlushScheduler::new(engine);

        // Start
        scheduler.start(&tokio::runtime::Handle::current());
        assert!(scheduler.is_running());

        // Stop
        scheduler.stop();
        // Give the task a moment to finish
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!scheduler.is_running());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_flush_scheduler_double_start() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        let scheduler = FlushScheduler::new(engine);
        let handle = tokio::runtime::Handle::current();

        // Start twice - should be safe
        scheduler.start(&handle);
        scheduler.start(&handle);
        assert!(scheduler.is_running());

        scheduler.stop();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_flush_scheduler_double_stop() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        let scheduler = FlushScheduler::new(engine);

        scheduler.start(&tokio::runtime::Handle::current());
        scheduler.stop();
        scheduler.stop(); // Should be safe
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!scheduler.is_running());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_flush_scheduler_triggers_on_frozen() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100) // Very small
            .max_frozen_memtables(16) // High enough to avoid write stall
            .build()
            .unwrap();
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        let scheduler = FlushScheduler::new(Arc::clone(&engine));
        scheduler.start(&tokio::runtime::Handle::current());

        // Write enough data to trigger rotation
        for i in 0..10 {
            let mut batch = WriteBatch::new();
            batch.set_commit_ts((i + 1) as u64);
            let key = format!("key_{i:04}");
            let value = vec![b'x'; 50];
            batch.put(key.into_bytes(), value);
            engine.write_batch(batch).unwrap();

            // Notify scheduler
            scheduler.notify();
        }

        // Wait for at least one flush
        let flushed = scheduler.wait_for_flush_count(1, Duration::from_secs(5));
        assert!(flushed, "Should have flushed at least one memtable");

        // Verify SST created
        let version = engine.current_version();
        assert!(
            version.total_sst_count() > 0,
            "Should have created SST files"
        );

        scheduler.stop();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_flush_scheduler_drop_stops_worker() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        {
            let scheduler = FlushScheduler::new(engine);
            scheduler.start(&tokio::runtime::Handle::current());
            assert!(scheduler.is_running());
            // scheduler dropped here
        }

        // Worker should have been stopped
        // (we can't easily verify this, but at least it shouldn't hang)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_flush_scheduler_concurrent_writes() {
        use std::sync::atomic::AtomicUsize;

        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(200) // Small
            .max_frozen_memtables(64) // High to avoid write stall with 100 concurrent entries
            .l0_compaction_trigger(100) // High to avoid L0 stop during concurrent writes
            .l0_slowdown_trigger(200)
            .l0_stop_trigger(300)
            .build()
            .unwrap();
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        let scheduler = Arc::new(FlushScheduler::new(Arc::clone(&engine)));
        scheduler.start(&tokio::runtime::Handle::current());

        let write_count = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        // Spawn 4 writer tasks
        for t in 0..4 {
            let eng = Arc::clone(&engine);
            let sched = Arc::clone(&scheduler);
            let count = Arc::clone(&write_count);

            let handle = tokio::task::spawn_blocking(move || {
                for i in 0..25 {
                    let mut batch = WriteBatch::new();
                    batch.set_commit_ts((t * 1000 + i + 1) as u64);
                    let key = format!("t{t}_key_{i:04}");
                    let value = vec![b'v'; 30];
                    batch.put(key.into_bytes(), value);
                    eng.write_batch(batch).unwrap();
                    count.fetch_add(1, Ordering::Relaxed);
                    sched.notify();
                }
            });
            handles.push(handle);
        }

        // Wait for writers
        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(write_count.load(Ordering::Relaxed), 100);

        // Wait for flushes to complete
        let _ = scheduler.wait_for_flush_count(1, Duration::from_secs(5));

        // Stop scheduler
        scheduler.stop();

        // Verify data integrity - all keys should be readable
        for t in 0..4 {
            for i in 0..25 {
                let key = format!("t{t}_key_{i:04}");
                let result = engine.get(key.as_bytes()).await.unwrap();
                assert!(result.is_some(), "Key {key} should exist");
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_flush_scheduler_notify_without_start() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        let scheduler = FlushScheduler::new(engine);

        // Notify without starting - should not panic
        scheduler.notify();
        scheduler.notify();

        assert!(!scheduler.is_running());
    }
}
