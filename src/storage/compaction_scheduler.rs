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
//! The `CompactionScheduler` runs a background thread that:
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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::lsm::LsmEngine;

/// Background compaction scheduler for LSM storage engine.
///
/// Runs a worker thread that compacts SST files when level thresholds are met.
/// Unlike `FlushScheduler`, the compaction worker loops on success to handle
/// cascading compaction (e.g., L0→L1 triggers L1→L2).
pub struct CompactionScheduler {
    /// Shared state between main thread and worker.
    inner: Arc<CompactionSchedulerInner>,

    /// Worker thread handle (None if not started or already stopped).
    worker_handle: Mutex<Option<JoinHandle<()>>>,
}

/// Shared state for the compaction scheduler.
struct CompactionSchedulerInner {
    /// The LSM engine to compact.
    engine: Arc<LsmEngine>,

    /// Shutdown signal.
    shutdown: AtomicBool,

    /// Condition variable for waking the worker.
    notify_cv: Condvar,

    /// Mutex for condition variable (protects nothing, just for Condvar).
    notify_mutex: Mutex<()>,

    /// Number of compactions completed (for testing/monitoring).
    compaction_count: std::sync::atomic::AtomicU64,
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
                notify_cv: Condvar::new(),
                notify_mutex: Mutex::new(()),
                compaction_count: std::sync::atomic::AtomicU64::new(0),
            }),
            worker_handle: Mutex::new(None),
        }
    }

    /// Start the background compaction worker.
    ///
    /// Does nothing if already started.
    pub fn start(&self) {
        let mut handle = self.worker_handle.lock().unwrap();
        if handle.is_some() {
            return; // Already started
        }

        let inner = Arc::clone(&self.inner);
        *handle = Some(thread::spawn(move || {
            inner.compaction_worker_loop();
        }));
    }

    /// Stop the background compaction worker.
    ///
    /// Signals the worker to stop and waits for it to finish.
    /// Unlike `FlushScheduler`, compaction is not drained on shutdown
    /// since it can safely resume on next startup.
    pub fn stop(&self) {
        // Signal shutdown
        self.inner.shutdown.store(true, Ordering::SeqCst);
        self.inner.notify_cv.notify_all();

        // Wait for worker to finish
        let mut handle = self.worker_handle.lock().unwrap();
        if let Some(h) = handle.take() {
            if h.join().is_err() {
                tracing::warn!("Compaction worker thread panicked");
            }
        }
    }

    /// Notify the scheduler that new L0 SSTs are available.
    ///
    /// Call this after flushing memtables to wake the compaction worker.
    pub fn notify(&self) {
        self.inner.notify_cv.notify_one();
    }

    /// Get a notifier callback for wiring to `LsmEngine::set_compaction_notify`.
    ///
    /// Returns an `Arc<dyn Fn() + Send + Sync>` that calls `notify()` when invoked.
    pub fn notifier(&self) -> Arc<dyn Fn() + Send + Sync> {
        let inner = Arc::clone(&self.inner);
        Arc::new(move || {
            inner.notify_cv.notify_one();
        })
    }

    /// Check if the scheduler is running.
    pub fn is_running(&self) -> bool {
        let handle = self.worker_handle.lock().unwrap();
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
            thread::sleep(Duration::from_millis(10));
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
    /// Main loop for the compaction worker thread.
    fn compaction_worker_loop(&self) {
        tracing::info!("Compaction worker started");

        while !self.shutdown.load(Ordering::Relaxed) {
            match self.engine.do_compaction() {
                Ok(true) => {
                    // Compaction succeeded - loop immediately for cascading compaction
                    self.compaction_count.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(
                        "Compaction completed, total: {}",
                        self.compaction_count.load(Ordering::Relaxed)
                    );
                    // Continue looping without waiting (cascading)
                    continue;
                }
                Ok(false) => {
                    // No work - wait for notification or timeout
                    let guard = self.notify_mutex.lock().unwrap();
                    let _ = self.notify_cv.wait_timeout(guard, Duration::from_secs(1));
                }
                Err(e) => {
                    tracing::error!("Compaction failed: {e}");
                    // Sleep before retrying to avoid busy loop on persistent errors
                    thread::sleep(Duration::from_millis(500));
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

    #[test]
    fn test_compaction_scheduler_start_stop() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        let scheduler = CompactionScheduler::new(engine);

        // Start
        scheduler.start();
        assert!(scheduler.is_running());

        // Stop
        scheduler.stop();
        assert!(!scheduler.is_running());
    }

    #[test]
    fn test_compaction_scheduler_double_start() {
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

    #[test]
    fn test_compaction_scheduler_drop_stops_worker() {
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

    #[test]
    fn test_compaction_scheduler_triggers_on_l0() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100) // Very small
            .max_frozen_memtables(16)
            .l0_compaction_trigger(4)
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
