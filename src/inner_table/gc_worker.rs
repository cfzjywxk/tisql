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

//! Background GC worker for drop-table data cleanup.
//!
//! Periodically scans `__all_gc_delete_range` for pending tasks and checks
//! whether all SSTs overlapping the dropped table's key range have been
//! compacted away. When no overlap remains, the task is marked 'done'.
//!
//! ## Design
//!
//! Follows the same pattern as `CompactionScheduler`:
//! - tokio::spawn (preferred) or std::thread::spawn (fallback)
//! - `Notify` for immediate wake, 30s poll interval
//! - Clean shutdown via `AtomicBool`

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

use crate::inner_table::bootstrap::update_gc_task_status;
use crate::inner_table::catalog_loader::{load_gc_tasks, GcTask};
use crate::storage::lsm::LsmEngine;
use crate::transaction::TxnService;

/// Background GC worker that marks completed drop-table tasks as done.
pub struct GcWorker<T: TxnService + 'static> {
    inner: Arc<GcWorkerInner<T>>,
    worker_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

struct GcWorkerInner<T: TxnService + 'static> {
    engine: Arc<LsmEngine>,
    txn_service: Arc<T>,
    shutdown: AtomicBool,
    notify: Notify,
}

impl<T: TxnService + 'static> GcWorker<T> {
    /// Create a new GC worker.
    pub fn new(engine: Arc<LsmEngine>, txn_service: Arc<T>) -> Self {
        Self {
            inner: Arc::new(GcWorkerInner {
                engine,
                txn_service,
                shutdown: AtomicBool::new(false),
                notify: Notify::new(),
            }),
            worker_handle: parking_lot::Mutex::new(None),
        }
    }

    /// Start the background GC worker on the given runtime.
    pub fn start(&self, handle: &tokio::runtime::Handle) {
        let mut worker = self.worker_handle.lock();
        if worker.is_some() {
            return;
        }

        let inner = Arc::clone(&self.inner);
        *worker = Some(handle.spawn(async move {
            inner.gc_worker_loop().await;
        }));
    }

    /// Stop the background GC worker.
    pub fn stop(&self) {
        self.inner.shutdown.store(true, Ordering::SeqCst);
        self.inner.notify.notify_one();

        let mut handle = self.worker_handle.lock();
        if let Some(jh) = handle.take() {
            jh.abort();
        }
    }

    /// Notify the GC worker to check tasks immediately.
    pub fn notify(&self) {
        self.inner.notify.notify_one();
    }
}

impl<T: TxnService + 'static> Drop for GcWorker<T> {
    fn drop(&mut self) {
        self.stop();
    }
}

impl<T: TxnService + 'static> GcWorkerInner<T> {
    /// Main loop (async — tokio task).
    async fn gc_worker_loop(&self) {
        tracing::info!("GC worker started (async)");

        while !self.shutdown.load(Ordering::Relaxed) {
            self.process_gc_tasks().await;

            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_secs(30)) => {}
            }
        }

        tracing::info!("GC worker stopped");
    }

    /// Process all pending GC tasks.
    async fn process_gc_tasks(&self) {
        // Proactive step 1: Remove obsolete SSTs (direct deletion, no compaction I/O)
        match self.engine.remove_obsolete_dropped_table_ssts() {
            Ok(count) if count > 0 => {
                tracing::info!("GC worker: removed {count} obsolete SSTs from dropped tables");
            }
            Err(e) => {
                tracing::warn!("GC worker: failed to remove obsolete SSTs: {e}");
            }
            _ => {}
        }

        // Proactive step 2: Trigger compaction for levels with remaining dropped table SSTs
        self.engine.trigger_compaction();

        let tasks = match load_gc_tasks(self.txn_service.as_ref()) {
            Ok(tasks) => tasks,
            Err(e) => {
                tracing::warn!("GC worker: failed to load tasks: {e}");
                return;
            }
        };

        let gc_safe_point = self.engine.gc_safe_point();

        for task in tasks {
            if self.shutdown.load(Ordering::Relaxed) {
                return;
            }
            if task.status == "done" {
                continue;
            }

            // Skip tasks not yet eligible for GC
            if task.drop_commit_ts == 0 || task.drop_commit_ts > gc_safe_point {
                continue;
            }

            // Ensure table is registered for compaction filtering
            self.engine
                .add_dropped_table(task.table_id, task.drop_commit_ts);

            // Check if all SSTs overlapping the key range have been compacted
            if self.check_sst_overlap(&task) {
                // Still overlapping — compaction will filter on next round
                continue;
            }

            // No overlap — mark task done
            if let Err(e) = self.mark_task_done(&task).await {
                tracing::warn!("GC worker: failed to mark task {} done: {e}", task.task_id);
                continue;
            }

            self.engine.remove_dropped_table(task.table_id);
            tracing::info!(
                "GC worker: completed task {} (table_id={})",
                task.task_id,
                task.table_id
            );
        }
    }

    /// Check if any SST file overlaps the task's key range.
    fn check_sst_overlap(&self, task: &GcTask) -> bool {
        let start_key = match hex_decode(&task.start_key_hex) {
            Some(k) => k,
            None => return true, // Can't decode — assume overlap
        };
        let end_key = match hex_decode(&task.end_key_hex) {
            Some(k) => k,
            None => return true,
        };

        let version = self.engine.current_version();
        for level_idx in 0..7 {
            let level_files = version.level(level_idx);
            for sst in level_files {
                // Check if SST key range overlaps [start_key, end_key)
                // SST range: [smallest_key, largest_key]
                // Overlap: sst.smallest_key < end_key && sst.largest_key >= start_key
                if sst.smallest_key.as_slice() < end_key.as_slice()
                    && sst.largest_key.as_slice() >= start_key.as_slice()
                {
                    return true;
                }
            }
        }
        false
    }

    /// Mark a GC task as 'done' in the inner table.
    async fn mark_task_done(&self, task: &GcTask) -> crate::error::Result<()> {
        let mut ctx = self.txn_service.begin(false)?;
        update_gc_task_status(
            &mut ctx,
            self.txn_service.as_ref(),
            task.task_id,
            task.table_id,
            &task.start_key_hex,
            &task.end_key_hex,
            task.drop_commit_ts,
            "done",
        )
        .await?;
        self.txn_service.commit(ctx).await?;
        Ok(())
    }
}

/// Decode a hex string to bytes.
fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        let byte = u8::from_str_radix(&s[i..i + 2], 16).ok()?;
        bytes.push(byte);
    }
    Some(bytes)
}
