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

//! Background GC worker for tablet-level DROP cleanup.
//!
//! Periodically scans `__all_gc_delete_range` for pending tasks. Once
//! `drop_commit_ts <= gc_safe_point`, the worker retires the target tablet
//! (stop workers + flush + unmount) and removes the full tablet directory.
//! The task row is then marked `done`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

use crate::inner_table::bootstrap::update_gc_task_status;
use crate::inner_table::catalog_loader::{load_gc_tasks, GcTask};
use crate::tablet::{TabletId, TabletManager};
use crate::transaction::{TxnService, TxnState};
use crate::util::error::{Result, TiSqlError};

/// Background GC worker that completes tablet-retirement tasks.
pub struct GcWorker<T: TxnService + 'static> {
    inner: Arc<GcWorkerInner<T>>,
    worker_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

struct GcWorkerInner<T: TxnService + 'static> {
    tablet_manager: Arc<TabletManager>,
    txn_service: Arc<T>,
    gc_safe_point: Arc<dyn Fn() -> u64 + Send + Sync>,
    shutdown: AtomicBool,
    notify: Notify,
}

impl<T: TxnService + 'static> GcWorker<T> {
    /// Create a new GC worker.
    pub fn new(
        tablet_manager: Arc<TabletManager>,
        txn_service: Arc<T>,
        gc_safe_point: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Self {
        Self {
            inner: Arc::new(GcWorkerInner {
                tablet_manager,
                txn_service,
                gc_safe_point,
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

        let handle = self.worker_handle.lock().take();
        if let Some(jh) = handle {
            // Drain the worker task to avoid leaving an in-flight tablet retire
            // operation detached during Database::close/drop.
            let _ = crate::io::block_on_sync(jh);
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
        // Defense in depth: periodically force-release stale commit reservations.
        // Keep reservations for active txn states only.
        let released = self
            .tablet_manager
            .system_tablet()
            .sweep_stale_commit_reservations(|txn_start_ts| {
                matches!(
                    self.txn_service.get_txn_state(txn_start_ts),
                    Some(TxnState::Running)
                        | Some(TxnState::Preparing)
                        | Some(TxnState::Prepared { .. })
                )
            });
        if released > 0 {
            tracing::warn!("GC worker: force-released {released} stale commit reservations");
        }

        let tasks = match load_gc_tasks(self.txn_service.as_ref()) {
            Ok(tasks) => tasks,
            Err(e) => {
                tracing::warn!("GC worker: failed to load tasks: {e}");
                return;
            }
        };

        let gc_safe_point = (self.gc_safe_point)();

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

            if let Err(e) = self.retire_tablet(task.tablet_id).await {
                tracing::warn!(
                    "GC worker: failed to retire tablet {:?} for task {}: {e}",
                    task.tablet_id,
                    task.task_id
                );
                continue;
            }

            // No overlap — mark task done
            if let Err(e) = self.mark_task_done(&task).await {
                tracing::warn!("GC worker: failed to mark task {} done: {e}", task.task_id);
                continue;
            }

            tracing::info!(
                "GC worker: completed task {} (tablet_id={:?}, table_id={})",
                task.task_id,
                task.tablet_id,
                task.table_id
            );
        }
    }

    async fn retire_tablet(&self, tablet_id: TabletId) -> Result<()> {
        if tablet_id == TabletId::System {
            return Err(TiSqlError::Storage(
                "GC task attempted to retire system tablet".to_string(),
            ));
        }

        let tablet_dir = self.tablet_manager.tablet_dir(tablet_id);
        let manager = Arc::clone(&self.tablet_manager);
        let remove_result = tokio::task::spawn_blocking(move || manager.remove_tablet(tablet_id))
            .await
            .map_err(|e| TiSqlError::Storage(format!("retire tablet join error: {e}")))?;
        let _ = remove_result?;

        if tablet_dir.exists() {
            let dir = tablet_dir.clone();
            tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&dir))
                .await
                .map_err(|e| TiSqlError::Storage(format!("delete tablet dir join error: {e}")))??;
        }
        Ok(())
    }

    /// Mark a GC task as 'done' in the inner table.
    async fn mark_task_done(&self, task: &GcTask) -> Result<()> {
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
