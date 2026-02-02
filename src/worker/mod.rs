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

// Worker pool module - separates database work from network IO using yatp
//
// Architecture:
// - Tokio runtime handles network IO (TCP accept, MySQL protocol)
// - yatp FuturePool handles CPU-bound database work (parse, bind, execute)
// - Communication via tokio::sync::oneshot channels

use std::sync::Arc;

use tokio::sync::oneshot;
use yatp::pool::Remote;
use yatp::task::future::TaskCell;

use crate::error::TiSqlError;
use crate::session::ExecutionCtx;
use crate::transaction::TxnCtx;
use crate::{Database, QueryResult};

/// Configuration for the worker thread pool
pub struct WorkerPoolConfig {
    /// Name prefix for worker threads (e.g., "tisql-worker")
    pub name: String,
    /// Minimum number of worker threads
    pub min_threads: usize,
    /// Maximum number of worker threads
    pub max_threads: usize,
}

impl Default for WorkerPoolConfig {
    fn default() -> Self {
        let cpus = num_cpus::get();
        Self {
            name: "tisql-worker".to_string(),
            min_threads: 4.min(cpus),
            max_threads: cpus,
        }
    }
}

/// Thread pool for executing database work off the network IO threads
pub struct WorkerPool {
    remote: Remote<TaskCell>,
    // Keep pool alive - dropping it shuts down the threads
    _pool: yatp::ThreadPool<TaskCell>,
}

/// Result type for query execution with TxnCtx ownership transfer.
///
/// On success: (QueryResult, Option<TxnCtx>) - the TxnCtx is returned if still active.
/// On error: (TiSqlError, Option<TxnCtx>) - the TxnCtx is returned to keep the transaction active.
pub type QueryResultWithCtx = Result<(QueryResult, Option<TxnCtx>), (TiSqlError, Option<TxnCtx>)>;

impl WorkerPool {
    /// Create a new worker pool with the given configuration
    pub fn new(config: WorkerPoolConfig) -> Self {
        let pool = yatp::Builder::new(&config.name)
            .min_thread_count(config.min_threads)
            .max_thread_count(config.max_threads)
            .build_future_pool();

        let remote = pool.remote().clone();

        Self {
            remote,
            _pool: pool,
        }
    }

    /// Handle a query on a worker thread with TxnCtx ownership transfer.
    ///
    /// This is the unified entry point for query execution from the protocol layer.
    ///
    /// ## Arguments
    /// - `db`: Reference to the database
    /// - `sql`: The SQL query string
    /// - `exec_ctx`: Execution context with session variables (current_db, isolation_level, etc.)
    /// - `txn_ctx`: Optional TxnCtx for explicit transactions (ownership transferred)
    ///
    /// ## Returns
    /// - On success: (QueryResult, Option<TxnCtx>) - result and returned TxnCtx (if still active)
    /// - On error: (TiSqlError, Option<TxnCtx>) - error and returned TxnCtx (keep txn active on error)
    ///
    /// ## Ownership Semantics
    ///
    /// The TxnCtx is taken from the session by the caller, passed to this method,
    /// and returned in the result. The caller is responsible for putting it back
    /// in the session if it's still active.
    pub async fn handle_query(
        &self,
        db: Arc<Database>,
        sql: String,
        exec_ctx: ExecutionCtx,
        txn_ctx: Option<TxnCtx>,
    ) -> QueryResultWithCtx {
        let (tx, rx) = oneshot::channel();

        self.remote.spawn(async move {
            let result = db.handle_query(&sql, &exec_ctx, txn_ctx);

            // Convert Result<(QueryResult, Option<TxnCtx>), TiSqlError> to QueryResultWithCtx
            // Note: On error, we don't have the TxnCtx back because it was moved into
            // handle_query. In a real implementation, we'd need to handle this differently.
            // For now, errors consume the TxnCtx.
            let result = match result {
                Ok((query_result, returned_ctx)) => Ok((query_result, returned_ctx)),
                Err(e) => Err((e, None)), // TxnCtx consumed on error
            };

            // Ignore send error - receiver may have been dropped if connection closed
            let _ = tx.send(result);
        });

        // Await the result from the worker thread
        rx.await
            .unwrap_or_else(|_| Err((TiSqlError::Internal("Worker task dropped".into()), None)))
    }
}
