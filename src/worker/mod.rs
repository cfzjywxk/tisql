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
//
// All DB-accessing commands (queries, BEGIN, COMMIT, ROLLBACK, SHOW) are
// dispatched to the worker pool to keep tokio threads free for network I/O.

use std::sync::Arc;

use tokio::sync::oneshot;
use yatp::pool::Remote;
use yatp::task::future::TaskCell;

use crate::error::TiSqlError;
use crate::session::ExecutionCtx;
use crate::transaction::{CommitInfo, TxnCtx};
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

/// Commands that can be dispatched to the worker pool.
///
/// Each variant represents a database operation that should run off the
/// tokio network thread to avoid blocking the event loop.
pub enum WorkerCommand {
    /// Execute a SQL query with optional transaction context.
    Query {
        sql: String,
        exec_ctx: ExecutionCtx,
        txn_ctx: Option<TxnCtx>,
    },
    /// BEGIN / START TRANSACTION.
    Begin { read_only: bool },
    /// COMMIT an explicit transaction.
    Commit { txn_ctx: TxnCtx },
    /// ROLLBACK an explicit transaction.
    Rollback { txn_ctx: TxnCtx },
    /// SHOW TABLES in a database.
    ShowTables { database: String },
    /// SHOW DATABASES.
    ShowDatabases,
}

/// Results returned from the worker pool.
pub enum WorkerResult {
    /// Query execution result with optional returned TxnCtx.
    Query {
        result: QueryResult,
        txn_ctx: Option<TxnCtx>,
    },
    /// A new transaction was started.
    Begin { txn_ctx: TxnCtx },
    /// Transaction committed successfully.
    Committed { info: CommitInfo },
    /// Transaction rolled back successfully.
    RolledBack,
    /// List of table names.
    Tables(Vec<String>),
    /// List of database/schema names.
    Databases(Vec<String>),
    /// An error occurred, with optional TxnCtx returned for session recovery.
    Error {
        error: TiSqlError,
        txn_ctx: Option<TxnCtx>,
    },
}

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

    /// Dispatch a command to the worker pool and await the result.
    ///
    /// This is the unified entry point for all DB-accessing operations.
    /// The command is executed on a yatp worker thread, keeping the
    /// tokio network thread free for I/O.
    ///
    /// The `Commit` variant uses `commit_async()` which yields during fsync,
    /// freeing the yatp thread for other work. All other variants remain
    /// synchronous (no I/O wait).
    pub async fn dispatch(&self, db: Arc<Database>, cmd: WorkerCommand) -> WorkerResult {
        let (tx, rx) = oneshot::channel();

        self.remote.spawn(async move {
            let result = Self::execute_command(&db, cmd).await;
            let _ = tx.send(result);
        });

        rx.await.unwrap_or_else(|_| WorkerResult::Error {
            error: TiSqlError::Internal("Worker task dropped".into()),
            txn_ctx: None,
        })
    }

    /// Execute a command on the worker thread.
    ///
    /// Only `Commit` actually awaits (on clog fsync). All other variants
    /// run synchronously — their CPU-bound work completes quickly.
    async fn execute_command(db: &Database, cmd: WorkerCommand) -> WorkerResult {
        match cmd {
            WorkerCommand::Query {
                sql,
                exec_ctx,
                txn_ctx,
            } => match db.handle_query(&sql, &exec_ctx, txn_ctx) {
                Ok((result, returned_ctx)) => WorkerResult::Query {
                    result,
                    txn_ctx: returned_ctx,
                },
                Err(e) => WorkerResult::Error {
                    error: e,
                    txn_ctx: None,
                },
            },
            WorkerCommand::Begin { read_only } => match db.begin_explicit(read_only) {
                Ok(ctx) => WorkerResult::Begin { txn_ctx: ctx },
                Err(e) => WorkerResult::Error {
                    error: e,
                    txn_ctx: None,
                },
            },
            WorkerCommand::Commit { txn_ctx } => match db.commit_async(txn_ctx).await {
                Ok(info) => WorkerResult::Committed { info },
                Err(e) => WorkerResult::Error {
                    error: e,
                    txn_ctx: None,
                },
            },
            WorkerCommand::Rollback { txn_ctx } => match db.rollback(txn_ctx) {
                Ok(()) => WorkerResult::RolledBack,
                Err(e) => WorkerResult::Error {
                    error: e,
                    txn_ctx: None,
                },
            },
            WorkerCommand::ShowTables { database } => match db.list_tables(&database) {
                Ok(tables) => WorkerResult::Tables(tables),
                Err(e) => WorkerResult::Error {
                    error: e,
                    txn_ctx: None,
                },
            },
            WorkerCommand::ShowDatabases => match db.list_schemas() {
                Ok(schemas) => WorkerResult::Databases(schemas),
                Err(e) => WorkerResult::Error {
                    error: e,
                    txn_ctx: None,
                },
            },
        }
    }

    /// Handle a query on a worker thread with TxnCtx ownership transfer.
    ///
    /// This is the legacy entry point kept for backward compatibility.
    /// New code should use `dispatch()` with `WorkerCommand::Query`.
    pub async fn handle_query(
        &self,
        db: Arc<Database>,
        sql: String,
        exec_ctx: ExecutionCtx,
        txn_ctx: Option<TxnCtx>,
    ) -> QueryResultWithCtx {
        let cmd = WorkerCommand::Query {
            sql,
            exec_ctx,
            txn_ctx,
        };
        match self.dispatch(db, cmd).await {
            WorkerResult::Query { result, txn_ctx } => Ok((result, txn_ctx)),
            WorkerResult::Error { error, txn_ctx } => Err((error, txn_ctx)),
            _ => Err((
                TiSqlError::Internal("Unexpected worker result for query".into()),
                None,
            )),
        }
    }
}
