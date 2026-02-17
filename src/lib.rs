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

// ============================================================================
// Module Structure
// ============================================================================
//
// Layer separation: upper layers only access lower layers via traits (interfaces)
//
// Public modules (expose interfaces/traits):
//   - error, types, util, codec - common types
//   - session - Session/QueryCtx (needed by protocol layer)
//   - protocol, worker - server infrastructure
//
// Internal modules (implementations hidden):
//   - catalog - Catalog trait public, MemoryCatalog internal
//   - clog - ClogService trait public, FileClogService internal
//   - storage - StorageEngine trait public, implementations internal
//   - transaction - TxnService trait public, TransactionService/ConcurrencyManager internal
//   - sql, executor - encapsulated in SQLEngine

// Public modules - common types and server infrastructure
pub mod codec;
pub mod error;
pub mod io;
pub mod lsn;
pub mod protocol;
pub mod session;
pub mod types;
pub mod util;
pub mod worker;

// Internal modules - only traits are re-exported, not implementations
mod catalog;
mod clog;
mod executor;
pub(crate) mod inner_table;
mod sql;
pub mod storage;
mod transaction;
mod tso;

// Re-export public interfaces (traits only) and commonly used types
pub use catalog::Catalog;
pub use clog::{ClogFsyncFuture, ClogService};
pub use lsn::{new_lsn_provider, LsnProvider, SharedLsnProvider};
pub use protocol::{MySqlServer, MYSQL_DEFAULT_PORT};
pub use session::{ExecutionCtx, Priority, QueryCtx, Session, SessionRegistry, SessionVars};
pub use storage::{PessimisticStorage, StorageEngine, V26BoundaryMode};
pub use transaction::{CommitInfo, TxnCtx, TxnService, TxnState};
pub use tso::TsoService;
pub use worker::QueryResponse;

// ============================================================================
// Test-only exports - implementation details for integration tests
// ============================================================================
// These are exposed for testing purposes only. They are NOT part of the public API.
#[doc(hidden)]
pub mod testkit {
    //! Test utilities and implementation re-exports.
    //!
    //! This module exposes internal implementation details for integration tests.
    //! These are NOT part of the public API and may change without notice.
    pub use crate::clog::{
        ClogBatch, ClogEntry, ClogOp, ClogOpRef, FileClogConfig, FileClogService, TruncateStats,
    };

    // Arena (from util - it's a general-purpose allocator)
    pub use crate::util::{ArenaConfig, PageArena, DEFAULT_PAGE_SIZE};

    // Production memtable engine
    pub use crate::storage::{MemTableEngine, MemoryStats, VersionedMemTableEngine};

    // LSM storage engine for testing
    pub use crate::storage::{
        CompactionExecutor, CompactionPicker, CompactionScheduler, CompactionTask, IlogConfig,
        IlogService, IlogTruncateStats, LsmConfig, LsmConfigBuilder, LsmEngine, LsmRecovery,
        LsmStats, ManifestDelta, MemTable, RecoveryResult, SstBuilder, SstBuilderOptions,
        SstIterator, SstMeta, SstReader, SstReaderRef, Version,
    };

    // Re-export FlushScheduler for testing
    pub use crate::storage::FlushScheduler;

    // Re-export IoService for testing
    pub use crate::io::IoService;

    pub use crate::transaction::{ConcurrencyManager, TransactionService};
    pub use crate::tso::LocalTso;

    // Executor types for testing
    pub use crate::executor::ExecutionResult;

    // Inner table infrastructure for testing
    pub use crate::inner_table::InnerSession;

    // Test helper extension trait for TransactionService
    use crate::clog::ClogService;
    use crate::error::Result;
    use crate::storage::PessimisticStorage;
    use crate::transaction::{CommitInfo, TxnService};
    use crate::tso::TsoService;

    use std::future::Future;

    /// Extension trait for TransactionService with test-only autocommit helpers.
    pub trait TxnServiceTestExt {
        /// Execute a single put with autocommit (test helper).
        fn autocommit_put<'a>(
            &'a self,
            key: &'a [u8],
            value: &'a [u8],
        ) -> impl Future<Output = Result<CommitInfo>> + Send + 'a;

        /// Execute a single delete with autocommit (test helper).
        fn autocommit_delete<'a>(
            &'a self,
            key: &'a [u8],
        ) -> impl Future<Output = Result<CommitInfo>> + Send + 'a;
    }

    impl<S, C, T> TxnServiceTestExt for TransactionService<S, C, T>
    where
        S: PessimisticStorage + 'static,
        C: ClogService + 'static,
        T: TsoService,
    {
        async fn autocommit_put(&self, key: &[u8], value: &[u8]) -> Result<CommitInfo> {
            let mut ctx = self.begin(false)?;
            match self.put(&mut ctx, key.to_vec(), value.to_vec()).await {
                Ok(()) => self.commit(ctx).await,
                Err(e) => {
                    let _ = self.rollback(ctx);
                    Err(e)
                }
            }
        }

        async fn autocommit_delete(&self, key: &[u8]) -> Result<CommitInfo> {
            let mut ctx = self.begin(false)?;
            match self.delete(&mut ctx, key.to_vec()).await {
                Ok(()) => self.commit(ctx).await,
                Err(e) => {
                    let _ = self.rollback(ctx);
                    Err(e)
                }
            }
        }
    }
}

// Internal imports (not re-exported)
use catalog::MvccCatalog;
use clog::{FileClogService, TruncateStats};
use error::Result;
use executor::{ExecutionOutput, ExecutionResult, Executor, SimpleExecutor};
use sql::{Binder, Parser};
use storage::{
    CompactionScheduler, FlushScheduler, IlogService, IlogTruncateStats, LsmEngine, LsmRecovery,
};
use transaction::{ConcurrencyManager, TransactionService};
use tso::LocalTso;
use types::{Lsn, Value};
use util::Timer;

use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "failpoints")]
use fail::fail_point;

// ============================================================================
// Database Configuration
// ============================================================================

/// Database configuration
#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    /// Data directory for persistence
    pub data_dir: PathBuf,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
        }
    }
}

impl DatabaseConfig {
    /// Create config with custom data directory
    pub fn with_data_dir(dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: dir.into(),
        }
    }
}

// ============================================================================
// SQL Engine - Parser, Binder, Executor
// ============================================================================

/// SQL Engine handles SQL text processing: parsing, binding, and execution.
///
/// This is the core SQL processing pipeline that transforms SQL strings
/// into query results through the transaction service.
///
/// All internal components (Parser, Binder, Executor) are encapsulated.
/// External callers only see the `handle_mp_query` interface.
struct SQLEngine {
    parser: Parser,
    executor: SimpleExecutor,
}

impl SQLEngine {
    /// Create a new SQL engine.
    fn new() -> Self {
        Self {
            parser: Parser::new(),
            executor: SimpleExecutor::new(),
        }
    }

    /// Handle MySQL protocol query text.
    ///
    /// Flow: SQL text -> parse -> bind -> execute through TxnService
    ///
    /// All SQL execution goes through this method. Internal components
    /// (Parser, Binder, Executor) are not exposed to callers.
    async fn handle_mp_query<T: TxnService, C: Catalog>(
        &self,
        sql: &str,
        query_ctx: &session::QueryCtx,
        txn_service: &T,
        catalog: &C,
    ) -> Result<ExecutionResult> {
        // Parse SQL text into AST
        let stmt = self.parser.parse_one(sql)?;

        // Use latest schema for binding (Timestamp::MAX).
        // For proper MVCC schema versioning within a transaction, the executor
        // would need to use a start_ts from the transaction context.
        let binder = Binder::new(catalog, &query_ctx.current_db);
        let plan = binder.bind(stmt)?;

        // Execute through TxnService
        // TODO: Use query_ctx for execution options (priority, isolation level, etc.)
        self.executor.execute(plan, txn_service, catalog).await
    }

    /// Execute SQL with session context for explicit transaction support.
    ///
    /// This method handles transaction control statements (BEGIN, COMMIT, ROLLBACK)
    /// and uses the session's active transaction context when available.
    async fn handle_mp_query_with_session<T: TxnService, C: Catalog>(
        &self,
        sql: &str,
        query_ctx: &session::QueryCtx,
        txn_service: &T,
        catalog: &C,
        session: &mut session::Session,
    ) -> Result<ExecutionResult> {
        // Parse SQL text into AST
        let stmt = self.parser.parse_one(sql)?;

        // Bind to logical plan
        let binder = Binder::new(catalog, &query_ctx.current_db);
        let plan = binder.bind(stmt)?;

        // Execute with session context for transaction control
        self.executor
            .execute_with_session(plan, txn_service, catalog, session)
            .await
    }
}

// ============================================================================
// Database - Main Entry Point
// ============================================================================

/// Internal type alias for concrete storage engine.
type DbStorage = LsmEngine;

/// Internal type alias for concrete transaction service.
/// Not exposed publicly - callers only see TxnService trait.
type DbTxnService = TransactionService<DbStorage, FileClogService, LocalTso>;

/// TiSQL Database instance.
///
/// Database is the main entry point that coordinates:
/// - SQL processing via SQLEngine
/// - Transaction management via TxnService
/// - Schema metadata via Catalog
///
/// ## Layer Separation
///
/// All internal components are encapsulated:
/// - SQLEngine (Parser, Binder, Executor) - not exposed
/// - TransactionService implementation - not exposed
/// - Storage engine implementation - not exposed
/// - Clog implementation - not exposed
///
/// External callers interact only through public methods like `handle_mp_query`.
/// Internal type alias for concrete catalog.
type DbCatalog = MvccCatalog<DbTxnService>;

pub struct Database {
    /// SQL engine for parsing, binding, execution (encapsulated)
    sql_engine: SQLEngine,
    /// Transaction service - internal implementation hidden
    txn_service: Arc<DbTxnService>,
    /// Schema metadata catalog (MVCC-based, persistent)
    catalog: DbCatalog,
    /// Ilog service for SST metadata persistence (kept for proper shutdown)
    #[allow(dead_code)]
    ilog: Arc<IlogService>,
    /// LSM storage engine (kept for flush on shutdown)
    storage: Arc<LsmEngine>,
    /// Background flush scheduler (Drop stops the worker)
    /// Declared before compaction_scheduler so it's dropped first (Rust drops in declaration order).
    flush_scheduler: FlushScheduler,
    /// Background compaction scheduler (Drop stops the worker)
    compaction_scheduler: CompactionScheduler,
    /// Background GC worker for drop-table data cleanup (Drop stops the worker)
    gc_worker: inner_table::gc_worker::GcWorker<DbTxnService>,
    /// Dedicated worker runtime for query execution (separate from protocol I/O).
    /// Created in `open()` for production; `None` in tests (falls back to tokio::spawn).
    worker_runtime: Option<tokio::runtime::Runtime>,
    /// Background runtime for flush, compaction, GC workers.
    bg_runtime: Option<tokio::runtime::Runtime>,
    /// I/O runtime for group commit (clog + ilog) and io_uring.
    io_runtime: Option<tokio::runtime::Runtime>,
    /// Registry of active explicit transactions for GC safe point computation.
    session_registry: Arc<session::SessionRegistry>,
}

/// One-shot log GC statistics.
#[derive(Debug, Default)]
pub struct LogGcStats {
    /// `Version.flushed_lsn` — the max LSN persisted to SST files.
    pub flushed_lsn: Lsn,
    /// Safe clog truncation boundary: `min(flushed_lsn, min_unflushed_lsn - 1)`.
    ///
    /// This accounts for the race window in `write_batch_inner()` where a
    /// lower-LSN write can land in a newer memtable than a higher-LSN write.
    /// Without this, clog truncation could delete entries still only in memory.
    pub safe_lsn: Lsn,
    /// Checkpoint LSN written in this GC cycle.
    pub checkpoint_lsn: Lsn,
    /// Clog truncation result.
    pub clog: TruncateStats,
    /// Ilog truncation result.
    pub ilog: IlogTruncateStats,
}

impl Database {
    /// Open database with persistence and recovery.
    ///
    /// This is the only way to create a Database instance.
    /// All databases use file-based persistence with LSM storage engine.
    pub fn open(config: DatabaseConfig) -> Result<Self> {
        log_info!("Opening TiSQL database at {:?}", config.data_dir);

        // 1. Create I/O runtime FIRST — recovery needs it for GroupCommitWriter + IoService
        let io_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("tisql-io")
            .enable_all()
            .build()
            .map_err(|e| {
                crate::error::TiSqlError::Internal(format!("Failed to create I/O runtime: {e}"))
            })?;

        log_info!("I/O runtime created with 2 threads");

        // 2. Recovery uses io_runtime handle for GroupCommitWriter + IoService
        let recovery = LsmRecovery::new(&config.data_dir);
        let recovery_result = recovery.recover(io_runtime.handle())?;

        log_info!(
            "LSM recovery complete: {} clog entries replayed, {} txns, flushed_lsn={}, max_commit_ts={}",
            recovery_result.stats.clog_entries,
            recovery_result.stats.txn_count,
            recovery_result.stats.flushed_lsn,
            recovery_result.stats.max_commit_ts
        );

        // Wrap storage in Arc
        let storage = Arc::new(recovery_result.engine);

        // 3. Create background runtime for flush, compaction, GC
        let bg_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("tisql-bg")
            .enable_all()
            .build()
            .map_err(|e| {
                crate::error::TiSqlError::Internal(format!(
                    "Failed to create background runtime: {e}"
                ))
            })?;

        log_info!("Background runtime created with 2 threads");

        // Start background flush scheduler on bg runtime
        let flush_scheduler = FlushScheduler::new(Arc::clone(&storage));
        flush_scheduler.start(bg_runtime.handle());

        // Start background compaction scheduler on bg runtime
        let compaction_scheduler = CompactionScheduler::new(Arc::clone(&storage));
        compaction_scheduler.start(bg_runtime.handle());

        // Wire: flush completion → notify compaction scheduler
        storage.set_compaction_notify(compaction_scheduler.notifier());

        // Create TSO service, starting from recovered max_commit_ts + 1
        let tso = Arc::new(LocalTso::new(recovery_result.stats.max_commit_ts + 1));

        // Create concurrency manager with recovered max_ts
        let concurrency_manager =
            Arc::new(ConcurrencyManager::new(recovery_result.stats.max_commit_ts));

        // Create transaction service with recovered components
        let txn_service = Arc::new(TransactionService::new_strict(
            Arc::clone(&storage),
            recovery_result.clog,
            Arc::clone(&tso),
            Arc::clone(&concurrency_manager),
        ));

        // Create MVCC catalog using same txn_service
        let catalog = MvccCatalog::new(Arc::clone(&txn_service));

        // Bootstrap if fresh database (no default schema exists)
        if !crate::io::block_on_sync(catalog.is_bootstrapped())? {
            log_info!("Fresh database - bootstrapping catalog");
            crate::io::block_on_sync(catalog.bootstrap())?;
        } else {
            // Load schema version from storage for existing database
            catalog.load_schema_version()?;
        }

        // Load pending GC tasks from inner tables and register with storage engine
        {
            use inner_table::catalog_loader::load_gc_tasks;
            match load_gc_tasks(txn_service.as_ref()) {
                Ok(tasks) => {
                    let mut pending_count = 0u32;
                    for task in tasks {
                        if task.status == "done" {
                            continue;
                        }
                        if task.drop_commit_ts > 0 {
                            storage.add_dropped_table(task.table_id, task.drop_commit_ts);
                            pending_count += 1;
                        } else {
                            // drop_commit_ts=0 means the commit happened but the
                            // follow-up update didn't complete. Use TSO as upper bound.
                            let ts = tso.get_ts();
                            storage.add_dropped_table(task.table_id, ts);
                            pending_count += 1;
                        }
                    }
                    if pending_count > 0 {
                        log_info!("Recovered {} pending GC delete-range tasks", pending_count);
                    }
                }
                Err(e) => {
                    log_warn!("Failed to load GC tasks (non-fatal): {}", e);
                }
            }
        }

        // Start background GC worker for drop-table cleanup
        let gc_worker =
            inner_table::gc_worker::GcWorker::new(Arc::clone(&storage), Arc::clone(&txn_service));
        gc_worker.start(bg_runtime.handle());

        // Create dedicated worker runtime for query execution.
        // Sized to available parallelism (defaults to CPU count).
        let worker_threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let worker_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name("tisql-worker")
            .enable_all()
            .build()
            .map_err(|e| {
                crate::error::TiSqlError::Internal(format!("Failed to create worker runtime: {e}"))
            })?;

        log_info!("Worker runtime created with {} threads", worker_threads);

        // Create session registry and wire GC safe point
        let session_registry = Arc::new(session::SessionRegistry::new());

        // Set initial GC safe point from TSO
        let initial_gc_ts = tso.get_ts();
        storage.set_gc_safe_point(initial_gc_ts);

        // Wire GC safe point updater: computes safe_point from active sessions
        {
            let registry = Arc::clone(&session_registry);
            let txn_svc = Arc::clone(&txn_service);
            storage.set_gc_safe_point_updater(Arc::new(move || -> u64 {
                match registry.min_start_ts() {
                    Some(min_ts) if min_ts > 0 => min_ts - 1,
                    _ => txn_svc.last_ts(),
                }
            }));
        }

        Ok(Self {
            sql_engine: SQLEngine::new(),
            txn_service,
            catalog,
            ilog: recovery_result.ilog,
            storage,
            flush_scheduler,
            compaction_scheduler,
            gc_worker,
            worker_runtime: Some(worker_runtime),
            bg_runtime: Some(bg_runtime),
            io_runtime: Some(io_runtime),
            session_registry,
        })
    }

    /// Get a handle to the dedicated worker runtime, if available.
    ///
    /// Returns `Some` in production (server mode), `None` in tests.
    /// When `None`, `dispatch_full_query` falls back to `tokio::spawn`.
    pub fn worker_handle(&self) -> Option<&tokio::runtime::Handle> {
        self.worker_runtime.as_ref().map(|rt| rt.handle())
    }

    /// Get a handle to the background runtime, if available.
    pub fn bg_handle(&self) -> Option<&tokio::runtime::Handle> {
        self.bg_runtime.as_ref().map(|rt| rt.handle())
    }

    /// Get a handle to the I/O runtime, if available.
    pub fn io_handle(&self) -> Option<&tokio::runtime::Handle> {
        self.io_runtime.as_ref().map(|rt| rt.handle())
    }

    /// Get the session registry for tracking active transactions.
    pub fn session_registry(&self) -> &Arc<session::SessionRegistry> {
        &self.session_registry
    }

    /// Handle MySQL protocol query text (COM_QUERY).
    ///
    /// This is the main entry point for SQL execution from the MySQL protocol.
    /// For prepared statements, use `handle_mp_prepare` and `handle_mp_execute` (TODO).
    ///
    /// Note: This method creates a temporary QueryCtx for backward compatibility.
    /// The proper way is to use `handle_mp_query_with_session` which takes a Session.
    pub async fn handle_mp_query(&self, sql: &str) -> Result<QueryResult> {
        // Create a temporary QueryCtx for backward compatibility
        let query_ctx = session::QueryCtx::new();
        self.handle_mp_query_with_ctx(sql, &query_ctx).await
    }

    /// Handle MySQL protocol query text with mutable Session for transaction control.
    ///
    /// This is the preferred method for the protocol layer - it supports:
    /// - BEGIN / START TRANSACTION: Starts explicit transaction
    /// - COMMIT: Commits the current explicit transaction
    /// - ROLLBACK: Rolls back the current explicit transaction
    /// - Other statements: Uses session's active transaction or auto-commit mode
    pub async fn handle_mp_query_with_session_mut(
        &self,
        sql: &str,
        session: &mut session::Session,
    ) -> Result<QueryResult> {
        let timer = Timer::new("query");
        let query_ctx = session.new_query_ctx();

        // Use session-aware execution for transaction control
        let result = self
            .sql_engine
            .handle_mp_query_with_session(
                sql,
                &query_ctx,
                self.txn_service.as_ref(),
                &self.catalog,
                session,
            )
            .await?;

        // Convert ExecutionResult to QueryResult
        let query_result = match result {
            ExecutionResult::Rows { schema, rows } => {
                let row_count = rows.len();
                let columns: Vec<String> = schema
                    .columns()
                    .iter()
                    .map(|c| c.name().to_string())
                    .collect();
                let data: Vec<Vec<String>> = rows
                    .iter()
                    .map(|row| row.iter().map(value_to_string).collect())
                    .collect();

                log_trace!(
                    "SQL: {} | elapsed={:.3}ms rows={}",
                    truncate_sql(sql, 50),
                    timer.elapsed_ms(),
                    row_count
                );

                QueryResult::Rows { columns, data }
            }
            ExecutionResult::Affected { count } => {
                log_trace!(
                    "SQL: {} | elapsed={:.3}ms affected={}",
                    truncate_sql(sql, 50),
                    timer.elapsed_ms(),
                    count
                );
                QueryResult::Affected(count)
            }
            ExecutionResult::Ok | ExecutionResult::OkWithEffect(_) => {
                log_trace!(
                    "SQL: {} | elapsed={:.3}ms",
                    truncate_sql(sql, 50),
                    timer.elapsed_ms()
                );
                QueryResult::Ok
            }
        };

        Ok(query_result)
    }

    /// Handle MySQL protocol query text with a QueryCtx.
    ///
    /// This method takes a pre-created QueryCtx that contains session context
    /// (current_db, isolation_level, etc.). All SQL processing (parse, bind,
    /// execute) is delegated to SQLEngine.
    ///
    /// This is the primary method used by the protocol layer via WorkerPool.
    pub async fn handle_mp_query_with_ctx(
        &self,
        sql: &str,
        query_ctx: &session::QueryCtx,
    ) -> Result<QueryResult> {
        let timer = Timer::new("query");

        // Delegate all SQL processing to SQLEngine
        // SQLEngine handles: parse -> bind -> execute through TxnService
        let result = self
            .sql_engine
            .handle_mp_query(sql, query_ctx, self.txn_service.as_ref(), &self.catalog)
            .await?;

        // Convert ExecutionResult to QueryResult
        let query_result = match result {
            ExecutionResult::Rows { schema, rows } => {
                let row_count = rows.len();
                let columns: Vec<String> = schema
                    .columns()
                    .iter()
                    .map(|c| c.name().to_string())
                    .collect();
                let data: Vec<Vec<String>> = rows
                    .iter()
                    .map(|row| row.iter().map(value_to_string).collect())
                    .collect();

                log_trace!(
                    "SQL: {} | elapsed={:.3}ms rows={}",
                    truncate_sql(sql, 50),
                    timer.elapsed_ms(),
                    row_count
                );

                QueryResult::Rows { columns, data }
            }
            ExecutionResult::Affected { count } => {
                log_trace!(
                    "SQL: {} | elapsed={:.3}ms affected={}",
                    truncate_sql(sql, 50),
                    timer.elapsed_ms(),
                    count
                );
                QueryResult::Affected(count)
            }
            ExecutionResult::Ok | ExecutionResult::OkWithEffect(_) => {
                log_trace!(
                    "SQL: {} | elapsed={:.3}ms",
                    truncate_sql(sql, 50),
                    timer.elapsed_ms()
                );
                QueryResult::Ok
            }
        };

        Ok(query_result)
    }

    /// List tables in the specified schema.
    pub fn list_tables(&self, schema: &str) -> Result<Vec<String>> {
        let tables = self.catalog.list_tables(schema)?;
        Ok(tables.into_iter().map(|t| t.name().to_string()).collect())
    }

    /// List all schemas (databases).
    pub fn list_schemas(&self) -> Result<Vec<String>> {
        self.catalog.list_schemas()
    }

    /// Get current V2.6 boundary mode (`off|shadow|on`).
    pub fn v26_boundary_mode(&self) -> V26BoundaryMode {
        self.storage.get_v26_mode()
    }

    /// Set V2.6 boundary mode at runtime for immediate rollout/rollback.
    pub fn set_v26_boundary_mode(&self, mode: V26BoundaryMode) {
        self.storage.set_v26_mode(mode);
    }

    // ========================================================================
    // Inner Session Factory
    // ========================================================================

    /// Create a new inner session for internal SQL execution.
    ///
    /// Requires Database to be Arc-wrapped by the caller.
    pub fn new_inner_session(self: &Arc<Self>, current_db: &str) -> inner_table::InnerSession {
        inner_table::InnerSession::new(Arc::clone(self), current_db)
    }

    // ========================================================================
    // Transaction Control (for Protocol Layer)
    // ========================================================================

    /// Begin an explicit transaction.
    ///
    /// This is called by the protocol layer when handling BEGIN/START TRANSACTION.
    /// Returns a TxnCtx that should be stored in the Session.
    pub fn begin_explicit(&self, read_only: bool) -> Result<transaction::TxnCtx> {
        self.txn_service.begin_explicit(read_only)
    }

    /// Commit a transaction (blocks on clog fsync via block_on_sync).
    ///
    /// This is called by inner-table operations and catalog DDL.
    /// Worker-dispatched commits go through the async TxnService::commit() path.
    pub fn commit(&self, ctx: transaction::TxnCtx) -> Result<CommitInfo> {
        crate::io::block_on_sync(self.txn_service.commit(ctx))
    }

    /// Rollback a transaction.
    ///
    /// This is called by the protocol layer when handling ROLLBACK.
    /// Takes ownership of the TxnCtx from the Session.
    pub fn rollback(&self, ctx: transaction::TxnCtx) -> Result<()> {
        self.txn_service.rollback(ctx)
    }

    /// Run one log GC cycle:
    /// 1) checkpoint ilog
    /// 2) truncate clog up to safe_lsn
    /// 3) truncate ilog before checkpoint_lsn
    ///
    /// The safe truncation boundary is `min(flushed_lsn, min_unflushed_lsn - 1)`,
    /// which accounts for the race window where a lower-LSN write can land in a
    /// newer (unflushed) memtable. Using `flushed_lsn` alone would risk deleting
    /// clog entries that are still only in volatile memory.
    pub fn run_log_gc_once(&self) -> Result<LogGcStats> {
        // Hold manifest_lock during version capture + checkpoint write.
        // This ensures all ilog records with LSN < checkpoint_lsn are
        // reflected in `version` — safe to truncate.
        let (version, checkpoint_lsn) = {
            let _manifest = self.storage.manifest_guard();

            let version = self.storage.current_version();

            #[cfg(feature = "failpoints")]
            fail_point!("log_gc_before_checkpoint");

            let checkpoint_lsn = self.ilog.write_checkpoint(version.as_ref())?;

            (version, checkpoint_lsn)
        };
        // manifest_lock released — flush/compaction can proceed

        // Compute authoritative V2.6 boundary from the SAME checkpointed version.
        let flushed_lsn = version.flushed_lsn();
        #[cfg(feature = "failpoints")]
        fail_point!("log_gc_after_checkpoint_before_safe_compute_v26");
        let safe_lsn = self
            .storage
            .compute_log_gc_boundary_with_caps(flushed_lsn)
            .safe_lsn;

        #[cfg(feature = "failpoints")]
        fail_point!("log_gc_after_safe_compute_before_clog_truncate_v26");

        #[cfg(feature = "failpoints")]
        fail_point!("log_gc_after_checkpoint_before_clog_truncate");

        let clog = self.txn_service.clog_service().truncate_to(safe_lsn)?;

        #[cfg(feature = "failpoints")]
        fail_point!("log_gc_after_clog_truncate_before_ilog_truncate");

        let ilog = self.ilog.truncate_before(checkpoint_lsn)?;

        #[cfg(feature = "failpoints")]
        fail_point!("log_gc_after_ilog_truncate");

        debug_assert!(
            safe_lsn <= flushed_lsn,
            "safe_lsn ({safe_lsn}) exceeds checkpointed flushed_lsn ({flushed_lsn})"
        );

        Ok(LogGcStats {
            flushed_lsn,
            safe_lsn,
            checkpoint_lsn,
            clog,
            ilog,
        })
    }

    // ========================================================================
    // Unified Query Execution (for Worker Pool)
    // ========================================================================

    /// Parse and bind SQL into a logical plan (fast, no I/O).
    ///
    /// This is the first phase of query processing, suitable for running
    /// inline on a tokio network thread. The resulting plan can then be
    /// dispatched to a blocking thread for execution.
    pub(crate) fn parse_and_bind(
        &self,
        sql: &str,
        exec_ctx: &session::ExecutionCtx,
    ) -> Result<sql::LogicalPlan> {
        let stmt = self.sql_engine.parser.parse_one(sql)?;
        let binder = sql::Binder::new(&self.catalog, &exec_ctx.current_db);
        binder.bind(stmt)
    }

    /// Execute a pre-bound logical plan.
    ///
    /// Returns `ExecutionOutput` (with lazy `Execution` for reads) and the
    /// optionally-updated `TxnCtx`. This method runs blocking I/O and should
    /// be called inside `tokio::task::spawn_blocking`.
    pub(crate) async fn execute_plan(
        &self,
        plan: sql::LogicalPlan,
        exec_ctx: &session::ExecutionCtx,
        txn_ctx: Option<transaction::TxnCtx>,
    ) -> Result<(ExecutionOutput, Option<transaction::TxnCtx>)> {
        let (output, ctx) = self
            .sql_engine
            .executor
            .execute_unified(
                plan,
                exec_ctx,
                self.txn_service.as_ref(),
                &self.catalog,
                txn_ctx,
            )
            .await?;

        // Intercept DDL effects to register dropped tables for GC
        if let ExecutionOutput::OkWithEffect(executor::DdlEffect::TableDropped {
            table_id,
            commit_ts,
        }) = &output
        {
            self.storage.add_dropped_table(*table_id, *commit_ts);
            self.gc_worker.notify();
        }

        Ok((output, ctx))
    }

    /// Close the database (flush memtables and sync logs).
    pub async fn close(&self) -> Result<()> {
        // Flush any pending memtable data to SSTs
        if let Err(e) = self.storage.flush_all_with_active() {
            log_warn!("Error flushing memtables on close: {}", e);
        }

        // Close commit log
        self.txn_service.clog_service().close().await?;

        // Close ilog
        self.ilog.close()?;

        log_info!("Database closed");
        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // 1. Stop background schedulers and GC worker
        self.gc_worker.stop();
        self.flush_scheduler.stop();
        self.compaction_scheduler.stop();

        // 2. Final flush (needs I/O runtime — ilog writes go through group commit)
        let _ = self.storage.flush_all_with_active();

        // 3. Close clog + ilog (sync pending writes)
        let _ = crate::io::block_on_sync(self.txn_service.clog_service().close());
        let _ = self.ilog.close();

        // 4. Shutdown all spawn_blocking task channels BEFORE dropping io_runtime.
        //
        // GroupCommitWriter (clog + ilog) and IoService each run a spawn_blocking
        // task on io_runtime that blocks on rx.recv(). Closing the sender channels
        // causes recv() to return Err, exiting the loops. Without this,
        // io_runtime.drop() blocks forever waiting for those tasks.
        self.txn_service.clog_service().shutdown();
        self.ilog.shutdown();
        self.storage.io_service().shutdown();

        // 5. Shut down runtimes.
        //
        // Tokio runtimes cannot be dropped from within an async context (panics).
        // If we are inside a tokio runtime (tests, or nested drop), move the
        // runtime objects to a dedicated thread for shutdown.
        let worker_rt = self.worker_runtime.take();
        let bg_rt = self.bg_runtime.take();
        let io_rt = self.io_runtime.take();

        let do_shutdown = move || {
            drop(worker_rt);
            drop(bg_rt);
            drop(io_rt);
        };

        if tokio::runtime::Handle::try_current().is_ok() {
            // Inside a tokio context — must not block. Spawn a thread for shutdown.
            std::thread::spawn(do_shutdown);
        } else {
            do_shutdown();
        }
    }
}

// ============================================================================
// Query Result
// ============================================================================

/// Query result for user-facing output
#[derive(Debug)]
pub enum QueryResult {
    Rows {
        columns: Vec<String>,
        data: Vec<Vec<String>>,
    },
    Affected(u64),
    Ok,
}

impl QueryResult {
    /// Format result as a table string
    pub fn to_table_string(&self) -> String {
        match self {
            QueryResult::Rows { columns, data } => {
                if columns.is_empty() {
                    return "Empty set".to_string();
                }

                // Calculate column widths
                let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
                for row in data {
                    for (i, val) in row.iter().enumerate() {
                        if i < widths.len() {
                            widths[i] = widths[i].max(val.len());
                        }
                    }
                }

                let mut output = String::new();

                // Header separator
                let sep: String = widths
                    .iter()
                    .map(|w| "-".repeat(*w + 2))
                    .collect::<Vec<_>>()
                    .join("+");
                output.push_str(&format!("+{sep}+\n"));

                // Header
                let header: String = columns
                    .iter()
                    .zip(&widths)
                    .map(|(c, w)| format!(" {:width$} ", c, width = *w))
                    .collect::<Vec<_>>()
                    .join("|");
                output.push_str(&format!("|{header}|\n"));
                output.push_str(&format!("+{sep}+\n"));

                // Data rows
                for row in data {
                    let row_str: String = row
                        .iter()
                        .zip(&widths)
                        .map(|(v, w)| format!(" {:width$} ", v, width = *w))
                        .collect::<Vec<_>>()
                        .join("|");
                    output.push_str(&format!("|{row_str}|\n"));
                }
                output.push_str(&format!("+{sep}+\n"));

                output.push_str(&format!("{} row(s) in set", data.len()));
                output
            }
            QueryResult::Affected(count) => {
                format!("Query OK, {count} row(s) affected")
            }
            QueryResult::Ok => "Query OK".to_string(),
        }
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Truncate SQL for logging (avoid huge queries in logs)
fn truncate_sql(sql: &str, max_len: usize) -> String {
    let sql = sql.trim().replace('\n', " ");
    if sql.len() <= max_len {
        sql
    } else {
        format!("{}...", &sql[..max_len])
    }
}

fn value_to_string(val: &Value) -> String {
    match val {
        Value::Null => "NULL".to_string(),
        Value::Boolean(b) => if *b { "true" } else { "false" }.to_string(),
        Value::TinyInt(v) => v.to_string(),
        Value::SmallInt(v) => v.to_string(),
        Value::Int(v) => v.to_string(),
        Value::BigInt(v) => v.to_string(),
        Value::Float(v) => v.to_string(),
        Value::Double(v) => v.to_string(),
        Value::Decimal(v) => v.clone(),
        Value::String(v) => v.clone(),
        Value::Bytes(v) => format!("{v:?}"),
        Value::Date(v) => format!("DATE({v})"),
        Value::Time(v) => format!("TIME({v})"),
        Value::DateTime(v) => format!("DATETIME({v})"),
        Value::Timestamp(v) => format!("TIMESTAMP({v})"),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inner_table::catalog_loader::load_gc_tasks;
    use tempfile::tempdir;
    use tokio::time::{sleep, Duration};

    fn create_test_db() -> (Database, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Database::open(config).unwrap();
        (db, dir)
    }

    #[tokio::test]
    async fn test_select_literal() {
        let (db, _dir) = create_test_db();
        let result = db.handle_mp_query("SELECT 1 + 1").await.unwrap();
        match result {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1);
                assert_eq!(data[0][0], "2");
            }
            _ => panic!("Expected rows"),
        }
    }

    #[tokio::test]
    async fn test_create_and_insert() {
        let (db, _dir) = create_test_db();

        db.handle_mp_query("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))")
            .await
            .unwrap();

        let result = db
            .handle_mp_query("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            .await
            .unwrap();
        match result {
            QueryResult::Affected(count) => assert_eq!(count, 1),
            _ => panic!("Expected affected count"),
        }

        let result = db
            .handle_mp_query("SELECT id, name FROM users")
            .await
            .unwrap();
        match result {
            QueryResult::Rows { data, columns } => {
                assert_eq!(columns, vec!["id", "name"]);
                assert_eq!(data.len(), 1);
                assert_eq!(data[0][0], "1");
                assert_eq!(data[0][1], "Alice");
            }
            _ => panic!("Expected rows"),
        }
    }

    #[tokio::test]
    async fn test_database_with_durability() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());

        // Create database and insert data
        {
            let db = Database::open(config.clone()).unwrap();
            db.handle_mp_query("CREATE TABLE t (id INT PRIMARY KEY, val VARCHAR(100))")
                .await
                .unwrap();
            db.handle_mp_query("INSERT INTO t VALUES (1, 'hello')")
                .await
                .unwrap();
            db.handle_mp_query("INSERT INTO t VALUES (2, 'world')")
                .await
                .unwrap();
            db.close().await.unwrap();
        }

        // Reopen - data should be in storage but catalog needs recovery
        // For now, this tests that the commit log file is created and can be recovered
        {
            let _db = Database::open(config).unwrap();
            // Note: Catalog is not persisted yet, so CREATE TABLE won't survive
            // But commit log entries are persisted
        }
    }

    #[tokio::test]
    async fn test_drop_table_gc_not_done_while_data_still_in_memtable() {
        let (db, _dir) = create_test_db();

        db.handle_mp_query("CREATE TABLE gc_pending (id INT PRIMARY KEY, val INT)")
            .await
            .unwrap();
        db.handle_mp_query("INSERT INTO gc_pending VALUES (1, 10), (2, 20), (3, 30)")
            .await
            .unwrap();

        // Make the task immediately eligible. The regression is about overlap checks,
        // not safe-point timing.
        db.storage.set_gc_safe_point(u64::MAX);

        db.handle_mp_query("DROP TABLE gc_pending").await.unwrap();

        // Keep waking the worker; status should remain pending until memtable data
        // is flushed/compacted away.
        let mut latest_status: Option<String> = None;
        for _ in 0..30 {
            db.gc_worker.notify();
            sleep(Duration::from_millis(30)).await;

            let tasks = load_gc_tasks(db.txn_service.as_ref()).unwrap();
            if let Some(task) = tasks.first() {
                latest_status = Some(task.status.clone());
                if task.status == "done" {
                    break;
                }
            }
        }

        let status = latest_status.expect("expected at least one gc task row");
        assert_eq!(
            status, "pending",
            "GC task should stay pending while dropped-table keys are still in memtables"
        );
    }

    #[tokio::test]
    async fn test_run_log_gc_once_reclaims_logs_and_recovers() {
        use crate::clog::{ClogBatch, ClogEntry, ClogOp, ClogService};
        use crate::storage::WriteBatch;

        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());

        let db = Database::open(config.clone()).unwrap();

        // Txn 1: write durable clog + memtable entry.
        let key1 = b"log_gc_key1".to_vec();
        let value1 = b"log_gc_value1".to_vec();
        let mut clog_batch1 = ClogBatch::new();
        clog_batch1.add(ClogEntry {
            lsn: 0,
            txn_id: 42,
            op: ClogOp::Put {
                key: key1.clone(),
                value: value1.clone(),
            },
        });
        let lsn1 = db
            .txn_service
            .clog_service()
            .write(&mut clog_batch1, true)
            .unwrap()
            .await
            .unwrap();

        let mut write_batch1 = WriteBatch::new();
        write_batch1.put(key1.clone(), value1.clone());
        write_batch1.set_commit_ts(100);
        write_batch1.set_clog_lsn(lsn1);
        db.storage.write_batch(write_batch1).unwrap();

        // Force key1 into a frozen memtable so the next flush has a remaining
        // in-memory bound from key2 (below), allowing flushed_lsn to advance.
        db.storage.freeze_active();

        // Txn 2: stays in active during first flush to provide min_lsn bound.
        let key2 = b"log_gc_key2".to_vec();
        let value2 = b"log_gc_value2".to_vec();
        let mut clog_batch2 = ClogBatch::new();
        clog_batch2.add(ClogEntry {
            lsn: 0,
            txn_id: 43,
            op: ClogOp::Put {
                key: key2.clone(),
                value: value2.clone(),
            },
        });
        let lsn2 = db
            .txn_service
            .clog_service()
            .write(&mut clog_batch2, true)
            .unwrap()
            .await
            .unwrap();

        let mut write_batch2 = WriteBatch::new();
        write_batch2.put(key2.clone(), value2.clone());
        write_batch2.set_commit_ts(101);
        write_batch2.set_clog_lsn(lsn2);
        db.storage.write_batch(write_batch2).unwrap();

        // Flush all memtables; with two memtables this should advance boundary
        // enough to reclaim at least txn1's clog entry.
        db.storage.flush_all_with_active().unwrap();
        let flushed_lsn = db.storage.current_version().flushed_lsn();

        let clog_size_before = db.txn_service.clog_service().file_size().unwrap();
        let stats = db.run_log_gc_once().unwrap();

        assert!(
            stats.flushed_lsn >= lsn1,
            "log GC should use flushed_lsn that includes flushed clog entries"
        );
        assert_eq!(stats.flushed_lsn, flushed_lsn);
        // When all memtables are flushed, safe_lsn == flushed_lsn.
        assert_eq!(stats.safe_lsn, flushed_lsn);
        assert!(
            stats.checkpoint_lsn >= stats.flushed_lsn,
            "checkpoint lsn should not be older than flushed_lsn snapshot"
        );
        assert!(
            stats.clog.entries_removed > 0,
            "clog should reclaim entries after flush boundary advances"
        );
        assert_eq!(db.ilog.file_size().unwrap(), stats.ilog.new_file_size);
        assert!(
            db.txn_service.clog_service().file_size().unwrap() <= clog_size_before,
            "clog file size should not grow after truncation"
        );

        db.close().await.unwrap();
        drop(db);

        let db2 = Database::open(config).unwrap();
        assert_eq!(db2.storage.get(&key1).await.unwrap(), Some(value1));
        assert_eq!(db2.storage.get(&key2).await.unwrap(), Some(value2));
    }

    #[tokio::test]
    async fn test_where_clause() {
        let (db, _dir) = create_test_db();

        db.handle_mp_query("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
            .await
            .unwrap();
        db.handle_mp_query("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
            .await
            .unwrap();

        let result = db
            .handle_mp_query("SELECT a, b FROM t WHERE a > 1")
            .await
            .unwrap();
        match result {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 2);
            }
            _ => panic!("Expected rows"),
        }
    }

    #[tokio::test]
    async fn test_order_by() {
        let (db, _dir) = create_test_db();

        db.handle_mp_query("CREATE TABLE t (a INT PRIMARY KEY)")
            .await
            .unwrap();
        db.handle_mp_query("INSERT INTO t VALUES (3), (1), (2)")
            .await
            .unwrap();

        let result = db
            .handle_mp_query("SELECT a FROM t ORDER BY a")
            .await
            .unwrap();
        match result {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data[0][0], "1");
                assert_eq!(data[1][0], "2");
                assert_eq!(data[2][0], "3");
            }
            _ => panic!("Expected rows"),
        }
    }

    #[tokio::test]
    async fn test_limit() {
        let (db, _dir) = create_test_db();

        db.handle_mp_query("CREATE TABLE t (a INT PRIMARY KEY)")
            .await
            .unwrap();
        db.handle_mp_query("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
            .await
            .unwrap();

        let result = db.handle_mp_query("SELECT a FROM t LIMIT 3").await.unwrap();
        match result {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 3);
            }
            _ => panic!("Expected rows"),
        }
    }

    #[tokio::test]
    async fn test_update() {
        let (db, _dir) = create_test_db();

        db.handle_mp_query("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
            .await
            .unwrap();
        db.handle_mp_query("INSERT INTO t VALUES (1, 100), (2, 200)")
            .await
            .unwrap();

        db.handle_mp_query("UPDATE t SET val = 999 WHERE id = 1")
            .await
            .unwrap();

        let result = db
            .handle_mp_query("SELECT id, val FROM t WHERE id = 1")
            .await
            .unwrap();
        match result {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data[0][1], "999");
            }
            _ => panic!("Expected rows"),
        }
    }

    #[tokio::test]
    async fn test_delete() {
        let (db, _dir) = create_test_db();

        db.handle_mp_query("CREATE TABLE t (a INT PRIMARY KEY)")
            .await
            .unwrap();
        db.handle_mp_query("INSERT INTO t VALUES (1), (2), (3)")
            .await
            .unwrap();

        db.handle_mp_query("DELETE FROM t WHERE a = 2")
            .await
            .unwrap();

        let result = db.handle_mp_query("SELECT a FROM t").await.unwrap();
        match result {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 2);
            }
            _ => panic!("Expected rows"),
        }
    }
}
