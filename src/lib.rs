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
//   - util - shared utilities
//   - catalog - metadata traits and catalog::types
//   - catalog::types - SQL schema/value types
//   - session - Session/QueryCtx (needed by protocol layer)
//   - protocol, worker - server infrastructure
//   - tablet - storage/tablet engine module
//   - log - clog/ilog/lsn durability modules
//
// Internal modules (implementations hidden):
//   - transaction - TxnService trait public, TransactionService/ConcurrencyManager internal
//   - sql, executor - Parser and SimpleExecutor on Database

// Public modules - common types and server infrastructure
pub mod log;
pub mod protocol;
pub mod runtime;
pub mod session;
pub mod tablet;
pub mod util;
pub mod worker;

// Backward-compatible module aliases for existing paths.
pub use log::clog;
pub use log::lsn;
pub use util::codec;
pub use util::io;

// Internal modules - only traits are re-exported, not concrete implementations
pub mod catalog;
mod executor;
pub(crate) mod inner_table;
mod sql;
mod transaction;
mod tso;

// Re-export public interfaces (traits only) and commonly used types
pub use catalog::Catalog;
pub use clog::{ClogFsyncFuture, ClogService};
pub use lsn::{new_lsn_provider, LsnProvider, SharedLsnProvider};
pub use protocol::{MySqlServer, MYSQL_DEFAULT_PORT};
pub use runtime::{RuntimeThreadOverrides, RuntimeThreads};
pub use session::{ExecutionCtx, Priority, QueryCtx, Session, SessionRegistry, SessionVars};
pub use tablet::{PessimisticStorage, StorageEngine, V26BoundaryMode};
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
    pub use crate::tablet::{MemTableEngine, MemoryStats, VersionedMemTableEngine};

    // LSM storage engine for testing
    pub use crate::tablet::{
        CompactionExecutor, CompactionPicker, CompactionScheduler, CompactionTask, IlogConfig,
        IlogService, IlogTruncateStats, LsmConfig, LsmConfigBuilder, LsmEngine, LsmRecovery,
        LsmStats, ManifestDelta, MemTable, RecoveryResult, SstBuilder, SstBuilderOptions,
        SstIterator, SstMeta, SstReader, SstReaderRef, TabletManager, Version,
    };

    // Re-export FlushScheduler for testing
    pub use crate::tablet::FlushScheduler;

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
    use crate::inner_table::core_tables::ALL_META_TABLE_ID;
    use crate::tablet::PessimisticStorage;
    use crate::transaction::{CommitInfo, TxnService};
    use crate::tso::TsoService;
    use crate::util::error::Result;

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
            match self
                .put(&mut ctx, ALL_META_TABLE_ID, key.to_vec(), value.to_vec())
                .await
            {
                Ok(()) => self.commit(ctx).await,
                Err(e) => {
                    let _ = self.rollback(ctx);
                    Err(e)
                }
            }
        }

        async fn autocommit_delete(&self, key: &[u8]) -> Result<CommitInfo> {
            let mut ctx = self.begin(false)?;
            match self.delete(&mut ctx, ALL_META_TABLE_ID, key.to_vec()).await {
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
use catalog::types::{Lsn, Value};
use catalog::MvccCatalog;
use clog::{FileClogService, TruncateStats};
use executor::{ExecutionOutput, ExecutionResult, Executor, SimpleExecutor};
use sql::Parser;
use tablet::{
    route_table_to_tablet, GlobalLogGcBoundary, IlogService, IlogTruncateStats, LsmEngine,
    LsmRecovery, RoutedTabletStorage, TabletId, TabletManager,
};
use transaction::{ConcurrencyManager, TransactionService};
use tso::LocalTso;
use util::error::Result;

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
    /// Runtime thread-count overrides.
    pub runtime_threads: RuntimeThreadOverrides,
    /// Enable adaptive encoded result batches (phase 3).
    pub enable_encoded_result_batch: bool,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            runtime_threads: RuntimeThreadOverrides::default(),
            enable_encoded_result_batch: true,
        }
    }
}

impl DatabaseConfig {
    /// Create config with custom data directory
    pub fn with_data_dir(dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: dir.into(),
            ..Self::default()
        }
    }

    /// Set runtime thread-count overrides.
    pub fn with_runtime_threads(mut self, overrides: RuntimeThreadOverrides) -> Self {
        self.runtime_threads = overrides;
        self
    }

    /// Enable/disable adaptive encoded result batches.
    pub fn with_encoded_result_batch(mut self, enabled: bool) -> Self {
        self.enable_encoded_result_batch = enabled;
        self
    }
}

// ============================================================================
// Database - Main Entry Point
// ============================================================================

/// Internal type alias for concrete storage engine.
type DbStorage = RoutedTabletStorage;

/// Internal type alias for concrete transaction service.
/// Not exposed publicly - callers only see TxnService trait.
type DbTxnService = TransactionService<DbStorage, FileClogService, LocalTso>;

/// TiSQL Database instance.
///
/// Database is the main entry point that coordinates:
/// - SQL processing via Parser + SimpleExecutor
/// - Transaction management via TxnService
/// - Schema metadata via Catalog
///
/// ## Layer Separation
///
/// All internal components are encapsulated:
/// - Parser, Binder, Executor - not exposed
/// - TransactionService implementation - not exposed
/// - Storage engine implementation - not exposed
/// - Clog implementation - not exposed
///
/// External callers interact only through public methods like `execute_query`.
/// Internal type alias for concrete catalog.
type DbCatalog = MvccCatalog<DbTxnService>;

pub struct Database {
    /// SQL parser (encapsulated)
    parser: Parser,
    /// SQL executor (encapsulated)
    executor: SimpleExecutor,
    /// Transaction service - internal implementation hidden
    txn_service: Arc<DbTxnService>,
    /// Schema metadata catalog (MVCC-based, persistent)
    catalog: DbCatalog,
    /// Ilog service for SST metadata persistence (kept for proper shutdown)
    ilog: Arc<IlogService>,
    /// LSM storage engine (kept for flush on shutdown)
    storage: Arc<LsmEngine>,
    /// Tablet lifecycle manager (phase-4: per-tablet manifest + background workers).
    tablet_manager: Arc<TabletManager>,
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
    /// Result-path mode toggle for adaptive encoded batches.
    enable_encoded_result_batch: bool,
}

fn drop_runtime_safely(runtime: tokio::runtime::Runtime) {
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(move || {
            drop(runtime);
        });
    } else {
        drop(runtime);
    }
}

struct OpenRuntimeGuard {
    runtime: Option<tokio::runtime::Runtime>,
}

impl OpenRuntimeGuard {
    fn new(runtime: tokio::runtime::Runtime) -> Self {
        Self {
            runtime: Some(runtime),
        }
    }

    fn handle(&self) -> &tokio::runtime::Handle {
        self.runtime
            .as_ref()
            .expect("runtime guard must hold runtime")
            .handle()
    }

    fn into_inner(mut self) -> tokio::runtime::Runtime {
        self.runtime
            .take()
            .expect("runtime guard must hold runtime")
    }
}

impl Drop for OpenRuntimeGuard {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            drop_runtime_safely(runtime);
        }
    }
}

/// One-shot log GC statistics.
#[derive(Debug, Default)]
pub struct LogGcStats {
    /// Global checkpoint flushed cap: min(`Version.flushed_lsn`) across tablets.
    pub flushed_lsn: Lsn,
    /// Safe shared-clog truncation boundary across all mounted tablets.
    ///
    /// Computed as min of checkpoint cap and all conservative runtime caps
    /// (unflushed/in-flight/reservation).
    pub safe_lsn: Lsn,
    /// System-tablet checkpoint LSN written in this GC cycle.
    ///
    /// Kept for backward compatibility with existing single-tablet checks.
    pub checkpoint_lsn: Lsn,
    /// Clog truncation result.
    pub clog: TruncateStats,
    /// System-tablet ilog truncation result.
    ///
    /// Kept for backward compatibility with existing single-tablet checks.
    pub ilog: IlogTruncateStats,
    /// Detailed global boundary decomposition.
    pub boundary: GlobalLogGcBoundary,
    /// Checkpoint snapshots captured for each tablet.
    pub tablet_checkpoints: Vec<TabletGcCheckpoint>,
    /// Ilog truncation results for each tablet.
    pub tablet_ilogs: Vec<TabletIlogGcStats>,
}

/// One tablet checkpoint snapshot used during one GC cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TabletGcCheckpoint {
    pub tablet_id: TabletId,
    pub flushed_lsn: Lsn,
    pub checkpoint_lsn: Lsn,
}

/// One tablet ilog truncation result from one GC cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletIlogGcStats {
    pub tablet_id: TabletId,
    pub checkpoint_lsn: Lsn,
    pub truncate: IlogTruncateStats,
}

impl Database {
    /// Open database with persistence and recovery.
    ///
    /// This is the only way to create a Database instance.
    /// All databases use file-based persistence with LSM storage engine.
    pub fn open(config: DatabaseConfig) -> Result<Self> {
        log_info!("Opening TiSQL database at {:?}", config.data_dir);

        let cpu_count = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let planned_threads = config
            .runtime_threads
            .apply(RuntimeThreads::plan(cpu_count));
        let total_threads = planned_threads.protocol
            + planned_threads.worker
            + planned_threads.background
            + planned_threads.io;
        log_info!(
            "Runtime thread plan (cpu={}): protocol={}, worker={}, bg={}, io={}, total={}",
            cpu_count,
            planned_threads.protocol,
            planned_threads.worker,
            planned_threads.background,
            planned_threads.io,
            total_threads
        );
        if cpu_count <= 2 {
            log_warn!(
                "Detected low CPU count ({}). Throughput may be limited; consider explicit runtime thread overrides.",
                cpu_count
            );
        } else if total_threads > cpu_count {
            log_warn!(
                "Runtime split oversubscribes CPUs (cpu={}, total_threads={}). This is acceptable for role isolation in fast-dev mode.",
                cpu_count,
                total_threads
            );
        }

        // 1. Create I/O runtime FIRST — recovery needs it for GroupCommitWriter + IoService
        let io_runtime = OpenRuntimeGuard::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(planned_threads.io)
                .thread_name("tisql-io")
                .enable_all()
                .build()
                .map_err(|e| {
                    crate::util::error::TiSqlError::Internal(format!(
                        "Failed to create I/O runtime: {e}"
                    ))
                })?,
        );

        log_info!("I/O runtime created with {} threads", planned_threads.io);

        // 2. Staged recovery bootstrap (system ilog + raw clog, no replay yet).
        let mut recovery =
            LsmRecovery::new(&config.data_dir).recover_bootstrap(io_runtime.handle())?;

        // Wrap recovered system tablet in manager first.
        let storage = Arc::new(recovery.engine);
        let tablet_manager = Arc::new(TabletManager::new(
            &config.data_dir,
            Arc::clone(&recovery.lsn_provider),
            Arc::clone(&storage),
        )?);

        // Recover previously-mounted non-system tablets from disk before TSO init,
        // so startup max_commit_ts accounts for their SST metadata too.
        let mut mounted_recovered = 0usize;
        let mut max_commit_ts = recovery.stats.max_commit_ts;
        let recovered_dirs = tablet_manager.discover_existing_tablet_dirs_for_recovery()?;
        for tablet_id in recovered_dirs {
            if tablet_id == TabletId::System {
                continue;
            }
            let recovered = LsmRecovery::recover_tablet(
                &tablet_manager.tablet_dir(tablet_id),
                Arc::clone(&recovery.lsn_provider),
                Arc::clone(&recovery.io_service),
                io_runtime.handle(),
            )?;
            max_commit_ts = max_commit_ts.max(recovered.max_ts_from_ssts);
            recovery.stats.orphan_ssts_cleaned += recovered.orphan_ssts_cleaned;
            tablet_manager.insert_tablet(tablet_id, Arc::new(recovered.engine))?;
            mounted_recovered += 1;
        }

        // Stage-A replay: system-only transactions first, so catalog state is
        // up-to-date before catalog-derived inventory registration.
        let system_replay = LsmRecovery::replay_clog_to_tablets(
            tablet_manager.as_ref(),
            &recovery.clog_entries,
            false,
        )?;
        max_commit_ts = max_commit_ts.max(system_replay.max_commit_ts);

        // Create TSO/concurrency services using conservative recovered max ts.
        let tso = Arc::new(LocalTso::new(max_commit_ts + 1));
        let concurrency_manager = Arc::new(ConcurrencyManager::new(max_commit_ts));

        // Create transaction service with recovered shared clog.
        let routed_storage = Arc::new(RoutedTabletStorage::new(Arc::clone(&tablet_manager)));
        let txn_service = Arc::new(TransactionService::new(
            Arc::clone(&routed_storage),
            Arc::clone(&recovery.clog),
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

        // Build desired tablet inventory from catalog metadata.
        {
            use inner_table::catalog_loader::load_catalog;
            let (cache, _) = load_catalog(txn_service.as_ref())?;
            let desired = tablet_manager.register_catalog_inventory(&cache)?;

            let mut mounted_from_catalog = 0usize;
            for tablet_id in tablet_manager.desired_tablets() {
                if tablet_id == TabletId::System || tablet_manager.get_tablet(tablet_id).is_some() {
                    continue;
                }
                let recovered = LsmRecovery::recover_tablet(
                    &tablet_manager.tablet_dir(tablet_id),
                    Arc::clone(&recovery.lsn_provider),
                    Arc::clone(&recovery.io_service),
                    io_runtime.handle(),
                )?;
                max_commit_ts = max_commit_ts.max(recovered.max_ts_from_ssts);
                recovery.stats.orphan_ssts_cleaned += recovered.orphan_ssts_cleaned;
                tablet_manager.insert_tablet(tablet_id, Arc::new(recovered.engine))?;
                mounted_from_catalog += 1;
            }

            log_info!(
                "Tablet inventory prepared: mounted={}, desired={}, recovered_non_system={}, mounted_from_catalog={}",
                tablet_manager.all_tablets().len(),
                desired.len(),
                mounted_recovered,
                mounted_from_catalog
            );
        }

        // Stage-B replay: transactions touching non-system keyspace.
        let tablet_replay = LsmRecovery::replay_clog_to_tablets(
            tablet_manager.as_ref(),
            &recovery.clog_entries,
            true,
        )?;
        max_commit_ts = max_commit_ts.max(tablet_replay.max_commit_ts);

        let total_txn_replayed = system_replay.txn_count + tablet_replay.txn_count;
        recovery.stats.clog_entries = recovery.clog_entries.len();
        recovery.stats.txn_count = total_txn_replayed;
        recovery.stats.max_commit_ts = max_commit_ts;
        recovery.stats.final_lsn = recovery.lsn_provider.current_lsn();

        // Keep runtime TSO/visibility guards aligned with final recovered max.
        tso.set_ts(max_commit_ts + 1);
        concurrency_manager.update_max_ts(max_commit_ts);

        log_info!(
            "LSM recovery complete: {} clog entries loaded, {} txns replayed, flushed_lsn={}, max_commit_ts={}",
            recovery.stats.clog_entries,
            recovery.stats.txn_count,
            recovery.stats.flushed_lsn,
            recovery.stats.max_commit_ts
        );

        // 3. Create background runtime for per-tablet flush/compaction/GC.
        let bg_runtime = OpenRuntimeGuard::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(planned_threads.background)
                .thread_name("tisql-bg")
                .enable_all()
                .build()
                .map_err(|e| {
                    crate::util::error::TiSqlError::Internal(format!(
                        "Failed to create background runtime: {e}"
                    ))
                })?,
        );

        log_info!(
            "Background runtime created with {} threads",
            planned_threads.background
        );
        tablet_manager.bind_background_runtime(bg_runtime.handle())?;

        // Create session registry and wire GC safe point.
        let session_registry = Arc::new(session::SessionRegistry::new());
        let gc_safe_point_updater: Arc<dyn Fn() -> u64 + Send + Sync> = {
            let registry = Arc::clone(&session_registry);
            let txn_svc = Arc::clone(&txn_service);
            Arc::new(move || -> u64 {
                match registry.min_start_ts() {
                    Some(min_ts) if min_ts > 0 => min_ts - 1,
                    // Read current TSO without advancing it; GC safe-point polling
                    // runs frequently (compaction + background worker loops).
                    _ => txn_svc.last_ts(),
                }
            })
        };

        // Set initial GC safe point from TSO and keep compaction GC pinned to
        // active explicit readers.
        storage.set_gc_safe_point(tso.get_ts());
        storage.set_gc_safe_point_updater(Arc::clone(&gc_safe_point_updater));

        // Load pending GC tasks from inner tables (for observability).
        {
            use inner_table::catalog_loader::load_gc_tasks;
            match load_gc_tasks(txn_service.as_ref()) {
                Ok(tasks) => {
                    let pending_count = tasks.iter().filter(|task| task.status != "done").count();
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
        let gc_worker = inner_table::gc_worker::GcWorker::new(
            Arc::clone(&tablet_manager),
            Arc::clone(&txn_service),
            Arc::clone(&gc_safe_point_updater),
        );
        gc_worker.start(bg_runtime.handle());

        let worker_runtime = OpenRuntimeGuard::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(planned_threads.worker)
                .thread_name("tisql-worker")
                .enable_all()
                .build()
                .map_err(|e| {
                    crate::util::error::TiSqlError::Internal(format!(
                        "Failed to create worker runtime: {e}"
                    ))
                })?,
        );

        log_info!(
            "Worker runtime created with {} threads",
            planned_threads.worker
        );

        Ok(Self {
            parser: Parser::new(),
            executor: SimpleExecutor::new(),
            txn_service,
            catalog,
            ilog: recovery.ilog,
            storage,
            tablet_manager,
            gc_worker,
            worker_runtime: Some(worker_runtime.into_inner()),
            bg_runtime: Some(bg_runtime.into_inner()),
            io_runtime: Some(io_runtime.into_inner()),
            session_registry,
            enable_encoded_result_batch: config.enable_encoded_result_batch,
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

    /// Whether adaptive encoded result batches are enabled.
    pub fn encoded_result_batch_enabled(&self) -> bool {
        self.enable_encoded_result_batch
    }

    /// Get the tablet manager.
    pub fn tablet_manager(&self) -> &Arc<TabletManager> {
        &self.tablet_manager
    }

    /// Get the session registry for tracking active transactions.
    pub fn session_registry(&self) -> &Arc<session::SessionRegistry> {
        &self.session_registry
    }

    /// Trigger one immediate GC-worker wake-up.
    pub fn notify_gc_worker(&self) {
        self.gc_worker.notify();
    }

    fn mount_tablet_if_needed(&self, tablet_id: TabletId) -> Result<()> {
        if tablet_id == TabletId::System || self.tablet_manager.get_tablet(tablet_id).is_some() {
            return Ok(());
        }

        let io_rt = self.io_runtime.as_ref().ok_or_else(|| {
            util::error::TiSqlError::Internal(
                "I/O runtime not available while mounting tablet".to_string(),
            )
        })?;
        let recovered = LsmRecovery::recover_tablet(
            &self.tablet_manager.tablet_dir(tablet_id),
            self.tablet_manager.shared_lsn_provider(),
            Arc::clone(self.storage.io_service()),
            io_rt.handle(),
        )?;
        self.tablet_manager
            .insert_tablet(tablet_id, Arc::new(recovered.engine))?;
        Ok(())
    }

    fn mount_table_tablet_if_needed(&self, table_id: u64) -> Result<()> {
        self.mount_tablet_if_needed(route_table_to_tablet(table_id))
    }

    // ========================================================================
    // Test / Convenience Query Execution
    // ========================================================================

    /// Execute SQL and return a materialized QueryResult.
    ///
    /// Uses the production path: parse_and_bind → execute_plan → into_result.
    /// This is a convenience wrapper for tests and the integration test runner.
    pub async fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        let exec_ctx = session::ExecutionCtx::with_db("default");
        let plan = self.parse_and_bind(sql, &exec_ctx)?;
        let (output, _) = self.execute_plan(plan, &exec_ctx, None).await;
        let output = output?;
        Self::to_query_result(output.into_result().await?)
    }

    /// Execute SQL with session context (explicit transactions) and return QueryResult.
    ///
    /// Mirrors InnerSession::execute() pattern: takes + returns txn_ctx through session.
    pub async fn execute_query_with_session(
        &self,
        sql: &str,
        session: &mut session::Session,
    ) -> Result<QueryResult> {
        let exec_ctx = session::ExecutionCtx::from_session(session);
        let plan = self.parse_and_bind(sql, &exec_ctx)?;
        let txn_ctx = session.take_current_txn();
        let (output, returned_ctx) = self.execute_plan(plan, &exec_ctx, txn_ctx).await;
        if let Some(ctx) = returned_ctx {
            session.set_current_txn(ctx);
        }
        let output = output?;
        Self::to_query_result(output.into_result().await?)
    }

    /// Convert ExecutionResult to QueryResult (stringified for wire/test output).
    fn to_query_result(result: ExecutionResult) -> Result<QueryResult> {
        Ok(match result {
            ExecutionResult::Rows { schema, rows } => QueryResult::Rows {
                columns: schema
                    .columns()
                    .iter()
                    .map(|c| c.name().to_string())
                    .collect(),
                data: rows
                    .iter()
                    .map(|row| row.iter().map(value_to_string).collect())
                    .collect(),
            },
            ExecutionResult::Affected { count } => QueryResult::Affected(count),
            ExecutionResult::Ok | ExecutionResult::OkWithEffect(_) => QueryResult::Ok,
        })
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
    /// 1) checkpoint every mounted tablet manifest
    /// 2) truncate shared clog up to global safe_lsn
    /// 3) truncate each tablet ilog before its own checkpoint_lsn
    ///
    /// Shared-clog safety uses global minimum caps across all mounted tablets.
    pub async fn run_log_gc_once(&self) -> Result<LogGcStats> {
        #[cfg(feature = "failpoints")]
        fail_point!("log_gc_before_checkpoint");

        let captures = self
            .tablet_manager
            .checkpoint_and_capture_all_tablets()
            .await?;
        let tablet_checkpoints: Vec<_> = captures
            .iter()
            .map(|capture| TabletGcCheckpoint {
                tablet_id: capture.tablet_id,
                flushed_lsn: capture.version.flushed_lsn(),
                checkpoint_lsn: capture.checkpoint_lsn,
            })
            .collect();

        #[cfg(feature = "failpoints")]
        fail_point!("log_gc_after_checkpoint_before_safe_compute_v26");
        let boundary = self
            .tablet_manager
            .compute_global_log_gc_boundary_with_caps(&captures)?;
        let safe_lsn = boundary.safe_lsn;

        #[cfg(feature = "failpoints")]
        fail_point!("log_gc_after_safe_compute_before_clog_truncate_v26");

        #[cfg(feature = "failpoints")]
        fail_point!("log_gc_after_checkpoint_before_clog_truncate");

        let clog = self
            .txn_service
            .clog_service()
            .truncate_to(safe_lsn)
            .await?;

        #[cfg(feature = "failpoints")]
        fail_point!("log_gc_after_clog_truncate_before_ilog_truncate");

        let tablet_ilogs_raw = self
            .tablet_manager
            .truncate_tablet_ilogs_before(&captures)
            .await?;
        let tablet_ilogs: Vec<_> = tablet_ilogs_raw
            .iter()
            .map(|entry| TabletIlogGcStats {
                tablet_id: entry.tablet_id,
                checkpoint_lsn: entry.checkpoint_lsn,
                truncate: entry.stats.clone(),
            })
            .collect();

        #[cfg(feature = "failpoints")]
        fail_point!("log_gc_after_ilog_truncate");

        let system_checkpoint = captures
            .iter()
            .find(|capture| capture.tablet_id == TabletId::System)
            .map(|capture| capture.checkpoint_lsn)
            .unwrap_or(0);
        let system_ilog = tablet_ilogs_raw
            .iter()
            .find(|entry| entry.tablet_id == TabletId::System)
            .map(|entry| entry.stats.clone())
            .unwrap_or_default();

        debug_assert!(
            safe_lsn <= boundary.checkpoint_flushed_cap,
            "safe_lsn ({safe_lsn}) exceeds checkpoint cap ({})",
            boundary.checkpoint_flushed_cap
        );

        Ok(LogGcStats {
            flushed_lsn: boundary.checkpoint_flushed_cap,
            safe_lsn,
            checkpoint_lsn: system_checkpoint,
            clog,
            ilog: system_ilog,
            boundary,
            tablet_checkpoints,
            tablet_ilogs,
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
        let stmt = self.parser.parse_one(sql)?;
        let binder = sql::Binder::new(&self.catalog, &exec_ctx.current_db);
        binder.bind(stmt)
    }

    /// Execute a pre-bound logical plan.
    ///
    /// Returns statement execution result and the optionally-updated `TxnCtx`.
    ///
    /// On explicit transaction statement failure, this still returns the
    /// original `TxnCtx` so callers can keep the transaction open.
    pub(crate) async fn execute_plan(
        &self,
        plan: sql::LogicalPlan,
        exec_ctx: &session::ExecutionCtx,
        txn_ctx: Option<transaction::TxnCtx>,
    ) -> (Result<ExecutionOutput>, Option<transaction::TxnCtx>) {
        let (mut output, ctx) = self
            .executor
            .execute_unified(
                plan,
                exec_ctx,
                self.txn_service.as_ref(),
                &self.catalog,
                txn_ctx,
            )
            .await;

        if let Ok(ExecutionOutput::OkWithEffect(effect)) = &output {
            let post_effect = match effect {
                executor::DdlEffect::TableCreated { table_id } => {
                    self.mount_table_tablet_if_needed(*table_id)
                }
                executor::DdlEffect::TableDropped => {
                    self.gc_worker.notify();
                    Ok(())
                }
            };
            if let Err(e) = post_effect {
                output = Err(e);
            }
        }

        (output, ctx)
    }

    /// Close the database (flush memtables and sync logs).
    pub async fn close(&self) -> Result<()> {
        // Stop background workers first so no new manifest edits are submitted.
        self.gc_worker.stop();
        self.tablet_manager.stop_background_workers();

        // Flush any pending memtable data to SSTs
        if let Err(e) = self.tablet_manager.flush_all_with_active() {
            log_warn!("Error flushing memtables on close: {}", e);
        }

        // Drain/stop manifest writer before closing ilog.
        self.tablet_manager.shutdown_manifest_writers();

        // Close commit log
        self.txn_service.clog_service().close().await?;

        // Close mounted user-tablet ilogs first, then the shared system ilog.
        self.tablet_manager.close_non_system_tablet_ilogs()?;
        self.ilog.close()?;

        log_info!("Database closed");
        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // 1. Stop background schedulers and GC worker
        self.gc_worker.stop();
        self.tablet_manager.stop_background_workers();

        // 2. Final flush (needs I/O runtime — ilog writes go through group commit)
        let _ = self.tablet_manager.flush_all_with_active();

        // 3. Drain/stop manifest writer before ilog close.
        self.tablet_manager.shutdown_manifest_writers();

        // 4. Close clog + ilog (sync pending writes)
        let _ = crate::io::block_on_sync(self.txn_service.clog_service().close());
        let _ = self.tablet_manager.close_non_system_tablet_ilogs();
        let _ = self.ilog.close();

        // 5. Shutdown all spawn_blocking task channels BEFORE dropping io_runtime.
        //
        // GroupCommitWriter (clog + ilog) and IoService each run a spawn_blocking
        // task on io_runtime that blocks on rx.recv(). Closing the sender channels
        // causes recv() to return Err, exiting the loops. Without this,
        // io_runtime.drop() blocks forever waiting for those tasks.
        self.txn_service.clog_service().shutdown();
        self.tablet_manager.shutdown_non_system_tablet_ilogs();
        self.ilog.shutdown();
        self.storage.io_service().shutdown();

        // 6. Shut down runtimes.
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
    use crate::inner_table::core_tables::{
        ALL_COLUMN_TABLE_ID, ALL_GC_DELETE_RANGE_TABLE_ID, ALL_INDEX_TABLE_ID, ALL_META_TABLE_ID,
        ALL_SCHEMA_TABLE_ID, ALL_TABLE_TABLE_ID, USER_TABLE_ID_START,
    };
    use crate::tablet::{
        encode_key, encode_pk, is_tombstone, IlogConfig, IlogService, LsmConfig, MvccIterator,
        MvccKey, TabletEngine, TabletId, Version,
    };
    use tempfile::tempdir;
    use tokio::time::{sleep, Duration};

    fn create_test_db() -> (Database, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Database::open(config).unwrap();
        (db, dir)
    }

    fn mount_user_tablet(db: &Database, table_id: u64) {
        let tablet_id = TabletId::Table { table_id };
        if db.tablet_manager().get_tablet(tablet_id).is_some() {
            return;
        }
        let dir = db.tablet_manager().tablet_dir(tablet_id);
        let tablet = Arc::new(TabletEngine::open(LsmConfig::new(&dir)).unwrap());
        db.tablet_manager()
            .insert_tablet(tablet_id, tablet)
            .unwrap();
    }

    fn mount_user_tablet_durable(db: &Database, table_id: u64) {
        let tablet_id = TabletId::Table { table_id };
        if db.tablet_manager().get_tablet(tablet_id).is_some() {
            return;
        }

        let dir = db.tablet_manager().tablet_dir(tablet_id);
        std::fs::create_dir_all(&dir).unwrap();

        let lsn_provider = db.tablet_manager().shared_lsn_provider();
        let io = Arc::clone(db.storage.io_service());
        let io_handle = db
            .io_runtime
            .as_ref()
            .expect("database must have io runtime")
            .handle();
        let ilog = Arc::new(
            IlogService::open(
                IlogConfig::new(&dir),
                Arc::clone(&lsn_provider),
                Arc::clone(&io),
                io_handle,
            )
            .unwrap(),
        );
        let tablet = Arc::new(
            TabletEngine::open_with_recovery(
                LsmConfig::new(&dir),
                lsn_provider,
                ilog,
                Version::new(),
                io,
            )
            .unwrap(),
        );
        db.tablet_manager()
            .insert_tablet(tablet_id, tablet)
            .unwrap();
    }

    fn table_int_pk_key(table_id: u64, id: i32) -> Vec<u8> {
        let pk = encode_pk(&[Value::Int(id)]);
        encode_key(table_id, &pk)
    }

    fn table_id_by_name(db: &Database, table_name: &str) -> u64 {
        db.catalog
            .get_table("default", table_name)
            .unwrap()
            .unwrap_or_else(|| panic!("table {table_name} not found"))
            .id()
    }

    fn tablet_has_table_entries(tablet: &TabletEngine, table_id: u64) -> bool {
        let start = encode_key(table_id, &[]);
        let end = encode_key(table_id + 1, &[]);
        let range = MvccKey::encode(&start, u64::MAX)..MvccKey::encode(&end, 0);
        let mut iter = tablet.scan_iter(range, 0).unwrap();
        crate::io::block_on_sync(async {
            iter.advance().await.unwrap();
            iter.valid()
        })
    }

    fn tablet_get_value(tablet: &TabletEngine, key: &[u8]) -> Option<Vec<u8>> {
        let start = MvccKey::encode(key, u64::MAX);
        let end = MvccKey::encode(key, 0)
            .next_key()
            .unwrap_or_else(MvccKey::unbounded);
        let range = start..end;
        let mut iter = tablet.scan_iter(range, 0).unwrap();
        crate::io::block_on_sync(async {
            iter.advance().await.unwrap();
            while iter.valid() {
                if iter.user_key() == key {
                    if is_tombstone(iter.value()) {
                        return None;
                    }
                    return Some(iter.value().to_vec());
                }
                iter.advance().await.unwrap();
            }
            None
        })
    }

    async fn prepare_phase4_two_tablet_recovery_case(
        dir: &std::path::Path,
        flush_table1: bool,
        flush_table2: bool,
    ) -> (u64, u64, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, u64) {
        use crate::clog::{
            ClogBatch, ClogEntry, ClogOp, ClogService, FileClogConfig, FileClogService,
        };
        use crate::lsn::new_lsn_provider;

        let io = crate::io::IoService::new_for_test(64).unwrap();
        let lsn_provider = new_lsn_provider();

        let system_dir = dir.join("tablets").join("system");
        std::fs::create_dir_all(&system_dir).unwrap();
        let system_ilog = Arc::new(
            IlogService::open(
                IlogConfig::new(&system_dir),
                Arc::clone(&lsn_provider),
                Arc::clone(&io),
                &tokio::runtime::Handle::current(),
            )
            .unwrap(),
        );
        let system_tablet = Arc::new(
            TabletEngine::open_with_recovery(
                LsmConfig::new(&system_dir),
                Arc::clone(&lsn_provider),
                Arc::clone(&system_ilog),
                Version::new(),
                Arc::clone(&io),
            )
            .unwrap(),
        );

        let manager =
            TabletManager::new(dir, Arc::clone(&lsn_provider), Arc::clone(&system_tablet)).unwrap();

        let table1_id = USER_TABLE_ID_START + 410;
        let table2_id = USER_TABLE_ID_START + 411;
        let tablet1_id = TabletId::Table {
            table_id: table1_id,
        };
        let tablet2_id = TabletId::Table {
            table_id: table2_id,
        };

        for tablet_id in [tablet1_id, tablet2_id] {
            let tablet_dir = manager.tablet_dir(tablet_id);
            let ilog = Arc::new(
                IlogService::open(
                    IlogConfig::new(&tablet_dir),
                    Arc::clone(&lsn_provider),
                    Arc::clone(&io),
                    &tokio::runtime::Handle::current(),
                )
                .unwrap(),
            );
            let tablet = Arc::new(
                TabletEngine::open_with_recovery(
                    LsmConfig::new(&tablet_dir),
                    Arc::clone(&lsn_provider),
                    ilog,
                    Version::new(),
                    Arc::clone(&io),
                )
                .unwrap(),
            );
            manager.insert_tablet(tablet_id, tablet).unwrap();
        }

        let key1 = table_int_pk_key(table1_id, 1);
        let key2 = table_int_pk_key(table2_id, 1);
        let val1 = b"phase4_v1".to_vec();
        let val2 = b"phase4_v2".to_vec();

        let clog = FileClogService::open_with_lsn_provider(
            FileClogConfig::with_dir(dir),
            Arc::clone(&lsn_provider),
            Arc::clone(&io),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        let mut clog_batch = ClogBatch::new();
        clog_batch.add(ClogEntry {
            lsn: 0,
            txn_id: 9001,
            op: ClogOp::Put {
                key: key1.clone(),
                value: val1.clone(),
            },
        });
        clog_batch.add(ClogEntry {
            lsn: 0,
            txn_id: 9001,
            op: ClogOp::Put {
                key: key2.clone(),
                value: val2.clone(),
            },
        });
        clog_batch.add(ClogEntry {
            lsn: 0,
            txn_id: 9001,
            op: ClogOp::Commit { commit_ts: 9001 },
        });
        let commit_lsn = clog.write(&mut clog_batch, true).unwrap().await.unwrap();

        let tablet1 = manager.get_tablet(tablet1_id).unwrap();
        let tablet2 = manager.get_tablet(tablet2_id).unwrap();

        let mut wb1 = crate::tablet::WriteBatch::new();
        wb1.set_commit_ts(9001);
        wb1.set_clog_lsn(commit_lsn);
        wb1.put(key1.clone(), val1.clone());
        tablet1.write_batch(wb1).unwrap();

        let mut wb2 = crate::tablet::WriteBatch::new();
        wb2.set_commit_ts(9001);
        wb2.set_clog_lsn(commit_lsn);
        wb2.put(key2.clone(), val2.clone());
        tablet2.write_batch(wb2).unwrap();

        if flush_table1 {
            tablet1.flush_all_with_active().unwrap();
        }
        if flush_table2 {
            tablet2.flush_all_with_active().unwrap();
        }

        clog.close().await.unwrap();
        clog.shutdown();
        io.shutdown();

        (table1_id, table2_id, key1, val1, key2, val2, commit_lsn)
    }

    #[tokio::test]
    async fn test_select_literal() {
        let (db, _dir) = create_test_db();
        let result = db.execute_query("SELECT 1 + 1").await.unwrap();
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

        db.execute_query("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))")
            .await
            .unwrap();

        let result = db
            .execute_query("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            .await
            .unwrap();
        match result {
            QueryResult::Affected(count) => assert_eq!(count, 1),
            _ => panic!("Expected affected count"),
        }

        let result = db
            .execute_query("SELECT id, name FROM users")
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
            db.execute_query("CREATE TABLE t (id INT PRIMARY KEY, val VARCHAR(100))")
                .await
                .unwrap();
            db.execute_query("INSERT INTO t VALUES (1, 'hello')")
                .await
                .unwrap();
            db.execute_query("INSERT INTO t VALUES (2, 'world')")
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
    async fn test_phase2_fresh_db_creates_system_tablet_dir() {
        let dir = tempdir().unwrap();
        let db = Database::open(DatabaseConfig::with_data_dir(dir.path())).unwrap();
        assert!(dir.path().join("tablets").join("system").exists());
        assert_eq!(db.tablet_manager.all_tablets().len(), 1);
        assert!(db.tablet_manager.get_tablet(TabletId::System).is_some());
    }

    #[tokio::test]
    async fn test_phase2_bootstrap_inner_tables_do_not_create_dedicated_tablets() {
        let dir = tempdir().unwrap();
        let _db = Database::open(DatabaseConfig::with_data_dir(dir.path())).unwrap();

        let tablets_dir = dir.path().join("tablets");
        assert!(tablets_dir.join("system").exists());
        for inner_table_id in [
            ALL_META_TABLE_ID,
            ALL_SCHEMA_TABLE_ID,
            ALL_TABLE_TABLE_ID,
            ALL_COLUMN_TABLE_ID,
            ALL_INDEX_TABLE_ID,
            ALL_GC_DELETE_RANGE_TABLE_ID,
        ] {
            assert!(
                !tablets_dir.join(format!("t_{inner_table_id}")).exists(),
                "inner table id {inner_table_id} must stay on system tablet"
            );
        }
    }

    #[tokio::test]
    async fn test_phase2_reopen_registers_user_tablet_inventory() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());

        {
            let db = Database::open(config.clone()).unwrap();
            db.execute_query("CREATE TABLE t_phase2 (id INT PRIMARY KEY, v INT)")
                .await
                .unwrap();
            db.close().await.unwrap();
        }

        let db = Database::open(config).unwrap();
        let user_tablet = TabletId::Table {
            table_id: USER_TABLE_ID_START,
        };
        assert!(dir
            .path()
            .join("tablets")
            .join(user_tablet.dir_name())
            .exists());
        assert!(
            db.tablet_manager.desired_tablets().contains(&user_tablet),
            "catalog-derived user table should be present in desired inventory on reopen"
        );
        assert!(
            db.tablet_manager.get_tablet(user_tablet).is_some(),
            "reopen should mount catalog-derived user tablet at runtime"
        );
    }

    #[tokio::test]
    async fn test_phase2_reopen_discovers_existing_user_tablet_dirs() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());

        {
            let db = Database::open(config.clone()).unwrap();
            db.close().await.unwrap();
        }

        let table_tablet = TabletId::Table {
            table_id: USER_TABLE_ID_START + 77,
        };
        let index_tablet = TabletId::LocalIndex {
            table_id: USER_TABLE_ID_START + 77,
            index_id: 9001,
        };
        let table_dir = dir.path().join("tablets").join(table_tablet.dir_name());
        let index_dir = dir.path().join("tablets").join(index_tablet.dir_name());
        let table_ilog =
            IlogService::open_with_thread(IlogConfig::new(&table_dir), new_lsn_provider()).unwrap();
        table_ilog.close().unwrap();
        let index_ilog =
            IlogService::open_with_thread(IlogConfig::new(&index_dir), new_lsn_provider()).unwrap();
        index_ilog.close().unwrap();

        let db = Database::open(config).unwrap();
        let desired = db.tablet_manager.desired_tablets();
        assert!(desired.contains(&table_tablet));
        assert!(desired.contains(&index_tablet));
        assert!(db.tablet_manager.get_tablet(table_tablet).is_some());
        assert!(db.tablet_manager.get_tablet(index_tablet).is_some());
    }

    #[tokio::test]
    async fn test_phase2_reopen_without_tablets_dir_stays_compatible() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());

        {
            let db = Database::open(config.clone()).unwrap();
            db.execute_query("CREATE TABLE t_phase2_legacy (id INT PRIMARY KEY, v INT)")
                .await
                .unwrap();
            db.execute_query("INSERT INTO t_phase2_legacy VALUES (1, 10)")
                .await
                .unwrap();
            db.close().await.unwrap();
        }

        let tablets_dir = dir.path().join("tablets");
        assert!(tablets_dir.exists());
        std::fs::remove_dir_all(&tablets_dir).unwrap();
        assert!(!tablets_dir.exists());

        let db = Database::open(config).unwrap();
        assert!(tablets_dir.join("system").exists());
        match db
            .execute_query("SELECT v FROM t_phase2_legacy WHERE id = 1")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1);
                assert_eq!(data[0][0], "10");
            }
            other => panic!("expected row result after legacy reopen, got {other:?}"),
        }
    }

    // ==================== Phase-2 QA Tests ====================

    /// T2.3a: Database::open produces a working TabletManager with system tablet.
    /// This covers the recovery→TabletManager path end-to-end: fresh DB creates
    /// manager with system tablet, and the manager is accessible via getter.
    #[tokio::test]
    async fn test_phase2_qa_recovery_produces_tablet_manager() {
        let dir = tempdir().unwrap();
        let db = Database::open(DatabaseConfig::with_data_dir(dir.path())).unwrap();

        let manager = db.tablet_manager();
        // System tablet always mounted
        assert!(
            manager.get_tablet(TabletId::System).is_some(),
            "system tablet must be mounted after recovery"
        );
        // Only system tablet is mounted in Phase 2
        assert_eq!(
            manager.all_tablets().len(),
            1,
            "phase-2 should have exactly one mounted tablet (system)"
        );
        // min_flushed_lsn should return Some (system tablet exists)
        assert!(
            manager.min_flushed_lsn().is_some(),
            "min_flushed_lsn should be Some with system tablet mounted"
        );
    }

    /// T2.4: CREATE TABLE produces a tablet dir in desired inventory on reopen.
    ///
    /// Note: CREATE INDEX via SQL is not yet supported; index tablets can only
    /// be tested at the unit level (derive_tablet_inventory with IndexDef).
    #[tokio::test]
    async fn test_phase2_qa_ddl_creates_tablet_dirs_on_reopen() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());

        {
            let db = Database::open(config.clone()).unwrap();
            db.execute_query("CREATE TABLE t_qa (id INT PRIMARY KEY, c1 INT, c2 VARCHAR(100))")
                .await
                .unwrap();
            db.close().await.unwrap();
        }

        // Reopen — catalog recovery should register the table tablet.
        let db = Database::open(config).unwrap();
        let desired = db.tablet_manager().desired_tablets();

        assert!(
            desired.contains(&TabletId::System),
            "system tablet must always be desired"
        );

        // Find user table tablet (ID is auto-allocated starting at USER_TABLE_ID_START)
        let has_user_table = desired
            .iter()
            .any(|t| matches!(t, TabletId::Table { table_id } if *table_id >= USER_TABLE_ID_START));
        assert!(
            has_user_table,
            "desired inventory should include user table tablet after DDL"
        );

        // Verify actual directory exists on disk
        let tablets_root = dir.path().join("tablets");
        let user_table_dirs: Vec<_> = std::fs::read_dir(&tablets_root)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_str().is_some_and(|n| n.starts_with("t_")))
            .collect();
        assert!(
            !user_table_dirs.is_empty(),
            "at least one user table dir should exist under tablets/"
        );
    }

    /// T2.4: Multiple CREATE TABLE statements create multiple distinct tablet dirs.
    #[tokio::test]
    async fn test_phase2_qa_multiple_tables_create_multiple_dirs() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());

        {
            let db = Database::open(config.clone()).unwrap();
            db.execute_query("CREATE TABLE t_multi_1 (id INT PRIMARY KEY)")
                .await
                .unwrap();
            db.execute_query("CREATE TABLE t_multi_2 (id INT PRIMARY KEY)")
                .await
                .unwrap();
            db.execute_query("CREATE TABLE t_multi_3 (id INT PRIMARY KEY)")
                .await
                .unwrap();
            db.close().await.unwrap();
        }

        let db = Database::open(config).unwrap();
        let desired = db.tablet_manager().desired_tablets();

        let user_table_count = desired
            .iter()
            .filter(
                |t| matches!(t, TabletId::Table { table_id } if *table_id >= USER_TABLE_ID_START),
            )
            .count();
        assert_eq!(
            user_table_count, 3,
            "3 user tables should produce 3 table tablet entries in desired inventory"
        );

        // Each should have its own dir on disk
        let tablets_root = dir.path().join("tablets");
        let table_dirs: Vec<_> = std::fs::read_dir(&tablets_root)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_str().is_some_and(|n| n.starts_with("t_")))
            .collect();
        assert_eq!(
            table_dirs.len(),
            3,
            "3 distinct table dirs should exist under tablets/"
        );
    }

    /// T2.3c: Close/reopen cycle preserves data and tablet manager state.
    #[tokio::test]
    async fn test_phase2_qa_reopen_preserves_data_and_manager() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());

        {
            let db = Database::open(config.clone()).unwrap();
            db.execute_query("CREATE TABLE t_reopen (id INT PRIMARY KEY, val INT)")
                .await
                .unwrap();
            db.execute_query("INSERT INTO t_reopen VALUES (1, 100)")
                .await
                .unwrap();
            db.execute_query("INSERT INTO t_reopen VALUES (2, 200)")
                .await
                .unwrap();
            db.close().await.unwrap();
        }

        let db = Database::open(config).unwrap();

        // Data is preserved
        match db
            .execute_query("SELECT val FROM t_reopen WHERE id = 2")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1);
                assert_eq!(data[0][0], "200");
            }
            other => panic!("expected row result, got {other:?}"),
        }

        // TabletManager has system tablet mounted
        assert!(db.tablet_manager().get_tablet(TabletId::System).is_some());
        // Desired inventory includes the user table
        let desired = db.tablet_manager().desired_tablets();
        let has_user = desired
            .iter()
            .any(|t| matches!(t, TabletId::Table { table_id } if *table_id >= USER_TABLE_ID_START));
        assert!(
            has_user,
            "desired inventory should include user table after reopen"
        );
    }

    /// T2.4c: Flush through storage path (which TabletManager delegates to system
    /// tablet) advances flushed_lsn visible through manager.
    #[tokio::test]
    async fn test_phase2_qa_flush_advances_manager_flushed_lsn() {
        let dir = tempdir().unwrap();
        let db = Database::open(DatabaseConfig::with_data_dir(dir.path())).unwrap();

        let flushed_before = db.tablet_manager().min_flushed_lsn().unwrap();

        // Write some data to create clog+memtable entries
        db.execute_query("CREATE TABLE t_flush_qa (id INT PRIMARY KEY, val INT)")
            .await
            .unwrap();
        db.execute_query("INSERT INTO t_flush_qa VALUES (1, 10)")
            .await
            .unwrap();

        // Force flush
        db.storage.flush_all_with_active().unwrap();

        let flushed_after = db.tablet_manager().min_flushed_lsn().unwrap();
        assert!(
            flushed_after >= flushed_before,
            "flushed_lsn should not regress after flush (before={flushed_before}, after={flushed_after})"
        );
    }

    #[tokio::test]
    async fn test_phase3_cross_tablet_commit_routes_to_mounted_tablets() {
        let dir = tempdir().unwrap();
        let db = Database::open(DatabaseConfig::with_data_dir(dir.path())).unwrap();

        db.execute_query("CREATE TABLE p3_t1 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        db.execute_query("CREATE TABLE p3_t2 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();

        let table1 = table_id_by_name(&db, "p3_t1");
        let table2 = table_id_by_name(&db, "p3_t2");
        mount_user_tablet(&db, table1);
        mount_user_tablet(&db, table2);

        let mut session = Session::new();
        db.execute_query_with_session("BEGIN", &mut session)
            .await
            .unwrap();
        db.execute_query_with_session("INSERT INTO p3_t1 VALUES (1, 10)", &mut session)
            .await
            .unwrap();
        db.execute_query_with_session("INSERT INTO p3_t2 VALUES (1, 20)", &mut session)
            .await
            .unwrap();
        db.execute_query_with_session("COMMIT", &mut session)
            .await
            .unwrap();

        match db
            .execute_query("SELECT v FROM p3_t1 WHERE id = 1")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1);
                assert_eq!(data[0][0], "10");
            }
            other => panic!("expected rows for p3_t1, got {other:?}"),
        }
        match db
            .execute_query("SELECT v FROM p3_t2 WHERE id = 1")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1);
                assert_eq!(data[0][0], "20");
            }
            other => panic!("expected rows for p3_t2, got {other:?}"),
        }

        let system = db.tablet_manager().system_tablet();
        let tablet1 = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table1 })
            .unwrap();
        let tablet2 = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table2 })
            .unwrap();

        assert!(!tablet_has_table_entries(&system, table1));
        assert!(!tablet_has_table_entries(&system, table2));
        assert!(tablet_has_table_entries(&tablet1, table1));
        assert!(tablet_has_table_entries(&tablet2, table2));
    }

    #[tokio::test]
    async fn test_phase3_cross_tablet_rollback_cleans_all_tablets() {
        let dir = tempdir().unwrap();
        let db = Database::open(DatabaseConfig::with_data_dir(dir.path())).unwrap();

        db.execute_query("CREATE TABLE p3_rb_t1 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        db.execute_query("CREATE TABLE p3_rb_t2 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();

        let table1 = table_id_by_name(&db, "p3_rb_t1");
        let table2 = table_id_by_name(&db, "p3_rb_t2");
        mount_user_tablet(&db, table1);
        mount_user_tablet(&db, table2);

        let mut session = Session::new();
        db.execute_query_with_session("BEGIN", &mut session)
            .await
            .unwrap();
        db.execute_query_with_session("INSERT INTO p3_rb_t1 VALUES (1, 10)", &mut session)
            .await
            .unwrap();
        db.execute_query_with_session("INSERT INTO p3_rb_t2 VALUES (1, 20)", &mut session)
            .await
            .unwrap();
        db.execute_query_with_session("ROLLBACK", &mut session)
            .await
            .unwrap();

        let tablet1 = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table1 })
            .unwrap();
        let tablet2 = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table2 })
            .unwrap();
        assert!(!tablet_has_table_entries(&tablet1, table1));
        assert!(!tablet_has_table_entries(&tablet2, table2));

        match db
            .execute_query("SELECT v FROM p3_rb_t1 WHERE id = 1")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert!(data.is_empty(), "p3_rb_t1 should be empty after rollback");
            }
            other => panic!("expected rows for p3_rb_t1 lookup, got {other:?}"),
        }
        match db
            .execute_query("SELECT v FROM p3_rb_t2 WHERE id = 1")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert!(data.is_empty(), "p3_rb_t2 should be empty after rollback");
            }
            other => panic!("expected rows for p3_rb_t2 lookup, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_phase3_cross_tablet_snapshot_isolation() {
        let dir = tempdir().unwrap();
        let db = Database::open(DatabaseConfig::with_data_dir(dir.path())).unwrap();

        db.execute_query("CREATE TABLE p3_si_t1 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        db.execute_query("CREATE TABLE p3_si_t2 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();

        let table1 = table_id_by_name(&db, "p3_si_t1");
        let table2 = table_id_by_name(&db, "p3_si_t2");
        mount_user_tablet(&db, table1);
        mount_user_tablet(&db, table2);

        let key1 = table_int_pk_key(table1, 1);
        let key2 = table_int_pk_key(table2, 1);

        let mut writer = db.txn_service.begin_explicit(false).unwrap();
        db.txn_service
            .put(&mut writer, table1, key1.clone(), b"v1".to_vec())
            .await
            .unwrap();
        db.txn_service
            .put(&mut writer, table2, key2.clone(), b"v2".to_vec())
            .await
            .unwrap();

        let reader_before = db.txn_service.begin(true).unwrap();
        db.txn_service.commit(writer).await.unwrap();

        assert_eq!(
            db.txn_service
                .get(&reader_before, table1, &key1)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            db.txn_service
                .get(&reader_before, table2, &key2)
                .await
                .unwrap(),
            None
        );

        let reader_after = db.txn_service.begin(true).unwrap();
        assert_eq!(
            db.txn_service
                .get(&reader_after, table1, &key1)
                .await
                .unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            db.txn_service
                .get(&reader_after, table2, &key2)
                .await
                .unwrap(),
            Some(b"v2".to_vec())
        );
    }

    #[tokio::test]
    async fn test_phase3_lock_conflict_only_on_same_tablet() {
        let dir = tempdir().unwrap();
        let db = Database::open(DatabaseConfig::with_data_dir(dir.path())).unwrap();

        db.execute_query("CREATE TABLE p3_lock_t1 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        db.execute_query("CREATE TABLE p3_lock_t2 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();

        let table1 = table_id_by_name(&db, "p3_lock_t1");
        let table2 = table_id_by_name(&db, "p3_lock_t2");
        mount_user_tablet(&db, table1);
        mount_user_tablet(&db, table2);

        let key1 = table_int_pk_key(table1, 1);
        let key2 = table_int_pk_key(table2, 1);

        let mut txn1 = db.txn_service.begin_explicit(false).unwrap();
        db.txn_service
            .put(&mut txn1, table1, key1.clone(), b"owner1".to_vec())
            .await
            .unwrap();

        let mut txn2 = db.txn_service.begin_explicit(false).unwrap();
        db.txn_service
            .put(&mut txn2, table2, key2.clone(), b"owner2".to_vec())
            .await
            .unwrap();

        let mut txn3 = db.txn_service.begin_explicit(false).unwrap();
        let conflict = db
            .txn_service
            .put(&mut txn3, table1, key1.clone(), b"conflict".to_vec())
            .await;
        assert!(matches!(
            conflict,
            Err(crate::util::error::TiSqlError::KeyIsLocked { .. })
        ));

        db.txn_service.commit(txn2).await.unwrap();
        db.txn_service.rollback(txn1).unwrap();
        db.txn_service.rollback(txn3).unwrap();
    }

    // ==================== Phase-3 QA Tests ====================

    /// T3.5a (QA): Cross-tablet commit via TxnService API (not SQL).
    /// Two user tablets, explicit txn writes to both, commit → both visible.
    #[tokio::test]
    async fn test_qa_phase3_cross_tablet_commit_via_txn_api() {
        let dir = tempdir().unwrap();
        let db = Database::open(DatabaseConfig::with_data_dir(dir.path())).unwrap();

        db.execute_query("CREATE TABLE qa_t1 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        db.execute_query("CREATE TABLE qa_t2 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();

        let table1 = table_id_by_name(&db, "qa_t1");
        let table2 = table_id_by_name(&db, "qa_t2");
        mount_user_tablet(&db, table1);
        mount_user_tablet(&db, table2);

        let key1 = table_int_pk_key(table1, 1);
        let key2 = table_int_pk_key(table2, 1);

        let mut ctx = db.txn_service.begin_explicit(false).unwrap();
        db.txn_service
            .put(&mut ctx, table1, key1.clone(), b"t1_val".to_vec())
            .await
            .unwrap();
        db.txn_service
            .put(&mut ctx, table2, key2.clone(), b"t2_val".to_vec())
            .await
            .unwrap();
        db.txn_service.commit(ctx).await.unwrap();

        // Both values visible to a new reader
        let reader = db.txn_service.begin(true).unwrap();
        assert_eq!(
            db.txn_service.get(&reader, table1, &key1).await.unwrap(),
            Some(b"t1_val".to_vec())
        );
        assert_eq!(
            db.txn_service.get(&reader, table2, &key2).await.unwrap(),
            Some(b"t2_val".to_vec())
        );

        // Verify data landed on correct tablets (not system)
        let system = db.tablet_manager().system_tablet();
        let tablet1 = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table1 })
            .unwrap();
        let tablet2 = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table2 })
            .unwrap();
        assert!(tablet_has_table_entries(&tablet1, table1));
        assert!(tablet_has_table_entries(&tablet2, table2));
        assert!(!tablet_has_table_entries(&system, table1));
        assert!(!tablet_has_table_entries(&system, table2));
    }

    /// T3.5b (QA): Cross-tablet rollback via TxnService API.
    /// Explicit txn writes to both tablets, rollback → neither visible.
    #[tokio::test]
    async fn test_qa_phase3_cross_tablet_rollback_via_txn_api() {
        let dir = tempdir().unwrap();
        let db = Database::open(DatabaseConfig::with_data_dir(dir.path())).unwrap();

        db.execute_query("CREATE TABLE qa_rb_t1 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        db.execute_query("CREATE TABLE qa_rb_t2 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();

        let table1 = table_id_by_name(&db, "qa_rb_t1");
        let table2 = table_id_by_name(&db, "qa_rb_t2");
        mount_user_tablet(&db, table1);
        mount_user_tablet(&db, table2);

        let key1 = table_int_pk_key(table1, 1);
        let key2 = table_int_pk_key(table2, 1);

        let mut ctx = db.txn_service.begin_explicit(false).unwrap();
        db.txn_service
            .put(&mut ctx, table1, key1.clone(), b"rb_v1".to_vec())
            .await
            .unwrap();
        db.txn_service
            .put(&mut ctx, table2, key2.clone(), b"rb_v2".to_vec())
            .await
            .unwrap();
        db.txn_service.rollback(ctx).unwrap();

        // Neither value visible to new reader
        let reader = db.txn_service.begin(true).unwrap();
        assert_eq!(
            db.txn_service.get(&reader, table1, &key1).await.unwrap(),
            None,
            "rolled-back key on tablet1 should not be visible"
        );
        assert_eq!(
            db.txn_service.get(&reader, table2, &key2).await.unwrap(),
            None,
            "rolled-back key on tablet2 should not be visible"
        );

        // Tablets should have no entries for these tables
        let tablet1 = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table1 })
            .unwrap();
        let tablet2 = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table2 })
            .unwrap();
        assert!(!tablet_has_table_entries(&tablet1, table1));
        assert!(!tablet_has_table_entries(&tablet2, table2));
    }

    /// T3.5c (QA): Read-your-writes within an explicit cross-tablet txn.
    /// Write to t1 and t2, read both back within the same explicit txn.
    #[tokio::test]
    async fn test_qa_phase3_cross_tablet_read_your_writes() {
        let dir = tempdir().unwrap();
        let db = Database::open(DatabaseConfig::with_data_dir(dir.path())).unwrap();

        db.execute_query("CREATE TABLE qa_ryw_t1 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        db.execute_query("CREATE TABLE qa_ryw_t2 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();

        let table1 = table_id_by_name(&db, "qa_ryw_t1");
        let table2 = table_id_by_name(&db, "qa_ryw_t2");
        mount_user_tablet(&db, table1);
        mount_user_tablet(&db, table2);

        let key1 = table_int_pk_key(table1, 1);
        let key2 = table_int_pk_key(table2, 1);

        let mut ctx = db.txn_service.begin_explicit(false).unwrap();
        db.txn_service
            .put(&mut ctx, table1, key1.clone(), b"ryw_v1".to_vec())
            .await
            .unwrap();
        db.txn_service
            .put(&mut ctx, table2, key2.clone(), b"ryw_v2".to_vec())
            .await
            .unwrap();

        // Read own writes BEFORE commit — should see pending data
        let val1 = db.txn_service.get(&ctx, table1, &key1).await.unwrap();
        let val2 = db.txn_service.get(&ctx, table2, &key2).await.unwrap();

        assert_eq!(
            val1,
            Some(b"ryw_v1".to_vec()),
            "explicit txn should see own pending write on tablet1"
        );
        assert_eq!(
            val2,
            Some(b"ryw_v2".to_vec()),
            "explicit txn should see own pending write on tablet2"
        );

        db.txn_service.rollback(ctx).unwrap();
    }

    /// T3.5d (QA): Cross-tablet snapshot isolation via TxnService API.
    /// Writer commits to t1+t2; reader-before sees neither; reader-after sees both.
    #[tokio::test]
    async fn test_qa_phase3_cross_tablet_snapshot_isolation_api() {
        let dir = tempdir().unwrap();
        let db = Database::open(DatabaseConfig::with_data_dir(dir.path())).unwrap();

        db.execute_query("CREATE TABLE qa_si_t1 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        db.execute_query("CREATE TABLE qa_si_t2 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();

        let table1 = table_id_by_name(&db, "qa_si_t1");
        let table2 = table_id_by_name(&db, "qa_si_t2");
        mount_user_tablet(&db, table1);
        mount_user_tablet(&db, table2);

        let key1 = table_int_pk_key(table1, 1);
        let key2 = table_int_pk_key(table2, 1);

        // Writer txn
        let mut writer = db.txn_service.begin_explicit(false).unwrap();
        db.txn_service
            .put(&mut writer, table1, key1.clone(), b"si_v1".to_vec())
            .await
            .unwrap();
        db.txn_service
            .put(&mut writer, table2, key2.clone(), b"si_v2".to_vec())
            .await
            .unwrap();

        // Reader-before: snapshot taken before commit
        let reader_before = db.txn_service.begin(true).unwrap();

        db.txn_service.commit(writer).await.unwrap();

        // Reader-before sees neither (snapshot isolation)
        assert_eq!(
            db.txn_service
                .get(&reader_before, table1, &key1)
                .await
                .unwrap(),
            None,
            "reader-before should not see committed data on tablet1"
        );
        assert_eq!(
            db.txn_service
                .get(&reader_before, table2, &key2)
                .await
                .unwrap(),
            None,
            "reader-before should not see committed data on tablet2"
        );

        // Reader-after sees both
        let reader_after = db.txn_service.begin(true).unwrap();
        assert_eq!(
            db.txn_service
                .get(&reader_after, table1, &key1)
                .await
                .unwrap(),
            Some(b"si_v1".to_vec()),
        );
        assert_eq!(
            db.txn_service
                .get(&reader_after, table2, &key2)
                .await
                .unwrap(),
            Some(b"si_v2".to_vec()),
        );
    }

    /// T3.4d (QA): finalize tracks clog_lsn on EVERY touched tablet's memtable.
    /// Verify via min_unflushed_lsn on each tablet.
    #[tokio::test]
    async fn test_qa_phase3_finalize_tracks_clog_lsn_on_every_tablet() {
        let dir = tempdir().unwrap();
        let db = Database::open(DatabaseConfig::with_data_dir(dir.path())).unwrap();

        db.execute_query("CREATE TABLE qa_lsn_t1 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        db.execute_query("CREATE TABLE qa_lsn_t2 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();

        let table1 = table_id_by_name(&db, "qa_lsn_t1");
        let table2 = table_id_by_name(&db, "qa_lsn_t2");
        mount_user_tablet(&db, table1);
        mount_user_tablet(&db, table2);

        let key1 = table_int_pk_key(table1, 1);
        let key2 = table_int_pk_key(table2, 1);

        let mut ctx = db.txn_service.begin_explicit(false).unwrap();
        db.txn_service
            .put(&mut ctx, table1, key1.clone(), b"lsn_v1".to_vec())
            .await
            .unwrap();
        db.txn_service
            .put(&mut ctx, table2, key2.clone(), b"lsn_v2".to_vec())
            .await
            .unwrap();
        let info = db.txn_service.commit(ctx).await.unwrap();

        let tablet1 = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table1 })
            .unwrap();
        let tablet2 = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table2 })
            .unwrap();

        // Both tablets should have min_unflushed_lsn equal to the commit LSN
        let lsn1 = tablet1.min_unflushed_lsn();
        let lsn2 = tablet2.min_unflushed_lsn();
        assert!(
            lsn1.is_some(),
            "tablet1 should have unflushed LSN after commit"
        );
        assert!(
            lsn2.is_some(),
            "tablet2 should have unflushed LSN after commit"
        );
        assert_eq!(
            lsn1.unwrap(),
            info.lsn,
            "tablet1's min_unflushed_lsn should match commit LSN"
        );
        assert_eq!(
            lsn2.unwrap(),
            info.lsn,
            "tablet2's min_unflushed_lsn should match commit LSN"
        );
    }

    /// T3.6b (QA): Reservation is released only after all tablets finalized.
    /// Check that no reservation is active after a successful cross-tablet commit.
    #[tokio::test]
    async fn test_qa_phase3_reservation_released_after_commit() {
        let dir = tempdir().unwrap();
        let db = Database::open(DatabaseConfig::with_data_dir(dir.path())).unwrap();

        db.execute_query("CREATE TABLE qa_res_t1 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        db.execute_query("CREATE TABLE qa_res_t2 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();

        let table1 = table_id_by_name(&db, "qa_res_t1");
        let table2 = table_id_by_name(&db, "qa_res_t2");
        mount_user_tablet(&db, table1);
        mount_user_tablet(&db, table2);

        // Verify no reservation before
        let stats_before = db.tablet_manager().commit_reservation_stats();
        let active_before = stats_before.active;

        let key1 = table_int_pk_key(table1, 1);
        let key2 = table_int_pk_key(table2, 1);

        let mut ctx = db.txn_service.begin_explicit(false).unwrap();
        db.txn_service
            .put(&mut ctx, table1, key1, b"r_v1".to_vec())
            .await
            .unwrap();
        db.txn_service
            .put(&mut ctx, table2, key2, b"r_v2".to_vec())
            .await
            .unwrap();
        db.txn_service.commit(ctx).await.unwrap();

        // After commit, reservation should be released
        let stats_after = db.tablet_manager().commit_reservation_stats();
        assert_eq!(
            stats_after.active, active_before,
            "reservation should be released after successful cross-tablet commit"
        );
        assert!(
            stats_after.total_allocated > stats_before.total_allocated,
            "should have allocated at least one reservation"
        );
        assert_eq!(
            stats_after.total_allocated - stats_before.total_allocated,
            stats_after.total_released - stats_before.total_released,
            "alloc count should equal release count after commit"
        );
    }

    /// T3.5 (QA): mutation_tablets properly records tablet routing from metadata-first put.
    /// After writes via put, the commit path should use grouped operations.
    #[tokio::test]
    async fn test_qa_phase3_mutation_tablets_recording() {
        let dir = tempdir().unwrap();
        let db = Database::open(DatabaseConfig::with_data_dir(dir.path())).unwrap();

        db.execute_query("CREATE TABLE qa_mt_t1 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        db.execute_query("CREATE TABLE qa_mt_t2 (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();

        let table1 = table_id_by_name(&db, "qa_mt_t1");
        let table2 = table_id_by_name(&db, "qa_mt_t2");
        mount_user_tablet(&db, table1);
        mount_user_tablet(&db, table2);

        let key1 = table_int_pk_key(table1, 1);
        let key2 = table_int_pk_key(table2, 1);

        // Multiple writes to same table — should all share same tablet
        let key1b = table_int_pk_key(table1, 2);

        let mut ctx = db.txn_service.begin_explicit(false).unwrap();
        db.txn_service
            .put(&mut ctx, table1, key1.clone(), b"mv1".to_vec())
            .await
            .unwrap();
        db.txn_service
            .put(&mut ctx, table1, key1b.clone(), b"mv1b".to_vec())
            .await
            .unwrap();
        db.txn_service
            .put(&mut ctx, table2, key2.clone(), b"mv2".to_vec())
            .await
            .unwrap();

        // Verify mutation_tablets has entries for all keys
        assert_eq!(ctx.mutation_tablets.len(), 3);
        assert_eq!(
            ctx.mutation_tablets.get(&key1).copied(),
            Some(TabletId::Table { table_id: table1 })
        );
        assert_eq!(
            ctx.mutation_tablets.get(&key1b).copied(),
            Some(TabletId::Table { table_id: table1 })
        );
        assert_eq!(
            ctx.mutation_tablets.get(&key2).copied(),
            Some(TabletId::Table { table_id: table2 })
        );

        db.txn_service.commit(ctx).await.unwrap();
    }

    /// T3.3 (QA): SQL scan routes correctly — cross-table SELECT sees isolated data.
    #[tokio::test]
    async fn test_qa_phase3_sql_scan_isolation_across_tablets() {
        let dir = tempdir().unwrap();
        let db = Database::open(DatabaseConfig::with_data_dir(dir.path())).unwrap();

        db.execute_query("CREATE TABLE qa_scan_a (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        db.execute_query("CREATE TABLE qa_scan_b (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();

        let table_a = table_id_by_name(&db, "qa_scan_a");
        let table_b = table_id_by_name(&db, "qa_scan_b");
        mount_user_tablet(&db, table_a);
        mount_user_tablet(&db, table_b);

        // Insert into both tables
        db.execute_query("INSERT INTO qa_scan_a VALUES (1, 100), (2, 200)")
            .await
            .unwrap();
        db.execute_query("INSERT INTO qa_scan_b VALUES (1, 300), (2, 400), (3, 500)")
            .await
            .unwrap();

        // Verify each table scan returns only its own data
        match db
            .execute_query("SELECT id, v FROM qa_scan_a")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 2, "qa_scan_a should have exactly 2 rows");
            }
            other => panic!("expected rows for qa_scan_a, got {other:?}"),
        }
        match db
            .execute_query("SELECT id, v FROM qa_scan_b")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 3, "qa_scan_b should have exactly 3 rows");
            }
            other => panic!("expected rows for qa_scan_b, got {other:?}"),
        }

        // Verify data is on correct tablets
        let tablet_a = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table_a })
            .unwrap();
        let tablet_b = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table_b })
            .unwrap();
        assert!(tablet_has_table_entries(&tablet_a, table_a));
        assert!(tablet_has_table_entries(&tablet_b, table_b));
        assert!(!tablet_has_table_entries(&tablet_a, table_b));
        assert!(!tablet_has_table_entries(&tablet_b, table_a));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase4_recovery_one_flushed_one_unflushed_routes_to_correct_tablets() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let (table1, table2, key1, val1, key2, val2, commit_lsn) =
            prepare_phase4_two_tablet_recovery_case(dir.path(), true, false).await;

        let db = Database::open(config).unwrap();
        let tablet1 = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table1 })
            .expect("tablet1 should recover from per-tablet ilog");
        let tablet2 = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table2 })
            .expect("tablet2 should be mounted for clog replay");
        let system = db.tablet_manager().system_tablet();

        assert_eq!(tablet_get_value(&tablet1, &key1), Some(val1));
        assert_eq!(
            tablet_get_value(&tablet2, &key2),
            Some(val2),
            "system_value={:?}",
            tablet_get_value(&system, &key2),
        );
        assert!(!tablet_has_table_entries(&system, table1));
        assert!(!tablet_has_table_entries(&system, table2));
        let _ = commit_lsn;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase4_recovery_two_tablets_after_flush() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let (table1, table2, key1, val1, key2, val2, _) =
            prepare_phase4_two_tablet_recovery_case(dir.path(), true, true).await;

        let db = Database::open(config).unwrap();
        let tablet1 = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table1 })
            .expect("tablet1 should recover from per-tablet ilog");
        let tablet2 = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table2 })
            .expect("tablet2 should recover from per-tablet ilog");
        let system = db.tablet_manager().system_tablet();

        assert_eq!(tablet_get_value(&tablet1, &key1), Some(val1));
        assert_eq!(tablet_get_value(&tablet2, &key2), Some(val2));
        assert!(!tablet_has_table_entries(&system, table1));
        assert!(!tablet_has_table_entries(&system, table2));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase4_recovery_clog_replay_routes_to_correct_tablets() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let (table1, table2, key1, val1, key2, val2, _) =
            prepare_phase4_two_tablet_recovery_case(dir.path(), false, false).await;

        let db = Database::open(config).unwrap();
        let tablet1 = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table1 })
            .expect("tablet1 should be mounted for replay");
        let tablet2 = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table2 })
            .expect("tablet2 should be mounted for replay");
        let system = db.tablet_manager().system_tablet();

        assert_eq!(tablet_get_value(&tablet1, &key1), Some(val1));
        assert_eq!(tablet_get_value(&tablet2, &key2), Some(val2));
        assert!(!tablet_has_table_entries(&system, table1));
        assert!(!tablet_has_table_entries(&system, table2));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase4_recovery_repeated_reopen_idempotent() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let (table1, table2, key1, val1, key2, val2, _) =
            prepare_phase4_two_tablet_recovery_case(dir.path(), false, false).await;

        let db1 = Database::open(config.clone()).unwrap();
        let t1 = db1
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table1 })
            .unwrap();
        let t2 = db1
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table2 })
            .unwrap();
        assert_eq!(tablet_get_value(&t1, &key1), Some(val1.clone()));
        assert_eq!(tablet_get_value(&t2, &key2), Some(val2.clone()));
        drop(db1);

        let db2 = Database::open(config).unwrap();
        let t1 = db2
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table1 })
            .unwrap();
        let t2 = db2
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table2 })
            .unwrap();
        assert_eq!(tablet_get_value(&t1, &key1), Some(val1));
        assert_eq!(tablet_get_value(&t2, &key2), Some(val2));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase4_recovery_ilog_failure_one_tablet_isolated() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let (table1, table2, _key1, _val1, key2, val2, _) =
            prepare_phase4_two_tablet_recovery_case(dir.path(), true, true).await;

        let broken_ilog = dir
            .path()
            .join("tablets")
            .join(format!("t_{table1}"))
            .join("ilog")
            .join("tisql.ilog");
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&broken_ilog)
                .unwrap();
            f.write_all(b"bad!").unwrap();
            f.flush().unwrap();
        }

        let open_err = match Database::open(config) {
            Ok(_) => panic!("open should fail with corrupted tablet ilog"),
            Err(e) => e,
        };
        let msg = format!("{open_err}");
        assert!(
            msg.contains("Invalid ilog file magic")
                || msg.contains("Failed")
                || msg.contains("ilog"),
            "unexpected error message: {msg}"
        );

        let lsn_provider = crate::lsn::new_lsn_provider();
        let io = crate::io::IoService::new_for_test(64).unwrap();
        let recovered = LsmRecovery::recover_tablet(
            &dir.path().join("tablets").join(format!("t_{table2}")),
            lsn_provider,
            io,
            &tokio::runtime::Handle::current(),
        )
        .expect("healthy tablet must remain recoverable when another tablet ilog is corrupted");
        assert_eq!(
            tablet_get_value(&recovered.engine, &key2),
            Some(val2),
            "healthy tablet data must remain intact"
        );
    }

    #[tokio::test]
    async fn test_drop_table_gc_waits_for_safe_point_pin() {
        let (db, _dir) = create_test_db();

        db.execute_query("CREATE TABLE gc_pending (id INT PRIMARY KEY, val INT)")
            .await
            .unwrap();
        db.execute_query("INSERT INTO gc_pending VALUES (1, 10), (2, 20), (3, 30)")
            .await
            .unwrap();
        let table_id = table_id_by_name(&db, "gc_pending");
        let tablet_id = TabletId::Table { table_id };
        let tablet_dir = db.tablet_manager().tablet_dir(tablet_id);
        assert!(tablet_dir.exists());

        // Pin safe point to 0 so drop GC cannot retire the tablet yet.
        db.session_registry().register(99, 1);

        db.execute_query("DROP TABLE gc_pending").await.unwrap();

        // Keep waking the worker; task should stay pending while pinned.
        let mut latest_status: Option<String> = None;
        for _ in 0..30 {
            db.gc_worker.notify();
            sleep(Duration::from_millis(30)).await;

            let tasks = load_gc_tasks(db.txn_service.as_ref()).unwrap();
            if let Some(task) = tasks.first() {
                latest_status = Some(task.status.clone());
            }
        }

        let status = latest_status.expect("expected at least one gc task row");
        assert_eq!(
            status, "pending",
            "GC task should stay pending while safe point is pinned"
        );
        assert!(tablet_dir.exists());
        assert!(db.tablet_manager().get_tablet(tablet_id).is_some());

        db.session_registry().unregister(99);
        for _ in 0..100 {
            db.gc_worker.notify();
            sleep(Duration::from_millis(20)).await;
            if !tablet_dir.exists() {
                break;
            }
        }
        assert!(!tablet_dir.exists());
        assert!(db.tablet_manager().get_tablet(tablet_id).is_none());
    }

    #[tokio::test]
    async fn test_run_log_gc_once_reclaims_logs_and_recovers() {
        use crate::clog::{ClogBatch, ClogEntry, ClogOp, ClogService};
        use crate::tablet::WriteBatch;

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
        let stats = db.run_log_gc_once().await.unwrap();

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
    async fn test_phase5_run_log_gc_once_uses_global_min_boundary() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Database::open(config.clone()).unwrap();

        db.execute_query("CREATE TABLE gc_fast (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        db.execute_query("CREATE TABLE gc_slow (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();

        let fast_id = table_id_by_name(&db, "gc_fast");
        let slow_id = table_id_by_name(&db, "gc_slow");
        mount_user_tablet_durable(&db, fast_id);
        mount_user_tablet_durable(&db, slow_id);

        db.execute_query("INSERT INTO gc_fast VALUES (1, 10)")
            .await
            .unwrap();
        db.execute_query("INSERT INTO gc_slow VALUES (1, 20)")
            .await
            .unwrap();

        let fast_tablet = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: fast_id })
            .unwrap();
        let slow_tablet = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: slow_id })
            .unwrap();
        fast_tablet.flush_all_with_active().unwrap();
        let slow_cap = slow_tablet.min_unflushed_lsn().unwrap().saturating_sub(1);

        let clog_size_before = db.txn_service.clog_service().file_size().unwrap();
        let stats = db.run_log_gc_once().await.unwrap();
        assert!(
            stats.safe_lsn <= slow_cap,
            "safe_lsn={} must be <= slow-tablet cap={slow_cap}",
            stats.safe_lsn
        );
        assert!(
            stats.tablet_checkpoints.len() >= 3,
            "expected checkpoints for system + two user tablets"
        );
        assert!(
            db.txn_service.clog_service().file_size().unwrap() <= clog_size_before,
            "clog file should not grow after GC truncation"
        );

        db.close().await.unwrap();
        drop(db);

        let db2 = Database::open(config).unwrap();
        match db2
            .execute_query("SELECT v FROM gc_fast WHERE id = 1")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => assert_eq!(data.len(), 1),
            other => panic!("unexpected result for gc_fast: {other:?}"),
        }
        match db2
            .execute_query("SELECT v FROM gc_slow WHERE id = 1")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => assert_eq!(data.len(), 1),
            other => panic!("unexpected result for gc_slow: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_phase5_run_log_gc_once_truncates_each_tablet_ilog() {
        let dir = tempdir().unwrap();
        let db = Database::open(DatabaseConfig::with_data_dir(dir.path())).unwrap();

        db.execute_query("CREATE TABLE gc_ilog_a (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        db.execute_query("CREATE TABLE gc_ilog_b (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();

        let a_id = table_id_by_name(&db, "gc_ilog_a");
        let b_id = table_id_by_name(&db, "gc_ilog_b");
        mount_user_tablet_durable(&db, a_id);
        mount_user_tablet_durable(&db, b_id);

        db.execute_query("INSERT INTO gc_ilog_a VALUES (1, 100)")
            .await
            .unwrap();
        db.execute_query("INSERT INTO gc_ilog_b VALUES (1, 200)")
            .await
            .unwrap();

        let stats = db.run_log_gc_once().await.unwrap();
        assert!(
            stats
                .tablet_ilogs
                .iter()
                .any(|entry| entry.tablet_id == TabletId::System),
            "system tablet ilog truncation must be recorded"
        );
        assert!(
            stats
                .tablet_ilogs
                .iter()
                .any(|entry| entry.tablet_id == TabletId::Table { table_id: a_id }),
            "tablet A ilog truncation must be recorded"
        );
        assert!(
            stats
                .tablet_ilogs
                .iter()
                .any(|entry| entry.tablet_id == TabletId::Table { table_id: b_id }),
            "tablet B ilog truncation must be recorded"
        );

        for entry in &stats.tablet_ilogs {
            let file_size = if entry.tablet_id == TabletId::System {
                db.ilog.file_size().unwrap()
            } else {
                let ilog_path = db
                    .tablet_manager()
                    .tablet_dir(entry.tablet_id)
                    .join("ilog")
                    .join("tisql.ilog");
                std::fs::metadata(&ilog_path).unwrap().len()
            };
            assert_eq!(
                file_size, entry.truncate.new_file_size,
                "reported ilog size must match on-disk size for {:?}",
                entry.tablet_id
            );
        }
    }

    /// T5.3d (QA): Cross-tablet txn clog entries are kept as a complete transaction
    /// group during clog truncation — never split.
    ///
    /// Scenario:
    /// 1. Baseline txn (system-only) — establishes truncatable clog entries.
    /// 2. Flush all tablets so baseline is fully durable in SSTs.
    /// 3. Cross-tablet txn: INSERT into table_a (tablet A) + table_b (tablet B).
    /// 4. Flush only tablet A (asymmetric progress).
    /// 5. Run log GC — boundary must protect the cross-tablet txn.
    /// 6. Verify clog retained enough entries for recovery.
    /// 7. Drop DB (simulate crash — tablet B data only in clog).
    /// 8. Reopen and verify both tables have their cross-tablet txn data.
    ///
    /// If the clog truncation split the cross-tablet txn (removed Puts but kept
    /// Commit, or vice versa), recovery would lose data on tablet B.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase5_clog_truncation_preserves_cross_tablet_txn_group() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Database::open(config.clone()).unwrap();

        // Create two user tables.
        db.execute_query("CREATE TABLE txn_split_a (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        db.execute_query("CREATE TABLE txn_split_b (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        let table_a = table_id_by_name(&db, "txn_split_a");
        let table_b = table_id_by_name(&db, "txn_split_b");
        mount_user_tablet_durable(&db, table_a);
        mount_user_tablet_durable(&db, table_b);

        // Step 1-2: Write baseline data and flush everything so that the
        // baseline clog entries become truncatable.
        db.execute_query("INSERT INTO txn_split_a VALUES (100, 1000)")
            .await
            .unwrap();
        db.execute_query("INSERT INTO txn_split_b VALUES (100, 2000)")
            .await
            .unwrap();
        db.tablet_manager().flush_all_with_active().unwrap();

        // Run one GC cycle to truncate the baseline entries.
        let baseline_gc = db.run_log_gc_once().await.unwrap();
        assert!(
            baseline_gc.clog.entries_removed > 0 || baseline_gc.safe_lsn == 0,
            "baseline GC should either truncate entries or have safe_lsn=0 (fresh db)"
        );

        // Step 3: Cross-tablet explicit txn — writes to both tablets.
        db.execute_query("BEGIN").await.unwrap();
        db.execute_query("INSERT INTO txn_split_a VALUES (200, 3000)")
            .await
            .unwrap();
        db.execute_query("INSERT INTO txn_split_b VALUES (200, 4000)")
            .await
            .unwrap();
        db.execute_query("COMMIT").await.unwrap();

        // Step 4: Flush only tablet A — creates asymmetric progress.
        // Tablet A: cross-tablet txn data is in SSTs (flushed).
        // Tablet B: cross-tablet txn data is only in memtable (unflushed).
        let tablet_a_engine = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table_a })
            .unwrap();
        tablet_a_engine.flush_all_with_active().unwrap();

        let tablet_b_engine = db
            .tablet_manager()
            .get_tablet(TabletId::Table { table_id: table_b })
            .unwrap();
        let b_unflushed = tablet_b_engine.min_unflushed_lsn();
        assert!(
            b_unflushed.is_some(),
            "tablet B must have unflushed data from the cross-tablet txn"
        );

        // Step 5: Run log GC — the global boundary must protect the cross-tablet
        // txn because tablet B still has unflushed data.
        let gc_stats = db.run_log_gc_once().await.unwrap();
        assert!(
            gc_stats.safe_lsn < b_unflushed.unwrap(),
            "safe_lsn={} must be < tablet B's min_unflushed_lsn={} to protect the cross-tablet txn",
            gc_stats.safe_lsn,
            b_unflushed.unwrap(),
        );

        // Verify the cross-tablet txn data is still readable before crash.
        match db
            .execute_query("SELECT v FROM txn_split_a WHERE id = 200")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1, "tablet A cross-txn row must be readable");
                assert_eq!(data[0][0], "3000");
            }
            other => panic!("unexpected result: {other:?}"),
        }
        match db
            .execute_query("SELECT v FROM txn_split_b WHERE id = 200")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1, "tablet B cross-txn row must be readable");
                assert_eq!(data[0][0], "4000");
            }
            other => panic!("unexpected result: {other:?}"),
        }

        // Step 7: Simulate crash — drop without flushing tablet B.
        // Tablet B's cross-tablet txn data is only recoverable from clog replay.
        drop(db);

        // Step 8: Reopen and verify both tablets have the cross-tablet txn data.
        // If clog truncation split the txn group, tablet B's data would be lost.
        let db2 = Database::open(config).unwrap();

        // Baseline data (from step 1) must survive (it was flushed to SSTs).
        match db2
            .execute_query("SELECT v FROM txn_split_a WHERE id = 100")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1, "tablet A baseline row must survive recovery");
            }
            other => panic!("unexpected result: {other:?}"),
        }
        match db2
            .execute_query("SELECT v FROM txn_split_b WHERE id = 100")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1, "tablet B baseline row must survive recovery");
            }
            other => panic!("unexpected result: {other:?}"),
        }

        // Cross-tablet txn data — the critical assertion.
        match db2
            .execute_query("SELECT v FROM txn_split_a WHERE id = 200")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(
                    data.len(),
                    1,
                    "tablet A cross-txn row must survive clog truncation + recovery"
                );
                assert_eq!(data[0][0], "3000");
            }
            other => panic!("unexpected result: {other:?}"),
        }
        match db2
            .execute_query("SELECT v FROM txn_split_b WHERE id = 200")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(
                    data.len(),
                    1,
                    "tablet B cross-txn row must survive clog truncation + recovery (txn group intact)"
                );
                assert_eq!(data[0][0], "4000");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_where_clause() {
        let (db, _dir) = create_test_db();

        db.execute_query("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
            .await
            .unwrap();
        db.execute_query("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
            .await
            .unwrap();

        let result = db
            .execute_query("SELECT a, b FROM t WHERE a > 1")
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

        db.execute_query("CREATE TABLE t (a INT PRIMARY KEY)")
            .await
            .unwrap();
        db.execute_query("INSERT INTO t VALUES (3), (1), (2)")
            .await
            .unwrap();

        let result = db
            .execute_query("SELECT a FROM t ORDER BY a")
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

        db.execute_query("CREATE TABLE t (a INT PRIMARY KEY)")
            .await
            .unwrap();
        db.execute_query("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
            .await
            .unwrap();

        let result = db.execute_query("SELECT a FROM t LIMIT 3").await.unwrap();
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

        db.execute_query("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
            .await
            .unwrap();
        db.execute_query("INSERT INTO t VALUES (1, 100), (2, 200)")
            .await
            .unwrap();

        db.execute_query("UPDATE t SET val = 999 WHERE id = 1")
            .await
            .unwrap();

        let result = db
            .execute_query("SELECT id, val FROM t WHERE id = 1")
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

        db.execute_query("CREATE TABLE t (a INT PRIMARY KEY)")
            .await
            .unwrap();
        db.execute_query("INSERT INTO t VALUES (1), (2), (3)")
            .await
            .unwrap();

        db.execute_query("DELETE FROM t WHERE a = 2").await.unwrap();

        let result = db.execute_query("SELECT a FROM t").await.unwrap();
        match result {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 2);
            }
            _ => panic!("Expected rows"),
        }
    }
}
