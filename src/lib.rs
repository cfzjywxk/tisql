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
mod sql;
pub mod storage;
mod transaction;
mod tso;

// Re-export public interfaces (traits only) and commonly used types
pub use catalog::Catalog;
pub use clog::ClogService;
pub use lsn::{new_lsn_provider, LsnProvider, SharedLsnProvider};
pub use protocol::{MySqlServer, MYSQL_DEFAULT_PORT};
pub use session::{Priority, QueryCtx, Session, SessionVars};
pub use storage::{PessimisticStorage, StorageEngine};
pub use transaction::{CommitInfo, TxnCtx, TxnService, TxnState};
pub use tso::TsoService;
pub use worker::{WorkerPool, WorkerPoolConfig};

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
        ClogBatch, ClogEntry, ClogOp, FileClogConfig, FileClogService, TruncateStats,
    };

    // Arena (from util - it's a general-purpose allocator)
    pub use crate::util::{ArenaConfig, PageArena, DEFAULT_PAGE_SIZE};

    // Production memtable engine
    pub use crate::storage::{MemTableEngine, MemoryStats, VersionedMemTableEngine};

    // LSM storage engine for testing
    pub use crate::storage::{
        CompactionExecutor, CompactionPicker, CompactionTask, IlogConfig, IlogService, LsmConfig,
        LsmConfigBuilder, LsmEngine, LsmRecovery, LsmStats, ManifestDelta, MemTable,
        RecoveryResult, SstBuilder, SstBuilderOptions, SstIterator, SstMeta, SstReader,
        SstReaderRef, Version,
    };

    pub use crate::transaction::{ConcurrencyManager, TransactionService};
    pub use crate::tso::LocalTso;

    // Test helper extension trait for TransactionService
    use crate::clog::ClogService;
    use crate::error::Result;
    use crate::storage::PessimisticStorage;
    use crate::transaction::{CommitInfo, TxnService};
    use crate::tso::TsoService;

    /// Extension trait for TransactionService with test-only autocommit helpers.
    pub trait TxnServiceTestExt {
        /// Execute a single put with autocommit (test helper).
        fn autocommit_put(&self, key: &[u8], value: &[u8]) -> Result<CommitInfo>;

        /// Execute a single delete with autocommit (test helper).
        fn autocommit_delete(&self, key: &[u8]) -> Result<CommitInfo>;
    }

    impl<S, C, T> TxnServiceTestExt for TransactionService<S, C, T>
    where
        S: PessimisticStorage + 'static,
        C: ClogService + 'static,
        T: TsoService,
    {
        fn autocommit_put(&self, key: &[u8], value: &[u8]) -> Result<CommitInfo> {
            let mut ctx = self.begin(false)?;
            self.put(&mut ctx, key.to_vec(), value.to_vec())?;
            self.commit(ctx)
        }

        fn autocommit_delete(&self, key: &[u8]) -> Result<CommitInfo> {
            let mut ctx = self.begin(false)?;
            self.delete(&mut ctx, key.to_vec())?;
            self.commit(ctx)
        }
    }
}

// Internal imports (not re-exported)
use catalog::MvccCatalog;
use clog::FileClogService;
use error::Result;
use executor::{ExecutionResult, Executor, SimpleExecutor};
use sql::{Binder, Parser};
use storage::{IlogService, LsmEngine, LsmRecovery};
use transaction::{ConcurrencyManager, TransactionService};
use tso::LocalTso;
use types::Value;
use util::Timer;

use std::path::PathBuf;
use std::sync::Arc;

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
    fn handle_mp_query<T: TxnService, C: Catalog>(
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
        self.executor.execute(plan, txn_service, catalog)
    }

    /// Execute SQL with session context for explicit transaction support.
    ///
    /// This method handles transaction control statements (BEGIN, COMMIT, ROLLBACK)
    /// and uses the session's active transaction context when available.
    fn handle_mp_query_with_session<T: TxnService, C: Catalog>(
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
}

impl Database {
    /// Open database with persistence and recovery.
    ///
    /// This is the only way to create a Database instance.
    /// All databases use file-based persistence with LSM storage engine.
    pub fn open(config: DatabaseConfig) -> Result<Self> {
        log_info!("Opening TiSQL database at {:?}", config.data_dir);

        // Use LsmRecovery for coordinated recovery of storage and logs
        let recovery = LsmRecovery::new(&config.data_dir);
        let recovery_result = recovery.recover()?;

        log_info!(
            "LSM recovery complete: {} clog entries replayed, {} txns, flushed_lsn={}, max_commit_ts={}",
            recovery_result.stats.clog_entries,
            recovery_result.stats.txn_count,
            recovery_result.stats.flushed_lsn,
            recovery_result.stats.max_commit_ts
        );

        // Wrap storage in Arc
        let storage = Arc::new(recovery_result.engine);

        // Create TSO service, starting from recovered max_commit_ts + 1
        let tso = Arc::new(LocalTso::new(recovery_result.stats.max_commit_ts + 1));

        // Create concurrency manager with recovered max_ts
        let concurrency_manager =
            Arc::new(ConcurrencyManager::new(recovery_result.stats.max_commit_ts));

        // Create transaction service with recovered components
        let txn_service = Arc::new(TransactionService::new(
            Arc::clone(&storage),
            recovery_result.clog,
            Arc::clone(&tso),
            Arc::clone(&concurrency_manager),
        ));

        // Create MVCC catalog using same txn_service
        let catalog = MvccCatalog::new(Arc::clone(&txn_service));

        // Bootstrap if fresh database (no default schema exists)
        if !catalog.is_bootstrapped()? {
            log_info!("Fresh database - bootstrapping catalog");
            catalog.bootstrap()?;
        } else {
            // Load schema version from storage for existing database
            catalog.load_schema_version()?;
        }

        Ok(Self {
            sql_engine: SQLEngine::new(),
            txn_service,
            catalog,
            ilog: recovery_result.ilog,
            storage,
        })
    }

    /// Handle MySQL protocol query text (COM_QUERY).
    ///
    /// This is the main entry point for SQL execution from the MySQL protocol.
    /// For prepared statements, use `handle_mp_prepare` and `handle_mp_execute` (TODO).
    ///
    /// Note: This method creates a temporary QueryCtx for backward compatibility.
    /// The proper way is to use `handle_mp_query_with_session` which takes a Session.
    pub fn handle_mp_query(&self, sql: &str) -> Result<QueryResult> {
        // Create a temporary QueryCtx for backward compatibility
        let query_ctx = session::QueryCtx::new();
        self.handle_mp_query_with_ctx(sql, &query_ctx)
    }

    /// Handle MySQL protocol query text with a Session (immutable).
    ///
    /// This method creates a QueryCtx from the Session before executing.
    /// For explicit transaction support (BEGIN/COMMIT/ROLLBACK), use
    /// `handle_mp_query_with_session_mut` instead.
    pub fn handle_mp_query_with_session(
        &self,
        sql: &str,
        session: &session::Session,
    ) -> Result<QueryResult> {
        let query_ctx = session.new_query_ctx();
        self.handle_mp_query_with_ctx(sql, &query_ctx)
    }

    /// Handle MySQL protocol query text with mutable Session for transaction control.
    ///
    /// This is the preferred method for the protocol layer - it supports:
    /// - BEGIN / START TRANSACTION: Starts explicit transaction
    /// - COMMIT: Commits the current explicit transaction
    /// - ROLLBACK: Rolls back the current explicit transaction
    /// - Other statements: Uses session's active transaction or auto-commit mode
    pub fn handle_mp_query_with_session_mut(
        &self,
        sql: &str,
        session: &mut session::Session,
    ) -> Result<QueryResult> {
        let timer = Timer::new("query");
        let query_ctx = session.new_query_ctx();

        // Use session-aware execution for transaction control
        let result = self.sql_engine.handle_mp_query_with_session(
            sql,
            &query_ctx,
            self.txn_service.as_ref(),
            &self.catalog,
            session,
        )?;

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
            ExecutionResult::Ok => {
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
    pub fn handle_mp_query_with_ctx(
        &self,
        sql: &str,
        query_ctx: &session::QueryCtx,
    ) -> Result<QueryResult> {
        let timer = Timer::new("query");

        // Delegate all SQL processing to SQLEngine
        // SQLEngine handles: parse -> bind -> execute through TxnService
        let result = self.sql_engine.handle_mp_query(
            sql,
            query_ctx,
            self.txn_service.as_ref(),
            &self.catalog,
        )?;

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
            ExecutionResult::Ok => {
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

    /// Close the database (flush memtables and sync logs).
    pub fn close(&self) -> Result<()> {
        // Flush any pending memtable data to SSTs
        if let Err(e) = self.storage.flush_all_with_active() {
            log_warn!("Error flushing memtables on close: {}", e);
        }

        // Close commit log
        self.txn_service.clog_service().close()?;

        // Close ilog
        self.ilog.close()?;

        log_info!("Database closed");
        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // Best-effort flush on drop
        let _ = self.storage.flush_all_with_active();
        let _ = self.txn_service.clog_service().close();
        let _ = self.ilog.close();
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
    use tempfile::tempdir;

    fn create_test_db() -> (Database, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Database::open(config).unwrap();
        (db, dir)
    }

    #[test]
    fn test_select_literal() {
        let (db, _dir) = create_test_db();
        let result = db.handle_mp_query("SELECT 1 + 1").unwrap();
        match result {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1);
                assert_eq!(data[0][0], "2");
            }
            _ => panic!("Expected rows"),
        }
    }

    #[test]
    fn test_create_and_insert() {
        let (db, _dir) = create_test_db();

        db.handle_mp_query("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))")
            .unwrap();

        let result = db
            .handle_mp_query("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            .unwrap();
        match result {
            QueryResult::Affected(count) => assert_eq!(count, 1),
            _ => panic!("Expected affected count"),
        }

        let result = db.handle_mp_query("SELECT id, name FROM users").unwrap();
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

    #[test]
    fn test_database_with_durability() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());

        // Create database and insert data
        {
            let db = Database::open(config.clone()).unwrap();
            db.handle_mp_query("CREATE TABLE t (id INT PRIMARY KEY, val VARCHAR(100))")
                .unwrap();
            db.handle_mp_query("INSERT INTO t VALUES (1, 'hello')")
                .unwrap();
            db.handle_mp_query("INSERT INTO t VALUES (2, 'world')")
                .unwrap();
            db.close().unwrap();
        }

        // Reopen - data should be in storage but catalog needs recovery
        // For now, this tests that the commit log file is created and can be recovered
        {
            let _db = Database::open(config).unwrap();
            // Note: Catalog is not persisted yet, so CREATE TABLE won't survive
            // But commit log entries are persisted
        }
    }

    #[test]
    fn test_where_clause() {
        let (db, _dir) = create_test_db();

        db.handle_mp_query("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
            .unwrap();
        db.handle_mp_query("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
            .unwrap();

        let result = db
            .handle_mp_query("SELECT a, b FROM t WHERE a > 1")
            .unwrap();
        match result {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 2);
            }
            _ => panic!("Expected rows"),
        }
    }

    #[test]
    fn test_order_by() {
        let (db, _dir) = create_test_db();

        db.handle_mp_query("CREATE TABLE t (a INT PRIMARY KEY)")
            .unwrap();
        db.handle_mp_query("INSERT INTO t VALUES (3), (1), (2)")
            .unwrap();

        let result = db.handle_mp_query("SELECT a FROM t ORDER BY a").unwrap();
        match result {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data[0][0], "1");
                assert_eq!(data[1][0], "2");
                assert_eq!(data[2][0], "3");
            }
            _ => panic!("Expected rows"),
        }
    }

    #[test]
    fn test_limit() {
        let (db, _dir) = create_test_db();

        db.handle_mp_query("CREATE TABLE t (a INT PRIMARY KEY)")
            .unwrap();
        db.handle_mp_query("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
            .unwrap();

        let result = db.handle_mp_query("SELECT a FROM t LIMIT 3").unwrap();
        match result {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 3);
            }
            _ => panic!("Expected rows"),
        }
    }

    #[test]
    fn test_update() {
        let (db, _dir) = create_test_db();

        db.handle_mp_query("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
            .unwrap();
        db.handle_mp_query("INSERT INTO t VALUES (1, 100), (2, 200)")
            .unwrap();

        db.handle_mp_query("UPDATE t SET val = 999 WHERE id = 1")
            .unwrap();

        let result = db
            .handle_mp_query("SELECT id, val FROM t WHERE id = 1")
            .unwrap();
        match result {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data[0][1], "999");
            }
            _ => panic!("Expected rows"),
        }
    }

    #[test]
    fn test_delete() {
        let (db, _dir) = create_test_db();

        db.handle_mp_query("CREATE TABLE t (a INT PRIMARY KEY)")
            .unwrap();
        db.handle_mp_query("INSERT INTO t VALUES (1), (2), (3)")
            .unwrap();

        db.handle_mp_query("DELETE FROM t WHERE a = 2").unwrap();

        let result = db.handle_mp_query("SELECT a FROM t").unwrap();
        match result {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 2);
            }
            _ => panic!("Expected rows"),
        }
    }
}
