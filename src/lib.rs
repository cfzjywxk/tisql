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
pub use protocol::{MySqlServer, MYSQL_DEFAULT_PORT};
pub use session::{Priority, QueryCtx, Session, SessionVars};
pub use storage::StorageEngine;
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
    pub use crate::clog::{FileClogConfig, FileClogService};

    // Arena (from util - it's a general-purpose allocator)
    pub use crate::util::{ArenaConfig, PageArena, DEFAULT_PAGE_SIZE};

    // Storage implementation
    pub use crate::storage::{ArenaMemTableEngine, MemoryStats};

    // ArenaSkipList (exposed for testing low-level skiplist behavior)
    pub use crate::storage::skiplist::ArenaSkipList;

    pub use crate::transaction::{ConcurrencyManager, Lock, TransactionService};
    pub use crate::tso::LocalTso;
}

// Internal imports (not re-exported)
use catalog::MvccCatalog;
use clog::{FileClogConfig, FileClogService};
use error::Result;
use executor::{ExecutionResult, Executor, SimpleExecutor};
use sql::{Binder, Parser};
use storage::ArenaMemTableEngine;
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
}

// ============================================================================
// Database - Main Entry Point
// ============================================================================

/// Internal type alias for concrete storage engine.
type DbStorage = ArenaMemTableEngine;

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
}

impl Database {
    /// Open database with persistence and recovery.
    ///
    /// This is the only way to create a Database instance.
    /// All databases use file-based persistence.
    pub fn open(config: DatabaseConfig) -> Result<Self> {
        log_info!("Opening TiSQL database at {:?}", config.data_dir);

        // Create commit log config and recover
        let clog_config = FileClogConfig::with_dir(&config.data_dir);
        let (clog_service, entries) = FileClogService::recover(clog_config)?;
        let clog_service = Arc::new(clog_service);

        // Create TSO service for timestamp allocation
        let tso = Arc::new(LocalTso::new(1));

        // Create concurrency manager (for locks and max_ts)
        let concurrency_manager = Arc::new(ConcurrencyManager::new(0));

        // Create storage engine (pure key-value store, no transaction logic)
        let storage = Arc::new(ArenaMemTableEngine::new());

        // Create transaction service
        let txn_service = Arc::new(TransactionService::new(
            Arc::clone(&storage),
            Arc::clone(&clog_service),
            Arc::clone(&tso),
            Arc::clone(&concurrency_manager),
        ));

        // Recover state by replaying commit log
        // This also recovers catalog metadata (stored with 'm' prefix keys)
        if !entries.is_empty() {
            let stats = txn_service.recover(&entries)?;
            log_info!(
                "Recovery complete: {} txns, {} puts, {} deletes applied, {} entries rolled back",
                stats.committed_txns,
                stats.applied_puts,
                stats.applied_deletes,
                stats.rolled_back_entries
            );
        }

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

    /// Handle MySQL protocol query text with a Session.
    ///
    /// This is the preferred method - it creates a QueryCtx from the Session
    /// before executing the statement.
    pub fn handle_mp_query_with_session(
        &self,
        sql: &str,
        session: &session::Session,
    ) -> Result<QueryResult> {
        let query_ctx = session.new_query_ctx();
        self.handle_mp_query_with_ctx(sql, &query_ctx)
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

    /// List tables in default schema.
    pub fn list_tables(&self) -> Result<Vec<String>> {
        let tables = self.catalog.list_tables("default")?;
        Ok(tables.into_iter().map(|t| t.name().to_string()).collect())
    }

    /// Close the database (flush commit log).
    pub fn close(&self) -> Result<()> {
        self.txn_service.clog_service().close()?;
        log_info!("Database closed");
        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let _ = self.txn_service.clog_service().close();
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

        db.handle_mp_query("CREATE TABLE users (id INT, name VARCHAR(255))")
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
            db.handle_mp_query("CREATE TABLE t (id INT, val VARCHAR(100))")
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

        db.handle_mp_query("CREATE TABLE t (a INT, b INT)").unwrap();
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

        db.handle_mp_query("CREATE TABLE t (a INT)").unwrap();
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

        db.handle_mp_query("CREATE TABLE t (a INT)").unwrap();
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

        db.handle_mp_query("CREATE TABLE t (id INT, val INT)")
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

        db.handle_mp_query("CREATE TABLE t (a INT)").unwrap();
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
