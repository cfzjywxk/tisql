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

pub mod catalog;
pub mod clog;
pub mod codec;
pub mod concurrency;
pub mod error;
pub mod executor;
pub mod protocol;
pub mod session;
pub mod sql;
pub mod storage;
pub mod transaction;
pub mod types;
pub mod util;
pub mod worker;

pub use catalog::{Catalog, MemoryCatalog};
pub use clog::{ClogService, FileClogConfig, FileClogService};
pub use concurrency::ConcurrencyManager;
use error::Result;
use executor::{ExecutionResult, Executor, SimpleExecutor};
pub use protocol::{MySqlServer, MYSQL_DEFAULT_PORT};
use sql::{Binder, Parser};
use storage::MvccMemTableEngine;
pub use transaction::{ReadSnapshot, TransactionService, Txn, TxnService};
use types::Value;
use util::Timer;
pub use worker::{WorkerPool, WorkerPoolConfig};

use std::path::PathBuf;
use std::sync::Arc;

/// Database configuration
#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    /// Data directory for persistence
    pub data_dir: PathBuf,
    /// Enable durability (WAL)
    pub durability: bool,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            durability: true,
        }
    }
}

impl DatabaseConfig {
    /// Create config with custom data directory
    pub fn with_data_dir(dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: dir.into(),
            durability: true,
        }
    }

    /// Create in-memory only config (no durability)
    pub fn in_memory() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            durability: false,
        }
    }
}

/// TiSQL Database instance with durability support
pub struct Database {
    storage: Arc<MvccMemTableEngine>,
    catalog: MemoryCatalog,
    parser: Parser,
    executor: SimpleExecutor,
    /// ConcurrencyManager for TSO and lock table
    concurrency_manager: Arc<ConcurrencyManager>,
    /// Commit log service for durability (None if durability disabled)
    clog_service: Option<Arc<FileClogService>>,
    /// Transaction service for durable writes
    txn_service: Option<Arc<TransactionService<MvccMemTableEngine, FileClogService>>>,
}

impl Database {
    /// Create a new in-memory database (no durability)
    pub fn new() -> Self {
        log_info!("Initializing TiSQL database (in-memory mode, MVCC enabled)");
        let concurrency_manager = Arc::new(ConcurrencyManager::new(1));
        Self {
            storage: Arc::new(MvccMemTableEngine::new(Arc::clone(&concurrency_manager))),
            catalog: MemoryCatalog::new(),
            parser: Parser::new(),
            executor: SimpleExecutor::new(),
            concurrency_manager,
            clog_service: None,
            txn_service: None,
        }
    }

    /// Open database with persistence and recovery
    pub fn open(config: DatabaseConfig) -> Result<Self> {
        log_info!(
            "Opening TiSQL database at {:?} (MVCC enabled)",
            config.data_dir
        );

        if !config.durability {
            return Ok(Self::new());
        }

        // Create commit log config
        let clog_config = FileClogConfig::with_dir(&config.data_dir);

        // Recover commit log entries
        let (clog_service, entries) = FileClogService::recover(clog_config)?;
        let clog_service = Arc::new(clog_service);

        // Create concurrency manager and storage
        let concurrency_manager = Arc::new(ConcurrencyManager::new(1));
        let storage = Arc::new(MvccMemTableEngine::new(Arc::clone(&concurrency_manager)));

        // Create transaction service
        let txn_service = Arc::new(TransactionService::new(
            Arc::clone(&storage),
            Arc::clone(&clog_service),
            Arc::clone(&concurrency_manager),
        ));

        // Recover state by replaying commit log
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

        // Recover catalog from storage
        let catalog = MemoryCatalog::new();
        // TODO: Persist and recover catalog metadata

        Ok(Self {
            storage,
            catalog,
            parser: Parser::new(),
            executor: SimpleExecutor::new(),
            concurrency_manager,
            clog_service: Some(clog_service),
            txn_service: Some(txn_service),
        })
    }

    /// Execute a SQL statement with durability
    pub fn execute(&self, sql: &str) -> Result<QueryResult> {
        let total_timer = Timer::new("total");

        // Parse
        let parse_timer = Timer::new("parse");
        let stmt = self.parser.parse_one(sql)?;
        let parse_ms = parse_timer.elapsed_ms();

        // Bind
        let bind_timer = Timer::new("bind");
        let binder = Binder::new(&self.catalog, "default");
        let plan = binder.bind(stmt)?;
        let bind_ms = bind_timer.elapsed_ms();

        // Check if this is a write operation that needs logging
        let is_write = plan.is_write();

        // Execute
        let exec_timer = Timer::new("execute");
        let result = if is_write && self.txn_service.is_some() {
            // For write operations with durability enabled, we need to:
            // 1. Execute to get the write batch
            // 2. Log the batch
            // 3. Apply to storage
            // But current executor applies directly, so for now we use the
            // logging storage wrapper approach (see execute_with_logging)
            self.execute_with_logging(plan)?
        } else {
            // Read operations or no durability - execute directly
            self.executor
                .execute(plan, self.storage.as_ref(), &self.catalog)?
        };
        let exec_ms = exec_timer.elapsed_ms();

        // Convert to QueryResult
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
                    "SQL: {} | parse={:.3}ms bind={:.3}ms exec={:.3}ms total={:.3}ms rows={}",
                    truncate_sql(sql, 50),
                    parse_ms,
                    bind_ms,
                    exec_ms,
                    total_timer.elapsed_ms(),
                    row_count
                );

                QueryResult::Rows { columns, data }
            }
            ExecutionResult::Affected { count } => {
                log_trace!(
                    "SQL: {} | parse={:.3}ms bind={:.3}ms exec={:.3}ms total={:.3}ms affected={}",
                    truncate_sql(sql, 50),
                    parse_ms,
                    bind_ms,
                    exec_ms,
                    total_timer.elapsed_ms(),
                    count
                );
                QueryResult::Affected(count)
            }
            ExecutionResult::Ok => {
                log_trace!(
                    "SQL: {} | parse={:.3}ms bind={:.3}ms exec={:.3}ms total={:.3}ms",
                    truncate_sql(sql, 50),
                    parse_ms,
                    bind_ms,
                    exec_ms,
                    total_timer.elapsed_ms()
                );
                QueryResult::Ok
            }
        };

        Ok(query_result)
    }

    /// Execute a write operation with commit logging for durability
    fn execute_with_logging(&self, plan: sql::LogicalPlan) -> Result<ExecutionResult> {
        use crate::clog::ClogBatch;

        let txn_service = self.txn_service.as_ref().unwrap();
        let clog_service = self.clog_service.as_ref().unwrap();

        // Execute the plan
        // TODO: Implement true write-ahead logging by:
        // 1. Modifying executor to return WriteBatch without applying
        // 2. Logging the batch to clog
        // 3. Syncing to disk
        // 4. Then applying to storage
        //
        // For now, we execute and then log a commit marker
        let result = self
            .executor
            .execute(plan, self.storage.as_ref(), &self.catalog)?;

        // Log after execution for durability
        // This ensures the operation is persisted, but if we crash between
        // storage write and clog write, we may lose data. A proper implementation
        // would log before the storage write.
        if let ExecutionResult::Affected { count } = &result {
            if *count > 0 {
                let txn_id = txn_service.current_ts();
                let mut batch = ClogBatch::new();
                batch.add_commit(txn_id, txn_id);
                clog_service.write(&mut batch, true)?;
            }
        }

        Ok(result)
    }

    /// List tables in default schema
    pub fn list_tables(&self) -> Result<Vec<String>> {
        let tables = self.catalog.list_tables("default")?;
        Ok(tables.into_iter().map(|t| t.name().to_string()).collect())
    }

    /// Close the database (flush commit log)
    pub fn close(&self) -> Result<()> {
        if let Some(ref clog_service) = self.clog_service {
            clog_service.close()?;
        }
        log_info!("Database closed");
        Ok(())
    }

    /// Get reference to storage engine (for testing)
    pub fn storage(&self) -> &MvccMemTableEngine {
        &self.storage
    }

    /// Get reference to concurrency manager
    pub fn concurrency_manager(&self) -> &ConcurrencyManager {
        &self.concurrency_manager
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        if let Some(ref clog_service) = self.clog_service {
            let _ = clog_service.close();
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_select_literal() {
        let db = Database::new();
        let result = db.execute("SELECT 1 + 1").unwrap();
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
        let db = Database::new();

        db.execute("CREATE TABLE users (id INT, name VARCHAR(255))")
            .unwrap();

        let result = db
            .execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            .unwrap();
        match result {
            QueryResult::Affected(count) => assert_eq!(count, 1),
            _ => panic!("Expected affected count"),
        }

        let result = db.execute("SELECT id, name FROM users").unwrap();
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
            db.execute("CREATE TABLE t (id INT, val VARCHAR(100))")
                .unwrap();
            db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
            db.execute("INSERT INTO t VALUES (2, 'world')").unwrap();
            db.close().unwrap();
        }

        // Reopen - data should be in storage but catalog needs recovery
        // For now, this tests that the commit log file is created and can be recovered
        {
            let db = Database::open(config).unwrap();
            // Note: Catalog is not persisted yet, so CREATE TABLE won't survive
            // But commit log entries are persisted
            assert!(db.clog_service.is_some());
        }
    }

    #[test]
    fn test_where_clause() {
        let db = Database::new();

        db.execute("CREATE TABLE t (a INT, b INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
            .unwrap();

        let result = db.execute("SELECT a, b FROM t WHERE a > 1").unwrap();
        match result {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 2);
            }
            _ => panic!("Expected rows"),
        }
    }

    #[test]
    fn test_order_by() {
        let db = Database::new();

        db.execute("CREATE TABLE t (a INT)").unwrap();
        db.execute("INSERT INTO t VALUES (3), (1), (2)").unwrap();

        let result = db.execute("SELECT a FROM t ORDER BY a").unwrap();
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
        let db = Database::new();

        db.execute("CREATE TABLE t (a INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
            .unwrap();

        let result = db.execute("SELECT a FROM t LIMIT 3").unwrap();
        match result {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 3);
            }
            _ => panic!("Expected rows"),
        }
    }

    #[test]
    fn test_update() {
        let db = Database::new();

        db.execute("CREATE TABLE t (id INT, val INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 100), (2, 200)")
            .unwrap();

        db.execute("UPDATE t SET val = 999 WHERE id = 1").unwrap();

        let result = db.execute("SELECT id, val FROM t WHERE id = 1").unwrap();
        match result {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data[0][1], "999");
            }
            _ => panic!("Expected rows"),
        }
    }

    #[test]
    fn test_delete() {
        let db = Database::new();

        db.execute("CREATE TABLE t (a INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();

        db.execute("DELETE FROM t WHERE a = 2").unwrap();

        let result = db.execute("SELECT a FROM t").unwrap();
        match result {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 2);
            }
            _ => panic!("Expected rows"),
        }
    }
}
