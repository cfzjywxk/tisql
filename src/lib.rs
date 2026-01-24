pub mod catalog;
pub mod codec;
pub mod error;
pub mod executor;
pub mod log;
pub mod protocol;
pub mod session;
pub mod sql;
pub mod storage;
pub mod transaction;
pub mod types;
pub mod util;
pub mod worker;

pub use catalog::{Catalog, MemoryCatalog};
use error::Result;
use executor::{ExecutionResult, Executor, SimpleExecutor};
pub use protocol::{MySqlServer, MYSQL_DEFAULT_PORT};
use sql::{Binder, Parser};
use storage::MemTableEngine;
use types::Value;
use util::Timer;
pub use worker::{WorkerPool, WorkerPoolConfig};

/// TiSQL Database instance
pub struct Database {
    storage: MemTableEngine,
    catalog: MemoryCatalog,
    parser: Parser,
    executor: SimpleExecutor,
}

impl Database {
    pub fn new() -> Self {
        log_info!("Initializing TiSQL database");
        Self {
            storage: MemTableEngine::new(),
            catalog: MemoryCatalog::new(),
            parser: Parser::new(),
            executor: SimpleExecutor::new(),
        }
    }

    /// Execute a SQL statement
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

        // Execute
        let exec_timer = Timer::new("execute");
        let result = self.executor.execute(plan, &self.storage, &self.catalog)?;
        let exec_ms = exec_timer.elapsed_ms();

        // Convert to QueryResult
        let query_result = match result {
            ExecutionResult::Rows { schema, rows } => {
                let row_count = rows.len();
                let columns: Vec<String> = schema.columns().iter().map(|c| c.name().to_string()).collect();
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

    /// List tables in default schema
    pub fn list_tables(&self) -> Result<Vec<String>> {
        let tables = self.catalog.list_tables("default")?;
        Ok(tables.into_iter().map(|t| t.name().to_string()).collect())
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
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
                let sep: String = widths.iter().map(|w| "-".repeat(*w + 2)).collect::<Vec<_>>().join("+");
                output.push_str(&format!("+{}+\n", sep));

                // Header
                let header: String = columns
                    .iter()
                    .zip(&widths)
                    .map(|(c, w)| format!(" {:width$} ", c, width = *w))
                    .collect::<Vec<_>>()
                    .join("|");
                output.push_str(&format!("|{}|\n", header));
                output.push_str(&format!("+{}+\n", sep));

                // Data rows
                for row in data {
                    let row_str: String = row
                        .iter()
                        .zip(&widths)
                        .map(|(v, w)| format!(" {:width$} ", v, width = *w))
                        .collect::<Vec<_>>()
                        .join("|");
                    output.push_str(&format!("|{}|\n", row_str));
                }
                output.push_str(&format!("+{}+\n", sep));

                output.push_str(&format!("{} row(s) in set", data.len()));
                output
            }
            QueryResult::Affected(count) => {
                format!("Query OK, {} row(s) affected", count)
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
        Value::Bytes(v) => format!("{:?}", v),
        Value::Date(v) => format!("DATE({})", v),
        Value::Time(v) => format!("TIME({})", v),
        Value::DateTime(v) => format!("DATETIME({})", v),
        Value::Timestamp(v) => format!("TIMESTAMP({})", v),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        db.execute("CREATE TABLE users (id INT, name VARCHAR(255))").unwrap();

        let result = db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
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
    fn test_where_clause() {
        let db = Database::new();

        db.execute("CREATE TABLE t (a INT, b INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();

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
        db.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)").unwrap();

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
        db.execute("INSERT INTO t VALUES (1, 100), (2, 200)").unwrap();

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
