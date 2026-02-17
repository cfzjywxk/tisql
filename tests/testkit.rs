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

//! TestKit - Testing utilities for TiSQL
//!
//! Provides a session-like API for testing:
//! ```rust
//! let tk = TestKit::new();
//! tk.must_exec("CREATE TABLE t (id INT PRIMARY KEY)");
//! tk.must_exec("INSERT INTO t VALUES (1), (2)");
//! tk.must_query("SELECT * FROM t").check(rows!["1", "2"]);
//! ```

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tisql::{Database, DatabaseConfig, QueryResult};

const DEFAULT_QUERY_TIMEOUT_SECS: u64 = 15;
const QUERY_TIMEOUT_ENV: &str = "TISQL_TESTKIT_QUERY_TIMEOUT_SECS";

/// TestKit provides a convenient API for testing SQL execution
pub struct TestKit {
    db: Arc<Database>,
    query_timeout: Duration,
    // Keep temp dir alive - dropping it deletes the directory
    _temp_dir: TempDir,
}

impl TestKit {
    /// Create a new TestKit with a fresh database
    pub fn new() -> Self {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let config = DatabaseConfig::with_data_dir(temp_dir.path());
        let db = Database::open(config).expect("Failed to open database");

        Self {
            db: Arc::new(db),
            query_timeout: testkit_query_timeout(),
            _temp_dir: temp_dir,
        }
    }

    async fn execute_with_timeout(&self, sql: &str) -> Result<QueryResult, QueryExecutionError> {
        match tokio::time::timeout(self.query_timeout, self.db.execute_query(sql)).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(e)) => Err(QueryExecutionError::Sql(e.to_string())),
            Err(_) => Err(QueryExecutionError::Timeout(self.query_timeout)),
        }
    }

    /// Execute SQL and panic on error
    pub async fn must_exec(&self, sql: &str) -> ExecResult {
        match self.execute_with_timeout(sql).await {
            Ok(result) => ExecResult { result },
            Err(e) => panic!("SQL execution failed: {e}\nSQL: {sql}"),
        }
    }

    /// Execute SQL and expect an error
    pub async fn must_exec_err(&self, sql: &str) -> String {
        match self.execute_with_timeout(sql).await {
            Ok(_) => panic!("Expected error but got success\nSQL: {sql}"),
            Err(QueryExecutionError::Sql(e)) => e,
            Err(QueryExecutionError::Timeout(timeout)) => {
                panic!("Expected SQL error, but execution timed out after {timeout:?}\nSQL: {sql}",)
            }
        }
    }

    /// Execute SQL that should return rows
    pub async fn must_query(&self, sql: &str) -> QueryChecker {
        match self.execute_with_timeout(sql).await {
            Ok(QueryResult::Rows { columns, data }) => QueryChecker { columns, data },
            Ok(other) => panic!("Expected rows but got: {other:?}\nSQL: {sql}"),
            Err(e) => panic!("Query failed: {e}\nSQL: {sql}"),
        }
    }

    /// Execute SQL without checking result
    #[allow(dead_code)]
    pub async fn exec(&self, sql: &str) -> Result<QueryResult, String> {
        self.execute_with_timeout(sql)
            .await
            .map_err(|e| e.to_string())
    }

    /// Get the underlying database
    #[allow(dead_code)]
    pub fn db(&self) -> Arc<Database> {
        Arc::clone(&self.db)
    }
}

impl Default for TestKit {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
enum QueryExecutionError {
    Sql(String),
    Timeout(Duration),
}

impl std::fmt::Display for QueryExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryExecutionError::Sql(err) => write!(f, "{err}"),
            QueryExecutionError::Timeout(timeout) => {
                write!(f, "timed out after {timeout:?} waiting for query execution")
            }
        }
    }
}

fn testkit_query_timeout() -> Duration {
    let parsed = std::env::var(QUERY_TIMEOUT_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|secs| *secs > 0);

    Duration::from_secs(parsed.unwrap_or(DEFAULT_QUERY_TIMEOUT_SECS))
}

/// Result of an execution
pub struct ExecResult {
    result: QueryResult,
}

impl ExecResult {
    /// Check affected row count
    pub fn check_affected(&self, expected: u64) {
        match &self.result {
            QueryResult::Affected(count) => {
                assert_eq!(
                    *count, expected,
                    "Expected {expected} affected rows, got {count}"
                );
            }
            other => panic!("Expected affected count but got: {other:?}"),
        }
    }

    /// Check for OK result
    pub fn check_ok(&self) {
        match &self.result {
            QueryResult::Ok => {}
            other => panic!("Expected OK but got: {other:?}"),
        }
    }
}

/// Checker for query results
pub struct QueryChecker {
    columns: Vec<String>,
    data: Vec<Vec<String>>,
}

impl QueryChecker {
    /// Check that result matches expected rows
    pub fn check(&self, expected: Vec<Vec<&str>>) {
        assert_eq!(
            self.data.len(),
            expected.len(),
            "Row count mismatch: expected {}, got {}",
            expected.len(),
            self.data.len()
        );

        for (i, (actual_row, expected_row)) in self.data.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                actual_row.len(),
                expected_row.len(),
                "Column count mismatch in row {}: expected {}, got {}",
                i,
                expected_row.len(),
                actual_row.len()
            );

            for (j, (actual, expected)) in actual_row.iter().zip(expected_row.iter()).enumerate() {
                assert_eq!(
                    actual, *expected,
                    "Value mismatch at row {i}, column {j}: expected '{expected}', got '{actual}'"
                );
            }
        }
    }

    /// Check that result has expected number of rows
    pub fn check_row_count(&self, expected: usize) {
        assert_eq!(
            self.data.len(),
            expected,
            "Row count mismatch: expected {}, got {}",
            expected,
            self.data.len()
        );
    }

    /// Check column names
    pub fn check_columns(&self, expected: Vec<&str>) {
        let expected: Vec<String> = expected.into_iter().map(String::from).collect();
        assert_eq!(
            self.columns, expected,
            "Column mismatch: expected {:?}, got {:?}",
            expected, self.columns
        );
    }

    /// Get the data for further inspection
    #[allow(dead_code)]
    pub fn rows(&self) -> &Vec<Vec<String>> {
        &self.data
    }

    /// Get the columns for further inspection
    #[allow(dead_code)]
    pub fn columns(&self) -> &Vec<String> {
        &self.columns
    }
}

/// Helper macro to create expected rows
#[macro_export]
macro_rules! rows {
    () => { vec![] as Vec<Vec<&str>> };
    ($([$($val:expr),* $(,)?]),* $(,)?) => {
        vec![$(vec![$($val),*]),*]
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_testkit_basic() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(100))")
            .await
            .check_ok();
        tk.must_exec("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')")
            .await
            .check_affected(2);

        tk.must_query("SELECT id, name FROM t ORDER BY id")
            .await
            .check(rows![["1", "Alice"], ["2", "Bob"]]);

        tk.must_exec("DROP TABLE t").await.check_ok();
    }

    #[tokio::test]
    async fn test_testkit_query_checker() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
            .await;
        tk.must_exec("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
            .await;

        let result = tk.must_query("SELECT a, b FROM t ORDER BY a").await;
        result.check_row_count(3);
        result.check_columns(vec!["a", "b"]);
        result.check(rows![["1", "10"], ["2", "20"], ["3", "30"]]);
    }

    #[tokio::test]
    async fn test_testkit_error() {
        let tk = TestKit::new();

        let err = tk.must_exec_err("SELECT * FROM non_existent_table").await;
        assert!(err.contains("not found") || err.contains("Table"));
    }
}
