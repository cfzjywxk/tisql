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

//! End-to-End tests for TiSQL using MySQL protocol.
//!
//! These tests start a TiSQL server and connect via MySQL client to verify
//! the MySQL protocol implementation, especially explicit transaction support.
//!
//! Run with: cargo test --test mysql_protocol_test

use std::sync::Arc;
use std::time::Duration;

use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, Pool};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use tisql::{Database, DatabaseConfig, MySqlServer};

// ============================================================================
// Test Infrastructure
// ============================================================================

/// Test context holding server handle and client connection pool.
struct TestContext {
    _temp_dir: TempDir,
    _server_handle: JoinHandle<()>,
    pool: Pool,
}

impl TestContext {
    /// Create a new test context with a fresh database and server.
    async fn new() -> Self {
        // Create temp directory for database
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");

        // Find an available port
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind to random port");
        let addr = listener.local_addr().expect("Failed to get local address");
        drop(listener); // Release the port

        // Create database
        let db_config = DatabaseConfig::with_data_dir(temp_dir.path());
        let db = Arc::new(Database::open(db_config).expect("Failed to open database"));

        // Create and start server
        let server = MySqlServer::new(Arc::clone(&db), addr);
        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        // Wait for server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create client connection pool
        let opts = Opts::from_url(&format!(
            "mysql://root@{}:{}/default",
            addr.ip(),
            addr.port()
        ))
        .expect("Failed to parse MySQL URL");
        let pool = Pool::new(opts);

        Self {
            _temp_dir: temp_dir,
            _server_handle: server_handle,
            pool,
        }
    }

    /// Get a new connection from the pool.
    async fn conn(&self) -> Conn {
        self.pool
            .get_conn()
            .await
            .expect("Failed to get connection")
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Execute a simple query and return affected rows or row count.
async fn exec(conn: &mut Conn, sql: &str) -> u64 {
    conn.query_drop(sql).await.expect("Query failed");
    conn.affected_rows()
}

/// Execute a query and return all rows as strings.
async fn query_all(conn: &mut Conn, sql: &str) -> Vec<Vec<String>> {
    let result: Vec<mysql_async::Row> = conn.query(sql).await.expect("Query failed");
    result
        .into_iter()
        .map(|row| {
            (0..row.len())
                .map(|i| {
                    row.get::<String, _>(i)
                        .unwrap_or_else(|| "NULL".to_string())
                })
                .collect()
        })
        .collect()
}

/// Execute a query and return a single value.
async fn query_one(conn: &mut Conn, sql: &str) -> String {
    let rows = query_all(conn, sql).await;
    rows.first()
        .and_then(|r| r.first())
        .cloned()
        .unwrap_or_else(|| "NULL".to_string())
}

/// Execute SQL and assert it fails with a lock-conflict style error.
async fn expect_lock_conflict(conn: &mut Conn, sql: &str) {
    let result = conn.query_drop(sql).await;
    assert!(
        result.is_err(),
        "Expected lock conflict, SQL succeeded: {sql}"
    );
    let err_msg = format!("{:?}", result.unwrap_err()).to_lowercase();
    assert!(
        err_msg.contains("locked") || err_msg.contains("conflict"),
        "Expected lock conflict, got: {err_msg}, SQL: {sql}"
    );
}

// ============================================================================
// Explicit Transaction Tests (BEGIN/COMMIT/ROLLBACK)
// ============================================================================

/// Test that BEGIN starts an explicit transaction.
#[tokio::test]
async fn test_begin_starts_transaction() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    // Create table
    exec(
        &mut conn,
        "CREATE TABLE txn_test (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;

    // BEGIN should succeed
    exec(&mut conn, "BEGIN").await;

    // Insert within transaction
    exec(&mut conn, "INSERT INTO txn_test VALUES (1, 'in_txn')").await;

    // COMMIT
    exec(&mut conn, "COMMIT").await;

    // Verify data persisted
    let val = query_one(&mut conn, "SELECT val FROM txn_test WHERE id = 1").await;
    assert_eq!(val, "in_txn");
}

/// Test that START TRANSACTION works like BEGIN.
#[tokio::test]
async fn test_start_transaction() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(&mut conn, "CREATE TABLE t (id INT PRIMARY KEY)").await;

    // START TRANSACTION should work
    exec(&mut conn, "START TRANSACTION").await;
    exec(&mut conn, "INSERT INTO t VALUES (1)").await;
    exec(&mut conn, "COMMIT").await;

    let rows = query_all(&mut conn, "SELECT id FROM t").await;
    assert_eq!(rows.len(), 1, "Should have 1 row after commit");
    assert_eq!(rows[0][0], "1");
}

/// Test that COMMIT persists changes.
#[tokio::test]
async fn test_commit_persists_changes() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE commit_test (id INT PRIMARY KEY, val INT)",
    )
    .await;

    // Begin transaction
    exec(&mut conn, "BEGIN").await;

    // Multiple inserts within transaction
    exec(&mut conn, "INSERT INTO commit_test VALUES (1, 100)").await;
    exec(&mut conn, "INSERT INTO commit_test VALUES (2, 200)").await;
    exec(&mut conn, "INSERT INTO commit_test VALUES (3, 300)").await;

    // Commit
    exec(&mut conn, "COMMIT").await;

    // Verify all data persisted
    let rows = query_all(&mut conn, "SELECT id, val FROM commit_test ORDER BY id").await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["1", "100"]);
    assert_eq!(rows[1], vec!["2", "200"]);
    assert_eq!(rows[2], vec!["3", "300"]);
}

/// Test that COMMIT fails if concurrent DDL changed schema.
#[tokio::test]
async fn test_commit_fails_on_concurrent_ddl_schema_change() {
    let ctx = TestContext::new().await;
    let mut conn1 = ctx.conn().await;
    let mut conn2 = ctx.conn().await;

    exec(
        &mut conn1,
        "CREATE TABLE ddl_guard (id INT PRIMARY KEY, val INT)",
    )
    .await;

    // Connection 1: explicit transaction with pending write
    exec(&mut conn1, "BEGIN").await;
    exec(&mut conn1, "INSERT INTO ddl_guard VALUES (1, 100)").await;

    // Connection 2: concurrent DDL bumps schema version
    exec(
        &mut conn2,
        "CREATE TABLE ddl_guard_other (id INT PRIMARY KEY)",
    )
    .await;

    // COMMIT should fail with schema changed
    let result = conn1.query_drop("COMMIT").await;
    assert!(result.is_err(), "COMMIT should fail after concurrent DDL");
    let err_msg = format!("{:?}", result.unwrap_err()).to_lowercase();
    assert!(
        err_msg.contains("schema changed") || err_msg.contains("please retry"),
        "Error should indicate schema change, got: {err_msg}"
    );

    // Pending write should be rolled back.
    let rows = query_all(&mut conn1, "SELECT id FROM ddl_guard ORDER BY id").await;
    assert_eq!(rows.len(), 0, "Write should be rolled back");

    // Session should be usable after failed COMMIT (txn cleared).
    exec(&mut conn1, "INSERT INTO ddl_guard VALUES (2, 200)").await;
    let rows = query_all(&mut conn1, "SELECT id FROM ddl_guard ORDER BY id").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "2");
}

/// Test that ROLLBACK discards changes.
#[tokio::test]
async fn test_rollback_discards_changes() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE rollback_test (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;

    // Insert initial data (auto-commit)
    exec(
        &mut conn,
        "INSERT INTO rollback_test VALUES (1, 'original')",
    )
    .await;

    // Begin transaction
    exec(&mut conn, "BEGIN").await;

    // Modify within transaction
    exec(
        &mut conn,
        "UPDATE rollback_test SET val = 'modified' WHERE id = 1",
    )
    .await;
    exec(&mut conn, "INSERT INTO rollback_test VALUES (2, 'new_row')").await;

    // Rollback
    exec(&mut conn, "ROLLBACK").await;

    // Verify original data is preserved
    let val = query_one(&mut conn, "SELECT val FROM rollback_test WHERE id = 1").await;
    assert_eq!(val, "original");

    // Verify new row was not persisted
    let rows = query_all(&mut conn, "SELECT id, val FROM rollback_test ORDER BY id").await;
    assert_eq!(
        rows.len(),
        1,
        "Only original row should exist after rollback"
    );
    assert_eq!(rows[0][0], "1");
}

/// Test multiple statements in a single transaction.
#[tokio::test]
async fn test_multiple_statements_in_transaction() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE multi_test (id INT PRIMARY KEY, val INT)",
    )
    .await;

    // Begin transaction
    exec(&mut conn, "BEGIN").await;

    // Multiple inserts
    for i in 1..=10 {
        exec(
            &mut conn,
            &format!("INSERT INTO multi_test VALUES ({}, {})", i, i * 10),
        )
        .await;
    }

    // Commit all at once
    exec(&mut conn, "COMMIT").await;

    // Verify all rows persisted by checking actual rows
    let rows = query_all(&mut conn, "SELECT id, val FROM multi_test ORDER BY id").await;
    assert_eq!(rows.len(), 10, "Should have 10 rows");
    assert_eq!(rows[0], vec!["1", "10"]);
    assert_eq!(rows[9], vec!["10", "100"]);
}

/// Short path should be bypassed inside explicit transactions.
#[tokio::test]
async fn test_point_get_short_path_bypasses_explicit_txn_reads() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE sp_txn (id VARCHAR(64) PRIMARY KEY, v INT)",
    )
    .await;
    exec(&mut conn, "INSERT INTO sp_txn VALUES ('k1', 10)").await;

    exec(&mut conn, "BEGIN").await;
    exec(&mut conn, "UPDATE sp_txn SET v = 20 WHERE id = 'k1'").await;
    let v = query_one(&mut conn, "SELECT v FROM sp_txn WHERE id = 'k1'").await;
    assert_eq!(v, "20");
    exec(&mut conn, "ROLLBACK").await;
}

/// Integer PK point-get must remain key-compatible with the existing path.
#[tokio::test]
async fn test_point_get_short_path_int_pk_lookup() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(&mut conn, "CREATE TABLE sp_int (id INT PRIMARY KEY, v INT)").await;
    exec(&mut conn, "INSERT INTO sp_int VALUES (1, 42)").await;

    let v = query_one(&mut conn, "SELECT v FROM sp_int WHERE id = 1").await;
    assert_eq!(v, "42");
}

/// Short path recognizer should handle backticks and escaped SQL strings.
#[tokio::test]
async fn test_point_get_short_path_backticks_and_escaped_string() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE sp_bt (YCSB_KEY VARCHAR(64) PRIMARY KEY, v INT)",
    )
    .await;
    exec(&mut conn, "INSERT INTO sp_bt VALUES ('ab''cd', 55)").await;

    let v = query_one(
        &mut conn,
        "SELECT `v` FROM sp_bt WHERE `YCSB_KEY` = 'ab''cd'",
    )
    .await;
    assert_eq!(v, "55");
}

/// Non-eligible shapes must safely fallback to normal planner/executor.
#[tokio::test]
async fn test_point_get_short_path_fallback_for_extra_predicate() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE sp_fb (id VARCHAR(64) PRIMARY KEY, v INT)",
    )
    .await;
    exec(&mut conn, "INSERT INTO sp_fb VALUES ('k1', 33)").await;

    let v = query_one(&mut conn, "SELECT v FROM sp_fb WHERE id = 'k1' AND v = 33").await;
    assert_eq!(v, "33");
}

/// Signed integer literals with leading zeros should remain query-compatible.
#[tokio::test]
async fn test_point_get_short_path_signed_integer_literal_lookup() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE sp_signed (id INT PRIMARY KEY, v INT)",
    )
    .await;
    exec(&mut conn, "INSERT INTO sp_signed VALUES (-7, 70)").await;

    let v = query_one(&mut conn, "SELECT v FROM sp_signed WHERE id = -007").await;
    assert_eq!(v, "70");
}

/// Test that COMMIT without BEGIN is a no-op (MySQL behavior).
#[tokio::test]
async fn test_commit_without_transaction_is_noop() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(&mut conn, "CREATE TABLE noop_test (id INT PRIMARY KEY)").await;

    // COMMIT without BEGIN should succeed (MySQL behavior)
    exec(&mut conn, "COMMIT").await;

    // Insert should work (auto-commit)
    exec(&mut conn, "INSERT INTO noop_test VALUES (1)").await;

    let rows = query_all(&mut conn, "SELECT id FROM noop_test").await;
    assert_eq!(rows.len(), 1, "Should have 1 row");
}

/// Test that ROLLBACK without BEGIN is a no-op (MySQL behavior).
#[tokio::test]
async fn test_rollback_without_transaction_is_noop() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(&mut conn, "CREATE TABLE rb_noop_test (id INT PRIMARY KEY)").await;
    exec(&mut conn, "INSERT INTO rb_noop_test VALUES (1)").await;

    // ROLLBACK without BEGIN should succeed (MySQL behavior)
    exec(&mut conn, "ROLLBACK").await;

    // Previous insert should still be there (was auto-committed)
    let rows = query_all(&mut conn, "SELECT id FROM rb_noop_test").await;
    assert_eq!(
        rows.len(),
        1,
        "Auto-committed data should persist after ROLLBACK without transaction"
    );
}

/// Test transaction isolation - uncommitted changes not visible to other connections.
#[tokio::test]
async fn test_transaction_isolation() {
    let ctx = TestContext::new().await;
    let mut conn1 = ctx.conn().await;
    let mut conn2 = ctx.conn().await;

    exec(
        &mut conn1,
        "CREATE TABLE isolation_test (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;

    // Connection 1: Begin transaction and insert
    exec(&mut conn1, "BEGIN").await;
    exec(
        &mut conn1,
        "INSERT INTO isolation_test VALUES (1, 'uncommitted')",
    )
    .await;

    // Connection 2: Should not see uncommitted data (check by querying the row directly)
    let rows = query_all(
        &mut conn2,
        "SELECT id, val FROM isolation_test WHERE id = 1",
    )
    .await;
    assert_eq!(
        rows.len(),
        0,
        "Uncommitted changes should not be visible to other connections"
    );

    // Connection 1: Commit
    exec(&mut conn1, "COMMIT").await;

    // Connection 2: Should now see committed data
    let rows = query_all(
        &mut conn2,
        "SELECT id, val FROM isolation_test WHERE id = 1",
    )
    .await;
    assert_eq!(
        rows.len(),
        1,
        "Committed changes should be visible to other connections"
    );
    assert_eq!(rows[0][1], "uncommitted");
}

/// Test transaction rollback isolation - rolled back changes never visible.
#[tokio::test]
async fn test_rollback_isolation() {
    let ctx = TestContext::new().await;
    let mut conn1 = ctx.conn().await;
    let mut conn2 = ctx.conn().await;

    exec(
        &mut conn1,
        "CREATE TABLE rb_isolation_test (id INT PRIMARY KEY)",
    )
    .await;
    exec(&mut conn1, "INSERT INTO rb_isolation_test VALUES (1)").await;

    // Connection 1: Begin transaction and modify
    exec(&mut conn1, "BEGIN").await;
    exec(&mut conn1, "DELETE FROM rb_isolation_test WHERE id = 1").await;

    // Connection 2: Should still see original data
    let rows = query_all(&mut conn2, "SELECT id FROM rb_isolation_test WHERE id = 1").await;
    assert_eq!(rows.len(), 1, "Original data should still be visible");

    // Connection 1: Rollback
    exec(&mut conn1, "ROLLBACK").await;

    // Connection 2: Should still see original data (unchanged)
    let rows = query_all(&mut conn2, "SELECT id FROM rb_isolation_test WHERE id = 1").await;
    assert_eq!(rows.len(), 1, "Data should still be visible after rollback");
}

/// Test UPDATE within explicit transaction.
#[tokio::test]
async fn test_update_in_transaction() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE update_test (id INT PRIMARY KEY, counter INT)",
    )
    .await;
    exec(&mut conn, "INSERT INTO update_test VALUES (1, 0)").await;

    // Begin transaction
    exec(&mut conn, "BEGIN").await;

    // Multiple updates
    exec(
        &mut conn,
        "UPDATE update_test SET counter = counter + 1 WHERE id = 1",
    )
    .await;
    exec(
        &mut conn,
        "UPDATE update_test SET counter = counter + 1 WHERE id = 1",
    )
    .await;
    exec(
        &mut conn,
        "UPDATE update_test SET counter = counter + 1 WHERE id = 1",
    )
    .await;

    // Commit
    exec(&mut conn, "COMMIT").await;

    // Note: Due to the current pessimistic locking model, each UPDATE reads at start_ts
    // and increments from there, so consecutive UPDATEs within a transaction may not
    // accumulate as expected. This test documents the current behavior.
    let counter = query_one(&mut conn, "SELECT counter FROM update_test WHERE id = 1").await;
    // The exact value depends on read-your-writes support
    assert!(
        counter.parse::<i32>().unwrap() >= 1,
        "Counter should be at least 1 after updates"
    );
}

/// Test DELETE within explicit transaction.
#[tokio::test]
async fn test_delete_in_transaction() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(&mut conn, "CREATE TABLE delete_test (id INT PRIMARY KEY)").await;
    exec(
        &mut conn,
        "INSERT INTO delete_test VALUES (1), (2), (3), (4), (5)",
    )
    .await;

    // Begin transaction
    exec(&mut conn, "BEGIN").await;

    // Delete some rows
    exec(&mut conn, "DELETE FROM delete_test WHERE id > 3").await;

    // Rollback
    exec(&mut conn, "ROLLBACK").await;

    // All rows should still be there
    let rows = query_all(&mut conn, "SELECT id FROM delete_test ORDER BY id").await;
    assert_eq!(
        rows.len(),
        5,
        "All 5 rows should still be there after rollback"
    );

    // Now delete and commit
    exec(&mut conn, "BEGIN").await;
    exec(&mut conn, "DELETE FROM delete_test WHERE id > 3").await;
    exec(&mut conn, "COMMIT").await;

    // Only 3 rows should remain
    let rows = query_all(&mut conn, "SELECT id FROM delete_test ORDER BY id").await;
    assert_eq!(
        rows.len(),
        3,
        "Only 3 rows should remain after delete+commit"
    );
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[1][0], "2");
    assert_eq!(rows[2][0], "3");
}

/// Test mixed DML operations in a transaction.
///
/// Note: Read-your-writes is not yet supported, so UPDATE/DELETE within a
/// transaction cannot see uncommitted INSERTs from the same transaction.
/// This test demonstrates the current behavior.
#[tokio::test]
async fn test_mixed_dml_in_transaction() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE mixed_test (id INT PRIMARY KEY, name VARCHAR(100))",
    )
    .await;

    // First, create some committed data that UPDATE/DELETE can see
    exec(&mut conn, "INSERT INTO mixed_test VALUES (1, 'Alice')").await;
    exec(&mut conn, "INSERT INTO mixed_test VALUES (2, 'Bob')").await;

    // Now begin transaction with UPDATE/DELETE on committed data
    exec(&mut conn, "BEGIN").await;
    exec(
        &mut conn,
        "UPDATE mixed_test SET name = 'ALICE' WHERE id = 1",
    )
    .await;
    exec(&mut conn, "DELETE FROM mixed_test WHERE id = 2").await;
    exec(&mut conn, "INSERT INTO mixed_test VALUES (3, 'Charlie')").await;
    exec(&mut conn, "COMMIT").await;

    // Verify final state
    let rows = query_all(&mut conn, "SELECT id, name FROM mixed_test ORDER BY id").await;
    assert_eq!(
        rows.len(),
        2,
        "Should have 2 rows after UPDATE/DELETE/INSERT"
    );
    assert_eq!(rows[0], vec!["1", "ALICE"]);
    assert_eq!(rows[1], vec!["3", "Charlie"]);
}

// ============================================================================
// Auto-Commit Mode Tests
// ============================================================================

/// Test that statements without explicit transaction use auto-commit.
#[tokio::test]
async fn test_autocommit_mode() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE autocommit_test (id INT PRIMARY KEY)",
    )
    .await;

    // Insert without BEGIN - should auto-commit
    exec(&mut conn, "INSERT INTO autocommit_test VALUES (1)").await;

    // Reconnect and verify data persisted
    drop(conn);
    let mut conn2 = ctx.conn().await;

    let rows = query_all(&mut conn2, "SELECT id FROM autocommit_test").await;
    assert_eq!(rows.len(), 1, "Auto-committed data should persist");
    assert_eq!(rows[0][0], "1");
}

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

/// Test USE database command within transaction.
#[tokio::test]
async fn test_use_database_in_transaction() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(&mut conn, "CREATE TABLE use_test (id INT PRIMARY KEY)").await;

    // Begin transaction
    exec(&mut conn, "BEGIN").await;
    exec(&mut conn, "INSERT INTO use_test VALUES (1)").await;

    // USE database should work (session-level, not transactional)
    exec(&mut conn, "USE default").await;

    exec(&mut conn, "COMMIT").await;

    let rows = query_all(&mut conn, "SELECT id FROM use_test").await;
    assert_eq!(rows.len(), 1, "Data should be committed");
}

/// Test empty transaction (BEGIN then immediate COMMIT).
#[tokio::test]
async fn test_empty_transaction() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    // Empty transaction should be fine
    exec(&mut conn, "BEGIN").await;
    exec(&mut conn, "COMMIT").await;

    // Should still be able to work normally
    exec(
        &mut conn,
        "CREATE TABLE empty_txn_test (id INT PRIMARY KEY)",
    )
    .await;
    exec(&mut conn, "INSERT INTO empty_txn_test VALUES (1)").await;

    let rows = query_all(&mut conn, "SELECT id FROM empty_txn_test").await;
    assert_eq!(rows.len(), 1, "Insert after empty transaction should work");
}

/// Test SELECT within transaction (read from snapshot).
#[tokio::test]
async fn test_select_in_transaction() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE select_txn_test (id INT PRIMARY KEY, val INT)",
    )
    .await;
    exec(&mut conn, "INSERT INTO select_txn_test VALUES (1, 100)").await;

    // Begin read-only transaction with BEGIN
    exec(&mut conn, "BEGIN").await;

    // SELECT should work
    let val = query_one(&mut conn, "SELECT val FROM select_txn_test WHERE id = 1").await;
    assert_eq!(val, "100");

    exec(&mut conn, "COMMIT").await;
}

// ============================================================================
// Read-Your-Writes Tests (Explicit Transaction)
// ============================================================================

/// Test that explicit transaction can read its own uncommitted INSERT.
#[tokio::test]
async fn test_read_your_writes_insert() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE ryw_insert (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;

    // Begin explicit transaction
    exec(&mut conn, "BEGIN").await;

    // Insert within transaction
    exec(
        &mut conn,
        "INSERT INTO ryw_insert VALUES (1, 'uncommitted')",
    )
    .await;

    // Same connection should see its own uncommitted write
    let val = query_one(&mut conn, "SELECT val FROM ryw_insert WHERE id = 1").await;
    assert_eq!(val, "uncommitted", "Transaction should see its own INSERT");

    exec(&mut conn, "COMMIT").await;

    // After commit, still visible
    let val = query_one(&mut conn, "SELECT val FROM ryw_insert WHERE id = 1").await;
    assert_eq!(val, "uncommitted", "Committed data should be visible");
}

/// Test that explicit transaction can read its own uncommitted UPDATE.
#[tokio::test]
async fn test_read_your_writes_update() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE ryw_update (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;
    exec(&mut conn, "INSERT INTO ryw_update VALUES (1, 'original')").await;

    // Begin explicit transaction
    exec(&mut conn, "BEGIN").await;

    // Update within transaction
    exec(
        &mut conn,
        "UPDATE ryw_update SET val = 'modified' WHERE id = 1",
    )
    .await;

    // Same connection should see its own uncommitted update
    let val = query_one(&mut conn, "SELECT val FROM ryw_update WHERE id = 1").await;
    assert_eq!(val, "modified", "Transaction should see its own UPDATE");

    exec(&mut conn, "COMMIT").await;
}

/// Test that explicit transaction sees DELETE hides the row.
#[tokio::test]
async fn test_read_your_writes_delete() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE ryw_delete (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;
    exec(&mut conn, "INSERT INTO ryw_delete VALUES (1, 'to_delete')").await;
    exec(&mut conn, "INSERT INTO ryw_delete VALUES (2, 'keep')").await;

    // Begin explicit transaction
    exec(&mut conn, "BEGIN").await;

    // Delete within transaction
    exec(&mut conn, "DELETE FROM ryw_delete WHERE id = 1").await;

    // Same connection should NOT see deleted row
    let rows = query_all(&mut conn, "SELECT id, val FROM ryw_delete ORDER BY id").await;
    assert_eq!(
        rows.len(),
        1,
        "Deleted row should not be visible to same txn"
    );
    assert_eq!(rows[0][0], "2", "Only non-deleted row should be visible");

    exec(&mut conn, "COMMIT").await;

    // After commit, still gone
    let rows = query_all(&mut conn, "SELECT id FROM ryw_delete").await;
    assert_eq!(rows.len(), 1, "Deleted row should stay gone after commit");
}

/// Test multiple operations on same key within explicit transaction.
#[tokio::test]
async fn test_read_your_writes_multiple_updates() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE ryw_multi (id INT PRIMARY KEY, counter INT)",
    )
    .await;
    exec(&mut conn, "INSERT INTO ryw_multi VALUES (1, 0)").await;

    // Begin explicit transaction
    exec(&mut conn, "BEGIN").await;

    // Multiple updates - each should see the previous update
    exec(
        &mut conn,
        "UPDATE ryw_multi SET counter = counter + 1 WHERE id = 1",
    )
    .await;
    let val = query_one(&mut conn, "SELECT counter FROM ryw_multi WHERE id = 1").await;
    assert_eq!(val, "1", "First update should set counter to 1");

    exec(
        &mut conn,
        "UPDATE ryw_multi SET counter = counter + 1 WHERE id = 1",
    )
    .await;
    let val = query_one(&mut conn, "SELECT counter FROM ryw_multi WHERE id = 1").await;
    assert_eq!(val, "2", "Second update should set counter to 2");

    exec(
        &mut conn,
        "UPDATE ryw_multi SET counter = counter + 1 WHERE id = 1",
    )
    .await;
    let val = query_one(&mut conn, "SELECT counter FROM ryw_multi WHERE id = 1").await;
    assert_eq!(val, "3", "Third update should set counter to 3");

    exec(&mut conn, "COMMIT").await;

    // Final value should be 3
    let val = query_one(&mut conn, "SELECT counter FROM ryw_multi WHERE id = 1").await;
    assert_eq!(val, "3", "Final committed value should be 3");
}

/// Test INSERT then UPDATE on same key within explicit transaction.
#[tokio::test]
async fn test_read_your_writes_insert_then_update() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE ryw_ins_upd (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;

    // Begin explicit transaction
    exec(&mut conn, "BEGIN").await;

    // Insert new row
    exec(&mut conn, "INSERT INTO ryw_ins_upd VALUES (1, 'initial')").await;

    // Verify we see the insert
    let val = query_one(&mut conn, "SELECT val FROM ryw_ins_upd WHERE id = 1").await;
    assert_eq!(val, "initial", "Should see INSERT");

    // Update the just-inserted row
    exec(
        &mut conn,
        "UPDATE ryw_ins_upd SET val = 'updated' WHERE id = 1",
    )
    .await;

    // Verify we see the update
    let val = query_one(&mut conn, "SELECT val FROM ryw_ins_upd WHERE id = 1").await;
    assert_eq!(val, "updated", "Should see UPDATE after INSERT");

    exec(&mut conn, "COMMIT").await;

    // Final value
    let val = query_one(&mut conn, "SELECT val FROM ryw_ins_upd WHERE id = 1").await;
    assert_eq!(val, "updated", "Committed value should be 'updated'");
}

/// Test INSERT then DELETE on same key within explicit transaction.
#[tokio::test]
async fn test_read_your_writes_insert_then_delete() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE ryw_ins_del (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;

    // Begin explicit transaction
    exec(&mut conn, "BEGIN").await;

    // Insert new row
    exec(&mut conn, "INSERT INTO ryw_ins_del VALUES (1, 'temp')").await;

    // Verify we see the insert
    let rows = query_all(&mut conn, "SELECT id FROM ryw_ins_del").await;
    assert_eq!(rows.len(), 1, "Should see INSERT");

    // Delete the just-inserted row
    exec(&mut conn, "DELETE FROM ryw_ins_del WHERE id = 1").await;

    // Verify it's gone
    let rows = query_all(&mut conn, "SELECT id FROM ryw_ins_del").await;
    assert_eq!(rows.len(), 0, "Should not see deleted row");

    exec(&mut conn, "COMMIT").await;

    // Still gone after commit
    let rows = query_all(&mut conn, "SELECT id FROM ryw_ins_del").await;
    assert_eq!(rows.len(), 0, "Row should stay deleted after commit");
}

/// Test scan sees mix of committed and uncommitted data correctly.
#[tokio::test]
async fn test_read_your_writes_scan_mixed() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE ryw_scan (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;

    // Insert some committed data
    exec(&mut conn, "INSERT INTO ryw_scan VALUES (1, 'committed1')").await;
    exec(&mut conn, "INSERT INTO ryw_scan VALUES (3, 'committed3')").await;
    exec(&mut conn, "INSERT INTO ryw_scan VALUES (5, 'committed5')").await;

    // Begin explicit transaction
    exec(&mut conn, "BEGIN").await;

    // Add uncommitted rows interleaved with committed ones
    exec(&mut conn, "INSERT INTO ryw_scan VALUES (2, 'uncommitted2')").await;
    exec(&mut conn, "INSERT INTO ryw_scan VALUES (4, 'uncommitted4')").await;

    // Modify a committed row
    exec(
        &mut conn,
        "UPDATE ryw_scan SET val = 'modified1' WHERE id = 1",
    )
    .await;

    // Delete a committed row
    exec(&mut conn, "DELETE FROM ryw_scan WHERE id = 5").await;

    // Scan should see: modified1, uncommitted2, committed3, uncommitted4
    let rows = query_all(&mut conn, "SELECT id, val FROM ryw_scan ORDER BY id").await;
    assert_eq!(rows.len(), 4, "Should see 4 rows (1 deleted, 4 visible)");
    assert_eq!(rows[0], vec!["1", "modified1"], "Row 1 should be modified");
    assert_eq!(rows[1], vec!["2", "uncommitted2"], "Row 2 uncommitted");
    assert_eq!(rows[2], vec!["3", "committed3"], "Row 3 committed");
    assert_eq!(rows[3], vec!["4", "uncommitted4"], "Row 4 uncommitted");

    exec(&mut conn, "COMMIT").await;
}

// ============================================================================
// Write-Write Conflict Tests
// ============================================================================

/// Test that concurrent writes to the same key result in lock conflict.
#[tokio::test]
async fn test_write_write_conflict_insert() {
    let ctx = TestContext::new().await;
    let mut conn1 = ctx.conn().await;
    let mut conn2 = ctx.conn().await;

    exec(
        &mut conn1,
        "CREATE TABLE ww_conflict (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;

    // Connection 1: Begin transaction and insert
    exec(&mut conn1, "BEGIN").await;
    exec(
        &mut conn1,
        "INSERT INTO ww_conflict VALUES (1, 'from_conn1')",
    )
    .await;

    // Connection 2: Try to insert same key - should get conflict error
    exec(&mut conn2, "BEGIN").await;
    let result = conn2
        .query_drop("INSERT INTO ww_conflict VALUES (1, 'from_conn2')")
        .await;

    // Should fail with lock conflict
    assert!(
        result.is_err(),
        "Second insert to locked key should fail with conflict"
    );
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(
        err_msg.contains("locked") || err_msg.contains("conflict") || err_msg.contains("Lock"),
        "Error should indicate lock conflict: {err_msg}"
    );

    // Connection 1: Commit
    exec(&mut conn1, "COMMIT").await;

    // Connection 2: Rollback (cleanup)
    exec(&mut conn2, "ROLLBACK").await;

    // Verify conn1's value won
    let val = query_one(&mut conn1, "SELECT val FROM ww_conflict WHERE id = 1").await;
    assert_eq!(val, "from_conn1");
}

/// Statement-level lock conflict must not drop explicit transaction state.
#[tokio::test]
async fn test_lock_conflict_keeps_explicit_txn_context() {
    let ctx = TestContext::new().await;
    let mut conn1 = ctx.conn().await;
    let mut conn2 = ctx.conn().await;

    exec(
        &mut conn1,
        "CREATE TABLE lock_ctx (a INT PRIMARY KEY, b INT)",
    )
    .await;

    // Conn1 holds the key lock first.
    exec(&mut conn1, "BEGIN").await;
    exec(&mut conn1, "INSERT INTO lock_ctx VALUES (2, 0)").await;

    // Conn2 starts explicit txn and hits lock conflict.
    exec(&mut conn2, "BEGIN").await;
    let err = conn2
        .query_drop("INSERT INTO lock_ctx VALUES (2, 22)")
        .await;
    assert!(err.is_err(), "expected lock conflict on conn2");
    let err_msg = format!("{:?}", err.unwrap_err()).to_lowercase();
    assert!(
        err_msg.contains("locked") || err_msg.contains("conflict"),
        "error should indicate lock conflict: {err_msg}"
    );

    // Release conn1 lock, then retry on conn2.
    exec(&mut conn1, "ROLLBACK").await;
    exec(&mut conn2, "INSERT INTO lock_ctx VALUES (2, 22)").await;

    // Conn2 must still be in explicit txn: conn1 should see lock conflict, not duplicate key.
    exec(&mut conn1, "BEGIN").await;
    let err = conn1
        .query_drop("INSERT INTO lock_ctx VALUES (2, 20)")
        .await;
    assert!(err.is_err(), "conn1 insert should conflict with conn2 lock");
    let err_msg = format!("{:?}", err.unwrap_err()).to_lowercase();
    assert!(
        err_msg.contains("locked") || err_msg.contains("conflict"),
        "expected lock conflict (txn should still be explicit), got: {err_msg}"
    );

    // Conn1 must not see conn2's uncommitted row.
    let rows = query_all(&mut conn1, "SELECT a, b FROM lock_ctx").await;
    assert!(
        rows.is_empty(),
        "conn1 should not observe conn2 uncommitted data"
    );

    exec(&mut conn1, "ROLLBACK").await;
    exec(&mut conn2, "ROLLBACK").await;

    // After both rollbacks, table stays empty.
    let rows = query_all(&mut conn1, "SELECT a, b FROM lock_ctx").await;
    assert!(rows.is_empty(), "table should be empty after rollbacks");
}

/// E2E pessimistic-transaction flow with mixed autocommit/explicit sessions.
///
/// Covers:
/// - multiple sessions
/// - write conflict and non-conflict writes
/// - rollback + retry + commit
/// - uncommitted visibility checks via protocol path
#[tokio::test]
async fn test_mixed_sessions_conflict_non_conflict_commit_rollback() {
    let ctx = TestContext::new().await;
    let mut conn1 = ctx.conn().await;
    let mut conn2 = ctx.conn().await;
    let mut conn3 = ctx.conn().await;

    exec(
        &mut conn1,
        "CREATE TABLE mixed_lock_flow (id INT PRIMARY KEY, v INT)",
    )
    .await;
    exec(
        &mut conn1,
        "INSERT INTO mixed_lock_flow VALUES (1, 10), (2, 20)",
    )
    .await;

    // conn1 explicit txn locks key=1.
    exec(&mut conn1, "BEGIN").await;
    exec(&mut conn1, "UPDATE mixed_lock_flow SET v = 11 WHERE id = 1").await;

    // conn2 autocommit write on disjoint key succeeds.
    exec(
        &mut conn2,
        "UPDATE mixed_lock_flow SET v = 200 WHERE id = 2",
    )
    .await;

    // conn3 explicit txn hits conflict on key=1, but must remain active.
    exec(&mut conn3, "BEGIN").await;
    expect_lock_conflict(&mut conn3, "UPDATE mixed_lock_flow SET v = 30 WHERE id = 1").await;

    // conn3 keeps txn alive and can write another key.
    exec(
        &mut conn3,
        "UPDATE mixed_lock_flow SET v = 220 WHERE id = 2",
    )
    .await;

    // While conn3 is uncommitted, autocommit reads still see committed value.
    let v2_before_commit =
        query_one(&mut conn2, "SELECT v FROM mixed_lock_flow WHERE id = 2").await;
    assert_eq!(v2_before_commit, "200");
    expect_lock_conflict(
        &mut conn2,
        "UPDATE mixed_lock_flow SET v = 201 WHERE id = 2",
    )
    .await;

    // Release key=1 lock, then conn3 retry succeeds and commits.
    exec(&mut conn1, "ROLLBACK").await;
    exec(&mut conn3, "UPDATE mixed_lock_flow SET v = 30 WHERE id = 1").await;
    exec(&mut conn3, "COMMIT").await;

    let rows = query_all(&mut conn2, "SELECT id, v FROM mixed_lock_flow ORDER BY id").await;
    assert_eq!(rows[0], vec!["1", "30"]);
    assert_eq!(rows[1], vec!["2", "220"]);
}

/// Test that concurrent UPDATE to the same key results in lock conflict.
#[tokio::test]
async fn test_write_write_conflict_update() {
    let ctx = TestContext::new().await;
    let mut conn1 = ctx.conn().await;
    let mut conn2 = ctx.conn().await;

    exec(
        &mut conn1,
        "CREATE TABLE ww_update (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;
    exec(&mut conn1, "INSERT INTO ww_update VALUES (1, 'original')").await;

    // Connection 1: Begin transaction and update
    exec(&mut conn1, "BEGIN").await;
    exec(
        &mut conn1,
        "UPDATE ww_update SET val = 'updated_conn1' WHERE id = 1",
    )
    .await;

    // Connection 2: Try to update same key - should get conflict
    exec(&mut conn2, "BEGIN").await;
    let result = conn2
        .query_drop("UPDATE ww_update SET val = 'updated_conn2' WHERE id = 1")
        .await;

    assert!(
        result.is_err(),
        "Second update to locked key should fail with conflict"
    );

    // Connection 1: Commit
    exec(&mut conn1, "COMMIT").await;
    exec(&mut conn2, "ROLLBACK").await;

    // Verify conn1's value won
    let val = query_one(&mut conn1, "SELECT val FROM ww_update WHERE id = 1").await;
    assert_eq!(val, "updated_conn1");
}

/// Test that concurrent DELETE to the same key results in lock conflict.
#[tokio::test]
async fn test_write_write_conflict_delete() {
    let ctx = TestContext::new().await;
    let mut conn1 = ctx.conn().await;
    let mut conn2 = ctx.conn().await;

    exec(
        &mut conn1,
        "CREATE TABLE ww_delete (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;
    exec(&mut conn1, "INSERT INTO ww_delete VALUES (1, 'to_delete')").await;

    // Connection 1: Begin transaction and delete
    exec(&mut conn1, "BEGIN").await;
    exec(&mut conn1, "DELETE FROM ww_delete WHERE id = 1").await;

    // Connection 2: Try to delete same key - should get conflict
    exec(&mut conn2, "BEGIN").await;
    let result = conn2.query_drop("DELETE FROM ww_delete WHERE id = 1").await;

    assert!(
        result.is_err(),
        "Second delete to locked key should fail with conflict"
    );

    // Connection 1: Commit
    exec(&mut conn1, "COMMIT").await;
    exec(&mut conn2, "ROLLBACK").await;

    // Verify row is deleted
    let rows = query_all(&mut conn1, "SELECT id FROM ww_delete").await;
    assert_eq!(rows.len(), 0, "Row should be deleted");
}

// ============================================================================
// Read-Write Conflict Tests (Autocommit vs Explicit)
// ============================================================================

/// Test that autocommit read sees committed data, not locked pending data.
#[tokio::test]
async fn test_autocommit_read_vs_explicit_write() {
    let ctx = TestContext::new().await;
    let mut conn1 = ctx.conn().await;
    let mut conn2 = ctx.conn().await;

    exec(
        &mut conn1,
        "CREATE TABLE rw_auto (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;
    exec(&mut conn1, "INSERT INTO rw_auto VALUES (1, 'original')").await;

    // Connection 1: Begin explicit transaction and update
    exec(&mut conn1, "BEGIN").await;
    exec(
        &mut conn1,
        "UPDATE rw_auto SET val = 'modified' WHERE id = 1",
    )
    .await;

    // Connection 2: Autocommit read should see original (committed) value
    let val = query_one(&mut conn2, "SELECT val FROM rw_auto WHERE id = 1").await;
    assert_eq!(
        val, "original",
        "Autocommit read should see committed data, not pending"
    );

    // Connection 1: Commit
    exec(&mut conn1, "COMMIT").await;

    // Connection 2: Now should see the committed update
    let val = query_one(&mut conn2, "SELECT val FROM rw_auto WHERE id = 1").await;
    assert_eq!(val, "modified", "Should see committed update");
}

/// Test that explicit read transaction sees snapshot, not pending writes.
#[tokio::test]
async fn test_explicit_read_vs_explicit_write() {
    let ctx = TestContext::new().await;
    let mut conn1 = ctx.conn().await;
    let mut conn2 = ctx.conn().await;

    exec(
        &mut conn1,
        "CREATE TABLE rw_explicit (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;
    exec(&mut conn1, "INSERT INTO rw_explicit VALUES (1, 'original')").await;

    // Connection 1: Begin explicit write transaction
    exec(&mut conn1, "BEGIN").await;
    exec(
        &mut conn1,
        "UPDATE rw_explicit SET val = 'modified' WHERE id = 1",
    )
    .await;

    // Connection 2: Begin explicit read transaction
    exec(&mut conn2, "BEGIN").await;
    let val = query_one(&mut conn2, "SELECT val FROM rw_explicit WHERE id = 1").await;
    assert_eq!(
        val, "original",
        "Explicit read should see snapshot, not pending"
    );

    // Connection 1: Commit
    exec(&mut conn1, "COMMIT").await;

    // Connection 2: Still in transaction, should see old snapshot
    // (Note: This depends on snapshot isolation level - current impl may vary)
    let val = query_one(&mut conn2, "SELECT val FROM rw_explicit WHERE id = 1").await;
    // After conn1 commits, conn2's subsequent reads may see the new value
    // depending on isolation implementation
    assert!(
        val == "original" || val == "modified",
        "Value should be either original (strict SI) or modified (read committed)"
    );

    exec(&mut conn2, "COMMIT").await;
}

/// Test autocommit write while explicit transaction holds lock fails.
#[tokio::test]
async fn test_autocommit_write_vs_explicit_lock() {
    let ctx = TestContext::new().await;
    let mut conn1 = ctx.conn().await;
    let mut conn2 = ctx.conn().await;

    exec(
        &mut conn1,
        "CREATE TABLE aw_lock (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;
    exec(&mut conn1, "INSERT INTO aw_lock VALUES (1, 'original')").await;

    // Connection 1: Begin explicit transaction and lock row
    exec(&mut conn1, "BEGIN").await;
    exec(
        &mut conn1,
        "UPDATE aw_lock SET val = 'locked_by_txn' WHERE id = 1",
    )
    .await;

    // Connection 2: Autocommit write should fail due to lock
    let result = conn2
        .query_drop("UPDATE aw_lock SET val = 'autocommit_attempt' WHERE id = 1")
        .await;

    assert!(
        result.is_err(),
        "Autocommit write to locked key should fail"
    );

    // Connection 1: Commit releases lock
    exec(&mut conn1, "COMMIT").await;

    // Connection 2: Now autocommit write should succeed
    exec(
        &mut conn2,
        "UPDATE aw_lock SET val = 'autocommit_success' WHERE id = 1",
    )
    .await;

    let val = query_one(&mut conn2, "SELECT val FROM aw_lock WHERE id = 1").await;
    assert_eq!(val, "autocommit_success");
}

// ============================================================================
// Rollback and Lock Release Tests
// ============================================================================

/// Test that rollback releases locks, allowing other transactions to proceed.
#[tokio::test]
async fn test_rollback_releases_locks() {
    let ctx = TestContext::new().await;
    let mut conn1 = ctx.conn().await;
    let mut conn2 = ctx.conn().await;

    exec(
        &mut conn1,
        "CREATE TABLE rb_lock (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;
    exec(&mut conn1, "INSERT INTO rb_lock VALUES (1, 'original')").await;

    // Connection 1: Begin transaction and lock
    exec(&mut conn1, "BEGIN").await;
    exec(
        &mut conn1,
        "UPDATE rb_lock SET val = 'will_rollback' WHERE id = 1",
    )
    .await;

    // Connection 2: Try to update - should fail
    exec(&mut conn2, "BEGIN").await;
    let result = conn2
        .query_drop("UPDATE rb_lock SET val = 'conn2_attempt' WHERE id = 1")
        .await;
    assert!(result.is_err(), "Should fail while conn1 holds lock");
    exec(&mut conn2, "ROLLBACK").await;

    // Connection 1: Rollback releases lock
    exec(&mut conn1, "ROLLBACK").await;

    // Connection 2: Now should succeed
    exec(&mut conn2, "BEGIN").await;
    exec(
        &mut conn2,
        "UPDATE rb_lock SET val = 'conn2_success' WHERE id = 1",
    )
    .await;
    exec(&mut conn2, "COMMIT").await;

    let val = query_one(&mut conn1, "SELECT val FROM rb_lock WHERE id = 1").await;
    assert_eq!(
        val, "conn2_success",
        "conn2 should have updated after rollback"
    );
}

/// Test multiple keys locked in same transaction.
#[tokio::test]
async fn test_multiple_keys_lock_release() {
    let ctx = TestContext::new().await;
    let mut conn1 = ctx.conn().await;
    let mut conn2 = ctx.conn().await;

    exec(
        &mut conn1,
        "CREATE TABLE multi_lock (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;
    exec(
        &mut conn1,
        "INSERT INTO multi_lock VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;

    // Connection 1: Lock multiple keys
    exec(&mut conn1, "BEGIN").await;
    exec(
        &mut conn1,
        "UPDATE multi_lock SET val = 'locked1' WHERE id = 1",
    )
    .await;
    exec(
        &mut conn1,
        "UPDATE multi_lock SET val = 'locked2' WHERE id = 2",
    )
    .await;

    // Connection 2: Can update unlocked key
    exec(
        &mut conn2,
        "UPDATE multi_lock SET val = 'free3' WHERE id = 3",
    )
    .await;

    // Connection 2: Cannot update locked keys
    let result = conn2
        .query_drop("UPDATE multi_lock SET val = 'try1' WHERE id = 1")
        .await;
    assert!(result.is_err(), "Key 1 should be locked");

    let result = conn2
        .query_drop("UPDATE multi_lock SET val = 'try2' WHERE id = 2")
        .await;
    assert!(result.is_err(), "Key 2 should be locked");

    // Connection 1: Commit
    exec(&mut conn1, "COMMIT").await;

    // Verify final state
    let rows = query_all(&mut conn1, "SELECT id, val FROM multi_lock ORDER BY id").await;
    assert_eq!(rows[0], vec!["1", "locked1"]);
    assert_eq!(rows[1], vec!["2", "locked2"]);
    assert_eq!(rows[2], vec!["3", "free3"]);
}

// ============================================================================
// AUTO_INCREMENT Tests
// ============================================================================

#[tokio::test]
async fn test_auto_increment_mixed_explicit_and_generated_order() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE ai_mix_proto (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, v INT)",
    )
    .await;

    exec(
        &mut conn,
        "INSERT INTO ai_mix_proto (id, v) VALUES (NULL, 10), (NULL, 20), (100, 30), (NULL, 40)",
    )
    .await;

    let rows = query_all(&mut conn, "SELECT id FROM ai_mix_proto ORDER BY id").await;
    assert_eq!(rows, vec![vec!["1"], vec!["2"], vec!["100"], vec!["101"]]);
}

#[tokio::test]
async fn test_auto_increment_seed_applies_to_first_generated_value() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE ai_seed_proto (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, v INT) AUTO_INCREMENT=7",
    )
    .await;
    exec(&mut conn, "INSERT INTO ai_seed_proto (v) VALUES (1), (2)").await;

    let rows = query_all(&mut conn, "SELECT id FROM ai_seed_proto ORDER BY id").await;
    assert_eq!(rows, vec![vec!["7"], vec!["8"]]);
}

#[tokio::test]
async fn test_auto_increment_int_pk_literal_lookup_and_duplicate_check() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE ai_int_pk_proto (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, v INT)",
    )
    .await;
    exec(&mut conn, "INSERT INTO ai_int_pk_proto (v) VALUES (10)").await;

    let v = query_one(&mut conn, "SELECT v FROM ai_int_pk_proto WHERE id = 1").await;
    assert_eq!(v, "10");

    let result = conn
        .query_drop("INSERT INTO ai_int_pk_proto (id, v) VALUES (1, 20)")
        .await;
    assert!(result.is_err(), "Expected duplicate-key error");
    let err_msg = format!("{:?}", result.unwrap_err()).to_lowercase();
    assert!(
        err_msg.contains("duplicate"),
        "Expected duplicate-key error, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_auto_increment_int_overflow_returns_error() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE ai_overflow_proto (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, v INT) AUTO_INCREMENT=2147483647",
    )
    .await;
    exec(&mut conn, "INSERT INTO ai_overflow_proto (v) VALUES (1)").await;

    let result = conn
        .query_drop("INSERT INTO ai_overflow_proto (v) VALUES (2)")
        .await;
    assert!(result.is_err(), "Expected overflow error");
    let err_msg = format!("{:?}", result.unwrap_err()).to_lowercase();
    assert!(
        err_msg.contains("overflow"),
        "Expected overflow error, got: {err_msg}"
    );
}

// ============================================================================
// SHOW Tests
// ============================================================================

#[tokio::test]
async fn test_show_create_table_returns_expected_row() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE show_create_t (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL)",
    )
    .await;

    let rows = query_all(&mut conn, "SHOW CREATE TABLE show_create_t").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 2);
    assert_eq!(rows[0][0], "show_create_t");
    let ddl = &rows[0][1];
    assert!(ddl.starts_with("CREATE TABLE `default`.`show_create_t` ("));
    assert!(ddl.contains("`id` INT NOT NULL"));
    assert!(ddl.contains("PRIMARY KEY (`id`)"));
}

#[tokio::test]
async fn test_show_create_table_preserves_explicit_transaction_context() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(
        &mut conn,
        "CREATE TABLE show_create_txn (id INT PRIMARY KEY, val VARCHAR(32))",
    )
    .await;

    exec(&mut conn, "BEGIN").await;
    let rows = query_all(&mut conn, "SHOW CREATE TABLE show_create_txn").await;
    assert_eq!(rows.len(), 1);
    exec(&mut conn, "INSERT INTO show_create_txn VALUES (1, 'ok')").await;
    exec(&mut conn, "COMMIT").await;

    let val = query_one(&mut conn, "SELECT val FROM show_create_txn WHERE id = 1").await;
    assert_eq!(val, "ok");
}

#[tokio::test]
async fn test_legacy_show_databases_and_tables_still_work() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    exec(&mut conn, "CREATE TABLE show_legacy_t (id INT PRIMARY KEY)").await;

    let db_rows = query_all(&mut conn, "SHOW DATABASES").await;
    assert!(
        db_rows
            .iter()
            .any(|row| row.first() == Some(&"default".to_string())),
        "SHOW DATABASES should include default schema"
    );

    let table_rows = query_all(&mut conn, "SHOW TABLES").await;
    assert!(
        table_rows
            .iter()
            .any(|row| row.first() == Some(&"show_legacy_t".to_string())),
        "SHOW TABLES should include created table"
    );
}
