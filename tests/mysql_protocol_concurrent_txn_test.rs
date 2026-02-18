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

//! Additional E2E tests for concurrent transaction scenarios.
//!
//! These tests specifically target transaction context and session bugs
//! that could lead to unexpected commits or dirty reads.

use std::sync::Arc;
use std::time::Duration;

use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, Pool};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use tisql::{Database, DatabaseConfig, MySqlServer};

/// Test context holding server handle and client connection pool.
struct TestContext {
    _temp_dir: TempDir,
    _server_handle: JoinHandle<()>,
    pool: Pool,
}

impl TestContext {
    /// Create a new test context with a fresh database and server.
    async fn new() -> Self {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind to random port");
        let addr = listener.local_addr().expect("Failed to get local address");
        drop(listener);

        let db_config = DatabaseConfig::with_data_dir(temp_dir.path());
        let db = Arc::new(Database::open(db_config).expect("Failed to open database"));

        let server = MySqlServer::new(Arc::clone(&db), addr);
        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

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

    async fn conn(&self) -> Conn {
        self.pool
            .get_conn()
            .await
            .expect("Failed to get connection")
    }
}

async fn exec(conn: &mut Conn, sql: &str) -> u64 {
    conn.query_drop(sql).await.expect("Query failed");
    conn.affected_rows()
}

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

async fn query_one(conn: &mut Conn, sql: &str) -> String {
    let rows = query_all(conn, sql).await;
    rows.first()
        .and_then(|r| r.first())
        .cloned()
        .unwrap_or_else(|| "NULL".to_string())
}

/// Test that verifies no dirty read can occur across concurrent connections.
#[tokio::test]
async fn test_concurrent_txn_no_dirty_read_complex() {
    let ctx = TestContext::new().await;
    let mut conn1 = ctx.conn().await;
    let mut conn2 = ctx.conn().await;
    let mut conn3 = ctx.conn().await;

    // Setup
    exec(
        &mut conn1,
        "CREATE TABLE complex_test (id INT PRIMARY KEY, val INT)",
    )
    .await;
    exec(
        &mut conn1,
        "INSERT INTO complex_test VALUES (1, 100), (2, 200), (3, 300)",
    )
    .await;

    // Conn1: Begin and modify all rows
    exec(&mut conn1, "BEGIN").await;
    exec(
        &mut conn1,
        "UPDATE complex_test SET val = val + 1 WHERE id = 1",
    )
    .await;
    exec(
        &mut conn1,
        "UPDATE complex_test SET val = val + 1 WHERE id = 2",
    )
    .await;
    exec(
        &mut conn1,
        "UPDATE complex_test SET val = val + 1 WHERE id = 3",
    )
    .await;

    // Conn2: Begin and read - should see original values
    exec(&mut conn2, "BEGIN").await;
    let rows = query_all(&mut conn2, "SELECT val FROM complex_test ORDER BY id").await;
    assert_eq!(rows[0][0], "100", "Conn2 should see original value");
    assert_eq!(rows[1][0], "200", "Conn2 should see original value");
    assert_eq!(rows[2][0], "300", "Conn2 should see original value");

    // Conn3: Autocommit read - should also see original
    let rows = query_all(&mut conn3, "SELECT val FROM complex_test ORDER BY id").await;
    assert_eq!(rows[0][0], "100", "Conn3 should see original value");

    // Conn1: Rollback
    exec(&mut conn1, "ROLLBACK").await;

    // Conn2: Still in transaction, should still see original values
    let rows = query_all(&mut conn2, "SELECT val FROM complex_test ORDER BY id").await;
    assert_eq!(
        rows[0][0], "100",
        "Conn2 should still see original after conn1 rollback"
    );

    // Conn2: Commit
    exec(&mut conn2, "COMMIT").await;

    // All connections should now see original values
    let rows = query_all(&mut conn1, "SELECT val FROM complex_test ORDER BY id").await;
    assert_eq!(rows[0][0], "100");
    assert_eq!(rows[1][0], "200");
    assert_eq!(rows[2][0], "300");
}

/// Test that transaction context is properly isolated per connection.
#[tokio::test]
async fn test_txn_context_not_leaked_between_connections() {
    let ctx = TestContext::new().await;
    let mut conn1 = ctx.conn().await;
    let mut conn2 = ctx.conn().await;

    // Setup
    exec(&mut conn1, "CREATE TABLE leak_test (id INT PRIMARY KEY)").await;

    // Conn1: Begin transaction
    exec(&mut conn1, "BEGIN").await;
    exec(&mut conn1, "INSERT INTO leak_test VALUES (1)").await;

    // Conn2: Should not see uncommitted data from conn1
    let rows = query_all(&mut conn2, "SELECT * FROM leak_test").await;
    assert!(
        rows.is_empty(),
        "Conn2 should not see conn1's uncommitted data"
    );

    // Conn2: Should be able to start its own transaction
    exec(&mut conn2, "BEGIN").await;
    exec(&mut conn2, "INSERT INTO leak_test VALUES (2)").await;

    // Conn1: Should not see conn2's uncommitted data
    let rows = query_all(&mut conn1, "SELECT * FROM leak_test WHERE id = 2").await;
    assert!(
        rows.is_empty(),
        "Conn1 should not see conn2's uncommitted data"
    );

    // Conn1: Verify it sees its own uncommitted data
    let rows = query_all(&mut conn1, "SELECT * FROM leak_test WHERE id = 1").await;
    assert_eq!(rows.len(), 1, "Conn1 should see its own uncommitted data");

    // Both commit
    exec(&mut conn1, "COMMIT").await;
    exec(&mut conn2, "COMMIT").await;

    // Both rows should now be visible
    let rows = query_all(&mut conn1, "SELECT * FROM leak_test ORDER BY id").await;
    assert_eq!(rows.len(), 2);
}

/// Test that session can be reused immediately after rollback.
#[tokio::test]
async fn test_rollback_then_immediate_reuse() {
    let ctx = TestContext::new().await;
    let mut conn = ctx.conn().await;

    // Setup
    exec(
        &mut conn,
        "CREATE TABLE reuse_test (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;

    // Insert initial data
    exec(&mut conn, "INSERT INTO reuse_test VALUES (1, 'initial')").await;

    // Begin transaction
    exec(&mut conn, "BEGIN").await;
    exec(
        &mut conn,
        "UPDATE reuse_test SET val = 'modified' WHERE id = 1",
    )
    .await;

    // Verify uncommitted change visible to self
    let val = query_one(&mut conn, "SELECT val FROM reuse_test WHERE id = 1").await;
    assert_eq!(val, "modified");

    // Rollback
    exec(&mut conn, "ROLLBACK").await;

    // Verify rollback effective
    let val = query_one(&mut conn, "SELECT val FROM reuse_test WHERE id = 1").await;
    assert_eq!(val, "initial");

    // Immediately reuse - autocommit insert
    exec(&mut conn, "INSERT INTO reuse_test VALUES (2, 'autocommit')").await;

    // Verify autocommit worked
    let rows = query_all(&mut conn, "SELECT * FROM reuse_test ORDER BY id").await;
    assert_eq!(rows.len(), 2);

    // Immediately start new transaction
    exec(&mut conn, "BEGIN").await;
    exec(&mut conn, "INSERT INTO reuse_test VALUES (3, 'explicit')").await;
    exec(&mut conn, "COMMIT").await;

    // Verify final state
    let rows = query_all(&mut conn, "SELECT * FROM reuse_test ORDER BY id").await;
    assert_eq!(rows.len(), 3);
}

/// Test that explicit transaction state is preserved across statement errors.
#[tokio::test]
async fn test_explicit_txn_state_preserved_across_errors() {
    let ctx = TestContext::new().await;
    let mut conn1 = ctx.conn().await;
    let mut conn2 = ctx.conn().await;

    // Setup
    exec(
        &mut conn1,
        "CREATE TABLE state_test (id INT PRIMARY KEY, val INT)",
    )
    .await;
    exec(
        &mut conn1,
        "INSERT INTO state_test VALUES (1, 100), (2, 200)",
    )
    .await;

    // Conn1: Begin and lock row 1
    exec(&mut conn1, "BEGIN").await;
    exec(&mut conn1, "UPDATE state_test SET val = 999 WHERE id = 1").await;

    // Conn2: Begin, try to modify locked row (should fail)
    exec(&mut conn2, "BEGIN").await;
    let result = conn2
        .query_drop("UPDATE state_test SET val = 888 WHERE id = 1")
        .await;
    assert!(result.is_err(), "Should get lock conflict");

    // CRITICAL: Conn2 should still be in explicit transaction
    // Try another operation that should succeed within the same transaction
    exec(&mut conn2, "UPDATE state_test SET val = 777 WHERE id = 2").await;

    // Verify the update worked
    let val = query_one(&mut conn2, "SELECT val FROM state_test WHERE id = 2").await;
    assert_eq!(val, "777");

    // Conn2: Insert new row
    exec(&mut conn2, "INSERT INTO state_test VALUES (3, 300)").await;

    // Conn1: Rollback releases lock
    exec(&mut conn1, "ROLLBACK").await;

    // Conn2: Can now commit all its work atomically
    exec(&mut conn2, "COMMIT").await;

    // Verify: id=1 should still be 100 (conn1 rolled back)
    // id=2 should be 777 (conn2 committed)
    // id=3 should be 300 (conn2 committed)
    let rows = query_all(&mut conn1, "SELECT id, val FROM state_test ORDER BY id").await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["1", "100"]);
    assert_eq!(rows[1], vec!["2", "777"]);
    assert_eq!(rows[2], vec!["3", "300"]);
}

/// Test that autocommit statements don't see uncommitted explicit txn data
/// even with complex interleaving.
#[tokio::test]
async fn test_autocommit_never_sees_uncommitted_explicit() {
    let ctx = TestContext::new().await;
    let mut explicit_conn = ctx.conn().await;
    let mut autocommit_conn = ctx.conn().await;

    // Setup
    exec(
        &mut explicit_conn,
        "CREATE TABLE isolation_check (id INT PRIMARY KEY, val VARCHAR(100))",
    )
    .await;
    exec(
        &mut explicit_conn,
        "INSERT INTO isolation_check VALUES (1, 'committed')",
    )
    .await;

    // Start explicit transaction
    exec(&mut explicit_conn, "BEGIN").await;

    // Multiple modifications in explicit transaction
    exec(
        &mut explicit_conn,
        "UPDATE isolation_check SET val = 'uncommitted1' WHERE id = 1",
    )
    .await;
    exec(
        &mut explicit_conn,
        "INSERT INTO isolation_check VALUES (2, 'uncommitted2')",
    )
    .await;

    // Autocommit reads should NEVER see uncommitted data
    for i in 0..5 {
        let val = query_one(
            &mut autocommit_conn,
            "SELECT val FROM isolation_check WHERE id = 1",
        )
        .await;
        assert_eq!(
            val, "committed",
            "Autocommit read #{i} should never see uncommitted data"
        );

        let rows = query_all(
            &mut autocommit_conn,
            "SELECT * FROM isolation_check WHERE id = 2",
        )
        .await;
        assert!(
            rows.is_empty(),
            "Autocommit read #{i} should not see uncommitted insert"
        );
    }

    // Rollback explicit transaction
    exec(&mut explicit_conn, "ROLLBACK").await;

    // Autocommit should still see original data
    let val = query_one(
        &mut autocommit_conn,
        "SELECT val FROM isolation_check WHERE id = 1",
    )
    .await;
    assert_eq!(val, "committed");

    // Row 2 should not exist
    let rows = query_all(
        &mut autocommit_conn,
        "SELECT * FROM isolation_check WHERE id = 2",
    )
    .await;
    assert!(rows.is_empty());
}

/// Test concurrent transactions with multiple operations to ensure
/// no transaction context leakage between operations.
#[tokio::test]
async fn test_concurrent_txn_context_integrity() {
    let ctx = TestContext::new().await;
    let mut conn1 = ctx.conn().await;
    let mut conn2 = ctx.conn().await;
    let mut conn3 = ctx.conn().await;

    // Setup
    exec(
        &mut conn1,
        "CREATE TABLE integrity_test (id INT PRIMARY KEY, val INT, marker VARCHAR(10))",
    )
    .await;
    exec(
        &mut conn1,
        "INSERT INTO integrity_test VALUES (1, 0, 'initial')",
    )
    .await;
    exec(
        &mut conn1,
        "INSERT INTO integrity_test VALUES (2, 0, 'initial')",
    )
    .await;

    // Each connection starts its own transaction
    exec(&mut conn1, "BEGIN").await;
    exec(&mut conn2, "BEGIN").await;
    exec(&mut conn3, "BEGIN").await;

    // Each updates different row
    exec(
        &mut conn1,
        "UPDATE integrity_test SET val = 1, marker = 'conn1' WHERE id = 1",
    )
    .await;
    exec(
        &mut conn2,
        "UPDATE integrity_test SET val = 2, marker = 'conn2' WHERE id = 2",
    )
    .await;
    exec(
        &mut conn3,
        "INSERT INTO integrity_test VALUES (3, 3, 'conn3')",
    )
    .await;

    // Each should only see its own changes + committed data
    let rows1 = query_all(
        &mut conn1,
        "SELECT id, marker FROM integrity_test ORDER BY id",
    )
    .await;
    assert_eq!(rows1.len(), 2); // Only sees row 1 and 2 (committed), not row 3
    assert_eq!(rows1[0][1], "conn1"); // Own change visible
    assert_eq!(rows1[1][1], "initial"); // Row 2 still has initial value

    let rows2 = query_all(
        &mut conn2,
        "SELECT id, marker FROM integrity_test ORDER BY id",
    )
    .await;
    assert_eq!(rows2.len(), 2);
    assert_eq!(rows2[0][1], "initial"); // Row 1 still has initial value
    assert_eq!(rows2[1][1], "conn2"); // Own change visible

    // Commit in order
    exec(&mut conn3, "COMMIT").await;
    exec(&mut conn2, "COMMIT").await;
    exec(&mut conn1, "COMMIT").await;

    // Verify final state
    let rows = query_all(
        &mut conn1,
        "SELECT id, val, marker FROM integrity_test ORDER BY id",
    )
    .await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["1", "1", "conn1"]);
    assert_eq!(rows[1], vec!["2", "2", "conn2"]);
    assert_eq!(rows[2], vec!["3", "3", "conn3"]);
}
