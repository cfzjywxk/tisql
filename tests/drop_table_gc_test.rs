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

//! Integration tests for Drop Table GC.
//!
//! Tests the end-to-end flow: DROP TABLE creates a GC task row in
//! `__all_gc_delete_range`, and on recovery the dropped table IDs
//! are rebuilt from the inner table.
//!
//! Run with: cargo test --test drop_table_gc_test

use std::sync::Arc;

use tempfile::tempdir;

use tisql::testkit::ExecutionResult;
use tisql::types::Value;
use tisql::{Database, DatabaseConfig, QueryResult};

/// Create a fresh database in a temp directory.
fn create_test_db() -> (Arc<Database>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let config = DatabaseConfig::with_data_dir(dir.path());
    let db = Arc::new(Database::open(config).unwrap());
    (db, dir)
}

/// Helper: query via handle_mp_query and return rows as Vec<Vec<String>>.
async fn query_rows(db: &Database, sql: &str) -> Vec<Vec<String>> {
    match db.handle_mp_query(sql).await.unwrap() {
        QueryResult::Rows { data, .. } => data,
        other => panic!("Expected rows, got: {other:?}"),
    }
}

/// Helper: execute SQL via handle_mp_query, expecting success.
async fn exec(db: &Database, sql: &str) {
    db.handle_mp_query(sql).await.unwrap();
}

/// Query GC tasks from inner table and return rows.
async fn query_gc_tasks(db: &Arc<Database>) -> Vec<tisql::types::Row> {
    let sess = db.new_inner_session("__tisql_inner");
    let result = sess.execute_inner_sql(
        "SELECT task_id, table_id, start_key, end_key, drop_commit_ts, status FROM __all_gc_delete_range ORDER BY task_id",
    )
    .await
    .unwrap();
    match result {
        ExecutionResult::Rows { rows, .. } => rows,
        _ => panic!("expected rows from GC task query"),
    }
}

/// After DROP TABLE, a GC task row should exist in `__all_gc_delete_range`.
#[tokio::test]
async fn test_drop_table_gc_task_persists() {
    let (db, _dir) = create_test_db();

    // Create and populate a table
    exec(&db, "CREATE TABLE gc_test (id INT PRIMARY KEY, val INT)").await;
    exec(&db, "INSERT INTO gc_test VALUES (1, 100), (2, 200)").await;

    // Drop the table — this should create a GC task
    exec(&db, "DROP TABLE gc_test").await;

    let rows = query_gc_tasks(&db).await;
    assert!(!rows.is_empty(), "Should have at least one GC task");
    let row = &rows[0];

    // task_id should be 1 (first GC task)
    assert_eq!(
        *row.get(0).unwrap(),
        Value::BigInt(1),
        "task_id should be 1"
    );

    // table_id should be a valid table ID (>= 1000 for user tables)
    match row.get(1).unwrap() {
        Value::BigInt(table_id) => {
            assert!(
                *table_id >= 1000,
                "table_id should be >= 1000 for user tables, got {table_id}"
            );
        }
        other => panic!("Expected BigInt for table_id, got: {other:?}"),
    }

    // start_key and end_key should be non-empty hex strings
    match row.get(2).unwrap() {
        Value::String(s) => {
            assert!(!s.is_empty(), "start_key should not be empty");
            assert!(s.len() % 2 == 0, "start_key should be even-length hex");
        }
        other => panic!("Expected String for start_key, got: {other:?}"),
    }

    match row.get(3).unwrap() {
        Value::String(s) => {
            assert!(!s.is_empty(), "end_key should not be empty");
            assert!(s.len() % 2 == 0, "end_key should be even-length hex");
        }
        other => panic!("Expected String for end_key, got: {other:?}"),
    }

    // drop_commit_ts should be > 0 (updated after commit)
    match row.get(4).unwrap() {
        Value::BigInt(ts) => {
            assert!(*ts > 0, "drop_commit_ts should be > 0, got {ts}");
        }
        other => panic!("Expected BigInt for drop_commit_ts, got: {other:?}"),
    }

    // status should be "pending"
    assert_eq!(
        *row.get(5).unwrap(),
        Value::String("pending".into()),
        "status should be 'pending'"
    );
}

/// After dropping multiple tables, multiple GC tasks should exist.
#[tokio::test]
async fn test_drop_table_multiple_gc_tasks() {
    let (db, _dir) = create_test_db();

    exec(&db, "CREATE TABLE t1 (id INT PRIMARY KEY)").await;
    exec(&db, "INSERT INTO t1 VALUES (1)").await;
    exec(&db, "CREATE TABLE t2 (id INT PRIMARY KEY)").await;
    exec(&db, "INSERT INTO t2 VALUES (1)").await;
    exec(&db, "CREATE TABLE t3 (id INT PRIMARY KEY)").await;
    exec(&db, "INSERT INTO t3 VALUES (1)").await;

    exec(&db, "DROP TABLE t1").await;
    exec(&db, "DROP TABLE t2").await;
    exec(&db, "DROP TABLE t3").await;

    let rows = query_gc_tasks(&db).await;
    assert_eq!(rows.len(), 3, "Should have 3 GC tasks");
    // Task IDs should be sequential: 1, 2, 3
    assert_eq!(*rows[0].get(0).unwrap(), Value::BigInt(1));
    assert_eq!(*rows[1].get(0).unwrap(), Value::BigInt(2));
    assert_eq!(*rows[2].get(0).unwrap(), Value::BigInt(3));
}

/// After DROP TABLE + restart, the GC task should still be present.
#[tokio::test]
async fn test_drop_table_gc_recovery() {
    let dir = tempdir().unwrap();
    let config = DatabaseConfig::with_data_dir(dir.path());

    // First session: create table, insert data, drop table
    {
        let db = Arc::new(Database::open(config.clone()).unwrap());
        exec(&db, "CREATE TABLE recover_me (id INT PRIMARY KEY, val INT)").await;
        exec(&db, "INSERT INTO recover_me VALUES (1, 42), (2, 84)").await;
        exec(&db, "DROP TABLE recover_me").await;

        let rows = query_gc_tasks(&db).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].get(5).unwrap(), Value::String("pending".into()));

        db.close().await.unwrap();
    }

    // Second session: verify GC task survives restart
    {
        let db = Arc::new(Database::open(config.clone()).unwrap());

        // Table should still be gone
        let result = db.handle_mp_query("SELECT * FROM recover_me").await;
        assert!(
            result.is_err(),
            "dropped table should not exist after restart"
        );

        // GC task should still be present in inner table
        let rows = query_gc_tasks(&db).await;
        assert_eq!(rows.len(), 1, "GC task should survive restart");
        assert_eq!(*rows[0].get(0).unwrap(), Value::BigInt(1));
        match rows[0].get(4).unwrap() {
            Value::BigInt(ts) => {
                assert!(*ts > 0, "drop_commit_ts should be > 0 after recovery");
            }
            other => panic!("Expected BigInt, got: {other:?}"),
        }
        assert_eq!(*rows[0].get(5).unwrap(), Value::String("pending".into()));

        db.close().await.unwrap();
    }
}

/// After DROP TABLE + crash (no explicit close), the GC task should be recovered.
#[tokio::test]
async fn test_drop_table_gc_crash_recovery() {
    let dir = tempdir().unwrap();
    let config = DatabaseConfig::with_data_dir(dir.path());

    // First session: create and populate
    {
        let db = Arc::new(Database::open(config.clone()).unwrap());
        exec(&db, "CREATE TABLE crash_test (id INT PRIMARY KEY)").await;
        exec(&db, "INSERT INTO crash_test VALUES (1), (2), (3)").await;
        db.close().await.unwrap();
    }

    // Second session: drop table + crash (no close)
    {
        let db = Arc::new(Database::open(config.clone()).unwrap());
        exec(&db, "DROP TABLE crash_test").await;
        // Intentional crash — no db.close()
    }

    // Third session: verify recovery
    {
        let db = Arc::new(Database::open(config.clone()).unwrap());

        // Table should be gone
        let result = db.handle_mp_query("SELECT * FROM crash_test").await;
        assert!(
            result.is_err(),
            "dropped table should not exist after crash recovery"
        );

        // GC task should be recovered
        let rows = query_gc_tasks(&db).await;
        assert_eq!(rows.len(), 1, "GC task should survive crash recovery");

        db.close().await.unwrap();
    }
}

/// Verify GC task ID counter persists across restarts.
#[tokio::test]
async fn test_gc_task_id_counter_persists() {
    let dir = tempdir().unwrap();
    let config = DatabaseConfig::with_data_dir(dir.path());

    // First session: create and drop two tables
    {
        let db = Arc::new(Database::open(config.clone()).unwrap());
        exec(&db, "CREATE TABLE t1 (id INT PRIMARY KEY)").await;
        exec(&db, "CREATE TABLE t2 (id INT PRIMARY KEY)").await;
        exec(&db, "DROP TABLE t1").await;
        exec(&db, "DROP TABLE t2").await;
        db.close().await.unwrap();
    }

    // Second session: drop another table — task_id should continue from 3
    {
        let db = Arc::new(Database::open(config.clone()).unwrap());
        exec(&db, "CREATE TABLE t3 (id INT PRIMARY KEY)").await;
        exec(&db, "DROP TABLE t3").await;

        let rows = query_gc_tasks(&db).await;
        assert_eq!(rows.len(), 3, "Should have 3 GC tasks total");
        assert_eq!(*rows[0].get(0).unwrap(), Value::BigInt(1));
        assert_eq!(*rows[1].get(0).unwrap(), Value::BigInt(2));
        assert_eq!(*rows[2].get(0).unwrap(), Value::BigInt(3));

        db.close().await.unwrap();
    }
}

/// After DROP TABLE, creating a new table with the same name should work.
#[tokio::test]
async fn test_drop_and_recreate_table() {
    let (db, _dir) = create_test_db();

    exec(&db, "CREATE TABLE reuse (id INT PRIMARY KEY)").await;
    exec(&db, "INSERT INTO reuse VALUES (1)").await;
    exec(&db, "DROP TABLE reuse").await;

    // Recreate with same name
    exec(&db, "CREATE TABLE reuse (id INT PRIMARY KEY, val INT)").await;
    exec(&db, "INSERT INTO reuse VALUES (10, 100)").await;

    // New table should work
    let rows = query_rows(&db, "SELECT id, val FROM reuse").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "10");
    assert_eq!(rows[0][1], "100");

    // GC task for old table should still exist
    let gc_rows = query_gc_tasks(&db).await;
    assert_eq!(gc_rows.len(), 1, "Should have 1 GC task for old table");
    assert_eq!(*gc_rows[0].get(5).unwrap(), Value::String("pending".into()));
}
