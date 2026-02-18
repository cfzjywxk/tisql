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

//! Integration tests for Phase-6 tablet lifecycle + tablet-level drop GC.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::tempdir;

use tisql::catalog::types::Value;
use tisql::tablet::TabletId;
use tisql::testkit::ExecutionResult;
use tisql::{Database, DatabaseConfig};

fn create_test_db() -> (Arc<Database>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::open(DatabaseConfig::with_data_dir(dir.path())).unwrap());
    (db, dir)
}

async fn exec(db: &Database, sql: &str) {
    db.execute_query(sql).await.unwrap();
}

async fn table_id_by_name(db: &Arc<Database>, table_name: &str) -> u64 {
    let sess = db.new_inner_session("__tisql_inner");
    let sql = format!(
        "SELECT table_id FROM __all_table WHERE table_name = '{table_name}' ORDER BY table_id LIMIT 1"
    );
    let result = sess.execute_inner_sql(&sql).await.unwrap();
    let rows = match result {
        ExecutionResult::Rows { rows, .. } => rows,
        _ => panic!("expected rows for table id query"),
    };
    match rows.first().and_then(|row| row.get(0)) {
        Some(Value::BigInt(id)) => *id as u64,
        other => panic!("unexpected table_id row: {other:?}"),
    }
}

async fn gc_task_statuses_for_table(db: &Arc<Database>, table_id: u64) -> Vec<String> {
    let sess = db.new_inner_session("__tisql_inner");
    let sql = format!(
        "SELECT status FROM __all_gc_delete_range WHERE table_id = {table_id} ORDER BY task_id"
    );
    let result = sess.execute_inner_sql(&sql).await.unwrap();
    let rows = match result {
        ExecutionResult::Rows { rows, .. } => rows,
        _ => panic!("expected rows for gc task query"),
    };
    rows.into_iter()
        .filter_map(|row| match row.get(0) {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect()
}

async fn wait_until<F>(timeout: Duration, mut cond: F)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("condition not met within {timeout:?}");
}

#[tokio::test]
async fn test_phase6_create_table_creates_and_mounts_tablet() {
    let (db, dir) = create_test_db();

    exec(&db, "CREATE TABLE t_gc_create (id INT PRIMARY KEY, v INT)").await;
    let table_id = table_id_by_name(&db, "t_gc_create").await;
    let tablet_id = TabletId::Table { table_id };
    let tablet_dir = dir.path().join("tablets").join(format!("t_{table_id}"));

    assert!(tablet_dir.exists(), "tablet directory should be created");
    assert!(
        tablet_dir.join("ilog").join("tisql.ilog").exists(),
        "tablet ilog file should be created"
    );
    assert!(db.tablet_manager().get_tablet(tablet_id).is_some());
    assert!(db.tablet_manager().has_running_workers(tablet_id));

    db.close().await.unwrap();
}

#[tokio::test]
async fn test_phase6_inner_tables_remain_in_system_tablet() {
    let (db, dir) = create_test_db();
    let tablets_root = dir.path().join("tablets");

    assert!(tablets_root.join("system").exists());
    for table_id in 1..=6 {
        assert!(
            !tablets_root.join(format!("t_{table_id}")).exists(),
            "inner table {table_id} must not have dedicated tablet dir"
        );
    }

    db.close().await.unwrap();
}

#[tokio::test]
async fn test_phase6_drop_table_gc_removes_tablet_directory() {
    let (db, dir) = create_test_db();

    exec(&db, "CREATE TABLE t_gc_drop (id INT PRIMARY KEY, v INT)").await;
    exec(&db, "INSERT INTO t_gc_drop VALUES (1, 10), (2, 20)").await;

    let table_id = table_id_by_name(&db, "t_gc_drop").await;
    let tablet_id = TabletId::Table { table_id };
    let tablet_dir = dir.path().join("tablets").join(format!("t_{table_id}"));
    assert!(tablet_dir.exists());

    exec(&db, "DROP TABLE t_gc_drop").await;
    let statuses_after_drop = gc_task_statuses_for_table(&db, table_id).await;
    assert!(
        !statuses_after_drop.is_empty(),
        "drop should enqueue at least one GC task row"
    );
    db.notify_gc_worker();

    wait_until(Duration::from_secs(5), || {
        db.notify_gc_worker();
        db.tablet_manager().get_tablet(tablet_id).is_none() && !tablet_dir.exists()
    })
    .await;

    let statuses = gc_task_statuses_for_table(&db, table_id).await;
    assert!(!statuses.is_empty(), "drop should create GC task rows");
    assert!(statuses.iter().all(|s| s == "done"));

    db.close().await.unwrap();
}

#[tokio::test]
async fn test_phase6_gc_waits_for_active_reader_safe_point() {
    let (db, dir) = create_test_db();

    exec(&db, "CREATE TABLE t_gc_pin (id INT PRIMARY KEY, v INT)").await;
    exec(&db, "INSERT INTO t_gc_pin VALUES (1, 100)").await;
    let table_id = table_id_by_name(&db, "t_gc_pin").await;
    let tablet_id = TabletId::Table { table_id };
    let tablet_dir = dir.path().join("tablets").join(format!("t_{table_id}"));

    // Pin GC safe point to 0 (simulates a long-running explicit reader).
    db.session_registry().register(42, 1);

    exec(&db, "DROP TABLE t_gc_pin").await;
    db.notify_gc_worker();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        tablet_dir.exists(),
        "tablet must not be deleted while pinned"
    );
    assert!(db.tablet_manager().get_tablet(tablet_id).is_some());

    let statuses = gc_task_statuses_for_table(&db, table_id).await;
    assert!(
        statuses.iter().any(|s| s == "pending"),
        "task should stay pending while safe point is pinned"
    );

    db.session_registry().unregister(42);
    db.notify_gc_worker();

    wait_until(Duration::from_secs(5), || {
        db.notify_gc_worker();
        db.tablet_manager().get_tablet(tablet_id).is_none() && !tablet_dir.exists()
    })
    .await;

    let statuses = gc_task_statuses_for_table(&db, table_id).await;
    assert!(statuses.iter().all(|s| s == "done"));

    db.close().await.unwrap();
}

#[tokio::test]
async fn test_phase6_pending_drop_gc_resumes_after_restart() {
    let dir = tempdir().unwrap();
    let config = DatabaseConfig::with_data_dir(dir.path());

    let table_id = {
        let db = Arc::new(Database::open(config.clone()).unwrap());
        exec(&db, "CREATE TABLE t_gc_restart (id INT PRIMARY KEY, v INT)").await;
        exec(&db, "INSERT INTO t_gc_restart VALUES (1, 1), (2, 2)").await;
        let table_id = table_id_by_name(&db, "t_gc_restart").await;

        db.session_registry().register(7, 1);
        exec(&db, "DROP TABLE t_gc_restart").await;
        db.notify_gc_worker();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let statuses = gc_task_statuses_for_table(&db, table_id).await;
        assert!(statuses.iter().any(|s| s == "pending"));

        db.close().await.unwrap();
        table_id
    };

    let db2 = Arc::new(Database::open(config).unwrap());
    let tablet_dir = dir.path().join("tablets").join(format!("t_{table_id}"));

    db2.notify_gc_worker();
    wait_until(Duration::from_secs(5), || {
        db2.notify_gc_worker();
        !tablet_dir.exists()
    })
    .await;

    let statuses = gc_task_statuses_for_table(&db2, table_id).await;
    assert!(
        !statuses.is_empty() && statuses.iter().all(|s| s == "done"),
        "pending GC tasks should be resumed and completed after restart"
    );

    db2.close().await.unwrap();
}
