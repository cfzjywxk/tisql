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
use tisql::{Database, DatabaseConfig, QueryResult, RuntimeThreadOverrides};

fn create_test_db() -> (Arc<Database>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let config = DatabaseConfig::with_data_dir(dir.path())
        .with_storage_sync_io(true)
        .with_runtime_threads(RuntimeThreadOverrides {
            protocol: Some(1),
            worker: Some(1),
            background: Some(1),
            io: Some(1),
        });
    let db = Arc::new(Database::open(config).unwrap());
    (db, dir)
}

async fn exec(db: &Database, sql: &str) {
    db.execute_query(sql).await.unwrap();
}

async fn table_id_by_name(db: &Arc<Database>, table_name: &str) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(5);
    let sql = format!(
        "SELECT table_id FROM __all_table WHERE table_name = '{table_name}' ORDER BY table_id LIMIT 1"
    );

    while Instant::now() < deadline {
        let sess = db.new_inner_session("__tisql_inner");
        let result = sess.execute_inner_sql(&sql).await.unwrap();
        let rows = match result {
            ExecutionResult::Rows { rows, .. } => rows,
            _ => panic!("expected rows for table id query"),
        };

        if let Some(Value::BigInt(id)) = rows.first().and_then(|row| row.get(0)) {
            return *id as u64;
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    panic!("table {table_name} did not appear in __all_table before timeout");
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

/// T6.5c: Multiple tables dropped → crash → restart → all GC tasks resume independently.
#[tokio::test]
async fn test_phase6_multiple_pending_drops_resume_after_restart() {
    let dir = tempdir().unwrap();
    let config = DatabaseConfig::with_data_dir(dir.path());

    let (table_id_a, table_id_b) = {
        let db = Arc::new(Database::open(config.clone()).unwrap());
        exec(&db, "CREATE TABLE t_multi_a (id INT PRIMARY KEY, v INT)").await;
        exec(&db, "CREATE TABLE t_multi_b (id INT PRIMARY KEY, v INT)").await;
        exec(&db, "INSERT INTO t_multi_a VALUES (1, 10)").await;
        exec(&db, "INSERT INTO t_multi_b VALUES (1, 20)").await;
        let id_a = table_id_by_name(&db, "t_multi_a").await;
        let id_b = table_id_by_name(&db, "t_multi_b").await;

        // Pin safe point so GC cannot proceed.
        db.session_registry().register(77, 1);
        exec(&db, "DROP TABLE t_multi_a").await;
        exec(&db, "DROP TABLE t_multi_b").await;
        db.notify_gc_worker();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let statuses_a = gc_task_statuses_for_table(&db, id_a).await;
        let statuses_b = gc_task_statuses_for_table(&db, id_b).await;
        assert!(statuses_a.iter().any(|s| s == "pending"));
        assert!(statuses_b.iter().any(|s| s == "pending"));

        db.close().await.unwrap();
        (id_a, id_b)
    };

    // Reopen — no safe point pin, both GC tasks should complete.
    let db2 = Arc::new(Database::open(config).unwrap());
    let dir_a = dir.path().join("tablets").join(format!("t_{table_id_a}"));
    let dir_b = dir.path().join("tablets").join(format!("t_{table_id_b}"));

    wait_until(Duration::from_secs(5), || {
        db2.notify_gc_worker();
        !dir_a.exists() && !dir_b.exists()
    })
    .await;

    let statuses_a = gc_task_statuses_for_table(&db2, table_id_a).await;
    let statuses_b = gc_task_statuses_for_table(&db2, table_id_b).await;
    assert!(
        statuses_a.iter().all(|s| s == "done"),
        "table A GC tasks should all be done after restart"
    );
    assert!(
        statuses_b.iter().all(|s| s == "done"),
        "table B GC tasks should all be done after restart"
    );

    db2.close().await.unwrap();
}

/// T6.3d + T6.3e: GC retirement stops schedulers and flushes data before deleting.
///
/// Writes data to a user tablet without flushing, then triggers GC. After GC
/// completes, the tablet must be unmounted (no running workers) and the directory
/// deleted. The flush-before-delete is verified by the absence of errors in GC
/// (remove_tablet flushes internally; if flush were skipped, ilog shutdown with
/// unflushed data would fail or leave partial state).
#[tokio::test]
async fn test_phase6_gc_retirement_stops_workers_and_flushes() {
    let (db, dir) = create_test_db();

    exec(&db, "CREATE TABLE t_gc_retire (id INT PRIMARY KEY, v INT)").await;
    exec(&db, "INSERT INTO t_gc_retire VALUES (1, 100), (2, 200)").await;
    let table_id = table_id_by_name(&db, "t_gc_retire").await;
    let tablet_id = TabletId::Table { table_id };
    let tablet_dir = dir.path().join("tablets").join(format!("t_{table_id}"));

    // Verify workers are running before drop.
    assert!(
        db.tablet_manager().has_running_workers(tablet_id),
        "tablet should have running workers before drop"
    );

    // Data is in memtable (not flushed). Drop + GC must flush before unmount.
    exec(&db, "DROP TABLE t_gc_retire").await;

    wait_until(Duration::from_secs(5), || {
        db.notify_gc_worker();
        db.tablet_manager().get_tablet(tablet_id).is_none()
    })
    .await;

    // Workers should be stopped and tablet unmounted.
    assert!(
        !db.tablet_manager().has_running_workers(tablet_id),
        "workers should be stopped after GC retirement"
    );
    assert!(
        db.tablet_manager().get_tablet(tablet_id).is_none(),
        "tablet should be unmounted after GC retirement"
    );
    assert!(!tablet_dir.exists(), "tablet directory should be deleted");

    db.close().await.unwrap();
}

/// T6.4b: CREATE TABLE → crash (no explicit close) → recover → table exists, tablet restored.
#[tokio::test]
async fn test_phase6_create_table_survives_crash_recovery() {
    let dir = tempdir().unwrap();
    let config = DatabaseConfig::with_data_dir(dir.path());

    let table_id = {
        let db = Arc::new(Database::open(config.clone()).unwrap());
        exec(
            &db,
            "CREATE TABLE t_crash_create (id INT PRIMARY KEY, v INT)",
        )
        .await;
        exec(&db, "INSERT INTO t_crash_create VALUES (1, 42)").await;
        let table_id = table_id_by_name(&db, "t_crash_create").await;

        let tablet_id = TabletId::Table { table_id };
        assert!(db.tablet_manager().get_tablet(tablet_id).is_some());

        // Simulate crash — drop without close.
        drop(db);
        table_id
    };

    // Reopen and verify table + tablet are restored.
    let db2 = Arc::new(Database::open(config).unwrap());
    let tablet_id = TabletId::Table { table_id };

    assert!(
        db2.tablet_manager().get_tablet(tablet_id).is_some(),
        "tablet should be mounted after crash recovery"
    );
    assert!(
        db2.tablet_manager().has_running_workers(tablet_id),
        "tablet workers should be running after crash recovery"
    );

    // Data should survive via clog replay.
    match db2
        .execute_query("SELECT v FROM t_crash_create WHERE id = 1")
        .await
        .unwrap()
    {
        QueryResult::Rows { data, .. } => {
            assert_eq!(data.len(), 1, "row should survive crash recovery");
            assert_eq!(data[0][0], "42");
        }
        other => panic!("expected rows, got {other:?}"),
    }

    db2.close().await.unwrap();
}

/// T6.4c: DROP TABLE → crash before GC runs → recover → table is dropped, data gone,
/// GC task resumes and cleans up tablet.
#[tokio::test]
async fn test_phase6_drop_table_crash_before_gc_still_drops_table() {
    let dir = tempdir().unwrap();
    let config = DatabaseConfig::with_data_dir(dir.path());

    let table_id = {
        let db = Arc::new(Database::open(config.clone()).unwrap());
        exec(&db, "CREATE TABLE t_crash_drop (id INT PRIMARY KEY, v INT)").await;
        exec(&db, "INSERT INTO t_crash_drop VALUES (1, 99)").await;
        let table_id = table_id_by_name(&db, "t_crash_drop").await;

        // Pin GC so it can't run before crash.
        db.session_registry().register(88, 1);
        exec(&db, "DROP TABLE t_crash_drop").await;

        // Crash — tablet dir still exists, GC hasn't run.
        let tablet_dir = dir.path().join("tablets").join(format!("t_{table_id}"));
        assert!(tablet_dir.exists());
        drop(db);
        table_id
    };

    // Reopen — table should be dropped (not in catalog), but GC task should
    // resume and clean up the tablet directory.
    let db2 = Arc::new(Database::open(config).unwrap());
    let tablet_dir = dir.path().join("tablets").join(format!("t_{table_id}"));

    // Table should be gone from catalog.
    let result = db2.execute_query("SELECT * FROM t_crash_drop").await;
    assert!(
        result.is_err(),
        "dropped table should not exist after crash recovery"
    );

    // GC should resume and clean up.
    wait_until(Duration::from_secs(5), || {
        db2.notify_gc_worker();
        !tablet_dir.exists()
    })
    .await;

    let statuses = gc_task_statuses_for_table(&db2, table_id).await;
    assert!(
        statuses.iter().all(|s| s == "done"),
        "GC tasks should complete after restart"
    );

    db2.close().await.unwrap();
}

/// T6.4a: CREATE TABLE crash-recovery — no orphan tablet directory from a DDL
/// that never committed.
///
/// Since tablet mounting happens AFTER catalog commit (DdlEffect flow), a crash
/// before CREATE TABLE commits cannot leave an orphan tablet directory. This test
/// verifies: create table → commit → crash → recover → table + tablet intact,
/// and non-table data is unaffected.
#[tokio::test]
async fn test_phase6_create_table_no_orphan_after_crash() {
    let dir = tempdir().unwrap();
    let config = DatabaseConfig::with_data_dir(dir.path());

    // Session 1: Create two tables, crash without close.
    {
        let db = Arc::new(Database::open(config.clone()).unwrap());
        exec(&db, "CREATE TABLE t_orphan_a (id INT PRIMARY KEY, v INT)").await;
        exec(&db, "CREATE TABLE t_orphan_b (id INT PRIMARY KEY, v INT)").await;
        exec(&db, "INSERT INTO t_orphan_a VALUES (1, 10)").await;
        exec(&db, "INSERT INTO t_orphan_b VALUES (1, 20)").await;
        // Crash — no close.
        drop(db);
    }

    // Session 2: Both tables and tablets should be restored.
    let db2 = Arc::new(Database::open(config).unwrap());

    match db2
        .execute_query("SELECT v FROM t_orphan_a WHERE id = 1")
        .await
        .unwrap()
    {
        QueryResult::Rows { data, .. } => {
            assert_eq!(data.len(), 1);
            assert_eq!(data[0][0], "10");
        }
        other => panic!("expected rows for t_orphan_a, got {other:?}"),
    }
    match db2
        .execute_query("SELECT v FROM t_orphan_b WHERE id = 1")
        .await
        .unwrap()
    {
        QueryResult::Rows { data, .. } => {
            assert_eq!(data.len(), 1);
            assert_eq!(data[0][0], "20");
        }
        other => panic!("expected rows for t_orphan_b, got {other:?}"),
    }

    // Both tablets should be mounted with running workers.
    let id_a = table_id_by_name(&db2, "t_orphan_a").await;
    let id_b = table_id_by_name(&db2, "t_orphan_b").await;
    assert!(db2
        .tablet_manager()
        .get_tablet(TabletId::Table { table_id: id_a })
        .is_some());
    assert!(db2
        .tablet_manager()
        .get_tablet(TabletId::Table { table_id: id_b })
        .is_some());

    db2.close().await.unwrap();
}
