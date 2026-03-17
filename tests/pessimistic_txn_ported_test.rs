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

//! Pessimistic transaction regression coverage adapted from TiDB tests.
//!
//! Reference: tests/realtikvtest/pessimistictest/pessimistic_test.go in TiDB.
//! These cases cover only features currently supported by TiSQL.

use std::future::Future;
use std::time::Duration;

use tempfile::TempDir;
use tisql::util::error::TiSqlError;
use tisql::{Database, DatabaseConfig, QueryResult, Session};

const DEFAULT_CASE_TIMEOUT_SECS: u64 = 20;
const CASE_TIMEOUT_ENV: &str = "TISQL_PESSIMISTIC_CASE_TIMEOUT_SECS";

fn open_test_db() -> (Database, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let config = DatabaseConfig::with_data_dir(dir.path())
        .with_lock_wait_timeout(Duration::from_millis(100));
    let db = Database::open(config).unwrap();
    (db, dir)
}

fn case_timeout() -> Duration {
    let parsed = std::env::var(CASE_TIMEOUT_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|secs| *secs > 0);
    Duration::from_secs(parsed.unwrap_or(DEFAULT_CASE_TIMEOUT_SECS))
}

async fn run_with_case_timeout<F>(name: &str, fut: F)
where
    F: Future<Output = ()>,
{
    let timeout = case_timeout();
    tokio::time::timeout(timeout, fut)
        .await
        .unwrap_or_else(|_| panic!("{name} timed out after {timeout:?}"));
}

async fn exec(db: &Database, sql: &str) -> QueryResult {
    db.execute_query(sql)
        .await
        .unwrap_or_else(|e| panic!("SQL failed: {e}\nSQL: {sql}"))
}

async fn exec_with_session(db: &Database, session: &mut Session, sql: &str) -> QueryResult {
    db.execute_query_with_session(sql, session)
        .await
        .unwrap_or_else(|e| panic!("SQL failed: {e}\nSQL: {sql}"))
}

async fn query_rows(db: &Database, sql: &str) -> Vec<Vec<String>> {
    match exec(db, sql).await {
        QueryResult::Rows { data, .. } => data,
        other => panic!("Expected rows, got {other:?}\nSQL: {sql}"),
    }
}

async fn query_rows_with_session(
    db: &Database,
    session: &mut Session,
    sql: &str,
) -> Vec<Vec<String>> {
    match exec_with_session(db, session, sql).await {
        QueryResult::Rows { data, .. } => data,
        other => panic!("Expected rows, got {other:?}\nSQL: {sql}"),
    }
}

async fn expect_lock_wait_timeout(result: Result<QueryResult, TiSqlError>, sql: &str) {
    match result {
        Err(TiSqlError::LockWaitTimeout) => {}
        Err(e) => panic!("Expected LockWaitTimeout, got {e:?}\nSQL: {sql}"),
        Ok(r) => panic!("Expected LockWaitTimeout, got success {r:?}\nSQL: {sql}"),
    }
}

#[tokio::test]
async fn test_ported_explicit_conflict_rollback_then_retry_and_commit() {
    run_with_case_timeout(
        "test_ported_explicit_conflict_rollback_then_retry_and_commit",
        async {
            let (db, _dir) = open_test_db();
            let mut s1 = Session::new();
            let mut s2 = Session::new();

            exec(&db, "CREATE TABLE t_lock_retry (a INT PRIMARY KEY, b INT)").await;

            exec_with_session(&db, &mut s1, "BEGIN").await;
            exec_with_session(&db, &mut s1, "INSERT INTO t_lock_retry VALUES (2, 0)").await;

            exec_with_session(&db, &mut s2, "BEGIN").await;
            expect_lock_wait_timeout(
                db.execute_query_with_session("INSERT INTO t_lock_retry VALUES (2, 22)", &mut s2)
                    .await,
                "INSERT INTO t_lock_retry VALUES (2, 22)",
            )
            .await;
            assert!(
                !s2.has_active_txn(),
                "lock wait timeout must clear explicit transaction"
            );

            exec_with_session(&db, &mut s1, "ROLLBACK").await;
            exec_with_session(&db, &mut s2, "BEGIN").await;
            exec_with_session(&db, &mut s2, "INSERT INTO t_lock_retry VALUES (2, 22)").await;

            let uncommitted = query_rows(&db, "SELECT a, b FROM t_lock_retry").await;
            assert!(
                uncommitted.is_empty(),
                "autocommit reads must not see uncommitted explicit writes"
            );

            exec_with_session(&db, &mut s2, "COMMIT").await;

            let rows = query_rows(&db, "SELECT a, b FROM t_lock_retry ORDER BY a").await;
            assert_eq!(rows, vec![vec!["2".to_string(), "22".to_string()]]);

            db.close().await.unwrap();
        },
    )
    .await;
}

#[tokio::test]
async fn test_ported_lock_wait_timeout_aborts_explicit_txn_and_requires_rebegin() {
    run_with_case_timeout(
        "test_ported_lock_wait_timeout_aborts_explicit_txn_and_requires_rebegin",
        async {
            let (db, _dir) = open_test_db();
            let mut s1 = Session::new();
            let mut s2 = Session::new();

            exec(&db, "CREATE TABLE t_txn_keep (a INT PRIMARY KEY, b INT)").await;

            exec_with_session(&db, &mut s1, "BEGIN").await;
            exec_with_session(&db, &mut s1, "INSERT INTO t_txn_keep VALUES (1, 10)").await;

            exec_with_session(&db, &mut s2, "BEGIN").await;
            expect_lock_wait_timeout(
                db.execute_query_with_session("INSERT INTO t_txn_keep VALUES (1, 20)", &mut s2)
                    .await,
                "INSERT INTO t_txn_keep VALUES (1, 20)",
            )
            .await;

            assert!(
                !s2.has_active_txn(),
                "explicit txn should be cleared after lock wait timeout"
            );

            exec_with_session(&db, &mut s1, "ROLLBACK").await;
            exec_with_session(&db, &mut s2, "BEGIN").await;
            exec_with_session(&db, &mut s2, "INSERT INTO t_txn_keep VALUES (3, 30)").await;
            exec_with_session(&db, &mut s2, "INSERT INTO t_txn_keep VALUES (1, 20)").await;
            exec_with_session(&db, &mut s2, "COMMIT").await;

            let rows = query_rows(&db, "SELECT a, b FROM t_txn_keep ORDER BY a").await;
            assert_eq!(
                rows,
                vec![
                    vec!["1".to_string(), "20".to_string()],
                    vec!["3".to_string(), "30".to_string()],
                ]
            );

            db.close().await.unwrap();
        },
    )
    .await;
}

#[tokio::test]
async fn test_ported_autocommit_vs_explicit_write_conflict_and_retry_after_commit() {
    run_with_case_timeout(
        "test_ported_autocommit_vs_explicit_write_conflict_and_retry_after_commit",
        async {
            let (db, _dir) = open_test_db();
            let mut s1 = Session::new();

            exec(
                &db,
                "CREATE TABLE t_auto_conflict (id INT PRIMARY KEY, v INT)",
            )
            .await;
            exec(&db, "INSERT INTO t_auto_conflict VALUES (1, 100)").await;

            exec_with_session(&db, &mut s1, "BEGIN").await;
            exec_with_session(
                &db,
                &mut s1,
                "UPDATE t_auto_conflict SET v = 101 WHERE id = 1",
            )
            .await;

            expect_lock_wait_timeout(
                db.execute_query("UPDATE t_auto_conflict SET v = 102 WHERE id = 1")
                    .await,
                "UPDATE t_auto_conflict SET v = 102 WHERE id = 1",
            )
            .await;

            exec_with_session(&db, &mut s1, "COMMIT").await;
            exec(&db, "UPDATE t_auto_conflict SET v = 102 WHERE id = 1").await;

            let rows = query_rows(&db, "SELECT id, v FROM t_auto_conflict").await;
            assert_eq!(rows, vec![vec!["1".to_string(), "102".to_string()]]);

            db.close().await.unwrap();
        },
    )
    .await;
}

#[tokio::test]
async fn test_ported_explicit_disjoint_writes_commit_without_conflict() {
    run_with_case_timeout(
        "test_ported_explicit_disjoint_writes_commit_without_conflict",
        async {
            let (db, _dir) = open_test_db();
            let mut s1 = Session::new();
            let mut s2 = Session::new();

            exec(
                &db,
                "CREATE TABLE t_no_conflict (id INT PRIMARY KEY, v INT)",
            )
            .await;

            exec_with_session(&db, &mut s1, "BEGIN").await;
            exec_with_session(&db, &mut s1, "INSERT INTO t_no_conflict VALUES (1, 10)").await;

            exec_with_session(&db, &mut s2, "BEGIN").await;
            exec_with_session(&db, &mut s2, "INSERT INTO t_no_conflict VALUES (2, 20)").await;

            exec_with_session(&db, &mut s1, "COMMIT").await;
            exec_with_session(&db, &mut s2, "COMMIT").await;

            let rows = query_rows(&db, "SELECT id, v FROM t_no_conflict ORDER BY id").await;
            assert_eq!(
                rows,
                vec![
                    vec!["1".to_string(), "10".to_string()],
                    vec!["2".to_string(), "20".to_string()],
                ]
            );

            db.close().await.unwrap();
        },
    )
    .await;
}

#[tokio::test]
async fn test_ported_autocommit_write_without_conflict_while_explicit_holds_other_lock() {
    run_with_case_timeout(
        "test_ported_autocommit_write_without_conflict_while_explicit_holds_other_lock",
        async {
            let (db, _dir) = open_test_db();
            let mut s1 = Session::new();

            exec(
                &db,
                "CREATE TABLE t_auto_no_conflict (id INT PRIMARY KEY, v INT)",
            )
            .await;
            exec(
                &db,
                "INSERT INTO t_auto_no_conflict VALUES (1, 10), (2, 20)",
            )
            .await;

            exec_with_session(&db, &mut s1, "BEGIN").await;
            exec_with_session(
                &db,
                &mut s1,
                "UPDATE t_auto_no_conflict SET v = 11 WHERE id = 1",
            )
            .await;

            exec(&db, "UPDATE t_auto_no_conflict SET v = 21 WHERE id = 2").await;

            // Inside s1 transaction, own update is visible while autocommit update on id=2 is not.
            let s1_rows = query_rows_with_session(
                &db,
                &mut s1,
                "SELECT id, v FROM t_auto_no_conflict ORDER BY id",
            )
            .await;
            assert_eq!(
                s1_rows,
                vec![
                    vec!["1".to_string(), "11".to_string()],
                    vec!["2".to_string(), "20".to_string()],
                ]
            );

            exec_with_session(&db, &mut s1, "ROLLBACK").await;

            let rows = query_rows(&db, "SELECT id, v FROM t_auto_no_conflict ORDER BY id").await;
            assert_eq!(
                rows,
                vec![
                    vec!["1".to_string(), "10".to_string()],
                    vec!["2".to_string(), "21".to_string()],
                ]
            );

            db.close().await.unwrap();
        },
    )
    .await;
}
