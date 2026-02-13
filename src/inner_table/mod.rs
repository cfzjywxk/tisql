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

//! Inner SQL execution and system table infrastructure.
//!
//! Provides [`InnerSession`] — a standalone SQL execution environment for
//! internal use (GC workers, catalog operations, background tasks). Inspired
//! by OceanBase's `ObInnerSQLConnection`.
//!
//! Also provides core system table definitions, bootstrap logic, and catalog
//! cache loading for the inner-table-based catalog.
//!
//! ## Usage
//!
//! ```ignore
//! let db = Arc::new(Database::open(config)?);
//! let mut sess = db.new_inner_session("default");
//!
//! // Auto-commit (each statement is its own transaction)
//! sess.execute_inner_sql("CREATE TABLE t (id INT PRIMARY KEY)").await?;
//! sess.execute_inner_sql("INSERT INTO t VALUES (1)").await?;
//!
//! // Explicit transaction
//! sess.begin()?;
//! sess.execute("INSERT INTO t VALUES (2)").await?;
//! sess.execute("INSERT INTO t VALUES (3)").await?;
//! sess.commit()?;
//! ```

pub(crate) mod bootstrap;
pub(crate) mod catalog_loader;
pub(crate) mod core_tables;
pub(crate) mod gc_worker;

use std::sync::Arc;

use crate::error::Result;
use crate::executor::ExecutionResult;
use crate::session::{ExecutionCtx, Session};
use crate::Database;

/// Inner session for internal SQL execution.
///
/// Wraps `Arc<Database>` + a `Session` to provide full SQL execution
/// with auto-commit and explicit transaction support. Uses `Arc` to
/// avoid lifetime parameters — can be stored, moved, or held in structs.
///
/// Active explicit transactions are automatically rolled back on drop
/// to prevent dangling locks.
pub struct InnerSession {
    db: Arc<Database>,
    session: Session,
}

impl InnerSession {
    /// Create an inner session targeting a specific database/schema.
    pub fn new(db: Arc<Database>, current_db: &str) -> Self {
        let mut session = Session::new();
        session.set_current_db(current_db);
        Self { db, session }
    }

    /// Start an explicit transaction.
    pub fn begin(&mut self) -> Result<()> {
        let ctx = self.db.begin_explicit(false)?;
        self.session.set_current_txn(ctx);
        Ok(())
    }

    /// Commit the active explicit transaction (blocks on fsync).
    pub fn commit(&mut self) -> Result<()> {
        if let Some(ctx) = self.session.take_current_txn() {
            self.db.commit(ctx)?;
        }
        Ok(())
    }

    /// Rollback the active explicit transaction.
    pub fn rollback(&mut self) -> Result<()> {
        if let Some(ctx) = self.session.take_current_txn() {
            self.db.rollback(ctx)?;
        }
        Ok(())
    }

    /// Execute SQL using session state.
    ///
    /// - If explicit txn active: uses it (writes don't auto-commit)
    /// - If no explicit txn: auto-commits each statement
    /// - Handles BEGIN/COMMIT/ROLLBACK SQL via execute_unified
    /// - Returns materialized result (rows available for processing)
    pub async fn execute(&mut self, sql: &str) -> Result<ExecutionResult> {
        let exec_ctx = ExecutionCtx::from_session(&self.session);
        let plan = self.db.parse_and_bind(sql, &exec_ctx)?;
        let txn_ctx = self.session.take_current_txn();
        let (output, returned_ctx) = self.db.execute_plan(plan, &exec_ctx, txn_ctx).await?;
        if let Some(ctx) = returned_ctx {
            self.session.set_current_txn(ctx);
        }
        output.into_result().await
    }

    /// Execute SQL in auto-commit mode, ignoring any active txn.
    ///
    /// Always creates an implicit transaction per statement.
    /// Returns materialized result for internal processing.
    pub async fn execute_inner_sql(&self, sql: &str) -> Result<ExecutionResult> {
        let exec_ctx = ExecutionCtx::from_session(&self.session);
        let plan = self.db.parse_and_bind(sql, &exec_ctx)?;
        let (output, _) = self.db.execute_plan(plan, &exec_ctx, None).await?;
        output.into_result().await
    }
}

impl Drop for InnerSession {
    fn drop(&mut self) {
        if let Some(ctx) = self.session.take_current_txn() {
            let _ = self.db.rollback(ctx);
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;
    use crate::DatabaseConfig;
    use tempfile::tempdir;

    fn create_test_db() -> (Arc<Database>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Arc::new(Database::open(config).unwrap());
        (db, dir)
    }

    #[tokio::test]
    async fn test_inner_session_autocommit_select() {
        let (db, _dir) = create_test_db();
        let sess = db.new_inner_session("default");
        let result = sess.execute_inner_sql("SELECT 1 + 1").await.unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(*rows[0].get(0).unwrap(), Value::BigInt(2));
            }
            _ => panic!("expected rows"),
        }
    }

    #[tokio::test]
    async fn test_inner_session_autocommit_ddl_dml_read() {
        let (db, _dir) = create_test_db();
        let sess = db.new_inner_session("default");

        sess.execute_inner_sql("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(100))")
            .await
            .unwrap();
        sess.execute_inner_sql("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
            .await
            .unwrap();

        let result = sess
            .execute_inner_sql("SELECT id, name FROM t ORDER BY id")
            .await
            .unwrap();
        match result {
            ExecutionResult::Rows { schema, rows } => {
                assert_eq!(schema.columns().len(), 2);
                assert_eq!(schema.columns()[0].name(), "id");
                assert_eq!(schema.columns()[1].name(), "name");
                assert_eq!(rows.len(), 2);
                assert_eq!(*rows[0].get(0).unwrap(), Value::Int(1));
                assert_eq!(*rows[0].get(1).unwrap(), Value::String("alice".into()));
                assert_eq!(*rows[1].get(0).unwrap(), Value::Int(2));
                assert_eq!(*rows[1].get(1).unwrap(), Value::String("bob".into()));
            }
            _ => panic!("expected rows"),
        }
    }

    #[tokio::test]
    async fn test_inner_session_explicit_txn() {
        let (db, _dir) = create_test_db();
        let mut sess = db.new_inner_session("default");

        sess.execute_inner_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();

        // Begin explicit txn, insert, read within txn
        sess.begin().unwrap();
        sess.execute("INSERT INTO t VALUES (1, 100)").await.unwrap();

        let result = sess.execute("SELECT v FROM t WHERE id = 1").await.unwrap();
        match &result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(*rows[0].get(0).unwrap(), Value::Int(100));
            }
            _ => panic!("expected rows"),
        }

        sess.commit().unwrap();

        // After commit, still visible via auto-commit read
        let result = sess
            .execute_inner_sql("SELECT v FROM t WHERE id = 1")
            .await
            .unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(*rows[0].get(0).unwrap(), Value::Int(100));
            }
            _ => panic!("expected rows"),
        }
    }

    #[tokio::test]
    async fn test_inner_session_rollback() {
        let (db, _dir) = create_test_db();
        let mut sess = db.new_inner_session("default");

        sess.execute_inner_sql("CREATE TABLE t (id INT PRIMARY KEY)")
            .await
            .unwrap();
        sess.execute_inner_sql("INSERT INTO t VALUES (1)")
            .await
            .unwrap();

        sess.begin().unwrap();
        sess.execute("INSERT INTO t VALUES (2)").await.unwrap();
        sess.rollback().unwrap();

        let result = sess
            .execute_inner_sql("SELECT id FROM t ORDER BY id")
            .await
            .unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(*rows[0].get(0).unwrap(), Value::Int(1));
            }
            _ => panic!("expected rows"),
        }
    }

    #[tokio::test]
    async fn test_inner_session_execute_txn_via_sql() {
        let (db, _dir) = create_test_db();
        let mut sess = db.new_inner_session("default");

        sess.execute_inner_sql("CREATE TABLE t (id INT PRIMARY KEY)")
            .await
            .unwrap();

        sess.execute("BEGIN").await.unwrap();
        sess.execute("INSERT INTO t VALUES (1)").await.unwrap();
        sess.execute("INSERT INTO t VALUES (2)").await.unwrap();
        sess.execute("COMMIT").await.unwrap();

        let result = sess
            .execute_inner_sql("SELECT id FROM t ORDER BY id")
            .await
            .unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 2),
            _ => panic!("expected rows"),
        }
    }

    #[tokio::test]
    async fn test_inner_session_drop_rollbacks() {
        let (db, _dir) = create_test_db();

        let sess = db.new_inner_session("default");
        sess.execute_inner_sql("CREATE TABLE t (id INT PRIMARY KEY)")
            .await
            .unwrap();
        drop(sess);

        {
            let mut sess = db.new_inner_session("default");
            sess.begin().unwrap();
            sess.execute("INSERT INTO t VALUES (99)").await.unwrap();
            // sess dropped here — Drop impl rollbacks active txn
        }

        let sess = db.new_inner_session("default");
        let result = sess.execute_inner_sql("SELECT id FROM t").await.unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 0),
            _ => panic!("expected rows"),
        }
    }

    #[tokio::test]
    async fn test_inner_session_affected_count() {
        let (db, _dir) = create_test_db();
        let sess = db.new_inner_session("default");

        sess.execute_inner_sql("CREATE TABLE t (id INT PRIMARY KEY)")
            .await
            .unwrap();
        let result = sess
            .execute_inner_sql("INSERT INTO t VALUES (1), (2), (3)")
            .await
            .unwrap();
        match result {
            ExecutionResult::Affected { count } => assert_eq!(count, 3),
            _ => panic!("expected affected"),
        }
    }

    #[tokio::test]
    async fn test_inner_session_different_db() {
        let (db, _dir) = create_test_db();

        // Create table in "test" schema
        let sess = db.new_inner_session("test");
        sess.execute_inner_sql("CREATE TABLE t (id INT PRIMARY KEY)")
            .await
            .unwrap();
        sess.execute_inner_sql("INSERT INTO t VALUES (1)")
            .await
            .unwrap();

        // Read from "test" schema
        let result = sess.execute_inner_sql("SELECT id FROM t").await.unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!("expected rows"),
        }

        // Different session on "default" should NOT see it
        let sess2 = db.new_inner_session("default");
        let result = sess2.execute_inner_sql("SELECT id FROM t").await;
        assert!(result.is_err());
    }
}
