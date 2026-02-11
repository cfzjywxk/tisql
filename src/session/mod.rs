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

//! Session management for TiSQL.
//!
//! This module provides session-related abstractions following TiDB's design:
//!
//! - [`Session`]: Per-connection state, created when a MySQL connection is established
//! - [`SessionVars`]: Session-level configuration and variables
//! - [`QueryCtx`]: Per-statement context, created before each SQL execution
//!
//! ## Architecture
//!
//! ```text
//! MySQL Connection
//!       │
//!       ▼
//!    Session (connection lifetime)
//!       │
//!       ├── SessionVars (session configuration)
//!       │     ├── current_db
//!       │     ├── isolation_level
//!       │     ├── autocommit
//!       │     └── timezone, sql_mode, etc.
//!       │
//!       ├── current_txn: Option<TxnCtx> (explicit transaction state)
//!       │
//!       └── For each SQL statement:
//!             └── QueryCtx (statement lifetime)
//!                   ├── statement_id
//!                   ├── priority
//!                   └── inherited from SessionVars
//! ```
//!
//! ## Explicit Transaction Lifecycle
//!
//! ```text
//! BEGIN ─────────► current_txn = Some(TxnCtx)
//!                        │
//!           ┌────────────┴────────────┐
//!           │ (execute statements     │
//!           │  using current_txn)     │
//!           └────────────┬────────────┘
//!                        │
//!        ┌───────────────┼───────────────┐
//!        │               │               │
//!    COMMIT          (error)        ROLLBACK
//!        │               │               │
//!        ▼               ▼               ▼
//!   current_txn = None  (keep txn)  current_txn = None
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::transaction::{IsolationLevel, TxnCtx};
use crate::types::Timestamp;

// ============================================================================
// Session ID Generator
// ============================================================================

/// Global session ID counter
static SESSION_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Global statement context ID counter
static STMT_CTX_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Allocate a new unique session ID.
pub fn alloc_session_id() -> u64 {
    SESSION_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Allocate a new unique statement context ID.
fn alloc_stmt_ctx_id() -> u64 {
    STMT_CTX_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

// ============================================================================
// Session Registry — tracks active explicit transactions for GC safe point
// ============================================================================

/// Registry tracking active explicit transactions across all sessions.
///
/// Used to compute the GC safe point: versions older than the minimum
/// active start_ts cannot be garbage collected because an active reader
/// may still need them.
pub struct SessionRegistry {
    active_txns: parking_lot::RwLock<HashMap<u64, Timestamp>>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self {
            active_txns: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Register an active explicit transaction for a session.
    pub fn register(&self, session_id: u64, start_ts: Timestamp) {
        self.active_txns.write().insert(session_id, start_ts);
    }

    /// Unregister a session's transaction (on COMMIT/ROLLBACK/disconnect).
    pub fn unregister(&self, session_id: u64) {
        self.active_txns.write().remove(&session_id);
    }

    /// Return the minimum start_ts among all active explicit transactions.
    ///
    /// Returns `None` if no explicit transactions are active.
    pub fn min_start_ts(&self) -> Option<Timestamp> {
        let guard = self.active_txns.read();
        guard.values().copied().min()
    }
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Execution Context (Read-Only Session Snapshot)
// ============================================================================

/// Read-only execution context extracted from Session.
///
/// Contains session variables needed during query execution. This is a snapshot
/// of session state at the time of extraction - changes to session after
/// extraction won't be reflected.
///
/// This struct is passed through the execution path (WorkerPool -> Database ->
/// SQLEngine -> Executor) to provide access to session variables without
/// passing the mutable Session itself.
#[derive(Clone, Debug)]
pub struct ExecutionCtx {
    /// Current database for SQL binding
    pub current_db: String,

    /// Transaction isolation level
    pub isolation_level: IsolationLevel,

    /// Autocommit mode
    pub autocommit: bool,
    // Future: Add more session variables as needed
    // - timezone
    // - sql_mode
    // - max_execution_time
    // - etc.
}

impl ExecutionCtx {
    /// Create an ExecutionCtx from a Session.
    ///
    /// This extracts the relevant session variables as a read-only snapshot.
    pub fn from_session(session: &Session) -> Self {
        Self {
            current_db: session.vars.current_db.clone(),
            isolation_level: session.vars.isolation_level,
            autocommit: session.vars.autocommit,
        }
    }

    /// Create an ExecutionCtx with just a database name (for backward compat).
    pub fn with_db(current_db: impl Into<String>) -> Self {
        Self {
            current_db: current_db.into(),
            isolation_level: IsolationLevel::default(),
            autocommit: true,
        }
    }
}

impl Default for ExecutionCtx {
    fn default() -> Self {
        Self {
            current_db: "default".to_string(),
            isolation_level: IsolationLevel::default(),
            autocommit: true,
        }
    }
}

// ============================================================================
// Session Variables
// ============================================================================

/// Session-level variables and configuration.
///
/// These are set per-connection and persist for the connection lifetime.
/// Similar to TiDB's `SessionVars`.
#[derive(Clone, Debug)]
pub struct SessionVars {
    /// Current database/schema
    pub current_db: String,

    /// Transaction isolation level
    pub isolation_level: IsolationLevel,

    /// Autocommit mode (each statement is its own transaction)
    pub autocommit: bool,

    /// Connection character set (for future use)
    pub charset: String,

    /// Connection collation (for future use)
    pub collation: String,
    // Future: Add more session variables
    // - timezone
    // - sql_mode
    // - max_execution_time
    // - innodb_lock_wait_timeout
    // - etc.
}

impl Default for SessionVars {
    fn default() -> Self {
        Self {
            current_db: "default".to_string(),
            isolation_level: IsolationLevel::default(),
            autocommit: true,
            charset: "utf8mb4".to_string(),
            collation: "utf8mb4_general_ci".to_string(),
        }
    }
}

impl SessionVars {
    /// Create new session variables with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the current database.
    pub fn set_current_db(&mut self, db: impl Into<String>) {
        self.current_db = db.into();
    }

    /// Set autocommit mode.
    pub fn set_autocommit(&mut self, autocommit: bool) {
        self.autocommit = autocommit;
    }

    /// Set isolation level.
    pub fn set_isolation_level(&mut self, level: IsolationLevel) {
        self.isolation_level = level;
    }
}

// ============================================================================
// Session
// ============================================================================

/// Session represents a MySQL client connection.
///
/// Each MySQL connection creates a new Session instance that lives
/// for the duration of the connection. Sessions hold connection-specific
/// state like current database, session variables, and prepared statements.
///
/// Similar to TiDB's `session` struct in `pkg/session/session.go`.
pub struct Session {
    /// Unique session ID (connection ID in MySQL terms)
    id: u64,

    /// Session-level variables
    vars: SessionVars,

    /// Active transaction context for explicit transactions.
    ///
    /// - `None`: No explicit transaction active (auto-commit mode)
    /// - `Some(ctx)`: Explicit transaction in progress (started with BEGIN)
    ///
    /// When `current_txn` is `Some`, all statements use this context
    /// instead of creating implicit transactions.
    current_txn: Option<TxnCtx>,
    // Future: Add more session state
    // - prepared_stmts: HashMap<u32, PreparedStmt>
    // - last_insert_id: u64
    // - found_rows: u64
    // - user: Option<UserIdentity>
}

impl Session {
    /// Create a new session with a unique ID.
    pub fn new() -> Self {
        Self {
            id: alloc_session_id(),
            vars: SessionVars::new(),
            current_txn: None,
        }
    }

    /// Create a new session with a specific ID (for testing).
    pub fn with_id(id: u64) -> Self {
        Self {
            id,
            vars: SessionVars::new(),
            current_txn: None,
        }
    }

    /// Get the session ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get session variables (immutable).
    pub fn vars(&self) -> &SessionVars {
        &self.vars
    }

    /// Get session variables (mutable).
    pub fn vars_mut(&mut self) -> &mut SessionVars {
        &mut self.vars
    }

    /// Set the current database.
    pub fn set_current_db(&mut self, db: impl Into<String>) {
        self.vars.set_current_db(db);
    }

    /// Get the current database.
    pub fn current_db(&self) -> &str {
        &self.vars.current_db
    }

    /// Create a QueryCtx for executing a statement.
    ///
    /// This should be called before each SQL statement execution.
    /// The QueryCtx inherits relevant settings from SessionVars.
    pub fn new_query_ctx(&self) -> QueryCtx {
        QueryCtx::from_session(self)
    }

    // ========================================================================
    // Transaction State Management
    // ========================================================================

    /// Check if there is an active explicit transaction.
    #[inline]
    pub fn has_active_txn(&self) -> bool {
        self.current_txn.is_some()
    }

    /// Get a reference to the current transaction context, if any.
    #[inline]
    pub fn current_txn(&self) -> Option<&TxnCtx> {
        self.current_txn.as_ref()
    }

    /// Get a mutable reference to the current transaction context, if any.
    #[inline]
    pub fn current_txn_mut(&mut self) -> Option<&mut TxnCtx> {
        self.current_txn.as_mut()
    }

    /// Set the current transaction context.
    ///
    /// Called when BEGIN/START TRANSACTION is executed.
    pub fn set_current_txn(&mut self, ctx: TxnCtx) {
        self.current_txn = Some(ctx);
    }

    /// Take and clear the current transaction context.
    ///
    /// Called when COMMIT or ROLLBACK is executed.
    /// Returns the transaction context so it can be committed or rolled back.
    pub fn take_current_txn(&mut self) -> Option<TxnCtx> {
        self.current_txn.take()
    }

    /// Clear the current transaction without returning it.
    ///
    /// Used when the transaction was already consumed (e.g., on error).
    pub fn clear_current_txn(&mut self) {
        self.current_txn = None;
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Query Context (Statement Context)
// ============================================================================

/// Statement priority level.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Priority {
    #[default]
    Normal,
    Low,
    High,
}

/// Per-statement execution context.
///
/// QueryCtx is created before each SQL statement execution and holds
/// statement-specific state and configuration. Similar to TiDB's
/// `StatementContext` in `pkg/sessionctx/stmtctx/stmtctx.go`.
///
/// ## Lifecycle
///
/// 1. Created via `Session::new_query_ctx()` before statement execution
/// 2. Passed to SQLEngine for query processing
/// 3. Accumulates execution state (affected rows, warnings, etc.)
/// 4. Discarded after statement completes
#[derive(Clone, Debug)]
pub struct QueryCtx {
    /// Unique statement context ID
    id: u64,

    /// Session ID this context belongs to
    session_id: u64,

    /// Current database for this statement (inherited from session)
    pub current_db: String,

    /// Isolation level for this statement
    pub isolation_level: IsolationLevel,

    /// Statement priority
    pub priority: Priority,

    /// Whether this is an internal SQL (not from user)
    pub is_internal: bool,

    // Execution state (populated during execution)
    /// Number of rows affected by DML
    affected_rows: u64,

    /// Number of rows found (for SELECT)
    found_rows: u64,

    /// Last insert ID (for auto-increment)
    last_insert_id: u64,
    // Future: Add more statement context fields
    // - warnings: Vec<Warning>
    // - in_insert_stmt, in_update_stmt, etc.
    // - memory_tracker
    // - plan_digest
    // - original_sql
}

impl QueryCtx {
    /// Create a new QueryCtx with default values.
    pub fn new() -> Self {
        Self {
            id: alloc_stmt_ctx_id(),
            session_id: 0,
            current_db: "default".to_string(),
            isolation_level: IsolationLevel::default(),
            priority: Priority::default(),
            is_internal: false,
            affected_rows: 0,
            found_rows: 0,
            last_insert_id: 0,
        }
    }

    /// Create a QueryCtx from a Session.
    ///
    /// This is the standard way to create a QueryCtx - it inherits
    /// relevant settings from the session.
    pub fn from_session(session: &Session) -> Self {
        Self {
            id: alloc_stmt_ctx_id(),
            session_id: session.id(),
            current_db: session.vars.current_db.clone(),
            isolation_level: session.vars.isolation_level,
            priority: Priority::default(),
            is_internal: false,
            affected_rows: 0,
            found_rows: 0,
            last_insert_id: 0,
        }
    }

    /// Get the statement context ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the session ID.
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    /// Set the number of affected rows.
    pub fn set_affected_rows(&mut self, count: u64) {
        self.affected_rows = count;
    }

    /// Get the number of affected rows.
    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    /// Set the number of found rows.
    pub fn set_found_rows(&mut self, count: u64) {
        self.found_rows = count;
    }

    /// Get the number of found rows.
    pub fn found_rows(&self) -> u64 {
        self.found_rows
    }

    /// Set the last insert ID.
    pub fn set_last_insert_id(&mut self, id: u64) {
        self.last_insert_id = id;
    }

    /// Get the last insert ID.
    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }

    /// Set statement priority.
    pub fn set_priority(&mut self, priority: Priority) {
        self.priority = priority;
    }

    /// Mark this as an internal SQL statement.
    pub fn set_internal(&mut self, internal: bool) {
        self.is_internal = internal;
    }
}

impl Default for QueryCtx {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_creation() {
        let s1 = Session::new();
        let s2 = Session::new();

        // Sessions should have unique IDs
        assert_ne!(s1.id(), s2.id());

        // Default values
        assert_eq!(s1.current_db(), "default");
        assert!(s1.vars().autocommit);
    }

    #[test]
    fn test_session_vars() {
        let mut session = Session::new();

        session.set_current_db("mydb");
        assert_eq!(session.current_db(), "mydb");

        session.vars_mut().set_autocommit(false);
        assert!(!session.vars().autocommit);
    }

    #[test]
    fn test_query_ctx_from_session() {
        let mut session = Session::new();
        session.set_current_db("testdb");

        let ctx = session.new_query_ctx();

        assert_eq!(ctx.session_id(), session.id());
        assert_eq!(ctx.current_db, "testdb");
        assert_eq!(ctx.priority, Priority::Normal);
    }

    #[test]
    fn test_query_ctx_execution_state() {
        let mut ctx = QueryCtx::new();

        ctx.set_affected_rows(10);
        assert_eq!(ctx.affected_rows(), 10);

        ctx.set_found_rows(100);
        assert_eq!(ctx.found_rows(), 100);

        ctx.set_last_insert_id(42);
        assert_eq!(ctx.last_insert_id(), 42);
    }

    #[test]
    fn test_unique_stmt_ctx_ids() {
        let ctx1 = QueryCtx::new();
        let ctx2 = QueryCtx::new();

        assert_ne!(ctx1.id(), ctx2.id());
    }

    #[test]
    fn test_session_transaction_state() {
        use crate::transaction::TxnState;

        let mut session = Session::new();

        // Initially no active transaction
        assert!(!session.has_active_txn());
        assert!(session.current_txn().is_none());

        // Create a mock TxnCtx (using internal constructor for testing)
        let ctx = TxnCtx::new_for_test(1, 100, false, true);
        assert!(ctx.is_explicit());

        // Set active transaction
        session.set_current_txn(ctx);
        assert!(session.has_active_txn());
        assert!(session.current_txn().is_some());
        assert_eq!(session.current_txn().unwrap().start_ts(), 100);

        // Take transaction
        let taken = session.take_current_txn();
        assert!(taken.is_some());
        assert!(!session.has_active_txn());
        assert_eq!(taken.unwrap().state(), TxnState::Running);
    }

    #[test]
    fn test_session_transaction_mutable_access() {
        let mut session = Session::new();
        let ctx = TxnCtx::new_for_test(2, 200, false, true);

        session.set_current_txn(ctx);

        // Mutable access
        let ctx_mut = session.current_txn_mut().unwrap();
        assert_eq!(ctx_mut.start_ts(), 200);
    }
}
