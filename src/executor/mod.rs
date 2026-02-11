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

mod simple;

// Crate-internal implementation
pub(crate) use simple::SimpleExecutor;

use crate::catalog::Catalog;
use crate::error::Result;
use crate::session::{ExecutionCtx, Session};
use crate::sql::LogicalPlan;
use crate::transaction::{TxnCtx, TxnService};
use crate::types::{Row, Schema};

/// Query execution result (materialized — all rows in memory).
pub enum ExecutionResult {
    /// Query returned rows
    Rows { schema: Schema, rows: Vec<Row> },
    /// DML affected rows
    Affected { count: u64 },
    /// DDL success
    Ok,
}

// ============================================================================
// Volcano-Style Execution (Streaming)
// ============================================================================

/// Lazy row-producing execution handle (volcano-style).
///
/// Wraps a materialized result set and yields rows one at a time via `next()`.
/// Created and consumed inside a single blocking task — not `Send`.
///
/// Future optimization: replace the inner `Vec<Row>` with a true pull-based
/// iterator over the storage engine (no materialization).
pub struct Execution {
    rows: std::vec::IntoIter<Row>,
}

impl Execution {
    pub(crate) fn new(rows: Vec<Row>) -> Self {
        Self {
            rows: rows.into_iter(),
        }
    }

    /// Pull the next row (volcano-style).
    #[inline]
    pub fn next(&mut self) -> Option<Row> {
        self.rows.next()
    }
}

/// Output from plan execution.
///
/// Unlike `ExecutionResult` (which materializes all rows), `ExecutionOutput`
/// wraps an `Execution` handle that yields rows lazily.
pub enum ExecutionOutput {
    /// Read query — stream rows via Execution.
    Rows(Execution),
    /// DML — affected row count.
    Affected { count: u64 },
    /// DDL/session command.
    Ok,
}

impl From<ExecutionResult> for ExecutionOutput {
    fn from(result: ExecutionResult) -> Self {
        match result {
            ExecutionResult::Rows { rows, .. } => ExecutionOutput::Rows(Execution::new(rows)),
            ExecutionResult::Affected { count } => ExecutionOutput::Affected { count },
            ExecutionResult::Ok => ExecutionOutput::Ok,
        }
    }
}

/// Executor trait - executes plans through TxnService
///
/// All statement execution goes through the transaction service:
/// - Read statements: TxnService.begin(true) creates read-only transaction
/// - Write statements: TxnService.begin(false) + commit() with durability
/// - Transaction control: BEGIN/COMMIT/ROLLBACK managed via Session
pub trait Executor {
    /// Execute a logical plan through the transaction service.
    ///
    /// For read operations (SELECT):
    /// 1. Creates a read-only transaction via txn_service.begin(true)
    /// 2. Reads are performed at transaction's start_ts
    ///
    /// For write operations (INSERT/UPDATE/DELETE):
    /// 1. Creates a transaction via txn_service.begin(false)
    /// 2. Writes are buffered in transaction
    /// 3. Transaction is committed with durability
    fn execute<T: TxnService, C: Catalog>(
        &self,
        plan: LogicalPlan,
        txn_service: &T,
        catalog: &C,
    ) -> Result<ExecutionResult>;

    /// Execute a logical plan with session context for explicit transactions.
    ///
    /// This method is used when a Session is available to manage transaction state
    /// across multiple statements. It handles:
    /// - BEGIN: Creates explicit transaction, stores in session
    /// - COMMIT: Commits session's active transaction
    /// - ROLLBACK: Rolls back session's active transaction
    /// - Other statements: Uses session's active transaction if present,
    ///   otherwise creates implicit transaction
    fn execute_with_session<T: TxnService, C: Catalog>(
        &self,
        plan: LogicalPlan,
        txn_service: &T,
        catalog: &C,
        session: &mut Session,
    ) -> Result<ExecutionResult>;

    /// Execute a logical plan with optional TxnCtx (unified entry point).
    ///
    /// This is the new unified execution method that:
    /// - Takes ExecutionCtx with session variables
    /// - Takes optional TxnCtx for explicit transactions
    /// - Returns the (potentially updated) TxnCtx along with the result
    /// - Transaction control (BEGIN/COMMIT/ROLLBACK) is handled at protocol layer
    ///
    /// ## Arguments
    /// - `plan`: The logical plan to execute
    /// - `exec_ctx`: Execution context with session variables
    /// - `txn_service`: Transaction service for data access
    /// - `catalog`: Schema catalog
    /// - `txn_ctx`: Optional TxnCtx for explicit transactions
    ///
    /// ## Returns
    /// - `(ExecutionResult, Option<TxnCtx>)`: Result and returned TxnCtx (if still active)
    fn execute_unified<T: TxnService, C: Catalog>(
        &self,
        plan: LogicalPlan,
        exec_ctx: &ExecutionCtx,
        txn_service: &T,
        catalog: &C,
        txn_ctx: Option<TxnCtx>,
    ) -> Result<(ExecutionResult, Option<TxnCtx>)>;
}
