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

use std::future::Future;
use std::pin::Pin;

use crate::catalog::types::{IndexId, Row, Schema, TableId};
use crate::catalog::Catalog;
use crate::session::ExecutionCtx;
use crate::sql::LogicalPlan;
use crate::transaction::{TxnCtx, TxnService};
use crate::util::error::Result;

/// Side effect from a DDL operation that requires post-execution action.
pub enum DdlEffect {
    /// A table was created and should have a dedicated tablet mounted.
    TableCreated { table_id: TableId },
    /// A local index was created and should have a dedicated tablet mounted.
    IndexCreated {
        table_id: TableId,
        index_id: IndexId,
    },
    /// A table drop committed and GC worker should process pending retire tasks.
    TableDropped,
    /// An index drop committed and GC worker should process pending retire tasks.
    IndexDropped,
}

/// Query execution result (materialized — all rows in memory).
pub enum ExecutionResult {
    /// Query returned rows
    Rows { schema: Schema, rows: Vec<Row> },
    /// DML affected rows
    Affected { count: u64 },
    /// DDL success
    Ok,
    /// DDL success with a side effect requiring post-execution action.
    OkWithEffect(DdlEffect),
}

// ============================================================================
// Volcano-Style Execution (Streaming)
// ============================================================================

/// Type-erased async row producer for the volcano-style pipeline.
///
/// `Operator<I>` is generic over `I: MvccIterator`. To return it through
/// the non-generic `Execution`, we erase the type via this trait.
pub(crate) trait RowPuller: Send {
    fn next(&mut self) -> Pin<Box<dyn Future<Output = Result<Option<Row>>> + Send + '_>>;
}

/// Backward-compat row puller wrapping a materialized `Vec<Row>`.
///
/// Used for `From<ExecutionResult>` conversion (write results). In production,
/// write results are always `Affected`/`Ok`, so this is effectively dead code
/// in the hot path.
struct VecPuller(std::vec::IntoIter<Row>);

impl RowPuller for VecPuller {
    fn next(&mut self) -> Pin<Box<dyn Future<Output = Result<Option<Row>>> + Send + '_>> {
        Box::pin(std::future::ready(Ok(self.0.next())))
    }
}

/// Lazy row-producing execution handle (volcano-style, async).
///
/// Wraps a `RowPuller` that yields rows one at a time via async `next()`.
/// In the production path, the puller is a live operator tree backed by
/// storage iterators — zero materialization.
pub struct Execution {
    puller: Box<dyn RowPuller>,
}

impl Execution {
    /// Create from a type-erased row puller (production path — streaming).
    pub(crate) fn from_puller(puller: Box<dyn RowPuller>) -> Self {
        Self { puller }
    }

    /// Create from a materialized row vec (backward-compat / test path).
    pub(crate) fn from_rows(rows: Vec<Row>) -> Self {
        Self {
            puller: Box::new(VecPuller(rows.into_iter())),
        }
    }

    /// Pull the next row (volcano-style, async).
    pub async fn next(&mut self) -> Result<Option<Row>> {
        self.puller.next().await
    }
}

/// Output from plan execution.
///
/// Unlike `ExecutionResult` (which materializes all rows), `ExecutionOutput`
/// wraps an `Execution` handle that yields rows lazily via async `next()`.
pub enum ExecutionOutput {
    /// Read query — stream rows via Execution.
    Rows { schema: Schema, exec: Execution },
    /// DML — affected row count.
    Affected { count: u64 },
    /// DDL/session command.
    Ok,
    /// DDL success with side effect.
    OkWithEffect(DdlEffect),
}

impl ExecutionOutput {
    /// Materialize into an `ExecutionResult` (test / legacy paths only).
    pub(crate) async fn into_result(self) -> Result<ExecutionResult> {
        match self {
            ExecutionOutput::Rows { schema, mut exec } => {
                let mut rows = Vec::new();
                while let Some(row) = exec.next().await? {
                    rows.push(row);
                }
                Ok(ExecutionResult::Rows { schema, rows })
            }
            ExecutionOutput::Affected { count } => Ok(ExecutionResult::Affected { count }),
            ExecutionOutput::Ok => Ok(ExecutionResult::Ok),
            ExecutionOutput::OkWithEffect(effect) => Ok(ExecutionResult::OkWithEffect(effect)),
        }
    }
}

impl From<ExecutionResult> for ExecutionOutput {
    fn from(result: ExecutionResult) -> Self {
        match result {
            ExecutionResult::Rows { schema, rows } => ExecutionOutput::Rows {
                schema,
                exec: Execution::from_rows(rows),
            },
            ExecutionResult::Affected { count } => ExecutionOutput::Affected { count },
            ExecutionResult::Ok => ExecutionOutput::Ok,
            ExecutionResult::OkWithEffect(effect) => ExecutionOutput::OkWithEffect(effect),
        }
    }
}

/// Executor trait - executes plans through TxnService
///
/// All statement execution goes through the transaction service:
/// - Read statements: TxnService.begin(true) creates read-only transaction
/// - Write statements: TxnService.begin(false) + commit() with durability
/// - Transaction control: BEGIN/COMMIT/ROLLBACK managed via Session
pub trait Executor: Send + Sync {
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
    /// - `(Result<ExecutionOutput>, Option<TxnCtx>)`:
    ///   - `Ok(output)` on successful statement execution
    ///   - `Err(error)` on statement failure
    ///   - `Option<TxnCtx>` always carries the post-statement transaction context
    ///     (for explicit transactions, statement errors should keep the txn open)
    fn execute_unified<T: TxnService, C: Catalog>(
        &self,
        plan: LogicalPlan,
        exec_ctx: &ExecutionCtx,
        txn_service: &T,
        catalog: &C,
        txn_ctx: Option<TxnCtx>,
    ) -> impl std::future::Future<Output = (Result<ExecutionOutput>, Option<TxnCtx>)> + Send;
}
