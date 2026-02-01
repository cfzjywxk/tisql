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
use crate::session::Session;
use crate::sql::LogicalPlan;
use crate::transaction::TxnService;
use crate::types::{Row, Schema};

/// Query execution result
pub enum ExecutionResult {
    /// Query returned rows
    Rows { schema: Schema, rows: Vec<Row> },
    /// DML affected rows
    Affected { count: u64 },
    /// DDL success
    Ok,
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
}
