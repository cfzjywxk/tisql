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

pub use simple::SimpleExecutor;

use crate::catalog::Catalog;
use crate::error::Result;
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
/// - Read statements: TxnService.snapshot() allocates start_ts
/// - Write statements: TxnService.begin() + commit() with durability
pub trait Executor {
    /// Execute a logical plan through the transaction service.
    ///
    /// For read operations (SELECT):
    /// 1. Creates a snapshot via txn_service.snapshot()
    /// 2. Reads are performed at snapshot's start_ts
    ///
    /// For write operations (INSERT/UPDATE/DELETE):
    /// 1. Creates a transaction via txn_service.begin()
    /// 2. Writes are buffered in transaction
    /// 3. Transaction is committed with durability
    fn execute<T: TxnService, C: Catalog>(
        &self,
        plan: LogicalPlan,
        txn_service: &T,
        catalog: &C,
    ) -> Result<ExecutionResult>;
}
