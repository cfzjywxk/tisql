mod simple;

pub use simple::SimpleExecutor;

use crate::catalog::Catalog;
use crate::error::Result;
use crate::sql::LogicalPlan;
use crate::storage::StorageEngine;
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

/// Executor trait
pub trait Executor {
    fn execute<S: StorageEngine, C: Catalog>(
        &self,
        plan: LogicalPlan,
        storage: &S,
        catalog: &C,
    ) -> Result<ExecutionResult>;
}
