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

//! Volcano-style executor with true pull-based operator tree.
//!
//! Each operator (Scan, Project, Filter, Limit, Sort, Aggregate) is its own
//! struct with `open()` / `next()` / `schema()`. Parents pull rows from
//! children one at a time. Pipelining operators (Filter, Project, Limit)
//! never materialize intermediate results.
//!
//! All statement execution goes through the transaction service:
//! - Read statements: Use begin(read_only=true) for snapshot reads
//! - Write statements: Use begin(read_only=false) with commit

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::catalog::types::{ColumnInfo, DataType, Row, Schema, Value};
use crate::catalog::{Catalog, TableDef};
use crate::kernel::execution::{
    DeleteRequest, ExecutionBackend, ExecutionRow, ExecutionRowStream, InsertRequest, InsertRow,
    LockRequest, LogicalPointKey, LogicalScanBounds, PointGetRequest, ProjectionSpec, RowKey,
    ScanBound, ScanRequest, UpdateRequest, UpdateRow, WriteRequest,
};
use crate::session::{ExecutionCtx, StatementCtx};
use crate::sql::{AggFunc, BinaryOp, Expr, LogicalPlan, OrderByExpr, UnaryOp};
use crate::transaction::{TxnCtx, TxnService};
use crate::util::error::{Result, TiSqlError};

use super::{DdlEffect, Execution, ExecutionOutput, Executor, RowPuller};

#[path = "simple_support.rs"]
mod support;

use self::support::*;

// ============================================================================
// Operator Structs
// ============================================================================

/// Table scan operator — reads rows from storage via TxnScanCursor.
struct ScanOp<I: ExecutionRowStream> {
    iter: I,
    schema: Schema,
    filter: Option<Expr>,
    projection: Option<Vec<usize>>,
}

/// VALUES clause operator — yields literal rows.
struct ValuesOp {
    rows: std::vec::IntoIter<Row>,
    schema: Schema,
}

/// Empty result operator — always returns None.
struct EmptyOp {
    schema: Schema,
}

/// Projection operator — evaluates expressions on each row.
struct ProjectOp<I: ExecutionRowStream> {
    child: Box<Operator<I>>,
    exprs: Vec<(Expr, String)>,
    schema: Schema,
}

/// Filter operator — keeps rows matching predicate.
struct FilterOp<I: ExecutionRowStream> {
    child: Box<Operator<I>>,
    predicate: Expr,
}

/// Limit/offset operator — caps output row count.
struct LimitOp<I: ExecutionRowStream> {
    child: Box<Operator<I>>,
    limit: Option<usize>,
    offset: usize,
    skipped: usize,
    returned: usize,
}

/// Sort operator — materializes all rows on first next(), then yields sorted.
struct SortOp<I: ExecutionRowStream> {
    child: Box<Operator<I>>,
    order_by: Vec<OrderByExpr>,
    sorted_rows: Option<std::vec::IntoIter<Row>>,
}

/// Aggregate operator — materializes all rows on first next(), computes aggregates.
struct AggregateOp<I: ExecutionRowStream> {
    child: Box<Operator<I>>,
    group_by: Vec<Expr>,
    agg_exprs: Vec<(AggFunc, Expr, String)>,
    result: Option<std::vec::IntoIter<Row>>,
    schema: Schema,
}

// ============================================================================
// Operator Enum (concrete dispatch, no dyn traits)
// ============================================================================

/// Volcano-style operator tree node.
///
/// Generic over `I: TxnScanCursor` — the storage iterator type, known at
/// compile time via `TxnService::ScanCursor`.
enum Operator<I: ExecutionRowStream> {
    Scan(ScanOp<I>),
    Values(ValuesOp),
    Empty(EmptyOp),
    Project(ProjectOp<I>),
    Filter(FilterOp<I>),
    Limit(LimitOp<I>),
    Sort(SortOp<I>),
    Aggregate(AggregateOp<I>),
}

impl<I: ExecutionRowStream> Operator<I> {
    /// Cascade open to children. Leaf nodes are no-ops.
    /// No I/O happens here — real work is deferred to first `next()` call.
    fn open(&mut self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            match self {
                Operator::Scan(_) | Operator::Values(_) | Operator::Empty(_) => Ok(()),
                Operator::Project(op) => op.child.open().await,
                Operator::Filter(op) => op.child.open().await,
                Operator::Limit(op) => op.child.open().await,
                Operator::Sort(op) => op.child.open().await,
                Operator::Aggregate(op) => op.child.open().await,
            }
        })
    }

    /// Pull the next row (volcano-style). Returns None when exhausted.
    fn next(&mut self) -> Pin<Box<dyn Future<Output = Result<Option<Row>>> + Send + '_>> {
        Box::pin(async move {
            match self {
                Operator::Scan(op) => op.next().await,
                Operator::Values(op) => Ok(op.rows.next()),
                Operator::Empty(_) => Ok(None),
                Operator::Project(op) => op.next().await,
                Operator::Filter(op) => op.next().await,
                Operator::Limit(op) => op.next().await,
                Operator::Sort(op) => op.next().await,
                Operator::Aggregate(op) => op.next().await,
            }
        })
    }

    /// Schema of the operator's output.
    fn schema(&self) -> &Schema {
        match self {
            Operator::Scan(op) => &op.schema,
            Operator::Values(op) => &op.schema,
            Operator::Empty(op) => &op.schema,
            Operator::Project(op) => &op.schema,
            Operator::Filter(op) => op.child.schema(),
            Operator::Limit(op) => op.child.schema(),
            Operator::Sort(op) => op.child.schema(),
            Operator::Aggregate(op) => &op.schema,
        }
    }
}

impl<I: ExecutionRowStream> RowPuller for Operator<I> {
    fn next(&mut self) -> Pin<Box<dyn Future<Output = Result<Option<Row>>> + Send + '_>> {
        Operator::next(self)
    }
}

// ============================================================================
// Individual Operator Implementations
// ============================================================================

impl<I: ExecutionRowStream> ScanOp<I> {
    async fn next(&mut self) -> Result<Option<Row>> {
        while let Some(execution_row) = self.iter.next_row().await? {
            let row = execution_row.row;

            if let Some(ref filter_expr) = self.filter {
                let result = eval_expr(filter_expr, &row)?;
                if !value_to_bool(&result)? {
                    continue;
                }
            }

            let projected_row = if let Some(ref indices) = self.projection {
                let values = indices
                    .iter()
                    .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                    .collect();
                Row::new(values)
            } else {
                row
            };

            return Ok(Some(projected_row));
        }

        Ok(None)
    }
}

impl<I: ExecutionRowStream> ProjectOp<I> {
    async fn next(&mut self) -> Result<Option<Row>> {
        if let Some(row) = self.child.next().await? {
            let values = self
                .exprs
                .iter()
                .map(|(expr, _)| eval_expr(expr, &row))
                .collect::<Result<Vec<_>>>()?;
            Ok(Some(Row::new(values)))
        } else {
            Ok(None)
        }
    }
}

impl<I: ExecutionRowStream> FilterOp<I> {
    async fn next(&mut self) -> Result<Option<Row>> {
        while let Some(row) = self.child.next().await? {
            let result = eval_expr(&self.predicate, &row)?;
            if value_to_bool(&result)? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

impl<I: ExecutionRowStream> LimitOp<I> {
    async fn next(&mut self) -> Result<Option<Row>> {
        // Skip offset rows
        while self.skipped < self.offset {
            if self.child.next().await?.is_none() {
                return Ok(None);
            }
            self.skipped += 1;
        }

        // Check limit
        if let Some(limit) = self.limit {
            if self.returned >= limit {
                return Ok(None);
            }
        }

        let row = self.child.next().await?;
        if row.is_some() {
            self.returned += 1;
        }
        Ok(row)
    }
}

impl<I: ExecutionRowStream> SortOp<I> {
    async fn next(&mut self) -> Result<Option<Row>> {
        if self.sorted_rows.is_none() {
            // Materialize all rows from child, then sort
            let mut rows = Vec::new();
            while let Some(row) = self.child.next().await? {
                rows.push(row);
            }
            rows.sort_by(|a, b| {
                for order in &self.order_by {
                    let val_a = eval_expr(&order.expr, a).unwrap_or(Value::Null);
                    let val_b = eval_expr(&order.expr, b).unwrap_or(Value::Null);
                    let cmp = compare_values(&val_a, &val_b);
                    let cmp = if order.asc { cmp } else { cmp.reverse() };
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
            self.sorted_rows = Some(rows.into_iter());
        }

        Ok(self.sorted_rows.as_mut().unwrap().next())
    }
}

impl<I: ExecutionRowStream> AggregateOp<I> {
    async fn next(&mut self) -> Result<Option<Row>> {
        if self.result.is_none() {
            // Materialize all rows from child
            let mut rows = Vec::new();
            while let Some(row) = self.child.next().await? {
                rows.push(row);
            }

            if self.group_by.is_empty() && self.agg_exprs.is_empty() {
                self.result = Some(rows.into_iter());
            } else if self.group_by.is_empty() {
                let agg_values = self
                    .agg_exprs
                    .iter()
                    .map(|(func, arg, _)| compute_aggregate(func, arg, &rows))
                    .collect::<Result<Vec<_>>>()?;
                self.result = Some(vec![Row::new(agg_values)].into_iter());
            } else {
                return Err(TiSqlError::Execution("GROUP BY not yet implemented".into()));
            }
        }

        Ok(self.result.as_mut().unwrap().next())
    }
}

// ============================================================================
// Operator Tree Builder
// ============================================================================

/// Build an operator tree from a logical plan.
///
/// Read access stays behind `ExecutionBackend`; the executor does not encode
/// physical keys or touch transaction scan cursors directly here.
async fn build_read_operator<B: ExecutionBackend>(
    plan: LogicalPlan,
    execution_backend: &B,
    ctx: &TxnCtx,
) -> Result<Operator<B::Scan>> {
    match plan {
        LogicalPlan::Scan {
            table,
            filter,
            projection,
        } => {
            let schema = Schema::new(
                table
                    .columns()
                    .iter()
                    .map(|c| {
                        ColumnInfo::new(c.name().to_string(), c.data_type().clone(), c.nullable())
                    })
                    .collect(),
            );

            let bounds = match filter
                .as_ref()
                .and_then(|predicate| extract_pk_scan_bounds(&table, predicate))
            {
                Some(LogicalPkScanBounds::Empty) => {
                    return Ok(Operator::Empty(EmptyOp { schema }));
                }
                Some(LogicalPkScanBounds::Bounded(bounds)) => bounds,
                None => LogicalScanBounds::FullTable,
            };

            let iter = execution_backend.scan(
                ctx,
                ScanRequest {
                    table,
                    bounds,
                    projection: ProjectionSpec::All,
                },
            )?;

            Ok(Operator::Scan(ScanOp {
                iter,
                schema,
                filter,
                projection,
            }))
        }

        LogicalPlan::PointGet { table, key } => {
            let schema = Schema::new(
                table
                    .columns()
                    .iter()
                    .map(|c| {
                        ColumnInfo::new(c.name().to_string(), c.data_type().clone(), c.nullable())
                    })
                    .collect(),
            );
            let mut rows = Vec::new();
            if let Some(row) = execution_backend
                .point_get(
                    ctx,
                    PointGetRequest {
                        table,
                        key,
                        projection: ProjectionSpec::All,
                    },
                )
                .await?
            {
                rows.push(row.row);
            }

            Ok(Operator::Values(ValuesOp {
                rows: rows.into_iter(),
                schema,
            }))
        }

        LogicalPlan::Values { rows, schema } => {
            let result_rows = rows
                .into_iter()
                .map(|row| {
                    let values = row
                        .into_iter()
                        .map(|expr| eval_expr(&expr, &Row::new(vec![])))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Row::new(values))
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(Operator::Values(ValuesOp {
                rows: result_rows.into_iter(),
                schema,
            }))
        }

        LogicalPlan::Empty { schema } => Ok(Operator::Empty(EmptyOp { schema })),

        LogicalPlan::Project { input, exprs } => {
            let child = Box::pin(build_read_operator(*input, execution_backend, ctx)).await?;
            let schema = Schema::new(
                exprs
                    .iter()
                    .map(|(expr, alias)| ColumnInfo::new(alias.clone(), expr.data_type(), true))
                    .collect(),
            );
            Ok(Operator::Project(ProjectOp {
                child: Box::new(child),
                exprs,
                schema,
            }))
        }

        LogicalPlan::Filter { input, predicate } => {
            let child = Box::pin(build_read_operator(*input, execution_backend, ctx)).await?;
            Ok(Operator::Filter(FilterOp {
                child: Box::new(child),
                predicate,
            }))
        }

        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let child = Box::pin(build_read_operator(*input, execution_backend, ctx)).await?;
            Ok(Operator::Limit(LimitOp {
                child: Box::new(child),
                limit,
                offset,
                skipped: 0,
                returned: 0,
            }))
        }

        LogicalPlan::Sort { input, order_by } => {
            let child = Box::pin(build_read_operator(*input, execution_backend, ctx)).await?;
            Ok(Operator::Sort(SortOp {
                child: Box::new(child),
                order_by,
                sorted_rows: None,
            }))
        }

        LogicalPlan::Aggregate {
            input,
            group_by,
            agg_exprs,
        } => {
            let child = Box::pin(build_read_operator(*input, execution_backend, ctx)).await?;
            let schema = Schema::new(
                agg_exprs
                    .iter()
                    .map(|(_, _, alias)| ColumnInfo::new(alias.clone(), DataType::Double, true))
                    .collect(),
            );
            Ok(Operator::Aggregate(AggregateOp {
                child: Box::new(child),
                group_by,
                agg_exprs,
                result: None,
                schema,
            }))
        }

        other => Err(TiSqlError::Execution(format!(
            "Unsupported read plan: {:?}",
            std::mem::discriminant(&other)
        ))),
    }
}

// ============================================================================
// SimpleExecutor
// ============================================================================

// ---- Execute-phase profiling for auto-commit point reads ----
static EXEC_PROFILE_COUNT: AtomicU64 = AtomicU64::new(0);
static EXEC_PROFILE_BEGIN_US_TOTAL: AtomicU64 = AtomicU64::new(0);
static EXEC_PROFILE_BUILD_OP_US_TOTAL: AtomicU64 = AtomicU64::new(0);
const EXEC_PROFILE_LOG_EVERY: u64 = 10_000;

fn record_exec_profile(begin_us: u64, build_op_us: u64) {
    let count = EXEC_PROFILE_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    EXEC_PROFILE_BEGIN_US_TOTAL.fetch_add(begin_us, Ordering::Relaxed);
    EXEC_PROFILE_BUILD_OP_US_TOTAL.fetch_add(build_op_us, Ordering::Relaxed);

    if count % EXEC_PROFILE_LOG_EVERY != 0 {
        return;
    }

    let begin_total = EXEC_PROFILE_BEGIN_US_TOTAL.load(Ordering::Relaxed);
    let build_total = EXEC_PROFILE_BUILD_OP_US_TOTAL.load(Ordering::Relaxed);
    let avg_begin = begin_total / count;
    let avg_build = build_total / count;
    let avg_total = avg_begin + avg_build;
    crate::log_info!(
        "[read-exec-profile] reads={} avg_us(begin/build_op/total)={}/{}/{}",
        count,
        avg_begin,
        avg_build,
        avg_total
    );
}

/// Simple volcano-style executor
pub struct SimpleExecutor;

impl SimpleExecutor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SimpleExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor for SimpleExecutor {
    async fn execute_unified<T: TxnService, B: ExecutionBackend, C: Catalog>(
        &self,
        plan: LogicalPlan,
        _exec_ctx: &ExecutionCtx,
        stmt_ctx: &mut StatementCtx,
        txn_service: &T,
        execution_backend: &B,
        catalog: &C,
        txn_ctx: Option<TxnCtx>,
    ) -> (Result<ExecutionOutput>, Option<TxnCtx>) {
        // Handle session-level commands (USE database is handled at protocol layer)
        if let LogicalPlan::UseDatabase { .. } = &plan {
            return (Ok(ExecutionOutput::Ok), txn_ctx);
        }

        // Handle transaction control statements directly.
        if plan.is_transaction_control() {
            return match plan {
                LogicalPlan::Begin { read_only } => {
                    if let Some(ctx) = txn_ctx {
                        return (
                            Err(TiSqlError::Internal(
                                "Nested transactions are not supported. Use COMMIT or ROLLBACK first."
                                    .into(),
                            )),
                            Some(ctx),
                        );
                    }
                    match txn_service.begin_explicit(read_only) {
                        Ok(mut ctx) => {
                            ctx.set_schema_version(catalog.current_schema_version());
                            (Ok(ExecutionOutput::Ok), Some(ctx))
                        }
                        Err(e) => (Err(e), None),
                    }
                }
                LogicalPlan::Commit => {
                    if let Some(ctx) = txn_ctx {
                        match self
                            .commit_with_schema_check(ctx, txn_service, catalog)
                            .await
                        {
                            Ok(()) => (Ok(ExecutionOutput::Ok), None),
                            Err(e) => (Err(e), None),
                        }
                    } else {
                        (Ok(ExecutionOutput::Ok), None)
                    }
                }
                LogicalPlan::Rollback => {
                    if let Some(ctx) = txn_ctx {
                        match txn_service.rollback(ctx) {
                            Ok(()) => (Ok(ExecutionOutput::Ok), None),
                            Err(e) => (Err(e), None),
                        }
                    } else {
                        (Ok(ExecutionOutput::Ok), None)
                    }
                }
                _ => unreachable!(),
            };
        }

        match txn_ctx {
            Some(mut ctx) => {
                if plan.is_write() {
                    if plan.is_ddl() {
                        return (
                            Err(TiSqlError::Internal(
                                "DDL statements are not allowed within explicit transactions. Use COMMIT first."
                                    .into(),
                            )),
                            Some(ctx),
                        );
                    }

                    match self
                        .execute_write_with_ctx(
                            plan,
                            stmt_ctx,
                            &mut ctx,
                            execution_backend,
                            catalog,
                        )
                        .await
                    {
                        Ok(result) => (Ok(result), Some(ctx)),
                        Err(e) => (Err(e), Some(ctx)),
                    }
                } else {
                    // Streaming read in explicit txn — no materialization
                    match self
                        .execute_with_ctx(plan, &ctx, execution_backend, catalog)
                        .await
                    {
                        Ok(output) => (Ok(output), Some(ctx)),
                        Err(e) => (Err(e), Some(ctx)),
                    }
                }
            }
            None => {
                if plan.is_write() {
                    match self
                        .execute_write(plan, stmt_ctx, txn_service, execution_backend, catalog)
                        .await
                    {
                        Ok(result) => (Ok(result), None),
                        Err(e) => (Err(e), None),
                    }
                } else {
                    // Auto-commit streaming read — no materialization
                    let is_read = plan.is_read_query();
                    let begin_t = Instant::now();
                    match txn_service.begin(true) {
                        Ok(ctx) => {
                            let begin_us = begin_t.elapsed().as_micros() as u64;
                            let build_t = Instant::now();
                            match self
                                .execute_with_ctx(plan, &ctx, execution_backend, catalog)
                                .await
                            {
                                Ok(output) => {
                                    if is_read {
                                        let build_op_us = build_t.elapsed().as_micros() as u64;
                                        record_exec_profile(begin_us, build_op_us);
                                    }
                                    (Ok(output), None)
                                }
                                Err(e) => (Err(e), None),
                            }
                        }
                        Err(e) => (Err(e), None),
                    }
                }
            }
        }
    }
}

impl SimpleExecutor {
    /// Commit explicit transaction with schema-version validation.
    ///
    /// Mirrors auto-commit write behavior: if schema changed after transaction
    /// start and this transaction has pending writes, abort with SchemaChanged.
    async fn commit_with_schema_check<T: TxnService, C: Catalog>(
        &self,
        ctx: TxnCtx,
        txn_service: &T,
        catalog: &C,
    ) -> Result<()> {
        if !ctx.mutations.is_empty() {
            if let Some(schema_version_at_start) = ctx.schema_version() {
                let schema_version_now = catalog.current_schema_version();
                if schema_version_now != schema_version_at_start {
                    txn_service.rollback(ctx)?;
                    return Err(TiSqlError::SchemaChanged);
                }
            }
        }

        txn_service.commit(ctx).await?;
        Ok(())
    }

    /// Execute a write plan using a read-write transaction.
    ///
    /// Creates a transaction, executes writes (buffered), then commits.
    /// Checks schema version at commit to detect concurrent DDL changes.
    async fn execute_write<T: TxnService, B: ExecutionBackend, C: Catalog>(
        &self,
        plan: LogicalPlan,
        stmt_ctx: &mut StatementCtx,
        txn_service: &T,
        execution_backend: &B,
        catalog: &C,
    ) -> Result<ExecutionOutput> {
        // DDL operations don't need transaction (catalog operations)
        match &plan {
            LogicalPlan::CreateTable { .. } | LogicalPlan::DropTable { .. } => {
                return self.execute_ddl(plan, catalog).await;
            }
            _ => {}
        }

        // Capture schema version at start (no IO - just atomic read)
        let schema_version_at_start = catalog.current_schema_version();

        // Begin a read-write transaction (allocates txn_id and start_ts)
        let mut ctx = txn_service.begin(false)?;
        // Execute the write plan and get the result
        let result = match self
            .execute_write_with_ctx(plan, stmt_ctx, &mut ctx, execution_backend, catalog)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                let _ = txn_service.rollback(ctx);
                return Err(e);
            }
        };

        // Check schema version before commit (no IO - just atomic read)
        // This read lock will block if DDL is in the middle of committing
        let schema_version_now = catalog.current_schema_version();
        if schema_version_now != schema_version_at_start {
            txn_service.rollback(ctx)?;
            return Err(TiSqlError::SchemaChanged);
        }

        // Commit the transaction
        txn_service.commit(ctx).await?;

        Ok(result)
    }

    /// Execute DDL operations (no transaction needed).
    async fn execute_ddl<C: Catalog>(
        &self,
        plan: LogicalPlan,
        catalog: &C,
    ) -> Result<ExecutionOutput> {
        match plan {
            LogicalPlan::CreateTable {
                mut table,
                if_not_exists,
                auto_increment_offset,
            } => {
                // Require primary key for now (hidden row-id not fully supported)
                if table.primary_key().is_empty() {
                    return Err(TiSqlError::Catalog(format!(
                        "Table '{}' must have a PRIMARY KEY (tables without primary key are not supported yet)",
                        table.name()
                    )));
                }

                if catalog.get_table(table.schema(), table.name())?.is_some() {
                    if if_not_exists {
                        return Ok(ExecutionOutput::Ok);
                    }
                    return Err(TiSqlError::Catalog(format!(
                        "Table '{}' already exists",
                        table.name()
                    )));
                }

                if let Some(seed) = auto_increment_offset {
                    table.set_auto_increment_id(seed.saturating_sub(1));
                }

                let table_id = catalog.create_table(table).await?;
                Ok(ExecutionOutput::OkWithEffect(DdlEffect::TableCreated {
                    table_id,
                }))
            }

            LogicalPlan::DropTable {
                schema,
                table,
                if_exists,
            } => {
                if catalog.get_table(&schema, &table)?.is_none() {
                    if if_exists {
                        return Ok(ExecutionOutput::Ok);
                    }
                    return Err(TiSqlError::TableNotFound(format!("{schema}.{table}")));
                }

                let _info = catalog.drop_table(&schema, &table).await?;
                Ok(ExecutionOutput::OkWithEffect(DdlEffect::TableDropped))
            }

            _ => Err(TiSqlError::Execution("Not a DDL operation".into())),
        }
    }

    /// Execute read operations using a transaction context (streaming).
    ///
    /// Builds a volcano-style operator tree from the plan, opens it,
    /// and returns a live `ExecutionOutput` — zero materialization.
    async fn execute_with_ctx<B: ExecutionBackend, C: Catalog>(
        &self,
        plan: LogicalPlan,
        ctx: &TxnCtx,
        execution_backend: &B,
        _catalog: &C,
    ) -> Result<ExecutionOutput> {
        let mut op = build_read_operator(plan, execution_backend, ctx).await?;
        op.open().await?;
        let schema = op.schema().clone();
        Ok(ExecutionOutput::Rows {
            schema,
            exec: Execution::from_puller(Box::new(op)),
        })
    }

    /// Execute write operations using a transaction context.
    async fn execute_write_with_ctx<B: ExecutionBackend, C: Catalog>(
        &self,
        plan: LogicalPlan,
        _stmt_ctx: &mut StatementCtx,
        ctx: &mut TxnCtx,
        execution_backend: &B,
        catalog: &C,
    ) -> Result<ExecutionOutput> {
        match plan {
            LogicalPlan::Insert {
                table,
                columns,
                values,
                ignore,
            } => {
                let pk_indices = table.pk_column_indices();
                let auto_inc_column = table
                    .columns()
                    .iter()
                    .enumerate()
                    .find(|(_, col)| col.auto_increment());
                let table_id = table.id();
                let mut explicit_auto_inc_observations = Vec::new();
                let mut rows = Vec::new();

                for row_exprs in values {
                    let mut row_values = vec![Value::Null; table.columns().len()];

                    for (col_idx, &col_id) in columns.iter().enumerate() {
                        let table_col_idx = table
                            .columns()
                            .iter()
                            .position(|c| c.id() == col_id)
                            .ok_or_else(|| {
                            TiSqlError::ColumnNotFound(format!("id={col_id}"))
                        })?;

                        let value = eval_expr(&row_exprs[col_idx], &Row::new(vec![]))?;
                        row_values[table_col_idx] = value;
                    }

                    let row_id_for_key = if pk_indices.is_empty() {
                        Some(catalog.next_auto_increment(table_id)?)
                    } else {
                        None
                    };

                    let mut explicit_auto_inc_observe = None;

                    if let Some((idx, col)) = auto_inc_column {
                        match classify_auto_increment_row_value(&row_values[idx], col.data_type())?
                        {
                            AutoIncrementRowAction::Generate => {
                                let next_id = match row_id_for_key {
                                    Some(v) => v,
                                    None => catalog.next_auto_increment(table_id)?,
                                };
                                row_values[idx] =
                                    generated_auto_increment_value(next_id, col.data_type())?;
                            }
                            AutoIncrementRowAction::ExplicitNoObserve => {}
                            AutoIncrementRowAction::ExplicitObserve(v) => {
                                explicit_auto_inc_observe = Some(v);
                            }
                        }
                    }

                    normalize_row_values_for_table(&table, &mut row_values)?;

                    let key = if let Some(handle) = row_id_for_key {
                        RowKey::HiddenRowId(handle as i64)
                    } else {
                        let pk_values: Vec<_> =
                            pk_indices.iter().map(|&i| row_values[i].clone()).collect();
                        RowKey::PrimaryKey(pk_values)
                    };

                    if let Some(explicit_v) = explicit_auto_inc_observe {
                        explicit_auto_inc_observations.push(explicit_v);
                    }
                    rows.push(InsertRow {
                        key,
                        row: Row::new(row_values),
                    });
                }

                let outcome = execution_backend
                    .apply_write(
                        ctx,
                        WriteRequest::Insert(InsertRequest {
                            table: table.clone(),
                            rows,
                            ignore_duplicates: ignore,
                        }),
                    )
                    .await?;

                for explicit_v in explicit_auto_inc_observations {
                    catalog.observe_auto_increment_explicit(table_id, explicit_v)?;
                }

                Ok(ExecutionOutput::Affected {
                    count: outcome.affected_rows,
                })
            }

            LogicalPlan::Delete { table, input } => match build_dml_read_plan(*input, &table)? {
                DmlReadPlan::Empty => Ok(ExecutionOutput::Affected { count: 0 }),
                DmlReadPlan::PointGet { key, filter } => {
                    let row = execution_backend
                        .point_get(
                            ctx,
                            PointGetRequest {
                                table: table.clone(),
                                key,
                                projection: ProjectionSpec::All,
                            },
                        )
                        .await?;

                    let Some(row) = row else {
                        return Ok(ExecutionOutput::Affected { count: 0 });
                    };

                    let write_req = if row_matches_filter(filter.as_ref(), &row.row)? {
                        Some(WriteRequest::Delete(DeleteRequest {
                            table: table.clone(),
                            keys: vec![row.key],
                        }))
                    } else {
                        Some(WriteRequest::Lock(LockRequest {
                            table: table.clone(),
                            keys: vec![row.key],
                        }))
                    };

                    let outcome = execution_backend
                        .apply_write(ctx, write_req.unwrap())
                        .await?;
                    Ok(ExecutionOutput::Affected {
                        count: outcome.affected_rows,
                    })
                }
                DmlReadPlan::Scan { bounds, filter } => {
                    let scanned_rows = collect_execution_scan_rows(
                        execution_backend,
                        &*ctx,
                        ScanRequest {
                            table: table.clone(),
                            bounds,
                            projection: ProjectionSpec::All,
                        },
                    )
                    .await?;
                    let mut delete_keys = Vec::new();
                    let mut lock_keys = Vec::new();

                    for row in scanned_rows {
                        if row_matches_filter(filter.as_ref(), &row.row)? {
                            delete_keys.push(row.key);
                        } else if ctx.is_explicit() {
                            lock_keys.push(row.key);
                        }
                    }

                    let write_req = compose_write_request(
                        (!delete_keys.is_empty()).then(|| {
                            WriteRequest::Delete(DeleteRequest {
                                table: table.clone(),
                                keys: delete_keys,
                            })
                        }),
                        (!lock_keys.is_empty()).then(|| LockRequest {
                            table: table.clone(),
                            keys: lock_keys,
                        }),
                    );

                    let Some(write_req) = write_req else {
                        return Ok(ExecutionOutput::Affected { count: 0 });
                    };

                    let outcome = execution_backend.apply_write(ctx, write_req).await?;
                    Ok(ExecutionOutput::Affected {
                        count: outcome.affected_rows,
                    })
                }
            },

            LogicalPlan::Update {
                table,
                assignments,
                input,
            } => {
                let assignment_slots = assignments
                    .into_iter()
                    .map(|(col_id, expr)| {
                        let col_idx = table
                            .columns()
                            .iter()
                            .position(|c| c.id() == col_id)
                            .ok_or_else(|| TiSqlError::ColumnNotFound(format!("id={col_id}")))?;
                        Ok((col_idx, expr))
                    })
                    .collect::<Result<Vec<_>>>()?;

                match build_dml_read_plan(*input, &table)? {
                    DmlReadPlan::Empty => Ok(ExecutionOutput::Affected { count: 0 }),
                    DmlReadPlan::PointGet { key, filter } => {
                        let row = execution_backend
                            .point_get(
                                ctx,
                                PointGetRequest {
                                    table: table.clone(),
                                    key,
                                    projection: ProjectionSpec::All,
                                },
                            )
                            .await?;

                        let Some(row) = row else {
                            return Ok(ExecutionOutput::Affected { count: 0 });
                        };

                        let mut update_rows = Vec::new();
                        let mut lock_keys = Vec::new();

                        if row_matches_filter(filter.as_ref(), &row.row)? {
                            let mut new_row = row.row.clone();
                            for (col_idx, expr) in &assignment_slots {
                                let new_value = cast_value(
                                    &eval_expr(expr, &new_row)?,
                                    table.columns()[*col_idx].data_type(),
                                )?;
                                new_row.set(*col_idx, new_value);
                            }
                            update_rows.push(UpdateRow {
                                current_key: row.key,
                                current_row: row.row,
                                new_row,
                            });
                        } else {
                            lock_keys.push(row.key);
                        }

                        let write_req = compose_write_request(
                            (!update_rows.is_empty()).then(|| {
                                WriteRequest::Update(UpdateRequest {
                                    table: table.clone(),
                                    rows: update_rows,
                                })
                            }),
                            (!lock_keys.is_empty()).then(|| LockRequest {
                                table: table.clone(),
                                keys: lock_keys,
                            }),
                        );

                        let Some(write_req) = write_req else {
                            return Ok(ExecutionOutput::Affected { count: 0 });
                        };

                        let outcome = execution_backend.apply_write(ctx, write_req).await?;
                        Ok(ExecutionOutput::Affected {
                            count: outcome.affected_rows,
                        })
                    }
                    DmlReadPlan::Scan { bounds, filter } => {
                        let scanned_rows = collect_execution_scan_rows(
                            execution_backend,
                            &*ctx,
                            ScanRequest {
                                table: table.clone(),
                                bounds,
                                projection: ProjectionSpec::All,
                            },
                        )
                        .await?;

                        let mut update_rows = Vec::new();
                        let mut lock_keys = Vec::new();

                        for row in scanned_rows {
                            if row_matches_filter(filter.as_ref(), &row.row)? {
                                let mut new_row = row.row.clone();
                                for (col_idx, expr) in &assignment_slots {
                                    let new_value = cast_value(
                                        &eval_expr(expr, &new_row)?,
                                        table.columns()[*col_idx].data_type(),
                                    )?;
                                    new_row.set(*col_idx, new_value);
                                }
                                update_rows.push(UpdateRow {
                                    current_key: row.key,
                                    current_row: row.row,
                                    new_row,
                                });
                            } else if ctx.is_explicit() {
                                lock_keys.push(row.key);
                            }
                        }

                        let write_req = compose_write_request(
                            (!update_rows.is_empty()).then(|| {
                                WriteRequest::Update(UpdateRequest {
                                    table: table.clone(),
                                    rows: update_rows,
                                })
                            }),
                            (!lock_keys.is_empty()).then(|| LockRequest {
                                table: table.clone(),
                                keys: lock_keys,
                            }),
                        );

                        let Some(write_req) = write_req else {
                            return Ok(ExecutionOutput::Affected { count: 0 });
                        };

                        let outcome = execution_backend.apply_write(ctx, write_req).await?;
                        Ok(ExecutionOutput::Affected {
                            count: outcome.affected_rows,
                        })
                    }
                }
            }

            _ => Err(TiSqlError::Execution(format!(
                "Unsupported write plan: {:?}",
                std::mem::discriminant(&plan)
            ))),
        }
    }
}
