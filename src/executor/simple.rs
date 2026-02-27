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

use crate::catalog::types::{ColumnId, ColumnInfo, DataType, Key, Row, Schema, TableId, Value};
use crate::catalog::{Catalog, TableDef};
use crate::session::ExecutionCtx;
use crate::sql::{AggFunc, BinaryOp, Expr, LogicalPlan, OrderByExpr, UnaryOp};
use crate::tablet::{
    decode_row_to_values, encode_int_key, encode_key, encode_pk, encode_row, next_key_bound,
    MvccIterator,
};
use crate::transaction::{TxnCtx, TxnService};
use crate::util::error::{Result, TiSqlError};

use super::{DdlEffect, Execution, ExecutionOutput, Executor, RowPuller};

// ============================================================================
// Expression Evaluation (free functions)
// ============================================================================

fn eval_expr(expr: &Expr, row: &Row) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),

        Expr::Column { column_idx, .. } => Ok(row.get(*column_idx).cloned().unwrap_or(Value::Null)),

        Expr::BinaryOp { left, op, right } => {
            let left_val = eval_expr(left, row)?;
            let right_val = eval_expr(right, row)?;
            eval_binary_op(&left_val, *op, &right_val)
        }

        Expr::UnaryOp { op, expr } => {
            let val = eval_expr(expr, row)?;
            eval_unary_op(*op, &val)
        }

        Expr::IsNull { expr, negated } => {
            let val = eval_expr(expr, row)?;
            let is_null = val.is_null();
            Ok(Value::Boolean(if *negated { !is_null } else { is_null }))
        }

        Expr::Aggregate { arg, .. } => eval_expr(arg, row),

        Expr::Cast { expr, data_type } => {
            let val = eval_expr(expr, row)?;
            cast_value(&val, data_type)
        }

        _ => Err(TiSqlError::Execution(format!(
            "Unsupported expression: {:?}",
            std::mem::discriminant(expr)
        ))),
    }
}

fn eval_binary_op(left: &Value, op: BinaryOp, right: &Value) -> Result<Value> {
    // Handle NULL
    if left.is_null() || right.is_null() {
        return match op {
            BinaryOp::And => {
                if let Value::Boolean(false) = left {
                    return Ok(Value::Boolean(false));
                }
                if let Value::Boolean(false) = right {
                    return Ok(Value::Boolean(false));
                }
                Ok(Value::Null)
            }
            BinaryOp::Or => {
                if let Value::Boolean(true) = left {
                    return Ok(Value::Boolean(true));
                }
                if let Value::Boolean(true) = right {
                    return Ok(Value::Boolean(true));
                }
                Ok(Value::Null)
            }
            _ => Ok(Value::Null),
        };
    }

    match op {
        BinaryOp::Add => numeric_op(left, right, |a, b| a + b),
        BinaryOp::Sub => numeric_op(left, right, |a, b| a - b),
        BinaryOp::Mul => numeric_op(left, right, |a, b| a * b),
        BinaryOp::Div => numeric_op(left, right, |a, b| if b != 0.0 { a / b } else { f64::NAN }),
        BinaryOp::Mod => numeric_op(left, right, |a, b| a % b),

        BinaryOp::Eq => Ok(Value::Boolean(
            compare_values(left, right) == std::cmp::Ordering::Equal,
        )),
        BinaryOp::Ne => Ok(Value::Boolean(
            compare_values(left, right) != std::cmp::Ordering::Equal,
        )),
        BinaryOp::Lt => Ok(Value::Boolean(
            compare_values(left, right) == std::cmp::Ordering::Less,
        )),
        BinaryOp::Le => Ok(Value::Boolean(
            compare_values(left, right) != std::cmp::Ordering::Greater,
        )),
        BinaryOp::Gt => Ok(Value::Boolean(
            compare_values(left, right) == std::cmp::Ordering::Greater,
        )),
        BinaryOp::Ge => Ok(Value::Boolean(
            compare_values(left, right) != std::cmp::Ordering::Less,
        )),

        BinaryOp::And => {
            let l = value_to_bool(left)?;
            let r = value_to_bool(right)?;
            Ok(Value::Boolean(l && r))
        }
        BinaryOp::Or => {
            let l = value_to_bool(left)?;
            let r = value_to_bool(right)?;
            Ok(Value::Boolean(l || r))
        }

        BinaryOp::Concat => {
            let l = value_to_string(left);
            let r = value_to_string(right);
            Ok(Value::String(format!("{l}{r}")))
        }

        BinaryOp::Like => {
            let text = value_to_string(left);
            let pattern = value_to_string(right);
            Ok(Value::Boolean(like_match(&text, &pattern)))
        }
    }
}

fn eval_unary_op(op: UnaryOp, val: &Value) -> Result<Value> {
    if val.is_null() {
        return Ok(Value::Null);
    }

    match op {
        UnaryOp::Not => {
            let b = value_to_bool(val)?;
            Ok(Value::Boolean(!b))
        }
        UnaryOp::Neg => match val {
            Value::TinyInt(v) => Ok(Value::TinyInt(-v)),
            Value::SmallInt(v) => Ok(Value::SmallInt(-v)),
            Value::Int(v) => Ok(Value::Int(-v)),
            Value::BigInt(v) => Ok(Value::BigInt(-v)),
            Value::Float(v) => Ok(Value::Float(-v)),
            Value::Double(v) => Ok(Value::Double(-v)),
            _ => Err(TiSqlError::Execution(
                "Cannot negate non-numeric value".into(),
            )),
        },
        UnaryOp::Plus => Ok(val.clone()),
    }
}

fn numeric_op<F>(left: &Value, right: &Value, op: F) -> Result<Value>
where
    F: Fn(f64, f64) -> f64,
{
    let l = value_to_f64(left)?;
    let r = value_to_f64(right)?;
    let result = op(l, r);

    // Check if both operands are integer types (any combination)
    let left_is_int = matches!(
        left,
        Value::TinyInt(_) | Value::SmallInt(_) | Value::Int(_) | Value::BigInt(_)
    );
    let right_is_int = matches!(
        right,
        Value::TinyInt(_) | Value::SmallInt(_) | Value::Int(_) | Value::BigInt(_)
    );

    if left_is_int && right_is_int && result.fract() == 0.0 {
        // Both are integers and result has no fractional part - return BigInt
        // (BigInt can represent all integer types without loss)
        Ok(Value::BigInt(result as i64))
    } else if matches!((left, right), (Value::Float(_), _) | (_, Value::Float(_))) {
        Ok(Value::Float(result as f32))
    } else {
        Ok(Value::Double(result))
    }
}

fn value_to_f64(val: &Value) -> Result<f64> {
    match val {
        Value::TinyInt(v) => Ok(*v as f64),
        Value::SmallInt(v) => Ok(*v as f64),
        Value::Int(v) => Ok(*v as f64),
        Value::BigInt(v) => Ok(*v as f64),
        Value::Float(v) => Ok(*v as f64),
        Value::Double(v) => Ok(*v),
        Value::String(s) => s
            .parse()
            .map_err(|_| TiSqlError::Execution(format!("Cannot convert '{s}' to number"))),
        _ => Err(TiSqlError::Execution("Cannot convert to number".into())),
    }
}

fn value_to_bool(val: &Value) -> Result<bool> {
    match val {
        Value::Boolean(b) => Ok(*b),
        Value::TinyInt(v) => Ok(*v != 0),
        Value::SmallInt(v) => Ok(*v != 0),
        Value::Int(v) => Ok(*v != 0),
        Value::BigInt(v) => Ok(*v != 0),
        Value::Null => Ok(false),
        _ => Err(TiSqlError::Execution("Cannot convert to boolean".into())),
    }
}

fn value_to_string(val: &Value) -> String {
    match val {
        Value::String(s) => s.clone(),
        Value::TinyInt(v) => v.to_string(),
        Value::SmallInt(v) => v.to_string(),
        Value::Int(v) => v.to_string(),
        Value::BigInt(v) => v.to_string(),
        Value::Float(v) => v.to_string(),
        Value::Double(v) => v.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Null => "NULL".to_string(),
        _ => format!("{val:?}"),
    }
}

fn compare_values(left: &Value, right: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,

        (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
        (Value::String(a), Value::String(b)) => a.cmp(b),

        _ => {
            let l = value_to_f64(left).unwrap_or(f64::NAN);
            let r = value_to_f64(right).unwrap_or(f64::NAN);
            l.partial_cmp(&r).unwrap_or(Ordering::Equal)
        }
    }
}

fn like_match(text: &str, pattern: &str) -> bool {
    let regex_pattern = pattern.replace('%', ".*").replace('_', ".");
    regex::Regex::new(&format!("^{regex_pattern}$"))
        .map(|re| re.is_match(text))
        .unwrap_or(false)
}

fn cast_value(val: &Value, target: &DataType) -> Result<Value> {
    if val.is_null() {
        return Ok(Value::Null);
    }

    match target {
        DataType::Int => {
            let n = value_to_f64(val)? as i32;
            Ok(Value::Int(n))
        }
        DataType::BigInt => {
            let n = value_to_f64(val)? as i64;
            Ok(Value::BigInt(n))
        }
        DataType::Double => {
            let n = value_to_f64(val)?;
            Ok(Value::Double(n))
        }
        DataType::Varchar(_) | DataType::Text => Ok(Value::String(value_to_string(val))),
        DataType::Boolean => Ok(Value::Boolean(value_to_bool(val)?)),
        _ => Err(TiSqlError::Execution(format!(
            "Cast to {target:?} not implemented"
        ))),
    }
}

fn compute_aggregate(func: &AggFunc, arg: &Expr, rows: &[Row]) -> Result<Value> {
    if rows.is_empty() {
        return match func {
            AggFunc::Count => Ok(Value::BigInt(0)),
            _ => Ok(Value::Null),
        };
    }

    match func {
        AggFunc::Count => {
            let count = rows
                .iter()
                .filter(|row| !eval_expr(arg, row).map(|v| v.is_null()).unwrap_or(true))
                .count();
            Ok(Value::BigInt(count as i64))
        }
        AggFunc::Sum => {
            let sum: f64 = rows
                .iter()
                .filter_map(|row| {
                    eval_expr(arg, row).ok().and_then(|v| {
                        if v.is_null() {
                            None
                        } else {
                            value_to_f64(&v).ok()
                        }
                    })
                })
                .sum();
            Ok(Value::Double(sum))
        }
        AggFunc::Avg => {
            let values: Vec<f64> = rows
                .iter()
                .filter_map(|row| {
                    eval_expr(arg, row).ok().and_then(|v| {
                        if v.is_null() {
                            None
                        } else {
                            value_to_f64(&v).ok()
                        }
                    })
                })
                .collect();

            if values.is_empty() {
                Ok(Value::Null)
            } else {
                let avg = values.iter().sum::<f64>() / values.len() as f64;
                Ok(Value::Double(avg))
            }
        }
        AggFunc::Min => {
            let min = rows
                .iter()
                .filter_map(|row| eval_expr(arg, row).ok())
                .filter(|v| !v.is_null())
                .min_by(compare_values);
            Ok(min.unwrap_or(Value::Null))
        }
        AggFunc::Max => {
            let max = rows
                .iter()
                .filter_map(|row| eval_expr(arg, row).ok())
                .filter(|v| !v.is_null())
                .max_by(compare_values);
            Ok(max.unwrap_or(Value::Null))
        }
    }
}

// ============================================================================
// Operator Structs
// ============================================================================

/// Table scan operator — reads rows from storage via MvccIterator.
struct ScanOp<I: MvccIterator> {
    iter: I,
    schema: Schema,
    col_ids: Vec<ColumnId>,
    data_types: Vec<DataType>,
    filter: Option<Expr>,
    projection: Option<Vec<usize>>,
    initialized: bool,
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
struct ProjectOp<I: MvccIterator> {
    child: Box<Operator<I>>,
    exprs: Vec<(Expr, String)>,
    schema: Schema,
}

/// Filter operator — keeps rows matching predicate.
struct FilterOp<I: MvccIterator> {
    child: Box<Operator<I>>,
    predicate: Expr,
}

/// Limit/offset operator — caps output row count.
struct LimitOp<I: MvccIterator> {
    child: Box<Operator<I>>,
    limit: Option<usize>,
    offset: usize,
    skipped: usize,
    returned: usize,
}

/// Sort operator — materializes all rows on first next(), then yields sorted.
struct SortOp<I: MvccIterator> {
    child: Box<Operator<I>>,
    order_by: Vec<OrderByExpr>,
    sorted_rows: Option<std::vec::IntoIter<Row>>,
}

/// Aggregate operator — materializes all rows on first next(), computes aggregates.
struct AggregateOp<I: MvccIterator> {
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
/// Generic over `I: MvccIterator` — the storage iterator type, known at
/// compile time via `TxnService::ScanIter`.
enum Operator<I: MvccIterator> {
    Scan(ScanOp<I>),
    Values(ValuesOp),
    Empty(EmptyOp),
    Project(ProjectOp<I>),
    Filter(FilterOp<I>),
    Limit(LimitOp<I>),
    Sort(SortOp<I>),
    Aggregate(AggregateOp<I>),
}

impl<I: MvccIterator> Operator<I> {
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

impl<I: MvccIterator> RowPuller for Operator<I> {
    fn next(&mut self) -> Pin<Box<dyn Future<Output = Result<Option<Row>>> + Send + '_>> {
        Operator::next(self)
    }
}

// ============================================================================
// Individual Operator Implementations
// ============================================================================

impl<I: MvccIterator> ScanOp<I> {
    async fn next(&mut self) -> Result<Option<Row>> {
        if !self.initialized {
            self.initialized = true;
            self.iter.advance().await?;
        }

        while self.iter.valid() {
            let value = self.iter.value();
            let values = decode_row_to_values(value, &self.col_ids, &self.data_types)?;
            let row = Row::new(values);

            if let Some(ref filter_expr) = self.filter {
                let result = eval_expr(filter_expr, &row)?;
                if !value_to_bool(&result)? {
                    self.iter.advance().await?;
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

            self.iter.advance().await?;
            return Ok(Some(projected_row));
        }

        Ok(None)
    }
}

impl<I: MvccIterator> ProjectOp<I> {
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

impl<I: MvccIterator> FilterOp<I> {
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

impl<I: MvccIterator> LimitOp<I> {
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

impl<I: MvccIterator> SortOp<I> {
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

impl<I: MvccIterator> AggregateOp<I> {
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
/// `Scan` and `PointGet` touch `txn_service`; all other nodes just wrap
/// their child operator in `Box`.
async fn build_read_operator<T: TxnService>(
    plan: LogicalPlan,
    txn_service: &T,
    ctx: &TxnCtx,
) -> Result<Operator<T::ScanIter>> {
    match plan {
        LogicalPlan::Scan {
            table,
            filter,
            projection,
        } => {
            let table_id = table.id();
            let start_key = encode_key(table_id, &[]);
            let end_key = encode_key(table_id + 1, &[]);

            let col_ids: Vec<ColumnId> = table.columns().iter().map(|c| c.id()).collect();
            let data_types: Vec<DataType> = table
                .columns()
                .iter()
                .map(|c| c.data_type().clone())
                .collect();

            let schema = Schema::new(
                table
                    .columns()
                    .iter()
                    .map(|c| {
                        ColumnInfo::new(c.name().to_string(), c.data_type().clone(), c.nullable())
                    })
                    .collect(),
            );

            let scan_range = match filter
                .as_ref()
                .and_then(|predicate| extract_pk_scan_range(&table, predicate))
            {
                Some(PkScanRange::Empty) => {
                    return Ok(Operator::Empty(EmptyOp { schema }));
                }
                Some(PkScanRange::Bounded(range)) => range,
                None => start_key..end_key,
            };

            let iter = txn_service.scan_iter(ctx, table_id, scan_range)?;

            Ok(Operator::Scan(ScanOp {
                iter,
                schema,
                col_ids,
                data_types,
                filter,
                projection,
                initialized: false,
            }))
        }

        LogicalPlan::PointGet { table, key } => {
            let col_ids: Vec<ColumnId> = table.columns().iter().map(|c| c.id()).collect();
            let data_types: Vec<DataType> = table
                .columns()
                .iter()
                .map(|c| c.data_type().clone())
                .collect();
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
            if let Some(value) = txn_service.get(ctx, table.id(), &key).await? {
                let decoded = decode_row_to_values(&value, &col_ids, &data_types)?;
                rows.push(Row::new(decoded));
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
            let child = Box::pin(build_read_operator(*input, txn_service, ctx)).await?;
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
            let child = Box::pin(build_read_operator(*input, txn_service, ctx)).await?;
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
            let child = Box::pin(build_read_operator(*input, txn_service, ctx)).await?;
            Ok(Operator::Limit(LimitOp {
                child: Box::new(child),
                limit,
                offset,
                skipped: 0,
                returned: 0,
            }))
        }

        LogicalPlan::Sort { input, order_by } => {
            let child = Box::pin(build_read_operator(*input, txn_service, ctx)).await?;
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
            let child = Box::pin(build_read_operator(*input, txn_service, ctx)).await?;
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

enum DmlReadPlan {
    Empty,
    PointGet {
        key: Key,
        filter: Option<Expr>,
    },
    Scan {
        range: std::ops::Range<Key>,
        filter: Option<Expr>,
    },
}

enum PkScanRange {
    Empty,
    Bounded(std::ops::Range<Key>),
}

#[derive(Clone)]
struct PkBound {
    encoded: Vec<u8>,
    inclusive: bool,
}

enum AutoIncrementRowAction {
    Generate,
    ExplicitNoObserve,
    ExplicitObserve(u64),
}

fn generated_auto_increment_value(next_id: u64, data_type: &DataType) -> Result<Value> {
    match data_type {
        DataType::Int => {
            if next_id > i32::MAX as u64 {
                return Err(TiSqlError::Execution(
                    "AUTO_INCREMENT overflow for INT column".into(),
                ));
            }
            Ok(Value::BigInt(next_id as i64))
        }
        DataType::BigInt => {
            if next_id > i64::MAX as u64 {
                return Err(TiSqlError::Execution(
                    "AUTO_INCREMENT overflow for BIGINT column".into(),
                ));
            }
            Ok(Value::BigInt(next_id as i64))
        }
        _ => Err(TiSqlError::Execution(
            "AUTO_INCREMENT is only supported for INT/BIGINT".into(),
        )),
    }
}

fn classify_auto_increment_row_value(
    value: &Value,
    data_type: &DataType,
) -> Result<AutoIncrementRowAction> {
    if value.is_null() {
        return Ok(AutoIncrementRowAction::Generate);
    }

    let signed = match value {
        Value::TinyInt(v) => i64::from(*v),
        Value::SmallInt(v) => i64::from(*v),
        Value::Int(v) => i64::from(*v),
        Value::BigInt(v) => *v,
        _ => {
            return Err(TiSqlError::TypeMismatch {
                expected: "INT/BIGINT".into(),
                got: format!("{value:?}"),
            });
        }
    };

    match data_type {
        DataType::Int => {
            if signed < i32::MIN as i64 || signed > i32::MAX as i64 {
                return Err(TiSqlError::Execution(
                    "AUTO_INCREMENT explicit value out of INT range".into(),
                ));
            }
        }
        DataType::BigInt => {}
        _ => {
            return Err(TiSqlError::Execution(
                "AUTO_INCREMENT is only supported for INT/BIGINT".into(),
            ));
        }
    }

    if signed == 0 {
        Ok(AutoIncrementRowAction::Generate)
    } else if signed > 0 {
        Ok(AutoIncrementRowAction::ExplicitObserve(signed as u64))
    } else {
        Ok(AutoIncrementRowAction::ExplicitNoObserve)
    }
}

fn conjoin_predicate(existing: Option<Expr>, predicate: Expr) -> Option<Expr> {
    match existing {
        Some(prev) => Some(Expr::BinaryOp {
            left: Box::new(prev),
            op: BinaryOp::And,
            right: Box::new(predicate),
        }),
        None => Some(predicate),
    }
}

fn row_matches_filter(filter: Option<&Expr>, row: &Row) -> Result<bool> {
    let Some(predicate) = filter else {
        return Ok(true);
    };
    let result = eval_expr(predicate, row)?;
    value_to_bool(&result)
}

async fn lock_key_if_explicit<T: TxnService>(
    txn_service: &T,
    ctx: &mut TxnCtx,
    table_id: TableId,
    key: &[u8],
) -> Result<()> {
    if ctx.is_explicit() {
        txn_service.lock_key(ctx, table_id, key).await?;
    }
    Ok(())
}

async fn collect_scan_rows<T: TxnService>(
    txn_service: &T,
    ctx: &TxnCtx,
    table_id: TableId,
    range: std::ops::Range<Key>,
    col_ids: &[ColumnId],
    data_types: &[DataType],
) -> Result<Vec<(Key, Vec<Value>)>> {
    let mut iter = txn_service.scan_iter(ctx, table_id, range)?;
    iter.advance().await?;

    let mut rows = Vec::new();
    while iter.valid() {
        let key = iter.user_key().to_vec();
        let values = decode_row_to_values(iter.value(), col_ids, data_types)?;
        rows.push((key, values));
        iter.advance().await?;
    }
    Ok(rows)
}

fn collect_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
        } => {
            collect_conjuncts(left, out);
            collect_conjuncts(right, out);
        }
        _ => out.push(expr),
    }
}

fn point_get_literal_matches_pk(pk_type: &DataType, literal: &Value) -> bool {
    match (pk_type, literal) {
        (_, Value::Null) => false,
        (DataType::Boolean, Value::Boolean(_)) => true,
        (
            DataType::TinyInt | DataType::SmallInt | DataType::Int | DataType::BigInt,
            Value::TinyInt(_) | Value::SmallInt(_) | Value::Int(_) | Value::BigInt(_),
        ) => true,
        (DataType::Float | DataType::Double, Value::Float(_) | Value::Double(_)) => true,
        (DataType::Decimal { .. }, Value::Decimal(_)) => true,
        (DataType::Char(_) | DataType::Varchar(_) | DataType::Text, Value::String(_)) => true,
        (DataType::Blob, Value::Bytes(_)) => true,
        (DataType::Date, Value::Date(_)) => true,
        (DataType::Time, Value::Time(_)) => true,
        (DataType::DateTime, Value::DateTime(_)) => true,
        (DataType::Timestamp, Value::Timestamp(_)) => true,
        _ => false,
    }
}

fn normalize_pk_comparison(expr: &Expr, pk_column_idx: usize) -> Option<(BinaryOp, &Value)> {
    let Expr::BinaryOp { left, op, right } = expr else {
        return None;
    };

    match (left.as_ref(), right.as_ref()) {
        (Expr::Column { column_idx, .. }, Expr::Literal(literal))
            if *column_idx == pk_column_idx =>
        {
            Some((*op, literal))
        }
        (Expr::Literal(literal), Expr::Column { column_idx, .. })
            if *column_idx == pk_column_idx =>
        {
            let normalized = match op {
                BinaryOp::Eq => BinaryOp::Eq,
                BinaryOp::Lt => BinaryOp::Gt,
                BinaryOp::Le => BinaryOp::Ge,
                BinaryOp::Gt => BinaryOp::Lt,
                BinaryOp::Ge => BinaryOp::Le,
                _ => return None,
            };
            Some((normalized, literal))
        }
        _ => None,
    }
}

fn combine_lower(existing: Option<PkBound>, candidate: PkBound) -> Option<PkBound> {
    match existing {
        None => Some(candidate),
        Some(current) => match current.encoded.cmp(&candidate.encoded) {
            std::cmp::Ordering::Less => Some(candidate),
            std::cmp::Ordering::Greater => Some(current),
            std::cmp::Ordering::Equal => Some(PkBound {
                encoded: current.encoded,
                inclusive: current.inclusive && candidate.inclusive,
            }),
        },
    }
}

fn combine_upper(existing: Option<PkBound>, candidate: PkBound) -> Option<PkBound> {
    match existing {
        None => Some(candidate),
        Some(current) => match current.encoded.cmp(&candidate.encoded) {
            std::cmp::Ordering::Less => Some(current),
            std::cmp::Ordering::Greater => Some(candidate),
            std::cmp::Ordering::Equal => Some(PkBound {
                encoded: current.encoded,
                inclusive: current.inclusive && candidate.inclusive,
            }),
        },
    }
}

fn extract_pk_scan_range(table: &TableDef, filter: &Expr) -> Option<PkScanRange> {
    let pk_indices = table.pk_column_indices();
    if pk_indices.len() != 1 {
        return None;
    }
    let pk_column_idx = pk_indices[0];
    let pk_column = table.columns().get(pk_column_idx)?;

    let mut lower: Option<PkBound> = None;
    let mut upper: Option<PkBound> = None;

    let mut conjuncts = Vec::new();
    collect_conjuncts(filter, &mut conjuncts);
    let mut found_pk_predicate = false;

    for conjunct in conjuncts {
        let Some((op, literal)) = normalize_pk_comparison(conjunct, pk_column_idx) else {
            continue;
        };
        if !point_get_literal_matches_pk(pk_column.data_type(), literal) {
            continue;
        }

        let encoded = encode_pk(std::slice::from_ref(literal));
        found_pk_predicate = true;

        match op {
            BinaryOp::Eq => {
                let bound = PkBound {
                    encoded,
                    inclusive: true,
                };
                lower = combine_lower(lower, bound.clone());
                upper = combine_upper(upper, bound);
            }
            BinaryOp::Gt => {
                lower = combine_lower(
                    lower,
                    PkBound {
                        encoded,
                        inclusive: false,
                    },
                );
            }
            BinaryOp::Ge => {
                lower = combine_lower(
                    lower,
                    PkBound {
                        encoded,
                        inclusive: true,
                    },
                );
            }
            BinaryOp::Lt => {
                upper = combine_upper(
                    upper,
                    PkBound {
                        encoded,
                        inclusive: false,
                    },
                );
            }
            BinaryOp::Le => {
                upper = combine_upper(
                    upper,
                    PkBound {
                        encoded,
                        inclusive: true,
                    },
                );
            }
            _ => {}
        }
    }

    if !found_pk_predicate {
        return None;
    }

    if let (Some(l), Some(u)) = (&lower, &upper) {
        match l.encoded.cmp(&u.encoded) {
            std::cmp::Ordering::Greater => return Some(PkScanRange::Empty),
            std::cmp::Ordering::Equal if !l.inclusive || !u.inclusive => {
                return Some(PkScanRange::Empty);
            }
            _ => {}
        }
    }

    let table_start = encode_key(table.id(), &[]);
    let table_end = encode_key(table.id() + 1, &[]);

    let mut range_start = table_start;
    if let Some(l) = lower {
        range_start = encode_key(table.id(), &l.encoded);
        if !l.inclusive && !next_key_bound(&mut range_start) {
            return Some(PkScanRange::Empty);
        }
    }

    let mut range_end = table_end.clone();
    if let Some(u) = upper {
        range_end = encode_key(table.id(), &u.encoded);
        if u.inclusive && !next_key_bound(&mut range_end) {
            range_end = table_end;
        }
    }

    if range_start >= range_end {
        return Some(PkScanRange::Empty);
    }

    Some(PkScanRange::Bounded(range_start..range_end))
}

fn build_dml_read_plan(input: LogicalPlan, table: &TableDef) -> Result<DmlReadPlan> {
    let mut filter = None;
    let mut current = input;

    loop {
        match current {
            LogicalPlan::Filter { input, predicate } => {
                filter = conjoin_predicate(filter, predicate);
                current = *input;
            }
            LogicalPlan::Scan {
                table: scan_table,
                filter: scan_filter,
                ..
            } => {
                if scan_table.id() != table.id() {
                    return Err(TiSqlError::Execution(
                        "DML input scan table does not match target table".into(),
                    ));
                }

                if let Some(scan_filter) = scan_filter {
                    filter = conjoin_predicate(filter, scan_filter);
                }

                let table_start = encode_key(table.id(), &[]);
                let table_end = encode_key(table.id() + 1, &[]);
                let range = match filter
                    .as_ref()
                    .and_then(|predicate| extract_pk_scan_range(table, predicate))
                {
                    Some(PkScanRange::Empty) => return Ok(DmlReadPlan::Empty),
                    Some(PkScanRange::Bounded(range)) => range,
                    None => table_start..table_end,
                };

                return Ok(DmlReadPlan::Scan { range, filter });
            }
            LogicalPlan::PointGet {
                table: point_get_table,
                key,
            } => {
                if point_get_table.id() != table.id() {
                    return Err(TiSqlError::Execution(
                        "DML input point-get table does not match target table".into(),
                    ));
                }
                return Ok(DmlReadPlan::PointGet { key, filter });
            }
            LogicalPlan::Empty { .. } => return Ok(DmlReadPlan::Empty),
            other => {
                return Err(TiSqlError::Execution(format!(
                    "Unsupported DML input plan: {:?}",
                    std::mem::discriminant(&other)
                )));
            }
        }
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
    async fn execute_unified<T: TxnService, C: Catalog>(
        &self,
        plan: LogicalPlan,
        _exec_ctx: &ExecutionCtx,
        txn_service: &T,
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
                        .execute_write_with_ctx(plan, &mut ctx, txn_service, catalog)
                        .await
                    {
                        Ok(result) => (Ok(result), Some(ctx)),
                        Err(e) => (Err(e), Some(ctx)),
                    }
                } else {
                    // Streaming read in explicit txn — no materialization
                    match self
                        .execute_with_ctx(plan, &ctx, txn_service, catalog)
                        .await
                    {
                        Ok(output) => (Ok(output), Some(ctx)),
                        Err(e) => (Err(e), Some(ctx)),
                    }
                }
            }
            None => {
                if plan.is_write() {
                    match self.execute_write(plan, txn_service, catalog).await {
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
                                .execute_with_ctx(plan, &ctx, txn_service, catalog)
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
    async fn execute_write<T: TxnService, C: Catalog>(
        &self,
        plan: LogicalPlan,
        txn_service: &T,
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
            .execute_write_with_ctx(plan, &mut ctx, txn_service, catalog)
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
    async fn execute_with_ctx<T: TxnService, C: Catalog>(
        &self,
        plan: LogicalPlan,
        ctx: &TxnCtx,
        txn_service: &T,
        _catalog: &C,
    ) -> Result<ExecutionOutput> {
        let mut op = build_read_operator(plan, txn_service, ctx).await?;
        op.open().await?;
        let schema = op.schema().clone();
        Ok(ExecutionOutput::Rows {
            schema,
            exec: Execution::from_puller(Box::new(op)),
        })
    }

    /// Execute write operations using a transaction context.
    async fn execute_write_with_ctx<T: TxnService, C: Catalog>(
        &self,
        plan: LogicalPlan,
        ctx: &mut TxnCtx,
        txn_service: &T,
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
                let mut count = 0u64;
                let auto_inc_column = table
                    .columns()
                    .iter()
                    .enumerate()
                    .find(|(_, col)| col.auto_increment());

                // Get column IDs for encoding
                let col_ids: Vec<ColumnId> = table.columns().iter().map(|c| c.id()).collect();

                for row_exprs in values {
                    // Build row values
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

                    // For tables without explicit PK, allocate hidden row-id
                    let row_id_for_key = if pk_indices.is_empty() {
                        Some(catalog.next_auto_increment(table.id())?)
                    } else {
                        None
                    };

                    let mut explicit_auto_inc_observe = None;

                    // Handle explicit/null AUTO_INCREMENT semantics in row order.
                    if let Some((idx, col)) = auto_inc_column {
                        match classify_auto_increment_row_value(&row_values[idx], col.data_type())?
                        {
                            AutoIncrementRowAction::Generate => {
                                let next_id = match row_id_for_key {
                                    Some(v) => v,
                                    None => catalog.next_auto_increment(table.id())?,
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

                    let key = if let Some(handle) = row_id_for_key {
                        encode_int_key(table.id(), handle as i64)
                    } else {
                        let pk_values: Vec<_> =
                            pk_indices.iter().map(|&i| row_values[i].clone()).collect();
                        let pk_bytes = encode_pk(&pk_values);
                        encode_key(table.id(), &pk_bytes)
                    };

                    // Check for duplicate primary key
                    let duplicate_check_begin = Instant::now();
                    let duplicate_exists = txn_service.get(ctx, table.id(), &key).await?.is_some();
                    txn_service.record_duplicate_check_duration(duplicate_check_begin.elapsed());
                    if duplicate_exists {
                        if ignore {
                            txn_service.lock_key(ctx, table.id(), &key).await?;
                            continue;
                        }
                        let pk_desc = if pk_indices.is_empty() {
                            format!("row_id={}", row_id_for_key.unwrap_or(0))
                        } else {
                            let pk_values: Vec<_> =
                                pk_indices.iter().map(|&i| row_values[i].clone()).collect();
                            format!("{pk_values:?}")
                        };
                        return Err(TiSqlError::DuplicateKey(format!(
                            "Duplicate entry '{}' for key '{}.PRIMARY'",
                            pk_desc,
                            table.name()
                        )));
                    }

                    // Encode row using TiDB codec format
                    let value = encode_row(&col_ids, &row_values);

                    // Buffer write in transaction
                    txn_service.put(ctx, table.id(), &key, value).await?;
                    if let Some(explicit_v) = explicit_auto_inc_observe {
                        catalog.observe_auto_increment_explicit(table.id(), explicit_v)?;
                    }
                    count += 1;
                }

                Ok(ExecutionOutput::Affected { count })
            }

            LogicalPlan::Delete { table, input } => {
                let table_id = table.id();
                let col_ids: Vec<ColumnId> = table.columns().iter().map(|c| c.id()).collect();
                let data_types: Vec<DataType> = table
                    .columns()
                    .iter()
                    .map(|c| c.data_type().clone())
                    .collect();
                let mut count = 0u64;
                match build_dml_read_plan(*input, &table)? {
                    DmlReadPlan::Empty => {}
                    DmlReadPlan::PointGet { key, filter } => {
                        if let Some(value) = txn_service.get(ctx, table_id, &key).await? {
                            let values = decode_row_to_values(&value, &col_ids, &data_types)?;
                            let row = Row::new(values);
                            if row_matches_filter(filter.as_ref(), &row)? {
                                txn_service.delete(ctx, table_id, &key).await?;
                                count += 1;
                            } else {
                                txn_service.lock_key(ctx, table_id, &key).await?;
                            }
                        }
                    }
                    DmlReadPlan::Scan { range, filter } => {
                        if ctx.is_explicit() {
                            let scanned_rows = collect_scan_rows(
                                txn_service,
                                &*ctx,
                                table_id,
                                range,
                                &col_ids,
                                &data_types,
                            )
                            .await?;
                            for (key, values) in scanned_rows {
                                let row = Row::new(values);
                                if row_matches_filter(filter.as_ref(), &row)? {
                                    txn_service.delete(ctx, table_id, &key).await?;
                                    count += 1;
                                } else {
                                    txn_service.lock_key(ctx, table_id, &key).await?;
                                }
                            }
                        } else {
                            let mut iter = txn_service.scan_iter(ctx, table_id, range)?;
                            iter.advance().await?;
                            while iter.valid() {
                                let key = iter.user_key();
                                let values =
                                    decode_row_to_values(iter.value(), &col_ids, &data_types)?;
                                let row = Row::new(values);
                                if row_matches_filter(filter.as_ref(), &row)? {
                                    txn_service.delete(ctx, table_id, key).await?;
                                    count += 1;
                                } else {
                                    // In implicit autocommit this is a no-op; keep the helper
                                    // for defensive consistency with explicit paths.
                                    lock_key_if_explicit(txn_service, ctx, table_id, key).await?;
                                }
                                iter.advance().await?;
                            }
                        }
                    }
                }
                Ok(ExecutionOutput::Affected { count })
            }

            LogicalPlan::Update {
                table,
                assignments,
                input,
            } => {
                let table_id = table.id();
                let pk_indices = table.pk_column_indices();
                let pk_column_ids: Vec<ColumnId> = pk_indices
                    .iter()
                    .map(|&idx| table.columns()[idx].id())
                    .collect();
                let pk_assigned = assignments
                    .iter()
                    .any(|(col_id, _)| pk_column_ids.contains(col_id));
                let col_ids: Vec<ColumnId> = table.columns().iter().map(|c| c.id()).collect();
                let data_types: Vec<DataType> = table
                    .columns()
                    .iter()
                    .map(|c| c.data_type().clone())
                    .collect();
                let mut count = 0u64;

                match build_dml_read_plan(*input, &table)? {
                    DmlReadPlan::Empty => {}
                    DmlReadPlan::PointGet { key, filter } => {
                        if let Some(value) = txn_service.get(ctx, table_id, &key).await? {
                            let original_values =
                                decode_row_to_values(&value, &col_ids, &data_types)?;
                            let mut row = Row::new(original_values.clone());
                            if row_matches_filter(filter.as_ref(), &row)? {
                                for (col_id, expr) in &assignments {
                                    let col_idx = table
                                        .columns()
                                        .iter()
                                        .position(|c| c.id() == *col_id)
                                        .ok_or_else(|| {
                                            TiSqlError::ColumnNotFound(format!("id={col_id}"))
                                        })?;
                                    let new_value = eval_expr(expr, &row)?;
                                    row.set(col_idx, new_value);
                                }

                                let maybe_new_key = if pk_indices.is_empty() || !pk_assigned {
                                    None
                                } else {
                                    let pk_values: Vec<_> = pk_indices
                                        .iter()
                                        .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                                        .collect();
                                    Some(encode_key(table_id, &encode_pk(&pk_values)))
                                };
                                let target_key = maybe_new_key.as_deref().unwrap_or(&key);
                                let key_changed = maybe_new_key
                                    .as_ref()
                                    .is_some_and(|new_key| new_key.as_slice() != key.as_slice());

                                let row_unchanged =
                                    !key_changed && row.values() == original_values.as_slice();
                                if row_unchanged {
                                    txn_service.lock_key(ctx, table_id, &key).await?;
                                } else {
                                    if key_changed
                                        && txn_service
                                            .get(ctx, table_id, target_key)
                                            .await?
                                            .is_some()
                                    {
                                        let pk_values: Vec<_> = pk_indices
                                            .iter()
                                            .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                                            .collect();
                                        return Err(TiSqlError::DuplicateKey(format!(
                                            "Duplicate entry '{pk_values:?}' for key '{}.PRIMARY'",
                                            table.name()
                                        )));
                                    }

                                    if key_changed {
                                        txn_service.delete(ctx, table_id, &key).await?;
                                    }
                                    let new_value = encode_row(&col_ids, row.values());
                                    txn_service
                                        .put(ctx, table_id, target_key, new_value)
                                        .await?;
                                }
                                count += 1;
                            } else {
                                txn_service.lock_key(ctx, table_id, &key).await?;
                            }
                        }
                    }
                    DmlReadPlan::Scan { range, filter } => {
                        if ctx.is_explicit() {
                            let scanned_rows = collect_scan_rows(
                                txn_service,
                                &*ctx,
                                table_id,
                                range,
                                &col_ids,
                                &data_types,
                            )
                            .await?;
                            for (old_key, original_values) in scanned_rows {
                                let mut row = Row::new(original_values.clone());

                                if row_matches_filter(filter.as_ref(), &row)? {
                                    for (col_id, expr) in &assignments {
                                        let col_idx = table
                                            .columns()
                                            .iter()
                                            .position(|c| c.id() == *col_id)
                                            .ok_or_else(|| {
                                                TiSqlError::ColumnNotFound(format!("id={col_id}"))
                                            })?;
                                        let new_value = eval_expr(expr, &row)?;
                                        row.set(col_idx, new_value);
                                    }

                                    let maybe_new_key = if pk_indices.is_empty() || !pk_assigned {
                                        None
                                    } else {
                                        let pk_values: Vec<_> = pk_indices
                                            .iter()
                                            .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                                            .collect();
                                        Some(encode_key(table_id, &encode_pk(&pk_values)))
                                    };
                                    let target_key =
                                        maybe_new_key.as_deref().unwrap_or(old_key.as_slice());
                                    let key_changed =
                                        maybe_new_key.as_ref().is_some_and(|new_key| {
                                            new_key.as_slice() != old_key.as_slice()
                                        });

                                    let row_unchanged =
                                        !key_changed && row.values() == original_values.as_slice();
                                    if row_unchanged {
                                        // In implicit autocommit this is a no-op; keep the helper
                                        // for defensive consistency with explicit paths.
                                        lock_key_if_explicit(txn_service, ctx, table_id, &old_key)
                                            .await?;
                                    } else {
                                        if key_changed
                                            && txn_service
                                                .get(ctx, table_id, target_key)
                                                .await?
                                                .is_some()
                                        {
                                            let pk_values: Vec<_> = pk_indices
                                                .iter()
                                                .map(|&i| {
                                                    row.get(i).cloned().unwrap_or(Value::Null)
                                                })
                                                .collect();
                                            return Err(TiSqlError::DuplicateKey(format!(
                                                "Duplicate entry '{pk_values:?}' for key '{}.PRIMARY'",
                                                table.name()
                                            )));
                                        }

                                        if key_changed {
                                            txn_service.delete(ctx, table_id, &old_key).await?;
                                        }

                                        let new_value = encode_row(&col_ids, row.values());
                                        txn_service
                                            .put(ctx, table_id, target_key, new_value)
                                            .await?;
                                    }
                                    count += 1;
                                } else {
                                    txn_service.lock_key(ctx, table_id, &old_key).await?;
                                }
                            }
                        } else {
                            let mut iter = txn_service.scan_iter(ctx, table_id, range)?;
                            iter.advance().await?;
                            while iter.valid() {
                                let key_ref = iter.user_key();
                                let original_values =
                                    decode_row_to_values(iter.value(), &col_ids, &data_types)?;
                                let mut row = Row::new(original_values.clone());

                                if row_matches_filter(filter.as_ref(), &row)? {
                                    for (col_id, expr) in &assignments {
                                        let col_idx = table
                                            .columns()
                                            .iter()
                                            .position(|c| c.id() == *col_id)
                                            .ok_or_else(|| {
                                                TiSqlError::ColumnNotFound(format!("id={col_id}"))
                                            })?;
                                        let new_value = eval_expr(expr, &row)?;
                                        row.set(col_idx, new_value);
                                    }

                                    let maybe_new_key = if pk_indices.is_empty() || !pk_assigned {
                                        None
                                    } else {
                                        let pk_values: Vec<_> = pk_indices
                                            .iter()
                                            .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                                            .collect();
                                        Some(encode_key(table_id, &encode_pk(&pk_values)))
                                    };
                                    let target_key = maybe_new_key.as_deref().unwrap_or(key_ref);
                                    let key_changed = maybe_new_key
                                        .as_ref()
                                        .is_some_and(|new_key| new_key.as_slice() != key_ref);

                                    let row_unchanged =
                                        !key_changed && row.values() == original_values.as_slice();
                                    if row_unchanged {
                                        txn_service.lock_key(ctx, table_id, key_ref).await?;
                                    } else {
                                        if key_changed
                                            && txn_service
                                                .get(ctx, table_id, target_key)
                                                .await?
                                                .is_some()
                                        {
                                            let pk_values: Vec<_> = pk_indices
                                                .iter()
                                                .map(|&i| {
                                                    row.get(i).cloned().unwrap_or(Value::Null)
                                                })
                                                .collect();
                                            return Err(TiSqlError::DuplicateKey(format!(
                                                "Duplicate entry '{pk_values:?}' for key '{}.PRIMARY'",
                                                table.name()
                                            )));
                                        }

                                        if key_changed {
                                            txn_service.delete(ctx, table_id, key_ref).await?;
                                        }

                                        let new_value = encode_row(&col_ids, row.values());
                                        txn_service
                                            .put(ctx, table_id, target_key, new_value)
                                            .await?;
                                    }
                                    count += 1;
                                } else {
                                    // In implicit autocommit this is a no-op; keep the helper
                                    // for defensive consistency with explicit paths.
                                    lock_key_if_explicit(txn_service, ctx, table_id, key_ref)
                                        .await?;
                                }
                                iter.advance().await?;
                            }
                        }
                    }
                }

                Ok(ExecutionOutput::Affected { count })
            }

            _ => Err(TiSqlError::Execution(format!(
                "Unsupported write plan: {:?}",
                std::mem::discriminant(&plan)
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use parking_lot::Mutex;

    use super::*;
    use crate::catalog::types::{Lsn, RawValue, TableId, Timestamp, TxnId};
    use crate::catalog::{ColumnDef, MemoryCatalog, TableDef};
    use crate::tablet::MvccKey;
    use crate::transaction::{CommitInfo, TxnState};

    struct DummyIter;

    impl MvccIterator for DummyIter {
        async fn seek(&mut self, _target: &MvccKey) -> Result<()> {
            Ok(())
        }

        async fn advance(&mut self) -> Result<()> {
            Ok(())
        }

        fn valid(&self) -> bool {
            false
        }

        fn user_key(&self) -> &[u8] {
            panic!("dummy iterator should never be used")
        }

        fn timestamp(&self) -> Timestamp {
            panic!("dummy iterator should never be used")
        }

        fn value(&self) -> &[u8] {
            panic!("dummy iterator should never be used")
        }
    }

    struct MockPointGetTxnService {
        table_id: TableId,
        expected_key: Option<Vec<u8>>,
        row_value: Option<RawValue>,
        get_calls: AtomicUsize,
        scan_calls: AtomicUsize,
        put_calls: AtomicUsize,
        delete_calls: AtomicUsize,
        lock_calls: AtomicUsize,
        last_scan_range: Mutex<Option<Range<Vec<u8>>>>,
    }

    impl TxnService for MockPointGetTxnService {
        type ScanIter = DummyIter;

        fn begin(&self, read_only: bool) -> Result<TxnCtx> {
            Ok(TxnCtx::new(1, 1, read_only))
        }

        fn begin_explicit(&self, read_only: bool) -> Result<TxnCtx> {
            Ok(TxnCtx::new_explicit(1, 1, read_only))
        }

        fn get_txn_state(&self, _start_ts: Timestamp) -> Option<TxnState> {
            Some(TxnState::Running)
        }

        async fn get<'a>(
            &'a self,
            _ctx: &'a TxnCtx,
            table_id: TableId,
            key: &'a [u8],
        ) -> Result<Option<RawValue>> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(table_id, self.table_id);
            if let Some(expected_key) = &self.expected_key {
                assert_eq!(key, expected_key.as_slice());
            }
            Ok(self.row_value.clone())
        }

        fn scan_iter(
            &self,
            _ctx: &TxnCtx,
            table_id: TableId,
            range: Range<Vec<u8>>,
        ) -> Result<Self::ScanIter> {
            assert_eq!(table_id, self.table_id);
            self.scan_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_scan_range.lock() = Some(range);
            Ok(DummyIter)
        }

        async fn put<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            table_id: TableId,
            _key: &'a [u8],
            _value: RawValue,
        ) -> Result<()> {
            assert_eq!(table_id, self.table_id);
            self.put_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn delete<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            table_id: TableId,
            _key: &'a [u8],
        ) -> Result<()> {
            assert_eq!(table_id, self.table_id);
            self.delete_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn lock_key<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            table_id: TableId,
            _key: &'a [u8],
        ) -> Result<()> {
            assert_eq!(table_id, self.table_id);
            self.lock_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn commit(&self, _ctx: TxnCtx) -> Result<CommitInfo> {
            Ok(CommitInfo {
                txn_id: 1 as TxnId,
                commit_ts: 2,
                lsn: 3 as Lsn,
            })
        }

        fn rollback(&self, _ctx: TxnCtx) -> Result<()> {
            Ok(())
        }
    }

    struct MockRowsIter {
        rows: Vec<(Key, RawValue)>,
        index: usize,
        started: bool,
    }

    impl MockRowsIter {
        fn new(rows: Vec<(Key, RawValue)>) -> Self {
            Self {
                rows,
                index: 0,
                started: false,
            }
        }
    }

    impl MvccIterator for MockRowsIter {
        async fn seek(&mut self, _target: &MvccKey) -> Result<()> {
            self.started = true;
            self.index = 0;
            Ok(())
        }

        async fn advance(&mut self) -> Result<()> {
            if !self.started {
                self.started = true;
            } else {
                self.index += 1;
            }
            Ok(())
        }

        fn valid(&self) -> bool {
            self.started && self.index < self.rows.len()
        }

        fn user_key(&self) -> &[u8] {
            &self.rows[self.index].0
        }

        fn timestamp(&self) -> Timestamp {
            0
        }

        fn value(&self) -> &[u8] {
            &self.rows[self.index].1
        }
    }

    struct MockScanRowsTxnService {
        table_id: TableId,
        rows: Vec<(Key, RawValue)>,
        scan_calls: AtomicUsize,
        put_calls: AtomicUsize,
        delete_calls: AtomicUsize,
        lock_calls: AtomicUsize,
    }

    impl TxnService for MockScanRowsTxnService {
        type ScanIter = MockRowsIter;

        fn begin(&self, read_only: bool) -> Result<TxnCtx> {
            Ok(TxnCtx::new(1, 1, read_only))
        }

        fn begin_explicit(&self, read_only: bool) -> Result<TxnCtx> {
            Ok(TxnCtx::new_explicit(1, 1, read_only))
        }

        fn get_txn_state(&self, _start_ts: Timestamp) -> Option<TxnState> {
            Some(TxnState::Running)
        }

        async fn get<'a>(
            &'a self,
            _ctx: &'a TxnCtx,
            table_id: TableId,
            _key: &'a [u8],
        ) -> Result<Option<RawValue>> {
            assert_eq!(table_id, self.table_id);
            Ok(None)
        }

        fn scan_iter(
            &self,
            _ctx: &TxnCtx,
            table_id: TableId,
            _range: Range<Vec<u8>>,
        ) -> Result<Self::ScanIter> {
            assert_eq!(table_id, self.table_id);
            self.scan_calls.fetch_add(1, Ordering::SeqCst);
            Ok(MockRowsIter::new(self.rows.clone()))
        }

        async fn put<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            table_id: TableId,
            _key: &'a [u8],
            _value: RawValue,
        ) -> Result<()> {
            assert_eq!(table_id, self.table_id);
            self.put_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn delete<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            table_id: TableId,
            _key: &'a [u8],
        ) -> Result<()> {
            assert_eq!(table_id, self.table_id);
            self.delete_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn lock_key<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            table_id: TableId,
            _key: &'a [u8],
        ) -> Result<()> {
            assert_eq!(table_id, self.table_id);
            self.lock_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn commit(&self, _ctx: TxnCtx) -> Result<CommitInfo> {
            Ok(CommitInfo {
                txn_id: 1 as TxnId,
                commit_ts: 2,
                lsn: 3 as Lsn,
            })
        }

        fn rollback(&self, _ctx: TxnCtx) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_point_get_plan_uses_txn_get_instead_of_scan_iter() {
        let table = TableDef::new(
            101,
            "t".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::BigInt, false, None, false),
                ColumnDef::new(2, "name".into(), DataType::Varchar(32), true, None, false),
            ],
            vec![1],
        );
        let pk = vec![Value::BigInt(7)];
        let key = encode_key(table.id(), &encode_pk(&pk));
        let row = encode_row(&[1, 2], &[Value::BigInt(7), Value::String("alice".into())]);

        let service = MockPointGetTxnService {
            table_id: table.id(),
            expected_key: Some(key.clone()),
            row_value: Some(row),
            get_calls: AtomicUsize::new(0),
            scan_calls: AtomicUsize::new(0),
            put_calls: AtomicUsize::new(0),
            delete_calls: AtomicUsize::new(0),
            lock_calls: AtomicUsize::new(0),
            last_scan_range: Mutex::new(None),
        };
        let ctx = TxnCtx::new(11, 100, true);

        let mut op = build_read_operator(
            LogicalPlan::PointGet {
                table,
                key: key.clone(),
            },
            &service,
            &ctx,
        )
        .await
        .unwrap();

        op.open().await.unwrap();
        let first = op
            .next()
            .await
            .unwrap()
            .expect("point-get should return one row");
        assert_eq!(first.get(0), Some(&Value::BigInt(7)));
        assert_eq!(first.get(1), Some(&Value::String("alice".into())));
        assert!(op.next().await.unwrap().is_none());
        assert_eq!(service.get_calls.load(Ordering::SeqCst), 1);
        assert_eq!(service.scan_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_point_get_plan_not_found_returns_empty() {
        let table = TableDef::new(
            101,
            "t".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::BigInt, false, None, false),
                ColumnDef::new(2, "name".into(), DataType::Varchar(32), true, None, false),
            ],
            vec![1],
        );
        let key = encode_key(table.id(), &encode_pk(&[Value::BigInt(8)]));

        let service = MockPointGetTxnService {
            table_id: table.id(),
            expected_key: Some(key.clone()),
            row_value: None,
            get_calls: AtomicUsize::new(0),
            scan_calls: AtomicUsize::new(0),
            put_calls: AtomicUsize::new(0),
            delete_calls: AtomicUsize::new(0),
            lock_calls: AtomicUsize::new(0),
            last_scan_range: Mutex::new(None),
        };
        let ctx = TxnCtx::new(12, 101, true);

        let mut op = build_read_operator(LogicalPlan::PointGet { table, key }, &service, &ctx)
            .await
            .unwrap();

        op.open().await.unwrap();
        assert!(op.next().await.unwrap().is_none());
        assert_eq!(service.get_calls.load(Ordering::SeqCst), 1);
        assert_eq!(service.scan_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_extract_pk_scan_range_for_closed_interval() {
        let table = TableDef::new(
            202,
            "t".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::BigInt, false, None, false),
                ColumnDef::new(2, "name".into(), DataType::Varchar(32), true, None, false),
            ],
            vec![1],
        );

        let id_col = Expr::Column {
            table_idx: None,
            column_idx: 0,
            name: "id".into(),
            data_type: DataType::BigInt,
        };
        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(id_col.clone()),
                op: BinaryOp::Ge,
                right: Box::new(Expr::Literal(Value::BigInt(1))),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(id_col),
                op: BinaryOp::Le,
                right: Box::new(Expr::Literal(Value::BigInt(5))),
            }),
        };

        match extract_pk_scan_range(&table, &predicate) {
            Some(PkScanRange::Bounded(range)) => {
                let expected_start = encode_key(table.id(), &encode_pk(&[Value::BigInt(1)]));
                let mut expected_end = encode_key(table.id(), &encode_pk(&[Value::BigInt(5)]));
                assert!(next_key_bound(&mut expected_end));
                assert_eq!(range.start, expected_start);
                assert_eq!(range.end, expected_end);
            }
            _ => panic!("expected bounded pk range"),
        }
    }

    #[test]
    fn test_extract_pk_scan_range_accepts_integer_literal_type_mismatch() {
        let table = TableDef::new(
            203,
            "t".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::Int, false, None, false),
                ColumnDef::new(2, "name".into(), DataType::Varchar(32), true, None, false),
            ],
            vec![1],
        );

        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table_idx: None,
                column_idx: 0,
                name: "id".into(),
                data_type: DataType::Int,
            }),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Value::BigInt(7))),
        };

        match extract_pk_scan_range(&table, &predicate) {
            Some(PkScanRange::Bounded(range)) => {
                let expected_start = encode_key(table.id(), &encode_pk(&[Value::BigInt(7)]));
                let mut expected_end = encode_key(table.id(), &encode_pk(&[Value::BigInt(7)]));
                assert!(next_key_bound(&mut expected_end));
                assert_eq!(range.start, expected_start);
                assert_eq!(range.end, expected_end);
            }
            _ => panic!("expected bounded pk range"),
        }
    }

    #[tokio::test]
    async fn test_scan_operator_uses_pk_range_from_filter() {
        let table = TableDef::new(
            210,
            "t".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::BigInt, false, None, false),
                ColumnDef::new(2, "name".into(), DataType::Varchar(32), true, None, false),
            ],
            vec![1],
        );

        let id_col = Expr::Column {
            table_idx: None,
            column_idx: 0,
            name: "id".into(),
            data_type: DataType::BigInt,
        };
        let filter = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(id_col.clone()),
                op: BinaryOp::Ge,
                right: Box::new(Expr::Literal(Value::BigInt(1))),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(id_col),
                op: BinaryOp::Le,
                right: Box::new(Expr::Literal(Value::BigInt(5))),
            }),
        };

        let service = MockPointGetTxnService {
            table_id: table.id(),
            expected_key: None,
            row_value: None,
            get_calls: AtomicUsize::new(0),
            scan_calls: AtomicUsize::new(0),
            put_calls: AtomicUsize::new(0),
            delete_calls: AtomicUsize::new(0),
            lock_calls: AtomicUsize::new(0),
            last_scan_range: Mutex::new(None),
        };
        let ctx = TxnCtx::new(15, 150, true);

        let mut op = build_read_operator(
            LogicalPlan::Scan {
                table: table.clone(),
                projection: None,
                filter: Some(filter),
            },
            &service,
            &ctx,
        )
        .await
        .unwrap();

        op.open().await.unwrap();
        assert!(op.next().await.unwrap().is_none());
        assert_eq!(service.scan_calls.load(Ordering::SeqCst), 1);

        let observed = service
            .last_scan_range
            .lock()
            .clone()
            .expect("scan range should be captured");
        let expected_start = encode_key(table.id(), &encode_pk(&[Value::BigInt(1)]));
        let mut expected_end = encode_key(table.id(), &encode_pk(&[Value::BigInt(5)]));
        assert!(next_key_bound(&mut expected_end));
        assert_eq!(observed.start, expected_start);
        assert_eq!(observed.end, expected_end);
    }

    #[tokio::test]
    async fn test_delete_point_get_non_matching_filter_locks_key() {
        let table = TableDef::new(
            220,
            "t".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::BigInt, false, None, false),
                ColumnDef::new(2, "name".into(), DataType::Varchar(32), true, None, false),
            ],
            vec![1],
        );
        let key = encode_key(table.id(), &encode_pk(&[Value::BigInt(7)]));
        let row = encode_row(&[1, 2], &[Value::BigInt(7), Value::String("alice".into())]);

        let service = MockPointGetTxnService {
            table_id: table.id(),
            expected_key: Some(key.clone()),
            row_value: Some(row),
            get_calls: AtomicUsize::new(0),
            scan_calls: AtomicUsize::new(0),
            put_calls: AtomicUsize::new(0),
            delete_calls: AtomicUsize::new(0),
            lock_calls: AtomicUsize::new(0),
            last_scan_range: Mutex::new(None),
        };

        let mut ctx = TxnCtx::new(16, 160, false);
        let plan = LogicalPlan::Delete {
            table: table.clone(),
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::PointGet {
                    table,
                    key: key.clone(),
                }),
                predicate: Expr::BinaryOp {
                    left: Box::new(Expr::Column {
                        table_idx: None,
                        column_idx: 1,
                        name: "name".into(),
                        data_type: DataType::Varchar(32),
                    }),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::Literal(Value::String("bob".into()))),
                },
            }),
        };

        let output = SimpleExecutor::new()
            .execute_write_with_ctx(plan, &mut ctx, &service, &MemoryCatalog::new())
            .await
            .unwrap();

        assert!(matches!(output, ExecutionOutput::Affected { count } if count == 0));
        assert_eq!(service.get_calls.load(Ordering::SeqCst), 1);
        assert_eq!(service.lock_calls.load(Ordering::SeqCst), 1);
        assert_eq!(service.delete_calls.load(Ordering::SeqCst), 0);
        assert_eq!(service.scan_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_delete_scan_non_matching_filter_skips_lock_for_implicit_ctx() {
        let table = TableDef::new(
            221,
            "t".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::BigInt, false, None, false),
                ColumnDef::new(2, "name".into(), DataType::Varchar(32), true, None, false),
            ],
            vec![1],
        );
        let key = encode_key(table.id(), &encode_pk(&[Value::BigInt(7)]));
        let row = encode_row(&[1, 2], &[Value::BigInt(7), Value::String("alice".into())]);

        let service = MockScanRowsTxnService {
            table_id: table.id(),
            rows: vec![(key, row)],
            scan_calls: AtomicUsize::new(0),
            put_calls: AtomicUsize::new(0),
            delete_calls: AtomicUsize::new(0),
            lock_calls: AtomicUsize::new(0),
        };

        let mut ctx = TxnCtx::new(17, 161, false);
        let plan = LogicalPlan::Delete {
            table: table.clone(),
            input: Box::new(LogicalPlan::Scan {
                table,
                projection: None,
                filter: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column {
                        table_idx: None,
                        column_idx: 1,
                        name: "name".into(),
                        data_type: DataType::Varchar(32),
                    }),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::Literal(Value::String("bob".into()))),
                }),
            }),
        };

        let output = SimpleExecutor::new()
            .execute_write_with_ctx(plan, &mut ctx, &service, &MemoryCatalog::new())
            .await
            .unwrap();

        assert!(matches!(output, ExecutionOutput::Affected { count } if count == 0));
        assert_eq!(service.scan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(service.delete_calls.load(Ordering::SeqCst), 0);
        assert_eq!(service.lock_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_delete_scan_non_matching_filter_locks_for_explicit_ctx() {
        let table = TableDef::new(
            222,
            "t".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::BigInt, false, None, false),
                ColumnDef::new(2, "name".into(), DataType::Varchar(32), true, None, false),
            ],
            vec![1],
        );
        let key = encode_key(table.id(), &encode_pk(&[Value::BigInt(8)]));
        let row = encode_row(&[1, 2], &[Value::BigInt(8), Value::String("alice".into())]);

        let service = MockScanRowsTxnService {
            table_id: table.id(),
            rows: vec![(key, row)],
            scan_calls: AtomicUsize::new(0),
            put_calls: AtomicUsize::new(0),
            delete_calls: AtomicUsize::new(0),
            lock_calls: AtomicUsize::new(0),
        };

        let mut ctx = TxnCtx::new_explicit(18, 162, false);
        let plan = LogicalPlan::Delete {
            table: table.clone(),
            input: Box::new(LogicalPlan::Scan {
                table,
                projection: None,
                filter: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column {
                        table_idx: None,
                        column_idx: 1,
                        name: "name".into(),
                        data_type: DataType::Varchar(32),
                    }),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::Literal(Value::String("bob".into()))),
                }),
            }),
        };

        let output = SimpleExecutor::new()
            .execute_write_with_ctx(plan, &mut ctx, &service, &MemoryCatalog::new())
            .await
            .unwrap();

        assert!(matches!(output, ExecutionOutput::Affected { count } if count == 0));
        assert_eq!(service.scan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(service.delete_calls.load(Ordering::SeqCst), 0);
        assert_eq!(service.lock_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_update_noop_uses_lock_key_with_point_get_input() {
        let table = TableDef::new(
            303,
            "t".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::BigInt, false, None, false),
                ColumnDef::new(2, "name".into(), DataType::Varchar(32), true, None, false),
            ],
            vec![1],
        );
        let key = encode_key(table.id(), &encode_pk(&[Value::BigInt(7)]));
        let row = encode_row(&[1, 2], &[Value::BigInt(7), Value::String("alice".into())]);

        let service = MockPointGetTxnService {
            table_id: table.id(),
            expected_key: Some(key.clone()),
            row_value: Some(row),
            get_calls: AtomicUsize::new(0),
            scan_calls: AtomicUsize::new(0),
            put_calls: AtomicUsize::new(0),
            delete_calls: AtomicUsize::new(0),
            lock_calls: AtomicUsize::new(0),
            last_scan_range: Mutex::new(None),
        };

        let mut ctx = TxnCtx::new(31, 300, false);
        let plan = LogicalPlan::Update {
            table: table.clone(),
            assignments: vec![(
                2,
                Expr::Column {
                    table_idx: None,
                    column_idx: 1,
                    name: "name".into(),
                    data_type: DataType::Varchar(32),
                },
            )],
            input: Box::new(LogicalPlan::PointGet {
                table,
                key: key.clone(),
            }),
        };

        let output = SimpleExecutor::new()
            .execute_write_with_ctx(plan, &mut ctx, &service, &MemoryCatalog::new())
            .await
            .unwrap();

        assert!(matches!(output, ExecutionOutput::Affected { count } if count == 1));
        assert_eq!(service.get_calls.load(Ordering::SeqCst), 1);
        assert_eq!(service.lock_calls.load(Ordering::SeqCst), 1);
        assert_eq!(service.put_calls.load(Ordering::SeqCst), 0);
        assert_eq!(service.delete_calls.load(Ordering::SeqCst), 0);
        assert_eq!(service.scan_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_update_point_get_non_matching_filter_locks_key() {
        let table = TableDef::new(
            304,
            "t".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::BigInt, false, None, false),
                ColumnDef::new(2, "name".into(), DataType::Varchar(32), true, None, false),
            ],
            vec![1],
        );
        let key = encode_key(table.id(), &encode_pk(&[Value::BigInt(7)]));
        let row = encode_row(&[1, 2], &[Value::BigInt(7), Value::String("alice".into())]);

        let service = MockPointGetTxnService {
            table_id: table.id(),
            expected_key: Some(key.clone()),
            row_value: Some(row),
            get_calls: AtomicUsize::new(0),
            scan_calls: AtomicUsize::new(0),
            put_calls: AtomicUsize::new(0),
            delete_calls: AtomicUsize::new(0),
            lock_calls: AtomicUsize::new(0),
            last_scan_range: Mutex::new(None),
        };

        let mut ctx = TxnCtx::new(32, 301, false);
        let plan = LogicalPlan::Update {
            table: table.clone(),
            assignments: vec![(2, Expr::Literal(Value::String("bob".into())))],
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::PointGet {
                    table,
                    key: key.clone(),
                }),
                predicate: Expr::BinaryOp {
                    left: Box::new(Expr::Column {
                        table_idx: None,
                        column_idx: 1,
                        name: "name".into(),
                        data_type: DataType::Varchar(32),
                    }),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::Literal(Value::String("carol".into()))),
                },
            }),
        };

        let output = SimpleExecutor::new()
            .execute_write_with_ctx(plan, &mut ctx, &service, &MemoryCatalog::new())
            .await
            .unwrap();

        assert!(matches!(output, ExecutionOutput::Affected { count } if count == 0));
        assert_eq!(service.get_calls.load(Ordering::SeqCst), 1);
        assert_eq!(service.lock_calls.load(Ordering::SeqCst), 1);
        assert_eq!(service.put_calls.load(Ordering::SeqCst), 0);
        assert_eq!(service.delete_calls.load(Ordering::SeqCst), 0);
        assert_eq!(service.scan_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_update_scan_uses_pk_range_from_filter() {
        let table = TableDef::new(
            404,
            "t".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::BigInt, false, None, false),
                ColumnDef::new(2, "name".into(), DataType::Varchar(32), true, None, false),
            ],
            vec![1],
        );

        let id_col = Expr::Column {
            table_idx: None,
            column_idx: 0,
            name: "id".into(),
            data_type: DataType::BigInt,
        };
        let filter = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(id_col.clone()),
                op: BinaryOp::Ge,
                right: Box::new(Expr::Literal(Value::BigInt(1))),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(id_col),
                op: BinaryOp::Le,
                right: Box::new(Expr::Literal(Value::BigInt(5))),
            }),
        };

        let service = MockPointGetTxnService {
            table_id: table.id(),
            expected_key: None,
            row_value: None,
            get_calls: AtomicUsize::new(0),
            scan_calls: AtomicUsize::new(0),
            put_calls: AtomicUsize::new(0),
            delete_calls: AtomicUsize::new(0),
            lock_calls: AtomicUsize::new(0),
            last_scan_range: Mutex::new(None),
        };

        let mut ctx = TxnCtx::new(41, 400, false);
        let plan = LogicalPlan::Update {
            table: table.clone(),
            assignments: vec![(2, Expr::Literal(Value::String("new_name".into())))],
            input: Box::new(LogicalPlan::Scan {
                table: table.clone(),
                projection: None,
                filter: Some(filter),
            }),
        };

        let output = SimpleExecutor::new()
            .execute_write_with_ctx(plan, &mut ctx, &service, &MemoryCatalog::new())
            .await
            .unwrap();

        assert!(matches!(output, ExecutionOutput::Affected { count } if count == 0));
        assert_eq!(service.scan_calls.load(Ordering::SeqCst), 1);
        let observed = service
            .last_scan_range
            .lock()
            .clone()
            .expect("scan range should be captured");
        let expected_start = encode_key(table.id(), &encode_pk(&[Value::BigInt(1)]));
        let mut expected_end = encode_key(table.id(), &encode_pk(&[Value::BigInt(5)]));
        assert!(next_key_bound(&mut expected_end));
        assert_eq!(observed.start, expected_start);
        assert_eq!(observed.end, expected_end);
    }

    #[tokio::test]
    async fn test_delete_scan_uses_pk_range_from_filter() {
        let table = TableDef::new(
            405,
            "t".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::BigInt, false, None, false),
                ColumnDef::new(2, "name".into(), DataType::Varchar(32), true, None, false),
            ],
            vec![1],
        );

        let id_col = Expr::Column {
            table_idx: None,
            column_idx: 0,
            name: "id".into(),
            data_type: DataType::BigInt,
        };
        let filter = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(id_col.clone()),
                op: BinaryOp::Ge,
                right: Box::new(Expr::Literal(Value::BigInt(1))),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(id_col),
                op: BinaryOp::Le,
                right: Box::new(Expr::Literal(Value::BigInt(5))),
            }),
        };

        let service = MockPointGetTxnService {
            table_id: table.id(),
            expected_key: None,
            row_value: None,
            get_calls: AtomicUsize::new(0),
            scan_calls: AtomicUsize::new(0),
            put_calls: AtomicUsize::new(0),
            delete_calls: AtomicUsize::new(0),
            lock_calls: AtomicUsize::new(0),
            last_scan_range: Mutex::new(None),
        };

        let mut ctx = TxnCtx::new(42, 401, false);
        let plan = LogicalPlan::Delete {
            table: table.clone(),
            input: Box::new(LogicalPlan::Scan {
                table: table.clone(),
                projection: None,
                filter: Some(filter),
            }),
        };

        let output = SimpleExecutor::new()
            .execute_write_with_ctx(plan, &mut ctx, &service, &MemoryCatalog::new())
            .await
            .unwrap();

        assert!(matches!(output, ExecutionOutput::Affected { count } if count == 0));
        assert_eq!(service.scan_calls.load(Ordering::SeqCst), 1);
        let observed = service
            .last_scan_range
            .lock()
            .clone()
            .expect("scan range should be captured");
        let expected_start = encode_key(table.id(), &encode_pk(&[Value::BigInt(1)]));
        let mut expected_end = encode_key(table.id(), &encode_pk(&[Value::BigInt(5)]));
        assert!(next_key_bound(&mut expected_end));
        assert_eq!(observed.start, expected_start);
        assert_eq!(observed.end, expected_end);
    }

    #[tokio::test]
    async fn test_insert_ignore_duplicate_locks_existing_key() {
        let table = TableDef::new(
            505,
            "t".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::BigInt, false, None, false),
                ColumnDef::new(2, "name".into(), DataType::Varchar(32), true, None, false),
            ],
            vec![1],
        );
        let key = encode_key(table.id(), &encode_pk(&[Value::BigInt(7)]));

        let service = MockPointGetTxnService {
            table_id: table.id(),
            expected_key: Some(key),
            row_value: Some(b"existing".to_vec()),
            get_calls: AtomicUsize::new(0),
            scan_calls: AtomicUsize::new(0),
            put_calls: AtomicUsize::new(0),
            delete_calls: AtomicUsize::new(0),
            lock_calls: AtomicUsize::new(0),
            last_scan_range: Mutex::new(None),
        };

        let plan = LogicalPlan::Insert {
            table,
            columns: vec![1, 2],
            values: vec![vec![
                Expr::Literal(Value::BigInt(7)),
                Expr::Literal(Value::String("alice".into())),
            ]],
            ignore: true,
        };

        let mut ctx = TxnCtx::new(51, 500, false);
        let output = SimpleExecutor::new()
            .execute_write_with_ctx(plan, &mut ctx, &service, &MemoryCatalog::new())
            .await
            .unwrap();

        assert!(matches!(output, ExecutionOutput::Affected { count } if count == 0));
        assert_eq!(service.get_calls.load(Ordering::SeqCst), 1);
        assert_eq!(service.lock_calls.load(Ordering::SeqCst), 1);
        assert_eq!(service.put_calls.load(Ordering::SeqCst), 0);
        assert_eq!(service.scan_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_lock_key_if_explicit_skips_implicit_ctx() {
        let service = MockPointGetTxnService {
            table_id: 606,
            expected_key: None,
            row_value: None,
            get_calls: AtomicUsize::new(0),
            scan_calls: AtomicUsize::new(0),
            put_calls: AtomicUsize::new(0),
            delete_calls: AtomicUsize::new(0),
            lock_calls: AtomicUsize::new(0),
            last_scan_range: Mutex::new(None),
        };

        let mut ctx = TxnCtx::new(61, 600, false);
        lock_key_if_explicit(&service, &mut ctx, 606, b"k")
            .await
            .unwrap();

        assert_eq!(service.lock_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_lock_key_if_explicit_locks_explicit_ctx() {
        let service = MockPointGetTxnService {
            table_id: 607,
            expected_key: None,
            row_value: None,
            get_calls: AtomicUsize::new(0),
            scan_calls: AtomicUsize::new(0),
            put_calls: AtomicUsize::new(0),
            delete_calls: AtomicUsize::new(0),
            lock_calls: AtomicUsize::new(0),
            last_scan_range: Mutex::new(None),
        };

        let mut ctx = TxnCtx::new_explicit(62, 601, false);
        lock_key_if_explicit(&service, &mut ctx, 607, b"k")
            .await
            .unwrap();

        assert_eq!(service.lock_calls.load(Ordering::SeqCst), 1);
    }
}
