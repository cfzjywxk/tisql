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

use crate::catalog::types::{ColumnId, ColumnInfo, DataType, Row, Schema, Value};
use crate::catalog::Catalog;
use crate::session::ExecutionCtx;
use crate::sql::{AggFunc, BinaryOp, Expr, LogicalPlan, OrderByExpr, UnaryOp};
use crate::tablet::{
    decode_row_to_values, encode_int_key, encode_key, encode_pk, encode_row, MvccIterator,
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
/// Only `Scan` touches `txn_service` (calls `scan_iter()`). All other nodes
/// just wrap their child operator in `Box`.
fn build_read_operator<T: TxnService>(
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

            let iter = txn_service.scan_iter(ctx, table_id, start_key..end_key)?;

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
            let child = build_read_operator(*input, txn_service, ctx)?;
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
            let child = build_read_operator(*input, txn_service, ctx)?;
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
            let child = build_read_operator(*input, txn_service, ctx)?;
            Ok(Operator::Limit(LimitOp {
                child: Box::new(child),
                limit,
                offset,
                skipped: 0,
                returned: 0,
            }))
        }

        LogicalPlan::Sort { input, order_by } => {
            let child = build_read_operator(*input, txn_service, ctx)?;
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
            let child = build_read_operator(*input, txn_service, ctx)?;
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
                    match txn_service.begin(true) {
                        Ok(ctx) => match self
                            .execute_with_ctx(plan, &ctx, txn_service, catalog)
                            .await
                        {
                            Ok(output) => (Ok(output), None),
                            Err(e) => (Err(e), None),
                        },
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
                table,
                if_not_exists,
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
        let mut op = build_read_operator(plan, txn_service, ctx)?;
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
            } => {
                let pk_indices = table.pk_column_indices();
                let mut count = 0u64;

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

                    // Handle auto-increment
                    for (idx, col) in table.columns().iter().enumerate() {
                        if col.auto_increment() && row_values[idx].is_null() {
                            let next_id = match row_id_for_key {
                                Some(v) => v,
                                None => catalog.next_auto_increment(table.id())?,
                            };
                            row_values[idx] = Value::BigInt(next_id as i64);
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
                    if txn_service.get(ctx, table.id(), &key).await?.is_some() {
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
                    txn_service.put(ctx, table.id(), key, value).await?;
                    count += 1;
                }

                Ok(ExecutionOutput::Affected { count })
            }

            LogicalPlan::Delete { table, filter } => {
                let table_id = table.id();
                let start_key = encode_key(table_id, &[]);
                let end_key = encode_key(table_id + 1, &[]);

                let col_ids: Vec<ColumnId> = table.columns().iter().map(|c| c.id()).collect();
                let data_types: Vec<DataType> = table
                    .columns()
                    .iter()
                    .map(|c| c.data_type().clone())
                    .collect();

                let mut count = 0u64;

                // Scan using transaction's snapshot (reads at start_ts)
                // Use zero-copy streaming iteration - process one row at a time
                let mut iter = txn_service.scan_iter(ctx, table_id, start_key..end_key)?;

                iter.advance().await?; // Position on first entry
                while iter.valid() {
                    // Zero-copy: references to underlying storage
                    let key = iter.user_key();
                    let value = iter.value();
                    let values = decode_row_to_values(value, &col_ids, &data_types)?;
                    let row = Row::new(values);

                    // Apply filter
                    if let Some(ref filter_expr) = filter {
                        let result = eval_expr(filter_expr, &row)?;
                        if !value_to_bool(&result)? {
                            iter.advance().await?;
                            continue;
                        }
                    }

                    // Buffer delete in transaction (need to clone key for ownership)
                    txn_service.delete(ctx, table_id, key.to_vec()).await?;
                    count += 1;
                    iter.advance().await?;
                }

                Ok(ExecutionOutput::Affected { count })
            }

            LogicalPlan::Update {
                table,
                assignments,
                filter,
            } => {
                let table_id = table.id();
                let pk_indices = table.pk_column_indices();
                let start_key = encode_key(table_id, &[]);
                let end_key = encode_key(table_id + 1, &[]);

                let col_ids: Vec<ColumnId> = table.columns().iter().map(|c| c.id()).collect();
                let data_types: Vec<DataType> = table
                    .columns()
                    .iter()
                    .map(|c| c.data_type().clone())
                    .collect();

                let mut count = 0u64;

                // Scan using transaction's snapshot
                // Use zero-copy streaming iteration - process one row at a time
                let mut iter = txn_service.scan_iter(ctx, table_id, start_key..end_key)?;

                iter.advance().await?; // Position on first entry
                while iter.valid() {
                    // Zero-copy: references to underlying storage
                    let key_ref = iter.user_key();
                    let value = iter.value();
                    let values = decode_row_to_values(value, &col_ids, &data_types)?;
                    let mut row = Row::new(values);

                    // Apply filter
                    if let Some(ref filter_expr) = filter {
                        let result = eval_expr(filter_expr, &row)?;
                        if !value_to_bool(&result)? {
                            iter.advance().await?;
                            continue;
                        }
                    }

                    // Apply assignments
                    for (col_id, expr) in &assignments {
                        let col_idx = table
                            .columns()
                            .iter()
                            .position(|c| c.id() == *col_id)
                            .ok_or_else(|| TiSqlError::ColumnNotFound(format!("id={col_id}")))?;

                        let new_value = eval_expr(expr, &row)?;
                        row.set(col_idx, new_value);
                    }

                    // Compute new key if PK changed
                    let new_key = if pk_indices.is_empty() {
                        key_ref.to_vec() // Clone only when needed
                    } else {
                        let pk_values: Vec<_> = pk_indices
                            .iter()
                            .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                            .collect();
                        let pk_bytes = encode_pk(&pk_values);
                        encode_key(table_id, &pk_bytes)
                    };

                    // Check for duplicate key when PK changed
                    if !pk_indices.is_empty() && new_key.as_slice() != key_ref {
                        // Check if new key already exists (would conflict with another row)
                        if txn_service.get(ctx, table_id, &new_key).await?.is_some() {
                            let pk_values: Vec<_> = pk_indices
                                .iter()
                                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                                .collect();
                            return Err(TiSqlError::DuplicateKey(format!(
                                "Duplicate entry '{pk_values:?}' for key '{}.PRIMARY'",
                                table.name()
                            )));
                        }
                        // Delete old key (need to clone for ownership)
                        txn_service.delete(ctx, table_id, key_ref.to_vec()).await?;
                    }

                    // Write new row
                    let new_value = encode_row(&col_ids, row.values());
                    txn_service.put(ctx, table_id, new_key, new_value).await?;
                    count += 1;
                    iter.advance().await?;
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
