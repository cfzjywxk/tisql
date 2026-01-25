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

//! Simple volcano-style executor using TxnService for all operations.
//!
//! All statement execution goes through the transaction service:
//! - Read statements: Use begin(read_only=true) for snapshot reads
//! - Write statements: Use begin(read_only=false) with commit

use crate::catalog::Catalog;
use crate::error::{Result, TiSqlError};
use crate::sql::{AggFunc, BinaryOp, Expr, LogicalPlan, UnaryOp};
use crate::storage::{decode_row_to_values, encode_int_key, encode_key, encode_pk, encode_row};
use crate::transaction::{TxnCtx, TxnService};
use crate::types::{ColumnId, ColumnInfo, DataType, Row, Schema, Value};

use super::{ExecutionResult, Executor};

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
    fn execute<T: TxnService, C: Catalog>(
        &self,
        plan: LogicalPlan,
        txn_service: &T,
        catalog: &C,
    ) -> Result<ExecutionResult> {
        // Determine if this is a read or write operation
        if plan.is_write() {
            self.execute_write(plan, txn_service, catalog)
        } else {
            self.execute_read(plan, txn_service, catalog)
        }
    }
}

impl SimpleExecutor {
    /// Execute a read-only plan using a read-only transaction.
    ///
    /// Creates a transaction with read_only=true, which allocates start_ts
    /// from TSO for consistent reads.
    fn execute_read<T: TxnService, C: Catalog>(
        &self,
        plan: LogicalPlan,
        txn_service: &T,
        catalog: &C,
    ) -> Result<ExecutionResult> {
        // Begin a read-only transaction (allocates start_ts)
        let ctx = txn_service.begin(true)?;

        // Execute the read plan using the transaction context
        self.execute_with_ctx(plan, &ctx, txn_service, catalog)
    }

    /// Execute a write plan using a read-write transaction.
    ///
    /// Creates a transaction, executes writes (buffered), then commits.
    /// Checks schema version at commit to detect concurrent DDL changes.
    fn execute_write<T: TxnService, C: Catalog>(
        &self,
        plan: LogicalPlan,
        txn_service: &T,
        catalog: &C,
    ) -> Result<ExecutionResult> {
        // DDL operations don't need transaction (catalog operations)
        match &plan {
            LogicalPlan::CreateTable { .. } | LogicalPlan::DropTable { .. } => {
                return self.execute_ddl(plan, catalog);
            }
            _ => {}
        }

        // Capture schema version at start (no IO - just atomic read)
        let schema_version_at_start = catalog.current_schema_version();

        // Begin a read-write transaction (allocates txn_id and start_ts)
        let mut ctx = txn_service.begin(false)?;

        // Execute the write plan and get the result
        let result = self.execute_write_with_ctx(plan, &mut ctx, txn_service, catalog)?;

        // Check schema version before commit (no IO - just atomic read)
        // This read lock will block if DDL is in the middle of committing
        let schema_version_now = catalog.current_schema_version();
        if schema_version_now != schema_version_at_start {
            txn_service.rollback(ctx)?;
            return Err(TiSqlError::SchemaChanged);
        }

        // Commit the transaction
        txn_service.commit(ctx)?;

        Ok(result)
    }

    /// Execute DDL operations (no transaction needed).
    fn execute_ddl<C: Catalog>(&self, plan: LogicalPlan, catalog: &C) -> Result<ExecutionResult> {
        match plan {
            LogicalPlan::CreateTable {
                table,
                if_not_exists,
            } => {
                if catalog.get_table(table.schema(), table.name())?.is_some() {
                    if if_not_exists {
                        return Ok(ExecutionResult::Ok);
                    }
                    return Err(TiSqlError::Catalog(format!(
                        "Table '{}' already exists",
                        table.name()
                    )));
                }

                catalog.create_table(table)?;
                Ok(ExecutionResult::Ok)
            }

            LogicalPlan::DropTable {
                schema,
                table,
                if_exists,
            } => {
                if catalog.get_table(&schema, &table)?.is_none() {
                    if if_exists {
                        return Ok(ExecutionResult::Ok);
                    }
                    return Err(TiSqlError::TableNotFound(format!("{schema}.{table}")));
                }

                catalog.drop_table(&schema, &table)?;
                Ok(ExecutionResult::Ok)
            }

            _ => Err(TiSqlError::Execution("Not a DDL operation".into())),
        }
    }

    /// Execute read operations using a transaction context.
    #[allow(clippy::only_used_in_recursion)]
    fn execute_with_ctx<T: TxnService, C: Catalog>(
        &self,
        plan: LogicalPlan,
        ctx: &TxnCtx,
        txn_service: &T,
        catalog: &C,
    ) -> Result<ExecutionResult> {
        match plan {
            LogicalPlan::Values { rows, schema } => {
                let result_rows = rows
                    .into_iter()
                    .map(|row| {
                        let values = row
                            .into_iter()
                            .map(|expr| self.eval_expr(&expr, &Row::new(vec![])))
                            .collect::<Result<Vec<_>>>()?;
                        Ok(Row::new(values))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(ExecutionResult::Rows {
                    schema,
                    rows: result_rows,
                })
            }

            LogicalPlan::Project { input, exprs } => {
                let input_result = self.execute_with_ctx(*input, ctx, txn_service, catalog)?;

                match input_result {
                    ExecutionResult::Rows { rows, .. } => {
                        let schema = Schema::new(
                            exprs
                                .iter()
                                .map(|(expr, alias)| {
                                    ColumnInfo::new(alias.clone(), expr.data_type(), true)
                                })
                                .collect(),
                        );

                        let result_rows = rows
                            .iter()
                            .map(|row| {
                                let values = exprs
                                    .iter()
                                    .map(|(expr, _)| self.eval_expr(expr, row))
                                    .collect::<Result<Vec<_>>>()?;
                                Ok(Row::new(values))
                            })
                            .collect::<Result<Vec<_>>>()?;

                        Ok(ExecutionResult::Rows {
                            schema,
                            rows: result_rows,
                        })
                    }
                    other => Ok(other),
                }
            }

            LogicalPlan::Scan {
                table,
                filter,
                projection,
            } => {
                // Build key range for this table
                let table_id = table.id();
                let start_key = encode_key(table_id, &[]);
                let end_key = encode_key(table_id + 1, &[]);

                // Extract column IDs and data types for decoding
                let col_ids: Vec<ColumnId> = table.columns().iter().map(|c| c.id()).collect();
                let data_types: Vec<DataType> = table
                    .columns()
                    .iter()
                    .map(|c| c.data_type().clone())
                    .collect();

                // Scan using transaction service (reads at start_ts)
                let iter = txn_service.scan(ctx, start_key..end_key)?;

                let mut rows = Vec::new();
                for (_, value) in iter {
                    let values = decode_row_to_values(&value, &col_ids, &data_types)?;
                    let row = Row::new(values);

                    // Apply filter
                    if let Some(ref filter_expr) = filter {
                        let result = self.eval_expr(filter_expr, &row)?;
                        if !self.value_to_bool(&result)? {
                            continue;
                        }
                    }

                    // Apply projection
                    let projected_row = if let Some(ref indices) = projection {
                        let values = indices
                            .iter()
                            .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                            .collect();
                        Row::new(values)
                    } else {
                        row
                    };

                    rows.push(projected_row);
                }

                let schema = Schema::new(
                    table
                        .columns()
                        .iter()
                        .map(|c| {
                            ColumnInfo::new(
                                c.name().to_string(),
                                c.data_type().clone(),
                                c.nullable(),
                            )
                        })
                        .collect(),
                );

                Ok(ExecutionResult::Rows { schema, rows })
            }

            LogicalPlan::Filter { input, predicate } => {
                let input_result = self.execute_with_ctx(*input, ctx, txn_service, catalog)?;

                match input_result {
                    ExecutionResult::Rows { schema, rows } => {
                        let filtered_rows = rows
                            .into_iter()
                            .filter(|row| {
                                self.eval_expr(&predicate, row)
                                    .and_then(|v| self.value_to_bool(&v))
                                    .unwrap_or(false)
                            })
                            .collect();

                        Ok(ExecutionResult::Rows {
                            schema,
                            rows: filtered_rows,
                        })
                    }
                    other => Ok(other),
                }
            }

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let input_result = self.execute_with_ctx(*input, ctx, txn_service, catalog)?;

                match input_result {
                    ExecutionResult::Rows { schema, rows } => {
                        let limited_rows: Vec<_> = rows
                            .into_iter()
                            .skip(offset)
                            .take(limit.unwrap_or(usize::MAX))
                            .collect();

                        Ok(ExecutionResult::Rows {
                            schema,
                            rows: limited_rows,
                        })
                    }
                    other => Ok(other),
                }
            }

            LogicalPlan::Sort { input, order_by } => {
                let input_result = self.execute_with_ctx(*input, ctx, txn_service, catalog)?;

                match input_result {
                    ExecutionResult::Rows { schema, mut rows } => {
                        rows.sort_by(|a, b| {
                            for order in &order_by {
                                let val_a = self.eval_expr(&order.expr, a).unwrap_or(Value::Null);
                                let val_b = self.eval_expr(&order.expr, b).unwrap_or(Value::Null);
                                let cmp = self.compare_values(&val_a, &val_b);
                                let cmp = if order.asc { cmp } else { cmp.reverse() };
                                if cmp != std::cmp::Ordering::Equal {
                                    return cmp;
                                }
                            }
                            std::cmp::Ordering::Equal
                        });

                        Ok(ExecutionResult::Rows { schema, rows })
                    }
                    other => Ok(other),
                }
            }

            LogicalPlan::Aggregate {
                input,
                group_by,
                agg_exprs,
            } => {
                let input_result = self.execute_with_ctx(*input, ctx, txn_service, catalog)?;

                match input_result {
                    ExecutionResult::Rows { rows, .. } => {
                        if group_by.is_empty() && agg_exprs.is_empty() {
                            return Ok(ExecutionResult::Rows {
                                schema: Schema::new(vec![]),
                                rows,
                            });
                        }

                        if group_by.is_empty() {
                            let agg_values = agg_exprs
                                .iter()
                                .map(|(func, arg, _)| self.compute_aggregate(func, arg, &rows))
                                .collect::<Result<Vec<_>>>()?;

                            let schema = Schema::new(
                                agg_exprs
                                    .iter()
                                    .map(|(_, _, alias)| {
                                        ColumnInfo::new(alias.clone(), DataType::Double, true)
                                    })
                                    .collect(),
                            );

                            return Ok(ExecutionResult::Rows {
                                schema,
                                rows: vec![Row::new(agg_values)],
                            });
                        }

                        Err(TiSqlError::Execution("GROUP BY not yet implemented".into()))
                    }
                    other => Ok(other),
                }
            }

            _ => Err(TiSqlError::Execution(format!(
                "Unsupported read plan: {:?}",
                std::mem::discriminant(&plan)
            ))),
        }
    }

    /// Execute write operations using a transaction context.
    fn execute_write_with_ctx<T: TxnService, C: Catalog>(
        &self,
        plan: LogicalPlan,
        ctx: &mut TxnCtx,
        txn_service: &T,
        catalog: &C,
    ) -> Result<ExecutionResult> {
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

                        let value = self.eval_expr(&row_exprs[col_idx], &Row::new(vec![]))?;
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

                    // Encode row using TiDB codec format
                    let value = encode_row(&col_ids, &row_values);

                    // Buffer write in transaction
                    txn_service.put(ctx, key, value)?;
                    count += 1;
                }

                Ok(ExecutionResult::Affected { count })
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
                let iter = txn_service.scan(ctx, start_key..end_key)?;
                let entries: Vec<_> = iter.collect();

                for (key, value) in entries {
                    let values = decode_row_to_values(&value, &col_ids, &data_types)?;
                    let row = Row::new(values);

                    // Apply filter
                    if let Some(ref filter_expr) = filter {
                        let result = self.eval_expr(filter_expr, &row)?;
                        if !self.value_to_bool(&result)? {
                            continue;
                        }
                    }

                    // Buffer delete in transaction
                    txn_service.delete(ctx, key)?;
                    count += 1;
                }

                Ok(ExecutionResult::Affected { count })
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
                let iter = txn_service.scan(ctx, start_key..end_key)?;
                let entries: Vec<_> = iter.collect();

                for (key, value) in entries {
                    let values = decode_row_to_values(&value, &col_ids, &data_types)?;
                    let mut row = Row::new(values);

                    // Apply filter
                    if let Some(ref filter_expr) = filter {
                        let result = self.eval_expr(filter_expr, &row)?;
                        if !self.value_to_bool(&result)? {
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

                        let new_value = self.eval_expr(expr, &row)?;
                        row.set(col_idx, new_value);
                    }

                    // Compute new key if PK changed
                    let new_key = if pk_indices.is_empty() {
                        key.clone()
                    } else {
                        let pk_values: Vec<_> = pk_indices
                            .iter()
                            .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                            .collect();
                        let pk_bytes = encode_pk(&pk_values);
                        encode_key(table_id, &pk_bytes)
                    };

                    // Delete old key if PK changed
                    if !pk_indices.is_empty() && new_key != key {
                        txn_service.delete(ctx, key)?;
                    }

                    // Write new row
                    let new_value = encode_row(&col_ids, row.values());
                    txn_service.put(ctx, new_key, new_value)?;
                    count += 1;
                }

                Ok(ExecutionResult::Affected { count })
            }

            _ => Err(TiSqlError::Execution(format!(
                "Unsupported write plan: {:?}",
                std::mem::discriminant(&plan)
            ))),
        }
    }

    fn eval_expr(&self, expr: &Expr, row: &Row) -> Result<Value> {
        match expr {
            Expr::Literal(v) => Ok(v.clone()),

            Expr::Column { column_idx, .. } => {
                Ok(row.get(*column_idx).cloned().unwrap_or(Value::Null))
            }

            Expr::BinaryOp { left, op, right } => {
                let left_val = self.eval_expr(left, row)?;
                let right_val = self.eval_expr(right, row)?;
                self.eval_binary_op(&left_val, *op, &right_val)
            }

            Expr::UnaryOp { op, expr } => {
                let val = self.eval_expr(expr, row)?;
                self.eval_unary_op(*op, &val)
            }

            Expr::IsNull { expr, negated } => {
                let val = self.eval_expr(expr, row)?;
                let is_null = val.is_null();
                Ok(Value::Boolean(if *negated { !is_null } else { is_null }))
            }

            Expr::Aggregate { func: _, arg, .. } => self.eval_expr(arg, row),

            Expr::Cast { expr, data_type } => {
                let val = self.eval_expr(expr, row)?;
                self.cast_value(&val, data_type)
            }

            _ => Err(TiSqlError::Execution(format!(
                "Unsupported expression: {:?}",
                std::mem::discriminant(expr)
            ))),
        }
    }

    fn eval_binary_op(&self, left: &Value, op: BinaryOp, right: &Value) -> Result<Value> {
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
            BinaryOp::Add => self.numeric_op(left, right, |a, b| a + b),
            BinaryOp::Sub => self.numeric_op(left, right, |a, b| a - b),
            BinaryOp::Mul => self.numeric_op(left, right, |a, b| a * b),
            BinaryOp::Div => {
                self.numeric_op(left, right, |a, b| if b != 0.0 { a / b } else { f64::NAN })
            }
            BinaryOp::Mod => self.numeric_op(left, right, |a, b| a % b),

            BinaryOp::Eq => Ok(Value::Boolean(
                self.compare_values(left, right) == std::cmp::Ordering::Equal,
            )),
            BinaryOp::Ne => Ok(Value::Boolean(
                self.compare_values(left, right) != std::cmp::Ordering::Equal,
            )),
            BinaryOp::Lt => Ok(Value::Boolean(
                self.compare_values(left, right) == std::cmp::Ordering::Less,
            )),
            BinaryOp::Le => Ok(Value::Boolean(
                self.compare_values(left, right) != std::cmp::Ordering::Greater,
            )),
            BinaryOp::Gt => Ok(Value::Boolean(
                self.compare_values(left, right) == std::cmp::Ordering::Greater,
            )),
            BinaryOp::Ge => Ok(Value::Boolean(
                self.compare_values(left, right) != std::cmp::Ordering::Less,
            )),

            BinaryOp::And => {
                let l = self.value_to_bool(left)?;
                let r = self.value_to_bool(right)?;
                Ok(Value::Boolean(l && r))
            }
            BinaryOp::Or => {
                let l = self.value_to_bool(left)?;
                let r = self.value_to_bool(right)?;
                Ok(Value::Boolean(l || r))
            }

            BinaryOp::Concat => {
                let l = self.value_to_string(left);
                let r = self.value_to_string(right);
                Ok(Value::String(format!("{l}{r}")))
            }

            BinaryOp::Like => {
                let text = self.value_to_string(left);
                let pattern = self.value_to_string(right);
                Ok(Value::Boolean(self.like_match(&text, &pattern)))
            }
        }
    }

    fn eval_unary_op(&self, op: UnaryOp, val: &Value) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::Null);
        }

        match op {
            UnaryOp::Not => {
                let b = self.value_to_bool(val)?;
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

    fn numeric_op<F>(&self, left: &Value, right: &Value, op: F) -> Result<Value>
    where
        F: Fn(f64, f64) -> f64,
    {
        let l = self.value_to_f64(left)?;
        let r = self.value_to_f64(right)?;
        let result = op(l, r);

        match (left, right) {
            (Value::BigInt(_), Value::BigInt(_)) if result.fract() == 0.0 => {
                Ok(Value::BigInt(result as i64))
            }
            (Value::Int(_), Value::Int(_)) if result.fract() == 0.0 => {
                Ok(Value::Int(result as i32))
            }
            _ => Ok(Value::Double(result)),
        }
    }

    fn value_to_f64(&self, val: &Value) -> Result<f64> {
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

    fn value_to_bool(&self, val: &Value) -> Result<bool> {
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

    fn value_to_string(&self, val: &Value) -> String {
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

    fn compare_values(&self, left: &Value, right: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (left, right) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,

            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),

            _ => {
                let l = self.value_to_f64(left).unwrap_or(f64::NAN);
                let r = self.value_to_f64(right).unwrap_or(f64::NAN);
                l.partial_cmp(&r).unwrap_or(Ordering::Equal)
            }
        }
    }

    fn like_match(&self, text: &str, pattern: &str) -> bool {
        let regex_pattern = pattern.replace('%', ".*").replace('_', ".");
        regex::Regex::new(&format!("^{regex_pattern}$"))
            .map(|re| re.is_match(text))
            .unwrap_or(false)
    }

    fn cast_value(&self, val: &Value, target: &DataType) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::Null);
        }

        match target {
            DataType::Int => {
                let n = self.value_to_f64(val)? as i32;
                Ok(Value::Int(n))
            }
            DataType::BigInt => {
                let n = self.value_to_f64(val)? as i64;
                Ok(Value::BigInt(n))
            }
            DataType::Double => {
                let n = self.value_to_f64(val)?;
                Ok(Value::Double(n))
            }
            DataType::Varchar(_) | DataType::Text => Ok(Value::String(self.value_to_string(val))),
            DataType::Boolean => Ok(Value::Boolean(self.value_to_bool(val)?)),
            _ => Err(TiSqlError::Execution(format!(
                "Cast to {target:?} not implemented"
            ))),
        }
    }

    fn compute_aggregate(&self, func: &AggFunc, arg: &Expr, rows: &[Row]) -> Result<Value> {
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
                    .filter(|row| {
                        !self
                            .eval_expr(arg, row)
                            .map(|v| v.is_null())
                            .unwrap_or(true)
                    })
                    .count();
                Ok(Value::BigInt(count as i64))
            }
            AggFunc::Sum => {
                let sum: f64 = rows
                    .iter()
                    .filter_map(|row| {
                        self.eval_expr(arg, row).ok().and_then(|v| {
                            if v.is_null() {
                                None
                            } else {
                                self.value_to_f64(&v).ok()
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
                        self.eval_expr(arg, row).ok().and_then(|v| {
                            if v.is_null() {
                                None
                            } else {
                                self.value_to_f64(&v).ok()
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
                    .filter_map(|row| self.eval_expr(arg, row).ok())
                    .filter(|v| !v.is_null())
                    .min_by(|a, b| self.compare_values(a, b));
                Ok(min.unwrap_or(Value::Null))
            }
            AggFunc::Max => {
                let max = rows
                    .iter()
                    .filter_map(|row| self.eval_expr(arg, row).ok())
                    .filter(|v| !v.is_null())
                    .max_by(|a, b| self.compare_values(a, b));
                Ok(max.unwrap_or(Value::Null))
            }
        }
    }
}
