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

use std::collections::BTreeMap;

use sqlparser::ast::{
    self, Expr as SqlExpr, GroupByExpr, Query, Select, SelectItem, SetExpr,
    Statement as SqlStatement, TableFactor, TableWithJoins, Value as SqlValue,
};

use crate::catalog::types::{ColumnInfo, DataType, Schema, Timestamp, Value};
use crate::catalog::{Catalog, ColumnDef, TableDef};
use crate::tablet::{encode_key, encode_pk};
use crate::util::error::{Result, TiSqlError};

use super::plan::{AggFunc, BinaryOp, Expr, JoinType, LogicalPlan, OrderByExpr, UnaryOp};
use super::show_create::render_create_table;

/// SQL Binder - resolves names, checks types, produces logical plan
pub struct Binder<'a, C: Catalog> {
    catalog: &'a C,
    current_schema: String,
    /// Timestamp for MVCC schema reads. If None, uses latest schema.
    snapshot_ts: Option<Timestamp>,
}

struct PointGetRewrite {
    plan: LogicalPlan,
    residual_predicate: Option<Expr>,
}

impl<'a, C: Catalog> Binder<'a, C> {
    /// Create a new Binder that reads the latest schema (for DDL).
    pub fn new(catalog: &'a C, current_schema: &str) -> Self {
        Self {
            catalog,
            current_schema: current_schema.to_string(),
            snapshot_ts: None,
        }
    }

    /// Bind a SQL statement to a logical plan
    pub fn bind(&self, stmt: SqlStatement) -> Result<LogicalPlan> {
        match stmt {
            SqlStatement::Query(query) => self.bind_query(*query),
            SqlStatement::Insert {
                ignore,
                table_name,
                columns,
                source,
                ..
            } => self.bind_insert(ignore, table_name, columns, source),
            SqlStatement::Update {
                table,
                assignments,
                selection,
                ..
            } => self.bind_update(table, assignments, selection),
            SqlStatement::Delete {
                from, selection, ..
            } => self.bind_delete(from, selection),
            SqlStatement::CreateTable {
                name,
                columns,
                constraints,
                if_not_exists,
                ..
            } => self.bind_create_table(name, columns, constraints, if_not_exists),
            SqlStatement::Drop {
                object_type,
                names,
                if_exists,
                ..
            } => self.bind_drop(object_type, names, if_exists),
            SqlStatement::Use { db_name } => Ok(LogicalPlan::UseDatabase {
                db_name: db_name.value.clone(),
            }),
            SqlStatement::ShowCreate { obj_type, obj_name } => {
                self.bind_show_create(obj_type, obj_name)
            }

            // Transaction control statements
            // Note: MySQL's BEGIN is mapped to StartTransaction in sqlparser
            SqlStatement::StartTransaction { modes, .. } => {
                // Check for READ ONLY mode
                let read_only = modes.iter().any(|m| {
                    matches!(
                        m,
                        ast::TransactionMode::AccessMode(ast::TransactionAccessMode::ReadOnly)
                    )
                });
                Ok(LogicalPlan::Begin { read_only })
            }
            SqlStatement::Commit { .. } => Ok(LogicalPlan::Commit),
            SqlStatement::Rollback { .. } => Ok(LogicalPlan::Rollback),

            _ => Err(TiSqlError::Bind(format!(
                "Unsupported statement type: {:?} for: {}",
                std::mem::discriminant(&stmt),
                stmt
            ))),
        }
    }

    fn bind_query(&self, query: Query) -> Result<LogicalPlan> {
        let mut plan = self.bind_set_expr(*query.body)?;

        // ORDER BY - need table context for column resolution
        if !query.order_by.is_empty() {
            let tables = self.collect_tables_owned(&plan);
            let table_refs: Vec<&TableDef> = tables.iter().collect();
            let order_by = query
                .order_by
                .iter()
                .map(|o| {
                    Ok(OrderByExpr {
                        expr: self.bind_expr(&o.expr, &table_refs)?,
                        asc: o.asc.unwrap_or(true),
                        nulls_first: o.nulls_first.unwrap_or(false),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by,
            };
        }

        // LIMIT / OFFSET
        if query.limit.is_some() || query.offset.is_some() {
            let limit = query.limit.map(|l| self.expr_to_usize(&l)).transpose()?;
            let offset = query
                .offset
                .map(|o| self.expr_to_usize(&o.value))
                .transpose()?
                .unwrap_or(0);
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit,
                offset,
            };
        }

        Ok(plan)
    }

    fn bind_set_expr(&self, set_expr: SetExpr) -> Result<LogicalPlan> {
        match set_expr {
            SetExpr::Select(select) => self.bind_select(*select),
            SetExpr::Values(values) => self.bind_values(values),
            _ => Err(TiSqlError::Bind("Unsupported set expression".into())),
        }
    }

    fn bind_select(&self, select: Select) -> Result<LogicalPlan> {
        // Start with FROM clause
        let mut plan = if select.from.is_empty() {
            // SELECT without FROM (e.g., SELECT 1+1)
            LogicalPlan::Values {
                rows: vec![vec![]],
                schema: Schema::new(vec![]),
            }
        } else {
            self.bind_from(&select.from)?
        };

        // Collect table defs (owned) to avoid borrow issues
        let tables = self.collect_tables_owned(&plan);
        let table_refs: Vec<&TableDef> = tables.iter().collect();

        // Bind all expressions first, before modifying plan
        let mut where_predicate = select
            .selection
            .as_ref()
            .map(|s| self.bind_expr(s, &table_refs))
            .transpose()?;

        let group_by_exprs = match &select.group_by {
            GroupByExpr::All => vec![],
            GroupByExpr::Expressions(exprs) => exprs
                .iter()
                .map(|e| self.bind_expr(e, &table_refs))
                .collect::<Result<Vec<_>>>()?,
        };

        let (proj_exprs, _has_agg) = self.bind_select_items(&select.projection, &table_refs)?;

        let having_predicate = select
            .having
            .as_ref()
            .map(|h| self.bind_expr(h, &table_refs))
            .transpose()?;

        // Point-get optimization:
        // If WHERE provides equality bindings for all PK columns, replace
        // table scan with a direct point-get plan.
        if let Some(predicate) = where_predicate.as_ref() {
            if let Some(rewrite) = self.try_build_point_get(&plan, predicate) {
                plan = rewrite.plan;
                where_predicate = rewrite.residual_predicate;
            }
        }

        // Now build the plan tree
        // WHERE clause
        if let Some(predicate) = where_predicate {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }

        // GROUP BY
        if !group_by_exprs.is_empty() {
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by: group_by_exprs,
                agg_exprs: vec![],
            };
        }

        // SELECT list (projection)
        plan = LogicalPlan::Project {
            input: Box::new(plan),
            exprs: proj_exprs,
        };

        // HAVING (applied after aggregation)
        if let Some(predicate) = having_predicate {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }

        Ok(plan)
    }

    fn bind_from(&self, from: &[TableWithJoins]) -> Result<LogicalPlan> {
        if from.is_empty() {
            return Err(TiSqlError::Bind("Empty FROM clause".into()));
        }

        let mut plan = self.bind_table_with_joins(&from[0])?;

        for table_with_joins in &from[1..] {
            let right = self.bind_table_with_joins(table_with_joins)?;
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                on: Expr::Literal(Value::Boolean(true)),
                join_type: JoinType::Cross,
            };
        }

        Ok(plan)
    }

    fn bind_table_with_joins(&self, table: &TableWithJoins) -> Result<LogicalPlan> {
        let mut plan = self.bind_table_factor(&table.relation)?;

        for join in &table.joins {
            let right = self.bind_table_factor(&join.relation)?;
            let tables = [self.collect_tables(&plan), self.collect_tables(&right)].concat();

            let (join_type, on) = match &join.join_operator {
                ast::JoinOperator::Inner(constraint) => (
                    JoinType::Inner,
                    self.bind_join_constraint(constraint, &tables)?,
                ),
                ast::JoinOperator::LeftOuter(constraint) => (
                    JoinType::Left,
                    self.bind_join_constraint(constraint, &tables)?,
                ),
                ast::JoinOperator::RightOuter(constraint) => (
                    JoinType::Right,
                    self.bind_join_constraint(constraint, &tables)?,
                ),
                ast::JoinOperator::FullOuter(constraint) => (
                    JoinType::Full,
                    self.bind_join_constraint(constraint, &tables)?,
                ),
                ast::JoinOperator::CrossJoin => {
                    (JoinType::Cross, Expr::Literal(Value::Boolean(true)))
                }
                _ => return Err(TiSqlError::Bind("Unsupported join type".into())),
            };

            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                on,
                join_type,
            };
        }

        Ok(plan)
    }

    fn bind_join_constraint(
        &self,
        constraint: &ast::JoinConstraint,
        tables: &[&TableDef],
    ) -> Result<Expr> {
        match constraint {
            ast::JoinConstraint::On(expr) => self.bind_expr(expr, tables),
            ast::JoinConstraint::None => Ok(Expr::Literal(Value::Boolean(true))),
            _ => Err(TiSqlError::Bind("Unsupported join constraint".into())),
        }
    }

    fn bind_table_factor(&self, factor: &TableFactor) -> Result<LogicalPlan> {
        match factor {
            TableFactor::Table { name, .. } => {
                let table_name = &name.0;
                let (schema, table) = if table_name.len() == 1 {
                    (self.current_schema.as_str(), table_name[0].value.as_str())
                } else if table_name.len() == 2 {
                    (table_name[0].value.as_str(), table_name[1].value.as_str())
                } else {
                    return Err(TiSqlError::Bind("Invalid table name".into()));
                };

                // Use MVCC-aware schema lookup if snapshot_ts is set
                let table_def = match self.snapshot_ts {
                    Some(ts) => self.catalog.get_table_at(schema, table, ts)?,
                    None => self.catalog.get_table(schema, table)?,
                }
                .ok_or_else(|| TiSqlError::TableNotFound(format!("{schema}.{table}")))?;

                Ok(LogicalPlan::Scan {
                    table: table_def,
                    projection: None,
                    filter: None,
                })
            }
            _ => Err(TiSqlError::Bind("Unsupported table factor".into())),
        }
    }

    fn bind_select_items(
        &self,
        items: &[SelectItem],
        tables: &[&TableDef],
    ) -> Result<(Vec<(Expr, String)>, bool)> {
        let mut exprs = Vec::new();
        let mut has_agg = false;

        for item in items {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let bound = self.bind_expr(expr, tables)?;
                    if self.contains_aggregate(&bound) {
                        has_agg = true;
                    }
                    let alias = self.expr_alias(expr);
                    exprs.push((bound, alias));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let bound = self.bind_expr(expr, tables)?;
                    if self.contains_aggregate(&bound) {
                        has_agg = true;
                    }
                    exprs.push((bound, alias.value.clone()));
                }
                SelectItem::Wildcard(_) => {
                    for table in tables {
                        for (idx, col) in table.columns().iter().enumerate() {
                            exprs.push((
                                Expr::Column {
                                    table_idx: None,
                                    column_idx: idx,
                                    name: col.name().to_string(),
                                    data_type: col.data_type().clone(),
                                },
                                col.name().to_string(),
                            ));
                        }
                    }
                }
                SelectItem::QualifiedWildcard(name, _) => {
                    let table_name = name
                        .0
                        .last()
                        .ok_or_else(|| TiSqlError::Bind("Invalid qualified wildcard".into()))?
                        .value
                        .as_str();

                    let table = tables
                        .iter()
                        .find(|t| t.name() == table_name)
                        .ok_or_else(|| TiSqlError::TableNotFound(table_name.into()))?;

                    for (idx, col) in table.columns().iter().enumerate() {
                        exprs.push((
                            Expr::Column {
                                table_idx: None,
                                column_idx: idx,
                                name: col.name().to_string(),
                                data_type: col.data_type().clone(),
                            },
                            col.name().to_string(),
                        ));
                    }
                }
            }
        }

        Ok((exprs, has_agg))
    }

    fn bind_expr(&self, expr: &SqlExpr, tables: &[&TableDef]) -> Result<Expr> {
        match expr {
            SqlExpr::Identifier(ident) => {
                // Look for column in tables
                for (table_idx, table) in tables.iter().enumerate() {
                    if let Some(col_idx) = table.column_index(&ident.value) {
                        let col = &table.columns()[col_idx];
                        return Ok(Expr::Column {
                            table_idx: Some(table_idx),
                            column_idx: col_idx,
                            name: col.name().to_string(),
                            data_type: col.data_type().clone(),
                        });
                    }
                }
                Err(TiSqlError::ColumnNotFound(ident.value.clone()))
            }

            SqlExpr::CompoundIdentifier(idents) => {
                if idents.len() != 2 {
                    return Err(TiSqlError::Bind("Invalid column reference".into()));
                }
                let table_name = &idents[0].value;
                let col_name = &idents[1].value;

                for (table_idx, table) in tables.iter().enumerate() {
                    if table.name() == table_name {
                        if let Some(col_idx) = table.column_index(col_name) {
                            let col = &table.columns()[col_idx];
                            return Ok(Expr::Column {
                                table_idx: Some(table_idx),
                                column_idx: col_idx,
                                name: col.name().to_string(),
                                data_type: col.data_type().clone(),
                            });
                        }
                    }
                }
                Err(TiSqlError::ColumnNotFound(format!(
                    "{table_name}.{col_name}"
                )))
            }

            SqlExpr::Value(v) => Ok(Expr::Literal(self.bind_value(v)?)),

            SqlExpr::BinaryOp { left, op, right } => {
                let left_expr = self.bind_expr(left, tables)?;
                let right_expr = self.bind_expr(right, tables)?;
                let bin_op = self.bind_binary_op(op)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(left_expr),
                    op: bin_op,
                    right: Box::new(right_expr),
                })
            }

            SqlExpr::UnaryOp { op, expr } => {
                let inner = self.bind_expr(expr, tables)?;
                let unary_op = match op {
                    ast::UnaryOperator::Not => UnaryOp::Not,
                    ast::UnaryOperator::Minus => UnaryOp::Neg,
                    ast::UnaryOperator::Plus => UnaryOp::Plus,
                    _ => return Err(TiSqlError::Bind(format!("Unsupported unary op: {op:?}"))),
                };
                Ok(Expr::UnaryOp {
                    op: unary_op,
                    expr: Box::new(inner),
                })
            }

            SqlExpr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                let args: Vec<Expr> = func
                    .args
                    .iter()
                    .filter_map(|arg| {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) = arg {
                            Some(self.bind_expr(e, tables))
                        } else if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) =
                            arg
                        {
                            Some(Ok(Expr::Literal(Value::Int(1)))) // COUNT(*)
                        } else {
                            None
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                // Check for aggregate functions
                match name.as_str() {
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => {
                        let agg_func = match name.as_str() {
                            "COUNT" => AggFunc::Count,
                            "SUM" => AggFunc::Sum,
                            "AVG" => AggFunc::Avg,
                            "MIN" => AggFunc::Min,
                            "MAX" => AggFunc::Max,
                            _ => unreachable!(),
                        };
                        let arg = args
                            .into_iter()
                            .next()
                            .unwrap_or(Expr::Literal(Value::Int(1)));
                        Ok(Expr::Aggregate {
                            func: agg_func,
                            arg: Box::new(arg),
                            distinct: func.distinct,
                        })
                    }
                    _ => Ok(Expr::Function { name, args }),
                }
            }

            SqlExpr::IsNull(expr) => {
                let inner = self.bind_expr(expr, tables)?;
                Ok(Expr::IsNull {
                    expr: Box::new(inner),
                    negated: false,
                })
            }

            SqlExpr::IsNotNull(expr) => {
                let inner = self.bind_expr(expr, tables)?;
                Ok(Expr::IsNull {
                    expr: Box::new(inner),
                    negated: true,
                })
            }

            SqlExpr::Nested(inner) => self.bind_expr(inner, tables),

            _ => Err(TiSqlError::Bind(format!(
                "Unsupported expression: {:?}",
                std::mem::discriminant(expr)
            ))),
        }
    }

    fn bind_value(&self, value: &SqlValue) -> Result<Value> {
        match value {
            SqlValue::Number(n, _) => {
                if n.contains('.') {
                    Ok(Value::Double(n.parse().map_err(|_| {
                        TiSqlError::Bind(format!("Invalid number: {n}"))
                    })?))
                } else {
                    Ok(Value::BigInt(n.parse().map_err(|_| {
                        TiSqlError::Bind(format!("Invalid integer: {n}"))
                    })?))
                }
            }
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                Ok(Value::String(s.clone()))
            }
            SqlValue::Boolean(b) => Ok(Value::Boolean(*b)),
            SqlValue::Null => Ok(Value::Null),
            _ => Err(TiSqlError::Bind(format!("Unsupported value: {value:?}"))),
        }
    }

    fn bind_binary_op(&self, op: &ast::BinaryOperator) -> Result<BinaryOp> {
        match op {
            ast::BinaryOperator::Plus => Ok(BinaryOp::Add),
            ast::BinaryOperator::Minus => Ok(BinaryOp::Sub),
            ast::BinaryOperator::Multiply => Ok(BinaryOp::Mul),
            ast::BinaryOperator::Divide => Ok(BinaryOp::Div),
            ast::BinaryOperator::Modulo => Ok(BinaryOp::Mod),
            ast::BinaryOperator::Eq => Ok(BinaryOp::Eq),
            ast::BinaryOperator::NotEq => Ok(BinaryOp::Ne),
            ast::BinaryOperator::Lt => Ok(BinaryOp::Lt),
            ast::BinaryOperator::LtEq => Ok(BinaryOp::Le),
            ast::BinaryOperator::Gt => Ok(BinaryOp::Gt),
            ast::BinaryOperator::GtEq => Ok(BinaryOp::Ge),
            ast::BinaryOperator::And => Ok(BinaryOp::And),
            ast::BinaryOperator::Or => Ok(BinaryOp::Or),
            _ => Err(TiSqlError::Bind(format!("Unsupported operator: {op:?}"))),
        }
    }

    fn bind_insert(
        &self,
        ignore: bool,
        table_name: ast::ObjectName,
        columns: Vec<ast::Ident>,
        source: Option<Box<Query>>,
    ) -> Result<LogicalPlan> {
        let name = table_name.to_string();
        // Use MVCC-aware schema lookup if snapshot_ts is set
        let table = match self.snapshot_ts {
            Some(ts) => self.catalog.get_table_at(&self.current_schema, &name, ts)?,
            None => self.catalog.get_table(&self.current_schema, &name)?,
        }
        .ok_or_else(|| TiSqlError::TableNotFound(name.clone()))?;

        let col_ids: Vec<u32> = if columns.is_empty() {
            // All columns
            table.columns().iter().map(|c| c.id()).collect()
        } else {
            columns
                .iter()
                .map(|ident| {
                    table
                        .column_by_name(&ident.value)
                        .map(|c| c.id())
                        .ok_or_else(|| TiSqlError::ColumnNotFound(ident.value.clone()))
                })
                .collect::<Result<Vec<_>>>()?
        };

        let values = match source.as_ref().map(|s| s.body.as_ref()) {
            Some(SetExpr::Values(values)) => values
                .rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|e| self.bind_expr(e, &[]))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?,
            _ => return Err(TiSqlError::Bind("INSERT requires VALUES clause".into())),
        };

        Ok(LogicalPlan::Insert {
            table,
            columns: col_ids,
            values,
            ignore,
        })
    }

    fn bind_update(
        &self,
        table: ast::TableWithJoins,
        assignments: Vec<ast::Assignment>,
        selection: Option<SqlExpr>,
    ) -> Result<LogicalPlan> {
        let table_def = self.bind_table_factor(&table.relation)?;
        let table = match &table_def {
            LogicalPlan::Scan { table, .. } => table.clone(),
            _ => return Err(TiSqlError::Bind("Invalid UPDATE target".into())),
        };

        let tables = vec![&table];

        let assigns = assignments
            .iter()
            .map(|a| {
                // In sqlparser 0.40+, Assignment has `id` field (Vec<Ident>)
                let col_name =
                    a.id.first()
                        .ok_or_else(|| TiSqlError::Bind("Invalid assignment target".into()))?;
                let col = table
                    .column_by_name(&col_name.value)
                    .ok_or_else(|| TiSqlError::ColumnNotFound(col_name.value.clone()))?;
                let value = self.bind_expr(&a.value, &tables)?;
                Ok((col.id(), value))
            })
            .collect::<Result<Vec<_>>>()?;

        let filter = selection.map(|s| self.bind_expr(&s, &tables)).transpose()?;
        let input = self.build_dml_input_plan(&table, filter);

        Ok(LogicalPlan::Update {
            table,
            assignments: assigns,
            input: Box::new(input),
        })
    }

    fn bind_delete(
        &self,
        from: Vec<ast::TableWithJoins>,
        selection: Option<SqlExpr>,
    ) -> Result<LogicalPlan> {
        if from.is_empty() {
            return Err(TiSqlError::Bind("DELETE requires FROM clause".into()));
        }

        let table_def = self.bind_table_factor(&from[0].relation)?;
        let table = match &table_def {
            LogicalPlan::Scan { table, .. } => table.clone(),
            _ => return Err(TiSqlError::Bind("Invalid DELETE target".into())),
        };

        let tables = vec![&table];

        let filter = selection.map(|s| self.bind_expr(&s, &tables)).transpose()?;
        let input = self.build_dml_input_plan(&table, filter);

        Ok(LogicalPlan::Delete {
            table,
            input: Box::new(input),
        })
    }

    fn bind_create_table(
        &self,
        name: ast::ObjectName,
        columns: Vec<ast::ColumnDef>,
        constraints: Vec<ast::TableConstraint>,
        if_not_exists: bool,
    ) -> Result<LogicalPlan> {
        let table_name = name
            .0
            .last()
            .ok_or_else(|| TiSqlError::Bind("Invalid table name".into()))?
            .value
            .clone();

        let schema = if name.0.len() > 1 {
            name.0[0].value.clone()
        } else {
            self.current_schema.clone()
        };

        let table_id = self.catalog.next_table_id()?;

        let mut col_defs = Vec::new();
        let mut primary_key = Vec::new();

        for (idx, col) in columns.iter().enumerate() {
            let col_id = idx as u32;
            let data_type = self.bind_data_type(&col.data_type)?;

            let mut nullable = true;
            let auto_increment = false;

            for option in &col.options {
                match &option.option {
                    ast::ColumnOption::NotNull => nullable = false,
                    ast::ColumnOption::Null => nullable = true,
                    ast::ColumnOption::Unique { is_primary, .. } => {
                        if *is_primary {
                            primary_key.push(col_id);
                            nullable = false;
                        }
                    }
                    _ => {}
                }
            }

            col_defs.push(ColumnDef::new(
                col_id,
                col.name.value.clone(),
                data_type,
                nullable,
                None,
                auto_increment,
            ));
        }

        // Check table constraints for PRIMARY KEY
        for constraint in &constraints {
            if let ast::TableConstraint::Unique {
                columns: pk_cols,
                is_primary: true,
                ..
            } = constraint
            {
                for pk_col in pk_cols {
                    if let Some(col) = col_defs.iter().find(|c| c.name() == pk_col.value) {
                        if !primary_key.contains(&col.id()) {
                            primary_key.push(col.id());
                        }
                    }
                }
            }
        }

        let table_def = TableDef::new(table_id, table_name, schema, col_defs, primary_key);

        Ok(LogicalPlan::CreateTable {
            table: table_def,
            if_not_exists,
        })
    }

    fn bind_drop(
        &self,
        object_type: ast::ObjectType,
        names: Vec<ast::ObjectName>,
        if_exists: bool,
    ) -> Result<LogicalPlan> {
        match object_type {
            ast::ObjectType::Table => {
                let name = names
                    .first()
                    .ok_or_else(|| TiSqlError::Bind("DROP TABLE requires table name".into()))?;

                let (schema, table) = if name.0.len() == 1 {
                    (self.current_schema.clone(), name.0[0].value.clone())
                } else if name.0.len() == 2 {
                    (name.0[0].value.clone(), name.0[1].value.clone())
                } else {
                    return Err(TiSqlError::Bind("Invalid table name".into()));
                };

                Ok(LogicalPlan::DropTable {
                    schema,
                    table,
                    if_exists,
                })
            }
            _ => Err(TiSqlError::Bind(format!(
                "DROP {object_type:?} not supported"
            ))),
        }
    }

    fn bind_values(&self, values: ast::Values) -> Result<LogicalPlan> {
        let rows = values
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|e| self.bind_expr(e, &[]))
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;

        // Infer schema from first row
        let schema = if let Some(first_row) = rows.first() {
            let columns = first_row
                .iter()
                .enumerate()
                .map(|(i, expr)| ColumnInfo::new(format!("column{i}"), expr.data_type(), true))
                .collect();
            Schema::new(columns)
        } else {
            Schema::new(vec![])
        };

        Ok(LogicalPlan::Values { rows, schema })
    }

    fn bind_show_create(
        &self,
        obj_type: ast::ShowCreateObject,
        obj_name: ast::ObjectName,
    ) -> Result<LogicalPlan> {
        if obj_type != ast::ShowCreateObject::Table {
            return Err(TiSqlError::Bind(format!(
                "SHOW CREATE {obj_type} not supported"
            )));
        }

        let (schema_name, table_name) = match obj_name.0.len() {
            1 => (self.current_schema.as_str(), obj_name.0[0].value.as_str()),
            2 => (obj_name.0[0].value.as_str(), obj_name.0[1].value.as_str()),
            _ => return Err(TiSqlError::Bind("Invalid table name".into())),
        };

        let table = match self.snapshot_ts {
            Some(ts) => self.catalog.get_table_at(schema_name, table_name, ts)?,
            None => self.catalog.get_table(schema_name, table_name)?,
        }
        .ok_or_else(|| TiSqlError::TableNotFound(format!("{schema_name}.{table_name}")))?;

        let ddl = render_create_table(&table)?;
        let values_schema = Schema::new(vec![
            ColumnInfo::new("Table".to_string(), DataType::Varchar(255), false),
            ColumnInfo::new("Create Table".to_string(), DataType::Text, false),
        ]);
        let rows = vec![vec![
            Expr::Literal(Value::String(table.name().to_string())),
            Expr::Literal(Value::String(ddl)),
        ]];

        Ok(LogicalPlan::Values {
            rows,
            schema: values_schema,
        })
    }

    fn bind_data_type(&self, dt: &ast::DataType) -> Result<DataType> {
        match dt {
            ast::DataType::Boolean => Ok(DataType::Boolean),
            ast::DataType::TinyInt(_) => Ok(DataType::TinyInt),
            ast::DataType::SmallInt(_) => Ok(DataType::SmallInt),
            ast::DataType::Int(_) | ast::DataType::Integer(_) => Ok(DataType::Int),
            ast::DataType::BigInt(_) => Ok(DataType::BigInt),
            ast::DataType::Float(_) => Ok(DataType::Float),
            ast::DataType::Double | ast::DataType::DoublePrecision => Ok(DataType::Double),
            ast::DataType::Decimal(info) | ast::DataType::Numeric(info) => {
                let (p, s) = match info {
                    ast::ExactNumberInfo::PrecisionAndScale(p, s) => (*p as u8, *s as u8),
                    ast::ExactNumberInfo::Precision(p) => (*p as u8, 0),
                    ast::ExactNumberInfo::None => (38, 10),
                };
                Ok(DataType::Decimal {
                    precision: p,
                    scale: s,
                })
            }
            ast::DataType::Char(len) => {
                let n = len
                    .as_ref()
                    .map(|l| match l {
                        ast::CharacterLength::IntegerLength { length, .. } => *length as u16,
                        ast::CharacterLength::Max => 255,
                    })
                    .unwrap_or(1);
                Ok(DataType::Char(n))
            }
            ast::DataType::Varchar(len) => {
                let n = len
                    .as_ref()
                    .map(|l| match l {
                        ast::CharacterLength::IntegerLength { length, .. } => *length as u16,
                        ast::CharacterLength::Max => 65535,
                    })
                    .unwrap_or(255);
                Ok(DataType::Varchar(n))
            }
            ast::DataType::Text => Ok(DataType::Text),
            ast::DataType::Blob(_) => Ok(DataType::Blob),
            ast::DataType::Date => Ok(DataType::Date),
            ast::DataType::Time(_, _) => Ok(DataType::Time),
            ast::DataType::Datetime(_) => Ok(DataType::DateTime),
            ast::DataType::Timestamp(_, _) => Ok(DataType::Timestamp),
            _ => Err(TiSqlError::Bind(format!("Unsupported data type: {dt:?}"))),
        }
    }

    fn try_build_point_get(&self, plan: &LogicalPlan, predicate: &Expr) -> Option<PointGetRewrite> {
        let table = match plan {
            LogicalPlan::Scan {
                table,
                filter: None,
                projection: None,
            } => table,
            _ => return None,
        };

        let pk_indices = table.pk_column_indices();
        if pk_indices.is_empty() {
            return None;
        }

        let mut conjuncts = Vec::new();
        Self::collect_conjuncts(predicate, &mut conjuncts);
        let mut consumed = vec![false; conjuncts.len()];

        let mut pk_eq_values: BTreeMap<usize, Value> = BTreeMap::new();
        for (idx, conjunct) in conjuncts.iter().enumerate() {
            let Some((column_idx, literal)) = Self::extract_column_eq_literal(conjunct) else {
                continue;
            };
            if !pk_indices.contains(&column_idx) {
                continue;
            }

            let column = table.columns().get(column_idx)?;
            if !Self::point_get_literal_matches_pk(column.data_type(), literal) {
                return None;
            }

            match pk_eq_values.get(&column_idx) {
                Some(existing) if existing != literal => {
                    return Some(PointGetRewrite {
                        plan: LogicalPlan::Empty {
                            schema: Self::table_to_schema(table),
                        },
                        residual_predicate: None,
                    });
                }
                Some(_) => {}
                None => {
                    pk_eq_values.insert(column_idx, literal.clone());
                }
            }
            consumed[idx] = true;
        }

        let mut pk_values = Vec::with_capacity(pk_indices.len());
        for idx in &pk_indices {
            pk_values.push(pk_eq_values.get(idx)?.clone());
        }

        let residual_predicate = Self::conjoin_exprs(
            conjuncts
                .into_iter()
                .zip(consumed)
                .filter_map(|(expr, is_consumed)| (!is_consumed).then_some(expr.clone()))
                .collect(),
        );
        let key = encode_key(table.id(), &encode_pk(&pk_values));
        Some(PointGetRewrite {
            plan: LogicalPlan::PointGet {
                table: table.clone(),
                key,
            },
            residual_predicate,
        })
    }

    fn build_dml_input_plan(&self, table: &TableDef, predicate: Option<Expr>) -> LogicalPlan {
        let base_scan = LogicalPlan::Scan {
            table: table.clone(),
            projection: None,
            filter: None,
        };

        let Some(predicate) = predicate else {
            return base_scan;
        };

        if let Some(rewrite) = self.try_build_point_get(&base_scan, &predicate) {
            if let Some(residual) = rewrite.residual_predicate {
                LogicalPlan::Filter {
                    input: Box::new(rewrite.plan),
                    predicate: residual,
                }
            } else {
                rewrite.plan
            }
        } else {
            LogicalPlan::Scan {
                table: table.clone(),
                projection: None,
                filter: Some(predicate),
            }
        }
    }

    fn conjoin_exprs(exprs: Vec<Expr>) -> Option<Expr> {
        let mut iter = exprs.into_iter();
        let first = iter.next()?;
        Some(iter.fold(first, |acc, expr| Expr::BinaryOp {
            left: Box::new(acc),
            op: BinaryOp::And,
            right: Box::new(expr),
        }))
    }

    fn collect_conjuncts<'b>(expr: &'b Expr, out: &mut Vec<&'b Expr>) {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                Self::collect_conjuncts(left, out);
                Self::collect_conjuncts(right, out);
            }
            _ => out.push(expr),
        }
    }

    fn extract_column_eq_literal(expr: &Expr) -> Option<(usize, &Value)> {
        let Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } = expr
        else {
            return None;
        };

        match (left.as_ref(), right.as_ref()) {
            (Expr::Column { column_idx, .. }, Expr::Literal(v)) => Some((*column_idx, v)),
            (Expr::Literal(v), Expr::Column { column_idx, .. }) => Some((*column_idx, v)),
            _ => None,
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
            (DataType::Float, Value::Float(_)) => true,
            (DataType::Double, Value::Double(_)) => true,
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

    fn table_to_schema(table: &TableDef) -> Schema {
        Schema::new(
            table
                .columns()
                .iter()
                .map(|c| ColumnInfo::new(c.name().to_string(), c.data_type().clone(), c.nullable()))
                .collect(),
        )
    }

    #[allow(clippy::only_used_in_recursion)]
    fn collect_tables<'b>(&'b self, plan: &'b LogicalPlan) -> Vec<&'b TableDef> {
        match plan {
            LogicalPlan::Scan { table, .. } | LogicalPlan::PointGet { table, .. } => vec![table],
            LogicalPlan::Project { input, .. } => self.collect_tables(input),
            LogicalPlan::Filter { input, .. } => self.collect_tables(input),
            LogicalPlan::Join { left, right, .. } => {
                let mut tables = self.collect_tables(left);
                tables.extend(self.collect_tables(right));
                tables
            }
            LogicalPlan::Aggregate { input, .. } => self.collect_tables(input),
            LogicalPlan::Sort { input, .. } => self.collect_tables(input),
            LogicalPlan::Limit { input, .. } => self.collect_tables(input),
            _ => vec![],
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn collect_tables_owned(&self, plan: &LogicalPlan) -> Vec<TableDef> {
        match plan {
            LogicalPlan::Scan { table, .. } | LogicalPlan::PointGet { table, .. } => {
                vec![table.clone()]
            }
            LogicalPlan::Project { input, .. } => self.collect_tables_owned(input),
            LogicalPlan::Filter { input, .. } => self.collect_tables_owned(input),
            LogicalPlan::Join { left, right, .. } => {
                let mut tables = self.collect_tables_owned(left);
                tables.extend(self.collect_tables_owned(right));
                tables
            }
            LogicalPlan::Aggregate { input, .. } => self.collect_tables_owned(input),
            LogicalPlan::Sort { input, .. } => self.collect_tables_owned(input),
            LogicalPlan::Limit { input, .. } => self.collect_tables_owned(input),
            _ => vec![],
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn contains_aggregate(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Aggregate { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                self.contains_aggregate(left) || self.contains_aggregate(right)
            }
            Expr::UnaryOp { expr, .. } => self.contains_aggregate(expr),
            Expr::Function { args, .. } => args.iter().any(|a| self.contains_aggregate(a)),
            _ => false,
        }
    }

    fn expr_alias(&self, expr: &SqlExpr) -> String {
        match expr {
            SqlExpr::Identifier(ident) => ident.value.clone(),
            SqlExpr::CompoundIdentifier(idents) => {
                idents.last().map(|i| i.value.clone()).unwrap_or_default()
            }
            SqlExpr::Function(func) => func.name.to_string(),
            _ => "?column?".to_string(),
        }
    }

    fn expr_to_usize(&self, expr: &SqlExpr) -> Result<usize> {
        match expr {
            SqlExpr::Value(SqlValue::Number(n, _)) => n
                .parse()
                .map_err(|_| TiSqlError::Bind("Invalid LIMIT/OFFSET value".into())),
            _ => Err(TiSqlError::Bind("LIMIT/OFFSET must be a number".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser as SqlParser;

    use super::*;
    use crate::catalog::{DefaultValue, IndexDef, MemoryCatalog};
    use crate::tablet::{encode_key, encode_pk};

    fn bind_sql(catalog: &MemoryCatalog, sql: &str) -> LogicalPlan {
        bind_sql_result(catalog, sql).unwrap()
    }

    fn bind_sql_result(catalog: &MemoryCatalog, sql: &str) -> Result<LogicalPlan> {
        let mut stmts = SqlParser::parse_sql(&MySqlDialect {}, sql).unwrap();
        let stmt = stmts.remove(0);
        Binder::new(catalog, "default").bind(stmt)
    }

    fn setup_table() -> MemoryCatalog {
        let catalog = MemoryCatalog::new();
        let table = TableDef::new(
            42,
            "t".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::BigInt, false, None, false),
                ColumnDef::new(2, "name".into(), DataType::Varchar(64), true, None, false),
            ],
            vec![1],
        );
        crate::io::block_on_sync(catalog.create_table(table)).unwrap();
        catalog
    }

    fn setup_table_with_composite_pk() -> MemoryCatalog {
        let catalog = setup_table();
        let table = TableDef::new(
            43,
            "t2".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::BigInt, false, None, false),
                ColumnDef::new(
                    2,
                    "region".into(),
                    DataType::Varchar(16),
                    false,
                    None,
                    false,
                ),
                ColumnDef::new(3, "name".into(), DataType::Varchar(64), true, None, false),
            ],
            vec![1, 2],
        );
        crate::io::block_on_sync(catalog.create_table(table)).unwrap();
        catalog
    }

    fn setup_table_with_int_pk() -> MemoryCatalog {
        let catalog = MemoryCatalog::new();
        let table = TableDef::new(
            44,
            "t_int".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::Int, false, None, false),
                ColumnDef::new(2, "name".into(), DataType::Varchar(64), true, None, false),
            ],
            vec![1],
        );
        crate::io::block_on_sync(catalog.create_table(table)).unwrap();
        catalog
    }

    fn setup_table_for_show_create(schema: &str, table_name: &str) -> MemoryCatalog {
        let catalog = MemoryCatalog::new();
        if schema != "default" {
            crate::io::block_on_sync(catalog.create_schema(schema)).unwrap();
        }

        let mut table = TableDef::new(
            100,
            table_name.to_string(),
            schema.to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::Int, false, None, true),
                ColumnDef::new(
                    2,
                    "name".into(),
                    DataType::Varchar(64),
                    true,
                    Some(DefaultValue::String("o'reilly".into())),
                    false,
                ),
                ColumnDef::new(
                    3,
                    "enabled".into(),
                    DataType::Boolean,
                    false,
                    Some(DefaultValue::Bool(true)),
                    false,
                ),
            ],
            vec![1],
        );
        table.add_index(IndexDef::new(7, "idx_name".into(), vec![2], false));
        table.add_index(IndexDef::new(8, "uniq_enabled".into(), vec![3], true));
        for _ in 0..5 {
            table.increment_auto_id();
        }
        crate::io::block_on_sync(catalog.create_table(table)).unwrap();
        catalog
    }

    #[test]
    fn test_bind_select_pk_eq_with_residual_filter_generates_point_get() {
        let catalog = setup_table();
        let plan = bind_sql(&catalog, "SELECT * FROM t WHERE id = 1 AND name = 'alice'");
        match plan {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::Filter { input, .. } => {
                    assert!(matches!(*input, LogicalPlan::PointGet { .. }));
                }
                _ => panic!("expected Filter over PointGet"),
            },
            _ => panic!("expected Project plan root"),
        }
    }

    #[test]
    fn test_bind_select_pk_only_eq_generates_bare_point_get() {
        let catalog = setup_table();
        let plan = bind_sql(&catalog, "SELECT * FROM t WHERE id = 1");
        match plan {
            LogicalPlan::Project { input, .. } => {
                assert!(matches!(*input, LogicalPlan::PointGet { .. }));
            }
            _ => panic!("expected Project plan root"),
        }
    }

    #[test]
    fn test_bind_select_non_pk_filter_keeps_scan() {
        let catalog = setup_table();
        let plan = bind_sql(&catalog, "SELECT * FROM t WHERE name = 'alice'");
        match plan {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::Filter { input, .. } => {
                    assert!(matches!(*input, LogicalPlan::Scan { .. }));
                }
                _ => panic!("expected Filter over Scan"),
            },
            _ => panic!("expected Project plan root"),
        }
    }

    #[test]
    fn test_bind_select_pk_eq_with_string_literal_keeps_scan() {
        let catalog = setup_table();
        let plan = bind_sql(&catalog, "SELECT * FROM t WHERE id = '1'");
        match plan {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::Filter { input, .. } => {
                    assert!(matches!(*input, LogicalPlan::Scan { .. }));
                }
                _ => panic!("expected Filter over Scan"),
            },
            _ => panic!("expected Project plan root"),
        }
    }

    #[test]
    fn test_bind_select_int_pk_literal_uses_point_get() {
        let catalog = setup_table_with_int_pk();
        let plan = bind_sql(&catalog, "SELECT * FROM t_int WHERE id = 1");
        match plan {
            LogicalPlan::Project { input, .. } => {
                assert!(matches!(*input, LogicalPlan::PointGet { .. }));
            }
            _ => panic!("expected Project plan root"),
        }
    }

    #[test]
    fn test_bind_select_pk_eq_null_keeps_scan() {
        let catalog = setup_table();
        let plan = bind_sql(&catalog, "SELECT * FROM t WHERE id = NULL");
        match plan {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::Filter { input, .. } => {
                    assert!(matches!(*input, LogicalPlan::Scan { .. }));
                }
                _ => panic!("expected Filter over Scan"),
            },
            _ => panic!("expected Project plan root"),
        }
    }

    #[test]
    fn test_bind_select_composite_pk_eq_generates_point_get() {
        let catalog = setup_table_with_composite_pk();
        let plan = bind_sql(&catalog, "SELECT * FROM t2 WHERE id = 7 AND region = 'us'");
        match plan {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::PointGet { key, .. } => {
                    let expected = encode_key(
                        43,
                        &encode_pk(&[Value::BigInt(7), Value::String("us".into())]),
                    );
                    assert_eq!(key, expected);
                }
                _ => panic!("expected bare PointGet"),
            },
            _ => panic!("expected Project plan root"),
        }
    }

    #[test]
    fn test_bind_select_composite_pk_missing_component_keeps_scan() {
        let catalog = setup_table_with_composite_pk();
        let plan = bind_sql(&catalog, "SELECT * FROM t2 WHERE id = 7");
        match plan {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::Filter { input, .. } => {
                    assert!(matches!(*input, LogicalPlan::Scan { .. }));
                }
                _ => panic!("expected Filter over Scan"),
            },
            _ => panic!("expected Project plan root"),
        }
    }

    #[test]
    fn test_bind_select_or_predicate_keeps_scan() {
        let catalog = setup_table();
        let plan = bind_sql(&catalog, "SELECT * FROM t WHERE id = 1 OR name = 'alice'");
        match plan {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::Filter { input, .. } => {
                    assert!(matches!(*input, LogicalPlan::Scan { .. }));
                }
                _ => panic!("expected Filter over Scan"),
            },
            _ => panic!("expected Project plan root"),
        }
    }

    #[test]
    fn test_bind_select_point_get_with_order_by_and_limit() {
        let catalog = setup_table();
        let plan = bind_sql(
            &catalog,
            "SELECT * FROM t WHERE id = 1 ORDER BY name LIMIT 1",
        );
        match plan {
            LogicalPlan::Limit { input, .. } => match *input {
                LogicalPlan::Sort { input, .. } => match *input {
                    LogicalPlan::Project { input, .. } => {
                        assert!(matches!(*input, LogicalPlan::PointGet { .. }));
                    }
                    _ => panic!("expected Project under Sort"),
                },
                _ => panic!("expected Sort under Limit"),
            },
            _ => panic!("expected Limit plan root"),
        }
    }

    #[test]
    fn test_bind_select_contradictory_pk_eq_generates_empty() {
        let catalog = setup_table();
        let plan = bind_sql(&catalog, "SELECT * FROM t WHERE id = 1 AND id = 2");
        match plan {
            LogicalPlan::Project { input, .. } => {
                assert!(matches!(*input, LogicalPlan::Empty { .. }));
            }
            _ => panic!("expected Project plan root"),
        }
    }

    #[test]
    fn test_bind_update_pk_eq_generates_point_get_child() {
        let catalog = setup_table();
        let plan = bind_sql(&catalog, "UPDATE t SET name = 'bob' WHERE id = 1");
        match plan {
            LogicalPlan::Update { input, .. } => {
                assert!(matches!(*input, LogicalPlan::PointGet { .. }));
            }
            _ => panic!("expected Update root"),
        }
    }

    #[test]
    fn test_bind_delete_pk_eq_with_residual_generates_filter_over_point_get_child() {
        let catalog = setup_table();
        let plan = bind_sql(&catalog, "DELETE FROM t WHERE id = 1 AND name = 'alice'");
        match plan {
            LogicalPlan::Delete { input, .. } => match *input {
                LogicalPlan::Filter { input, .. } => {
                    assert!(matches!(*input, LogicalPlan::PointGet { .. }));
                }
                _ => panic!("expected Filter over PointGet"),
            },
            _ => panic!("expected Delete root"),
        }
    }

    #[test]
    fn test_bind_insert_ignore_sets_plan_flag() {
        let catalog = setup_table();
        let plan = bind_sql(
            &catalog,
            "INSERT IGNORE INTO t(id, name) VALUES (1, 'alice')",
        );
        match plan {
            LogicalPlan::Insert { ignore, .. } => assert!(ignore),
            _ => panic!("expected Insert root"),
        }
    }

    #[test]
    fn test_bind_show_create_table_returns_values_plan() {
        let catalog = setup_table_for_show_create("default", "show_t");
        let plan = bind_sql(&catalog, "SHOW CREATE TABLE show_t");
        match plan {
            LogicalPlan::Values { rows, schema } => {
                assert_eq!(schema.column_count(), 2);
                assert_eq!(schema.column(0).unwrap().name(), "Table");
                assert_eq!(schema.column(1).unwrap().name(), "Create Table");
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].len(), 2);
                match &rows[0][0] {
                    Expr::Literal(Value::String(v)) => assert_eq!(v, "show_t"),
                    _ => panic!("expected table name literal"),
                }
                match &rows[0][1] {
                    Expr::Literal(Value::String(v)) => {
                        assert!(v.starts_with("CREATE TABLE `default`.`show_t` ("));
                        assert!(v.contains("PRIMARY KEY (`id`)"));
                        assert!(v.contains("KEY `idx_name` (`name`)"));
                    }
                    _ => panic!("expected create table literal"),
                }
            }
            _ => panic!("expected Values root"),
        }
    }

    #[test]
    fn test_bind_show_create_table_schema_qualified_name() {
        let catalog = setup_table_for_show_create("analytics", "events");
        let plan = bind_sql(&catalog, "SHOW CREATE TABLE analytics.events");
        match plan {
            LogicalPlan::Values { rows, .. } => match &rows[0][1] {
                Expr::Literal(Value::String(v)) => {
                    assert!(v.starts_with("CREATE TABLE `analytics`.`events` ("));
                }
                _ => panic!("expected create table literal"),
            },
            _ => panic!("expected Values root"),
        }
    }

    #[test]
    fn test_bind_show_create_invalid_multipart_name_returns_error() {
        let catalog = setup_table();
        let err = bind_sql_result(&catalog, "SHOW CREATE TABLE a.b.c").unwrap_err();
        assert!(matches!(err, TiSqlError::Bind(_)));
        assert!(err.to_string().contains("Invalid table name"));
    }

    #[test]
    fn test_bind_show_create_empty_object_name_returns_error() {
        let catalog = setup_table();
        let stmt = SqlStatement::ShowCreate {
            obj_type: ast::ShowCreateObject::Table,
            obj_name: ast::ObjectName(vec![]),
        };
        let err = Binder::new(&catalog, "default").bind(stmt).unwrap_err();
        assert!(matches!(err, TiSqlError::Bind(_)));
        assert!(err.to_string().contains("Invalid table name"));
    }

    #[test]
    fn test_bind_show_create_not_found_returns_table_not_found() {
        let catalog = setup_table();
        let err = bind_sql_result(&catalog, "SHOW CREATE TABLE not_exist").unwrap_err();
        assert!(matches!(err, TiSqlError::TableNotFound(_)));
        assert!(err.to_string().contains("default.not_exist"));
    }

    #[test]
    fn test_bind_show_create_unsupported_object_type_returns_error() {
        let catalog = setup_table();
        let err = bind_sql_result(&catalog, "SHOW CREATE VIEW v").unwrap_err();
        assert!(matches!(err, TiSqlError::Bind(_)));
        assert!(err.to_string().contains("SHOW CREATE VIEW not supported"));
    }
}
