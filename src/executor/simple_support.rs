use super::*;

// ============================================================================
// Expression Evaluation (free functions)
// ============================================================================

pub(super) fn eval_expr(expr: &Expr, row: &Row) -> Result<Value> {
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

pub(super) fn eval_binary_op(left: &Value, op: BinaryOp, right: &Value) -> Result<Value> {
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

pub(super) fn eval_unary_op(op: UnaryOp, val: &Value) -> Result<Value> {
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

pub(super) fn numeric_op<F>(left: &Value, right: &Value, op: F) -> Result<Value>
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

pub(super) fn value_to_f64(val: &Value) -> Result<f64> {
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

pub(super) fn value_to_bool(val: &Value) -> Result<bool> {
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

pub(super) fn value_to_string(val: &Value) -> String {
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

pub(super) fn compare_values(left: &Value, right: &Value) -> std::cmp::Ordering {
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

pub(super) fn like_match(text: &str, pattern: &str) -> bool {
    let regex_pattern = pattern.replace('%', ".*").replace('_', ".");
    regex::Regex::new(&format!("^{regex_pattern}$"))
        .map(|re| re.is_match(text))
        .unwrap_or(false)
}

pub(super) fn cast_value(val: &Value, target: &DataType) -> Result<Value> {
    if val.is_null() {
        return Ok(Value::Null);
    }

    match (val, target) {
        (Value::Boolean(v), DataType::Boolean) => return Ok(Value::Boolean(*v)),
        (Value::TinyInt(v), DataType::TinyInt) => return Ok(Value::TinyInt(*v)),
        (Value::SmallInt(v), DataType::SmallInt) => return Ok(Value::SmallInt(*v)),
        (Value::Int(v), DataType::Int) => return Ok(Value::Int(*v)),
        (Value::BigInt(v), DataType::BigInt) => return Ok(Value::BigInt(*v)),
        (Value::Float(v), DataType::Float) => return Ok(Value::Float(*v)),
        (Value::Double(v), DataType::Double) => return Ok(Value::Double(*v)),
        (Value::Decimal(v), DataType::Decimal { .. }) => return Ok(Value::Decimal(v.clone())),
        (Value::String(v), DataType::Char(_))
        | (Value::String(v), DataType::Varchar(_))
        | (Value::String(v), DataType::Text) => return Ok(Value::String(v.clone())),
        (Value::Bytes(v), DataType::Blob) => return Ok(Value::Bytes(v.clone())),
        (Value::Date(v), DataType::Date) => return Ok(Value::Date(*v)),
        (Value::Time(v), DataType::Time) => return Ok(Value::Time(*v)),
        (Value::DateTime(v), DataType::DateTime) => return Ok(Value::DateTime(*v)),
        (Value::Timestamp(v), DataType::Timestamp) => return Ok(Value::Timestamp(*v)),
        _ => {}
    }

    match target {
        DataType::Boolean => Ok(Value::Boolean(value_to_bool(val)?)),
        DataType::TinyInt => Ok(Value::TinyInt(value_to_f64(val)? as i8)),
        DataType::SmallInt => Ok(Value::SmallInt(value_to_f64(val)? as i16)),
        DataType::Int => Ok(Value::Int(value_to_f64(val)? as i32)),
        DataType::BigInt => Ok(Value::BigInt(value_to_f64(val)? as i64)),
        DataType::Float => Ok(Value::Float(value_to_f64(val)? as f32)),
        DataType::Double => Ok(Value::Double(value_to_f64(val)?)),
        DataType::Decimal { .. } => Ok(Value::Decimal(value_to_string(val))),
        DataType::Char(_) | DataType::Varchar(_) | DataType::Text => {
            Ok(Value::String(value_to_string(val)))
        }
        DataType::Blob => match val {
            Value::String(s) => Ok(Value::Bytes(s.as_bytes().to_vec())),
            Value::Bytes(bytes) => Ok(Value::Bytes(bytes.clone())),
            _ => Err(TiSqlError::Execution("Cannot convert to blob".into())),
        },
        DataType::Date => Ok(Value::Date(value_to_f64(val)? as i32)),
        DataType::Time => Ok(Value::Time(value_to_f64(val)? as i64)),
        DataType::DateTime => Ok(Value::DateTime(value_to_f64(val)? as i64)),
        DataType::Timestamp => Ok(Value::Timestamp(value_to_f64(val)? as i64)),
    }
}

pub(super) fn normalize_row_values_for_table(
    table: &TableDef,
    row_values: &mut [Value],
) -> Result<()> {
    for (value, column) in row_values.iter_mut().zip(table.columns()) {
        *value = cast_value(value, column.data_type())?;
    }
    Ok(())
}

pub(super) fn compute_aggregate(func: &AggFunc, arg: &Expr, rows: &[Row]) -> Result<Value> {
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

pub(super) enum DmlReadPlan {
    Empty,
    PointGet {
        key: LogicalPointKey,
        filter: Option<Expr>,
    },
    Scan {
        bounds: LogicalScanBounds,
        filter: Option<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum LogicalPkScanBounds {
    Empty,
    Bounded(LogicalScanBounds),
}

pub(super) enum AutoIncrementRowAction {
    Generate,
    ExplicitNoObserve,
    ExplicitObserve(u64),
}

pub(super) fn generated_auto_increment_value(next_id: u64, data_type: &DataType) -> Result<Value> {
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

pub(super) fn classify_auto_increment_row_value(
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

pub(super) fn conjoin_predicate(existing: Option<Expr>, predicate: Expr) -> Option<Expr> {
    match existing {
        Some(prev) => Some(Expr::BinaryOp {
            left: Box::new(prev),
            op: BinaryOp::And,
            right: Box::new(predicate),
        }),
        None => Some(predicate),
    }
}

pub(super) fn row_matches_filter(filter: Option<&Expr>, row: &Row) -> Result<bool> {
    let Some(predicate) = filter else {
        return Ok(true);
    };
    let result = eval_expr(predicate, row)?;
    value_to_bool(&result)
}

pub(super) async fn collect_execution_scan_rows<B: ExecutionBackend>(
    execution_backend: &B,
    ctx: &TxnCtx,
    req: ScanRequest,
) -> Result<Vec<ExecutionRow>> {
    let mut scan = execution_backend.scan(ctx, req)?;
    let mut rows = Vec::new();
    while let Some(row) = scan.next_row().await? {
        rows.push(row);
    }
    Ok(rows)
}

pub(super) fn compose_write_request(
    primary: Option<WriteRequest>,
    lock_request: Option<LockRequest>,
) -> Option<WriteRequest> {
    match (primary, lock_request) {
        (None, None) => None,
        (Some(primary), None) => Some(primary),
        (None, Some(lock_request)) => Some(WriteRequest::Lock(lock_request)),
        (Some(primary), Some(lock_request)) => Some(WriteRequest::Batch(vec![
            primary,
            WriteRequest::Lock(lock_request),
        ])),
    }
}

pub(super) fn collect_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
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

pub(super) fn point_get_literal_matches_pk(pk_type: &DataType, literal: &Value) -> bool {
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

pub(super) fn compare_scan_values(left: &[Value], right: &[Value]) -> Option<std::cmp::Ordering> {
    left.iter()
        .zip(right)
        .map(|(left, right)| compare_scan_value(left, right))
        .find(|ordering| !matches!(ordering, Some(std::cmp::Ordering::Equal)))
        .unwrap_or(Some(left.len().cmp(&right.len())))
}

pub(super) fn compare_scan_value(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::TinyInt(left), Value::TinyInt(right)) => Some(left.cmp(right)),
        (Value::SmallInt(left), Value::SmallInt(right)) => Some(left.cmp(right)),
        (Value::Int(left), Value::Int(right)) => Some(left.cmp(right)),
        (Value::BigInt(left), Value::BigInt(right)) => Some(left.cmp(right)),
        (Value::TinyInt(left), Value::BigInt(right)) => Some(i64::from(*left).cmp(right)),
        (Value::BigInt(left), Value::TinyInt(right)) => Some(left.cmp(&i64::from(*right))),
        (Value::Int(left), Value::BigInt(right)) => Some(i64::from(*left).cmp(right)),
        (Value::BigInt(left), Value::Int(right)) => Some(left.cmp(&i64::from(*right))),
        (Value::SmallInt(left), Value::BigInt(right)) => Some(i64::from(*left).cmp(right)),
        (Value::BigInt(left), Value::SmallInt(right)) => Some(left.cmp(&i64::from(*right))),
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        (Value::Boolean(left), Value::Boolean(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

pub(super) fn combine_lower_logical(
    existing: Option<ScanBound>,
    candidate: ScanBound,
) -> Option<ScanBound> {
    match existing {
        None => Some(candidate),
        Some(current) => match compare_scan_values(&current.values, &candidate.values)? {
            std::cmp::Ordering::Less => Some(candidate),
            std::cmp::Ordering::Greater => Some(current),
            std::cmp::Ordering::Equal => Some(ScanBound {
                values: current.values,
                inclusive: current.inclusive && candidate.inclusive,
            }),
        },
    }
}

pub(super) fn combine_upper_logical(
    existing: Option<ScanBound>,
    candidate: ScanBound,
) -> Option<ScanBound> {
    match existing {
        None => Some(candidate),
        Some(current) => match compare_scan_values(&current.values, &candidate.values)? {
            std::cmp::Ordering::Less => Some(current),
            std::cmp::Ordering::Greater => Some(candidate),
            std::cmp::Ordering::Equal => Some(ScanBound {
                values: current.values,
                inclusive: current.inclusive && candidate.inclusive,
            }),
        },
    }
}

pub(super) fn extract_pk_scan_bounds(
    table: &TableDef,
    filter: &Expr,
) -> Option<LogicalPkScanBounds> {
    let pk_indices = table.pk_column_indices();
    if pk_indices.len() != 1 {
        return None;
    }
    let pk_column_idx = pk_indices[0];
    let pk_column = table.columns().get(pk_column_idx)?;

    let mut lower: Option<ScanBound> = None;
    let mut upper: Option<ScanBound> = None;

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

        let normalized_literal = cast_value(literal, pk_column.data_type()).ok()?;
        found_pk_predicate = true;
        match op {
            BinaryOp::Eq => {
                let bound = ScanBound {
                    values: vec![normalized_literal.clone()],
                    inclusive: true,
                };
                lower = combine_lower_logical(lower, bound.clone());
                upper = combine_upper_logical(upper, bound);
            }
            BinaryOp::Gt => {
                lower = combine_lower_logical(
                    lower,
                    ScanBound {
                        values: vec![normalized_literal.clone()],
                        inclusive: false,
                    },
                );
            }
            BinaryOp::Ge => {
                lower = combine_lower_logical(
                    lower,
                    ScanBound {
                        values: vec![normalized_literal.clone()],
                        inclusive: true,
                    },
                );
            }
            BinaryOp::Lt => {
                upper = combine_upper_logical(
                    upper,
                    ScanBound {
                        values: vec![normalized_literal.clone()],
                        inclusive: false,
                    },
                );
            }
            BinaryOp::Le => {
                upper = combine_upper_logical(
                    upper,
                    ScanBound {
                        values: vec![normalized_literal.clone()],
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

    if let (Some(lower), Some(upper)) = (&lower, &upper) {
        match compare_scan_values(&lower.values, &upper.values)? {
            std::cmp::Ordering::Greater => return Some(LogicalPkScanBounds::Empty),
            std::cmp::Ordering::Equal if !lower.inclusive || !upper.inclusive => {
                return Some(LogicalPkScanBounds::Empty);
            }
            _ => {}
        }
    }

    Some(LogicalPkScanBounds::Bounded(
        LogicalScanBounds::PrimaryKeyRange { lower, upper },
    ))
}

pub(super) fn normalize_pk_comparison(
    expr: &Expr,
    pk_column_idx: usize,
) -> Option<(BinaryOp, &Value)> {
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

pub(super) fn build_dml_read_plan(input: LogicalPlan, table: &TableDef) -> Result<DmlReadPlan> {
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

                let bounds = match filter
                    .as_ref()
                    .and_then(|predicate| extract_pk_scan_bounds(table, predicate))
                {
                    Some(LogicalPkScanBounds::Empty) => return Ok(DmlReadPlan::Empty),
                    Some(LogicalPkScanBounds::Bounded(bounds)) => bounds,
                    None => LogicalScanBounds::FullTable,
                };

                return Ok(DmlReadPlan::Scan { bounds, filter });
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ColumnDef;

    fn int_pk_table() -> TableDef {
        TableDef::new(
            42,
            "t".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::Int, false, None, false),
                ColumnDef::new(2, "v".into(), DataType::Int, true, None, false),
            ],
            vec![1],
        )
    }

    fn string_pk_table() -> TableDef {
        TableDef::new(
            43,
            "t_str".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::Varchar(16), false, None, false),
                ColumnDef::new(2, "v".into(), DataType::Int, true, None, false),
            ],
            vec![1],
        )
    }

    fn col_expr(idx: usize, name: &str, data_type: DataType) -> Expr {
        Expr::Column {
            table_idx: None,
            column_idx: idx,
            name: name.to_string(),
            data_type,
        }
    }

    #[test]
    fn test_eval_expr_covers_is_null_aggregate_and_cast() {
        let row = Row::new(vec![Value::Null, Value::String("7".into())]);

        let is_null = Expr::IsNull {
            expr: Box::new(col_expr(0, "id", DataType::Int)),
            negated: false,
        };
        assert_eq!(eval_expr(&is_null, &row).unwrap(), Value::Boolean(true));

        let aggregate = Expr::Aggregate {
            func: AggFunc::Count,
            arg: Box::new(col_expr(1, "v", DataType::Varchar(8))),
            distinct: false,
        };
        assert_eq!(
            eval_expr(&aggregate, &row).unwrap(),
            Value::String("7".into())
        );

        let cast = Expr::Cast {
            expr: Box::new(col_expr(1, "v", DataType::Varchar(8))),
            data_type: DataType::Int,
        };
        assert_eq!(eval_expr(&cast, &row).unwrap(), Value::Int(7));
    }

    #[test]
    fn test_eval_binary_op_handles_null_logic_concat_and_like() {
        assert_eq!(
            eval_binary_op(&Value::Null, BinaryOp::And, &Value::Boolean(false)).unwrap(),
            Value::Boolean(false)
        );
        assert_eq!(
            eval_binary_op(&Value::Null, BinaryOp::Or, &Value::Boolean(true)).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_binary_op(&Value::Null, BinaryOp::Eq, &Value::Int(1)).unwrap(),
            Value::Null
        );
        assert_eq!(
            eval_binary_op(
                &Value::String("ab".into()),
                BinaryOp::Concat,
                &Value::Int(12),
            )
            .unwrap(),
            Value::String("ab12".into())
        );
        assert_eq!(
            eval_binary_op(
                &Value::String("alice".into()),
                BinaryOp::Like,
                &Value::String("a%".into()),
            )
            .unwrap(),
            Value::Boolean(true)
        );
        assert!(!like_match("alice", "["));
    }

    #[test]
    fn test_eval_unary_and_value_conversion_helpers_cover_branches() {
        assert_eq!(
            eval_unary_op(UnaryOp::Not, &Value::Int(0)).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            eval_unary_op(UnaryOp::Neg, &Value::TinyInt(1)).unwrap(),
            Value::TinyInt(-1)
        );
        assert_eq!(
            eval_unary_op(UnaryOp::Neg, &Value::SmallInt(2)).unwrap(),
            Value::SmallInt(-2)
        );
        assert_eq!(
            eval_unary_op(UnaryOp::Neg, &Value::Int(3)).unwrap(),
            Value::Int(-3)
        );
        assert_eq!(
            eval_unary_op(UnaryOp::Neg, &Value::BigInt(4)).unwrap(),
            Value::BigInt(-4)
        );
        assert_eq!(
            eval_unary_op(UnaryOp::Neg, &Value::Float(1.5)).unwrap(),
            Value::Float(-1.5)
        );
        assert_eq!(
            eval_unary_op(UnaryOp::Neg, &Value::Double(2.5)).unwrap(),
            Value::Double(-2.5)
        );
        assert_eq!(
            eval_unary_op(UnaryOp::Plus, &Value::Double(2.5)).unwrap(),
            Value::Double(2.5)
        );
        assert!(eval_unary_op(UnaryOp::Neg, &Value::String("x".into())).is_err());

        assert_eq!(
            numeric_op(&Value::BigInt(5), &Value::Int(2), |a, b| a + b).unwrap(),
            Value::BigInt(7)
        );
        assert_eq!(
            numeric_op(&Value::Float(1.5), &Value::Int(2), |a, b| a + b).unwrap(),
            Value::Float(3.5)
        );
        assert_eq!(
            numeric_op(&Value::Double(1.5), &Value::Int(2), |a, b| a + b).unwrap(),
            Value::Double(3.5)
        );

        assert_eq!(value_to_f64(&Value::TinyInt(1)).unwrap(), 1.0);
        assert_eq!(value_to_f64(&Value::SmallInt(2)).unwrap(), 2.0);
        assert_eq!(value_to_f64(&Value::Float(3.5)).unwrap(), 3.5);
        assert_eq!(value_to_f64(&Value::Double(4.5)).unwrap(), 4.5);
        assert_eq!(value_to_f64(&Value::String("5.5".into())).unwrap(), 5.5);
        assert!(value_to_f64(&Value::Boolean(true)).is_err());

        assert!(value_to_bool(&Value::TinyInt(1)).unwrap());
        assert!(value_to_bool(&Value::SmallInt(1)).unwrap());
        assert!(value_to_bool(&Value::Int(1)).unwrap());
        assert!(value_to_bool(&Value::BigInt(1)).unwrap());
        assert!(!value_to_bool(&Value::Null).unwrap());
        assert!(value_to_bool(&Value::String("x".into())).is_err());

        assert_eq!(value_to_string(&Value::String("x".into())), "x");
        assert_eq!(value_to_string(&Value::TinyInt(1)), "1");
        assert_eq!(value_to_string(&Value::SmallInt(2)), "2");
        assert_eq!(value_to_string(&Value::BigInt(3)), "3");
        assert_eq!(value_to_string(&Value::Float(1.5)), "1.5");
        assert_eq!(value_to_string(&Value::Double(2.5)), "2.5");
        assert_eq!(value_to_string(&Value::Boolean(true)), "true");
        assert_eq!(value_to_string(&Value::Null), "NULL");

        assert_eq!(
            compare_values(&Value::Null, &Value::Null),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            compare_values(&Value::Null, &Value::Int(1)),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::Boolean(false), &Value::Boolean(true)),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn test_cast_value_covers_exact_and_conversion_paths() {
        let decimal_ty = DataType::Decimal {
            precision: 10,
            scale: 2,
        };

        assert_eq!(
            cast_value(&Value::Boolean(true), &DataType::Boolean).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            cast_value(&Value::TinyInt(1), &DataType::TinyInt).unwrap(),
            Value::TinyInt(1)
        );
        assert_eq!(
            cast_value(&Value::SmallInt(2), &DataType::SmallInt).unwrap(),
            Value::SmallInt(2)
        );
        assert_eq!(
            cast_value(&Value::Int(3), &DataType::Int).unwrap(),
            Value::Int(3)
        );
        assert_eq!(
            cast_value(&Value::Float(1.5), &DataType::Float).unwrap(),
            Value::Float(1.5)
        );
        assert_eq!(
            cast_value(&Value::Double(2.5), &DataType::Double).unwrap(),
            Value::Double(2.5)
        );
        assert_eq!(
            cast_value(&Value::Decimal("3.14".into()), &decimal_ty).unwrap(),
            Value::Decimal("3.14".into())
        );
        assert_eq!(
            cast_value(&Value::String("x".into()), &DataType::Varchar(8)).unwrap(),
            Value::String("x".into())
        );
        assert_eq!(
            cast_value(&Value::Bytes(vec![1, 2]), &DataType::Blob).unwrap(),
            Value::Bytes(vec![1, 2])
        );
        assert_eq!(
            cast_value(&Value::Date(1), &DataType::Date).unwrap(),
            Value::Date(1)
        );
        assert_eq!(
            cast_value(&Value::Time(2), &DataType::Time).unwrap(),
            Value::Time(2)
        );
        assert_eq!(
            cast_value(&Value::DateTime(3), &DataType::DateTime).unwrap(),
            Value::DateTime(3)
        );
        assert_eq!(
            cast_value(&Value::Timestamp(4), &DataType::Timestamp).unwrap(),
            Value::Timestamp(4)
        );

        assert_eq!(
            cast_value(&Value::Int(9), &DataType::Boolean).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            cast_value(&Value::String("9".into()), &DataType::BigInt).unwrap(),
            Value::BigInt(9)
        );
        assert_eq!(
            cast_value(&Value::String("9".into()), &DataType::Float).unwrap(),
            Value::Float(9.0)
        );
        assert_eq!(
            cast_value(&Value::String("9".into()), &DataType::Double).unwrap(),
            Value::Double(9.0)
        );
        assert_eq!(
            cast_value(&Value::Int(7), &decimal_ty).unwrap(),
            Value::Decimal("7".into())
        );
        assert_eq!(
            cast_value(&Value::Int(7), &DataType::Text).unwrap(),
            Value::String("7".into())
        );
        assert_eq!(
            cast_value(&Value::String("blob".into()), &DataType::Blob).unwrap(),
            Value::Bytes(b"blob".to_vec())
        );
        assert_eq!(
            cast_value(&Value::Int(7), &DataType::Date).unwrap(),
            Value::Date(7)
        );
        assert_eq!(
            cast_value(&Value::Int(7), &DataType::Time).unwrap(),
            Value::Time(7)
        );
        assert_eq!(
            cast_value(&Value::Int(7), &DataType::DateTime).unwrap(),
            Value::DateTime(7)
        );
        assert_eq!(
            cast_value(&Value::Int(7), &DataType::Timestamp).unwrap(),
            Value::Timestamp(7)
        );
        assert!(cast_value(&Value::Int(7), &DataType::Blob).is_err());

        let table = TableDef::new(
            44,
            "norm".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "a".into(), DataType::Int, false, None, false),
                ColumnDef::new(2, "b".into(), DataType::Varchar(8), false, None, false),
            ],
            vec![1],
        );
        let mut row_values = vec![Value::BigInt(5), Value::Int(6)];
        normalize_row_values_for_table(&table, &mut row_values).unwrap();
        assert_eq!(row_values, vec![Value::Int(5), Value::String("6".into())]);
    }

    #[test]
    fn test_compute_aggregate_and_auto_increment_helpers_cover_core_cases() {
        let rows = vec![
            Row::new(vec![Value::Int(1), Value::Int(10)]),
            Row::new(vec![Value::Int(2), Value::Null]),
            Row::new(vec![Value::Int(3), Value::Int(20)]),
        ];
        let arg = col_expr(1, "v", DataType::Int);

        assert_eq!(
            compute_aggregate(&AggFunc::Count, &arg, &rows).unwrap(),
            Value::BigInt(2)
        );
        assert_eq!(
            compute_aggregate(&AggFunc::Sum, &arg, &rows).unwrap(),
            Value::Double(30.0)
        );
        assert_eq!(
            compute_aggregate(&AggFunc::Avg, &arg, &rows).unwrap(),
            Value::Double(15.0)
        );
        assert_eq!(
            compute_aggregate(&AggFunc::Min, &arg, &rows).unwrap(),
            Value::Int(10)
        );
        assert_eq!(
            compute_aggregate(&AggFunc::Max, &arg, &rows).unwrap(),
            Value::Int(20)
        );
        assert_eq!(
            compute_aggregate(&AggFunc::Sum, &arg, &[]).unwrap(),
            Value::Null
        );

        assert!(generated_auto_increment_value(i32::MAX as u64 + 1, &DataType::Int).is_err());
        assert_eq!(
            generated_auto_increment_value(9, &DataType::BigInt).unwrap(),
            Value::BigInt(9)
        );
        assert!(generated_auto_increment_value(1, &DataType::Varchar(8)).is_err());

        assert!(matches!(
            classify_auto_increment_row_value(&Value::Null, &DataType::Int).unwrap(),
            AutoIncrementRowAction::Generate
        ));
        assert!(matches!(
            classify_auto_increment_row_value(&Value::BigInt(0), &DataType::Int).unwrap(),
            AutoIncrementRowAction::Generate
        ));
        assert!(matches!(
            classify_auto_increment_row_value(&Value::BigInt(-1), &DataType::BigInt).unwrap(),
            AutoIncrementRowAction::ExplicitNoObserve
        ));
        assert!(matches!(
            classify_auto_increment_row_value(&Value::Int(7), &DataType::Int).unwrap(),
            AutoIncrementRowAction::ExplicitObserve(7)
        ));
        assert!(
            classify_auto_increment_row_value(&Value::String("x".into()), &DataType::Int).is_err()
        );
        assert!(
            classify_auto_increment_row_value(&Value::BigInt(1), &DataType::Varchar(8)).is_err()
        );
    }

    #[test]
    fn test_predicate_and_write_request_helpers_cover_branching_paths() {
        let base = Expr::BinaryOp {
            left: Box::new(col_expr(0, "id", DataType::Int)),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Value::Int(1))),
        };
        let extra = Expr::BinaryOp {
            left: Box::new(col_expr(1, "v", DataType::Int)),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Value::Int(2))),
        };
        let combined = conjoin_predicate(Some(base.clone()), extra.clone()).unwrap();
        let mut conjuncts = Vec::new();
        collect_conjuncts(&combined, &mut conjuncts);
        assert_eq!(conjuncts.len(), 2);

        let row = Row::new(vec![Value::Int(2), Value::Int(2)]);
        assert!(row_matches_filter(Some(&combined), &row).unwrap());
        assert!(row_matches_filter(None, &row).unwrap());

        let delete = WriteRequest::Delete(DeleteRequest {
            table: int_pk_table(),
            keys: vec![RowKey::PrimaryKey(vec![Value::Int(1)])],
        });
        let lock = LockRequest {
            table: int_pk_table(),
            keys: vec![RowKey::PrimaryKey(vec![Value::Int(2)])],
        };
        assert!(compose_write_request(None, None).is_none());
        assert!(matches!(
            compose_write_request(Some(delete.clone()), None),
            Some(WriteRequest::Delete(_))
        ));
        assert!(matches!(
            compose_write_request(None, Some(lock.clone())),
            Some(WriteRequest::Lock(_))
        ));
        assert!(matches!(
            compose_write_request(Some(delete), Some(lock)),
            Some(WriteRequest::Batch(_))
        ));
    }

    #[test]
    fn test_point_key_and_scan_bound_helpers_cover_mixed_pk_cases() {
        assert!(!point_get_literal_matches_pk(&DataType::Int, &Value::Null));
        assert!(point_get_literal_matches_pk(
            &DataType::Boolean,
            &Value::Boolean(true)
        ));
        assert!(point_get_literal_matches_pk(
            &DataType::Float,
            &Value::Float(1.0)
        ));
        assert!(point_get_literal_matches_pk(
            &DataType::Double,
            &Value::Double(1.0)
        ));
        assert!(point_get_literal_matches_pk(
            &DataType::Decimal {
                precision: 10,
                scale: 2,
            },
            &Value::Decimal("1.0".into())
        ));
        assert!(point_get_literal_matches_pk(
            &DataType::Blob,
            &Value::Bytes(vec![1])
        ));
        assert!(point_get_literal_matches_pk(
            &DataType::Date,
            &Value::Date(1)
        ));
        assert!(point_get_literal_matches_pk(
            &DataType::Time,
            &Value::Time(1)
        ));
        assert!(point_get_literal_matches_pk(
            &DataType::DateTime,
            &Value::DateTime(1)
        ));
        assert!(point_get_literal_matches_pk(
            &DataType::Timestamp,
            &Value::Timestamp(1)
        ));

        assert_eq!(
            compare_scan_values(&[Value::Int(1)], &[Value::BigInt(1)]),
            Some(std::cmp::Ordering::Equal)
        );
        assert_eq!(
            compare_scan_values(&[Value::Int(1)], &[Value::BigInt(2)]),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(
            compare_scan_values(&[Value::String("a".into())], &[Value::String("b".into())]),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(
            compare_scan_values(&[Value::Boolean(false)], &[Value::Boolean(true)]),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(
            compare_scan_values(&[Value::Int(1)], &[Value::Int(1), Value::Int(2)]),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(
            compare_scan_value(&Value::Float(1.0), &Value::Double(1.0)),
            None
        );

        let lower_a = ScanBound {
            values: vec![Value::Int(1)],
            inclusive: true,
        };
        let lower_b = ScanBound {
            values: vec![Value::Int(2)],
            inclusive: false,
        };
        assert_eq!(
            combine_lower_logical(Some(lower_a.clone()), lower_b.clone()).unwrap(),
            lower_b
        );
        assert!(
            !combine_upper_logical(
                Some(ScanBound {
                    values: vec![Value::Int(5)],
                    inclusive: true,
                }),
                ScanBound {
                    values: vec![Value::Int(5)],
                    inclusive: false,
                },
            )
            .unwrap()
            .inclusive
        );

        let table = int_pk_table();
        let id_col = col_expr(0, "id", DataType::Int);
        let filter = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Literal(Value::BigInt(2))),
                op: BinaryOp::Le,
                right: Box::new(id_col.clone()),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(id_col.clone()),
                op: BinaryOp::Lt,
                right: Box::new(Expr::Literal(Value::BigInt(5))),
            }),
        };
        assert_eq!(
            extract_pk_scan_bounds(&table, &filter),
            Some(LogicalPkScanBounds::Bounded(
                LogicalScanBounds::PrimaryKeyRange {
                    lower: Some(ScanBound {
                        values: vec![Value::Int(2)],
                        inclusive: true,
                    }),
                    upper: Some(ScanBound {
                        values: vec![Value::Int(5)],
                        inclusive: false,
                    }),
                }
            ))
        );

        let conflicting = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(id_col.clone()),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(Value::BigInt(5))),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(id_col),
                op: BinaryOp::Lt,
                right: Box::new(Expr::Literal(Value::BigInt(5))),
            }),
        };
        assert_eq!(
            extract_pk_scan_bounds(&table, &conflicting),
            Some(LogicalPkScanBounds::Empty)
        );
        assert_eq!(
            normalize_pk_comparison(
                &Expr::BinaryOp {
                    left: Box::new(Expr::Literal(Value::BigInt(3))),
                    op: BinaryOp::Lt,
                    right: Box::new(col_expr(0, "id", DataType::Int)),
                },
                0,
            ),
            Some((BinaryOp::Gt, &Value::BigInt(3)))
        );
    }

    #[test]
    fn test_build_dml_read_plan_covers_variants_and_validation_errors() {
        let table = int_pk_table();
        let other = string_pk_table();

        let scan_filter = Expr::BinaryOp {
            left: Box::new(col_expr(0, "id", DataType::Int)),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Value::BigInt(1))),
        };
        let scan_plan = LogicalPlan::Scan {
            table: table.clone(),
            projection: None,
            filter: Some(scan_filter),
        };
        assert!(matches!(
            build_dml_read_plan(scan_plan, &table).unwrap(),
            DmlReadPlan::Scan { .. }
        ));

        let point_get = LogicalPlan::PointGet {
            table: table.clone(),
            key: LogicalPointKey::PrimaryKey(vec![Value::BigInt(1)]),
        };
        assert!(matches!(
            build_dml_read_plan(point_get, &table).unwrap(),
            DmlReadPlan::PointGet { .. }
        ));

        assert!(matches!(
            build_dml_read_plan(
                LogicalPlan::Empty {
                    schema: Schema::new(vec![])
                },
                &table
            )
            .unwrap(),
            DmlReadPlan::Empty
        ));

        let wrong_scan = LogicalPlan::Scan {
            table: other.clone(),
            projection: None,
            filter: None,
        };
        assert!(build_dml_read_plan(wrong_scan, &table).is_err());

        let wrong_point_get = LogicalPlan::PointGet {
            table: other,
            key: LogicalPointKey::PrimaryKey(vec![Value::String("a".into())]),
        };
        assert!(build_dml_read_plan(wrong_point_get, &table).is_err());

        let unsupported = LogicalPlan::Values {
            rows: vec![],
            schema: Schema::new(vec![]),
        };
        assert!(build_dml_read_plan(unsupported, &table).is_err());
    }
}
