use crate::catalog::TableDef;
use crate::types::{ColumnId, DataType, Schema, Value};

/// Logical plan tree
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Table scan
    Scan {
        table: TableDef,
        projection: Option<Vec<usize>>, // Column indices to project
        filter: Option<Expr>,
    },

    /// Projection (SELECT expressions)
    Project {
        input: Box<LogicalPlan>,
        exprs: Vec<(Expr, String)>, // (expr, alias)
    },

    /// Filter (WHERE clause)
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },

    /// Join
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        on: Expr,
        join_type: JoinType,
    },

    /// Aggregation (GROUP BY)
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        agg_exprs: Vec<(AggFunc, Expr, String)>, // (func, arg, alias)
    },

    /// Sort (ORDER BY)
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<OrderByExpr>,
    },

    /// Limit (LIMIT/OFFSET)
    Limit {
        input: Box<LogicalPlan>,
        limit: Option<usize>,
        offset: usize,
    },

    /// Insert
    Insert {
        table: TableDef,
        columns: Vec<ColumnId>,
        values: Vec<Vec<Expr>>, // Each inner vec is a row
    },

    /// Update
    Update {
        table: TableDef,
        assignments: Vec<(ColumnId, Expr)>,
        filter: Option<Expr>,
    },

    /// Delete
    Delete {
        table: TableDef,
        filter: Option<Expr>,
    },

    /// Create table
    CreateTable {
        table: TableDef,
        if_not_exists: bool,
    },

    /// Drop table
    DropTable {
        schema: String,
        table: String,
        if_exists: bool,
    },

    /// Values (for SELECT without FROM)
    Values {
        rows: Vec<Vec<Expr>>,
        schema: Schema,
    },

    /// Empty result (for internal use)
    Empty {
        schema: Schema,
    },
}

/// Expression tree
#[derive(Debug, Clone)]
pub enum Expr {
    /// Column reference (table_idx in join context, column_idx)
    Column {
        table_idx: Option<usize>,
        column_idx: usize,
        name: String,
        data_type: DataType,
    },

    /// Literal value
    Literal(Value),

    /// Binary operation
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },

    /// Unary operation
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },

    /// Function call
    Function {
        name: String,
        args: Vec<Expr>,
    },

    /// Aggregate function
    Aggregate {
        func: AggFunc,
        arg: Box<Expr>,
        distinct: bool,
    },

    /// CASE WHEN
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },

    /// IS NULL / IS NOT NULL
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },

    /// IN list
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },

    /// BETWEEN
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },

    /// LIKE
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
    },

    /// Subquery (for later)
    Subquery(Box<LogicalPlan>),

    /// EXISTS subquery
    Exists {
        subquery: Box<LogicalPlan>,
        negated: bool,
    },

    /// Wildcard (*) - only valid in certain contexts
    Wildcard,

    /// Qualified wildcard (table.*) - only valid in certain contexts
    QualifiedWildcard {
        table: String,
    },

    /// Cast expression
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
}

impl Expr {
    /// Get the data type of this expression
    pub fn data_type(&self) -> DataType {
        match self {
            Expr::Column { data_type, .. } => data_type.clone(),
            Expr::Literal(v) => v.data_type(),
            Expr::BinaryOp { left, op, .. } => {
                match op {
                    BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le
                    | BinaryOp::Gt | BinaryOp::Ge | BinaryOp::And | BinaryOp::Or
                    | BinaryOp::Like => DataType::Boolean,
                    _ => left.data_type(),
                }
            }
            Expr::UnaryOp { op, expr } => {
                match op {
                    UnaryOp::Not => DataType::Boolean,
                    UnaryOp::Neg | UnaryOp::Plus => expr.data_type(),
                }
            }
            Expr::Aggregate { func, .. } => {
                match func {
                    AggFunc::Count => DataType::BigInt,
                    AggFunc::Sum | AggFunc::Avg => DataType::Double,
                    AggFunc::Min | AggFunc::Max => DataType::Double, // Simplified
                }
            }
            Expr::IsNull { .. } => DataType::Boolean,
            Expr::InList { .. } => DataType::Boolean,
            Expr::Between { .. } => DataType::Boolean,
            Expr::Like { .. } => DataType::Boolean,
            Expr::Exists { .. } => DataType::Boolean,
            Expr::Cast { data_type, .. } => data_type.clone(),
            _ => DataType::Int, // Default fallback
        }
    }

    /// Check if this is a constant expression
    pub fn is_const(&self) -> bool {
        match self {
            Expr::Literal(_) => true,
            Expr::BinaryOp { left, right, .. } => left.is_const() && right.is_const(),
            Expr::UnaryOp { expr, .. } => expr.is_const(),
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryOp {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,

    // Comparison
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,

    // Logical
    And,
    Or,

    // String
    Like,
    Concat,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UnaryOp {
    Not,
    Neg,
    Plus,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: bool,
    pub nulls_first: bool,
}
