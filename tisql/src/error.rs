use thiserror::Error;

pub type Result<T> = std::result::Result<T, TiSqlError>;

#[derive(Error, Debug)]
pub enum TiSqlError {
    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Catalog error: {0}")]
    Catalog(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("SQL parse error: {0}")]
    Parse(String),

    #[error("SQL bind error: {0}")]
    Bind(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },

    #[error("Duplicate key: {0}")]
    DuplicateKey(String),

    #[error("Transaction conflict")]
    TransactionConflict,

    #[error("Transaction aborted")]
    TransactionAborted,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    Internal(String),
}
