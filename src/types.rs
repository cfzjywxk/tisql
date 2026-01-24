use serde::{Deserialize, Serialize};

pub type TableId = u64;
pub type ColumnId = u32;
pub type IndexId = u64;
pub type TxnId = u64;
pub type Timestamp = u64;
pub type Lsn = u64;

pub type Key = Vec<u8>;
pub type RawValue = Vec<u8>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Decimal { precision: u8, scale: u8 },
    Char(u16),
    Varchar(u16),
    Text,
    Blob,
    Date,
    Time,
    DateTime,
    Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    Decimal(String), // Use string for exact decimal representation
    String(String),
    Bytes(Vec<u8>),
    Date(i32),       // Days since epoch
    Time(i64),       // Microseconds since midnight
    DateTime(i64),   // Microseconds since epoch
    Timestamp(i64),  // Microseconds since epoch
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Int, // Default, should be handled specially
            Value::Boolean(_) => DataType::Boolean,
            Value::TinyInt(_) => DataType::TinyInt,
            Value::SmallInt(_) => DataType::SmallInt,
            Value::Int(_) => DataType::Int,
            Value::BigInt(_) => DataType::BigInt,
            Value::Float(_) => DataType::Float,
            Value::Double(_) => DataType::Double,
            Value::Decimal(_) => DataType::Decimal { precision: 38, scale: 10 },
            Value::String(_) => DataType::Text,
            Value::Bytes(_) => DataType::Blob,
            Value::Date(_) => DataType::Date,
            Value::Time(_) => DataType::Time,
            Value::DateTime(_) => DataType::DateTime,
            Value::Timestamp(_) => DataType::Timestamp,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn get(&self, idx: usize) -> Option<&Value> {
        self.values.get(idx)
    }
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub columns: Vec<ColumnInfo>,
}

#[derive(Clone, Debug)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Schema {
    pub fn new(columns: Vec<ColumnInfo>) -> Self {
        Self { columns }
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}
