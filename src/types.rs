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

//! Core types used throughout TiSQL.
//!
//! This module defines fundamental types that are shared across all layers:
//! - ID types (TableId, ColumnId, etc.)
//! - Value and DataType for SQL values
//! - Row and Schema for query results

use serde::{Deserialize, Serialize};

// ============================================================================
// ID Types
// ============================================================================

/// Unique identifier for a table.
pub type TableId = u64;

/// Unique identifier for a column within a table.
pub type ColumnId = u32;

/// Unique identifier for an index.
pub type IndexId = u64;

/// Unique identifier for a transaction.
pub type TxnId = u64;

/// Logical timestamp for MVCC.
pub type Timestamp = u64;

/// Log sequence number for WAL.
pub type Lsn = u64;

// ============================================================================
// Storage Types
// ============================================================================

/// Raw key bytes for storage layer.
pub type Key = Vec<u8>;

/// Raw value bytes for storage layer.
pub type RawValue = Vec<u8>;

// ============================================================================
// SQL Data Types
// ============================================================================

/// SQL data types supported by TiSQL.
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

// ============================================================================
// SQL Values
// ============================================================================

/// Runtime SQL value.
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
    Date(i32),      // Days since epoch
    Time(i64),      // Microseconds since midnight
    DateTime(i64),  // Microseconds since epoch
    Timestamp(i64), // Microseconds since epoch
}

impl Value {
    /// Get the data type of this value.
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
            Value::Decimal(_) => DataType::Decimal {
                precision: 38,
                scale: 10,
            },
            Value::String(_) => DataType::Text,
            Value::Bytes(_) => DataType::Blob,
            Value::Date(_) => DataType::Date,
            Value::Time(_) => DataType::Time,
            Value::DateTime(_) => DataType::DateTime,
            Value::Timestamp(_) => DataType::Timestamp,
        }
    }

    /// Check if this value is NULL.
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

// ============================================================================
// Row - A single row of values
// ============================================================================

/// A row of values (result of a query or input for insert).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Row {
    values: Vec<Value>,
}

impl Row {
    /// Create a new row from values.
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    /// Get a reference to a value by index.
    #[inline]
    pub fn get(&self, idx: usize) -> Option<&Value> {
        self.values.get(idx)
    }

    /// Get the number of values in this row.
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if this row is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Iterate over values.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &Value> {
        self.values.iter()
    }

    /// Get a slice of all values.
    #[inline]
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Consume self and return the underlying values.
    #[inline]
    pub fn into_values(self) -> Vec<Value> {
        self.values
    }

    /// Get a mutable reference to a value by index.
    #[inline]
    pub fn get_mut(&mut self, idx: usize) -> Option<&mut Value> {
        self.values.get_mut(idx)
    }

    /// Set a value at the given index.
    #[inline]
    pub fn set(&mut self, idx: usize, value: Value) {
        if idx < self.values.len() {
            self.values[idx] = value;
        }
    }
}

// ============================================================================
// Schema - Column metadata
// ============================================================================

/// Column metadata for query results.
#[derive(Clone, Debug)]
pub struct ColumnInfo {
    name: String,
    data_type: DataType,
    nullable: bool,
}

impl ColumnInfo {
    /// Create a new column info.
    pub fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
        }
    }

    /// Get the column name.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the data type.
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Check if the column is nullable.
    #[inline]
    pub fn nullable(&self) -> bool {
        self.nullable
    }
}

/// Schema describes the structure of a result set.
#[derive(Clone, Debug)]
pub struct Schema {
    columns: Vec<ColumnInfo>,
}

impl Schema {
    /// Create a new schema from column info.
    pub fn new(columns: Vec<ColumnInfo>) -> Self {
        Self { columns }
    }

    /// Get the number of columns.
    #[inline]
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Find a column index by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Get a column by index.
    #[inline]
    pub fn column(&self, idx: usize) -> Option<&ColumnInfo> {
        self.columns.get(idx)
    }

    /// Iterate over columns.
    #[inline]
    pub fn columns(&self) -> &[ColumnInfo] {
        &self.columns
    }
}
