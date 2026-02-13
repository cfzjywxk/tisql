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

//! Core system table definitions for the inner-table-based catalog.
//!
//! Defines hardcoded schemas for the 5 core system tables following
//! the OceanBase bootstrap pattern. These tables are the single source
//! of truth for all schema metadata.

use crate::catalog::{ColumnDef, DefaultValue, TableDef};
use crate::error::{Result, TiSqlError};
use crate::types::DataType;

// ============================================================================
// Constants
// ============================================================================

/// System schema name for inner tables.
pub const INNER_SCHEMA: &str = "__tisql_inner";

/// System schema ID.
pub const INNER_SCHEMA_ID: u64 = 1;

/// Default schema ID.
pub const DEFAULT_SCHEMA_ID: u64 = 2;

/// Test schema ID.
pub const TEST_SCHEMA_ID: u64 = 3;

/// User table IDs start from this value (1–999 reserved for system tables).
pub const USER_TABLE_ID_START: u64 = 1000;

/// User schema IDs start from this value.
pub const USER_SCHEMA_ID_START: u64 = 100;

// Fixed table IDs for core system tables
pub const ALL_META_TABLE_ID: u64 = 1;
pub const ALL_SCHEMA_TABLE_ID: u64 = 2;
pub const ALL_TABLE_TABLE_ID: u64 = 3;
pub const ALL_COLUMN_TABLE_ID: u64 = 4;
pub const ALL_INDEX_TABLE_ID: u64 = 5;
pub const ALL_GC_DELETE_RANGE_TABLE_ID: u64 = 6;

// Well-known meta IDs in __all_meta
pub const META_BOOTSTRAP_VERSION: i64 = 1;
pub const META_NEXT_TABLE_ID: i64 = 2;
pub const META_NEXT_INDEX_ID: i64 = 3;
pub const META_SCHEMA_VERSION: i64 = 4;
pub const META_NEXT_SCHEMA_ID: i64 = 5;
pub const META_NEXT_GC_TASK_ID: i64 = 6;

/// Meta key names (indexed by meta_id).
pub const META_KEYS: &[&str] = &[
    "",                  // 0 — unused
    "bootstrap_version", // 1
    "next_table_id",     // 2
    "next_index_id",     // 3
    "schema_version",    // 4
    "next_schema_id",    // 5
    "next_gc_task_id",   // 6
];

// ============================================================================
// Core Table Definitions
// ============================================================================

/// Build the hardcoded TableDef for `__all_meta`.
pub fn all_meta_def() -> TableDef {
    TableDef::new(
        ALL_META_TABLE_ID,
        "__all_meta".to_string(),
        INNER_SCHEMA.to_string(),
        vec![
            ColumnDef::new(0, "meta_id".into(), DataType::BigInt, false, None, false),
            ColumnDef::new(
                1,
                "meta_key".into(),
                DataType::Varchar(128),
                false,
                None,
                false,
            ),
            ColumnDef::new(
                2,
                "meta_value".into(),
                DataType::Varchar(1024),
                false,
                None,
                false,
            ),
            ColumnDef::new(3, "updated_ts".into(), DataType::BigInt, false, None, false),
        ],
        vec![0], // PK = meta_id
    )
}

/// Build the hardcoded TableDef for `__all_schema`.
pub fn all_schema_def() -> TableDef {
    TableDef::new(
        ALL_SCHEMA_TABLE_ID,
        "__all_schema".to_string(),
        INNER_SCHEMA.to_string(),
        vec![
            ColumnDef::new(0, "schema_id".into(), DataType::BigInt, false, None, false),
            ColumnDef::new(
                1,
                "schema_name".into(),
                DataType::Varchar(128),
                false,
                None,
                false,
            ),
        ],
        vec![0], // PK = schema_id
    )
}

/// Build the hardcoded TableDef for `__all_table`.
pub fn all_table_def() -> TableDef {
    TableDef::new(
        ALL_TABLE_TABLE_ID,
        "__all_table".to_string(),
        INNER_SCHEMA.to_string(),
        vec![
            ColumnDef::new(0, "table_id".into(), DataType::BigInt, false, None, false),
            ColumnDef::new(1, "schema_id".into(), DataType::BigInt, false, None, false),
            ColumnDef::new(
                2,
                "table_name".into(),
                DataType::Varchar(128),
                false,
                None,
                false,
            ),
            ColumnDef::new(
                3,
                "primary_key_columns".into(),
                DataType::Varchar(256),
                false,
                None,
                false,
            ),
            ColumnDef::new(
                4,
                "auto_increment_id".into(),
                DataType::BigInt,
                false,
                None,
                false,
            ),
        ],
        vec![0], // PK = table_id
    )
}

/// Build the hardcoded TableDef for `__all_column`.
pub fn all_column_def() -> TableDef {
    TableDef::new(
        ALL_COLUMN_TABLE_ID,
        "__all_column".to_string(),
        INNER_SCHEMA.to_string(),
        vec![
            ColumnDef::new(0, "column_key".into(), DataType::BigInt, false, None, false),
            ColumnDef::new(1, "table_id".into(), DataType::BigInt, false, None, false),
            ColumnDef::new(2, "column_id".into(), DataType::BigInt, false, None, false),
            ColumnDef::new(
                3,
                "column_name".into(),
                DataType::Varchar(128),
                false,
                None,
                false,
            ),
            ColumnDef::new(
                4,
                "data_type".into(),
                DataType::Varchar(64),
                false,
                None,
                false,
            ),
            ColumnDef::new(
                5,
                "ordinal_position".into(),
                DataType::Int,
                false,
                None,
                false,
            ),
            ColumnDef::new(6, "is_nullable".into(), DataType::Int, false, None, false),
            ColumnDef::new(
                7,
                "default_value".into(),
                DataType::Varchar(256),
                true,
                None,
                false,
            ),
            ColumnDef::new(
                8,
                "is_auto_increment".into(),
                DataType::Int,
                false,
                None,
                false,
            ),
        ],
        vec![0], // PK = column_key
    )
}

/// Build the hardcoded TableDef for `__all_index`.
pub fn all_index_def() -> TableDef {
    TableDef::new(
        ALL_INDEX_TABLE_ID,
        "__all_index".to_string(),
        INNER_SCHEMA.to_string(),
        vec![
            ColumnDef::new(0, "index_key".into(), DataType::BigInt, false, None, false),
            ColumnDef::new(1, "table_id".into(), DataType::BigInt, false, None, false),
            ColumnDef::new(2, "index_id".into(), DataType::BigInt, false, None, false),
            ColumnDef::new(
                3,
                "index_name".into(),
                DataType::Varchar(128),
                false,
                None,
                false,
            ),
            ColumnDef::new(
                4,
                "column_ids".into(),
                DataType::Varchar(256),
                false,
                None,
                false,
            ),
            ColumnDef::new(5, "is_unique".into(), DataType::Int, false, None, false),
        ],
        vec![0], // PK = index_key
    )
}

/// Build the hardcoded TableDef for `__all_gc_delete_range`.
pub fn all_gc_delete_range_def() -> TableDef {
    TableDef::new(
        ALL_GC_DELETE_RANGE_TABLE_ID,
        "__all_gc_delete_range".to_string(),
        INNER_SCHEMA.to_string(),
        vec![
            ColumnDef::new(0, "task_id".into(), DataType::BigInt, false, None, false),
            ColumnDef::new(1, "table_id".into(), DataType::BigInt, false, None, false),
            ColumnDef::new(
                2,
                "start_key".into(),
                DataType::Varchar(512),
                false,
                None,
                false,
            ),
            ColumnDef::new(
                3,
                "end_key".into(),
                DataType::Varchar(512),
                false,
                None,
                false,
            ),
            ColumnDef::new(
                4,
                "drop_commit_ts".into(),
                DataType::BigInt,
                false,
                None,
                false,
            ),
            ColumnDef::new(
                5,
                "status".into(),
                DataType::Varchar(16),
                false,
                None,
                false,
            ),
        ],
        vec![0], // PK = task_id
    )
}

/// Returns all 6 core table definitions.
pub fn core_table_defs() -> Vec<TableDef> {
    vec![
        all_meta_def(),
        all_schema_def(),
        all_table_def(),
        all_column_def(),
        all_index_def(),
        all_gc_delete_range_def(),
    ]
}

// ============================================================================
// DataType Format/Parse Helpers
// ============================================================================

/// Format a `DataType` as a SQL-style string (e.g., `"VARCHAR(128)"`).
pub fn format_data_type(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::TinyInt => "TINYINT".to_string(),
        DataType::SmallInt => "SMALLINT".to_string(),
        DataType::Int => "INT".to_string(),
        DataType::BigInt => "BIGINT".to_string(),
        DataType::Float => "FLOAT".to_string(),
        DataType::Double => "DOUBLE".to_string(),
        DataType::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})"),
        DataType::Char(len) => format!("CHAR({len})"),
        DataType::Varchar(len) => format!("VARCHAR({len})"),
        DataType::Text => "TEXT".to_string(),
        DataType::Blob => "BLOB".to_string(),
        DataType::Date => "DATE".to_string(),
        DataType::Time => "TIME".to_string(),
        DataType::DateTime => "DATETIME".to_string(),
        DataType::Timestamp => "TIMESTAMP".to_string(),
    }
}

/// Parse a SQL-style type string back into a `DataType`.
pub fn parse_data_type(s: &str) -> Result<DataType> {
    let upper = s.trim().to_uppercase();
    if upper == "BOOLEAN" || upper == "BOOL" {
        return Ok(DataType::Boolean);
    }
    if upper == "TINYINT" {
        return Ok(DataType::TinyInt);
    }
    if upper == "SMALLINT" {
        return Ok(DataType::SmallInt);
    }
    if upper == "INT" || upper == "INTEGER" {
        return Ok(DataType::Int);
    }
    if upper == "BIGINT" {
        return Ok(DataType::BigInt);
    }
    if upper == "FLOAT" {
        return Ok(DataType::Float);
    }
    if upper == "DOUBLE" {
        return Ok(DataType::Double);
    }
    if upper == "TEXT" {
        return Ok(DataType::Text);
    }
    if upper == "BLOB" {
        return Ok(DataType::Blob);
    }
    if upper == "DATE" {
        return Ok(DataType::Date);
    }
    if upper == "TIME" {
        return Ok(DataType::Time);
    }
    if upper == "DATETIME" {
        return Ok(DataType::DateTime);
    }
    if upper == "TIMESTAMP" {
        return Ok(DataType::Timestamp);
    }

    // Parameterized types: DECIMAL(p,s), CHAR(n), VARCHAR(n)
    if let Some(inner) = upper
        .strip_prefix("DECIMAL(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let parts: Vec<&str> = inner.split(',').collect();
        if parts.len() == 2 {
            let precision: u8 = parts[0]
                .trim()
                .parse()
                .map_err(|_| TiSqlError::Catalog(format!("Invalid DECIMAL precision in '{s}'")))?;
            let scale: u8 = parts[1]
                .trim()
                .parse()
                .map_err(|_| TiSqlError::Catalog(format!("Invalid DECIMAL scale in '{s}'")))?;
            return Ok(DataType::Decimal { precision, scale });
        }
    }
    if let Some(inner) = upper
        .strip_prefix("CHAR(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let len: u16 = inner
            .trim()
            .parse()
            .map_err(|_| TiSqlError::Catalog(format!("Invalid CHAR length in '{s}'")))?;
        return Ok(DataType::Char(len));
    }
    if let Some(inner) = upper
        .strip_prefix("VARCHAR(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let len: u16 = inner
            .trim()
            .parse()
            .map_err(|_| TiSqlError::Catalog(format!("Invalid VARCHAR length in '{s}'")))?;
        return Ok(DataType::Varchar(len));
    }

    Err(TiSqlError::Catalog(format!("Unknown data type: '{s}'")))
}

// ============================================================================
// DefaultValue Format/Parse Helpers
// ============================================================================

/// Format a `DefaultValue` as a string for storage in `__all_column`.
pub fn format_default(d: &DefaultValue) -> String {
    match d {
        DefaultValue::Null => "NULL".to_string(),
        DefaultValue::Int(v) => v.to_string(),
        DefaultValue::Float(v) => v.to_string(),
        DefaultValue::String(s) => format!("'{s}'"),
        DefaultValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        DefaultValue::CurrentTimestamp => "CURRENT_TIMESTAMP".to_string(),
    }
}

/// Parse a default value string back into a `DefaultValue`.
pub fn parse_default(s: &str) -> Option<DefaultValue> {
    let trimmed = s.trim();
    if trimmed.eq_ignore_ascii_case("NULL") {
        return Some(DefaultValue::Null);
    }
    if trimmed.eq_ignore_ascii_case("TRUE") {
        return Some(DefaultValue::Bool(true));
    }
    if trimmed.eq_ignore_ascii_case("FALSE") {
        return Some(DefaultValue::Bool(false));
    }
    if trimmed.eq_ignore_ascii_case("CURRENT_TIMESTAMP") {
        return Some(DefaultValue::CurrentTimestamp);
    }
    // Quoted string
    if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
        return Some(DefaultValue::String(
            trimmed[1..trimmed.len() - 1].to_string(),
        ));
    }
    // Integer
    if let Ok(v) = trimmed.parse::<i64>() {
        return Some(DefaultValue::Int(v));
    }
    // Float
    if let Ok(v) = trimmed.parse::<f64>() {
        return Some(DefaultValue::Float(v));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_core_table_defs_count() {
        let defs = core_table_defs();
        assert_eq!(defs.len(), 6);
        assert_eq!(defs[0].id(), ALL_META_TABLE_ID);
        assert_eq!(defs[1].id(), ALL_SCHEMA_TABLE_ID);
        assert_eq!(defs[2].id(), ALL_TABLE_TABLE_ID);
        assert_eq!(defs[3].id(), ALL_COLUMN_TABLE_ID);
        assert_eq!(defs[4].id(), ALL_INDEX_TABLE_ID);
        assert_eq!(defs[5].id(), ALL_GC_DELETE_RANGE_TABLE_ID);
    }

    #[test]
    fn test_core_tables_have_correct_names() {
        let defs = core_table_defs();
        assert_eq!(defs[0].name(), "__all_meta");
        assert_eq!(defs[1].name(), "__all_schema");
        assert_eq!(defs[2].name(), "__all_table");
        assert_eq!(defs[3].name(), "__all_column");
        assert_eq!(defs[4].name(), "__all_index");
        assert_eq!(defs[5].name(), "__all_gc_delete_range");
    }

    #[test]
    fn test_core_tables_in_inner_schema() {
        for def in core_table_defs() {
            assert_eq!(def.schema(), INNER_SCHEMA);
        }
    }

    #[test]
    fn test_format_parse_data_type_roundtrip() {
        let types = vec![
            DataType::Boolean,
            DataType::TinyInt,
            DataType::SmallInt,
            DataType::Int,
            DataType::BigInt,
            DataType::Float,
            DataType::Double,
            DataType::Decimal {
                precision: 10,
                scale: 2,
            },
            DataType::Char(50),
            DataType::Varchar(128),
            DataType::Text,
            DataType::Blob,
            DataType::Date,
            DataType::Time,
            DataType::DateTime,
            DataType::Timestamp,
        ];

        for dt in &types {
            let s = format_data_type(dt);
            let parsed = parse_data_type(&s).unwrap();
            assert_eq!(&parsed, dt, "Roundtrip failed for {s}");
        }
    }

    #[test]
    fn test_format_parse_default_roundtrip() {
        let defaults = vec![
            DefaultValue::Null,
            DefaultValue::Int(42),
            DefaultValue::Bool(true),
            DefaultValue::Bool(false),
            DefaultValue::String("hello".to_string()),
            DefaultValue::CurrentTimestamp,
        ];

        for d in &defaults {
            let s = format_default(d);
            let parsed = parse_default(&s);
            assert!(parsed.is_some(), "Failed to parse default: {s}");
        }
    }

    #[test]
    fn test_all_column_has_9_columns() {
        let def = all_column_def();
        assert_eq!(def.columns().len(), 9);
    }

    #[test]
    fn test_all_index_has_6_columns() {
        let def = all_index_def();
        assert_eq!(def.columns().len(), 6);
    }

    #[test]
    fn test_all_gc_delete_range_has_6_columns() {
        let def = all_gc_delete_range_def();
        assert_eq!(def.columns().len(), 6);
        assert_eq!(def.columns()[0].name(), "task_id");
        assert_eq!(def.columns()[5].name(), "status");
    }
}
