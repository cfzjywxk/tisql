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

use std::fmt::Write;

use crate::catalog::types::DataType;
use crate::catalog::{DefaultValue, TableDef};
use crate::util::error::{Result, TiSqlError};

/// Render a canonical `CREATE TABLE` statement from catalog metadata.
pub(crate) fn render_create_table(table: &TableDef) -> Result<String> {
    let mut out = String::with_capacity(estimate_capacity(table));
    out.push_str("CREATE TABLE ");
    append_quoted_ident(&mut out, table.schema());
    out.push('.');
    append_quoted_ident(&mut out, table.name());
    out.push_str(" (");

    let mut wrote_item = false;
    for column in table.columns() {
        append_item_separator(&mut out, &mut wrote_item);
        out.push_str("  ");
        append_quoted_ident(&mut out, column.name());
        out.push(' ');
        append_data_type(&mut out, column.data_type())?;
        if !column.nullable() {
            out.push_str(" NOT NULL");
        }
        if let Some(default) = column.default() {
            out.push_str(" DEFAULT ");
            append_default_value(&mut out, default)?;
        }
        if column.auto_increment() {
            out.push_str(" AUTO_INCREMENT");
        }
    }

    if !table.primary_key().is_empty() {
        append_item_separator(&mut out, &mut wrote_item);
        out.push_str("  PRIMARY KEY (");
        append_column_list_by_id(&mut out, table, table.primary_key())?;
        out.push(')');
    }

    for index in table.indexes() {
        append_item_separator(&mut out, &mut wrote_item);
        out.push_str("  ");
        if index.unique() {
            out.push_str("UNIQUE KEY ");
        } else {
            out.push_str("KEY ");
        }
        append_quoted_ident(&mut out, index.name());
        out.push_str(" (");
        append_column_list_by_id(&mut out, table, index.columns())?;
        out.push(')');
    }

    out.push_str("\n)");
    if table.auto_increment_id() > 0 {
        write!(&mut out, " AUTO_INCREMENT={}", table.auto_increment_id())
            .map_err(|_| format_error())?;
    }
    Ok(out)
}

fn estimate_capacity(table: &TableDef) -> usize {
    let mut cap = 64usize;
    cap = cap.saturating_add(table.schema().len());
    cap = cap.saturating_add(table.name().len());
    cap = cap.saturating_add(table.columns().len().saturating_mul(48));
    cap = cap.saturating_add(table.indexes().len().saturating_mul(40));
    cap = cap.saturating_add(table.primary_key().len().saturating_mul(8));
    cap
}

fn append_item_separator(out: &mut String, wrote_item: &mut bool) {
    if *wrote_item {
        out.push_str(",\n");
    } else {
        out.push('\n');
        *wrote_item = true;
    }
}

fn append_column_list_by_id(out: &mut String, table: &TableDef, column_ids: &[u32]) -> Result<()> {
    for (idx, col_id) in column_ids.iter().enumerate() {
        if idx > 0 {
            out.push_str(", ");
        }
        let col = table.column_by_id(*col_id).ok_or_else(|| {
            TiSqlError::Internal(format!(
                "Column ID {col_id} not found in table {}.{}",
                table.schema(),
                table.name()
            ))
        })?;
        append_quoted_ident(out, col.name());
    }
    Ok(())
}

fn append_quoted_ident(out: &mut String, ident: &str) {
    out.push('`');
    for ch in ident.chars() {
        if ch == '`' {
            out.push_str("``");
        } else {
            out.push(ch);
        }
    }
    out.push('`');
}

fn append_data_type(out: &mut String, data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Boolean => out.push_str("BOOLEAN"),
        DataType::TinyInt => out.push_str("TINYINT"),
        DataType::SmallInt => out.push_str("SMALLINT"),
        DataType::Int => out.push_str("INT"),
        DataType::BigInt => out.push_str("BIGINT"),
        DataType::Float => out.push_str("FLOAT"),
        DataType::Double => out.push_str("DOUBLE"),
        DataType::Decimal { precision, scale } => {
            write!(out, "DECIMAL({precision},{scale})").map_err(|_| format_error())?
        }
        DataType::Char(len) => write!(out, "CHAR({len})").map_err(|_| format_error())?,
        DataType::Varchar(len) => write!(out, "VARCHAR({len})").map_err(|_| format_error())?,
        DataType::Text => out.push_str("TEXT"),
        DataType::Blob => out.push_str("BLOB"),
        DataType::Date => out.push_str("DATE"),
        DataType::Time => out.push_str("TIME"),
        DataType::DateTime => out.push_str("DATETIME"),
        DataType::Timestamp => out.push_str("TIMESTAMP"),
    }
    Ok(())
}

fn append_default_value(out: &mut String, value: &DefaultValue) -> Result<()> {
    match value {
        DefaultValue::Null => out.push_str("NULL"),
        DefaultValue::Int(v) => write!(out, "{v}").map_err(|_| format_error())?,
        DefaultValue::Float(v) => {
            let mut buf = ryu::Buffer::new();
            out.push_str(buf.format(*v));
        }
        DefaultValue::String(v) => append_quoted_string_literal(out, v),
        DefaultValue::Bool(v) => {
            if *v {
                out.push_str("TRUE");
            } else {
                out.push_str("FALSE");
            }
        }
        DefaultValue::CurrentTimestamp => out.push_str("CURRENT_TIMESTAMP"),
    }
    Ok(())
}

fn append_quoted_string_literal(out: &mut String, value: &str) {
    out.push('\'');
    for ch in value.chars() {
        if ch == '\'' {
            out.push_str("''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
}

fn format_error() -> TiSqlError {
    TiSqlError::Internal("Failed to render SHOW CREATE TABLE output".into())
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Statement as SqlStatement, TableConstraint};
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser as SqlParser;

    use super::*;
    use crate::catalog::{ColumnDef, IndexDef};

    fn parse_one(sql: &str) -> SqlStatement {
        let mut stmts = SqlParser::parse_sql(&MySqlDialect {}, sql).unwrap();
        assert_eq!(stmts.len(), 1);
        stmts.remove(0)
    }

    fn sample_table() -> TableDef {
        let mut table = TableDef::new(
            1,
            "orders".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::BigInt, false, None, true),
                ColumnDef::new(
                    2,
                    "name".into(),
                    DataType::Varchar(255),
                    true,
                    Some(DefaultValue::Null),
                    false,
                ),
                ColumnDef::new(
                    3,
                    "active".into(),
                    DataType::Boolean,
                    false,
                    Some(DefaultValue::Bool(true)),
                    false,
                ),
                ColumnDef::new(
                    4,
                    "note".into(),
                    DataType::Text,
                    true,
                    Some(DefaultValue::String("o'reilly".into())),
                    false,
                ),
            ],
            vec![1],
        );
        table.add_index(IndexDef::new(7, "idx_name".into(), vec![2], false));
        table.add_index(IndexDef::new(8, "uniq_active".into(), vec![3], true));
        for _ in 0..5 {
            table.increment_auto_id();
        }
        table
    }

    #[test]
    fn test_render_create_table_layout_and_keywords() {
        let sql = render_create_table(&sample_table()).unwrap();
        let expected = "\
CREATE TABLE `default`.`orders` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(255) DEFAULT NULL,
  `active` BOOLEAN NOT NULL DEFAULT TRUE,
  `note` TEXT DEFAULT 'o''reilly',
  PRIMARY KEY (`id`),
  KEY `idx_name` (`name`),
  UNIQUE KEY `uniq_active` (`active`)
) AUTO_INCREMENT=5";
        assert_eq!(sql, expected);
        assert!(!sql.ends_with('\n'));
        assert!(!sql.contains("IF NOT EXISTS"));
    }

    #[test]
    fn test_render_create_table_datatype_coverage() {
        let table = TableDef::new(
            2,
            "all_types".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "c1".into(), DataType::Boolean, true, None, false),
                ColumnDef::new(2, "c2".into(), DataType::TinyInt, true, None, false),
                ColumnDef::new(3, "c3".into(), DataType::SmallInt, true, None, false),
                ColumnDef::new(4, "c4".into(), DataType::Int, true, None, false),
                ColumnDef::new(5, "c5".into(), DataType::BigInt, true, None, false),
                ColumnDef::new(6, "c6".into(), DataType::Float, true, None, false),
                ColumnDef::new(7, "c7".into(), DataType::Double, true, None, false),
                ColumnDef::new(
                    8,
                    "c8".into(),
                    DataType::Decimal {
                        precision: 12,
                        scale: 4,
                    },
                    true,
                    None,
                    false,
                ),
                ColumnDef::new(9, "c9".into(), DataType::Char(8), true, None, false),
                ColumnDef::new(10, "c10".into(), DataType::Varchar(32), true, None, false),
                ColumnDef::new(11, "c11".into(), DataType::Text, true, None, false),
                ColumnDef::new(12, "c12".into(), DataType::Blob, true, None, false),
                ColumnDef::new(13, "c13".into(), DataType::Date, true, None, false),
                ColumnDef::new(14, "c14".into(), DataType::Time, true, None, false),
                ColumnDef::new(15, "c15".into(), DataType::DateTime, true, None, false),
                ColumnDef::new(16, "c16".into(), DataType::Timestamp, true, None, false),
            ],
            vec![],
        );
        let sql = render_create_table(&table).unwrap();
        assert!(sql.contains("BOOLEAN"));
        assert!(sql.contains("TINYINT"));
        assert!(sql.contains("SMALLINT"));
        assert!(sql.contains("INT"));
        assert!(sql.contains("BIGINT"));
        assert!(sql.contains("FLOAT"));
        assert!(sql.contains("DOUBLE"));
        assert!(sql.contains("DECIMAL(12,4)"));
        assert!(sql.contains("CHAR(8)"));
        assert!(sql.contains("VARCHAR(32)"));
        assert!(sql.contains("TEXT"));
        assert!(sql.contains("BLOB"));
        assert!(sql.contains("DATE"));
        assert!(sql.contains("TIME"));
        assert!(sql.contains("DATETIME"));
        assert!(sql.contains("TIMESTAMP"));
    }

    #[test]
    fn test_render_identifier_and_string_escaping() {
        let table = TableDef::new(
            3,
            "t`b".to_string(),
            "db`x".to_string(),
            vec![ColumnDef::new(
                1,
                "c`1".into(),
                DataType::Varchar(16),
                true,
                Some(DefaultValue::String("a'b".into())),
                false,
            )],
            vec![],
        );
        let sql = render_create_table(&table).unwrap();
        assert!(sql.contains("CREATE TABLE `db``x`.`t``b`"));
        assert!(sql.contains("`c``1` VARCHAR(16) DEFAULT 'a''b'"));
    }

    #[test]
    fn test_render_roundtrip_parse_preserves_key_clauses() {
        let mut table = TableDef::new(
            4,
            "rt".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::Int, false, None, true),
                ColumnDef::new(
                    2,
                    "name".into(),
                    DataType::Varchar(64),
                    true,
                    Some(DefaultValue::Null),
                    false,
                ),
                ColumnDef::new(
                    3,
                    "created_at".into(),
                    DataType::Timestamp,
                    false,
                    Some(DefaultValue::CurrentTimestamp),
                    false,
                ),
            ],
            vec![1],
        );
        table.add_index(IndexDef::new(11, "idx_name".into(), vec![2], false));
        for _ in 0..7 {
            table.increment_auto_id();
        }

        let rendered = render_create_table(&table).unwrap();
        let stmt = parse_one(&rendered);
        match stmt {
            SqlStatement::CreateTable {
                name,
                columns,
                constraints,
                auto_increment_offset,
                ..
            } => {
                assert_eq!(name.0.len(), 2);
                assert_eq!(name.0[0].value, "default");
                assert_eq!(name.0[1].value, "rt");
                assert_eq!(columns.len(), 3);
                assert!(constraints.iter().any(|c| matches!(
                    c,
                    TableConstraint::Unique {
                        is_primary: true,
                        columns,
                        ..
                    } if columns.len() == 1 && columns[0].value == "id"
                )));
                assert!(constraints.iter().any(|c| matches!(
                    c,
                    TableConstraint::Index {
                        name: Some(index_name),
                        columns,
                        ..
                    } if index_name.value == "idx_name"
                        && columns.len() == 1
                        && columns[0].value == "name"
                )));
                assert_eq!(auto_increment_offset, Some(7));
            }
            _ => panic!("expected CREATE TABLE statement"),
        }
    }
}
