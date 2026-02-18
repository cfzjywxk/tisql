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

//! Catalog cache and loading from inner tables.
//!
//! Provides [`CatalogCache`] — an in-memory cache of all schema metadata
//! reconstructed from inner tables at startup and kept in sync by DDL.

use std::collections::HashMap;

use crate::catalog::types::{DataType, TableId, Timestamp, Value};
use crate::catalog::{ColumnDef, IndexDef, TableDef};
use crate::codec::key::{encode_record_key_with_handle, gen_table_record_prefix};
use crate::codec::row::decode_row_to_values;
use crate::inner_table::core_tables::*;
use crate::tablet::MvccIterator;
use crate::transaction::{TxnCtx, TxnService};
use crate::util::error::{Result, TiSqlError};

// ============================================================================
// CatalogCache
// ============================================================================

/// In-memory cache of all schema metadata, rebuilt from inner tables at startup.
#[derive(Debug)]
pub struct CatalogCache {
    /// schema_name → schema_id
    pub schemas: HashMap<String, u64>,
    /// schema_id → schema_name
    pub schema_names: HashMap<u64, String>,
    /// (schema_name, table_name) → TableDef
    pub tables: HashMap<(String, String), TableDef>,
    /// table_id → (schema_name, table_name)
    pub table_id_map: HashMap<TableId, (String, String)>,
}

impl CatalogCache {
    fn new() -> Self {
        Self {
            schemas: HashMap::new(),
            schema_names: HashMap::new(),
            tables: HashMap::new(),
            table_id_map: HashMap::new(),
        }
    }
}

// ============================================================================
// Counters returned from load_catalog
// ============================================================================

/// All counters loaded from `__all_meta`.
pub struct CatalogCounters {
    pub schema_version: u64,
    pub next_table_id: u64,
    pub next_index_id: u64,
    pub next_schema_id: u64,
    pub next_gc_task_id: u64,
}

// ============================================================================
// Load Catalog
// ============================================================================

/// Load the entire catalog from inner tables into memory.
///
/// Scans `__all_schema`, `__all_table`, `__all_column`, `__all_index`, and
/// reads counters from `__all_meta`. Returns the populated cache and counters.
pub fn load_catalog<T: TxnService>(txn: &T) -> Result<(CatalogCache, CatalogCounters)> {
    let ctx = txn.begin(true)?;
    let mut cache = CatalogCache::new();

    // 1. Load counters from __all_meta
    let counters = load_counters(txn, &ctx)?;

    // 2. Load schemas
    load_schemas(txn, &ctx, &mut cache)?;

    // 3. Load tables + columns + indexes
    load_tables(txn, &ctx, &mut cache)?;

    Ok((cache, counters))
}

/// Scan a table's rows. Returns decoded rows as Vec<Vec<Value>>.
fn scan_table_rows<T: TxnService>(
    txn: &T,
    ctx: &TxnCtx,
    table_id: u64,
    col_ids: &[u32],
    data_types: &[DataType],
) -> Result<Vec<Vec<Value>>> {
    let prefix = gen_table_record_prefix(table_id);
    let mut end = prefix.clone();
    // Increment last byte for exclusive end bound
    if let Some(last) = end.last_mut() {
        *last = last.saturating_add(1);
    }

    let mut iter = txn.scan_iter(ctx, prefix..end)?;
    let mut rows = Vec::new();

    crate::io::block_on_sync(iter.advance())?;
    while iter.valid() {
        let value = iter.value();
        let decoded = decode_row_to_values(value, col_ids, data_types)?;
        rows.push(decoded);
        crate::io::block_on_sync(iter.advance())?;
    }

    Ok(rows)
}

/// Load counters from `__all_meta`.
fn load_counters<T: TxnService>(txn: &T, ctx: &TxnCtx) -> Result<CatalogCounters> {
    let mut schema_version = 0u64;
    let mut next_table_id = USER_TABLE_ID_START;
    let mut next_index_id = 1u64;
    let mut next_schema_id = USER_SCHEMA_ID_START;
    let mut next_gc_task_id = 1u64;

    // Read each well-known meta row
    for meta_id in [
        META_BOOTSTRAP_VERSION,
        META_NEXT_TABLE_ID,
        META_NEXT_INDEX_ID,
        META_SCHEMA_VERSION,
        META_NEXT_SCHEMA_ID,
        META_NEXT_GC_TASK_ID,
    ] {
        let key = encode_record_key_with_handle(ALL_META_TABLE_ID, meta_id);
        if let Some(data) = crate::io::block_on_sync(txn.get(ctx, &key))? {
            let col_ids = &[0, 1, 2, 3];
            let data_types = &[
                DataType::BigInt,
                DataType::Varchar(128),
                DataType::Varchar(1024),
                DataType::BigInt,
            ];
            let values = decode_row_to_values(&data, col_ids, data_types)?;
            // meta_value is at index 2
            if let Value::String(ref s) = values[2] {
                let v: u64 = s
                    .parse()
                    .map_err(|_| TiSqlError::Catalog(format!("Invalid meta value: '{s}'")))?;
                match meta_id {
                    META_NEXT_TABLE_ID => next_table_id = v,
                    META_NEXT_INDEX_ID => next_index_id = v,
                    META_SCHEMA_VERSION => schema_version = v,
                    META_NEXT_SCHEMA_ID => next_schema_id = v,
                    META_NEXT_GC_TASK_ID => next_gc_task_id = v,
                    _ => {} // bootstrap_version — ignored for now
                }
            }
        }
    }

    Ok(CatalogCounters {
        schema_version,
        next_table_id,
        next_index_id,
        next_schema_id,
        next_gc_task_id,
    })
}

/// Load schemas from `__all_schema` into the cache.
fn load_schemas<T: TxnService>(txn: &T, ctx: &TxnCtx, cache: &mut CatalogCache) -> Result<()> {
    let col_ids = &[0, 1];
    let data_types = &[DataType::BigInt, DataType::Varchar(128)];

    let rows = scan_table_rows(txn, ctx, ALL_SCHEMA_TABLE_ID, col_ids, data_types)?;

    for row in rows {
        if let (Value::BigInt(id), Value::String(ref name)) = (&row[0], &row[1]) {
            cache.schemas.insert(name.clone(), *id as u64);
            cache.schema_names.insert(*id as u64, name.clone());
        }
    }

    Ok(())
}

/// Load tables, columns, and indexes from inner tables into the cache.
fn load_tables<T: TxnService>(txn: &T, ctx: &TxnCtx, cache: &mut CatalogCache) -> Result<()> {
    // Load raw table rows
    let table_col_ids = &[0, 1, 2, 3, 4];
    let table_data_types = &[
        DataType::BigInt,
        DataType::BigInt,
        DataType::Varchar(128),
        DataType::Varchar(256),
        DataType::BigInt,
    ];
    let table_rows = scan_table_rows(
        txn,
        ctx,
        ALL_TABLE_TABLE_ID,
        table_col_ids,
        table_data_types,
    )?;

    // Load all column rows, grouped by table_id
    let col_col_ids = &[0, 1, 2, 3, 4, 5, 6, 7, 8];
    let col_data_types = &[
        DataType::BigInt,       // column_key
        DataType::BigInt,       // table_id
        DataType::BigInt,       // column_id
        DataType::Varchar(128), // column_name
        DataType::Varchar(64),  // data_type
        DataType::Int,          // ordinal_position
        DataType::Int,          // is_nullable
        DataType::Varchar(256), // default_value (nullable)
        DataType::Int,          // is_auto_increment
    ];
    let col_rows = scan_table_rows(txn, ctx, ALL_COLUMN_TABLE_ID, col_col_ids, col_data_types)?;

    // Group columns by table_id
    let mut columns_by_table: HashMap<u64, Vec<Vec<Value>>> = HashMap::new();
    for row in col_rows {
        if let Value::BigInt(table_id) = row[1] {
            columns_by_table
                .entry(table_id as u64)
                .or_default()
                .push(row);
        }
    }

    // Load all index rows, grouped by table_id
    let idx_col_ids = &[0, 1, 2, 3, 4, 5];
    let idx_data_types = &[
        DataType::BigInt,       // index_key
        DataType::BigInt,       // table_id
        DataType::BigInt,       // index_id
        DataType::Varchar(128), // index_name
        DataType::Varchar(256), // column_ids
        DataType::Int,          // is_unique
    ];
    let idx_rows = scan_table_rows(txn, ctx, ALL_INDEX_TABLE_ID, idx_col_ids, idx_data_types)?;

    let mut indexes_by_table: HashMap<u64, Vec<Vec<Value>>> = HashMap::new();
    for row in idx_rows {
        if let Value::BigInt(table_id) = row[1] {
            indexes_by_table
                .entry(table_id as u64)
                .or_default()
                .push(row);
        }
    }

    // Reconstruct TableDef objects
    for table_row in table_rows {
        let table_id = match table_row[0] {
            Value::BigInt(v) => v as u64,
            _ => continue,
        };
        let schema_id = match table_row[1] {
            Value::BigInt(v) => v as u64,
            _ => continue,
        };
        let table_name = match &table_row[2] {
            Value::String(s) => s.clone(),
            _ => continue,
        };
        let pk_str = match &table_row[3] {
            Value::String(s) => s.clone(),
            _ => continue,
        };
        let auto_increment_id = match table_row[4] {
            Value::BigInt(v) => v as u64,
            _ => 0,
        };

        // Resolve schema name from schema_id
        let schema_name = match cache.schema_names.get(&schema_id) {
            Some(name) => name.clone(),
            None => continue, // orphaned table — skip
        };

        // Parse primary key columns
        let primary_key: Vec<u32> = if pk_str.is_empty() {
            Vec::new()
        } else {
            pk_str
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect()
        };

        // Reconstruct columns (sorted by ordinal_position)
        let mut columns = Vec::new();
        if let Some(col_rows) = columns_by_table.get(&table_id) {
            let mut sorted_cols: Vec<&Vec<Value>> = col_rows.iter().collect();
            sorted_cols.sort_by_key(|r| match r[5] {
                Value::Int(v) => v,
                _ => 0,
            });

            for col_row in sorted_cols {
                let col_id = match col_row[2] {
                    Value::BigInt(v) => v as u32,
                    _ => continue,
                };
                let col_name = match &col_row[3] {
                    Value::String(s) => s.clone(),
                    _ => continue,
                };
                let data_type_str = match &col_row[4] {
                    Value::String(s) => s.clone(),
                    _ => continue,
                };
                let is_nullable = match col_row[6] {
                    Value::Int(v) => v != 0,
                    _ => false,
                };
                let default_value = match &col_row[7] {
                    Value::String(s) => parse_default(s),
                    Value::Null => None,
                    _ => None,
                };
                let is_auto_increment = match col_row[8] {
                    Value::Int(v) => v != 0,
                    _ => false,
                };

                let data_type = parse_data_type(&data_type_str)?;
                columns.push(ColumnDef::new(
                    col_id,
                    col_name,
                    data_type,
                    is_nullable,
                    default_value,
                    is_auto_increment,
                ));
            }
        }

        // Build TableDef
        let mut table_def = TableDef::new(
            table_id,
            table_name.clone(),
            schema_name.clone(),
            columns,
            primary_key,
        );

        // Set auto_increment_id
        for _ in 0..auto_increment_id {
            table_def.increment_auto_id();
        }

        // Reconstruct indexes
        if let Some(idx_rows) = indexes_by_table.get(&table_id) {
            for idx_row in idx_rows {
                let index_id = match idx_row[2] {
                    Value::BigInt(v) => v as u64,
                    _ => continue,
                };
                let index_name = match &idx_row[3] {
                    Value::String(s) => s.clone(),
                    _ => continue,
                };
                let col_ids_str = match &idx_row[4] {
                    Value::String(s) => s.clone(),
                    _ => continue,
                };
                let is_unique = match idx_row[5] {
                    Value::Int(v) => v != 0,
                    _ => false,
                };

                let col_ids: Vec<u32> = col_ids_str
                    .split(',')
                    .filter_map(|s| s.trim().parse().ok())
                    .collect();

                table_def.add_index(IndexDef::new(index_id, index_name, col_ids, is_unique));
            }
        }

        // Insert into cache
        let key = (schema_name.clone(), table_name.clone());
        cache.table_id_map.insert(table_id, key.clone());
        cache.tables.insert(key, table_def);
    }

    Ok(())
}

// ============================================================================
// Timestamp-Aware Scan (for MVCC reads)
// ============================================================================

/// Scan tables at a specific timestamp (bypasses cache).
pub fn scan_tables_at<T: TxnService>(
    txn: &T,
    schema_id: u64,
    ts: Timestamp,
) -> Result<Vec<TableDef>> {
    let ctx = TxnCtx::new(0, ts, true);

    // Scan all tables
    let table_col_ids = &[0, 1, 2, 3, 4];
    let table_data_types = &[
        DataType::BigInt,
        DataType::BigInt,
        DataType::Varchar(128),
        DataType::Varchar(256),
        DataType::BigInt,
    ];
    let table_rows = scan_table_rows(
        txn,
        &ctx,
        ALL_TABLE_TABLE_ID,
        table_col_ids,
        table_data_types,
    )?;

    // Filter by schema_id
    let matching_tables: Vec<&Vec<Value>> = table_rows
        .iter()
        .filter(|r| matches!(r[1], Value::BigInt(v) if v as u64 == schema_id))
        .collect();

    if matching_tables.is_empty() {
        return Ok(Vec::new());
    }

    // Scan columns
    let col_col_ids = &[0, 1, 2, 3, 4, 5, 6, 7, 8];
    let col_data_types = &[
        DataType::BigInt,
        DataType::BigInt,
        DataType::BigInt,
        DataType::Varchar(128),
        DataType::Varchar(64),
        DataType::Int,
        DataType::Int,
        DataType::Varchar(256),
        DataType::Int,
    ];
    let col_rows = scan_table_rows(txn, &ctx, ALL_COLUMN_TABLE_ID, col_col_ids, col_data_types)?;

    let mut columns_by_table: HashMap<u64, Vec<Vec<Value>>> = HashMap::new();
    for row in col_rows {
        if let Value::BigInt(table_id) = row[1] {
            columns_by_table
                .entry(table_id as u64)
                .or_default()
                .push(row);
        }
    }

    // Scan indexes
    let idx_col_ids = &[0, 1, 2, 3, 4, 5];
    let idx_data_types = &[
        DataType::BigInt,
        DataType::BigInt,
        DataType::BigInt,
        DataType::Varchar(128),
        DataType::Varchar(256),
        DataType::Int,
    ];
    let idx_rows = scan_table_rows(txn, &ctx, ALL_INDEX_TABLE_ID, idx_col_ids, idx_data_types)?;

    let mut indexes_by_table: HashMap<u64, Vec<Vec<Value>>> = HashMap::new();
    for row in idx_rows {
        if let Value::BigInt(table_id) = row[1] {
            indexes_by_table
                .entry(table_id as u64)
                .or_default()
                .push(row);
        }
    }

    // Load schema names for resolution
    let schema_col_ids = &[0, 1];
    let schema_data_types = &[DataType::BigInt, DataType::Varchar(128)];
    let schema_rows = scan_table_rows(
        txn,
        &ctx,
        ALL_SCHEMA_TABLE_ID,
        schema_col_ids,
        schema_data_types,
    )?;
    let mut schema_names: HashMap<u64, String> = HashMap::new();
    for row in schema_rows {
        if let (Value::BigInt(id), Value::String(ref name)) = (&row[0], &row[1]) {
            schema_names.insert(*id as u64, name.clone());
        }
    }

    let mut result = Vec::new();
    for table_row in &matching_tables {
        if let Some(table_def) = reconstruct_table_def(
            table_row,
            &schema_names,
            &columns_by_table,
            &indexes_by_table,
        )? {
            result.push(table_def);
        }
    }

    Ok(result)
}

/// Scan schemas at a specific timestamp (bypasses cache).
pub fn schema_exists_at<T: TxnService>(txn: &T, name: &str, ts: Timestamp) -> Result<bool> {
    let ctx = TxnCtx::new(0, ts, true);
    let col_ids = &[0, 1];
    let data_types = &[DataType::BigInt, DataType::Varchar(128)];
    let rows = scan_table_rows(txn, &ctx, ALL_SCHEMA_TABLE_ID, col_ids, data_types)?;
    Ok(rows
        .iter()
        .any(|r| matches!(&r[1], Value::String(s) if s == name)))
}

/// Get a single table at a specific timestamp (bypasses cache).
pub fn get_table_at<T: TxnService>(
    txn: &T,
    schema: &str,
    table: &str,
    ts: Timestamp,
) -> Result<Option<TableDef>> {
    let ctx = TxnCtx::new(0, ts, true);

    // Find schema_id
    let schema_col_ids = &[0, 1];
    let schema_data_types = &[DataType::BigInt, DataType::Varchar(128)];
    let schema_rows = scan_table_rows(
        txn,
        &ctx,
        ALL_SCHEMA_TABLE_ID,
        schema_col_ids,
        schema_data_types,
    )?;

    let mut schema_names: HashMap<u64, String> = HashMap::new();
    let mut schema_id = None;
    for row in &schema_rows {
        if let (Value::BigInt(id), Value::String(ref name)) = (&row[0], &row[1]) {
            schema_names.insert(*id as u64, name.clone());
            if name == schema {
                schema_id = Some(*id as u64);
            }
        }
    }

    let schema_id = match schema_id {
        Some(id) => id,
        None => return Ok(None),
    };

    // Scan tables to find matching one
    let table_col_ids = &[0, 1, 2, 3, 4];
    let table_data_types = &[
        DataType::BigInt,
        DataType::BigInt,
        DataType::Varchar(128),
        DataType::Varchar(256),
        DataType::BigInt,
    ];
    let table_rows = scan_table_rows(
        txn,
        &ctx,
        ALL_TABLE_TABLE_ID,
        table_col_ids,
        table_data_types,
    )?;

    let matching = table_rows.iter().find(|r| {
        matches!((&r[1], &r[2]), (Value::BigInt(sid), Value::String(tname))
            if *sid as u64 == schema_id && tname == table)
    });

    let table_row = match matching {
        Some(r) => r,
        None => return Ok(None),
    };

    let table_id = match table_row[0] {
        Value::BigInt(v) => v as u64,
        _ => return Ok(None),
    };

    // Load columns for this table
    let col_col_ids = &[0, 1, 2, 3, 4, 5, 6, 7, 8];
    let col_data_types = &[
        DataType::BigInt,
        DataType::BigInt,
        DataType::BigInt,
        DataType::Varchar(128),
        DataType::Varchar(64),
        DataType::Int,
        DataType::Int,
        DataType::Varchar(256),
        DataType::Int,
    ];
    let col_rows = scan_table_rows(txn, &ctx, ALL_COLUMN_TABLE_ID, col_col_ids, col_data_types)?;

    let mut columns_by_table: HashMap<u64, Vec<Vec<Value>>> = HashMap::new();
    for row in col_rows {
        if let Value::BigInt(tid) = row[1] {
            if tid as u64 == table_id {
                columns_by_table.entry(tid as u64).or_default().push(row);
            }
        }
    }

    // Load indexes for this table
    let idx_col_ids = &[0, 1, 2, 3, 4, 5];
    let idx_data_types = &[
        DataType::BigInt,
        DataType::BigInt,
        DataType::BigInt,
        DataType::Varchar(128),
        DataType::Varchar(256),
        DataType::Int,
    ];
    let idx_rows = scan_table_rows(txn, &ctx, ALL_INDEX_TABLE_ID, idx_col_ids, idx_data_types)?;

    let mut indexes_by_table: HashMap<u64, Vec<Vec<Value>>> = HashMap::new();
    for row in idx_rows {
        if let Value::BigInt(tid) = row[1] {
            if tid as u64 == table_id {
                indexes_by_table.entry(tid as u64).or_default().push(row);
            }
        }
    }

    reconstruct_table_def(
        table_row,
        &schema_names,
        &columns_by_table,
        &indexes_by_table,
    )
}

/// Get a table by ID at a specific timestamp (bypasses cache).
pub fn get_table_by_id_at<T: TxnService>(
    txn: &T,
    id: TableId,
    ts: Timestamp,
) -> Result<Option<TableDef>> {
    let ctx = TxnCtx::new(0, ts, true);

    // Read the table row directly by handle
    let key = encode_record_key_with_handle(ALL_TABLE_TABLE_ID, id as i64);
    let table_data = match crate::io::block_on_sync(txn.get(&ctx, &key))? {
        Some(data) => data,
        None => return Ok(None),
    };

    let table_col_ids = &[0, 1, 2, 3, 4];
    let table_data_types = &[
        DataType::BigInt,
        DataType::BigInt,
        DataType::Varchar(128),
        DataType::Varchar(256),
        DataType::BigInt,
    ];
    let table_row = decode_row_to_values(&table_data, table_col_ids, table_data_types)?;

    // Load schema names
    let schema_col_ids = &[0, 1];
    let schema_data_types = &[DataType::BigInt, DataType::Varchar(128)];
    let schema_rows = scan_table_rows(
        txn,
        &ctx,
        ALL_SCHEMA_TABLE_ID,
        schema_col_ids,
        schema_data_types,
    )?;
    let mut schema_names: HashMap<u64, String> = HashMap::new();
    for row in schema_rows {
        if let (Value::BigInt(sid), Value::String(ref name)) = (&row[0], &row[1]) {
            schema_names.insert(*sid as u64, name.clone());
        }
    }

    // Load columns
    let col_col_ids = &[0, 1, 2, 3, 4, 5, 6, 7, 8];
    let col_data_types = &[
        DataType::BigInt,
        DataType::BigInt,
        DataType::BigInt,
        DataType::Varchar(128),
        DataType::Varchar(64),
        DataType::Int,
        DataType::Int,
        DataType::Varchar(256),
        DataType::Int,
    ];
    let col_rows = scan_table_rows(txn, &ctx, ALL_COLUMN_TABLE_ID, col_col_ids, col_data_types)?;
    let mut columns_by_table: HashMap<u64, Vec<Vec<Value>>> = HashMap::new();
    for row in col_rows {
        if let Value::BigInt(tid) = row[1] {
            if tid as u64 == id {
                columns_by_table.entry(tid as u64).or_default().push(row);
            }
        }
    }

    // Load indexes
    let idx_col_ids = &[0, 1, 2, 3, 4, 5];
    let idx_data_types = &[
        DataType::BigInt,
        DataType::BigInt,
        DataType::BigInt,
        DataType::Varchar(128),
        DataType::Varchar(256),
        DataType::Int,
    ];
    let idx_rows = scan_table_rows(txn, &ctx, ALL_INDEX_TABLE_ID, idx_col_ids, idx_data_types)?;
    let mut indexes_by_table: HashMap<u64, Vec<Vec<Value>>> = HashMap::new();
    for row in idx_rows {
        if let Value::BigInt(tid) = row[1] {
            if tid as u64 == id {
                indexes_by_table.entry(tid as u64).or_default().push(row);
            }
        }
    }

    reconstruct_table_def(
        &table_row,
        &schema_names,
        &columns_by_table,
        &indexes_by_table,
    )
}

// ============================================================================
// Shared Reconstruction Helper
// ============================================================================

/// Reconstruct a TableDef from a decoded table row + grouped columns/indexes.
fn reconstruct_table_def(
    table_row: &[Value],
    schema_names: &HashMap<u64, String>,
    columns_by_table: &HashMap<u64, Vec<Vec<Value>>>,
    indexes_by_table: &HashMap<u64, Vec<Vec<Value>>>,
) -> Result<Option<TableDef>> {
    let table_id = match table_row[0] {
        Value::BigInt(v) => v as u64,
        _ => return Ok(None),
    };
    let schema_id = match table_row[1] {
        Value::BigInt(v) => v as u64,
        _ => return Ok(None),
    };
    let table_name = match &table_row[2] {
        Value::String(s) => s.clone(),
        _ => return Ok(None),
    };
    let pk_str = match &table_row[3] {
        Value::String(s) => s.clone(),
        _ => return Ok(None),
    };
    let auto_increment_id = match table_row[4] {
        Value::BigInt(v) => v as u64,
        _ => 0,
    };

    let schema_name = match schema_names.get(&schema_id) {
        Some(name) => name.clone(),
        None => return Ok(None),
    };

    let primary_key: Vec<u32> = if pk_str.is_empty() {
        Vec::new()
    } else {
        pk_str
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect()
    };

    // Reconstruct columns
    let mut columns = Vec::new();
    if let Some(col_rows) = columns_by_table.get(&table_id) {
        let mut sorted_cols: Vec<&Vec<Value>> = col_rows.iter().collect();
        sorted_cols.sort_by_key(|r| match r[5] {
            Value::Int(v) => v,
            _ => 0,
        });

        for col_row in sorted_cols {
            let col_id = match col_row[2] {
                Value::BigInt(v) => v as u32,
                _ => continue,
            };
            let col_name = match &col_row[3] {
                Value::String(s) => s.clone(),
                _ => continue,
            };
            let data_type_str = match &col_row[4] {
                Value::String(s) => s.clone(),
                _ => continue,
            };
            let is_nullable = match col_row[6] {
                Value::Int(v) => v != 0,
                _ => false,
            };
            let default_value = match &col_row[7] {
                Value::String(s) => parse_default(s),
                _ => None,
            };
            let is_auto_increment = match col_row[8] {
                Value::Int(v) => v != 0,
                _ => false,
            };

            let data_type = parse_data_type(&data_type_str)?;
            columns.push(ColumnDef::new(
                col_id,
                col_name,
                data_type,
                is_nullable,
                default_value,
                is_auto_increment,
            ));
        }
    }

    let mut table_def = TableDef::new(table_id, table_name, schema_name, columns, primary_key);

    // Restore auto_increment_id
    for _ in 0..auto_increment_id {
        table_def.increment_auto_id();
    }

    // Reconstruct indexes
    if let Some(idx_rows) = indexes_by_table.get(&table_id) {
        for idx_row in idx_rows {
            let index_id = match idx_row[2] {
                Value::BigInt(v) => v as u64,
                _ => continue,
            };
            let index_name = match &idx_row[3] {
                Value::String(s) => s.clone(),
                _ => continue,
            };
            let col_ids_str = match &idx_row[4] {
                Value::String(s) => s.clone(),
                _ => continue,
            };
            let is_unique = match idx_row[5] {
                Value::Int(v) => v != 0,
                _ => false,
            };

            let col_ids: Vec<u32> = col_ids_str
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect();

            table_def.add_index(IndexDef::new(index_id, index_name, col_ids, is_unique));
        }
    }

    Ok(Some(table_def))
}

// ============================================================================
// GC Task Loading
// ============================================================================

/// A GC delete-range task loaded from `__all_gc_delete_range`.
pub struct GcTask {
    pub task_id: i64,
    pub table_id: u64,
    pub start_key_hex: String,
    pub end_key_hex: String,
    pub drop_commit_ts: u64,
    pub status: String,
}

/// Load all GC tasks from `__all_gc_delete_range`.
///
/// Returns all tasks (including 'done' ones for completeness;
/// callers should filter by status as needed).
pub fn load_gc_tasks<T: TxnService>(txn: &T) -> Result<Vec<GcTask>> {
    let ctx = txn.begin(true)?;
    let col_ids = &[0, 1, 2, 3, 4, 5];
    let data_types = &[
        DataType::BigInt,       // task_id
        DataType::BigInt,       // table_id
        DataType::Varchar(512), // start_key
        DataType::Varchar(512), // end_key
        DataType::BigInt,       // drop_commit_ts
        DataType::Varchar(16),  // status
    ];

    let rows = scan_table_rows(txn, &ctx, ALL_GC_DELETE_RANGE_TABLE_ID, col_ids, data_types)?;

    let mut tasks = Vec::new();
    for row in rows {
        let task_id = match row[0] {
            Value::BigInt(v) => v,
            _ => continue,
        };
        let table_id = match row[1] {
            Value::BigInt(v) => v as u64,
            _ => continue,
        };
        let start_key_hex = match &row[2] {
            Value::String(s) => s.clone(),
            _ => continue,
        };
        let end_key_hex = match &row[3] {
            Value::String(s) => s.clone(),
            _ => continue,
        };
        let drop_commit_ts = match row[4] {
            Value::BigInt(v) => v as u64,
            _ => 0,
        };
        let status = match &row[5] {
            Value::String(s) => s.clone(),
            _ => continue,
        };

        tasks.push(GcTask {
            task_id,
            table_id,
            start_key_hex,
            end_key_hex,
            drop_commit_ts,
            status,
        });
    }

    Ok(tasks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clog::{FileClogConfig, FileClogService};
    use crate::inner_table::bootstrap;
    use crate::tablet::MemTableEngine;
    use crate::transaction::{ConcurrencyManager, TransactionService};
    use crate::tso::LocalTso;
    use std::sync::Arc;
    use tempfile::tempdir;

    type TestTxnService = TransactionService<MemTableEngine, FileClogService, LocalTso>;

    fn make_test_io() -> Arc<crate::io::IoService> {
        crate::io::IoService::new_for_test(32).unwrap()
    }

    fn create_test_txn() -> (Arc<TestTxnService>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let clog_config = FileClogConfig::with_dir(dir.path());
        let io_handle = tokio::runtime::Handle::current();
        let storage = Arc::new(MemTableEngine::new());
        let (clog_service, _) = FileClogService::recover_with_lsn_provider(
            clog_config,
            storage.lsn_provider(),
            make_test_io(),
            &io_handle,
        )
        .unwrap();
        let clog_service = Arc::new(clog_service);
        let tso = Arc::new(LocalTso::new(1));
        let concurrency_manager = Arc::new(ConcurrencyManager::new(0));
        let txn_service = Arc::new(TransactionService::new(
            storage,
            clog_service,
            tso,
            concurrency_manager,
        ));
        (txn_service, dir)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_load_catalog_after_bootstrap() {
        let (txn, _dir) = create_test_txn();
        bootstrap::bootstrap_core_tables(txn.as_ref())
            .await
            .unwrap();

        let (cache, counters) = load_catalog(txn.as_ref()).unwrap();

        // Check schemas
        assert!(cache.schemas.contains_key(INNER_SCHEMA));
        assert!(cache.schemas.contains_key("default"));
        assert!(cache.schemas.contains_key("test"));
        assert_eq!(cache.schemas[INNER_SCHEMA], INNER_SCHEMA_ID);
        assert_eq!(cache.schemas["default"], DEFAULT_SCHEMA_ID);
        assert_eq!(cache.schemas["test"], TEST_SCHEMA_ID);

        // Check counters
        assert_eq!(counters.next_table_id, USER_TABLE_ID_START);
        assert_eq!(counters.next_index_id, 1);
        assert_eq!(counters.schema_version, 1);
        assert_eq!(counters.next_schema_id, USER_SCHEMA_ID_START);

        // Check core tables are loaded
        assert_eq!(cache.tables.len(), 6);
        assert!(cache
            .tables
            .contains_key(&(INNER_SCHEMA.to_string(), "__all_meta".to_string())));
        assert!(cache
            .tables
            .contains_key(&(INNER_SCHEMA.to_string(), "__all_table".to_string())));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_load_catalog_core_table_columns() {
        let (txn, _dir) = create_test_txn();
        bootstrap::bootstrap_core_tables(txn.as_ref())
            .await
            .unwrap();

        let (cache, _) = load_catalog(txn.as_ref()).unwrap();

        // __all_table should have 5 columns
        let all_table = cache
            .tables
            .get(&(INNER_SCHEMA.to_string(), "__all_table".to_string()))
            .unwrap();
        assert_eq!(all_table.columns().len(), 5);
        assert_eq!(all_table.columns()[0].name(), "table_id");
        assert_eq!(all_table.columns()[1].name(), "schema_id");
        assert_eq!(all_table.columns()[2].name(), "table_name");

        // __all_column should have 9 columns
        let all_column = cache
            .tables
            .get(&(INNER_SCHEMA.to_string(), "__all_column".to_string()))
            .unwrap();
        assert_eq!(all_column.columns().len(), 9);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_load_catalog_table_id_map() {
        let (txn, _dir) = create_test_txn();
        bootstrap::bootstrap_core_tables(txn.as_ref())
            .await
            .unwrap();

        let (cache, _) = load_catalog(txn.as_ref()).unwrap();

        // All 6 core tables should be in table_id_map
        assert_eq!(cache.table_id_map.len(), 6);
        assert_eq!(
            cache.table_id_map[&ALL_META_TABLE_ID],
            (INNER_SCHEMA.to_string(), "__all_meta".to_string())
        );
        assert_eq!(
            cache.table_id_map[&ALL_TABLE_TABLE_ID],
            (INNER_SCHEMA.to_string(), "__all_table".to_string())
        );
    }
}
