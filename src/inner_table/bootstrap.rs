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

//! Bootstrap writer for inner-table-based catalog.
//!
//! Writes core system table rows directly to KV storage (no SQL dependency).
//! All writes happen in a single transaction — if bootstrap fails mid-write,
//! the transaction is not committed and next startup retries from scratch.

use crate::catalog::types::Value;
use crate::codec::key::encode_record_key_with_handle;
use crate::codec::row::encode_row;
use crate::inner_table::core_tables::*;
use crate::transaction::{TxnCtx, TxnService};
use crate::util::error::Result;

/// Check if the database has been bootstrapped by reading the `__all_meta`
/// bootstrap_version row (meta_id=1) via direct KV read.
pub async fn is_bootstrapped<T: TxnService>(txn: &T) -> Result<bool> {
    let ctx = txn.begin(true)?;
    let key = encode_record_key_with_handle(ALL_META_TABLE_ID, META_BOOTSTRAP_VERSION);
    Ok(txn.get(&ctx, ALL_META_TABLE_ID, &key).await?.is_some())
}

/// Bootstrap all core system tables via direct KV writes in a single transaction.
///
/// Writes:
/// 1. `__all_schema` rows (inner, default, test)
/// 2. `__all_table` rows (one per core table)
/// 3. `__all_column` rows (columns of all 5 core tables)
/// 4. `__all_meta` rows (bootstrap_version, next_table_id, next_index_id, schema_version, next_schema_id)
pub async fn bootstrap_core_tables<T: TxnService>(txn: &T) -> Result<()> {
    let mut ctx = txn.begin(false)?;

    // 1. Write __all_schema rows
    write_schema_row(&mut ctx, txn, INNER_SCHEMA_ID, INNER_SCHEMA).await?;
    write_schema_row(&mut ctx, txn, DEFAULT_SCHEMA_ID, "default").await?;
    write_schema_row(&mut ctx, txn, TEST_SCHEMA_ID, "test").await?;

    // 2. Write __all_table rows for all 5 core tables
    for table_def in core_table_defs() {
        write_table_row(&mut ctx, txn, &table_def).await?;
    }

    // 3. Write __all_column rows for all columns of all 5 core tables
    for table_def in core_table_defs() {
        for (ordinal, col) in table_def.columns().iter().enumerate() {
            write_column_row(&mut ctx, txn, table_def.id(), col, ordinal).await?;
        }
    }

    // 4. Write __all_meta bootstrap entries
    write_meta_row(
        &mut ctx,
        txn,
        META_BOOTSTRAP_VERSION,
        "bootstrap_version",
        "1",
    )
    .await?;
    write_meta_row(
        &mut ctx,
        txn,
        META_NEXT_TABLE_ID,
        "next_table_id",
        &USER_TABLE_ID_START.to_string(),
    )
    .await?;
    write_meta_row(&mut ctx, txn, META_NEXT_INDEX_ID, "next_index_id", "1").await?;
    write_meta_row(&mut ctx, txn, META_SCHEMA_VERSION, "schema_version", "1").await?;
    write_meta_row(
        &mut ctx,
        txn,
        META_NEXT_SCHEMA_ID,
        "next_schema_id",
        &USER_SCHEMA_ID_START.to_string(),
    )
    .await?;
    write_meta_row(&mut ctx, txn, META_NEXT_GC_TASK_ID, "next_gc_task_id", "1").await?;

    txn.commit(ctx).await?;
    Ok(())
}

// ============================================================================
// Private Helpers
// ============================================================================

/// Write a row into `__all_meta` (table_id=1).
async fn write_meta_row<T: TxnService>(
    ctx: &mut TxnCtx,
    txn: &T,
    meta_id: i64,
    meta_key: &str,
    meta_value: &str,
) -> Result<()> {
    let key = encode_record_key_with_handle(ALL_META_TABLE_ID, meta_id);
    let col_ids = &[0, 1, 2, 3];
    let values = &[
        Value::BigInt(meta_id),
        Value::String(meta_key.to_string()),
        Value::String(meta_value.to_string()),
        Value::BigInt(0), // updated_ts placeholder
    ];
    let row_data = encode_row(col_ids, values);
    txn.put(ctx, ALL_META_TABLE_ID, key, row_data).await
}

/// Write a row into `__all_schema` (table_id=2).
async fn write_schema_row<T: TxnService>(
    ctx: &mut TxnCtx,
    txn: &T,
    schema_id: u64,
    schema_name: &str,
) -> Result<()> {
    let key = encode_record_key_with_handle(ALL_SCHEMA_TABLE_ID, schema_id as i64);
    let col_ids = &[0, 1];
    let values = &[
        Value::BigInt(schema_id as i64),
        Value::String(schema_name.to_string()),
    ];
    let row_data = encode_row(col_ids, values);
    txn.put(ctx, ALL_SCHEMA_TABLE_ID, key, row_data).await
}

/// Write a row into `__all_table` (table_id=3).
async fn write_table_row<T: TxnService>(
    ctx: &mut TxnCtx,
    txn: &T,
    table: &crate::catalog::TableDef,
) -> Result<()> {
    let pk_str = table
        .primary_key()
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");

    // For core tables during bootstrap, schema_id is always INNER_SCHEMA_ID
    let key = encode_record_key_with_handle(ALL_TABLE_TABLE_ID, table.id() as i64);
    let col_ids = &[0, 1, 2, 3, 4];
    let values = &[
        Value::BigInt(table.id() as i64),
        Value::BigInt(INNER_SCHEMA_ID as i64),
        Value::String(table.name().to_string()),
        Value::String(pk_str),
        Value::BigInt(table.auto_increment_id() as i64),
    ];
    let row_data = encode_row(col_ids, values);
    txn.put(ctx, ALL_TABLE_TABLE_ID, key, row_data).await
}

/// Write a row into `__all_column` (table_id=4).
///
/// Synthetic PK: `table_id * 10000 + column_id`.
async fn write_column_row<T: TxnService>(
    ctx: &mut TxnCtx,
    txn: &T,
    table_id: u64,
    col: &crate::catalog::ColumnDef,
    ordinal: usize,
) -> Result<()> {
    let column_key = table_id * 10000 + col.id() as u64;
    let key = encode_record_key_with_handle(ALL_COLUMN_TABLE_ID, column_key as i64);
    let col_ids = &[0, 1, 2, 3, 4, 5, 6, 7, 8];
    let default_val = match col.default() {
        Some(d) => Value::String(format_default(d)),
        None => Value::Null,
    };
    let values = &[
        Value::BigInt(column_key as i64),
        Value::BigInt(table_id as i64),
        Value::BigInt(col.id() as i64),
        Value::String(col.name().to_string()),
        Value::String(format_data_type(col.data_type())),
        Value::Int(ordinal as i32),
        Value::Int(col.nullable() as i32),
        default_val,
        Value::Int(col.auto_increment() as i32),
    ];
    let row_data = encode_row(col_ids, values);
    txn.put(ctx, ALL_COLUMN_TABLE_ID, key, row_data).await
}

/// Write a row into `__all_table` for a user table with an explicit schema_id.
pub(crate) async fn write_user_table_row<T: TxnService>(
    ctx: &mut TxnCtx,
    txn: &T,
    table: &crate::catalog::TableDef,
    schema_id: u64,
) -> Result<()> {
    let pk_str = table
        .primary_key()
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");

    let key = encode_record_key_with_handle(ALL_TABLE_TABLE_ID, table.id() as i64);
    let col_ids = &[0, 1, 2, 3, 4];
    let values = &[
        Value::BigInt(table.id() as i64),
        Value::BigInt(schema_id as i64),
        Value::String(table.name().to_string()),
        Value::String(pk_str),
        Value::BigInt(table.auto_increment_id() as i64),
    ];
    let row_data = encode_row(col_ids, values);
    txn.put(ctx, ALL_TABLE_TABLE_ID, key, row_data).await
}

/// Write column rows into `__all_column` for a user table.
pub(crate) async fn write_user_column_rows<T: TxnService>(
    ctx: &mut TxnCtx,
    txn: &T,
    table_id: u64,
    columns: &[crate::catalog::ColumnDef],
) -> Result<()> {
    for (ordinal, col) in columns.iter().enumerate() {
        write_column_row(ctx, txn, table_id, col, ordinal).await?;
    }
    Ok(())
}

/// Write an index row into `__all_index` (table_id=5).
///
/// Synthetic PK: `table_id * 10000 + index_id`.
pub(crate) async fn write_index_row<T: TxnService>(
    ctx: &mut TxnCtx,
    txn: &T,
    table_id: u64,
    index: &crate::catalog::IndexDef,
) -> Result<()> {
    let index_key = table_id * 10000 + index.id();
    let key = encode_record_key_with_handle(ALL_INDEX_TABLE_ID, index_key as i64);
    let col_ids_str = index
        .columns()
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let col_ids = &[0, 1, 2, 3, 4, 5];
    let values = &[
        Value::BigInt(index_key as i64),
        Value::BigInt(table_id as i64),
        Value::BigInt(index.id() as i64),
        Value::String(index.name().to_string()),
        Value::String(col_ids_str),
        Value::Int(index.unique() as i32),
    ];
    let row_data = encode_row(col_ids, values);
    txn.put(ctx, ALL_INDEX_TABLE_ID, key, row_data).await
}

/// Delete all column rows for a table from `__all_column`.
pub(crate) async fn delete_column_rows<T: TxnService>(
    ctx: &mut TxnCtx,
    txn: &T,
    table_id: u64,
    columns: &[crate::catalog::ColumnDef],
) -> Result<()> {
    for col in columns {
        let column_key = table_id * 10000 + col.id() as u64;
        let key = encode_record_key_with_handle(ALL_COLUMN_TABLE_ID, column_key as i64);
        txn.delete(ctx, ALL_COLUMN_TABLE_ID, key).await?;
    }
    Ok(())
}

/// Delete all index rows for a table from `__all_index`.
pub(crate) async fn delete_index_rows<T: TxnService>(
    ctx: &mut TxnCtx,
    txn: &T,
    table_id: u64,
    indexes: &[crate::catalog::IndexDef],
) -> Result<()> {
    for index in indexes {
        let index_key = table_id * 10000 + index.id();
        let key = encode_record_key_with_handle(ALL_INDEX_TABLE_ID, index_key as i64);
        txn.delete(ctx, ALL_INDEX_TABLE_ID, key).await?;
    }
    Ok(())
}

/// Write a row into `__all_gc_delete_range` (table_id=6).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_gc_task_row<T: TxnService>(
    ctx: &mut TxnCtx,
    txn: &T,
    task_id: i64,
    table_id: u64,
    start_key_hex: &str,
    end_key_hex: &str,
    drop_commit_ts: u64,
    status: &str,
) -> Result<()> {
    let key = encode_record_key_with_handle(ALL_GC_DELETE_RANGE_TABLE_ID, task_id);
    let col_ids = &[0, 1, 2, 3, 4, 5];
    let values = &[
        Value::BigInt(task_id),
        Value::BigInt(table_id as i64),
        Value::String(start_key_hex.to_string()),
        Value::String(end_key_hex.to_string()),
        Value::BigInt(drop_commit_ts as i64),
        Value::String(status.to_string()),
    ];
    let row_data = encode_row(col_ids, values);
    txn.put(ctx, ALL_GC_DELETE_RANGE_TABLE_ID, key, row_data)
        .await
}

/// Update the status of a GC task in `__all_gc_delete_range`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn update_gc_task_status<T: TxnService>(
    ctx: &mut TxnCtx,
    txn: &T,
    task_id: i64,
    table_id: u64,
    start_key_hex: &str,
    end_key_hex: &str,
    drop_commit_ts: u64,
    status: &str,
) -> Result<()> {
    write_gc_task_row(
        ctx,
        txn,
        task_id,
        table_id,
        start_key_hex,
        end_key_hex,
        drop_commit_ts,
        status,
    )
    .await
}

/// Update a meta row in `__all_meta` (overwrite existing row).
pub(crate) async fn update_meta_row<T: TxnService>(
    ctx: &mut TxnCtx,
    txn: &T,
    meta_id: i64,
    value: u64,
) -> Result<()> {
    let meta_key = META_KEYS[meta_id as usize];
    write_meta_row(ctx, txn, meta_id, meta_key, &value.to_string()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clog::{FileClogConfig, FileClogService};
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
    async fn test_not_bootstrapped_initially() {
        let (txn, _dir) = create_test_txn();
        assert!(!is_bootstrapped(txn.as_ref()).await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bootstrap_then_detect() {
        let (txn, _dir) = create_test_txn();
        assert!(!is_bootstrapped(txn.as_ref()).await.unwrap());
        bootstrap_core_tables(txn.as_ref()).await.unwrap();
        assert!(is_bootstrapped(txn.as_ref()).await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bootstrap_idempotent_detection() {
        let (txn, _dir) = create_test_txn();
        bootstrap_core_tables(txn.as_ref()).await.unwrap();
        // Calling is_bootstrapped multiple times should always return true
        assert!(is_bootstrapped(txn.as_ref()).await.unwrap());
        assert!(is_bootstrapped(txn.as_ref()).await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bootstrap_writes_meta_rows() {
        let (txn, _dir) = create_test_txn();
        bootstrap_core_tables(txn.as_ref()).await.unwrap();

        // Verify all 6 meta rows exist
        let ctx = txn.begin(true).unwrap();
        for meta_id in 1..=6i64 {
            let key = encode_record_key_with_handle(ALL_META_TABLE_ID, meta_id);
            assert!(
                txn.get(&ctx, ALL_META_TABLE_ID, &key)
                    .await
                    .unwrap()
                    .is_some(),
                "Meta row {meta_id} missing"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bootstrap_writes_schema_rows() {
        let (txn, _dir) = create_test_txn();
        bootstrap_core_tables(txn.as_ref()).await.unwrap();

        let ctx = txn.begin(true).unwrap();
        // inner, default, test schemas
        for schema_id in [INNER_SCHEMA_ID, DEFAULT_SCHEMA_ID, TEST_SCHEMA_ID] {
            let key = encode_record_key_with_handle(ALL_SCHEMA_TABLE_ID, schema_id as i64);
            assert!(
                txn.get(&ctx, ALL_META_TABLE_ID, &key)
                    .await
                    .unwrap()
                    .is_some(),
                "Schema row {schema_id} missing"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bootstrap_writes_table_rows() {
        let (txn, _dir) = create_test_txn();
        bootstrap_core_tables(txn.as_ref()).await.unwrap();

        let ctx = txn.begin(true).unwrap();
        for table_id in 1..=5u64 {
            let key = encode_record_key_with_handle(ALL_TABLE_TABLE_ID, table_id as i64);
            assert!(
                txn.get(&ctx, ALL_META_TABLE_ID, &key)
                    .await
                    .unwrap()
                    .is_some(),
                "Table row {table_id} missing"
            );
        }
    }
}
