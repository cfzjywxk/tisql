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

//! MVCC-based persistent catalog using inner system tables.
//!
//! Schema metadata is stored in normalized inner SQL tables (`__all_table`,
//! `__all_column`, etc.) and cached in memory for fast lookups. DDL operations
//! write rows to inner tables and update the cache atomically.

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::codec::key::{encode_record_key_with_handle, gen_table_record_prefix};
use crate::codec::row::encode_row;
use crate::error::{Result, TiSqlError};
use crate::inner_table::bootstrap::{
    self, delete_column_rows, delete_index_rows, update_meta_row, write_gc_task_row,
    write_index_row, write_user_column_rows, write_user_table_row,
};
use crate::inner_table::catalog_loader::{self, CatalogCache};
use crate::inner_table::core_tables::*;
use crate::transaction::{TxnCtx, TxnService};
use crate::types::{IndexId, TableId, Timestamp, Value};

use super::{Catalog, IndexDef, TableDef};

/// Schema state protected by RwLock for DDL/DML concurrency control.
///
/// DDL operations hold write lock during commit (atomic version update + cache update).
/// DML operations hold read lock briefly to check version (no contention between DMLs).
struct SchemaState {
    /// Schema version, incremented on each DDL commit.
    version: u64,
    /// In-memory cache of all schema metadata.
    cache: CatalogCache,
}

/// MVCC-based catalog backed by inner system tables.
///
/// ## Storage Model
///
/// All schema metadata is stored in 5 core inner tables:
/// - `__all_meta` — global counters (schema_version, next_table_id, etc.)
/// - `__all_schema` — schema definitions
/// - `__all_table` — table definitions
/// - `__all_column` — column definitions
/// - `__all_index` — index definitions
///
/// ## DDL/DML Concurrency Control
///
/// DDL serialization uses `tokio::sync::Mutex` (Send-safe across await points).
/// Cache reads use `parking_lot::RwLock` (fast, no async overhead for DML reads).
///
/// DDL pattern: acquire ddl_mutex → validate under read lock → async txn work
/// (no parking_lot lock held) → update cache under write lock.
///
/// ## ID Generation
///
/// Uses atomic counters for table_id, index_id, schema_id to avoid transaction
/// conflicts during concurrent DDL operations. IDs are persisted to `__all_meta`.
pub struct MvccCatalog<T: TxnService> {
    txn_service: std::sync::Arc<T>,
    /// Schema state + cache protected by RwLock for fast DML reads.
    schema_state: RwLock<SchemaState>,
    /// Serializes DDL operations. tokio::sync::Mutex is Send-safe across await points,
    /// unlike parking_lot::RwLockWriteGuard.
    ddl_mutex: tokio::sync::Mutex<()>,
    /// Atomic counter for table IDs.
    next_table_id: AtomicU64,
    /// Atomic counter for index IDs.
    next_index_id: AtomicU64,
    /// Atomic counter for schema IDs.
    next_schema_id: AtomicU64,
    /// Atomic counter for GC task IDs.
    next_gc_task_id: AtomicU64,
}

impl<T: TxnService> MvccCatalog<T> {
    /// Create a new MVCC catalog.
    ///
    /// For fresh databases, call `bootstrap()` after creation.
    /// For existing databases, call `load_schema_version()` to recover state.
    pub fn new(txn_service: std::sync::Arc<T>) -> Self {
        Self {
            txn_service,
            schema_state: RwLock::new(SchemaState {
                version: 0,
                cache: CatalogCache {
                    schemas: Default::default(),
                    schema_names: Default::default(),
                    tables: Default::default(),
                    table_id_map: Default::default(),
                },
            }),
            ddl_mutex: tokio::sync::Mutex::new(()),
            next_table_id: AtomicU64::new(USER_TABLE_ID_START),
            next_index_id: AtomicU64::new(1),
            next_schema_id: AtomicU64::new(USER_SCHEMA_ID_START),
            next_gc_task_id: AtomicU64::new(1),
        }
    }

    /// Bootstrap the catalog by writing core system tables to storage.
    ///
    /// Call this only for fresh databases where no metadata exists.
    pub async fn bootstrap(&self) -> Result<()> {
        bootstrap::bootstrap_core_tables(self.txn_service.as_ref()).await?;
        self.load_schema_version()
    }

    /// Check if the catalog has been bootstrapped.
    pub async fn is_bootstrapped(&self) -> Result<bool> {
        bootstrap::is_bootstrapped(self.txn_service.as_ref()).await
    }

    /// Load schema version, counters, and cache from inner tables.
    ///
    /// Call this after opening an existing database to recover state.
    pub fn load_schema_version(&self) -> Result<()> {
        let (cache, counters) = catalog_loader::load_catalog(self.txn_service.as_ref())?;

        let mut state = self.schema_state.write();
        state.version = counters.schema_version;
        state.cache = cache;

        self.next_table_id
            .store(counters.next_table_id, Ordering::SeqCst);
        self.next_index_id
            .store(counters.next_index_id, Ordering::SeqCst);
        self.next_schema_id
            .store(counters.next_schema_id, Ordering::SeqCst);
        self.next_gc_task_id
            .store(counters.next_gc_task_id, Ordering::SeqCst);

        Ok(())
    }

    /// Write schema version to `__all_meta` inner table.
    /// Called during DDL txn work (no cache lock held).
    async fn write_schema_version_to_meta(&self, ctx: &mut TxnCtx, new_version: u64) -> Result<()> {
        update_meta_row(
            ctx,
            self.txn_service.as_ref(),
            META_SCHEMA_VERSION,
            new_version,
        )
        .await
    }

    fn begin_internal(&self) -> Result<TxnCtx> {
        self.txn_service.begin(false)
    }

    async fn commit_internal(&self, ctx: TxnCtx) -> Result<crate::transaction::CommitInfo> {
        self.txn_service.commit(ctx).await
    }
}

impl<T: TxnService> Catalog for MvccCatalog<T> {
    async fn create_schema(&self, name: &str) -> Result<()> {
        let _ddl = self.ddl_mutex.lock().await;

        // Phase 1: Validate + extract data (brief read lock)
        let (schema_id, new_version) = {
            let state = self.schema_state.read();
            if state.cache.schemas.contains_key(name) {
                return Err(TiSqlError::Catalog(format!(
                    "Schema '{name}' already exists"
                )));
            }
            let schema_id = self.next_schema_id.fetch_add(1, Ordering::SeqCst);
            (schema_id, state.version + 1)
        };

        // Phase 2: Async txn work (no parking_lot lock held)
        let mut ctx = self.begin_internal()?;

        let key = encode_record_key_with_handle(ALL_SCHEMA_TABLE_ID, schema_id as i64);
        let col_ids = &[0, 1];
        let values = &[
            Value::BigInt(schema_id as i64),
            Value::String(name.to_string()),
        ];
        let row_data = encode_row(col_ids, values);
        self.txn_service.put(&mut ctx, key, row_data).await?;

        let current_next = self.next_schema_id.load(Ordering::SeqCst);
        update_meta_row(
            &mut ctx,
            self.txn_service.as_ref(),
            META_NEXT_SCHEMA_ID,
            current_next,
        )
        .await?;

        self.write_schema_version_to_meta(&mut ctx, new_version)
            .await?;
        self.commit_internal(ctx).await?;

        // Phase 3: Update cache (brief write lock)
        let mut state = self.schema_state.write();
        state.version = new_version;
        state.cache.schemas.insert(name.to_string(), schema_id);
        state.cache.schema_names.insert(schema_id, name.to_string());

        Ok(())
    }

    async fn drop_schema(&self, name: &str) -> Result<()> {
        if name == "default" {
            return Err(TiSqlError::Catalog(
                "Cannot drop default schema".to_string(),
            ));
        }

        let _ddl = self.ddl_mutex.lock().await;

        // Phase 1: Validate (brief read lock)
        let (schema_id, new_version) = {
            let state = self.schema_state.read();
            let schema_id = match state.cache.schemas.get(name) {
                Some(&id) => id,
                None => return Err(TiSqlError::Catalog(format!("Schema '{name}' not found"))),
            };
            let has_tables = state.cache.tables.keys().any(|(s, _)| s == name);
            if has_tables {
                return Err(TiSqlError::Catalog(format!(
                    "Schema '{name}' is not empty, drop tables first"
                )));
            }
            (schema_id, state.version + 1)
        };

        // Phase 2: Async txn work
        let mut ctx = self.begin_internal()?;

        let key = encode_record_key_with_handle(ALL_SCHEMA_TABLE_ID, schema_id as i64);
        self.txn_service.delete(&mut ctx, key).await?;

        self.write_schema_version_to_meta(&mut ctx, new_version)
            .await?;
        self.commit_internal(ctx).await?;

        // Phase 3: Update cache
        let mut state = self.schema_state.write();
        state.version = new_version;
        state.cache.schemas.remove(name);
        state.cache.schema_names.remove(&schema_id);

        Ok(())
    }

    fn list_schemas(&self) -> Result<Vec<String>> {
        let state = self.schema_state.read();
        Ok(state
            .cache
            .schemas
            .keys()
            .filter(|name| !name.starts_with("__"))
            .cloned()
            .collect())
    }

    fn schema_exists(&self, name: &str) -> Result<bool> {
        let state = self.schema_state.read();
        Ok(state.cache.schemas.contains_key(name))
    }

    async fn create_table(&self, table: TableDef) -> Result<TableId> {
        let _ddl = self.ddl_mutex.lock().await;

        // Phase 1: Validate (brief read lock)
        let (schema_id, new_version) = {
            let state = self.schema_state.read();
            let key = (table.schema().to_string(), table.name().to_string());
            if state.cache.tables.contains_key(&key) {
                return Err(TiSqlError::Catalog(format!(
                    "Table '{}' already exists in schema '{}'",
                    table.name(),
                    table.schema()
                )));
            }
            let schema_id = match state.cache.schemas.get(table.schema()) {
                Some(&id) => id,
                None => {
                    return Err(TiSqlError::Catalog(format!(
                        "Schema '{}' not found",
                        table.schema()
                    )));
                }
            };
            (schema_id, state.version + 1)
        };

        let table_id = table.id();

        // Phase 2: Async txn work
        let mut ctx = self.begin_internal()?;

        write_user_table_row(&mut ctx, self.txn_service.as_ref(), &table, schema_id).await?;
        write_user_column_rows(
            &mut ctx,
            self.txn_service.as_ref(),
            table_id,
            table.columns(),
        )
        .await?;

        let current_next = self.next_table_id.load(Ordering::SeqCst);
        update_meta_row(
            &mut ctx,
            self.txn_service.as_ref(),
            META_NEXT_TABLE_ID,
            current_next,
        )
        .await?;

        self.write_schema_version_to_meta(&mut ctx, new_version)
            .await?;
        self.commit_internal(ctx).await?;

        // Phase 3: Update cache
        let mut state = self.schema_state.write();
        state.version = new_version;
        let key = (table.schema().to_string(), table.name().to_string());
        state.cache.table_id_map.insert(table_id, key.clone());
        state.cache.tables.insert(key, table);

        Ok(table_id)
    }

    async fn drop_table(&self, schema: &str, table: &str) -> Result<super::DropTableInfo> {
        let _ddl = self.ddl_mutex.lock().await;

        // Phase 1: Validate + extract data (brief read lock)
        let (table_def, new_version, start_key_hex, end_key_hex, gc_task_id) = {
            let state = self.schema_state.read();
            let key = (schema.to_string(), table.to_string());
            let table_def = match state.cache.tables.get(&key) {
                Some(t) => t.clone(),
                None => {
                    return Err(TiSqlError::TableNotFound(format!("{schema}.{table}")));
                }
            };

            let table_id = table_def.id();
            let start_key = gen_table_record_prefix(table_id);
            let mut end_key = start_key.clone();
            if let Some(last) = end_key.last_mut() {
                *last = last.saturating_add(1);
            }
            let start_key_hex = start_key
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            let end_key_hex = end_key
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();

            let gc_task_id = self.next_gc_task_id.fetch_add(1, Ordering::SeqCst) as i64;
            (
                table_def,
                state.version + 1,
                start_key_hex,
                end_key_hex,
                gc_task_id,
            )
        };

        let table_id = table_def.id();

        // Phase 2: Async txn work
        let mut ctx = self.begin_internal()?;

        let table_key = encode_record_key_with_handle(ALL_TABLE_TABLE_ID, table_id as i64);
        self.txn_service.delete(&mut ctx, table_key).await?;

        delete_column_rows(
            &mut ctx,
            self.txn_service.as_ref(),
            table_id,
            table_def.columns(),
        )
        .await?;

        delete_index_rows(
            &mut ctx,
            self.txn_service.as_ref(),
            table_id,
            table_def.indexes(),
        )
        .await?;

        write_gc_task_row(
            &mut ctx,
            self.txn_service.as_ref(),
            gc_task_id,
            table_id,
            &start_key_hex,
            &end_key_hex,
            0,
            "pending",
        )
        .await?;

        let current_next = self.next_gc_task_id.load(Ordering::SeqCst);
        update_meta_row(
            &mut ctx,
            self.txn_service.as_ref(),
            META_NEXT_GC_TASK_ID,
            current_next,
        )
        .await?;

        self.write_schema_version_to_meta(&mut ctx, new_version)
            .await?;

        let commit_info = self.commit_internal(ctx).await?;
        let commit_ts = commit_info.commit_ts;

        // Update GC task with the real drop_commit_ts in a new auto-commit txn
        {
            let mut ctx2 = self.begin_internal()?;
            write_gc_task_row(
                &mut ctx2,
                self.txn_service.as_ref(),
                gc_task_id,
                table_id,
                &start_key_hex,
                &end_key_hex,
                commit_ts,
                "pending",
            )
            .await?;
            self.commit_internal(ctx2).await?;
        }

        // Phase 3: Update cache
        let mut state = self.schema_state.write();
        state.version = new_version;
        let key = (schema.to_string(), table.to_string());
        state.cache.tables.remove(&key);
        state.cache.table_id_map.remove(&table_id);

        Ok(super::DropTableInfo {
            table_id,
            commit_ts,
        })
    }

    fn get_table(&self, schema: &str, table: &str) -> Result<Option<TableDef>> {
        let state = self.schema_state.read();
        let key = (schema.to_string(), table.to_string());
        Ok(state.cache.tables.get(&key).cloned())
    }

    fn get_table_by_id(&self, id: TableId) -> Result<Option<TableDef>> {
        let state = self.schema_state.read();
        match state.cache.table_id_map.get(&id) {
            Some(key) => Ok(state.cache.tables.get(key).cloned()),
            None => Ok(None),
        }
    }

    fn list_tables(&self, schema: &str) -> Result<Vec<TableDef>> {
        let state = self.schema_state.read();
        Ok(state
            .cache
            .tables
            .iter()
            .filter(|((s, _), _)| s == schema)
            .map(|(_, t)| t.clone())
            .collect())
    }

    async fn create_index(&self, table_id: TableId, index: IndexDef) -> Result<IndexId> {
        let _ddl = self.ddl_mutex.lock().await;

        // Phase 1: Validate (brief read lock)
        let (table_key, new_version) = {
            let state = self.schema_state.read();
            let table_key = match state.cache.table_id_map.get(&table_id) {
                Some(k) => k.clone(),
                None => {
                    return Err(TiSqlError::Catalog(format!(
                        "Table with ID {table_id} not found"
                    )));
                }
            };
            let table_def = state.cache.tables.get(&table_key).ok_or_else(|| {
                TiSqlError::Catalog(format!("Table with ID {table_id} not found"))
            })?;
            if table_def.indexes().iter().any(|i| i.name() == index.name()) {
                return Err(TiSqlError::Catalog(format!(
                    "Index '{}' already exists",
                    index.name()
                )));
            }
            (table_key, state.version + 1)
        };

        let index_id = index.id();

        // Phase 2: Async txn work
        let mut ctx = self.begin_internal()?;

        write_index_row(&mut ctx, self.txn_service.as_ref(), table_id, &index).await?;

        let current_next = self.next_index_id.load(Ordering::SeqCst);
        update_meta_row(
            &mut ctx,
            self.txn_service.as_ref(),
            META_NEXT_INDEX_ID,
            current_next,
        )
        .await?;

        self.write_schema_version_to_meta(&mut ctx, new_version)
            .await?;
        self.commit_internal(ctx).await?;

        // Phase 3: Update cache
        let mut state = self.schema_state.write();
        state.version = new_version;
        if let Some(cached_table) = state.cache.tables.get_mut(&table_key) {
            cached_table.add_index(index);
        }

        Ok(index_id)
    }

    async fn drop_index(&self, table_id: TableId, index_name: &str) -> Result<()> {
        let _ddl = self.ddl_mutex.lock().await;

        // Phase 1: Validate (brief read lock)
        let (table_key, index_key, new_version) = {
            let state = self.schema_state.read();
            let table_key = match state.cache.table_id_map.get(&table_id) {
                Some(k) => k.clone(),
                None => {
                    return Err(TiSqlError::Catalog(format!(
                        "Table with ID {table_id} not found"
                    )));
                }
            };
            let table_def = state.cache.tables.get(&table_key).ok_or_else(|| {
                TiSqlError::Catalog(format!("Table with ID {table_id} not found"))
            })?;
            let index = table_def
                .indexes()
                .iter()
                .find(|i| i.name() == index_name)
                .ok_or_else(|| TiSqlError::Catalog(format!("Index '{index_name}' not found")))?;
            let index_key = table_id * 10000 + index.id();
            (table_key, index_key, state.version + 1)
        };

        // Phase 2: Async txn work
        let mut ctx = self.begin_internal()?;

        let key = encode_record_key_with_handle(ALL_INDEX_TABLE_ID, index_key as i64);
        self.txn_service.delete(&mut ctx, key).await?;

        self.write_schema_version_to_meta(&mut ctx, new_version)
            .await?;
        self.commit_internal(ctx).await?;

        // Phase 3: Update cache
        let mut state = self.schema_state.write();
        state.version = new_version;
        if let Some(cached_table) = state.cache.tables.get_mut(&table_key) {
            cached_table.remove_index(index_name);
        }

        Ok(())
    }

    fn next_auto_increment(&self, table_id: TableId) -> Result<u64> {
        let mut state = self.schema_state.write();

        let table_key = match state.cache.table_id_map.get(&table_id) {
            Some(k) => k.clone(),
            None => {
                return Err(TiSqlError::Catalog(format!(
                    "Table with ID {table_id} not found"
                )));
            }
        };

        let schema_name = table_key.0.clone();
        let schema_id = match state.cache.schemas.get(&schema_name) {
            Some(&id) => id,
            None => {
                return Err(TiSqlError::Catalog(format!(
                    "Schema '{schema_name}' not found"
                )));
            }
        };

        let table_def =
            state.cache.tables.get_mut(&table_key).ok_or_else(|| {
                TiSqlError::Catalog(format!("Table with ID {table_id} not found"))
            })?;

        let new_id = table_def.increment_auto_id();

        // Persist: use block_on_sync since this is a sync method
        let mut ctx = self.begin_internal()?;
        crate::io::block_on_sync(write_user_table_row(
            &mut ctx,
            self.txn_service.as_ref(),
            table_def,
            schema_id,
        ))?;
        crate::io::block_on_sync(self.commit_internal(ctx))?;

        Ok(new_id)
    }

    fn next_table_id(&self) -> Result<TableId> {
        Ok(self.next_table_id.fetch_add(1, Ordering::SeqCst))
    }

    fn next_index_id(&self) -> Result<IndexId> {
        Ok(self.next_index_id.fetch_add(1, Ordering::SeqCst))
    }

    // MVCC-aware reads (bypass cache, scan inner tables at timestamp)

    fn get_table_at(&self, schema: &str, table: &str, ts: Timestamp) -> Result<Option<TableDef>> {
        catalog_loader::get_table_at(self.txn_service.as_ref(), schema, table, ts)
    }

    fn get_table_by_id_at(&self, id: TableId, ts: Timestamp) -> Result<Option<TableDef>> {
        catalog_loader::get_table_by_id_at(self.txn_service.as_ref(), id, ts)
    }

    fn list_tables_at(&self, schema: &str, ts: Timestamp) -> Result<Vec<TableDef>> {
        let state = self.schema_state.read();
        let schema_id = match state.cache.schemas.get(schema) {
            Some(&id) => id,
            None => return Ok(Vec::new()),
        };
        drop(state);
        catalog_loader::scan_tables_at(self.txn_service.as_ref(), schema_id, ts)
    }

    fn schema_exists_at(&self, name: &str, ts: Timestamp) -> Result<bool> {
        catalog_loader::schema_exists_at(self.txn_service.as_ref(), name, ts)
    }

    fn current_schema_version(&self) -> u64 {
        let state = self.schema_state.read();
        state.version
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ColumnDef;
    use crate::clog::{FileClogConfig, FileClogService};
    use crate::storage::MemTableEngine;
    use crate::transaction::{ConcurrencyManager, TransactionService};
    use crate::tso::LocalTso;
    use crate::types::DataType;
    use std::sync::Arc;
    use tempfile::tempdir;

    type TestTxnService = TransactionService<MemTableEngine, FileClogService, LocalTso>;

    fn make_test_io() -> Arc<crate::io::IoService> {
        crate::io::IoService::new_for_test(32).unwrap()
    }

    fn create_test_catalog() -> (MvccCatalog<TestTxnService>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let clog_config = FileClogConfig::with_dir(dir.path());
        let io_handle = tokio::runtime::Handle::current();
        let (clog_service, _) =
            FileClogService::recover(clog_config, make_test_io(), &io_handle).unwrap();
        let clog_service = Arc::new(clog_service);

        let tso = Arc::new(LocalTso::new(1));
        let concurrency_manager = Arc::new(ConcurrencyManager::new(0));
        let storage = Arc::new(MemTableEngine::new());

        let txn_service = Arc::new(TransactionService::new(
            Arc::clone(&storage),
            Arc::clone(&clog_service),
            Arc::clone(&tso),
            Arc::clone(&concurrency_manager),
        ));

        let catalog = MvccCatalog::new(txn_service);
        (catalog, dir)
    }

    async fn create_test_catalog_bootstrapped() -> (MvccCatalog<TestTxnService>, tempfile::TempDir)
    {
        let (catalog, dir) = create_test_catalog();
        catalog.bootstrap().await.unwrap();
        (catalog, dir)
    }

    fn make_test_table(catalog: &MvccCatalog<TestTxnService>, name: &str) -> TableDef {
        let table_id = catalog.next_table_id().unwrap();
        TableDef::new(
            table_id,
            name.to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(0, "id".to_string(), DataType::Int, false, None, true),
                ColumnDef::new(
                    1,
                    "name".to_string(),
                    DataType::Varchar(255),
                    true,
                    None,
                    false,
                ),
            ],
            vec![0],
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_create_get_table() {
        let (catalog, _dir) = create_test_catalog_bootstrapped().await;
        let table = make_test_table(&catalog, "users");
        let table_id = table.id();

        catalog.create_table(table).await.unwrap();

        let retrieved = catalog.get_table("default", "users").unwrap().unwrap();
        assert_eq!(retrieved.id(), table_id);
        assert_eq!(retrieved.name(), "users");

        let by_id = catalog.get_table_by_id(table_id).unwrap().unwrap();
        assert_eq!(by_id.name(), "users");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_drop_table() {
        let (catalog, _dir) = create_test_catalog_bootstrapped().await;
        let table = make_test_table(&catalog, "users");

        catalog.create_table(table).await.unwrap();
        assert!(catalog.get_table("default", "users").unwrap().is_some());

        catalog.drop_table("default", "users").await.unwrap();
        assert!(catalog.get_table("default", "users").unwrap().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_list_tables() {
        let (catalog, _dir) = create_test_catalog_bootstrapped().await;

        let t1 = make_test_table(&catalog, "users");
        let t2 = make_test_table(&catalog, "orders");

        catalog.create_table(t1).await.unwrap();
        catalog.create_table(t2).await.unwrap();

        let tables = catalog.list_tables("default").unwrap();
        assert_eq!(tables.len(), 2);

        let names: Vec<&str> = tables.iter().map(|t| t.name()).collect();
        assert!(names.contains(&"users"));
        assert!(names.contains(&"orders"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_schema_operations() {
        let (catalog, _dir) = create_test_catalog_bootstrapped().await;

        // Default and test schemas should exist after bootstrap
        assert!(catalog.schema_exists("default").unwrap());
        assert!(catalog.schema_exists("test").unwrap());

        // Create new schema
        catalog.create_schema("myschema").await.unwrap();
        assert!(catalog.schema_exists("myschema").unwrap());

        // List schemas (should not include __tisql_inner)
        let schemas = catalog.list_schemas().unwrap();
        assert!(schemas.contains(&"default".to_string()));
        assert!(schemas.contains(&"test".to_string()));
        assert!(schemas.contains(&"myschema".to_string()));
        assert!(!schemas.contains(&INNER_SCHEMA.to_string()));

        // Drop schema
        catalog.drop_schema("myschema").await.unwrap();
        assert!(!catalog.schema_exists("myschema").unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_auto_increment() {
        let (catalog, _dir) = create_test_catalog_bootstrapped().await;
        let table = make_test_table(&catalog, "users");
        let table_id = table.id();

        catalog.create_table(table).await.unwrap();

        assert_eq!(catalog.next_auto_increment(table_id).unwrap(), 1);
        assert_eq!(catalog.next_auto_increment(table_id).unwrap(), 2);
        assert_eq!(catalog.next_auto_increment(table_id).unwrap(), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_table_id_generation() {
        let (catalog, _dir) = create_test_catalog_bootstrapped().await;

        let id1 = catalog.next_table_id().unwrap();
        let id2 = catalog.next_table_id().unwrap();
        let id3 = catalog.next_table_id().unwrap();

        // User IDs start at USER_TABLE_ID_START
        assert_eq!(id1, USER_TABLE_ID_START);
        assert_eq!(id2, USER_TABLE_ID_START + 1);
        assert_eq!(id3, USER_TABLE_ID_START + 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_duplicate_table_error() {
        let (catalog, _dir) = create_test_catalog_bootstrapped().await;
        let table = make_test_table(&catalog, "users");

        catalog.create_table(table).await.unwrap();

        let table2 = make_test_table(&catalog, "users");
        let result = catalog.create_table(table2).await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_schema_version_increments_on_ddl() {
        let (catalog, _dir) = create_test_catalog_bootstrapped().await;

        // After bootstrap, version should be > 0
        let initial_version = catalog.current_schema_version();
        assert!(
            initial_version > 0,
            "Initial version should be > 0 after bootstrap"
        );

        // Create a table - version should increment
        let table = make_test_table(&catalog, "users");
        catalog.create_table(table).await.unwrap();
        let after_create = catalog.current_schema_version();
        assert_eq!(
            after_create,
            initial_version + 1,
            "Version should increment after create_table"
        );

        // Drop a table - version should increment again
        catalog.drop_table("default", "users").await.unwrap();
        let after_drop = catalog.current_schema_version();
        assert_eq!(
            after_drop,
            after_create + 1,
            "Version should increment after drop_table"
        );

        // Create schema - version should increment
        catalog.create_schema("test_schema").await.unwrap();
        let after_schema = catalog.current_schema_version();
        assert_eq!(
            after_schema,
            after_drop + 1,
            "Version should increment after create_schema"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_schema_version_persists_across_reload() {
        let dir = tempdir().unwrap();
        let initial_version;
        let io_handle = tokio::runtime::Handle::current();

        // First session: create catalog and tables
        {
            let clog_config = FileClogConfig::with_dir(dir.path());
            let (clog_service, _) =
                FileClogService::recover(clog_config, make_test_io(), &io_handle).unwrap();
            let clog_service = Arc::new(clog_service);

            let tso = Arc::new(LocalTso::new(1));
            let concurrency_manager = Arc::new(ConcurrencyManager::new(0));
            let storage = Arc::new(MemTableEngine::new());

            let txn_service = Arc::new(TransactionService::new(
                Arc::clone(&storage),
                Arc::clone(&clog_service),
                Arc::clone(&tso),
                Arc::clone(&concurrency_manager),
            ));

            let catalog = MvccCatalog::new(txn_service);
            catalog.bootstrap().await.unwrap();

            // Create some tables to increment version
            let table = make_test_table(&catalog, "t1");
            catalog.create_table(table).await.unwrap();
            let table = make_test_table(&catalog, "t2");
            catalog.create_table(table).await.unwrap();

            initial_version = catalog.current_schema_version();
            assert!(
                initial_version > 2,
                "Version should be > 2 after creating tables"
            );
        }

        // Second session: reload and verify version
        {
            let clog_config = FileClogConfig::with_dir(dir.path());
            let (clog_service, entries) =
                FileClogService::recover(clog_config, make_test_io(), &io_handle).unwrap();
            let clog_service = Arc::new(clog_service);

            let tso = Arc::new(LocalTso::new(1));
            let concurrency_manager = Arc::new(ConcurrencyManager::new(0));
            let storage = Arc::new(MemTableEngine::new());

            let txn_service = Arc::new(TransactionService::new(
                Arc::clone(&storage),
                Arc::clone(&clog_service),
                Arc::clone(&tso),
                Arc::clone(&concurrency_manager),
            ));

            // Recover entries
            txn_service.recover(&entries).unwrap();

            let catalog = MvccCatalog::new(Arc::clone(&txn_service));
            catalog.load_schema_version().unwrap();

            let recovered_version = catalog.current_schema_version();
            assert_eq!(
                recovered_version, initial_version,
                "Schema version should be recovered from storage"
            );

            // Creating another table should continue from the recovered version
            let table = make_test_table(&catalog, "t3");
            catalog.create_table(table).await.unwrap();
            assert_eq!(
                catalog.current_schema_version(),
                initial_version + 1,
                "Version should increment from recovered value"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bootstrap_creates_inner_tables_in_cache() {
        let (catalog, _dir) = create_test_catalog_bootstrapped().await;

        // Inner tables should be in cache but hidden from list_schemas
        let schemas = catalog.list_schemas().unwrap();
        assert!(!schemas.contains(&INNER_SCHEMA.to_string()));

        // But the inner schema should exist
        assert!(catalog.schema_exists(INNER_SCHEMA).unwrap());

        // Core tables should be accessible via get_table
        let all_table = catalog.get_table(INNER_SCHEMA, "__all_table").unwrap();
        assert!(all_table.is_some());
        assert_eq!(all_table.unwrap().columns().len(), 5);
    }
}
