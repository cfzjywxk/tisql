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

//! MVCC-based persistent catalog implementation.
//!
//! This module provides a catalog implementation that persists schema metadata
//! using the same MVCC storage as user data. Schema metadata is stored with
//! 'm' prefix keys and recovered automatically via clog replay.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::codec::key::{
    encode_global_key, encode_schema_key, encode_table_id_key, encode_table_key, gen_table_prefix,
    META_PREFIX, META_SCHEMA,
};
use crate::error::{Result, TiSqlError};
use crate::transaction::{CommitInfo, TxnCtx, TxnScanIterator, TxnService};
use crate::types::{IndexId, TableId, Timestamp};

use super::{Catalog, IndexDef, TableDef};

/// Schema state protected by RwLock for DDL/DML concurrency control.
///
/// DDL operations hold write lock during commit (atomic version update).
/// DML operations hold read lock briefly to check version (no contention between DMLs).
struct SchemaState {
    /// Schema version, incremented on each DDL commit.
    version: u64,
}

/// MVCC-based catalog implementation.
///
/// Uses the same storage engine and transaction service as user data.
/// Schema metadata is stored with 'm' prefix keys and recovered
/// automatically via clog replay.
///
/// ## DDL/DML Concurrency Control
///
/// Uses RwLock to ensure atomicity between DDL commit and schema version update:
/// - DDL holds write lock during entire commit + version increment
/// - DML holds read lock briefly to check version at commit time
///
/// ## ID Generation
///
/// Uses atomic counters for table_id and index_id to avoid transaction conflicts
/// during concurrent DDL operations. IDs are persisted to storage for durability.
pub struct MvccCatalog<T: TxnService> {
    txn_service: Arc<T>,
    /// Schema state protected by RwLock for DDL/DML concurrency control.
    schema_state: RwLock<SchemaState>,
    /// Atomic counter for table IDs (no transaction conflicts).
    next_table_id: AtomicU64,
    /// Atomic counter for index IDs (no transaction conflicts).
    next_index_id: AtomicU64,
}

impl<T: TxnService> MvccCatalog<T> {
    /// Create a new MVCC catalog.
    ///
    /// For fresh databases, call `bootstrap()` after creation.
    /// For existing databases, call `load_schema_version()` to recover state.
    pub fn new(txn_service: Arc<T>) -> Self {
        Self {
            txn_service,
            schema_state: RwLock::new(SchemaState { version: 0 }),
            next_table_id: AtomicU64::new(1),
            next_index_id: AtomicU64::new(1),
        }
    }

    /// Bootstrap the catalog (create default schema, initialize counters).
    ///
    /// Call this only for fresh databases where no metadata exists.
    pub fn bootstrap(&self) -> Result<()> {
        // Hold write lock during bootstrap to ensure atomicity
        let mut state = self.schema_state.write().unwrap();

        // Initialize schema_version counter to 1
        let mut ctx = self.txn_service.begin(false)?;
        let key = encode_global_key("schema_version");
        self.txn_service
            .put(&mut ctx, key, 1u64.to_be_bytes().to_vec())?;

        // Initialize next_table_id counter to 1
        let key = encode_global_key("next_table_id");
        self.txn_service
            .put(&mut ctx, key, 1u64.to_be_bytes().to_vec())?;

        // Initialize next_index_id counter to 1
        let key = encode_global_key("next_index_id");
        self.txn_service
            .put(&mut ctx, key, 1u64.to_be_bytes().to_vec())?;

        self.txn_service.commit(ctx)?;

        // Initialize atomic counters
        self.next_table_id.store(1, Ordering::SeqCst);
        self.next_index_id.store(1, Ordering::SeqCst);

        // Create default schema (this will increment version to 2)
        // Release lock first since create_schema needs it
        drop(state);
        self.create_schema("default")?;

        // Create test schema for MySQL compatibility (e.g., E2E tests expect it)
        self.create_schema("test")?;

        // Reload the version after bootstrap
        state = self.schema_state.write().unwrap();
        state.version = self.load_schema_version_from_storage()?;

        Ok(())
    }

    /// Check if the catalog has been bootstrapped (i.e., default schema exists).
    pub fn is_bootstrapped(&self) -> Result<bool> {
        self.schema_exists("default")
    }

    /// Load schema version and ID counters from storage (for recovery).
    ///
    /// Call this after opening an existing database to recover state.
    pub fn load_schema_version(&self) -> Result<()> {
        let version = self.load_schema_version_from_storage()?;
        let mut state = self.schema_state.write().unwrap();
        state.version = version;

        // Load ID counters
        let table_id = self.load_counter_from_storage("next_table_id")?;
        let index_id = self.load_counter_from_storage("next_index_id")?;
        self.next_table_id.store(table_id, Ordering::SeqCst);
        self.next_index_id.store(index_id, Ordering::SeqCst);

        Ok(())
    }

    /// Read schema version from storage.
    fn load_schema_version_from_storage(&self) -> Result<u64> {
        let ctx = self.txn_service.begin(true)?;
        let key = encode_global_key("schema_version");
        match self.txn_service.get(&ctx, &key)? {
            Some(v) => {
                if v.len() != 8 {
                    return Err(TiSqlError::Catalog(
                        "Invalid schema_version value".to_string(),
                    ));
                }
                Ok(u64::from_be_bytes(v.try_into().unwrap()))
            }
            None => Ok(0), // Not bootstrapped yet
        }
    }

    /// Read a counter value from storage.
    fn load_counter_from_storage(&self, name: &str) -> Result<u64> {
        let ctx = self.txn_service.begin(true)?;
        let key = encode_global_key(name);
        match self.txn_service.get(&ctx, &key)? {
            Some(v) => {
                if v.len() != 8 {
                    return Err(TiSqlError::Catalog(format!(
                        "Invalid counter value for '{name}'"
                    )));
                }
                Ok(u64::from_be_bytes(v.try_into().unwrap()))
            }
            None => Ok(1), // Default to 1 if not found
        }
    }

    /// Increment schema version in storage and memory.
    /// Must be called while holding write lock on schema_state.
    fn increment_schema_version(&self, ctx: &mut TxnCtx, state: &mut SchemaState) -> Result<()> {
        state.version += 1;
        let key = encode_global_key("schema_version");
        self.txn_service
            .put(ctx, key, state.version.to_be_bytes().to_vec())
    }

    /// Persist the current table ID counter to storage.
    /// Called as part of DDL transaction to ensure durability.
    fn persist_table_id_counter(&self, ctx: &mut TxnCtx, value: u64) -> Result<()> {
        let key = encode_global_key("next_table_id");
        self.txn_service.put(ctx, key, value.to_be_bytes().to_vec())
    }

    /// Persist the current index ID counter to storage.
    /// Called as part of DDL transaction to ensure durability.
    fn persist_index_id_counter(&self, ctx: &mut TxnCtx, value: u64) -> Result<()> {
        let key = encode_global_key("next_index_id");
        self.txn_service.put(ctx, key, value.to_be_bytes().to_vec())
    }

    /// Begin an internal transaction for DDL operations.
    fn begin_internal(&self) -> Result<TxnCtx> {
        self.txn_service.begin(false)
    }

    /// Commit an internal transaction.
    fn commit_internal(&self, ctx: TxnCtx) -> Result<CommitInfo> {
        self.txn_service.commit(ctx)
    }

    /// Read a value at a specific timestamp.
    fn get_at(&self, key: &[u8], ts: Timestamp) -> Result<Option<Vec<u8>>> {
        // Create a read-only context at the specified timestamp
        let ctx = TxnCtx::new(0, ts, true);
        self.txn_service.get(&ctx, key)
    }
}

impl<T: TxnService> Catalog for MvccCatalog<T> {
    fn create_schema(&self, name: &str) -> Result<()> {
        // Hold write lock during entire DDL commit for atomicity
        let mut state = self.schema_state.write().unwrap();

        let mut ctx = self.begin_internal()?;
        let key = encode_schema_key(name);

        // Check if schema already exists
        if self.txn_service.get(&ctx, &key)?.is_some() {
            return Err(TiSqlError::Catalog(format!(
                "Schema '{name}' already exists"
            )));
        }

        // Write schema marker (just a flag, empty value is fine)
        self.txn_service.put(&mut ctx, key, vec![1])?;

        // Increment schema version as part of this transaction
        self.increment_schema_version(&mut ctx, &mut state)?;

        self.commit_internal(ctx)?;
        // Write lock released here - DML can now see new version
        Ok(())
    }

    fn drop_schema(&self, name: &str) -> Result<()> {
        if name == "default" {
            return Err(TiSqlError::Catalog(
                "Cannot drop default schema".to_string(),
            ));
        }

        // Hold write lock during entire DDL commit for atomicity
        let mut state = self.schema_state.write().unwrap();

        let mut ctx = self.begin_internal()?;
        let key = encode_schema_key(name);

        // Check if schema exists
        if self.txn_service.get(&ctx, &key)?.is_none() {
            return Err(TiSqlError::Catalog(format!("Schema '{name}' not found")));
        }

        // Check if schema has tables (need to release lock temporarily)
        drop(state);
        let tables = self.list_tables(name)?;
        if !tables.is_empty() {
            return Err(TiSqlError::Catalog(format!(
                "Schema '{name}' is not empty, drop tables first"
            )));
        }
        // Re-acquire write lock
        state = self.schema_state.write().unwrap();

        // Delete schema marker
        self.txn_service.delete(&mut ctx, key)?;

        // Increment schema version as part of this transaction
        self.increment_schema_version(&mut ctx, &mut state)?;

        self.commit_internal(ctx)?;
        Ok(())
    }

    fn list_schemas(&self) -> Result<Vec<String>> {
        let ctx = self.txn_service.begin(true)?;

        // Scan for schema keys: 'm' + META_SCHEMA + name
        let start = vec![META_PREFIX, META_SCHEMA];
        let end = vec![META_PREFIX, META_SCHEMA + 1];

        let mut iter = self.txn_service.scan_iter(&ctx, start..end)?;
        let mut schemas: Vec<String> = Vec::new();
        iter.advance()?;
        while iter.valid() {
            let key = iter.user_key();
            if key.len() > 2 && key[0] == META_PREFIX && key[1] == META_SCHEMA {
                if let Ok(name) = String::from_utf8(key[2..].to_vec()) {
                    schemas.push(name);
                }
            }
            iter.advance()?;
        }

        Ok(schemas)
    }

    fn schema_exists(&self, name: &str) -> Result<bool> {
        let ctx = self.txn_service.begin(true)?;
        let key = encode_schema_key(name);
        Ok(self.txn_service.get(&ctx, &key)?.is_some())
    }

    fn create_table(&self, table: TableDef) -> Result<TableId> {
        // Hold write lock during entire DDL commit for atomicity
        let mut state = self.schema_state.write().unwrap();

        let mut ctx = self.begin_internal()?;

        // Check if table already exists
        let key = encode_table_key(table.schema(), table.name());
        if self.txn_service.get(&ctx, &key)?.is_some() {
            return Err(TiSqlError::Catalog(format!(
                "Table '{}' already exists in schema '{}'",
                table.name(),
                table.schema()
            )));
        }

        // Check if schema exists
        let schema_key = encode_schema_key(table.schema());
        if self.txn_service.get(&ctx, &schema_key)?.is_none() {
            return Err(TiSqlError::Catalog(format!(
                "Schema '{}' not found",
                table.schema()
            )));
        }

        // Serialize and write table definition
        let table_id = table.id();
        let value = bincode::serialize(&table).map_err(|e| TiSqlError::Catalog(e.to_string()))?;
        self.txn_service.put(&mut ctx, key, value)?;

        // Write table ID mapping
        let id_key = encode_table_id_key(table_id);
        let id_value = format!("{}:{}", table.schema(), table.name());
        self.txn_service
            .put(&mut ctx, id_key, id_value.into_bytes())?;

        // Persist the current table ID counter for durability
        let current_next_id = self.next_table_id.load(Ordering::SeqCst);
        self.persist_table_id_counter(&mut ctx, current_next_id)?;

        // Increment schema version as part of this transaction
        self.increment_schema_version(&mut ctx, &mut state)?;

        self.commit_internal(ctx)?;
        Ok(table_id)
    }

    fn drop_table(&self, schema: &str, table: &str) -> Result<()> {
        // Hold write lock during entire DDL commit for atomicity
        let mut state = self.schema_state.write().unwrap();

        let mut ctx = self.begin_internal()?;
        let key = encode_table_key(schema, table);

        // Get table to find its ID
        let table_def = match self.txn_service.get(&ctx, &key)? {
            Some(v) => bincode::deserialize::<TableDef>(&v)
                .map_err(|e| TiSqlError::Catalog(e.to_string()))?,
            None => {
                return Err(TiSqlError::TableNotFound(format!("{schema}.{table}")));
            }
        };

        // Delete table definition
        self.txn_service.delete(&mut ctx, key)?;

        // Delete table ID mapping
        let id_key = encode_table_id_key(table_def.id());
        self.txn_service.delete(&mut ctx, id_key)?;

        // Increment schema version as part of this transaction
        self.increment_schema_version(&mut ctx, &mut state)?;

        self.commit_internal(ctx)?;
        Ok(())
    }

    fn get_table(&self, schema: &str, table: &str) -> Result<Option<TableDef>> {
        let ctx = self.txn_service.begin(true)?;
        let key = encode_table_key(schema, table);

        match self.txn_service.get(&ctx, &key)? {
            Some(v) => {
                let table_def = bincode::deserialize::<TableDef>(&v)
                    .map_err(|e| TiSqlError::Catalog(e.to_string()))?;
                Ok(Some(table_def))
            }
            None => Ok(None),
        }
    }

    fn get_table_by_id(&self, id: TableId) -> Result<Option<TableDef>> {
        let ctx = self.txn_service.begin(true)?;
        let id_key = encode_table_id_key(id);

        // First get schema:table from ID mapping
        let mapping = match self.txn_service.get(&ctx, &id_key)? {
            Some(v) => String::from_utf8(v).map_err(|_| {
                TiSqlError::Catalog("Invalid table ID mapping encoding".to_string())
            })?,
            None => return Ok(None),
        };

        // Parse schema:table
        let parts: Vec<&str> = mapping.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(TiSqlError::Catalog(
                "Invalid table ID mapping format".to_string(),
            ));
        }
        let (schema, table) = (parts[0], parts[1]);

        // Get table definition
        let table_key = encode_table_key(schema, table);
        match self.txn_service.get(&ctx, &table_key)? {
            Some(v) => {
                let table_def = bincode::deserialize::<TableDef>(&v)
                    .map_err(|e| TiSqlError::Catalog(e.to_string()))?;
                Ok(Some(table_def))
            }
            None => Ok(None),
        }
    }

    fn list_tables(&self, schema: &str) -> Result<Vec<TableDef>> {
        let ctx = self.txn_service.begin(true)?;

        // Generate key range for all tables in the schema
        let prefix = gen_table_prefix(schema);
        let mut end = prefix.clone();
        // Increment last byte to get exclusive end
        if let Some(last) = end.last_mut() {
            *last = last.saturating_add(1);
        }

        let mut iter = self.txn_service.scan_iter(&ctx, prefix.clone()..end)?;
        let mut tables = Vec::new();

        iter.advance()?;
        while iter.valid() {
            let key = iter.user_key();
            let value = iter.value();
            // Verify key starts with our prefix
            if key.starts_with(&prefix) {
                match bincode::deserialize::<TableDef>(value) {
                    Ok(table_def) => tables.push(table_def),
                    Err(e) => {
                        return Err(TiSqlError::Catalog(format!(
                            "Failed to deserialize table: {e}"
                        )));
                    }
                }
            }
            iter.advance()?;
        }

        Ok(tables)
    }

    fn create_index(&self, table_id: TableId, index: IndexDef) -> Result<IndexId> {
        // Hold write lock during entire DDL commit for atomicity
        let mut state = self.schema_state.write().unwrap();

        let mut ctx = self.begin_internal()?;

        // Get table by ID
        let id_key = encode_table_id_key(table_id);
        let mapping = match self.txn_service.get(&ctx, &id_key)? {
            Some(v) => String::from_utf8(v).map_err(|_| {
                TiSqlError::Catalog("Invalid table ID mapping encoding".to_string())
            })?,
            None => {
                return Err(TiSqlError::Catalog(format!(
                    "Table with ID {table_id} not found"
                )))
            }
        };

        let parts: Vec<&str> = mapping.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(TiSqlError::Catalog(
                "Invalid table ID mapping format".to_string(),
            ));
        }
        let (schema, table) = (parts[0], parts[1]);

        // Get and update table definition
        let table_key = encode_table_key(schema, table);
        let mut table_def = match self.txn_service.get(&ctx, &table_key)? {
            Some(v) => bincode::deserialize::<TableDef>(&v)
                .map_err(|e| TiSqlError::Catalog(e.to_string()))?,
            None => {
                return Err(TiSqlError::Catalog(format!(
                    "Table with ID {table_id} not found"
                )))
            }
        };

        // Check for duplicate index name
        if table_def.indexes().iter().any(|i| i.name() == index.name()) {
            return Err(TiSqlError::Catalog(format!(
                "Index '{}' already exists",
                index.name()
            )));
        }

        let index_id = index.id();
        table_def.add_index(index);

        // Write updated table definition
        let value =
            bincode::serialize(&table_def).map_err(|e| TiSqlError::Catalog(e.to_string()))?;
        self.txn_service.put(&mut ctx, table_key, value)?;

        // Persist the current index ID counter for durability
        let current_next_id = self.next_index_id.load(Ordering::SeqCst);
        self.persist_index_id_counter(&mut ctx, current_next_id)?;

        // Increment schema version as part of this transaction
        self.increment_schema_version(&mut ctx, &mut state)?;

        self.commit_internal(ctx)?;
        Ok(index_id)
    }

    fn drop_index(&self, table_id: TableId, index_name: &str) -> Result<()> {
        // Hold write lock during entire DDL commit for atomicity
        let mut state = self.schema_state.write().unwrap();

        let mut ctx = self.begin_internal()?;

        // Get table by ID
        let id_key = encode_table_id_key(table_id);
        let mapping = match self.txn_service.get(&ctx, &id_key)? {
            Some(v) => String::from_utf8(v).map_err(|_| {
                TiSqlError::Catalog("Invalid table ID mapping encoding".to_string())
            })?,
            None => {
                return Err(TiSqlError::Catalog(format!(
                    "Table with ID {table_id} not found"
                )))
            }
        };

        let parts: Vec<&str> = mapping.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(TiSqlError::Catalog(
                "Invalid table ID mapping format".to_string(),
            ));
        }
        let (schema, table) = (parts[0], parts[1]);

        // Get and update table definition
        let table_key = encode_table_key(schema, table);
        let mut table_def = match self.txn_service.get(&ctx, &table_key)? {
            Some(v) => bincode::deserialize::<TableDef>(&v)
                .map_err(|e| TiSqlError::Catalog(e.to_string()))?,
            None => {
                return Err(TiSqlError::Catalog(format!(
                    "Table with ID {table_id} not found"
                )))
            }
        };

        // Remove index
        table_def
            .remove_index(index_name)
            .ok_or_else(|| TiSqlError::Catalog(format!("Index '{index_name}' not found")))?;

        // Write updated table definition
        let value =
            bincode::serialize(&table_def).map_err(|e| TiSqlError::Catalog(e.to_string()))?;
        self.txn_service.put(&mut ctx, table_key, value)?;

        // Increment schema version as part of this transaction
        self.increment_schema_version(&mut ctx, &mut state)?;

        self.commit_internal(ctx)?;
        Ok(())
    }

    fn next_auto_increment(&self, table_id: TableId) -> Result<u64> {
        let mut ctx = self.begin_internal()?;

        // Get table by ID
        let id_key = encode_table_id_key(table_id);
        let mapping = match self.txn_service.get(&ctx, &id_key)? {
            Some(v) => String::from_utf8(v).map_err(|_| {
                TiSqlError::Catalog("Invalid table ID mapping encoding".to_string())
            })?,
            None => {
                return Err(TiSqlError::Catalog(format!(
                    "Table with ID {table_id} not found"
                )))
            }
        };

        let parts: Vec<&str> = mapping.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(TiSqlError::Catalog(
                "Invalid table ID mapping format".to_string(),
            ));
        }
        let (schema, table) = (parts[0], parts[1]);

        // Get and update table definition
        let table_key = encode_table_key(schema, table);
        let mut table_def = match self.txn_service.get(&ctx, &table_key)? {
            Some(v) => bincode::deserialize::<TableDef>(&v)
                .map_err(|e| TiSqlError::Catalog(e.to_string()))?,
            None => {
                return Err(TiSqlError::Catalog(format!(
                    "Table with ID {table_id} not found"
                )))
            }
        };

        // Increment auto_increment_id
        let new_id = table_def.increment_auto_id();

        // Write updated table definition
        let value =
            bincode::serialize(&table_def).map_err(|e| TiSqlError::Catalog(e.to_string()))?;
        self.txn_service.put(&mut ctx, table_key, value)?;

        self.commit_internal(ctx)?;
        Ok(new_id)
    }

    fn next_table_id(&self) -> Result<TableId> {
        // Atomically allocate ID - no transaction conflict
        Ok(self.next_table_id.fetch_add(1, Ordering::SeqCst))
    }

    fn next_index_id(&self) -> Result<IndexId> {
        // Atomically allocate ID - no transaction conflict
        Ok(self.next_index_id.fetch_add(1, Ordering::SeqCst))
    }

    // MVCC-aware reads

    fn get_table_at(&self, schema: &str, table: &str, ts: Timestamp) -> Result<Option<TableDef>> {
        let key = encode_table_key(schema, table);

        match self.get_at(&key, ts)? {
            Some(v) => {
                let table_def = bincode::deserialize::<TableDef>(&v)
                    .map_err(|e| TiSqlError::Catalog(e.to_string()))?;
                Ok(Some(table_def))
            }
            None => Ok(None),
        }
    }

    fn get_table_by_id_at(&self, id: TableId, ts: Timestamp) -> Result<Option<TableDef>> {
        let id_key = encode_table_id_key(id);

        // First get schema:table from ID mapping
        let mapping = match self.get_at(&id_key, ts)? {
            Some(v) => String::from_utf8(v).map_err(|_| {
                TiSqlError::Catalog("Invalid table ID mapping encoding".to_string())
            })?,
            None => return Ok(None),
        };

        // Parse schema:table
        let parts: Vec<&str> = mapping.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(TiSqlError::Catalog(
                "Invalid table ID mapping format".to_string(),
            ));
        }
        let (schema, table) = (parts[0], parts[1]);

        // Get table definition at the same timestamp
        self.get_table_at(schema, table, ts)
    }

    fn list_tables_at(&self, schema: &str, ts: Timestamp) -> Result<Vec<TableDef>> {
        // For list_tables_at, we need to scan with a specific timestamp
        // This requires creating a TxnCtx at the specific timestamp
        let ctx = TxnCtx::new(0, ts, true);

        let prefix = gen_table_prefix(schema);
        let mut end = prefix.clone();
        if let Some(last) = end.last_mut() {
            *last = last.saturating_add(1);
        }

        let mut iter = self.txn_service.scan_iter(&ctx, prefix.clone()..end)?;
        let mut tables = Vec::new();

        iter.advance()?;
        while iter.valid() {
            let key = iter.user_key();
            let value = iter.value();
            if key.starts_with(&prefix) {
                match bincode::deserialize::<TableDef>(value) {
                    Ok(table_def) => tables.push(table_def),
                    Err(e) => {
                        return Err(TiSqlError::Catalog(format!(
                            "Failed to deserialize table: {e}"
                        )));
                    }
                }
            }
            iter.advance()?;
        }

        Ok(tables)
    }

    fn schema_exists_at(&self, name: &str, ts: Timestamp) -> Result<bool> {
        let key = encode_schema_key(name);
        Ok(self.get_at(&key, ts)?.is_some())
    }

    fn current_schema_version(&self) -> u64 {
        // Read lock - concurrent with other DMLs, blocked by DDL commit
        let state = self.schema_state.read().unwrap();
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

    fn create_test_catalog() -> (MvccCatalog<TestTxnService>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let clog_config = FileClogConfig::with_dir(dir.path());
        let (clog_service, _) = FileClogService::recover(clog_config).unwrap();
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
        catalog.bootstrap().unwrap();

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

    #[test]
    fn test_create_get_table() {
        let (catalog, _dir) = create_test_catalog();
        let table = make_test_table(&catalog, "users");
        let table_id = table.id();

        catalog.create_table(table).unwrap();

        let retrieved = catalog.get_table("default", "users").unwrap().unwrap();
        assert_eq!(retrieved.id(), table_id);
        assert_eq!(retrieved.name(), "users");

        let by_id = catalog.get_table_by_id(table_id).unwrap().unwrap();
        assert_eq!(by_id.name(), "users");
    }

    #[test]
    fn test_drop_table() {
        let (catalog, _dir) = create_test_catalog();
        let table = make_test_table(&catalog, "users");

        catalog.create_table(table).unwrap();
        assert!(catalog.get_table("default", "users").unwrap().is_some());

        catalog.drop_table("default", "users").unwrap();
        assert!(catalog.get_table("default", "users").unwrap().is_none());
    }

    #[test]
    fn test_list_tables() {
        let (catalog, _dir) = create_test_catalog();

        let t1 = make_test_table(&catalog, "users");
        let t2 = make_test_table(&catalog, "orders");

        catalog.create_table(t1).unwrap();
        catalog.create_table(t2).unwrap();

        let tables = catalog.list_tables("default").unwrap();
        assert_eq!(tables.len(), 2);

        let names: Vec<&str> = tables.iter().map(|t| t.name()).collect();
        assert!(names.contains(&"users"));
        assert!(names.contains(&"orders"));
    }

    #[test]
    fn test_schema_operations() {
        let (catalog, _dir) = create_test_catalog();

        // Default and test schemas should exist after bootstrap
        assert!(catalog.schema_exists("default").unwrap());
        assert!(catalog.schema_exists("test").unwrap());

        // Create new schema
        catalog.create_schema("myschema").unwrap();
        assert!(catalog.schema_exists("myschema").unwrap());

        // List schemas
        let schemas = catalog.list_schemas().unwrap();
        assert!(schemas.contains(&"default".to_string()));
        assert!(schemas.contains(&"test".to_string()));
        assert!(schemas.contains(&"myschema".to_string()));

        // Drop schema
        catalog.drop_schema("myschema").unwrap();
        assert!(!catalog.schema_exists("myschema").unwrap());
    }

    #[test]
    fn test_auto_increment() {
        let (catalog, _dir) = create_test_catalog();
        let table = make_test_table(&catalog, "users");
        let table_id = table.id();

        catalog.create_table(table).unwrap();

        assert_eq!(catalog.next_auto_increment(table_id).unwrap(), 1);
        assert_eq!(catalog.next_auto_increment(table_id).unwrap(), 2);
        assert_eq!(catalog.next_auto_increment(table_id).unwrap(), 3);
    }

    #[test]
    fn test_table_id_generation() {
        let (catalog, _dir) = create_test_catalog();

        let id1 = catalog.next_table_id().unwrap();
        let id2 = catalog.next_table_id().unwrap();
        let id3 = catalog.next_table_id().unwrap();

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }

    #[test]
    fn test_duplicate_table_error() {
        let (catalog, _dir) = create_test_catalog();
        let table = make_test_table(&catalog, "users");

        catalog.create_table(table).unwrap();

        let table2 = make_test_table(&catalog, "users");
        let result = catalog.create_table(table2);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_version_increments_on_ddl() {
        let (catalog, _dir) = create_test_catalog();

        // After bootstrap, version should be > 0
        let initial_version = catalog.current_schema_version();
        assert!(
            initial_version > 0,
            "Initial version should be > 0 after bootstrap"
        );

        // Create a table - version should increment
        let table = make_test_table(&catalog, "users");
        catalog.create_table(table).unwrap();
        let after_create = catalog.current_schema_version();
        assert_eq!(
            after_create,
            initial_version + 1,
            "Version should increment after create_table"
        );

        // Drop a table - version should increment again
        catalog.drop_table("default", "users").unwrap();
        let after_drop = catalog.current_schema_version();
        assert_eq!(
            after_drop,
            after_create + 1,
            "Version should increment after drop_table"
        );

        // Create schema - version should increment
        catalog.create_schema("test_schema").unwrap();
        let after_schema = catalog.current_schema_version();
        assert_eq!(
            after_schema,
            after_drop + 1,
            "Version should increment after create_schema"
        );
    }

    #[test]
    fn test_schema_version_persists_across_reload() {
        let dir = tempdir().unwrap();
        let initial_version;

        // First session: create catalog and tables
        {
            let clog_config = FileClogConfig::with_dir(dir.path());
            let (clog_service, _) = FileClogService::recover(clog_config).unwrap();
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
            catalog.bootstrap().unwrap();

            // Create some tables to increment version
            let table = make_test_table(&catalog, "t1");
            catalog.create_table(table).unwrap();
            let table = make_test_table(&catalog, "t2");
            catalog.create_table(table).unwrap();

            initial_version = catalog.current_schema_version();
            assert!(
                initial_version > 2,
                "Version should be > 2 after creating tables"
            );
        }

        // Second session: reload and verify version
        {
            let clog_config = FileClogConfig::with_dir(dir.path());
            let (clog_service, entries) = FileClogService::recover(clog_config).unwrap();
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
            catalog.create_table(table).unwrap();
            assert_eq!(
                catalog.current_schema_version(),
                initial_version + 1,
                "Version should increment from recovered value"
            );
        }
    }
}
