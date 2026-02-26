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

//! In-memory catalog implementation.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::catalog::types::{IndexId, TableId};
use crate::util::error::{Result, TiSqlError};

use super::{Catalog, IndexDef, TableDef};

/// In-memory catalog implementation.
///
/// This implementation stores all metadata in memory and is suitable for
/// testing and development. Data is lost when the process exits.
pub struct MemoryCatalog {
    /// Schema name -> set of table names
    schemas: RwLock<HashMap<String, HashMap<String, TableDef>>>,
    /// Table ID -> TableDef (for quick lookup by ID)
    tables_by_id: RwLock<HashMap<TableId, (String, String)>>, // (schema, table_name)
    /// ID generators
    next_table_id: AtomicU64,
    next_index_id: AtomicU64,
}

impl MemoryCatalog {
    pub fn new() -> Self {
        let catalog = Self {
            schemas: RwLock::new(HashMap::new()),
            tables_by_id: RwLock::new(HashMap::new()),
            next_table_id: AtomicU64::new(1),
            next_index_id: AtomicU64::new(1),
        };

        // Create default schema
        catalog.create_schema_sync("default").unwrap();

        // Create test schema for MySQL compatibility (e.g., E2E tests expect it)
        catalog.create_schema_sync("test").unwrap();

        catalog
    }

    /// Synchronous schema creation for use in constructor.
    fn create_schema_sync(&self, name: &str) -> Result<()> {
        let mut schemas = self.schemas.write();
        if schemas.contains_key(name) {
            return Err(TiSqlError::Catalog(format!(
                "Schema '{name}' already exists"
            )));
        }
        schemas.insert(name.to_string(), HashMap::new());
        Ok(())
    }
}

impl Default for MemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Catalog for MemoryCatalog {
    async fn create_schema(&self, name: &str) -> Result<()> {
        self.create_schema_sync(name)
    }

    async fn drop_schema(&self, name: &str) -> Result<()> {
        let mut schemas = self.schemas.write();
        let mut tables_by_id = self.tables_by_id.write();

        if let Some(tables) = schemas.remove(name) {
            // Remove all tables from tables_by_id
            for table in tables.values() {
                tables_by_id.remove(&table.id());
            }
            Ok(())
        } else {
            Err(TiSqlError::Catalog(format!("Schema '{name}' not found")))
        }
    }

    fn list_schemas(&self) -> Result<Vec<String>> {
        let schemas = self.schemas.read();
        Ok(schemas.keys().cloned().collect())
    }

    fn schema_exists(&self, name: &str) -> Result<bool> {
        let schemas = self.schemas.read();
        Ok(schemas.contains_key(name))
    }

    async fn create_table(&self, table: TableDef) -> Result<TableId> {
        let mut schemas = self.schemas.write();
        let mut tables_by_id = self.tables_by_id.write();

        let schema_tables = schemas
            .get_mut(table.schema())
            .ok_or_else(|| TiSqlError::Catalog(format!("Schema '{}' not found", table.schema())))?;

        if schema_tables.contains_key(table.name()) {
            return Err(TiSqlError::Catalog(format!(
                "Table '{}' already exists in schema '{}'",
                table.name(),
                table.schema()
            )));
        }

        let table_id = table.id();
        let schema_name = table.schema().to_string();
        let table_name = table.name().to_string();

        schema_tables.insert(table_name.clone(), table);
        tables_by_id.insert(table_id, (schema_name, table_name));

        Ok(table_id)
    }

    async fn drop_table(&self, schema: &str, table: &str) -> Result<super::DropTableInfo> {
        let mut schemas = self.schemas.write();
        let mut tables_by_id = self.tables_by_id.write();

        let schema_tables = schemas
            .get_mut(schema)
            .ok_or_else(|| TiSqlError::Catalog(format!("Schema '{schema}' not found")))?;

        if let Some(table_def) = schema_tables.remove(table) {
            let table_id = table_def.id();
            tables_by_id.remove(&table_id);
            Ok(super::DropTableInfo {
                table_id,
                commit_ts: 0,
            })
        } else {
            Err(TiSqlError::TableNotFound(format!("{schema}.{table}")))
        }
    }

    fn get_table(&self, schema: &str, table: &str) -> Result<Option<TableDef>> {
        let schemas = self.schemas.read();
        Ok(schemas
            .get(schema)
            .and_then(|tables| tables.get(table).cloned()))
    }

    fn get_table_by_id(&self, id: TableId) -> Result<Option<TableDef>> {
        let tables_by_id = self.tables_by_id.read();
        let schemas = self.schemas.read();

        if let Some((schema, table_name)) = tables_by_id.get(&id) {
            Ok(schemas
                .get(schema)
                .and_then(|tables| tables.get(table_name).cloned()))
        } else {
            Ok(None)
        }
    }

    fn list_tables(&self, schema: &str) -> Result<Vec<TableDef>> {
        let schemas = self.schemas.read();
        Ok(schemas
            .get(schema)
            .map(|tables| tables.values().cloned().collect())
            .unwrap_or_default())
    }

    async fn create_index(&self, table_id: TableId, index: IndexDef) -> Result<IndexId> {
        let tables_by_id = self.tables_by_id.read();
        let mut schemas = self.schemas.write();

        let (schema, table_name) = tables_by_id
            .get(&table_id)
            .ok_or_else(|| TiSqlError::Catalog(format!("Table with ID {table_id} not found")))?
            .clone();

        let table = schemas
            .get_mut(&schema)
            .and_then(|tables| tables.get_mut(&table_name))
            .ok_or_else(|| TiSqlError::Catalog("Table not found".into()))?;

        // Check for duplicate index name
        if table.indexes().iter().any(|i| i.name() == index.name()) {
            return Err(TiSqlError::Catalog(format!(
                "Index '{}' already exists",
                index.name()
            )));
        }

        let index_id = index.id();
        table.add_index(index);

        Ok(index_id)
    }

    async fn drop_index(&self, table_id: TableId, index_name: &str) -> Result<()> {
        let tables_by_id = self.tables_by_id.read();
        let mut schemas = self.schemas.write();

        let (schema, table_name) = tables_by_id
            .get(&table_id)
            .ok_or_else(|| TiSqlError::Catalog(format!("Table with ID {table_id} not found")))?
            .clone();

        let table = schemas
            .get_mut(&schema)
            .and_then(|tables| tables.get_mut(&table_name))
            .ok_or_else(|| TiSqlError::Catalog("Table not found".into()))?;

        table
            .remove_index(index_name)
            .ok_or_else(|| TiSqlError::Catalog(format!("Index '{index_name}' not found")))?;

        Ok(())
    }

    fn next_auto_increment(&self, table_id: TableId) -> Result<u64> {
        let tables_by_id = self.tables_by_id.read();
        let mut schemas = self.schemas.write();

        let (schema, table_name) = tables_by_id
            .get(&table_id)
            .ok_or_else(|| TiSqlError::Catalog(format!("Table with ID {table_id} not found")))?
            .clone();

        let table = schemas
            .get_mut(&schema)
            .and_then(|tables| tables.get_mut(&table_name))
            .ok_or_else(|| TiSqlError::Catalog("Table not found".into()))?;

        Ok(table.increment_auto_id())
    }

    fn observe_auto_increment_explicit(&self, table_id: TableId, explicit_v: u64) -> Result<()> {
        let tables_by_id = self.tables_by_id.read();
        let mut schemas = self.schemas.write();

        let (schema, table_name) = tables_by_id
            .get(&table_id)
            .ok_or_else(|| TiSqlError::Catalog(format!("Table with ID {table_id} not found")))?
            .clone();

        let table = schemas
            .get_mut(&schema)
            .and_then(|tables| tables.get_mut(&table_name))
            .ok_or_else(|| TiSqlError::Catalog("Table not found".into()))?;

        if explicit_v > table.auto_increment_id() {
            table.set_auto_increment_id(explicit_v);
        }
        Ok(())
    }

    fn next_table_id(&self) -> Result<TableId> {
        Ok(self.next_table_id.fetch_add(1, Ordering::SeqCst))
    }

    fn next_index_id(&self) -> Result<IndexId> {
        Ok(self.next_index_id.fetch_add(1, Ordering::SeqCst))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::types::DataType;
    use crate::catalog::ColumnDef;

    fn make_test_table(catalog: &MemoryCatalog, name: &str) -> TableDef {
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

    #[tokio::test]
    async fn test_create_get_table() {
        let catalog = MemoryCatalog::new();
        let table = make_test_table(&catalog, "users");
        let table_id = table.id();

        catalog.create_table(table).await.unwrap();

        let retrieved = catalog.get_table("default", "users").unwrap().unwrap();
        assert_eq!(retrieved.id(), table_id);
        assert_eq!(retrieved.name(), "users");

        let by_id = catalog.get_table_by_id(table_id).unwrap().unwrap();
        assert_eq!(by_id.name(), "users");
    }

    #[tokio::test]
    async fn test_drop_table() {
        let catalog = MemoryCatalog::new();
        let table = make_test_table(&catalog, "users");

        catalog.create_table(table).await.unwrap();
        assert!(catalog.get_table("default", "users").unwrap().is_some());

        catalog.drop_table("default", "users").await.unwrap();
        assert!(catalog.get_table("default", "users").unwrap().is_none());
    }

    #[tokio::test]
    async fn test_auto_increment() {
        let catalog = MemoryCatalog::new();
        let table = make_test_table(&catalog, "users");
        let table_id = table.id();

        catalog.create_table(table).await.unwrap();

        assert_eq!(catalog.next_auto_increment(table_id).unwrap(), 1);
        assert_eq!(catalog.next_auto_increment(table_id).unwrap(), 2);
        assert_eq!(catalog.next_auto_increment(table_id).unwrap(), 3);
    }

    #[tokio::test]
    async fn test_auto_increment_observe_explicit() {
        let catalog = MemoryCatalog::new();
        let table = make_test_table(&catalog, "users");
        let table_id = table.id();

        catalog.create_table(table).await.unwrap();

        assert_eq!(catalog.next_auto_increment(table_id).unwrap(), 1);
        catalog
            .observe_auto_increment_explicit(table_id, 100)
            .unwrap();
        assert_eq!(catalog.next_auto_increment(table_id).unwrap(), 101);
        catalog
            .observe_auto_increment_explicit(table_id, 50)
            .unwrap();
        assert_eq!(catalog.next_auto_increment(table_id).unwrap(), 102);
    }
}
