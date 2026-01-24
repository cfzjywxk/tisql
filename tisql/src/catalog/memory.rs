use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use crate::error::{Result, TiSqlError};
use crate::types::{IndexId, TableId};

use super::{Catalog, IndexDef, TableDef};

/// In-memory catalog implementation
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
        catalog.create_schema("default").unwrap();

        catalog
    }
}

impl Default for MemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Catalog for MemoryCatalog {
    fn create_schema(&self, name: &str) -> Result<()> {
        let mut schemas = self.schemas.write().unwrap();
        if schemas.contains_key(name) {
            return Err(TiSqlError::Catalog(format!(
                "Schema '{}' already exists",
                name
            )));
        }
        schemas.insert(name.to_string(), HashMap::new());
        Ok(())
    }

    fn drop_schema(&self, name: &str) -> Result<()> {
        let mut schemas = self.schemas.write().unwrap();
        let mut tables_by_id = self.tables_by_id.write().unwrap();

        if let Some(tables) = schemas.remove(name) {
            // Remove all tables from tables_by_id
            for table in tables.values() {
                tables_by_id.remove(&table.id);
            }
            Ok(())
        } else {
            Err(TiSqlError::Catalog(format!(
                "Schema '{}' not found",
                name
            )))
        }
    }

    fn list_schemas(&self) -> Result<Vec<String>> {
        let schemas = self.schemas.read().unwrap();
        Ok(schemas.keys().cloned().collect())
    }

    fn schema_exists(&self, name: &str) -> Result<bool> {
        let schemas = self.schemas.read().unwrap();
        Ok(schemas.contains_key(name))
    }

    fn create_table(&self, table: TableDef) -> Result<TableId> {
        let mut schemas = self.schemas.write().unwrap();
        let mut tables_by_id = self.tables_by_id.write().unwrap();

        let schema_tables = schemas
            .get_mut(&table.schema)
            .ok_or_else(|| TiSqlError::Catalog(format!("Schema '{}' not found", table.schema)))?;

        if schema_tables.contains_key(&table.name) {
            return Err(TiSqlError::Catalog(format!(
                "Table '{}' already exists in schema '{}'",
                table.name, table.schema
            )));
        }

        let table_id = table.id;
        let schema_name = table.schema.clone();
        let table_name = table.name.clone();

        schema_tables.insert(table_name.clone(), table);
        tables_by_id.insert(table_id, (schema_name, table_name));

        Ok(table_id)
    }

    fn drop_table(&self, schema: &str, table: &str) -> Result<()> {
        let mut schemas = self.schemas.write().unwrap();
        let mut tables_by_id = self.tables_by_id.write().unwrap();

        let schema_tables = schemas
            .get_mut(schema)
            .ok_or_else(|| TiSqlError::Catalog(format!("Schema '{}' not found", schema)))?;

        if let Some(table_def) = schema_tables.remove(table) {
            tables_by_id.remove(&table_def.id);
            Ok(())
        } else {
            Err(TiSqlError::TableNotFound(format!("{}.{}", schema, table)))
        }
    }

    fn get_table(&self, schema: &str, table: &str) -> Result<Option<TableDef>> {
        let schemas = self.schemas.read().unwrap();
        Ok(schemas
            .get(schema)
            .and_then(|tables| tables.get(table).cloned()))
    }

    fn get_table_by_id(&self, id: TableId) -> Result<Option<TableDef>> {
        let tables_by_id = self.tables_by_id.read().unwrap();
        let schemas = self.schemas.read().unwrap();

        if let Some((schema, table_name)) = tables_by_id.get(&id) {
            Ok(schemas
                .get(schema)
                .and_then(|tables| tables.get(table_name).cloned()))
        } else {
            Ok(None)
        }
    }

    fn list_tables(&self, schema: &str) -> Result<Vec<TableDef>> {
        let schemas = self.schemas.read().unwrap();
        Ok(schemas
            .get(schema)
            .map(|tables| tables.values().cloned().collect())
            .unwrap_or_default())
    }

    fn create_index(&self, table_id: TableId, index: IndexDef) -> Result<IndexId> {
        let tables_by_id = self.tables_by_id.read().unwrap();
        let mut schemas = self.schemas.write().unwrap();

        let (schema, table_name) = tables_by_id
            .get(&table_id)
            .ok_or_else(|| TiSqlError::Catalog(format!("Table with ID {} not found", table_id)))?
            .clone();

        let table = schemas
            .get_mut(&schema)
            .and_then(|tables| tables.get_mut(&table_name))
            .ok_or_else(|| TiSqlError::Catalog("Table not found".into()))?;

        // Check for duplicate index name
        if table.indexes.iter().any(|i| i.name == index.name) {
            return Err(TiSqlError::Catalog(format!(
                "Index '{}' already exists",
                index.name
            )));
        }

        let index_id = index.id;
        table.indexes.push(index);

        Ok(index_id)
    }

    fn drop_index(&self, table_id: TableId, index_name: &str) -> Result<()> {
        let tables_by_id = self.tables_by_id.read().unwrap();
        let mut schemas = self.schemas.write().unwrap();

        let (schema, table_name) = tables_by_id
            .get(&table_id)
            .ok_or_else(|| TiSqlError::Catalog(format!("Table with ID {} not found", table_id)))?
            .clone();

        let table = schemas
            .get_mut(&schema)
            .and_then(|tables| tables.get_mut(&table_name))
            .ok_or_else(|| TiSqlError::Catalog("Table not found".into()))?;

        let idx = table
            .indexes
            .iter()
            .position(|i| i.name == index_name)
            .ok_or_else(|| TiSqlError::Catalog(format!("Index '{}' not found", index_name)))?;

        table.indexes.remove(idx);
        Ok(())
    }

    fn next_auto_increment(&self, table_id: TableId) -> Result<u64> {
        let tables_by_id = self.tables_by_id.read().unwrap();
        let mut schemas = self.schemas.write().unwrap();

        let (schema, table_name) = tables_by_id
            .get(&table_id)
            .ok_or_else(|| TiSqlError::Catalog(format!("Table with ID {} not found", table_id)))?
            .clone();

        let table = schemas
            .get_mut(&schema)
            .and_then(|tables| tables.get_mut(&table_name))
            .ok_or_else(|| TiSqlError::Catalog("Table not found".into()))?;

        table.auto_increment_id += 1;
        Ok(table.auto_increment_id)
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
    use crate::catalog::ColumnDef;
    use crate::types::DataType;

    fn make_test_table(catalog: &MemoryCatalog, name: &str) -> TableDef {
        let table_id = catalog.next_table_id().unwrap();
        TableDef {
            id: table_id,
            name: name.to_string(),
            schema: "default".to_string(),
            columns: vec![
                ColumnDef {
                    id: 0,
                    name: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                    default: None,
                    auto_increment: true,
                },
                ColumnDef {
                    id: 1,
                    name: "name".to_string(),
                    data_type: DataType::Varchar(255),
                    nullable: true,
                    default: None,
                    auto_increment: false,
                },
            ],
            primary_key: vec![0],
            indexes: vec![],
            auto_increment_id: 0,
        }
    }

    #[test]
    fn test_create_get_table() {
        let catalog = MemoryCatalog::new();
        let table = make_test_table(&catalog, "users");
        let table_id = table.id;

        catalog.create_table(table).unwrap();

        let retrieved = catalog.get_table("default", "users").unwrap().unwrap();
        assert_eq!(retrieved.id, table_id);
        assert_eq!(retrieved.name, "users");

        let by_id = catalog.get_table_by_id(table_id).unwrap().unwrap();
        assert_eq!(by_id.name, "users");
    }

    #[test]
    fn test_drop_table() {
        let catalog = MemoryCatalog::new();
        let table = make_test_table(&catalog, "users");

        catalog.create_table(table).unwrap();
        assert!(catalog.get_table("default", "users").unwrap().is_some());

        catalog.drop_table("default", "users").unwrap();
        assert!(catalog.get_table("default", "users").unwrap().is_none());
    }

    #[test]
    fn test_auto_increment() {
        let catalog = MemoryCatalog::new();
        let table = make_test_table(&catalog, "users");
        let table_id = table.id;

        catalog.create_table(table).unwrap();

        assert_eq!(catalog.next_auto_increment(table_id).unwrap(), 1);
        assert_eq!(catalog.next_auto_increment(table_id).unwrap(), 2);
        assert_eq!(catalog.next_auto_increment(table_id).unwrap(), 3);
    }
}
