mod memory;

pub use memory::MemoryCatalog;

use crate::error::Result;
use crate::types::{ColumnId, DataType, IndexId, TableId};

#[derive(Clone, Debug)]
pub struct ColumnDef {
    pub id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<DefaultValue>,
    pub auto_increment: bool,
}

#[derive(Clone, Debug)]
pub enum DefaultValue {
    Null,
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    CurrentTimestamp,
}

#[derive(Clone, Debug)]
pub struct TableDef {
    pub id: TableId,
    pub name: String,
    pub schema: String, // database name
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<ColumnId>,
    pub indexes: Vec<IndexDef>,
    pub auto_increment_id: u64, // Current auto-increment value
}

impl TableDef {
    pub fn column_by_name(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn column_by_id(&self, id: ColumnId) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.id == id)
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn pk_column_indices(&self) -> Vec<usize> {
        self.primary_key
            .iter()
            .filter_map(|&id| self.columns.iter().position(|c| c.id == id))
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct IndexDef {
    pub id: IndexId,
    pub name: String,
    pub columns: Vec<ColumnId>,
    pub unique: bool,
}

/// Catalog interface - metadata management
pub trait Catalog: Send + Sync {
    /// Schema (database) operations
    fn create_schema(&self, name: &str) -> Result<()>;
    fn drop_schema(&self, name: &str) -> Result<()>;
    fn list_schemas(&self) -> Result<Vec<String>>;
    fn schema_exists(&self, name: &str) -> Result<bool>;

    /// Table operations
    fn create_table(&self, table: TableDef) -> Result<TableId>;
    fn drop_table(&self, schema: &str, table: &str) -> Result<()>;
    fn get_table(&self, schema: &str, table: &str) -> Result<Option<TableDef>>;
    fn get_table_by_id(&self, id: TableId) -> Result<Option<TableDef>>;
    fn list_tables(&self, schema: &str) -> Result<Vec<TableDef>>;

    /// Index operations
    fn create_index(&self, table_id: TableId, index: IndexDef) -> Result<IndexId>;
    fn drop_index(&self, table_id: TableId, index_name: &str) -> Result<()>;

    /// Auto-increment support
    fn next_auto_increment(&self, table_id: TableId) -> Result<u64>;

    /// Generate unique IDs
    fn next_table_id(&self) -> Result<TableId>;
    fn next_index_id(&self) -> Result<IndexId>;
}
