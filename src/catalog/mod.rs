//! Catalog layer - schema metadata management.
//!
//! This module provides the `Catalog` trait for managing database metadata:
//! - Schemas (databases)
//! - Tables and columns
//! - Indexes
//!
//! ## Module Structure
//! - `Catalog` trait - Interface for metadata operations
//! - `MemoryCatalog` - In-memory implementation
//! - `TableDef`, `ColumnDef`, `IndexDef` - Schema definitions

mod memory;

pub use memory::MemoryCatalog;

use crate::error::Result;
use crate::types::{ColumnId, DataType, IndexId, TableId};

// ============================================================================
// Column Definition
// ============================================================================

/// Definition of a table column.
#[derive(Clone, Debug)]
pub struct ColumnDef {
    id: ColumnId,
    name: String,
    data_type: DataType,
    nullable: bool,
    default: Option<DefaultValue>,
    auto_increment: bool,
}

impl ColumnDef {
    /// Create a new column definition.
    pub fn new(
        id: ColumnId,
        name: String,
        data_type: DataType,
        nullable: bool,
        default: Option<DefaultValue>,
        auto_increment: bool,
    ) -> Self {
        Self {
            id,
            name,
            data_type,
            nullable,
            default,
            auto_increment,
        }
    }

    /// Get the column ID.
    #[inline]
    pub fn id(&self) -> ColumnId {
        self.id
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

    /// Get the default value.
    #[inline]
    pub fn default(&self) -> Option<&DefaultValue> {
        self.default.as_ref()
    }

    /// Check if the column is auto-increment.
    #[inline]
    pub fn auto_increment(&self) -> bool {
        self.auto_increment
    }
}

/// Default value for a column.
#[derive(Clone, Debug)]
pub enum DefaultValue {
    Null,
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    CurrentTimestamp,
}

// ============================================================================
// Index Definition
// ============================================================================

/// Definition of an index.
#[derive(Clone, Debug)]
pub struct IndexDef {
    id: IndexId,
    name: String,
    columns: Vec<ColumnId>,
    unique: bool,
}

impl IndexDef {
    /// Create a new index definition.
    pub fn new(id: IndexId, name: String, columns: Vec<ColumnId>, unique: bool) -> Self {
        Self {
            id,
            name,
            columns,
            unique,
        }
    }

    /// Get the index ID.
    #[inline]
    pub fn id(&self) -> IndexId {
        self.id
    }

    /// Get the index name.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the column IDs in this index.
    #[inline]
    pub fn columns(&self) -> &[ColumnId] {
        &self.columns
    }

    /// Check if this is a unique index.
    #[inline]
    pub fn unique(&self) -> bool {
        self.unique
    }
}

// ============================================================================
// Table Definition
// ============================================================================

/// Definition of a table.
#[derive(Clone, Debug)]
pub struct TableDef {
    id: TableId,
    name: String,
    schema: String, // database name
    columns: Vec<ColumnDef>,
    primary_key: Vec<ColumnId>,
    indexes: Vec<IndexDef>,
    auto_increment_id: u64,
}

impl TableDef {
    /// Create a new table definition.
    pub fn new(
        id: TableId,
        name: String,
        schema: String,
        columns: Vec<ColumnDef>,
        primary_key: Vec<ColumnId>,
    ) -> Self {
        Self {
            id,
            name,
            schema,
            columns,
            primary_key,
            indexes: Vec::new(),
            auto_increment_id: 0,
        }
    }

    /// Get the table ID.
    #[inline]
    pub fn id(&self) -> TableId {
        self.id
    }

    /// Get the table name.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the schema (database) name.
    #[inline]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Get the columns.
    #[inline]
    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }

    /// Get the primary key column IDs.
    #[inline]
    pub fn primary_key(&self) -> &[ColumnId] {
        &self.primary_key
    }

    /// Get the indexes.
    #[inline]
    pub fn indexes(&self) -> &[IndexDef] {
        &self.indexes
    }

    /// Get the current auto-increment ID.
    #[inline]
    pub fn auto_increment_id(&self) -> u64 {
        self.auto_increment_id
    }

    /// Find a column by name.
    pub fn column_by_name(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Find a column by ID.
    pub fn column_by_id(&self, id: ColumnId) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.id == id)
    }

    /// Get the index of a column by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Get the indices of primary key columns.
    pub fn pk_column_indices(&self) -> Vec<usize> {
        self.primary_key
            .iter()
            .filter_map(|&id| self.columns.iter().position(|c| c.id == id))
            .collect()
    }

    // Internal methods for catalog implementations
    pub(crate) fn add_index(&mut self, index: IndexDef) {
        self.indexes.push(index);
    }

    pub(crate) fn remove_index(&mut self, name: &str) -> Option<IndexDef> {
        if let Some(idx) = self.indexes.iter().position(|i| i.name == name) {
            Some(self.indexes.remove(idx))
        } else {
            None
        }
    }

    pub(crate) fn increment_auto_id(&mut self) -> u64 {
        self.auto_increment_id += 1;
        self.auto_increment_id
    }
}

// ============================================================================
// Catalog Trait
// ============================================================================

/// Catalog interface - metadata management.
///
/// The Catalog trait defines operations for managing database schema metadata.
/// Implementations should be thread-safe (Send + Sync).
pub trait Catalog: Send + Sync {
    // Schema (database) operations

    /// Create a new schema.
    fn create_schema(&self, name: &str) -> Result<()>;

    /// Drop a schema.
    fn drop_schema(&self, name: &str) -> Result<()>;

    /// List all schemas.
    fn list_schemas(&self) -> Result<Vec<String>>;

    /// Check if a schema exists.
    fn schema_exists(&self, name: &str) -> Result<bool>;

    // Table operations

    /// Create a new table.
    fn create_table(&self, table: TableDef) -> Result<TableId>;

    /// Drop a table.
    fn drop_table(&self, schema: &str, table: &str) -> Result<()>;

    /// Get a table by name.
    fn get_table(&self, schema: &str, table: &str) -> Result<Option<TableDef>>;

    /// Get a table by ID.
    fn get_table_by_id(&self, id: TableId) -> Result<Option<TableDef>>;

    /// List all tables in a schema.
    fn list_tables(&self, schema: &str) -> Result<Vec<TableDef>>;

    // Index operations

    /// Create an index on a table.
    fn create_index(&self, table_id: TableId, index: IndexDef) -> Result<IndexId>;

    /// Drop an index.
    fn drop_index(&self, table_id: TableId, index_name: &str) -> Result<()>;

    // Auto-increment support

    /// Get and increment the auto-increment value for a table.
    fn next_auto_increment(&self, table_id: TableId) -> Result<u64>;

    // ID generation

    /// Generate a new unique table ID.
    fn next_table_id(&self) -> Result<TableId>;

    /// Generate a new unique index ID.
    fn next_index_id(&self) -> Result<IndexId>;
}
