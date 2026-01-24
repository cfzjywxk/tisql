mod memtable;

pub use memtable::MemTableEngine;

// Re-export codec functions
pub use crate::codec::key::{
    decode_record_key, encode_record_key, encode_record_key_with_handle,
    encode_values_for_key as encode_pk, Handle,
};

use crate::error::{Result, TiSqlError};
use crate::types::{Key, RawValue, Row, TableId, Timestamp};
use std::ops::Range;

/// Encode a table key (TiDB-compatible format).
/// Format: 't' + tableID + "_r" + user_key
///
/// This uses the new codec for proper ordering.
pub fn encode_key(table_id: TableId, user_key: &[u8]) -> Vec<u8> {
    encode_record_key(table_id, user_key)
}

/// Decode a table key back to (table_id, user_key).
pub fn decode_key(key: &[u8]) -> Result<(TableId, Vec<u8>)> {
    let (table_id, handle) = decode_record_key(key)?;
    let user_key = match handle {
        Handle::Int(_) => handle.encoded(),
        Handle::Common(bytes) => bytes,
    };
    Ok((table_id, user_key))
}

/// Encode a row using bincode.
///
/// Note: For now, we continue using bincode for row values since it preserves
/// all type information. The codec::row module provides the TiDB-compatible
/// compact row format for future use when schema info is available during decode.
pub fn encode_row(row: &Row) -> Result<Vec<u8>> {
    bincode::serialize(row).map_err(|e| TiSqlError::Storage(e.to_string()))
}

/// Decode a row from bincode.
pub fn decode_row(data: &[u8]) -> Result<Row> {
    bincode::deserialize(data).map_err(|e| TiSqlError::Storage(e.to_string()))
}

/// Core KV storage interface - all engines implement this
pub trait StorageEngine: Send + Sync + 'static {
    /// Point lookup
    fn get(&self, key: &[u8]) -> Result<Option<RawValue>>;

    /// Point lookup at specific timestamp (for MVCC)
    fn get_at(&self, key: &[u8], ts: Timestamp) -> Result<Option<RawValue>>;

    /// Write a single key-value
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key
    fn delete(&self, key: &[u8]) -> Result<()>;

    /// Atomic batch write
    fn write_batch(&self, batch: WriteBatch) -> Result<()>;

    /// Create a consistent snapshot for reads
    fn snapshot(&self) -> Result<Box<dyn Snapshot>>;

    /// Range scan - returns iterator
    fn scan(&self, range: Range<Key>) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>>;
}

/// Immutable snapshot for consistent reads
pub trait Snapshot: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<RawValue>>;
    fn scan(&self, range: Range<Key>) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>>;
}

/// Batch of writes to apply atomically
#[derive(Default, Clone)]
pub struct WriteBatch {
    pub ops: Vec<WriteOp>,
}

#[derive(Clone)]
pub enum WriteOp {
    Put { key: Key, value: RawValue },
    Delete { key: Key },
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: impl Into<Key>, value: impl Into<RawValue>) {
        self.ops.push(WriteOp::Put {
            key: key.into(),
            value: value.into(),
        });
    }

    pub fn delete(&mut self, key: impl Into<Key>) {
        self.ops.push(WriteOp::Delete { key: key.into() });
    }

    pub fn clear(&mut self) {
        self.ops.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }
}
