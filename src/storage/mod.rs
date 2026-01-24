//! Storage layer - provides key-value storage abstraction.
//!
//! This module defines the core storage interface (`StorageEngine`) and related types.
//! All storage implementations must implement this trait.
//!
//! ## Module Structure
//! - `StorageEngine` trait - Core KV storage interface
//! - `Snapshot` trait - Consistent read snapshot
//! - `WriteBatch` - Atomic batch writes
//! - `MemTableEngine` - In-memory implementation for testing
//!
//! ## Key Encoding
//! Keys are encoded using TiDB-compatible format via the codec module.
//! The storage layer is agnostic to key structure - it just stores bytes.

mod memtable;

pub use memtable::MemTableEngine;

use crate::error::Result;
use crate::types::{Key, RawValue, TableId, Timestamp};
use std::ops::Range;

// ============================================================================
// Key Encoding (re-exports from codec for convenience)
// ============================================================================

use crate::codec::key::{decode_record_key, encode_record_key, Handle};

/// Encode a table key (TiDB-compatible format).
/// Format: 't' + tableID + "_r" + user_key
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

// Re-export commonly used codec functions for row encoding
pub use crate::codec::key::encode_values_for_key as encode_pk;
pub use crate::codec::row::{decode_row_to_values, encode_row};

// ============================================================================
// Storage Engine Trait
// ============================================================================

/// Core KV storage interface - all engines implement this.
///
/// This trait defines the fundamental operations for key-value storage:
/// - Point lookups (get)
/// - Range scans
/// - Writes (single and batch)
/// - Snapshots for consistent reads
///
/// Implementations should be thread-safe (Send + Sync).
pub trait StorageEngine: Send + Sync + 'static {
    /// Point lookup - returns the value for a key, or None if not found.
    fn get(&self, key: &[u8]) -> Result<Option<RawValue>>;

    /// Point lookup at specific timestamp (for MVCC).
    fn get_at(&self, key: &[u8], ts: Timestamp) -> Result<Option<RawValue>>;

    /// Write a single key-value pair.
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key.
    fn delete(&self, key: &[u8]) -> Result<()>;

    /// Apply a batch of writes atomically.
    fn write_batch(&self, batch: WriteBatch) -> Result<()>;

    /// Create a consistent snapshot for reads.
    fn snapshot(&self) -> Result<Box<dyn Snapshot>>;

    /// Range scan - returns an iterator over key-value pairs.
    fn scan(&self, range: Range<Key>) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>>;
}

// ============================================================================
// Snapshot Trait
// ============================================================================

/// Immutable snapshot for consistent reads.
///
/// A snapshot represents a consistent view of the database at a point in time.
/// Reads from a snapshot will not see writes that occur after the snapshot was created.
pub trait Snapshot: Send + Sync {
    /// Point lookup within the snapshot.
    fn get(&self, key: &[u8]) -> Result<Option<RawValue>>;

    /// Range scan within the snapshot.
    fn scan(&self, range: Range<Key>) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>>;
}

// ============================================================================
// WriteBatch - Atomic batch operations
// ============================================================================

/// A write operation (put or delete).
#[derive(Clone, Debug)]
pub enum WriteOp {
    Put { key: Key, value: RawValue },
    Delete { key: Key },
}

/// Batch of writes to apply atomically.
///
/// Use `WriteBatch` to group multiple writes that should be applied
/// as a single atomic operation.
#[derive(Default, Clone)]
pub struct WriteBatch {
    ops: Vec<WriteOp>,
}

impl WriteBatch {
    /// Create a new empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a put operation to the batch.
    pub fn put(&mut self, key: impl Into<Key>, value: impl Into<RawValue>) {
        self.ops.push(WriteOp::Put {
            key: key.into(),
            value: value.into(),
        });
    }

    /// Add a delete operation to the batch.
    pub fn delete(&mut self, key: impl Into<Key>) {
        self.ops.push(WriteOp::Delete { key: key.into() });
    }

    /// Clear all operations from the batch.
    pub fn clear(&mut self) {
        self.ops.clear();
    }

    /// Check if the batch is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Get the number of operations in the batch.
    #[inline]
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Iterate over the operations.
    pub fn iter(&self) -> impl Iterator<Item = &WriteOp> {
        self.ops.iter()
    }

    /// Consume the batch and return the operations.
    pub(crate) fn into_ops(self) -> Vec<WriteOp> {
        self.ops
    }
}
