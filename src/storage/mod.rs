mod memtable;
mod encoding;

pub use memtable::MemTableEngine;
pub use encoding::{encode_key, decode_key, encode_pk, encode_row, decode_row};

use crate::error::Result;
use crate::types::{Key, RawValue, Timestamp};
use std::ops::Range;

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
