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

//! Storage layer - provides key-value storage abstraction.
//!
//! This module defines the core storage interface (`StorageEngine`) and related types.
//! All storage implementations must implement this trait.
//!
//! ## IMPORTANT: Use TxnService for All Reads
//!
//! **DO NOT use `StorageEngine` methods directly for reading data.**
//!
//! All reads should go through [`TxnService`](crate::transaction::TxnService) to ensure
//! proper MVCC semantics, transaction isolation, and read-your-writes consistency:
//!
//! ```ignore
//! // CORRECT: Use TxnService for reads
//! let ctx = txn_service.begin(true)?;  // read-only transaction
//! let value = txn_service.get(&ctx, key)?;
//! let iter = txn_service.scan(&ctx, range)?;
//!
//! // WRONG: Direct storage access bypasses MVCC and buffered writes
//! let value = storage.get(key)?;  // DON'T DO THIS
//! ```
//!
//! The non-MVCC methods (`get`, `scan`) exist only for:
//! - Internal recovery operations
//! - Testing and debugging
//! - Low-level infrastructure code
//!
//! ## Storage Implementations
//!
//! Two storage engines are available:
//!
//! - [`CrossbeamMemTableEngine`] (default, aliased as `MemTableEngine`): Uses crossbeam's
//!   lock-free skip list with epoch-based memory reclamation. Better write throughput due
//!   to smaller node size (~40 bytes vs 192 bytes).
//!
//! - [`ArenaMemTableEngine`]: Uses a custom arena-based skip list for predictable memory
//!   management and bulk deallocation. Useful for scenarios where memory release timing
//!   is critical.
//!
//! ## Key Encoding
//!
//! Keys are encoded using TiDB-compatible format via the codec module.
//! The storage layer is agnostic to key structure - it just stores bytes.

pub mod config;
pub mod memtable;
pub mod sstable;
pub mod version;

// ============================================================================
// Storage Implementation
// ============================================================================

// Production default: Crossbeam-based memtable with MVCC key encoding
// (better write throughput due to smaller node size and epoch-based GC)
pub use memtable::MemTableEngine;
pub use memtable::MemoryStats;

// Re-export arena-based memtable for comparison/benchmarking
pub use memtable::ArenaMemTableEngine;
pub use memtable::ArenaMemoryStats;

// Re-export BTreeMap-based memtable for comparison/benchmarking
pub use memtable::BTreeMemTableEngine;

// Re-export CrossbeamMemTableEngine for comparison/benchmarking
pub use memtable::CrossbeamMemTableEngine;

// Re-export LSM MemTable wrapper
pub use memtable::MemTable;

// Re-export LSM configuration
pub use config::{LsmConfig, LsmConfigBuilder};
pub use config::{
    DEFAULT_BLOCK_SIZE as LSM_DEFAULT_BLOCK_SIZE, DEFAULT_L0_COMPACTION_TRIGGER,
    DEFAULT_L1_MAX_SIZE, DEFAULT_LEVEL_SIZE_MULTIPLIER, DEFAULT_MAX_FROZEN_MEMTABLES,
    DEFAULT_MEMTABLE_SIZE, DEFAULT_TARGET_FILE_SIZE,
};

// Re-export version management
pub use version::{ManifestDelta, Version, VersionBuilder, MAX_LEVELS};

// Re-export SST types for persistent storage
pub use sstable::{
    // Builder types
    CompressionType,
    // Iterator types
    ConcatIterator,
    // Block types
    DataBlock,
    DataBlockBuilder,
    Footer,
    IndexBlock,
    IndexBlockBuilder,
    IndexEntry,
    SstBuilder,
    SstBuilderOptions,
    SstIterator,
    SstMeta,
    // Reader types
    SstReader,
    SstReaderRef,
    DEFAULT_BLOCK_SIZE,
    FOOTER_SIZE,
    SST_MAGIC,
    SST_VERSION,
};

use std::collections::HashMap;
use std::ops::Range;

use crate::error::Result;
use crate::types::{Key, RawValue, TableId, Timestamp};

// ============================================================================
// Key Encoding (re-exports from codec for convenience)
// ============================================================================

use crate::codec::key::{
    decode_record_key, encode_record_key, encode_record_key_with_handle, Handle,
};

/// Encode a table key (TiDB-compatible format).
/// Format: 't' + tableID + "_r" + user_key
pub fn encode_key(table_id: TableId, user_key: &[u8]) -> Vec<u8> {
    encode_record_key(table_id, user_key)
}

/// Encode a table key with an integer handle.
///
/// This is used for tables without an explicit primary key, where we fall back to
/// a hidden row-id (similar to TiDB's `_tidb_rowid`).
pub fn encode_int_key(table_id: TableId, handle: i64) -> Vec<u8> {
    encode_record_key_with_handle(table_id, handle)
}

/// Decode a table key back to (table_id, user_key).
#[allow(dead_code)]
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

/// Core KV storage interface with MVCC versioning.
///
/// This trait defines a pure key-value storage layer. Implementations should be
/// thread-safe (Send + Sync) and provide MVCC versioning through timestamped writes.
///
/// # Layer Separation
///
/// The storage layer is responsible ONLY for:
/// - Storing and retrieving versioned key-value pairs
/// - Atomic batch writes with explicit timestamps
///
/// The storage layer does NOT handle:
/// - Lock management (handled by ConcurrencyManager in transaction layer)
/// - Timestamp allocation (handled by TsoService in transaction layer)
/// - Transaction coordination (handled by TransactionService)
///
/// # Usage
///
/// **For application code, always use [`TxnService`](crate::transaction::TxnService)
/// instead of calling these methods directly.**
///
/// Storage methods are intended for:
/// - Internal use by `TransactionService`
/// - Recovery operations
/// - Testing infrastructure
///
/// # Methods
///
/// | Method | Description |
/// |--------|-------------|
/// | `get` | Read latest version (for recovery/testing) |
/// | `get_at` | Read at specific timestamp (MVCC) |
/// | `scan` | Scan latest versions (for recovery/testing) |
/// | `scan_at` | Scan at specific timestamp (MVCC) |
/// | `write_batch` | Atomic writes with commit_ts (requires timestamp) |
pub trait StorageEngine: Send + Sync + 'static {
    /// Point lookup - returns the latest version of a key.
    ///
    /// Returns the most recently committed version regardless of timestamp.
    /// Primarily used for recovery operations and testing.
    ///
    /// For MVCC-aware reads, use `get_at()` or `TxnService::get()`.
    fn get(&self, key: &[u8]) -> Result<Option<RawValue>>;

    /// Point lookup at specific timestamp (MVCC-aware).
    ///
    /// Returns the value visible at the given timestamp, i.e., the latest
    /// version with `commit_ts <= ts`.
    ///
    /// This is called internally by `TxnService::get()`.
    fn get_at(&self, key: &[u8], ts: Timestamp) -> Result<Option<RawValue>>;

    /// Apply a batch of writes atomically.
    ///
    /// # Requirements
    ///
    /// The batch MUST have `commit_ts` set via `batch.set_commit_ts()`.
    /// This ensures proper MVCC versioning.
    ///
    /// # Errors
    ///
    /// Returns an error if `commit_ts` is not set.
    fn write_batch(&self, batch: WriteBatch) -> Result<()>;

    /// Range scan - returns latest versions of all keys in range.
    ///
    /// Returns the most recently committed version of each key.
    /// Primarily used for recovery operations and testing.
    ///
    /// For MVCC-aware scans, use `scan_at()` or `TxnService::scan()`.
    fn scan(&self, range: &Range<Key>) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>>;

    /// Range scan at specific timestamp (MVCC-aware).
    ///
    /// Returns key-value pairs visible at the given timestamp. For each key,
    /// returns the latest version with `commit_ts <= ts`.
    ///
    /// This is called internally by `TxnService::scan()`.
    fn scan_at(
        &self,
        range: &Range<Key>,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>>;
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
///
/// # Key Deduplication
///
/// WriteBatch maintains at most one operation per key. If you call `put(k, v1)`
/// followed by `put(k, v2)`, only `v2` will be committed. Similarly, `put(k, v)`
/// followed by `delete(k)` results in only the delete. This is "last write wins"
/// semantics required for correct MVCC behavior.
#[derive(Default, Clone, Debug)]
pub struct WriteBatch {
    /// Operations indexed by key - ensures at most one op per key (last write wins)
    ops: HashMap<Key, WriteOp>,
    /// Commit timestamp for MVCC (set by TransactionService)
    commit_ts: Option<Timestamp>,
}

impl WriteBatch {
    /// Create a new empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a put operation to the batch.
    ///
    /// If the key already exists in the batch (from a previous put or delete),
    /// the old operation is replaced. This ensures "last write wins" semantics.
    pub fn put(&mut self, key: impl Into<Key>, value: impl Into<RawValue>) {
        let key = key.into();
        self.ops.insert(
            key.clone(),
            WriteOp::Put {
                key,
                value: value.into(),
            },
        );
    }

    /// Add a delete operation to the batch.
    ///
    /// If the key already exists in the batch (from a previous put or delete),
    /// the old operation is replaced. This ensures "last write wins" semantics.
    pub fn delete(&mut self, key: impl Into<Key>) {
        let key = key.into();
        self.ops.insert(key.clone(), WriteOp::Delete { key });
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
        self.ops.values()
    }

    /// Get the operation for a specific key, if any.
    pub fn get(&self, key: &[u8]) -> Option<&WriteOp> {
        self.ops.get(key)
    }

    /// Consume the batch and return the operations.
    pub(crate) fn into_ops(self) -> Vec<WriteOp> {
        self.ops.into_values().collect()
    }

    /// Set the commit timestamp for MVCC.
    pub fn set_commit_ts(&mut self, ts: Timestamp) {
        self.commit_ts = Some(ts);
    }

    /// Get the commit timestamp.
    pub fn commit_ts(&self) -> Option<Timestamp> {
        self.commit_ts
    }

    /// Get all keys in this batch.
    pub fn keys(&self) -> impl Iterator<Item = &Key> {
        self.ops.keys()
    }
}
