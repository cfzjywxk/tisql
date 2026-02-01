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
//! let iter = txn_service.scan_iter(&ctx, range)?;
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
//! The default memtable engine is [`VersionedMemTableEngine`] (aliased as `MemTableEngine`),
//! which uses an OceanBase-style design storing each user key once with a linked list of
//! versions for space efficiency and better cache locality.
//!
//! ## Key Encoding
//!
//! Keys are encoded using TiDB-compatible format via the codec module.
//! The storage layer is agnostic to key structure - it just stores bytes.

pub mod compaction;
pub mod config;
pub mod ilog;
pub mod lsm;
pub mod memtable;
pub mod mvcc;
pub mod recovery;
pub mod sstable;
pub mod version;

// ============================================================================
// Storage Implementation
// ============================================================================

// Production memtable engine: OceanBase-style versioned memtable
// Each user key is stored once with a linked list of versions, providing:
// - Space efficiency: key stored once per row, not repeated per version
// - Fast point lookups: seek to user key, traverse short version chain
// - Better cache locality: all versions of a key are adjacent in memory
pub use memtable::ArcVersionedMemTableIterator;
pub use memtable::MemTableEngine;
pub use memtable::MemoryStats;
pub use memtable::VersionedMemTableEngine;
pub use memtable::VersionedMemoryStats;

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

// Re-export LSM engine
pub use lsm::{LsmEngine, LsmStats, TieredMergeIterator};

// Re-export compaction types
pub use compaction::{CompactionExecutor, CompactionPicker, CompactionTask, MergeIterator};

// Re-export ilog types
pub use ilog::{IlogConfig, IlogRecord, IlogService, VersionSnapshot};

// Re-export recovery types
pub use recovery::{LsmRecovery, RecoveryResult, RecoveryStats};

// Re-export MVCC codec types
pub use mvcc::{
    decode_mvcc_key, encode_mvcc_key, extract_key, is_tombstone, next_key_bound, prev_key_bound,
    MvccIterator, MvccKey, TIMESTAMP_SIZE, TOMBSTONE,
};

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
    // MvccIterator wrapper for SST
    SstMvccIterator,
    // Reader types
    SstReader,
    SstReaderRef,
    DEFAULT_BLOCK_SIZE,
    FOOTER_SIZE,
    SST_MAGIC,
    SST_VERSION,
};

use std::collections::BTreeMap;
use std::ops::Range;

use crate::error::Result;
use crate::types::{Key, RawValue, TableId, Timestamp};

// ============================================================================
// Key Encoding (re-exports from codec for convenience)
// ============================================================================

use crate::codec::key::{encode_record_key, encode_record_key_with_handle};

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

// Re-export commonly used codec functions for row encoding
pub use crate::codec::key::encode_values_for_key as encode_pk;
pub use crate::codec::row::{decode_row_to_values, encode_row};

// ============================================================================
// Storage Engine Trait
// ============================================================================

/// Core KV storage interface for MVCC key-value storage.
///
/// This trait defines a pure key-value storage layer that operates on `MvccKey` format.
/// All keys in the storage layer are MVCC-encoded: `key || !commit_ts` (8-byte big-endian, bitwise NOT).
/// Implementations should be thread-safe (Send + Sync).
///
/// # Key Type Separation
///
/// - **Storage layer**: Uses `MvccKey` exclusively (except GC compaction filter)
/// - **Transaction layer**: Accepts `Key` (user keys) and encodes to `MvccKey` internally
///
/// # Layer Separation
///
/// The storage layer is responsible ONLY for:
/// - Storing `MvccKey`-value pairs
/// - Returning `MvccKey` on iteration
/// - Atomic batch writes (MVCC encoding done internally from user key + commit_ts)
///
/// The storage layer does NOT handle:
/// - MVCC read semantics (finding latest version <= ts) - handled by transaction layer
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
/// | `scan_iter` | Create streaming iterator over range |
/// | `write_batch` | Atomic writes with commit_ts (MVCC encoding done internally) |
pub trait StorageEngine: Send + Sync + 'static {
    /// The iterator type returned by `scan_iter`.
    ///
    /// Using an associated type avoids boxing and dynamic dispatch overhead.
    /// Each storage implementation defines its own concrete iterator type.
    type Iter: MvccIterator;

    /// Create a streaming iterator over MVCC keys in range.
    ///
    /// Returns a streaming iterator over all MVCC key-value pairs in the given range,
    /// including all versions and tombstones. Keys are in `MvccKey` format (`key || !commit_ts`).
    ///
    /// The transaction layer is responsible for:
    /// - MVCC filtering (finding latest version with ts <= read_ts)
    /// - Tombstone handling (treating tombstones as deleted)
    /// - Deduplication (returning only the latest visible version per key)
    ///
    /// # Arguments
    ///
    /// * `range` - MVCC key range to scan. Use `MvccKey::encode(key, ts)` to build bounds.
    ///
    /// # Returns
    ///
    /// An iterator that yields entries in MVCC key order.
    fn scan_iter(&self, range: Range<MvccKey>) -> Result<Self::Iter>;

    /// Apply a batch of writes atomically.
    ///
    /// The batch contains user keys (not MVCC keys). The storage layer encodes
    /// MVCC keys internally using the batch's `commit_ts`.
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
}

// ============================================================================
// WriteBatch - Atomic batch operations
// ============================================================================

/// A write operation (put or delete).
///
/// Note: The key is stored separately in WriteBatch's HashMap, not in WriteOp.
/// This avoids redundant key storage and cloning.
#[derive(Clone, Debug)]
pub enum WriteOp {
    Put { value: RawValue },
    Delete,
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
///
/// # Iteration Order
///
/// Keys are iterated in lexicographic order (via BTreeMap), ensuring deterministic
/// ordering across runs. This improves reproducibility and debuggability of the
/// commit log, as entries for a transaction are written in consistent order.
#[derive(Default, Clone, Debug)]
pub struct WriteBatch {
    /// Operations indexed by key - ensures at most one op per key (last write wins).
    /// BTreeMap ensures deterministic iteration order (lexicographic by key).
    ops: BTreeMap<Key, WriteOp>,
    /// Commit timestamp for MVCC (set by TransactionService)
    commit_ts: Option<Timestamp>,
    /// CLOG LSN for recovery ordering (set by TransactionService after clog write)
    /// This ensures storage and clog share the same LSN for proper recovery semantics.
    clog_lsn: Option<u64>,
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
        self.ops.insert(
            key.into(),
            WriteOp::Put {
                value: value.into(),
            },
        );
    }

    /// Add a delete operation to the batch.
    ///
    /// If the key already exists in the batch (from a previous put or delete),
    /// the old operation is replaced. This ensures "last write wins" semantics.
    pub fn delete(&mut self, key: impl Into<Key>) {
        self.ops.insert(key.into(), WriteOp::Delete);
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

    /// Iterate over the operations as (key, op) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&Key, &WriteOp)> {
        self.ops.iter()
    }

    /// Get the operation for a specific key, if any.
    pub fn get(&self, key: &[u8]) -> Option<&WriteOp> {
        self.ops.get(key)
    }

    /// Consume the batch and return the operations as (key, op) pairs.
    pub(crate) fn into_iter(self) -> impl Iterator<Item = (Key, WriteOp)> {
        self.ops.into_iter()
    }

    /// Set the commit timestamp for MVCC.
    pub fn set_commit_ts(&mut self, ts: Timestamp) {
        self.commit_ts = Some(ts);
    }

    /// Get the commit timestamp.
    pub fn commit_ts(&self) -> Option<Timestamp> {
        self.commit_ts
    }

    /// Set the CLOG LSN for recovery ordering.
    ///
    /// This is set by TransactionService after writing to the commit log.
    /// The storage layer uses this LSN instead of allocating an independent one,
    /// ensuring clog and storage share the same LSN for proper recovery semantics.
    pub fn set_clog_lsn(&mut self, lsn: u64) {
        self.clog_lsn = Some(lsn);
    }

    /// Get the CLOG LSN if set.
    pub fn clog_lsn(&self) -> Option<u64> {
        self.clog_lsn
    }

    /// Get all keys in this batch.
    pub fn keys(&self) -> impl Iterator<Item = &Key> {
        self.ops.keys()
    }
}

// ============================================================================
// Pessimistic Storage Trait - For Explicit Transactions
// ============================================================================

/// Pessimistic storage operations for explicit transactions.
///
/// This trait extends `StorageEngine` with methods for pessimistic locking:
/// - `put_pending()`: Write a pending value with owner_start_ts
/// - `finalize_pending()`: Convert pending writes to committed on commit
/// - `abort_pending()`: Mark pending writes as aborted on rollback
/// - `get_lock_owner()`: Check if a key is locked
///
/// ## Design
///
/// Pessimistic transactions write pending nodes directly to storage with
/// `owner_start_ts > 0`. The node's `ts` field is 0 until commit, when it's
/// set to `commit_ts`. Readers skip nodes where `owner_start_ts > 0` and
/// `ts == 0` (uncommitted).
///
/// ## Usage
///
/// ```ignore
/// // Explicit transaction: acquire lock on write
/// let result = storage.put_pending(key, value, txn.start_ts);
/// match result {
///     Ok(()) => { /* lock acquired, value written */ },
///     Err(lock_owner) => { /* blocked by another transaction */ },
/// }
///
/// // On commit: finalize all pending writes
/// storage.finalize_pending(&keys, txn.start_ts, commit_ts);
///
/// // On rollback: abort all pending writes
/// storage.abort_pending(&keys, txn.start_ts);
/// ```
pub trait PessimisticStorage: StorageEngine {
    /// Write a pending value for pessimistic transactions.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to write
    /// * `value` - The value to write (use TOMBSTONE for deletes)
    /// * `owner_start_ts` - The transaction's start_ts (used as lock identifier)
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Write successful, lock acquired
    /// * `Err(lock_owner)` - Key is locked by another transaction with start_ts = lock_owner
    ///
    /// If the key is already locked by the same transaction (owner_start_ts matches),
    /// the value is updated in place without error.
    fn put_pending(
        &self,
        key: &[u8],
        value: RawValue,
        owner_start_ts: Timestamp,
    ) -> std::result::Result<(), Timestamp>;

    /// Check if a key is locked by a pending write.
    ///
    /// # Returns
    ///
    /// * `None` - Key is not locked
    /// * `Some(owner_start_ts)` - Key is locked by transaction with this start_ts
    fn get_lock_owner(&self, key: &[u8]) -> Option<Timestamp>;

    /// Finalize all pending writes for a transaction.
    ///
    /// Called during commit. Converts pending nodes to committed by setting
    /// their `ts` field to `commit_ts` and clearing `owner_start_ts`.
    ///
    /// Only affects nodes where `owner_start_ts` matches.
    fn finalize_pending(&self, keys: &[Key], owner_start_ts: Timestamp, commit_ts: Timestamp);

    /// Abort all pending writes for a transaction.
    ///
    /// Called during rollback. Marks pending nodes as aborted so readers skip them.
    /// Does not physically remove nodes to avoid use-after-free issues.
    ///
    /// Only affects nodes where `owner_start_ts` matches.
    fn abort_pending(&self, keys: &[Key], owner_start_ts: Timestamp);
}
