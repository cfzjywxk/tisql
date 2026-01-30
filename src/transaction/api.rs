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

//! Transaction service API definitions.
//!
//! This module defines the unified transaction service interface following
//! OceanBase's design pattern where all transaction operations go through
//! a single service trait.
//!
//! ## Key Design Principles
//!
//! 1. **Unified interface**: All transaction operations in one trait (`TxnService`)
//! 2. **Context-based**: Transaction state is held in `TxnCtx`, passed to operations
//! 3. **No read-only distinction**: Even "reads" may write in distributed txn (lock resolution)
//!
//! ## Usage
//!
//! ```ignore
//! // Begin a transaction
//! let mut ctx = txn_service.begin(false)?;  // read_only = false
//!
//! // Read operations
//! let value = txn_service.get(&ctx, key)?;
//!
//! // Write operations
//! txn_service.put(&mut ctx, key, value)?;
//!
//! // Commit
//! let info = txn_service.commit(ctx)?;
//! ```

use std::ops::Range;

use crate::error::Result;
use crate::storage::WriteBatch;
use crate::types::{Key, Lsn, RawValue, Timestamp, TxnId};

/// Type alias for a boxed scan iterator that yields fallible key-value pairs.
///
/// The inner `Result` allows propagation of I/O or corruption errors that may
/// occur during streaming iteration over storage.
pub type ScanIterator<'a> = Box<dyn Iterator<Item = Result<(Key, RawValue)>> + 'a>;

/// Transaction state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    /// Transaction is active and can perform operations.
    Active,
    /// Transaction has been committed.
    Committed,
    /// Transaction has been aborted/rolled back.
    Aborted,
}

/// Transaction context - holds transaction state.
///
/// Similar to OceanBase's `ObTxDesc`. This struct is passed to all
/// transaction operations on `TxnService`.
///
/// The context is created by `TxnService::begin()` and consumed by
/// `commit()` or `rollback()`.
#[derive(Debug)]
pub struct TxnCtx {
    /// Unique transaction ID.
    pub(crate) txn_id: TxnId,
    /// Start timestamp for MVCC reads.
    pub(crate) start_ts: Timestamp,
    /// Current transaction state.
    pub(crate) state: TxnState,
    /// Whether this is a read-only transaction.
    pub(crate) read_only: bool,
    /// Buffered writes (for read-write transactions).
    pub(crate) write_buffer: WriteBatch,
}

impl TxnCtx {
    /// Create a new transaction context.
    pub(crate) fn new(txn_id: TxnId, start_ts: Timestamp, read_only: bool) -> Self {
        Self {
            txn_id,
            start_ts,
            state: TxnState::Active,
            read_only,
            write_buffer: WriteBatch::new(),
        }
    }

    /// Get the transaction ID.
    #[inline]
    pub fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    /// Get the start timestamp.
    #[inline]
    pub fn start_ts(&self) -> Timestamp {
        self.start_ts
    }

    /// Check if this is a read-only transaction.
    #[inline]
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Check if the transaction is still valid (active).
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.state == TxnState::Active
    }

    /// Get the current state.
    #[inline]
    pub fn state(&self) -> TxnState {
        self.state
    }
}

/// Transaction Service - **THE** interface for all data access operations.
///
/// This follows OceanBase's `ObTransService` pattern where all transaction
/// operations go through a single service interface. Transaction state is
/// held in `TxnCtx` and passed to each operation.
///
/// # IMPORTANT: Always Use TxnService for Reads
///
/// **All reads MUST go through `TxnService`** to ensure proper MVCC semantics.
/// Do not access `StorageEngine` directly for reading data.
///
/// ```ignore
/// // CORRECT: MVCC-aware read with transaction semantics
/// let ctx = txn_service.begin(true)?;  // read-only transaction
/// let value = txn_service.get(&ctx, key)?;
/// let iter = txn_service.scan(&ctx, range)?;
///
/// // WRONG: Bypasses MVCC visibility rules
/// let value = storage.get(key)?;  // DON'T DO THIS
/// ```
///
/// ## Why TxnService for Reads?
///
/// 1. **MVCC Visibility**: Reads see data at `ctx.start_ts`, ignoring uncommitted
///    writes and later commits for snapshot isolation.
///
/// 2. **Lock Checking**: Reads check for conflicting locks from concurrent
///    transactions, returning `KeyIsLocked` error when blocked.
///
/// 3. **Read-Your-Writes**: Within a transaction, reads see buffered writes
///    that haven't been committed yet.
///
/// 4. **Consistent Timestamps**: The `TxnCtx` carries `start_ts` for debugging
///    and ensures all operations in a transaction use the same snapshot.
///
/// ## Design Rationale
///
/// In distributed transactions (2PC), even "read" operations may need to:
/// - Push forward `min_commit_ts` of concurrent writes
/// - Resolve stale locks
///
/// Therefore, there's no fundamental distinction between read-only and
/// read-write transactions at the API level. The `read_only` flag is
/// a hint for optimization, not a hard constraint.
pub trait TxnService: Send + Sync {
    // === Factory ===

    /// Begin a new transaction.
    ///
    /// Allocates a `start_ts` from TSO and returns a transaction context.
    /// If `read_only` is true, write operations will error.
    fn begin(&self, read_only: bool) -> Result<TxnCtx>;

    // === Data Operations ===

    /// Read a key within the transaction.
    ///
    /// For read-write transactions, this checks the write buffer first,
    /// then reads from storage at `start_ts`.
    ///
    /// For read-only transactions, this reads directly from storage.
    fn get(&self, ctx: &TxnCtx, key: &[u8]) -> Result<Option<RawValue>>;

    /// Scan a range of keys within the transaction.
    ///
    /// For read-write transactions, this merges buffered writes with storage,
    /// implementing read-your-writes semantics.
    ///
    /// Returns an iterator over key-value pairs visible at `start_ts`.
    /// Each item is wrapped in `Result` to propagate I/O or corruption errors
    /// that may occur during streaming iteration over storage.
    fn scan(&self, ctx: &TxnCtx, range: Range<Key>) -> Result<ScanIterator<'_>>;

    /// Buffer a put operation.
    ///
    /// The write is not visible until commit.
    ///
    /// # Errors
    ///
    /// - `ReadOnlyTransaction` if the transaction was started with `read_only = true`
    /// - `TransactionNotActive` if the transaction is not in `Active` state
    fn put(&self, ctx: &mut TxnCtx, key: Key, value: RawValue) -> Result<()>;

    /// Buffer a delete operation.
    ///
    /// The delete is not visible until commit.
    ///
    /// # Errors
    ///
    /// - `ReadOnlyTransaction` if the transaction was started with `read_only = true`
    /// - `TransactionNotActive` if the transaction is not in `Active` state
    fn delete(&self, ctx: &mut TxnCtx, key: Key) -> Result<()>;

    // === Finalization ===

    /// Commit the transaction.
    ///
    /// For read-only transactions with no writes, returns immediately.
    /// For read-write transactions:
    /// 1. Acquire in-memory locks
    /// 2. Write to commit log
    /// 3. Apply to storage with commit_ts
    /// 4. Release locks
    fn commit(&self, ctx: TxnCtx) -> Result<CommitInfo>;

    /// Rollback the transaction.
    ///
    /// All buffered writes are discarded.
    fn rollback(&self, ctx: TxnCtx) -> Result<()>;
}

/// Information returned after a successful commit.
#[derive(Debug, Clone)]
pub struct CommitInfo {
    /// Transaction ID
    pub txn_id: TxnId,
    /// Commit timestamp
    pub commit_ts: Timestamp,
    /// Log sequence number (for durability tracking)
    pub lsn: Lsn,
}

/// Transaction isolation levels.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Snapshot Isolation (TiDB default)
    #[default]
    SnapshotIsolation,

    /// Read Committed
    ReadCommitted,

    /// Repeatable Read (MySQL default)
    RepeatableRead,

    /// Serializable
    Serializable,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isolation_level_default() {
        assert_eq!(IsolationLevel::default(), IsolationLevel::SnapshotIsolation);
    }

    #[test]
    fn test_txn_ctx_new() {
        let ctx = TxnCtx::new(1, 100, false);
        assert_eq!(ctx.txn_id(), 1);
        assert_eq!(ctx.start_ts(), 100);
        assert!(!ctx.is_read_only());
        assert!(ctx.is_valid());
        assert_eq!(ctx.state(), TxnState::Active);
    }

    #[test]
    fn test_txn_ctx_read_only() {
        let ctx = TxnCtx::new(2, 200, true);
        assert!(ctx.is_read_only());
        assert!(ctx.is_valid());
    }
}
