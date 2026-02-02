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

//! Transaction service implementation following OceanBase's unified pattern.
//!
//! All transaction operations go through this service, with transaction
//! state held in `TxnCtx` and passed to each operation.

use std::ops::Range;
use std::sync::Arc;

#[cfg(feature = "failpoints")]
use fail::fail_point;

use crate::clog::{ClogEntry, ClogOp, ClogService};
use crate::error::{Result, TiSqlError};
use crate::log_warn;
use crate::tso::TsoService;

use super::api::{CommitInfo, TxnCtx, TxnService, TxnState};
use super::concurrency::ConcurrencyManager;
use crate::storage::mvcc::{is_tombstone, MvccIterator, MvccKey, TOMBSTONE};
use crate::storage::{PessimisticStorage, StorageEngine, WriteBatch, WriteOp};
use crate::types::{Key, RawValue, Timestamp, TxnId};

/// Transaction service manages transactions with durability and MVCC.
///
/// This is the unified interface for all transaction operations, following
/// OceanBase's `ObTransService` pattern. Transaction state is held in `TxnCtx`
/// and passed to each operation.
///
/// All write operations use pessimistic locking via PessimisticStorage:
/// 1. Write pending nodes to storage (locks acquired immediately)
/// 2. On commit: finalize pending nodes with commit_ts
/// 3. On rollback: abort pending nodes
pub struct TransactionService<S: StorageEngine, L: ClogService, T: TsoService> {
    /// Storage engine for state
    storage: Arc<S>,
    /// Commit log service for durability
    clog_service: Arc<L>,
    /// TSO service for timestamp allocation
    tso: Arc<T>,
    /// ConcurrencyManager for lock table and max_ts tracking
    concurrency_manager: Arc<ConcurrencyManager>,
}

impl<S: StorageEngine, L: ClogService, T: TsoService> TransactionService<S, L, T> {
    /// Create a new transaction service
    pub fn new(
        storage: Arc<S>,
        clog_service: Arc<L>,
        tso: Arc<T>,
        concurrency_manager: Arc<ConcurrencyManager>,
    ) -> Self {
        Self {
            storage,
            clog_service,
            tso,
            concurrency_manager,
        }
    }

    /// Get the next timestamp from TSO
    fn get_ts(&self) -> Timestamp {
        let ts = self.tso.get_ts();
        // Update max_ts in concurrency manager for MVCC visibility
        self.concurrency_manager.update_max_ts(ts);
        ts
    }

    /// Get reference to storage engine
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Get reference to TSO service
    pub fn tso(&self) -> &T {
        &self.tso
    }

    /// Execute a write batch with durability guarantees (for direct/autocommit writes).
    ///
    /// Recover state by replaying commit log entries.
    ///
    /// This should be called at startup to rebuild the in-memory state
    /// from the durable commit log.
    pub fn recover(&self, entries: &[ClogEntry]) -> Result<RecoveryStats> {
        let mut stats = RecoveryStats::default();
        let mut max_ts: Timestamp = 0;

        // First pass: identify committed transactions, find max commit_ts,
        // and build txn metadata (commit_ts, commit LSN, and max LSN per txn)
        let mut committed_txns = std::collections::HashSet::new();
        let mut txn_commit_ts: std::collections::HashMap<TxnId, Timestamp> =
            std::collections::HashMap::new();
        let mut txn_commit_lsn: std::collections::HashMap<TxnId, u64> =
            std::collections::HashMap::new();
        let mut txn_max_lsn: std::collections::HashMap<TxnId, u64> =
            std::collections::HashMap::new();

        for entry in entries {
            // Track max LSN per transaction for recovery ordering
            txn_max_lsn
                .entry(entry.txn_id)
                .and_modify(|lsn| *lsn = (*lsn).max(entry.lsn))
                .or_insert(entry.lsn);

            if let ClogOp::Commit { commit_ts } = entry.op {
                committed_txns.insert(entry.txn_id);
                txn_commit_ts.insert(entry.txn_id, commit_ts);
                txn_commit_lsn.insert(entry.txn_id, entry.lsn);
                max_ts = max_ts.max(commit_ts);
                stats.committed_txns += 1;
            }
        }

        // Second pass: apply committed operations with their commit_ts and LSN
        // Also validate per-txn shape: ops should appear before their Commit record
        for entry in entries {
            if !committed_txns.contains(&entry.txn_id) {
                stats.rolled_back_entries += 1;
                continue;
            }

            let commit_ts = *txn_commit_ts.get(&entry.txn_id).unwrap_or(&max_ts);
            // Use the max LSN of the transaction (the commit record's LSN) for proper ordering
            let lsn = *txn_max_lsn.get(&entry.txn_id).unwrap_or(&entry.lsn);

            // Validate per-txn shape: non-commit ops should have LSN < commit LSN
            // This detects potential log corruption where ops appear after commit
            if let Some(&commit_lsn) = txn_commit_lsn.get(&entry.txn_id) {
                if !matches!(entry.op, ClogOp::Commit { .. }) && entry.lsn > commit_lsn {
                    log_warn!(
                        "Recovery: txn {} has op at LSN {} after commit at LSN {} (possible corruption)",
                        entry.txn_id, entry.lsn, commit_lsn
                    );
                    stats.post_commit_ops += 1;
                    // Still apply the op to preserve all data, but flag it
                }
            }

            match &entry.op {
                ClogOp::Put { key, value } => {
                    // Create a write batch with commit_ts and clog LSN
                    let mut batch = WriteBatch::new();
                    batch.put(key.clone(), value.clone());
                    batch.set_commit_ts(commit_ts);
                    batch.set_clog_lsn(lsn);
                    self.storage.write_batch(batch)?;
                    stats.applied_puts += 1;
                }
                ClogOp::Delete { key } => {
                    let mut batch = WriteBatch::new();
                    batch.delete(key.clone());
                    batch.set_commit_ts(commit_ts);
                    batch.set_clog_lsn(lsn);
                    self.storage.write_batch(batch)?;
                    stats.applied_deletes += 1;
                }
                ClogOp::Commit { .. } | ClogOp::Rollback => {
                    // Already processed
                }
            }
        }

        // Update TSO to continue from recovered state
        self.tso.set_ts(max_ts + 1);
        self.concurrency_manager.update_max_ts(max_ts);

        Ok(stats)
    }

    /// Get commit log service reference
    pub fn clog_service(&self) -> &L {
        &self.clog_service
    }

    /// Get concurrency manager reference
    pub fn concurrency_manager(&self) -> &ConcurrencyManager {
        &self.concurrency_manager
    }

    /// Check if transaction is in active state
    fn check_active(ctx: &TxnCtx) -> Result<()> {
        match ctx.state {
            TxnState::Running => Ok(()),
            TxnState::Prepared { .. } => Err(crate::error::TiSqlError::Internal(
                "Transaction already in prepared state".into(),
            )),
            TxnState::Committed { .. } => Err(crate::error::TiSqlError::Internal(
                "Transaction already committed".into(),
            )),
            TxnState::Aborted => Err(crate::error::TiSqlError::Internal(
                "Transaction already aborted".into(),
            )),
        }
    }
}

/// Implement TxnService trait for TransactionService
///
/// Note: `S: PessimisticStorage` is required to support explicit transactions
/// with pessimistic locking. Since `PessimisticStorage: StorageEngine`, this
/// is backwards compatible with code that only uses implicit transactions.
impl<S: PessimisticStorage + 'static, L: ClogService + 'static, T: TsoService> TxnService
    for TransactionService<S, L, T>
{
    type ScanIter = MvccScanIterator<S::Iter>;

    fn begin(&self, read_only: bool) -> Result<TxnCtx> {
        // Allocate txn_id and start_ts from TSO
        let txn_id = self.get_ts();
        let start_ts = self.get_ts();

        Ok(TxnCtx::new(txn_id, start_ts, read_only))
    }

    fn begin_explicit(&self, read_only: bool) -> Result<TxnCtx> {
        // Allocate txn_id and start_ts from TSO
        let txn_id = self.get_ts();
        let start_ts = self.get_ts();

        // Register transaction in state cache for visibility tracking
        self.concurrency_manager.register_txn(start_ts);

        Ok(TxnCtx::new_explicit(txn_id, start_ts, read_only))
    }

    fn get(&self, ctx: &TxnCtx, key: &[u8]) -> Result<Option<RawValue>> {
        // For explicit transactions, use get_with_owner for read-your-writes support.
        // This allows the transaction to see its own pending writes.
        if ctx.is_explicit() {
            let value = self.storage.get_with_owner(key, ctx.start_ts, ctx.start_ts);
            // Filter out tombstones (deleted keys)
            return Ok(value.filter(|v| !is_tombstone(v)));
        }

        // MVCC read: find the latest version with commit_ts <= start_ts
        // Note: Pending/aborted nodes are skipped by the iterator (pessimistic locking in storage)
        //
        // MVCC key encoding: key || !commit_ts (8 bytes big-endian, bitwise NOT)
        // This means higher timestamps produce SMALLER encoded keys.
        //
        // To find visible versions:
        // - Start at MvccKey::encode(key, start_ts) - the smallest MVCC key we'd accept
        // - End at next key in key space at MAX_TS
        // - The first entry with decoded_key == key and entry_ts <= start_ts is our result
        let seek_key = MvccKey::encode(key, ctx.start_ts);
        let end_key = MvccKey::encode(key, 0)
            .next_key()
            .unwrap_or_else(MvccKey::unbounded);

        // Build scan range for MVCC keys
        let scan_range = seek_key..end_key;

        // Use streaming iterator to avoid materializing all entries.
        // We only need the first visible version.
        // For non-explicit transactions, pass 0 as owner_ts (no read-your-writes needed here
        // since explicit transactions use get_with_owner directly above)
        let mut iter = self.storage.scan_iter(scan_range, 0)?;
        iter.advance()?; // Position on first entry

        // Find the first entry matching our key with ts <= start_ts.
        // Use zero-copy user_key()/timestamp() instead of decode() to avoid allocation.
        while iter.valid() {
            let entry_key = iter.user_key();
            let entry_ts = iter.timestamp();

            // Check exact key match and timestamp visibility
            if entry_key == key && entry_ts <= ctx.start_ts {
                let value = iter.value();
                if is_tombstone(value) {
                    return Ok(None); // Key was deleted
                }
                return Ok(Some(value.to_vec()));
            }
            iter.advance()?;
        }

        Ok(None)
    }

    fn scan_iter(&self, ctx: &TxnCtx, range: Range<Key>) -> Result<MvccScanIterator<S::Iter>> {
        // MVCC scan: find the latest version of each key with commit_ts <= start_ts
        //
        // Build MVCC key range:
        // - Start: MvccKey::encode(range.start, MAX) - smallest MVCC key with this prefix
        //   We use Timestamp::MAX because !MAX = 0, producing the smallest suffix.
        //   This ensures we find all keys that start with range.start, even when
        //   range.start is a prefix (like table scan without a specific row key).
        //   Visibility filtering by start_ts happens in find_next_visible().
        // - End: MvccKey::encode(range.end, 0) - largest MVCC key for range.end (exclusive)
        let start_mvcc = MvccKey::encode(&range.start, Timestamp::MAX);
        let end_mvcc = if range.end.is_empty() {
            MvccKey::unbounded()
        } else {
            // Use ts=0 to get the largest MVCC key for range.end (exclusive)
            MvccKey::encode(&range.end, 0)
        };
        let mvcc_range = start_mvcc..end_mvcc;

        // Create storage iterator with owner_ts for read-your-writes support.
        // For explicit transactions, pass start_ts as owner_ts to see pending writes.
        // For implicit transactions, pass 0 (no pending writes to see).
        let owner_ts = if ctx.is_explicit() { ctx.start_ts } else { 0 };
        let storage_iter = self.storage.scan_iter(mvcc_range, owner_ts)?;

        // Create streaming scan iterator that wraps storage with MVCC filtering
        Ok(MvccScanIterator::new(storage_iter, ctx.start_ts, range))
    }

    fn put(&self, ctx: &mut TxnCtx, key: Key, value: RawValue) -> Result<()> {
        if ctx.state != TxnState::Running {
            return Err(TiSqlError::TransactionNotActive(format!("{:?}", ctx.state)));
        }
        if ctx.read_only {
            return Err(TiSqlError::ReadOnlyTransaction);
        }

        if ctx.is_explicit() {
            // Explicit transaction: write pending node directly to storage
            // This acquires the lock immediately (pessimistic locking)
            match self.storage.put_pending(&key, value, ctx.start_ts) {
                Ok(()) => {
                    // Track key for commit/rollback
                    ctx.add_locked_key(key);
                    Ok(())
                }
                Err(lock_owner) => {
                    // Key is locked by another transaction
                    Err(TiSqlError::KeyIsLocked {
                        key,
                        lock_ts: lock_owner,
                        primary: vec![], // Unknown for now
                    })
                }
            }
        } else {
            // Implicit transaction: buffer the write
            ctx.write_buffer.put(key, value);
            Ok(())
        }
    }

    fn delete(&self, ctx: &mut TxnCtx, key: Key) -> Result<()> {
        if ctx.state != TxnState::Running {
            return Err(TiSqlError::TransactionNotActive(format!("{:?}", ctx.state)));
        }
        if ctx.read_only {
            return Err(TiSqlError::ReadOnlyTransaction);
        }

        if ctx.is_explicit() {
            // Explicit transaction: write pending tombstone directly to storage
            // This acquires the lock immediately (pessimistic locking)
            match self
                .storage
                .put_pending(&key, TOMBSTONE.to_vec(), ctx.start_ts)
            {
                Ok(()) => {
                    // Track key for commit/rollback
                    ctx.add_locked_key(key);
                    Ok(())
                }
                Err(lock_owner) => {
                    // Key is locked by another transaction
                    Err(TiSqlError::KeyIsLocked {
                        key,
                        lock_ts: lock_owner,
                        primary: vec![],
                    })
                }
            }
        } else {
            // Implicit transaction: buffer the delete
            ctx.write_buffer.delete(key);
            Ok(())
        }
    }

    fn commit(&self, mut ctx: TxnCtx) -> Result<CommitInfo> {
        Self::check_active(&ctx)?;

        if ctx.is_explicit() {
            // =================================================================
            // Explicit Transaction Commit (Pessimistic Locking)
            // =================================================================
            // Pending writes are already in storage. We just need to:
            // 1. Prepare (compute commit_ts)
            // 2. Finalize pending nodes (set their ts to commit_ts)
            // 3. Update txn state

            let locked_keys = std::mem::take(&mut ctx.locked_keys);

            if locked_keys.is_empty() {
                // No writes - just mark as committed
                self.concurrency_manager.remove_txn(ctx.start_ts);
                ctx.state = TxnState::Committed {
                    commit_ts: ctx.start_ts,
                };
                return Ok(CommitInfo {
                    txn_id: ctx.txn_id,
                    commit_ts: ctx.start_ts,
                    lsn: 0,
                });
            }

            // Get commit_ts: max(max_ts + 1, tso_ts)
            // For OceanBase-style 1PC: prepared_ts = commit_ts
            let max_ts = self.concurrency_manager.max_ts();
            let tso_ts = self.get_ts();
            let commit_ts = std::cmp::max(max_ts + 1, tso_ts);
            let txn_id = ctx.txn_id;

            // Prepare phase: register prepared state for visibility tracking
            self.concurrency_manager
                .prepare_txn(ctx.start_ts, commit_ts)?;

            // Finalize all pending nodes by setting their commit_ts
            // This makes the writes visible to readers with read_ts >= commit_ts
            self.storage
                .finalize_pending(&locked_keys, ctx.start_ts, commit_ts);

            // Update state cache: Prepared -> Committed
            self.concurrency_manager
                .commit_txn(ctx.start_ts, commit_ts)?;

            // Mark context as committed
            ctx.state = TxnState::Committed { commit_ts };

            // Note: For now, explicit transactions don't write to clog.
            // TODO: Add clog support for explicit transactions
            Ok(CommitInfo {
                txn_id,
                commit_ts,
                lsn: 0,
            })
        } else {
            // =================================================================
            // Implicit Transaction Commit (Optimistic Locking)
            // =================================================================
            // Writes are buffered. We need to:
            // 1. Acquire in-memory locks
            // 2. Write to clog
            // 3. Apply to storage

            if ctx.write_buffer.is_empty() {
                // No writes - just mark as committed
                ctx.state = TxnState::Committed {
                    commit_ts: ctx.start_ts,
                };
                return Ok(CommitInfo {
                    txn_id: ctx.txn_id,
                    commit_ts: ctx.start_ts,
                    lsn: 0,
                });
            }

            let txn_id = ctx.txn_id;
            let start_ts = ctx.start_ts;

            // Take ownership of write buffer (swap with empty)
            let storage_batch = std::mem::take(&mut ctx.write_buffer);

            // Phase 1: Acquire locks by writing pending nodes to storage.
            // This prevents concurrent writers from conflicting.
            let mut locked_keys: Vec<Key> = Vec::with_capacity(storage_batch.len());

            for (key, op) in storage_batch.iter() {
                let value = match op {
                    WriteOp::Put { value } => value.clone(),
                    WriteOp::Delete => TOMBSTONE.to_vec(),
                };

                match self.storage.put_pending(key, value, start_ts) {
                    Ok(()) => {
                        locked_keys.push(key.clone());
                    }
                    Err(lock_owner) => {
                        // Conflict - abort all pending writes we made
                        if !locked_keys.is_empty() {
                            self.storage.abort_pending(&locked_keys, start_ts);
                        }
                        return Err(TiSqlError::KeyIsLocked {
                            key: key.clone(),
                            lock_ts: lock_owner,
                            primary: vec![],
                        });
                    }
                }
            }

            // FAILPOINT: After locks acquired, before commit_ts computation
            #[cfg(feature = "failpoints")]
            fail_point!("txn_after_lock_before_commit_ts");

            // Phase 2: Get commit_ts AFTER acquiring locks
            let max_ts = self.concurrency_manager.max_ts();
            let tso_ts = self.get_ts();
            let commit_ts = std::cmp::max(max_ts + 1, tso_ts);

            // Write to commit log before finalizing (for durability)
            let lsn = self
                .clog_service
                .write_batch(txn_id, &storage_batch, commit_ts, true)?;

            // Phase 3: Finalize all pending nodes with commit_ts
            self.storage
                .finalize_pending(&locked_keys, start_ts, commit_ts);

            // Mark as committed
            ctx.state = TxnState::Committed { commit_ts };

            Ok(CommitInfo {
                txn_id,
                commit_ts,
                lsn,
            })
        }
    }

    fn rollback(&self, mut ctx: TxnCtx) -> Result<()> {
        Self::check_active(&ctx)?;

        if ctx.is_explicit() {
            // =================================================================
            // Explicit Transaction Rollback (Pessimistic Locking)
            // =================================================================
            // Pending writes are in storage. We need to:
            // 1. Abort pending nodes (mark as aborted so readers skip them)
            // 2. Update txn state cache

            let locked_keys = std::mem::take(&mut ctx.locked_keys);

            // Abort all pending nodes in storage
            if !locked_keys.is_empty() {
                self.storage.abort_pending(&locked_keys, ctx.start_ts);
            }

            // Update state cache: Running -> Aborted, then remove
            self.concurrency_manager.abort_txn(ctx.start_ts)?;
            self.concurrency_manager.remove_txn(ctx.start_ts);

            // Mark context as aborted
            ctx.state = TxnState::Aborted;
        } else {
            // =================================================================
            // Implicit Transaction Rollback (Optimistic Locking)
            // =================================================================
            // Writes are only in the buffer, not in storage

            // Clear write buffer
            ctx.write_buffer.clear();

            // Mark as aborted
            ctx.state = TxnState::Aborted;
        }

        Ok(())
    }
}

// ============================================================================
// MvccScanIterator - Transaction layer wrapper for storage iterator
// ============================================================================

/// Streaming MVCC scan iterator wrapping the storage iterator.
///
/// This is a thin wrapper that provides MVCC visibility filtering:
/// - Filters entries by `read_ts` (only entries with `ts <= read_ts` are visible)
/// - Filters out tombstones (deleted keys)
/// - Skips older versions of the same key (returns only latest visible version)
///
/// ## Iterator Pattern
///
/// Construction does NOT position the iterator. Call `advance()` first:
///
/// ```ignore
/// let mut iter = scan_iter(ctx, range)?;
/// iter.advance()?;  // Position on first entry
/// while iter.valid() {
///     let key = iter.user_key();
///     let value = iter.value();
///     iter.advance()?;
/// }
/// ```
///
/// ## Error Handling
///
/// Storage errors (I/O, corruption) are propagated through `advance()`.
/// After an error, `valid()` returns false.
///
/// ## Zero-Copy Design
///
/// Uses lazy skipping to avoid value cloning: after finding a visible entry,
/// the storage iterator stays positioned on it. Older versions are skipped
/// at the start of the next `advance()` call. This allows `user_key()`,
/// `timestamp()`, and `value()` to return references directly from storage.
pub struct MvccScanIterator<I: MvccIterator> {
    /// Storage iterator producing MVCC key-value pairs
    storage_iter: I,
    /// Transaction's read timestamp for MVCC filtering
    read_ts: Timestamp,
    /// Key range for filtering
    range: Range<Key>,
    /// Key to skip on next advance (older versions of last returned key).
    /// Also used for tombstone skipping within the same advance call.
    last_returned_key: Option<Vec<u8>>,
    /// Whether positioned on a valid entry (storage_iter points to it)
    is_positioned: bool,
}

impl<I: MvccIterator> MvccScanIterator<I> {
    /// Create a new scan iterator.
    ///
    /// The iterator is NOT positioned after construction. Call `advance()`
    /// to position on the first entry.
    ///
    /// # Arguments
    ///
    /// * `storage_iter` - Iterator over storage (MVCC key-value pairs)
    /// * `read_ts` - Transaction's read timestamp for MVCC visibility
    /// * `range` - Key range for filtering
    pub fn new(storage_iter: I, read_ts: Timestamp, range: Range<Key>) -> Self {
        Self {
            storage_iter,
            read_ts,
            range,
            last_returned_key: None,
            is_positioned: false,
        }
    }

    /// Find the next visible entry from storage.
    ///
    /// Uses lazy skipping: older versions of the previously returned key are
    /// skipped at the start of this call (not at the end of the previous call).
    /// This allows `user_key()`, `timestamp()`, and `value()` to return
    /// references directly from the storage iterator without caching.
    ///
    /// After finding a visible entry, the storage iterator stays positioned on it.
    /// The key is recorded in `last_returned_key` for skipping on the next call.
    fn find_next_visible(&mut self) -> crate::error::Result<()> {
        // Lazy skip: skip older versions of the previously returned key
        if let Some(ref skip_key) = self.last_returned_key {
            while self.storage_iter.valid() && self.storage_iter.user_key() == skip_key.as_slice() {
                self.storage_iter.advance()?;
            }
        }
        self.last_returned_key = None;
        self.is_positioned = false;

        // Initialize storage iterator if needed
        if !self.storage_iter.valid() {
            self.storage_iter.advance()?;
        }

        while self.storage_iter.valid() {
            let user_key = self.storage_iter.user_key();
            let ts = self.storage_iter.timestamp();

            // Check if past end of range
            if !self.range.end.is_empty() && user_key >= self.range.end.as_slice() {
                return Ok(()); // is_positioned stays false
            }

            // Skip if before start of range
            if user_key < self.range.start.as_slice() {
                self.storage_iter.advance()?;
                continue;
            }

            // Skip if not visible at read_ts
            if ts > self.read_ts {
                self.storage_iter.advance()?;
                continue;
            }

            // Found a visible entry - check if tombstone
            if is_tombstone(self.storage_iter.value()) {
                // Tombstone: skip this key and all older versions immediately
                // (we don't return tombstones, so no lazy skip needed)
                let skip_key = user_key.to_vec();
                self.storage_iter.advance()?;
                while self.storage_iter.valid()
                    && self.storage_iter.user_key() == skip_key.as_slice()
                {
                    self.storage_iter.advance()?;
                }
                continue;
            }

            // Found a visible, non-tombstone entry
            // Record key for lazy skip on next advance, keep iterator positioned here
            self.last_returned_key = Some(user_key.to_vec());
            self.is_positioned = true;
            return Ok(());
        }

        Ok(()) // is_positioned stays false - exhausted
    }
}

impl<I: MvccIterator> MvccIterator for MvccScanIterator<I> {
    fn seek(&mut self, _target: &MvccKey) -> crate::error::Result<()> {
        // MvccScanIterator is created for a specific range.
        // Seeking within the result is not supported - this is a forward-only iterator.
        // The iterator should be created with the appropriate range instead.
        Ok(())
    }

    fn advance(&mut self) -> crate::error::Result<()> {
        self.find_next_visible()
    }

    fn valid(&self) -> bool {
        self.is_positioned
    }

    fn user_key(&self) -> &[u8] {
        debug_assert!(self.is_positioned, "user_key() called on invalid iterator");
        self.storage_iter.user_key()
    }

    fn timestamp(&self) -> Timestamp {
        debug_assert!(self.is_positioned, "timestamp() called on invalid iterator");
        self.storage_iter.timestamp()
    }

    fn value(&self) -> &[u8] {
        debug_assert!(self.is_positioned, "value() called on invalid iterator");
        self.storage_iter.value()
    }
}

/// Statistics from recovery
#[derive(Default, Debug)]
pub struct RecoveryStats {
    /// Number of committed transactions
    pub committed_txns: u64,
    /// Number of put operations applied
    pub applied_puts: u64,
    /// Number of delete operations applied
    pub applied_deletes: u64,
    /// Number of entries from uncommitted transactions (rolled back)
    pub rolled_back_entries: u64,
    /// Number of ops that appeared after their txn's Commit record (possible corruption)
    pub post_commit_ops: u64,
}

impl RecoveryStats {
    /// Total operations applied
    pub fn total_applied(&self) -> u64 {
        self.applied_puts + self.applied_deletes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clog::{FileClogConfig, FileClogService};
    use crate::storage::MemTableEngine;
    use crate::tso::LocalTso;
    use tempfile::tempdir;

    type TestStorage = MemTableEngine;

    fn create_test_service() -> (
        Arc<TestStorage>,
        TransactionService<TestStorage, FileClogService, LocalTso>,
        tempfile::TempDir,
    ) {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let clog_service = Arc::new(FileClogService::open(config).unwrap());
        let tso = Arc::new(LocalTso::new(1));
        let cm = Arc::new(ConcurrencyManager::new(0));
        let storage = Arc::new(MemTableEngine::new());
        let txn_service = TransactionService::new(Arc::clone(&storage), clog_service, tso, cm);
        (storage, txn_service, dir)
    }

    // ========================================================================
    // Test Helpers Using MvccKey
    // ========================================================================

    fn get_at_for_test<S: StorageEngine>(
        storage: &S,
        key: &[u8],
        ts: Timestamp,
    ) -> Option<RawValue> {
        let seek_key = MvccKey::encode(key, ts);
        let end_key = MvccKey::encode(key, 0)
            .next_key()
            .unwrap_or_else(MvccKey::unbounded);
        let range = seek_key..end_key;

        let mut iter = storage.scan_iter(range, 0).unwrap();
        iter.advance().unwrap(); // Position on first entry

        while iter.valid() {
            let decoded_key = iter.user_key();
            let entry_ts = iter.timestamp();
            let value = iter.value();
            if decoded_key == key && entry_ts <= ts {
                if is_tombstone(value) {
                    return None;
                }
                return Some(value.to_vec());
            }
            iter.advance().unwrap();
        }
        None
    }

    fn get_for_test<S: StorageEngine>(storage: &S, key: &[u8]) -> Option<RawValue> {
        get_at_for_test(storage, key, Timestamp::MAX)
    }

    // Import test extension trait for autocommit helpers
    use crate::testkit::TxnServiceTestExt;

    #[test]
    fn test_autocommit_put() {
        let (storage, txn_service, _dir) = create_test_service();

        // Execute writes using proper transaction flow
        let info = txn_service.autocommit_put(b"key1", b"value1").unwrap();
        assert!(info.txn_id > 0);
        assert!(info.commit_ts > 0);
        assert!(info.lsn > 0);

        txn_service.autocommit_put(b"key2", b"value2").unwrap();

        // Verify storage
        let v1 = get_for_test(&*storage, b"key1");
        assert_eq!(v1, Some(b"value1".to_vec()));
        let v2 = get_for_test(&*storage, b"key2");
        assert_eq!(v2, Some(b"value2".to_vec()));
    }

    #[test]
    fn test_recover() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        // Write some data
        {
            let clog_service = Arc::new(FileClogService::open(config.clone()).unwrap());
            let tso = Arc::new(LocalTso::new(1));
            let cm = Arc::new(ConcurrencyManager::new(0));
            let storage = Arc::new(MemTableEngine::new());
            let txn_service = TransactionService::new(storage, clog_service.clone(), tso, cm);

            txn_service.autocommit_put(b"k1", b"v1").unwrap();
            txn_service.autocommit_put(b"k2", b"v2").unwrap();
            txn_service.autocommit_delete(b"k1").unwrap();

            clog_service.close().unwrap();
        }

        // Recover
        let (clog_service, entries) = FileClogService::recover(config).unwrap();
        let clog_service = Arc::new(clog_service);
        let tso = Arc::new(LocalTso::new(1));
        let cm = Arc::new(ConcurrencyManager::new(0));
        let storage = Arc::new(MemTableEngine::new());
        let txn_service = TransactionService::new(Arc::clone(&storage), clog_service, tso, cm);

        let stats = txn_service.recover(&entries).unwrap();
        assert_eq!(stats.committed_txns, 3);
        assert_eq!(stats.applied_puts, 2);
        assert_eq!(stats.applied_deletes, 1);

        // Verify recovered state
        assert!(get_for_test(&*storage, b"k1").is_none()); // Was deleted
        assert_eq!(get_for_test(&*storage, b"k2"), Some(b"v2".to_vec()));

        // Verify TSO advanced (use tso().last_ts() to check state)
        assert!(txn_service.tso().last_ts() > 3);
    }

    #[test]
    fn test_mvcc_versions_after_writes() {
        let (storage, txn_service, _dir) = create_test_service();

        // Write v1
        let info1 = txn_service.autocommit_put(b"key", b"v1").unwrap();
        let ts1 = info1.commit_ts;

        // Write v2
        let info2 = txn_service.autocommit_put(b"key", b"v2").unwrap();
        let ts2 = info2.commit_ts;

        // Reading at latest should see v2
        let v = get_for_test(&*storage, b"key");
        assert_eq!(v, Some(b"v2".to_vec()));

        // Reading at ts1 should see v1 (MVCC visibility)
        let v = get_at_for_test(&*storage, b"key", ts1);
        assert_eq!(v, Some(b"v1".to_vec()));

        // Reading at ts2 should see v2
        let v = get_at_for_test(&*storage, b"key", ts2);
        assert_eq!(v, Some(b"v2".to_vec()));
    }

    // ========================================================================
    // TxnService trait tests (new unified API)
    // ========================================================================

    #[test]
    fn test_txn_service_begin() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Begin a read-write transaction
        let ctx = txn_service.begin(false).unwrap();

        // Transaction should have valid timestamp
        assert!(ctx.start_ts() > 0);
        assert!(ctx.is_valid());
        assert!(!ctx.is_read_only());
    }

    #[test]
    fn test_txn_service_begin_read_only() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Begin a read-only transaction
        let ctx = txn_service.begin(true).unwrap();

        assert!(ctx.start_ts() > 0);
        assert!(ctx.is_valid());
        assert!(ctx.is_read_only());
    }

    #[test]
    fn test_txn_service_put_commit() {
        let (storage, txn_service, _dir) = create_test_service();

        let mut ctx = txn_service.begin(false).unwrap();

        // Put via transaction
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();

        // Not yet in storage (buffered, uncommitted)
        let storage_value = get_for_test(&*storage, b"key1");
        assert!(storage_value.is_none());

        // Commit
        let info = txn_service.commit(ctx).unwrap();
        assert!(info.commit_ts > 0);
        assert!(info.lsn > 0);

        // Now visible in storage
        let storage_value = get_for_test(&*storage, b"key1");
        assert_eq!(storage_value, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_txn_service_delete_commit() {
        let (storage, txn_service, _dir) = create_test_service();

        // First commit a value
        txn_service.autocommit_put(b"key1", b"value1").unwrap();
        assert_eq!(get_for_test(&*storage, b"key1"), Some(b"value1".to_vec()));

        // Delete via transaction
        let mut ctx = txn_service.begin(false).unwrap();
        txn_service.delete(&mut ctx, b"key1".to_vec()).unwrap();
        txn_service.commit(ctx).unwrap();

        // Should be deleted in storage
        assert!(get_for_test(&*storage, b"key1").is_none());
    }

    #[test]
    fn test_txn_service_rollback() {
        let (storage, txn_service, _dir) = create_test_service();

        let mut ctx = txn_service.begin(false).unwrap();

        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();

        // Rollback
        txn_service.rollback(ctx).unwrap();

        // Data should NOT be in storage
        let value = get_for_test(&*storage, b"key1");
        assert!(value.is_none());
    }

    #[test]
    fn test_txn_service_snapshot_isolation() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Write initial data
        txn_service.autocommit_put(b"key", b"v1").unwrap();

        // Begin transaction
        let ctx = txn_service.begin(true).unwrap();

        // Transaction should see v1
        let value = txn_service.get(&ctx, b"key").unwrap();
        assert_eq!(value, Some(b"v1".to_vec()));

        // Write v2 via autocommit (outside the transaction)
        txn_service.autocommit_put(b"key", b"v2").unwrap();

        // Transaction should still see v1 (snapshot isolation)
        let value = txn_service.get(&ctx, b"key").unwrap();
        assert_eq!(value, Some(b"v1".to_vec()));

        // But reading via storage should see v2
        let latest = get_for_test(txn_service.storage(), b"key");
        assert_eq!(latest, Some(b"v2".to_vec()));
    }

    #[test]
    fn test_txn_service_read_only_rejects_writes() {
        let (storage, txn_service, _dir) = create_test_service();

        let mut ctx = txn_service.begin(true).unwrap();

        // Put should error for read-only transaction
        let result = txn_service.put(&mut ctx, b"key1".to_vec(), b"value1".to_vec());
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TiSqlError::ReadOnlyTransaction
        ));

        // Delete should also error
        let result = txn_service.delete(&mut ctx, b"key1".to_vec());
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TiSqlError::ReadOnlyTransaction
        ));

        // Buffer should be empty
        assert!(ctx.write_buffer.is_empty());

        // Commit should succeed (no-op for read-only)
        let info = txn_service.commit(ctx).unwrap();
        assert_eq!(info.lsn, 0); // No log written

        // Nothing in storage
        let value = get_for_test(&*storage, b"key1");
        assert!(value.is_none());
    }

    #[test]
    fn test_txn_service_scan_mvcc() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Write some data via autocommit
        txn_service.autocommit_put(b"a", b"1").unwrap();
        txn_service.autocommit_put(b"b", b"2").unwrap();
        txn_service.autocommit_put(b"c", b"3").unwrap();

        // Begin a read-only transaction
        let ctx = txn_service.begin(true).unwrap();

        // Scan should see all data
        let mut iter = txn_service
            .scan_iter(&ctx, b"a".to_vec()..b"d".to_vec())
            .unwrap();
        let mut results = Vec::new();
        iter.advance().unwrap();
        while iter.valid() {
            results.push((iter.user_key().to_vec(), iter.value().to_vec()));
            iter.advance().unwrap();
        }

        assert_eq!(
            results.len(),
            3,
            "scan should see 3 keys, start_ts={}, got {:?}",
            ctx.start_ts(),
            results
        );
    }

    #[test]
    fn test_scan_storage_only() {
        // Test: Storage has data, scan returns it
        let (_storage, txn_service, _dir) = create_test_service();

        // Commit some data first
        txn_service.autocommit_put(b"m", b"m_value").unwrap();
        txn_service.autocommit_put(b"n", b"n_value").unwrap();
        txn_service.autocommit_put(b"o", b"o_value").unwrap();

        // Begin a read-only transaction
        let ctx = txn_service.begin(true).unwrap();

        // Scan should return storage data
        let mut iter = txn_service
            .scan_iter(&ctx, b"m".to_vec()..b"p".to_vec())
            .unwrap();
        let mut results = Vec::new();
        iter.advance().unwrap();
        while iter.valid() {
            results.push((iter.user_key().to_vec(), iter.value().to_vec()));
            iter.advance().unwrap();
        }

        assert_eq!(results.len(), 3, "should see all 3 storage keys");
        assert_eq!(results[0], (b"m".to_vec(), b"m_value".to_vec()));
        assert_eq!(results[1], (b"n".to_vec(), b"n_value".to_vec()));
        assert_eq!(results[2], (b"o".to_vec(), b"o_value".to_vec()));
    }

    #[test]
    fn test_scan_empty_range() {
        // Test: Empty range returns nothing
        let (_storage, txn_service, _dir) = create_test_service();

        // Commit data outside range
        txn_service.autocommit_put(b"aaa", b"before").unwrap();
        txn_service.autocommit_put(b"zzz", b"after").unwrap();

        let ctx = txn_service.begin(true).unwrap();

        // Scan a range that has no data
        let mut iter = txn_service
            .scan_iter(&ctx, b"mmm".to_vec()..b"nnn".to_vec())
            .unwrap();
        iter.advance().unwrap();
        assert!(!iter.valid(), "should be empty - no data in range");
    }

    #[test]
    fn test_scan_range_boundaries() {
        // Test: Keys exactly at range boundaries
        let (_storage, txn_service, _dir) = create_test_service();

        // Storage: "aaa", "bbb", "ccc" (lexicographically ordered)
        txn_service.autocommit_put(b"aaa", b"a_val").unwrap();
        txn_service.autocommit_put(b"bbb", b"b_val").unwrap();
        txn_service.autocommit_put(b"ccc", b"c_val").unwrap();

        let ctx = txn_service.begin(true).unwrap();

        // Range [aaa, ccc) - includes "aaa", excludes "ccc"
        let mut iter = txn_service
            .scan_iter(&ctx, b"aaa".to_vec()..b"ccc".to_vec())
            .unwrap();
        let mut results = Vec::new();
        iter.advance().unwrap();
        while iter.valid() {
            results.push((iter.user_key().to_vec(), iter.value().to_vec()));
            iter.advance().unwrap();
        }

        assert_eq!(results.len(), 2, "should see aaa and bbb only");
        assert_eq!(results[0], (b"aaa".to_vec(), b"a_val".to_vec()));
        assert_eq!(results[1], (b"bbb".to_vec(), b"b_val".to_vec()));
    }

    #[test]
    fn test_tso_advances_on_begin() {
        let (_storage, txn_service, _dir) = create_test_service();

        let ts1 = txn_service.tso().last_ts();

        // Each begin advances the timestamp (allocates txn_id and start_ts)
        let _ctx = txn_service.begin(false).unwrap();
        let ts2 = txn_service.tso().last_ts();
        assert!(ts2 > ts1);

        // Each begin also advances the timestamp
        let _ctx2 = txn_service.begin(true).unwrap();
        let ts3 = txn_service.tso().last_ts();
        assert!(ts3 > ts2);
    }

    // ========================================================================
    // Error Propagation Tests for MvccScanIterator
    // ========================================================================

    /// Mock MvccIterator that injects an error after N successful iterations.
    struct ErrorInjectingIterator {
        entries: Vec<(MvccKey, Vec<u8>)>,
        position: usize,
        error_after: usize,
        has_errored: bool,
    }

    impl ErrorInjectingIterator {
        fn new(entries: Vec<(MvccKey, Vec<u8>)>, error_after: usize) -> Self {
            Self {
                entries,
                position: 0,
                error_after,
                has_errored: false,
            }
        }

        fn current(&self) -> Option<&(MvccKey, Vec<u8>)> {
            self.entries.get(self.position)
        }
    }

    impl MvccIterator for ErrorInjectingIterator {
        fn seek(&mut self, _target: &MvccKey) -> Result<()> {
            self.position = 0;
            Ok(())
        }

        fn advance(&mut self) -> Result<()> {
            if self.has_errored {
                return Ok(());
            }

            // Increment first to move past current entry
            self.position += 1;

            // Inject error after specified number of successful advance() calls
            if self.position > self.error_after && !self.has_errored {
                self.has_errored = true;
                return Err(TiSqlError::Storage(
                    "simulated I/O error during scan".to_string(),
                ));
            }
            Ok(())
        }

        fn valid(&self) -> bool {
            !self.has_errored && self.position < self.entries.len()
        }

        fn user_key(&self) -> &[u8] {
            self.current()
                .expect("user_key() called on invalid iterator")
                .0
                .key()
        }

        fn timestamp(&self) -> Timestamp {
            self.current()
                .expect("timestamp() called on invalid iterator")
                .0
                .timestamp()
        }

        fn value(&self) -> &[u8] {
            &self
                .current()
                .expect("value() called on invalid iterator")
                .1
        }
    }

    /// Helper to collect scan results using streaming API
    fn collect_scan_results<I: MvccIterator>(
        mut iter: MvccScanIterator<I>,
    ) -> Vec<Result<(Key, RawValue)>> {
        let mut results = Vec::new();
        loop {
            match MvccIterator::advance(&mut iter) {
                Ok(()) => {
                    if !iter.valid() {
                        break;
                    }
                    results.push(Ok((iter.user_key().to_vec(), iter.value().to_vec())));
                }
                Err(e) => {
                    results.push(Err(e));
                    break;
                }
            }
        }
        results
    }

    #[test]
    fn test_mvcc_scan_iterator_propagates_storage_error() {
        // Create test data: 3 entries (a@5, b@5, c@5)
        let entries = vec![
            (MvccKey::encode(b"a", 5), b"value_a".to_vec()),
            (MvccKey::encode(b"b", 5), b"value_b".to_vec()),
            (MvccKey::encode(b"c", 5), b"value_c".to_vec()),
        ];

        // Error after 1 successful advance() call on mock.
        // Lazy skip design:
        // - First find_next_visible: position=0, return "a" (no advance needed to read first entry)
        // - Second find_next_visible: lazy skip "a" (advance 0→1, ok), return "b"
        // - Third find_next_visible: lazy skip "b" (advance 1→2, 2>1=ERROR!)
        // Result: 2 Ok entries + 1 Err
        let mock_iter = ErrorInjectingIterator::new(entries, 1);

        let scan_iter = MvccScanIterator::new(
            mock_iter,
            10, // read_ts
            b"a".to_vec()..b"z".to_vec(),
        );

        let results = collect_scan_results(scan_iter);

        // Should have 2 Ok entries followed by error
        assert_eq!(results.len(), 3, "expected 3 items: 2 Ok + 1 Err");

        // First two results should be Ok
        assert!(results[0].is_ok(), "first result should be Ok");
        assert!(results[1].is_ok(), "second result should be Ok");

        // Third result should be Err
        assert!(results[2].is_err(), "third result should be Err");
        let err = results[2].as_ref().unwrap_err();
        assert!(
            matches!(err, TiSqlError::Storage(msg) if msg.contains("simulated I/O error")),
            "expected Storage error, got {err:?}"
        );
    }

    #[test]
    fn test_mvcc_scan_iterator_error_on_first_advance() {
        // Create test data
        let entries = vec![
            (MvccKey::encode(b"a", 5), b"value_a".to_vec()),
            (MvccKey::encode(b"b", 5), b"value_b".to_vec()),
        ];

        // Error after 0 successful advance() calls on mock.
        // Lazy skip design:
        // - First find_next_visible: position=0, return "a" (no advance needed)
        // - Second find_next_visible: lazy skip "a" (advance 0→1, 1>0=ERROR!)
        // Result: 1 Ok entry + 1 Err
        let mock_iter = ErrorInjectingIterator::new(entries, 0);

        let scan_iter = MvccScanIterator::new(mock_iter, 10, b"a".to_vec()..b"z".to_vec());

        let results = collect_scan_results(scan_iter);

        // One successful entry, then error when trying to advance
        assert_eq!(results.len(), 2, "expected 2 items: 1 Ok + 1 Err");
        assert!(results[0].is_ok(), "first should be Ok");
        assert!(results[1].is_err(), "second should be error");
    }

    #[test]
    fn test_mvcc_scan_iterator_error_after_all_entries() {
        // Create test data
        let entries = vec![
            (MvccKey::encode(b"a", 5), b"value_a".to_vec()),
            (MvccKey::encode(b"b", 5), b"value_b".to_vec()),
        ];

        // Error after 10 successful next() calls - won't trigger (only 2 entries)
        let mock_iter = ErrorInjectingIterator::new(entries, 10);

        let scan_iter = MvccScanIterator::new(mock_iter, 10, b"a".to_vec()..b"z".to_vec());

        let results = collect_scan_results(scan_iter);

        // All entries should succeed
        assert_eq!(results.len(), 2, "expected 2 successful entries");
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());
    }

    #[test]
    fn test_mvcc_scan_iterator_no_silent_truncation() {
        // This test verifies the original bug is fixed:
        // Previously, errors would cause silent truncation (returning None)
        // Now, errors must be explicitly returned to the caller

        let entries = vec![
            (MvccKey::encode(b"a", 5), b"value_a".to_vec()),
            (MvccKey::encode(b"b", 5), b"value_b".to_vec()),
            (MvccKey::encode(b"c", 5), b"value_c".to_vec()),
        ];

        // Error after 1 successful advance() call on mock.
        // Lazy skip: return "a" (no advance), lazy skip "a" (advance #1 ok) return "b",
        //            lazy skip "b" (advance #2, 2>1=ERROR)
        // Result: 2 successful entries before error
        let mock_iter = ErrorInjectingIterator::new(entries.clone(), 1);

        let scan_iter = MvccScanIterator::new(mock_iter, 10, b"a".to_vec()..b"z".to_vec());

        let results = collect_scan_results(scan_iter);

        // Count successful and error results
        let successful_count = results.iter().filter(|r| r.is_ok()).count();
        let saw_error = results.iter().any(|r| r.is_err());

        // Verify error is a Storage error
        if let Some(Err(e)) = results.iter().find(|r| r.is_err()) {
            assert!(matches!(e, TiSqlError::Storage(_)));
        }

        // CRITICAL: We must have seen the error, not silent truncation
        assert!(
            saw_error,
            "error must be propagated to caller, not silently dropped"
        );
        assert_eq!(
            successful_count, 2,
            "should have exactly 2 successful entries before error"
        );
    }

    // ========================================================================
    // Explicit Transaction Tests (Pessimistic Locking)
    // ========================================================================

    #[test]
    fn test_explicit_txn_begin() {
        let (_storage, txn_service, _dir) = create_test_service();

        let ctx = txn_service.begin_explicit(false).unwrap();

        assert!(ctx.start_ts() > 0);
        assert!(ctx.is_valid());
        assert!(!ctx.is_read_only());
        assert!(ctx.is_explicit());
    }

    #[test]
    fn test_explicit_txn_put_commit() {
        let (storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Put via explicit transaction (pessimistic: writes go to storage immediately)
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();

        // Key should be tracked
        assert_eq!(ctx.locked_keys.len(), 1);
        assert_eq!(ctx.locked_keys[0], b"key1".to_vec());

        // Commit
        let info = txn_service.commit(ctx).unwrap();
        assert!(info.commit_ts > 0);

        // Data should be visible in storage
        let value = get_for_test(&*storage, b"key1");
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_explicit_txn_put_rollback() {
        let (storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Put via explicit transaction
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();

        // Rollback
        txn_service.rollback(ctx).unwrap();

        // Data should NOT be visible in storage (pending node is aborted)
        let value = get_for_test(&*storage, b"key1");
        assert!(value.is_none());
    }

    #[test]
    fn test_explicit_txn_delete_commit() {
        let (storage, txn_service, _dir) = create_test_service();

        // First commit a value (via implicit txn)
        txn_service.autocommit_put(b"key1", b"value1").unwrap();
        assert_eq!(get_for_test(&*storage, b"key1"), Some(b"value1".to_vec()));

        // Start explicit transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Delete via explicit transaction
        txn_service.delete(&mut ctx, b"key1".to_vec()).unwrap();

        // Commit
        txn_service.commit(ctx).unwrap();

        // Data should be deleted
        let value = get_for_test(&*storage, b"key1");
        assert!(value.is_none());
    }

    #[test]
    fn test_explicit_txn_multiple_writes() {
        let (storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Multiple puts
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        txn_service
            .put(&mut ctx, b"key2".to_vec(), b"value2".to_vec())
            .unwrap();
        txn_service
            .put(&mut ctx, b"key3".to_vec(), b"value3".to_vec())
            .unwrap();

        // All keys should be tracked
        assert_eq!(ctx.locked_keys.len(), 3);

        // Commit
        txn_service.commit(ctx).unwrap();

        // All data should be visible
        assert_eq!(get_for_test(&*storage, b"key1"), Some(b"value1".to_vec()));
        assert_eq!(get_for_test(&*storage, b"key2"), Some(b"value2".to_vec()));
        assert_eq!(get_for_test(&*storage, b"key3"), Some(b"value3".to_vec()));
    }

    #[test]
    fn test_explicit_txn_update_same_key() {
        let (storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Write key twice within same transaction
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"v1".to_vec())
            .unwrap();
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"v2".to_vec())
            .unwrap();

        // Key should only be tracked once
        assert_eq!(ctx.locked_keys.len(), 1);

        // Commit
        txn_service.commit(ctx).unwrap();

        // Should see latest value
        let value = get_for_test(&*storage, b"key1");
        assert_eq!(value, Some(b"v2".to_vec()));
    }

    #[test]
    fn test_explicit_txn_read_only_rejects_writes() {
        let (_storage, txn_service, _dir) = create_test_service();

        let mut ctx = txn_service.begin_explicit(true).unwrap();

        // Put should error for read-only explicit transaction
        let result = txn_service.put(&mut ctx, b"key1".to_vec(), b"value1".to_vec());
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TiSqlError::ReadOnlyTransaction
        ));

        // No keys should be locked
        assert!(ctx.locked_keys.is_empty());

        // Commit should succeed (no-op for read-only)
        let info = txn_service.commit(ctx).unwrap();
        assert_eq!(info.lsn, 0);
    }

    #[test]
    fn test_explicit_txn_empty_commit() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction but don't write anything
        let ctx = txn_service.begin_explicit(false).unwrap();
        assert!(ctx.locked_keys.is_empty());

        // Commit should succeed (no-op)
        let info = txn_service.commit(ctx).unwrap();
        assert_eq!(info.lsn, 0);
    }

    #[test]
    fn test_explicit_txn_empty_rollback() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction but don't write anything
        let ctx = txn_service.begin_explicit(false).unwrap();

        // Rollback should succeed (no-op)
        txn_service.rollback(ctx).unwrap();
    }

    // ==================== Read-Your-Writes Tests ====================

    #[test]
    fn test_read_your_writes_get() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Write a value
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();

        // Read it back (read-your-writes)
        let value = txn_service.get(&ctx, b"key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // Clean up
        txn_service.rollback(ctx).unwrap();
    }

    #[test]
    fn test_read_your_writes_update() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Write initial value
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"v1".to_vec())
            .unwrap();

        // Update it
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"v2".to_vec())
            .unwrap();

        // Read should return the updated value
        let value = txn_service.get(&ctx, b"key1").unwrap();
        assert_eq!(value, Some(b"v2".to_vec()));

        // Clean up
        txn_service.rollback(ctx).unwrap();
    }

    #[test]
    fn test_read_your_writes_delete() {
        let (_storage, txn_service, _dir) = create_test_service();

        // First, commit an initial value
        let mut setup_ctx = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut setup_ctx, b"key1".to_vec(), b"initial".to_vec())
            .unwrap();
        txn_service.commit(setup_ctx).unwrap();

        // Start new transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Delete the key
        txn_service.delete(&mut ctx, b"key1".to_vec()).unwrap();

        // Read should return None (deleted)
        let value = txn_service.get(&ctx, b"key1").unwrap();
        assert_eq!(value, None);

        // Clean up
        txn_service.rollback(ctx).unwrap();
    }

    #[test]
    fn test_read_your_writes_scan() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Write multiple values
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        txn_service
            .put(&mut ctx, b"key2".to_vec(), b"value2".to_vec())
            .unwrap();
        txn_service
            .put(&mut ctx, b"key3".to_vec(), b"value3".to_vec())
            .unwrap();

        // Scan should see all pending writes
        let mut scan_iter = txn_service
            .scan_iter(&ctx, b"key".to_vec()..b"key~".to_vec())
            .unwrap();

        // Collect results
        let mut results = vec![];
        scan_iter.advance().unwrap();
        while scan_iter.valid() {
            results.push((scan_iter.user_key().to_vec(), scan_iter.value().to_vec()));
            scan_iter.advance().unwrap();
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (b"key1".to_vec(), b"value1".to_vec()));
        assert_eq!(results[1], (b"key2".to_vec(), b"value2".to_vec()));
        assert_eq!(results[2], (b"key3".to_vec(), b"value3".to_vec()));

        // Clean up
        txn_service.rollback(ctx).unwrap();
    }

    #[test]
    fn test_read_your_writes_isolation() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Start two explicit transactions
        let mut ctx1 = txn_service.begin_explicit(false).unwrap();
        let ctx2 = txn_service.begin_explicit(false).unwrap();

        // Txn1 writes a value
        txn_service
            .put(&mut ctx1, b"key1".to_vec(), b"from_txn1".to_vec())
            .unwrap();

        // Txn1 should see its own write
        let value1 = txn_service.get(&ctx1, b"key1").unwrap();
        assert_eq!(value1, Some(b"from_txn1".to_vec()));

        // Txn2 should NOT see txn1's pending write
        let value2 = txn_service.get(&ctx2, b"key1").unwrap();
        assert_eq!(value2, None);

        // Clean up
        txn_service.rollback(ctx1).unwrap();
        txn_service.rollback(ctx2).unwrap();
    }

    #[test]
    fn test_read_your_writes_with_committed_base() {
        let (_storage, txn_service, _dir) = create_test_service();

        // First, commit a base value
        let mut setup_ctx = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut setup_ctx, b"key1".to_vec(), b"committed".to_vec())
            .unwrap();
        txn_service.commit(setup_ctx).unwrap();

        // Start new transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Update the key
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"pending".to_vec())
            .unwrap();

        // Read should return the pending value, not the committed one
        let value = txn_service.get(&ctx, b"key1").unwrap();
        assert_eq!(value, Some(b"pending".to_vec()));

        // Clean up
        txn_service.rollback(ctx).unwrap();
    }

    #[test]
    fn test_read_your_writes_delete_then_insert() {
        let (_storage, txn_service, _dir) = create_test_service();

        // First, commit a base value
        let mut setup_ctx = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut setup_ctx, b"key1".to_vec(), b"original".to_vec())
            .unwrap();
        txn_service.commit(setup_ctx).unwrap();

        // Start new transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Delete
        txn_service.delete(&mut ctx, b"key1".to_vec()).unwrap();

        // Should see None
        let value = txn_service.get(&ctx, b"key1").unwrap();
        assert_eq!(value, None);

        // Re-insert with new value
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"reinserted".to_vec())
            .unwrap();

        // Should see the new value
        let value = txn_service.get(&ctx, b"key1").unwrap();
        assert_eq!(value, Some(b"reinserted".to_vec()));

        // Clean up
        txn_service.rollback(ctx).unwrap();
    }

    // ========================================================================
    // Additional coverage tests for edge cases
    // ========================================================================

    #[test]
    fn test_concurrency_manager_getter() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Test that concurrency_manager() getter works
        let cm = txn_service.concurrency_manager();
        assert_eq!(cm.max_ts(), 0);

        // After begin, max_ts should be updated
        let _ctx = txn_service.begin(false).unwrap();
        assert!(cm.max_ts() > 0);
    }

    #[test]
    fn test_clog_service_getter() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Test that clog_service() getter works
        let clog = txn_service.clog_service();

        // Write something to test the getter works
        txn_service.autocommit_put(b"test", b"value").unwrap();

        // After write, clog should have entries
        assert!(clog.max_lsn().unwrap() >= 1);
    }

    #[test]
    fn test_put_on_committed_transaction() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Begin and commit a transaction
        let mut ctx = txn_service.begin(false).unwrap();
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        let commit_info = txn_service.commit(ctx).unwrap();

        // Manually create a committed context
        let mut committed_ctx =
            TxnCtx::new(commit_info.txn_id + 1, commit_info.commit_ts + 1, false);
        committed_ctx.state = TxnState::Committed {
            commit_ts: commit_info.commit_ts,
        };

        // Try to put on committed transaction - should fail
        let result = txn_service.put(&mut committed_ctx, b"key2".to_vec(), b"value2".to_vec());
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TiSqlError::TransactionNotActive(_)
        ));
    }

    #[test]
    fn test_delete_on_committed_transaction() {
        let (_storage, txn_service, _dir) = create_test_service();

        let ctx = txn_service.begin(false).unwrap();
        let commit_info = txn_service.commit(ctx).unwrap();

        // Create committed context
        let mut committed_ctx =
            TxnCtx::new(commit_info.txn_id + 1, commit_info.commit_ts + 1, false);
        committed_ctx.state = TxnState::Committed {
            commit_ts: commit_info.commit_ts,
        };

        // Try to delete on committed transaction - should fail
        let result = txn_service.delete(&mut committed_ctx, b"key1".to_vec());
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TiSqlError::TransactionNotActive(_)
        ));
    }

    #[test]
    fn test_put_on_aborted_transaction() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Create an aborted context
        let mut aborted_ctx = TxnCtx::new(1, 1, false);
        aborted_ctx.state = TxnState::Aborted;

        // Try to put on aborted transaction - should fail
        let result = txn_service.put(&mut aborted_ctx, b"key1".to_vec(), b"value1".to_vec());
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TiSqlError::TransactionNotActive(_)
        ));
    }

    #[test]
    fn test_delete_on_aborted_transaction() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Create an aborted context
        let mut aborted_ctx = TxnCtx::new(1, 1, false);
        aborted_ctx.state = TxnState::Aborted;

        // Try to delete on aborted transaction - should fail
        let result = txn_service.delete(&mut aborted_ctx, b"key1".to_vec());
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TiSqlError::TransactionNotActive(_)
        ));
    }

    #[test]
    fn test_commit_on_aborted_transaction() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Create an aborted context
        let mut aborted_ctx = TxnCtx::new(1, 1, false);
        aborted_ctx.state = TxnState::Aborted;

        // Try to commit aborted transaction - should fail
        let result = txn_service.commit(aborted_ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_on_already_committed() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Create a committed context
        let mut committed_ctx = TxnCtx::new(1, 1, false);
        committed_ctx.state = TxnState::Committed { commit_ts: 10 };

        // Try to commit again - should fail
        let result = txn_service.commit(committed_ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_recover_with_uncommitted_entries() {
        // Test recovery where some transactions are not committed (rolled back)
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        {
            let clog_service = Arc::new(FileClogService::open(config.clone()).unwrap());

            // Write entries directly to clog without commit record
            // This simulates a crash before commit
            clog_service
                .append(
                    ClogEntry {
                        txn_id: 100,
                        lsn: 1,
                        op: ClogOp::Put {
                            key: b"uncommitted_key".to_vec(),
                            value: b"uncommitted_value".to_vec(),
                        },
                    },
                    true,
                )
                .unwrap();

            // Write a committed transaction
            clog_service
                .append(
                    ClogEntry {
                        txn_id: 101,
                        lsn: 2,
                        op: ClogOp::Put {
                            key: b"committed_key".to_vec(),
                            value: b"committed_value".to_vec(),
                        },
                    },
                    true,
                )
                .unwrap();
            clog_service
                .append(
                    ClogEntry {
                        txn_id: 101,
                        lsn: 3,
                        op: ClogOp::Commit { commit_ts: 200 },
                    },
                    true,
                )
                .unwrap();

            clog_service.close().unwrap();
        }

        // Recover
        let (clog_service, entries) = FileClogService::recover(config).unwrap();
        let clog_service = Arc::new(clog_service);
        let tso = Arc::new(LocalTso::new(1));
        let cm = Arc::new(ConcurrencyManager::new(0));
        let storage = Arc::new(MemTableEngine::new());
        let txn_service = TransactionService::new(Arc::clone(&storage), clog_service, tso, cm);

        let stats = txn_service.recover(&entries).unwrap();

        // Should have 1 committed txn, 1 rolled back entry
        assert_eq!(stats.committed_txns, 1);
        assert_eq!(stats.rolled_back_entries, 1);
        assert_eq!(stats.applied_puts, 1);

        // Only committed key should be present
        assert!(get_for_test(&*storage, b"uncommitted_key").is_none());
        assert_eq!(
            get_for_test(&*storage, b"committed_key"),
            Some(b"committed_value".to_vec())
        );
    }

    #[test]
    fn test_scan_with_unbounded_end() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Write some data
        txn_service.autocommit_put(b"key1", b"value1").unwrap();
        txn_service.autocommit_put(b"key2", b"value2").unwrap();
        txn_service.autocommit_put(b"key3", b"value3").unwrap();

        let ctx = txn_service.begin(true).unwrap();

        // Scan with empty end (unbounded)
        let mut iter = txn_service
            .scan_iter(&ctx, b"key1".to_vec()..vec![])
            .unwrap();
        let mut results = Vec::new();
        iter.advance().unwrap();
        while iter.valid() {
            results.push(iter.user_key().to_vec());
            iter.advance().unwrap();
        }

        // Should see all keys starting from key1
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], b"key1");
        assert_eq!(results[1], b"key2");
        assert_eq!(results[2], b"key3");
    }

    #[test]
    fn test_get_iterates_through_versions() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Write multiple versions of same key
        let info1 = txn_service.autocommit_put(b"key", b"v1").unwrap();
        let info2 = txn_service.autocommit_put(b"key", b"v2").unwrap();
        let info3 = txn_service.autocommit_put(b"key", b"v3").unwrap();

        // Read at different timestamps
        let ctx1 = TxnCtx::new(0, info1.commit_ts, true);
        let value1 = txn_service.get(&ctx1, b"key").unwrap();
        assert_eq!(value1, Some(b"v1".to_vec()));

        let ctx2 = TxnCtx::new(0, info2.commit_ts, true);
        let value2 = txn_service.get(&ctx2, b"key").unwrap();
        assert_eq!(value2, Some(b"v2".to_vec()));

        let ctx3 = TxnCtx::new(0, info3.commit_ts, true);
        let value3 = txn_service.get(&ctx3, b"key").unwrap();
        assert_eq!(value3, Some(b"v3".to_vec()));
    }

    #[test]
    fn test_get_with_tombstone_in_middle() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Write, delete, write again
        let _info1 = txn_service.autocommit_put(b"key", b"v1").unwrap();
        let info2 = txn_service.autocommit_delete(b"key").unwrap();
        let info3 = txn_service.autocommit_put(b"key", b"v3").unwrap();

        // At delete timestamp, should see None
        let ctx2 = TxnCtx::new(0, info2.commit_ts, true);
        let value2 = txn_service.get(&ctx2, b"key").unwrap();
        assert_eq!(value2, None);

        // At v3 timestamp, should see v3
        let ctx3 = TxnCtx::new(0, info3.commit_ts, true);
        let value3 = txn_service.get(&ctx3, b"key").unwrap();
        assert_eq!(value3, Some(b"v3".to_vec()));
    }

    #[test]
    fn test_explicit_txn_write_conflict() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Transaction 1: Lock key
        let mut ctx1 = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut ctx1, b"conflict_key".to_vec(), b"value1".to_vec())
            .unwrap();

        // Transaction 2: Try to write same key - should fail
        let mut ctx2 = txn_service.begin_explicit(false).unwrap();
        let result = txn_service.put(&mut ctx2, b"conflict_key".to_vec(), b"value2".to_vec());

        assert!(result.is_err());
        match result.unwrap_err() {
            TiSqlError::KeyIsLocked { key, lock_ts, .. } => {
                assert_eq!(key, b"conflict_key".to_vec());
                assert_eq!(lock_ts, ctx1.start_ts);
            }
            other => panic!("Expected KeyIsLocked error, got: {other:?}"),
        }

        // Cleanup
        txn_service.rollback(ctx1).unwrap();
        txn_service.rollback(ctx2).unwrap();
    }

    #[test]
    fn test_explicit_txn_delete_conflict() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Commit initial value
        txn_service.autocommit_put(b"delete_key", b"value").unwrap();

        // Transaction 1: Lock key with delete
        let mut ctx1 = txn_service.begin_explicit(false).unwrap();
        txn_service
            .delete(&mut ctx1, b"delete_key".to_vec())
            .unwrap();

        // Transaction 2: Try to delete same key - should fail
        let mut ctx2 = txn_service.begin_explicit(false).unwrap();
        let result = txn_service.delete(&mut ctx2, b"delete_key".to_vec());

        assert!(result.is_err());
        match result.unwrap_err() {
            TiSqlError::KeyIsLocked { key, lock_ts, .. } => {
                assert_eq!(key, b"delete_key".to_vec());
                assert_eq!(lock_ts, ctx1.start_ts);
            }
            other => panic!("Expected KeyIsLocked error, got: {other:?}"),
        }

        // Cleanup
        txn_service.rollback(ctx1).unwrap();
        txn_service.rollback(ctx2).unwrap();
    }

    #[test]
    fn test_recover_with_delete_operation() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        {
            let clog_service = Arc::new(FileClogService::open(config.clone()).unwrap());
            let tso = Arc::new(LocalTso::new(1));
            let cm = Arc::new(ConcurrencyManager::new(0));
            let storage = Arc::new(MemTableEngine::new());
            let txn_service = TransactionService::new(storage, clog_service.clone(), tso, cm);

            // Put then delete
            txn_service.autocommit_put(b"del_key", b"value").unwrap();
            txn_service.autocommit_delete(b"del_key").unwrap();

            clog_service.close().unwrap();
        }

        // Recover
        let (clog_service, entries) = FileClogService::recover(config).unwrap();
        let clog_service = Arc::new(clog_service);
        let tso = Arc::new(LocalTso::new(1));
        let cm = Arc::new(ConcurrencyManager::new(0));
        let storage = Arc::new(MemTableEngine::new());
        let txn_service = TransactionService::new(Arc::clone(&storage), clog_service, tso, cm);

        let stats = txn_service.recover(&entries).unwrap();

        assert_eq!(stats.committed_txns, 2);
        assert_eq!(stats.applied_puts, 1);
        assert_eq!(stats.applied_deletes, 1);

        // Key should be deleted
        assert!(get_for_test(&*storage, b"del_key").is_none());
    }

    #[test]
    fn test_recover_empty_entries() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let clog_service = Arc::new(FileClogService::open(config).unwrap());
        let tso = Arc::new(LocalTso::new(1));
        let cm = Arc::new(ConcurrencyManager::new(0));
        let storage = Arc::new(MemTableEngine::new());
        let txn_service = TransactionService::new(Arc::clone(&storage), clog_service, tso, cm);

        // Recover with empty entries
        let stats = txn_service.recover(&[]).unwrap();

        assert_eq!(stats.committed_txns, 0);
        assert_eq!(stats.applied_puts, 0);
        assert_eq!(stats.applied_deletes, 0);
        assert_eq!(stats.rolled_back_entries, 0);
    }

    #[test]
    fn test_storage_getter() {
        let (storage, txn_service, _dir) = create_test_service();

        // Test storage() getter returns the same storage
        let storage_ref = txn_service.storage();
        txn_service
            .autocommit_put(b"test_key", b"test_value")
            .unwrap();

        // Should be able to read from both references
        let value1 = get_for_test(&*storage, b"test_key");
        let value2 = get_for_test(storage_ref, b"test_key");
        assert_eq!(value1, value2);
        assert_eq!(value1, Some(b"test_value".to_vec()));
    }

    #[test]
    fn test_tso_getter() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Test tso() getter
        let tso = txn_service.tso();
        let ts1 = tso.last_ts();

        // After begin, TSO should advance
        let _ctx = txn_service.begin(false).unwrap();
        let ts2 = tso.last_ts();

        assert!(ts2 > ts1);
    }
}
