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
use super::concurrency::{ConcurrencyManager, Lock};
use crate::storage::mvcc::{is_tombstone, MvccIterator, MvccKey};
use crate::storage::{StorageEngine, WriteBatch};
use crate::types::{Key, RawValue, Timestamp, TxnId};

/// Transaction service manages transactions with durability and MVCC.
///
/// This is the unified interface for all transaction operations, following
/// OceanBase's `ObTransService` pattern. Transaction state is held in `TxnCtx`
/// and passed to each operation.
///
/// All write operations follow the 1PC pattern:
/// 1. Acquire in-memory locks (blocks concurrent readers)
/// 2. Log to commit log
/// 3. Sync commit log to disk (durability)
/// 4. Apply to storage with commit_ts
/// 5. Release locks (on guard drop)
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
            TxnState::Active => Ok(()),
            TxnState::Committed => Err(crate::error::TiSqlError::Internal(
                "Transaction already committed".into(),
            )),
            TxnState::Aborted => Err(crate::error::TiSqlError::Internal(
                "Transaction already aborted".into(),
            )),
        }
    }
}

/// Implement TxnService trait for TransactionService
impl<S: StorageEngine + 'static, L: ClogService + 'static, T: TsoService> TxnService
    for TransactionService<S, L, T>
{
    type ScanIter = MvccScanIterator<S::Iter>;

    fn begin(&self, read_only: bool) -> Result<TxnCtx> {
        // Allocate txn_id and start_ts from TSO
        let txn_id = self.get_ts();
        let start_ts = self.get_ts();

        Ok(TxnCtx::new(txn_id, start_ts, read_only))
    }

    fn get(&self, ctx: &TxnCtx, key: &[u8]) -> Result<Option<RawValue>> {
        // Check for locks from other transactions
        self.concurrency_manager.check_lock(key, ctx.start_ts)?;

        // MVCC read: find the latest version with commit_ts <= start_ts
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
        let mut iter = self.storage.scan_iter(scan_range)?;
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
        // Check for locks in range.
        //
        // Note: empty end (vec![]) means "unbounded" (scan to infinity), not empty range.
        // range.is_empty() uses lexicographic comparison (start >= end), which incorrectly
        // returns true for unbounded scans since any non-empty start >= empty end.
        // We must explicitly handle the unbounded case.
        if range.end.is_empty() || !range.is_empty() {
            self.concurrency_manager
                .check_range(&range.start, &range.end, ctx.start_ts)?;
        }

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

        // Create streaming iterator over storage
        let storage_iter = self.storage.scan_iter(start_mvcc..end_mvcc)?;

        // Create streaming scan iterator that wraps storage with MVCC filtering
        Ok(MvccScanIterator::new(storage_iter, ctx.start_ts, range))
    }

    fn put(&self, ctx: &mut TxnCtx, key: Key, value: RawValue) -> Result<()> {
        if ctx.state != TxnState::Active {
            return Err(TiSqlError::TransactionNotActive(format!("{:?}", ctx.state)));
        }
        if ctx.read_only {
            return Err(TiSqlError::ReadOnlyTransaction);
        }
        ctx.write_buffer.put(key, value);
        Ok(())
    }

    fn delete(&self, ctx: &mut TxnCtx, key: Key) -> Result<()> {
        if ctx.state != TxnState::Active {
            return Err(TiSqlError::TransactionNotActive(format!("{:?}", ctx.state)));
        }
        if ctx.read_only {
            return Err(TiSqlError::ReadOnlyTransaction);
        }
        ctx.write_buffer.delete(key);
        Ok(())
    }

    fn commit(&self, mut ctx: TxnCtx) -> Result<CommitInfo> {
        Self::check_active(&ctx)?;

        if ctx.write_buffer.is_empty() {
            // No writes - just mark as committed
            ctx.state = TxnState::Committed;
            return Ok(CommitInfo {
                txn_id: ctx.txn_id,
                commit_ts: ctx.start_ts,
                lsn: 0,
            });
        }

        // Acquire in-memory locks BEFORE getting commit_ts.
        // This prevents the "time-travel" anomaly where a reader with start_ts > commit_ts
        // could miss data that commits after they started reading.
        //
        // By acquiring locks first:
        // 1. Any concurrent reader will see the lock and be blocked
        // 2. We then get commit_ts which is guaranteed > any concurrent reader's start_ts
        //    because: each begin() calls get_ts() which updates max_ts, so
        //    max_ts >= any_concurrent_reader.start_ts, and commit_ts = max(max_ts + 1, tso_ts)

        // Select primary key deterministically: lexicographically smallest key.
        // This is important for future 2PC/lock resolution:
        // - Primary key must be stable and deterministic across retries
        // - TiDB uses the first key in sorted order as primary
        // - Once selected, the primary should not change for the transaction
        //
        // WriteBatch uses BTreeMap, so keys().next() gives the smallest key in O(1).
        let primary_key: Arc<[u8]> = ctx
            .write_buffer
            .keys()
            .next()
            .map(|k| Arc::from(k.as_slice()))
            .unwrap_or_else(|| Arc::from(&[][..]));

        // Lock uses transaction's start_ts, not a fresh timestamp.
        // This is critical for lock resolution in 2PC:
        // - Readers use lock.ts to determine if the lock is from an old/stale txn
        // - Lock resolvers check if txn with start_ts is still alive
        // - Using start_ts ensures consistent behavior across all keys in the txn
        let lock = Lock {
            ts: ctx.start_ts,
            primary: primary_key,
        };

        // lock_keys takes an iterator - no need to clone keys into a Vec
        let _guards = self
            .concurrency_manager
            .lock_keys(ctx.write_buffer.keys(), lock)?;

        // FAILPOINT: After locks acquired, before commit_ts computation
        #[cfg(feature = "failpoints")]
        fail_point!("txn_after_lock_before_commit_ts");

        // Get commit_ts AFTER acquiring locks to ensure commit_ts > concurrent reader's start_ts
        let max_ts = self.concurrency_manager.max_ts();
        let tso_ts = self.get_ts();
        let commit_ts = std::cmp::max(max_ts + 1, tso_ts);
        let txn_id = ctx.txn_id;

        // Take ownership of write buffer (swap with empty)
        let mut storage_batch = std::mem::take(&mut ctx.write_buffer);

        // Write to commit log directly from batch references (zero-copy).
        // The clog serializes directly from &storage_batch without cloning key/value data.
        let lsn = self
            .clog_service
            .write_batch(txn_id, &storage_batch, commit_ts, true)?;

        // Apply to storage with commit_ts AND clog LSN for proper recovery ordering.
        // Now we consume the batch (move ownership to storage).
        storage_batch.set_commit_ts(commit_ts);
        storage_batch.set_clog_lsn(lsn);
        self.storage.write_batch(storage_batch)?;

        // Mark as committed (guards will be dropped, releasing locks)
        ctx.state = TxnState::Committed;

        Ok(CommitInfo {
            txn_id,
            commit_ts,
            lsn,
        })
    }

    fn rollback(&self, mut ctx: TxnCtx) -> Result<()> {
        Self::check_active(&ctx)?;

        // Clear write buffer
        ctx.write_buffer.clear();

        // Mark as aborted
        ctx.state = TxnState::Aborted;

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
pub struct MvccScanIterator<I: MvccIterator> {
    /// Storage iterator producing MVCC key-value pairs
    storage_iter: I,
    /// Transaction's read timestamp for MVCC filtering
    read_ts: Timestamp,
    /// Key range for filtering
    range: Range<Key>,
    /// Current entry (key, timestamp, value). None if not positioned on a valid entry.
    current: Option<(Vec<u8>, Timestamp, Vec<u8>)>,
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
            current: None,
        }
    }

    /// Find the next visible entry from storage.
    ///
    /// Advances the storage iterator until we find an entry that:
    /// - Is within the key range
    /// - Has `ts <= read_ts` (MVCC visibility)
    /// - Is not a tombstone
    ///
    /// After finding a visible entry, skips all older versions of the same key.
    fn find_next_visible(&mut self) -> crate::error::Result<()> {
        self.current = None;

        // Initialize storage iterator if needed
        if !self.storage_iter.valid() {
            self.storage_iter.advance()?;
        }

        while self.storage_iter.valid() {
            let user_key = self.storage_iter.user_key();
            let ts = self.storage_iter.timestamp();

            // Check if past end of range
            if !self.range.end.is_empty() && user_key >= self.range.end.as_slice() {
                return Ok(()); // current stays None
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
                // Skip this key and all older versions
                let skip_key = user_key.to_vec();
                self.storage_iter.advance()?;
                while self.storage_iter.valid()
                    && self.storage_iter.user_key() == skip_key.as_slice()
                {
                    self.storage_iter.advance()?;
                }
                continue;
            }

            // Found a visible, non-tombstone entry - cache it
            let key = self.storage_iter.user_key().to_vec();
            let timestamp = self.storage_iter.timestamp();
            let value = self.storage_iter.value().to_vec();
            self.current = Some((key.clone(), timestamp, value));

            // Skip all older versions of this key
            self.storage_iter.advance()?;
            while self.storage_iter.valid() && self.storage_iter.user_key() == key.as_slice() {
                self.storage_iter.advance()?;
            }

            return Ok(());
        }

        Ok(()) // current stays None - exhausted
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
        self.current.is_some()
    }

    fn user_key(&self) -> &[u8] {
        self.current
            .as_ref()
            .map(|(k, _, _)| k.as_slice())
            .expect("Iterator not valid")
    }

    fn timestamp(&self) -> Timestamp {
        self.current
            .as_ref()
            .map(|(_, ts, _)| *ts)
            .expect("Iterator not valid")
    }

    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .map(|(_, _, v)| v.as_slice())
            .expect("Iterator not valid")
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

        let mut iter = storage.scan_iter(range).unwrap();
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
    fn test_autocommit_with_locking() {
        let (storage, txn_service, _dir) = create_test_service();
        let cm = txn_service.concurrency_manager();

        // Before write, no locks
        assert_eq!(cm.lock_count(), 0);

        // Execute write
        txn_service.autocommit_put(b"key1", b"value1").unwrap();

        // After write, locks should be released
        assert_eq!(cm.lock_count(), 0);

        // Data should be there
        let v = get_for_test(&*storage, b"key1");
        assert_eq!(v, Some(b"value1".to_vec()));
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

        // Error after 2 successful advance() calls on mock.
        // Caching iterator behavior:
        // - Cache "a", skip "a" (advance #1, ok), return "a"
        // - Cache "b", skip "b" (advance #2, ok), return "b"
        // - Cache "c", skip "c" (advance #3, position=2 > 1, ERROR!)
        // Result: 2 Ok entries + 1 Err
        let mock_iter = ErrorInjectingIterator::new(entries, 2);

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

        // Error after 1 successful advance() call on mock.
        // Caching iterator behavior:
        // - Cache "a", skip "a" (advance #1, ok), return "a"
        // - Cache "b", skip "b" (advance #2, position=1 > 0, ERROR!)
        // Result: 1 Ok entry + 1 Err
        let mock_iter = ErrorInjectingIterator::new(entries, 1);

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

        // Error after 2 successful advance() calls on mock.
        // Caching: cache & skip "a" (advance #1 ok), cache & skip "b" (advance #2 ok),
        //          cache & skip "c" (advance #3, 2>1=true, ERROR)
        // Result: 2 successful entries before error
        let mock_iter = ErrorInjectingIterator::new(entries.clone(), 2);

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
    // Regression Test: Unbounded scan must check locks
    // ========================================================================

    #[test]
    fn test_scan_unbounded_end_checks_locks() {
        // Regression test for: Range::is_empty() returns true for unbounded end
        // (vec![] is lexicographically smallest, so start >= [] is true),
        // which caused lock checks to be skipped.
        let (_storage, txn_service, _dir) = create_test_service();

        // Write some data
        txn_service.autocommit_put(b"a", b"1").unwrap();
        txn_service.autocommit_put(b"b", b"2").unwrap();
        txn_service.autocommit_put(b"c", b"3").unwrap();

        // Directly acquire a lock using ConcurrencyManager to simulate a concurrent writer.
        // This bypasses the transaction flow where locks are only held briefly during commit.
        let cm = txn_service.concurrency_manager();
        let lock = Lock {
            ts: 100, // Lock timestamp
            primary: Arc::from(&b"b"[..]),
        };
        let _guard = cm.lock_key(&b"b".to_vec(), &lock).unwrap();

        // Start a reader transaction with start_ts >= lock.ts (so it should be blocked)
        // We need to advance the TSO past the lock timestamp
        while txn_service.tso().last_ts() < 100 {
            let _ = txn_service.tso().get_ts();
        }
        let read_ctx = txn_service.begin(true).unwrap();
        assert!(
            read_ctx.start_ts() >= 100,
            "reader must have snapshot at or after lock.ts"
        );

        // Unbounded end scan (empty vec) should check locks and fail
        let result = txn_service.scan_iter(&read_ctx, b"a".to_vec()..vec![]);
        match result {
            Err(TiSqlError::KeyIsLocked { key, .. }) => {
                assert_eq!(key, b"b", "should be locked on key 'b'");
            }
            Err(e) => panic!("expected KeyIsLocked, got {e:?}"),
            Ok(_) => panic!("unbounded scan should be blocked by lock"),
        }

        // Bounded scan containing the locked key should also fail
        let result = txn_service.scan_iter(&read_ctx, b"a".to_vec()..b"d".to_vec());
        match result {
            Err(TiSqlError::KeyIsLocked { .. }) => {}
            Err(e) => panic!("expected KeyIsLocked, got {e:?}"),
            Ok(_) => panic!("bounded scan should also be blocked by lock"),
        }

        // Bounded scan NOT containing the locked key should succeed
        let result = txn_service.scan_iter(&read_ctx, b"c".to_vec()..b"d".to_vec());
        assert!(
            result.is_ok(),
            "scan not containing locked key should succeed"
        );

        // Drop the lock guard to release the lock
        drop(_guard);

        // Now the scan should succeed
        let new_read_ctx = txn_service.begin(true).unwrap();
        let result = txn_service.scan_iter(&new_read_ctx, b"a".to_vec()..vec![]);
        assert!(result.is_ok(), "scan should succeed after lock released");
    }
}
