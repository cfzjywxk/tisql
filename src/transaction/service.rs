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
use crate::tso::TsoService;

use super::api::{CommitInfo, TxnCtx, TxnService, TxnState};
use super::concurrency::{ConcurrencyManager, Lock};
use crate::storage::mvcc::{is_tombstone, MvccIterator, MvccKey};
use crate::storage::{StorageEngine, WriteBatch, WriteOp};
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

        // First pass: identify committed transactions and find max commit_ts
        let mut committed_txns = std::collections::HashSet::new();
        for entry in entries {
            if let ClogOp::Commit { commit_ts } = entry.op {
                committed_txns.insert(entry.txn_id);
                max_ts = max_ts.max(commit_ts);
                stats.committed_txns += 1;
            }
        }

        // Build a map of txn_id -> commit_ts for applying writes with correct ts
        let mut txn_commit_ts: std::collections::HashMap<TxnId, Timestamp> =
            std::collections::HashMap::new();
        for entry in entries {
            if let ClogOp::Commit { commit_ts } = entry.op {
                txn_commit_ts.insert(entry.txn_id, commit_ts);
            }
        }

        // Second pass: apply committed operations with their commit_ts
        for entry in entries {
            if !committed_txns.contains(&entry.txn_id) {
                stats.rolled_back_entries += 1;
                continue;
            }

            let commit_ts = *txn_commit_ts.get(&entry.txn_id).unwrap_or(&max_ts);

            match &entry.op {
                ClogOp::Put { key, value } => {
                    // Create a write batch with commit_ts
                    let mut batch = WriteBatch::new();
                    batch.put(key.clone(), value.clone());
                    batch.set_commit_ts(commit_ts);
                    self.storage.write_batch(batch)?;
                    stats.applied_puts += 1;
                }
                ClogOp::Delete { key } => {
                    let mut batch = WriteBatch::new();
                    batch.delete(key.clone());
                    batch.set_commit_ts(commit_ts);
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
    fn begin(&self, read_only: bool) -> Result<TxnCtx> {
        // Allocate txn_id and start_ts from TSO
        let txn_id = self.get_ts();
        let start_ts = self.get_ts();

        Ok(TxnCtx::new(txn_id, start_ts, read_only))
    }

    fn get(&self, ctx: &TxnCtx, key: &[u8]) -> Result<Option<RawValue>> {
        // First check write buffer for uncommitted writes (read-your-writes)
        // WriteBatch has at most one op per key (last write wins)
        if let Some(op) = ctx.write_buffer.get(key) {
            return match op {
                WriteOp::Put { value } => Ok(Some(value.clone())),
                WriteOp::Delete => Ok(None),
            };
        }

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

        let entries = self.storage.scan(scan_range)?;

        // Find the first entry matching our key with ts <= start_ts
        for (mvcc_key, value) in entries {
            let (decoded_key, entry_ts) = mvcc_key.decode();
            // Check exact key match and timestamp visibility
            if decoded_key == key && entry_ts <= ctx.start_ts {
                if is_tombstone(&value) {
                    return Ok(None); // Key was deleted
                }
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    fn scan(
        &self,
        ctx: &TxnCtx,
        range: Range<Key>,
    ) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>> {
        // Check for locks in range
        if !range.is_empty() {
            self.concurrency_manager
                .check_range(&range.start, &range.end, ctx.start_ts)?;
        }

        // MVCC scan: find the latest version of each key with commit_ts <= start_ts
        //
        // Build MVCC key range:
        // - Start: MvccKey::encode(range.start, MAX) - smallest MVCC key for range.start
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

        // Extract and sort buffer entries in range (owned copy)
        let buffer_entries: Vec<(Key, Option<RawValue>)> = ctx
            .write_buffer
            .iter()
            .filter(|(k, _)| *k >= &range.start && (range.end.is_empty() || *k < &range.end))
            .map(|(k, op)| {
                let value = match op {
                    WriteOp::Put { value } => Some(value.clone()),
                    WriteOp::Delete => None,
                };
                (k.clone(), value)
            })
            .collect::<Vec<_>>();

        // Create streaming scan iterator that merges storage with write buffer
        let scan_iter = MvccScanIterator::new(storage_iter, ctx.start_ts, range, buffer_entries);

        Ok(Box::new(scan_iter))
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
        //    because: start_ts = max(TSO, max_ts), and we update max_ts via lock_keys

        // Select primary key deterministically: lexicographically smallest key.
        // This is important for future 2PC/lock resolution:
        // - Primary key must be stable and deterministic across retries
        // - TiDB uses the first key in sorted order as primary
        // - Once selected, the primary should not change for the transaction
        let primary_key: Arc<[u8]> = ctx
            .write_buffer
            .keys()
            .min()
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
// MvccScanIterator - Streaming scan with write buffer merge
// ============================================================================

/// Streaming scan iterator that merges storage results with write buffer.
///
/// This iterator provides efficient MVCC scanning by:
/// - Streaming from storage iterator (no full materialization of storage)
/// - Deduplicating by tracking last_user_key (no HashMap for dedup)
/// - Merging write buffer entries sorted in memory
/// - Returning results in sorted order (no final sort needed)
///
/// ## MVCC Semantics
///
/// For each user key, we return the latest visible version where:
/// - `entry_ts <= read_ts` (MVCC visibility)
/// - Tombstones are filtered out (deleted keys not returned)
/// - Write buffer entries override storage entries (read-your-writes)
pub struct MvccScanIterator<I: MvccIterator> {
    /// Storage iterator producing MVCC key-value pairs
    storage_iter: I,
    /// Transaction's read timestamp for MVCC filtering
    read_ts: Timestamp,
    /// Key range for filtering
    range: Range<Key>,
    /// Sorted write buffer entries in range (key, Option<value> where None = delete)
    buffer_entries: Vec<(Key, Option<RawValue>)>,
    /// Current position in buffer_entries
    buffer_pos: usize,
    /// Last user key returned (for deduplication)
    last_user_key: Option<Key>,
    /// Pending result from storage (user_key, Option<value>)
    pending_storage: Option<(Key, Option<RawValue>)>,
    /// Whether storage iterator is exhausted
    storage_exhausted: bool,
}

impl<I: MvccIterator> MvccScanIterator<I> {
    /// Create a new scan iterator.
    ///
    /// # Arguments
    ///
    /// * `storage_iter` - Iterator over storage (MVCC key-value pairs)
    /// * `read_ts` - Transaction's read timestamp for MVCC visibility
    /// * `range` - Key range for filtering
    /// * `buffer_entries` - Pre-extracted write buffer entries (key, Option<value>)
    ///
    /// The `buffer_entries` should already be filtered to the range but may be unsorted.
    pub fn new(
        storage_iter: I,
        read_ts: Timestamp,
        range: Range<Key>,
        mut buffer_entries: Vec<(Key, Option<RawValue>)>,
    ) -> Self {
        // Sort buffer entries by key
        buffer_entries.sort_by(|a, b| a.0.cmp(&b.0));

        Self {
            storage_iter,
            read_ts,
            range,
            buffer_entries,
            buffer_pos: 0,
            last_user_key: None,
            pending_storage: None,
            storage_exhausted: false,
        }
    }

    /// Advance storage iterator to next unique user key visible at read_ts.
    fn advance_storage(&mut self) -> crate::error::Result<()> {
        self.pending_storage = None;

        while self.storage_iter.valid() {
            let mvcc_key = self.storage_iter.key();
            let (user_key, ts) = mvcc_key.decode();

            // Check if past end of range
            if !self.range.end.is_empty() && user_key >= self.range.end {
                self.storage_exhausted = true;
                return Ok(());
            }

            // Skip if before start of range
            if user_key < self.range.start {
                self.storage_iter.next()?;
                continue;
            }

            // Skip if not visible at read_ts
            if ts > self.read_ts {
                self.storage_iter.next()?;
                continue;
            }

            // Skip if we've already processed this user key
            if let Some(ref last_key) = self.last_user_key {
                if &user_key == last_key {
                    self.storage_iter.next()?;
                    continue;
                }
            }

            // Found a visible entry
            let value = self.storage_iter.value();
            let result = if is_tombstone(value) {
                (user_key, None)
            } else {
                (user_key, Some(value.to_vec()))
            };
            self.pending_storage = Some(result);

            // Advance past all versions of this key
            self.storage_iter.next()?;
            return Ok(());
        }

        self.storage_exhausted = true;
        Ok(())
    }

    /// Peek at the next buffer entry.
    fn peek_buffer(&self) -> Option<&(Key, Option<RawValue>)> {
        self.buffer_entries.get(self.buffer_pos)
    }

    /// Advance buffer position.
    fn advance_buffer(&mut self) {
        self.buffer_pos += 1;
    }
}

impl<I: MvccIterator> Iterator for MvccScanIterator<I> {
    type Item = (Key, RawValue);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Ensure we have a pending storage result if available
            if self.pending_storage.is_none()
                && !self.storage_exhausted
                && self.advance_storage().is_err()
            {
                // On error, stop iteration
                return None;
            }

            // Get next candidates from storage and buffer
            let storage_entry = self.pending_storage.as_ref();
            let buffer_entry = self.peek_buffer();

            match (storage_entry, buffer_entry) {
                (None, None) => {
                    // Both exhausted
                    return None;
                }
                (Some((storage_key, storage_value)), None) => {
                    // Only storage entry available
                    let key = storage_key.clone();
                    let value = storage_value.clone();
                    self.last_user_key = Some(key.clone());
                    self.pending_storage = None;

                    if let Some(val) = value {
                        return Some((key, val));
                    }
                    // Tombstone, continue to next
                }
                (None, Some((buffer_key, buffer_value))) => {
                    // Only buffer entry available
                    let key = buffer_key.clone();
                    let value = buffer_value.clone();
                    self.last_user_key = Some(key.clone());
                    self.advance_buffer();

                    if let Some(val) = value {
                        return Some((key, val));
                    }
                    // Deleted, continue to next
                }
                (Some((storage_key, storage_value)), Some((buffer_key, buffer_value))) => {
                    use std::cmp::Ordering;

                    match storage_key.cmp(buffer_key) {
                        Ordering::Less => {
                            // Storage key comes first
                            let key = storage_key.clone();
                            let value = storage_value.clone();
                            self.last_user_key = Some(key.clone());
                            self.pending_storage = None;

                            if let Some(val) = value {
                                return Some((key, val));
                            }
                            // Tombstone, continue to next
                        }
                        Ordering::Greater => {
                            // Buffer key comes first
                            let key = buffer_key.clone();
                            let value = buffer_value.clone();
                            self.last_user_key = Some(key.clone());
                            self.advance_buffer();

                            if let Some(val) = value {
                                return Some((key, val));
                            }
                            // Deleted, continue to next
                        }
                        Ordering::Equal => {
                            // Same key - buffer overrides storage
                            let key = buffer_key.clone();
                            let value = buffer_value.clone();
                            self.last_user_key = Some(key.clone());
                            self.pending_storage = None;
                            self.advance_buffer();

                            if let Some(val) = value {
                                return Some((key, val));
                            }
                            // Deleted, continue to next
                        }
                    }
                }
            }
        }
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

        let results = storage.scan(range).unwrap();

        for (mvcc_key, value) in results {
            let (decoded_key, entry_ts) = mvcc_key.decode();
            if decoded_key == key && entry_ts <= ts {
                if is_tombstone(&value) {
                    return None;
                }
                return Some(value);
            }
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
    fn test_txn_service_get_put() {
        let (storage, txn_service, _dir) = create_test_service();

        let mut ctx = txn_service.begin(false).unwrap();

        // Put via transaction
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();

        // Should see buffered write
        let value = txn_service.get(&ctx, b"key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // Not yet in storage
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
    fn test_txn_service_delete() {
        let (_storage, txn_service, _dir) = create_test_service();

        let mut ctx = txn_service.begin(false).unwrap();

        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        txn_service.delete(&mut ctx, b"key1".to_vec()).unwrap();

        // Should see delete in buffer
        let value = txn_service.get(&ctx, b"key1").unwrap();
        assert!(value.is_none());
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
        let mut ctx = txn_service.begin(false).unwrap();

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

        // Transaction write
        txn_service
            .put(&mut ctx, b"key".to_vec(), b"v3".to_vec())
            .unwrap();

        // Transaction should see its own write
        let value = txn_service.get(&ctx, b"key").unwrap();
        assert_eq!(value, Some(b"v3".to_vec()));
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
        let results: Vec<_> = txn_service
            .scan(&ctx, b"a".to_vec()..b"d".to_vec())
            .unwrap()
            .collect();

        assert_eq!(
            results.len(),
            3,
            "scan should see 3 keys, start_ts={}, got {:?}",
            ctx.start_ts(),
            results
        );
    }

    #[test]
    fn test_txn_service_scan_read_your_writes() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Write some data via autocommit first
        txn_service.autocommit_put(b"a", b"1").unwrap();
        txn_service.autocommit_put(b"b", b"2").unwrap();

        // Begin a read-write transaction
        let mut ctx = txn_service.begin(false).unwrap();

        // Buffer some writes
        txn_service
            .put(&mut ctx, b"c".to_vec(), b"3".to_vec())
            .unwrap();
        txn_service
            .put(&mut ctx, b"b".to_vec(), b"modified".to_vec())
            .unwrap(); // Override existing
        txn_service.delete(&mut ctx, b"a".to_vec()).unwrap(); // Delete existing

        // Scan should merge buffered writes with storage (read-your-writes)
        let results: Vec<_> = txn_service
            .scan(&ctx, b"a".to_vec()..b"d".to_vec())
            .unwrap()
            .collect();

        // Should see:
        // - 'a' deleted (removed from results)
        // - 'b' modified (buffered write overrides storage)
        // - 'c' added (buffered write)
        assert_eq!(results.len(), 2, "scan should see 2 keys (a deleted)");

        let result_map: std::collections::HashMap<_, _> = results.into_iter().collect();
        assert_eq!(result_map.get(b"b".as_slice()), Some(&b"modified".to_vec()));
        assert_eq!(result_map.get(b"c".as_slice()), Some(&b"3".to_vec()));
        assert!(
            !result_map.contains_key(b"a".as_slice()),
            "a should be deleted"
        );
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
}
