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

use crate::clog::{ClogBatch, ClogEntry, ClogOp, ClogService};
use crate::error::{Result, TiSqlError};
use crate::tso::TsoService;

use super::api::{CommitInfo, TxnCtx, TxnService, TxnState};
use super::concurrency::{ConcurrencyManager, Lock};
use crate::storage::{StorageEngine, WriteBatch, WriteOp};
use crate::types::{Key, Lsn, RawValue, Timestamp, TxnId};

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
    /// This is the core method for autocommit transactions with 1PC:
    /// 1. Allocate commit_ts from TSO
    /// 2. Acquire in-memory locks (blocks readers)
    /// 3. Log all operations to commit log
    /// 4. Sync commit log to disk (durability)
    /// 5. Apply to storage with commit_ts
    /// 6. Release locks (guards dropped)
    pub fn execute_write(&self, batch: WriteBatch) -> Result<(TxnId, Timestamp, Lsn)> {
        if batch.is_empty() {
            let ts = self.get_ts();
            return Ok((0, ts, 0));
        }

        // Collect keys for locking (need owned Keys for lock_keys API)
        let keys: Vec<Key> = batch.keys().cloned().collect();

        // Acquire in-memory locks BEFORE getting commit_ts
        // This prevents the "time-travel" anomaly where a reader with start_ts > commit_ts
        // could miss data that commits after they started reading.
        //
        // By acquiring locks first:
        // 1. Any concurrent reader will see the lock and be blocked
        // 2. We then get commit_ts which is guaranteed > any concurrent reader's start_ts
        //    because: start_ts = max(TSO, max_ts), and we update max_ts via lock_keys
        //
        // Get a preliminary timestamp for the lock (will be updated to commit_ts)
        let preliminary_ts = self.get_ts();
        let lock = Lock {
            ts: preliminary_ts,
            primary: keys.first().cloned().unwrap_or_default(),
        };
        let _guards = self.concurrency_manager.lock_keys(&keys, lock)?;

        // FAILPOINT: After locks acquired, before commit_ts computation.
        // This is used to test the "commit in the past" fix - any reader
        // starting now will have its start_ts recorded in max_ts, ensuring
        // the writer's commit_ts > reader's start_ts.
        #[cfg(feature = "failpoints")]
        fail_point!("txn_after_lock_before_commit_ts");

        // Now get commit_ts AFTER acquiring locks
        // This ensures commit_ts > any concurrent reader's start_ts
        let max_ts = self.concurrency_manager.max_ts();
        let tso_ts = self.get_ts();
        let commit_ts = std::cmp::max(max_ts + 1, tso_ts);
        let txn_id = commit_ts;

        // FAILPOINT: After commit_ts set, before any writes
        // Readers checking now will see the lock and be blocked
        #[cfg(feature = "failpoints")]
        fail_point!("txn_after_lock_acquired");

        // Build commit log batch and storage batch from write ops
        // Take ownership of ops to avoid extra clones
        let mut clog_batch = ClogBatch::new();
        let mut storage_batch = WriteBatch::new();

        for op in batch.into_ops() {
            match op {
                WriteOp::Put { key, value } => {
                    // Clone for clog, use owned for storage
                    clog_batch.add_put(txn_id, key.clone(), value.clone());
                    storage_batch.put(key, value);
                }
                WriteOp::Delete { key } => {
                    clog_batch.add_delete(txn_id, key.clone());
                    storage_batch.delete(key);
                }
            }
        }

        // Add commit record
        clog_batch.add_commit(txn_id, commit_ts);

        // Write to commit log with sync (durability guarantee)
        let lsn = self.clog_service.write(&mut clog_batch, true)?;

        // FAILPOINT: After clog write, before storage apply
        // Data is durable but not yet visible in storage
        #[cfg(feature = "failpoints")]
        fail_point!("txn_after_clog_write");

        // Apply to storage engine with commit_ts
        storage_batch.set_commit_ts(commit_ts);
        self.storage.write_batch(storage_batch)?;

        // FAILPOINT: After storage apply, before lock release
        // Data is visible in storage but locks still held
        #[cfg(feature = "failpoints")]
        fail_point!("txn_after_storage_apply");

        // Guards dropped here -> locks released
        Ok((txn_id, commit_ts, lsn))
    }

    /// Execute a single put with autocommit (for testing and simple writes).
    pub fn autocommit_put(&self, key: &[u8], value: &[u8]) -> Result<(TxnId, Timestamp, Lsn)> {
        let mut batch = WriteBatch::new();
        batch.put(key.to_vec(), value.to_vec());
        self.execute_write(batch)
    }

    /// Execute a single delete with autocommit (for testing and simple writes).
    pub fn autocommit_delete(&self, key: &[u8]) -> Result<(TxnId, Timestamp, Lsn)> {
        let mut batch = WriteBatch::new();
        batch.delete(key.to_vec());
        self.execute_write(batch)
    }

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
        // Iterate in reverse order to get the most recent operation
        for op in ctx.write_buffer.iter().rev() {
            match op {
                WriteOp::Put { key: k, value } if k.as_slice() == key => {
                    return Ok(Some(value.clone()));
                }
                WriteOp::Delete { key: k } if k.as_slice() == key => {
                    return Ok(None);
                }
                _ => {}
            }
        }

        // Check for locks from other transactions
        self.concurrency_manager.check_lock(key, ctx.start_ts)?;

        // Read from storage at snapshot timestamp
        self.storage.get_at(key, ctx.start_ts)
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

        // Get storage scan results (pass reference to avoid cloning)
        let storage_iter = self.storage.scan_at(&range, ctx.start_ts)?;

        // Collect buffered writes in range for read-your-writes semantics
        // Use a map to track the latest operation per key
        let mut buffer_ops: std::collections::BTreeMap<Key, Option<RawValue>> =
            std::collections::BTreeMap::new();

        for op in ctx.write_buffer.iter() {
            match op {
                WriteOp::Put { key, value } => {
                    if key >= &range.start && key < &range.end {
                        buffer_ops.insert(key.clone(), Some(value.clone()));
                    }
                }
                WriteOp::Delete { key } => {
                    if key >= &range.start && key < &range.end {
                        buffer_ops.insert(key.clone(), None); // None = deleted
                    }
                }
            }
        }

        // Merge storage results with buffered writes
        // Buffered writes override storage values
        let mut results: std::collections::BTreeMap<Key, RawValue> = storage_iter.collect();

        for (key, value_opt) in buffer_ops {
            match value_opt {
                Some(value) => {
                    results.insert(key, value);
                }
                None => {
                    results.remove(&key); // Buffered delete removes the key
                }
            }
        }

        Ok(Box::new(results.into_iter()))
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

        // Collect keys for locking (need owned Keys for lock_keys API)
        let keys: Vec<Key> = ctx.write_buffer.keys().cloned().collect();

        // Acquire in-memory locks BEFORE getting commit_ts
        // This prevents the "time-travel" anomaly (see execute_write for details)
        let preliminary_ts = self.get_ts();
        let lock = Lock {
            ts: preliminary_ts,
            primary: keys.first().cloned().unwrap_or_default(),
        };
        let _guards = self.concurrency_manager.lock_keys(&keys, lock)?;

        // FAILPOINT: After locks acquired, before commit_ts computation
        #[cfg(feature = "failpoints")]
        fail_point!("txn_after_lock_before_commit_ts");

        // Get commit_ts AFTER acquiring locks to ensure commit_ts > concurrent reader's start_ts
        let max_ts = self.concurrency_manager.max_ts();
        let tso_ts = self.get_ts();
        let commit_ts = std::cmp::max(max_ts + 1, tso_ts);

        // Build commit log batch from write buffer ops
        // Use into_ops() to take ownership and avoid cloning
        let mut clog_batch = ClogBatch::new();
        let txn_id = ctx.txn_id;
        let ops = ctx.write_buffer.into_ops();
        let mut storage_batch = WriteBatch::new();

        for op in ops {
            match op {
                WriteOp::Put { key, value } => {
                    // Add to clog (takes ownership)
                    clog_batch.add_put(txn_id, key.clone(), value.clone());
                    // Add to storage batch
                    storage_batch.put(key, value);
                }
                WriteOp::Delete { key } => {
                    clog_batch.add_delete(txn_id, key.clone());
                    storage_batch.delete(key);
                }
            }
        }
        clog_batch.add_commit(txn_id, commit_ts);

        // Write to commit log with sync (durability guarantee)
        let lsn = self.clog_service.write(&mut clog_batch, true)?;

        // Apply to storage with commit_ts
        storage_batch.set_commit_ts(commit_ts);
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
    use crate::storage::ArenaMemTableEngine;
    use crate::tso::LocalTso;
    use tempfile::tempdir;

    type TestStorage = ArenaMemTableEngine;

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
        let storage = Arc::new(ArenaMemTableEngine::new());
        let txn_service = TransactionService::new(Arc::clone(&storage), clog_service, tso, cm);
        (storage, txn_service, dir)
    }

    #[test]
    fn test_execute_write() {
        let (storage, txn_service, _dir) = create_test_service();

        // Execute a write
        let mut batch = WriteBatch::new();
        batch.put(b"key1".to_vec(), b"value1".to_vec());
        batch.put(b"key2".to_vec(), b"value2".to_vec());

        let (txn_id, commit_ts, lsn) = txn_service.execute_write(batch).unwrap();
        assert!(txn_id > 0);
        assert!(commit_ts > 0);
        assert!(lsn > 0);

        // Verify storage
        let v1 = storage.get(b"key1").unwrap().unwrap();
        assert_eq!(v1, b"value1");
        let v2 = storage.get(b"key2").unwrap().unwrap();
        assert_eq!(v2, b"value2");
    }

    #[test]
    fn test_execute_write_with_locking() {
        let (storage, txn_service, _dir) = create_test_service();
        let cm = txn_service.concurrency_manager();

        // Before write, no locks
        assert_eq!(cm.lock_count(), 0);

        // Execute write
        txn_service.autocommit_put(b"key1", b"value1").unwrap();

        // After write, locks should be released
        assert_eq!(cm.lock_count(), 0);

        // Data should be there
        let v = storage.get(b"key1").unwrap();
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
            let storage = Arc::new(ArenaMemTableEngine::new());
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
        let storage = Arc::new(ArenaMemTableEngine::new());
        let txn_service = TransactionService::new(Arc::clone(&storage), clog_service, tso, cm);

        let stats = txn_service.recover(&entries).unwrap();
        assert_eq!(stats.committed_txns, 3);
        assert_eq!(stats.applied_puts, 2);
        assert_eq!(stats.applied_deletes, 1);

        // Verify recovered state
        assert!(storage.get(b"k1").unwrap().is_none()); // Was deleted
        assert_eq!(storage.get(b"k2").unwrap().unwrap(), b"v2");

        // Verify TSO advanced (use tso().last_ts() to check state)
        assert!(txn_service.tso().last_ts() > 3);
    }

    #[test]
    fn test_mvcc_versions_after_writes() {
        let (storage, txn_service, _dir) = create_test_service();

        // Write v1
        let (_, ts1, _) = txn_service.autocommit_put(b"key", b"v1").unwrap();

        // Write v2
        let (_, ts2, _) = txn_service.autocommit_put(b"key", b"v2").unwrap();

        // Reading at latest should see v2
        let v = storage.get(b"key").unwrap();
        assert_eq!(v, Some(b"v2".to_vec()));

        // Reading at ts1 should see v1 (MVCC visibility)
        let v = storage.get_at(b"key", ts1).unwrap();
        assert_eq!(v, Some(b"v1".to_vec()));

        // Reading at ts2 should see v2
        let v = storage.get_at(b"key", ts2).unwrap();
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
        let storage_value = storage.get(b"key1").unwrap();
        assert!(storage_value.is_none());

        // Commit
        let info = txn_service.commit(ctx).unwrap();
        assert!(info.commit_ts > 0);
        assert!(info.lsn > 0);

        // Now visible in storage
        let storage_value = storage.get(b"key1").unwrap();
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
        let value = storage.get(b"key1").unwrap();
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
        let latest = txn_service.storage().get(b"key").unwrap();
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
        let value = storage.get(b"key1").unwrap();
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
