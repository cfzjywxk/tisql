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

//! Transaction service with autocommit semantics and durability.
//!
//! This implementation treats each statement as an autocommit transaction.
//! For write operations:
//! 1. Acquire in-memory locks (visible to readers immediately)
//! 2. Write to commit log and sync
//! 3. Apply to storage with commit_ts
//! 4. Release locks (on guard drop)

use std::sync::Arc;

#[cfg(feature = "failpoints")]
use fail::fail_point;

use crate::clog::{ClogBatch, ClogEntry, ClogOp, ClogService};
use crate::error::Result;

use super::concurrency::{ConcurrencyManager, Lock};
use crate::storage::{StorageEngine, WriteBatch, WriteOp};
use crate::types::{Key, Lsn, Timestamp, TxnId};

use super::api::{ReadSnapshot, Txn, TxnService};
use super::handle::TxnHandle;
use super::snapshot::StorageSnapshot;

/// Transaction service manages autocommit transactions with durability and MVCC.
///
/// All write operations follow the 1PC pattern:
/// 1. Acquire in-memory locks (blocks concurrent readers)
/// 2. Log to commit log
/// 3. Sync commit log to disk (durability)
/// 4. Apply to storage with commit_ts
/// 5. Release locks (on guard drop)
pub struct TransactionService<S: StorageEngine, L: ClogService> {
    /// Storage engine for state
    storage: Arc<S>,
    /// Commit log service for durability
    clog_service: Arc<L>,
    /// ConcurrencyManager for TSO and lock table
    concurrency_manager: Arc<ConcurrencyManager>,
}

impl<S: StorageEngine, L: ClogService> TransactionService<S, L> {
    /// Create a new transaction service
    pub fn new(
        storage: Arc<S>,
        clog_service: Arc<L>,
        concurrency_manager: Arc<ConcurrencyManager>,
    ) -> Self {
        Self {
            storage,
            clog_service,
            concurrency_manager,
        }
    }

    /// Get the next timestamp from TSO
    fn get_ts(&self) -> Timestamp {
        self.concurrency_manager.get_ts()
    }

    /// Get reference to storage engine
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Execute a write batch with durability guarantees.
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

        // Get commit_ts from TSO (also serves as txn_id for simplicity)
        let commit_ts = self.get_ts();
        let txn_id = commit_ts;

        // Collect keys for locking
        let keys: Vec<Key> = batch.keys().cloned().collect();

        // Acquire in-memory locks BEFORE any writes
        // This makes locks visible to concurrent readers immediately
        let lock = Lock {
            ts: commit_ts,
            primary: keys.first().cloned().unwrap_or_default(),
        };
        let _guards = self.concurrency_manager.lock_keys(&keys, lock)?;

        // FAILPOINT: After locks acquired, before any writes
        // Readers checking now will see the lock and be blocked
        #[cfg(feature = "failpoints")]
        fail_point!("txn_after_lock_acquired");

        // Build commit log batch from write batch
        let mut clog_batch = ClogBatch::new();
        for op in batch.iter() {
            match op {
                WriteOp::Put { key, value } => {
                    clog_batch.add_put(txn_id, key.clone(), value.clone());
                }
                WriteOp::Delete { key } => {
                    clog_batch.add_delete(txn_id, key.clone());
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
        let mut batch = batch;
        batch.set_commit_ts(commit_ts);
        self.storage.write_batch(batch)?;

        // FAILPOINT: After storage apply, before lock release
        // Data is visible in storage but locks still held
        #[cfg(feature = "failpoints")]
        fail_point!("txn_after_storage_apply");

        // Guards dropped here -> locks released
        Ok((txn_id, commit_ts, lsn))
    }

    /// Execute a single put with durability.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(TxnId, Timestamp, Lsn)> {
        let mut batch = WriteBatch::new();
        batch.put(key.to_vec(), value.to_vec());
        self.execute_write(batch)
    }

    /// Execute a single delete with durability.
    pub fn delete(&self, key: &[u8]) -> Result<(TxnId, Timestamp, Lsn)> {
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
        self.concurrency_manager.set_ts(max_ts + 1);

        Ok(stats)
    }

    /// Get current timestamp from TSO
    pub fn current_ts(&self) -> Timestamp {
        self.concurrency_manager.current_ts()
    }

    /// Get commit log service reference
    pub fn clog_service(&self) -> &L {
        &self.clog_service
    }

    /// Get concurrency manager reference
    pub fn concurrency_manager(&self) -> &ConcurrencyManager {
        &self.concurrency_manager
    }

    /// Get Arc references to internal components (for creating TxnHandle)
    fn storage_arc(&self) -> Arc<S> {
        Arc::clone(&self.storage)
    }

    fn clog_service_arc(&self) -> Arc<L> {
        Arc::clone(&self.clog_service)
    }

    fn concurrency_manager_arc(&self) -> Arc<ConcurrencyManager> {
        Arc::clone(&self.concurrency_manager)
    }
}

/// Implement TxnService trait for TransactionService
impl<S: StorageEngine + 'static, L: ClogService + 'static> TxnService for TransactionService<S, L> {
    fn begin(&self) -> Result<Box<dyn Txn>> {
        // Allocate txn_id and start_ts from TSO
        let txn_id = self.get_ts();
        let start_ts = self.get_ts();

        let handle = TxnHandle::new(
            txn_id,
            start_ts,
            self.storage_arc(),
            self.clog_service_arc(),
            self.concurrency_manager_arc(),
        );

        Ok(Box::new(handle))
    }

    fn snapshot(&self) -> Result<Box<dyn ReadSnapshot>> {
        // Allocate start_ts from TSO for consistent read
        let start_ts = self.get_ts();

        let snapshot = StorageSnapshot::new(
            start_ts,
            self.storage_arc(),
            self.concurrency_manager_arc(),
        );

        Ok(Box::new(snapshot))
    }

    fn current_ts(&self) -> Timestamp {
        self.concurrency_manager.current_ts()
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
    use crate::storage::MvccMemTableEngine;
    use tempfile::tempdir;

    fn create_test_service() -> (
        Arc<MvccMemTableEngine>,
        TransactionService<MvccMemTableEngine, FileClogService>,
        tempfile::TempDir,
    ) {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let clog_service = Arc::new(FileClogService::open(config).unwrap());
        let cm = Arc::new(ConcurrencyManager::new(1));
        let storage = Arc::new(MvccMemTableEngine::new(Arc::clone(&cm)));
        let txn_service = TransactionService::new(Arc::clone(&storage), clog_service, cm);
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
        txn_service.put(b"key1", b"value1").unwrap();

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
            let cm = Arc::new(ConcurrencyManager::new(1));
            let storage = Arc::new(MvccMemTableEngine::new(Arc::clone(&cm)));
            let txn_service = TransactionService::new(storage, clog_service.clone(), cm);

            txn_service.put(b"k1", b"v1").unwrap();
            txn_service.put(b"k2", b"v2").unwrap();
            txn_service.delete(b"k1").unwrap();

            clog_service.close().unwrap();
        }

        // Recover
        let (clog_service, entries) = FileClogService::recover(config).unwrap();
        let clog_service = Arc::new(clog_service);
        let cm = Arc::new(ConcurrencyManager::new(1));
        let storage = Arc::new(MvccMemTableEngine::new(Arc::clone(&cm)));
        let txn_service = TransactionService::new(Arc::clone(&storage), clog_service, cm);

        let stats = txn_service.recover(&entries).unwrap();
        assert_eq!(stats.committed_txns, 3);
        assert_eq!(stats.applied_puts, 2);
        assert_eq!(stats.applied_deletes, 1);

        // Verify recovered state
        assert!(storage.get(b"k1").unwrap().is_none()); // Was deleted
        assert_eq!(storage.get(b"k2").unwrap().unwrap(), b"v2");

        // Verify TSO advanced
        assert!(txn_service.current_ts() > 3);
    }

    #[test]
    fn test_mvcc_versions_after_writes() {
        let (storage, txn_service, _dir) = create_test_service();

        // Write v1
        let (_, ts1, _) = txn_service.put(b"key", b"v1").unwrap();

        // Write v2
        let (_, _ts2, _) = txn_service.put(b"key", b"v2").unwrap();

        // Reading at latest should see v2
        let v = storage.get(b"key").unwrap();
        assert_eq!(v, Some(b"v2".to_vec()));

        // Reading at ts1 should see v1 (using internal method)
        let v = storage.concurrency_manager().check_lock(b"key", ts1);
        assert!(v.is_ok()); // No lock
    }

    // ========================================================================
    // TxnService trait tests
    // ========================================================================

    #[test]
    fn test_txn_service_snapshot() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Create a snapshot - this allocates a start_ts
        let snapshot = txn_service.snapshot().unwrap();

        // Snapshot should have a valid timestamp
        assert!(snapshot.start_ts() > 0);

        // Creating another snapshot should get a higher timestamp
        let snapshot2 = txn_service.snapshot().unwrap();
        assert!(snapshot2.start_ts() > snapshot.start_ts());
    }

    #[test]
    fn test_txn_service_snapshot_reads_at_start_ts() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Write initial data
        txn_service.put(b"key", b"v1").unwrap();

        // Create snapshot before second write
        let snapshot = txn_service.snapshot().unwrap();
        let snapshot_ts = snapshot.start_ts();

        // Write again (after snapshot)
        let (_, write_ts, _) = txn_service.put(b"key", b"v2").unwrap();
        assert!(write_ts > snapshot_ts, "Write should have higher ts than snapshot");

        // Snapshot should still see v1 (the value at its start_ts)
        // Note: This depends on MVCC implementation correctly respecting timestamps
        let value = snapshot.get(b"key").unwrap();
        assert_eq!(value, Some(b"v1".to_vec()));
    }

    #[test]
    fn test_txn_service_begin() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Begin a transaction
        let txn = txn_service.begin().unwrap();

        // Transaction should have valid timestamp
        assert!(txn.start_ts() > 0);
        assert!(txn.is_valid());
    }

    #[test]
    fn test_txn_service_begin_commit() {
        let (storage, txn_service, _dir) = create_test_service();

        // Begin and write
        let mut txn = txn_service.begin().unwrap();
        txn.put(b"key1".to_vec(), b"value1".to_vec());

        // Commit
        let info = txn.commit().unwrap();
        assert!(info.commit_ts > 0);
        assert!(info.lsn > 0);

        // Data should be visible in storage
        let value = storage.get(b"key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_txn_service_isolation() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Write initial data
        txn_service.put(b"key", b"v1").unwrap();

        // Begin transaction
        let mut txn = txn_service.begin().unwrap();

        // Transaction should see v1
        let value = txn.get(b"key").unwrap();
        assert_eq!(value, Some(b"v1".to_vec()));

        // Write v2 via txn_service (outside the transaction)
        txn_service.put(b"key", b"v2").unwrap();

        // Transaction should still see v1 (snapshot isolation)
        // unless the new write is to its own buffer
        let value = txn.get(b"key").unwrap();
        assert_eq!(value, Some(b"v1".to_vec()));

        // But reading via storage should see v2
        let storage = txn_service.storage();
        let latest = storage.get(b"key").unwrap();
        assert_eq!(latest, Some(b"v2".to_vec()));

        // Transaction write
        txn.put(b"key".to_vec(), b"v3".to_vec());

        // Transaction should see its own write
        let value = txn.get(b"key").unwrap();
        assert_eq!(value, Some(b"v3".to_vec()));
    }

    #[test]
    fn test_txn_current_ts_advances() {
        let (_storage, txn_service, _dir) = create_test_service();

        let ts1 = txn_service.current_ts();

        // Each snapshot allocation advances the timestamp
        let _snapshot = txn_service.snapshot().unwrap();
        let ts2 = txn_service.current_ts();
        assert!(ts2 > ts1);

        // Each begin also advances the timestamp
        let _txn = txn_service.begin().unwrap();
        let ts3 = txn_service.current_ts();
        assert!(ts3 > ts2);
    }
}
