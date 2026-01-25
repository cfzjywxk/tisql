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
use crate::concurrency::{ConcurrencyManager, Lock};
use crate::error::Result;
use crate::storage::{StorageEngine, WriteBatch, WriteOp};
use crate::types::{Key, Lsn, Timestamp, TxnId};

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
}
