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
//! For write operations, logs are synced to disk before returning success.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::clog::{ClogBatch, ClogEntry, ClogOp, ClogService};
use crate::error::Result;
use crate::storage::{StorageEngine, WriteBatch, WriteOp};
use crate::types::{Lsn, Timestamp, TxnId};

/// Transaction service manages autocommit transactions with durability.
///
/// All write operations are:
/// 1. Logged to commit log
/// 2. Synced to disk (fsync)
/// 3. Applied to storage engine
/// 4. Then returned as success
pub struct TransactionService<S: StorageEngine, L: ClogService> {
    /// Storage engine for state
    storage: Arc<S>,
    /// Commit log service for durability
    clog_service: Arc<L>,
    /// Next transaction ID
    next_txn_id: AtomicU64,
    /// Current timestamp (simple monotonic counter for now)
    current_ts: AtomicU64,
}

impl<S: StorageEngine, L: ClogService> TransactionService<S, L> {
    /// Create a new transaction service
    pub fn new(storage: Arc<S>, clog_service: Arc<L>) -> Self {
        Self {
            storage,
            clog_service,
            next_txn_id: AtomicU64::new(1),
            current_ts: AtomicU64::new(1),
        }
    }

    /// Get the next transaction ID
    fn next_txn_id(&self) -> TxnId {
        self.next_txn_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the next timestamp
    fn next_ts(&self) -> Timestamp {
        self.current_ts.fetch_add(1, Ordering::SeqCst)
    }

    /// Get reference to storage engine
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Execute a write batch with durability guarantees.
    ///
    /// This is the core method for autocommit transactions:
    /// 1. Assign transaction ID
    /// 2. Log all operations to commit log
    /// 3. Sync commit log to disk (durability)
    /// 4. Apply to storage engine
    /// 5. Return commit timestamp
    pub fn execute_write(&self, batch: WriteBatch) -> Result<(TxnId, Timestamp, Lsn)> {
        if batch.is_empty() {
            let ts = self.next_ts();
            return Ok((0, ts, 0));
        }

        let txn_id = self.next_txn_id();
        let commit_ts = self.next_ts();

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

        // Apply to storage engine
        self.storage.write_batch(batch)?;

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
        let mut max_txn_id: TxnId = 0;
        let mut max_ts: Timestamp = 0;

        // First pass: identify committed transactions
        let mut committed_txns = std::collections::HashSet::new();
        for entry in entries {
            max_txn_id = max_txn_id.max(entry.txn_id);
            if let ClogOp::Commit { commit_ts } = entry.op {
                committed_txns.insert(entry.txn_id);
                max_ts = max_ts.max(commit_ts);
                stats.committed_txns += 1;
            }
        }

        // Second pass: apply committed operations
        for entry in entries {
            if !committed_txns.contains(&entry.txn_id) {
                stats.rolled_back_entries += 1;
                continue;
            }

            match &entry.op {
                ClogOp::Put { key, value } => {
                    self.storage.put(key, value)?;
                    stats.applied_puts += 1;
                }
                ClogOp::Delete { key } => {
                    self.storage.delete(key)?;
                    stats.applied_deletes += 1;
                }
                ClogOp::Commit { .. } | ClogOp::Rollback => {
                    // Already processed
                }
            }
        }

        // Update internal counters to continue from recovered state
        self.next_txn_id.store(max_txn_id + 1, Ordering::SeqCst);
        self.current_ts.store(max_ts + 1, Ordering::SeqCst);

        Ok(stats)
    }

    /// Get current timestamp
    pub fn current_ts(&self) -> Timestamp {
        self.current_ts.load(Ordering::SeqCst)
    }

    /// Get commit log service reference
    pub fn clog_service(&self) -> &L {
        &self.clog_service
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
    use tempfile::tempdir;

    #[test]
    fn test_execute_write() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let clog_service = Arc::new(FileClogService::open(config).unwrap());
        let storage = Arc::new(MemTableEngine::new());
        let txn_service = TransactionService::new(storage.clone(), clog_service);

        // Execute a write
        let mut batch = WriteBatch::new();
        batch.put(b"key1".to_vec(), b"value1".to_vec());
        batch.put(b"key2".to_vec(), b"value2".to_vec());

        let (txn_id, commit_ts, lsn) = txn_service.execute_write(batch).unwrap();
        assert_eq!(txn_id, 1);
        assert!(commit_ts > 0);
        assert!(lsn > 0);

        // Verify storage
        let v1 = storage.get(b"key1").unwrap().unwrap();
        assert_eq!(v1, b"value1");
        let v2 = storage.get(b"key2").unwrap().unwrap();
        assert_eq!(v2, b"value2");
    }

    #[test]
    fn test_recover() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        // Write some data
        {
            let clog_service = Arc::new(FileClogService::open(config.clone()).unwrap());
            let storage = Arc::new(MemTableEngine::new());
            let txn_service = TransactionService::new(storage, clog_service.clone());

            txn_service.put(b"k1", b"v1").unwrap();
            txn_service.put(b"k2", b"v2").unwrap();
            txn_service.delete(b"k1").unwrap();

            clog_service.close().unwrap();
        }

        // Recover
        let (clog_service, entries) = FileClogService::recover(config).unwrap();
        let clog_service = Arc::new(clog_service);
        let storage = Arc::new(MemTableEngine::new());
        let txn_service = TransactionService::new(storage.clone(), clog_service);

        let stats = txn_service.recover(&entries).unwrap();
        assert_eq!(stats.committed_txns, 3);
        assert_eq!(stats.applied_puts, 2);
        assert_eq!(stats.applied_deletes, 1);

        // Verify recovered state
        assert!(storage.get(b"k1").unwrap().is_none()); // Was deleted
        assert_eq!(storage.get(b"k2").unwrap().unwrap(), b"v2");

        // Verify counters advanced
        assert!(txn_service.current_ts() > 3);
    }
}
