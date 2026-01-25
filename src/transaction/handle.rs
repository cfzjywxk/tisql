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

//! Read-write transaction handle implementation.

use std::ops::Range;
use std::sync::Arc;

use crate::clog::{ClogBatch, ClogService};
use crate::concurrency::{ConcurrencyManager, KeyGuard, Lock};
use crate::error::Result;
use crate::storage::{StorageEngine, WriteBatch, WriteOp};
use crate::types::{Key, RawValue, Timestamp, TxnId};

use super::api::{CommitInfo, ReadSnapshot, Txn};

/// Transaction state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnState {
    /// Transaction is active and accepting operations
    Active,
    /// Transaction has been committed
    Committed,
    /// Transaction has been aborted/rolled back
    Aborted,
}

/// Read-write transaction implementation.
///
/// Buffers writes and applies them atomically on commit following 1PC pattern.
pub struct TxnHandle<S: StorageEngine, L: ClogService> {
    /// Transaction ID (assigned at begin time)
    txn_id: TxnId,
    /// Start timestamp for reads
    start_ts: Timestamp,
    /// Current state
    state: TxnState,
    /// Buffered writes
    write_buffer: WriteBatch,
    /// Reference to storage engine
    storage: Arc<S>,
    /// Reference to commit log service
    clog_service: Arc<L>,
    /// Reference to concurrency manager
    concurrency_manager: Arc<ConcurrencyManager>,
}

impl<S: StorageEngine, L: ClogService> TxnHandle<S, L> {
    /// Create a new transaction handle.
    pub fn new(
        txn_id: TxnId,
        start_ts: Timestamp,
        storage: Arc<S>,
        clog_service: Arc<L>,
        concurrency_manager: Arc<ConcurrencyManager>,
    ) -> Self {
        // Update max_ts for MVCC
        concurrency_manager.update_max_ts(start_ts);

        Self {
            txn_id,
            start_ts,
            state: TxnState::Active,
            write_buffer: WriteBatch::new(),
            storage,
            clog_service,
            concurrency_manager,
        }
    }

    /// Check if the transaction is in a valid state for operations.
    fn check_active(&self) -> Result<()> {
        match self.state {
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

impl<S: StorageEngine, L: ClogService> ReadSnapshot for TxnHandle<S, L> {
    fn start_ts(&self) -> Timestamp {
        self.start_ts
    }

    fn get(&self, key: &[u8]) -> Result<Option<RawValue>> {
        // First check our write buffer for uncommitted writes
        // Iterate in reverse order to get the most recent operation
        for op in self.write_buffer.iter().rev() {
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
        self.concurrency_manager.check_lock(key, self.start_ts)?;

        // Read from storage at our snapshot timestamp
        self.storage.get_at(key, self.start_ts)
    }

    fn scan(&self, range: Range<Key>) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>> {
        // Note: This simplified scan doesn't merge with write buffer
        // A full implementation would need to merge buffered writes with storage scan

        // Check for locks in range
        if !range.is_empty() {
            self.concurrency_manager
                .check_range(&range.start, &range.end, self.start_ts)?;
        }

        self.storage.scan(range)
    }
}

impl<S: StorageEngine, L: ClogService> Txn for TxnHandle<S, L> {
    fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    fn is_valid(&self) -> bool {
        self.state == TxnState::Active
    }

    fn put(&mut self, key: Key, value: RawValue) {
        if self.state == TxnState::Active {
            self.write_buffer.put(key, value);
        }
    }

    fn delete(&mut self, key: Key) {
        if self.state == TxnState::Active {
            self.write_buffer.delete(key);
        }
    }

    fn commit(mut self: Box<Self>) -> Result<CommitInfo> {
        self.check_active()?;

        if self.write_buffer.is_empty() {
            // No writes - just mark as committed
            self.state = TxnState::Committed;
            return Ok(CommitInfo {
                txn_id: self.txn_id,
                commit_ts: self.start_ts,
                lsn: 0,
            });
        }

        // Get commit_ts from TSO
        let commit_ts = self.concurrency_manager.get_ts();

        // Collect keys for locking
        let keys: Vec<Key> = self.write_buffer.keys().cloned().collect();

        // Acquire in-memory locks BEFORE any writes
        let lock = Lock {
            ts: commit_ts,
            primary: keys.first().cloned().unwrap_or_default(),
        };
        let _guards: Vec<KeyGuard> = self.concurrency_manager.lock_keys(&keys, lock)?;

        // Build commit log batch
        let mut clog_batch = ClogBatch::new();
        for op in self.write_buffer.iter() {
            match op {
                WriteOp::Put { key, value } => {
                    clog_batch.add_put(self.txn_id, key.clone(), value.clone());
                }
                WriteOp::Delete { key } => {
                    clog_batch.add_delete(self.txn_id, key.clone());
                }
            }
        }
        clog_batch.add_commit(self.txn_id, commit_ts);

        // Write to commit log with sync (durability guarantee)
        let lsn = self.clog_service.write(&mut clog_batch, true)?;

        // Apply to storage with commit_ts
        self.write_buffer.set_commit_ts(commit_ts);
        self.storage.write_batch(self.write_buffer.clone())?;

        // Mark as committed (guards will be dropped, releasing locks)
        self.state = TxnState::Committed;

        Ok(CommitInfo {
            txn_id: self.txn_id,
            commit_ts,
            lsn,
        })
    }

    fn rollback(mut self: Box<Self>) -> Result<()> {
        self.check_active()?;

        // Clear write buffer
        self.write_buffer.clear();

        // Mark as aborted
        self.state = TxnState::Aborted;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clog::{FileClogConfig, FileClogService};
    use crate::storage::MvccMemTableEngine;
    use tempfile::tempdir;

    fn create_test_txn() -> (
        Box<TxnHandle<MvccMemTableEngine, FileClogService>>,
        Arc<MvccMemTableEngine>,
        tempfile::TempDir,
    ) {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let clog_service = Arc::new(FileClogService::open(config).unwrap());
        let cm = Arc::new(ConcurrencyManager::new(1));
        let storage = Arc::new(MvccMemTableEngine::new(Arc::clone(&cm)));

        let txn_id = cm.get_ts();
        let start_ts = cm.get_ts();

        let txn = Box::new(TxnHandle::new(
            txn_id,
            start_ts,
            Arc::clone(&storage),
            clog_service,
            cm,
        ));

        (txn, storage, dir)
    }

    #[test]
    fn test_txn_put_get() {
        let (mut txn, _storage, _dir) = create_test_txn();

        txn.put(b"key1".to_vec(), b"value1".to_vec());

        // Should see buffered write
        let value = txn.get(b"key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_txn_delete() {
        let (mut txn, _storage, _dir) = create_test_txn();

        txn.put(b"key1".to_vec(), b"value1".to_vec());
        txn.delete(b"key1".to_vec());

        // Should see delete in buffer
        let value = txn.get(b"key1").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn test_txn_commit() {
        let (mut txn, storage, _dir) = create_test_txn();

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
    fn test_txn_rollback() {
        let (mut txn, storage, _dir) = create_test_txn();

        txn.put(b"key1".to_vec(), b"value1".to_vec());

        // Rollback
        txn.rollback().unwrap();

        // Data should NOT be in storage
        let value = storage.get(b"key1").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn test_txn_is_valid() {
        let (txn, _, _dir) = create_test_txn();

        assert!(txn.is_valid());

        // After commit, should not be valid
        let info = txn.commit();
        assert!(info.is_ok());
        // Can't test is_valid after consume, but the state change is tested
    }
}
