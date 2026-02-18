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

//! Concurrency control for MVCC transactions.
//!
//! This module provides:
//! - max_ts tracking for MVCC read visibility
//! - Transaction state cache for OceanBase-style pessimistic locking
//!
//! Note: Timestamp allocation (TSO) is handled by the separate `tso` module.
//!
//! ## Design
//!
//! The ConcurrencyManager uses OceanBase-style pessimistic locking where:
//! - Locks are stored in the storage layer as pending nodes (via PessimisticStorage)
//! - Transaction state is tracked in a centralized TxnStateCache
//! - Readers skip pending/aborted nodes for correct MVCC visibility
//!
//! ```text
//! Pessimistic Write Flow:
//!   1. put_pending(key, value, start_ts) -> write pending node to storage
//!   2. If locked by another txn -> return KeyIsLocked error
//!   3. On commit: finalize_pending() sets commit_ts, makes writes visible
//!   4. On rollback: abort_pending() marks nodes as aborted
//!
//! Reader:
//!   1. Scan storage, iterator skips pending/aborted nodes
//!   2. Only sees committed versions with ts <= read_ts
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

use crate::catalog::types::Timestamp;
use crate::util::error::Result;

use super::api::TxnState;
use super::txn_state_cache::TxnStateCache;

// ============================================================================
// ConcurrencyManager - The public API
// ============================================================================

/// ConcurrencyManager provides max_ts tracking and transaction state management.
///
/// This is a component for MVCC concurrency control:
/// 1. **max_ts** - Maximum timestamp seen, for MVCC visibility checks
/// 2. **TxnStateCache** - Centralized transaction state for pessimistic locking
///
/// Note: Timestamp allocation is handled by the separate `TsoService`.
/// Lock management is handled by the storage layer via `PessimisticStorage`.
///
/// ## Thread Safety
///
/// ConcurrencyManager is designed to be shared across threads via `Arc`.
/// All operations are lock-free using atomic operations.
pub struct ConcurrencyManager {
    /// Maximum timestamp seen by any transaction.
    /// Used for MVCC read visibility checks.
    max_ts: AtomicU64,

    /// Centralized transaction state cache for pessimistic transactions.
    /// Maps start_ts to TxnState for OceanBase-style state-based conflict detection.
    txn_states: TxnStateCache,
}

impl ConcurrencyManager {
    /// Create a new ConcurrencyManager.
    ///
    /// The initial_max_ts should be set based on recovered state (max commit_ts).
    pub fn new(initial_max_ts: Timestamp) -> Self {
        Self {
            max_ts: AtomicU64::new(initial_max_ts),
            txn_states: TxnStateCache::new(),
        }
    }

    /// Update max_ts if the given ts is greater.
    ///
    /// This is called when transactions allocate timestamps to ensure
    /// proper MVCC visibility tracking.
    pub fn update_max_ts(&self, ts: Timestamp) {
        let mut current = self.max_ts.load(Ordering::Relaxed);
        while ts > current {
            match self.max_ts.compare_exchange_weak(
                current,
                ts,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }
    }

    /// Get the maximum timestamp seen.
    pub fn max_ts(&self) -> Timestamp {
        self.max_ts.load(Ordering::SeqCst)
    }

    // ========================================================================
    // Transaction State Cache Methods (OceanBase-style pessimistic locking)
    // ========================================================================

    /// Register a new transaction with Running state.
    ///
    /// Called when a pessimistic transaction begins (BEGIN/START TRANSACTION).
    pub fn register_txn(&self, start_ts: Timestamp) {
        self.txn_states.register(start_ts);
    }

    /// Get the state of a transaction.
    ///
    /// Returns `None` if the transaction is not registered.
    /// Used by readers to determine visibility of pending writes.
    pub fn get_txn_state(&self, start_ts: Timestamp) -> Option<TxnState> {
        self.txn_states.get(start_ts)
    }

    /// Transition a transaction from Running to Preparing.
    ///
    /// Called before computing commit_ts. Readers encountering pending nodes
    /// from this transaction will spin-wait until the state transitions to Prepared.
    pub fn set_preparing_txn(&self, start_ts: Timestamp) -> Result<()> {
        self.txn_states.set_preparing(start_ts)
    }

    /// Transition a transaction from Running or Preparing to Prepared.
    ///
    /// The `prepared_ts` ensures atomic visibility:
    /// - Readers with `read_ts <= prepared_ts` can safely skip
    /// - Readers with `read_ts > prepared_ts` must wait
    pub fn prepare_txn(&self, start_ts: Timestamp, prepared_ts: Timestamp) -> Result<()> {
        self.txn_states.prepare(start_ts, prepared_ts)
    }

    /// Transition a transaction from Prepared to Committed.
    ///
    /// For single-server 1PC, `commit_ts = prepared_ts`.
    pub fn commit_txn(&self, start_ts: Timestamp, commit_ts: Timestamp) -> Result<()> {
        self.txn_states.commit(start_ts, commit_ts)
    }

    /// Abort a transaction.
    ///
    /// Can be called from Running or Prepared state.
    pub fn abort_txn(&self, start_ts: Timestamp) -> Result<()> {
        self.txn_states.abort(start_ts)
    }

    /// Remove a transaction from the state cache.
    ///
    /// Called after commit/abort is complete.
    pub fn remove_txn(&self, start_ts: Timestamp) {
        self.txn_states.remove(start_ts);
    }

    /// Get the number of active transactions in the state cache.
    pub fn active_txn_count(&self) -> usize {
        self.txn_states.len()
    }
}

impl Default for ConcurrencyManager {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_ts_update() {
        let cm = ConcurrencyManager::new(0);

        cm.update_max_ts(10);
        assert_eq!(cm.max_ts(), 10);

        // Lower value shouldn't update
        cm.update_max_ts(5);
        assert_eq!(cm.max_ts(), 10);

        // Higher value should update
        cm.update_max_ts(20);
        assert_eq!(cm.max_ts(), 20);
    }

    #[test]
    fn test_concurrent_max_ts() {
        use std::sync::Arc;
        use std::thread;

        let cm = Arc::new(ConcurrencyManager::new(0));
        let mut handles = vec![];

        for i in 0..10 {
            let cm = Arc::clone(&cm);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    cm.update_max_ts((i * 100 + j) as u64);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // max_ts should be at least 999 (10 threads * 100 iterations - 1)
        assert!(cm.max_ts() >= 999);
    }

    #[test]
    fn test_txn_state_lifecycle() {
        let cm = ConcurrencyManager::new(0);
        let start_ts = 100;

        // Register
        cm.register_txn(start_ts);
        assert_eq!(cm.get_txn_state(start_ts), Some(TxnState::Running));
        assert_eq!(cm.active_txn_count(), 1);

        // Prepare
        cm.prepare_txn(start_ts, 150).unwrap();
        assert_eq!(
            cm.get_txn_state(start_ts),
            Some(TxnState::Prepared { prepared_ts: 150 })
        );

        // Commit
        cm.commit_txn(start_ts, 150).unwrap();
        assert_eq!(
            cm.get_txn_state(start_ts),
            Some(TxnState::Committed { commit_ts: 150 })
        );

        // Remove
        cm.remove_txn(start_ts);
        assert_eq!(cm.get_txn_state(start_ts), None);
        assert_eq!(cm.active_txn_count(), 0);
    }

    #[test]
    fn test_txn_state_abort() {
        let cm = ConcurrencyManager::new(0);
        let start_ts = 200;

        // Register and abort
        cm.register_txn(start_ts);
        cm.abort_txn(start_ts).unwrap();
        assert_eq!(cm.get_txn_state(start_ts), Some(TxnState::Aborted));

        // Remove
        cm.remove_txn(start_ts);
        assert_eq!(cm.get_txn_state(start_ts), None);
    }
}
