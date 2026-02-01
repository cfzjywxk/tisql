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

//! Centralized transaction state cache for OceanBase-style pessimistic locking.
//!
//! This module provides a centralized cache mapping `start_ts` to `TxnState`.
//! Readers lookup transaction state by `owner_start_ts` from pending version nodes
//! to determine visibility, eliminating the need for a per-key lock table.
//!
//! ## Design (from OceanBase)
//!
//! 1. **Centralized state**: All transaction states in one cache
//! 2. **State-based conflict detection**: Readers lookup txn state, not per-key locks
//! 3. **Atomic visibility**: State transition is atomic, all writes visible atomically
//!
//! ## State Transitions
//!
//! ```text
//! register() ─────► Running
//!                      │
//!          prepare()   │   (prepared_ts = max(max_ts, tso_ts))
//!                      ▼
//!                  Prepared
//!                      │
//!       ┌──────────────┼──────────────┐
//!       │              │              │
//!  commit()           │         abort()
//!       ▼              │              ▼
//!   Committed          │          Aborted
//!       │              │              │
//!       └──────────────┴──────────────┘
//!                      │
//!                  remove()
//! ```

use dashmap::DashMap;

use crate::error::{Result, TiSqlError};
use crate::types::Timestamp;

use super::api::TxnState;

/// Centralized cache for transaction states.
///
/// This cache maps `start_ts` to `TxnState`, enabling readers to determine
/// visibility of pending writes without per-key lock tables.
///
/// ## Thread Safety
///
/// Uses `DashMap` for concurrent access. All operations are thread-safe.
pub struct TxnStateCache {
    /// Map from start_ts to transaction state
    states: DashMap<Timestamp, TxnState>,
}

impl TxnStateCache {
    /// Create a new empty transaction state cache.
    pub fn new() -> Self {
        Self {
            states: DashMap::new(),
        }
    }

    /// Register a new transaction with `Running` state.
    ///
    /// Called when a transaction begins (BEGIN/START TRANSACTION).
    pub fn register(&self, start_ts: Timestamp) {
        self.states.insert(start_ts, TxnState::Running);
    }

    /// Get the state of a transaction.
    ///
    /// Returns `None` if the transaction is not registered (never existed or already removed).
    pub fn get(&self, start_ts: Timestamp) -> Option<TxnState> {
        self.states.get(&start_ts).map(|v| *v)
    }

    /// Transition a transaction from Running to Prepared.
    ///
    /// The `prepared_ts` ensures atomic visibility:
    /// - Readers with `read_ts <= prepared_ts` can safely skip (commit_ts > read_ts guaranteed)
    /// - Readers with `read_ts > prepared_ts` must wait (KeyIsLocked)
    ///
    /// # Errors
    ///
    /// Returns error if the transaction is not in `Running` state.
    pub fn prepare(&self, start_ts: Timestamp, prepared_ts: Timestamp) -> Result<()> {
        let mut entry = self.states.get_mut(&start_ts).ok_or_else(|| {
            TiSqlError::Internal(format!(
                "Cannot prepare: transaction {start_ts} not found in state cache"
            ))
        })?;

        match *entry {
            TxnState::Running => {
                *entry = TxnState::Prepared { prepared_ts };
                Ok(())
            }
            ref state => Err(TiSqlError::Internal(format!(
                "Cannot prepare transaction {start_ts}: expected Running, found {state:?}"
            ))),
        }
    }

    /// Transition a transaction from Prepared to Committed.
    ///
    /// For single-server 1PC, `commit_ts = prepared_ts`.
    ///
    /// # Errors
    ///
    /// Returns error if the transaction is not in `Prepared` state.
    pub fn commit(&self, start_ts: Timestamp, commit_ts: Timestamp) -> Result<()> {
        let mut entry = self.states.get_mut(&start_ts).ok_or_else(|| {
            TiSqlError::Internal(format!(
                "Cannot commit: transaction {start_ts} not found in state cache"
            ))
        })?;

        match *entry {
            TxnState::Prepared { .. } => {
                *entry = TxnState::Committed { commit_ts };
                Ok(())
            }
            ref state => Err(TiSqlError::Internal(format!(
                "Cannot commit transaction {start_ts}: expected Prepared, found {state:?}"
            ))),
        }
    }

    /// Transition a transaction to Aborted state.
    ///
    /// Can be called from `Running` or `Prepared` state.
    ///
    /// # Errors
    ///
    /// Returns error if the transaction is already committed or aborted.
    pub fn abort(&self, start_ts: Timestamp) -> Result<()> {
        let mut entry = self.states.get_mut(&start_ts).ok_or_else(|| {
            TiSqlError::Internal(format!(
                "Cannot abort: transaction {start_ts} not found in state cache"
            ))
        })?;

        match *entry {
            TxnState::Running | TxnState::Prepared { .. } => {
                *entry = TxnState::Aborted;
                Ok(())
            }
            TxnState::Committed { .. } => Err(TiSqlError::Internal(format!(
                "Cannot abort transaction {start_ts}: already committed"
            ))),
            TxnState::Aborted => {
                // Idempotent: already aborted
                Ok(())
            }
        }
    }

    /// Remove a transaction from the cache.
    ///
    /// Called after commit/abort is complete and pending nodes are finalized.
    pub fn remove(&self, start_ts: Timestamp) {
        self.states.remove(&start_ts);
    }

    /// Get the number of transactions in the cache (for diagnostics).
    pub fn len(&self) -> usize {
        self.states.len()
    }
}

impl Default for TxnStateCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_get() {
        let cache = TxnStateCache::new();

        cache.register(100);
        assert_eq!(cache.get(100), Some(TxnState::Running));
        assert_eq!(cache.get(200), None);
    }

    #[test]
    fn test_prepare() {
        let cache = TxnStateCache::new();

        cache.register(100);
        cache.prepare(100, 150).unwrap();

        assert_eq!(
            cache.get(100),
            Some(TxnState::Prepared { prepared_ts: 150 })
        );
    }

    #[test]
    fn test_prepare_not_running_fails() {
        let cache = TxnStateCache::new();

        cache.register(100);
        cache.prepare(100, 150).unwrap();

        // Cannot prepare twice
        let result = cache.prepare(100, 160);
        assert!(result.is_err());
    }

    #[test]
    fn test_commit() {
        let cache = TxnStateCache::new();

        cache.register(100);
        cache.prepare(100, 150).unwrap();
        cache.commit(100, 150).unwrap();

        assert_eq!(cache.get(100), Some(TxnState::Committed { commit_ts: 150 }));
    }

    #[test]
    fn test_commit_not_prepared_fails() {
        let cache = TxnStateCache::new();

        cache.register(100);

        // Cannot commit without prepare
        let result = cache.commit(100, 150);
        assert!(result.is_err());
    }

    #[test]
    fn test_abort_from_running() {
        let cache = TxnStateCache::new();

        cache.register(100);
        cache.abort(100).unwrap();

        assert_eq!(cache.get(100), Some(TxnState::Aborted));
    }

    #[test]
    fn test_abort_from_prepared() {
        let cache = TxnStateCache::new();

        cache.register(100);
        cache.prepare(100, 150).unwrap();
        cache.abort(100).unwrap();

        assert_eq!(cache.get(100), Some(TxnState::Aborted));
    }

    #[test]
    fn test_abort_committed_fails() {
        let cache = TxnStateCache::new();

        cache.register(100);
        cache.prepare(100, 150).unwrap();
        cache.commit(100, 150).unwrap();

        let result = cache.abort(100);
        assert!(result.is_err());
    }

    #[test]
    fn test_abort_idempotent() {
        let cache = TxnStateCache::new();

        cache.register(100);
        cache.abort(100).unwrap();
        cache.abort(100).unwrap(); // Should not fail

        assert_eq!(cache.get(100), Some(TxnState::Aborted));
    }

    #[test]
    fn test_remove() {
        let cache = TxnStateCache::new();

        cache.register(100);
        assert_eq!(cache.len(), 1);

        cache.remove(100);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.get(100), None);
    }

    #[test]
    fn test_multiple_transactions() {
        let cache = TxnStateCache::new();

        cache.register(100);
        cache.register(200);
        cache.register(300);

        cache.prepare(100, 150).unwrap();
        cache.commit(100, 150).unwrap();
        cache.abort(200).unwrap();

        assert_eq!(cache.get(100), Some(TxnState::Committed { commit_ts: 150 }));
        assert_eq!(cache.get(200), Some(TxnState::Aborted));
        assert_eq!(cache.get(300), Some(TxnState::Running));
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let cache = Arc::new(TxnStateCache::new());
        let mut handles = vec![];

        // Multiple threads registering different transactions
        for i in 0..10 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    let ts = (i * 100 + j) as u64;
                    cache.register(ts);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(cache.len(), 1000);
    }
}
