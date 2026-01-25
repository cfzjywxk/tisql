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
//! - TSO (Timestamp Oracle) for monotonic timestamp allocation
//! - In-memory lock table for 1PC transaction atomicity
//!
//! ## Design
//!
//! The ConcurrencyManager follows TiKV's pattern for 1PC (one-phase commit)
//! transaction atomicity. The key insight is that readers must be blocked
//! by in-memory locks BEFORE they even get a storage snapshot.
//!
//! ```text
//! 1PC Write Flow:
//!   1. lock_keys(k1,k2,k3) -> acquire in-memory locks
//!   2. Locks IMMEDIATELY visible to readers
//!   3. Write to storage (may be partial during apply)
//!   4. Storage write completes
//!   5. Drop guards -> remove locks from memory
//!
//! Concurrent Reader:
//!   1. check_lock() -> check in-memory lock table
//!   2. Lock found? -> BLOCKED (KeyIsLocked error)
//!   3. No lock? -> proceed to storage read
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crossbeam_skiplist::SkipMap;

use crate::error::{Result, TiSqlError};
use crate::types::{Key, Timestamp};

/// Lock information stored in memory during transaction execution.
#[derive(Clone, Debug)]
pub struct Lock {
    /// Transaction's start_ts / commit_ts
    pub ts: Timestamp,
    /// Primary key (for future 2PC support)
    pub primary: Key,
}

/// Guard that holds a lock until dropped.
///
/// When the guard is dropped, the lock is automatically removed from
/// the lock table, making the key available for other transactions.
pub struct KeyGuard {
    key: Key,
    lock_table: Arc<SkipMap<Key, Lock>>,
}

impl Drop for KeyGuard {
    fn drop(&mut self) {
        self.lock_table.remove(&self.key);
    }
}

impl std::fmt::Debug for KeyGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyGuard").field("key", &self.key).finish()
    }
}

/// ConcurrencyManager provides TSO and in-memory lock table.
///
/// This is the central component for MVCC concurrency control:
/// 1. **TSO** - Monotonic timestamp allocation for start_ts and commit_ts
/// 2. **Lock Table** - In-memory locks that block readers during writes
///
/// ## Thread Safety
///
/// ConcurrencyManager is designed to be shared across threads via `Arc`.
/// All operations are lock-free using atomic operations and crossbeam-skiplist.
pub struct ConcurrencyManager {
    /// Monotonic timestamp counter (serves as TSO)
    current_ts: AtomicU64,

    /// Maximum timestamp seen by any transaction.
    /// Used for safe timestamp allocation in distributed scenarios.
    max_ts: AtomicU64,

    /// In-memory lock table: key -> Lock
    /// Readers check this BEFORE reading from storage.
    lock_table: Arc<SkipMap<Key, Lock>>,
}

impl ConcurrencyManager {
    /// Create a new ConcurrencyManager with initial timestamp.
    ///
    /// The initial_ts should be set based on recovered state (max commit_ts + 1).
    pub fn new(initial_ts: Timestamp) -> Self {
        Self {
            current_ts: AtomicU64::new(initial_ts),
            max_ts: AtomicU64::new(initial_ts),
            lock_table: Arc::new(SkipMap::new()),
        }
    }

    /// Allocate next timestamp (TSO).
    ///
    /// Returns a monotonically increasing timestamp.
    /// This is used for both start_ts and commit_ts.
    pub fn get_ts(&self) -> Timestamp {
        let ts = self.current_ts.fetch_add(1, Ordering::SeqCst);
        // Also update max_ts
        self.update_max_ts(ts);
        ts
    }

    /// Get current timestamp without incrementing.
    pub fn current_ts(&self) -> Timestamp {
        self.current_ts.load(Ordering::SeqCst)
    }

    /// Update max_ts if the given ts is greater.
    ///
    /// This is called by readers to ensure timestamp monotonicity.
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

    /// Set timestamp counter (used during recovery).
    pub fn set_ts(&self, ts: Timestamp) {
        self.current_ts.store(ts, Ordering::SeqCst);
        self.update_max_ts(ts);
    }

    /// Acquire locks for keys (called BEFORE write).
    ///
    /// Returns guards that release locks on drop. The locks are visible
    /// to readers immediately after this call returns.
    ///
    /// # Arguments
    /// * `keys` - Keys to lock
    /// * `lock` - Lock information (ts, primary key)
    ///
    /// # Returns
    /// * `Ok(guards)` - Locks acquired successfully
    /// * `Err(KeyIsLocked)` - A key is already locked by another transaction
    pub fn lock_keys(&self, keys: &[Key], lock: Lock) -> Result<Vec<KeyGuard>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        // Sort and de-duplicate keys to avoid self-conflicts and to provide a stable
        // acquisition order.
        let mut keys = keys.to_vec();
        keys.sort();
        keys.dedup();

        let mut guards = Vec::with_capacity(keys.len());

        for key in &keys {
            // Use compare_insert to avoid a check-then-insert race. SkipMap::insert will
            // remove an existing entry, which can break mutual exclusion if two writers
            // race on the same key.
            let entry = self
                .lock_table
                .compare_insert(key.clone(), lock.clone(), |_| false);

            let existing_lock = entry.value();
            if existing_lock.ts != lock.ts || existing_lock.primary != lock.primary {
                return Err(TiSqlError::KeyIsLocked {
                    key: key.clone(),
                    lock_ts: existing_lock.ts,
                    primary: existing_lock.primary.clone(),
                });
            }

            guards.push(KeyGuard {
                key: key.clone(),
                lock_table: Arc::clone(&self.lock_table),
            });
        }

        Ok(guards)
    }

    /// Check if a key is locked (called BEFORE read).
    ///
    /// Returns error if the key is locked by an in-progress transaction.
    /// This must be called BEFORE taking a storage snapshot.
    ///
    /// # Arguments
    /// * `key` - Key to check
    /// * `start_ts` - Reader's start timestamp
    pub fn check_lock(&self, key: &[u8], _start_ts: Timestamp) -> Result<()> {
        if let Some(entry) = self.lock_table.get(key) {
            let lock = entry.value();
            return Err(TiSqlError::KeyIsLocked {
                key: key.to_vec(),
                lock_ts: lock.ts,
                primary: lock.primary.clone(),
            });
        }
        Ok(())
    }

    /// Check range for locks.
    ///
    /// Returns error if any key in the range is locked.
    ///
    /// # Arguments
    /// * `start` - Range start (inclusive)
    /// * `end` - Range end (exclusive)
    /// * `start_ts` - Reader's start timestamp
    pub fn check_range(&self, start: &[u8], end: &[u8], _start_ts: Timestamp) -> Result<()> {
        // Check if any key in the range is locked
        if let Some(entry) = self.lock_table.range(start.to_vec()..end.to_vec()).next() {
            let lock = entry.value();
            return Err(TiSqlError::KeyIsLocked {
                key: entry.key().clone(),
                lock_ts: lock.ts,
                primary: lock.primary.clone(),
            });
        }
        Ok(())
    }

    /// Get number of active locks (for debugging/testing).
    pub fn lock_count(&self) -> usize {
        self.lock_table.len()
    }
}

impl Default for ConcurrencyManager {
    fn default() -> Self {
        Self::new(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tso_monotonic() {
        let cm = ConcurrencyManager::new(1);

        let ts1 = cm.get_ts();
        let ts2 = cm.get_ts();
        let ts3 = cm.get_ts();

        assert_eq!(ts1, 1);
        assert_eq!(ts2, 2);
        assert_eq!(ts3, 3);
        assert!(ts1 < ts2);
        assert!(ts2 < ts3);
    }

    #[test]
    fn test_max_ts_update() {
        let cm = ConcurrencyManager::new(1);

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
    fn test_lock_keys_success() {
        let cm = ConcurrencyManager::new(1);

        let keys = vec![b"key1".to_vec(), b"key2".to_vec()];
        let lock = Lock {
            ts: 100,
            primary: b"key1".to_vec(),
        };

        let guards = cm.lock_keys(&keys, lock).unwrap();
        assert_eq!(guards.len(), 2);
        assert_eq!(cm.lock_count(), 2);

        // Check locks are visible
        assert!(cm.check_lock(b"key1", 1).is_err());
        assert!(cm.check_lock(b"key2", 1).is_err());

        // Unlocked key should pass
        assert!(cm.check_lock(b"key3", 1).is_ok());
    }

    #[test]
    fn test_lock_release_on_drop() {
        let cm = ConcurrencyManager::new(1);

        let keys = vec![b"key1".to_vec()];
        let lock = Lock {
            ts: 100,
            primary: b"key1".to_vec(),
        };

        {
            let _guards = cm.lock_keys(&keys, lock).unwrap();
            assert_eq!(cm.lock_count(), 1);
            assert!(cm.check_lock(b"key1", 1).is_err());
        }

        // After guards dropped, lock should be released
        assert_eq!(cm.lock_count(), 0);
        assert!(cm.check_lock(b"key1", 1).is_ok());
    }

    #[test]
    fn test_lock_conflict() {
        let cm = ConcurrencyManager::new(1);

        let keys = vec![b"key1".to_vec()];
        let lock1 = Lock {
            ts: 100,
            primary: b"key1".to_vec(),
        };
        let lock2 = Lock {
            ts: 200,
            primary: b"key1".to_vec(),
        };

        // First lock succeeds
        let _guards = cm.lock_keys(&keys, lock1).unwrap();

        // Second lock on same key fails
        let result = cm.lock_keys(&keys, lock2);
        assert!(result.is_err());
    }

    #[test]
    fn test_check_range() {
        let cm = ConcurrencyManager::new(1);

        let keys = vec![b"key2".to_vec()];
        let lock = Lock {
            ts: 100,
            primary: b"key2".to_vec(),
        };

        let _guards = cm.lock_keys(&keys, lock).unwrap();

        // Range containing locked key should fail
        assert!(cm.check_range(b"key1", b"key3", 1).is_err());

        // Range not containing locked key should pass
        assert!(cm.check_range(b"key3", b"key5", 1).is_ok());
    }

    #[test]
    fn test_concurrent_tso() {
        use std::thread;

        let cm = Arc::new(ConcurrencyManager::new(1));
        let mut handles = vec![];

        for _ in 0..10 {
            let cm = Arc::clone(&cm);
            handles.push(thread::spawn(move || {
                let mut timestamps = vec![];
                for _ in 0..100 {
                    timestamps.push(cm.get_ts());
                }
                timestamps
            }));
        }

        let mut all_timestamps = vec![];
        for handle in handles {
            all_timestamps.extend(handle.join().unwrap());
        }

        // All timestamps should be unique
        all_timestamps.sort();
        for i in 1..all_timestamps.len() {
            assert!(all_timestamps[i] > all_timestamps[i - 1]);
        }
    }
}
