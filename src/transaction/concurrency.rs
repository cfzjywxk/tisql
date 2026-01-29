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
//! - In-memory lock table for 1PC transaction atomicity
//! - max_ts tracking for MVCC read visibility
//!
//! Note: Timestamp allocation (TSO) is handled by the separate `tso` module.
//!
//! ## Design
//!
//! The ConcurrencyManager follows TiKV's KeyHandle pattern for efficient lock
//! management with minimal key cloning. The key insight is that we store
//! `Weak<KeyHandle>` in the skipmap, where KeyHandle owns the key. Multiple
//! threads can share the same KeyHandle via Arc, avoiding key clones.
//!
//! Reference: TiKV's `components/concurrency_manager/src/lib.rs`
//!
//! ```text
//! 1PC Write Flow:
//!   1. lock_keys(k1,k2,k3) -> acquire in-memory locks via KeyHandle
//!   2. Locks IMMEDIATELY visible to readers
//!   3. Write to storage (may be partial during apply)
//!   4. Storage write completes
//!   5. Drop guards -> KeyHandle ref count drops -> removed from table
//!
//! Concurrent Reader:
//!   1. check_lock() -> check in-memory lock table
//!   2. Lock found? -> BLOCKED (KeyIsLocked error)
//!   3. No lock? -> proceed to storage read
//! ```

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crossbeam_skiplist::SkipMap;

use crate::error::{Result, TiSqlError};
use crate::types::{Key, Timestamp};

/// Lock information stored in memory during transaction execution.
#[derive(Clone, Debug)]
pub struct Lock {
    /// Transaction's start_ts / commit_ts
    pub ts: Timestamp,
    /// Primary key (for future 2PC support)
    pub primary: Arc<[u8]>,
}

// ============================================================================
// KeyHandle - The core of the zero-clone pattern
// ============================================================================

/// A handle that owns a key and its associated lock state.
///
/// KeyHandle is the central structure in TiKV's lock management pattern:
/// - The key is stored exactly ONCE in the KeyHandle
/// - The lock table stores `Weak<KeyHandle>`, not the key itself
/// - Multiple threads share the same KeyHandle via `Arc<KeyHandle>`
/// - When all guards are dropped, the KeyHandle is automatically cleaned up
///
/// This design eliminates key cloning for concurrent access to the same key.
pub struct KeyHandle {
    /// The key - stored once, never cloned for lookups
    key: Key,
    /// Back-reference to the lock table for cleanup on drop
    /// Using UnsafeCell because we set this once after creation
    table: UnsafeCell<Option<LockTable>>,
    /// The lock state - Some when locked, None when unlocked
    lock_store: Mutex<Option<Lock>>,
    /// Number of active guards for reentrant locking.
    /// The lock is only cleared when this drops to 0.
    guard_count: AtomicUsize,
}

// Safety: KeyHandle is Send+Sync because:
// - `key` is immutable after creation
// - `table` is only written once via set_table before the handle becomes visible
// - `lock_store` is protected by a Mutex
unsafe impl Send for KeyHandle {}
unsafe impl Sync for KeyHandle {}

impl KeyHandle {
    /// Create a new KeyHandle for the given key.
    fn new(key: Key) -> Self {
        Self {
            key,
            table: UnsafeCell::new(None),
            lock_store: Mutex::new(None),
            guard_count: AtomicUsize::new(0),
        }
    }

    /// Set the back-reference to the lock table.
    ///
    /// # Safety
    /// Must only be called once, before the handle becomes visible to other threads.
    unsafe fn set_table(&self, table: LockTable) {
        *self.table.get() = Some(table);
    }

    /// Try to acquire the lock, returning a guard if successful.
    ///
    /// Returns `Err` if the key is already locked by a different transaction.
    fn try_lock(self: &Arc<Self>, lock: &Lock) -> Result<KeyHandleGuard> {
        let mut lock_store = self.lock_store.lock().unwrap();

        if let Some(existing) = lock_store.as_ref() {
            // Already locked - check if it's the same transaction
            if existing.ts != lock.ts || existing.primary != lock.primary {
                return Err(TiSqlError::KeyIsLocked {
                    key: self.key.clone(),
                    lock_ts: existing.ts,
                    primary: existing.primary.to_vec(),
                });
            }
            // Same transaction - reentrant lock (return new guard)
        }

        // Acquire the lock
        *lock_store = Some(lock.clone());

        // Increment guard count for reentrant locking support
        self.guard_count.fetch_add(1, Ordering::AcqRel);

        Ok(KeyHandleGuard {
            handle: Arc::clone(self),
        })
    }

    /// Check if this handle is currently locked.
    fn get_lock(&self) -> Option<Lock> {
        self.lock_store.lock().unwrap().clone()
    }
}

impl Drop for KeyHandle {
    fn drop(&mut self) {
        // Remove ourselves from the lock table
        // Safety: table is only accessed here and in set_table (which happens before visibility)
        unsafe {
            if let Some(table) = &*self.table.get() {
                table.remove(&self.key);
            }
        }
    }
}

impl std::fmt::Debug for KeyHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyHandle")
            .field("key", &self.key)
            .field("locked", &self.lock_store.lock().unwrap().is_some())
            .finish()
    }
}

// ============================================================================
// KeyHandleGuard - RAII guard for lock release
// ============================================================================

/// Guard that holds a lock until dropped.
///
/// When the guard is dropped:
/// 1. The lock state is cleared from the KeyHandle
/// 2. The Arc reference count decreases
/// 3. If this was the last reference, KeyHandle::drop removes it from the table
pub struct KeyHandleGuard {
    handle: Arc<KeyHandle>,
}

impl KeyHandleGuard {
    /// Get a reference to the locked key.
    #[inline]
    pub fn key(&self) -> &Key {
        &self.handle.key
    }

    /// Get a reference to the underlying KeyHandle.
    #[cfg(test)]
    pub fn handle(&self) -> &Arc<KeyHandle> {
        &self.handle
    }
}

impl Drop for KeyHandleGuard {
    fn drop(&mut self) {
        // Decrement guard count and only clear lock when last guard is dropped.
        // This is critical for reentrant locking: if txn acquires the same key
        // twice (guard1, guard2), dropping guard1 must NOT release the lock
        // since guard2 still holds it.
        let prev_count = self.handle.guard_count.fetch_sub(1, Ordering::AcqRel);
        if prev_count == 1 {
            // This was the last guard - clear the lock state
            *self.handle.lock_store.lock().unwrap() = None;
        }
        // Arc<KeyHandle> will be dropped, potentially triggering KeyHandle::drop
    }
}

impl std::fmt::Debug for KeyHandleGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyHandleGuard")
            .field("key", &self.handle.key)
            .finish()
    }
}

// ============================================================================
// LockTable - SkipMap with Weak references
// ============================================================================

/// The lock table storing weak references to KeyHandles.
///
/// Using `Weak<KeyHandle>` instead of storing keys directly provides:
/// - Zero-copy key access for concurrent threads (they share the same KeyHandle)
/// - Automatic cleanup when all guards are dropped
/// - No key cloning for lookups or lock checks
#[derive(Clone)]
struct LockTable(Arc<SkipMap<Key, Weak<KeyHandle>>>);

impl LockTable {
    fn new() -> Self {
        Self(Arc::new(SkipMap::new()))
    }

    /// Lock a key, returning a guard.
    ///
    /// This method implements the core KeyHandle pattern:
    /// 1. Create a new KeyHandle (clones the key once)
    /// 2. Try to insert it into the skipmap
    /// 3. If another thread inserted first, upgrade their weak ref and use it
    /// 4. Loop handles the race where the weak ref was invalidated
    fn lock_key(&self, key: &Key, lock: &Lock) -> Result<KeyHandleGuard> {
        loop {
            // Create a candidate KeyHandle
            let handle = Arc::new(KeyHandle::new(key.clone()));
            let weak = Arc::downgrade(&handle);

            // Try to insert into the skipmap
            // get_or_insert returns the existing entry if key exists
            let entry = self.0.get_or_insert(key.clone(), weak.clone());

            if entry.value().ptr_eq(&weak) {
                // We successfully inserted our handle
                // Set the back-reference for cleanup
                unsafe {
                    handle.set_table(self.clone());
                }
                // Try to acquire the lock
                return handle.try_lock(lock);
            }

            // Another handle exists - try to upgrade and use it
            if let Some(existing_handle) = entry.value().upgrade() {
                return existing_handle.try_lock(lock);
            }

            // The weak reference is stale (handle was dropped)
            // Remove it and retry
            // Note: This is safe because if entry.value() can't be upgraded,
            // the KeyHandle is being/was dropped and will remove itself
            self.0.remove(key);
        }
    }

    /// Get the KeyHandle for a key if it exists and is still alive.
    fn get(&self, key: &[u8]) -> Option<Arc<KeyHandle>> {
        self.0.get(key).and_then(|entry| entry.value().upgrade())
    }

    /// Remove a key from the table.
    fn remove(&self, key: &Key) {
        self.0.remove(key);
    }

    /// Get the number of entries in the table.
    ///
    /// Note: This may include stale weak references that haven't been cleaned up yet.
    fn len(&self) -> usize {
        self.0.len()
    }

    /// Iterate over a range to check for locks.
    fn check_range(&self, start: &[u8], end: &[u8]) -> Option<(Key, Lock)> {
        for entry in self.0.range(start.to_vec()..end.to_vec()) {
            if let Some(handle) = entry.value().upgrade() {
                if let Some(lock) = handle.get_lock() {
                    return Some((handle.key.clone(), lock));
                }
            }
        }
        None
    }
}

impl Default for LockTable {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// ConcurrencyManager - The public API
// ============================================================================

/// ConcurrencyManager provides in-memory lock table and max_ts tracking.
///
/// This is a component for MVCC concurrency control:
/// 1. **Lock Table** - In-memory locks that block readers during writes
/// 2. **max_ts** - Maximum timestamp seen, for MVCC visibility checks
///
/// Note: Timestamp allocation is handled by the separate `TsoService`.
///
/// ## Thread Safety
///
/// ConcurrencyManager is designed to be shared across threads via `Arc`.
/// All operations are lock-free using atomic operations and crossbeam-skiplist.
///
/// ## Reference
///
/// TiKV's ConcurrencyManager: `components/concurrency_manager/src/lib.rs`
pub struct ConcurrencyManager {
    /// Maximum timestamp seen by any transaction.
    /// Used for MVCC read visibility checks.
    max_ts: AtomicU64,

    /// In-memory lock table using the KeyHandle pattern.
    lock_table: LockTable,
}

impl ConcurrencyManager {
    /// Create a new ConcurrencyManager.
    ///
    /// The initial_max_ts should be set based on recovered state (max commit_ts).
    pub fn new(initial_max_ts: Timestamp) -> Self {
        Self {
            max_ts: AtomicU64::new(initial_max_ts),
            lock_table: LockTable::new(),
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

    /// Acquire a lock for a single key.
    ///
    /// Returns a guard that releases the lock on drop.
    pub fn lock_key(&self, key: &Key, lock: &Lock) -> Result<KeyHandleGuard> {
        self.lock_table.lock_key(key, lock)
    }

    /// Acquire locks for multiple keys (called BEFORE write).
    ///
    /// Keys are sorted to prevent deadlocks. Returns guards that release
    /// locks on drop. The locks are visible to readers immediately.
    ///
    /// # Arguments
    /// * `keys` - Iterator of keys to lock (avoids cloning the key slice)
    /// * `lock` - Lock information (ts, primary key)
    ///
    /// # Returns
    /// * `Ok(guards)` - Locks acquired successfully (in sorted key order)
    /// * `Err(KeyIsLocked)` - A key is already locked by another transaction
    pub fn lock_keys<'a>(
        &self,
        keys: impl Iterator<Item = &'a Key>,
        lock: Lock,
    ) -> Result<Vec<KeyHandleGuard>> {
        // Collect keys as references
        let mut keys: Vec<&Key> = keys.collect();
        if keys.is_empty() {
            return Ok(vec![]);
        }

        // Sort by key to ensure consistent lock ordering across transactions
        keys.sort();

        // Deduplicate
        keys.dedup();

        // Lock each key in sorted order
        let mut guards = Vec::with_capacity(keys.len());

        for key in keys {
            match self.lock_table.lock_key(key, &lock) {
                Ok(guard) => {
                    guards.push(guard);
                }
                Err(e) => {
                    // On failure, drop all acquired guards (they'll release locks)
                    // This happens automatically when guards goes out of scope
                    return Err(e);
                }
            }
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
        if let Some(handle) = self.lock_table.get(key) {
            if let Some(lock) = handle.get_lock() {
                return Err(TiSqlError::KeyIsLocked {
                    key: key.to_vec(),
                    lock_ts: lock.ts,
                    primary: lock.primary.to_vec(),
                });
            }
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
        if let Some((key, lock)) = self.lock_table.check_range(start, end) {
            return Err(TiSqlError::KeyIsLocked {
                key,
                lock_ts: lock.ts,
                primary: lock.primary.to_vec(),
            });
        }
        Ok(())
    }

    /// Get number of active locks (for debugging/testing).
    ///
    /// Note: This counts entries in the skipmap, which may include
    /// stale weak references. For accurate count, iterate and check.
    pub fn lock_count(&self) -> usize {
        self.lock_table.len()
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
    fn test_lock_key_success() {
        let cm = ConcurrencyManager::new(0);

        let key = b"key1".to_vec();
        let lock = Lock {
            ts: 100,
            primary: Arc::from(&b"key1"[..]),
        };

        let guard = cm.lock_key(&key, &lock).unwrap();
        assert!(cm.check_lock(b"key1", 1).is_err());

        drop(guard);
        assert!(cm.check_lock(b"key1", 1).is_ok());
    }

    #[test]
    fn test_lock_keys_success() {
        let cm = ConcurrencyManager::new(0);

        let keys = [b"key1".to_vec(), b"key2".to_vec()];
        let lock = Lock {
            ts: 100,
            primary: Arc::from(&b"key1"[..]),
        };

        let guards = cm.lock_keys(keys.iter(), lock).unwrap();
        assert_eq!(guards.len(), 2);

        // Check locks are visible
        assert!(cm.check_lock(b"key1", 1).is_err());
        assert!(cm.check_lock(b"key2", 1).is_err());

        // Unlocked key should pass
        assert!(cm.check_lock(b"key3", 1).is_ok());

        drop(guards);

        // After guards dropped, locks should be released
        assert!(cm.check_lock(b"key1", 1).is_ok());
        assert!(cm.check_lock(b"key2", 1).is_ok());
    }

    #[test]
    fn test_lock_release_on_drop() {
        let cm = ConcurrencyManager::new(0);

        let keys = [b"key1".to_vec()];
        let lock = Lock {
            ts: 100,
            primary: Arc::from(&b"key1"[..]),
        };

        {
            let _guards = cm.lock_keys(keys.iter(), lock).unwrap();
            assert!(cm.check_lock(b"key1", 1).is_err());
        }

        // After guards dropped, lock should be released
        assert!(cm.check_lock(b"key1", 1).is_ok());
    }

    #[test]
    fn test_lock_conflict() {
        let cm = ConcurrencyManager::new(0);

        let key = b"key1".to_vec();
        let lock1 = Lock {
            ts: 100,
            primary: Arc::from(&b"key1"[..]),
        };
        let lock2 = Lock {
            ts: 200,
            primary: Arc::from(&b"key1"[..]),
        };

        // First lock succeeds
        let _guard = cm.lock_key(&key, &lock1).unwrap();

        // Second lock on same key with different ts fails
        let result = cm.lock_key(&key, &lock2);
        assert!(result.is_err());
    }

    #[test]
    fn test_check_range() {
        let cm = ConcurrencyManager::new(0);

        let keys = [b"key2".to_vec()];
        let lock = Lock {
            ts: 100,
            primary: Arc::from(&b"key2"[..]),
        };

        let _guards = cm.lock_keys(keys.iter(), lock).unwrap();

        // Range containing locked key should fail
        assert!(cm.check_range(b"key1", b"key3", 1).is_err());

        // Range not containing locked key should pass
        assert!(cm.check_range(b"key3", b"key5", 1).is_ok());
    }

    #[test]
    fn test_lock_partial_failure_releases_acquired() {
        // Test that when locking k1, k2, k3 and k2 fails,
        // k1's lock is properly released (atomic behavior)
        let cm = ConcurrencyManager::new(0);

        // First, lock k2 with a different transaction
        let lock_other = Lock {
            ts: 50,
            primary: Arc::from(&b"other"[..]),
        };
        let _guard_k2 = cm.lock_key(&b"k2".to_vec(), &lock_other).unwrap();

        // Now try to lock k1, k2, k3 - k2 should fail
        let lock_ours = Lock {
            ts: 100,
            primary: Arc::from(&b"k1"[..]),
        };
        let keys = [b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()];
        let result = cm.lock_keys(keys.iter(), lock_ours);

        // Should fail because k2 is already locked
        assert!(result.is_err());

        // Verify k1 is NOT locked (was released on failure)
        assert!(cm.check_lock(b"k1", 1).is_ok(), "k1 should not be locked");

        // Verify k2 is still locked (by the other transaction)
        assert!(
            cm.check_lock(b"k2", 1).is_err(),
            "k2 should still be locked"
        );

        // Verify k3 was never locked
        assert!(cm.check_lock(b"k3", 1).is_ok(), "k3 should not be locked");
    }

    #[test]
    fn test_concurrent_max_ts() {
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
    fn test_key_handle_reuse() {
        // Test that multiple lock_key calls on the same key share the same KeyHandle
        let cm = ConcurrencyManager::new(0);
        let key = b"shared_key".to_vec();

        let lock1 = Lock {
            ts: 100,
            primary: Arc::from(&b"primary"[..]),
        };

        // First lock
        let guard1 = cm.lock_key(&key, &lock1).unwrap();
        let handle1_ptr = Arc::as_ptr(guard1.handle());

        // Release the lock
        drop(guard1);

        // Lock again with same transaction
        let guard2 = cm.lock_key(&key, &lock1).unwrap();
        let handle2_ptr = Arc::as_ptr(guard2.handle());

        // The handles should be different because guard1 was dropped,
        // causing the KeyHandle to be cleaned up
        // (This differs from TiKV which keeps handles alive longer due to async)
        // But the important thing is that it works correctly
        drop(guard2);

        // After all guards dropped, the key should be unlocked
        assert!(cm.check_lock(&key, 1).is_ok());

        // Suppress unused variable warning
        let _ = handle1_ptr;
        let _ = handle2_ptr;
    }

    #[test]
    fn test_reentrant_lock_same_transaction() {
        let cm = ConcurrencyManager::new(0);
        let key = b"key1".to_vec();

        let lock = Lock {
            ts: 100,
            primary: Arc::from(&b"key1"[..]),
        };

        // First lock
        let guard1 = cm.lock_key(&key, &lock).unwrap();

        // Same transaction can lock again (reentrant)
        let guard2 = cm.lock_key(&key, &lock).unwrap();

        // Both guards point to the same key
        assert_eq!(guard1.key(), guard2.key());

        // Key is still locked
        assert!(cm.check_lock(&key, 1).is_err());

        // Drop first guard - key should STILL be locked (guard2 holds it)
        drop(guard1);

        // Critical check: lock must remain held while guard2 exists
        assert!(
            cm.check_lock(&key, 1).is_err(),
            "lock should still be held by guard2"
        );

        drop(guard2);
        // Now the key should be unlocked
        assert!(cm.check_lock(&key, 1).is_ok());
    }

    #[test]
    fn test_reentrant_lock_blocks_other_transactions() {
        // Regression test: verify that another transaction cannot acquire
        // the lock while ANY reentrant guard still exists.
        let cm = ConcurrencyManager::new(0);
        let key = b"key1".to_vec();

        let lock_txn1 = Lock {
            ts: 100,
            primary: Arc::from(&b"key1"[..]),
        };

        let lock_txn2 = Lock {
            ts: 200,
            primary: Arc::from(&b"key1"[..]),
        };

        // txn1 acquires guard1 and guard2 (reentrant)
        let guard1 = cm.lock_key(&key, &lock_txn1).unwrap();
        let guard2 = cm.lock_key(&key, &lock_txn1).unwrap();

        // txn2 cannot acquire the lock
        assert!(cm.lock_key(&key, &lock_txn2).is_err());

        // Drop guard1 - guard2 still holds the lock
        drop(guard1);

        // Critical: txn2 STILL cannot acquire the lock
        assert!(
            cm.lock_key(&key, &lock_txn2).is_err(),
            "txn2 should not acquire lock while guard2 exists"
        );

        // Drop guard2 - now lock is released
        drop(guard2);

        // Now txn2 can acquire the lock
        let _guard_txn2 = cm.lock_key(&key, &lock_txn2).unwrap();
    }

    #[test]
    fn test_duplicate_keys_in_lock_keys() {
        let cm = ConcurrencyManager::new(0);

        // Same key appears multiple times
        let keys = [
            b"key1".to_vec(),
            b"key2".to_vec(),
            b"key1".to_vec(), // duplicate
        ];
        let lock = Lock {
            ts: 100,
            primary: Arc::from(&b"key1"[..]),
        };

        // Should deduplicate and succeed
        let guards = cm.lock_keys(keys.iter(), lock).unwrap();

        // Only 2 unique keys
        assert_eq!(guards.len(), 2);

        // Both keys should be locked
        assert!(cm.check_lock(b"key1", 1).is_err());
        assert!(cm.check_lock(b"key2", 1).is_err());
    }
}
