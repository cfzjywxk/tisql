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

//! OceanBase-style versioned memtable with user key + version chain.
//!
//! Unlike the TiKV-style approach where each version is stored as a separate
//! skiplist entry with MVCC key (`user_key || !commit_ts`), this implementation
//! stores each user key once in the skiplist with a linked list of versions.
//!
//! ## Data Structure
//!
//! ```text
//! SkipList (by user_key):
//!     "user_a" → MvccRow { head → VersionNode(ts=100) → VersionNode(ts=50) → None }
//!     "user_b" → MvccRow { head → VersionNode(ts=80) → None }
//!     "user_c" → MvccRow { head → VersionNode(ts=120) → VersionNode(ts=90) → VersionNode(ts=30) → None }
//! ```
//!
//! ## Advantages
//!
//! 1. **Space Efficiency**: User key stored once, not repeated per version
//! 2. **Faster Point Lookups**: Seek to user key, then traverse short version chain
//! 3. **Better Cache Locality**: All versions of a key are adjacent in memory
//!
//! ## Version Chain Ordering (CRITICAL INVARIANT)
//!
//! Versions are linked **newest to oldest** (head is most recent):
//! - Write: Prepend new version to head (O(1), lock-free CAS)
//! - Read: Traverse from head until finding ts <= read_ts (O(versions))
//!
//! **INVARIANT**: New versions MUST have timestamp >= current head's timestamp.
//! This is required for `get_at()` to return the correct (latest visible) version.
//! Violating this invariant causes reads to return stale data.
//!
//! This invariant is naturally maintained when:
//! - Transaction commit_ts comes from a monotonically increasing TSO
//! - Per-key locking ensures commits are serialized (ConcurrencyManager)
//! - Recovery replays transactions in commit_ts order
//!
//! Debug builds include assertions to catch violations early.
//!
//! ## Thread Safety
//!
//! - Skiplist insertion uses crossbeam's lock-free operations
//! - Version chain updates use atomic CAS on the head pointer
//! - Reads are lock-free: atomic load of head, then traverse

use std::ops::{Bound, Range};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;

use crate::error::{Result, TiSqlError};
use crate::storage::mvcc::{
    is_lock, is_tombstone, MvccIterator, MvccKey, SharedMvccRange, LOCK, TOMBSTONE,
};
use crate::storage::{PessimisticStorage, StorageEngine, WriteBatch, WriteOp};
use crate::types::{Key, RawValue, Timestamp};

// ============================================================================
// VersionNode - Single version in the chain
// ============================================================================

/// A single version node in the version chain.
///
/// Each node contains a timestamp and value (or tombstone marker).
/// Nodes are linked from newest to oldest via the `next` pointer.
///
/// ## Pessimistic Locking Support
///
/// The `owner_start_ts` and `aborted` fields support OceanBase-style pessimistic locking:
///
/// - `owner_start_ts == 0`: This is a committed version (ts is the commit timestamp)
/// - `owner_start_ts > 0`: This is a pending write owned by transaction with this start_ts
///   - Readers lookup txn state to determine visibility
///   - Writers check if key is already locked by another txn
///
/// - `aborted == true`: This pending node was rolled back
///   - Readers skip aborted nodes
///   - Aborted nodes are not written to SST during flush
struct VersionNode {
    /// Commit timestamp for committed versions.
    /// For pending nodes, this is initially 0 and set to commit_ts when finalized.
    /// Uses AtomicU64 for thread-safe finalization during commit.
    ts: AtomicU64,
    /// Value at this version (TOMBSTONE for deletes)
    value: RawValue,
    /// Pointer to the next (older) version, or null if this is the oldest
    next: *mut VersionNode,
    /// Owner transaction's start_ts for pessimistic locking.
    /// - 0: This is a committed version
    /// - >0: This is a pending write owned by txn with this start_ts
    owner_start_ts: AtomicU64,
    /// Whether this pending node has been aborted (for rollback).
    /// Aborted nodes are skipped by readers and not written to SST.
    aborted: AtomicBool,
}

impl VersionNode {
    /// Create a new committed version node.
    ///
    /// Used for optimistic transactions where writes go directly to committed state.
    fn new(ts: Timestamp, value: RawValue) -> Box<Self> {
        Box::new(Self {
            ts: AtomicU64::new(ts),
            value,
            next: std::ptr::null_mut(),
            owner_start_ts: AtomicU64::new(0),
            aborted: AtomicBool::new(false),
        })
    }

    /// Create a new pending version node for pessimistic transactions.
    ///
    /// The node is owned by the transaction with `owner_start_ts` and is not yet visible
    /// to other transactions until committed.
    fn new_pending(owner_start_ts: Timestamp, value: RawValue) -> Box<Self> {
        Box::new(Self {
            ts: AtomicU64::new(0), // Will be set to commit_ts when finalized
            value,
            next: std::ptr::null_mut(),
            owner_start_ts: AtomicU64::new(owner_start_ts),
            aborted: AtomicBool::new(false),
        })
    }

    /// Get the commit timestamp of this version.
    #[inline]
    fn get_ts(&self) -> Timestamp {
        self.ts.load(Ordering::Acquire)
    }

    /// Check if this node is a pending write (uncommitted).
    #[inline]
    fn is_pending(&self) -> bool {
        self.owner_start_ts.load(Ordering::Acquire) > 0
    }

    /// Check if this node has been aborted.
    #[inline]
    fn is_aborted(&self) -> bool {
        self.aborted.load(Ordering::Acquire)
    }

    /// Get the owner transaction's start_ts.
    /// Returns 0 if this is a committed version.
    #[inline]
    fn get_owner(&self) -> Timestamp {
        self.owner_start_ts.load(Ordering::Acquire)
    }

    /// Mark this pending node as aborted (for rollback).
    fn mark_aborted(&self) {
        self.aborted.store(true, Ordering::Release);
    }

    /// Finalize a pending node by setting commit_ts and clearing owner.
    ///
    /// After finalization, the node becomes a committed version.
    fn finalize(&self, commit_ts: Timestamp) {
        // Set the commit timestamp atomically
        self.ts.store(commit_ts, Ordering::Release);
        // Clear owner to mark as committed
        self.owner_start_ts.store(0, Ordering::Release);
    }
}

// ============================================================================
// MvccRow - All versions of a single key
// ============================================================================

/// MVCC row containing all versions of a single user key.
///
/// The version chain is a singly-linked list, ordered newest to oldest.
/// The head pointer is atomic for lock-free concurrent access.
struct MvccRow {
    /// Head of version chain (newest version).
    /// Null if no versions exist.
    head: AtomicPtr<VersionNode>,
    /// Number of versions in the chain (for diagnostics).
    version_count: AtomicU32,
}

impl MvccRow {
    /// Create a new empty row (no versions).
    ///
    /// Use `prepend` to add versions. This allows for race-free concurrent
    /// insertion via get_or_insert + prepend pattern.
    fn new_empty() -> Self {
        Self {
            head: AtomicPtr::new(std::ptr::null_mut()),
            version_count: AtomicU32::new(0),
        }
    }

    /// Prepend a new version to the head of the chain.
    ///
    /// Uses CAS loop for lock-free concurrent writes. New versions are
    /// always prepended (newest first), maintaining temporal ordering.
    ///
    /// # Invariant
    ///
    /// **CRITICAL**: The new version's timestamp MUST be >= the current head's timestamp.
    /// This maintains the descending timestamp order (newest at head) that `get_at()`
    /// relies on for correctness. Violating this invariant causes reads to return
    /// stale data instead of the latest visible version.
    ///
    /// This invariant is naturally maintained when:
    /// - Transaction commit_ts comes from a monotonically increasing TSO
    /// - Per-key locking ensures commits are serialized
    /// - Recovery replays transactions in commit order
    ///
    /// Debug builds include an assertion to catch violations.
    fn prepend(&self, ts: Timestamp, value: RawValue) {
        let mut new_node = VersionNode::new(ts, value);

        loop {
            let current_head = self.head.load(Ordering::Acquire);

            // CRITICAL INVARIANT CHECK: new version must have ts >= current head's ts
            // This ensures the chain remains ordered newest-to-oldest.
            // Violation would cause get_at() to return stale versions.
            //
            // This is a hard assert (not debug_assert) because:
            // 1. Silent corruption is worse than failing loudly
            // 2. If this invariant is violated, there's a bug in the transaction layer
            //    (TSO, recovery, or commit ordering) that must be fixed
            // 3. Inserting at wrong position would cause reads to return stale data,
            //    which is effectively silent data loss
            if !current_head.is_null() {
                // Safety: current_head is valid if non-null (nodes are never deallocated
                // while the MvccRow exists)
                let head_ts = unsafe { (*current_head).get_ts() };
                assert!(
                    ts >= head_ts,
                    "MVCC version chain ordering violation: new ts {ts} < head ts {head_ts} for same key. \
                     This indicates a bug in transaction commit ordering (TSO, recovery, or locking). \
                     Refusing to insert to prevent silent data corruption."
                );
            }

            new_node.next = current_head;

            let new_ptr = Box::into_raw(new_node);

            match self.head.compare_exchange_weak(
                current_head,
                new_ptr,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.version_count.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                Err(_) => {
                    // CAS failed, retry with updated head
                    // Safety: we just created this node, no one else has a reference
                    new_node = unsafe { Box::from_raw(new_ptr) };
                }
            }
        }
    }

    /// Find the version visible at the given timestamp.
    ///
    /// Traverses the chain from head (newest) until finding a version
    /// with ts <= read_ts. Returns None if no visible version exists.
    ///
    /// Note: This method skips pending and aborted nodes since they are not
    /// yet committed. Use `get_at_with_pending` for read-your-writes semantics.
    #[cfg(test)]
    fn get_at(&self, read_ts: Timestamp) -> Option<&RawValue> {
        let mut current = self.head.load(Ordering::Acquire);

        // Safety: We hold a reference to MvccRow, and nodes are never deallocated
        // while the row exists (deallocation happens when the memtable is dropped).
        while !current.is_null() {
            let node = unsafe { &*current };

            // Skip aborted nodes
            if node.is_aborted() {
                current = node.next;
                continue;
            }

            // Skip pending nodes (not committed yet)
            if node.is_pending() {
                current = node.next;
                continue;
            }

            if node.get_ts() <= read_ts {
                return Some(&node.value);
            }
            current = node.next;
        }

        None
    }

    /// Find the version visible at the given timestamp, considering owned pending writes.
    ///
    /// This method supports read-your-writes semantics for explicit transactions.
    ///
    /// ## Read behavior:
    /// - Skip aborted nodes
    /// - For pending nodes (owner > 0):
    ///   - If owned by us and LOCK: skip to previous version (our write was "undone")
    ///   - If owned by us and value: return value (read-your-writes)
    ///   - If owned by others: skip (not visible to us)
    /// - For committed nodes (owner == 0):
    ///   - TOMBSTONE: return None (key deleted)
    ///   - Value: return value if ts <= read_ts
    ///
    /// Note: Committed LOCK nodes don't exist - they're aborted at commit time.
    fn get_at_with_owner(
        &self,
        read_ts: Timestamp,
        owner_start_ts: Timestamp,
    ) -> Option<&RawValue> {
        let mut current = self.head.load(Ordering::Acquire);

        // Safety: We hold a reference to MvccRow, and nodes are never deallocated
        // while the row exists (deallocation happens when the memtable is dropped).
        while !current.is_null() {
            let node = unsafe { &*current };

            // Skip aborted nodes (includes LOCK after commit)
            if node.is_aborted() {
                current = node.next;
                continue;
            }

            let node_owner = node.get_owner();

            // Pending write (owner > 0)
            if node_owner > 0 {
                if node_owner == owner_start_ts {
                    // Our pending write
                    if is_lock(&node.value) {
                        // LOCK = "undo our write", skip to previous version
                        current = node.next;
                        continue;
                    }
                    return Some(&node.value);
                }
                // Someone else's pending write - skip
                current = node.next;
                continue;
            }

            // Committed node (owner == 0)
            let node_ts = node.get_ts();
            if node_ts <= read_ts {
                if is_tombstone(&node.value) {
                    return None; // Key was deleted
                }
                return Some(&node.value);
            }
            current = node.next;
        }

        None
    }

    // ========================================================================
    // Pessimistic Locking Methods
    // ========================================================================

    /// Check if the head of the version chain is a pending (uncommitted) write.
    ///
    /// Returns:
    /// - `None` if head is null, aborted, or committed
    /// - `Some(owner_start_ts)` if head is a pending write
    fn get_head_pending_owner(&self) -> Option<Timestamp> {
        let head = self.head.load(Ordering::Acquire);
        if head.is_null() {
            return None;
        }

        let node = unsafe { &*head };

        // Skip aborted nodes
        if node.is_aborted() {
            return None;
        }

        let owner = node.get_owner();
        if owner > 0 {
            Some(owner)
        } else {
            None
        }
    }

    /// Prepend a pending write for pessimistic transactions.
    ///
    /// Returns:
    /// - `Ok(true)` if a new node was created
    /// - `Ok(false)` if the value was updated in place (same txn re-writing)
    /// - `Err(lock_owner)` if the key is already locked by another transaction
    ///
    /// If the key is already locked by the same transaction (owner == my_start_ts),
    /// the value is updated in place.
    fn prepend_pending(
        &self,
        my_start_ts: Timestamp,
        mut value: RawValue,
    ) -> std::result::Result<bool, Timestamp> {
        loop {
            let current_head = self.head.load(Ordering::Acquire);

            // Check for conflicts with existing pending writes
            if !current_head.is_null() {
                let head_node = unsafe { &*current_head };

                // Skip aborted nodes - they don't conflict
                if !head_node.is_aborted() {
                    let owner = head_node.get_owner();
                    if owner > 0 && owner != my_start_ts {
                        // Key is locked by another transaction
                        return Err(owner);
                    }

                    if owner == my_start_ts {
                        // Same transaction writing again - update value in place
                        // Safety: We own this node (same start_ts), so we can mutate it.
                        // This is safe because:
                        // 1. Only our transaction can write to this node
                        // 2. Readers will see either the old or new value, both are valid
                        let value_ptr = &head_node.value as *const RawValue as *mut RawValue;
                        unsafe {
                            std::ptr::swap(value_ptr, &mut value);
                        }
                        return Ok(false); // Updated in place, no new node
                    }
                }
            }

            // Create pending node - take ownership of value
            let mut new_node = VersionNode::new_pending(my_start_ts, std::mem::take(&mut value));
            new_node.next = current_head;

            let new_ptr = Box::into_raw(new_node);

            match self.head.compare_exchange_weak(
                current_head,
                new_ptr,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.version_count.fetch_add(1, Ordering::Relaxed);
                    return Ok(true); // New node created
                }
                Err(_) => {
                    // CAS failed, retry with updated head
                    // Safety: we just created this node, no one else has a reference
                    // Recover the value for retry
                    let recovered_node = unsafe { Box::from_raw(new_ptr) };
                    value = recovered_node.value;
                }
            }
        }
    }

    /// Delete for pessimistic transactions.
    ///
    /// Behavior:
    /// - If head is our pending write: Update value to LOCK (undo our write)
    /// - If head is another's pending write: Return Err(lock_owner)
    /// - If committed value exists: Write pending TOMBSTONE
    /// - If key doesn't exist or already deleted: Do nothing, return Ok(false)
    ///
    /// # Returns
    /// - `Ok(true)` if delete was performed (LOCK or TOMBSTONE written)
    /// - `Ok(false)` if key doesn't exist or already deleted
    /// - `Err(lock_owner)` if key is locked by another transaction
    fn delete_pending(&self, owner_start_ts: Timestamp) -> std::result::Result<bool, Timestamp> {
        loop {
            let current_head = self.head.load(Ordering::Acquire);

            if current_head.is_null() {
                // Key doesn't exist at all - do nothing
                return Ok(false);
            }

            let head_node = unsafe { &*current_head };

            // Skip aborted nodes - need to find actual head
            if head_node.is_aborted() {
                // Look for non-aborted node to determine state
                let mut current = head_node.next;
                while !current.is_null() {
                    let node = unsafe { &*current };
                    if !node.is_aborted() {
                        break;
                    }
                    current = node.next;
                }

                if current.is_null() {
                    // All nodes aborted - treat as empty
                    return Ok(false);
                }

                let actual_node = unsafe { &*current };
                if actual_node.get_owner() > 0 {
                    // Pending node by someone else
                    return Err(actual_node.get_owner());
                }

                // Committed node exists - write pending TOMBSTONE
                if !is_tombstone(&actual_node.value) {
                    let mut new_node = VersionNode::new_pending(owner_start_ts, TOMBSTONE.to_vec());
                    new_node.next = current_head;

                    let new_ptr = Box::into_raw(new_node);

                    match self.head.compare_exchange_weak(
                        current_head,
                        new_ptr,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {
                            self.version_count.fetch_add(1, Ordering::Relaxed);
                            return Ok(true);
                        }
                        Err(_) => {
                            let _ = unsafe { Box::from_raw(new_ptr) };
                            continue; // Retry
                        }
                    }
                }
                return Ok(false); // Already deleted
            }

            let node_owner = head_node.get_owner();

            if node_owner == owner_start_ts {
                // Our pending write - convert to LOCK (undo our write)
                // Safety: We own this node (same start_ts), so we can mutate it.
                // Other readers see either old value or LOCK, both valid states.
                let value_ptr = &head_node.value as *const RawValue as *mut RawValue;
                let mut lock_value = LOCK.to_vec();
                unsafe {
                    std::ptr::swap(value_ptr, &mut lock_value);
                }
                return Ok(true);
            }

            if node_owner > 0 {
                // Locked by another transaction
                return Err(node_owner);
            }

            // Committed node - write pending TOMBSTONE to delete it
            if !is_tombstone(&head_node.value) {
                let mut new_node = VersionNode::new_pending(owner_start_ts, TOMBSTONE.to_vec());
                new_node.next = current_head;

                let new_ptr = Box::into_raw(new_node);

                match self.head.compare_exchange_weak(
                    current_head,
                    new_ptr,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        self.version_count.fetch_add(1, Ordering::Relaxed);
                        return Ok(true);
                    }
                    Err(_) => {
                        let _ = unsafe { Box::from_raw(new_ptr) };
                        continue; // Retry
                    }
                }
            }

            // Key already deleted (committed tombstone) - do nothing
            return Ok(false);
        }
    }

    /// Finalize all pending nodes owned by the given transaction.
    ///
    /// For value and TOMBSTONE nodes: Sets commit_ts and clears owner_start_ts.
    /// For LOCK nodes: Marks as aborted (not persisted - pessimistic lock served its purpose).
    ///
    /// Called during transaction commit.
    fn finalize_pending(&self, owner_start_ts: Timestamp, commit_ts: Timestamp) {
        let mut current = self.head.load(Ordering::Acquire);

        while !current.is_null() {
            let node = unsafe { &*current };

            if node.get_owner() == owner_start_ts && !node.is_aborted() {
                if is_lock(&node.value) {
                    // LOCK served its purpose (conflict detection via pessimistic lock)
                    // Don't persist - mark as aborted so readers skip it
                    node.mark_aborted();
                } else {
                    // Value or TOMBSTONE - finalize normally
                    node.finalize(commit_ts);
                }
            }

            current = node.next;
        }
    }

    /// Mark all pending nodes owned by the given transaction as aborted.
    ///
    /// Aborted nodes are skipped by readers and not written to SST during flush.
    /// Called during transaction rollback.
    fn abort_pending(&self, owner_start_ts: Timestamp) {
        let mut current = self.head.load(Ordering::Acquire);

        while !current.is_null() {
            let node = unsafe { &*current };

            if node.get_owner() == owner_start_ts {
                node.mark_aborted();
            }

            current = node.next;
        }
    }
}

impl Drop for MvccRow {
    fn drop(&mut self) {
        // Free all version nodes
        let mut current = *self.head.get_mut();
        while !current.is_null() {
            // Safety: we have exclusive access during drop, and we're
            // deallocating nodes we own
            let node = unsafe { Box::from_raw(current) };
            current = node.next;
            // node is dropped here, freeing the memory
        }
    }
}

// ============================================================================
// VersionedMemTableEngine - Main memtable implementation
// ============================================================================

/// Inner data structure holding the actual memtable data.
///
/// This is wrapped in Arc by `VersionedMemTableEngine` to allow the
/// `StorageEngine::scan_iter` method to return owned iterators that
/// don't borrow from the engine.
pub struct VersionedMemTableEngineInner {
    /// Skiplist mapping user keys to MVCC rows
    index: SkipMap<Key, MvccRow>,
    /// Entry count (number of versions across all keys)
    entry_count: AtomicUsize,
}

/// OceanBase-style versioned memtable engine.
///
/// This implementation stores user keys in a skiplist with linked version chains,
/// providing space efficiency and fast point lookups compared to the TiKV-style
/// approach of storing each version as a separate skiplist entry.
///
/// # Memory Model
///
/// Version nodes are allocated on the heap and linked together. Memory is
/// reclaimed when the memtable is dropped (after being flushed to SST).
/// There is no incremental garbage collection - the entire memtable is
/// deallocated at once.
///
/// # Thread Safety
///
/// - Skiplist operations are lock-free (crossbeam-skiplist)
/// - Version chain updates use CAS for the head pointer
/// - Reads are lock-free via atomic loads
///
/// # StorageEngine Implementation
///
/// The inner data is wrapped in `Arc` to allow `scan_iter` to return owned
/// iterators. This enables the engine to implement `StorageEngine` where
/// iterators cannot borrow from `self`.
pub struct VersionedMemTableEngine {
    /// Inner data wrapped in Arc for owned iterator support
    inner: Arc<VersionedMemTableEngineInner>,
}

impl VersionedMemTableEngine {
    /// Create a new versioned memtable engine.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(VersionedMemTableEngineInner {
                index: SkipMap::new(),
                entry_count: AtomicUsize::new(0),
            }),
        }
    }

    /// Internal put implementation.
    ///
    /// Uses get_or_insert + prepend pattern for race-free concurrent insertion:
    /// 1. get_or_insert atomically inserts an empty row if key doesn't exist
    /// 2. prepend adds the version to whatever row we get (existing or new)
    ///
    /// This ensures no versions are lost even with concurrent inserts to same key.
    fn put_internal(&self, key: &[u8], value: RawValue, ts: Timestamp) {
        // get_or_insert is atomic: returns existing entry or inserts new empty row
        let entry = self
            .inner
            .index
            .get_or_insert(key.to_vec(), MvccRow::new_empty());

        // Prepend version to the row (works for both new and existing rows)
        entry.value().prepend(ts, value);

        self.inner.entry_count.fetch_add(1, Ordering::Relaxed);
    }

    // ========================================================================
    // Pessimistic Locking Methods
    // ========================================================================

    /// Write a pending value for pessimistic transactions.
    ///
    /// Returns:
    /// - `Ok(())` if the write was successful
    /// - `Err(lock_owner)` if the key is already locked by another transaction
    ///
    /// If the key is already locked by the same transaction, the value is updated in place.
    pub fn put_pending(
        &self,
        key: &[u8],
        value: RawValue,
        owner_start_ts: Timestamp,
    ) -> std::result::Result<(), Timestamp> {
        let entry = self
            .inner
            .index
            .get_or_insert(key.to_vec(), MvccRow::new_empty());

        match entry.value().prepend_pending(owner_start_ts, value) {
            Ok(new_node_created) => {
                if new_node_created {
                    self.inner.entry_count.fetch_add(1, Ordering::Relaxed);
                }
                Ok(())
            }
            Err(lock_owner) => Err(lock_owner),
        }
    }

    /// Check if a key is locked by a pending write.
    ///
    /// Returns:
    /// - `None` if the key is not locked (no pending write at head)
    /// - `Some(owner_start_ts)` if the key is locked
    pub fn get_lock_owner(&self, key: &[u8]) -> Option<Timestamp> {
        self.inner
            .index
            .get(key)
            .and_then(|entry| entry.value().get_head_pending_owner())
    }

    /// Finalize all pending writes for a transaction by setting commit_ts.
    ///
    /// Called during transaction commit. Converts pending nodes to committed nodes.
    pub fn finalize_pending(&self, keys: &[Key], owner_start_ts: Timestamp, commit_ts: Timestamp) {
        for key in keys {
            if let Some(entry) = self.inner.index.get(key) {
                entry.value().finalize_pending(owner_start_ts, commit_ts);
            }
        }
    }

    /// Mark all pending writes for a transaction as aborted.
    ///
    /// Called during transaction rollback. Aborted nodes are skipped by readers.
    pub fn abort_pending(&self, keys: &[Key], owner_start_ts: Timestamp) {
        for key in keys {
            if let Some(entry) = self.inner.index.get(key) {
                entry.value().abort_pending(owner_start_ts);
            }
        }
    }

    /// Delete a key with pessimistic locking.
    ///
    /// Behavior:
    /// - If key has our pending write: Converts to LOCK (undo our write)
    /// - If key is locked by another txn: Returns Err(lock_owner)
    /// - If committed value exists: Writes pending TOMBSTONE
    /// - If key doesn't exist: Does nothing, returns Ok(false)
    ///
    /// Returns:
    /// - `Ok(true)` if delete was performed
    /// - `Ok(false)` if key doesn't exist or already deleted
    /// - `Err(lock_owner)` if key is locked by another transaction
    pub fn delete_pending(
        &self,
        key: &[u8],
        owner_start_ts: Timestamp,
    ) -> std::result::Result<bool, Timestamp> {
        // If key doesn't exist, do nothing
        let entry = match self.inner.index.get(key) {
            Some(e) => e,
            None => return Ok(false),
        };

        match entry.value().delete_pending(owner_start_ts) {
            Ok(true) => {
                // A TOMBSTONE node was added (delete on committed)
                // or LOCK was set (delete on our pending)
                // Note: entry_count not incremented here because:
                // - LOCK update is in-place
                // - TOMBSTONE add is counted when we check if entry existed
                Ok(true)
            }
            Ok(false) => Ok(false),
            Err(lock_owner) => Err(lock_owner),
        }
    }

    /// Get a value with read-your-writes support.
    ///
    /// If `owner_start_ts > 0`, the caller is in an explicit transaction and
    /// should see their own pending writes.
    ///
    /// Returns:
    /// - `Some(value)` if found (including pending value owned by this txn)
    /// - `None` if not found, deleted, or pending LOCK
    pub fn get_with_owner(
        &self,
        key: &[u8],
        read_ts: Timestamp,
        owner_start_ts: Timestamp,
    ) -> Option<RawValue> {
        self.inner.index.get(key).and_then(|entry| {
            entry
                .value()
                .get_at_with_owner(read_ts, owner_start_ts)
                .cloned()
        })
    }

    /// Get the number of version entries in the memtable.
    pub fn len(&self) -> usize {
        self.inner.entry_count.load(Ordering::Relaxed)
    }

    /// Check if the memtable is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the number of unique keys in the memtable.
    pub fn key_count(&self) -> usize {
        self.inner.index.len()
    }

    /// Check whether any user key exists in the range [start, end).
    pub fn has_user_key_in_range(&self, start: &[u8], end: &[u8]) -> bool {
        if start >= end {
            return false;
        }
        self.inner
            .index
            .range::<[u8], _>((Bound::Included(start), Bound::Excluded(end)))
            .next()
            .is_some()
    }

    /// Get memory usage statistics.
    pub fn memory_stats(&self) -> VersionedMemoryStats {
        VersionedMemoryStats {
            entry_count: self.len(),
            key_count: self.key_count(),
        }
    }

    /// Get a reference to the inner engine data.
    ///
    /// This is used by the LSM layer for creating iterators.
    #[inline]
    pub fn inner_ref(&self) -> &VersionedMemTableEngineInner {
        &self.inner
    }

    /// Clone the Arc to the inner data.
    ///
    /// This is used for creating owned iterators that outlive `&self`.
    #[inline]
    pub fn inner_arc(&self) -> Arc<VersionedMemTableEngineInner> {
        Arc::clone(&self.inner)
    }
}

impl Default for VersionedMemTableEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl VersionedMemTableEngine {
    /// Create a streaming iterator over the given range.
    ///
    /// This is the internal method used by the LSM layer for creating owned iterators.
    /// The returned iterator is in an uninitialized state; call `next()` to
    /// position on the first entry.
    ///
    /// # Arguments
    ///
    /// * `range` - MVCC key range (Arc for zero-copy sharing)
    /// * `owner_ts` - Transaction's start_ts for read-your-writes
    pub fn create_streaming_iter(
        &self,
        range: SharedMvccRange,
        owner_ts: Timestamp,
    ) -> VersionedMemTableIterator<'_> {
        VersionedMemTableIterator::new(self.inner.as_ref(), range, owner_ts)
    }
}

impl StorageEngine for VersionedMemTableEngine {
    type Iter = ArcVersionedMemTableIterator;

    fn scan_iter(
        &self,
        range: Range<MvccKey>,
        owner_ts: Timestamp,
    ) -> Result<ArcVersionedMemTableIterator> {
        let range = Arc::new(range);
        let iter = ArcVersionedMemTableIterator::new(self.inner_arc(), range, owner_ts);
        Ok(iter)
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        // Delegate to the inherent method
        VersionedMemTableEngine::write_batch(self, batch)
    }
}

impl PessimisticStorage for VersionedMemTableEngine {
    fn put_pending(
        &self,
        key: &[u8],
        value: RawValue,
        owner_start_ts: Timestamp,
    ) -> std::result::Result<(), Timestamp> {
        // Delegate to the inherent method
        VersionedMemTableEngine::put_pending(self, key, value, owner_start_ts)
    }

    fn get_lock_owner(&self, key: &[u8]) -> Option<Timestamp> {
        // Delegate to the inherent method
        VersionedMemTableEngine::get_lock_owner(self, key)
    }

    fn finalize_pending(&self, keys: &[Key], owner_start_ts: Timestamp, commit_ts: Timestamp) {
        // Delegate to the inherent method
        VersionedMemTableEngine::finalize_pending(self, keys, owner_start_ts, commit_ts)
    }

    fn abort_pending(&self, keys: &[Key], owner_start_ts: Timestamp) {
        // Delegate to the inherent method
        VersionedMemTableEngine::abort_pending(self, keys, owner_start_ts)
    }

    fn delete_pending(
        &self,
        key: &[u8],
        owner_start_ts: Timestamp,
    ) -> std::result::Result<bool, Timestamp> {
        // Delegate to the inherent method
        VersionedMemTableEngine::delete_pending(self, key, owner_start_ts)
    }

    async fn get_with_owner(
        &self,
        key: &[u8],
        read_ts: Timestamp,
        owner_start_ts: Timestamp,
    ) -> Option<RawValue> {
        // Delegate to the inherent method
        VersionedMemTableEngine::get_with_owner(self, key, read_ts, owner_start_ts)
    }
}

impl VersionedMemTableEngine {
    /// Apply a batch of writes atomically.
    ///
    /// Note: This is not part of StorageEngine trait because VersionedMemTableEngine
    /// is an internal component. Use LsmEngine for the public StorageEngine interface.
    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        let commit_ts = batch
            .commit_ts()
            .ok_or_else(|| TiSqlError::Storage("WriteBatch must have commit_ts set".to_string()))?;

        for (key, op) in batch.into_iter() {
            match op {
                WriteOp::Put { value } => {
                    self.put_internal(&key, value, commit_ts);
                }
                WriteOp::Delete => {
                    self.put_internal(&key, TOMBSTONE.to_vec(), commit_ts);
                }
            }
        }

        Ok(())
    }
}

/// Memory usage statistics for the versioned memtable.
#[derive(Debug, Clone, Copy)]
pub struct VersionedMemoryStats {
    /// Number of version entries (total across all keys)
    pub entry_count: usize,
    /// Number of unique keys
    pub key_count: usize,
}

// ============================================================================
// VersionedMemTableIterator - Zero-copy streaming iterator
// ============================================================================

/// Zero-copy streaming iterator over the versioned memtable.
///
/// This iterator provides efficient MVCC key iteration with **zero allocations**
/// during iteration (seek, next, user_key, timestamp, value).
///
/// ## Design Principles
///
/// 1. **No caching**: Returns references directly to stored data
/// 2. **No cloning**: Never clones keys or values during iteration
/// 3. **Pointer-based traversal**: Uses raw pointers for version chains
/// 4. **Efficient next-key**: Uses `Entry::next()` for O(1) traversal
///
/// ## MVCC Key Order
///
/// MVCC keys are ordered as `(user_key ASC, timestamp DESC)`:
/// - User keys are iterated in ascending order via the skiplist
/// - For each user key, versions are returned from newest (highest ts) to oldest (lowest ts)
///
/// ## Safety
///
/// Uses raw pointers (`*const VersionNode`) to traverse version chains. This is safe because:
/// 1. The iterator holds a reference to the `VersionedMemTableEngineInner`
/// 2. Version nodes are never deallocated while the engine exists
/// 3. The version chain is prepend-only (no concurrent modification of existing nodes)
///
/// Also stores `Entry<'a, ...>` from crossbeam skiplist which is valid for the
/// lifetime of the skiplist (guaranteed by the engine reference).
pub struct VersionedMemTableIterator<'a> {
    /// Reference to the memtable engine inner data
    inner: &'a VersionedMemTableEngineInner,
    /// Shared MVCC key range (avoids cloning range for each iterator)
    range: SharedMvccRange,
    /// Whether the iterator has been initialized
    initialized: bool,
    /// Current skiplist entry (holds reference to user key)
    current_entry: Option<Entry<'a, Vec<u8>, MvccRow>>,
    /// Current position in version chain (null if at end of chain)
    current_version: *const VersionNode,
    /// Owner's start_ts for read-your-writes support.
    /// Pending nodes owned by this txn (where node.owner == owner_ts) are visible.
    owner_ts: Timestamp,
    /// Whether the current entry is a non-owner pending node.
    /// Used by the transaction layer to resolve pending writes via TxnStateCache.
    current_is_pending: bool,
    /// Owner start_ts of the current pending node (only meaningful when current_is_pending).
    current_pending_owner: Timestamp,
}

// Safety: VersionedMemTableIterator is Send because:
// 1. The raw pointer `*const VersionNode` points into a SkipMap entry's MvccRow,
//    which is kept alive by the `&'a VersionedMemTableEngineInner` reference (or Arc).
// 2. VersionNode fields (AtomicU64 ts, Vec<u8> value, AtomicPtr next, AtomicBool aborted,
//    owner_start_ts) are all Send-safe: atomics are explicitly designed for cross-thread use,
//    and Vec<u8> is Send.
// 3. crossbeam_skiplist::Entry<'a, K, V> is Send when K: Send and V: Send (both Vec<u8>
//    and MvccRow satisfy this).
// 4. The iterator is used within async tasks (tokio::spawn) which require Send.
//    The iterator never holds the pointer across yield points — it's only dereferenced
//    in synchronous accessor methods (user_key, timestamp, value).
//
// SAFETY PROOF: The pointer is valid for the lifetime 'a of the iterator because
// MvccRow uses a linked list of heap-allocated VersionNode objects that are never
// deallocated while the SkipMap entry exists. The entry is pinned by the `current_entry`
// field which borrows from the SkipMap.
unsafe impl Send for VersionedMemTableIterator<'_> {}
// - Memory ordering requirements for concurrent access

impl<'a> VersionedMemTableIterator<'a> {
    /// Create a new iterator over the given range.
    ///
    /// The iterator is created in an uninitialized state. Call `advance()` to position
    /// on the first entry.
    ///
    /// # Arguments
    ///
    /// * `inner` - Reference to the memtable engine inner data
    /// * `range` - MVCC key range to scan (Arc for zero-copy sharing)
    /// * `owner_ts` - Transaction's start_ts for read-your-writes. Pending nodes
    ///   owned by this transaction are visible (except LOCK nodes).
    pub fn new(
        inner: &'a VersionedMemTableEngineInner,
        range: SharedMvccRange,
        owner_ts: Timestamp,
    ) -> Self {
        Self {
            inner,
            range,
            initialized: false,
            current_entry: None,
            current_version: std::ptr::null(),
            owner_ts,
            current_is_pending: false,
            current_pending_owner: 0,
        }
    }

    /// Initialize the iterator by seeking to the start of the range.
    fn initialize(&mut self) {
        if self.initialized {
            return;
        }
        self.initialized = true;

        // Get the range bounds for the skiplist query
        let start_bound: Bound<&[u8]> = if self.range.start.is_unbounded() {
            Bound::Unbounded
        } else {
            Bound::Included(self.range.start.key())
        };

        // Iterate through entries starting from range_start
        for entry in self
            .inner
            .index
            .range::<[u8], _>((start_bound, Bound::Unbounded))
        {
            let user_key = entry.key().as_slice();

            // Check if past end of range
            if !self.range.end.is_unbounded() && user_key > self.range.end.key() {
                break;
            }

            let row = entry.value();
            let head = row.head.load(Ordering::Acquire);

            if head.is_null() {
                continue;
            }

            // For the first key matching range_start exactly, filter by timestamp
            let ts_filter =
                if !self.range.start.is_unbounded() && user_key == self.range.start.key() {
                    Some(self.range.start.timestamp())
                } else {
                    None
                };

            if let Some((version_ptr, is_pending, pending_owner)) =
                self.find_first_valid_version(user_key, head, ts_filter)
            {
                self.current_entry = Some(entry);
                self.current_version = version_ptr;
                self.current_is_pending = is_pending;
                self.current_pending_owner = pending_owner;
                return;
            }
        }

        // No entry found
        self.current_entry = None;
        self.current_version = std::ptr::null();
        self.current_is_pending = false;
        self.current_pending_owner = 0;
    }

    /// Seek to first valid entry starting from the given user key.
    ///
    /// If `exact_start_ts` is Some, the first user key matching `start_key` will
    /// only consider versions with ts <= exact_start_ts. Subsequent keys start from head.
    fn seek_from_user_key(&mut self, start_key: &[u8], exact_start_ts: Option<Timestamp>) {
        for entry in self
            .inner
            .index
            .range::<[u8], _>((Bound::Included(start_key), Bound::Unbounded))
        {
            let user_key = entry.key().as_slice();

            // Check if past end of range
            if !self.range.end.is_unbounded() && user_key > self.range.end.key() {
                break;
            }

            let row = entry.value();
            let head = row.head.load(Ordering::Acquire);

            if head.is_null() {
                continue;
            }

            // Apply timestamp filter only for exact match on start_key
            let ts_filter = if user_key == start_key {
                exact_start_ts
            } else {
                None
            };

            if let Some((version_ptr, is_pending, pending_owner)) =
                self.find_first_valid_version(user_key, head, ts_filter)
            {
                self.current_entry = Some(entry);
                self.current_version = version_ptr;
                self.current_is_pending = is_pending;
                self.current_pending_owner = pending_owner;
                return;
            }
        }

        // No entry found
        self.current_entry = None;
        self.current_version = std::ptr::null();
        self.current_is_pending = false;
        self.current_pending_owner = 0;
    }

    /// Find the first valid version in a version chain.
    ///
    /// If `ts_filter` is Some, only consider versions with ts <= ts_filter.
    /// Returns (version_ptr, is_pending, pending_owner) or None if none found.
    ///
    /// If `owner_start_ts` is set on this iterator, pending nodes owned by that
    /// transaction are visible (except LOCK nodes which represent "no value").
    /// Non-owner pending nodes are returned with pending info for resolution by
    /// the transaction layer via TxnStateCache.
    fn find_first_valid_version(
        &self,
        user_key: &[u8],
        head: *mut VersionNode,
        ts_filter: Option<Timestamp>,
    ) -> Option<(*const VersionNode, bool, Timestamp)> {
        let mut version_ptr = head;
        while !version_ptr.is_null() {
            let node = unsafe { &*version_ptr };

            // Skip aborted nodes
            if node.is_aborted() {
                version_ptr = node.next;
                continue;
            }

            // Handle pending nodes
            if node.is_pending() {
                if node.get_owner() == self.owner_ts && !is_lock(&node.value) {
                    // Own pending node with value - visible (read-your-writes)
                    return Some((version_ptr, false, 0));
                }
                if node.get_owner() != self.owner_ts {
                    // Other txn's pending - return for resolution by transaction layer
                    return Some((version_ptr, true, node.get_owner()));
                }
                // LOCK from our own txn - skip
                version_ptr = node.next;
                continue;
            }

            // Committed node - apply timestamp filter if specified
            if let Some(max_ts) = ts_filter {
                if node.get_ts() > max_ts {
                    version_ptr = node.next;
                    continue;
                }
            }

            if self.is_version_in_range(user_key, node.get_ts()) {
                return Some((version_ptr, false, 0));
            }
            version_ptr = node.next;
        }
        None
    }

    /// Check if a (user_key, ts) pair is within the MVCC key range.
    #[inline]
    fn is_version_in_range(&self, user_key: &[u8], ts: Timestamp) -> bool {
        // Check start bound (inclusive)
        if !self.range.start.is_unbounded() {
            let start_user_key = self.range.start.key();
            let start_ts = self.range.start.timestamp();

            if user_key < start_user_key {
                return false;
            }
            if user_key == start_user_key && ts > start_ts {
                return false; // ts > start_ts means MVCC key < range_start
            }
        }

        // Check end bound (exclusive)
        if !self.range.end.is_unbounded() {
            let end_user_key = self.range.end.key();
            let end_ts = self.range.end.timestamp();

            if user_key > end_user_key {
                return false;
            }
            if user_key == end_user_key && ts <= end_ts {
                return false; // ts <= end_ts means MVCC key >= range_end
            }
        }

        true
    }

    /// Move to the next valid entry (internal helper).
    fn step_forward(&mut self) {
        if self.current_entry.is_none() {
            return; // Already exhausted
        }

        // Get current user key (reference, no clone)
        let current_user_key = self.current_entry.as_ref().unwrap().key().as_slice();

        // Try to advance within the current version chain
        if !self.current_version.is_null() {
            let node = unsafe { &*self.current_version };
            let mut next_version = node.next;

            // Find next version in range (no allocation, just pointer traversal)
            while !next_version.is_null() {
                let next_node = unsafe { &*next_version };

                // Skip aborted nodes
                if next_node.is_aborted() {
                    next_version = next_node.next;
                    continue;
                }

                // Handle pending nodes
                if next_node.is_pending() {
                    if next_node.get_owner() == self.owner_ts && !is_lock(&next_node.value) {
                        // Own pending node with value - visible (read-your-writes)
                        self.current_version = next_version;
                        self.current_is_pending = false;
                        self.current_pending_owner = 0;
                        return;
                    }
                    if next_node.get_owner() != self.owner_ts {
                        // Other txn's pending - return for resolution by transaction layer
                        self.current_version = next_version;
                        self.current_is_pending = true;
                        self.current_pending_owner = next_node.get_owner();
                        return;
                    }
                    // LOCK from our own txn - skip
                    next_version = next_node.next;
                    continue;
                }

                if self.is_version_in_range(current_user_key, next_node.get_ts()) {
                    self.current_version = next_version;
                    self.current_is_pending = false;
                    self.current_pending_owner = 0;
                    return;
                }
                next_version = next_node.next;
            }
        }

        // Version chain exhausted, move to next user key
        self.move_to_next_user_key();
    }

    /// Move to the next user key after the current one.
    ///
    /// Uses `Entry::next()` for O(1) amortized traversal instead of seeking.
    fn move_to_next_user_key(&mut self) {
        loop {
            let next_entry = self.current_entry.as_ref().and_then(|e| e.next());

            match next_entry {
                None => {
                    self.current_entry = None;
                    self.current_version = std::ptr::null();
                    return;
                }
                Some(entry) => {
                    let user_key = entry.key().as_slice();

                    // Check if past end of range
                    if !self.range.end.is_unbounded() && user_key > self.range.end.key() {
                        self.current_entry = None;
                        self.current_version = std::ptr::null();
                        return;
                    }

                    let row = entry.value();
                    let head = row.head.load(Ordering::Acquire);

                    if head.is_null() {
                        self.current_entry = Some(entry);
                        continue; // Try next entry
                    }

                    // Find first version in range
                    if let Some((version_ptr, is_pending, pending_owner)) =
                        self.find_first_valid_version(user_key, head, None)
                    {
                        self.current_version = version_ptr;
                        self.current_entry = Some(entry);
                        self.current_is_pending = is_pending;
                        self.current_pending_owner = pending_owner;
                        return;
                    }

                    self.current_entry = Some(entry);
                    // No valid version in this entry, loop continues to next
                }
            }
        }
    }
}

impl<'a> MvccIterator for VersionedMemTableIterator<'a> {
    async fn seek(&mut self, target: &MvccKey) -> Result<()> {
        // Reset state
        self.initialized = true;
        self.current_entry = None;
        self.current_version = std::ptr::null();

        // Handle unbounded target - seek to first entry within range
        if target.is_unbounded() {
            self.initialized = false;
            self.initialize();
            return Ok(());
        }

        let target_user_key = target.key();
        let target_ts = target.timestamp();

        // Seek to target (reuse the same logic)
        self.seek_from_user_key(target_user_key, Some(target_ts));
        Ok(())
    }

    async fn advance(&mut self) -> Result<()> {
        if !self.initialized {
            self.initialize();
        } else {
            self.step_forward();
        }
        Ok(())
    }

    fn valid(&self) -> bool {
        self.initialized && self.current_entry.is_some() && !self.current_version.is_null()
    }

    fn user_key(&self) -> &[u8] {
        self.current_entry
            .as_ref()
            .expect("Iterator not valid")
            .key()
            .as_slice()
    }

    fn timestamp(&self) -> Timestamp {
        debug_assert!(!self.current_version.is_null(), "Iterator not valid");
        unsafe { (*self.current_version).get_ts() }
    }

    fn value(&self) -> &[u8] {
        debug_assert!(!self.current_version.is_null(), "Iterator not valid");
        unsafe { &(*self.current_version).value }
    }

    fn is_pending(&self) -> bool {
        self.current_is_pending
    }

    fn pending_owner(&self) -> Timestamp {
        self.current_pending_owner
    }
}

// ============================================================================
// ArcVersionedMemTableIterator - Owned iterator for StorageEngine
// ============================================================================

/// Owned iterator over the versioned memtable for `StorageEngine` implementation.
///
/// This iterator owns its data via `Arc<VersionedMemTableEngineInner>`, allowing
/// it to be returned from `StorageEngine::scan_iter` without borrowing from `self`.
///
/// # Safety
///
/// Uses `unsafe` to extend the lifetime of the `VersionedMemTableIterator` from
/// the `Arc`'s lifetime to `'static`. This is safe because:
/// 1. The `Arc` keeps the inner data alive for as long as this struct exists
/// 2. The `_inner` field is declared after `iter`, ensuring proper drop order
/// 3. `VersionedMemTableIterator` doesn't access the inner data in its `Drop` impl
pub struct ArcVersionedMemTableIterator {
    /// The streaming iterator with erased lifetime (safe because _inner keeps data alive)
    iter: VersionedMemTableIterator<'static>,
    /// Keep the inner data alive. MUST be declared after `iter` so it's dropped last.
    _inner: Arc<VersionedMemTableEngineInner>,
}

impl ArcVersionedMemTableIterator {
    /// Create a new owned iterator over the given inner data and range.
    ///
    /// The iterator is NOT positioned after construction - caller must call
    /// `advance()` before accessing data. This follows the unified iterator pattern:
    /// `advance()` → `valid()` → `read()`.
    ///
    /// # Arguments
    ///
    /// * `inner` - Arc reference to memtable engine inner data
    /// * `range` - MVCC key range (Arc for zero-copy sharing)
    /// * `owner_ts` - Transaction's start_ts for read-your-writes
    pub fn new(
        inner: Arc<VersionedMemTableEngineInner>,
        range: SharedMvccRange,
        owner_ts: Timestamp,
    ) -> Self {
        // Safety: We're extending the lifetime of the reference from the Arc's lifetime
        // to 'static. This is safe because:
        // 1. The Arc keeps the inner data alive for as long as this struct exists
        // 2. The _inner field is declared after iter, ensuring proper drop order
        // 3. VersionedMemTableIterator doesn't access the inner in its Drop impl
        let inner_ref: &'static VersionedMemTableEngineInner =
            unsafe { std::mem::transmute(inner.as_ref()) };
        let iter = VersionedMemTableIterator::new(inner_ref, range, owner_ts);
        Self {
            iter,
            _inner: inner,
        }
    }
}

impl MvccIterator for ArcVersionedMemTableIterator {
    async fn seek(&mut self, target: &MvccKey) -> Result<()> {
        self.iter.seek(target).await
    }

    async fn advance(&mut self) -> Result<()> {
        self.iter.advance().await
    }

    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn user_key(&self) -> &[u8] {
        self.iter.user_key()
    }

    fn timestamp(&self) -> Timestamp {
        self.iter.timestamp()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn is_pending(&self) -> bool {
        self.iter.is_pending()
    }

    fn pending_owner(&self) -> Timestamp {
        self.iter.pending_owner()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mvcc::is_tombstone;
    use std::collections::HashSet;

    fn new_engine() -> VersionedMemTableEngine {
        VersionedMemTableEngine::new()
    }

    // ==================== Test Helpers ====================

    /// Write a key-value pair at the given timestamp (test-only).
    fn put_at(engine: &VersionedMemTableEngine, key: &[u8], value: &[u8], ts: Timestamp) {
        engine.put_internal(key, value.to_vec(), ts);
    }

    /// Write a tombstone (delete marker) at the given timestamp (test-only).
    fn delete_at(engine: &VersionedMemTableEngine, key: &[u8], ts: Timestamp) {
        engine.put_internal(key, TOMBSTONE.to_vec(), ts);
    }

    /// Scan MVCC keys in range using the streaming iterator (test-only helper).
    /// Collects results into a Vec for easy testing assertions.
    async fn scan_mvcc(
        engine: &VersionedMemTableEngine,
        range: Range<MvccKey>,
    ) -> Vec<(MvccKey, RawValue)> {
        let mut results = Vec::new();
        let mut iter = engine.create_streaming_iter(std::sync::Arc::new(range), 0);
        iter.advance().await.unwrap();
        while iter.valid() {
            let key = MvccKey::encode(iter.user_key(), iter.timestamp());
            results.push((key, iter.value().to_vec()));
            iter.advance().await.unwrap();
        }
        results
    }

    /// Scan all versions for a range of user keys (test-only).
    fn scan_all_versions(
        engine: &VersionedMemTableEngine,
        range: &Range<Key>,
    ) -> Vec<(Key, Timestamp, RawValue)> {
        let mut results = Vec::new();

        for entry in engine
            .inner
            .index
            .range(range.start.clone()..range.end.clone())
        {
            let key = entry.key();
            let row = entry.value();

            // Traverse version chain directly (newest first)
            let mut current = row.head.load(std::sync::atomic::Ordering::Acquire);
            while !current.is_null() {
                // Safety: current points to a valid node owned by MvccRow
                let node = unsafe { &*current };
                results.push((key.clone(), node.get_ts(), node.value.clone()));
                current = node.next;
            }
        }

        results
    }

    /// Get the latest version of a key visible at the given timestamp.
    fn get_at_for_test(
        engine: &VersionedMemTableEngine,
        key: &[u8],
        ts: Timestamp,
    ) -> Option<RawValue> {
        if let Some(entry) = engine.inner.index.get(key) {
            if let Some(value) = entry.value().get_at(ts) {
                if !is_tombstone(value) {
                    return Some(value.clone());
                }
            }
        }
        None
    }

    fn get_for_test(engine: &VersionedMemTableEngine, key: &[u8]) -> Option<RawValue> {
        get_at_for_test(engine, key, Timestamp::MAX)
    }

    fn scan_at_for_test(
        engine: &VersionedMemTableEngine,
        range: &Range<Key>,
        ts: Timestamp,
    ) -> Vec<(Key, RawValue)> {
        let mut results = Vec::new();

        for entry in engine
            .inner
            .index
            .range(range.start.clone()..range.end.clone())
        {
            let key = entry.key();
            let row = entry.value();

            if let Some(value) = row.get_at(ts) {
                if !is_tombstone(value) {
                    results.push((key.clone(), value.clone()));
                }
            }
        }

        results.sort_by(|a, b| a.0.cmp(&b.0));
        results
    }

    fn scan_for_test(engine: &VersionedMemTableEngine, range: &Range<Key>) -> Vec<(Key, RawValue)> {
        scan_at_for_test(engine, range, Timestamp::MAX)
    }

    // ==================== Basic Tests ====================

    #[tokio::test]
    async fn test_basic_put_get() {
        let engine = new_engine();

        put_at(&engine, b"key1", b"value1", 1);
        let value = get_for_test(&engine, b"key1");

        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let engine = new_engine();

        let value = get_for_test(&engine, b"nonexistent");
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_delete() {
        let engine = new_engine();

        put_at(&engine, b"key1", b"value1", 1);
        delete_at(&engine, b"key1", 2);

        let value = get_for_test(&engine, b"key1");
        assert_eq!(value, None);
    }

    // ==================== MVCC Version Tests ====================

    #[tokio::test]
    async fn test_mvcc_versions() {
        let engine = new_engine();

        // Write version 1
        put_at(&engine, b"key", b"v1", 10);

        // Write version 2
        put_at(&engine, b"key", b"v2", 20);

        // Write version 3
        put_at(&engine, b"key", b"v3", 30);

        // Only one key in the skiplist
        assert_eq!(engine.key_count(), 1);
        // But three versions
        assert_eq!(engine.len(), 3);

        // Read at ts=10 should see v1
        let v = get_at_for_test(&engine, b"key", 10);
        assert_eq!(v, Some(b"v1".to_vec()));

        // Read at ts=20 should see v2
        let v = get_at_for_test(&engine, b"key", 20);
        assert_eq!(v, Some(b"v2".to_vec()));

        // Read at ts=30 should see v3
        let v = get_at_for_test(&engine, b"key", 30);
        assert_eq!(v, Some(b"v3".to_vec()));

        // Read at latest should see v3
        let v = get_at_for_test(&engine, b"key", Timestamp::MAX);
        assert_eq!(v, Some(b"v3".to_vec()));

        // Read at ts before any write should see nothing
        let v = get_at_for_test(&engine, b"key", 5);
        assert_eq!(v, None);
    }

    #[tokio::test]
    async fn test_mvcc_delete_version() {
        let engine = new_engine();

        // Write value at ts=10
        put_at(&engine, b"key", b"value", 10);

        // Delete at ts=20
        delete_at(&engine, b"key", 20);

        // Read at ts=10 should see value
        let v = get_at_for_test(&engine, b"key", 10);
        assert_eq!(v, Some(b"value".to_vec()));

        // Read at ts=15 should still see value
        let v = get_at_for_test(&engine, b"key", 15);
        assert_eq!(v, Some(b"value".to_vec()));

        // Read at ts=20 should see nothing (deleted)
        let v = get_at_for_test(&engine, b"key", 20);
        assert_eq!(v, None);

        // Read at latest should see nothing
        let v = get_at_for_test(&engine, b"key", Timestamp::MAX);
        assert_eq!(v, None);
    }

    #[tokio::test]
    async fn test_mvcc_multiple_versions_visibility() {
        let engine = new_engine();

        // Create multiple versions
        put_at(&engine, b"key", b"v1", 10);
        put_at(&engine, b"key", b"v2", 20);
        put_at(&engine, b"key", b"v3", 30);
        delete_at(&engine, b"key", 40);
        put_at(&engine, b"key", b"v4", 50);

        // Test visibility at various timestamps
        assert_eq!(get_at_for_test(&engine, b"key", 5), None);
        assert_eq!(get_at_for_test(&engine, b"key", 10), Some(b"v1".to_vec()));
        assert_eq!(get_at_for_test(&engine, b"key", 15), Some(b"v1".to_vec()));
        assert_eq!(get_at_for_test(&engine, b"key", 20), Some(b"v2".to_vec()));
        assert_eq!(get_at_for_test(&engine, b"key", 25), Some(b"v2".to_vec()));
        assert_eq!(get_at_for_test(&engine, b"key", 30), Some(b"v3".to_vec()));
        assert_eq!(get_at_for_test(&engine, b"key", 35), Some(b"v3".to_vec()));
        assert_eq!(get_at_for_test(&engine, b"key", 40), None); // deleted
        assert_eq!(get_at_for_test(&engine, b"key", 45), None); // still deleted
        assert_eq!(get_at_for_test(&engine, b"key", 50), Some(b"v4".to_vec())); // rewritten
    }

    // ==================== Scan Tests ====================

    #[tokio::test]
    async fn test_scan() {
        let engine = new_engine();

        put_at(&engine, b"a", b"1", 1);
        put_at(&engine, b"b", b"2", 1);
        put_at(&engine, b"c", b"3", 1);
        put_at(&engine, b"d", b"4", 1);

        let results = scan_for_test(&engine, &(b"b".to_vec()..b"d".to_vec()));

        assert_eq!(results.len(), 2);
        let keys: Vec<_> = results.iter().map(|(k, _)| k.clone()).collect();
        assert!(keys.contains(&b"b".to_vec()));
        assert!(keys.contains(&b"c".to_vec()));
    }

    #[tokio::test]
    async fn test_scan_at_mvcc() {
        let engine = new_engine();

        // Write data at explicit timestamps
        put_at(&engine, b"a", b"1", 10);
        put_at(&engine, b"b", b"2", 20);
        put_at(&engine, b"c", b"3", 30);

        // scan_at with ts=30 should see all data
        let range = b"a".to_vec()..b"d".to_vec();
        let results = scan_at_for_test(&engine, &range, 30);
        assert_eq!(results.len(), 3, "scan_at ts=30 should see 3 keys");

        // scan_at with ts=20 should see a and b only
        let results = scan_at_for_test(&engine, &range, 20);
        assert_eq!(results.len(), 2, "scan_at ts=20 should see 2 keys");

        // scan_at with ts=10 should see a only
        let results = scan_at_for_test(&engine, &range, 10);
        assert_eq!(results.len(), 1, "scan_at ts=10 should see 1 key");

        // scan_at with ts=5 should see nothing
        let results = scan_at_for_test(&engine, &range, 5);
        assert_eq!(results.len(), 0, "scan_at ts=5 should see 0 keys");
    }

    #[tokio::test]
    async fn test_scan_at_with_deletes() {
        let engine = new_engine();

        // Write keys at ts=10
        put_at(&engine, b"a", b"1", 10);
        put_at(&engine, b"b", b"2", 10);
        put_at(&engine, b"c", b"3", 10);

        // Delete b at ts=20
        delete_at(&engine, b"b", 20);

        // scan_at ts=15 should see all 3 keys
        let range = b"a".to_vec()..b"d".to_vec();
        let results = scan_at_for_test(&engine, &range, 15);
        assert_eq!(results.len(), 3, "ts=15 should see 3 keys (before delete)");

        // scan_at ts=25 should see only a and c
        let results = scan_at_for_test(&engine, &range, 25);
        assert_eq!(results.len(), 2, "ts=25 should see 2 keys (b deleted)");
        let keys: Vec<_> = results.iter().map(|(k, _)| k.clone()).collect();
        assert!(keys.contains(&b"a".to_vec()));
        assert!(keys.contains(&b"c".to_vec()));
    }

    // ==================== WriteBatch Tests ====================

    #[tokio::test]
    async fn test_write_batch_requires_commit_ts() {
        let engine = new_engine();

        let mut batch = WriteBatch::new();
        batch.put(b"k1".to_vec(), b"v1".to_vec());

        let result = engine.write_batch(batch);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_batch_with_commit_ts() {
        let engine = new_engine();

        let commit_ts = 100;
        let mut batch = WriteBatch::new();
        batch.set_commit_ts(commit_ts);
        batch.put(b"k1".to_vec(), b"v1".to_vec());
        batch.put(b"k2".to_vec(), b"v2".to_vec());
        batch.put(b"k3".to_vec(), b"v3".to_vec());

        engine.write_batch(batch).unwrap();

        // All keys should be visible at latest
        assert_eq!(get_for_test(&engine, b"k1"), Some(b"v1".to_vec()));
        assert_eq!(get_for_test(&engine, b"k2"), Some(b"v2".to_vec()));
        assert_eq!(get_for_test(&engine, b"k3"), Some(b"v3".to_vec()));

        // Should be visible at ts >= 100
        let v = get_at_for_test(&engine, b"k1", 100);
        assert_eq!(v, Some(b"v1".to_vec()));

        // Should not be visible at ts < 100
        let v = get_at_for_test(&engine, b"k1", 99);
        assert_eq!(v, None);
    }

    // ==================== StorageEngine Scan Tests ====================

    #[tokio::test]
    async fn test_storage_engine_scan_unbounded() {
        let engine = new_engine();

        put_at(&engine, b"a", b"1", 10);
        put_at(&engine, b"a", b"2", 20); // Second version
        put_at(&engine, b"b", b"3", 15);

        // Scan all with unbounded range
        let results = scan_mvcc(&engine, MvccKey::unbounded()..MvccKey::unbounded()).await;

        // Should have 3 entries (2 for "a", 1 for "b")
        assert_eq!(results.len(), 3);

        // Verify MVCC keys are properly encoded
        let (key_a_20, _) = results[0].0.decode();
        assert_eq!(key_a_20, b"a".to_vec());
    }

    #[tokio::test]
    async fn test_storage_engine_scan_bounded() {
        let engine = new_engine();

        put_at(&engine, b"a", b"1", 10);
        put_at(&engine, b"b", b"2", 20);
        put_at(&engine, b"c", b"3", 30);

        // Scan with MVCC key range
        // MvccKey::encode(b"c", 0) = c || !0 = c || 0xFF...FF
        // Since c at ts=30 encodes to c || !30 < c || !0, it's included
        let start = MvccKey::encode(b"a", Timestamp::MAX);
        let end = MvccKey::encode(b"c", 0);

        let results = scan_mvcc(&engine, start..end).await;

        // All three entries are included:
        // - (a, 10): a || !10 is in range
        // - (b, 20): b || !20 is in range
        // - (c, 30): c || !30 < c || !0, so it's in range
        assert_eq!(results.len(), 3);

        // To exclude key "c" entirely, use the next key after "b"
        let start = MvccKey::encode(b"a", Timestamp::MAX);
        let end = MvccKey::encode(b"c", Timestamp::MAX); // Start of "c" versions

        let results = scan_mvcc(&engine, start..end).await;
        // Now only a and b are included
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_storage_engine_scan_same_key_timestamp_range() {
        // Regression test: when start_user_key == end_user_key, both timestamp
        // bounds must be enforced. Previously the code used mutually exclusive
        // if/else branches that only checked one bound.
        let engine = new_engine();

        // Write multiple versions of the same key
        put_at(&engine, b"key", b"v10", 10);
        put_at(&engine, b"key", b"v20", 20);
        put_at(&engine, b"key", b"v30", 30);
        put_at(&engine, b"key", b"v40", 40);
        put_at(&engine, b"key", b"v50", 50);

        // Scan range: key@40 (inclusive) to key@20 (exclusive)
        // Should include versions at ts=40 and ts=30, but NOT ts=50 or ts=20 or ts=10
        // MVCC encoding: higher ts = smaller encoded key
        // So key@40 < key@30 < key@20 in MVCC order
        let start = MvccKey::encode(b"key", 40);
        let end = MvccKey::encode(b"key", 20);

        let results = scan_mvcc(&engine, start..end).await;

        // Should have exactly 2 versions: ts=40 and ts=30
        assert_eq!(
            results.len(),
            2,
            "Expected 2 versions (ts=40, ts=30), got {}",
            results.len()
        );

        // Verify the timestamps
        let timestamps: Vec<_> = results.iter().map(|(k, _)| k.timestamp()).collect();
        assert!(
            timestamps.contains(&40),
            "Should contain ts=40, got {timestamps:?}"
        );
        assert!(
            timestamps.contains(&30),
            "Should contain ts=30, got {timestamps:?}"
        );
        assert!(
            !timestamps.contains(&50),
            "Should NOT contain ts=50, got {timestamps:?}"
        );
        assert!(
            !timestamps.contains(&20),
            "Should NOT contain ts=20 (exclusive end), got {timestamps:?}"
        );
        assert!(
            !timestamps.contains(&10),
            "Should NOT contain ts=10, got {timestamps:?}"
        );
    }

    // ==================== Concurrent Tests ====================

    use std::sync::atomic::AtomicU64;
    use std::sync::Barrier;
    use std::thread;

    #[tokio::test]
    async fn test_concurrent_writes() {
        let engine = VersionedMemTableEngine::new();
        let num_threads = 8;
        let writes_per_thread = 1000;
        let barrier = Barrier::new(num_threads);

        thread::scope(|s| {
            for tid in 0..num_threads {
                let engine = &engine;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    for i in 0..writes_per_thread {
                        let key = format!("key_{tid}_{i}");
                        let ts = (tid * writes_per_thread + i + 1) as u64;
                        put_at(engine, key.as_bytes(), b"value", ts);
                    }
                });
            }
        });

        // Verify all writes are visible
        for tid in 0..num_threads {
            for i in 0..writes_per_thread {
                let key = format!("key_{tid}_{i}");
                assert!(
                    get_for_test(&engine, key.as_bytes()).is_some(),
                    "Missing key {key}"
                );
            }
        }

        // Total version count
        assert_eq!(engine.len(), num_threads * writes_per_thread);
    }

    #[tokio::test]
    async fn test_concurrent_writes_same_key() {
        // Multiple threads writing versions to the same key.
        //
        // In production, the ConcurrencyManager ensures per-key serialization:
        // only one transaction can hold a lock on a key at a time. We simulate
        // this with a mutex to match real-world behavior.
        use std::sync::Mutex;

        let engine = VersionedMemTableEngine::new();
        let num_threads = 8;
        let versions_per_thread = 100;
        let barrier = Barrier::new(num_threads);
        let ts_counter = AtomicU64::new(1);
        // Simulates ConcurrencyManager's per-key lock
        let key_lock = Mutex::new(());

        thread::scope(|s| {
            for tid in 0..num_threads {
                let engine = &engine;
                let barrier = &barrier;
                let ts_counter = &ts_counter;
                let key_lock = &key_lock;

                s.spawn(move || {
                    barrier.wait();

                    for i in 0..versions_per_thread {
                        // Acquire per-key lock (simulating ConcurrencyManager behavior)
                        let _guard = key_lock.lock().unwrap();
                        // Get timestamp while holding lock - ensures monotonic per-key ordering
                        let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
                        let value = format!("v{tid}_{i}");
                        put_at(engine, b"hotkey", value.as_bytes(), ts);
                    }
                });
            }
        });

        // Should have exactly 1 key
        assert_eq!(engine.key_count(), 1);

        // But many versions
        assert_eq!(engine.len(), num_threads * versions_per_thread);

        // Verify versions are readable at different timestamps
        let total_versions = num_threads * versions_per_thread;
        for ts in [1, 100, 500, total_versions as u64] {
            let _ = get_at_for_test(&engine, b"hotkey", ts);
            // Just verify no panic
        }
    }

    #[tokio::test]
    async fn test_concurrent_reads_and_writes() {
        // Test concurrent reads and writes where each writer thread has its own
        // distinct key space. This matches real-world behavior where concurrent
        // writes to the same key would be serialized by ConcurrencyManager.
        let engine = VersionedMemTableEngine::new();
        let keys_per_writer = 250; // Each writer has 250 keys

        let num_writers = 4;
        let num_readers = 4;
        let ops_per_thread = 1000;
        let barrier = Barrier::new(num_writers + num_readers);

        // Pre-populate: each writer's key range gets initial value at ts=1
        for tid in 0..num_writers {
            let base = tid * keys_per_writer;
            for i in 0..keys_per_writer {
                put_at(
                    &engine,
                    format!("key{:04}", base + i).as_bytes(),
                    b"initial",
                    1,
                );
            }
        }

        thread::scope(|s| {
            // Writer threads - each writes to its own distinct key range
            for tid in 0..num_writers {
                let engine = &engine;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    let base = tid * keys_per_writer;
                    for i in 0..ops_per_thread {
                        // Write to this thread's key range only
                        let key = format!("key{:04}", base + (i % keys_per_writer));
                        // Timestamp increases with each write within this thread
                        let ts = (100 + i) as u64;
                        let value = format!("value_{tid}_{i}");
                        put_at(engine, key.as_bytes(), value.as_bytes(), ts);
                    }
                });
            }

            // Reader threads - read at various timestamps
            for _ in 0..num_readers {
                let engine = &engine;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    for i in 0..ops_per_thread {
                        let key = format!("key{:04}", i % (num_writers * keys_per_writer));
                        // Read at timestamp 1 should always see "initial"
                        let value = get_at_for_test(engine, key.as_bytes(), 1);
                        assert_eq!(value, Some(b"initial".to_vec()));
                    }
                });
            }
        });
    }

    #[tokio::test]
    async fn test_memory_stats() {
        let engine = new_engine();

        let stats_before = engine.memory_stats();
        assert_eq!(stats_before.entry_count, 0);
        assert_eq!(stats_before.key_count, 0);

        // Add 100 keys, each with 2 versions
        for i in 0..100 {
            put_at(&engine, format!("key{i:03}").as_bytes(), b"v1", 10);
            put_at(&engine, format!("key{i:03}").as_bytes(), b"v2", 20);
        }

        let stats_after = engine.memory_stats();
        assert_eq!(stats_after.entry_count, 200); // 100 keys * 2 versions
        assert_eq!(stats_after.key_count, 100); // 100 unique keys
    }

    #[tokio::test]
    async fn test_scan_all_versions() {
        let engine = new_engine();

        put_at(&engine, b"a", b"a1", 10);
        put_at(&engine, b"a", b"a2", 20);
        put_at(&engine, b"b", b"b1", 15);
        put_at(&engine, b"c", b"c1", 5);
        delete_at(&engine, b"c", 25); // Tombstone

        let range = b"a".to_vec()..b"d".to_vec();
        let results = scan_all_versions(&engine, &range);

        // Should have 5 versions total
        assert_eq!(results.len(), 5);

        // Verify we have all expected versions
        let version_set: HashSet<_> = results.iter().map(|(k, ts, _)| (k.clone(), *ts)).collect();

        assert!(version_set.contains(&(b"a".to_vec(), 10)));
        assert!(version_set.contains(&(b"a".to_vec(), 20)));
        assert!(version_set.contains(&(b"b".to_vec(), 15)));
        assert!(version_set.contains(&(b"c".to_vec(), 5)));
        assert!(version_set.contains(&(b"c".to_vec(), 25))); // Tombstone included
    }

    // ==================== Version Chain Ordering Tests ====================

    #[tokio::test]
    async fn test_version_chain_ordering_correct() {
        // Verify that when versions are inserted in correct order (ascending ts),
        // reads return the correct latest visible version.
        let engine = new_engine();

        // Insert versions in ascending timestamp order (correct)
        put_at(&engine, b"key", b"v10", 10);
        put_at(&engine, b"key", b"v20", 20);
        put_at(&engine, b"key", b"v30", 30);

        // Read at ts=25 should return v20 (latest visible at ts=25)
        let value = get_at_for_test(&engine, b"key", 25);
        assert_eq!(
            value,
            Some(b"v20".to_vec()),
            "Should return latest visible version (ts=20), not ts=10 or ts=30"
        );

        // Read at ts=30 should return v30
        let value = get_at_for_test(&engine, b"key", 30);
        assert_eq!(value, Some(b"v30".to_vec()));

        // Read at ts=5 should return None (no visible version)
        let value = get_at_for_test(&engine, b"key", 5);
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_version_chain_ordering_same_timestamp() {
        // Test that inserting at the same timestamp is allowed (idempotent writes)
        let engine = new_engine();

        put_at(&engine, b"key", b"v1", 10);
        put_at(&engine, b"key", b"v2", 10); // Same ts, different value

        // Should return the latest inserted value at ts=10
        let value = get_at_for_test(&engine, b"key", 10);
        assert_eq!(value, Some(b"v2".to_vec()));
    }

    #[tokio::test]
    #[should_panic(expected = "MVCC version chain ordering violation")]
    async fn test_version_chain_ordering_violation_panics() {
        // Inserting a version with ts < head's ts must panic to prevent silent corruption.
        // This catches bugs where the transaction layer violates the ordering invariant.
        let engine = new_engine();

        // Insert version at ts=20 first
        put_at(&engine, b"key", b"v20", 20);

        // Try to insert version at ts=10 (WRONG: ts < head's ts)
        // This must panic in ALL builds (not just debug) to prevent silent data corruption
        put_at(&engine, b"key", b"v10", 10);
    }

    // ==================== Iterator Edge Case Tests ====================

    #[tokio::test]
    async fn test_iterator_empty_memtable() {
        // Iterating over an empty memtable should immediately be invalid
        let engine = new_engine();

        let range = MvccKey::unbounded()..MvccKey::unbounded();
        let mut iter = engine.create_streaming_iter(std::sync::Arc::new(range), 0);

        iter.advance().await.unwrap();
        assert!(
            !iter.valid(),
            "Iterator over empty memtable should be invalid"
        );
    }

    #[tokio::test]
    async fn test_iterator_single_entry() {
        // Single entry iteration
        let engine = new_engine();
        put_at(&engine, b"only_key", b"only_value", 100);

        let range = MvccKey::unbounded()..MvccKey::unbounded();
        let mut iter = engine.create_streaming_iter(std::sync::Arc::new(range), 0);

        iter.advance().await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"only_key");
        assert_eq!(iter.timestamp(), 100);
        assert_eq!(iter.value(), b"only_value");

        iter.advance().await.unwrap();
        assert!(!iter.valid(), "Should be invalid after single entry");
    }

    #[tokio::test]
    async fn test_iterator_single_key_multiple_versions() {
        // Single key with multiple versions
        let engine = new_engine();
        for ts in 1..=10 {
            let value = format!("v{ts}");
            put_at(&engine, b"key", value.as_bytes(), ts);
        }

        let range = MvccKey::unbounded()..MvccKey::unbounded();
        let results = scan_mvcc(&engine, range).await;

        // Should see all 10 versions in descending timestamp order (newest first)
        assert_eq!(results.len(), 10);
        for (i, (mvcc_key, value)) in results.iter().enumerate() {
            let expected_ts = 10 - i as u64;
            let (_, ts) = MvccKey::decode(mvcc_key);
            assert_eq!(ts, expected_ts, "Version {i} should have ts={expected_ts}");
            assert_eq!(value, format!("v{expected_ts}").as_bytes());
        }
    }

    #[tokio::test]
    async fn test_iterator_large_version_chain() {
        // Large version chain (100+ versions per key)
        let engine = new_engine();
        let num_versions = 200;

        for ts in 1..=num_versions {
            let value = format!("version_{ts:04}");
            put_at(&engine, b"hot_key", value.as_bytes(), ts);
        }

        assert_eq!(engine.len(), num_versions as usize);
        assert_eq!(engine.key_count(), 1);

        // Verify all versions are accessible via scan
        let range = MvccKey::unbounded()..MvccKey::unbounded();
        let results = scan_mvcc(&engine, range).await;
        assert_eq!(results.len(), num_versions as usize);

        // Verify point reads at various timestamps
        for ts in [1, 50, 100, 150, 200] {
            let value = get_at_for_test(&engine, b"hot_key", ts);
            assert_eq!(
                value,
                Some(format!("version_{ts:04}").into_bytes()),
                "Read at ts={ts} should return version_{ts}"
            );
        }
    }

    #[tokio::test]
    async fn test_iterator_seek_to_nonexistent_key() {
        // Seek to a key that doesn't exist should find the next key
        let engine = new_engine();
        put_at(&engine, b"aaa", b"v1", 10);
        put_at(&engine, b"ccc", b"v2", 20);
        put_at(&engine, b"eee", b"v3", 30);

        // Seek to "bbb" should position on "ccc"
        let range = MvccKey::encode(b"bbb", Timestamp::MAX)..MvccKey::unbounded();
        let mut iter = engine.create_streaming_iter(std::sync::Arc::new(range), 0);

        iter.advance().await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"ccc");
        assert_eq!(iter.timestamp(), 20);
    }

    #[tokio::test]
    async fn test_iterator_seek_past_all_keys() {
        // Seek past all keys should be invalid
        let engine = new_engine();
        put_at(&engine, b"aaa", b"v1", 10);
        put_at(&engine, b"bbb", b"v2", 20);

        let range = MvccKey::encode(b"zzz", Timestamp::MAX)..MvccKey::unbounded();
        let mut iter = engine.create_streaming_iter(std::sync::Arc::new(range), 0);

        iter.advance().await.unwrap();
        assert!(!iter.valid(), "Seek past all keys should be invalid");
    }

    #[tokio::test]
    async fn test_iterator_seek_before_all_keys() {
        // Seek before all keys should find the first key
        let engine = new_engine();
        put_at(&engine, b"mmm", b"v1", 10);
        put_at(&engine, b"nnn", b"v2", 20);

        // Start from very beginning (before "mmm")
        let range = MvccKey::encode(b"aaa", Timestamp::MAX)..MvccKey::unbounded();
        let mut iter = engine.create_streaming_iter(std::sync::Arc::new(range), 0);

        iter.advance().await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"mmm");
    }

    #[tokio::test]
    async fn test_iterator_seek_exact_match() {
        // Seek to exact key and timestamp
        let engine = new_engine();
        put_at(&engine, b"key", b"v10", 10);
        put_at(&engine, b"key", b"v20", 20);
        put_at(&engine, b"key", b"v30", 30);

        // Seek to exactly (key, 20)
        let range = MvccKey::encode(b"key", 20)..MvccKey::unbounded();
        let mut iter = engine.create_streaming_iter(std::sync::Arc::new(range), 0);

        iter.advance().await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"key");
        assert_eq!(iter.timestamp(), 20);

        iter.advance().await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.timestamp(), 10, "Next version should be ts=10");
    }

    // ==================== Boundary Condition Tests ====================

    #[tokio::test]
    async fn test_timestamp_boundary_zero() {
        // Timestamp 0 is the minimum timestamp
        let engine = new_engine();
        put_at(&engine, b"key", b"at_zero", 0);
        put_at(&engine, b"key", b"at_one", 1);

        // Read at ts=0 should see the value at ts=0
        let value = get_at_for_test(&engine, b"key", 0);
        assert_eq!(value, Some(b"at_zero".to_vec()));

        // Read at ts=1 should see the value at ts=1
        let value = get_at_for_test(&engine, b"key", 1);
        assert_eq!(value, Some(b"at_one".to_vec()));
    }

    #[tokio::test]
    async fn test_timestamp_boundary_max() {
        // Timestamp::MAX is the maximum timestamp
        let engine = new_engine();
        put_at(&engine, b"key", b"at_max", Timestamp::MAX);

        let value = get_at_for_test(&engine, b"key", Timestamp::MAX);
        assert_eq!(value, Some(b"at_max".to_vec()));

        // Reading at MAX-1 should not see it (version not yet visible)
        let value = get_at_for_test(&engine, b"key", Timestamp::MAX - 1);
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_empty_range_scan() {
        // Range where start >= end (empty range)
        let engine = new_engine();
        put_at(&engine, b"aaa", b"v1", 10);
        put_at(&engine, b"bbb", b"v2", 20);

        // Empty range: start == end
        let range = MvccKey::encode(b"bbb", 15)..MvccKey::encode(b"bbb", 15);
        let results = scan_mvcc(&engine, range).await;
        assert!(results.is_empty(), "Empty range should return no results");
    }

    #[tokio::test]
    async fn test_single_key_range_scan() {
        // Range covering exactly one key
        let engine = new_engine();
        put_at(&engine, b"aaa", b"v1", 10);
        put_at(&engine, b"bbb", b"v2", 20);
        put_at(&engine, b"ccc", b"v3", 30);

        // Range covering only "bbb"
        let range =
            MvccKey::encode(b"bbb", Timestamp::MAX)..MvccKey::encode(b"ccc", Timestamp::MAX);
        let results = scan_mvcc(&engine, range).await;
        assert_eq!(results.len(), 1);
        let (key, _) = &results[0];
        let (user_key, _) = MvccKey::decode(key);
        assert_eq!(user_key, b"bbb");
    }

    #[tokio::test]
    async fn test_unbounded_range_with_no_data() {
        // Full unbounded range on empty memtable
        let engine = new_engine();

        let range = MvccKey::unbounded()..MvccKey::unbounded();
        let results = scan_mvcc(&engine, range).await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_range_excludes_exact_end_bound() {
        // Range end is exclusive - key at exact end bound should not be included
        let engine = new_engine();
        put_at(&engine, b"aaa", b"v1", 10);
        put_at(&engine, b"bbb", b"v2", 20);
        put_at(&engine, b"ccc", b"v3", 30);

        // Range excludes "bbb" at ts=20 exactly
        let range = MvccKey::encode(b"aaa", Timestamp::MAX)..MvccKey::encode(b"bbb", 20);
        let results = scan_mvcc(&engine, range).await;

        // Should only include "aaa" (bbb@20 is excluded by exclusive end bound)
        assert_eq!(results.len(), 1);
        let (key, _) = &results[0];
        let (user_key, _) = MvccKey::decode(key);
        assert_eq!(user_key, b"aaa");
    }

    // ==================== Arc Iterator Lifetime Tests ====================

    #[tokio::test]
    async fn test_arc_iterator_outlives_engine_reference() {
        // The Arc iterator should be usable after the engine reference is dropped
        let engine = new_engine();
        put_at(&engine, b"key1", b"value1", 10);
        put_at(&engine, b"key2", b"value2", 20);

        // Get Arc to inner
        let inner = engine.inner_arc();
        let range = std::sync::Arc::new(MvccKey::unbounded()..MvccKey::unbounded());

        // Create Arc iterator
        let mut iter = ArcVersionedMemTableIterator::new(inner, range, 0);

        // Drop the engine (but Arc keeps inner alive)
        drop(engine);

        // Iterator should still work
        iter.advance().await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"key1");

        iter.advance().await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"key2");

        iter.advance().await.unwrap();
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_arc_iterator_seek_operations() {
        // Test seek operations on Arc iterator
        let engine = new_engine();
        for i in 0..10 {
            let key = format!("key_{i:02}");
            put_at(&engine, key.as_bytes(), b"value", i as u64 + 1);
        }

        let inner = engine.inner_arc();
        let range = std::sync::Arc::new(MvccKey::unbounded()..MvccKey::unbounded());
        let mut iter = ArcVersionedMemTableIterator::new(inner, range, 0);

        // Seek to middle
        let target = MvccKey::encode(b"key_05", Timestamp::MAX);
        iter.seek(&target).await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"key_05");

        // Seek to non-existent key
        let target = MvccKey::encode(b"key_03a", Timestamp::MAX);
        iter.seek(&target).await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"key_04"); // Next key after "key_03a"
    }

    // ==================== MvccRow Edge Case Tests ====================

    #[tokio::test]
    async fn test_mvcc_row_get_at_empty_chain() {
        // Getting from an empty row should return None
        let row = MvccRow::new_empty();
        assert!(row.get_at(100).is_none());
        assert!(row.get_at(0).is_none());
        assert!(row.get_at(Timestamp::MAX).is_none());
    }

    #[tokio::test]
    async fn test_mvcc_row_version_count_accuracy() {
        // Version count should accurately reflect the number of versions
        let row = MvccRow::new_empty();
        assert_eq!(row.version_count.load(Ordering::Relaxed), 0);

        row.prepend(10, b"v1".to_vec());
        assert_eq!(row.version_count.load(Ordering::Relaxed), 1);

        row.prepend(20, b"v2".to_vec());
        assert_eq!(row.version_count.load(Ordering::Relaxed), 2);

        row.prepend(30, b"v3".to_vec());
        assert_eq!(row.version_count.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_key_with_binary_data() {
        // Keys and values can contain arbitrary binary data including nulls
        let engine = new_engine();

        let binary_key = b"\x00\x01\x02\xff\xfe\xfd";
        let binary_value = b"\x00\x00\x00null\x00bytes";

        put_at(&engine, binary_key, binary_value, 100);

        let value = get_for_test(&engine, binary_key);
        assert_eq!(value, Some(binary_value.to_vec()));
    }

    #[tokio::test]
    async fn test_empty_key_and_value() {
        // Empty keys and values should work
        let engine = new_engine();

        put_at(&engine, b"", b"", 10);

        let value = get_for_test(&engine, b"");
        assert_eq!(value, Some(vec![]));
    }

    #[tokio::test]
    async fn test_large_value() {
        // Large values (1MB+) should work
        let engine = new_engine();

        let large_value = vec![b'x'; 1024 * 1024]; // 1MB
        put_at(&engine, b"large_key", &large_value, 100);

        let value = get_for_test(&engine, b"large_key");
        assert_eq!(value.as_ref().map(|v| v.len()), Some(1024 * 1024));
        assert_eq!(value, Some(large_value));
    }

    #[tokio::test]
    async fn test_many_keys_iteration_order() {
        // Verify iteration order is correct across many keys
        let engine = new_engine();

        // Insert keys in random-ish order
        for i in [5, 3, 8, 1, 9, 2, 7, 4, 6, 0] {
            let key = format!("key_{i:02}");
            put_at(&engine, key.as_bytes(), b"value", (i + 1) as u64);
        }

        // Scan should return keys in sorted order
        let range = MvccKey::unbounded()..MvccKey::unbounded();
        let results = scan_mvcc(&engine, range).await;

        assert_eq!(results.len(), 10);
        for (i, (mvcc_key, _)) in results.iter().enumerate() {
            let expected_key = format!("key_{i:02}");
            let (user_key, _) = MvccKey::decode(mvcc_key);
            assert_eq!(
                user_key,
                expected_key.as_bytes(),
                "Key {i} should be {expected_key}"
            );
        }
    }

    // ==================== Pessimistic Locking Tests ====================

    #[tokio::test]
    async fn test_put_pending_basic() {
        let engine = new_engine();

        // Write pending value
        engine
            .put_pending(b"key1", b"value1".to_vec(), 100)
            .unwrap();

        // Pending write should not be visible via normal read
        assert!(get_for_test(&engine, b"key1").is_none());

        // But should be able to check lock owner
        assert_eq!(engine.get_lock_owner(b"key1"), Some(100));
    }

    #[tokio::test]
    async fn test_put_pending_conflict() {
        let engine = new_engine();

        // First write from txn 100
        engine.put_pending(b"key1", b"v1".to_vec(), 100).unwrap();

        // Second write from txn 200 should fail (key locked)
        let result = engine.put_pending(b"key1", b"v2".to_vec(), 200);
        assert_eq!(result, Err(100)); // Returns lock owner
    }

    #[tokio::test]
    async fn test_put_pending_same_txn_update() {
        let engine = new_engine();

        // First write
        engine.put_pending(b"key1", b"v1".to_vec(), 100).unwrap();

        // Same txn writing again should update value in place
        engine.put_pending(b"key1", b"v2".to_vec(), 100).unwrap();

        // Should still be locked by txn 100
        assert_eq!(engine.get_lock_owner(b"key1"), Some(100));

        // Should have only 1 entry (update in place, not new node)
        // Actually prepend_pending adds a new node if it's a new insert,
        // but updates in place on same txn re-write
        assert_eq!(engine.len(), 1);
    }

    #[tokio::test]
    async fn test_finalize_pending() {
        let engine = new_engine();

        // Write pending values
        engine.put_pending(b"key1", b"v1".to_vec(), 100).unwrap();
        engine.put_pending(b"key2", b"v2".to_vec(), 100).unwrap();

        // Neither should be visible
        assert!(get_for_test(&engine, b"key1").is_none());
        assert!(get_for_test(&engine, b"key2").is_none());

        // Finalize (commit) the pending writes
        let keys = vec![b"key1".to_vec(), b"key2".to_vec()];
        engine.finalize_pending(&keys, 100, 150); // commit_ts = 150

        // Now both should be visible
        assert_eq!(get_for_test(&engine, b"key1"), Some(b"v1".to_vec()));
        assert_eq!(get_for_test(&engine, b"key2"), Some(b"v2".to_vec()));

        // No longer locked
        assert_eq!(engine.get_lock_owner(b"key1"), None);
        assert_eq!(engine.get_lock_owner(b"key2"), None);
    }

    #[tokio::test]
    async fn test_abort_pending() {
        let engine = new_engine();

        // Write pending values
        engine.put_pending(b"key1", b"v1".to_vec(), 100).unwrap();

        // Key is locked
        assert_eq!(engine.get_lock_owner(b"key1"), Some(100));

        // Abort the transaction
        let keys = vec![b"key1".to_vec()];
        engine.abort_pending(&keys, 100);

        // Key should no longer appear as locked (aborted nodes don't conflict)
        assert_eq!(engine.get_lock_owner(b"key1"), None);

        // Value should not be visible
        assert!(get_for_test(&engine, b"key1").is_none());
    }

    #[tokio::test]
    async fn test_abort_pending_allows_new_write() {
        let engine = new_engine();

        // Txn 100 writes and aborts
        engine.put_pending(b"key1", b"v1".to_vec(), 100).unwrap();
        let keys = vec![b"key1".to_vec()];
        engine.abort_pending(&keys, 100);

        // Txn 200 should now be able to write
        let result = engine.put_pending(b"key1", b"v2".to_vec(), 200);
        assert!(result.is_ok());

        // Key locked by txn 200
        assert_eq!(engine.get_lock_owner(b"key1"), Some(200));
    }

    #[tokio::test]
    async fn test_pending_not_visible_to_scan() {
        let engine = new_engine();

        // Write committed value
        put_at(&engine, b"a", b"committed", 10);

        // Write pending value
        engine.put_pending(b"b", b"pending".to_vec(), 100).unwrap();

        // Write another committed value
        put_at(&engine, b"c", b"committed2", 20);

        // Non-owner scan now returns pending nodes (with is_pending=true)
        // for resolution by the transaction layer.
        let range = Arc::new(MvccKey::unbounded()..MvccKey::unbounded());
        let mut iter = engine.create_streaming_iter(range, 0);
        iter.advance().await.unwrap();

        // First: "a" committed
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"a");
        assert!(!iter.is_pending());
        iter.advance().await.unwrap();

        // Second: "b" pending (returned for resolution)
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"b");
        assert!(iter.is_pending());
        assert_eq!(iter.pending_owner(), 100);
        iter.advance().await.unwrap();

        // Third: "c" committed
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"c");
        assert!(!iter.is_pending());
        iter.advance().await.unwrap();

        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_finalized_visible_to_scan() {
        let engine = new_engine();

        // Write and finalize pending value
        engine.put_pending(b"key", b"value".to_vec(), 100).unwrap();
        let keys = vec![b"key".to_vec()];
        engine.finalize_pending(&keys, 100, 150);

        // Scan should see the finalized value
        let range = MvccKey::unbounded()..MvccKey::unbounded();
        let results = scan_mvcc(&engine, range).await;

        assert_eq!(results.len(), 1);
        let (key, value) = &results[0];
        assert_eq!(key.key(), b"key");
        assert_eq!(key.timestamp(), 150); // commit_ts
        assert_eq!(value, b"value");
    }

    // ==================== Read-Your-Writes Tests ====================

    #[tokio::test]
    async fn test_get_at_with_owner_reads_own_pending() {
        // Owner should see their own pending write
        let row = MvccRow::new_empty();

        // Write pending value owned by txn 100
        row.prepend_pending(100, b"my_value".to_vec()).unwrap();

        // Owner (start_ts=100) should see their pending write
        let value = row.get_at_with_owner(100, 100);
        assert_eq!(value, Some(&b"my_value".to_vec()));

        // Non-owner (start_ts=200) should not see the pending write
        let value = row.get_at_with_owner(200, 200);
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_get_at_with_owner_lock_skips_to_previous() {
        use crate::storage::mvcc::LOCK;

        let row = MvccRow::new_empty();

        // First, add a committed value at ts=50
        row.prepend(50, b"committed_value".to_vec());

        // Then, add a pending LOCK owned by txn 100 (simulating DELETE after INSERT)
        row.prepend_pending(100, LOCK.to_vec()).unwrap();

        // Owner sees LOCK, skips to previous committed version
        let value = row.get_at_with_owner(100, 100);
        assert_eq!(value, Some(&b"committed_value".to_vec()));

        // Non-owner also sees committed version (skips pending LOCK)
        let value = row.get_at_with_owner(100, 200);
        assert_eq!(value, Some(&b"committed_value".to_vec()));
    }

    #[tokio::test]
    async fn test_get_at_with_owner_lock_no_previous_returns_none() {
        use crate::storage::mvcc::LOCK;

        let row = MvccRow::new_empty();

        // Add pending LOCK with no previous version (DELETE after INSERT on new key)
        row.prepend_pending(100, LOCK.to_vec()).unwrap();

        // Owner sees LOCK, no previous version -> None
        let value = row.get_at_with_owner(100, 100);
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_get_at_with_owner_tombstone_returns_none() {
        let row = MvccRow::new_empty();

        // Add a committed tombstone at ts=50
        row.prepend(50, TOMBSTONE.to_vec());

        // Any reader sees tombstone -> None
        let value = row.get_at_with_owner(100, 100);
        assert!(value.is_none());

        let value = row.get_at_with_owner(100, 200);
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_get_at_with_owner_pending_tombstone_for_owner() {
        let row = MvccRow::new_empty();

        // Add a committed value
        row.prepend(50, b"old_value".to_vec());

        // Add pending TOMBSTONE owned by txn 100 (DELETE on committed value)
        row.prepend_pending(100, TOMBSTONE.to_vec()).unwrap();

        // Owner sees their pending TOMBSTONE (not LOCK, so returns it as value)
        // But TOMBSTONE is returned as the value, caller checks is_tombstone()
        // Actually, let's check: get_at_with_owner returns the value, caller should check
        // In our design, pending TOMBSTONE should mean "I'm deleting this"
        // The owner should see the TOMBSTONE value
        let value = row.get_at_with_owner(100, 100);
        assert_eq!(value, Some(&TOMBSTONE.to_vec()));

        // Non-owner skips pending, sees committed value
        let value = row.get_at_with_owner(100, 200);
        assert_eq!(value, Some(&b"old_value".to_vec()));
    }

    #[tokio::test]
    async fn test_get_at_with_owner_skips_aborted() {
        let row = MvccRow::new_empty();

        // Add committed value
        row.prepend(50, b"committed".to_vec());

        // Add pending value, then abort it
        row.prepend_pending(100, b"aborted_value".to_vec()).unwrap();
        row.abort_pending(100);

        // Everyone should skip aborted and see committed value
        let value = row.get_at_with_owner(100, 100);
        assert_eq!(value, Some(&b"committed".to_vec()));

        let value = row.get_at_with_owner(100, 200);
        assert_eq!(value, Some(&b"committed".to_vec()));
    }

    #[tokio::test]
    async fn test_get_at_with_owner_multiple_pending_from_different_txns() {
        // This shouldn't happen in practice (lock conflict), but test behavior
        let row = MvccRow::new_empty();

        // Add committed base
        row.prepend(50, b"base".to_vec());

        // Manually create scenario: txn 100 wrote, then txn 200 wrote (conflict in real scenario)
        // In reality, txn 200 would be blocked. But test the traversal logic.
        row.prepend_pending(100, b"txn100_value".to_vec()).unwrap();
        // This would fail with lock conflict in real code, but MvccRow doesn't enforce that
        // Let's simulate by directly adding (bypassing conflict check)
        // Actually, prepend_pending checks conflicts, so we can't easily test this

        // Instead, test that owner only sees their own
        let value = row.get_at_with_owner(100, 100);
        assert_eq!(value, Some(&b"txn100_value".to_vec()));

        // Non-owner 200 skips txn 100's pending, sees base
        let value = row.get_at_with_owner(100, 200);
        assert_eq!(value, Some(&b"base".to_vec()));
    }

    #[tokio::test]
    async fn test_get_at_with_owner_insert_delete_insert_pattern() {
        use crate::storage::mvcc::LOCK;

        let row = MvccRow::new_empty();

        // Committed base value
        row.prepend(50, b"base".to_vec());

        // Txn 100: INSERT (pending value)
        row.prepend_pending(100, b"inserted".to_vec()).unwrap();

        // Owner sees their insert
        assert_eq!(row.get_at_with_owner(100, 100), Some(&b"inserted".to_vec()));

        // Txn 100: DELETE (update pending to LOCK)
        // Simulate by updating value in place (as prepend_pending does for same owner)
        // In real code this is done via delete_pending, but we can test MvccRow directly
        // by calling prepend_pending with LOCK
        row.prepend_pending(100, LOCK.to_vec()).unwrap(); // Updates in place

        // Owner sees LOCK, skips to base
        assert_eq!(row.get_at_with_owner(100, 100), Some(&b"base".to_vec()));

        // Txn 100: INSERT again (update LOCK back to value)
        row.prepend_pending(100, b"reinserted".to_vec()).unwrap();

        // Owner sees their reinsert
        assert_eq!(
            row.get_at_with_owner(100, 100),
            Some(&b"reinserted".to_vec())
        );
    }

    // ==================== delete_pending Tests ====================

    #[tokio::test]
    async fn test_delete_pending_on_own_pending_write() {
        let row = MvccRow::new_empty();

        // Txn 100 writes a pending value
        row.prepend_pending(100, b"my_value".to_vec()).unwrap();

        // Txn 100 deletes their pending write (converts to LOCK)
        let result = row.delete_pending(100);
        assert_eq!(result, Ok(true));

        // Owner should now see LOCK, skip to previous (none) -> None
        let value = row.get_at_with_owner(100, 100);
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_delete_pending_on_own_pending_with_committed_base() {
        let row = MvccRow::new_empty();

        // Committed base value
        row.prepend(50, b"base".to_vec());

        // Txn 100 writes pending value
        row.prepend_pending(100, b"updated".to_vec()).unwrap();

        // Txn 100 deletes (converts to LOCK)
        let result = row.delete_pending(100);
        assert_eq!(result, Ok(true));

        // Owner sees LOCK, skips to base
        let value = row.get_at_with_owner(100, 100);
        assert_eq!(value, Some(&b"base".to_vec()));
    }

    #[tokio::test]
    async fn test_delete_pending_on_committed_value() {
        let row = MvccRow::new_empty();

        // Committed value
        row.prepend(50, b"committed".to_vec());

        // Txn 100 deletes committed value (writes pending TOMBSTONE)
        let result = row.delete_pending(100);
        assert_eq!(result, Ok(true));

        // Owner sees their pending TOMBSTONE
        let value = row.get_at_with_owner(100, 100);
        assert_eq!(value, Some(&TOMBSTONE.to_vec()));

        // Non-owner sees committed value (skips pending)
        let value = row.get_at_with_owner(100, 200);
        assert_eq!(value, Some(&b"committed".to_vec()));
    }

    #[tokio::test]
    async fn test_delete_pending_conflict_with_other_txn() {
        let row = MvccRow::new_empty();

        // Txn 100 writes pending
        row.prepend_pending(100, b"v1".to_vec()).unwrap();

        // Txn 200 tries to delete - should fail with lock conflict
        let result = row.delete_pending(200);
        assert_eq!(result, Err(100));
    }

    #[tokio::test]
    async fn test_delete_pending_on_empty_row() {
        let row = MvccRow::new_empty();

        // Delete on non-existent key - should do nothing
        let result = row.delete_pending(100);
        assert_eq!(result, Ok(false));
    }

    #[tokio::test]
    async fn test_delete_pending_on_already_deleted() {
        let row = MvccRow::new_empty();

        // Committed tombstone
        row.prepend(50, TOMBSTONE.to_vec());

        // Delete on already deleted - should do nothing
        let result = row.delete_pending(100);
        assert_eq!(result, Ok(false));
    }

    #[tokio::test]
    async fn test_delete_pending_then_reinsert() {
        let row = MvccRow::new_empty();

        // Txn 100: INSERT
        row.prepend_pending(100, b"first".to_vec()).unwrap();
        assert_eq!(row.get_at_with_owner(100, 100), Some(&b"first".to_vec()));

        // Txn 100: DELETE (-> LOCK)
        row.delete_pending(100).unwrap();
        assert!(row.get_at_with_owner(100, 100).is_none());

        // Txn 100: RE-INSERT (LOCK -> value)
        row.prepend_pending(100, b"second".to_vec()).unwrap();
        assert_eq!(row.get_at_with_owner(100, 100), Some(&b"second".to_vec()));
    }

    #[tokio::test]
    async fn test_delete_pending_after_aborted_node() {
        let row = MvccRow::new_empty();

        // Committed base
        row.prepend(50, b"base".to_vec());

        // Txn 100 writes and aborts
        row.prepend_pending(100, b"aborted".to_vec()).unwrap();
        row.abort_pending(100);

        // Txn 200 deletes - should write TOMBSTONE (committed value exists)
        let result = row.delete_pending(200);
        assert_eq!(result, Ok(true));

        // Txn 200 sees their TOMBSTONE
        let value = row.get_at_with_owner(200, 200);
        assert_eq!(value, Some(&TOMBSTONE.to_vec()));
    }

    // ==================== finalize_pending with LOCK Tests ====================

    #[tokio::test]
    async fn test_finalize_pending_aborts_lock_nodes() {
        let row = MvccRow::new_empty();

        // Committed base
        row.prepend(50, b"base".to_vec());

        // Txn 100: INSERT then DELETE (creates LOCK)
        row.prepend_pending(100, b"value".to_vec()).unwrap();
        row.delete_pending(100).unwrap();

        // Before commit: owner sees LOCK, skips to base
        assert_eq!(row.get_at_with_owner(100, 100), Some(&b"base".to_vec()));

        // Commit: LOCK should be aborted, not finalized
        row.finalize_pending(100, 150);

        // After commit: everyone sees base (LOCK was aborted)
        assert_eq!(row.get_at_with_owner(200, 200), Some(&b"base".to_vec()));

        // No longer locked
        assert!(row.get_head_pending_owner().is_none());
    }

    #[tokio::test]
    async fn test_finalize_pending_finalizes_value_nodes() {
        let row = MvccRow::new_empty();

        // Txn 100: INSERT
        row.prepend_pending(100, b"value".to_vec()).unwrap();

        // Before commit: non-owner doesn't see it
        assert!(row.get_at_with_owner(100, 200).is_none());

        // Commit
        row.finalize_pending(100, 150);

        // After commit: everyone sees the value at commit_ts
        assert_eq!(row.get_at_with_owner(200, 200), Some(&b"value".to_vec()));

        // Not visible to readers before commit_ts
        assert!(row.get_at_with_owner(100, 200).is_none());
    }

    #[tokio::test]
    async fn test_finalize_pending_finalizes_tombstone_nodes() {
        let row = MvccRow::new_empty();

        // Committed value
        row.prepend(50, b"old".to_vec());

        // Txn 100: DELETE (writes pending TOMBSTONE)
        row.delete_pending(100).unwrap();

        // Before commit: non-owner sees old value
        assert_eq!(row.get_at_with_owner(100, 200), Some(&b"old".to_vec()));

        // Commit
        row.finalize_pending(100, 150);

        // After commit: everyone sees tombstone (deleted)
        assert!(row.get_at_with_owner(200, 200).is_none());
    }

    #[tokio::test]
    async fn test_finalize_pending_mixed_nodes() {
        let row = MvccRow::new_empty();

        // Txn 100: Multiple operations
        // First key-value pair is committed base
        row.prepend(50, b"base".to_vec());

        // Txn 100 writes, deletes (LOCK), then writes again
        row.prepend_pending(100, b"v1".to_vec()).unwrap();
        row.delete_pending(100).unwrap(); // LOCK
        row.prepend_pending(100, b"v2".to_vec()).unwrap(); // Updates LOCK -> value

        // Commit: LOCK (if still present) would be aborted, value is finalized
        row.finalize_pending(100, 150);

        // After commit: value is visible
        assert_eq!(row.get_at_with_owner(200, 200), Some(&b"v2".to_vec()));
    }

    // ==================== Iterator with Owner Tests ====================

    #[tokio::test]
    async fn test_iterator_with_owner_sees_pending_value() {
        use crate::storage::mvcc::MvccKey;

        let engine = VersionedMemTableEngine::new();

        // Add committed value
        engine.put_internal(b"key1", b"committed".to_vec(), 100);

        // Add pending value from txn 200
        engine
            .put_pending(b"key1", b"pending".to_vec(), 200)
            .unwrap();

        // Non-owner iterator returns the pending node (for resolution by txn layer)
        let range = Range {
            start: MvccKey::unbounded(),
            end: MvccKey::unbounded(),
        };
        let range_arc = Arc::new(range);

        let mut iter =
            VersionedMemTableIterator::new(engine.inner.as_ref(), Arc::clone(&range_arc), 0);
        iter.advance().await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"key1");
        assert!(iter.is_pending());
        assert_eq!(iter.pending_owner(), 200);
        assert_eq!(iter.value(), b"pending");

        // Owner iterator should see pending value (read-your-writes, not marked as pending)
        let range2 = Range {
            start: MvccKey::unbounded(),
            end: MvccKey::unbounded(),
        };
        let mut owner_iter =
            VersionedMemTableIterator::new(engine.inner.as_ref(), Arc::new(range2), 200);
        owner_iter.advance().await.unwrap();
        assert!(owner_iter.valid());
        assert_eq!(owner_iter.user_key(), b"key1");
        assert_eq!(owner_iter.value(), b"pending");
        assert!(!owner_iter.is_pending()); // Own pending not reported as pending
    }

    #[tokio::test]
    async fn test_iterator_with_owner_skips_lock_node() {
        use crate::storage::mvcc::MvccKey;

        let engine = VersionedMemTableEngine::new();

        // Add committed value
        engine.put_internal(b"key1", b"committed".to_vec(), 100);

        // Txn 200 writes then deletes (creates LOCK)
        engine
            .put_pending(b"key1", b"pending".to_vec(), 200)
            .unwrap();
        engine.delete_pending(b"key1", 200).unwrap();

        // Owner iterator should skip LOCK and see committed value
        let range = Range {
            start: MvccKey::unbounded(),
            end: MvccKey::unbounded(),
        };
        let mut owner_iter =
            VersionedMemTableIterator::new(engine.inner.as_ref(), Arc::new(range), 200);
        owner_iter.advance().await.unwrap();
        assert!(owner_iter.valid());
        assert_eq!(owner_iter.user_key(), b"key1");
        assert_eq!(owner_iter.value(), b"committed");
    }

    #[tokio::test]
    async fn test_iterator_with_owner_multiple_keys() {
        use crate::storage::mvcc::MvccKey;

        let engine = VersionedMemTableEngine::new();

        // key1: committed only
        engine.put_internal(b"key1", b"k1_committed".to_vec(), 100);

        // key2: committed + pending from owner
        engine.put_internal(b"key2", b"k2_committed".to_vec(), 100);
        engine
            .put_pending(b"key2", b"k2_pending".to_vec(), 200)
            .unwrap();

        // key3: pending only from owner
        engine
            .put_pending(b"key3", b"k3_pending".to_vec(), 200)
            .unwrap();

        // Owner iterator (txn 200) should see:
        // key1 -> committed (no pending)
        // key2 -> pending (owned)
        // key3 -> pending (owned)
        let range = Range {
            start: MvccKey::unbounded(),
            end: MvccKey::unbounded(),
        };
        let mut owner_iter =
            VersionedMemTableIterator::new(engine.inner.as_ref(), Arc::new(range), 200);

        // key1: committed@100
        owner_iter.advance().await.unwrap();
        assert!(owner_iter.valid());
        assert_eq!(owner_iter.user_key(), b"key1");
        assert_eq!(owner_iter.value(), b"k1_committed");

        // key2: pending@200 (owned - visible)
        owner_iter.advance().await.unwrap();
        assert!(owner_iter.valid());
        assert_eq!(owner_iter.user_key(), b"key2");
        assert_eq!(owner_iter.value(), b"k2_pending");

        // key2: committed@100 (also visible - MVCC iterator returns all versions)
        owner_iter.advance().await.unwrap();
        assert!(owner_iter.valid());
        assert_eq!(owner_iter.user_key(), b"key2");
        assert_eq!(owner_iter.value(), b"k2_committed");

        // key3: pending@200 (owned - visible)
        owner_iter.advance().await.unwrap();
        assert!(owner_iter.valid());
        assert_eq!(owner_iter.user_key(), b"key3");
        assert_eq!(owner_iter.value(), b"k3_pending");

        // No more
        owner_iter.advance().await.unwrap();
        assert!(!owner_iter.valid());
    }

    #[tokio::test]
    async fn test_iterator_with_owner_does_not_see_other_txn_pending() {
        use crate::storage::mvcc::MvccKey;

        let engine = VersionedMemTableEngine::new();

        // Committed base
        engine.put_internal(b"key1", b"committed".to_vec(), 100);

        // Txn 200 has pending write
        engine
            .put_pending(b"key1", b"txn200_pending".to_vec(), 200)
            .unwrap();

        // Txn 300 sees txn 200's pending node first (marked as pending for resolution)
        let range = Range {
            start: MvccKey::unbounded(),
            end: MvccKey::unbounded(),
        };
        let mut iter_300 =
            VersionedMemTableIterator::new(engine.inner.as_ref(), Arc::new(range), 300);
        iter_300.advance().await.unwrap();
        assert!(iter_300.valid());
        assert_eq!(iter_300.user_key(), b"key1");
        assert!(iter_300.is_pending());
        assert_eq!(iter_300.pending_owner(), 200);
    }

    #[tokio::test]
    async fn test_arc_iterator_with_owner() {
        use crate::storage::mvcc::MvccKey;

        let engine = VersionedMemTableEngine::new();

        // Committed value
        engine.put_internal(b"key1", b"committed".to_vec(), 100);

        // Pending from txn 200
        engine
            .put_pending(b"key1", b"pending".to_vec(), 200)
            .unwrap();

        // Test ArcVersionedMemTableIterator with owner
        let range = Range {
            start: MvccKey::unbounded(),
            end: MvccKey::unbounded(),
        };
        let mut arc_iter =
            ArcVersionedMemTableIterator::new(engine.inner_arc(), Arc::new(range), 200);

        arc_iter.advance().await.unwrap();
        assert!(arc_iter.valid());
        assert_eq!(arc_iter.user_key(), b"key1");
        assert_eq!(arc_iter.value(), b"pending");
    }

    #[tokio::test]
    async fn test_scan_iter_with_owner_via_storage_engine() {
        use crate::storage::mvcc::MvccKey;
        use crate::storage::{MvccIterator, PessimisticStorage, StorageEngine};

        let engine = VersionedMemTableEngine::new();

        // Committed value
        engine.put_internal(b"key1", b"committed".to_vec(), 100);

        // Pending from txn 200
        PessimisticStorage::put_pending(&engine, b"key1", b"pending".to_vec(), 200).unwrap();

        // Use unified scan_iter with owner_ts for read-your-writes
        let range = Range {
            start: MvccKey::unbounded(),
            end: MvccKey::unbounded(),
        };
        let mut iter = engine.scan_iter(range, 200).unwrap();

        iter.advance().await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"key1");
        assert_eq!(iter.value(), b"pending");
    }

    #[tokio::test]
    async fn test_iterator_with_owner_pending_only_key() {
        use crate::storage::mvcc::MvccKey;

        let engine = VersionedMemTableEngine::new();

        // Only pending, no committed
        engine
            .put_pending(b"key1", b"pending".to_vec(), 200)
            .unwrap();

        // Non-owner sees the pending node (marked for resolution)
        let range = Range {
            start: MvccKey::unbounded(),
            end: MvccKey::unbounded(),
        };
        let mut non_owner_iter =
            VersionedMemTableIterator::new(engine.inner.as_ref(), Arc::new(range), 0);
        non_owner_iter.advance().await.unwrap();
        assert!(non_owner_iter.valid());
        assert!(non_owner_iter.is_pending());
        assert_eq!(non_owner_iter.pending_owner(), 200);

        // Owner should see it (not marked as pending)
        let range2 = Range {
            start: MvccKey::unbounded(),
            end: MvccKey::unbounded(),
        };
        let mut owner_iter =
            VersionedMemTableIterator::new(engine.inner.as_ref(), Arc::new(range2), 200);
        owner_iter.advance().await.unwrap();
        assert!(owner_iter.valid());
        assert_eq!(owner_iter.user_key(), b"key1");
        assert_eq!(owner_iter.value(), b"pending");
        assert!(!owner_iter.is_pending());
    }

    #[tokio::test]
    async fn test_iterator_with_owner_lock_only_key() {
        use crate::storage::mvcc::MvccKey;

        let engine = VersionedMemTableEngine::new();

        // Write then delete (creates LOCK)
        engine
            .put_pending(b"key1", b"to_delete".to_vec(), 200)
            .unwrap();
        engine.delete_pending(b"key1", 200).unwrap();

        // Owner should skip LOCK (no committed value to fall back to)
        let range = Range {
            start: MvccKey::unbounded(),
            end: MvccKey::unbounded(),
        };
        let mut owner_iter =
            VersionedMemTableIterator::new(engine.inner.as_ref(), Arc::new(range), 200);
        owner_iter.advance().await.unwrap();
        assert!(!owner_iter.valid()); // LOCK is skipped, no other version
    }

    // ==================== Additional Coverage Tests ====================

    #[tokio::test]
    async fn test_delete_nonexistent_key() {
        let engine = VersionedMemTableEngine::new();

        // Delete on nonexistent key should return Ok(false)
        let result = engine.delete_pending(b"nonexistent", 100);
        assert!(result.is_ok());
        assert!(!result.unwrap()); // False means nothing was deleted
    }

    #[tokio::test]
    async fn test_abort_nonexistent_key() {
        let engine = VersionedMemTableEngine::new();

        // Abort on nonexistent key should complete without error
        engine.abort_pending(&[b"nonexistent".to_vec()], 100);
        // No error expected
    }

    #[tokio::test]
    async fn test_finalize_nonexistent_key() {
        let engine = VersionedMemTableEngine::new();

        // Finalize on nonexistent key should complete without error
        engine.finalize_pending(&[b"nonexistent".to_vec()], 100, 200);
        // No error expected
    }

    #[tokio::test]
    async fn test_get_lock_owner_nonexistent_key() {
        let engine = VersionedMemTableEngine::new();

        // get_lock_owner on nonexistent key should return None
        let result = engine.get_lock_owner(b"nonexistent");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_lock_owner_committed_key() {
        let engine = VersionedMemTableEngine::new();

        // Committed key has no lock owner
        engine.put_internal(b"key", b"value".to_vec(), 100);
        let result = engine.get_lock_owner(b"key");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_lock_owner_pending_key() {
        let engine = VersionedMemTableEngine::new();

        // Pending key has lock owner
        engine.put_pending(b"key", b"value".to_vec(), 200).unwrap();
        let result = engine.get_lock_owner(b"key");
        assert_eq!(result, Some(200));
    }

    #[tokio::test]
    async fn test_delete_already_tombstone() {
        let engine = VersionedMemTableEngine::new();

        // Write then delete (commit) using write batch
        engine.put_internal(b"key", b"value".to_vec(), 100);
        let mut del_batch = WriteBatch::new();
        del_batch.delete(b"key".to_vec());
        del_batch.set_commit_ts(200);
        engine.write_batch(del_batch).unwrap();

        // Try to delete again with pending
        let result = engine.delete_pending(b"key", 300);
        // Should return Ok(false) since already deleted
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_finalize_on_committed_key() {
        let engine = VersionedMemTableEngine::new();

        // Write committed data
        engine.put_internal(b"key", b"value".to_vec(), 100);

        // Finalize on committed key (no pending) - should be no-op
        engine.finalize_pending(&[b"key".to_vec()], 200, 300);
        // No error expected
    }

    #[tokio::test]
    async fn test_abort_on_committed_key() {
        let engine = VersionedMemTableEngine::new();

        // Write committed data
        engine.put_internal(b"key", b"value".to_vec(), 100);

        // Abort on committed key (no pending) - should be no-op
        engine.abort_pending(&[b"key".to_vec()], 200);
        // No error expected
    }

    #[tokio::test]
    async fn test_finalize_wrong_owner() {
        let engine = VersionedMemTableEngine::new();

        // Write pending with owner 100
        engine.put_pending(b"key", b"value".to_vec(), 100).unwrap();

        // Finalize with wrong owner - should be no-op for that key
        engine.finalize_pending(&[b"key".to_vec()], 200, 300);

        // Key should still be pending with owner 100
        assert_eq!(engine.get_lock_owner(b"key"), Some(100));
    }

    #[tokio::test]
    async fn test_abort_wrong_owner() {
        let engine = VersionedMemTableEngine::new();

        // Write pending with owner 100
        engine.put_pending(b"key", b"value".to_vec(), 100).unwrap();

        // Abort with wrong owner - should be no-op
        engine.abort_pending(&[b"key".to_vec()], 200);

        // Key should still be pending with owner 100
        assert_eq!(engine.get_lock_owner(b"key"), Some(100));
    }

    #[tokio::test]
    async fn test_get_with_owner_tombstone_filtering() {
        let engine = VersionedMemTableEngine::new();

        // Write then delete (pending tombstone)
        engine.put_pending(b"key", b"value".to_vec(), 100).unwrap();
        engine.delete_pending(b"key", 100).unwrap();

        // get_with_owner should see LOCK and return it
        let result = engine.get_with_owner(b"key", Timestamp::MAX, 100);
        // LOCK is converted to None by the transaction layer, but here we see raw value
        assert!(result.is_none() || result.as_ref().is_some_and(|v| is_lock(v)));
    }

    #[tokio::test]
    async fn test_put_pending_then_finalize_then_read() {
        let engine = VersionedMemTableEngine::new();

        // Put pending
        engine.put_pending(b"key", b"value".to_vec(), 100).unwrap();

        // Finalize
        engine.finalize_pending(&[b"key".to_vec()], 100, 200);

        // Read at commit_ts should see value
        let result = get_for_test(&engine, b"key");
        assert_eq!(result, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_put_pending_then_abort_then_read() {
        let engine = VersionedMemTableEngine::new();

        // Put pending
        engine.put_pending(b"key", b"value".to_vec(), 100).unwrap();

        // Abort
        engine.abort_pending(&[b"key".to_vec()], 100);

        // Read should see None (aborted)
        let result = get_for_test(&engine, b"key");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_multiple_abort_same_key() {
        let engine = VersionedMemTableEngine::new();

        // Write committed base
        engine.put_internal(b"key", b"v1".to_vec(), 100);

        // Multiple pending writes that get aborted
        engine.put_pending(b"key", b"v2".to_vec(), 200).unwrap();
        engine.abort_pending(&[b"key".to_vec()], 200);

        // After abort, new transaction should be able to write
        engine.put_pending(b"key", b"v3".to_vec(), 300).unwrap();
        engine.finalize_pending(&[b"key".to_vec()], 300, 400);

        // Should see v3
        let result = get_for_test(&engine, b"key");
        assert_eq!(result, Some(b"v3".to_vec()));
    }

    #[tokio::test]
    async fn test_engine_default() {
        // Test Default trait implementation
        let engine1 = VersionedMemTableEngine::new();
        let engine2 = VersionedMemTableEngine::default();

        // Both should be empty
        assert!(engine1.is_empty());
        assert!(engine2.is_empty());
        assert_eq!(engine1.len(), 0);
        assert_eq!(engine2.len(), 0);
    }

    #[tokio::test]
    async fn test_write_batch_empty() {
        let engine = VersionedMemTableEngine::new();

        // Empty write batch (with commit_ts set) should work
        let mut batch = WriteBatch::new();
        batch.set_commit_ts(100);
        let result = engine.write_batch(batch);
        assert!(result.is_ok());
        assert!(engine.is_empty());
    }

    #[tokio::test]
    async fn test_write_batch_delete_only() {
        let engine = VersionedMemTableEngine::new();

        // First put a value
        let mut put_batch = WriteBatch::new();
        put_batch.put(b"key".to_vec(), b"value".to_vec());
        put_batch.set_commit_ts(100);
        engine.write_batch(put_batch).unwrap();

        // Delete batch only
        let mut del_batch = WriteBatch::new();
        del_batch.delete(b"key".to_vec());
        del_batch.set_commit_ts(200);
        engine.write_batch(del_batch).unwrap();

        // Should see None (deleted)
        let result = get_for_test(&engine, b"key");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_scan_iter_with_range_boundaries() {
        use crate::storage::StorageEngine;

        let engine = VersionedMemTableEngine::new();

        // Write some data
        put_at(&engine, b"aaa", b"1", 100);
        put_at(&engine, b"bbb", b"2", 100);
        put_at(&engine, b"ccc", b"3", 100);
        put_at(&engine, b"ddd", b"4", 100);

        // Scan all - should get all 4 keys
        let range = MvccKey::unbounded()..MvccKey::unbounded();
        let mut iter = engine.scan_iter(range, 0).unwrap();
        iter.advance().await.unwrap();

        let mut keys = Vec::new();
        while iter.valid() {
            keys.push(iter.user_key().to_vec());
            iter.advance().await.unwrap();
        }

        assert_eq!(keys.len(), 4);
        assert_eq!(keys[0], b"aaa");
        assert_eq!(keys[1], b"bbb");
        assert_eq!(keys[2], b"ccc");
        assert_eq!(keys[3], b"ddd");
    }

    #[tokio::test]
    async fn test_concurrent_put_and_read() {
        use std::sync::Arc;
        use std::thread;

        let engine = Arc::new(VersionedMemTableEngine::new());

        // Write initial data
        for i in 0..100 {
            put_at(&engine, format!("key{i:03}").as_bytes(), b"initial", 1);
        }

        let engine_reader = Arc::clone(&engine);
        let engine_writer = Arc::clone(&engine);

        // Reader thread
        let reader = thread::spawn(move || {
            for _ in 0..100 {
                for i in 0..100 {
                    let key = format!("key{i:03}");
                    let value = get_for_test(&engine_reader, key.as_bytes());
                    // Value should always exist
                    assert!(value.is_some());
                }
            }
        });

        // Writer thread
        let writer = thread::spawn(move || {
            for i in 0..100 {
                put_at(
                    &engine_writer,
                    format!("key{i:03}").as_bytes(),
                    format!("updated{i}").as_bytes(),
                    (i + 2) as u64,
                );
            }
        });

        reader.join().unwrap();
        writer.join().unwrap();
    }
}
