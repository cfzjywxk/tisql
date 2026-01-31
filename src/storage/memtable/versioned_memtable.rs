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

use std::ops::Range;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicUsize, Ordering};

use crossbeam_skiplist::SkipMap;

use crate::error::{Result, TiSqlError};
use crate::storage::mvcc::{MvccIterator, MvccKey, TOMBSTONE};
use crate::storage::{StorageEngine, WriteBatch, WriteOp};
use crate::types::{Key, RawValue, Timestamp};

// ============================================================================
// VersionNode - Single version in the chain
// ============================================================================

/// A single version node in the version chain.
///
/// Each node contains a timestamp and value (or tombstone marker).
/// Nodes are linked from newest to oldest via the `next` pointer.
struct VersionNode {
    /// Commit timestamp for this version
    ts: Timestamp,
    /// Value at this version (TOMBSTONE for deletes)
    value: RawValue,
    /// Pointer to the next (older) version, or null if this is the oldest
    next: *mut VersionNode,
}

impl VersionNode {
    /// Create a new version node.
    fn new(ts: Timestamp, value: RawValue) -> Box<Self> {
        Box::new(Self {
            ts,
            value,
            next: std::ptr::null_mut(),
        })
    }

    /// Check if this version is a tombstone (delete marker).
    #[inline]
    #[allow(dead_code)]
    fn is_tombstone(&self) -> bool {
        self.value == TOMBSTONE
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
                let head_ts = unsafe { (*current_head).ts };
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
    #[allow(dead_code)] // Used by tests
    fn get_at(&self, read_ts: Timestamp) -> Option<&RawValue> {
        let mut current = self.head.load(Ordering::Acquire);

        // Safety: We hold a reference to MvccRow, and nodes are never deallocated
        // while the row exists (deallocation happens when the memtable is dropped).
        while !current.is_null() {
            let node = unsafe { &*current };
            if node.ts <= read_ts {
                return Some(&node.value);
            }
            current = node.next;
        }

        None
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
pub struct VersionedMemTableEngine {
    /// Skiplist mapping user keys to MVCC rows
    index: SkipMap<Key, MvccRow>,
    /// Entry count (number of versions across all keys)
    entry_count: AtomicUsize,
}

impl VersionedMemTableEngine {
    /// Create a new versioned memtable engine.
    pub fn new() -> Self {
        Self {
            index: SkipMap::new(),
            entry_count: AtomicUsize::new(0),
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
        let entry = self.index.get_or_insert(key.to_vec(), MvccRow::new_empty());

        // Prepend version to the row (works for both new and existing rows)
        entry.value().prepend(ts, value);

        self.entry_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the number of version entries in the memtable.
    pub fn len(&self) -> usize {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Check if the memtable is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the number of unique keys in the memtable.
    pub fn key_count(&self) -> usize {
        self.index.len()
    }

    /// Get memory usage statistics.
    pub fn memory_stats(&self) -> VersionedMemoryStats {
        VersionedMemoryStats {
            entry_count: self.len(),
            key_count: self.key_count(),
        }
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
    pub fn create_streaming_iter(&self, range: Range<MvccKey>) -> VersionedMemTableIterator<'_> {
        VersionedMemTableIterator::new(self, range)
    }
}

impl StorageEngine for VersionedMemTableEngine {
    /// Create a streaming iterator over MVCC keys in range.
    ///
    /// This is a true streaming iterator - no materialization occurs.
    /// Iteration happens directly over the skiplist and version chains.
    fn scan_iter(&self, range: Range<MvccKey>) -> Result<Box<dyn MvccIterator + '_>> {
        let mut iter = VersionedMemTableIterator::new(self, range);
        // Initialize iterator to position on first entry
        iter.next()?;
        Ok(Box::new(iter))
    }

    /// Apply a batch of writes atomically.
    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
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
// VersionedMemTableIterator - True streaming iterator for versioned memtable
// ============================================================================

/// True streaming iterator over the versioned memtable (no materialization).
///
/// This iterator provides efficient MVCC key iteration by:
/// - Directly traversing the skiplist (user keys in ascending order)
/// - Traversing version chains for each user key (timestamps descending)
/// - Filtering by MVCC range bounds during iteration
/// - Using raw pointers to navigate version chains (safe because memtable keeps data alive)
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
/// 1. The iterator holds a reference to the `VersionedMemTableEngine`
/// 2. Version nodes are never deallocated while the engine exists
/// 3. The version chain is prepend-only (no concurrent modification of existing nodes)
pub struct VersionedMemTableIterator<'a> {
    /// Reference to the memtable engine
    engine: &'a VersionedMemTableEngine,
    /// Start of MVCC key range (inclusive)
    range_start: MvccKey,
    /// End of MVCC key range (exclusive)
    range_end: MvccKey,
    /// Whether the iterator has been initialized
    initialized: bool,
    /// Current user key being iterated (None if exhausted)
    current_user_key: Option<Vec<u8>>,
    /// Current position in version chain (null if at end of chain)
    current_version: *const VersionNode,
    /// Cached current MVCC key (for returning reference)
    cached_key: Option<MvccKey>,
    /// Cached current value (for returning reference)
    cached_value: Option<Vec<u8>>,
}

// Safety: The iterator only holds references to data owned by the memtable.
// The memtable guarantees that version nodes are not deallocated while it exists.
unsafe impl<'a> Send for VersionedMemTableIterator<'a> {}
unsafe impl<'a> Sync for VersionedMemTableIterator<'a> {}

impl<'a> VersionedMemTableIterator<'a> {
    /// Create a new iterator over the given range.
    ///
    /// The iterator is created in an uninitialized state. Call `next()` to position
    /// on the first entry.
    pub fn new(engine: &'a VersionedMemTableEngine, range: Range<MvccKey>) -> Self {
        Self {
            engine,
            range_start: range.start,
            range_end: range.end,
            initialized: false,
            current_user_key: None,
            current_version: std::ptr::null(),
            cached_key: None,
            cached_value: None,
        }
    }

    /// Initialize the iterator by seeking to the start of the range.
    fn initialize(&mut self) {
        if self.initialized {
            return;
        }
        self.initialized = true;

        // Determine the starting user key
        let start_user_key = if self.range_start.is_unbounded() {
            Vec::new()
        } else {
            self.range_start.key().to_vec()
        };
        let start_ts = if self.range_start.is_unbounded() {
            Timestamp::MAX
        } else {
            self.range_start.timestamp()
        };

        // Find the first user key >= start_user_key
        for entry in self.engine.index.range(start_user_key.clone()..) {
            let user_key = entry.key();

            // Check if past end of range
            if !self.range_end.is_unbounded() {
                let end_user_key = self.range_end.key();
                if user_key.as_slice() > end_user_key {
                    break;
                }
            }

            // Get the row's version chain
            let row = entry.value();
            let head = row.head.load(Ordering::Acquire);

            if head.is_null() {
                continue; // Empty row, skip
            }

            // If this is the exact start user key, find the right version
            if user_key.as_slice() == start_user_key.as_slice() {
                // Find first version with ts <= start_ts
                let mut version_ptr = head;
                while !version_ptr.is_null() {
                    let node = unsafe { &*version_ptr };
                    if node.ts <= start_ts && self.is_version_in_range(user_key, node.ts) {
                        self.current_user_key = Some(user_key.clone());
                        self.current_version = version_ptr;
                        self.cache_current();
                        return;
                    }
                    version_ptr = node.next;
                }
                // No valid version in this key, continue to next key
            } else {
                // This is a key after start_user_key, start from head (newest version)
                let mut version_ptr = head;
                while !version_ptr.is_null() {
                    let node = unsafe { &*version_ptr };
                    if self.is_version_in_range(user_key, node.ts) {
                        self.current_user_key = Some(user_key.clone());
                        self.current_version = version_ptr;
                        self.cache_current();
                        return;
                    }
                    version_ptr = node.next;
                }
            }
        }

        // No entry found
        self.current_user_key = None;
        self.current_version = std::ptr::null();
        self.cached_key = None;
        self.cached_value = None;
    }

    /// Check if a (user_key, ts) pair is within the MVCC key range.
    fn is_version_in_range(&self, user_key: &[u8], ts: Timestamp) -> bool {
        // Check start bound (inclusive)
        if !self.range_start.is_unbounded() {
            let start_user_key = self.range_start.key();
            let start_ts = self.range_start.timestamp();

            if user_key < start_user_key {
                return false;
            }
            if user_key == start_user_key && ts > start_ts {
                return false; // ts > start_ts means MVCC key < range_start
            }
        }

        // Check end bound (exclusive)
        if !self.range_end.is_unbounded() {
            let end_user_key = self.range_end.key();
            let end_ts = self.range_end.timestamp();

            if user_key > end_user_key {
                return false;
            }
            if user_key == end_user_key && ts <= end_ts {
                return false; // ts <= end_ts means MVCC key >= range_end
            }
        }

        true
    }

    /// Cache the current entry's key and value.
    fn cache_current(&mut self) {
        if let Some(ref user_key) = self.current_user_key {
            if !self.current_version.is_null() {
                let node = unsafe { &*self.current_version };
                self.cached_key = Some(MvccKey::encode(user_key, node.ts));
                self.cached_value = Some(node.value.clone());
            }
        }
    }

    /// Advance to the next valid entry.
    fn advance(&mut self) {
        if self.current_user_key.is_none() {
            return; // Already exhausted
        }

        let current_key = self.current_user_key.clone().unwrap();

        // Try to advance within the current version chain
        if !self.current_version.is_null() {
            let node = unsafe { &*self.current_version };
            let mut next_version = node.next;

            // Find next version in range
            while !next_version.is_null() {
                let next_node = unsafe { &*next_version };
                if self.is_version_in_range(&current_key, next_node.ts) {
                    self.current_version = next_version;
                    self.cache_current();
                    return;
                }
                next_version = next_node.next;
            }
        }

        // Version chain exhausted, move to next user key
        self.move_to_next_user_key(&current_key);
    }

    /// Move to the next user key after the given key.
    fn move_to_next_user_key(&mut self, current_key: &[u8]) {
        // Find next user key after current_key
        let mut next_key_bytes = current_key.to_vec();
        // Increment to get exclusive start
        increment_bytes(&mut next_key_bytes);

        for entry in self.engine.index.range(next_key_bytes..) {
            let user_key = entry.key();

            // Check if past end of range
            if !self.range_end.is_unbounded() {
                let end_user_key = self.range_end.key();
                if user_key.as_slice() > end_user_key {
                    break;
                }
            }

            let row = entry.value();
            let head = row.head.load(Ordering::Acquire);

            if head.is_null() {
                continue;
            }

            // Find first version in range
            let mut version_ptr = head;
            while !version_ptr.is_null() {
                let node = unsafe { &*version_ptr };
                if self.is_version_in_range(user_key, node.ts) {
                    self.current_user_key = Some(user_key.clone());
                    self.current_version = version_ptr;
                    self.cache_current();
                    return;
                }
                version_ptr = node.next;
            }
        }

        // No more entries
        self.current_user_key = None;
        self.current_version = std::ptr::null();
        self.cached_key = None;
        self.cached_value = None;
    }
}

/// Increment a byte array to get the next key (for exclusive range start).
///
/// For keys like `[0xFF; 8]`, we append a zero byte to get `[0xFF; 8, 0x00]`
/// which is lexicographically greater. This ensures we skip past the current
/// key when iterating.
fn increment_bytes(bytes: &mut Vec<u8>) {
    if bytes.is_empty() {
        bytes.push(0);
        return;
    }

    for i in (0..bytes.len()).rev() {
        if bytes[i] < 255 {
            bytes[i] += 1;
            return;
        }
        bytes[i] = 0;
    }
    // All bytes were 255, append a zero byte to get the next key.
    // Example: [0xFF, 0xFF] -> [0xFF, 0xFF, 0x00]
    // This is lexicographically greater because the original key is a prefix.
    // Reset the bytes to their original values first.
    for byte in bytes.iter_mut() {
        *byte = 0xFF;
    }
    bytes.push(0);
}

impl<'a> MvccIterator for VersionedMemTableIterator<'a> {
    fn seek(&mut self, target: &MvccKey) -> Result<()> {
        // Reset state and seek to target
        self.initialized = true;
        self.current_user_key = None;
        self.current_version = std::ptr::null();
        self.cached_key = None;
        self.cached_value = None;

        // Handle unbounded target - seek to first entry within range
        if target.is_unbounded() {
            self.initialized = false;
            self.initialize();
            return Ok(());
        }

        let target_user_key = target.key();
        let target_ts = target.timestamp();

        for entry in self.engine.index.range(target_user_key.to_vec()..) {
            let user_key = entry.key();

            // Check if past end of range
            if !self.range_end.is_unbounded() {
                let end_user_key = self.range_end.key();
                if user_key.as_slice() > end_user_key {
                    break;
                }
            }

            let row = entry.value();
            let head = row.head.load(Ordering::Acquire);

            if head.is_null() {
                continue;
            }

            // If this is the target user key, find version with ts <= target_ts
            if user_key.as_slice() == target_user_key {
                let mut version_ptr = head;
                while !version_ptr.is_null() {
                    let node = unsafe { &*version_ptr };
                    if node.ts <= target_ts && self.is_version_in_range(user_key, node.ts) {
                        self.current_user_key = Some(user_key.clone());
                        self.current_version = version_ptr;
                        self.cache_current();
                        return Ok(());
                    }
                    version_ptr = node.next;
                }
                // No valid version at target key, continue to next key
            } else {
                // This is a key after target, start from head
                let mut version_ptr = head;
                while !version_ptr.is_null() {
                    let node = unsafe { &*version_ptr };
                    if self.is_version_in_range(user_key, node.ts) {
                        self.current_user_key = Some(user_key.clone());
                        self.current_version = version_ptr;
                        self.cache_current();
                        return Ok(());
                    }
                    version_ptr = node.next;
                }
            }
        }

        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        if !self.initialized {
            self.initialize();
        } else {
            self.advance();
        }
        Ok(())
    }

    fn valid(&self) -> bool {
        self.initialized && self.current_user_key.is_some() && self.cached_key.is_some()
    }

    fn key(&self) -> &MvccKey {
        self.cached_key.as_ref().expect("Iterator not valid")
    }

    fn value(&self) -> &[u8] {
        self.cached_value.as_ref().expect("Iterator not valid")
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
    fn scan_mvcc(
        engine: &VersionedMemTableEngine,
        range: Range<MvccKey>,
    ) -> Vec<(MvccKey, RawValue)> {
        let mut results = Vec::new();
        let mut iter = engine.create_streaming_iter(range);
        iter.next().unwrap();
        while iter.valid() {
            results.push((iter.key().clone(), iter.value().to_vec()));
            iter.next().unwrap();
        }
        results
    }

    /// Scan all versions for a range of user keys (test-only).
    fn scan_all_versions(
        engine: &VersionedMemTableEngine,
        range: &Range<Key>,
    ) -> Vec<(Key, Timestamp, RawValue)> {
        let mut results = Vec::new();

        for entry in engine.index.range(range.start.clone()..range.end.clone()) {
            let key = entry.key();
            let row = entry.value();

            // Traverse version chain directly (newest first)
            let mut current = row.head.load(std::sync::atomic::Ordering::Acquire);
            while !current.is_null() {
                // Safety: current points to a valid node owned by MvccRow
                let node = unsafe { &*current };
                results.push((key.clone(), node.ts, node.value.clone()));
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
        if let Some(entry) = engine.index.get(key) {
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

        for entry in engine.index.range(range.start.clone()..range.end.clone()) {
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

    #[test]
    fn test_basic_put_get() {
        let engine = new_engine();

        put_at(&engine, b"key1", b"value1", 1);
        let value = get_for_test(&engine, b"key1");

        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_get_nonexistent() {
        let engine = new_engine();

        let value = get_for_test(&engine, b"nonexistent");
        assert_eq!(value, None);
    }

    #[test]
    fn test_delete() {
        let engine = new_engine();

        put_at(&engine, b"key1", b"value1", 1);
        delete_at(&engine, b"key1", 2);

        let value = get_for_test(&engine, b"key1");
        assert_eq!(value, None);
    }

    // ==================== MVCC Version Tests ====================

    #[test]
    fn test_mvcc_versions() {
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

    #[test]
    fn test_mvcc_delete_version() {
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

    #[test]
    fn test_mvcc_multiple_versions_visibility() {
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

    #[test]
    fn test_scan() {
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

    #[test]
    fn test_scan_at_mvcc() {
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

    #[test]
    fn test_scan_at_with_deletes() {
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

    #[test]
    fn test_write_batch_requires_commit_ts() {
        let engine = new_engine();

        let mut batch = WriteBatch::new();
        batch.put(b"k1".to_vec(), b"v1".to_vec());

        let result = engine.write_batch(batch);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_batch_with_commit_ts() {
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

    #[test]
    fn test_storage_engine_scan_unbounded() {
        let engine = new_engine();

        put_at(&engine, b"a", b"1", 10);
        put_at(&engine, b"a", b"2", 20); // Second version
        put_at(&engine, b"b", b"3", 15);

        // Scan all with unbounded range
        let results = scan_mvcc(&engine, MvccKey::unbounded()..MvccKey::unbounded());

        // Should have 3 entries (2 for "a", 1 for "b")
        assert_eq!(results.len(), 3);

        // Verify MVCC keys are properly encoded
        let (key_a_20, _) = results[0].0.decode();
        assert_eq!(key_a_20, b"a".to_vec());
    }

    #[test]
    fn test_storage_engine_scan_bounded() {
        let engine = new_engine();

        put_at(&engine, b"a", b"1", 10);
        put_at(&engine, b"b", b"2", 20);
        put_at(&engine, b"c", b"3", 30);

        // Scan with MVCC key range
        // MvccKey::encode(b"c", 0) = c || !0 = c || 0xFF...FF
        // Since c at ts=30 encodes to c || !30 < c || !0, it's included
        let start = MvccKey::encode(b"a", Timestamp::MAX);
        let end = MvccKey::encode(b"c", 0);

        let results = scan_mvcc(&engine, start..end);

        // All three entries are included:
        // - (a, 10): a || !10 is in range
        // - (b, 20): b || !20 is in range
        // - (c, 30): c || !30 < c || !0, so it's in range
        assert_eq!(results.len(), 3);

        // To exclude key "c" entirely, use the next key after "b"
        let start = MvccKey::encode(b"a", Timestamp::MAX);
        let end = MvccKey::encode(b"c", Timestamp::MAX); // Start of "c" versions

        let results = scan_mvcc(&engine, start..end);
        // Now only a and b are included
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_storage_engine_scan_same_key_timestamp_range() {
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

        let results = scan_mvcc(&engine, start..end);

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

    #[test]
    fn test_concurrent_writes() {
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
                        put_at(&engine, key.as_bytes(), b"value", ts);
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

    #[test]
    fn test_concurrent_writes_same_key() {
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
                        put_at(&engine, b"hotkey", value.as_bytes(), ts);
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

    #[test]
    fn test_concurrent_reads_and_writes() {
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
                        put_at(&engine, key.as_bytes(), value.as_bytes(), ts);
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

    #[test]
    fn test_memory_stats() {
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

    #[test]
    fn test_scan_all_versions() {
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

    #[test]
    fn test_version_chain_ordering_correct() {
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

    #[test]
    fn test_version_chain_ordering_same_timestamp() {
        // Test that inserting at the same timestamp is allowed (idempotent writes)
        let engine = new_engine();

        put_at(&engine, b"key", b"v1", 10);
        put_at(&engine, b"key", b"v2", 10); // Same ts, different value

        // Should return the latest inserted value at ts=10
        let value = get_at_for_test(&engine, b"key", 10);
        assert_eq!(value, Some(b"v2".to_vec()));
    }

    #[test]
    #[should_panic(expected = "MVCC version chain ordering violation")]
    fn test_version_chain_ordering_violation_panics() {
        // Inserting a version with ts < head's ts must panic to prevent silent corruption.
        // This catches bugs where the transaction layer violates the ordering invariant.
        let engine = new_engine();

        // Insert version at ts=20 first
        put_at(&engine, b"key", b"v20", 20);

        // Try to insert version at ts=10 (WRONG: ts < head's ts)
        // This must panic in ALL builds (not just debug) to prevent silent data corruption
        put_at(&engine, b"key", b"v10", 10);
    }
}
