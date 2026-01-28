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

//! MVCC-aware memtable using arena-based skip list.
//!
//! This implementation replaces crossbeam-skiplist with our custom ArenaSkipList
//! to avoid memory fragmentation and provide predictable memory management.
//!
//! ## Key Encoding
//!
//! MVCC keys are encoded as: `key || !commit_ts` (descending order)
//!
//! The bitwise NOT of commit_ts ensures that:
//! - Keys are sorted in descending timestamp order
//! - Scanning from a key finds the latest visible version first
//!
//! ## Tombstones
//!
//! Deletes are represented as tombstone markers (empty value with a flag).
//! This allows MVCC to correctly handle deleted keys at specific versions.
//!
//! ## Memory Management
//!
//! Unlike crossbeam-skiplist which uses epoch-based reclamation, this
//! implementation uses arena allocation. Memory is released in bulk when
//! the memtable is dropped (e.g., after flush to disk).

use std::mem::ManuallyDrop;
use std::ops::Range;

use super::arena_skiplist::ArenaSkipList;
use crate::error::{Result, TiSqlError};
use crate::storage::mvcc::{
    decode_mvcc_key, encode_mvcc_key, increment_bytes, is_tombstone, MvccKey, TOMBSTONE,
};
use crate::storage::{StorageEngine, WriteBatch, WriteOp};
use crate::types::{Key, RawValue, Timestamp};
use crate::util::arena::PageArena;

/// MVCC-aware memtable engine using arena-based skip list.
///
/// This engine stores multiple versions of each key, keyed by MVCC key (`key || !commit_ts`).
/// Reads automatically find the latest visible version.
///
/// # Memory Model
///
/// This struct owns both the arena (memory pool) and the skip list. The skip list
/// holds a reference to the arena, so we must ensure the skip list is dropped before
/// the arena. This is achieved through `ManuallyDrop` and a custom `Drop` impl.
///
/// # Layer Separation
///
/// This is a pure storage layer with NO transaction logic:
/// - NO lock management (handled by ConcurrencyManager in transaction layer)
/// - NO timestamp allocation (handled by TsoService in transaction layer)
/// - NO transaction coordination (handled by TransactionService)
///
/// All writes require explicit `commit_ts` via `write_batch()`.
///
/// # Usage
///
/// For application code, use [`TxnService`](crate::transaction::TxnService):
///
/// ```ignore
/// let ctx = txn_service.begin(true)?;  // read-only transaction
/// let value = txn_service.get(&ctx, key)?;
/// ```
pub struct ArenaMemTableEngine {
    /// The arena that owns all allocated memory.
    /// This is boxed to ensure a stable address for the skip list reference.
    arena: Box<PageArena>,

    /// The skip list storing MVCC-encoded keys.
    ///
    /// Safety: This field uses a 'static lifetime, but the actual lifetime is
    /// tied to `arena`. We use ManuallyDrop to ensure the skip list is dropped
    /// before the arena in our custom Drop implementation.
    ///
    /// Note: ArenaSkipList provides interior mutability via atomic operations,
    /// so no UnsafeCell wrapper is needed.
    list: ManuallyDrop<ArenaSkipList<'static, Key, RawValue>>,
}

// Safety: ArenaMemTableEngine can be sent between threads and shared because:
// 1. PageArena is Send + Sync
// 2. ArenaSkipList is Send + Sync (documented in skiplist.rs)
// 3. All operations on the skip list are lock-free and thread-safe
unsafe impl Send for ArenaMemTableEngine {}
unsafe impl Sync for ArenaMemTableEngine {}

impl ArenaMemTableEngine {
    /// Create a new arena-based MVCC memtable engine.
    ///
    /// The storage engine is a pure key-value store with MVCC versioning.
    /// Lock checking, timestamp allocation, and transaction control are
    /// handled by the transaction layer.
    pub fn new() -> Self {
        // 1. Allocate arena on heap (stable address)
        let arena = Box::new(PageArena::with_defaults());

        // 2. Get raw pointer to arena
        let arena_ptr: *const PageArena = &*arena;

        // 3. Create skip list with 'static lifetime
        // SAFETY: We guarantee the arena outlives the skip list by:
        // - `arena` is Box-allocated (stable heap address that won't move)
        // - `arena` is owned by `Self`, not borrowed
        // - Custom `Drop` impl drops skip list before arena (via ManuallyDrop)
        // - The 'static lifetime is a lie, but safe because the skip list cannot
        //   outlive Self, which owns the arena
        let arena_ref: &'static PageArena = unsafe { &*arena_ptr };
        let list = ArenaSkipList::new(arena_ref);

        Self {
            arena,
            list: ManuallyDrop::new(list),
        }
    }

    /// Create a new arena-based MVCC memtable with custom arena capacity.
    ///
    /// # Arguments
    ///
    /// * `page_size` - Size of each arena page in bytes (default: 256KB)
    /// * `max_pages` - Maximum number of pages (default: 1024)
    pub fn with_capacity(page_size: usize, max_pages: usize) -> Self {
        use crate::util::arena::ArenaConfig;

        let config = ArenaConfig {
            page_size,
            max_pages,
            ..Default::default()
        };
        let arena = Box::new(PageArena::new(config));

        let arena_ptr: *const PageArena = &*arena;
        // SAFETY: Same guarantees as in `new()`:
        // - `arena` is Box-allocated (stable heap address)
        // - Custom `Drop` impl drops skip list before arena
        // - The skip list cannot outlive Self, which owns the arena
        let arena_ref: &'static PageArena = unsafe { &*arena_ptr };
        let list = ArenaSkipList::new(arena_ref);

        Self {
            arena,
            list: ManuallyDrop::new(list),
        }
    }

    /// Get a reference to the skip list.
    #[inline]
    fn list(&self) -> &ArenaSkipList<'static, Key, RawValue> {
        &self.list
    }

    /// Get the latest version of a key visible at the given timestamp.
    ///
    /// This scans from `key || !ts` to find the first version
    /// with commit_ts <= ts.
    pub fn get_at(&self, key: &[u8], ts: Timestamp) -> Result<Option<RawValue>> {
        // Build scan key: key || !ts
        // Due to !ts encoding, this will find the first entry >= key with ts' <= ts
        let start = encode_mvcc_key(key, ts);

        // We need to find keys that start with our key
        let mut end_key = key.to_vec();
        increment_bytes(&mut end_key);

        // Use iter_from to start at the right position
        for (mvcc_key, value) in self.list().iter_from(&start) {
            // Check if we've gone past our key prefix
            if mvcc_key >= &end_key {
                break;
            }

            if let Some((decoded_key, _entry_ts)) = decode_mvcc_key(mvcc_key) {
                // Verify this is still our key (exact match)
                if decoded_key == key {
                    if is_tombstone(value) {
                        return Ok(None);
                    }
                    return Ok(Some(value.clone()));
                }
            }
        }

        Ok(None)
    }

    /// Write a key-value pair at the given timestamp.
    ///
    /// This method is exposed for benchmarking and testing. Production code
    /// should use `write_batch()` instead.
    pub fn put_at(&self, key: &[u8], value: &[u8], ts: Timestamp) {
        let mvcc_key = encode_mvcc_key(key, ts);
        // ArenaSkipList::insert returns Some if key exists, None if inserted
        // For MVCC, each key is unique (includes timestamp), so this should always insert
        let _ = self.list().insert(mvcc_key, value.to_vec());
    }

    /// Write a tombstone (delete marker) at the given timestamp.
    ///
    /// This method is exposed for benchmarking and testing. Production code
    /// should use `write_batch()` instead.
    pub fn delete_at(&self, key: &[u8], ts: Timestamp) {
        let mvcc_key = encode_mvcc_key(key, ts);
        let _ = self.list().insert(mvcc_key, TOMBSTONE.to_vec());
    }

    /// Get the number of entries in the memtable.
    pub fn len(&self) -> usize {
        self.list().len()
    }

    /// Check if the memtable is empty.
    pub fn is_empty(&self) -> bool {
        self.list().is_empty()
    }

    /// Get memory usage statistics.
    pub fn memory_stats(&self) -> MemoryStats {
        MemoryStats {
            allocated_bytes: self.arena.allocated_bytes(),
            entry_count: self.list().len(),
        }
    }
}

impl Default for ArenaMemTableEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ArenaMemTableEngine {
    fn drop(&mut self) {
        // SAFETY: We must drop the skip list before the arena.
        // The skip list holds a reference to the arena, so if we let the
        // default drop order run (arena might be dropped first), we'd have
        // a dangling reference.
        //
        // This is safe because:
        // 1. `ManuallyDrop::drop` requires `&mut self`, which we have
        // 2. We only call this once (in Drop), and ManuallyDrop prevents double-drop
        // 3. After this call, `self.list` is in an uninitialized state, but that's
        //    fine because we're in Drop and won't access it again
        // 4. The arena is still valid at this point and will be dropped after
        unsafe {
            ManuallyDrop::drop(&mut self.list);
        }
        // arena is dropped automatically after this
    }
}

impl ArenaMemTableEngine {
    /// Scan all entries in range, returning MVCC keys.
    ///
    /// Returns ALL versions of each key (not just the latest), including tombstones.
    /// This is critical for correct MVCC semantics in the SST layer.
    ///
    /// # Arguments
    /// * `range` - MVCC key range to scan. Use `MvccKey::unbounded()` for unbounded.
    pub fn scan_mvcc(&self, range: Range<MvccKey>) -> Result<Vec<(MvccKey, RawValue)>> {
        let mut results = Vec::new();

        // Handle truly unbounded range
        let is_unbounded = range.start.is_unbounded() && range.end.is_unbounded();

        if is_unbounded {
            // Iterate entire skiplist without artificial bounds
            for (mvcc_key, value) in self.list().iter() {
                // Safety: keys in skiplist are valid MVCC keys
                let mvcc = MvccKey::from_bytes_unchecked(mvcc_key.clone());
                results.push((mvcc, value.clone()));
            }
        } else {
            // For bounded range, iterate from start and check bounds
            let start_bytes: Vec<u8> = range.start.into();
            let end_bytes: Vec<u8> = range.end.into();

            for (mvcc_key, value) in self.list().iter_from(&start_bytes) {
                // Check if past end of range
                if !end_bytes.is_empty() && mvcc_key >= &end_bytes {
                    break;
                }

                // Include ALL versions (no deduplication) and ALL values including tombstones
                // Safety: keys in skiplist are valid MVCC keys
                let mvcc = MvccKey::from_bytes_unchecked(mvcc_key.clone());
                results.push((mvcc, value.clone()));
            }
        }

        Ok(results)
    }
}

impl StorageEngine for ArenaMemTableEngine {
    fn get_at(&self, key: &[u8], ts: Timestamp) -> Result<Option<RawValue>> {
        ArenaMemTableEngine::get_at(self, key, ts)
    }

    fn scan(&self, range: Range<MvccKey>) -> Result<Vec<(MvccKey, RawValue)>> {
        self.scan_mvcc(range)
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        // commit_ts is required - storage layer doesn't allocate timestamps
        let commit_ts = batch
            .commit_ts()
            .ok_or_else(|| TiSqlError::Storage("WriteBatch must have commit_ts set".to_string()))?;

        for op in batch.into_ops() {
            match op {
                WriteOp::Put { key, value } => {
                    self.put_at(&key, &value, commit_ts);
                }
                WriteOp::Delete { key } => {
                    self.delete_at(&key, commit_ts);
                }
            }
        }

        Ok(())
    }
}

/// Memory usage statistics for the arena memtable.
#[derive(Debug, Clone, Copy)]
pub struct MemoryStats {
    /// Total bytes allocated from the arena
    pub allocated_bytes: u64,
    /// Number of entries in the skip list
    pub entry_count: usize,
}

#[cfg(test)]
#[allow(clippy::uninlined_format_args)]
mod tests {
    use super::*;
    use crate::storage::mvcc::MvccKey;
    use crate::storage::StorageEngine;
    use std::ops::Range;

    fn new_engine() -> ArenaMemTableEngine {
        ArenaMemTableEngine::new()
    }

    // ==================== Test Helpers Using MvccKey ====================

    fn get_at_for_test(
        engine: &ArenaMemTableEngine,
        key: &[u8],
        ts: Timestamp,
    ) -> Option<RawValue> {
        let start = MvccKey::encode(key, ts);
        let end = MvccKey::encode(key, 0)
            .next_key()
            .unwrap_or_else(MvccKey::unbounded);
        let range = start..end;

        let results = engine.scan(range).unwrap();

        for (mvcc_key, value) in results {
            let (decoded_key, entry_ts) = mvcc_key.decode();
            if decoded_key == key && entry_ts <= ts {
                if is_tombstone(&value) {
                    return None;
                }
                return Some(value);
            }
        }
        None
    }

    fn get_for_test(engine: &ArenaMemTableEngine, key: &[u8]) -> Option<RawValue> {
        get_at_for_test(engine, key, Timestamp::MAX)
    }

    fn scan_for_test(engine: &ArenaMemTableEngine, range: &Range<Key>) -> Vec<(Key, RawValue)> {
        scan_at_for_test(engine, range, Timestamp::MAX)
    }

    fn scan_at_for_test(
        engine: &ArenaMemTableEngine,
        range: &Range<Key>,
        ts: Timestamp,
    ) -> Vec<(Key, RawValue)> {
        let start = MvccKey::encode(&range.start, ts);
        let end = MvccKey::encode(&range.end, ts);
        let mvcc_range = start..end;

        let results = engine.scan(mvcc_range).unwrap();

        let mut seen_keys: std::collections::HashSet<Key> = std::collections::HashSet::new();
        let mut output = Vec::new();

        for (mvcc_key, value) in results {
            let (decoded_key, entry_ts) = mvcc_key.decode();
            if decoded_key < range.start || decoded_key >= range.end {
                continue;
            }
            if entry_ts > ts {
                continue;
            }
            if seen_keys.contains(&decoded_key) {
                continue;
            }
            seen_keys.insert(decoded_key.clone());
            if !is_tombstone(&value) {
                output.push((decoded_key, value));
            }
        }

        output.sort_by(|a, b| a.0.cmp(&b.0));
        output
    }

    #[test]
    fn test_encode_decode_mvcc_key() {
        let key = b"test_key";
        let ts: Timestamp = 12345;

        let mvcc_key = encode_mvcc_key(key, ts);
        let (decoded_key, decoded_ts) = decode_mvcc_key(&mvcc_key).unwrap();

        assert_eq!(decoded_key, key.to_vec());
        assert_eq!(decoded_ts, ts);
    }

    #[test]
    fn test_mvcc_key_ordering() {
        // Higher timestamps should come first (smaller encoded value)
        let key = b"key";
        let mvcc_100 = encode_mvcc_key(key, 100);
        let mvcc_50 = encode_mvcc_key(key, 50);
        let mvcc_1 = encode_mvcc_key(key, 1);

        assert!(mvcc_100 < mvcc_50);
        assert!(mvcc_50 < mvcc_1);
    }

    #[test]
    fn test_basic_put_get() {
        let engine = new_engine();

        // Use put_at with explicit timestamp
        engine.put_at(b"key1", b"value1", 1);
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

        engine.put_at(b"key1", b"value1", 1);
        engine.delete_at(b"key1", 2);

        let value = get_for_test(&engine, b"key1");
        assert_eq!(value, None);
    }

    #[test]
    fn test_mvcc_versions() {
        let engine = new_engine();

        // Write version 1
        engine.put_at(b"key", b"v1", 10);

        // Write version 2
        engine.put_at(b"key", b"v2", 20);

        // Write version 3
        engine.put_at(b"key", b"v3", 30);

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
        engine.put_at(b"key", b"value", 10);

        // Delete at ts=20
        engine.delete_at(b"key", 20);

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
    fn test_write_batch_requires_commit_ts() {
        let engine = new_engine();

        // WriteBatch without commit_ts should error
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

    #[test]
    fn test_scan() {
        let engine = new_engine();

        engine.put_at(b"a", b"1", 1);
        engine.put_at(b"b", b"2", 1);
        engine.put_at(b"c", b"3", 1);
        engine.put_at(b"d", b"4", 1);

        let range = b"b".to_vec()..b"d".to_vec();
        let results = scan_for_test(&engine, &range);

        assert_eq!(results.len(), 2);
        let keys: Vec<_> = results.iter().map(|(k, _)| k.clone()).collect();
        assert!(keys.contains(&b"b".to_vec()));
        assert!(keys.contains(&b"c".to_vec()));
    }

    #[test]
    fn test_increment_bytes() {
        let mut v = vec![0u8];
        increment_bytes(&mut v);
        assert_eq!(v, vec![1u8]);

        let mut v = vec![255u8];
        increment_bytes(&mut v);
        assert_eq!(v, vec![1u8, 0u8]);

        let mut v = vec![1u8, 255u8];
        increment_bytes(&mut v);
        assert_eq!(v, vec![2u8, 0u8]);
    }

    #[test]
    fn test_scan_at_mvcc() {
        let engine = new_engine();

        // Write data at explicit timestamps
        engine.put_at(b"a", b"1", 10);
        engine.put_at(b"b", b"2", 20);
        engine.put_at(b"c", b"3", 30);

        // scan_at with ts=30 should see all data
        let range = b"a".to_vec()..b"d".to_vec();
        let results: Vec<_> = scan_at_for_test(&engine, &range, 30);
        assert_eq!(results.len(), 3, "scan_at ts=30 should see 3 keys");

        // scan_at with ts=20 should see a and b only
        let results: Vec<_> = scan_at_for_test(&engine, &range, 20);
        assert_eq!(results.len(), 2, "scan_at ts=20 should see 2 keys");

        // scan_at with ts=10 should see a only
        let results: Vec<_> = scan_at_for_test(&engine, &range, 10);
        assert_eq!(results.len(), 1, "scan_at ts=10 should see 1 key");

        // scan_at with ts=5 should see nothing
        let results: Vec<_> = scan_at_for_test(&engine, &range, 5);
        assert_eq!(results.len(), 0, "scan_at ts=5 should see 0 keys");
    }

    #[test]
    fn test_scan_at_with_updates() {
        let engine = new_engine();

        // Write initial version at ts=10
        engine.put_at(b"key", b"v1", 10);

        // Update at ts=20
        engine.put_at(b"key", b"v2", 20);

        // scan_at ts=15 should see v1
        let range = b"key".to_vec()..b"kez".to_vec();
        let results: Vec<_> = scan_at_for_test(&engine, &range, 15);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, b"v1".to_vec());

        // scan_at ts=25 should see v2 (latest)
        let results: Vec<_> = scan_at_for_test(&engine, &range, 25);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, b"v2".to_vec());
    }

    #[test]
    fn test_scan_at_with_deletes() {
        let engine = new_engine();

        // Write keys at ts=10
        engine.put_at(b"a", b"1", 10);
        engine.put_at(b"b", b"2", 10);
        engine.put_at(b"c", b"3", 10);

        // Delete b at ts=20
        engine.delete_at(b"b", 20);

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

    #[test]
    fn test_mvcc_read_invisible_future_write() {
        // Test that reads don't see future writes (timestamp isolation)
        let engine = new_engine();

        // Writer writes at ts=100
        engine.put_at(b"key", b"future_value", 100);

        // Reader reading at ts=50 should NOT see the write
        let value = get_at_for_test(&engine, b"key", 50);
        assert_eq!(value, None, "Should not see future writes");

        // Reader reading at ts=100 SHOULD see the write
        let value = get_at_for_test(&engine, b"key", 100);
        assert_eq!(value, Some(b"future_value".to_vec()));
    }

    #[test]
    fn test_mvcc_multiple_versions_visibility() {
        let engine = new_engine();

        // Create multiple versions
        engine.put_at(b"key", b"v1", 10);
        engine.put_at(b"key", b"v2", 20);
        engine.put_at(b"key", b"v3", 30);
        engine.delete_at(b"key", 40);
        engine.put_at(b"key", b"v4", 50);

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

    #[test]
    fn test_memory_stats() {
        let engine = new_engine();

        let stats_before = engine.memory_stats();
        assert_eq!(stats_before.entry_count, 0);

        for i in 0..100 {
            engine.put_at(format!("key{:03}", i).as_bytes(), b"value", 1);
        }

        let stats_after = engine.memory_stats();
        assert_eq!(stats_after.entry_count, 100);
        assert!(stats_after.allocated_bytes > stats_before.allocated_bytes);
    }

    // ==================== Concurrent Tests ====================

    use std::sync::Barrier;
    use std::thread;

    #[test]
    fn test_concurrent_writes() {
        let engine = ArenaMemTableEngine::new();
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
                        let key = format!("key_{}_{}", tid, i);
                        let ts = (tid * writes_per_thread + i + 1) as u64;
                        engine.put_at(key.as_bytes(), b"value", ts);
                    }
                });
            }
        });

        // Verify all writes are visible
        for tid in 0..num_threads {
            for i in 0..writes_per_thread {
                let key = format!("key_{}_{}", tid, i);
                assert!(
                    get_for_test(&engine, key.as_bytes()).is_some(),
                    "Missing key {}",
                    key
                );
            }
        }
    }

    #[test]
    fn test_concurrent_reads_and_writes() {
        let engine = ArenaMemTableEngine::new();

        // Pre-populate some data
        for i in 0..1000 {
            engine.put_at(format!("key{:04}", i).as_bytes(), b"initial", 1);
        }

        let num_writers = 4;
        let num_readers = 4;
        let ops_per_thread = 1000;
        let barrier = Barrier::new(num_writers + num_readers);

        thread::scope(|s| {
            // Writer threads - write new versions
            for tid in 0..num_writers {
                let engine = &engine;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    for i in 0..ops_per_thread {
                        let key = format!("key{:04}", i % 1000);
                        let ts = (100 + tid * ops_per_thread + i) as u64;
                        let value = format!("value_{}_{}", tid, i);
                        engine.put_at(key.as_bytes(), value.as_bytes(), ts);
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
                        let key = format!("key{:04}", i % 1000);
                        // Read at timestamp 1 should always see "initial"
                        let value = get_at_for_test(engine, key.as_bytes(), 1);
                        assert_eq!(value, Some(b"initial".to_vec()));
                    }
                });
            }
        });
    }

    #[test]
    fn test_concurrent_scans() {
        let engine = ArenaMemTableEngine::new();

        // Pre-populate data
        for i in 0..100 {
            engine.put_at(format!("key{:03}", i).as_bytes(), b"value", 1);
        }

        let num_threads = 8;
        let scans_per_thread = 100;
        let barrier = Barrier::new(num_threads);

        thread::scope(|s| {
            for _ in 0..num_threads {
                let engine = &engine;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    let range = b"key000".to_vec()..b"key100".to_vec();
                    for _ in 0..scans_per_thread {
                        let results = scan_for_test(engine, &range);
                        assert_eq!(results.len(), 100);
                    }
                });
            }
        });
    }

    #[test]
    fn test_stress_mixed_operations() {
        let engine = ArenaMemTableEngine::new();
        let num_threads = 16;
        let ops_per_thread = 5000;
        let barrier = Barrier::new(num_threads);

        thread::scope(|s| {
            for tid in 0..num_threads {
                let engine = &engine;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    for i in 0..ops_per_thread {
                        let key = format!("stress_{}_{}", tid, i % 100);
                        let ts = (tid * ops_per_thread + i + 1) as u64;

                        match i % 4 {
                            0 | 1 => {
                                // Write
                                engine.put_at(key.as_bytes(), b"value", ts);
                            }
                            2 => {
                                // Read
                                let _ = get_for_test(engine, key.as_bytes());
                            }
                            _ => {
                                // Delete
                                engine.delete_at(key.as_bytes(), ts);
                            }
                        }
                    }
                });
            }
        });

        // Verify the engine is in a valid state by scanning
        let range = b"stress".to_vec()..b"strest".to_vec();
        let results = scan_for_test(&engine, &range);
        println!("Stress test: {} entries remaining", results.len());
    }
}
