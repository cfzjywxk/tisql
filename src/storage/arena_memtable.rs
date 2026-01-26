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
//! MVCC keys are encoded as: `user_key || !commit_ts` (descending order)
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

use super::skiplist::ArenaSkipList;
use super::{StorageEngine, WriteBatch, WriteOp};
use crate::error::{Result, TiSqlError};
use crate::types::{Key, RawValue, Timestamp};
use crate::util::arena::PageArena;

/// Tombstone marker for deleted keys.
///
/// We use a byte sequence that starts with NUL bytes (uncommon in user data)
/// followed by a magic string and a version byte, making collision extremely unlikely.
/// The probability of collision is ~2^-120 for random 16-byte values.
const TOMBSTONE: &[u8] = b"\x00\x00\x00\x00TISQL_TOMBSTONE\x01";

/// Encode MVCC key: user_key || !commit_ts (8 bytes, big-endian)
///
/// The bitwise NOT ensures descending order: higher timestamps come first.
fn encode_mvcc_key(user_key: &[u8], ts: Timestamp) -> Key {
    let mut mvcc_key = Vec::with_capacity(user_key.len() + 8);
    mvcc_key.extend_from_slice(user_key);
    // Bitwise NOT for descending order
    mvcc_key.extend_from_slice(&(!ts).to_be_bytes());
    mvcc_key
}

/// Decode MVCC key to (user_key, commit_ts).
fn decode_mvcc_key(mvcc_key: &[u8]) -> Option<(Key, Timestamp)> {
    if mvcc_key.len() < 8 {
        return None;
    }
    let user_key = mvcc_key[..mvcc_key.len() - 8].to_vec();
    let ts_bytes: [u8; 8] = mvcc_key[mvcc_key.len() - 8..].try_into().ok()?;
    // Reverse the bitwise NOT
    let ts = !u64::from_be_bytes(ts_bytes);
    Some((user_key, ts))
}

/// Check if a value is a tombstone.
fn is_tombstone(value: &[u8]) -> bool {
    value == TOMBSTONE
}

/// Increment byte array by 1 (for range end bound).
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
    // All bytes were 255, add a new byte
    bytes.insert(0, 1);
}

/// MVCC-aware memtable engine using arena-based skip list.
///
/// This engine stores multiple versions of each key, keyed by `user_key || !commit_ts`.
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
        // Safety: We guarantee the arena outlives the skip list by:
        // - arena is heap-allocated (stable address)
        // - Custom Drop impl drops skip list before arena
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
    /// This scans from `user_key || !ts` to find the first version
    /// with commit_ts <= ts.
    fn get_at_internal(&self, user_key: &[u8], ts: Timestamp) -> Result<Option<RawValue>> {
        // Build scan key: user_key || !ts
        // Due to !ts encoding, this will find the first key >= user_key with ts' <= ts
        let start = encode_mvcc_key(user_key, ts);

        // We need to find keys that start with user_key
        let mut end_key = user_key.to_vec();
        increment_bytes(&mut end_key);

        // Use iter_from to start at the right position
        for (mvcc_key, value) in self.list().iter_from(&start) {
            // Check if we've gone past our user_key prefix
            if mvcc_key >= &end_key {
                break;
            }

            if let Some((key, _entry_ts)) = decode_mvcc_key(mvcc_key) {
                // Verify this is still our key (prefix match)
                if key == user_key {
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
    /// This is an internal method used by `write_batch()`.
    pub(crate) fn put_at(&self, user_key: &[u8], value: &[u8], ts: Timestamp) {
        let mvcc_key = encode_mvcc_key(user_key, ts);
        // ArenaSkipList::insert returns Some if key exists, None if inserted
        // For MVCC, each key is unique (includes timestamp), so this should always insert
        let _ = self.list().insert(mvcc_key, value.to_vec());
    }

    /// Write a tombstone (delete marker) at the given timestamp.
    ///
    /// This is an internal method used by `write_batch()`.
    pub(crate) fn delete_at(&self, user_key: &[u8], ts: Timestamp) {
        let mvcc_key = encode_mvcc_key(user_key, ts);
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
        // Safety: We must drop the skip list before the arena.
        // The skip list holds a reference to the arena, so if we let the
        // default drop order run (arena might be dropped first), we'd have
        // a dangling reference.
        //
        // ManuallyDrop::drop takes &mut self, so this is safe.
        unsafe {
            ManuallyDrop::drop(&mut self.list);
        }
        // arena is dropped automatically after this
    }
}

impl StorageEngine for ArenaMemTableEngine {
    fn get(&self, key: &[u8]) -> Result<Option<RawValue>> {
        // Get at latest timestamp (Timestamp::MAX means get the latest version)
        self.get_at(key, Timestamp::MAX)
    }

    fn get_at(&self, key: &[u8], ts: Timestamp) -> Result<Option<RawValue>> {
        // Find latest version <= ts
        self.get_at_internal(key, ts)
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

    fn scan(&self, range: &Range<Key>) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>> {
        // Default scan uses MAX timestamp (latest visible version)
        self.scan_at(range, Timestamp::MAX)
    }

    fn scan_at(
        &self,
        range: &Range<Key>,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>> {
        // Scan MVCC keys and deduplicate by user_key
        //
        // MVCC key encoding: user_key || !commit_ts
        // Due to !commit_ts, higher timestamps produce SMALLER encoded keys.
        //
        // To find all versions within the user key range:
        // - start: use Timestamp::MAX to get the SMALLEST mvcc key for range.start
        // - end: use 0 to get the LARGEST mvcc key just before range.end
        //
        // Then filter by entry_ts <= ts during iteration for visibility.
        let start_mvcc = encode_mvcc_key(&range.start, Timestamp::MAX);
        let end_mvcc = encode_mvcc_key(&range.end, 0);

        let mut results = Vec::new();
        let mut last_user_key: Option<Key> = None;

        for (mvcc_key, value) in self.list().iter_from(&start_mvcc) {
            // Stop when we reach the end bound
            if mvcc_key >= &end_mvcc {
                break;
            }

            if let Some((user_key, entry_ts)) = decode_mvcc_key(mvcc_key) {
                // Check if within user range
                if user_key < range.start || user_key >= range.end {
                    continue;
                }

                // Skip if we already have this key (we want the latest visible version)
                if let Some(ref last) = last_user_key {
                    if &user_key == last {
                        continue;
                    }
                }

                // Check if this version is visible at the given timestamp
                if entry_ts <= ts {
                    if !is_tombstone(value) {
                        results.push((user_key.clone(), value.clone()));
                    }
                    last_user_key = Some(user_key);
                }
            }
        }

        Ok(Box::new(results.into_iter()))
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

    fn new_engine() -> ArenaMemTableEngine {
        ArenaMemTableEngine::new()
    }

    #[test]
    fn test_encode_decode_mvcc_key() {
        let user_key = b"test_key";
        let ts: Timestamp = 12345;

        let mvcc_key = encode_mvcc_key(user_key, ts);
        let (decoded_key, decoded_ts) = decode_mvcc_key(&mvcc_key).unwrap();

        assert_eq!(decoded_key, user_key.to_vec());
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
        let value = engine.get(b"key1").unwrap();

        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_get_nonexistent() {
        let engine = new_engine();

        let value = engine.get(b"nonexistent").unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_delete() {
        let engine = new_engine();

        engine.put_at(b"key1", b"value1", 1);
        engine.delete_at(b"key1", 2);

        let value = engine.get(b"key1").unwrap();
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
        let v = engine.get_at(b"key", 10).unwrap();
        assert_eq!(v, Some(b"v1".to_vec()));

        // Read at ts=20 should see v2
        let v = engine.get_at(b"key", 20).unwrap();
        assert_eq!(v, Some(b"v2".to_vec()));

        // Read at ts=30 should see v3
        let v = engine.get_at(b"key", 30).unwrap();
        assert_eq!(v, Some(b"v3".to_vec()));

        // Read at latest should see v3
        let v = engine.get_at(b"key", Timestamp::MAX).unwrap();
        assert_eq!(v, Some(b"v3".to_vec()));

        // Read at ts before any write should see nothing
        let v = engine.get_at(b"key", 5).unwrap();
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
        let v = engine.get_at(b"key", 10).unwrap();
        assert_eq!(v, Some(b"value".to_vec()));

        // Read at ts=15 should still see value
        let v = engine.get_at(b"key", 15).unwrap();
        assert_eq!(v, Some(b"value".to_vec()));

        // Read at ts=20 should see nothing (deleted)
        let v = engine.get_at(b"key", 20).unwrap();
        assert_eq!(v, None);

        // Read at latest should see nothing
        let v = engine.get_at(b"key", Timestamp::MAX).unwrap();
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
        assert_eq!(engine.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(engine.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(engine.get(b"k3").unwrap(), Some(b"v3".to_vec()));

        // Should be visible at ts >= 100
        let v = engine.get_at(b"k1", 100).unwrap();
        assert_eq!(v, Some(b"v1".to_vec()));

        // Should not be visible at ts < 100
        let v = engine.get_at(b"k1", 99).unwrap();
        assert_eq!(v, None);
    }

    #[test]
    fn test_scan() {
        let engine = new_engine();

        engine.put_at(b"a", b"1", 1);
        engine.put_at(b"b", b"2", 1);
        engine.put_at(b"c", b"3", 1);
        engine.put_at(b"d", b"4", 1);

        let results: Vec<_> = engine
            .scan(&(b"b".to_vec()..b"d".to_vec()))
            .unwrap()
            .collect();

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
        let results: Vec<_> = engine.scan_at(&range, 30).unwrap().collect();
        assert_eq!(results.len(), 3, "scan_at ts=30 should see 3 keys");

        // scan_at with ts=20 should see a and b only
        let results: Vec<_> = engine.scan_at(&range, 20).unwrap().collect();
        assert_eq!(results.len(), 2, "scan_at ts=20 should see 2 keys");

        // scan_at with ts=10 should see a only
        let results: Vec<_> = engine.scan_at(&range, 10).unwrap().collect();
        assert_eq!(results.len(), 1, "scan_at ts=10 should see 1 key");

        // scan_at with ts=5 should see nothing
        let results: Vec<_> = engine.scan_at(&range, 5).unwrap().collect();
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
        let results: Vec<_> = engine.scan_at(&range, 15).unwrap().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, b"v1".to_vec());

        // scan_at ts=25 should see v2 (latest)
        let results: Vec<_> = engine.scan_at(&range, 25).unwrap().collect();
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
        let results: Vec<_> = engine.scan_at(&range, 15).unwrap().collect();
        assert_eq!(results.len(), 3, "ts=15 should see 3 keys (before delete)");

        // scan_at ts=25 should see only a and c
        let results: Vec<_> = engine.scan_at(&range, 25).unwrap().collect();
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
        let value = engine.get_at(b"key", 50).unwrap();
        assert_eq!(value, None, "Should not see future writes");

        // Reader reading at ts=100 SHOULD see the write
        let value = engine.get_at(b"key", 100).unwrap();
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
        assert_eq!(engine.get_at(b"key", 5).unwrap(), None);
        assert_eq!(engine.get_at(b"key", 10).unwrap(), Some(b"v1".to_vec()));
        assert_eq!(engine.get_at(b"key", 15).unwrap(), Some(b"v1".to_vec()));
        assert_eq!(engine.get_at(b"key", 20).unwrap(), Some(b"v2".to_vec()));
        assert_eq!(engine.get_at(b"key", 25).unwrap(), Some(b"v2".to_vec()));
        assert_eq!(engine.get_at(b"key", 30).unwrap(), Some(b"v3".to_vec()));
        assert_eq!(engine.get_at(b"key", 35).unwrap(), Some(b"v3".to_vec()));
        assert_eq!(engine.get_at(b"key", 40).unwrap(), None); // deleted
        assert_eq!(engine.get_at(b"key", 45).unwrap(), None); // still deleted
        assert_eq!(engine.get_at(b"key", 50).unwrap(), Some(b"v4".to_vec())); // rewritten
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
                    engine.get(key.as_bytes()).unwrap().is_some(),
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
                        let value = engine.get_at(key.as_bytes(), 1).unwrap();
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
                        let results: Vec<_> = engine.scan(&range).unwrap().collect();
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
                                let _ = engine.get(key.as_bytes());
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
        let results: Vec<_> = engine
            .scan(&(b"stress".to_vec()..b"strest".to_vec()))
            .unwrap()
            .collect();
        println!("Stress test: {} entries remaining", results.len());
    }
}
