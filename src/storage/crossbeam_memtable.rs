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

//! MVCC-aware memtable using crossbeam-skiplist.
//!
//! This implementation uses crossbeam's lock-free skip list with epoch-based
//! memory reclamation. It provides the same MVCC semantics as ArenaMemTableEngine
//! but with different memory management characteristics:
//!
//! - **Pros**: Better write throughput due to smaller node size and epoch-based GC
//! - **Cons**: Memory is reclaimed lazily through epochs, not in bulk
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
//! ## Known Issues
//!
//! The crossbeam-skiplist crate has a known memory leak in iterator operations
//! (see https://github.com/crossbeam-rs/crossbeam/pull/1217). This implementation
//! minimizes iterator usage patterns that trigger the leak.

use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_skiplist::SkipMap;

use super::{StorageEngine, WriteBatch, WriteOp};
use crate::error::{Result, TiSqlError};
use crate::types::{Key, RawValue, Timestamp};

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

/// MVCC-aware memtable engine using crossbeam-skiplist.
///
/// This engine stores multiple versions of each key, keyed by `user_key || !commit_ts`.
/// Reads automatically find the latest visible version.
///
/// # Memory Model
///
/// Unlike ArenaMemTableEngine which uses arena allocation with bulk deallocation,
/// this implementation uses crossbeam's epoch-based garbage collection. Memory is
/// reclaimed lazily when no threads are accessing old data.
///
/// # Layer Separation
///
/// This is a pure storage layer with NO transaction logic:
/// - NO lock management (handled by ConcurrencyManager in transaction layer)
/// - NO timestamp allocation (handled by TsoService in transaction layer)
/// - NO transaction coordination (handled by TransactionService)
///
/// All writes require explicit `commit_ts` via `write_batch()`.
pub struct CrossbeamMemTableEngine {
    /// The skip list storing MVCC-encoded keys.
    list: SkipMap<Key, RawValue>,
    /// Entry count (tracked separately for O(1) len())
    entry_count: AtomicUsize,
}

impl CrossbeamMemTableEngine {
    /// Create a new crossbeam-based MVCC memtable engine.
    ///
    /// The storage engine is a pure key-value store with MVCC versioning.
    /// Lock checking, timestamp allocation, and transaction control are
    /// handled by the transaction layer.
    pub fn new() -> Self {
        Self {
            list: SkipMap::new(),
            entry_count: AtomicUsize::new(0),
        }
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

        // Use range to iterate from start position
        for entry in self.list.range(start..) {
            let mvcc_key = entry.key();

            // Check if we've gone past our user_key prefix
            if mvcc_key >= &end_key {
                break;
            }

            if let Some((key, _entry_ts)) = decode_mvcc_key(mvcc_key) {
                // Verify this is still our key (prefix match)
                if key == user_key {
                    let value = entry.value();
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
    pub fn put_at(&self, user_key: &[u8], value: &[u8], ts: Timestamp) {
        let mvcc_key = encode_mvcc_key(user_key, ts);
        // SkipMap::insert returns the entry (we ignore it)
        // For MVCC, each key is unique (includes timestamp), so this should always insert
        self.list.insert(mvcc_key, value.to_vec());
        self.entry_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Write a tombstone (delete marker) at the given timestamp.
    ///
    /// This method is exposed for benchmarking and testing. Production code
    /// should use `write_batch()` instead.
    pub fn delete_at(&self, user_key: &[u8], ts: Timestamp) {
        let mvcc_key = encode_mvcc_key(user_key, ts);
        self.list.insert(mvcc_key, TOMBSTONE.to_vec());
        self.entry_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the number of entries in the memtable.
    pub fn len(&self) -> usize {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Check if the memtable is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get memory usage statistics.
    ///
    /// Note: Unlike ArenaMemTableEngine, we cannot accurately report allocated bytes
    /// because crossbeam-skiplist manages its own memory internally.
    pub fn memory_stats(&self) -> MemoryStats {
        MemoryStats {
            // Estimate: each entry uses ~40 bytes for node overhead + key/value size
            // This is a rough approximation since we don't have access to actual allocation
            estimated_bytes: 0, // Cannot determine accurately
            entry_count: self.len(),
        }
    }
}

impl Default for CrossbeamMemTableEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageEngine for CrossbeamMemTableEngine {
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
        // - end: use Timestamp::MAX to get the SMALLEST mvcc key for range.end
        //   (since range.end is exclusive, we stop right at the first version of range.end)
        //
        // Then filter by entry_ts <= ts during iteration for visibility.
        let start_mvcc = encode_mvcc_key(&range.start, Timestamp::MAX);
        let end_mvcc = encode_mvcc_key(&range.end, Timestamp::MAX);

        // Collect results to avoid holding iterator references for too long
        // (minimizes the memory leak from crossbeam-skiplist iterator issue)
        let mut results = Vec::new();
        let mut last_user_key: Option<Key> = None;

        for entry in self.list.range(start_mvcc..end_mvcc) {
            let mvcc_key = entry.key();

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
                    let value = entry.value();
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

/// Memory usage statistics for the crossbeam memtable.
#[derive(Debug, Clone, Copy)]
pub struct MemoryStats {
    /// Estimated bytes (cannot be determined accurately for crossbeam-skiplist)
    pub estimated_bytes: u64,
    /// Number of entries in the skip list
    pub entry_count: usize,
}

#[cfg(test)]
#[allow(clippy::uninlined_format_args)]
mod tests {
    use super::*;

    fn new_engine() -> CrossbeamMemTableEngine {
        CrossbeamMemTableEngine::new()
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
    }

    // ==================== Concurrent Tests ====================

    use std::sync::Barrier;
    use std::thread;

    #[test]
    fn test_concurrent_writes() {
        let engine = CrossbeamMemTableEngine::new();
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
        let engine = CrossbeamMemTableEngine::new();

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
        let engine = CrossbeamMemTableEngine::new();

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
        let engine = CrossbeamMemTableEngine::new();
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

    // ==================== Epoch-Based Reclamation (ERB) Tests ====================
    //
    // These tests verify the behavior of crossbeam-skiplist's epoch-based memory
    // management and iterator safety.

    #[test]
    fn test_erb_iterator_consistency_during_writes() {
        // Test that iterators see a consistent snapshot even while writes happen
        let engine = CrossbeamMemTableEngine::new();

        // Pre-populate with initial data
        for i in 0..100 {
            engine.put_at(format!("key{:03}", i).as_bytes(), b"initial", 1);
        }

        let num_readers = 4;
        let num_writers = 4;
        let barrier = Barrier::new(num_readers + num_writers);

        thread::scope(|s| {
            // Reader threads - iterate and verify consistency
            for _ in 0..num_readers {
                let engine = &engine;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    // Each iteration should see a consistent view
                    for _ in 0..50 {
                        let range = b"key000".to_vec()..b"key100".to_vec();
                        let results: Vec<_> = engine.scan_at(&range, 1).unwrap().collect();
                        // Should always see exactly 100 keys at ts=1
                        assert_eq!(
                            results.len(),
                            100,
                            "Iterator should see consistent snapshot"
                        );
                    }
                });
            }

            // Writer threads - add new versions (should not affect ts=1 readers)
            for tid in 0..num_writers {
                let engine = &engine;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    for i in 0..100 {
                        let key = format!("key{:03}", i);
                        let ts = (100 + tid * 100 + i) as u64;
                        engine.put_at(key.as_bytes(), b"updated", ts);
                    }
                });
            }
        });
    }

    #[test]
    fn test_erb_entry_lifetime_during_iteration() {
        // Test that entries remain valid during iteration
        use crossbeam_skiplist::SkipMap;

        let map: SkipMap<i64, String> = SkipMap::new();

        // Insert entries
        for i in 0..1000 {
            map.insert(i, format!("value_{}", i));
        }

        // Iterate and verify each entry's value is accessible
        for entry in map.iter() {
            let key = *entry.key();
            let value = entry.value();
            assert_eq!(value, &format!("value_{}", key));
        }

        // Concurrent iteration and insertion
        let barrier = Barrier::new(4);

        thread::scope(|s| {
            for tid in 0..2 {
                let map = &map;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    // Iterate while others insert
                    for entry in map.iter() {
                        let _ = entry.value().len();
                    }
                });

                s.spawn(move || {
                    barrier.wait();

                    // Insert while others iterate
                    for i in 0..500 {
                        let key = 1000 + tid * 500 + i;
                        map.insert(key, format!("new_value_{}", key));
                    }
                });
            }
        });
    }

    #[test]
    fn test_erb_range_iteration_safety() {
        // Test that range iterators are safe with concurrent modifications
        let engine = CrossbeamMemTableEngine::new();

        // Create multiple key ranges
        for prefix in ["aaa", "bbb", "ccc", "ddd"] {
            for i in 0..50 {
                let key = format!("{}{:03}", prefix, i);
                engine.put_at(key.as_bytes(), b"value", 1);
            }
        }

        let barrier = Barrier::new(8);

        thread::scope(|s| {
            // Range readers
            for prefix in ["aaa", "bbb", "ccc", "ddd"] {
                let engine = &engine;
                let barrier = &barrier;
                let prefix = prefix.to_string();

                s.spawn(move || {
                    barrier.wait();

                    for _ in 0..100 {
                        let start = format!("{}000", prefix);
                        let end = format!("{}999", prefix);
                        let range = start.as_bytes().to_vec()..end.as_bytes().to_vec();
                        let results: Vec<_> = engine.scan(&range).unwrap().collect();
                        assert!(
                            results.len() >= 50,
                            "Should see at least original 50 entries"
                        );
                    }
                });
            }

            // Writers to different ranges
            for (tid, prefix) in ["aaa", "bbb", "ccc", "ddd"].iter().enumerate() {
                let engine = &engine;
                let barrier = &barrier;
                let prefix = prefix.to_string();

                s.spawn(move || {
                    barrier.wait();

                    for i in 0..50 {
                        let key = format!("{}{:03}", prefix, 50 + i);
                        let ts = (tid * 100 + i + 2) as u64;
                        engine.put_at(key.as_bytes(), b"new_value", ts);
                    }
                });
            }
        });
    }

    #[test]
    fn test_erb_epoch_pin_unpin_cycles() {
        // Test many pin/unpin cycles don't cause issues
        use crossbeam_epoch as epoch;

        let engine = CrossbeamMemTableEngine::new();

        // Pre-populate
        for i in 0..100 {
            engine.put_at(format!("key{:03}", i).as_bytes(), b"value", 1);
        }

        let barrier = Barrier::new(8);

        thread::scope(|s| {
            for _ in 0..8 {
                let engine = &engine;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    // Many short-lived operations that pin/unpin epochs
                    for i in 0..1000 {
                        // Each get() internally pins the epoch
                        let key = format!("key{:03}", i % 100);
                        let _ = engine.get(key.as_bytes());

                        // Occasionally force epoch advancement
                        if i % 100 == 0 {
                            let guard = epoch::pin();
                            guard.flush();
                        }
                    }
                });
            }
        });

        // Verify data integrity
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let value = engine.get(key.as_bytes()).unwrap();
            assert!(value.is_some(), "Key {} should exist", key);
        }
    }

    #[test]
    fn test_erb_iterator_drop_safety() {
        // Test that dropping iterators mid-iteration is safe
        let engine = CrossbeamMemTableEngine::new();

        for i in 0..1000 {
            engine.put_at(format!("key{:05}", i).as_bytes(), b"value", 1);
        }

        let barrier = Barrier::new(4);

        thread::scope(|s| {
            for _ in 0..4 {
                let engine = &engine;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    for _ in 0..100 {
                        let range = b"key00000".to_vec()..b"key99999".to_vec();
                        // Create iterator and drop it after reading only some entries
                        let mut iter = engine.scan(&range).unwrap();
                        for _ in 0..10 {
                            if iter.next().is_none() {
                                break;
                            }
                        }
                        // Iterator dropped here - should be safe
                    }
                });
            }
        });
    }

    #[test]
    fn test_erb_concurrent_many_versions() {
        // Test epoch behavior with many versions of the same key
        let engine = CrossbeamMemTableEngine::new();

        let num_threads = 8;
        let versions_per_thread = 1000;
        let barrier = Barrier::new(num_threads);

        thread::scope(|s| {
            for tid in 0..num_threads {
                let engine = &engine;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    // Each thread writes many versions of the same key
                    for i in 0..versions_per_thread {
                        let ts = (tid * versions_per_thread + i + 1) as u64;
                        engine.put_at(b"hotkey", format!("v{}_{}", tid, i).as_bytes(), ts);
                    }
                });
            }
        });

        // Verify we can read at different timestamps
        let total_versions = num_threads * versions_per_thread;
        for ts in [1, 100, 1000, total_versions as u64] {
            let value = engine.get_at(b"hotkey", ts);
            assert!(value.is_ok(), "Should be able to read at ts={}", ts);
        }

        // Entry count should be num_threads * versions_per_thread
        assert_eq!(
            engine.len(),
            total_versions,
            "Should have {} entries",
            total_versions
        );
    }
}
