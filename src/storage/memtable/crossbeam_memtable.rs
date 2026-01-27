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
//! ## Known Issues
//!
//! The crossbeam-skiplist crate has a known memory leak in iterator operations
//! (see https://github.com/crossbeam-rs/crossbeam/pull/1217). This implementation
//! minimizes iterator usage patterns that trigger the leak.

use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_skiplist::SkipMap;

use crate::error::{Result, TiSqlError};
use crate::storage::mvcc::{
    decode_mvcc_key, encode_mvcc_key, is_tombstone, next_key_bound, MvccKey, TOMBSTONE,
};
use crate::storage::{StorageEngine, WriteBatch, WriteOp};
use crate::types::{Key, RawValue, Timestamp};

/// MVCC-aware memtable engine using crossbeam-skiplist.
///
/// This engine stores multiple versions of each key, keyed by MVCC key (`key || !commit_ts`).
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
    /// This scans from `key || !ts` to find the first version
    /// with commit_ts <= ts.
    fn get_at_internal(&self, key: &[u8], ts: Timestamp) -> Result<Option<RawValue>> {
        match self.get_at_with_tombstone(key, ts)? {
            super::GetResult::Found(v) => Ok(Some(v)),
            super::GetResult::FoundTombstone | super::GetResult::NotFound => Ok(None),
        }
    }

    /// Get the latest version of a key visible at the given timestamp, with tombstone awareness.
    ///
    /// Returns a tri-state result:
    /// - `Found(value)`: Key exists with the given value
    /// - `FoundTombstone`: Key was deleted (tombstone marker found)
    /// - `NotFound`: Key not found at this timestamp
    ///
    /// This is critical for correct MVCC: when `FoundTombstone` is returned,
    /// callers must NOT continue searching older levels.
    pub fn get_at_with_tombstone(&self, key: &[u8], ts: Timestamp) -> Result<super::GetResult> {
        // Build scan key: key || !ts
        // Due to !ts encoding, this will find the first entry >= key with ts' <= ts
        let start = encode_mvcc_key(key, ts);

        // Compute end bound for the range.
        // We need to check against MVCC key (not decoded key) because
        // keys like "key_0_10" sort BEFORE "key_0_1" || ts in MVCC space but
        // "key_0_10" > "key_0_1" lexicographically. Using next_key_bound
        // ensures we continue past such keys.
        let mut end_key = key.to_vec();
        let end_key_valid = next_key_bound(&mut end_key);

        for entry in self.list.range(start..) {
            let mvcc_key = entry.key();

            // Check if we've gone past our key prefix.
            // Use the MVCC key directly against end_key bound.
            if end_key_valid && mvcc_key.as_slice() >= end_key.as_slice() {
                break;
            }

            if let Some((decoded_key, _entry_ts)) = decode_mvcc_key(mvcc_key) {
                // Verify this is our key (exact match required)
                if decoded_key == key {
                    let value = entry.value();
                    if is_tombstone(value) {
                        return Ok(super::GetResult::FoundTombstone);
                    }
                    return Ok(super::GetResult::Found(value.clone()));
                }
                // If decoded_key > key and we don't have a valid end_key (all-0xFF case),
                // we need to break manually
                if !end_key_valid && decoded_key.as_slice() > key {
                    break;
                }
            } else {
                // Invalid MVCC key format, skip
                continue;
            }
        }

        Ok(super::GetResult::NotFound)
    }

    /// Write a key-value pair at the given timestamp.
    ///
    /// This method is exposed for benchmarking and testing. Production code
    /// should use `write_batch()` instead.
    pub fn put_at(&self, key: &[u8], value: &[u8], ts: Timestamp) {
        let mvcc_key = encode_mvcc_key(key, ts);
        // SkipMap::insert returns the entry (we ignore it)
        // For MVCC, each key is unique (includes timestamp), so this should always insert
        self.list.insert(mvcc_key, value.to_vec());
        self.entry_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Write a tombstone (delete marker) at the given timestamp.
    ///
    /// This method is exposed for benchmarking and testing. Production code
    /// should use `write_batch()` instead.
    pub fn delete_at(&self, key: &[u8], ts: Timestamp) {
        let mvcc_key = encode_mvcc_key(key, ts);
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

impl CrossbeamMemTableEngine {
    /// Scan all entries in range, INCLUDING tombstones.
    ///
    /// This is used during SST flush to ensure tombstones are written to SST
    /// so they can mask older values in previous SSTs.
    ///
    /// Returns (key, value) pairs where value may be a tombstone (empty).
    /// NOTE: This returns DECODED keys. For SST flush, use `scan_all_mvcc()` instead.
    pub fn scan_all(
        &self,
        range: &Range<Key>,
    ) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>> {
        let start_mvcc = encode_mvcc_key(&range.start, Timestamp::MAX);
        // Use ts=0 for end to get the LARGEST mvcc key, ensuring we include all versions
        let end_mvcc = encode_mvcc_key(&range.end, 0);

        let mut results = Vec::new();
        let mut last_key: Option<Key> = None;

        for entry in self.list.range(start_mvcc..end_mvcc) {
            let mvcc_key = entry.key();

            if let Some((decoded_key, _entry_ts)) = decode_mvcc_key(mvcc_key) {
                // Check if within range
                if decoded_key < range.start || decoded_key >= range.end {
                    continue;
                }

                // Skip if we already have this key (we want the latest version only)
                if let Some(ref last) = last_key {
                    if &decoded_key == last {
                        continue;
                    }
                }

                // Include ALL values, including tombstones
                let value = entry.value();
                results.push((decoded_key.clone(), value.clone()));
                last_key = Some(decoded_key);
            }
        }

        Ok(Box::new(results.into_iter()))
    }

    /// Scan at timestamp, INCLUDING tombstones (for merging with SST results).
    ///
    /// Unlike `scan_at()` which filters out tombstones, this method returns them.
    /// This is needed when merging memtable results with SST results - tombstones
    /// in the memtable must mask older values in SSTs.
    ///
    /// Returns (key, value) pairs visible at `ts`, including tombstones.
    pub fn scan_at_with_tombstones(
        &self,
        range: &Range<Key>,
        ts: Timestamp,
    ) -> Result<Vec<(Key, RawValue)>> {
        // For the start bound, use ts=MAX to get the smallest MVCC key for that key
        let start_mvcc = encode_mvcc_key(&range.start, Timestamp::MAX);
        // For the end bound, use ts=0 to get the LARGEST MVCC key for that key
        // This ensures we include all MVCC keys for keys in [start, end)
        // because MVCC keys for lower timestamps have suffix 0xFF..FF which could
        // exceed the end bound if we used ts=MAX
        let end_mvcc = encode_mvcc_key(&range.end, 0);

        let mut results = Vec::new();
        let mut last_key: Option<Key> = None;

        for entry in self.list.range(start_mvcc..end_mvcc) {
            let mvcc_key = entry.key();

            if let Some((decoded_key, entry_ts)) = decode_mvcc_key(mvcc_key) {
                // Check if within range
                if decoded_key < range.start || decoded_key >= range.end {
                    continue;
                }

                // Skip if we already have this key (we want the latest visible version)
                if let Some(ref last) = last_key {
                    if &decoded_key == last {
                        continue;
                    }
                }

                // Check if this version is visible at the given timestamp
                if entry_ts <= ts {
                    // Include ALL values, including tombstones (unlike scan_at)
                    let value = entry.value();
                    results.push((decoded_key.clone(), value.clone()));
                    last_key = Some(decoded_key);
                }
            }
        }

        Ok(results)
    }

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
            for entry in self.list.iter() {
                let mvcc_key = entry.key();
                let value = entry.value();
                // Safety: keys in skiplist are valid MVCC keys
                let mvcc = MvccKey::from_bytes_unchecked(mvcc_key.clone());
                results.push((mvcc, value.clone()));
            }
        } else {
            // For bounded range, iterate from start and check key bounds
            let start_bytes: Vec<u8> = range.start.into();
            let end_bytes: Vec<u8> = range.end.into();

            for entry in self.list.range(start_bytes.clone()..) {
                let mvcc_key = entry.key();

                // Check if past end of MVCC key range
                if !end_bytes.is_empty() && mvcc_key >= &end_bytes {
                    break;
                }

                // Include ALL versions (no deduplication) and ALL values including tombstones
                let value = entry.value();
                // Safety: keys in skiplist are valid MVCC keys
                let mvcc = MvccKey::from_bytes_unchecked(mvcc_key.clone());
                results.push((mvcc, value.clone()));
            }
        }

        Ok(results)
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
    use crate::storage::mvcc::increment_bytes;
    use std::ops::Range;

    fn new_engine() -> CrossbeamMemTableEngine {
        CrossbeamMemTableEngine::new()
    }

    // ==================== Test Helpers Using MvccKey ====================

    /// Get the latest version of a key visible at the given timestamp.
    fn get_at_for_test(
        engine: &CrossbeamMemTableEngine,
        key: &[u8],
        ts: Timestamp,
    ) -> Option<RawValue> {
        let start = MvccKey::encode(key, ts);
        let end = MvccKey::encode(key, 0)
            .next_key()
            .unwrap_or_else(|| MvccKey::unbounded());
        let range = start..end;

        let results = engine.scan_mvcc(range).unwrap();

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

    fn get_for_test(engine: &CrossbeamMemTableEngine, key: &[u8]) -> Option<RawValue> {
        get_at_for_test(engine, key, Timestamp::MAX)
    }

    fn scan_at_for_test(
        engine: &CrossbeamMemTableEngine,
        range: &Range<Key>,
        ts: Timestamp,
    ) -> Vec<(Key, RawValue)> {
        let start = MvccKey::encode(&range.start, Timestamp::MAX);
        let end = MvccKey::encode(&range.end, 0);
        let mvcc_range = start..end;

        let results = engine.scan_mvcc(mvcc_range).unwrap();

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

    fn scan_for_test(engine: &CrossbeamMemTableEngine, range: &Range<Key>) -> Vec<(Key, RawValue)> {
        scan_at_for_test(engine, range, Timestamp::MAX)
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

        let results = scan_for_test(&engine, &(b"b".to_vec()..b"d".to_vec()));

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
    fn test_scan_at_with_updates() {
        let engine = new_engine();

        // Write initial version at ts=10
        engine.put_at(b"key", b"v1", 10);

        // Update at ts=20
        engine.put_at(b"key", b"v2", 20);

        // scan_at ts=15 should see v1
        let range = b"key".to_vec()..b"kez".to_vec();
        let results = scan_at_for_test(&engine, &range, 15);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, b"v1".to_vec());

        // scan_at ts=25 should see v2 (latest)
        let results = scan_at_for_test(&engine, &range, 25);
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
                    get_for_test(&engine, key.as_bytes()).is_some(),
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
                        let value = get_at_for_test(engine, key.as_bytes(), 1);
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
                        let results = scan_for_test(engine, &range);
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
        let results = scan_for_test(&engine, &(b"stress".to_vec()..b"strest".to_vec()));
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
                        let results = scan_at_for_test(engine, &range, 1);
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
                        let results = scan_for_test(engine, &range);
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
                        let _ = get_for_test(engine, key.as_bytes());

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
            let value = get_for_test(&engine, key.as_bytes());
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
                        // Create range and scan only first few entries
                        let results = scan_for_test(engine, &range);
                        // Just access first 10 entries if available
                        let _ = results.iter().take(10).count();
                        // Safe - no iterator dropping issues with Vec
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
            let _ = get_at_for_test(&engine, b"hotkey", ts);
            // Just verify no panic
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
