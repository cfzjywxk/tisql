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

//! MVCC-aware memtable using BTreeMap with RwLock.
//!
//! This implementation uses a standard library BTreeMap protected by a
//! reader-writer lock. It provides a baseline comparison for lock-free
//! implementations.
//!
//! ## Characteristics
//!
//! - **Pros**: Good cache locality for reads, simple implementation
//! - **Cons**: Write contention under high concurrency, readers block writers
//!
//! ## Key Encoding
//!
//! MVCC keys are encoded as: `key || !commit_ts` (descending order)
//!
//! The bitwise NOT of commit_ts ensures that:
//! - Keys are sorted in descending timestamp order
//! - Scanning from a key finds the latest visible version first

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;

use crate::error::{Result, TiSqlError};
use crate::storage::mvcc::{
    decode_mvcc_key, encode_mvcc_key, increment_bytes, is_tombstone, MvccKey, TOMBSTONE,
};
use crate::storage::{StorageEngine, WriteBatch, WriteOp};
use crate::types::{Key, RawValue, Timestamp};

/// MVCC-aware memtable engine using BTreeMap with RwLock.
///
/// This provides a baseline comparison for lock-free implementations.
/// Uses reader-writer lock for concurrent access.
pub struct BTreeMemTableEngine {
    /// The BTreeMap storing MVCC-encoded keys, protected by RwLock.
    map: RwLock<BTreeMap<Key, RawValue>>,
    /// Entry count (tracked separately for O(1) len())
    entry_count: AtomicUsize,
}

impl BTreeMemTableEngine {
    /// Create a new BTreeMap-based MVCC memtable engine.
    pub fn new() -> Self {
        Self {
            map: RwLock::new(BTreeMap::new()),
            entry_count: AtomicUsize::new(0),
        }
    }

    /// Get the latest version of a key visible at the given timestamp.
    pub fn get_at(&self, key: &[u8], ts: Timestamp) -> Result<Option<RawValue>> {
        let start = encode_mvcc_key(key, ts);
        let mut end_key = key.to_vec();
        increment_bytes(&mut end_key);

        let map = self.map.read().unwrap();

        for (mvcc_key, value) in map.range(start..) {
            if mvcc_key >= &end_key {
                break;
            }

            if let Some((decoded_key, _entry_ts)) = decode_mvcc_key(mvcc_key) {
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
    pub fn put_at(&self, key: &[u8], value: &[u8], ts: Timestamp) {
        let mvcc_key = encode_mvcc_key(key, ts);
        let mut map = self.map.write().unwrap();
        map.insert(mvcc_key, value.to_vec());
        self.entry_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Write a tombstone (delete marker) at the given timestamp.
    pub fn delete_at(&self, key: &[u8], ts: Timestamp) {
        let mvcc_key = encode_mvcc_key(key, ts);
        let mut map = self.map.write().unwrap();
        map.insert(mvcc_key, TOMBSTONE.to_vec());
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
}

impl Default for BTreeMemTableEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl BTreeMemTableEngine {
    /// Scan all entries in range, returning MVCC keys.
    ///
    /// Returns ALL versions of each key (not just the latest), including tombstones.
    ///
    /// # Arguments
    /// * `range` - MVCC key range to scan. Use `MvccKey::unbounded()` for unbounded.
    pub fn scan_mvcc(&self, range: Range<MvccKey>) -> Result<Vec<(MvccKey, RawValue)>> {
        let map = self.map.read().unwrap();
        let mut results = Vec::new();

        // Handle truly unbounded range
        let is_unbounded = range.start.is_unbounded() && range.end.is_unbounded();

        if is_unbounded {
            for (mvcc_key, value) in map.iter() {
                // Safety: keys in map are valid MVCC keys
                let mvcc = MvccKey::from_bytes_unchecked(mvcc_key.clone());
                results.push((mvcc, value.clone()));
            }
        } else {
            let start_bytes: Vec<u8> = range.start.into();
            let end_bytes: Vec<u8> = range.end.into();

            for (mvcc_key, value) in map.range(start_bytes.clone()..) {
                // Check if past end of range
                if !end_bytes.is_empty() && mvcc_key >= &end_bytes {
                    break;
                }

                // Safety: keys in map are valid MVCC keys
                let mvcc = MvccKey::from_bytes_unchecked(mvcc_key.clone());
                results.push((mvcc, value.clone()));
            }
        }

        Ok(results)
    }
}

impl StorageEngine for BTreeMemTableEngine {
    fn get_at(&self, key: &[u8], ts: Timestamp) -> Result<Option<RawValue>> {
        BTreeMemTableEngine::get_at(self, key, ts)
    }

    fn scan(&self, range: Range<MvccKey>) -> Result<Vec<(MvccKey, RawValue)>> {
        self.scan_mvcc(range)
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Range;

    // ==================== Test Helpers Using MvccKey ====================

    fn get_at_for_test(
        engine: &BTreeMemTableEngine,
        key: &[u8],
        ts: Timestamp,
    ) -> Option<RawValue> {
        let start = MvccKey::encode(key, ts);
        let end = MvccKey::encode(key, 0)
            .next_key()
            .unwrap_or_else(MvccKey::unbounded);
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

    fn get_for_test(engine: &BTreeMemTableEngine, key: &[u8]) -> Option<RawValue> {
        get_at_for_test(engine, key, Timestamp::MAX)
    }

    fn scan_for_test(engine: &BTreeMemTableEngine, range: &Range<Key>) -> Vec<(Key, RawValue)> {
        let start = MvccKey::encode(&range.start, Timestamp::MAX);
        let end = MvccKey::encode(&range.end, 0);
        let mvcc_range = start..end;

        let results = engine.scan_mvcc(mvcc_range).unwrap();

        let mut seen_keys: std::collections::HashSet<Key> = std::collections::HashSet::new();
        let mut output = Vec::new();

        for (mvcc_key, value) in results {
            let (decoded_key, _entry_ts) = mvcc_key.decode();
            if decoded_key < range.start || decoded_key >= range.end {
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
    fn test_basic_put_get() {
        let engine = BTreeMemTableEngine::new();
        engine.put_at(b"key1", b"value1", 1);
        let value = get_for_test(&engine, b"key1");
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_mvcc_versions() {
        let engine = BTreeMemTableEngine::new();

        engine.put_at(b"key", b"v1", 10);
        engine.put_at(b"key", b"v2", 20);
        engine.put_at(b"key", b"v3", 30);

        assert_eq!(get_at_for_test(&engine, b"key", 10), Some(b"v1".to_vec()));
        assert_eq!(get_at_for_test(&engine, b"key", 20), Some(b"v2".to_vec()));
        assert_eq!(get_at_for_test(&engine, b"key", 30), Some(b"v3".to_vec()));
        assert_eq!(get_at_for_test(&engine, b"key", 5), None);
    }

    #[test]
    fn test_delete() {
        let engine = BTreeMemTableEngine::new();

        engine.put_at(b"key", b"value", 10);
        engine.delete_at(b"key", 20);

        assert_eq!(
            get_at_for_test(&engine, b"key", 10),
            Some(b"value".to_vec())
        );
        assert_eq!(get_at_for_test(&engine, b"key", 20), None);
    }

    #[test]
    fn test_scan() {
        let engine = BTreeMemTableEngine::new();

        engine.put_at(b"a", b"1", 1);
        engine.put_at(b"b", b"2", 1);
        engine.put_at(b"c", b"3", 1);

        let results = scan_for_test(&engine, &(b"a".to_vec()..b"c".to_vec()));
        assert_eq!(results.len(), 2);
    }

    use std::sync::Barrier;
    use std::thread;

    #[test]
    fn test_concurrent_writes() {
        let engine = BTreeMemTableEngine::new();
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
                        engine.put_at(key.as_bytes(), b"value", ts);
                    }
                });
            }
        });

        // Verify all writes are visible
        for tid in 0..num_threads {
            for i in 0..writes_per_thread {
                let key = format!("key_{tid}_{i}");
                assert!(get_for_test(&engine, key.as_bytes()).is_some());
            }
        }
    }

    #[test]
    fn test_concurrent_reads_and_writes() {
        let engine = BTreeMemTableEngine::new();

        // Pre-populate
        for i in 0..1000 {
            engine.put_at(format!("key{i:04}").as_bytes(), b"initial", 1);
        }

        let num_writers = 4;
        let num_readers = 4;
        let ops_per_thread = 1000;
        let barrier = Barrier::new(num_writers + num_readers);

        thread::scope(|s| {
            for tid in 0..num_writers {
                let engine = &engine;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    for i in 0..ops_per_thread {
                        let key = format!("key{:04}", i % 1000);
                        let ts = (100 + tid * ops_per_thread + i) as u64;
                        engine.put_at(key.as_bytes(), b"updated", ts);
                    }
                });
            }

            for _ in 0..num_readers {
                let engine = &engine;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    for i in 0..ops_per_thread {
                        let key = format!("key{:04}", i % 1000);
                        let value = get_at_for_test(engine, key.as_bytes(), 1);
                        assert_eq!(value, Some(b"initial".to_vec()));
                    }
                });
            }
        });
    }
}
