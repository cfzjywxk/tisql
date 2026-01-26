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
//! MVCC keys are encoded as: `user_key || !commit_ts` (descending order)
//!
//! The bitwise NOT of commit_ts ensures that:
//! - Keys are sorted in descending timestamp order
//! - Scanning from a key finds the latest visible version first

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;

use super::{StorageEngine, WriteBatch, WriteOp};
use crate::error::{Result, TiSqlError};
use crate::types::{Key, RawValue, Timestamp};

/// Tombstone marker for deleted keys.
const TOMBSTONE: &[u8] = b"\x00\x00\x00\x00TISQL_TOMBSTONE\x01";

/// Encode MVCC key: user_key || !commit_ts (8 bytes, big-endian)
fn encode_mvcc_key(user_key: &[u8], ts: Timestamp) -> Key {
    let mut mvcc_key = Vec::with_capacity(user_key.len() + 8);
    mvcc_key.extend_from_slice(user_key);
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
    bytes.insert(0, 1);
}

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
    fn get_at_internal(&self, user_key: &[u8], ts: Timestamp) -> Result<Option<RawValue>> {
        let start = encode_mvcc_key(user_key, ts);
        let mut end_key = user_key.to_vec();
        increment_bytes(&mut end_key);

        let map = self.map.read().unwrap();

        for (mvcc_key, value) in map.range(start..) {
            if mvcc_key >= &end_key {
                break;
            }

            if let Some((key, _entry_ts)) = decode_mvcc_key(mvcc_key) {
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
    pub fn put_at(&self, user_key: &[u8], value: &[u8], ts: Timestamp) {
        let mvcc_key = encode_mvcc_key(user_key, ts);
        let mut map = self.map.write().unwrap();
        map.insert(mvcc_key, value.to_vec());
        self.entry_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Write a tombstone (delete marker) at the given timestamp.
    pub fn delete_at(&self, user_key: &[u8], ts: Timestamp) {
        let mvcc_key = encode_mvcc_key(user_key, ts);
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

impl StorageEngine for BTreeMemTableEngine {
    fn get(&self, key: &[u8]) -> Result<Option<RawValue>> {
        self.get_at(key, Timestamp::MAX)
    }

    fn get_at(&self, key: &[u8], ts: Timestamp) -> Result<Option<RawValue>> {
        self.get_at_internal(key, ts)
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

    fn scan(&self, range: &Range<Key>) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>> {
        self.scan_at(range, Timestamp::MAX)
    }

    fn scan_at(
        &self,
        range: &Range<Key>,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>> {
        let start_mvcc = encode_mvcc_key(&range.start, Timestamp::MAX);
        let end_mvcc = encode_mvcc_key(&range.end, Timestamp::MAX);

        let map = self.map.read().unwrap();

        let mut results = Vec::new();
        let mut last_user_key: Option<Key> = None;

        for (mvcc_key, value) in map.range(start_mvcc..end_mvcc) {
            if let Some((user_key, entry_ts)) = decode_mvcc_key(mvcc_key) {
                if user_key < range.start || user_key >= range.end {
                    continue;
                }

                if let Some(ref last) = last_user_key {
                    if &user_key == last {
                        continue;
                    }
                }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_put_get() {
        let engine = BTreeMemTableEngine::new();
        engine.put_at(b"key1", b"value1", 1);
        let value = engine.get(b"key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_mvcc_versions() {
        let engine = BTreeMemTableEngine::new();

        engine.put_at(b"key", b"v1", 10);
        engine.put_at(b"key", b"v2", 20);
        engine.put_at(b"key", b"v3", 30);

        assert_eq!(engine.get_at(b"key", 10).unwrap(), Some(b"v1".to_vec()));
        assert_eq!(engine.get_at(b"key", 20).unwrap(), Some(b"v2".to_vec()));
        assert_eq!(engine.get_at(b"key", 30).unwrap(), Some(b"v3".to_vec()));
        assert_eq!(engine.get_at(b"key", 5).unwrap(), None);
    }

    #[test]
    fn test_delete() {
        let engine = BTreeMemTableEngine::new();

        engine.put_at(b"key", b"value", 10);
        engine.delete_at(b"key", 20);

        assert_eq!(engine.get_at(b"key", 10).unwrap(), Some(b"value".to_vec()));
        assert_eq!(engine.get_at(b"key", 20).unwrap(), None);
    }

    #[test]
    fn test_scan() {
        let engine = BTreeMemTableEngine::new();

        engine.put_at(b"a", b"1", 1);
        engine.put_at(b"b", b"2", 1);
        engine.put_at(b"c", b"3", 1);

        let results: Vec<_> = engine
            .scan(&(b"a".to_vec()..b"c".to_vec()))
            .unwrap()
            .collect();
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
                assert!(engine.get(key.as_bytes()).unwrap().is_some());
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
                        let value = engine.get_at(key.as_bytes(), 1).unwrap();
                        assert_eq!(value, Some(b"initial".to_vec()));
                    }
                });
            }
        });
    }
}
