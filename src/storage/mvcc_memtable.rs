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

use std::ops::Range;
use std::sync::Arc;

use crossbeam_skiplist::SkipMap;

use crate::error::Result;
use crate::transaction::ConcurrencyManager;
use crate::types::{Key, RawValue, Timestamp};

use super::{Snapshot, StorageEngine, WriteBatch, WriteOp};

/// Tombstone marker for deleted keys.
/// We use a special byte sequence that's unlikely to be a valid value.
const TOMBSTONE: &[u8] = b"\x00\x00\x00TOMBSTONE\x00\x00\x00";

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

/// Inner storage that can be shared via Arc.
struct MvccMemTableInner {
    /// Concurrent skipmap storing MVCC-encoded keys.
    data: SkipMap<Key, RawValue>,
    /// ConcurrencyManager for lock checking and TSO.
    concurrency_manager: Arc<ConcurrencyManager>,
}

/// MVCC-aware memtable engine using crossbeam-skiplist.
///
/// This engine stores multiple versions of each key, keyed by `user_key || !commit_ts`.
/// Reads automatically find the latest visible version.
pub struct MvccMemTableEngine {
    inner: Arc<MvccMemTableInner>,
}

impl MvccMemTableEngine {
    /// Create a new MVCC memtable engine.
    pub fn new(concurrency_manager: Arc<ConcurrencyManager>) -> Self {
        Self {
            inner: Arc::new(MvccMemTableInner {
                data: SkipMap::new(),
                concurrency_manager,
            }),
        }
    }

    /// Get the latest version of a key visible at the given timestamp.
    ///
    /// This scans from `user_key || !ts` to find the first version
    /// with commit_ts <= ts.
    fn get_at_internal(&self, user_key: &[u8], ts: Timestamp) -> Result<Option<RawValue>> {
        get_at_internal(&self.inner.data, user_key, ts)
    }

    /// Write a key-value pair at the given timestamp.
    fn put_at(&self, user_key: &[u8], value: &[u8], ts: Timestamp) {
        let mvcc_key = encode_mvcc_key(user_key, ts);
        self.inner.data.insert(mvcc_key, value.to_vec());
    }

    /// Write a tombstone (delete marker) at the given timestamp.
    fn delete_at(&self, user_key: &[u8], ts: Timestamp) {
        let mvcc_key = encode_mvcc_key(user_key, ts);
        self.inner.data.insert(mvcc_key, TOMBSTONE.to_vec());
    }

    /// Get reference to the ConcurrencyManager.
    pub fn concurrency_manager(&self) -> &ConcurrencyManager {
        &self.inner.concurrency_manager
    }
}

/// Shared get_at_internal logic for both engine and snapshot.
fn get_at_internal(
    data: &SkipMap<Key, RawValue>,
    user_key: &[u8],
    ts: Timestamp,
) -> Result<Option<RawValue>> {
    // Build scan bounds
    // Start: user_key || !ts (will find versions <= ts)
    let start = encode_mvcc_key(user_key, ts);

    // We need to find keys that start with user_key
    // The end bound should be the first key > user_key
    let mut end_key = user_key.to_vec();
    increment_bytes(&mut end_key);

    for entry in data.range(start..end_key) {
        let mvcc_key = entry.key();
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

impl StorageEngine for MvccMemTableEngine {
    fn get(&self, key: &[u8]) -> Result<Option<RawValue>> {
        // Get at latest timestamp (Timestamp::MAX means get the latest version)
        self.get_at(key, Timestamp::MAX)
    }

    fn get_at(&self, key: &[u8], ts: Timestamp) -> Result<Option<RawValue>> {
        // 1. Check in-memory lock table
        self.inner.concurrency_manager.check_lock(key, ts)?;

        // 2. Find latest version <= ts
        self.get_at_internal(key, ts)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // For simple put without transaction context, use current ts
        let ts = self.inner.concurrency_manager.get_ts();
        self.put_at(key, value, ts);
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        // For simple delete without transaction context, use current ts
        let ts = self.inner.concurrency_manager.get_ts();
        self.delete_at(key, ts);
        Ok(())
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        // Get commit_ts from batch, or allocate new one
        let commit_ts = batch
            .commit_ts()
            .unwrap_or_else(|| self.inner.concurrency_manager.get_ts());

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

    fn snapshot(&self) -> Result<Box<dyn Snapshot>> {
        // For MVCC, snapshot is just a timestamp
        let ts = self.inner.concurrency_manager.get_ts();
        Ok(Box::new(MvccSnapshot {
            ts,
            inner: Arc::clone(&self.inner),
        }))
    }

    fn scan(&self, range: Range<Key>) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>> {
        // Get current timestamp for visibility
        let ts = Timestamp::MAX;

        // Check range for locks
        self.inner
            .concurrency_manager
            .check_range(&range.start, &range.end, ts)?;

        // Scan MVCC keys and deduplicate by user_key
        let start_mvcc = encode_mvcc_key(&range.start, ts);
        let end_mvcc = encode_mvcc_key(&range.end, 0); // 0 is the "oldest" version

        let mut results = Vec::new();
        let mut last_user_key: Option<Key> = None;

        for entry in self.inner.data.range(start_mvcc..end_mvcc) {
            if let Some((user_key, entry_ts)) = decode_mvcc_key(entry.key()) {
                // Check if within user range
                if user_key < range.start || user_key >= range.end {
                    continue;
                }

                // Skip if we already have this key (we want the latest version)
                if let Some(ref last) = last_user_key {
                    if &user_key == last {
                        continue;
                    }
                }

                // Check if this version is visible
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

/// MVCC snapshot at a specific timestamp.
pub struct MvccSnapshot {
    ts: Timestamp,
    inner: Arc<MvccMemTableInner>,
}

impl Snapshot for MvccSnapshot {
    fn get(&self, key: &[u8]) -> Result<Option<RawValue>> {
        // Check lock
        self.inner.concurrency_manager.check_lock(key, self.ts)?;

        // Get at snapshot timestamp
        get_at_internal(&self.inner.data, key, self.ts)
    }

    fn scan(&self, range: Range<Key>) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>> {
        // Check range for locks
        self.inner
            .concurrency_manager
            .check_range(&range.start, &range.end, self.ts)?;

        let start_mvcc = encode_mvcc_key(&range.start, self.ts);
        let end_mvcc = encode_mvcc_key(&range.end, 0);

        let mut results = Vec::new();
        let mut last_user_key: Option<Key> = None;

        for entry in self.inner.data.range(start_mvcc..end_mvcc) {
            if let Some((user_key, entry_ts)) = decode_mvcc_key(entry.key()) {
                if user_key < range.start || user_key >= range.end {
                    continue;
                }

                if let Some(ref last) = last_user_key {
                    if &user_key == last {
                        continue;
                    }
                }

                if entry_ts <= self.ts {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn new_engine() -> MvccMemTableEngine {
        let cm = Arc::new(ConcurrencyManager::new(1));
        MvccMemTableEngine::new(cm)
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

        engine.put(b"key1", b"value1").unwrap();
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

        engine.put(b"key1", b"value1").unwrap();
        engine.delete(b"key1").unwrap();

        let value = engine.get(b"key1").unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_mvcc_versions() {
        let cm = Arc::new(ConcurrencyManager::new(1));
        let engine = MvccMemTableEngine::new(Arc::clone(&cm));

        // Write version 1
        let ts1 = cm.get_ts();
        engine.put_at(b"key", b"v1", ts1);

        // Write version 2
        let ts2 = cm.get_ts();
        engine.put_at(b"key", b"v2", ts2);

        // Write version 3
        let ts3 = cm.get_ts();
        engine.put_at(b"key", b"v3", ts3);

        // Read at ts1 should see v1
        let v = engine.get_at_internal(b"key", ts1).unwrap();
        assert_eq!(v, Some(b"v1".to_vec()));

        // Read at ts2 should see v2
        let v = engine.get_at_internal(b"key", ts2).unwrap();
        assert_eq!(v, Some(b"v2".to_vec()));

        // Read at ts3 should see v3
        let v = engine.get_at_internal(b"key", ts3).unwrap();
        assert_eq!(v, Some(b"v3".to_vec()));

        // Read at latest should see v3
        let v = engine.get_at_internal(b"key", Timestamp::MAX).unwrap();
        assert_eq!(v, Some(b"v3".to_vec()));

        // Read at ts before any write should see nothing
        let v = engine.get_at_internal(b"key", 0).unwrap();
        assert_eq!(v, None);
    }

    #[test]
    fn test_mvcc_delete_version() {
        let cm = Arc::new(ConcurrencyManager::new(1));
        let engine = MvccMemTableEngine::new(Arc::clone(&cm));

        // Write value
        let ts1 = cm.get_ts();
        engine.put_at(b"key", b"value", ts1);

        // Delete at later timestamp
        let ts2 = cm.get_ts();
        engine.delete_at(b"key", ts2);

        // Read at ts1 should see value
        let v = engine.get_at_internal(b"key", ts1).unwrap();
        assert_eq!(v, Some(b"value".to_vec()));

        // Read at ts2 should see nothing (deleted)
        let v = engine.get_at_internal(b"key", ts2).unwrap();
        assert_eq!(v, None);

        // Read at latest should see nothing
        let v = engine.get_at_internal(b"key", Timestamp::MAX).unwrap();
        assert_eq!(v, None);
    }

    #[test]
    fn test_write_batch() {
        let engine = new_engine();

        let mut batch = WriteBatch::new();
        batch.put(b"k1".to_vec(), b"v1".to_vec());
        batch.put(b"k2".to_vec(), b"v2".to_vec());
        batch.put(b"k3".to_vec(), b"v3".to_vec());

        engine.write_batch(batch).unwrap();

        assert_eq!(engine.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(engine.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(engine.get(b"k3").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn test_write_batch_with_commit_ts() {
        let cm = Arc::new(ConcurrencyManager::new(1));
        let engine = MvccMemTableEngine::new(Arc::clone(&cm));

        let commit_ts = 100;
        let mut batch = WriteBatch::new();
        batch.set_commit_ts(commit_ts);
        batch.put(b"key".to_vec(), b"value".to_vec());

        engine.write_batch(batch).unwrap();

        // Should be visible at ts >= 100
        let v = engine.get_at_internal(b"key", 100).unwrap();
        assert_eq!(v, Some(b"value".to_vec()));

        // Should not be visible at ts < 100
        let v = engine.get_at_internal(b"key", 99).unwrap();
        assert_eq!(v, None);
    }

    #[test]
    fn test_lock_blocks_read() {
        use crate::error::TiSqlError;
        use crate::transaction::Lock;

        let cm = Arc::new(ConcurrencyManager::new(1));
        let engine = MvccMemTableEngine::new(Arc::clone(&cm));

        // Put some data
        engine.put(b"key", b"value").unwrap();

        // Acquire lock
        let lock = Lock {
            ts: 100,
            primary: b"key".to_vec(),
        };
        let _guards = cm.lock_keys(&[b"key".to_vec()], lock).unwrap();

        // Read should be blocked
        let result = engine.get_at(b"key", 1);
        assert!(result.is_err());
        match result {
            Err(TiSqlError::KeyIsLocked { .. }) => {}
            _ => panic!("Expected KeyIsLocked error"),
        }
    }

    #[test]
    fn test_snapshot() {
        let cm = Arc::new(ConcurrencyManager::new(1));
        let engine = MvccMemTableEngine::new(Arc::clone(&cm));

        // Write v1
        engine.put(b"key", b"v1").unwrap();

        // Take snapshot
        let snapshot = engine.snapshot().unwrap();

        // Write v2 after snapshot
        engine.put(b"key", b"v2").unwrap();

        // Snapshot should still see v1 (actually it sees latest before snapshot ts)
        // Since our snapshot is at the time of creation, it should see v1
        let v = snapshot.get(b"key").unwrap();
        assert_eq!(v, Some(b"v1".to_vec()));

        // Engine should see v2
        let v = engine.get(b"key").unwrap();
        assert_eq!(v, Some(b"v2".to_vec()));
    }

    #[test]
    fn test_scan() {
        let engine = new_engine();

        engine.put(b"a", b"1").unwrap();
        engine.put(b"b", b"2").unwrap();
        engine.put(b"c", b"3").unwrap();
        engine.put(b"d", b"4").unwrap();

        let results: Vec<_> = engine.scan(b"b".to_vec()..b"d".to_vec()).unwrap().collect();

        assert_eq!(results.len(), 2);
        // Note: scan may not preserve exact order due to MVCC encoding
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
}
