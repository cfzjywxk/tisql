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

//! MemTable wrapper with LSM-tree metadata.
//!
//! This module provides a wrapper around the underlying memtable engine
//! that tracks LSM-specific metadata:
//!
//! - Unique memtable ID for ordering
//! - Approximate size for flush decisions
//! - LSN range for recovery
//! - Creation time for age-based policies
//!
//! ## Lifecycle
//!
//! ```text
//! Active MemTable -> Frozen MemTable -> Flushed to SST -> Deleted
//! ```
//!
//! Only the active memtable accepts writes. When it reaches the size threshold,
//! it is frozen and a new active memtable is created. Frozen memtables are
//! flushed to SST files asynchronously.

use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use crate::error::Result;
use crate::storage::mvcc::MvccKey;
use crate::storage::{StorageEngine, WriteBatch};
use crate::types::RawValue;

use super::VersionedMemTableEngine;

/// MemTable wrapper with LSM metadata.
///
/// This wraps the underlying memtable engine and tracks additional metadata
/// needed for LSM-tree operations:
///
/// - `id`: Unique identifier for ordering memtables
/// - `approximate_size`: Estimated memory usage for flush decisions
/// - `min_lsn`/`max_lsn`: LSN range for recovery
/// - `created_at`: Creation time for age-based policies
pub struct MemTable {
    /// Unique memtable ID (monotonically increasing)
    id: u64,

    /// Underlying memtable engine (OceanBase-style versioned memtable)
    inner: VersionedMemTableEngine,

    /// Approximate memory size in bytes.
    ///
    /// This is an estimate based on key/value sizes plus overhead.
    /// Used to decide when to freeze the memtable.
    approximate_size: AtomicUsize,

    /// Minimum LSN (Log Sequence Number) in this memtable.
    ///
    /// Set on first write. Used during recovery to determine
    /// which WAL entries have been persisted.
    min_lsn: AtomicU64,

    /// Maximum LSN in this memtable.
    ///
    /// Updated on each write. Used for recovery ordering.
    max_lsn: AtomicU64,

    /// Whether this memtable is frozen (read-only).
    ///
    /// Once frozen, no more writes are accepted.
    frozen: AtomicBool,

    /// Creation time for age-based policies.
    created_at: Instant,
}

impl MemTable {
    /// Create a new active memtable with the given ID.
    ///
    /// The memtable starts empty and accepts writes until frozen.
    pub fn new(id: u64) -> Self {
        Self {
            id,
            inner: VersionedMemTableEngine::new(),
            approximate_size: AtomicUsize::new(0),
            min_lsn: AtomicU64::new(u64::MAX),
            max_lsn: AtomicU64::new(0),
            frozen: AtomicBool::new(false),
            created_at: Instant::now(),
        }
    }

    /// Get the memtable ID.
    #[inline]
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the approximate memory size in bytes.
    #[inline]
    pub fn approximate_size(&self) -> usize {
        self.approximate_size.load(Ordering::Relaxed)
    }

    /// Get the minimum LSN, or None if no writes have been made.
    pub fn min_lsn(&self) -> Option<u64> {
        let lsn = self.min_lsn.load(Ordering::Acquire);
        if lsn == u64::MAX {
            None
        } else {
            Some(lsn)
        }
    }

    /// Get the maximum LSN, or None if no writes have been made.
    pub fn max_lsn(&self) -> Option<u64> {
        let lsn = self.max_lsn.load(Ordering::Acquire);
        if lsn == 0 && self.min_lsn.load(Ordering::Acquire) == u64::MAX {
            None
        } else {
            Some(lsn)
        }
    }

    /// Check if this memtable is frozen (read-only).
    #[inline]
    pub fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::Acquire)
    }

    /// Freeze this memtable, making it read-only.
    ///
    /// After freezing, no more writes are accepted. The memtable
    /// can still be read from until it is flushed to SST.
    pub fn freeze(&self) {
        self.frozen.store(true, Ordering::Release);
    }

    /// Get the creation time.
    #[inline]
    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    /// Get the age of this memtable.
    #[inline]
    pub fn age(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    /// Get the number of entries in the memtable.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check if the memtable is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Write a batch of operations with the given LSN.
    ///
    /// Returns an error if the memtable is frozen.
    pub fn write_batch_with_lsn(&self, batch: WriteBatch, lsn: u64) -> Result<()> {
        if self.is_frozen() {
            return Err(crate::error::TiSqlError::Storage(
                "Cannot write to frozen memtable".to_string(),
            ));
        }

        // Estimate size before write
        let size_delta = estimate_batch_size(&batch);

        // Perform the write
        self.inner.write_batch(batch)?;

        // Update metadata
        self.approximate_size
            .fetch_add(size_delta, Ordering::Relaxed);

        // Update LSN range atomically
        // For min_lsn, we want the smallest value seen
        let mut current = self.min_lsn.load(Ordering::Relaxed);
        while lsn < current {
            match self.min_lsn.compare_exchange_weak(
                current,
                lsn,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }

        // For max_lsn, we want the largest value seen
        let mut current = self.max_lsn.load(Ordering::Relaxed);
        while lsn > current {
            match self.max_lsn.compare_exchange_weak(
                current,
                lsn,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }

        Ok(())
    }

    /// Get the underlying engine reference for iteration.
    ///
    /// This is used during flush to iterate over all entries.
    pub fn inner(&self) -> &VersionedMemTableEngine {
        &self.inner
    }
}

/// Estimate the memory size of a write batch.
///
/// This is a rough estimate: key size + value size + overhead per entry.
fn estimate_batch_size(batch: &WriteBatch) -> usize {
    // VersionedMemTableEngine has different overhead than crossbeam:
    // - ~64 bytes for VersionNode (ts, value vec, next pointer)
    // - Skiplist node overhead shared across versions of same key
    const ENTRY_OVERHEAD: usize = 64;

    batch
        .iter()
        .map(|(key, op)| match op {
            crate::storage::WriteOp::Put { value } => key.len() + value.len() + ENTRY_OVERHEAD,
            crate::storage::WriteOp::Delete => key.len() + ENTRY_OVERHEAD,
        })
        .sum()
}

// Implement StorageEngine for MemTable to allow transparent use
impl StorageEngine for MemTable {
    fn scan(&self, range: Range<MvccKey>) -> Result<Vec<(MvccKey, RawValue)>> {
        self.inner.scan(range)
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        // Use clog_lsn from batch if available, otherwise use 0 (for testing/recovery scenarios).
        // Note: Production code should use write_batch_with_lsn() directly via LsmEngine
        // to ensure proper LSN tracking for recovery ordering.
        let lsn = batch.clog_lsn().unwrap_or(0);
        self.write_batch_with_lsn(batch, lsn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mvcc::is_tombstone;
    use crate::types::{Key, Timestamp};

    fn new_batch(commit_ts: Timestamp) -> WriteBatch {
        let mut batch = WriteBatch::new();
        batch.set_commit_ts(commit_ts);
        batch
    }

    // ==================== Test Helpers Using MvccKey ====================

    /// Get the latest version of a key visible at the given timestamp.
    fn get_at_for_test(mt: &MemTable, key: &[u8], ts: Timestamp) -> Option<RawValue> {
        let start = MvccKey::encode(key, ts);
        let end = MvccKey::encode(key, 0)
            .next_key()
            .unwrap_or_else(MvccKey::unbounded);
        let range = start..end;

        let results = mt.scan(range).unwrap();

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

    fn get_for_test(mt: &MemTable, key: &[u8]) -> Option<RawValue> {
        get_at_for_test(mt, key, Timestamp::MAX)
    }

    fn scan_for_test(mt: &MemTable, range: &Range<Key>) -> Vec<(Key, RawValue)> {
        let start = MvccKey::encode(&range.start, Timestamp::MAX);
        let end = MvccKey::encode(&range.end, 0);
        let mvcc_range = start..end;

        let results = mt.scan(mvcc_range).unwrap();

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
    fn test_memtable_new() {
        let mt = MemTable::new(1);
        assert_eq!(mt.id(), 1);
        assert_eq!(mt.approximate_size(), 0);
        assert_eq!(mt.min_lsn(), None);
        assert_eq!(mt.max_lsn(), None);
        assert!(!mt.is_frozen());
        assert!(mt.is_empty());
    }

    #[test]
    fn test_memtable_write_and_read() {
        let mt = MemTable::new(1);

        let mut batch = new_batch(100);
        batch.put(b"key1".to_vec(), b"value1".to_vec());
        batch.put(b"key2".to_vec(), b"value2".to_vec());

        mt.write_batch_with_lsn(batch, 1).unwrap();

        assert_eq!(get_for_test(&mt, b"key1"), Some(b"value1".to_vec()));
        assert_eq!(get_for_test(&mt, b"key2"), Some(b"value2".to_vec()));
        assert!(mt.approximate_size() > 0);
        assert_eq!(mt.len(), 2);
    }

    #[test]
    fn test_memtable_lsn_tracking() {
        let mt = MemTable::new(1);

        // First write at LSN 5
        let mut batch = new_batch(100);
        batch.put(b"k1".to_vec(), b"v1".to_vec());
        mt.write_batch_with_lsn(batch, 5).unwrap();

        assert_eq!(mt.min_lsn(), Some(5));
        assert_eq!(mt.max_lsn(), Some(5));

        // Second write at LSN 10
        let mut batch = new_batch(101);
        batch.put(b"k2".to_vec(), b"v2".to_vec());
        mt.write_batch_with_lsn(batch, 10).unwrap();

        assert_eq!(mt.min_lsn(), Some(5));
        assert_eq!(mt.max_lsn(), Some(10));

        // Third write at LSN 3 (out of order)
        let mut batch = new_batch(102);
        batch.put(b"k3".to_vec(), b"v3".to_vec());
        mt.write_batch_with_lsn(batch, 3).unwrap();

        assert_eq!(mt.min_lsn(), Some(3));
        assert_eq!(mt.max_lsn(), Some(10));
    }

    #[test]
    fn test_memtable_freeze() {
        let mt = MemTable::new(1);

        let mut batch = new_batch(100);
        batch.put(b"key1".to_vec(), b"value1".to_vec());
        mt.write_batch_with_lsn(batch, 1).unwrap();

        // Freeze the memtable
        mt.freeze();
        assert!(mt.is_frozen());

        // Reads should still work
        assert_eq!(get_for_test(&mt, b"key1"), Some(b"value1".to_vec()));

        // Writes should fail
        let mut batch = new_batch(101);
        batch.put(b"key2".to_vec(), b"value2".to_vec());
        let result = mt.write_batch_with_lsn(batch, 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_memtable_write_batch_trait() {
        let mt = MemTable::new(1);

        let mut batch = new_batch(100);
        batch.put(b"key".to_vec(), b"value".to_vec());

        // Use the StorageEngine trait method
        mt.write_batch(batch).unwrap();

        assert_eq!(get_for_test(&mt, b"key"), Some(b"value".to_vec()));
        assert!(mt.approximate_size() > 0);
    }

    #[test]
    fn test_memtable_mvcc_read() {
        let mt = MemTable::new(1);

        // Write version at ts=10
        let mut batch = new_batch(10);
        batch.put(b"key".to_vec(), b"v1".to_vec());
        mt.write_batch_with_lsn(batch, 1).unwrap();

        // Write version at ts=20
        let mut batch = new_batch(20);
        batch.put(b"key".to_vec(), b"v2".to_vec());
        mt.write_batch_with_lsn(batch, 2).unwrap();

        // Read at different timestamps
        assert_eq!(get_at_for_test(&mt, b"key", 5), None);
        assert_eq!(get_at_for_test(&mt, b"key", 10), Some(b"v1".to_vec()));
        assert_eq!(get_at_for_test(&mt, b"key", 15), Some(b"v1".to_vec()));
        assert_eq!(get_at_for_test(&mt, b"key", 20), Some(b"v2".to_vec()));
        assert_eq!(get_at_for_test(&mt, b"key", 25), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_memtable_scan() {
        let mt = MemTable::new(1);

        let mut batch = new_batch(100);
        batch.put(b"a".to_vec(), b"1".to_vec());
        batch.put(b"b".to_vec(), b"2".to_vec());
        batch.put(b"c".to_vec(), b"3".to_vec());
        batch.put(b"d".to_vec(), b"4".to_vec());
        mt.write_batch_with_lsn(batch, 1).unwrap();

        let range = b"b".to_vec()..b"d".to_vec();
        let results = scan_for_test(&mt, &range);

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_memtable_age() {
        let mt = MemTable::new(1);

        // Sleep briefly to ensure age is measurable
        std::thread::sleep(std::time::Duration::from_millis(10));

        assert!(mt.age() >= std::time::Duration::from_millis(10));
    }

    #[test]
    fn test_estimate_batch_size() {
        let mut batch = new_batch(100);
        batch.put(b"key".to_vec(), b"value".to_vec()); // 3 + 5 = 8 bytes + overhead
        batch.put(b"longer_key".to_vec(), b"longer_value".to_vec()); // 10 + 12 = 22 bytes + overhead

        let size = estimate_batch_size(&batch);
        // Each entry has 64 bytes overhead, so:
        // (3 + 5 + 64) + (10 + 12 + 64) = 72 + 86 = 158
        assert!(size >= 8 + 22, "Size should include key/value bytes");
        assert!(size > 8 + 22, "Size should include overhead");
    }

    #[test]
    fn test_memtable_delete() {
        let mt = MemTable::new(1);

        // Write a value
        let mut batch = new_batch(10);
        batch.put(b"key".to_vec(), b"value".to_vec());
        mt.write_batch_with_lsn(batch, 1).unwrap();

        assert_eq!(get_for_test(&mt, b"key"), Some(b"value".to_vec()));

        // Delete the key
        let mut batch = new_batch(20);
        batch.delete(b"key".to_vec());
        mt.write_batch_with_lsn(batch, 2).unwrap();

        // Should be deleted at latest
        assert_eq!(get_for_test(&mt, b"key"), None);

        // Should still be visible at ts=15
        assert_eq!(get_at_for_test(&mt, b"key", 15), Some(b"value".to_vec()));
    }

    // ==================== Concurrent Tests ====================

    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn test_concurrent_writes() {
        let mt = Arc::new(MemTable::new(1));
        let num_threads = 4;
        let writes_per_thread = 1000;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|tid| {
                let mt = Arc::clone(&mt);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();

                    for i in 0..writes_per_thread {
                        let mut batch = WriteBatch::new();
                        let ts = (tid * writes_per_thread + i + 1) as Timestamp;
                        batch.set_commit_ts(ts);
                        let key = format!("key_{tid}_{i}");
                        batch.put(key.as_bytes().to_vec(), b"value".to_vec());

                        let lsn = (tid * writes_per_thread + i) as u64;
                        mt.write_batch_with_lsn(batch, lsn).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Verify all writes
        assert_eq!(mt.len(), num_threads * writes_per_thread);
        assert!(mt.approximate_size() > 0);
    }

    #[test]
    fn test_concurrent_reads_and_writes() {
        let mt = Arc::new(MemTable::new(1));

        // Pre-populate
        let mut batch = WriteBatch::new();
        batch.set_commit_ts(1);
        for i in 0..100 {
            batch.put(
                format!("key{i:03}").as_bytes().to_vec(),
                b"initial".to_vec(),
            );
        }
        mt.write_batch_with_lsn(batch, 0).unwrap();

        let num_readers = 4;
        let num_writers = 2;
        let barrier = Arc::new(Barrier::new(num_readers + num_writers));

        let handles: Vec<_> = (0..num_readers)
            .map(|_| {
                let mt = Arc::clone(&mt);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();

                    for i in 0..1000 {
                        let key = format!("key{:03}", i % 100);
                        let _ = get_for_test(&mt, key.as_bytes());
                    }
                })
            })
            .chain((0..num_writers).map(|tid| {
                let mt = Arc::clone(&mt);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();

                    for i in 0..500 {
                        let mut batch = WriteBatch::new();
                        let ts = (100 + tid * 500 + i) as Timestamp;
                        batch.set_commit_ts(ts);
                        let key = format!("new_key_{tid}_{i}");
                        batch.put(key.as_bytes().to_vec(), b"value".to_vec());

                        let lsn = (1 + tid * 500 + i) as u64;
                        mt.write_batch_with_lsn(batch, lsn).unwrap();
                    }
                })
            }))
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_concurrent_lsn_updates() {
        let mt = Arc::new(MemTable::new(1));
        let num_threads = 8;
        let writes_per_thread = 100;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|tid| {
                let mt = Arc::clone(&mt);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();

                    for i in 0..writes_per_thread {
                        let mut batch = WriteBatch::new();
                        let ts = (tid * writes_per_thread + i + 1) as Timestamp;
                        batch.set_commit_ts(ts);
                        batch.put(format!("k_{tid}_{i}").as_bytes().to_vec(), b"v".to_vec());

                        // Use varying LSNs to test min/max tracking
                        let lsn = ((tid * writes_per_thread + i) * 2 + 1) as u64;
                        mt.write_batch_with_lsn(batch, lsn).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Verify LSN tracking
        let min = mt.min_lsn().unwrap();
        let max = mt.max_lsn().unwrap();
        assert!(min <= max);
        // Min should be 1 (first thread, first write: 0 * 2 + 1 = 1)
        assert_eq!(min, 1);
    }
}
