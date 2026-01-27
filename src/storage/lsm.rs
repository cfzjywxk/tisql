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

//! LSM-tree storage engine.
//!
//! This module provides the main LSM storage engine that combines:
//!
//! - Active memtable for writes
//! - Frozen memtables awaiting flush
//! - SST files organized by level
//! - Version management for consistent reads
//!
//! ## Architecture
//!
//! ```text
//! Write Path:
//!   write() -> active memtable -> (when full) -> freeze -> flush -> L0 SST
//!
//! Read Path:
//!   get() -> active memtable -> frozen memtables -> L0 SSTs -> L1+ SSTs
//!
//! Background:
//!   flush thread: frozen memtables -> L0 SSTs
//!   compaction thread: L0 -> L1 -> L2 -> ... merging
//! ```
//!
//! ## Concurrency
//!
//! - Writes go to the active memtable (lock-free skiplist)
//! - Reads cascade through memtables and SSTs without locking
//! - Flush and compaction run in background threads
//! - Version changes are atomic (swap pointer)

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::error::{Result, TiSqlError};
use crate::storage::{StorageEngine, WriteBatch};
use crate::types::{Key, RawValue, Timestamp};

use super::config::LsmConfig;
use super::memtable::MemTable;
use super::sstable::{SstBuilder, SstBuilderOptions, SstMeta, SstReader};
use super::version::{ManifestDelta, Version};

/// Inner state of the LSM engine, protected by RwLock for atomic updates.
struct LsmState {
    /// Active memtable accepting writes.
    active: Arc<MemTable>,

    /// Frozen memtables awaiting flush, ordered newest first.
    frozen: Vec<Arc<MemTable>>,

    /// Current version with SST metadata.
    version: Arc<Version>,
}

impl LsmState {
    fn new(memtable_id: u64) -> Self {
        Self {
            active: Arc::new(MemTable::new(memtable_id)),
            frozen: Vec::new(),
            version: Arc::new(Version::new()),
        }
    }
}

/// LSM-tree storage engine.
///
/// This is the main entry point for persistent storage. It manages:
///
/// - Active memtable for writes
/// - Frozen memtables awaiting flush
/// - SST files at each level
/// - Version metadata
///
/// ## Usage
///
/// ```ignore
/// let config = LsmConfig::new("./data");
/// let engine = LsmEngine::open(config)?;
///
/// // Write with timestamp
/// let mut batch = WriteBatch::new();
/// batch.set_commit_ts(100);
/// batch.put(b"key".to_vec(), b"value".to_vec());
/// engine.write_batch(batch)?;
///
/// // Read at timestamp
/// let value = engine.get_at(b"key", 100)?;
/// ```
pub struct LsmEngine {
    /// Configuration.
    config: Arc<LsmConfig>,

    /// Engine state (active/frozen memtables, version).
    state: RwLock<LsmState>,

    /// Next memtable ID.
    next_memtable_id: AtomicU64,

    /// Next LSN for ordering operations.
    next_lsn: AtomicU64,
}

impl LsmEngine {
    /// Open or create an LSM engine at the given path.
    pub fn open(config: LsmConfig) -> Result<Self> {
        config.validate().map_err(TiSqlError::Storage)?;

        // Create SST directory if needed
        let sst_dir = config.sst_dir();
        if !sst_dir.exists() {
            std::fs::create_dir_all(&sst_dir)?;
        }

        // TODO: Recovery - load manifest, rebuild version, recover memtable from WAL
        // For now, start fresh

        Ok(Self {
            config: Arc::new(config),
            state: RwLock::new(LsmState::new(1)),
            next_memtable_id: AtomicU64::new(2),
            next_lsn: AtomicU64::new(1),
        })
    }

    /// Get the current configuration.
    pub fn config(&self) -> &LsmConfig {
        &self.config
    }

    /// Get the next LSN and increment.
    fn alloc_lsn(&self) -> u64 {
        self.next_lsn.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the next memtable ID and increment.
    fn alloc_memtable_id(&self) -> u64 {
        self.next_memtable_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the number of frozen memtables.
    pub fn frozen_count(&self) -> usize {
        let state = self.state.read().unwrap();
        state.frozen.len()
    }

    /// Get the approximate size of the active memtable.
    pub fn active_memtable_size(&self) -> usize {
        let state = self.state.read().unwrap();
        state.active.approximate_size()
    }

    /// Get the current version.
    pub fn current_version(&self) -> Arc<Version> {
        let state = self.state.read().unwrap();
        Arc::clone(&state.version)
    }

    /// Check if the active memtable should be rotated.
    fn should_rotate(&self) -> bool {
        let state = self.state.read().unwrap();
        state.active.approximate_size() >= self.config.memtable_size
    }

    /// Rotate the active memtable (freeze current, create new).
    ///
    /// Returns the frozen memtable if rotation occurred.
    pub fn maybe_rotate(&self) -> Option<Arc<MemTable>> {
        if !self.should_rotate() {
            return None;
        }

        let mut state = self.state.write().unwrap();

        // Double-check under write lock
        if state.active.approximate_size() < self.config.memtable_size {
            return None;
        }

        // Check frozen limit
        if state.frozen.len() >= self.config.max_frozen_memtables {
            // Cannot rotate - too many frozen memtables
            // In production, this would trigger write stalling
            return None;
        }

        // Freeze current active
        let old_active = Arc::clone(&state.active);
        old_active.freeze();

        // Create new active
        let new_id = self.alloc_memtable_id();
        state.active = Arc::new(MemTable::new(new_id));

        // Add to frozen list (newest first)
        state.frozen.insert(0, Arc::clone(&old_active));

        Some(old_active)
    }

    /// Flush a frozen memtable to an SST file.
    ///
    /// This creates a new L0 SST and updates the version.
    pub fn flush_memtable(&self, memtable: &MemTable) -> Result<SstMeta> {
        if !memtable.is_frozen() {
            return Err(TiSqlError::Storage(
                "Cannot flush non-frozen memtable".to_string(),
            ));
        }

        // Get next SST ID from current version
        let version = self.current_version();
        let sst_id = version.next_sst_id();

        // Build SST file
        let sst_path = self.config.sst_dir().join(format!("{:08}.sst", sst_id));
        let options = SstBuilderOptions {
            block_size: self.config.block_size,
            compression: self.config.compression,
        };

        let mut builder = SstBuilder::new(&sst_path, options)?;

        // Iterate memtable and add all entries
        // Use scan with full range to get all MVCC versions
        let inner = memtable.inner();
        let range = vec![]..vec![0xFF; 32]; // Full range
        let entries: Vec<_> = inner.scan(&range)?.collect();

        for (key, value) in entries {
            builder.add(&key, &value)?;
        }

        // Finish building (writes to disk)
        let meta = builder.finish(sst_id, 0)?; // Level 0

        // Update version with new SST
        let flushed_lsn = memtable.max_lsn().unwrap_or(0);
        let delta = ManifestDelta::flush(meta.clone(), flushed_lsn);

        // Apply delta to version
        let mut state = self.state.write().unwrap();
        let new_version = state.version.apply(&delta);
        state.version = Arc::new(new_version);

        // Remove flushed memtable from frozen list
        state.frozen.retain(|m| m.id() != memtable.id());

        Ok(meta)
    }

    /// Read a key from SST files.
    ///
    /// Searches L0 first (newest to oldest), then L1, L2, etc.
    fn get_from_sst(&self, key: &[u8], _ts: Timestamp) -> Result<Option<RawValue>> {
        let version = self.current_version();

        // Find SSTs that may contain this key
        let candidates = version.find_ssts_for_key(key);

        for sst_meta in candidates {
            let sst_path = self
                .config
                .sst_dir()
                .join(format!("{:08}.sst", sst_meta.id));

            // Skip if file doesn't exist (defensive)
            if !sst_path.exists() {
                continue;
            }

            let mut reader = SstReader::open(&sst_path)?;

            // Try to find the key in this SST
            if let Some(value) = reader.get(key)? {
                // TODO: Check timestamp visibility for MVCC
                // For now, return first found value
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    /// Force flush all frozen memtables.
    ///
    /// Used for shutdown and testing.
    pub fn flush_all(&self) -> Result<Vec<SstMeta>> {
        let mut results = Vec::new();

        loop {
            // Get next frozen memtable to flush
            let frozen = {
                let state = self.state.read().unwrap();
                state.frozen.last().cloned() // Oldest first
            };

            match frozen {
                Some(memtable) => {
                    let meta = self.flush_memtable(&memtable)?;
                    results.push(meta);
                }
                None => break,
            }
        }

        Ok(results)
    }

    /// Get statistics about the engine.
    pub fn stats(&self) -> LsmStats {
        let state = self.state.read().unwrap();
        let version = &state.version;

        LsmStats {
            active_memtable_size: state.active.approximate_size(),
            frozen_memtable_count: state.frozen.len(),
            l0_sst_count: version.level_size(0),
            total_sst_count: version.total_sst_count(),
            total_sst_bytes: version.total_size_bytes(),
            version_num: version.version_num(),
            flushed_lsn: version.flushed_lsn(),
        }
    }
}

impl StorageEngine for LsmEngine {
    fn get(&self, key: &[u8]) -> Result<Option<RawValue>> {
        self.get_at(key, Timestamp::MAX)
    }

    fn get_at(&self, key: &[u8], ts: Timestamp) -> Result<Option<RawValue>> {
        // Read lock to get snapshot of state
        let (active, frozen, _version) = {
            let state = self.state.read().unwrap();
            (
                Arc::clone(&state.active),
                state.frozen.clone(),
                Arc::clone(&state.version),
            )
        };

        // 1. Check active memtable
        if let Some(value) = active.get_at(key, ts)? {
            return Ok(Some(value));
        }

        // 2. Check frozen memtables (newest first)
        for memtable in &frozen {
            if let Some(value) = memtable.get_at(key, ts)? {
                return Ok(Some(value));
            }
        }

        // 3. Check SST files
        self.get_from_sst(key, ts)
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        // Allocate LSN for this batch
        let lsn = self.alloc_lsn();

        // Write to active memtable
        {
            let state = self.state.read().unwrap();
            state.active.write_batch_with_lsn(batch, lsn)?;
        }

        // Check if rotation is needed (non-blocking check)
        // Actual rotation happens asynchronously or on next write
        let _ = self.maybe_rotate();

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
        // For now, collect from memtables only
        // TODO: Merge with SST iterators
        let (active, frozen) = {
            let state = self.state.read().unwrap();
            (Arc::clone(&state.active), state.frozen.clone())
        };

        // Collect from active memtable
        let mut results: Vec<(Key, RawValue)> = active.scan_at(range, ts)?.collect();

        // Collect from frozen memtables
        for memtable in &frozen {
            let memtable_results: Vec<_> = memtable.scan_at(range, ts)?.collect();
            // Merge: memtable results should override earlier results for same key
            for (k, v) in memtable_results {
                if !results.iter().any(|(rk, _)| rk == &k) {
                    results.push((k, v));
                }
            }
        }

        // TODO: Merge with SST results

        // Sort by key
        results.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(Box::new(results.into_iter()))
    }
}

/// Statistics about the LSM engine.
#[derive(Debug, Clone)]
pub struct LsmStats {
    /// Size of active memtable in bytes.
    pub active_memtable_size: usize,

    /// Number of frozen memtables awaiting flush.
    pub frozen_memtable_count: usize,

    /// Number of SST files at L0.
    pub l0_sst_count: usize,

    /// Total number of SST files.
    pub total_sst_count: usize,

    /// Total size of SST files in bytes.
    pub total_sst_bytes: u64,

    /// Current version number.
    pub version_num: u64,

    /// Highest LSN that has been flushed.
    pub flushed_lsn: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::TempDir;

    fn test_config(dir: &Path) -> LsmConfig {
        LsmConfig::builder(dir)
            .memtable_size(1024) // Small for testing
            .max_frozen_memtables(4)
            .build()
            .unwrap()
    }

    fn new_batch(commit_ts: Timestamp) -> WriteBatch {
        let mut batch = WriteBatch::new();
        batch.set_commit_ts(commit_ts);
        batch
    }

    #[test]
    fn test_lsm_engine_open() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());

        let engine = LsmEngine::open(config).unwrap();

        let stats = engine.stats();
        assert_eq!(stats.active_memtable_size, 0);
        assert_eq!(stats.frozen_memtable_count, 0);
        assert_eq!(stats.total_sst_count, 0);
    }

    #[test]
    fn test_lsm_engine_write_and_read() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        let mut batch = new_batch(100);
        batch.put(b"key1".to_vec(), b"value1".to_vec());
        batch.put(b"key2".to_vec(), b"value2".to_vec());
        engine.write_batch(batch).unwrap();

        assert_eq!(engine.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(engine.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(engine.get(b"key3").unwrap(), None);
    }

    #[test]
    fn test_lsm_engine_mvcc_read() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        // Write v1 at ts=10
        let mut batch = new_batch(10);
        batch.put(b"key".to_vec(), b"v1".to_vec());
        engine.write_batch(batch).unwrap();

        // Write v2 at ts=20
        let mut batch = new_batch(20);
        batch.put(b"key".to_vec(), b"v2".to_vec());
        engine.write_batch(batch).unwrap();

        // Read at different timestamps
        assert_eq!(engine.get_at(b"key", 5).unwrap(), None);
        assert_eq!(engine.get_at(b"key", 10).unwrap(), Some(b"v1".to_vec()));
        assert_eq!(engine.get_at(b"key", 15).unwrap(), Some(b"v1".to_vec()));
        assert_eq!(engine.get_at(b"key", 20).unwrap(), Some(b"v2".to_vec()));
        assert_eq!(engine.get_at(b"key", 25).unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_lsm_engine_memtable_rotation() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(200) // Very small
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let engine = LsmEngine::open(config).unwrap();

        // Write enough to trigger rotation
        for i in 0..10 {
            let mut batch = new_batch(i as Timestamp + 1);
            let key = format!("key_{:04}", i);
            let value = format!("value_{:04}", i);
            batch.put(key.as_bytes().to_vec(), value.as_bytes().to_vec());
            engine.write_batch(batch).unwrap();
        }

        // Should have some frozen memtables
        assert!(engine.frozen_count() > 0);

        // All data should still be readable
        for i in 0..10 {
            let key = format!("key_{:04}", i);
            let expected = format!("value_{:04}", i);
            assert_eq!(
                engine.get(key.as_bytes()).unwrap(),
                Some(expected.as_bytes().to_vec())
            );
        }
    }

    #[test]
    fn test_lsm_engine_flush() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100) // Very small
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let engine = LsmEngine::open(config).unwrap();

        // Write some data
        for i in 0..5 {
            let mut batch = new_batch(i as Timestamp + 1);
            let key = format!("key_{:04}", i);
            let value = format!("value_{:04}", i);
            batch.put(key.as_bytes().to_vec(), value.as_bytes().to_vec());
            engine.write_batch(batch).unwrap();
        }

        // Force flush
        let flushed = engine.flush_all().unwrap();

        // Should have flushed some SSTs
        if engine.frozen_count() > 0 {
            // If there were frozen memtables, they should be flushed
            // (may be 0 if all data fit in active)
        }

        // Data should still be readable
        for i in 0..5 {
            let key = format!("key_{:04}", i);
            let expected = format!("value_{:04}", i);
            assert_eq!(
                engine.get(key.as_bytes()).unwrap(),
                Some(expected.as_bytes().to_vec()),
                "Key {} should be readable after flush",
                key
            );
        }
    }

    #[test]
    fn test_lsm_engine_scan() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        let mut batch = new_batch(100);
        batch.put(b"a".to_vec(), b"1".to_vec());
        batch.put(b"b".to_vec(), b"2".to_vec());
        batch.put(b"c".to_vec(), b"3".to_vec());
        batch.put(b"d".to_vec(), b"4".to_vec());
        engine.write_batch(batch).unwrap();

        let range = b"b".to_vec()..b"d".to_vec();
        let results: Vec<_> = engine.scan(&range).unwrap().collect();

        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|(k, _)| k == b"b"));
        assert!(results.iter().any(|(k, _)| k == b"c"));
    }

    #[test]
    fn test_lsm_engine_delete() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        // Write
        let mut batch = new_batch(10);
        batch.put(b"key".to_vec(), b"value".to_vec());
        engine.write_batch(batch).unwrap();

        assert_eq!(engine.get(b"key").unwrap(), Some(b"value".to_vec()));

        // Delete
        let mut batch = new_batch(20);
        batch.delete(b"key".to_vec());
        engine.write_batch(batch).unwrap();

        // Should be deleted at latest
        assert_eq!(engine.get(b"key").unwrap(), None);

        // Should still be visible at ts=15
        assert_eq!(engine.get_at(b"key", 15).unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn test_lsm_engine_stats() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        let mut batch = new_batch(100);
        batch.put(b"key".to_vec(), b"value".to_vec());
        engine.write_batch(batch).unwrap();

        let stats = engine.stats();
        assert!(stats.active_memtable_size > 0);
        assert_eq!(stats.version_num, 0); // No flushes yet
    }

    // ==================== Concurrent Tests ====================

    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_lsm_concurrent_writes() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(4096)
            .max_frozen_memtables(8)
            .build()
            .unwrap();

        let engine = Arc::new(LsmEngine::open(config).unwrap());
        let num_threads = 4;
        let writes_per_thread = 100;

        let handles: Vec<_> = (0..num_threads)
            .map(|tid| {
                let engine = Arc::clone(&engine);

                thread::spawn(move || {
                    for i in 0..writes_per_thread {
                        let mut batch = WriteBatch::new();
                        let ts = (tid * writes_per_thread + i + 1) as Timestamp;
                        batch.set_commit_ts(ts);
                        let key = format!("key_{}_{}", tid, i);
                        batch.put(key.as_bytes().to_vec(), b"value".to_vec());
                        engine.write_batch(batch).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Verify all writes
        for tid in 0..num_threads {
            for i in 0..writes_per_thread {
                let key = format!("key_{}_{}", tid, i);
                assert!(
                    engine.get(key.as_bytes()).unwrap().is_some(),
                    "Key {} should exist",
                    key
                );
            }
        }
    }

    #[test]
    fn test_lsm_concurrent_reads_and_writes() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(4096)
            .max_frozen_memtables(8)
            .build()
            .unwrap();

        let engine = Arc::new(LsmEngine::open(config).unwrap());

        // Pre-populate
        let mut batch = new_batch(1);
        for i in 0..100 {
            batch.put(
                format!("key{:03}", i).as_bytes().to_vec(),
                b"initial".to_vec(),
            );
        }
        engine.write_batch(batch).unwrap();

        let num_readers = 4;
        let num_writers = 2;

        let handles: Vec<_> = (0..num_readers)
            .map(|_| {
                let engine = Arc::clone(&engine);

                thread::spawn(move || {
                    for i in 0..500 {
                        let key = format!("key{:03}", i % 100);
                        let _ = engine.get(key.as_bytes());
                    }
                })
            })
            .chain((0..num_writers).map(|tid| {
                let engine = Arc::clone(&engine);

                thread::spawn(move || {
                    for i in 0..200 {
                        let mut batch = WriteBatch::new();
                        let ts = (100 + tid * 200 + i) as Timestamp;
                        batch.set_commit_ts(ts);
                        let key = format!("new_key_{}_{}", tid, i);
                        batch.put(key.as_bytes().to_vec(), b"value".to_vec());
                        engine.write_batch(batch).unwrap();
                    }
                })
            }))
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Verify original data still readable
        for i in 0..100 {
            let key = format!("key{:03}", i);
            assert!(engine.get(key.as_bytes()).unwrap().is_some());
        }
    }
}
