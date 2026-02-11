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

//! Compaction for LSM-tree storage engine.
//!
//! Compaction merges SST files to reduce read amplification and reclaim space
//! from deleted/overwritten keys. TiSQL uses leveled compaction:
//!
//! ## Strategy
//!
//! ```text
//! L0: Recent flushes, may overlap (triggered by file count)
//! L1: 10x L0 size limit, non-overlapping within level
//! L2: 10x L1 size limit, non-overlapping within level
//! ...
//! ```
//!
//! ## Compaction Types
//!
//! 1. **L0 -> L1**: Triggered when L0 file count exceeds threshold.
//!    Picks all L0 files and overlapping L1 files, merges into new L1 files.
//!
//! 2. **Ln -> Ln+1**: Triggered when level size exceeds threshold.
//!    Picks one file from Ln, finds overlapping Ln+1 files, merges.
//!
//! ## Key Merging Rules
//!
//! For MVCC, all versions of a key are kept until they're older than
//! the oldest active read timestamp (GC safe point).

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::Path;
use std::sync::Arc;

#[cfg(feature = "failpoints")]
use fail::fail_point;

use crate::error::Result;
use crate::types::RawValue;

use super::config::LsmConfig;
use super::sstable::{SstBuilder, SstBuilderOptions, SstIterator, SstMeta, SstReaderRef};
use super::version::{ManifestDelta, Version};

/// A compaction task describing which files to compact.
#[derive(Debug, Clone)]
pub struct CompactionTask {
    /// Input SST files (level, sst_id).
    pub inputs: Vec<(u32, u64)>,

    /// Output level for compacted files.
    pub output_level: u32,

    /// Whether this is a trivial move (no merge needed).
    pub is_trivial_move: bool,
}

/// Compaction picker selects files for compaction.
pub struct CompactionPicker {
    config: Arc<LsmConfig>,
}

impl CompactionPicker {
    /// Create a new compaction picker.
    pub fn new(config: Arc<LsmConfig>) -> Self {
        Self { config }
    }

    /// Pick the next compaction task based on current version state.
    ///
    /// Uses RocksDB-style score-based prioritization: computes a score for each
    /// level, sorts by score descending, and picks the highest-scoring level
    /// with score >= 1.0. This ensures the most urgent compaction runs first.
    ///
    /// Returns None if no compaction is needed (all scores < 1.0).
    pub fn pick(&self, version: &Version) -> Option<CompactionTask> {
        let scores = self.compute_compaction_scores(version);

        // Find highest-scoring level with score >= 1.0
        for (level, score) in &scores {
            if *score < 1.0 {
                break; // Sorted descending, so no more candidates
            }
            if *level == 0 {
                if let Some(task) = self.pick_l0_compaction(version) {
                    return Some(task);
                }
            } else if let Some(task) = self.pick_level_compaction(version, *level) {
                return Some(task);
            }
        }

        None
    }

    /// Compute compaction scores for all levels.
    ///
    /// Returns a list of (level, score) sorted by score descending.
    /// Score >= 1.0 means compaction is needed. Higher score = more urgent.
    ///
    /// - L0: `score = max(file_count / trigger, total_bytes / l1_max_size)`
    /// - Ln (n>=1): `score = level_size_bytes / level_max_size(n)`
    pub fn compute_compaction_scores(&self, version: &Version) -> Vec<(usize, f64)> {
        let mut scores = Vec::new();

        // L0 score: based on file count and size
        let l0_count = version.level_size(0);
        let file_score = l0_count as f64 / self.config.l0_compaction_trigger as f64;
        let l0_bytes = version.level_size_bytes(0);
        let size_score = if self.config.l1_max_size > 0 {
            l0_bytes as f64 / self.config.l1_max_size as f64
        } else {
            0.0
        };
        let l0_score = file_score.max(size_score);
        scores.push((0, l0_score));

        // Ln scores (n >= 1): based on level size vs max size
        for level in 1..self.config.max_levels - 1 {
            let level_bytes = version.level_size_bytes(level);
            let max_bytes = self.config.level_max_size(level) as u64;
            let score = if max_bytes > 0 {
                level_bytes as f64 / max_bytes as f64
            } else {
                0.0
            };
            scores.push((level, score));
        }

        // Sort by score descending
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        scores
    }

    /// Pick L0 -> L1 compaction.
    fn pick_l0_compaction(&self, version: &Version) -> Option<CompactionTask> {
        let l0_files = version.level(0);
        if l0_files.is_empty() {
            return None;
        }

        // Collect all L0 files
        let mut inputs: Vec<(u32, u64)> = l0_files.iter().map(|sst| (0, sst.id)).collect();

        // Find MVCC key range covered by L0 files
        let (min_mvcc_key, max_mvcc_key) = self.get_key_range(l0_files)?;

        // Find overlapping L1 files using MVCC key ranges directly.
        // Each MVCC key is an independent key in the storage engine.
        let l1_overlapping =
            version.find_overlapping_at_level_mvcc(1, &min_mvcc_key, &max_mvcc_key);
        for sst in l1_overlapping {
            inputs.push((1, sst.id));
        }

        Some(CompactionTask {
            inputs,
            output_level: 1,
            is_trivial_move: false,
        })
    }

    /// Pick Ln -> Ln+1 compaction.
    fn pick_level_compaction(&self, version: &Version, level: usize) -> Option<CompactionTask> {
        let level_files = version.level(level);
        if level_files.is_empty() {
            return None;
        }

        // Pick the oldest file (lowest ID = oldest)
        let target = level_files.iter().min_by_key(|sst| sst.id)?;

        let mut inputs = vec![(level as u32, target.id)];

        // Find overlapping files at next level using MVCC key ranges directly.
        // Each MVCC key is an independent key in the storage engine.
        let next_level = level + 1;
        if next_level < self.config.max_levels {
            let overlapping = version.find_overlapping_at_level_mvcc(
                next_level,
                &target.smallest_key,
                &target.largest_key,
            );

            // Check for trivial move (no overlap with next level)
            if overlapping.is_empty() {
                return Some(CompactionTask {
                    inputs,
                    output_level: next_level as u32,
                    is_trivial_move: true,
                });
            }

            for sst in overlapping {
                inputs.push((next_level as u32, sst.id));
            }
        }

        Some(CompactionTask {
            inputs,
            output_level: next_level as u32,
            is_trivial_move: false,
        })
    }

    /// Get the key range covered by a set of SST files.
    fn get_key_range(&self, files: &[Arc<SstMeta>]) -> Option<(Vec<u8>, Vec<u8>)> {
        if files.is_empty() {
            return None;
        }

        let min_key = files.iter().map(|sst| &sst.smallest_key).min()?.clone();

        let max_key = files.iter().map(|sst| &sst.largest_key).max()?.clone();

        Some((min_key, max_key))
    }
}

/// Entry in the merge iterator priority queue.
struct MergeEntry {
    key: Vec<u8>,
    value: RawValue,
    /// Source index (for tie-breaking: lower index = newer)
    source_idx: usize,
}

impl Eq for MergeEntry {}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.source_idx == other.source_idx
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap: smaller key comes first
        // For equal keys, smaller source_idx (newer file) comes first
        match other.key.cmp(&self.key) {
            Ordering::Equal => other.source_idx.cmp(&self.source_idx),
            other => other,
        }
    }
}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Merge iterator that combines multiple SST iterators.
///
/// Outputs all MVCC keys in sorted order. Each MVCC key (key || !ts) is
/// treated as an independent key. The caller is responsible for filtering
/// based on GC safe point if needed.
pub struct MergeIterator {
    /// Source iterators
    iters: Vec<SstIterator>,

    /// Priority queue for merging
    heap: BinaryHeap<MergeEntry>,

    /// Last key output (for deduplication if needed)
    last_key: Option<Vec<u8>>,
}

impl MergeIterator {
    /// Create a new merge iterator from SST readers.
    ///
    /// This is a pure construction operation - no I/O or seeking is performed.
    /// Call `seek_to_first()` after construction to position the iterator.
    ///
    /// Readers should be ordered from newest to oldest for correct
    /// tie-breaking behavior.
    pub fn new(readers: Vec<SstReaderRef>) -> Result<Self> {
        let mut iters = Vec::with_capacity(readers.len());
        for r in readers {
            let iter = SstIterator::new(r)?;
            iters.push(iter);
        }

        Ok(Self {
            iters,
            heap: BinaryHeap::new(),
            last_key: None,
        })
    }

    /// Position the iterator at the first entry.
    ///
    /// This must be called after `new()` before accessing entries.
    pub fn seek_to_first(&mut self) -> Result<()> {
        // Position all child iterators
        for iter in &mut self.iters {
            iter.seek_to_first()?;
        }

        // Initialize heap with first entry from each valid iterator
        self.heap.clear();
        for (idx, iter) in self.iters.iter().enumerate() {
            if iter.valid() {
                let key = iter.key().to_vec();
                let value = iter.value().to_vec();
                self.heap.push(MergeEntry {
                    key,
                    value,
                    source_idx: idx,
                });
            }
        }

        Ok(())
    }

    /// Check if the iterator has more entries.
    pub fn valid(&self) -> bool {
        !self.heap.is_empty()
    }

    /// Get the current key-value pair.
    pub fn current(&self) -> Option<(Vec<u8>, RawValue)> {
        self.heap.peek().map(|e| (e.key.clone(), e.value.clone()))
    }

    /// Advance to the next entry.
    pub fn advance(&mut self) -> Result<()> {
        if let Some(entry) = self.heap.pop() {
            let idx = entry.source_idx;
            self.last_key = Some(entry.key);

            // Advance the source iterator and push next entry
            self.iters[idx].advance()?;
            if self.iters[idx].valid() {
                let key = self.iters[idx].key().to_vec();
                let value = self.iters[idx].value().to_vec();
                self.heap.push(MergeEntry {
                    key,
                    value,
                    source_idx: idx,
                });
            }
        }
        Ok(())
    }

    /// Skip entries that share the same key prefix as the last output.
    ///
    /// For MVCC keys (key || !ts), this skips all versions of the same key.
    /// Useful for GC when you only want to keep the latest version.
    ///
    /// Note: This method extracts the key portion (removes timestamp suffix)
    /// to compare key prefixes.
    #[allow(dead_code)]
    pub fn skip_to_next_key(&mut self) -> Result<()> {
        let last_key_prefix = match &self.last_key {
            Some(k) if k.len() >= 8 => k[..k.len() - 8].to_vec(),
            _ => return Ok(()),
        };

        loop {
            let should_skip = match self.heap.peek() {
                Some(entry) if entry.key.len() >= 8 => {
                    let key_prefix = &entry.key[..entry.key.len() - 8];
                    key_prefix == last_key_prefix.as_slice()
                }
                _ => false,
            };

            if should_skip {
                self.advance()?;
            } else {
                break;
            }
        }
        Ok(())
    }
}

/// Execute a compaction task.
///
/// This reads from input SST files, merges them, and writes new SST files.
pub struct CompactionExecutor {
    config: Arc<LsmConfig>,
}

impl CompactionExecutor {
    /// Create a new compaction executor.
    pub fn new(config: Arc<LsmConfig>) -> Self {
        Self { config }
    }

    /// Execute a compaction task.
    ///
    /// `pre_allocated_ids` contains SST IDs allocated by the caller (LsmEngine)
    /// for crash-safe compaction. These IDs are recorded in CompactIntent before
    /// execution begins, enabling orphan cleanup on crash recovery.
    ///
    /// Returns the manifest delta to apply on success.
    pub fn execute(
        &self,
        task: &CompactionTask,
        version: &Version,
        sst_dir: &Path,
        pre_allocated_ids: &[u64],
        io: std::sync::Arc<crate::io::IoService>,
    ) -> Result<ManifestDelta> {
        // Handle trivial move (just update metadata, no I/O)
        if task.is_trivial_move {
            return self.execute_trivial_move(task, version);
        }

        if pre_allocated_ids.is_empty() {
            return Err(crate::error::TiSqlError::Storage(
                "pre_allocated_ids must not be empty for non-trivial compaction".to_string(),
            ));
        }

        // Collect input SST readers
        let mut readers = Vec::new();
        for (_level, sst_id) in &task.inputs {
            let sst_path = sst_dir.join(format!("{sst_id:08}.sst"));
            let reader = SstReaderRef::open(&sst_path, std::sync::Arc::clone(&io))?;
            readers.push(reader);
        }

        // Create merge iterator
        // Failpoint: crash before merge iterator creation
        #[cfg(feature = "failpoints")]
        fail_point!("compaction_before_merge");

        let mut merge_iter = MergeIterator::new(readers)?;
        merge_iter.seek_to_first()?;

        // Create output SST builder using pre-allocated IDs
        let mut id_idx = 0;
        let first_id = pre_allocated_ids[id_idx];
        let output_path = sst_dir.join(format!("{first_id:08}.sst"));
        let options = SstBuilderOptions {
            block_size: self.config.block_size,
            compression: self.config.compression,
        };
        let mut builder = SstBuilder::new(&output_path, options.clone())?;

        // Merge and write output
        let mut output_ssts = Vec::new();
        let mut current_size = 0usize;

        while merge_iter.valid() {
            if let Some((key, value)) = merge_iter.current() {
                // Failpoint: crash mid compaction write
                #[cfg(feature = "failpoints")]
                fail_point!("compaction_mid_write");

                builder.add(&key, &value)?;
                current_size += key.len() + value.len();

                // Check if we should split to a new SST
                if current_size >= self.config.target_file_size {
                    let sst_id = pre_allocated_ids[id_idx];
                    let meta = builder.finish(sst_id, task.output_level)?;
                    output_ssts.push(meta);
                    id_idx += 1;

                    // Start new output file
                    if id_idx >= pre_allocated_ids.len() {
                        return Err(crate::error::TiSqlError::Storage(
                            "Ran out of pre-allocated SST IDs during compaction".to_string(),
                        ));
                    }
                    let new_id = pre_allocated_ids[id_idx];
                    let new_path = sst_dir.join(format!("{new_id:08}.sst"));
                    builder = SstBuilder::new(&new_path, options.clone())?;
                    current_size = 0;
                }
            }
            merge_iter.advance()?;
        }

        // Finish last output file
        if current_size > 0 || output_ssts.is_empty() {
            let sst_id = pre_allocated_ids[id_idx];
            let meta = builder.finish(sst_id, task.output_level)?;
            output_ssts.push(meta);
        } else {
            builder.abort()?;
        }

        // Failpoint: crash after all SSTs written, before commit
        #[cfg(feature = "failpoints")]
        fail_point!("compaction_after_finish");

        // Build manifest delta
        let delta = ManifestDelta::compaction(output_ssts, task.inputs.clone());

        Ok(delta)
    }

    /// Execute a trivial move (no I/O, just metadata update).
    fn execute_trivial_move(
        &self,
        task: &CompactionTask,
        version: &Version,
    ) -> Result<ManifestDelta> {
        // Find the input SST and create a copy with updated level
        let (input_level, input_id) = task.inputs[0];

        let level_files = version.level(input_level as usize);
        let input_meta = level_files
            .iter()
            .find(|sst| sst.id == input_id)
            .ok_or_else(|| {
                crate::error::TiSqlError::Storage(format!("SST {input_id} not found"))
            })?;

        // Create new metadata with updated level
        let mut new_meta = (**input_meta).clone();
        new_meta.level = task.output_level;

        // Note: In a real implementation, we'd rename/move the file
        // For now, we just update metadata and the file stays in place

        let mut delta = ManifestDelta::new();
        delta.add_sst(new_meta);
        delta.delete_sst(input_level, input_id);

        Ok(delta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sstable::SstBuilder;
    use tempfile::TempDir;

    fn test_io() -> Arc<crate::io::IoService> {
        crate::io::IoService::new(32).unwrap()
    }

    fn test_config(dir: &Path) -> Arc<LsmConfig> {
        Arc::new(
            LsmConfig::builder(dir)
                .memtable_size(1024)
                .l0_compaction_trigger(4)
                .target_file_size(1024)
                .build()
                .unwrap(),
        )
    }

    fn make_sst(id: u64, level: u32, smallest: &[u8], largest: &[u8]) -> SstMeta {
        SstMeta {
            id,
            level,
            smallest_key: smallest.to_vec(),
            largest_key: largest.to_vec(),
            file_size: 1000,
            entry_count: 100,
            block_count: 10,
            min_ts: 1,
            max_ts: 100,
            created_at: 0,
        }
    }

    #[test]
    fn test_picker_no_compaction_needed() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let picker = CompactionPicker::new(config);

        let version = Version::new();
        assert!(picker.pick(&version).is_none());
    }

    #[test]
    fn test_picker_l0_compaction() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let picker = CompactionPicker::new(config);

        // Add 4 L0 files (triggers compaction)
        let version = Version::builder()
            .add_sst(make_sst(1, 0, b"a", b"e"))
            .add_sst(make_sst(2, 0, b"f", b"j"))
            .add_sst(make_sst(3, 0, b"k", b"o"))
            .add_sst(make_sst(4, 0, b"p", b"z"))
            .build();

        let task = picker.pick(&version).unwrap();
        assert_eq!(task.output_level, 1);
        assert!(!task.is_trivial_move);
        assert_eq!(task.inputs.len(), 4); // All 4 L0 files
    }

    #[test]
    fn test_picker_l0_with_l1_overlap() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let picker = CompactionPicker::new(config);

        // L0 files overlapping with some L1 files
        let version = Version::builder()
            .add_sst(make_sst(1, 0, b"a", b"m"))
            .add_sst(make_sst(2, 0, b"n", b"z"))
            .add_sst(make_sst(3, 0, b"b", b"k"))
            .add_sst(make_sst(4, 0, b"l", b"x"))
            .add_sst(make_sst(5, 1, b"a", b"f")) // Overlaps
            .add_sst(make_sst(6, 1, b"g", b"l")) // Overlaps
            .add_sst(make_sst(7, 1, b"m", b"r")) // Overlaps
            .build();

        let task = picker.pick(&version).unwrap();
        assert_eq!(task.output_level, 1);

        // Should include all L0 files and overlapping L1 files
        let l0_inputs: Vec<_> = task.inputs.iter().filter(|(l, _)| *l == 0).collect();
        let l1_inputs: Vec<_> = task.inputs.iter().filter(|(l, _)| *l == 1).collect();

        assert_eq!(l0_inputs.len(), 4);
        assert!(!l1_inputs.is_empty()); // At least some L1 overlap
    }

    #[test]
    fn test_picker_level_compaction() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(
            LsmConfig::builder(tmp.path())
                .l0_compaction_trigger(4)
                .l1_max_size(1000) // Very small to trigger compaction
                .build()
                .unwrap(),
        );
        let picker = CompactionPicker::new(config);

        // L1 exceeds size limit
        let version = Version::builder()
            .add_sst(make_sst(1, 1, b"a", b"m"))
            .add_sst(make_sst(2, 1, b"n", b"z"))
            .build();

        let task = picker.pick(&version).unwrap();
        assert_eq!(task.output_level, 2);
    }

    #[test]
    fn test_picker_trivial_move() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(
            LsmConfig::builder(tmp.path())
                .l0_compaction_trigger(4)
                .l1_max_size(500) // Very small
                .build()
                .unwrap(),
        );
        let picker = CompactionPicker::new(config);

        // L1 file with no L2 overlap -> trivial move
        let version = Version::builder()
            .add_sst(make_sst(1, 1, b"a", b"m"))
            // No L2 files
            .build();

        let task = picker.pick(&version).unwrap();
        assert_eq!(task.output_level, 2);
        assert!(task.is_trivial_move);
        assert_eq!(task.inputs.len(), 1);
    }

    #[test]
    fn test_merge_iterator_single_source() {
        let tmp = TempDir::new().unwrap();
        let sst_path = tmp.path().join("test.sst");

        // Create SST with test data
        let options = SstBuilderOptions::default();
        let mut builder = SstBuilder::new(&sst_path, options).unwrap();
        builder.add(b"key1", b"value1").unwrap();
        builder.add(b"key2", b"value2").unwrap();
        builder.add(b"key3", b"value3").unwrap();
        builder.finish(1, 0).unwrap();

        // Create merge iterator
        let reader = SstReaderRef::open(&sst_path, test_io()).unwrap();
        let mut iter = MergeIterator::new(vec![reader]).unwrap();
        iter.seek_to_first().unwrap();

        // Verify iteration
        assert!(iter.valid());
        let (k, v) = iter.current().unwrap();
        assert_eq!(k, b"key1");
        assert_eq!(v, b"value1");

        iter.advance().unwrap();
        let (k, _) = iter.current().unwrap();
        assert_eq!(k, b"key2");

        iter.advance().unwrap();
        let (k, _) = iter.current().unwrap();
        assert_eq!(k, b"key3");

        iter.advance().unwrap();
        assert!(!iter.valid());
    }

    #[test]
    fn test_merge_iterator_multiple_sources() {
        let tmp = TempDir::new().unwrap();

        // Create first SST: a, c, e
        let sst1_path = tmp.path().join("sst1.sst");
        let mut builder = SstBuilder::new(&sst1_path, SstBuilderOptions::default()).unwrap();
        builder.add(b"a", b"v1").unwrap();
        builder.add(b"c", b"v1").unwrap();
        builder.add(b"e", b"v1").unwrap();
        builder.finish(1, 0).unwrap();

        // Create second SST: b, d, f
        let sst2_path = tmp.path().join("sst2.sst");
        let mut builder = SstBuilder::new(&sst2_path, SstBuilderOptions::default()).unwrap();
        builder.add(b"b", b"v2").unwrap();
        builder.add(b"d", b"v2").unwrap();
        builder.add(b"f", b"v2").unwrap();
        builder.finish(2, 0).unwrap();

        // Merge both
        let reader1 = SstReaderRef::open(&sst1_path, test_io()).unwrap();
        let reader2 = SstReaderRef::open(&sst2_path, test_io()).unwrap();
        let mut iter = MergeIterator::new(vec![reader1, reader2]).unwrap();
        iter.seek_to_first().unwrap();

        // Should be in sorted order
        let mut keys = Vec::new();
        while iter.valid() {
            let (k, _) = iter.current().unwrap();
            keys.push(k);
            iter.advance().unwrap();
        }

        assert_eq!(
            keys,
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
                b"e".to_vec(),
                b"f".to_vec()
            ]
        );
    }

    #[test]
    fn test_merge_iterator_duplicate_keys() {
        let tmp = TempDir::new().unwrap();

        // Create first SST (newer): key->new
        let sst1_path = tmp.path().join("sst1.sst");
        let mut builder = SstBuilder::new(&sst1_path, SstBuilderOptions::default()).unwrap();
        builder.add(b"key", b"new").unwrap();
        builder.finish(1, 0).unwrap();

        // Create second SST (older): key->old
        let sst2_path = tmp.path().join("sst2.sst");
        let mut builder = SstBuilder::new(&sst2_path, SstBuilderOptions::default()).unwrap();
        builder.add(b"key", b"old").unwrap();
        builder.finish(2, 0).unwrap();

        // Merge with newer first
        let reader1 = SstReaderRef::open(&sst1_path, test_io()).unwrap();
        let reader2 = SstReaderRef::open(&sst2_path, test_io()).unwrap();
        let mut iter = MergeIterator::new(vec![reader1, reader2]).unwrap();
        iter.seek_to_first().unwrap();

        // First should be newer value
        let (k, v) = iter.current().unwrap();
        assert_eq!(k, b"key");
        assert_eq!(v, b"new");

        // Second should be older value (both versions preserved)
        iter.advance().unwrap();
        let (k, v) = iter.current().unwrap();
        assert_eq!(k, b"key");
        assert_eq!(v, b"old");
    }

    #[test]
    fn test_compaction_executor_basic() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let executor = CompactionExecutor::new(Arc::clone(&config));

        // Create L0 SSTs
        let sst1_path = config.sst_dir().join("00000001.sst");
        std::fs::create_dir_all(config.sst_dir()).unwrap();
        let mut builder = SstBuilder::new(&sst1_path, SstBuilderOptions::default()).unwrap();
        builder.add(b"a", b"v1").unwrap();
        builder.add(b"c", b"v1").unwrap();
        let meta1 = builder.finish(1, 0).unwrap();

        let sst2_path = config.sst_dir().join("00000002.sst");
        let mut builder = SstBuilder::new(&sst2_path, SstBuilderOptions::default()).unwrap();
        builder.add(b"b", b"v2").unwrap();
        builder.add(b"d", b"v2").unwrap();
        let meta2 = builder.finish(2, 0).unwrap();

        // Build version with these SSTs
        let version = Version::builder()
            .add_sst(meta1)
            .add_sst(meta2)
            .next_sst_id(3)
            .build();

        // Create compaction task
        let task = CompactionTask {
            inputs: vec![(0, 1), (0, 2)],
            output_level: 1,
            is_trivial_move: false,
        };

        // Execute compaction with pre-allocated IDs (tests use standard I/O)
        let pre_allocated_ids = vec![3, 4]; // Over-allocate for safety
        let delta = executor
            .execute(
                &task,
                &version,
                &config.sst_dir(),
                &pre_allocated_ids,
                test_io(),
            )
            .unwrap();

        // Verify delta
        assert!(!delta.new_ssts.is_empty());
        assert_eq!(delta.deleted_ssts.len(), 2);
        assert!(delta.new_ssts.iter().all(|sst| sst.level == 1));
    }

    // ==================== MergeIterator Additional Coverage Tests ====================

    #[test]
    fn test_merge_iterator_empty_sources() {
        let mut iter = MergeIterator::new(vec![]).unwrap();
        iter.seek_to_first().unwrap();

        assert!(!iter.valid());
        assert!(iter.current().is_none());
    }

    #[test]
    fn test_merge_iterator_single_empty_sst() {
        let tmp = TempDir::new().unwrap();
        let sst_path = tmp.path().join("empty.sst");

        // Create empty SST
        let builder = SstBuilder::new(&sst_path, SstBuilderOptions::default()).unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&sst_path, test_io()).unwrap();
        let mut iter = MergeIterator::new(vec![reader]).unwrap();
        iter.seek_to_first().unwrap();

        assert!(!iter.valid());
    }

    #[test]
    fn test_merge_iterator_not_positioned_after_new() {
        let tmp = TempDir::new().unwrap();
        let sst_path = tmp.path().join("test.sst");

        let mut builder = SstBuilder::new(&sst_path, SstBuilderOptions::default()).unwrap();
        builder.add(b"key", b"value").unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&sst_path, test_io()).unwrap();
        let iter = MergeIterator::new(vec![reader]).unwrap();

        // Iterator should not be valid until seek_to_first
        assert!(!iter.valid());
    }

    #[test]
    fn test_merge_iterator_advance_when_not_valid() {
        let tmp = TempDir::new().unwrap();
        let sst_path = tmp.path().join("test.sst");

        let mut builder = SstBuilder::new(&sst_path, SstBuilderOptions::default()).unwrap();
        builder.add(b"key", b"value").unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&sst_path, test_io()).unwrap();
        let mut iter = MergeIterator::new(vec![reader]).unwrap();
        iter.seek_to_first().unwrap();

        // Exhaust iterator
        iter.advance().unwrap();
        assert!(!iter.valid());

        // Advance on invalid should be no-op
        iter.advance().unwrap();
        assert!(!iter.valid());
    }

    #[test]
    fn test_merge_iterator_tie_breaking_newer_first() {
        let tmp = TempDir::new().unwrap();

        // Create SST1 (newer, index 0)
        let sst1_path = tmp.path().join("sst1.sst");
        let mut builder = SstBuilder::new(&sst1_path, SstBuilderOptions::default()).unwrap();
        builder.add(b"key", b"newer").unwrap();
        builder.finish(1, 0).unwrap();

        // Create SST2 (older, index 1)
        let sst2_path = tmp.path().join("sst2.sst");
        let mut builder = SstBuilder::new(&sst2_path, SstBuilderOptions::default()).unwrap();
        builder.add(b"key", b"older").unwrap();
        builder.finish(2, 0).unwrap();

        let reader1 = SstReaderRef::open(&sst1_path, test_io()).unwrap();
        let reader2 = SstReaderRef::open(&sst2_path, test_io()).unwrap();

        // Newer file first
        let mut iter = MergeIterator::new(vec![reader1, reader2]).unwrap();
        iter.seek_to_first().unwrap();

        // First entry should be from newer file (source_idx 0)
        let (k, v) = iter.current().unwrap();
        assert_eq!(k, b"key");
        assert_eq!(v, b"newer");

        // Second entry should be from older file (source_idx 1)
        iter.advance().unwrap();
        let (k, v) = iter.current().unwrap();
        assert_eq!(k, b"key");
        assert_eq!(v, b"older");

        iter.advance().unwrap();
        assert!(!iter.valid());
    }

    #[test]
    fn test_merge_iterator_large_merge() {
        let tmp = TempDir::new().unwrap();

        // Create 10 SSTs with interleaved keys
        let mut readers = Vec::new();
        for sst_idx in 0..10u32 {
            let sst_path = tmp.path().join(format!("sst{sst_idx}.sst"));
            let mut builder = SstBuilder::new(&sst_path, SstBuilderOptions::default()).unwrap();

            // Each SST has keys: sst_idx, sst_idx+10, sst_idx+20, ...
            for key_idx in 0..10 {
                let key = format!("key_{:05}", sst_idx as usize + key_idx * 10);
                let value = format!("v{sst_idx}");
                builder.add(key.as_bytes(), value.as_bytes()).unwrap();
            }
            builder.finish(sst_idx as u64 + 1, 0).unwrap();

            readers.push(SstReaderRef::open(&sst_path, test_io()).unwrap());
        }

        let mut iter = MergeIterator::new(readers).unwrap();
        iter.seek_to_first().unwrap();

        // Collect all keys
        let mut keys = Vec::new();
        while iter.valid() {
            let (k, _) = iter.current().unwrap();
            keys.push(String::from_utf8_lossy(&k).to_string());
            iter.advance().unwrap();
        }

        // Should have 100 keys in sorted order
        assert_eq!(keys.len(), 100);

        // Verify sorted order
        for i in 1..keys.len() {
            assert!(keys[i - 1] <= keys[i], "Keys should be sorted");
        }
    }

    #[test]
    fn test_merge_iterator_skip_to_next_key_basic() {
        let tmp = TempDir::new().unwrap();
        let sst_path = tmp.path().join("mvcc.sst");

        // Create MVCC key helper
        fn mvcc(key: &[u8], ts: u64) -> Vec<u8> {
            let mut result = key.to_vec();
            result.extend_from_slice(&(!ts).to_be_bytes());
            result
        }

        let mut builder = SstBuilder::new(&sst_path, SstBuilderOptions::default()).unwrap();
        // key1 with versions 100, 50, 25
        builder.add(&mvcc(b"key1", 100), b"v1_100").unwrap();
        builder.add(&mvcc(b"key1", 50), b"v1_50").unwrap();
        builder.add(&mvcc(b"key1", 25), b"v1_25").unwrap();
        // key2 with versions 200, 100
        builder.add(&mvcc(b"key2", 200), b"v2_200").unwrap();
        builder.add(&mvcc(b"key2", 100), b"v2_100").unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&sst_path, test_io()).unwrap();
        let mut iter = MergeIterator::new(vec![reader]).unwrap();
        iter.seek_to_first().unwrap();

        // First entry: key1@100
        let (k, _) = iter.current().unwrap();
        assert!(k.starts_with(b"key1"));

        // Skip to next key should jump to key2
        iter.advance().unwrap();
        iter.skip_to_next_key().unwrap();

        let (k, _) = iter.current().unwrap();
        assert!(k.starts_with(b"key2"));
    }

    #[test]
    fn test_merge_iterator_skip_to_next_key_at_last_key() {
        let tmp = TempDir::new().unwrap();
        let sst_path = tmp.path().join("mvcc.sst");

        fn mvcc(key: &[u8], ts: u64) -> Vec<u8> {
            let mut result = key.to_vec();
            result.extend_from_slice(&(!ts).to_be_bytes());
            result
        }

        let mut builder = SstBuilder::new(&sst_path, SstBuilderOptions::default()).unwrap();
        builder.add(&mvcc(b"only_key", 100), b"v100").unwrap();
        builder.add(&mvcc(b"only_key", 50), b"v50").unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&sst_path, test_io()).unwrap();
        let mut iter = MergeIterator::new(vec![reader]).unwrap();
        iter.seek_to_first().unwrap();

        // First entry
        iter.advance().unwrap();
        // Skip should exhaust iterator (no more keys)
        iter.skip_to_next_key().unwrap();
        assert!(!iter.valid());
    }

    #[test]
    fn test_merge_iterator_skip_to_next_key_short_key() {
        let tmp = TempDir::new().unwrap();
        let sst_path = tmp.path().join("test.sst");

        let mut builder = SstBuilder::new(&sst_path, SstBuilderOptions::default()).unwrap();
        // Keys shorter than 8 bytes (no MVCC timestamp)
        builder.add(b"a", b"va").unwrap();
        builder.add(b"b", b"vb").unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&sst_path, test_io()).unwrap();
        let mut iter = MergeIterator::new(vec![reader]).unwrap();
        iter.seek_to_first().unwrap();

        // Skip to next key with short key should be no-op
        iter.advance().unwrap();
        iter.skip_to_next_key().unwrap();

        // Should still be valid (skipping is based on key prefix comparison)
        // For keys < 8 bytes, there's no timestamp to strip, so nothing is skipped
        let (k, _) = iter.current().unwrap();
        assert_eq!(k, b"b");
    }

    #[test]
    fn test_merge_iterator_multiple_sources_with_overlap() {
        let tmp = TempDir::new().unwrap();

        // SST1: a, b, c
        let sst1_path = tmp.path().join("sst1.sst");
        let mut builder = SstBuilder::new(&sst1_path, SstBuilderOptions::default()).unwrap();
        builder.add(b"a", b"a1").unwrap();
        builder.add(b"b", b"b1").unwrap();
        builder.add(b"c", b"c1").unwrap();
        builder.finish(1, 0).unwrap();

        // SST2: b, c, d (overlaps with SST1)
        let sst2_path = tmp.path().join("sst2.sst");
        let mut builder = SstBuilder::new(&sst2_path, SstBuilderOptions::default()).unwrap();
        builder.add(b"b", b"b2").unwrap();
        builder.add(b"c", b"c2").unwrap();
        builder.add(b"d", b"d2").unwrap();
        builder.finish(2, 0).unwrap();

        let reader1 = SstReaderRef::open(&sst1_path, test_io()).unwrap();
        let reader2 = SstReaderRef::open(&sst2_path, test_io()).unwrap();

        let mut iter = MergeIterator::new(vec![reader1, reader2]).unwrap();
        iter.seek_to_first().unwrap();

        let mut entries = Vec::new();
        while iter.valid() {
            let (k, v) = iter.current().unwrap();
            entries.push((k, v));
            iter.advance().unwrap();
        }

        // Should have all entries, with duplicates
        assert_eq!(entries.len(), 6); // a, b(x2), c(x2), d
        assert_eq!(entries[0].0, b"a");
        assert_eq!(entries[1].0, b"b");
        assert_eq!(entries[1].1, b"b1"); // newer first
        assert_eq!(entries[2].0, b"b");
        assert_eq!(entries[2].1, b"b2"); // older second
    }

    #[test]
    fn test_merge_iterator_current_none_when_invalid() {
        let tmp = TempDir::new().unwrap();
        let sst_path = tmp.path().join("test.sst");

        let mut builder = SstBuilder::new(&sst_path, SstBuilderOptions::default()).unwrap();
        builder.add(b"key", b"value").unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&sst_path, test_io()).unwrap();
        let iter = MergeIterator::new(vec![reader]).unwrap();

        // Before seek_to_first, current should be None
        assert!(iter.current().is_none());
    }

    // ==================== MergeEntry Ordering Tests ====================

    #[test]
    fn test_merge_entry_ordering_by_key() {
        use std::cmp::Ordering;

        let entry_a = MergeEntry {
            key: b"a".to_vec(),
            value: b"va".to_vec(),
            source_idx: 0,
        };

        let entry_b = MergeEntry {
            key: b"b".to_vec(),
            value: b"vb".to_vec(),
            source_idx: 0,
        };

        // For min-heap, smaller key should have higher priority
        // Ord is inverted for BinaryHeap
        assert!(entry_a.cmp(&entry_b) == Ordering::Greater);
    }

    #[test]
    fn test_merge_entry_ordering_by_source_idx() {
        use std::cmp::Ordering;

        let entry_newer = MergeEntry {
            key: b"key".to_vec(),
            value: b"newer".to_vec(),
            source_idx: 0, // Newer source
        };

        let entry_older = MergeEntry {
            key: b"key".to_vec(),
            value: b"older".to_vec(),
            source_idx: 1, // Older source
        };

        // Same key: smaller source_idx (newer) should come first
        assert!(entry_newer.cmp(&entry_older) == Ordering::Greater);
    }

    // ==================== CompactionPicker Additional Tests ====================

    #[test]
    fn test_picker_respects_l0_trigger_threshold() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(
            LsmConfig::builder(tmp.path())
                .l0_compaction_trigger(5) // Higher threshold
                .build()
                .unwrap(),
        );
        let picker = CompactionPicker::new(config);

        // Only 3 L0 files (below threshold)
        let version = Version::builder()
            .add_sst(make_sst(1, 0, b"a", b"e"))
            .add_sst(make_sst(2, 0, b"f", b"j"))
            .add_sst(make_sst(3, 0, b"k", b"o"))
            .build();

        // Should NOT trigger compaction
        assert!(picker.pick(&version).is_none());
    }

    #[test]
    fn test_picker_l1_to_l2_compaction() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(
            LsmConfig::builder(tmp.path())
                .l0_compaction_trigger(10) // High threshold to avoid L0 compaction
                .l0_slowdown_trigger(12) // Must be >= l0_compaction_trigger
                .l0_stop_trigger(14) // Must be >= l0_slowdown_trigger
                .l1_max_size(100) // Very small L1 to trigger compaction
                .build()
                .unwrap(),
        );
        let picker = CompactionPicker::new(config);

        // L1 exceeds limit, should compact to L2
        let version = Version::builder()
            .add_sst(make_sst(1, 1, b"a", b"m"))
            .add_sst(make_sst(2, 1, b"n", b"z"))
            .build();

        let task = picker.pick(&version);
        assert!(task.is_some());
        let task = task.unwrap();
        assert_eq!(task.output_level, 2);
    }

    #[test]
    fn test_compaction_key_range_computation() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let executor = CompactionExecutor::new(Arc::clone(&config));

        // Create SSTs with specific key ranges
        std::fs::create_dir_all(config.sst_dir()).unwrap();

        let sst1_path = config.sst_dir().join("00000001.sst");
        let mut builder = SstBuilder::new(&sst1_path, SstBuilderOptions::default()).unwrap();
        builder.add(b"a", b"v1").unwrap();
        builder.add(b"b", b"v1").unwrap();
        let meta1 = builder.finish(1, 0).unwrap();

        let sst2_path = config.sst_dir().join("00000002.sst");
        let mut builder = SstBuilder::new(&sst2_path, SstBuilderOptions::default()).unwrap();
        builder.add(b"c", b"v2").unwrap();
        builder.add(b"d", b"v2").unwrap();
        let meta2 = builder.finish(2, 0).unwrap();

        let version = Version::builder()
            .add_sst(meta1)
            .add_sst(meta2)
            .next_sst_id(3)
            .build();

        let task = CompactionTask {
            inputs: vec![(0, 1), (0, 2)],
            output_level: 1,
            is_trivial_move: false,
        };

        let pre_allocated_ids = vec![3, 4];
        let delta = executor
            .execute(
                &task,
                &version,
                &config.sst_dir(),
                &pre_allocated_ids,
                test_io(),
            )
            .unwrap();

        // Output should span full key range
        assert!(!delta.new_ssts.is_empty());
        let output = &delta.new_ssts[0];
        assert!(output.smallest_key.as_slice() <= b"a".as_slice());
        assert!(output.largest_key.as_slice() >= b"d".as_slice());
    }

    // ==================== CompactionPicker Score Tests ====================

    #[test]
    fn test_picker_score_l0_by_file_count() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(
            LsmConfig::builder(tmp.path())
                .l0_compaction_trigger(4)
                .build()
                .unwrap(),
        );
        let picker = CompactionPicker::new(config);

        // 2 L0 files out of 4 trigger => score = 0.5
        let version = Version::builder()
            .add_sst(make_sst(1, 0, b"a", b"m"))
            .add_sst(make_sst(2, 0, b"n", b"z"))
            .build();

        let scores = picker.compute_compaction_scores(&version);
        let l0_score = scores.iter().find(|(level, _)| *level == 0).unwrap().1;
        assert!(
            (l0_score - 0.5).abs() < 0.01,
            "L0 score should be ~0.5, got {l0_score}"
        );

        // 4 L0 files out of 4 trigger => score = 1.0
        let version = Version::builder()
            .add_sst(make_sst(1, 0, b"a", b"g"))
            .add_sst(make_sst(2, 0, b"h", b"m"))
            .add_sst(make_sst(3, 0, b"n", b"s"))
            .add_sst(make_sst(4, 0, b"t", b"z"))
            .build();

        let scores = picker.compute_compaction_scores(&version);
        let l0_score = scores.iter().find(|(level, _)| *level == 0).unwrap().1;
        assert!(
            l0_score >= 1.0,
            "L0 score should be >= 1.0 with 4 files and trigger=4, got {l0_score}"
        );
    }

    #[test]
    fn test_picker_score_level_by_bytes() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(
            LsmConfig::builder(tmp.path())
                .l0_compaction_trigger(10) // High to avoid L0 triggering
                .l0_slowdown_trigger(12)
                .l0_stop_trigger(14)
                .l1_max_size(2000) // L1 max = 2000 bytes
                .build()
                .unwrap(),
        );
        let picker = CompactionPicker::new(config);

        // L1 has 2 files of 1000 bytes each = 2000 bytes total
        // score = 2000 / 2000 = 1.0
        let version = Version::builder()
            .add_sst(make_sst(1, 1, b"a", b"m")) // file_size = 1000
            .add_sst(make_sst(2, 1, b"n", b"z")) // file_size = 1000
            .build();

        let scores = picker.compute_compaction_scores(&version);
        let l1_score = scores.iter().find(|(level, _)| *level == 1).unwrap().1;
        assert!(
            (l1_score - 1.0).abs() < 0.01,
            "L1 score should be ~1.0, got {l1_score}"
        );
    }

    #[test]
    fn test_picker_highest_score_wins() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(
            LsmConfig::builder(tmp.path())
                .l0_compaction_trigger(10) // High to avoid L0 triggering
                .l0_slowdown_trigger(12)
                .l0_stop_trigger(14)
                .l1_max_size(5000) // L1 max = 5000
                .build()
                .unwrap(),
        );
        let picker = CompactionPicker::new(config);

        // L1 has 1000 bytes (score = 1000/5000 = 0.2)
        // L2 max = 5000 * 10 = 50000, we put 80000 bytes (score = 80000/50000 = 1.6)
        let mut big_sst = make_sst(10, 2, b"a", b"z");
        big_sst.file_size = 80000;

        let version = Version::builder()
            .add_sst(make_sst(1, 1, b"a", b"m")) // 1000 bytes in L1
            .add_sst(big_sst) // 80000 bytes in L2
            .build();

        let scores = picker.compute_compaction_scores(&version);

        // Scores should be sorted descending
        assert!(
            scores[0].1 >= scores[1].1,
            "Scores should be sorted descending"
        );

        // L2 should be the highest scoring level
        assert_eq!(scores[0].0, 2, "L2 should have the highest score");
    }

    #[test]
    fn test_picker_no_compaction_all_scores_below_one() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(
            LsmConfig::builder(tmp.path())
                .l0_compaction_trigger(10)
                .l0_slowdown_trigger(12)
                .l0_stop_trigger(14)
                .l1_max_size(100_000) // Very large L1 max
                .build()
                .unwrap(),
        );
        let picker = CompactionPicker::new(config);

        // 1 L0 file (score = 1/10 = 0.1), 1 L1 file (1000 bytes, score = 0.01)
        let version = Version::builder()
            .add_sst(make_sst(1, 0, b"a", b"z"))
            .add_sst(make_sst(2, 1, b"a", b"z"))
            .build();

        let scores = picker.compute_compaction_scores(&version);
        for (_level, score) in &scores {
            assert!(*score < 1.0, "All scores should be below 1.0");
        }

        // pick() should return None
        assert!(picker.pick(&version).is_none());
    }

    #[test]
    fn test_picker_empty_version_scores() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let picker = CompactionPicker::new(config);

        let version = Version::new();
        let scores = picker.compute_compaction_scores(&version);

        // All scores should be 0
        for (_level, score) in &scores {
            assert!(
                *score == 0.0,
                "Empty version should have score 0.0 for all levels"
            );
        }
    }
}
