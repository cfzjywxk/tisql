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

use crate::error::Result;
use crate::types::{Key, RawValue};

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
    /// Returns None if no compaction is needed.
    pub fn pick(&self, version: &Version) -> Option<CompactionTask> {
        // Priority 1: L0 compaction if too many files
        if version.level_size(0) >= self.config.l0_compaction_trigger {
            return self.pick_l0_compaction(version);
        }

        // Priority 2: Level compaction for oversized levels
        for level in 1..self.config.max_levels - 1 {
            let level_size = version.level_size_bytes(level);
            let max_size = self.config.level_max_size(level) as u64;

            if level_size > max_size {
                return self.pick_level_compaction(version, level);
            }
        }

        None
    }

    /// Pick L0 -> L1 compaction.
    fn pick_l0_compaction(&self, version: &Version) -> Option<CompactionTask> {
        let l0_files = version.level(0);
        if l0_files.is_empty() {
            return None;
        }

        // Collect all L0 files
        let mut inputs: Vec<(u32, u64)> = l0_files.iter().map(|sst| (0, sst.id)).collect();

        // Find key range covered by L0 files
        let (min_key, max_key) = self.get_key_range(l0_files)?;

        // Find overlapping L1 files
        let l1_overlapping = version.find_overlapping_at_level(1, &min_key, &max_key);
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

        // Find overlapping files at next level
        let next_level = level + 1;
        if next_level < self.config.max_levels {
            let overlapping = version.find_overlapping_at_level(
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
    fn get_key_range(&self, files: &[Arc<SstMeta>]) -> Option<(Key, Key)> {
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
    key: Key,
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
/// For MVCC keys (user_key || !ts), it outputs all versions in order.
/// The caller is responsible for filtering based on GC safe point.
pub struct MergeIterator {
    /// Source iterators
    iters: Vec<SstIterator>,

    /// Priority queue for merging
    heap: BinaryHeap<MergeEntry>,

    /// Last key output (for deduplication if needed)
    last_key: Option<Key>,
}

impl MergeIterator {
    /// Create a new merge iterator from SST readers.
    ///
    /// Readers should be ordered from newest to oldest for correct
    /// tie-breaking behavior.
    pub fn new(readers: Vec<SstReaderRef>) -> Result<Self> {
        let mut iters = Vec::with_capacity(readers.len());
        for r in readers {
            iters.push(SstIterator::new(r)?);
        }

        let mut heap = BinaryHeap::new();

        // Initialize: add first entry from each iterator
        for (idx, iter) in iters.iter().enumerate() {
            if iter.valid() {
                let key = iter.key().to_vec();
                let value = iter.value().to_vec();
                heap.push(MergeEntry {
                    key,
                    value,
                    source_idx: idx,
                });
            }
        }

        Ok(Self {
            iters,
            heap,
            last_key: None,
        })
    }

    /// Check if the iterator has more entries.
    pub fn valid(&self) -> bool {
        !self.heap.is_empty()
    }

    /// Get the current key-value pair.
    pub fn current(&self) -> Option<(Key, RawValue)> {
        self.heap.peek().map(|e| (e.key.clone(), e.value.clone()))
    }

    /// Advance to the next entry.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<()> {
        if let Some(entry) = self.heap.pop() {
            let idx = entry.source_idx;
            self.last_key = Some(entry.key);

            // Advance the source iterator and push next entry
            self.iters[idx].next()?;
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

    /// Skip entries with the same user key as the last output.
    ///
    /// Useful when you only want the latest version of each key.
    pub fn skip_to_next_user_key(&mut self) -> Result<()> {
        let last_user_key = match &self.last_key {
            Some(k) if k.len() >= 8 => k[..k.len() - 8].to_vec(),
            _ => return Ok(()),
        };

        loop {
            let should_skip = match self.heap.peek() {
                Some(entry) if entry.key.len() >= 8 => {
                    let user_key = &entry.key[..entry.key.len() - 8];
                    user_key == last_user_key.as_slice()
                }
                _ => false,
            };

            if should_skip {
                self.next()?;
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
    /// Returns the manifest delta to apply on success.
    pub fn execute(
        &self,
        task: &CompactionTask,
        version: &Version,
        sst_dir: &Path,
    ) -> Result<ManifestDelta> {
        // Handle trivial move (just update metadata, no I/O)
        if task.is_trivial_move {
            return self.execute_trivial_move(task, version);
        }

        // Collect input SST readers
        let mut readers = Vec::new();
        for (_level, sst_id) in &task.inputs {
            let sst_path = sst_dir.join(format!("{sst_id:08}.sst"));
            let reader = SstReaderRef::open(&sst_path)?;
            readers.push(reader);
        }

        // Create merge iterator
        let mut merge_iter = MergeIterator::new(readers)?;

        // Create output SST builder
        let next_sst_id = version.next_sst_id();
        let output_path = sst_dir.join(format!("{next_sst_id:08}.sst"));
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
                builder.add(&key, &value)?;
                current_size += key.len() + value.len();

                // Check if we should split to a new SST
                if current_size >= self.config.target_file_size {
                    let meta = builder
                        .finish(next_sst_id + output_ssts.len() as u64, task.output_level)?;
                    output_ssts.push(meta);

                    // Start new output file
                    let new_id = next_sst_id + output_ssts.len() as u64;
                    let new_path = sst_dir.join(format!("{new_id:08}.sst"));
                    builder = SstBuilder::new(&new_path, options.clone())?;
                    current_size = 0;
                }
            }
            merge_iter.next()?;
        }

        // Finish last output file
        if current_size > 0 || output_ssts.is_empty() {
            let meta = builder.finish(next_sst_id + output_ssts.len() as u64, task.output_level)?;
            output_ssts.push(meta);
        } else {
            builder.abort()?;
        }

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
        assert!(l1_inputs.len() >= 1); // At least some L1 overlap
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
        let reader = SstReaderRef::open(&sst_path).unwrap();
        let mut iter = MergeIterator::new(vec![reader]).unwrap();

        // Verify iteration
        assert!(iter.valid());
        let (k, v) = iter.current().unwrap();
        assert_eq!(k, b"key1");
        assert_eq!(v, b"value1");

        iter.next().unwrap();
        let (k, _) = iter.current().unwrap();
        assert_eq!(k, b"key2");

        iter.next().unwrap();
        let (k, _) = iter.current().unwrap();
        assert_eq!(k, b"key3");

        iter.next().unwrap();
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
        let reader1 = SstReaderRef::open(&sst1_path).unwrap();
        let reader2 = SstReaderRef::open(&sst2_path).unwrap();
        let mut iter = MergeIterator::new(vec![reader1, reader2]).unwrap();

        // Should be in sorted order
        let mut keys = Vec::new();
        while iter.valid() {
            let (k, _) = iter.current().unwrap();
            keys.push(k);
            iter.next().unwrap();
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
        let reader1 = SstReaderRef::open(&sst1_path).unwrap();
        let reader2 = SstReaderRef::open(&sst2_path).unwrap();
        let mut iter = MergeIterator::new(vec![reader1, reader2]).unwrap();

        // First should be newer value
        let (k, v) = iter.current().unwrap();
        assert_eq!(k, b"key");
        assert_eq!(v, b"new");

        // Second should be older value (both versions preserved)
        iter.next().unwrap();
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

        // Execute compaction
        let delta = executor
            .execute(&task, &version, &config.sst_dir())
            .unwrap();

        // Verify delta
        assert!(!delta.new_ssts.is_empty());
        assert_eq!(delta.deleted_ssts.len(), 2);
        assert!(delta.new_ssts.iter().all(|sst| sst.level == 1));
    }
}
