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

//! LSM-tree version management.
//!
//! This module tracks the current state of the LSM-tree, including:
//!
//! - SST files at each level
//! - Active and frozen memtables
//! - Next SST ID allocation
//!
//! ## Version vs VersionEdit
//!
//! - `Version`: Complete snapshot of LSM state (immutable after creation)
//! - `ManifestDelta`: Incremental change to a version (for persistence)
//!
//! ## Manifest Persistence
//!
//! Version changes are persisted via the clog (commit log) as manifest deltas.
//! On recovery, we replay deltas to reconstruct the current version.
//!
//! ## Level Layout
//!
//! ```text
//! Level 0: Recent flushes, may overlap
//! Level 1: 10x size of L0, no overlap
//! Level 2: 10x size of L1, no overlap
//! ...
//! Level N: Oldest data
//! ```

use std::sync::Arc;

use crate::storage::SstMeta;

/// Maximum number of levels in the LSM-tree.
pub const MAX_LEVELS: usize = 7;

/// Version represents a consistent snapshot of the LSM-tree state.
///
/// Versions are immutable - modifications create new versions.
/// This enables lock-free reads during compaction.
#[derive(Debug, Clone)]
pub struct Version {
    /// SST files at each level.
    ///
    /// Level 0 may have overlapping key ranges (recent flushes).
    /// Levels 1+ have non-overlapping key ranges within each level.
    levels: Vec<Vec<Arc<SstMeta>>>,

    /// Next SST file ID to allocate.
    next_sst_id: u64,

    /// Version number (monotonically increasing).
    version_num: u64,

    /// Maximum LSN that has been flushed to SST.
    ///
    /// Used for recovery to determine which WAL entries are safe to discard.
    flushed_lsn: u64,
}

impl Version {
    /// Create a new empty version.
    pub fn new() -> Self {
        Self {
            levels: vec![Vec::new(); MAX_LEVELS],
            next_sst_id: 1,
            version_num: 0,
            flushed_lsn: 0,
        }
    }

    /// Get SST files at a specific level.
    pub fn level(&self, level: usize) -> &[Arc<SstMeta>] {
        self.levels.get(level).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Get the number of SST files at a level.
    pub fn level_size(&self, level: usize) -> usize {
        self.levels.get(level).map(|v| v.len()).unwrap_or(0)
    }

    /// Get the total number of SST files across all levels.
    pub fn total_sst_count(&self) -> usize {
        self.levels.iter().map(|v| v.len()).sum()
    }

    /// Get the total size in bytes across all levels.
    pub fn total_size_bytes(&self) -> u64 {
        self.levels
            .iter()
            .flat_map(|v| v.iter())
            .map(|sst| sst.file_size)
            .sum()
    }

    /// Get the size in bytes at a specific level.
    pub fn level_size_bytes(&self, level: usize) -> u64 {
        self.levels
            .get(level)
            .map(|v| v.iter().map(|sst| sst.file_size).sum())
            .unwrap_or(0)
    }

    /// Get the next SST ID to allocate.
    pub fn next_sst_id(&self) -> u64 {
        self.next_sst_id
    }

    /// Get the version number.
    pub fn version_num(&self) -> u64 {
        self.version_num
    }

    /// Get the flushed LSN.
    pub fn flushed_lsn(&self) -> u64 {
        self.flushed_lsn
    }

    /// Get the maximum commit timestamp from all SST files.
    ///
    /// Used during recovery to ensure TSO is initialized to at least
    /// the highest timestamp already persisted in storage. This is
    /// critical when clog has been truncated - the SST metadata still
    /// preserves the max timestamp that was committed.
    ///
    /// Returns 0 if there are no SST files.
    pub fn max_ts(&self) -> u64 {
        self.levels
            .iter()
            .flat_map(|v| v.iter())
            .map(|sst| sst.max_ts)
            .max()
            .unwrap_or(0)
    }

    /// Find SST files that may contain a key.
    ///
    /// Returns SSTs from all levels that could contain the key,
    /// in order from newest to oldest (L0 first, then L1, etc.).
    pub fn find_ssts_for_key(&self, key: &[u8]) -> Vec<Arc<SstMeta>> {
        let mut result = Vec::new();

        for level_ssts in &self.levels {
            for sst in level_ssts {
                if sst.may_contain_key(key) {
                    result.push(Arc::clone(sst));
                }
            }
        }

        result
    }

    /// Find SST files that overlap with a key range.
    ///
    /// Used for compaction and range scans.
    pub fn find_ssts_for_range(&self, start: &[u8], end: &[u8]) -> Vec<Arc<SstMeta>> {
        let mut result = Vec::new();

        for level_ssts in &self.levels {
            for sst in level_ssts {
                if sst.overlaps(start, end) {
                    result.push(Arc::clone(sst));
                }
            }
        }

        result
    }

    /// Find overlapping SSTs at a specific level.
    ///
    /// Uses key-based overlap check (extracts key portion from MVCC keys).
    pub fn find_overlapping_at_level(
        &self,
        level: usize,
        start: &[u8],
        end: &[u8],
    ) -> Vec<Arc<SstMeta>> {
        self.levels
            .get(level)
            .map(|level_ssts| {
                level_ssts
                    .iter()
                    .filter(|sst| sst.overlaps(start, end))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Find overlapping SSTs at a specific level using MVCC key ranges.
    ///
    /// Compares MVCC keys directly without extracting key portions.
    /// Each MVCC key is treated as an independent key in the storage engine.
    pub fn find_overlapping_at_level_mvcc(
        &self,
        level: usize,
        start: &[u8],
        end: &[u8],
    ) -> Vec<Arc<SstMeta>> {
        self.levels
            .get(level)
            .map(|level_ssts| {
                level_ssts
                    .iter()
                    .filter(|sst| sst.overlaps_mvcc(start, end))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Check if an SST with the given ID exists in any level.
    pub fn has_sst(&self, sst_id: u64) -> bool {
        self.levels
            .iter()
            .any(|level_ssts| level_ssts.iter().any(|sst| sst.id == sst_id))
    }

    /// Get SST files at a specific level (for serialization).
    pub fn ssts_at_level(&self, level: u32) -> &[Arc<SstMeta>] {
        self.levels
            .get(level as usize)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Apply a manifest delta to create a new version.
    ///
    /// This creates a new Version with the delta applied.
    pub fn apply(&self, delta: &ManifestDelta) -> Self {
        let mut new = self.clone();
        new.version_num += 1;

        // Add new SSTs
        for sst in &delta.new_ssts {
            let level = sst.level as usize;
            if level < MAX_LEVELS {
                new.levels[level].push(Arc::clone(sst));
            }
        }

        // Remove deleted SSTs
        for (level, sst_id) in &delta.deleted_ssts {
            let level = *level as usize;
            if level < MAX_LEVELS {
                new.levels[level].retain(|sst| sst.id != *sst_id);
            }
        }

        // Update next_sst_id if any new SSTs were added
        if let Some(max_id) = delta.new_ssts.iter().map(|s| s.id).max() {
            new.next_sst_id = new.next_sst_id.max(max_id + 1);
        }

        // Update flushed LSN
        if let Some(lsn) = delta.flushed_lsn {
            new.flushed_lsn = new.flushed_lsn.max(lsn);
        }

        // Sort L0 by ID (newest first) for correct read order
        new.levels[0].sort_by(|a, b| b.id.cmp(&a.id));

        // For L1+, sort by smallest key for binary search
        for level in 1..MAX_LEVELS {
            new.levels[level].sort_by(|a, b| a.smallest_key.cmp(&b.smallest_key));
        }

        new
    }

    /// Create a version builder for incremental construction.
    pub fn builder() -> VersionBuilder {
        VersionBuilder::new()
    }
}

impl Default for Version {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for constructing Version instances.
pub struct VersionBuilder {
    version: Version,
}

impl VersionBuilder {
    /// Create a new builder with empty version.
    pub fn new() -> Self {
        Self {
            version: Version::new(),
        }
    }

    /// Set the next SST ID.
    pub fn next_sst_id(mut self, id: u64) -> Self {
        self.version.next_sst_id = id;
        self
    }

    /// Set the version number.
    pub fn version_num(mut self, num: u64) -> Self {
        self.version.version_num = num;
        self
    }

    /// Set the flushed LSN.
    pub fn flushed_lsn(mut self, lsn: u64) -> Self {
        self.version.flushed_lsn = lsn;
        self
    }

    /// Add an SST to a level (uses level from SstMeta).
    pub fn add_sst(mut self, sst: SstMeta) -> Self {
        let level = sst.level as usize;
        if level < MAX_LEVELS {
            self.version.levels[level].push(Arc::new(sst));
        }
        self
    }

    /// Add an SST to a specific level (overrides SstMeta level).
    pub fn add_sst_at_level(mut self, level: u32, sst: Arc<SstMeta>) -> Self {
        let level = level as usize;
        if level < MAX_LEVELS {
            self.version.levels[level].push(sst);
        }
        self
    }

    /// Build the version.
    pub fn build(mut self) -> Version {
        // Sort L0 by ID (newest first)
        self.version.levels[0].sort_by(|a, b| b.id.cmp(&a.id));

        // Sort L1+ by smallest key
        for level in 1..MAX_LEVELS {
            self.version.levels[level].sort_by(|a, b| a.smallest_key.cmp(&b.smallest_key));
        }

        self.version
    }
}

impl Default for VersionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// ManifestDelta represents an incremental change to the version.
///
/// Deltas are persisted to the manifest (via clog) for recovery.
/// On startup, we replay all deltas to reconstruct the current version.
#[derive(Debug, Clone, Default)]
pub struct ManifestDelta {
    /// New SST files added (from flush or compaction).
    pub new_ssts: Vec<Arc<SstMeta>>,

    /// SST files deleted (level, sst_id).
    ///
    /// Deleted during compaction when SSTs are merged.
    pub deleted_ssts: Vec<(u32, u64)>,

    /// Updated flushed LSN (after memtable flush).
    pub flushed_lsn: Option<u64>,

    /// Sequence number for ordering deltas.
    pub sequence: u64,
}

impl ManifestDelta {
    /// Create a new empty delta.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a delta for a memtable flush.
    pub fn flush(sst: SstMeta, flushed_lsn: u64) -> Self {
        Self {
            new_ssts: vec![Arc::new(sst)],
            deleted_ssts: Vec::new(),
            flushed_lsn: Some(flushed_lsn),
            sequence: 0,
        }
    }

    /// Create a delta for a compaction.
    pub fn compaction(new_ssts: Vec<SstMeta>, deleted_ssts: Vec<(u32, u64)>) -> Self {
        Self {
            new_ssts: new_ssts.into_iter().map(Arc::new).collect(),
            deleted_ssts,
            flushed_lsn: None,
            sequence: 0,
        }
    }

    /// Add a new SST to the delta.
    pub fn add_sst(&mut self, sst: SstMeta) {
        self.new_ssts.push(Arc::new(sst));
    }

    /// Add an SST to delete.
    pub fn delete_sst(&mut self, level: u32, sst_id: u64) {
        self.deleted_ssts.push((level, sst_id));
    }

    /// Set the flushed LSN.
    pub fn set_flushed_lsn(&mut self, lsn: u64) {
        self.flushed_lsn = Some(lsn);
    }

    /// Check if the delta is empty (no changes).
    pub fn is_empty(&self) -> bool {
        self.new_ssts.is_empty() && self.deleted_ssts.is_empty() && self.flushed_lsn.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_version_new() {
        let v = Version::new();
        assert_eq!(v.version_num(), 0);
        assert_eq!(v.next_sst_id(), 1);
        assert_eq!(v.flushed_lsn(), 0);
        assert_eq!(v.total_sst_count(), 0);
    }

    #[test]
    fn test_version_builder() {
        let sst1 = make_sst(1, 0, b"a", b"m");
        let sst2 = make_sst(2, 0, b"n", b"z");
        let sst3 = make_sst(3, 1, b"a", b"z");

        let v = Version::builder()
            .next_sst_id(4)
            .version_num(1)
            .flushed_lsn(100)
            .add_sst(sst1)
            .add_sst(sst2)
            .add_sst(sst3)
            .build();

        assert_eq!(v.version_num(), 1);
        assert_eq!(v.next_sst_id(), 4);
        assert_eq!(v.flushed_lsn(), 100);
        assert_eq!(v.level_size(0), 2);
        assert_eq!(v.level_size(1), 1);
        assert_eq!(v.total_sst_count(), 3);
    }

    #[test]
    fn test_version_apply_delta_add() {
        let v = Version::new();

        let sst = make_sst(1, 0, b"a", b"z");
        let delta = ManifestDelta::flush(sst, 50);

        let v2 = v.apply(&delta);

        assert_eq!(v2.version_num(), 1);
        assert_eq!(v2.level_size(0), 1);
        assert_eq!(v2.flushed_lsn(), 50);
        assert_eq!(v2.next_sst_id(), 2);
    }

    #[test]
    fn test_version_apply_delta_delete() {
        let sst = make_sst(1, 0, b"a", b"z");
        let v = Version::builder().add_sst(sst).build();

        assert_eq!(v.level_size(0), 1);

        let delta = ManifestDelta {
            new_ssts: vec![],
            deleted_ssts: vec![(0, 1)],
            flushed_lsn: None,
            sequence: 0,
        };

        let v2 = v.apply(&delta);

        assert_eq!(v2.level_size(0), 0);
    }

    #[test]
    fn test_version_apply_compaction() {
        // Start with 2 SSTs at L0
        let sst1 = make_sst(1, 0, b"a", b"m");
        let sst2 = make_sst(2, 0, b"n", b"z");
        let v = Version::builder()
            .add_sst(sst1)
            .add_sst(sst2)
            .next_sst_id(3)
            .build();

        assert_eq!(v.level_size(0), 2);

        // Compact L0 -> L1: delete both L0 SSTs, add one L1 SST
        let new_sst = make_sst(3, 1, b"a", b"z");
        let delta = ManifestDelta::compaction(vec![new_sst], vec![(0, 1), (0, 2)]);

        let v2 = v.apply(&delta);

        assert_eq!(v2.level_size(0), 0);
        assert_eq!(v2.level_size(1), 1);
        assert_eq!(v2.next_sst_id(), 4);
    }

    #[test]
    fn test_version_find_ssts_for_key() {
        let sst1 = make_sst(1, 0, b"a", b"m");
        let sst2 = make_sst(2, 0, b"n", b"z");
        let sst3 = make_sst(3, 1, b"a", b"g");
        let sst4 = make_sst(4, 1, b"h", b"z");

        let v = Version::builder()
            .add_sst(sst1)
            .add_sst(sst2)
            .add_sst(sst3)
            .add_sst(sst4)
            .build();

        // Key "b" should match sst1 (L0) and sst3 (L1)
        let matches = v.find_ssts_for_key(b"b");
        assert_eq!(matches.len(), 2);

        // Key "x" should match sst2 (L0) and sst4 (L1)
        let matches = v.find_ssts_for_key(b"x");
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn test_version_find_ssts_for_range() {
        let sst1 = make_sst(1, 0, b"a", b"e");
        let sst2 = make_sst(2, 0, b"f", b"j");
        let sst3 = make_sst(3, 0, b"k", b"z");

        let v = Version::builder()
            .add_sst(sst1)
            .add_sst(sst2)
            .add_sst(sst3)
            .build();

        // Range [c, h) should match sst1 and sst2
        let matches = v.find_ssts_for_range(b"c", b"h");
        assert_eq!(matches.len(), 2);

        // Range [l, z) should match only sst3
        let matches = v.find_ssts_for_range(b"l", b"z");
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn test_version_l0_ordering() {
        // L0 SSTs should be ordered newest first (highest ID first)
        let sst1 = make_sst(1, 0, b"a", b"z");
        let sst2 = make_sst(2, 0, b"a", b"z");
        let sst3 = make_sst(3, 0, b"a", b"z");

        let v = Version::builder()
            .add_sst(sst1)
            .add_sst(sst2)
            .add_sst(sst3)
            .build();

        let l0 = v.level(0);
        assert_eq!(l0[0].id, 3); // Newest first
        assert_eq!(l0[1].id, 2);
        assert_eq!(l0[2].id, 1);
    }

    #[test]
    fn test_version_l1_ordering() {
        // L1+ SSTs should be ordered by smallest key
        let sst1 = make_sst(1, 1, b"m", b"p");
        let sst2 = make_sst(2, 1, b"a", b"f");
        let sst3 = make_sst(3, 1, b"g", b"l");

        let v = Version::builder()
            .add_sst(sst1)
            .add_sst(sst2)
            .add_sst(sst3)
            .build();

        let l1 = v.level(1);
        assert_eq!(l1[0].id, 2); // smallest_key = "a"
        assert_eq!(l1[1].id, 3); // smallest_key = "g"
        assert_eq!(l1[2].id, 1); // smallest_key = "m"
    }

    #[test]
    fn test_manifest_delta_new() {
        let delta = ManifestDelta::new();
        assert!(delta.is_empty());
    }

    #[test]
    fn test_manifest_delta_flush() {
        let sst = make_sst(1, 0, b"a", b"z");
        let delta = ManifestDelta::flush(sst, 100);

        assert!(!delta.is_empty());
        assert_eq!(delta.new_ssts.len(), 1);
        assert_eq!(delta.flushed_lsn, Some(100));
    }

    #[test]
    fn test_manifest_delta_compaction() {
        let sst1 = make_sst(3, 1, b"a", b"m");
        let sst2 = make_sst(4, 1, b"n", b"z");

        let delta = ManifestDelta::compaction(vec![sst1, sst2], vec![(0, 1), (0, 2)]);

        assert!(!delta.is_empty());
        assert_eq!(delta.new_ssts.len(), 2);
        assert_eq!(delta.deleted_ssts.len(), 2);
        assert_eq!(delta.flushed_lsn, None);
    }

    #[test]
    fn test_version_level_size_bytes() {
        let mut sst1 = make_sst(1, 0, b"a", b"m");
        sst1.file_size = 1000;
        let mut sst2 = make_sst(2, 0, b"n", b"z");
        sst2.file_size = 2000;

        let v = Version::builder().add_sst(sst1).add_sst(sst2).build();

        assert_eq!(v.level_size_bytes(0), 3000);
        assert_eq!(v.total_size_bytes(), 3000);
    }

    #[test]
    fn test_find_overlapping_at_level() {
        let sst1 = make_sst(1, 1, b"a", b"e");
        let sst2 = make_sst(2, 1, b"f", b"j");
        let sst3 = make_sst(3, 1, b"k", b"z");

        let v = Version::builder()
            .add_sst(sst1)
            .add_sst(sst2)
            .add_sst(sst3)
            .build();

        // Range [c, h) at L1 should match sst1 and sst2
        let matches = v.find_overlapping_at_level(1, b"c", b"h");
        assert_eq!(matches.len(), 2);

        // Range [c, h) at L0 should find nothing (no L0 SSTs)
        let matches = v.find_overlapping_at_level(0, b"c", b"h");
        assert_eq!(matches.len(), 0);
    }
}
