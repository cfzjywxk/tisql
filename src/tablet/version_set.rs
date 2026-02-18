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

//! VersionSet and SuperVersion - Version management for LSM-tree.
//!
//! This module provides:
//!
//! - `VersionSet`: Manages the current SST version with atomic updates
//! - `SuperVersion`: Combined snapshot of memtables + SST version for readers
//!
//! ## Design
//!
//! Currently uses RwLock for simplicity. Future optimization can add
//! arc-swap for lock-free reads if needed.
//!
//! ## Usage
//!
//! ```ignore
//! let version_set = VersionSet::new(Version::new());
//!
//! // Read current version (acquires read lock briefly)
//! let version = version_set.current();
//!
//! // Apply delta (acquires write lock)
//! let delta = ManifestDelta::flush(sst_meta, flushed_lsn);
//! let new_version = version_set.apply_delta(&delta);
//!
//! // Get a SuperVersion snapshot for reading
//! let sv = engine.get_super_version();
//! // sv contains consistent snapshot of active, frozen, and SST version
//! ```

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::RwLock;

use super::memtable::MemTable;
use super::version::{ManifestDelta, Version};

/// Minimal VersionSet - wraps Version with clean API.
///
/// Uses parking_lot::RwLock for fair scheduling (no writer starvation of readers).
/// Future optimization can add arc-swap for lock-free reads.
pub struct VersionSet {
    current: RwLock<Arc<Version>>,
}

impl VersionSet {
    /// Create a new VersionSet with the given initial version.
    pub fn new(initial: Version) -> Self {
        Self {
            current: RwLock::new(Arc::new(initial)),
        }
    }

    /// Get the current version.
    ///
    /// Acquires a read lock briefly to clone the Arc.
    pub fn current(&self) -> Arc<Version> {
        Arc::clone(&self.current.read())
    }

    /// Apply a manifest delta and return the new version.
    ///
    /// Acquires a write lock to atomically update the current version.
    pub fn apply_delta(&self, delta: &ManifestDelta) -> Arc<Version> {
        let mut current = self.current.write();
        let new_version = current.apply(delta);
        let new_arc = Arc::new(new_version);
        *current = Arc::clone(&new_arc);
        new_arc
    }
}

// ============================================================================
// SuperVersion - Combined snapshot for readers
// ============================================================================

/// SuperVersion combines memtables + SST version into a single consistent snapshot.
///
/// This is what readers acquire - a point-in-time view of:
/// - Active memtable (at snapshot time)
/// - Frozen memtables (at snapshot time)
/// - SST files via Version (at snapshot time)
///
/// ## Snapshot Isolation
///
/// Once acquired, a SuperVersion provides a stable view even if:
/// - New writes go to the active memtable
/// - Memtables are frozen
/// - Memtables are flushed to SST
/// - Version is updated
///
/// The Arc references keep the underlying data alive until all readers finish.
///
/// ## Usage
///
/// ```ignore
/// let sv = engine.get_super_version();
///
/// // Use sv for reading - holds consistent snapshot
/// let iter = sv.new_iterator(range, owner_ts);
///
/// // sv can be held across multiple operations
/// // Drop sv when done to allow GC of old versions
/// ```
#[derive(Clone)]
pub struct SuperVersion {
    /// Active memtable at snapshot time.
    pub active: Arc<MemTable>,

    /// Frozen memtables at snapshot time (oldest first, newest at back).
    pub frozen: VecDeque<Arc<MemTable>>,

    /// SST file version at snapshot time.
    pub version: Arc<Version>,

    /// SuperVersion sequence number (for debugging and staleness detection).
    pub sv_number: u64,
}

impl SuperVersion {
    /// Create a new SuperVersion with the given components.
    pub fn new(
        active: Arc<MemTable>,
        frozen: VecDeque<Arc<MemTable>>,
        version: Arc<Version>,
        sv_number: u64,
    ) -> Self {
        Self {
            active,
            frozen,
            version,
            sv_number,
        }
    }

    /// Check if this SuperVersion is current (not stale).
    ///
    /// A SuperVersion becomes stale when a new one is created (memtable
    /// rotation, flush, etc.). Stale snapshots are still valid for reading,
    /// but callers may want to refresh for fresher data.
    pub fn is_current(&self, current_sv_number: u64) -> bool {
        self.sv_number == current_sv_number
    }

    /// Get the total number of memtables (active + frozen).
    pub fn memtable_count(&self) -> usize {
        1 + self.frozen.len()
    }

    /// Get the SST version number.
    pub fn version_num(&self) -> u64 {
        self.version.version_num()
    }
}

impl std::fmt::Debug for SuperVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SuperVersion")
            .field("sv_number", &self.sv_number)
            .field("active_memtable_id", &self.active.id())
            .field("frozen_count", &self.frozen.len())
            .field("version_num", &self.version.version_num())
            .field("sst_count", &self.version.total_sst_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tablet::sstable::SstMeta;

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
    fn test_version_set_new() {
        let vs = VersionSet::new(Version::new());
        let v = vs.current();
        assert_eq!(v.version_num(), 0);
        assert_eq!(v.total_sst_count(), 0);
    }

    #[test]
    fn test_version_set_apply_delta() {
        let vs = VersionSet::new(Version::new());

        // Apply a flush delta
        let sst = make_sst(1, 0, b"a", b"z");
        let delta = ManifestDelta::flush(sst, 100);
        let v2 = vs.apply_delta(&delta);

        assert_eq!(v2.version_num(), 1);
        assert_eq!(v2.level_size(0), 1);
        assert_eq!(v2.flushed_lsn(), 100);

        // Current should return the new version
        let current = vs.current();
        assert_eq!(current.version_num(), 1);
    }

    #[test]
    fn test_version_set_multiple_deltas() {
        let vs = VersionSet::new(Version::new());

        // Apply first delta
        let sst1 = make_sst(1, 0, b"a", b"m");
        let delta1 = ManifestDelta::flush(sst1, 50);
        vs.apply_delta(&delta1);

        // Apply second delta
        let sst2 = make_sst(2, 0, b"n", b"z");
        let delta2 = ManifestDelta::flush(sst2, 100);
        vs.apply_delta(&delta2);

        let v = vs.current();
        assert_eq!(v.version_num(), 2);
        assert_eq!(v.level_size(0), 2);
        assert_eq!(v.flushed_lsn(), 100);
    }

    #[test]
    fn test_version_set_old_version_survives() {
        let vs = VersionSet::new(Version::new());

        // Get a reference to the old version
        let old_version = vs.current();
        assert_eq!(old_version.version_num(), 0);

        // Apply delta
        let sst = make_sst(1, 0, b"a", b"z");
        let delta = ManifestDelta::flush(sst, 100);
        vs.apply_delta(&delta);

        // Old version should still be accessible
        assert_eq!(old_version.version_num(), 0);
        assert_eq!(old_version.level_size(0), 0);

        // New current should be different
        let new_version = vs.current();
        assert_eq!(new_version.version_num(), 1);
        assert_eq!(new_version.level_size(0), 1);
    }

    // ==================== SuperVersion Tests ====================

    #[test]
    fn test_super_version_new() {
        let active = Arc::new(MemTable::new(1));
        let frozen = VecDeque::from([Arc::new(MemTable::new(2)), Arc::new(MemTable::new(3))]);
        let version = Arc::new(Version::new());

        let sv = SuperVersion::new(active, frozen, version, 42);

        assert_eq!(sv.sv_number, 42);
        assert_eq!(sv.memtable_count(), 3); // 1 active + 2 frozen
        assert_eq!(sv.version_num(), 0);
    }

    #[test]
    fn test_super_version_is_current() {
        let sv = SuperVersion::new(
            Arc::new(MemTable::new(1)),
            VecDeque::new(),
            Arc::new(Version::new()),
            100,
        );

        assert!(sv.is_current(100));
        assert!(!sv.is_current(99));
        assert!(!sv.is_current(101));
    }

    #[test]
    fn test_super_version_clone() {
        let active = Arc::new(MemTable::new(1));
        let frozen = VecDeque::from([Arc::new(MemTable::new(2))]);
        let version = Arc::new(Version::new());

        let sv1 = SuperVersion::new(Arc::clone(&active), frozen.clone(), Arc::clone(&version), 1);
        let sv2 = sv1.clone();

        // Both should reference the same underlying data
        assert_eq!(sv1.sv_number, sv2.sv_number);
        assert_eq!(sv1.active.id(), sv2.active.id());
        assert_eq!(sv1.frozen.len(), sv2.frozen.len());

        // Arc strong count should be 3 (active, sv1.active, sv2.active)
        assert_eq!(Arc::strong_count(&active), 3);
    }

    #[test]
    fn test_super_version_debug() {
        let sv = SuperVersion::new(
            Arc::new(MemTable::new(42)),
            VecDeque::from([Arc::new(MemTable::new(41))]),
            Arc::new(Version::new()),
            123,
        );

        let debug_str = format!("{sv:?}");
        assert!(debug_str.contains("sv_number: 123"));
        assert!(debug_str.contains("active_memtable_id: 42"));
        assert!(debug_str.contains("frozen_count: 1"));
    }

    #[test]
    fn test_super_version_holds_refs() {
        let active = Arc::new(MemTable::new(1));
        let frozen_mt = Arc::new(MemTable::new(2));
        let version = Arc::new(Version::new());

        // Initial ref counts
        assert_eq!(Arc::strong_count(&active), 1);
        assert_eq!(Arc::strong_count(&frozen_mt), 1);
        assert_eq!(Arc::strong_count(&version), 1);

        // Create SuperVersion
        let sv = SuperVersion::new(
            Arc::clone(&active),
            VecDeque::from([Arc::clone(&frozen_mt)]),
            Arc::clone(&version),
            1,
        );

        // Ref counts should increase
        assert_eq!(Arc::strong_count(&active), 2);
        assert_eq!(Arc::strong_count(&frozen_mt), 2);
        assert_eq!(Arc::strong_count(&version), 2);

        // Drop SuperVersion
        drop(sv);

        // Ref counts should decrease
        assert_eq!(Arc::strong_count(&active), 1);
        assert_eq!(Arc::strong_count(&frozen_mt), 1);
        assert_eq!(Arc::strong_count(&version), 1);
    }

    #[test]
    fn test_super_version_snapshot_isolation() {
        // Simulate snapshot isolation: sv1 taken before version update,
        // sv2 taken after. They should see different versions.

        let vs = VersionSet::new(Version::new());
        let active = Arc::new(MemTable::new(1));
        let frozen: VecDeque<Arc<MemTable>> = VecDeque::new();

        // Take snapshot before any SST
        let sv1 = SuperVersion::new(Arc::clone(&active), frozen.clone(), vs.current(), 1);
        assert_eq!(sv1.version.total_sst_count(), 0);

        // Add an SST
        let sst = make_sst(1, 0, b"a", b"z");
        let delta = ManifestDelta::flush(sst, 100);
        vs.apply_delta(&delta);

        // Take snapshot after SST
        let sv2 = SuperVersion::new(Arc::clone(&active), frozen.clone(), vs.current(), 2);

        // sv1 still sees old version (no SSTs)
        assert_eq!(sv1.version.total_sst_count(), 0);
        assert_eq!(sv1.version.version_num(), 0);

        // sv2 sees new version (1 SST)
        assert_eq!(sv2.version.total_sst_count(), 1);
        assert_eq!(sv2.version.version_num(), 1);
    }
}
