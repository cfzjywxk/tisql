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

//! VersionSet - Manages the current LSM-tree version.
//!
//! This module provides a simple wrapper around `Version` with clean API
//! for getting the current version and applying deltas atomically.
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
//! ```

use std::sync::{Arc, RwLock};

use super::version::{ManifestDelta, Version};

/// Minimal VersionSet - wraps Version with clean API.
///
/// Currently uses RwLock (same semantics as before extraction).
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
        Arc::clone(&self.current.read().unwrap())
    }

    /// Apply a manifest delta and return the new version.
    ///
    /// Acquires a write lock to atomically update the current version.
    pub fn apply_delta(&self, delta: &ManifestDelta) -> Arc<Version> {
        let mut current = self.current.write().unwrap();
        let new_version = current.apply(delta);
        let new_arc = Arc::new(new_version);
        *current = Arc::clone(&new_arc);
        new_arc
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sstable::SstMeta;

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
}
