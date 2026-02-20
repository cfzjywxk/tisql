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

use std::hash::{Hash, Hasher};
use std::path::Path;

/// Tablet namespace used by cache keys.
pub type TabletCacheNs = u128;

/// Kind of block stored in shared block cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlockKind {
    Data,
    Index,
    Bloom,
}

/// Priority class used by shared block cache admission/eviction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CachePriority {
    Normal,
    High,
}

/// Shared block-cache key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockCacheKey {
    pub ns: TabletCacheNs,
    pub sst_id: u64,
    pub block_offset: u64,
    pub block_kind: BlockKind,
}

/// Reader-cache key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReaderCacheKey {
    pub ns: TabletCacheNs,
    pub sst_id: u64,
}

/// Row-cache key for snapshot point reads.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowCacheKey {
    pub ns: TabletCacheNs,
    pub user_key: Vec<u8>,
    pub read_ts: u64,
}

/// Stable namespace derived from a tablet's data directory.
pub fn tablet_namespace_from_dir(dir: &Path) -> TabletCacheNs {
    // Build 128-bit tag from two independent 64-bit hashes.
    let dir = dir.to_string_lossy();

    let mut h1 = std::collections::hash_map::DefaultHasher::new();
    "tisql-cache-ns-v1".hash(&mut h1);
    dir.hash(&mut h1);

    let mut h2 = std::collections::hash_map::DefaultHasher::new();
    "tisql-cache-ns-v2".hash(&mut h2);
    dir.hash(&mut h2);

    ((h1.finish() as u128) << 64) | h2.finish() as u128
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tablet_namespace_from_dir_stable_for_same_path() {
        let a = tablet_namespace_from_dir(Path::new("/tmp/tisql/tablet-1"));
        let b = tablet_namespace_from_dir(Path::new("/tmp/tisql/tablet-1"));
        assert_eq!(a, b);
    }

    #[test]
    fn test_tablet_namespace_from_dir_differs_for_different_paths() {
        let a = tablet_namespace_from_dir(Path::new("/tmp/tisql/tablet-1"));
        let b = tablet_namespace_from_dir(Path::new("/tmp/tisql/tablet-2"));
        assert_ne!(a, b);
    }
}
