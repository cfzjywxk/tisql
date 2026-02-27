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

use std::path::Path;

use crate::util::stable_hash64;

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

/// Row-cache key for committed snapshot versions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowCacheKey {
    pub ns: TabletCacheNs,
    pub user_key: Vec<u8>,
    pub commit_ts: u64,
}

/// Stable namespace derived from a tablet's data directory.
pub fn tablet_namespace_from_dir(dir: &Path) -> TabletCacheNs {
    const NS_SEED_HI: u64 = 0x5449_5351_4c2d_4e31; // "TISQL-N1"
    const NS_SEED_LO: u64 = 0x5449_5351_4c2d_4e32; // "TISQL-N2"

    let dir = dir.to_string_lossy();
    let h1 = stable_hash64(NS_SEED_HI, dir.as_bytes());
    let h2 = stable_hash64(NS_SEED_LO, dir.as_bytes());
    ((h1 as u128) << 64) | h2 as u128
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
    fn test_tablet_namespace_from_dir_known_vector() {
        assert_eq!(
            tablet_namespace_from_dir(Path::new("/tmp/tisql/tablet-1")),
            0x0238_e363_4248_fde9_bce2_2145_81e4_15cb
        );
    }

    #[test]
    fn test_tablet_namespace_from_dir_differs_for_different_paths() {
        let a = tablet_namespace_from_dir(Path::new("/tmp/tisql/tablet-1"));
        let b = tablet_namespace_from_dir(Path::new("/tmp/tisql/tablet-2"));
        assert_ne!(a, b);
    }
}
