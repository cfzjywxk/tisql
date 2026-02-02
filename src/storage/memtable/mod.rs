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

//! MemTable implementations for in-memory key-value storage.
//!
//! ## Production Engine
//!
//! [`VersionedMemTableEngine`] is the default production memtable engine. It uses an
//! OceanBase-style design where each user key is stored once in a skiplist with a
//! linked list of versions, providing:
//!
//! - **Space efficiency**: User key stored once per row, not repeated per version
//! - **Fast point lookups**: Seek to user key, traverse short version chain
//! - **Better cache locality**: All versions of a key are adjacent in memory

pub mod versioned_memtable;
pub mod wrapper;

// Default memtable engine (OceanBase-style versioned memtable)
pub use versioned_memtable::ArcVersionedMemTableIterator;
pub use versioned_memtable::VersionedMemTableEngine as MemTableEngine;
pub use versioned_memtable::VersionedMemTableEngine;
pub use versioned_memtable::VersionedMemTableIterator;
pub use versioned_memtable::VersionedMemoryStats;
pub use versioned_memtable::VersionedMemoryStats as MemoryStats;

// LSM MemTable wrapper with metadata
pub use wrapper::MemTable;

// ============================================================================
// Common Types
// ============================================================================

/// Result of a point lookup that distinguishes between "not found" and "tombstone found".
///
/// This tri-state is critical for correct MVCC behavior: when a tombstone is found
/// in a memtable, we must NOT continue searching older levels (SSTs), as that would
/// incorrectly "resurrect" deleted keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetResult {
    /// Key found with a value.
    Found(Vec<u8>),
    /// Key found but it's a tombstone (delete marker).
    FoundTombstone,
    /// Key not found at this level.
    NotFound,
}

impl GetResult {
    /// Convert to Option, treating both NotFound and FoundTombstone as None.
    pub fn into_option(self) -> Option<Vec<u8>> {
        match self {
            GetResult::Found(v) => Some(v),
            GetResult::FoundTombstone | GetResult::NotFound => None,
        }
    }

    /// Check if this result indicates the key was found (value or tombstone).
    pub fn is_found(&self) -> bool {
        matches!(self, GetResult::Found(_) | GetResult::FoundTombstone)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_result_found_into_option() {
        let result = GetResult::Found(b"value".to_vec());
        assert_eq!(result.into_option(), Some(b"value".to_vec()));
    }

    #[test]
    fn test_get_result_tombstone_into_option() {
        let result = GetResult::FoundTombstone;
        assert_eq!(result.into_option(), None);
    }

    #[test]
    fn test_get_result_not_found_into_option() {
        let result = GetResult::NotFound;
        assert_eq!(result.into_option(), None);
    }

    #[test]
    fn test_get_result_is_found() {
        assert!(GetResult::Found(b"v".to_vec()).is_found());
        assert!(GetResult::FoundTombstone.is_found());
        assert!(!GetResult::NotFound.is_found());
    }

    #[test]
    fn test_get_result_debug_and_clone() {
        let result = GetResult::Found(b"test".to_vec());
        let cloned = result.clone();
        assert_eq!(result, cloned);
        let _ = format!("{result:?}");
    }
}
