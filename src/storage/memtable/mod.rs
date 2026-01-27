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
//! This module provides multiple memtable implementations:
//!
//! - [`CrossbeamMemTableEngine`]: Lock-free skiplist with epoch-based GC (default)
//! - [`ArenaMemTableEngine`]: Arena-based skiplist with bulk deallocation
//! - [`BTreeMemTableEngine`]: BTreeMap + RwLock (baseline for comparison)
//!
//! All implementations support MVCC through timestamped keys.

pub mod arena_memtable;
pub mod arena_skiplist;
pub mod btree_memtable;
pub mod crossbeam_memtable;
pub mod wrapper;

// Re-export memtable engines
pub use arena_memtable::ArenaMemTableEngine;
pub use arena_memtable::MemoryStats as ArenaMemoryStats;
pub use btree_memtable::BTreeMemTableEngine;
pub use crossbeam_memtable::CrossbeamMemTableEngine;
pub use crossbeam_memtable::MemoryStats;

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

// Default memtable engine alias
pub use crossbeam_memtable::CrossbeamMemTableEngine as MemTableEngine;

// LSM MemTable wrapper with metadata
pub use wrapper::MemTable;
