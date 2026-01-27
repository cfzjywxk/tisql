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

// Default memtable engine alias
pub use crossbeam_memtable::CrossbeamMemTableEngine as MemTableEngine;

// LSM MemTable wrapper with metadata
pub use wrapper::MemTable;
