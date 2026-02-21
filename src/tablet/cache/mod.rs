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

pub mod block_cache;
pub mod key;
pub mod reader_cache;
pub mod row_cache;
pub mod suite;

pub use block_cache::{
    BlockCacheNamespaceUsage, BlockCacheStats, BlockCacheValue, SharedBlockCache,
};
pub use key::{
    tablet_namespace_from_dir, BlockCacheKey, BlockKind, CachePriority, ReaderCacheKey,
    RowCacheKey, TabletCacheNs,
};
pub use reader_cache::{ReaderCache, ReaderCacheStats};
pub use row_cache::{RowCache, RowCacheNamespaceUsage, RowCacheStats};
pub use suite::{CacheSuite, CacheSuiteConfig};
