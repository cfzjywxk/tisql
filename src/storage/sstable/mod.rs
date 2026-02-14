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

//! SST (Sorted String Table) file format and operations.
//!
//! This module provides the building blocks for SST files used in
//! persistent storage:
//!
//! - `block`: Data blocks and index blocks with CRC32 checksums
//! - `builder`: SST file builder for creating SST files
//! - `reader`: SST file reader for point lookups
//! - `iterator`: SST iterators for sequential access
//!
//! ## SST File Format
//!
//! ```text
//! +-------------------------------------------------------------------+
//! | Data Block 0                                                       |
//! +-------------------------------------------------------------------+
//! | Data Block 1                                                       |
//! +-------------------------------------------------------------------+
//! | ...                                                                |
//! +-------------------------------------------------------------------+
//! | Data Block N                                                       |
//! +-------------------------------------------------------------------+
//! | Index Block                                                        |
//! +-------------------------------------------------------------------+
//! | Footer (48 bytes)                                                  |
//! +-------------------------------------------------------------------+
//! ```

pub mod block;
pub mod builder;
pub mod iterator;
pub mod reader;

// Re-export commonly used types
pub use block::{
    DataBlock, DataBlockBuilder, DataBlockIterator, IndexBlock, IndexBlockBuilder, IndexEntry,
    DEFAULT_BLOCK_SIZE,
};

pub use builder::{
    AsyncSstBuilder, CompressionType, Footer, SstBuilder, SstBuilderOptions, SstMeta, FOOTER_SIZE,
    SST_MAGIC, SST_VERSION,
};

pub use reader::{SstReader, SstReaderRef};

pub use iterator::{ConcatIterator, SstIterator, SstMvccIterator};
