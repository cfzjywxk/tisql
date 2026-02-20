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

//! SST file builder for creating SST files from key-value entries.
//!
//! ## Usage
//!
//! ```ignore
//! let mut builder = SstBuilder::new(path, SstBuilderOptions::default())?;
//!
//! // Add entries in sorted order
//! builder.add(b"key1", b"value1")?;
//! builder.add(b"key2", b"value2")?;
//!
//! // Finish and get metadata
//! let meta = builder.finish(sst_id, level)?;
//! ```
//!
//! ## SST File Layout
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
//! | Bloom Meta Trailer (optional, fixed size)                          |
//! +-------------------------------------------------------------------+
//! | Footer (fixed size)                                                |
//! +-------------------------------------------------------------------+
//! ```

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::catalog::types::Timestamp;
use crate::io::{AlignedBuf, DmaFile, IoService, DMA_ALIGNMENT};
use crate::tablet::mvcc::extract_key;
use crate::util::error::{Result, TiSqlError};
use crate::util::fs::sync_dir;
use serde::{Deserialize, Serialize};

use super::block::{DataBlockBuilder, IndexBlockBuilder, DEFAULT_BLOCK_SIZE};
use super::bloom::BloomBuilder;

// ============================================================================
// Constants
// ============================================================================

/// SST file magic number: "TSST" (TiSQL SST)
pub const SST_MAGIC: u32 = 0x54535354;

/// SST file format version
pub const SST_VERSION: u32 = 1;

/// Footer size in bytes (fixed)
pub const FOOTER_SIZE: usize = 48;

/// Optional bloom metadata trailer size in bytes.
pub const BLOOM_META_TRAILER_SIZE: usize = 24;

const BLOOM_META_MAGIC: u64 = 0x4154_454d_4d4f_4f42; // "BOOMMETA" (LE)

/// Compression type: None (reserved for future)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum CompressionType {
    #[default]
    None = 0,
    // Future: Lz4 = 1, Zstd = 2, etc.
}

impl CompressionType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(CompressionType::None),
            _ => None,
        }
    }
}

// ============================================================================
// SstMeta
// ============================================================================

/// Metadata for a single SST file.
///
/// Note: `smallest_key` and `largest_key` are MVCC keys (key || !commit_ts),
/// not plain keys. Use `extract_key()` to get the key portion without timestamp.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SstMeta {
    /// Unique SST file ID
    pub id: u64,
    /// Level this SST belongs to
    pub level: u32,
    /// Smallest MVCC key in this SST (inclusive, includes timestamp suffix)
    pub smallest_key: Vec<u8>,
    /// Largest MVCC key in this SST (inclusive, includes timestamp suffix)
    pub largest_key: Vec<u8>,
    /// File size in bytes
    pub file_size: u64,
    /// Number of entries (key-value pairs)
    pub entry_count: u64,
    /// Number of data blocks
    pub block_count: u32,
    /// Minimum timestamp in this SST (for MVCC)
    pub min_ts: Timestamp,
    /// Maximum timestamp in this SST (for MVCC)
    pub max_ts: Timestamp,
    /// Creation time (unix timestamp)
    pub created_at: u64,
}

impl SstMeta {
    /// Check if a key might be in this SST based on key range.
    ///
    /// Since SSTs store MVCC keys (key || !commit_ts), we extract the
    /// key portion from smallest_key and largest_key for comparison.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check (without MVCC timestamp suffix)
    pub fn may_contain_key(&self, key: &[u8]) -> bool {
        // Extract key portions from MVCC keys (remove last 8 bytes of timestamp)
        let smallest = extract_key(&self.smallest_key);
        let largest = extract_key(&self.largest_key);

        key >= smallest && key <= largest
    }

    /// Check if this SST overlaps with the given key range.
    ///
    /// Since SSTs store MVCC keys, we extract key portions for comparison.
    ///
    /// # Arguments
    ///
    /// * `start` - Start of key range (inclusive, without MVCC timestamp suffix)
    /// * `end` - End of key range (exclusive, without MVCC timestamp suffix)
    pub fn overlaps(&self, start: &[u8], end: &[u8]) -> bool {
        // Extract key portions from MVCC keys
        let smallest = extract_key(&self.smallest_key);
        let largest = extract_key(&self.largest_key);

        // SST range: [smallest, largest]
        // Query range: [start, end)
        // Overlaps if: smallest < end AND largest >= start
        smallest < end && largest >= start
    }

    /// Check if this SST's MVCC key range overlaps with another range.
    ///
    /// This compares MVCC keys directly without extracting the key portion.
    /// Each MVCC key is treated as an independent key in the storage engine.
    ///
    /// # Arguments
    ///
    /// * `start` - Start of MVCC key range (inclusive)
    /// * `end` - End of MVCC key range (exclusive), empty slice means unbounded
    pub fn overlaps_mvcc(&self, start: &[u8], end: &[u8]) -> bool {
        // Query range is [start, end)
        // SST is [smallest_key, largest_key]
        let start_le_largest = self.largest_key.as_slice() >= start;
        let end_gt_smallest = end.is_empty() || end > self.smallest_key.as_slice();
        start_le_largest && end_gt_smallest
    }
}

// ============================================================================
// SstBuilderOptions
// ============================================================================

/// Options for SST builder.
#[derive(Debug, Clone)]
pub struct SstBuilderOptions {
    /// Target data block size (default: 4KB)
    pub block_size: usize,
    /// Compression type (default: None)
    pub compression: CompressionType,
    /// Whether bloom filter is enabled for this SST.
    pub bloom_enabled: bool,
    /// Bloom filter bits per key.
    pub bloom_bits_per_key: u32,
}

impl Default for SstBuilderOptions {
    fn default() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            compression: CompressionType::None,
            bloom_enabled: false,
            bloom_bits_per_key: 12,
        }
    }
}

impl SstBuilderOptions {
    /// Create options with custom block size.
    pub fn with_block_size(block_size: usize) -> Self {
        Self {
            block_size,
            ..Default::default()
        }
    }
}

// ============================================================================
// Footer
// ============================================================================

/// SST file footer containing file metadata and offsets.
///
/// Footer format (48 bytes):
/// ```text
/// +------------------------+
/// | index_offset (8 bytes) |
/// +------------------------+
/// | index_size (4 bytes)   |
/// +------------------------+
/// | num_blocks (4 bytes)   |
/// +------------------------+
/// | num_entries (8 bytes)  |
/// +------------------------+
/// | min_ts (8 bytes)       |
/// +------------------------+
/// | max_ts (8 bytes)       |
/// +------------------------+
/// | compression (1 byte)   |
/// +------------------------+
/// | reserved (3 bytes)     |
/// +------------------------+
/// | magic (4 bytes)        |
/// +------------------------+
/// ```
#[derive(Debug, Clone)]
pub struct Footer {
    /// Offset of the index block from file start
    pub index_offset: u64,
    /// Size of the index block in bytes
    pub index_size: u32,
    /// Number of data blocks
    pub num_blocks: u32,
    /// Total number of entries
    pub num_entries: u64,
    /// Minimum timestamp
    pub min_ts: Timestamp,
    /// Maximum timestamp
    pub max_ts: Timestamp,
    /// Compression type
    pub compression: CompressionType,
}

impl Footer {
    /// Encode footer to bytes.
    pub fn encode(&self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0u8; FOOTER_SIZE];
        let mut offset = 0;

        // index_offset (8)
        buf[offset..offset + 8].copy_from_slice(&self.index_offset.to_le_bytes());
        offset += 8;

        // index_size (4)
        buf[offset..offset + 4].copy_from_slice(&self.index_size.to_le_bytes());
        offset += 4;

        // num_blocks (4)
        buf[offset..offset + 4].copy_from_slice(&self.num_blocks.to_le_bytes());
        offset += 4;

        // num_entries (8)
        buf[offset..offset + 8].copy_from_slice(&self.num_entries.to_le_bytes());
        offset += 8;

        // min_ts (8)
        buf[offset..offset + 8].copy_from_slice(&self.min_ts.to_le_bytes());
        offset += 8;

        // max_ts (8)
        buf[offset..offset + 8].copy_from_slice(&self.max_ts.to_le_bytes());
        offset += 8;

        // compression (1)
        buf[offset] = self.compression as u8;
        offset += 1;

        // reserved (3)
        offset += 3;

        // magic (4)
        buf[offset..offset + 4].copy_from_slice(&SST_MAGIC.to_le_bytes());

        buf
    }

    /// Decode footer from bytes.
    pub fn decode(data: &[u8; FOOTER_SIZE]) -> Result<Self> {
        let mut offset = 0;

        // index_offset (8)
        let index_offset = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // index_size (4)
        let index_size = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;

        // num_blocks (4)
        let num_blocks = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;

        // num_entries (8)
        let num_entries = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // min_ts (8)
        let min_ts = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // max_ts (8)
        let max_ts = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // compression (1)
        let compression = CompressionType::from_u8(data[offset])
            .ok_or_else(|| TiSqlError::Storage("Unknown compression type".into()))?;
        offset += 1;

        // reserved (3)
        offset += 3;

        // magic (4)
        let magic = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        if magic != SST_MAGIC {
            return Err(TiSqlError::Storage(format!(
                "Invalid SST magic: expected {SST_MAGIC:#x}, got {magic:#x}"
            )));
        }

        Ok(Self {
            index_offset,
            index_size,
            num_blocks,
            num_entries,
            min_ts,
            max_ts,
            compression,
        })
    }
}

/// Optional bloom metadata trailer, placed right before the footer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BloomMetaTrailer {
    pub bloom_offset: u64,
    pub bloom_size: u32,
}

impl BloomMetaTrailer {
    pub fn encode(&self) -> [u8; BLOOM_META_TRAILER_SIZE] {
        let mut buf = [0u8; BLOOM_META_TRAILER_SIZE];
        buf[0..8].copy_from_slice(&BLOOM_META_MAGIC.to_le_bytes());
        buf[8..16].copy_from_slice(&self.bloom_offset.to_le_bytes());
        buf[16..20].copy_from_slice(&self.bloom_size.to_le_bytes());
        // [20..24] reserved for future use
        buf
    }

    pub fn decode_optional(data: &[u8; BLOOM_META_TRAILER_SIZE]) -> Option<Self> {
        let magic = u64::from_le_bytes(data[0..8].try_into().ok()?);
        if magic != BLOOM_META_MAGIC {
            return None;
        }
        let bloom_offset = u64::from_le_bytes(data[8..16].try_into().ok()?);
        let bloom_size = u32::from_le_bytes(data[16..20].try_into().ok()?);
        if bloom_size == 0 {
            return None;
        }
        Some(Self {
            bloom_offset,
            bloom_size,
        })
    }
}

// ============================================================================
// SstBuilder
// ============================================================================

/// Builder for creating SST files.
///
/// Entries must be added in sorted key order.
pub struct SstBuilder {
    /// Output file path
    path: PathBuf,
    /// Buffered writer
    writer: BufWriter<File>,
    /// Builder options
    options: SstBuilderOptions,
    /// Current data block builder
    data_block: DataBlockBuilder,
    /// Index block builder
    index_block: IndexBlockBuilder,
    /// Current write offset in file
    current_offset: u64,
    /// Number of entries written
    entry_count: u64,
    /// Number of data blocks written
    block_count: u32,
    /// First key in the entire SST
    first_key: Option<Vec<u8>>,
    /// Last key in the entire SST
    last_key: Option<Vec<u8>>,
    /// Minimum timestamp seen
    min_ts: Timestamp,
    /// Maximum timestamp seen
    max_ts: Timestamp,
    /// Whether the builder has been finished
    finished: bool,
    /// Optional bloom builder for per-SST full filter.
    bloom_builder: Option<BloomBuilder>,
    /// Last user key admitted into bloom (dedupe adjacent MVCC versions).
    last_bloom_user_key: Option<Vec<u8>>,
}

impl SstBuilder {
    /// Create a new SST builder.
    pub fn new<P: AsRef<Path>>(path: P, options: SstBuilderOptions) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::create(&path)?;
        let writer = BufWriter::new(file);
        let bloom_builder = if options.bloom_enabled {
            Some(BloomBuilder::new(options.bloom_bits_per_key))
        } else {
            None
        };

        Ok(Self {
            path,
            writer,
            data_block: DataBlockBuilder::with_block_size(options.block_size),
            index_block: IndexBlockBuilder::new(),
            options,
            current_offset: 0,
            entry_count: 0,
            block_count: 0,
            first_key: None,
            last_key: None,
            min_ts: Timestamp::MAX,
            max_ts: 0,
            finished: false,
            bloom_builder,
            last_bloom_user_key: None,
        })
    }

    /// Add a key-value entry.
    ///
    /// Keys must be added in sorted order. The key should include the MVCC
    /// timestamp suffix (key || !commit_ts).
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.finished {
            return Err(TiSqlError::Storage("Builder already finished".into()));
        }

        // Track first/last key
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.last_key = Some(key.to_vec());

        // Extract timestamp from MVCC key for min/max tracking
        if key.len() >= 8 {
            let ts_bytes: [u8; 8] = key[key.len() - 8..].try_into().unwrap();
            // Key format: key || !commit_ts, so we need to invert
            let ts = !u64::from_be_bytes(ts_bytes);
            self.min_ts = self.min_ts.min(ts);
            self.max_ts = self.max_ts.max(ts);
        }

        if let Some(ref mut bloom) = self.bloom_builder {
            let user_key = extract_key(key);
            let should_add = self
                .last_bloom_user_key
                .as_ref()
                .is_none_or(|last| last.as_slice() != user_key);
            if should_add {
                bloom.add(user_key);
                self.last_bloom_user_key = Some(user_key.to_vec());
            }
        }

        self.entry_count += 1;

        // Try to add to current block
        if !self.data_block.add(key, value) {
            // Block is full, flush it and start a new one
            self.flush_data_block()?;

            // Add to new block (should always succeed for first entry)
            if !self.data_block.add(key, value) {
                return Err(TiSqlError::Storage("Entry too large for block".into()));
            }
        }

        Ok(())
    }

    /// Add a key-value entry with explicit timestamp.
    ///
    /// This is a convenience method that updates min/max timestamp tracking.
    pub fn add_with_ts(&mut self, key: &[u8], value: &[u8], ts: Timestamp) -> Result<()> {
        self.min_ts = self.min_ts.min(ts);
        self.max_ts = self.max_ts.max(ts);
        self.add(key, value)
    }

    /// Flush the current data block to disk.
    fn flush_data_block(&mut self) -> Result<()> {
        if self.data_block.is_empty() {
            return Ok(());
        }

        let first_key = self.data_block.first_key().cloned().unwrap_or_default();
        let block_data = std::mem::replace(
            &mut self.data_block,
            DataBlockBuilder::with_block_size(self.options.block_size),
        )
        .finish();

        let block_offset = self.current_offset;
        let block_size = block_data.len() as u32;

        // Write data block
        self.writer.write_all(&block_data)?;
        self.current_offset += block_data.len() as u64;

        // Add index entry
        self.index_block
            .add_entry(&first_key, block_offset, block_size);
        self.block_count += 1;

        Ok(())
    }

    /// Get the estimated file size.
    pub fn estimated_size(&self) -> u64 {
        let bloom_meta_size = if self.options.bloom_enabled {
            BLOOM_META_TRAILER_SIZE as u64
        } else {
            0
        };
        let bloom_payload_size = self
            .bloom_builder
            .as_ref()
            .map_or(0, BloomBuilder::estimated_encoded_size);
        self.current_offset
            + self.data_block.estimated_size() as u64
            + self.index_block.estimated_size() as u64
            + bloom_payload_size
            + bloom_meta_size
            + FOOTER_SIZE as u64
    }

    /// Check if the builder is empty (no entries added).
    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// Get the number of entries added.
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Finish building the SST and return metadata.
    ///
    /// This flushes any remaining data, writes the index block and footer,
    /// and syncs the file to disk.
    pub fn finish(mut self, sst_id: u64, level: u32) -> Result<SstMeta> {
        if self.finished {
            return Err(TiSqlError::Storage("Builder already finished".into()));
        }
        self.finished = true;

        // Flush remaining data block
        self.flush_data_block()?;

        let mut bloom_span: Option<(u64, u32)> = None;

        // Write optional bloom block right before index block.
        if let Some(bloom_builder) = self.bloom_builder.take() {
            if !bloom_builder.is_empty() {
                let bloom_offset = self.current_offset;
                let bloom_data = bloom_builder.finish().encode();
                self.writer.write_all(&bloom_data)?;
                self.current_offset += bloom_data.len() as u64;
                bloom_span = Some((bloom_offset, bloom_data.len() as u32));
            }
        }

        // Write index block
        let index_offset = self.current_offset;
        let index_data = std::mem::take(&mut self.index_block).finish();
        let index_size = index_data.len() as u32;

        self.writer.write_all(&index_data)?;
        self.current_offset += index_data.len() as u64;

        if let Some((bloom_offset, bloom_size)) = bloom_span {
            let trailer = BloomMetaTrailer {
                bloom_offset,
                bloom_size,
            };
            self.writer.write_all(&trailer.encode())?;
            self.current_offset += BLOOM_META_TRAILER_SIZE as u64;
        }

        // Build and write footer
        let footer = Footer {
            index_offset,
            index_size,
            num_blocks: self.block_count,
            num_entries: self.entry_count,
            min_ts: if self.min_ts == Timestamp::MAX {
                0
            } else {
                self.min_ts
            },
            max_ts: self.max_ts,
            compression: self.options.compression,
        };

        self.writer.write_all(&footer.encode())?;
        self.current_offset += FOOTER_SIZE as u64;

        // Flush and sync file
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        // Sync parent directory to make the file's existence durable
        if let Some(parent) = self.path.parent() {
            sync_dir(parent)?;
        }

        let file_size = self.current_offset;

        Ok(SstMeta {
            id: sst_id,
            level,
            smallest_key: self.first_key.take().unwrap_or_default(),
            largest_key: self.last_key.take().unwrap_or_default(),
            file_size,
            entry_count: self.entry_count,
            block_count: self.block_count,
            min_ts: if self.min_ts == Timestamp::MAX {
                0
            } else {
                self.min_ts
            },
            max_ts: self.max_ts,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        })
    }

    /// Get the output file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Abort building and delete the partial file.
    pub fn abort(mut self) -> Result<()> {
        // Mark as finished so Drop doesn't try to delete again
        self.finished = true;
        // Delete the file (writer will be dropped when self is dropped)
        if self.path.exists() {
            std::fs::remove_file(&self.path)?;
        }
        Ok(())
    }
}

impl Drop for SstBuilder {
    fn drop(&mut self) {
        // If not finished, try to clean up the partial file
        if !self.finished && self.path.exists() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Async builder for creating SST files via io_uring-backed writes.
///
/// This mirrors `SstBuilder` semantics but executes writes/fsync through
/// `IoService` so flush/compaction paths can stay async end-to-end.
pub struct AsyncSstBuilder {
    /// Output file path
    path: PathBuf,
    /// File descriptor for io_uring writes (opened without O_DIRECT for unaligned appends)
    file: DmaFile,
    /// Shared io_uring service
    io: Arc<IoService>,
    /// Builder options
    options: SstBuilderOptions,
    /// Current data block builder
    data_block: DataBlockBuilder,
    /// Index block builder
    index_block: IndexBlockBuilder,
    /// Current write offset in file
    current_offset: u64,
    /// Number of entries written
    entry_count: u64,
    /// Number of data blocks written
    block_count: u32,
    /// First key in the entire SST
    first_key: Option<Vec<u8>>,
    /// Last key in the entire SST
    last_key: Option<Vec<u8>>,
    /// Minimum timestamp seen
    min_ts: Timestamp,
    /// Maximum timestamp seen
    max_ts: Timestamp,
    /// Whether the builder has been finished
    finished: bool,
    /// Optional bloom builder for per-SST full filter.
    bloom_builder: Option<BloomBuilder>,
    /// Last user key admitted into bloom.
    last_bloom_user_key: Option<Vec<u8>>,
}

impl AsyncSstBuilder {
    /// Create a new async SST builder.
    pub fn new<P: AsRef<Path>>(
        path: P,
        options: SstBuilderOptions,
        io: Arc<IoService>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = DmaFile::open_write_buffered(&path).map_err(|e| {
            TiSqlError::Storage(format!(
                "Failed to open SST file for async write {}: {e}",
                path.display()
            ))
        })?;
        let bloom_builder = if options.bloom_enabled {
            Some(BloomBuilder::new(options.bloom_bits_per_key))
        } else {
            None
        };

        Ok(Self {
            path,
            file,
            io,
            data_block: DataBlockBuilder::with_block_size(options.block_size),
            index_block: IndexBlockBuilder::new(),
            options,
            current_offset: 0,
            entry_count: 0,
            block_count: 0,
            first_key: None,
            last_key: None,
            min_ts: Timestamp::MAX,
            max_ts: 0,
            finished: false,
            bloom_builder,
            last_bloom_user_key: None,
        })
    }

    /// Add a key-value entry.
    ///
    /// Keys must be added in sorted order. The key should include the MVCC
    /// timestamp suffix (key || !commit_ts).
    pub async fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.finished {
            return Err(TiSqlError::Storage("Builder already finished".into()));
        }

        // Track first/last key
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.last_key = Some(key.to_vec());

        // Extract timestamp from MVCC key for min/max tracking
        if key.len() >= 8 {
            let ts_bytes: [u8; 8] = key[key.len() - 8..].try_into().unwrap();
            // Key format: key || !commit_ts, so we need to invert
            let ts = !u64::from_be_bytes(ts_bytes);
            self.min_ts = self.min_ts.min(ts);
            self.max_ts = self.max_ts.max(ts);
        }

        if let Some(ref mut bloom) = self.bloom_builder {
            let user_key = extract_key(key);
            let should_add = self
                .last_bloom_user_key
                .as_ref()
                .is_none_or(|last| last.as_slice() != user_key);
            if should_add {
                bloom.add(user_key);
                self.last_bloom_user_key = Some(user_key.to_vec());
            }
        }

        self.entry_count += 1;

        // Try to add to current block
        if !self.data_block.add(key, value) {
            // Block is full, flush it and start a new one
            self.flush_data_block().await?;

            // Add to new block (should always succeed for first entry)
            if !self.data_block.add(key, value) {
                return Err(TiSqlError::Storage("Entry too large for block".into()));
            }
        }

        Ok(())
    }

    /// Flush the current data block to disk via io_uring.
    async fn flush_data_block(&mut self) -> Result<()> {
        if self.data_block.is_empty() {
            return Ok(());
        }

        let first_key = self.data_block.first_key().cloned().unwrap_or_default();
        let block_data = std::mem::replace(
            &mut self.data_block,
            DataBlockBuilder::with_block_size(self.options.block_size),
        )
        .finish();

        let block_offset = self.current_offset;
        let block_size = block_data.len() as u32;
        self.write_all(&block_data).await?;

        // Add index entry
        self.index_block
            .add_entry(&first_key, block_offset, block_size);
        self.block_count += 1;

        Ok(())
    }

    /// Write a byte slice at the current append offset.
    async fn write_all(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let buf = AlignedBuf::from_slice(data, DMA_ALIGNMENT);
        let written = self
            .io
            .write_at(&self.file, self.current_offset, buf)
            .await
            .map_err(|e| {
                TiSqlError::Storage(format!(
                    "io_uring write failed for {}: {e}",
                    self.path.display()
                ))
            })?;

        if written != data.len() {
            return Err(TiSqlError::Storage(format!(
                "Short io_uring write for {}: wrote {written} of {} bytes",
                self.path.display(),
                data.len()
            )));
        }

        self.current_offset += written as u64;
        Ok(())
    }

    /// Finish building the SST and return metadata.
    pub async fn finish(mut self, sst_id: u64, level: u32) -> Result<SstMeta> {
        if self.finished {
            return Err(TiSqlError::Storage("Builder already finished".into()));
        }
        self.finished = true;

        // Flush remaining data block
        self.flush_data_block().await?;

        let mut bloom_span: Option<(u64, u32)> = None;

        // Write optional bloom block right before index block.
        if let Some(bloom_builder) = self.bloom_builder.take() {
            if !bloom_builder.is_empty() {
                let bloom_offset = self.current_offset;
                let bloom_data = bloom_builder.finish().encode();
                self.write_all(&bloom_data).await?;
                bloom_span = Some((bloom_offset, bloom_data.len() as u32));
            }
        }

        // Write index block
        let index_offset = self.current_offset;
        let index_data = std::mem::take(&mut self.index_block).finish();
        let index_size = index_data.len() as u32;
        self.write_all(&index_data).await?;

        if let Some((bloom_offset, bloom_size)) = bloom_span {
            let trailer = BloomMetaTrailer {
                bloom_offset,
                bloom_size,
            };
            self.write_all(&trailer.encode()).await?;
        }

        // Build and write footer
        let footer = Footer {
            index_offset,
            index_size,
            num_blocks: self.block_count,
            num_entries: self.entry_count,
            min_ts: if self.min_ts == Timestamp::MAX {
                0
            } else {
                self.min_ts
            },
            max_ts: self.max_ts,
            compression: self.options.compression,
        };

        self.write_all(&footer.encode()).await?;

        // Fsync data via io_uring
        self.io.fsync(&self.file).await.map_err(|e| {
            TiSqlError::Storage(format!(
                "io_uring fsync failed for {}: {e}",
                self.path.display()
            ))
        })?;

        // Sync parent directory to make the file's existence durable
        if let Some(parent) = self.path.parent() {
            sync_dir(parent)?;
        }

        let file_size = self.current_offset;

        Ok(SstMeta {
            id: sst_id,
            level,
            smallest_key: self.first_key.take().unwrap_or_default(),
            largest_key: self.last_key.take().unwrap_or_default(),
            file_size,
            entry_count: self.entry_count,
            block_count: self.block_count,
            min_ts: if self.min_ts == Timestamp::MAX {
                0
            } else {
                self.min_ts
            },
            max_ts: self.max_ts,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        })
    }

    /// Abort building and delete the partial file.
    pub fn abort(mut self) -> Result<()> {
        self.finished = true;
        if self.path.exists() {
            std::fs::remove_file(&self.path)?;
        }
        Ok(())
    }
}

impl Drop for AsyncSstBuilder {
    fn drop(&mut self) {
        // If not finished, try to clean up the partial file
        if !self.finished && self.path.exists() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // Helper to create MVCC key
    fn mvcc_key(key_bytes: &[u8], ts: u64) -> Vec<u8> {
        let mut result = key_bytes.to_vec();
        result.extend_from_slice(&(!ts).to_be_bytes());
        result
    }

    // ------------------------------------------------------------------------
    // Footer Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_footer_encode_decode() {
        let footer = Footer {
            index_offset: 12345,
            index_size: 1024,
            num_blocks: 10,
            num_entries: 1000,
            min_ts: 100,
            max_ts: 200,
            compression: CompressionType::None,
        };

        let encoded = footer.encode();
        assert_eq!(encoded.len(), FOOTER_SIZE);

        let decoded = Footer::decode(&encoded).unwrap();
        assert_eq!(decoded.index_offset, footer.index_offset);
        assert_eq!(decoded.index_size, footer.index_size);
        assert_eq!(decoded.num_blocks, footer.num_blocks);
        assert_eq!(decoded.num_entries, footer.num_entries);
        assert_eq!(decoded.min_ts, footer.min_ts);
        assert_eq!(decoded.max_ts, footer.max_ts);
        assert_eq!(decoded.compression, footer.compression);
    }

    #[test]
    fn test_footer_invalid_magic() {
        let mut data = [0u8; FOOTER_SIZE];
        // Set wrong magic at the end
        data[FOOTER_SIZE - 4..].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());

        let result = Footer::decode(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("magic"));
    }

    #[test]
    fn test_bloom_meta_trailer_encode_decode() {
        let trailer = BloomMetaTrailer {
            bloom_offset: 1234,
            bloom_size: 567,
        };
        let encoded = trailer.encode();
        let decoded = BloomMetaTrailer::decode_optional(&encoded).unwrap();
        assert_eq!(decoded, trailer);
    }

    #[test]
    fn test_bloom_meta_trailer_absent_when_magic_mismatch() {
        let trailer = [0u8; BLOOM_META_TRAILER_SIZE];
        assert!(BloomMetaTrailer::decode_optional(&trailer).is_none());
    }

    #[test]
    fn test_builder_writes_bloom_meta_trailer_when_bloom_enabled() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bloom_trailer.sst");
        let mut builder = SstBuilder::new(
            &path,
            SstBuilderOptions {
                bloom_enabled: true,
                bloom_bits_per_key: 12,
                ..SstBuilderOptions::default()
            },
        )
        .unwrap();
        builder.add(&mvcc_key(b"k1", 10), b"v1").unwrap();
        let meta = builder.finish(1, 0).unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), meta.file_size as usize);

        let footer_start = bytes.len() - FOOTER_SIZE;
        let footer_raw: [u8; FOOTER_SIZE] = bytes[footer_start..].try_into().unwrap();
        let footer = Footer::decode(&footer_raw).unwrap();

        let trailer_start = footer_start - BLOOM_META_TRAILER_SIZE;
        let trailer_raw: [u8; BLOOM_META_TRAILER_SIZE] =
            bytes[trailer_start..footer_start].try_into().unwrap();
        let trailer = BloomMetaTrailer::decode_optional(&trailer_raw).unwrap();

        assert!(trailer.bloom_offset < footer.index_offset);
        assert!(trailer.bloom_size > 0);
        assert_eq!(
            trailer.bloom_offset + trailer.bloom_size as u64,
            footer.index_offset
        );
    }

    #[test]
    fn test_builder_bloom_deduplicates_adjacent_mvcc_versions() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bloom_dedup.sst");
        let mut builder = SstBuilder::new(
            &path,
            SstBuilderOptions {
                bloom_enabled: true,
                bloom_bits_per_key: 12,
                ..SstBuilderOptions::default()
            },
        )
        .unwrap();

        // Same logical key across many MVCC versions (descending ts = sorted MVCC order).
        for ts in (100..=200u64).rev() {
            builder
                .add(&mvcc_key(b"dup-key", ts), format!("v{ts}").as_bytes())
                .unwrap();
        }
        builder.add(&mvcc_key(b"uniq-key", 88), b"u88").unwrap();
        builder.finish(2, 0).unwrap();

        let bytes = std::fs::read(&path).unwrap();
        let footer_start = bytes.len() - FOOTER_SIZE;
        let footer_raw: [u8; FOOTER_SIZE] = bytes[footer_start..].try_into().unwrap();
        let footer = Footer::decode(&footer_raw).unwrap();

        let trailer_start = footer_start - BLOOM_META_TRAILER_SIZE;
        let trailer_raw: [u8; BLOOM_META_TRAILER_SIZE] =
            bytes[trailer_start..footer_start].try_into().unwrap();
        let trailer = BloomMetaTrailer::decode_optional(&trailer_raw).unwrap();

        let mut expected = BloomBuilder::new(12);
        expected.add(b"dup-key");
        expected.add(b"uniq-key");
        let expected_size = expected.finish().encode().len() as u32;
        assert_eq!(trailer.bloom_size, expected_size);

        let bloom_start = trailer.bloom_offset as usize;
        let bloom_end = bloom_start + trailer.bloom_size as usize;
        let bloom =
            crate::tablet::sstable::bloom::BloomFilter::decode(&bytes[bloom_start..bloom_end])
                .unwrap();
        assert!(bloom.may_contain(b"dup-key"));
        assert!(bloom.may_contain(b"uniq-key"));
        assert_eq!(
            trailer.bloom_offset + trailer.bloom_size as u64,
            footer.index_offset
        );
    }

    // ------------------------------------------------------------------------
    // SstMeta Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_sst_meta_may_contain_key() {
        // SSTs now store MVCC keys, so smallest/largest are MVCC-encoded
        let meta = SstMeta {
            id: 1,
            level: 0,
            smallest_key: mvcc_key(b"bbb", 100), // MVCC key for "bbb" at ts=100
            largest_key: mvcc_key(b"ddd", 50),   // MVCC key for "ddd" at ts=50
            file_size: 1000,
            entry_count: 100,
            block_count: 5,
            min_ts: 50,
            max_ts: 100,
            created_at: 0,
        };

        assert!(!meta.may_contain_key(b"aaa")); // before range
        assert!(meta.may_contain_key(b"bbb")); // at start
        assert!(meta.may_contain_key(b"ccc")); // in range
        assert!(meta.may_contain_key(b"ddd")); // at end
        assert!(!meta.may_contain_key(b"eee")); // after range
    }

    #[test]
    fn test_sst_meta_overlaps() {
        // SSTs now store MVCC keys
        let meta = SstMeta {
            id: 1,
            level: 0,
            smallest_key: mvcc_key(b"bbb", 100),
            largest_key: mvcc_key(b"ddd", 50),
            file_size: 1000,
            entry_count: 100,
            block_count: 5,
            min_ts: 50,
            max_ts: 100,
            created_at: 0,
        };

        // Range completely before
        assert!(!meta.overlaps(b"aaa", b"aaz"));

        // Range overlaps start
        assert!(meta.overlaps(b"aaa", b"ccc"));

        // Range completely inside
        assert!(meta.overlaps(b"ccc", b"ccd"));

        // Range overlaps end
        assert!(meta.overlaps(b"ccc", b"eee"));

        // Range completely after
        assert!(!meta.overlaps(b"eee", b"fff"));

        // Range contains SST range
        assert!(meta.overlaps(b"aaa", b"fff"));
    }

    // ------------------------------------------------------------------------
    // SstBuilder Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_builder_empty_sst() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.sst");

        let builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        assert!(builder.is_empty());

        let meta = builder.finish(1, 0).unwrap();

        assert_eq!(meta.id, 1);
        assert_eq!(meta.level, 0);
        assert_eq!(meta.entry_count, 0);
        assert_eq!(meta.block_count, 0);
        assert!(meta.smallest_key.is_empty());
        assert!(meta.largest_key.is_empty());

        // File should exist
        assert!(path.exists());
    }

    #[test]
    fn test_builder_single_entry() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("single.sst");

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();

        let key = mvcc_key(b"test_key", 100);
        builder.add(&key, b"value").unwrap();

        let meta = builder.finish(1, 0).unwrap();

        assert_eq!(meta.entry_count, 1);
        assert_eq!(meta.block_count, 1);
        assert_eq!(meta.smallest_key, key);
        assert_eq!(meta.largest_key, key);
        assert_eq!(meta.min_ts, 100);
        assert_eq!(meta.max_ts, 100);
        assert!(meta.file_size > 0);
    }

    #[test]
    fn test_builder_multiple_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multi.sst");

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();

        // Add entries in sorted order (MVCC: higher ts first)
        let keys = [
            mvcc_key(b"key1", 200),
            mvcc_key(b"key1", 100),
            mvcc_key(b"key2", 150),
            mvcc_key(b"key3", 50),
        ];

        for key in &keys {
            builder.add(key, b"value").unwrap();
        }

        let meta = builder.finish(1, 0).unwrap();

        assert_eq!(meta.entry_count, 4);
        assert_eq!(meta.smallest_key, keys[0]);
        assert_eq!(meta.largest_key, keys[3]);
        assert_eq!(meta.min_ts, 50);
        assert_eq!(meta.max_ts, 200);
    }

    #[test]
    fn test_builder_multiple_blocks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multiblock.sst");

        // Use small block size to force multiple blocks
        let options = SstBuilderOptions::with_block_size(128);
        let mut builder = SstBuilder::new(&path, options).unwrap();

        // Add enough entries to span multiple blocks
        for i in 0..100 {
            let key = format!("key_{i:05}");
            let value = format!("value_{i:05}");
            builder.add(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let meta = builder.finish(1, 0).unwrap();

        assert_eq!(meta.entry_count, 100);
        assert!(meta.block_count > 1);
        assert_eq!(meta.smallest_key, b"key_00000".to_vec());
        assert_eq!(meta.largest_key, b"key_00099".to_vec());
    }

    #[test]
    fn test_builder_abort() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("abort.sst");

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.add(b"key", b"value").unwrap();

        // File should exist before abort
        assert!(path.exists());

        builder.abort().unwrap();

        // File should be deleted after abort
        assert!(!path.exists());
    }

    #[test]
    fn test_builder_drop_cleanup() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("drop.sst");

        {
            let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
            builder.add(b"key", b"value").unwrap();
            // Drop without finish - should clean up
        }

        // File should be deleted after drop
        assert!(!path.exists());
    }

    #[test]
    fn test_builder_finish_then_finish() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("double.sst");

        let builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        let _meta = builder.finish(1, 0).unwrap();

        // Can't call finish twice (builder consumed)
        // This is enforced at compile time by ownership
    }

    #[test]
    fn test_builder_estimated_size() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("size.sst");

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();

        let initial_size = builder.estimated_size();
        assert!(initial_size > 0); // At least footer

        builder.add(b"key1", b"value1").unwrap();
        let size_after_one = builder.estimated_size();
        assert!(size_after_one > initial_size);

        builder.add(b"key2", b"value2").unwrap();
        let size_after_two = builder.estimated_size();
        assert!(size_after_two > size_after_one);

        let meta = builder.finish(1, 0).unwrap();

        // Actual size should be close to estimated
        assert!(meta.file_size > 0);
    }

    #[test]
    fn test_builder_large_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("large.sst");

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();

        // Add entries with large keys and values
        let large_key = vec![b'k'; 1000];
        let large_value = vec![b'v'; 10000];

        builder.add(&large_key, &large_value).unwrap();

        let meta = builder.finish(1, 0).unwrap();

        assert_eq!(meta.entry_count, 1);
        assert!(meta.file_size > 11000); // Key + value + overhead
    }

    #[test]
    fn test_builder_with_explicit_ts() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("explicit_ts.sst");

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();

        // Keys without MVCC suffix
        builder.add_with_ts(b"key1", b"value1", 100).unwrap();
        builder.add_with_ts(b"key2", b"value2", 200).unwrap();
        builder.add_with_ts(b"key3", b"value3", 50).unwrap();

        let meta = builder.finish(1, 0).unwrap();

        assert_eq!(meta.min_ts, 50);
        assert_eq!(meta.max_ts, 200);
    }

    #[test]
    fn test_builder_file_sync() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sync.sst");

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();

        for i in 0..10 {
            let key = format!("key_{i}");
            builder.add(key.as_bytes(), b"value").unwrap();
        }

        let meta = builder.finish(1, 0).unwrap();

        // Verify file exists and has correct size
        let file_meta = std::fs::metadata(&path).unwrap();
        assert_eq!(file_meta.len(), meta.file_size);
    }
}
