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

//! SST file reader for reading SST files.
//!
//! ## Usage
//!
//! ```ignore
//! let reader = SstReader::open(path, io)?;
//! let block = reader.read_block(0)?;   // &self — no Mutex needed
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::io::{DmaFile, IoService};
use crate::tablet::cache::{
    tablet_namespace_from_dir, BlockCacheKey, BlockKind, CachePriority, SharedBlockCache,
};
use crate::util::error::{Result, TiSqlError};
use tokio::sync::Mutex;

use super::block::{DataBlock, IndexBlock, IndexEntry};
use super::bloom::BloomFilter;
use super::builder::{BloomMetaTrailer, Footer, BLOOM_META_TRAILER_SIZE, FOOTER_SIZE};

/// Open/read options for SST readers.
#[derive(Clone, Default)]
pub struct SstReadOptions {
    pub tablet_tag: u128,
    pub sst_id: u64,
    pub bloom_enabled: bool,
    pub fill_cache: bool,
    pub block_cache: Option<Arc<SharedBlockCache>>,
}

impl std::fmt::Debug for SstReadOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SstReadOptions")
            .field("tablet_tag", &self.tablet_tag)
            .field("sst_id", &self.sst_id)
            .field("bloom_enabled", &self.bloom_enabled)
            .field("fill_cache", &self.fill_cache)
            .field("block_cache", &self.block_cache.is_some())
            .finish()
    }
}

#[derive(Debug)]
enum BloomState {
    Uninitialized,
    Ready(Option<BloomFilter>),
}

// ============================================================================
// SstReader
// ============================================================================

/// Reader for SST files.
///
/// The reader loads the footer and index block into memory on open,
/// then reads data blocks on demand using positional reads (`&self`).
///
/// Reads go through io_uring via IoService with O_DIRECT (or automatic
/// fallback on filesystems that don't support it).
#[derive(Debug)]
pub struct SstReader {
    /// File path
    path: PathBuf,
    /// DMA file for io_uring reads
    file: DmaFile,
    /// io_uring I/O service
    io: Arc<IoService>,
    /// File size in bytes
    file_size: u64,
    /// Footer information
    footer: Footer,
    /// Index block (loaded on open)
    index: IndexBlock,
    /// Read/cache options.
    options: SstReadOptions,
    /// Optional bloom block location (offset, size).
    bloom_span: Option<(u64, u32)>,
    /// Lazy bloom decode state.
    bloom_state: Mutex<BloomState>,
}

impl SstReader {
    /// Open an SST file for reading.
    pub async fn open<P: AsRef<Path>>(path: P, io: Arc<IoService>) -> Result<Self> {
        Self::open_with_options(path, io, SstReadOptions::default()).await
    }

    /// Open an SST file for reading with explicit options.
    pub async fn open_with_options<P: AsRef<Path>>(
        path: P,
        io: Arc<IoService>,
        mut options: SstReadOptions,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if options.tablet_tag == 0 {
            options.tablet_tag = path.parent().and_then(|p| p.parent()).map_or_else(
                || tablet_namespace_from_dir(Path::new(".")),
                tablet_namespace_from_dir,
            );
        }
        if options.sst_id == 0 {
            options.sst_id = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
        }

        let file = DmaFile::open_read(&path).map_err(|e| {
            TiSqlError::Storage(format!("Failed to open SST {}: {e}", path.display()))
        })?;

        let file_size = file.file_size();
        if file_size < FOOTER_SIZE as u64 {
            return Err(TiSqlError::Storage(format!(
                "SST file too small: {file_size} bytes"
            )));
        }

        // Read footer via io_uring
        let footer_offset = file_size - FOOTER_SIZE as u64;
        let footer_data = io
            .read_at(&file, footer_offset, FOOTER_SIZE)
            .await
            .map_err(|e| TiSqlError::Storage(format!("Failed to read SST footer: {e}")))?;
        let footer_buf: [u8; FOOTER_SIZE] = footer_data[..FOOTER_SIZE]
            .try_into()
            .map_err(|_| TiSqlError::Storage("Footer read returned wrong size".into()))?;
        let footer = Footer::decode(&footer_buf)?;

        let data_end = file_size - FOOTER_SIZE as u64;
        let mut index_data_end = data_end;
        let mut bloom_span = None;

        if data_end >= BLOOM_META_TRAILER_SIZE as u64 {
            let trailer_offset = data_end - BLOOM_META_TRAILER_SIZE as u64;
            let trailer_buf = io
                .read_at(&file, trailer_offset, BLOOM_META_TRAILER_SIZE)
                .await
                .map_err(|e| {
                    TiSqlError::Storage(format!("Failed to read bloom trailer candidate: {e}"))
                })?;
            let trailer_arr: [u8; BLOOM_META_TRAILER_SIZE] = trailer_buf[..BLOOM_META_TRAILER_SIZE]
                .try_into()
                .map_err(|_| {
                    TiSqlError::Storage("Bloom trailer read returned wrong size".into())
                })?;

            if let Some(meta) = BloomMetaTrailer::decode_optional(&trailer_arr) {
                let bloom_end = meta
                    .bloom_offset
                    .checked_add(meta.bloom_size as u64)
                    .ok_or_else(|| {
                        TiSqlError::Storage("Bloom trailer offset + size overflows".into())
                    })?;
                if bloom_end > footer.index_offset {
                    crate::log_warn!(
                        "Ignoring invalid bloom trailer in {}: bloom_end={} > index_offset={}",
                        path.display(),
                        bloom_end,
                        footer.index_offset
                    );
                } else {
                    index_data_end = trailer_offset;
                    bloom_span = Some((meta.bloom_offset, meta.bloom_size));
                }
            }
        }

        // Validate index block bounds
        Self::validate_index_bounds(&footer, index_data_end)?;

        let index_cache_key = BlockCacheKey {
            ns: options.tablet_tag,
            sst_id: options.sst_id,
            block_offset: footer.index_offset,
            block_kind: BlockKind::Index,
        };
        let index_buf = if options.fill_cache {
            if let Some(cache) = options.block_cache.as_ref() {
                if let Some(hit) = cache.get(&index_cache_key) {
                    hit.as_ref().to_vec()
                } else {
                    let miss = io
                        .read_at(&file, footer.index_offset, footer.index_size as usize)
                        .await
                        .map_err(|e| {
                            TiSqlError::Storage(format!("Failed to read SST index: {e}"))
                        })?;
                    let miss_vec = miss.to_vec();
                    let _ = cache.insert(
                        index_cache_key,
                        Arc::<[u8]>::from(miss_vec.as_slice()),
                        miss_vec.len(),
                        CachePriority::High,
                    );
                    miss_vec
                }
            } else {
                io.read_at(&file, footer.index_offset, footer.index_size as usize)
                    .await
                    .map_err(|e| TiSqlError::Storage(format!("Failed to read SST index: {e}")))?
                    .to_vec()
            }
        } else {
            io.read_at(&file, footer.index_offset, footer.index_size as usize)
                .await
                .map_err(|e| TiSqlError::Storage(format!("Failed to read SST index: {e}")))?
                .to_vec()
        };
        let index = IndexBlock::decode(&index_buf)?;

        Ok(Self {
            path,
            file,
            io,
            file_size,
            footer,
            index,
            options,
            bloom_span,
            bloom_state: Mutex::new(BloomState::Uninitialized),
        })
    }

    /// Validate index block bounds against file data region.
    fn validate_index_bounds(footer: &Footer, data_end: u64) -> Result<()> {
        if footer.index_offset > data_end {
            return Err(TiSqlError::Storage(format!(
                "SST corrupted: index_offset {} exceeds data region end {}",
                footer.index_offset, data_end
            )));
        }
        let index_end = footer
            .index_offset
            .checked_add(footer.index_size as u64)
            .ok_or_else(|| {
                TiSqlError::Storage("SST corrupted: index offset + size overflows".into())
            })?;
        if index_end > data_end {
            return Err(TiSqlError::Storage(format!(
                "SST corrupted: index block end {index_end} exceeds data region end {data_end}"
            )));
        }
        const MAX_INDEX_SIZE: u32 = 256 * 1024 * 1024;
        if footer.index_size > MAX_INDEX_SIZE {
            return Err(TiSqlError::Storage(format!(
                "SST corrupted: index_size {} exceeds maximum {}",
                footer.index_size, MAX_INDEX_SIZE
            )));
        }
        Ok(())
    }

    /// Get the file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the file size.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Get the number of data blocks.
    pub fn num_blocks(&self) -> u32 {
        self.footer.num_blocks
    }

    /// Get the number of entries.
    pub fn num_entries(&self) -> u64 {
        self.footer.num_entries
    }

    /// Get the minimum timestamp.
    pub fn min_ts(&self) -> u64 {
        self.footer.min_ts
    }

    /// Get the maximum timestamp.
    pub fn max_ts(&self) -> u64 {
        self.footer.max_ts
    }

    /// Get the footer.
    pub fn footer(&self) -> &Footer {
        &self.footer
    }

    /// Get the index block.
    pub fn index(&self) -> &IndexBlock {
        &self.index
    }

    /// Read a data block by index.
    ///
    /// This is `&self` — no Mutex needed. Uses positional reads via io_uring.
    pub async fn read_block(&self, block_idx: usize) -> Result<DataBlock> {
        let entry = self.index.get_entry(block_idx).ok_or_else(|| {
            TiSqlError::Storage(format!(
                "Block index out of range: {} >= {}",
                block_idx,
                self.index.num_entries()
            ))
        })?;

        self.read_block_at(entry.block_offset, entry.block_size, BlockKind::Data)
            .await
    }

    async fn read_raw_block(&self, offset: u64, size: u32, kind: BlockKind) -> Result<Vec<u8>> {
        let cache_key = BlockCacheKey {
            ns: self.options.tablet_tag,
            sst_id: self.options.sst_id,
            block_offset: offset,
            block_kind: kind,
        };
        if self.options.fill_cache {
            if let Some(cache) = self.options.block_cache.as_ref() {
                if let Some(hit) = cache.get(&cache_key) {
                    return Ok(hit.as_ref().to_vec());
                }
                let miss = self
                    .io
                    .read_at(&self.file, offset, size as usize)
                    .await
                    .map_err(|e| TiSqlError::Storage(format!("io_uring read_block failed: {e}")))?;
                let miss_vec = miss.to_vec();
                let priority = match kind {
                    BlockKind::Data => CachePriority::Normal,
                    BlockKind::Index | BlockKind::Bloom => CachePriority::High,
                };
                let _ = cache.insert(
                    cache_key,
                    Arc::<[u8]>::from(miss_vec.as_slice()),
                    miss_vec.len(),
                    priority,
                );
                return Ok(miss_vec);
            }
        }

        self.io
            .read_at(&self.file, offset, size as usize)
            .await
            .map(|buf| buf.to_vec())
            .map_err(|e| TiSqlError::Storage(format!("io_uring read_block failed: {e}")))
    }

    /// Read a data block at the given offset and size.
    async fn read_block_at(&self, offset: u64, size: u32, kind: BlockKind) -> Result<DataBlock> {
        let buf = self.read_raw_block(offset, size, kind).await?;
        DataBlock::decode(&buf)
    }

    fn bloom_span(&self) -> Option<(u64, u32)> {
        self.bloom_span
    }

    /// Bloom check for one user key.
    ///
    /// Returns `Ok(true)` when bloom is unavailable/disabled (fail-open).
    pub async fn may_contain_user_key(&self, user_key: &[u8]) -> Result<bool> {
        if !self.options.bloom_enabled {
            return Ok(true);
        }

        let mut state = self.bloom_state.lock().await;
        match &mut *state {
            BloomState::Ready(Some(filter)) => return Ok(filter.may_contain(user_key)),
            BloomState::Ready(None) => return Ok(true),
            BloomState::Uninitialized => {}
        }

        let maybe_filter = match self.bloom_span() {
            Some((offset, size)) => {
                let raw = match self.read_raw_block(offset, size, BlockKind::Bloom).await {
                    Ok(raw) => raw,
                    Err(e) => {
                        crate::log_warn!(
                            "Failed to load bloom block from {} (id={}): {}",
                            self.path.display(),
                            self.options.sst_id,
                            e
                        );
                        Vec::new()
                    }
                };
                if raw.is_empty() {
                    None
                } else {
                    match BloomFilter::decode(&raw) {
                        Ok(filter) => Some(filter),
                        Err(e) => {
                            crate::log_warn!(
                                "Failed to decode bloom block from {} (id={}): {}. Falling back to full lookup.",
                                self.path.display(),
                                self.options.sst_id,
                                e
                            );
                            None
                        }
                    }
                }
            }
            None => None,
        };

        *state = BloomState::Ready(maybe_filter);
        match &*state {
            BloomState::Ready(Some(filter)) => Ok(filter.may_contain(user_key)),
            BloomState::Ready(None) => Ok(true),
            BloomState::Uninitialized => Ok(true),
        }
    }

    /// Find the block that may contain the given key using binary search.
    ///
    /// Returns the index of the block where the key would be located.
    /// If the key is smaller than all blocks, returns 0.
    /// If the key is larger than all blocks, returns the last block index.
    pub fn find_block(&self, key: &[u8]) -> usize {
        // search() returns the block with largest first_key <= key
        // If None (key is smaller than all first_keys), return 0
        self.index.search(key).unwrap_or(0)
    }

    /// Point lookup for a key.
    ///
    /// Returns the value if found, or None if not found.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.footer.num_blocks == 0 {
            return Ok(None);
        }

        // Find candidate block
        let block_idx = self.find_block(key);
        let block = self.read_block(block_idx).await?;

        // Search within block
        Ok(block.get(key).map(|v| v.to_vec()))
    }

    /// Get all index entries.
    pub fn index_entries(&self) -> Vec<IndexEntry> {
        self.index.entries().to_vec()
    }
}

// ============================================================================
// SstReaderRef - Shareable reader using Arc (no Mutex!)
// ============================================================================

/// A shareable SST reader wrapped in Arc.
///
/// Since `SstReader::read_block()` is `&self` (positional reads via io_uring,
/// no seek state), `SstReaderRef` doesn't need a Mutex. Multiple threads can
/// read concurrently.
#[derive(Clone)]
pub struct SstReaderRef {
    inner: Arc<SstReader>,
}

impl SstReaderRef {
    /// Create a new shared reader.
    pub fn new(reader: SstReader) -> Self {
        Self {
            inner: Arc::new(reader),
        }
    }

    /// Open an SST file and create a shared reader.
    pub async fn open<P: AsRef<Path>>(path: P, io: Arc<IoService>) -> Result<Self> {
        let reader = SstReader::open(path, io).await?;
        Ok(Self::new(reader))
    }

    /// Open an SST file and create a shared reader with explicit read options.
    pub async fn open_with_options<P: AsRef<Path>>(
        path: P,
        io: Arc<IoService>,
        options: SstReadOptions,
    ) -> Result<Self> {
        let reader = SstReader::open_with_options(path, io, options).await?;
        Ok(Self::new(reader))
    }

    /// Read a data block by index.
    pub async fn read_block(&self, block_idx: usize) -> Result<DataBlock> {
        self.inner.read_block(block_idx).await
    }

    /// Point lookup for a key.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }

    /// Bloom check for one user key.
    pub async fn may_contain_user_key(&self, user_key: &[u8]) -> Result<bool> {
        self.inner.may_contain_user_key(user_key).await
    }

    /// Get the number of blocks.
    pub fn num_blocks(&self) -> Result<u32> {
        Ok(self.inner.num_blocks())
    }

    /// Get the number of entries.
    pub fn num_entries(&self) -> Result<u64> {
        Ok(self.inner.num_entries())
    }

    /// Find the block that may contain the given key.
    pub fn find_block(&self, key: &[u8]) -> Result<usize> {
        Ok(self.inner.find_block(key))
    }

    /// Get the footer.
    pub fn footer(&self) -> Result<Footer> {
        Ok(self.inner.footer().clone())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tablet::sstable::builder::{SstBuilder, SstBuilderOptions};
    use tempfile::tempdir;

    // Helper to create MVCC key
    fn mvcc_key(key_bytes: &[u8], ts: u64) -> Vec<u8> {
        let mut result = key_bytes.to_vec();
        result.extend_from_slice(&(!ts).to_be_bytes());
        result
    }

    // Helper to create IoService for tests
    fn test_io() -> Arc<IoService> {
        IoService::new_for_test(32).unwrap()
    }

    // Helper to create an SST file with test data
    fn create_test_sst(path: &Path, entries: &[(&[u8], &[u8])]) -> Result<()> {
        let mut builder = SstBuilder::new(path, SstBuilderOptions::default())?;
        for (key, value) in entries {
            builder.add(key, value)?;
        }
        builder.finish(1, 0)?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Basic Reader Tests
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn test_reader_open_empty_sst() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.sst");
        let io = test_io();

        // Create empty SST
        let builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.finish(1, 0).unwrap();

        // Open and verify
        let reader = SstReader::open(&path, io).await.unwrap();
        assert_eq!(reader.num_blocks(), 0);
        assert_eq!(reader.num_entries(), 0);
    }

    #[tokio::test]
    async fn test_reader_open_single_entry() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("single.sst");
        let io = test_io();

        let key = mvcc_key(b"key", 100);
        create_test_sst(&path, &[(&key, b"value")]).unwrap();

        let reader = SstReader::open(&path, io).await.unwrap();
        assert_eq!(reader.num_entries(), 1);
        assert_eq!(reader.num_blocks(), 1);
    }

    #[tokio::test]
    async fn test_reader_open_multiple_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multi.sst");
        let io = test_io();

        let keys: Vec<_> = (0..100)
            .map(|i| format!("key_{i:05}").into_bytes())
            .collect();
        let entries: Vec<_> = keys
            .iter()
            .map(|k| (k.as_slice(), b"value".as_slice()))
            .collect();

        create_test_sst(&path, &entries).unwrap();

        let reader = SstReader::open(&path, io).await.unwrap();
        assert_eq!(reader.num_entries(), 100);
    }

    #[tokio::test]
    async fn test_reader_open_invalid_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("invalid.sst");
        let io = test_io();

        // Create a file with invalid content
        std::fs::write(&path, b"invalid data").unwrap();

        let result = SstReader::open(&path, io).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_reader_open_too_small() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("small.sst");
        let io = test_io();

        // Create a file smaller than footer size
        std::fs::write(&path, b"x").unwrap();

        let result = SstReader::open(&path, io).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too small"));
    }

    // ------------------------------------------------------------------------
    // Block Reading Tests
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn test_reader_read_block() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("block.sst");
        let io = test_io();

        let entries: Vec<_> = (0..10)
            .map(|i| {
                let key = format!("key_{i:05}").into_bytes();
                let value = format!("value_{i:05}").into_bytes();
                (key, value)
            })
            .collect();
        let entry_refs: Vec<_> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        create_test_sst(&path, &entry_refs).unwrap();

        let reader = SstReader::open(&path, io).await.unwrap();
        let block = reader.read_block(0).await.unwrap();

        // Block should contain the entries
        assert!(block.num_entries() > 0);
    }

    #[tokio::test]
    async fn test_reader_read_block_out_of_range() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("range.sst");
        let io = test_io();

        create_test_sst(&path, &[(b"key", b"value")]).unwrap();

        let reader = SstReader::open(&path, io).await.unwrap();
        let result = reader.read_block(100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_reader_multiple_blocks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multiblock.sst");
        let io = test_io();

        // Use small block size to force multiple blocks
        let options = SstBuilderOptions::with_block_size(128);
        let mut builder = SstBuilder::new(&path, options).unwrap();

        for i in 0..100 {
            let key = format!("key_{i:05}");
            let value = format!("value_{i:05}");
            builder.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        builder.finish(1, 0).unwrap();

        let reader = SstReader::open(&path, io).await.unwrap();
        assert!(reader.num_blocks() > 1);

        // Read all blocks
        for i in 0..reader.num_blocks() as usize {
            let block = reader.read_block(i).await.unwrap();
            assert!(block.num_entries() > 0);
        }
    }

    // ------------------------------------------------------------------------
    // Point Lookup Tests
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn test_reader_get_found() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("get.sst");
        let io = test_io();

        let entries: Vec<_> = (0..10)
            .map(|i| {
                let key = format!("key_{i:05}").into_bytes();
                let value = format!("value_{i:05}").into_bytes();
                (key, value)
            })
            .collect();
        let entry_refs: Vec<_> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        create_test_sst(&path, &entry_refs).unwrap();

        let reader = SstReader::open(&path, io).await.unwrap();

        // Find existing keys
        for i in 0..10 {
            let key = format!("key_{i:05}").into_bytes();
            let expected = format!("value_{i:05}").into_bytes();
            let result = reader.get(&key).await.unwrap();
            assert_eq!(result, Some(expected));
        }
    }

    #[tokio::test]
    async fn test_reader_get_not_found() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("notfound.sst");
        let io = test_io();

        create_test_sst(&path, &[(b"key_a", b"value_a"), (b"key_c", b"value_c")]).unwrap();

        let reader = SstReader::open(&path, io).await.unwrap();

        // Key before all entries
        assert_eq!(reader.get(b"key_0").await.unwrap(), None);

        // Key between entries
        assert_eq!(reader.get(b"key_b").await.unwrap(), None);

        // Key after all entries
        assert_eq!(reader.get(b"key_z").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_reader_get_empty_sst() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty_get.sst");
        let io = test_io();

        let builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReader::open(&path, io).await.unwrap();
        assert_eq!(reader.get(b"any_key").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_reader_get_multiblock() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("get_multiblock.sst");
        let io = test_io();

        // Use small block size
        let options = SstBuilderOptions::with_block_size(128);
        let mut builder = SstBuilder::new(&path, options).unwrap();

        for i in 0..100 {
            let key = format!("key_{i:05}");
            let value = format!("value_{i:05}");
            builder.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        builder.finish(1, 0).unwrap();

        let reader = SstReader::open(&path, io).await.unwrap();

        // Find keys across different blocks
        for i in [0, 25, 50, 75, 99] {
            let key = format!("key_{i:05}").into_bytes();
            let expected = format!("value_{i:05}").into_bytes();
            let result = reader.get(&key).await.unwrap();
            assert_eq!(result, Some(expected), "Failed for key {i}");
        }
    }

    // ------------------------------------------------------------------------
    // Find Block Tests
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn test_reader_find_block() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("find.sst");
        let io = test_io();

        // Use small block size
        let options = SstBuilderOptions::with_block_size(64);
        let mut builder = SstBuilder::new(&path, options).unwrap();

        // Add entries that will span multiple blocks
        for i in 0..50 {
            let key = format!("key_{i:05}");
            builder.add(key.as_bytes(), b"v").unwrap();
        }
        builder.finish(1, 0).unwrap();

        let reader = SstReader::open(&path, io).await.unwrap();

        // First key should be in first block
        let idx = reader.find_block(b"key_00000");
        assert_eq!(idx, 0);

        // Key after last should return last block
        let idx = reader.find_block(b"zzz");
        assert!(idx < reader.num_blocks() as usize);
    }

    // ------------------------------------------------------------------------
    // SstReaderRef Tests
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn test_reader_ref_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ref.sst");
        let io = test_io();

        create_test_sst(&path, &[(b"key", b"value")]).unwrap();

        let reader = SstReaderRef::open(&path, io).await.unwrap();
        assert_eq!(reader.num_entries().unwrap(), 1);
        assert_eq!(reader.get(b"key").await.unwrap(), Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_reader_ref_clone() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("clone.sst");
        let io = test_io();

        create_test_sst(&path, &[(b"key", b"value")]).unwrap();

        let reader1 = SstReaderRef::open(&path, Arc::clone(&io)).await.unwrap();
        let reader2 = reader1.clone();

        // Both should work
        assert_eq!(reader1.get(b"key").await.unwrap(), Some(b"value".to_vec()));
        assert_eq!(reader2.get(b"key").await.unwrap(), Some(b"value".to_vec()));
    }

    // ------------------------------------------------------------------------
    // Bloom + Block Cache Tests
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn test_reader_bloom_filter_enabled_and_optional() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bloom_on.sst");
        let io = test_io();

        let mut builder = SstBuilder::new(
            &path,
            SstBuilderOptions {
                bloom_enabled: true,
                bloom_bits_per_key: 20,
                ..SstBuilderOptions::default()
            },
        )
        .unwrap();
        builder.add(&mvcc_key(b"k1", 200), b"v1").unwrap();
        builder.add(&mvcc_key(b"k2", 100), b"v2").unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReader::open_with_options(
            &path,
            Arc::clone(&io),
            SstReadOptions {
                bloom_enabled: true,
                ..SstReadOptions::default()
            },
        )
        .await
        .unwrap();
        assert!(reader.may_contain_user_key(b"k1").await.unwrap());

        let mut negatives = 0;
        for probe in 0..128 {
            let key = format!("not-present-{probe:03}");
            if !reader.may_contain_user_key(key.as_bytes()).await.unwrap() {
                negatives += 1;
            }
        }
        assert!(
            negatives > 0,
            "expected bloom to prune at least one absent key"
        );

        let path_no_bloom = dir.path().join("bloom_off.sst");
        let mut no_bloom_builder =
            SstBuilder::new(&path_no_bloom, SstBuilderOptions::default()).unwrap();
        no_bloom_builder.add(&mvcc_key(b"k1", 200), b"v1").unwrap();
        no_bloom_builder.finish(2, 0).unwrap();

        let no_bloom_reader = SstReader::open_with_options(
            &path_no_bloom,
            io,
            SstReadOptions {
                bloom_enabled: true,
                ..SstReadOptions::default()
            },
        )
        .await
        .unwrap();
        // No bloom block should fail-open.
        assert!(no_bloom_reader
            .may_contain_user_key(b"definitely-missing")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_reader_bloom_decode_failure_is_fail_open() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bloom_corrupt.sst");
        let io = test_io();

        let mut builder = SstBuilder::new(
            &path,
            SstBuilderOptions {
                bloom_enabled: true,
                bloom_bits_per_key: 16,
                ..SstBuilderOptions::default()
            },
        )
        .unwrap();
        let mvcc = mvcc_key(b"k1", 100);
        builder.add(&mvcc, b"v1").unwrap();
        builder.finish(3, 0).unwrap();

        let reader = SstReader::open(&path, Arc::clone(&io)).await.unwrap();
        let data_end = reader
            .index_entries()
            .iter()
            .map(|e| e.block_offset + e.block_size as u64)
            .max()
            .unwrap_or(0);
        let bloom_size = reader.footer().index_offset.saturating_sub(data_end);
        assert!(bloom_size > 0, "expected bloom block to exist");
        drop(reader);

        let mut bytes = std::fs::read(&path).unwrap();
        bytes[data_end as usize] ^= 0x5A; // Corrupt bloom payload checksum.
        std::fs::write(&path, bytes).unwrap();

        let corrupted_reader = SstReader::open_with_options(
            &path,
            Arc::clone(&io),
            SstReadOptions {
                bloom_enabled: true,
                ..SstReadOptions::default()
            },
        )
        .await
        .unwrap();

        // Decode failure must fail-open for correctness.
        assert!(corrupted_reader
            .may_contain_user_key(b"missing-key")
            .await
            .unwrap());
        assert_eq!(
            corrupted_reader.get(&mvcc).await.unwrap(),
            Some(b"v1".to_vec())
        );
    }

    #[tokio::test]
    async fn test_reader_block_cache_hits_on_repeated_reads() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("cache_hits.sst");
        let io = test_io();

        let mut builder = SstBuilder::new(
            &path,
            SstBuilderOptions {
                block_size: 128,
                ..SstBuilderOptions::default()
            },
        )
        .unwrap();
        for i in 0..16 {
            builder
                .add(&mvcc_key(format!("k{i:03}").as_bytes(), 100), b"value")
                .unwrap();
        }
        builder.finish(4, 0).unwrap();

        let cache = Arc::new(SharedBlockCache::new(1 << 20));
        let reader = SstReader::open_with_options(
            &path,
            io,
            SstReadOptions {
                tablet_tag: 111,
                sst_id: 4,
                fill_cache: true,
                block_cache: Some(Arc::clone(&cache)),
                ..SstReadOptions::default()
            },
        )
        .await
        .unwrap();

        reader.read_block(0).await.unwrap();
        let after_first = cache.stats();
        reader.read_block(0).await.unwrap();
        let after_second = cache.stats();

        assert!(after_first.misses >= 1);
        assert!(after_second.hits > after_first.hits);
    }

    #[tokio::test]
    async fn test_reader_fill_cache_false_does_not_populate_cache() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("cache_fill_false.sst");
        let io = test_io();

        let mut builder = SstBuilder::new(
            &path,
            SstBuilderOptions {
                block_size: 128,
                ..SstBuilderOptions::default()
            },
        )
        .unwrap();
        for i in 0..16 {
            builder
                .add(&mvcc_key(format!("k{i:03}").as_bytes(), 100), b"value")
                .unwrap();
        }
        builder.finish(5, 0).unwrap();

        let cache = Arc::new(SharedBlockCache::new(1 << 20));
        let reader = SstReader::open_with_options(
            &path,
            io,
            SstReadOptions {
                tablet_tag: 222,
                sst_id: 5,
                fill_cache: false,
                block_cache: Some(Arc::clone(&cache)),
                ..SstReadOptions::default()
            },
        )
        .await
        .unwrap();

        reader.read_block(0).await.unwrap();
        reader.read_block(0).await.unwrap();

        let stats = cache.stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.inserts, 0);
    }

    #[tokio::test]
    async fn test_reader_open_caches_index_block() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("cache_index_open.sst");
        let io = test_io();

        let mut builder = SstBuilder::new(
            &path,
            SstBuilderOptions {
                block_size: 128,
                ..SstBuilderOptions::default()
            },
        )
        .unwrap();
        for i in 0..16 {
            builder
                .add(&mvcc_key(format!("k{i:03}").as_bytes(), 100), b"value")
                .unwrap();
        }
        builder.finish(6, 0).unwrap();

        let cache = Arc::new(SharedBlockCache::new(1 << 20));
        let options = SstReadOptions {
            tablet_tag: 333,
            sst_id: 6,
            fill_cache: true,
            block_cache: Some(Arc::clone(&cache)),
            ..SstReadOptions::default()
        };

        let _reader1 = SstReader::open_with_options(&path, Arc::clone(&io), options.clone())
            .await
            .unwrap();
        let after_open1 = cache.stats();
        assert!(after_open1.inserts >= 1);

        let _reader2 = SstReader::open_with_options(&path, io, options)
            .await
            .unwrap();
        let after_open2 = cache.stats();
        assert!(after_open2.hits > after_open1.hits);
    }

    // ------------------------------------------------------------------------
    // Timestamp Tests
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn test_reader_timestamp_tracking() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ts.sst");
        let io = test_io();

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();

        // Add keys with MVCC timestamps
        builder.add(&mvcc_key(b"k1", 100), b"v1").unwrap();
        builder.add(&mvcc_key(b"k2", 200), b"v2").unwrap();
        builder.add(&mvcc_key(b"k3", 50), b"v3").unwrap();

        builder.finish(1, 0).unwrap();

        let reader = SstReader::open(&path, io).await.unwrap();
        assert_eq!(reader.min_ts(), 50);
        assert_eq!(reader.max_ts(), 200);
    }

    // ------------------------------------------------------------------------
    // Integration Tests
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn test_reader_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("roundtrip.sst");
        let io = test_io();

        // Create entries with various sizes
        let entries: Vec<_> = (0..50)
            .map(|i| {
                let key = format!("key_{i:05}").into_bytes();
                let value = vec![b'v'; (i + 1) * 10]; // Variable size values
                (key, value)
            })
            .collect();

        // Build SST
        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        for (key, value) in &entries {
            builder.add(key, value).unwrap();
        }
        builder.finish(1, 0).unwrap();

        // Read and verify
        let reader = SstReader::open(&path, io).await.unwrap();
        assert_eq!(reader.num_entries(), 50);

        for (key, expected_value) in &entries {
            let result = reader.get(key).await.unwrap();
            assert_eq!(result.as_ref(), Some(expected_value));
        }
    }

    #[tokio::test]
    async fn test_reader_large_values() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("large.sst");
        let io = test_io();

        let key = b"large_key".to_vec();
        let value = vec![b'x'; 100_000]; // 100KB value

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.add(&key, &value).unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReader::open(&path, io).await.unwrap();
        let result = reader.get(&key).await.unwrap();
        assert_eq!(result, Some(value));
    }

    // ------------------------------------------------------------------------
    // Corruption Detection Tests
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn test_reader_corrupted_index_offset_too_large() {
        use crate::tablet::sstable::builder::{FOOTER_SIZE, SST_MAGIC};

        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt_offset.sst");
        let io = test_io();

        // Create a valid SST
        let key = mvcc_key(b"key", 100);
        create_test_sst(&path, &[(&key, b"value")]).unwrap();

        // Read file and corrupt the footer
        let mut data = std::fs::read(&path).unwrap();
        let file_size = data.len();

        // Create a corrupted footer with index_offset > data region
        let corrupt_offset = (file_size * 2) as u64; // Way past end of file
        data[file_size - FOOTER_SIZE..file_size - FOOTER_SIZE + 8]
            .copy_from_slice(&corrupt_offset.to_le_bytes());

        // Keep magic valid so we pass footer decode
        data[file_size - 4..].copy_from_slice(&SST_MAGIC.to_le_bytes());

        std::fs::write(&path, &data).unwrap();

        let result = SstReader::open(&path, io).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("index_offset") || err.contains("corrupted"));
    }

    #[tokio::test]
    async fn test_reader_corrupted_index_size_overflow() {
        use crate::tablet::sstable::builder::{FOOTER_SIZE, SST_MAGIC};

        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt_size_overflow.sst");
        let io = test_io();

        // Create a valid SST
        let key = mvcc_key(b"key", 100);
        create_test_sst(&path, &[(&key, b"value")]).unwrap();

        // Read file and corrupt the footer
        let mut data = std::fs::read(&path).unwrap();
        let file_size = data.len();

        // Set index_offset to near end and index_size to u32::MAX to cause overflow
        let data_end = (file_size - FOOTER_SIZE) as u64;
        let corrupt_offset = data_end - 1; // Valid offset
        let corrupt_size = u32::MAX; // Will overflow when added

        data[file_size - FOOTER_SIZE..file_size - FOOTER_SIZE + 8]
            .copy_from_slice(&corrupt_offset.to_le_bytes());
        data[file_size - FOOTER_SIZE + 8..file_size - FOOTER_SIZE + 12]
            .copy_from_slice(&corrupt_size.to_le_bytes());

        // Keep magic valid
        data[file_size - 4..].copy_from_slice(&SST_MAGIC.to_le_bytes());

        std::fs::write(&path, &data).unwrap();

        let result = SstReader::open(&path, io).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("overflow") || err.contains("corrupted"));
    }

    #[tokio::test]
    async fn test_reader_corrupted_index_end_exceeds_data() {
        use crate::tablet::sstable::builder::{FOOTER_SIZE, SST_MAGIC};

        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt_index_end.sst");
        let io = test_io();

        // Create a valid SST
        let key = mvcc_key(b"key", 100);
        create_test_sst(&path, &[(&key, b"value")]).unwrap();

        // Read file and corrupt the footer
        let mut data = std::fs::read(&path).unwrap();
        let file_size = data.len();

        // Set index_offset to 0 and index_size larger than data region
        let corrupt_offset: u64 = 0;
        let corrupt_size: u32 = file_size as u32; // Larger than data region

        data[file_size - FOOTER_SIZE..file_size - FOOTER_SIZE + 8]
            .copy_from_slice(&corrupt_offset.to_le_bytes());
        data[file_size - FOOTER_SIZE + 8..file_size - FOOTER_SIZE + 12]
            .copy_from_slice(&corrupt_size.to_le_bytes());

        // Keep magic valid
        data[file_size - 4..].copy_from_slice(&SST_MAGIC.to_le_bytes());

        std::fs::write(&path, &data).unwrap();

        let result = SstReader::open(&path, io).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("exceeds") || err.contains("corrupted"));
    }

    #[tokio::test]
    async fn test_reader_corrupted_index_size_exceeds_max() {
        use crate::tablet::sstable::builder::{FOOTER_SIZE, SST_MAGIC};

        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt_max_size.sst");
        let io = test_io();

        // Create a file with fake large data to make index_size validation trigger
        // We need a file large enough that the index_size check fires before bounds check
        let data_len = 300 * 1024 * 1024 + FOOTER_SIZE; // 300MB + footer
        let mut data = vec![0u8; data_len];

        // Create footer with huge index_size (> 256MB max)
        let corrupt_offset: u64 = 0;
        let corrupt_size: u32 = 300 * 1024 * 1024; // 300MB > 256MB max

        let footer_start = data_len - FOOTER_SIZE;
        data[footer_start..footer_start + 8].copy_from_slice(&corrupt_offset.to_le_bytes());
        data[footer_start + 8..footer_start + 12].copy_from_slice(&corrupt_size.to_le_bytes());

        // Set magic
        let magic_start = data_len - 4;
        data[magic_start..].copy_from_slice(&SST_MAGIC.to_le_bytes());

        std::fs::write(&path, &data).unwrap();

        let result = SstReader::open(&path, io).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("exceeds maximum") || err.contains("index_size"));

        // Cleanup large file
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn test_reader_file_size_getter() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("size_test.sst");
        let io = test_io();

        let key = mvcc_key(b"key", 100);
        create_test_sst(&path, &[(&key, b"value")]).unwrap();

        let reader = SstReader::open(&path, io).await.unwrap();
        assert!(reader.file_size() > 0);
        assert!(reader.file_size() >= FOOTER_SIZE as u64);
    }

    #[tokio::test]
    async fn test_reader_path_getter() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("path_test.sst");
        let io = test_io();

        let key = mvcc_key(b"key", 100);
        create_test_sst(&path, &[(&key, b"value")]).unwrap();

        let reader = SstReader::open(&path, io).await.unwrap();
        assert_eq!(reader.path(), path);
    }

    #[tokio::test]
    async fn test_reader_footer_and_index_getters() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("getters.sst");
        let io = test_io();

        let key = mvcc_key(b"key", 100);
        create_test_sst(&path, &[(&key, b"value")]).unwrap();

        let reader = SstReader::open(&path, io).await.unwrap();

        // Test footer getter
        let footer = reader.footer();
        assert_eq!(footer.num_entries, 1);

        // Test index getter
        let index = reader.index();
        assert!(!index.entries().is_empty() || reader.num_blocks() == 0);
    }

    #[tokio::test]
    async fn test_reader_min_max_ts() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ts_test.sst");
        let io = test_io();

        // Create SST with specific timestamps
        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.add(&mvcc_key(b"key1", 100), b"v1").unwrap();
        builder.add(&mvcc_key(b"key2", 200), b"v2").unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReader::open(&path, io).await.unwrap();
        // min_ts and max_ts depend on the MVCC key encoding
        assert!(reader.min_ts() <= reader.max_ts());
    }
}
