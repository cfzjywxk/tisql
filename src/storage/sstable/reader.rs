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

use crate::error::{Result, TiSqlError};
use crate::io::{DmaFile, IoService};

use super::block::{DataBlock, IndexBlock, IndexEntry};
use super::builder::{Footer, FOOTER_SIZE};

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
}

impl SstReader {
    /// Open an SST file for reading.
    pub async fn open<P: AsRef<Path>>(path: P, io: Arc<IoService>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
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

        // Validate index block bounds
        let data_end = file_size - FOOTER_SIZE as u64;
        Self::validate_index_bounds(&footer, data_end)?;

        // Read index block via io_uring
        let index_buf = io
            .read_at(&file, footer.index_offset, footer.index_size as usize)
            .await
            .map_err(|e| TiSqlError::Storage(format!("Failed to read SST index: {e}")))?;
        let index = IndexBlock::decode(&index_buf)?;

        Ok(Self {
            path,
            file,
            io,
            file_size,
            footer,
            index,
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

        self.read_block_at(entry.block_offset, entry.block_size)
            .await
    }

    /// Read a data block at the given offset and size.
    async fn read_block_at(&self, offset: u64, size: u32) -> Result<DataBlock> {
        let buf = self
            .io
            .read_at(&self.file, offset, size as usize)
            .await
            .map_err(|e| TiSqlError::Storage(format!("io_uring read_block failed: {e}")))?;
        DataBlock::decode(&buf)
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

    /// Read a data block by index.
    pub async fn read_block(&self, block_idx: usize) -> Result<DataBlock> {
        self.inner.read_block(block_idx).await
    }

    /// Point lookup for a key.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key).await
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
    use crate::storage::sstable::builder::{SstBuilder, SstBuilderOptions};
    use tempfile::tempdir;

    // Helper to create MVCC key
    fn mvcc_key(key_bytes: &[u8], ts: u64) -> Vec<u8> {
        let mut result = key_bytes.to_vec();
        result.extend_from_slice(&(!ts).to_be_bytes());
        result
    }

    // Helper to create IoService for tests
    fn test_io() -> Arc<IoService> {
        IoService::new(32).unwrap()
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
        use crate::storage::sstable::builder::{FOOTER_SIZE, SST_MAGIC};

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
        use crate::storage::sstable::builder::{FOOTER_SIZE, SST_MAGIC};

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
        use crate::storage::sstable::builder::{FOOTER_SIZE, SST_MAGIC};

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
        use crate::storage::sstable::builder::{FOOTER_SIZE, SST_MAGIC};

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
