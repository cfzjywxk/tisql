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
//! // Open an SST file
//! let reader = SstReader::open(path)?;
//!
//! // Read a data block
//! let block = reader.read_block(0)?;
//!
//! // Point lookup
//! if let Some(value) = reader.get(key)? {
//!     // Found
//! }
//!
//! // Iterate all entries
//! let mut iter = reader.iter()?;
//! while let Some((key, value)) = iter.next() {
//!     // Process entry
//! }
//! ```

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{Result, TiSqlError};

use super::block::{DataBlock, IndexBlock, IndexEntry};
use super::builder::{Footer, FOOTER_SIZE};

// ============================================================================
// SstReader
// ============================================================================

/// Reader for SST files.
///
/// The reader loads the footer and index block into memory on open,
/// then reads data blocks on demand.
#[derive(Debug)]
pub struct SstReader {
    /// File path
    path: PathBuf,
    /// File handle
    file: File,
    /// File size in bytes
    file_size: u64,
    /// Footer information
    footer: Footer,
    /// Index block (loaded on open)
    index: IndexBlock,
}

impl SstReader {
    /// Open an SST file for reading.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;

        // Get file size
        let file_size = file.metadata()?.len();
        if file_size < FOOTER_SIZE as u64 {
            return Err(TiSqlError::Storage(format!(
                "SST file too small: {file_size} bytes"
            )));
        }

        // Read footer
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut footer_buf = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_buf)?;
        let footer = Footer::decode(&footer_buf)?;

        // Read index block
        file.seek(SeekFrom::Start(footer.index_offset))?;
        let mut index_buf = vec![0u8; footer.index_size as usize];
        file.read_exact(&mut index_buf)?;
        let index = IndexBlock::decode(&index_buf)?;

        Ok(Self {
            path,
            file,
            file_size,
            footer,
            index,
        })
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
    pub fn read_block(&mut self, block_idx: usize) -> Result<DataBlock> {
        let entry = self.index.get_entry(block_idx).ok_or_else(|| {
            TiSqlError::Storage(format!(
                "Block index out of range: {} >= {}",
                block_idx,
                self.index.num_entries()
            ))
        })?;

        self.read_block_at(entry.block_offset, entry.block_size)
    }

    /// Read a data block at the given offset and size.
    fn read_block_at(&mut self, offset: u64, size: u32) -> Result<DataBlock> {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; size as usize];
        self.file.read_exact(&mut buf)?;
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
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.footer.num_blocks == 0 {
            return Ok(None);
        }

        // Find candidate block
        let block_idx = self.find_block(key);
        let block = self.read_block(block_idx)?;

        // Search within block
        Ok(block.get(key).map(|v| v.to_vec()))
    }

    /// Get all index entries.
    pub fn index_entries(&self) -> Vec<IndexEntry> {
        self.index.entries().to_vec()
    }
}

// ============================================================================
// SstReaderRef - Shareable reader using Arc
// ============================================================================

/// A shareable SST reader wrapped in Arc.
///
/// This allows the same SST file to be shared across multiple iterators
/// or threads (with interior mutability for file access).
#[derive(Clone)]
pub struct SstReaderRef {
    inner: Arc<std::sync::Mutex<SstReader>>,
}

impl SstReaderRef {
    /// Create a new shared reader.
    pub fn new(reader: SstReader) -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(reader)),
        }
    }

    /// Open an SST file and create a shared reader.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let reader = SstReader::open(path)?;
        Ok(Self::new(reader))
    }

    /// Read a data block by index.
    pub fn read_block(&self, block_idx: usize) -> Result<DataBlock> {
        self.inner
            .lock()
            .map_err(|e| TiSqlError::Storage(format!("Lock poisoned: {e}")))?
            .read_block(block_idx)
    }

    /// Point lookup for a key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner
            .lock()
            .map_err(|e| TiSqlError::Storage(format!("Lock poisoned: {e}")))?
            .get(key)
    }

    /// Get the number of blocks.
    pub fn num_blocks(&self) -> Result<u32> {
        Ok(self
            .inner
            .lock()
            .map_err(|e| TiSqlError::Storage(format!("Lock poisoned: {e}")))?
            .num_blocks())
    }

    /// Get the number of entries.
    pub fn num_entries(&self) -> Result<u64> {
        Ok(self
            .inner
            .lock()
            .map_err(|e| TiSqlError::Storage(format!("Lock poisoned: {e}")))?
            .num_entries())
    }

    /// Find the block that may contain the given key.
    pub fn find_block(&self, key: &[u8]) -> Result<usize> {
        Ok(self
            .inner
            .lock()
            .map_err(|e| TiSqlError::Storage(format!("Lock poisoned: {e}")))?
            .find_block(key))
    }

    /// Get the footer.
    pub fn footer(&self) -> Result<Footer> {
        Ok(self
            .inner
            .lock()
            .map_err(|e| TiSqlError::Storage(format!("Lock poisoned: {e}")))?
            .footer()
            .clone())
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

    #[test]
    fn test_reader_open_empty_sst() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.sst");

        // Create empty SST
        let builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.finish(1, 0).unwrap();

        // Open and verify
        let reader = SstReader::open(&path).unwrap();
        assert_eq!(reader.num_blocks(), 0);
        assert_eq!(reader.num_entries(), 0);
    }

    #[test]
    fn test_reader_open_single_entry() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("single.sst");

        let key = mvcc_key(b"key", 100);
        create_test_sst(&path, &[(&key, b"value")]).unwrap();

        let reader = SstReader::open(&path).unwrap();
        assert_eq!(reader.num_entries(), 1);
        assert_eq!(reader.num_blocks(), 1);
    }

    #[test]
    fn test_reader_open_multiple_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multi.sst");

        let keys: Vec<_> = (0..100)
            .map(|i| format!("key_{i:05}").into_bytes())
            .collect();
        let entries: Vec<_> = keys
            .iter()
            .map(|k| (k.as_slice(), b"value".as_slice()))
            .collect();

        create_test_sst(&path, &entries).unwrap();

        let reader = SstReader::open(&path).unwrap();
        assert_eq!(reader.num_entries(), 100);
    }

    #[test]
    fn test_reader_open_invalid_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("invalid.sst");

        // Create a file with invalid content
        std::fs::write(&path, b"invalid data").unwrap();

        let result = SstReader::open(&path);
        assert!(result.is_err());
    }

    #[test]
    fn test_reader_open_too_small() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("small.sst");

        // Create a file smaller than footer size
        std::fs::write(&path, b"x").unwrap();

        let result = SstReader::open(&path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too small"));
    }

    // ------------------------------------------------------------------------
    // Block Reading Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_reader_read_block() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("block.sst");

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

        let mut reader = SstReader::open(&path).unwrap();
        let block = reader.read_block(0).unwrap();

        // Block should contain the entries
        assert!(block.num_entries() > 0);
    }

    #[test]
    fn test_reader_read_block_out_of_range() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("range.sst");

        create_test_sst(&path, &[(b"key", b"value")]).unwrap();

        let mut reader = SstReader::open(&path).unwrap();
        let result = reader.read_block(100);
        assert!(result.is_err());
    }

    #[test]
    fn test_reader_multiple_blocks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multiblock.sst");

        // Use small block size to force multiple blocks
        let options = SstBuilderOptions::with_block_size(128);
        let mut builder = SstBuilder::new(&path, options).unwrap();

        for i in 0..100 {
            let key = format!("key_{i:05}");
            let value = format!("value_{i:05}");
            builder.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        builder.finish(1, 0).unwrap();

        let mut reader = SstReader::open(&path).unwrap();
        assert!(reader.num_blocks() > 1);

        // Read all blocks
        for i in 0..reader.num_blocks() as usize {
            let block = reader.read_block(i).unwrap();
            assert!(block.num_entries() > 0);
        }
    }

    // ------------------------------------------------------------------------
    // Point Lookup Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_reader_get_found() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("get.sst");

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

        let mut reader = SstReader::open(&path).unwrap();

        // Find existing keys
        for i in 0..10 {
            let key = format!("key_{i:05}").into_bytes();
            let expected = format!("value_{i:05}").into_bytes();
            let result = reader.get(&key).unwrap();
            assert_eq!(result, Some(expected));
        }
    }

    #[test]
    fn test_reader_get_not_found() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("notfound.sst");

        create_test_sst(&path, &[(b"key_a", b"value_a"), (b"key_c", b"value_c")]).unwrap();

        let mut reader = SstReader::open(&path).unwrap();

        // Key before all entries
        assert_eq!(reader.get(b"key_0").unwrap(), None);

        // Key between entries
        assert_eq!(reader.get(b"key_b").unwrap(), None);

        // Key after all entries
        assert_eq!(reader.get(b"key_z").unwrap(), None);
    }

    #[test]
    fn test_reader_get_empty_sst() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty_get.sst");

        let builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.finish(1, 0).unwrap();

        let mut reader = SstReader::open(&path).unwrap();
        assert_eq!(reader.get(b"any_key").unwrap(), None);
    }

    #[test]
    fn test_reader_get_multiblock() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("get_multiblock.sst");

        // Use small block size
        let options = SstBuilderOptions::with_block_size(128);
        let mut builder = SstBuilder::new(&path, options).unwrap();

        for i in 0..100 {
            let key = format!("key_{i:05}");
            let value = format!("value_{i:05}");
            builder.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        builder.finish(1, 0).unwrap();

        let mut reader = SstReader::open(&path).unwrap();

        // Find keys across different blocks
        for i in [0, 25, 50, 75, 99] {
            let key = format!("key_{i:05}").into_bytes();
            let expected = format!("value_{i:05}").into_bytes();
            let result = reader.get(&key).unwrap();
            assert_eq!(result, Some(expected), "Failed for key {i}");
        }
    }

    // ------------------------------------------------------------------------
    // Find Block Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_reader_find_block() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("find.sst");

        // Use small block size
        let options = SstBuilderOptions::with_block_size(64);
        let mut builder = SstBuilder::new(&path, options).unwrap();

        // Add entries that will span multiple blocks
        for i in 0..50 {
            let key = format!("key_{i:05}");
            builder.add(key.as_bytes(), b"v").unwrap();
        }
        builder.finish(1, 0).unwrap();

        let reader = SstReader::open(&path).unwrap();

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

    #[test]
    fn test_reader_ref_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ref.sst");

        create_test_sst(&path, &[(b"key", b"value")]).unwrap();

        let reader = SstReaderRef::open(&path).unwrap();
        assert_eq!(reader.num_entries().unwrap(), 1);
        assert_eq!(reader.get(b"key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn test_reader_ref_clone() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("clone.sst");

        create_test_sst(&path, &[(b"key", b"value")]).unwrap();

        let reader1 = SstReaderRef::open(&path).unwrap();
        let reader2 = reader1.clone();

        // Both should work
        assert_eq!(reader1.get(b"key").unwrap(), Some(b"value".to_vec()));
        assert_eq!(reader2.get(b"key").unwrap(), Some(b"value".to_vec()));
    }

    // ------------------------------------------------------------------------
    // Timestamp Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_reader_timestamp_tracking() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ts.sst");

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();

        // Add keys with MVCC timestamps
        builder.add(&mvcc_key(b"k1", 100), b"v1").unwrap();
        builder.add(&mvcc_key(b"k2", 200), b"v2").unwrap();
        builder.add(&mvcc_key(b"k3", 50), b"v3").unwrap();

        builder.finish(1, 0).unwrap();

        let reader = SstReader::open(&path).unwrap();
        assert_eq!(reader.min_ts(), 50);
        assert_eq!(reader.max_ts(), 200);
    }

    // ------------------------------------------------------------------------
    // Integration Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_reader_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("roundtrip.sst");

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
        let mut reader = SstReader::open(&path).unwrap();
        assert_eq!(reader.num_entries(), 50);

        for (key, expected_value) in &entries {
            let result = reader.get(key).unwrap();
            assert_eq!(result.as_ref(), Some(expected_value));
        }
    }

    #[test]
    fn test_reader_large_values() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("large.sst");

        let key = b"large_key".to_vec();
        let value = vec![b'x'; 100_000]; // 100KB value

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.add(&key, &value).unwrap();
        builder.finish(1, 0).unwrap();

        let mut reader = SstReader::open(&path).unwrap();
        let result = reader.get(&key).unwrap();
        assert_eq!(result, Some(value));
    }
}
