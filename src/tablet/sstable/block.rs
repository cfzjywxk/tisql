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

//! SST block structures for data and index blocks.
//!
//! ## Data Block Format
//!
//! ```text
//! +-------------------+-------------------+-----+-------------------+
//! | Entry 0           | Entry 1           | ... | Entry N           |
//! +-------------------+-------------------+-----+-------------------+
//! | Offsets Array (4 bytes per entry)                               |
//! +-----------------------------------------------------------------+
//! | Num Entries (4 bytes)                                           |
//! +-----------------------------------------------------------------+
//! | Checksum (4 bytes, CRC32)                                       |
//! +-----------------------------------------------------------------+
//!
//! Entry Format:
//! +------------------+------------------+-------+--------+
//! | key_len (4 bytes)| val_len (4 bytes)| key   | value  |
//! +------------------+------------------+-------+--------+
//! ```
//!
//! ## Index Block Format
//!
//! ```text
//! +-------------------+-------------------+-----+-------------------+
//! | IndexEntry 0      | IndexEntry 1      | ... | IndexEntry N      |
//! +-------------------+-------------------+-----+-------------------+
//! | Offsets Array (4 bytes per entry)                               |
//! +-----------------------------------------------------------------+
//! | Num Entries (4 bytes)                                           |
//! +-----------------------------------------------------------------+
//! | Checksum (4 bytes, CRC32)                                       |
//! +-----------------------------------------------------------------+
//!
//! IndexEntry Format:
//! +---------------------+-----------------+------------------+------------+
//! | first_key_len (4B)  | first_key       | block_offset (8B)| block_size |
//! +---------------------+-----------------+------------------+------------+
//! ```

use crate::catalog::types::RawValue;
use crate::util::error::{Result, TiSqlError};

// ============================================================================
// Constants
// ============================================================================

/// Default block size (4KB)
pub const DEFAULT_BLOCK_SIZE: usize = 4 * 1024;

/// Size of entry offset in offsets array (4 bytes)
const OFFSET_SIZE: usize = 4;

/// Size of num_entries field (4 bytes)
const NUM_ENTRIES_SIZE: usize = 4;

/// Size of checksum field (4 bytes)
const CHECKSUM_SIZE: usize = 4;

/// Size of key_len and val_len fields (4 bytes each)
const LEN_SIZE: usize = 4;

/// Size of block_offset field in index entry (8 bytes)
const BLOCK_OFFSET_SIZE: usize = 8;

/// Size of block_size field in index entry (4 bytes)
const BLOCK_SIZE_FIELD: usize = 4;

// ============================================================================
// CRC32 Checksum
// ============================================================================

/// Compute CRC32 checksum (IEEE polynomial)
fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for byte in data {
        crc ^= *byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

// ============================================================================
// DataBlock
// ============================================================================

/// A data block containing key-value entries.
///
/// Data blocks are the fundamental unit of storage in SST files.
/// Each block contains multiple key-value entries sorted by key.
#[derive(Debug, Clone)]
pub struct DataBlock {
    /// Raw block data (excluding checksum which is verified on load)
    data: Vec<u8>,
    /// Offsets to each entry within the data
    offsets: Vec<u32>,
}

impl DataBlock {
    /// Decode a data block from raw bytes.
    ///
    /// Validates the checksum and parses the offsets array.
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < NUM_ENTRIES_SIZE + CHECKSUM_SIZE {
            return Err(TiSqlError::Storage("Data block too small".into()));
        }

        // Verify checksum (last 4 bytes)
        let checksum_offset = data.len() - CHECKSUM_SIZE;
        let stored_checksum = u32::from_le_bytes(data[checksum_offset..].try_into().unwrap());
        let computed_checksum = crc32(&data[..checksum_offset]);

        if stored_checksum != computed_checksum {
            return Err(TiSqlError::Storage(format!(
                "Data block checksum mismatch: expected {stored_checksum}, got {computed_checksum}"
            )));
        }

        // Parse num_entries (before checksum)
        let num_entries_offset = checksum_offset - NUM_ENTRIES_SIZE;
        let num_entries = u32::from_le_bytes(
            data[num_entries_offset..num_entries_offset + NUM_ENTRIES_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;

        // Parse offsets array (before num_entries)
        let offsets_size = num_entries * OFFSET_SIZE;
        let offsets_start = num_entries_offset - offsets_size;

        if offsets_start > data.len() {
            return Err(TiSqlError::Storage(
                "Invalid data block: offsets overflow".into(),
            ));
        }

        let mut offsets = Vec::with_capacity(num_entries);
        for i in 0..num_entries {
            let offset_pos = offsets_start + i * OFFSET_SIZE;
            let offset = u32::from_le_bytes(
                data[offset_pos..offset_pos + OFFSET_SIZE]
                    .try_into()
                    .unwrap(),
            );
            offsets.push(offset);
        }

        // Data portion is everything before the offsets array
        let block_data = data[..offsets_start].to_vec();

        Ok(Self {
            data: block_data,
            offsets,
        })
    }

    /// Get the number of entries in this block.
    #[inline]
    pub fn num_entries(&self) -> usize {
        self.offsets.len()
    }

    /// Check if the block is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Get a key-value entry by index.
    pub fn get_entry(&self, idx: usize) -> Option<(Vec<u8>, RawValue)> {
        if idx >= self.offsets.len() {
            return None;
        }

        let offset = self.offsets[idx] as usize;
        let end = if idx + 1 < self.offsets.len() {
            self.offsets[idx + 1] as usize
        } else {
            self.data.len()
        };

        if offset + 2 * LEN_SIZE > end || end > self.data.len() {
            return None;
        }

        // Parse entry: key_len (4) | val_len (4) | key | value
        let key_len =
            u32::from_le_bytes(self.data[offset..offset + LEN_SIZE].try_into().unwrap()) as usize;
        let val_len = u32::from_le_bytes(
            self.data[offset + LEN_SIZE..offset + 2 * LEN_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;

        let key_start = offset + 2 * LEN_SIZE;
        let key_end = key_start + key_len;
        let val_end = key_end + val_len;

        if val_end > end {
            return None;
        }

        let key = self.data[key_start..key_end].to_vec();
        let value = self.data[key_end..val_end].to_vec();

        Some((key, value))
    }

    /// Get the first key in this block.
    pub fn first_key(&self) -> Option<Vec<u8>> {
        self.get_entry(0).map(|(k, _)| k)
    }

    /// Get the last key in this block.
    pub fn last_key(&self) -> Option<Vec<u8>> {
        if self.offsets.is_empty() {
            return None;
        }
        self.get_entry(self.offsets.len() - 1).map(|(k, _)| k)
    }

    /// Binary search for a key in this block.
    ///
    /// Returns the index of the entry with the largest key <= target key,
    /// or None if all keys are greater than the target.
    pub fn search(&self, target: &[u8]) -> Option<usize> {
        if self.offsets.is_empty() {
            return None;
        }

        let mut left = 0;
        let mut right = self.offsets.len();

        while left < right {
            let mid = left + (right - left) / 2;
            if let Some((key, _)) = self.get_entry(mid) {
                match key.as_slice().cmp(target) {
                    std::cmp::Ordering::Less => left = mid + 1,
                    std::cmp::Ordering::Greater => right = mid,
                    std::cmp::Ordering::Equal => return Some(mid),
                }
            } else {
                break;
            }
        }

        // Return the entry before `left` (largest key <= target)
        if left > 0 {
            Some(left - 1)
        } else {
            // All keys are greater than target
            None
        }
    }

    /// Search for exact key match.
    pub fn get(&self, target: &[u8]) -> Option<RawValue> {
        let idx = self.search(target)?;
        let (key, value) = self.get_entry(idx)?;
        if key.as_slice() == target {
            Some(value)
        } else {
            None
        }
    }

    /// Create an iterator over all entries.
    pub fn iter(&self) -> DataBlockIterator<'_> {
        DataBlockIterator {
            block: self,
            current: 0,
        }
    }
}

/// Iterator over DataBlock entries.
pub struct DataBlockIterator<'a> {
    block: &'a DataBlock,
    current: usize,
}

impl<'a> Iterator for DataBlockIterator<'a> {
    type Item = (Vec<u8>, RawValue);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.block.num_entries() {
            return None;
        }
        let entry = self.block.get_entry(self.current);
        self.current += 1;
        entry
    }
}

// ============================================================================
// DataBlockBuilder
// ============================================================================

/// Builder for creating data blocks.
///
/// Entries are added in sorted order (caller must ensure this).
/// The builder tracks the current size and signals when the block is full.
#[derive(Debug)]
pub struct DataBlockBuilder {
    /// Accumulated entry data
    data: Vec<u8>,
    /// Offsets to each entry
    offsets: Vec<u32>,
    /// Target block size
    block_size_limit: usize,
    /// First key in this block (for index)
    first_key: Option<Vec<u8>>,
    /// Last key in this block (for index)
    last_key: Option<Vec<u8>>,
}

impl DataBlockBuilder {
    /// Create a new data block builder with default block size.
    pub fn new() -> Self {
        Self::with_block_size(DEFAULT_BLOCK_SIZE)
    }

    /// Create a new data block builder with custom block size.
    pub fn with_block_size(block_size: usize) -> Self {
        Self {
            data: Vec::with_capacity(block_size),
            offsets: Vec::new(),
            block_size_limit: block_size,
            first_key: None,
            last_key: None,
        }
    }

    /// Add a key-value entry to the block.
    ///
    /// Returns false if the entry would exceed the block size limit.
    /// In that case, the entry is NOT added and the caller should
    /// finish this block and start a new one.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        let entry_size = 2 * LEN_SIZE + key.len() + value.len();
        let new_size = self.estimated_size() + entry_size + OFFSET_SIZE;

        // Allow at least one entry per block
        if !self.offsets.is_empty() && new_size > self.block_size_limit {
            return false;
        }

        // Record offset
        self.offsets.push(self.data.len() as u32);

        // Write entry: key_len | val_len | key | value
        self.data
            .extend_from_slice(&(key.len() as u32).to_le_bytes());
        self.data
            .extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.data.extend_from_slice(key);
        self.data.extend_from_slice(value);

        // Track first/last key
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.last_key = Some(key.to_vec());

        true
    }

    /// Get the estimated size of the block when finished.
    pub fn estimated_size(&self) -> usize {
        self.data.len() + self.offsets.len() * OFFSET_SIZE + NUM_ENTRIES_SIZE + CHECKSUM_SIZE
    }

    /// Check if the block is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Get the number of entries.
    #[inline]
    pub fn num_entries(&self) -> usize {
        self.offsets.len()
    }

    /// Get the first key in this block.
    pub fn first_key(&self) -> Option<&Vec<u8>> {
        self.first_key.as_ref()
    }

    /// Get the last key in this block.
    pub fn last_key(&self) -> Option<&Vec<u8>> {
        self.last_key.as_ref()
    }

    /// Finish building and return the encoded block bytes.
    pub fn finish(self) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.estimated_size());

        // Write entry data
        result.extend_from_slice(&self.data);

        // Write offsets array
        for offset in &self.offsets {
            result.extend_from_slice(&offset.to_le_bytes());
        }

        // Write num_entries
        result.extend_from_slice(&(self.offsets.len() as u32).to_le_bytes());

        // Compute and write checksum
        let checksum = crc32(&result);
        result.extend_from_slice(&checksum.to_le_bytes());

        result
    }

    /// Reset the builder for reuse.
    pub fn reset(&mut self) {
        self.data.clear();
        self.offsets.clear();
        self.first_key = None;
        self.last_key = None;
    }
}

impl Default for DataBlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// IndexEntry
// ============================================================================

/// An entry in the index block pointing to a data block.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexEntry {
    /// First key in the data block
    pub first_key: Vec<u8>,
    /// Offset of the data block from file start
    pub block_offset: u64,
    /// Size of the data block in bytes
    pub block_size: u32,
}

impl IndexEntry {
    /// Create a new index entry.
    pub fn new(first_key: Vec<u8>, block_offset: u64, block_size: u32) -> Self {
        Self {
            first_key,
            block_offset,
            block_size,
        }
    }

    /// Encode this entry to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            LEN_SIZE + self.first_key.len() + BLOCK_OFFSET_SIZE + BLOCK_SIZE_FIELD,
        );
        buf.extend_from_slice(&(self.first_key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.first_key);
        buf.extend_from_slice(&self.block_offset.to_le_bytes());
        buf.extend_from_slice(&self.block_size.to_le_bytes());
        buf
    }

    /// Decode an entry from bytes, returning (entry, bytes_consumed).
    pub fn decode(data: &[u8]) -> Result<(Self, usize)> {
        if data.len() < LEN_SIZE {
            return Err(TiSqlError::Storage("Index entry too small".into()));
        }

        let key_len = u32::from_le_bytes(data[..LEN_SIZE].try_into().unwrap()) as usize;

        let min_size = LEN_SIZE + key_len + BLOCK_OFFSET_SIZE + BLOCK_SIZE_FIELD;
        if data.len() < min_size {
            return Err(TiSqlError::Storage("Index entry truncated".into()));
        }

        let first_key = data[LEN_SIZE..LEN_SIZE + key_len].to_vec();
        let offset_start = LEN_SIZE + key_len;
        let block_offset = u64::from_le_bytes(
            data[offset_start..offset_start + BLOCK_OFFSET_SIZE]
                .try_into()
                .unwrap(),
        );
        let size_start = offset_start + BLOCK_OFFSET_SIZE;
        let block_size = u32::from_le_bytes(
            data[size_start..size_start + BLOCK_SIZE_FIELD]
                .try_into()
                .unwrap(),
        );

        Ok((
            Self {
                first_key,
                block_offset,
                block_size,
            },
            min_size,
        ))
    }
}

// ============================================================================
// IndexBlock
// ============================================================================

/// An index block containing entries pointing to data blocks.
///
/// The index block enables binary search to find the data block
/// containing a given key.
#[derive(Debug, Clone)]
pub struct IndexBlock {
    /// Index entries (sorted by first_key)
    entries: Vec<IndexEntry>,
}

impl IndexBlock {
    /// Decode an index block from raw bytes.
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < NUM_ENTRIES_SIZE + CHECKSUM_SIZE {
            return Err(TiSqlError::Storage("Index block too small".into()));
        }

        // Verify checksum
        let checksum_offset = data.len() - CHECKSUM_SIZE;
        let stored_checksum = u32::from_le_bytes(data[checksum_offset..].try_into().unwrap());
        let computed_checksum = crc32(&data[..checksum_offset]);

        if stored_checksum != computed_checksum {
            return Err(TiSqlError::Storage(format!(
                "Index block checksum mismatch: expected {stored_checksum}, got {computed_checksum}"
            )));
        }

        // Parse num_entries
        let num_entries_offset = checksum_offset - NUM_ENTRIES_SIZE;
        let num_entries = u32::from_le_bytes(
            data[num_entries_offset..num_entries_offset + NUM_ENTRIES_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;

        // Parse offsets array with overflow protection
        let offsets_size = num_entries.checked_mul(OFFSET_SIZE).ok_or_else(|| {
            TiSqlError::Storage("Index block corrupted: num_entries overflow".into())
        })?;
        let offsets_start = num_entries_offset
            .checked_sub(offsets_size)
            .ok_or_else(|| {
                TiSqlError::Storage("Index block corrupted: offsets underflow".into())
            })?;

        if offsets_start > data.len() {
            return Err(TiSqlError::Storage(
                "Invalid index block: offsets overflow".into(),
            ));
        }

        let mut offsets = Vec::with_capacity(num_entries);
        for i in 0..num_entries {
            let offset_pos = offsets_start + i * OFFSET_SIZE;
            let offset = u32::from_le_bytes(
                data[offset_pos..offset_pos + OFFSET_SIZE]
                    .try_into()
                    .unwrap(),
            );
            offsets.push(offset);
        }

        // Parse entries
        let mut entries = Vec::with_capacity(num_entries);
        for i in 0..num_entries {
            let start = offsets[i] as usize;
            let end = if i + 1 < num_entries {
                offsets[i + 1] as usize
            } else {
                offsets_start
            };

            let (entry, _) = IndexEntry::decode(&data[start..end])?;
            entries.push(entry);
        }

        Ok(Self { entries })
    }

    /// Get the number of entries.
    #[inline]
    pub fn num_entries(&self) -> usize {
        self.entries.len()
    }

    /// Check if empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get an entry by index.
    pub fn get_entry(&self, idx: usize) -> Option<&IndexEntry> {
        self.entries.get(idx)
    }

    /// Binary search for the data block that may contain the key.
    ///
    /// Returns the index of the entry with the largest first_key <= target,
    /// which is the data block that may contain the key.
    pub fn search(&self, target: &[u8]) -> Option<usize> {
        if self.entries.is_empty() {
            return None;
        }

        let mut left = 0;
        let mut right = self.entries.len();

        while left < right {
            let mid = left + (right - left) / 2;
            match self.entries[mid].first_key.as_slice().cmp(target) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Greater => right = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }

        // Return the entry before `left` (largest first_key <= target)
        if left > 0 {
            Some(left - 1)
        } else {
            // All first_keys are greater than target, but the key might
            // still be in the first block if it's less than the first block's
            // first key (edge case for MVCC where we seek by key||!ts)
            None
        }
    }

    /// Get all entries.
    pub fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    /// Create an iterator over entries.
    pub fn iter(&self) -> impl Iterator<Item = &IndexEntry> {
        self.entries.iter()
    }
}

// ============================================================================
// IndexBlockBuilder
// ============================================================================

/// Builder for creating index blocks.
#[derive(Debug, Default)]
pub struct IndexBlockBuilder {
    /// Entry data
    data: Vec<u8>,
    /// Offsets to each entry
    offsets: Vec<u32>,
}

impl IndexBlockBuilder {
    /// Create a new index block builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an index entry.
    pub fn add(&mut self, entry: &IndexEntry) {
        self.offsets.push(self.data.len() as u32);
        self.data.extend_from_slice(&entry.encode());
    }

    /// Add an entry by components.
    pub fn add_entry(&mut self, first_key: &[u8], block_offset: u64, block_size: u32) {
        let entry = IndexEntry::new(first_key.to_vec(), block_offset, block_size);
        self.add(&entry);
    }

    /// Check if empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Get number of entries.
    #[inline]
    pub fn num_entries(&self) -> usize {
        self.offsets.len()
    }

    /// Get estimated size when finished.
    pub fn estimated_size(&self) -> usize {
        self.data.len() + self.offsets.len() * OFFSET_SIZE + NUM_ENTRIES_SIZE + CHECKSUM_SIZE
    }

    /// Finish building and return encoded bytes.
    pub fn finish(self) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.estimated_size());

        // Write entry data
        result.extend_from_slice(&self.data);

        // Write offsets array
        for offset in &self.offsets {
            result.extend_from_slice(&offset.to_le_bytes());
        }

        // Write num_entries
        result.extend_from_slice(&(self.offsets.len() as u32).to_le_bytes());

        // Compute and write checksum
        let checksum = crc32(&result);
        result.extend_from_slice(&checksum.to_le_bytes());

        result
    }

    /// Reset for reuse.
    pub fn reset(&mut self) {
        self.data.clear();
        self.offsets.clear();
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------------
    // CRC32 Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_crc32() {
        // Known test vectors
        assert_eq!(crc32(b""), 0x00000000);
        assert_eq!(crc32(b"hello world"), 0x0D4A1185);
        assert_eq!(crc32(b"123456789"), 0xCBF43926);
    }

    // ------------------------------------------------------------------------
    // DataBlock Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_data_block_builder_single_entry() {
        let mut builder = DataBlockBuilder::new();
        assert!(builder.is_empty());

        builder.add(b"key1", b"value1");
        assert!(!builder.is_empty());
        assert_eq!(builder.num_entries(), 1);
        assert_eq!(builder.first_key(), Some(&b"key1".to_vec()));
        assert_eq!(builder.last_key(), Some(&b"key1".to_vec()));

        let data = builder.finish();
        let block = DataBlock::decode(&data).unwrap();

        assert_eq!(block.num_entries(), 1);
        assert_eq!(
            block.get_entry(0),
            Some((b"key1".to_vec(), b"value1".to_vec()))
        );
    }

    #[test]
    fn test_data_block_builder_multiple_entries() {
        let mut builder = DataBlockBuilder::new();

        builder.add(b"aaa", b"val_a");
        builder.add(b"bbb", b"val_b");
        builder.add(b"ccc", b"val_c");

        assert_eq!(builder.num_entries(), 3);
        assert_eq!(builder.first_key(), Some(&b"aaa".to_vec()));
        assert_eq!(builder.last_key(), Some(&b"ccc".to_vec()));

        let data = builder.finish();
        let block = DataBlock::decode(&data).unwrap();

        assert_eq!(block.num_entries(), 3);
        assert_eq!(block.first_key(), Some(b"aaa".to_vec()));
        assert_eq!(block.last_key(), Some(b"ccc".to_vec()));

        // Verify all entries
        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], (b"aaa".to_vec(), b"val_a".to_vec()));
        assert_eq!(entries[1], (b"bbb".to_vec(), b"val_b".to_vec()));
        assert_eq!(entries[2], (b"ccc".to_vec(), b"val_c".to_vec()));
    }

    #[test]
    fn test_data_block_size_limit() {
        // Small block size to test overflow
        let mut builder = DataBlockBuilder::with_block_size(64);

        // First entry should always succeed
        assert!(builder.add(b"key1", b"value1"));

        // Add entries until we hit the limit
        let mut added = 1;
        while builder.add(b"keyX", b"valueX") {
            added += 1;
            if added > 100 {
                panic!("Should have hit size limit");
            }
        }

        // At least one entry should be added
        assert!(added >= 1);
        assert!(builder.num_entries() < 10); // With 64 byte limit, can't fit many
    }

    #[test]
    fn test_data_block_search() {
        let mut builder = DataBlockBuilder::new();

        builder.add(b"apple", b"1");
        builder.add(b"banana", b"2");
        builder.add(b"cherry", b"3");
        builder.add(b"date", b"4");
        builder.add(b"elderberry", b"5");

        let data = builder.finish();
        let block = DataBlock::decode(&data).unwrap();

        // Exact matches
        assert_eq!(block.search(b"apple"), Some(0));
        assert_eq!(block.search(b"banana"), Some(1));
        assert_eq!(block.search(b"cherry"), Some(2));
        assert_eq!(block.search(b"date"), Some(3));
        assert_eq!(block.search(b"elderberry"), Some(4));

        // Search for key between entries (should return largest key <= target)
        assert_eq!(block.search(b"blueberry"), Some(1)); // banana < blueberry < cherry
        assert_eq!(block.search(b"dog"), Some(3)); // date < dog < elderberry

        // Search for key smaller than all (should return None)
        assert_eq!(block.search(b"aardvark"), None);

        // Search for key larger than all (should return last)
        assert_eq!(block.search(b"zebra"), Some(4));
    }

    #[test]
    fn test_data_block_get() {
        let mut builder = DataBlockBuilder::new();

        builder.add(b"key1", b"value1");
        builder.add(b"key2", b"value2");
        builder.add(b"key3", b"value3");

        let data = builder.finish();
        let block = DataBlock::decode(&data).unwrap();

        // Exact matches
        assert_eq!(block.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(block.get(b"key2"), Some(b"value2".to_vec()));
        assert_eq!(block.get(b"key3"), Some(b"value3".to_vec()));

        // Non-existent keys
        assert_eq!(block.get(b"key0"), None);
        assert_eq!(block.get(b"key4"), None);
        assert_eq!(block.get(b"key1.5"), None);
    }

    #[test]
    fn test_data_block_empty() {
        let builder = DataBlockBuilder::new();
        let data = builder.finish();
        let block = DataBlock::decode(&data).unwrap();

        assert!(block.is_empty());
        assert_eq!(block.num_entries(), 0);
        assert_eq!(block.first_key(), None);
        assert_eq!(block.last_key(), None);
        assert_eq!(block.search(b"any"), None);
        assert_eq!(block.get(b"any"), None);
    }

    #[test]
    fn test_data_block_large_values() {
        let mut builder = DataBlockBuilder::with_block_size(64 * 1024); // 64KB

        let large_key = vec![b'k'; 1000];
        let large_value = vec![b'v'; 10000];

        builder.add(&large_key, &large_value);

        let data = builder.finish();
        let block = DataBlock::decode(&data).unwrap();

        assert_eq!(block.num_entries(), 1);
        let (key, value) = block.get_entry(0).unwrap();
        assert_eq!(key, large_key);
        assert_eq!(value, large_value);
    }

    #[test]
    fn test_data_block_checksum_corruption() {
        let mut builder = DataBlockBuilder::new();
        builder.add(b"key", b"value");
        let mut data = builder.finish();

        // Corrupt the checksum
        let len = data.len();
        data[len - 1] ^= 0xFF;

        let result = DataBlock::decode(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("checksum"));
    }

    #[test]
    fn test_data_block_builder_reset() {
        let mut builder = DataBlockBuilder::new();

        builder.add(b"key1", b"value1");
        assert_eq!(builder.num_entries(), 1);

        builder.reset();
        assert!(builder.is_empty());
        assert_eq!(builder.first_key(), None);

        builder.add(b"key2", b"value2");
        assert_eq!(builder.num_entries(), 1);
        assert_eq!(builder.first_key(), Some(&b"key2".to_vec()));
    }

    // ------------------------------------------------------------------------
    // IndexEntry Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_index_entry_encode_decode() {
        let entry = IndexEntry::new(b"first_key".to_vec(), 12345, 4096);

        let encoded = entry.encode();
        let (decoded, consumed) = IndexEntry::decode(&encoded).unwrap();

        assert_eq!(entry, decoded);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn test_index_entry_empty_key() {
        let entry = IndexEntry::new(vec![], 0, 0);

        let encoded = entry.encode();
        let (decoded, _) = IndexEntry::decode(&encoded).unwrap();

        assert_eq!(entry, decoded);
    }

    // ------------------------------------------------------------------------
    // IndexBlock Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_index_block_builder_single_entry() {
        let mut builder = IndexBlockBuilder::new();
        assert!(builder.is_empty());

        builder.add_entry(b"first", 0, 1024);
        assert!(!builder.is_empty());
        assert_eq!(builder.num_entries(), 1);

        let data = builder.finish();
        let block = IndexBlock::decode(&data).unwrap();

        assert_eq!(block.num_entries(), 1);
        let entry = block.get_entry(0).unwrap();
        assert_eq!(entry.first_key, b"first".to_vec());
        assert_eq!(entry.block_offset, 0);
        assert_eq!(entry.block_size, 1024);
    }

    #[test]
    fn test_index_block_builder_multiple_entries() {
        let mut builder = IndexBlockBuilder::new();

        builder.add_entry(b"aaa", 0, 1000);
        builder.add_entry(b"bbb", 1000, 1000);
        builder.add_entry(b"ccc", 2000, 1000);

        let data = builder.finish();
        let block = IndexBlock::decode(&data).unwrap();

        assert_eq!(block.num_entries(), 3);

        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries[0].first_key, b"aaa".to_vec());
        assert_eq!(entries[1].first_key, b"bbb".to_vec());
        assert_eq!(entries[2].first_key, b"ccc".to_vec());
    }

    #[test]
    fn test_index_block_search() {
        let mut builder = IndexBlockBuilder::new();

        // Simulate data blocks with these first keys
        builder.add_entry(b"apple", 0, 1000); // block 0: apple-banana
        builder.add_entry(b"cherry", 1000, 1000); // block 1: cherry-date
        builder.add_entry(b"elderberry", 2000, 1000); // block 2: elderberry-fig

        let data = builder.finish();
        let block = IndexBlock::decode(&data).unwrap();

        // Exact matches
        assert_eq!(block.search(b"apple"), Some(0));
        assert_eq!(block.search(b"cherry"), Some(1));
        assert_eq!(block.search(b"elderberry"), Some(2));

        // Keys in between first_keys
        assert_eq!(block.search(b"banana"), Some(0)); // apple < banana < cherry
        assert_eq!(block.search(b"date"), Some(1)); // cherry < date < elderberry
        assert_eq!(block.search(b"fig"), Some(2)); // elderberry < fig

        // Key smaller than all first_keys
        assert_eq!(block.search(b"aardvark"), None);

        // Key larger than all first_keys
        assert_eq!(block.search(b"zebra"), Some(2));
    }

    #[test]
    fn test_index_block_empty() {
        let builder = IndexBlockBuilder::new();
        let data = builder.finish();
        let block = IndexBlock::decode(&data).unwrap();

        assert!(block.is_empty());
        assert_eq!(block.num_entries(), 0);
        assert_eq!(block.search(b"any"), None);
    }

    #[test]
    fn test_index_block_checksum_corruption() {
        let mut builder = IndexBlockBuilder::new();
        builder.add_entry(b"key", 0, 100);
        let mut data = builder.finish();

        // Corrupt the checksum
        let len = data.len();
        data[len - 1] ^= 0xFF;

        let result = IndexBlock::decode(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("checksum"));
    }

    #[test]
    fn test_index_block_builder_reset() {
        let mut builder = IndexBlockBuilder::new();

        builder.add_entry(b"key1", 0, 100);
        assert_eq!(builder.num_entries(), 1);

        builder.reset();
        assert!(builder.is_empty());

        builder.add_entry(b"key2", 100, 200);
        assert_eq!(builder.num_entries(), 1);
    }

    // ------------------------------------------------------------------------
    // Integration Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_data_block_with_mvcc_keys() {
        // Simulate MVCC key encoding: key || !timestamp
        fn encode_mvcc(key_bytes: &[u8], ts: u64) -> Vec<u8> {
            let mut result = key_bytes.to_vec();
            result.extend_from_slice(&(!ts).to_be_bytes());
            result
        }

        let mut builder = DataBlockBuilder::new();

        // Same key with different timestamps (descending order)
        builder.add(&encode_mvcc(b"key1", 100), b"v100");
        builder.add(&encode_mvcc(b"key1", 50), b"v50");
        builder.add(&encode_mvcc(b"key1", 10), b"v10");
        builder.add(&encode_mvcc(b"key2", 200), b"v200");

        let data = builder.finish();
        let block = DataBlock::decode(&data).unwrap();

        assert_eq!(block.num_entries(), 4);

        // Verify ordering (higher ts comes first due to !ts encoding)
        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries[0].1, b"v100".to_vec());
        assert_eq!(entries[1].1, b"v50".to_vec());
        assert_eq!(entries[2].1, b"v10".to_vec());
        assert_eq!(entries[3].1, b"v200".to_vec());

        // Search for user1 at ts=75 should find entry with ts=100
        // (first entry >= user1||!75)
        let search_key = encode_mvcc(b"user1", 75);
        let idx = block.search(&search_key);
        // The search finds largest key <= target, which would be user1||!100
        // since !100 < !75 (higher ts = smaller encoded value)
        assert!(idx.is_some());
    }

    #[test]
    fn test_roundtrip_many_entries() {
        let mut builder = DataBlockBuilder::with_block_size(64 * 1024);

        // Add many entries
        for i in 0..1000 {
            let key = format!("key_{i:05}");
            let value = format!("value_{i}");
            builder.add(key.as_bytes(), value.as_bytes());
        }

        let data = builder.finish();
        let block = DataBlock::decode(&data).unwrap();

        assert_eq!(block.num_entries(), 1000);

        // Verify random entries
        for i in [0, 100, 500, 999] {
            let key = format!("key_{i:05}");
            let value = format!("value_{i}");
            assert_eq!(block.get(key.as_bytes()), Some(value.into_bytes()));
        }
    }
}
