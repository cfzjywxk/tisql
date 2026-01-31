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

//! SST file iterators for sequential access to SST files.
//!
//! ## Usage
//!
//! ```ignore
//! // Create an iterator over an SST file
//! let iter = SstIterator::new(reader)?;
//!
//! // Iterate all entries
//! while iter.valid() {
//!     let key = iter.key();
//!     let value = iter.value();
//!     // Process entry
//!     iter.advance()?;
//! }
//!
//! // Seek to a specific key
//! iter.seek(b"target_key")?;
//! ```

use crate::error::Result;
use crate::storage::mvcc::{MvccIterator, MvccKey, SharedMvccRange, TIMESTAMP_SIZE};
use crate::types::{RawValue, Timestamp};

use super::block::DataBlock;
use super::reader::SstReaderRef;

// ============================================================================
// BlockState - Owns a block and tracks position
// ============================================================================

/// State for iterating within a data block.
struct BlockState {
    /// The data block
    block: DataBlock,
    /// Current entry index
    current: usize,
    /// Cached current entry (key, value)
    entry: Option<(Vec<u8>, RawValue)>,
}

impl BlockState {
    fn new(block: DataBlock) -> Self {
        let mut state = Self {
            block,
            current: 0,
            entry: None,
        };
        state.update_entry();
        state
    }

    fn valid(&self) -> bool {
        self.entry.is_some()
    }

    fn key(&self) -> &[u8] {
        self.entry.as_ref().map(|(k, _)| k.as_slice()).unwrap()
    }

    fn value(&self) -> &[u8] {
        self.entry.as_ref().map(|(_, v)| v.as_slice()).unwrap()
    }

    fn seek_to_first(&mut self) {
        self.current = 0;
        self.update_entry();
    }

    fn seek_to_last(&mut self) {
        let n = self.block.num_entries();
        self.current = if n > 0 { n - 1 } else { 0 };
        self.update_entry();
    }

    fn seek(&mut self, target: &[u8]) {
        // Use binary search to find first key >= target
        let n = self.block.num_entries();
        if n == 0 {
            self.entry = None;
            return;
        }

        let mut left = 0;
        let mut right = n;

        while left < right {
            let mid = left + (right - left) / 2;
            if let Some((key, _)) = self.block.get_entry(mid) {
                if key.as_slice() < target {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            } else {
                break;
            }
        }

        self.current = left;
        self.update_entry();
    }

    fn next(&mut self) {
        self.current += 1;
        self.update_entry();
    }

    fn prev(&mut self) {
        if self.current > 0 {
            self.current -= 1;
            self.update_entry();
        } else {
            self.entry = None;
        }
    }

    fn update_entry(&mut self) {
        self.entry = self.block.get_entry(self.current);
    }
}

// ============================================================================
// SstIterator
// ============================================================================

/// Iterator over entries in an SST file.
///
/// This iterator maintains state for the current block and position within
/// the block. It lazily loads blocks as needed.
pub struct SstIterator {
    /// Reference to the SST reader
    reader: SstReaderRef,
    /// Number of blocks in the SST
    num_blocks: u32,
    /// Current block index
    block_idx: usize,
    /// Current block state (owns the block)
    block_state: Option<BlockState>,
}

impl SstIterator {
    /// Create a new iterator starting at the beginning of the SST.
    pub fn new(reader: SstReaderRef) -> Result<Self> {
        let num_blocks = reader.num_blocks()?;

        let mut iter = Self {
            reader,
            num_blocks,
            block_idx: 0,
            block_state: None,
        };

        // Position at first entry if exists
        if num_blocks > 0 {
            iter.seek_to_first()?;
        }

        Ok(iter)
    }

    /// Check if the iterator is positioned on a valid entry.
    pub fn valid(&self) -> bool {
        self.block_state.as_ref().is_some_and(|s| s.valid())
    }

    /// Get the current key.
    ///
    /// Panics if the iterator is not valid.
    pub fn key(&self) -> &[u8] {
        self.block_state.as_ref().expect("Iterator not valid").key()
    }

    /// Get the current value.
    ///
    /// Panics if the iterator is not valid.
    pub fn value(&self) -> &[u8] {
        self.block_state
            .as_ref()
            .expect("Iterator not valid")
            .value()
    }

    /// Seek to the first entry in the SST.
    pub fn seek_to_first(&mut self) -> Result<()> {
        if self.num_blocks == 0 {
            self.block_state = None;
            return Ok(());
        }

        self.block_idx = 0;
        self.load_block()?;

        if let Some(ref mut state) = self.block_state {
            state.seek_to_first();
        }

        Ok(())
    }

    /// Seek to the last entry in the SST.
    pub fn seek_to_last(&mut self) -> Result<()> {
        if self.num_blocks == 0 {
            self.block_state = None;
            return Ok(());
        }

        self.block_idx = self.num_blocks as usize - 1;
        self.load_block()?;

        if let Some(ref mut state) = self.block_state {
            state.seek_to_last();
        }

        Ok(())
    }

    /// Seek to the first entry >= target key.
    ///
    /// If no such entry exists, the iterator becomes invalid.
    pub fn seek(&mut self, target: &[u8]) -> Result<()> {
        if self.num_blocks == 0 {
            self.block_state = None;
            return Ok(());
        }

        // Find the block that may contain the target key
        let block_idx = self.reader.find_block(target)?;
        self.block_idx = block_idx;
        self.load_block()?;

        if let Some(ref mut state) = self.block_state {
            state.seek(target);

            // If the current block doesn't have a key >= target,
            // try the next block
            if !state.valid() {
                self.next_block()?;
            }
        }

        Ok(())
    }

    /// Seek to the first entry > target key (seek for prev).
    pub fn seek_for_prev(&mut self, target: &[u8]) -> Result<()> {
        // First seek to target
        self.seek(target)?;

        if !self.valid() {
            // No key >= target, seek to last
            self.seek_to_last()?;
        } else if self.key() != target {
            // Current key > target, go to prev
            self.prev()?;
        }

        Ok(())
    }

    /// Move to the next entry.
    ///
    /// Note: This returns `Result` for I/O error handling, unlike `std::iter::Iterator`.
    pub fn advance(&mut self) -> Result<()> {
        if !self.valid() {
            return Ok(());
        }

        if let Some(ref mut state) = self.block_state {
            state.next();

            if !state.valid() {
                // Current block exhausted, move to next block
                self.next_block()?;
            }
        }

        Ok(())
    }

    /// Move to the previous entry.
    pub fn prev(&mut self) -> Result<()> {
        if !self.valid() {
            return Ok(());
        }

        if let Some(ref mut state) = self.block_state {
            state.prev();

            if !state.valid() {
                // Current block exhausted, move to previous block
                self.prev_block()?;
            }
        }

        Ok(())
    }

    /// Load the current block.
    fn load_block(&mut self) -> Result<()> {
        if self.block_idx >= self.num_blocks as usize {
            self.block_state = None;
            return Ok(());
        }

        let block = self.reader.read_block(self.block_idx)?;
        self.block_state = Some(BlockState::new(block));
        Ok(())
    }

    /// Move to the next block.
    fn next_block(&mut self) -> Result<()> {
        self.block_idx += 1;

        if self.block_idx >= self.num_blocks as usize {
            self.block_state = None;
            return Ok(());
        }

        self.load_block()?;

        if let Some(ref mut state) = self.block_state {
            state.seek_to_first();
        }

        Ok(())
    }

    /// Move to the previous block.
    fn prev_block(&mut self) -> Result<()> {
        if self.block_idx == 0 {
            self.block_state = None;
            return Ok(());
        }

        self.block_idx -= 1;
        self.load_block()?;

        if let Some(ref mut state) = self.block_state {
            state.seek_to_last();
        }

        Ok(())
    }
}

// ============================================================================
// Concat Iterator - Iterates over multiple SSTs in sequence
// ============================================================================

/// Iterator that concatenates multiple SST iterators.
///
/// This is used for iterating over multiple non-overlapping SST files
/// in sorted order (e.g., level 1+ where SSTs don't overlap).
pub struct ConcatIterator {
    /// SST readers in sorted order
    readers: Vec<SstReaderRef>,
    /// Current SST index
    sst_idx: usize,
    /// Current SST iterator
    current: Option<SstIterator>,
}

impl ConcatIterator {
    /// Create a new concat iterator over the given SST readers.
    ///
    /// Readers should be in sorted order (by key range).
    pub fn new(readers: Vec<SstReaderRef>) -> Result<Self> {
        let mut iter = Self {
            readers,
            sst_idx: 0,
            current: None,
        };

        iter.seek_to_first()?;
        Ok(iter)
    }

    /// Check if the iterator is valid.
    pub fn valid(&self) -> bool {
        self.current.as_ref().is_some_and(|c| c.valid())
    }

    /// Get the current key.
    pub fn key(&self) -> &[u8] {
        self.current.as_ref().expect("Iterator not valid").key()
    }

    /// Get the current value.
    pub fn value(&self) -> &[u8] {
        self.current.as_ref().expect("Iterator not valid").value()
    }

    /// Seek to the first entry.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.sst_idx = 0;
        self.load_current_sst()?;

        // Skip empty SSTs
        while !self.valid() && self.sst_idx + 1 < self.readers.len() {
            self.sst_idx += 1;
            self.load_current_sst()?;
        }

        Ok(())
    }

    /// Seek to a specific key.
    pub fn seek(&mut self, target: &[u8]) -> Result<()> {
        // Start from first SST and seek
        self.sst_idx = 0;

        while self.sst_idx < self.readers.len() {
            self.load_current_sst()?;

            if let Some(ref mut current) = self.current {
                current.seek(target)?;
                if current.valid() {
                    return Ok(());
                }
            }

            self.sst_idx += 1;
        }

        // No valid position found
        self.current = None;
        Ok(())
    }

    /// Move to the next entry.
    ///
    /// Note: This returns `Result` for I/O error handling, unlike `std::iter::Iterator`.
    pub fn advance(&mut self) -> Result<()> {
        if !self.valid() {
            return Ok(());
        }

        if let Some(ref mut current) = self.current {
            current.advance()?;
        }

        // If current SST exhausted, move to next
        while !self.valid() && self.sst_idx + 1 < self.readers.len() {
            self.sst_idx += 1;
            self.load_current_sst()?;
        }

        Ok(())
    }

    /// Load the iterator for the current SST.
    fn load_current_sst(&mut self) -> Result<()> {
        if self.sst_idx >= self.readers.len() {
            self.current = None;
            return Ok(());
        }

        self.current = Some(SstIterator::new(self.readers[self.sst_idx].clone())?);
        Ok(())
    }
}

// ============================================================================
// SstMvccIterator - MvccIterator wrapper for SstIterator
// ============================================================================

/// MvccIterator wrapper around SstIterator.
///
/// This wraps the existing SstIterator to implement the MvccIterator trait,
/// providing range filtering and proper MVCC key semantics.
///
/// ## Zero-Copy Design
///
/// This iterator returns references directly to data cached in the underlying
/// `BlockState`. No additional allocations occur during iteration - the
/// `user_key()`, `timestamp()`, and `value()` methods extract data from the
/// block's cached entry without copying.
///
/// ## Error Handling
///
/// If the SST contains corrupted MVCC keys (too short to be valid), the
/// iterator becomes invalid (`valid()` returns false).
pub struct SstMvccIterator {
    /// Underlying SST iterator
    inner: SstIterator,
    /// Shared range bounds for filtering (avoids cloning per iterator)
    range: SharedMvccRange,
    /// Whether the range is truly unbounded
    is_unbounded: bool,
}

impl SstMvccIterator {
    /// Create a new iterator over the given range.
    ///
    /// The iterator seeks to the start of the range and filters entries
    /// that are beyond the end bound.
    ///
    /// Takes `SharedMvccRange` (Arc) to avoid cloning when creating multiple iterators.
    pub fn new(reader: SstReaderRef, range: SharedMvccRange) -> Result<Self> {
        let is_unbounded = range.start.is_unbounded() && range.end.is_unbounded();
        let mut inner = SstIterator::new(reader)?;

        // Seek to start of range if bounded
        if !is_unbounded && !range.start.is_unbounded() {
            inner.seek(range.start.as_bytes())?;
        }

        Ok(Self {
            inner,
            range,
            is_unbounded,
        })
    }

    /// Check if the current position is within the range bounds.
    ///
    /// Returns true if:
    /// - The iterator is valid AND
    /// - The current key is a valid MVCC key (>= 8 bytes) AND
    /// - The current key is within the range start and end bounds
    #[inline]
    fn is_in_range(&self) -> bool {
        if !self.inner.valid() {
            return false;
        }

        let key_bytes = self.inner.key();

        // Check MVCC key validity (must have at least 8 bytes for timestamp)
        if key_bytes.len() < TIMESTAMP_SIZE {
            return false;
        }

        if self.is_unbounded {
            return true;
        }

        // Check range start bound
        if !self.range.start.is_unbounded() && key_bytes < self.range.start.as_bytes() {
            return false;
        }

        // Check range end bound
        if !self.range.end.is_unbounded() && key_bytes >= self.range.end.as_bytes() {
            return false;
        }

        true
    }
}

impl MvccIterator for SstMvccIterator {
    fn seek(&mut self, target: &MvccKey) -> Result<()> {
        self.inner.seek(target.as_bytes())
    }

    fn advance(&mut self) -> Result<()> {
        self.inner.advance()
    }

    fn valid(&self) -> bool {
        self.is_in_range()
    }

    fn user_key(&self) -> &[u8] {
        let key_bytes = self.inner.key();
        // Safe: is_in_range() already verified len >= TIMESTAMP_SIZE
        &key_bytes[..key_bytes.len() - TIMESTAMP_SIZE]
    }

    fn timestamp(&self) -> Timestamp {
        let key_bytes = self.inner.key();
        // Safe: is_in_range() already verified len >= TIMESTAMP_SIZE
        let ts_start = key_bytes.len() - TIMESTAMP_SIZE;
        let ts_bytes: [u8; 8] = key_bytes[ts_start..]
            .try_into()
            .expect("is_in_range verified length");
        !u64::from_be_bytes(ts_bytes)
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
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
    fn create_test_sst(
        path: &std::path::Path,
        entries: &[(&[u8], &[u8])],
    ) -> crate::error::Result<SstReaderRef> {
        let mut builder = SstBuilder::new(path, SstBuilderOptions::default())?;
        for (key, value) in entries {
            builder.add(key, value)?;
        }
        builder.finish(1, 0)?;
        SstReaderRef::open(path)
    }

    // Helper to create an SST with sequential keys
    fn create_sequential_sst(
        path: &std::path::Path,
        start: usize,
        count: usize,
    ) -> crate::error::Result<SstReaderRef> {
        let mut builder = SstBuilder::new(path, SstBuilderOptions::default())?;
        for i in start..start + count {
            let key = format!("key_{i:05}");
            let value = format!("value_{i:05}");
            builder.add(key.as_bytes(), value.as_bytes())?;
        }
        builder.finish(1, 0)?;
        SstReaderRef::open(path)
    }

    // ------------------------------------------------------------------------
    // SstIterator Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_iterator_empty_sst() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.sst");

        let builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&path).unwrap();
        let iter = SstIterator::new(reader).unwrap();

        assert!(!iter.valid());
    }

    #[test]
    fn test_iterator_single_entry() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("single.sst");

        let key = b"key".to_vec();
        let value = b"value".to_vec();
        let reader = create_test_sst(&path, &[(&key, &value)]).unwrap();

        let iter = SstIterator::new(reader).unwrap();

        assert!(iter.valid());
        assert_eq!(iter.key(), key.as_slice());
        assert_eq!(iter.value(), value.as_slice());
    }

    #[test]
    fn test_iterator_multiple_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multi.sst");

        let reader = create_sequential_sst(&path, 0, 10).unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        let mut count = 0;
        while iter.valid() {
            let expected_key = format!("key_{count:05}");
            let expected_value = format!("value_{count:05}");
            assert_eq!(iter.key(), expected_key.as_bytes());
            assert_eq!(iter.value(), expected_value.as_bytes());
            iter.advance().unwrap();
            count += 1;
        }

        assert_eq!(count, 10);
    }

    #[test]
    fn test_iterator_seek_to_first() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seek_first.sst");

        let reader = create_sequential_sst(&path, 0, 10).unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        // Move forward
        iter.advance().unwrap();
        iter.advance().unwrap();
        assert_eq!(iter.key(), b"key_00002".as_slice());

        // Seek to first
        iter.seek_to_first().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00000".as_slice());
    }

    #[test]
    fn test_iterator_seek_to_last() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seek_last.sst");

        let reader = create_sequential_sst(&path, 0, 10).unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        iter.seek_to_last().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00009".as_slice());
    }

    #[test]
    fn test_iterator_seek() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seek.sst");

        let reader = create_sequential_sst(&path, 0, 10).unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        // Seek to existing key
        iter.seek(b"key_00005").unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00005".as_slice());

        // Seek to non-existing key (between 5 and 6)
        iter.seek(b"key_00005z").unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00006".as_slice());

        // Seek to key before all
        iter.seek(b"aaa").unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00000".as_slice());

        // Seek to key after all
        iter.seek(b"zzz").unwrap();
        assert!(!iter.valid());
    }

    #[test]
    fn test_iterator_prev() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("prev.sst");

        let reader = create_sequential_sst(&path, 0, 10).unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        // Go to last and iterate backwards
        iter.seek_to_last().unwrap();

        let mut count = 9;
        while iter.valid() {
            let expected_key = format!("key_{count:05}");
            assert_eq!(iter.key(), expected_key.as_bytes());
            iter.prev().unwrap();
            if count == 0 {
                break;
            }
            count -= 1;
        }

        assert_eq!(count, 0);
    }

    #[test]
    fn test_iterator_multiple_blocks() {
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

        let reader = SstReaderRef::open(&path).unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        // Iterate forward through all blocks
        let mut count = 0;
        while iter.valid() {
            let expected_key = format!("key_{count:05}");
            assert_eq!(iter.key(), expected_key.as_bytes());
            iter.advance().unwrap();
            count += 1;
        }
        assert_eq!(count, 100);

        // Iterate backward
        iter.seek_to_last().unwrap();
        count = 99;
        while iter.valid() {
            let expected_key = format!("key_{count:05}");
            assert_eq!(iter.key(), expected_key.as_bytes());
            iter.prev().unwrap();
            if count == 0 {
                break;
            }
            count -= 1;
        }
    }

    #[test]
    fn test_iterator_seek_multiblock() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seek_multiblock.sst");

        let options = SstBuilderOptions::with_block_size(128);
        let mut builder = SstBuilder::new(&path, options).unwrap();

        for i in 0..100 {
            let key = format!("key_{i:05}");
            let value = format!("value_{i:05}");
            builder.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&path).unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        // Seek to various keys across blocks
        for target in [0, 25, 50, 75, 99] {
            let key = format!("key_{target:05}");
            iter.seek(key.as_bytes()).unwrap();
            assert!(iter.valid(), "Failed for target {target}");
            assert_eq!(iter.key(), key.as_bytes());
        }
    }

    #[test]
    fn test_iterator_mvcc_keys() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("mvcc.sst");

        // Create entries with MVCC timestamps (higher ts = smaller encoded key due to !ts)
        let entries = vec![
            (mvcc_key(b"k1", 200), b"v1_200".to_vec()),
            (mvcc_key(b"k1", 100), b"v1_100".to_vec()),
            (mvcc_key(b"k2", 150), b"v2_150".to_vec()),
        ];

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        for (key, value) in &entries {
            builder.add(key, value).unwrap();
        }
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&path).unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        // Verify order
        assert!(iter.valid());
        assert_eq!(iter.key(), entries[0].0.as_slice());
        iter.advance().unwrap();
        assert_eq!(iter.key(), entries[1].0.as_slice());
        iter.advance().unwrap();
        assert_eq!(iter.key(), entries[2].0.as_slice());
        iter.advance().unwrap();
        assert!(!iter.valid());
    }

    // ------------------------------------------------------------------------
    // ConcatIterator Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_concat_iterator_empty() {
        let iter = ConcatIterator::new(vec![]).unwrap();
        assert!(!iter.valid());
    }

    #[test]
    fn test_concat_iterator_single_sst() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("concat_single.sst");

        let reader = create_sequential_sst(&path, 0, 10).unwrap();
        let mut iter = ConcatIterator::new(vec![reader]).unwrap();

        let mut count = 0;
        while iter.valid() {
            let expected_key = format!("key_{count:05}");
            assert_eq!(iter.key(), expected_key.as_bytes());
            iter.advance().unwrap();
            count += 1;
        }
        assert_eq!(count, 10);
    }

    #[test]
    fn test_concat_iterator_multiple_ssts() {
        let dir = tempdir().unwrap();

        // Create three non-overlapping SSTs
        let reader1 = create_sequential_sst(&dir.path().join("sst1.sst"), 0, 10).unwrap();
        let reader2 = create_sequential_sst(&dir.path().join("sst2.sst"), 10, 10).unwrap();
        let reader3 = create_sequential_sst(&dir.path().join("sst3.sst"), 20, 10).unwrap();

        let mut iter = ConcatIterator::new(vec![reader1, reader2, reader3]).unwrap();

        // Should iterate through all 30 entries in order
        let mut count = 0;
        while iter.valid() {
            let expected_key = format!("key_{count:05}");
            assert_eq!(iter.key(), expected_key.as_bytes());
            iter.advance().unwrap();
            count += 1;
        }
        assert_eq!(count, 30);
    }

    #[test]
    fn test_concat_iterator_seek() {
        let dir = tempdir().unwrap();

        let reader1 = create_sequential_sst(&dir.path().join("sst1.sst"), 0, 10).unwrap();
        let reader2 = create_sequential_sst(&dir.path().join("sst2.sst"), 10, 10).unwrap();

        let mut iter = ConcatIterator::new(vec![reader1, reader2]).unwrap();

        // Seek to key in first SST
        iter.seek(b"key_00005").unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00005".as_slice());

        // Seek to key in second SST
        iter.seek(b"key_00015").unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00015".as_slice());
    }

    #[test]
    fn test_concat_iterator_with_empty_sst() {
        let dir = tempdir().unwrap();

        // Create an empty SST
        let empty_path = dir.path().join("empty.sst");
        let builder = SstBuilder::new(&empty_path, SstBuilderOptions::default()).unwrap();
        builder.finish(1, 0).unwrap();
        let empty_reader = SstReaderRef::open(&empty_path).unwrap();

        // Create a non-empty SST
        let non_empty_reader =
            create_sequential_sst(&dir.path().join("nonempty.sst"), 0, 5).unwrap();

        // Empty first, then non-empty
        let iter =
            ConcatIterator::new(vec![empty_reader.clone(), non_empty_reader.clone()]).unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00000".as_slice());

        // Non-empty first, then empty
        let mut iter = ConcatIterator::new(vec![non_empty_reader, empty_reader]).unwrap();
        let mut count = 0;
        while iter.valid() {
            count += 1;
            iter.advance().unwrap();
        }
        assert_eq!(count, 5);
    }
}
