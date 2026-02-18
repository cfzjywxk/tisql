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
//! ## Iterator Convention
//!
//! All iterators follow the "construct-then-position" pattern:
//! - `new()` creates an unpositioned iterator (no I/O)
//! - Call `seek()`, `seek_to_first()`, or `advance()` to position
//! - `valid()` returns false until positioned
//!
//! ## Usage
//!
//! ```ignore
//! // Create an iterator (not positioned)
//! let mut iter = SstIterator::new(reader)?;
//!
//! // Position at first entry
//! iter.seek_to_first()?;
//!
//! // Iterate all entries
//! while iter.valid() {
//!     let key = iter.key();
//!     let value = iter.value();
//!     iter.advance()?;
//! }
//!
//! // Or seek to a specific key
//! iter.seek(b"target_key")?;
//! ```

use crate::catalog::types::{RawValue, Timestamp};
use crate::tablet::mvcc::{MvccIterator, MvccKey, SharedMvccRange, TIMESTAMP_SIZE};
use crate::util::error::Result;

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
    /// Create a new iterator (not positioned).
    ///
    /// The iterator is NOT positioned after construction. Call `seek()`,
    /// `seek_to_first()`, or `advance()` to position it.
    pub fn new(reader: SstReaderRef) -> Result<Self> {
        let num_blocks = reader.num_blocks()?;

        Ok(Self {
            reader,
            num_blocks,
            block_idx: 0,
            block_state: None,
        })
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
    pub async fn seek_to_first(&mut self) -> Result<()> {
        if self.num_blocks == 0 {
            self.block_state = None;
            return Ok(());
        }

        self.block_idx = 0;
        self.load_block().await?;

        if let Some(ref mut state) = self.block_state {
            state.seek_to_first();
        }

        Ok(())
    }

    /// Seek to the last entry in the SST.
    pub async fn seek_to_last(&mut self) -> Result<()> {
        if self.num_blocks == 0 {
            self.block_state = None;
            return Ok(());
        }

        self.block_idx = self.num_blocks as usize - 1;
        self.load_block().await?;

        if let Some(ref mut state) = self.block_state {
            state.seek_to_last();
        }

        Ok(())
    }

    /// Seek to the first entry >= target key.
    ///
    /// If no such entry exists, the iterator becomes invalid.
    pub async fn seek(&mut self, target: &[u8]) -> Result<()> {
        if self.num_blocks == 0 {
            self.block_state = None;
            return Ok(());
        }

        // Find the block that may contain the target key
        let block_idx = self.reader.find_block(target)?;
        self.block_idx = block_idx;
        self.load_block().await?;

        if let Some(ref mut state) = self.block_state {
            state.seek(target);

            // If the current block doesn't have a key >= target,
            // try the next block
            if !state.valid() {
                self.next_block().await?;
            }
        }

        Ok(())
    }

    /// Seek to the first entry > target key (seek for prev).
    pub async fn seek_for_prev(&mut self, target: &[u8]) -> Result<()> {
        // First seek to target
        self.seek(target).await?;

        if !self.valid() {
            // No key >= target, seek to last
            self.seek_to_last().await?;
        } else if self.key() != target {
            // Current key > target, go to prev
            self.prev().await?;
        }

        Ok(())
    }

    /// Move to the next entry.
    ///
    /// Note: This returns `Result` for I/O error handling, unlike `std::iter::Iterator`.
    pub async fn advance(&mut self) -> Result<()> {
        if !self.valid() {
            return Ok(());
        }

        if let Some(ref mut state) = self.block_state {
            state.next();

            if !state.valid() {
                // Current block exhausted, move to next block
                self.next_block().await?;
            }
        }

        Ok(())
    }

    /// Move to the previous entry.
    pub async fn prev(&mut self) -> Result<()> {
        if !self.valid() {
            return Ok(());
        }

        if let Some(ref mut state) = self.block_state {
            state.prev();

            if !state.valid() {
                // Current block exhausted, move to previous block
                self.prev_block().await?;
            }
        }

        Ok(())
    }

    /// Load the current block.
    async fn load_block(&mut self) -> Result<()> {
        if self.block_idx >= self.num_blocks as usize {
            self.block_state = None;
            return Ok(());
        }

        let block = self.reader.read_block(self.block_idx).await?;
        self.block_state = Some(BlockState::new(block));
        Ok(())
    }

    /// Move to the next block.
    async fn next_block(&mut self) -> Result<()> {
        self.block_idx += 1;

        if self.block_idx >= self.num_blocks as usize {
            self.block_state = None;
            return Ok(());
        }

        self.load_block().await?;

        if let Some(ref mut state) = self.block_state {
            state.seek_to_first();
        }

        Ok(())
    }

    /// Move to the previous block.
    async fn prev_block(&mut self) -> Result<()> {
        if self.block_idx == 0 {
            self.block_state = None;
            return Ok(());
        }

        self.block_idx -= 1;
        self.load_block().await?;

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
    /// Create a new concat iterator (not positioned).
    ///
    /// The iterator is NOT positioned after construction. Call `seek()`,
    /// `seek_to_first()`, or `advance()` to position it.
    ///
    /// Readers should be in sorted order (by key range).
    pub fn new(readers: Vec<SstReaderRef>) -> Result<Self> {
        Ok(Self {
            readers,
            sst_idx: 0,
            current: None,
        })
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
    pub async fn seek_to_first(&mut self) -> Result<()> {
        self.sst_idx = 0;
        self.load_current_sst().await?;

        // Skip empty SSTs
        while !self.valid() && self.sst_idx + 1 < self.readers.len() {
            self.sst_idx += 1;
            self.load_current_sst().await?;
        }

        Ok(())
    }

    /// Seek to a specific key.
    pub async fn seek(&mut self, target: &[u8]) -> Result<()> {
        // Start from first SST and seek
        self.sst_idx = 0;

        while self.sst_idx < self.readers.len() {
            self.load_current_sst().await?;

            if let Some(ref mut current) = self.current {
                current.seek(target).await?;
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
    pub async fn advance(&mut self) -> Result<()> {
        if !self.valid() {
            return Ok(());
        }

        if let Some(ref mut current) = self.current {
            current.advance().await?;
        }

        // If current SST exhausted, move to next
        while !self.valid() && self.sst_idx + 1 < self.readers.len() {
            self.sst_idx += 1;
            self.load_current_sst().await?;
        }

        Ok(())
    }

    /// Load the iterator for the current SST and position at first entry.
    async fn load_current_sst(&mut self) -> Result<()> {
        if self.sst_idx >= self.readers.len() {
            self.current = None;
            return Ok(());
        }

        let mut iter = SstIterator::new(self.readers[self.sst_idx].clone())?;
        iter.seek_to_first().await?; // Position the iterator
        self.current = Some(iter);
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
    /// Create a new iterator over the given range (not positioned).
    ///
    /// The iterator is NOT positioned after construction. Call `seek()` or
    /// `advance()` to position it. The first `advance()` will position at
    /// the start of the range.
    ///
    /// Takes `SharedMvccRange` (Arc) to avoid cloning when creating multiple iterators.
    pub fn new(reader: SstReaderRef, range: SharedMvccRange) -> Result<Self> {
        let is_unbounded = range.start.is_unbounded() && range.end.is_unbounded();
        let inner = SstIterator::new(reader)?;

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
    async fn seek(&mut self, target: &MvccKey) -> Result<()> {
        // If seeking to unbounded target, use range start instead.
        // This ensures we position within the range, not at the beginning of the SST.
        // The VersionedMemTableIterator handles this similarly in its seek() method.
        if target.is_unbounded() && !self.range.start.is_unbounded() {
            self.inner.seek(self.range.start.as_bytes()).await
        } else {
            self.inner.seek(target.as_bytes()).await
        }
    }

    async fn advance(&mut self) -> Result<()> {
        // If not yet positioned, seek to range start first
        if !self.inner.valid() {
            if self.range.start.is_unbounded() {
                self.inner.seek_to_first().await?;
            } else {
                self.inner.seek(self.range.start.as_bytes()).await?;
            }
            return Ok(());
        }
        self.inner.advance().await
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
    use crate::tablet::sstable::builder::{SstBuilder, SstBuilderOptions};
    use tempfile::tempdir;

    fn test_io() -> std::sync::Arc<crate::io::IoService> {
        crate::io::IoService::new_for_test(32).unwrap()
    }

    // Helper to create MVCC key
    fn mvcc_key(key_bytes: &[u8], ts: u64) -> Vec<u8> {
        let mut result = key_bytes.to_vec();
        result.extend_from_slice(&(!ts).to_be_bytes());
        result
    }

    // Helper to create an SST file with test data
    async fn create_test_sst(
        path: &std::path::Path,
        entries: &[(&[u8], &[u8])],
    ) -> crate::util::error::Result<SstReaderRef> {
        let mut builder = SstBuilder::new(path, SstBuilderOptions::default())?;
        for (key, value) in entries {
            builder.add(key, value)?;
        }
        builder.finish(1, 0)?;
        SstReaderRef::open(path, test_io()).await
    }

    // Helper to create an SST with sequential keys
    async fn create_sequential_sst(
        path: &std::path::Path,
        start: usize,
        count: usize,
    ) -> crate::util::error::Result<SstReaderRef> {
        let mut builder = SstBuilder::new(path, SstBuilderOptions::default())?;
        for i in start..start + count {
            let key = format!("key_{i:05}");
            let value = format!("value_{i:05}");
            builder.add(key.as_bytes(), value.as_bytes())?;
        }
        builder.finish(1, 0)?;
        SstReaderRef::open(path, test_io()).await
    }

    // ------------------------------------------------------------------------
    // SstIterator Tests
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn test_iterator_empty_sst() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.sst");

        let builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&path, test_io()).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();
        iter.seek_to_first().await.unwrap(); // Position the iterator

        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_iterator_single_entry() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("single.sst");

        let key = b"key".to_vec();
        let value = b"value".to_vec();
        let reader = create_test_sst(&path, &[(&key, &value)]).await.unwrap();

        let mut iter = SstIterator::new(reader).unwrap();
        iter.seek_to_first().await.unwrap(); // Position the iterator

        assert!(iter.valid());
        assert_eq!(iter.key(), key.as_slice());
        assert_eq!(iter.value(), value.as_slice());
    }

    #[tokio::test]
    async fn test_iterator_multiple_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multi.sst");

        let reader = create_sequential_sst(&path, 0, 10).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();
        iter.seek_to_first().await.unwrap(); // Position the iterator

        let mut count = 0;
        while iter.valid() {
            let expected_key = format!("key_{count:05}");
            let expected_value = format!("value_{count:05}");
            assert_eq!(iter.key(), expected_key.as_bytes());
            assert_eq!(iter.value(), expected_value.as_bytes());
            iter.advance().await.unwrap();
            count += 1;
        }

        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn test_iterator_seek_to_first() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seek_first.sst");

        let reader = create_sequential_sst(&path, 0, 10).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();
        iter.seek_to_first().await.unwrap(); // Position the iterator

        // Move forward
        iter.advance().await.unwrap();
        iter.advance().await.unwrap();
        assert_eq!(iter.key(), b"key_00002".as_slice());

        // Seek to first
        iter.seek_to_first().await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00000".as_slice());
    }

    #[tokio::test]
    async fn test_iterator_seek_to_last() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seek_last.sst");

        let reader = create_sequential_sst(&path, 0, 10).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();
        // No need to seek_to_first - we're testing seek_to_last

        iter.seek_to_last().await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00009".as_slice());
    }

    #[tokio::test]
    async fn test_iterator_seek() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seek.sst");

        let reader = create_sequential_sst(&path, 0, 10).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();
        // No need to seek_to_first - we're testing seek

        // Seek to existing key
        iter.seek(b"key_00005").await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00005".as_slice());

        // Seek to non-existing key (between 5 and 6)
        iter.seek(b"key_00005z").await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00006".as_slice());

        // Seek to key before all
        iter.seek(b"aaa").await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00000".as_slice());

        // Seek to key after all
        iter.seek(b"zzz").await.unwrap();
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_iterator_prev() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("prev.sst");

        let reader = create_sequential_sst(&path, 0, 10).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        // Go to last and iterate backwards
        iter.seek_to_last().await.unwrap();

        let mut count = 9;
        while iter.valid() {
            let expected_key = format!("key_{count:05}");
            assert_eq!(iter.key(), expected_key.as_bytes());
            iter.prev().await.unwrap();
            if count == 0 {
                break;
            }
            count -= 1;
        }

        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_iterator_multiple_blocks() {
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

        let reader = SstReaderRef::open(&path, test_io()).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();
        iter.seek_to_first().await.unwrap(); // Position the iterator

        // Iterate forward through all blocks
        let mut count = 0;
        while iter.valid() {
            let expected_key = format!("key_{count:05}");
            assert_eq!(iter.key(), expected_key.as_bytes());
            iter.advance().await.unwrap();
            count += 1;
        }
        assert_eq!(count, 100);

        // Iterate backward
        iter.seek_to_last().await.unwrap();
        count = 99;
        while iter.valid() {
            let expected_key = format!("key_{count:05}");
            assert_eq!(iter.key(), expected_key.as_bytes());
            iter.prev().await.unwrap();
            if count == 0 {
                break;
            }
            count -= 1;
        }
    }

    #[tokio::test]
    async fn test_iterator_seek_multiblock() {
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

        let reader = SstReaderRef::open(&path, test_io()).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        // Seek to various keys across blocks
        for target in [0, 25, 50, 75, 99] {
            let key = format!("key_{target:05}");
            iter.seek(key.as_bytes()).await.unwrap();
            assert!(iter.valid(), "Failed for target {target}");
            assert_eq!(iter.key(), key.as_bytes());
        }
    }

    #[tokio::test]
    async fn test_iterator_mvcc_keys() {
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

        let reader = SstReaderRef::open(&path, test_io()).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();
        iter.seek_to_first().await.unwrap(); // Position the iterator

        // Verify order
        assert!(iter.valid());
        assert_eq!(iter.key(), entries[0].0.as_slice());
        iter.advance().await.unwrap();
        assert_eq!(iter.key(), entries[1].0.as_slice());
        iter.advance().await.unwrap();
        assert_eq!(iter.key(), entries[2].0.as_slice());
        iter.advance().await.unwrap();
        assert!(!iter.valid());
    }

    // ------------------------------------------------------------------------
    // ConcatIterator Tests
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn test_concat_iterator_empty() {
        let mut iter = ConcatIterator::new(vec![]).unwrap();
        iter.seek_to_first().await.unwrap(); // Position the iterator
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_concat_iterator_single_sst() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("concat_single.sst");

        let reader = create_sequential_sst(&path, 0, 10).await.unwrap();
        let mut iter = ConcatIterator::new(vec![reader]).unwrap();
        iter.seek_to_first().await.unwrap(); // Position the iterator

        let mut count = 0;
        while iter.valid() {
            let expected_key = format!("key_{count:05}");
            assert_eq!(iter.key(), expected_key.as_bytes());
            iter.advance().await.unwrap();
            count += 1;
        }
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn test_concat_iterator_multiple_ssts() {
        let dir = tempdir().unwrap();

        // Create three non-overlapping SSTs
        let reader1 = create_sequential_sst(&dir.path().join("sst1.sst"), 0, 10)
            .await
            .unwrap();
        let reader2 = create_sequential_sst(&dir.path().join("sst2.sst"), 10, 10)
            .await
            .unwrap();
        let reader3 = create_sequential_sst(&dir.path().join("sst3.sst"), 20, 10)
            .await
            .unwrap();

        let mut iter = ConcatIterator::new(vec![reader1, reader2, reader3]).unwrap();
        iter.seek_to_first().await.unwrap(); // Position the iterator

        // Should iterate through all 30 entries in order
        let mut count = 0;
        while iter.valid() {
            let expected_key = format!("key_{count:05}");
            assert_eq!(iter.key(), expected_key.as_bytes());
            iter.advance().await.unwrap();
            count += 1;
        }
        assert_eq!(count, 30);
    }

    #[tokio::test]
    async fn test_concat_iterator_seek() {
        let dir = tempdir().unwrap();

        let reader1 = create_sequential_sst(&dir.path().join("sst1.sst"), 0, 10)
            .await
            .unwrap();
        let reader2 = create_sequential_sst(&dir.path().join("sst2.sst"), 10, 10)
            .await
            .unwrap();

        let mut iter = ConcatIterator::new(vec![reader1, reader2]).unwrap();
        // No need to seek_to_first - we're testing seek

        // Seek to key in first SST
        iter.seek(b"key_00005").await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00005".as_slice());

        // Seek to key in second SST
        iter.seek(b"key_00015").await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00015".as_slice());
    }

    #[tokio::test]
    async fn test_concat_iterator_with_empty_sst() {
        let dir = tempdir().unwrap();

        // Create an empty SST
        let empty_path = dir.path().join("empty.sst");
        let builder = SstBuilder::new(&empty_path, SstBuilderOptions::default()).unwrap();
        builder.finish(1, 0).unwrap();
        let empty_reader = SstReaderRef::open(&empty_path, test_io()).await.unwrap();

        // Create a non-empty SST
        let non_empty_reader = create_sequential_sst(&dir.path().join("nonempty.sst"), 0, 5)
            .await
            .unwrap();

        // Empty first, then non-empty
        let mut iter =
            ConcatIterator::new(vec![empty_reader.clone(), non_empty_reader.clone()]).unwrap();
        iter.seek_to_first().await.unwrap(); // Position the iterator
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00000".as_slice());

        // Non-empty first, then empty
        let mut iter = ConcatIterator::new(vec![non_empty_reader, empty_reader]).unwrap();
        iter.seek_to_first().await.unwrap(); // Position the iterator
        let mut count = 0;
        while iter.valid() {
            count += 1;
            iter.advance().await.unwrap();
        }
        assert_eq!(count, 5);
    }

    // ------------------------------------------------------------------------
    // SstIterator Additional Coverage Tests
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn test_iterator_not_positioned_after_new() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let reader = create_sequential_sst(&path, 0, 5).await.unwrap();
        let iter = SstIterator::new(reader).unwrap();

        // Iterator should not be valid until positioned
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_iterator_advance_when_not_valid() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let reader = create_sequential_sst(&path, 0, 3).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        // Advance on invalid iterator should be no-op
        iter.advance().await.unwrap();
        assert!(!iter.valid());

        // Position and iterate to end
        iter.seek_to_first().await.unwrap();
        while iter.valid() {
            iter.advance().await.unwrap();
        }

        // Advance again should still be no-op
        iter.advance().await.unwrap();
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_iterator_prev_when_not_valid() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let reader = create_sequential_sst(&path, 0, 3).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        // Prev on invalid iterator should be no-op
        iter.prev().await.unwrap();
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_iterator_seek_for_prev_exact_match() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let reader = create_sequential_sst(&path, 0, 10).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        // Seek for prev with exact match should stay on that key
        iter.seek_for_prev(b"key_00005").await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00005".as_slice());
    }

    #[tokio::test]
    async fn test_iterator_seek_for_prev_between_keys() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let reader = create_sequential_sst(&path, 0, 10).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        // Seek for prev between keys should go to previous key
        iter.seek_for_prev(b"key_00005z").await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00005".as_slice());
    }

    #[tokio::test]
    async fn test_iterator_seek_for_prev_before_first() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let reader = create_sequential_sst(&path, 0, 10).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        // Seek for prev before first key should become invalid
        iter.seek_for_prev(b"aaa").await.unwrap();
        // The current implementation seeks to the target first, which finds key_00000
        // Since key_00000 > "aaa", it goes to prev which makes it invalid
        // Actually, seek finds key_00000, and since key_00000 != "aaa" and key_00000 > "aaa",
        // it tries to go prev, which makes it invalid
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_iterator_seek_for_prev_after_last() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let reader = create_sequential_sst(&path, 0, 10).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        // Seek for prev after last key should go to last key
        iter.seek_for_prev(b"zzz").await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00009".as_slice());
    }

    #[tokio::test]
    async fn test_iterator_reseek_while_iterating() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let reader = create_sequential_sst(&path, 0, 10).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        // Position and advance a few times
        iter.seek_to_first().await.unwrap();
        iter.advance().await.unwrap();
        iter.advance().await.unwrap();
        assert_eq!(iter.key(), b"key_00002".as_slice());

        // Reseek to a different position
        iter.seek(b"key_00007").await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00007".as_slice());

        // Continue iterating
        iter.advance().await.unwrap();
        assert_eq!(iter.key(), b"key_00008".as_slice());
    }

    #[tokio::test]
    async fn test_iterator_prev_across_blocks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        // Use small block size to force multiple blocks
        let options = SstBuilderOptions::with_block_size(64);
        let mut builder = SstBuilder::new(&path, options).unwrap();
        for i in 0..50 {
            let key = format!("key_{i:05}");
            let value = format!("value_{i:05}");
            builder.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&path, test_io()).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        // Seek to middle
        iter.seek(b"key_00025").await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00025".as_slice());

        // Iterate backwards
        for expected in (0..25).rev() {
            iter.prev().await.unwrap();
            let expected_key = format!("key_{expected:05}");
            assert!(iter.valid(), "Should be valid at key {expected}");
            assert_eq!(iter.key(), expected_key.as_bytes());
        }

        // One more prev should become invalid
        iter.prev().await.unwrap();
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_iterator_seek_to_first_and_last_empty_sst() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.sst");

        let builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&path, test_io()).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        iter.seek_to_first().await.unwrap();
        assert!(!iter.valid());

        iter.seek_to_last().await.unwrap();
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_iterator_single_entry_prev() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("single.sst");

        let reader = create_test_sst(&path, &[(b"key", b"value")]).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        iter.seek_to_last().await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key".as_slice());

        // Prev should become invalid
        iter.prev().await.unwrap();
        assert!(!iter.valid());
    }

    // ------------------------------------------------------------------------
    // ConcatIterator Additional Coverage Tests
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn test_concat_iterator_seek_to_gap() {
        let dir = tempdir().unwrap();

        // Create SSTs with gaps: key_00000-00009 and key_00020-00029
        let reader1 = create_sequential_sst(&dir.path().join("sst1.sst"), 0, 10)
            .await
            .unwrap();
        let reader2 = create_sequential_sst(&dir.path().join("sst2.sst"), 20, 10)
            .await
            .unwrap();

        let mut iter = ConcatIterator::new(vec![reader1, reader2]).unwrap();

        // Seek to key in gap should find next key
        iter.seek(b"key_00015").await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key_00020".as_slice());
    }

    #[tokio::test]
    async fn test_concat_iterator_seek_past_all() {
        let dir = tempdir().unwrap();

        let reader = create_sequential_sst(&dir.path().join("sst.sst"), 0, 10)
            .await
            .unwrap();
        let mut iter = ConcatIterator::new(vec![reader]).unwrap();

        iter.seek(b"zzz").await.unwrap();
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_concat_iterator_advance_exhausts_correctly() {
        let dir = tempdir().unwrap();

        let reader1 = create_sequential_sst(&dir.path().join("sst1.sst"), 0, 3)
            .await
            .unwrap();
        let reader2 = create_sequential_sst(&dir.path().join("sst2.sst"), 3, 3)
            .await
            .unwrap();

        let mut iter = ConcatIterator::new(vec![reader1, reader2]).unwrap();
        iter.seek_to_first().await.unwrap();

        // Collect all keys
        let mut keys = Vec::new();
        while iter.valid() {
            keys.push(iter.key().to_vec());
            iter.advance().await.unwrap();
        }

        assert_eq!(keys.len(), 6);
        assert_eq!(keys[0], b"key_00000".as_slice());
        assert_eq!(keys[2], b"key_00002".as_slice());
        assert_eq!(keys[3], b"key_00003".as_slice());
        assert_eq!(keys[5], b"key_00005".as_slice());
    }

    #[tokio::test]
    async fn test_concat_iterator_advance_when_invalid() {
        let dir = tempdir().unwrap();

        let reader = create_sequential_sst(&dir.path().join("sst.sst"), 0, 3)
            .await
            .unwrap();
        let mut iter = ConcatIterator::new(vec![reader]).unwrap();

        // Not positioned yet
        assert!(!iter.valid());

        // Advance should be no-op
        iter.advance().await.unwrap();
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_concat_iterator_multiple_empty_ssts() {
        let dir = tempdir().unwrap();

        // Create multiple empty SSTs with one non-empty in the middle
        let empty1_path = dir.path().join("empty1.sst");
        let empty2_path = dir.path().join("empty2.sst");
        let empty3_path = dir.path().join("empty3.sst");

        for path in [&empty1_path, &empty2_path, &empty3_path] {
            let builder = SstBuilder::new(path, SstBuilderOptions::default()).unwrap();
            builder.finish(1, 0).unwrap();
        }

        let io = test_io();
        let empty1 = SstReaderRef::open(&empty1_path, io.clone()).await.unwrap();
        let empty2 = SstReaderRef::open(&empty2_path, io.clone()).await.unwrap();
        let empty3 = SstReaderRef::open(&empty3_path, io).await.unwrap();
        let non_empty = create_sequential_sst(&dir.path().join("nonempty.sst"), 0, 3)
            .await
            .unwrap();

        // [empty, empty, non_empty, empty]
        let mut iter = ConcatIterator::new(vec![empty1, empty2, non_empty, empty3]).unwrap();
        iter.seek_to_first().await.unwrap();

        let mut count = 0;
        while iter.valid() {
            count += 1;
            iter.advance().await.unwrap();
        }
        assert_eq!(count, 3);
    }

    // ------------------------------------------------------------------------
    // SstMvccIterator Tests
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn test_sst_mvcc_iterator_basic() {
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let path = dir.path().join("mvcc.sst");

        // Create SST with MVCC keys
        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.add(&mvcc_key(b"key1", 100), b"v1_100").unwrap();
        builder.add(&mvcc_key(b"key1", 50), b"v1_50").unwrap();
        builder.add(&mvcc_key(b"key2", 200), b"v2_200").unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&path, test_io()).await.unwrap();
        let range = Arc::new(MvccKey::unbounded()..MvccKey::unbounded());
        let mut iter = SstMvccIterator::new(reader, range).unwrap();

        // Not positioned yet
        assert!(!iter.valid());

        // Position at start
        iter.advance().await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"key1");
        assert_eq!(iter.timestamp(), 100);
        assert_eq!(iter.value(), b"v1_100");

        iter.advance().await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"key1");
        assert_eq!(iter.timestamp(), 50);

        iter.advance().await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"key2");
        assert_eq!(iter.timestamp(), 200);

        iter.advance().await.unwrap();
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_sst_mvcc_iterator_with_range_start() {
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let path = dir.path().join("mvcc.sst");

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.add(&mvcc_key(b"a", 100), b"va").unwrap();
        builder.add(&mvcc_key(b"b", 100), b"vb").unwrap();
        builder.add(&mvcc_key(b"c", 100), b"vc").unwrap();
        builder.add(&mvcc_key(b"d", 100), b"vd").unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&path, test_io()).await.unwrap();
        // Range starting from "b"
        let range = Arc::new(MvccKey::encode(b"b", u64::MAX)..MvccKey::unbounded());
        let mut iter = SstMvccIterator::new(reader, range).unwrap();

        iter.advance().await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"b");

        iter.advance().await.unwrap();
        assert_eq!(iter.user_key(), b"c");

        iter.advance().await.unwrap();
        assert_eq!(iter.user_key(), b"d");

        iter.advance().await.unwrap();
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_sst_mvcc_iterator_with_range_end() {
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let path = dir.path().join("mvcc.sst");

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.add(&mvcc_key(b"a", 100), b"va").unwrap();
        builder.add(&mvcc_key(b"b", 100), b"vb").unwrap();
        builder.add(&mvcc_key(b"c", 100), b"vc").unwrap();
        builder.add(&mvcc_key(b"d", 100), b"vd").unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&path, test_io()).await.unwrap();
        // Range ending before "c" (exclusive)
        let range = Arc::new(MvccKey::unbounded()..MvccKey::encode(b"c", u64::MAX));
        let mut iter = SstMvccIterator::new(reader, range).unwrap();

        iter.advance().await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"a");

        iter.advance().await.unwrap();
        assert_eq!(iter.user_key(), b"b");

        // "c" should be excluded
        iter.advance().await.unwrap();
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_sst_mvcc_iterator_with_range_both_bounds() {
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let path = dir.path().join("mvcc.sst");

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.add(&mvcc_key(b"a", 100), b"va").unwrap();
        builder.add(&mvcc_key(b"b", 100), b"vb").unwrap();
        builder.add(&mvcc_key(b"c", 100), b"vc").unwrap();
        builder.add(&mvcc_key(b"d", 100), b"vd").unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&path, test_io()).await.unwrap();
        // Range [b, d) - should include b, c but not d
        let range = Arc::new(MvccKey::encode(b"b", u64::MAX)..MvccKey::encode(b"d", u64::MAX));
        let mut iter = SstMvccIterator::new(reader, range).unwrap();

        iter.advance().await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"b");

        iter.advance().await.unwrap();
        assert_eq!(iter.user_key(), b"c");

        iter.advance().await.unwrap();
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_sst_mvcc_iterator_seek() {
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let path = dir.path().join("mvcc.sst");

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.add(&mvcc_key(b"a", 100), b"va").unwrap();
        builder.add(&mvcc_key(b"b", 100), b"vb").unwrap();
        builder.add(&mvcc_key(b"c", 100), b"vc").unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&path, test_io()).await.unwrap();
        let range = Arc::new(MvccKey::unbounded()..MvccKey::unbounded());
        let mut iter = SstMvccIterator::new(reader, range).unwrap();

        // Seek to specific key
        let target = MvccKey::encode(b"b", 100);
        iter.seek(&target).await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"b");
        assert_eq!(iter.timestamp(), 100);
    }

    #[tokio::test]
    async fn test_sst_mvcc_iterator_seek_unbounded_target_with_range() {
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let path = dir.path().join("mvcc.sst");

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.add(&mvcc_key(b"a", 100), b"va").unwrap();
        builder.add(&mvcc_key(b"b", 100), b"vb").unwrap();
        builder.add(&mvcc_key(b"c", 100), b"vc").unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&path, test_io()).await.unwrap();
        // Range starting from "b"
        let range = Arc::new(MvccKey::encode(b"b", u64::MAX)..MvccKey::unbounded());
        let mut iter = SstMvccIterator::new(reader, range).unwrap();

        // Seek with unbounded target should use range start instead
        iter.seek(&MvccKey::unbounded()).await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"b");
    }

    #[tokio::test]
    async fn test_sst_mvcc_iterator_multiple_versions_same_key() {
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let path = dir.path().join("mvcc.sst");

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        // Multiple versions of same key (higher ts = smaller encoded key)
        builder.add(&mvcc_key(b"key", 100), b"v100").unwrap();
        builder.add(&mvcc_key(b"key", 50), b"v50").unwrap();
        builder.add(&mvcc_key(b"key", 25), b"v25").unwrap();
        builder.add(&mvcc_key(b"key", 10), b"v10").unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&path, test_io()).await.unwrap();
        let range = Arc::new(MvccKey::unbounded()..MvccKey::unbounded());
        let mut iter = SstMvccIterator::new(reader, range).unwrap();

        iter.advance().await.unwrap();
        assert_eq!(iter.timestamp(), 100);
        iter.advance().await.unwrap();
        assert_eq!(iter.timestamp(), 50);
        iter.advance().await.unwrap();
        assert_eq!(iter.timestamp(), 25);
        iter.advance().await.unwrap();
        assert_eq!(iter.timestamp(), 10);
        iter.advance().await.unwrap();
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_sst_mvcc_iterator_invalid_mvcc_key_too_short() {
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let path = dir.path().join("invalid.sst");

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        // Add a key that's too short to be a valid MVCC key (< 8 bytes)
        builder.add(b"short", b"value").unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&path, test_io()).await.unwrap();
        let range = Arc::new(MvccKey::unbounded()..MvccKey::unbounded());
        let mut iter = SstMvccIterator::new(reader, range).unwrap();

        // Advance should position but the entry should be filtered out
        iter.advance().await.unwrap();
        // The iterator should not be valid because the key is too short
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_sst_mvcc_iterator_empty_sst() {
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.sst");

        let builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&path, test_io()).await.unwrap();
        let range = Arc::new(MvccKey::unbounded()..MvccKey::unbounded());
        let mut iter = SstMvccIterator::new(reader, range).unwrap();

        iter.advance().await.unwrap();
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_sst_mvcc_iterator_range_filters_all() {
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let path = dir.path().join("mvcc.sst");

        let mut builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.add(&mvcc_key(b"a", 100), b"va").unwrap();
        builder.add(&mvcc_key(b"b", 100), b"vb").unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&path, test_io()).await.unwrap();
        // Range that doesn't overlap with any data
        let range = Arc::new(MvccKey::encode(b"x", u64::MAX)..MvccKey::encode(b"z", u64::MAX));
        let mut iter = SstMvccIterator::new(reader, range).unwrap();

        iter.advance().await.unwrap();
        assert!(!iter.valid());
    }

    // ------------------------------------------------------------------------
    // BlockState Tests (internal)
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn test_block_seek_empty_block() {
        // This tests the edge case where a block has no entries
        // In practice, SST builder would not create such a block,
        // but we test the BlockState behavior directly
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.sst");

        let builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.finish(1, 0).unwrap();

        let reader = SstReaderRef::open(&path, test_io()).await.unwrap();
        let mut iter = SstIterator::new(reader).unwrap();

        // Seek should handle empty SST gracefully
        iter.seek(b"any_key").await.unwrap();
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_iterator_seek_to_exact_boundary() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        // Create SST with specific keys
        let reader = create_test_sst(
            &path,
            &[
                (b"aaa", b"v1"),
                (b"bbb", b"v2"),
                (b"ccc", b"v3"),
                (b"ddd", b"v4"),
            ],
        )
        .await
        .unwrap();

        let mut iter = SstIterator::new(reader).unwrap();

        // Seek to exact first key
        iter.seek(b"aaa").await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"aaa".as_slice());

        // Seek to exact last key
        iter.seek(b"ddd").await.unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"ddd".as_slice());
    }
}
