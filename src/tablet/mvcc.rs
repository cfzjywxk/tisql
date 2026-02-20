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

//! MVCC key encoding for storage layer.
//!
//! All internal storage operations use MvccKey format. Keys are only decoded
//! at API boundaries (returning results to caller).
//!
//! ## Key Terminology
//!
//! | Term | Format | Description |
//! |------|--------|-------------|
//! | `Key` | Table-encoded bytes | `'t' + tableID + "_r" + pk_bytes` |
//! | `MvccKey` | `Key || !commit_ts` | Key with 8-byte timestamp suffix |
//!
//! ## MVCC Key Encoding
//!
//! MVCC keys are encoded as: `key || !commit_ts` (8 bytes, big-endian)
//!
//! The bitwise NOT of commit_ts ensures that:
//! - Keys are sorted in descending timestamp order
//! - Scanning from a key finds the latest visible version first
//!
//! ## Tombstones
//!
//! Deletes are represented as tombstone markers. This allows MVCC to correctly
//! handle deleted keys at specific versions.

use std::cmp::Ordering;
use std::ops::{Deref, Range};
use std::sync::Arc;

use crate::catalog::types::{Key, Timestamp};

/// Size of the timestamp suffix in MVCC keys (8 bytes).
pub const TIMESTAMP_SIZE: usize = 8;

/// Shared MVCC range for zero-allocation iterator construction.
///
/// By wrapping the range in `Arc`, multiple iterators (memtable, SST, level)
/// can share the same range data. `Arc::clone()` is just a refcount increment,
/// avoiding repeated `MvccKey` allocations during `scan_iter()`.
pub type SharedMvccRange = Arc<Range<MvccKey>>;

// ============================================================================
// MvccKey Newtype
// ============================================================================

/// An MVCC-encoded key: `key || !commit_ts`.
///
/// This newtype provides type safety by distinguishing MVCC keys from plain keys.
/// It wraps a `Vec<u8>` containing the encoded key with timestamp suffix.
///
/// ## Invariants
///
/// A valid `MvccKey` has at least `TIMESTAMP_SIZE` (8) bytes. The last 8 bytes
/// contain the bitwise-NOT of the commit timestamp in big-endian format.
///
/// ## Usage
///
/// ```ignore
/// use tisql::tablet::mvcc::MvccKey;
///
/// // Create from key and timestamp
/// let mvcc_key = MvccKey::encode(b"my_key", 100);
///
/// // Access components
/// assert_eq!(mvcc_key.key(), b"my_key");
/// assert_eq!(mvcc_key.timestamp(), 100);
///
/// // Use as bytes
/// storage.put(mvcc_key.as_bytes(), value);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MvccKey(Vec<u8>);

impl MvccKey {
    /// Create an MVCC key from raw bytes.
    ///
    /// Returns `None` if the bytes are too short to be a valid MVCC key
    /// (less than `TIMESTAMP_SIZE` bytes).
    ///
    /// # Arguments
    ///
    /// * `bytes` - The raw MVCC-encoded key bytes
    #[inline]
    pub fn from_bytes(bytes: Vec<u8>) -> Option<Self> {
        if bytes.len() < TIMESTAMP_SIZE {
            None
        } else {
            Some(Self(bytes))
        }
    }

    /// Create an MVCC key from raw bytes without validation.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `bytes.len() >= TIMESTAMP_SIZE`.
    #[inline]
    pub fn from_bytes_unchecked(bytes: Vec<u8>) -> Self {
        debug_assert!(bytes.len() >= TIMESTAMP_SIZE);
        Self(bytes)
    }

    /// Encode a key with timestamp to create an MVCC key.
    ///
    /// Format: `key || !commit_ts` (descending order by timestamp)
    #[inline]
    pub fn encode(key: &[u8], ts: Timestamp) -> Self {
        Self(encode_mvcc_key(key, ts))
    }

    /// Get the key portion (without timestamp suffix).
    #[inline]
    pub fn key(&self) -> &[u8] {
        &self.0[..self.0.len() - TIMESTAMP_SIZE]
    }

    /// Get the commit timestamp.
    #[inline]
    pub fn timestamp(&self) -> Timestamp {
        let ts_bytes: [u8; 8] = self.0[self.0.len() - TIMESTAMP_SIZE..]
            .try_into()
            .expect("MvccKey invariant violated: insufficient bytes");
        !u64::from_be_bytes(ts_bytes)
    }

    /// Decode into (key, timestamp) tuple.
    #[inline]
    pub fn decode(&self) -> (Key, Timestamp) {
        (self.key().to_vec(), self.timestamp())
    }

    /// Get the raw bytes of this MVCC key.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consume this MVCC key and return the inner bytes.
    #[inline]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// Check if this MVCC key has the given key prefix.
    #[inline]
    pub fn has_key(&self, key: &[u8]) -> bool {
        self.key() == key
    }

    /// Get the length of the encoded MVCC key.
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the encoded MVCC key is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Check if this is an unbounded placeholder (empty bytes).
    ///
    /// Returns true only for MvccKey created via `unbounded()`.
    /// Valid encoded MVCC keys always have at least 8 bytes.
    #[inline]
    pub fn is_unbounded(&self) -> bool {
        self.0.is_empty()
    }

    /// Create an unbounded placeholder for range operations.
    ///
    /// This creates an MvccKey with empty bytes, used to represent
    /// "no bound" in Range operations. Check with `is_unbounded()`.
    #[inline]
    pub fn unbounded() -> Self {
        Self(Vec::new())
    }

    // ========================================================================
    // Static Constructors for Seek Targets
    // ========================================================================

    /// Create an MvccKey for the first (newest) version of a key.
    ///
    /// This returns `(key, MAX_TS)` which is the smallest MVCC key for
    /// the given key in memory-comparable order.
    ///
    /// Use this as a seek target to find the newest version of a key.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Seek to first version of "user:123"
    /// let seek_key = MvccKey::first_for_key(b"user:123");
    /// iterator.seek(&seek_key);
    /// ```
    #[inline]
    pub fn first_for_key(key: &[u8]) -> Self {
        Self::encode(key, Timestamp::MAX)
    }

    /// Create an MvccKey for the last (oldest) version of a key.
    ///
    /// This returns `(key, 0)` which is the largest MVCC key for
    /// the given key in memory-comparable order.
    ///
    /// Use this as an end bound when scanning all versions of a key.
    #[inline]
    pub fn last_for_key(key: &[u8]) -> Self {
        Self::encode(key, 0)
    }

    /// Get the range of all versions for this key's base key.
    ///
    /// Returns `(first_version, last_version)` which can be used as
    /// `[start, end]` bounds for scanning all versions.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mvcc_key = MvccKey::encode(b"key", 100);
    /// let (start, end) = mvcc_key.key_version_range();
    /// // start = (key, MAX_TS), end = (key, 0)
    /// ```
    #[inline]
    pub fn key_version_range(&self) -> (Self, Self) {
        let key = self.key();
        (Self::first_for_key(key), Self::last_for_key(key))
    }

    // ========================================================================
    // Version Navigation (same key, different timestamps)
    // ========================================================================

    /// Get the next older version of this key.
    ///
    /// Returns `(key, ts - 1)` in MVCC order. This is the immediately
    /// following entry in memory-comparable order for the same key.
    ///
    /// Returns `None` if `ts == 0` (no older version can exist).
    ///
    /// # MVCC Ordering
    ///
    /// ```text
    /// (key, 100) → next_version() → (key, 99)
    /// (key, 1)   → next_version() → (key, 0)
    /// (key, 0)   → next_version() → None
    /// ```
    #[inline]
    pub fn next_version(&self) -> Option<Self> {
        let ts = self.timestamp();
        if ts == 0 {
            None
        } else {
            Some(Self::encode(self.key(), ts - 1))
        }
    }

    /// Get the previous (newer) version of this key.
    ///
    /// Returns `(key, ts + 1)` in MVCC order. This is the immediately
    /// preceding entry in memory-comparable order for the same key.
    ///
    /// Returns `None` if `ts == MAX` (no newer version can exist).
    #[inline]
    pub fn prev_version(&self) -> Option<Self> {
        let ts = self.timestamp();
        if ts == Timestamp::MAX {
            None
        } else {
            Some(Self::encode(self.key(), ts + 1))
        }
    }

    // ========================================================================
    // Key Navigation (different keys, for volcano model iteration)
    // ========================================================================

    /// Get the first version of the lexicographically next key.
    ///
    /// This skips all versions of the current key and returns the
    /// first (newest) version of the next key in memory-comparable order.
    ///
    /// Returns `None` if the key is all `0xFF` bytes (no successor exists).
    ///
    /// # Use Case
    ///
    /// In volcano-style iteration, after processing all versions of a key,
    /// use this to seek to the next key:
    ///
    /// ```ignore
    /// // After finding visible version for "key_a"
    /// if let Some(next) = current.next_key() {
    ///     iterator.seek(&next);  // Skip to "key_b"
    /// }
    /// ```
    ///
    /// # MVCC Ordering
    ///
    /// ```text
    /// (key_a, 100) → next_key() → (key_b, MAX_TS)
    /// (key_a, 0)   → next_key() → (key_b, MAX_TS)
    /// ```
    pub fn next_key(&self) -> Option<Self> {
        let mut next = self.key().to_vec();
        if next_key_bound(&mut next) {
            Some(Self::first_for_key(&next))
        } else {
            None
        }
    }

    /// Get the last version of the lexicographically previous key.
    ///
    /// This returns the last (oldest) version of the previous key,
    /// which is the immediately preceding entry in memory-comparable
    /// order that belongs to a different key.
    ///
    /// Returns `None` if the key is all `0x00` bytes (no predecessor exists).
    ///
    /// # Use Case
    ///
    /// For reverse iteration in volcano model:
    ///
    /// ```ignore
    /// if let Some(prev) = current.prev_key() {
    ///     iterator.seek_for_prev(&prev);  // Seek to end of previous key
    /// }
    /// ```
    pub fn prev_key(&self) -> Option<Self> {
        let mut prev = self.key().to_vec();
        if prev_key_bound(&mut prev) {
            Some(Self::last_for_key(&prev))
        } else {
            None
        }
    }

    /// Create a seek target for finding the version visible at a given timestamp.
    ///
    /// Returns `(key, read_ts)` which, when used as a seek target, will
    /// position the iterator at or before the first visible version.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Find version of "user:123" visible at ts=500
    /// let seek_key = MvccKey::seek_for_read(b"user:123", 500);
    /// iterator.seek(&seek_key);
    /// // Iterator now at first version with ts <= 500
    /// ```
    #[inline]
    pub fn seek_for_read(key: &[u8], read_ts: Timestamp) -> Self {
        Self::encode(key, read_ts)
    }
}

impl Deref for MvccKey {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8]> for MvccKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<MvccKey> for Vec<u8> {
    fn from(key: MvccKey) -> Self {
        key.0
    }
}

impl PartialOrd for MvccKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MvccKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

// ============================================================================
// MvccIterator Trait
// ============================================================================

use std::future::Future;

use crate::util::error::Result;

/// Streaming iterator over MVCC key-value pairs.
///
/// This trait provides a RocksDB-style iterator interface with explicit
/// positioning operations. Unlike `std::iter::Iterator`, this design:
///
/// - Supports I/O error propagation via `Result`
/// - Allows efficient seeking to arbitrary positions
/// - **Zero-copy**: Returns references to existing memory, no cloning
///
/// ## Usage Pattern
///
/// ```ignore
/// // owner_ts: 0 for autocommit reads, or start_ts for read-your-writes
/// let mut iter = storage.scan_iter(range, owner_ts)?;
/// iter.advance()?;  // Position on first entry
/// while iter.valid() {
///     let user_key = iter.user_key();
///     let timestamp = iter.timestamp();
///     let value = iter.value();
///     // Process entry - clone if you need to keep the data
///     iter.advance()?;
/// }
/// ```
///
/// ## Key Ordering
///
/// MVCC keys are ordered as `(user_key ASC, timestamp DESC)`:
/// - Keys with the same user key are grouped together
/// - Within a user key, newer versions (higher timestamps) come first
///
/// ## Accessor Methods
///
/// All accessor methods (`user_key()`, `timestamp()`, `value()`) return
/// references rather than owned data. Callers who need ownership must
/// clone explicitly. Note that some implementations (e.g., merge iterators)
/// may cache data internally to support deduplication.
///
/// ## Thread Safety
///
/// Iterators must be `Send` so they can be used across `.await` points
/// in async tasks spawned with `tokio::spawn`.
pub trait MvccIterator: Send {
    /// Seek to the first entry with MVCC key >= target.
    ///
    /// After seeking:
    /// - `valid()` returns true if an entry with MVCC key >= target exists
    /// - `user_key()` and `timestamp()` return the positioned entry's components
    fn seek(&mut self, target: &MvccKey) -> impl Future<Output = Result<()>> + Send;

    /// Advance to the next entry.
    ///
    /// This advances the iterator to the next key in MVCC order.
    ///
    /// Named `advance` instead of `next` to avoid conflict with `Iterator::next`.
    fn advance(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Check if the iterator is positioned on a valid entry.
    ///
    /// Returns false if:
    /// - Iterator is exhausted
    /// - Iterator is before the first entry
    /// - An I/O error occurred (check via previous operation's Result)
    fn valid(&self) -> bool;

    /// Get the current entry's user key (without timestamp suffix).
    ///
    /// Returns a reference to the key stored in the underlying data structure.
    /// No allocation or copying occurs.
    ///
    /// # Panics
    ///
    /// Panics if `!self.valid()`. Always check `valid()` first.
    fn user_key(&self) -> &[u8];

    /// Get the current entry's commit timestamp.
    ///
    /// # Panics
    ///
    /// Panics if `!self.valid()`. Always check `valid()` first.
    fn timestamp(&self) -> Timestamp;

    /// Get the current value.
    ///
    /// Returns a reference to the value stored in the underlying data structure.
    /// No allocation or copying occurs.
    ///
    /// # Panics
    ///
    /// Panics if `!self.valid()`. Always check `valid()` first.
    fn value(&self) -> &[u8];

    /// Whether the current entry is a pending (uncommitted) write.
    ///
    /// Default: false (SST entries are always committed).
    /// Memtable iterators override this to report non-owner pending nodes.
    fn is_pending(&self) -> bool {
        false
    }

    /// Get the owner transaction's start_ts for pending entries.
    ///
    /// Only meaningful when `is_pending()` returns true.
    /// Default: 0 (no owner).
    fn pending_owner(&self) -> Timestamp {
        0
    }
}

/// Tombstone marker for deleted keys.
///
/// We use a byte sequence that starts with NUL bytes (uncommon in user data)
/// followed by a magic string and a version byte, making collision extremely unlikely.
/// The probability of collision is ~2^-120 for random 16-byte values.
pub const TOMBSTONE: &[u8] = b"\x00\x00\x00\x00TISQL_TOMBSTONE\x01";

/// Lock marker for conflict detection without data change.
///
/// Used for:
/// - SELECT FOR UPDATE (lock without modifying)
/// - UPDATE with same value (lock without data change)
/// - DELETE after INSERT/UPDATE in same txn (undo our write)
///
/// LOCK is NOT persisted to SST. At commit time, LOCK nodes are marked
/// as aborted since the pessimistic lock already served its purpose.
/// Reads skip LOCK and see the previous committed version.
pub const LOCK: &[u8] = b"\x00\x00\x00\x00TISQL_LOCK\x01";

/// Encode a key with timestamp for MVCC storage.
///
/// Format: `key || !commit_ts` (descending order by timestamp)
///
/// The bitwise NOT ensures that higher timestamps produce smaller encoded values,
/// so iterating in ascending order returns newer versions first.
///
/// # Arguments
///
/// * `key` - The key to encode (table-encoded format)
/// * `ts` - The commit timestamp
///
/// # Returns
///
/// The MVCC-encoded key
#[inline]
pub fn encode_mvcc_key(key: &[u8], ts: Timestamp) -> Key {
    let mut mvcc_key = Vec::with_capacity(key.len() + TIMESTAMP_SIZE);
    mvcc_key.extend_from_slice(key);
    // Bitwise NOT for descending order
    mvcc_key.extend_from_slice(&(!ts).to_be_bytes());
    mvcc_key
}

/// Decode an MVCC key back to (key, commit_ts).
///
/// Returns `None` if the key is too short to be a valid MVCC key.
///
/// # Arguments
///
/// * `mvcc_key` - The MVCC-encoded key
///
/// # Returns
///
/// `Some((key, commit_ts))` if decoding succeeds, `None` otherwise
#[inline]
pub fn decode_mvcc_key(mvcc_key: &[u8]) -> Option<(Key, Timestamp)> {
    if mvcc_key.len() < TIMESTAMP_SIZE {
        return None;
    }
    let key = mvcc_key[..mvcc_key.len() - TIMESTAMP_SIZE].to_vec();
    let ts_bytes: [u8; 8] = mvcc_key[mvcc_key.len() - TIMESTAMP_SIZE..]
        .try_into()
        .ok()?;
    // Reverse the bitwise NOT
    let ts = !u64::from_be_bytes(ts_bytes);
    Some((key, ts))
}

/// Extract only the timestamp portion from an MVCC key.
///
/// This avoids allocating the user key when only timestamp visibility
/// checks are needed.
#[inline]
pub fn extract_timestamp(mvcc_key: &[u8]) -> Option<Timestamp> {
    if mvcc_key.len() < TIMESTAMP_SIZE {
        return None;
    }
    let ts_bytes: [u8; TIMESTAMP_SIZE] = mvcc_key[mvcc_key.len() - TIMESTAMP_SIZE..]
        .try_into()
        .ok()?;
    Some(!u64::from_be_bytes(ts_bytes))
}

/// Extract the key portion from an MVCC key (without timestamp).
///
/// This is more efficient than full decode when the timestamp is not needed.
/// Returns the original key if it's too short to be an MVCC key.
///
/// # Arguments
///
/// * `mvcc_key` - The MVCC-encoded key
///
/// # Returns
///
/// A slice containing just the key portion (without timestamp suffix)
#[inline]
pub fn extract_key(mvcc_key: &[u8]) -> &[u8] {
    if mvcc_key.len() >= TIMESTAMP_SIZE {
        &mvcc_key[..mvcc_key.len() - TIMESTAMP_SIZE]
    } else {
        mvcc_key
    }
}

/// Check if a value is a tombstone marker.
///
/// # Arguments
///
/// * `value` - The value to check
///
/// # Returns
///
/// `true` if the value is a tombstone, `false` otherwise
#[inline]
pub fn is_tombstone(value: &[u8]) -> bool {
    value == TOMBSTONE
}

/// Check if a value is a lock marker.
///
/// Lock markers indicate conflict detection without data change.
/// They are used for SELECT FOR UPDATE, same-value UPDATE, and
/// DELETE on pending writes (undo within same transaction).
///
/// # Arguments
///
/// * `value` - The value to check
///
/// # Returns
///
/// `true` if the value is a lock marker, `false` otherwise
#[inline]
pub fn is_lock(value: &[u8]) -> bool {
    value == LOCK
}

/// Compute the "next" key for range bounds.
///
/// Increments the byte array by 1. Returns `true` if the increment produced
/// a valid lexicographically greater key. Returns `false` if all bytes were
/// 0xFF (overflow case), indicating no valid successor exists.
///
/// For the all-0xFF case, the caller should use an alternative termination
/// condition (e.g., checking decoded key > target_key after confirming
/// prefix match failed).
///
/// # Arguments
///
/// * `bytes` - The byte array to increment (modified in place)
///
/// # Returns
///
/// `true` if increment succeeded, `false` if overflow occurred
pub fn next_key_bound(bytes: &mut Vec<u8>) -> bool {
    if bytes.is_empty() {
        bytes.push(0);
        return true;
    }

    for i in (0..bytes.len()).rev() {
        if bytes[i] < 255 {
            bytes[i] += 1;
            return true;
        }
        bytes[i] = 0;
    }
    // All bytes were 255 - no valid successor exists that maintains the same length.
    // Return false to signal caller needs alternative termination logic.
    false
}

/// Compute the lexicographically previous key.
///
/// Decrements the byte array by 1. Returns `true` if the decrement produced
/// a valid lexicographically smaller key. Returns `false` if all bytes were
/// 0x00 (underflow case), indicating no valid predecessor exists.
///
/// # Arguments
///
/// * `bytes` - The byte array to decrement (modified in place)
///
/// # Returns
///
/// `true` if decrement succeeded, `false` if underflow occurred
pub fn prev_key_bound(bytes: &mut [u8]) -> bool {
    if bytes.is_empty() {
        return false;
    }

    for i in (0..bytes.len()).rev() {
        if bytes[i] > 0 {
            bytes[i] -= 1;
            // Set all following bytes to 0xFF (largest possible suffix)
            bytes[(i + 1)..].fill(0xFF);
            return true;
        }
    }
    // All bytes were 0 - no valid predecessor exists
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== MvccKey Tests ====================

    #[test]
    fn test_mvcc_key_encode_decode() {
        let key = b"test_key";
        let ts: Timestamp = 12345;

        let mvcc_key = MvccKey::encode(key, ts);

        assert_eq!(mvcc_key.key(), key);
        assert_eq!(mvcc_key.timestamp(), ts);

        let (decoded_key, decoded_ts) = mvcc_key.decode();
        assert_eq!(decoded_key, key.to_vec());
        assert_eq!(decoded_ts, ts);
    }

    #[test]
    fn test_mvcc_key_from_bytes() {
        let key = b"key";
        let mvcc_bytes = encode_mvcc_key(key, 100);

        let mvcc_key = MvccKey::from_bytes(mvcc_bytes.clone()).unwrap();
        assert_eq!(mvcc_key.key(), key);
        assert_eq!(mvcc_key.timestamp(), 100);
        assert_eq!(mvcc_key.as_bytes(), &mvcc_bytes);
    }

    #[test]
    fn test_mvcc_key_from_bytes_too_short() {
        let short = vec![1, 2, 3]; // Less than 8 bytes
        assert!(MvccKey::from_bytes(short).is_none());
    }

    #[test]
    fn test_mvcc_key_ordering() {
        // Higher timestamps should come first (smaller encoded value)
        let mvcc_100 = MvccKey::encode(b"key", 100);
        let mvcc_50 = MvccKey::encode(b"key", 50);
        let mvcc_1 = MvccKey::encode(b"key", 1);

        assert!(mvcc_100 < mvcc_50);
        assert!(mvcc_50 < mvcc_1);
    }

    #[test]
    fn test_mvcc_key_has_key() {
        let mvcc_key = MvccKey::encode(b"my_key", 100);

        assert!(mvcc_key.has_key(b"my_key"));
        assert!(!mvcc_key.has_key(b"other_key"));
    }

    #[test]
    fn test_mvcc_key_deref() {
        let mvcc_key = MvccKey::encode(b"key", 100);
        let bytes: &[u8] = &mvcc_key; // Uses Deref
        assert_eq!(bytes, mvcc_key.as_bytes());
    }

    #[test]
    fn test_mvcc_key_into_bytes() {
        let mvcc_key = MvccKey::encode(b"key", 100);
        let expected = mvcc_key.as_bytes().to_vec();
        let bytes = mvcc_key.into_bytes();
        assert_eq!(bytes, expected);
    }

    // ==================== Navigation Tests ====================

    #[test]
    fn test_first_and_last_for_key() {
        let first = MvccKey::first_for_key(b"user:123");
        let last = MvccKey::last_for_key(b"user:123");

        assert_eq!(first.key(), b"user:123");
        assert_eq!(first.timestamp(), Timestamp::MAX);

        assert_eq!(last.key(), b"user:123");
        assert_eq!(last.timestamp(), 0);

        // First should come before last in memory-comparable order
        assert!(first < last);
    }

    #[test]
    fn test_key_version_range() {
        let mvcc_key = MvccKey::encode(b"key", 100);
        let (start, end) = mvcc_key.key_version_range();

        assert_eq!(start.key(), b"key");
        assert_eq!(start.timestamp(), Timestamp::MAX);
        assert_eq!(end.key(), b"key");
        assert_eq!(end.timestamp(), 0);

        // Original key should be between start and end
        assert!(start <= mvcc_key);
        assert!(mvcc_key <= end);
    }

    #[test]
    fn test_next_version() {
        let mvcc_100 = MvccKey::encode(b"key", 100);
        let next = mvcc_100.next_version().unwrap();

        assert_eq!(next.key(), b"key");
        assert_eq!(next.timestamp(), 99);

        // Next version should be greater in memory-comparable order
        assert!(mvcc_100 < next);
    }

    #[test]
    fn test_next_version_at_zero() {
        let mvcc_0 = MvccKey::encode(b"key", 0);
        assert!(mvcc_0.next_version().is_none());
    }

    #[test]
    fn test_prev_version() {
        let mvcc_100 = MvccKey::encode(b"key", 100);
        let prev = mvcc_100.prev_version().unwrap();

        assert_eq!(prev.key(), b"key");
        assert_eq!(prev.timestamp(), 101);

        // Previous version should be smaller in memory-comparable order
        assert!(prev < mvcc_100);
    }

    #[test]
    fn test_prev_version_at_max() {
        let mvcc_max = MvccKey::encode(b"key", Timestamp::MAX);
        assert!(mvcc_max.prev_version().is_none());
    }

    #[test]
    fn test_next_key() {
        let mvcc_a = MvccKey::encode(b"key_a", 100);
        let next = mvcc_a.next_key().unwrap();

        // Should be key_b at MAX_TS
        assert_eq!(next.key(), b"key_b");
        assert_eq!(next.timestamp(), Timestamp::MAX);

        // Next key should be greater than any version of current key
        let mvcc_a_0 = MvccKey::encode(b"key_a", 0);
        assert!(mvcc_a_0 < next);
    }

    #[test]
    fn test_next_key_overflow() {
        // All 0xFF bytes should return None
        let mvcc_ff = MvccKey::encode(&[0xFF, 0xFF, 0xFF], 100);
        assert!(mvcc_ff.next_key().is_none());
    }

    #[test]
    fn test_prev_key() {
        let mvcc_b = MvccKey::encode(b"key_b", 100);
        let prev = mvcc_b.prev_key().unwrap();

        // Should be key_a at ts=0 (last version)
        assert_eq!(prev.key(), b"key_a");
        assert_eq!(prev.timestamp(), 0);

        // Previous key should be less than any version of current key
        let mvcc_b_max = MvccKey::first_for_key(b"key_b");
        assert!(prev < mvcc_b_max);
    }

    #[test]
    fn test_prev_key_underflow() {
        // All 0x00 bytes should return None
        let mvcc_00 = MvccKey::encode(&[0x00, 0x00, 0x00], 100);
        assert!(mvcc_00.prev_key().is_none());
    }

    #[test]
    fn test_seek_for_read() {
        let seek = MvccKey::seek_for_read(b"user:123", 500);

        assert_eq!(seek.key(), b"user:123");
        assert_eq!(seek.timestamp(), 500);
    }

    #[test]
    fn test_navigation_chain() {
        // Test that navigation methods chain correctly
        let start = MvccKey::encode(b"key", 100);

        // Go to next older version
        let v99 = start.next_version().unwrap();
        assert_eq!(v99.timestamp(), 99);

        // Go back to newer version
        let v100 = v99.prev_version().unwrap();
        assert_eq!(v100.timestamp(), 100);
        assert_eq!(v100.key(), start.key());
    }

    #[test]
    fn test_prev_key_bound() {
        let mut v = vec![1u8];
        assert!(prev_key_bound(&mut v));
        assert_eq!(v, vec![0u8]);

        let mut v = vec![0u8];
        assert!(!prev_key_bound(&mut v)); // Underflow

        let mut v = vec![1u8, 0u8];
        assert!(prev_key_bound(&mut v));
        assert_eq!(v, vec![0u8, 0xFFu8]);

        let mut v = vec![2u8, 0u8, 0u8];
        assert!(prev_key_bound(&mut v));
        assert_eq!(v, vec![1u8, 0xFFu8, 0xFFu8]);
    }

    // ==================== Function Tests ====================

    #[test]
    fn test_encode_decode_mvcc_key() {
        let key = b"test_key";
        let ts: Timestamp = 12345;

        let mvcc_key = encode_mvcc_key(key, ts);
        let (decoded_key, decoded_ts) = decode_mvcc_key(&mvcc_key).unwrap();

        assert_eq!(decoded_key, key.to_vec());
        assert_eq!(decoded_ts, ts);
    }

    #[test]
    fn test_encode_mvcc_key_ordering() {
        // Higher timestamps should come first (smaller encoded value)
        let key = b"key";
        let mvcc_100 = encode_mvcc_key(key, 100);
        let mvcc_50 = encode_mvcc_key(key, 50);
        let mvcc_1 = encode_mvcc_key(key, 1);

        assert!(mvcc_100 < mvcc_50);
        assert!(mvcc_50 < mvcc_1);
    }

    #[test]
    fn test_extract_key() {
        let key = b"test_key";
        let mvcc_key = encode_mvcc_key(key, 100);

        let extracted = extract_key(&mvcc_key);
        assert_eq!(extracted, key);
    }

    #[test]
    fn test_extract_key_short() {
        // Short keys (< 8 bytes) are returned as-is
        let short = b"short";
        let extracted = extract_key(short);
        assert_eq!(extracted, short);
    }

    #[test]
    fn test_extract_timestamp() {
        let key = b"test_key";
        let ts: Timestamp = 12345;
        let mvcc_key = encode_mvcc_key(key, ts);
        assert_eq!(extract_timestamp(&mvcc_key), Some(ts));
    }

    #[test]
    fn test_extract_timestamp_short_key() {
        assert_eq!(extract_timestamp(b"short"), None);
    }

    #[test]
    fn test_extract_timestamp_boundaries() {
        let key = b"boundary";
        let min_mvcc = encode_mvcc_key(key, 0);
        let max_mvcc = encode_mvcc_key(key, Timestamp::MAX);
        assert_eq!(extract_timestamp(&min_mvcc), Some(0));
        assert_eq!(extract_timestamp(&max_mvcc), Some(Timestamp::MAX));
    }

    #[test]
    fn test_is_tombstone() {
        assert!(is_tombstone(TOMBSTONE));
        assert!(!is_tombstone(b"regular_value"));
        assert!(!is_tombstone(b""));
    }

    #[test]
    fn test_is_lock() {
        assert!(is_lock(LOCK));
        assert!(!is_lock(b"regular_value"));
        assert!(!is_lock(b""));
        // LOCK and TOMBSTONE are distinct
        assert!(!is_lock(TOMBSTONE));
        assert!(!is_tombstone(LOCK));
    }

    #[test]
    fn test_next_key_bound() {
        let mut v = vec![0u8];
        assert!(next_key_bound(&mut v));
        assert_eq!(v, vec![1u8]);

        let mut v = vec![255u8];
        assert!(!next_key_bound(&mut v)); // Overflow
        assert_eq!(v, vec![0u8]); // Wrapped around

        let mut v = vec![1u8, 255u8];
        assert!(next_key_bound(&mut v));
        assert_eq!(v, vec![2u8, 0u8]);
    }

    #[test]
    fn test_decode_short_key() {
        // Keys shorter than TIMESTAMP_SIZE can't be decoded
        let short = b"short";
        assert!(decode_mvcc_key(short).is_none());
    }

    #[test]
    fn test_timestamp_boundaries() {
        let key = b"key";

        // Test with min timestamp
        let mvcc_0 = encode_mvcc_key(key, 0);
        let (_, decoded_ts) = decode_mvcc_key(&mvcc_0).unwrap();
        assert_eq!(decoded_ts, 0);

        // Test with max timestamp
        let mvcc_max = encode_mvcc_key(key, Timestamp::MAX);
        let (_, decoded_ts) = decode_mvcc_key(&mvcc_max).unwrap();
        assert_eq!(decoded_ts, Timestamp::MAX);
    }

    #[test]
    fn test_mvcc_key_length() {
        let key = b"test";
        let mvcc_key = encode_mvcc_key(key, 100);

        assert_eq!(mvcc_key.len(), key.len() + TIMESTAMP_SIZE);
    }

    // ==================== Additional Coverage Tests ====================

    #[test]
    fn test_mvcc_key_from_bytes_unchecked() {
        let key = b"key";
        let mvcc_bytes = encode_mvcc_key(key, 100);

        // from_bytes_unchecked should work with valid bytes
        let mvcc_key = MvccKey::from_bytes_unchecked(mvcc_bytes.clone());
        assert_eq!(mvcc_key.key(), key);
        assert_eq!(mvcc_key.timestamp(), 100);
    }

    #[test]
    fn test_mvcc_key_len_method() {
        let mvcc_key = MvccKey::encode(b"test_key", 100);

        // Test len() method on MvccKey
        assert_eq!(mvcc_key.len(), b"test_key".len() + TIMESTAMP_SIZE);
    }

    #[test]
    fn test_mvcc_key_is_empty() {
        // Normal key is not empty
        let mvcc_key = MvccKey::encode(b"key", 100);
        assert!(!mvcc_key.is_empty());

        // Unbounded key is empty
        let unbounded = MvccKey::unbounded();
        assert!(unbounded.is_empty());
    }

    #[test]
    fn test_mvcc_key_is_unbounded() {
        // Normal key is not unbounded
        let mvcc_key = MvccKey::encode(b"key", 100);
        assert!(!mvcc_key.is_unbounded());

        // Unbounded key is unbounded
        let unbounded = MvccKey::unbounded();
        assert!(unbounded.is_unbounded());
    }

    #[test]
    fn test_mvcc_key_next_key_overflow() {
        // Test next_key with max bytes (overflow case)
        let mvcc_key = MvccKey::encode(&[255u8; 4], u64::MAX);
        let next = mvcc_key.next_key();
        // When overflow occurs, returns None
        assert!(next.is_none());
    }

    #[test]
    fn test_mvcc_key_next_key_normal() {
        let mvcc_key = MvccKey::encode(b"abc", 100);
        let next = mvcc_key.next_key();
        assert!(next.is_some());

        let next_key = next.unwrap();
        // Next key should be greater
        assert!(next_key > mvcc_key);
    }

    #[test]
    fn test_mvcc_key_clone() {
        let mvcc_key = MvccKey::encode(b"key", 100);
        let cloned = mvcc_key.clone();

        assert_eq!(cloned.key(), mvcc_key.key());
        assert_eq!(cloned.timestamp(), mvcc_key.timestamp());
    }

    #[test]
    fn test_mvcc_key_debug() {
        let mvcc_key = MvccKey::encode(b"key", 100);
        let debug_str = format!("{mvcc_key:?}");
        // Debug should contain MvccKey
        assert!(debug_str.contains("MvccKey"));
    }

    #[test]
    fn test_mvcc_key_hash() {
        use std::collections::HashSet;

        let key1 = MvccKey::encode(b"key", 100);
        let key2 = MvccKey::encode(b"key", 100);
        let key3 = MvccKey::encode(b"key", 200);

        let mut set = HashSet::new();
        set.insert(key1.clone());

        // Same key should already be in set
        assert!(set.contains(&key2));

        // Different key should not be in set
        assert!(!set.contains(&key3));
    }

    #[test]
    fn test_mvcc_key_ord() {
        let key1 = MvccKey::encode(b"aaa", 100);
        let key2 = MvccKey::encode(b"bbb", 100);
        let key3 = MvccKey::encode(b"aaa", 50);

        // Different user keys
        assert!(key1 < key2);

        // Same user key, different timestamps (higher ts comes first)
        assert!(key1 < key3);
    }

    #[test]
    fn test_shared_mvcc_range_clone() {
        use std::sync::Arc;

        let range = MvccKey::encode(b"start", 100)..MvccKey::encode(b"end", 0);
        let shared: SharedMvccRange = Arc::new(range.clone());
        let cloned = Arc::clone(&shared);

        assert_eq!(shared.start.key(), cloned.start.key());
        assert_eq!(shared.end.key(), cloned.end.key());
    }

    #[test]
    fn test_is_tombstone_with_vec() {
        assert!(is_tombstone(TOMBSTONE));
        assert!(is_tombstone(TOMBSTONE));
        assert!(!is_tombstone(b"normal_value"));
        assert!(!is_tombstone(&[])); // Empty is not tombstone
    }

    #[test]
    fn test_is_lock_with_vec() {
        assert!(is_lock(LOCK));
        assert!(is_lock(LOCK));
        assert!(!is_lock(b"normal_value"));
        assert!(!is_lock(&[])); // Empty is not lock
    }

    #[test]
    fn test_lock_and_tombstone_distinct() {
        // LOCK and TOMBSTONE should be different
        assert_ne!(LOCK, TOMBSTONE);
        assert!(!is_lock(TOMBSTONE));
        assert!(!is_tombstone(LOCK));
    }

    #[test]
    fn test_mvcc_key_empty_user_key() {
        // Test with empty user key
        let mvcc_key = MvccKey::encode(b"", 100);
        assert_eq!(mvcc_key.key(), b"");
        assert_eq!(mvcc_key.timestamp(), 100);
        assert_eq!(mvcc_key.len(), TIMESTAMP_SIZE);
    }

    #[test]
    fn test_mvcc_key_large_user_key() {
        // Test with large user key
        let large_key = vec![b'x'; 1000];
        let mvcc_key = MvccKey::encode(&large_key, 100);
        assert_eq!(mvcc_key.key(), &large_key[..]);
        assert_eq!(mvcc_key.timestamp(), 100);
        assert_eq!(mvcc_key.len(), 1000 + TIMESTAMP_SIZE);
    }
}
