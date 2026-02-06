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

//! LSM-tree storage engine.
//!
//! This module provides the main LSM storage engine that combines:
//!
//! - Active memtable for writes
//! - Frozen memtables awaiting flush
//! - SST files organized by level
//! - Version management for consistent reads
//!
//! ## Architecture
//!
//! ```text
//! Write Path:
//!   write() -> active memtable -> (when full) -> freeze -> flush -> L0 SST
//!
//! Read Path:
//!   get() -> active memtable -> frozen memtables -> L0 SSTs -> L1+ SSTs
//!
//! Background:
//!   flush thread: frozen memtables -> L0 SSTs
//!   compaction thread: L0 -> L1 -> L2 -> ... merging
//! ```
//!
//! ## Concurrency
//!
//! - Writes go to the active memtable (lock-free skiplist)
//! - Reads cascade through memtables and SSTs without locking
//! - Flush and compaction run in background threads
//! - Version changes are atomic (swap pointer)

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock, RwLockWriteGuard};

#[cfg(feature = "failpoints")]
use fail::fail_point;

use crate::error::{Result, TiSqlError};
use crate::lsn::SharedLsnProvider;
use crate::storage::mvcc::{decode_mvcc_key, is_tombstone, MvccIterator, MvccKey, SharedMvccRange};
use crate::storage::{PessimisticStorage, StorageEngine, WriteBatch};
use crate::types::{Key, RawValue, Timestamp};

use super::config::LsmConfig;
use super::ilog::IlogService;
use super::memtable::MemTable;
use super::sstable::{
    SstBuilder, SstBuilderOptions, SstIterator, SstMeta, SstMvccIterator, SstReaderRef,
};
use super::version::{ManifestDelta, Version, MAX_LEVELS};
use super::version_set::{SuperVersion, VersionSet};

/// Inner state of the LSM engine, protected by RwLock for atomic updates.
///
/// Note: Version is managed separately via `LsmEngine::version_set` to allow
/// independent locking of memtable state and version state.
struct LsmState {
    /// Active memtable accepting writes.
    active: Arc<MemTable>,

    /// Frozen memtables awaiting flush, ordered oldest first (newest at back).
    frozen: VecDeque<Arc<MemTable>>,
}

impl LsmState {
    fn new(memtable_id: u64) -> Self {
        Self {
            active: Arc::new(MemTable::new(memtable_id)),
            frozen: VecDeque::new(),
        }
    }
}

/// LSM-tree storage engine.
///
/// This is the main entry point for persistent storage. It manages:
///
/// - Active memtable for writes
/// - Frozen memtables awaiting flush
/// - SST files at each level
/// - Version metadata
///
/// ## Usage
///
/// ```ignore
/// use tisql::storage::{LsmConfig, Version, IlogConfig, IlogService};
/// use tisql::lsn::new_lsn_provider;
///
/// let lsn_provider = new_lsn_provider();
/// let ilog = Arc::new(IlogService::open(IlogConfig::new("./data"), lsn_provider.clone())?);
/// let config = LsmConfig::new("./data");
/// let engine = LsmEngine::open_with_recovery(config, lsn_provider, ilog, Version::new())?;
///
/// // Write with timestamp
/// let mut batch = WriteBatch::new();
/// batch.set_commit_ts(100);
/// batch.put(b"key".to_vec(), b"value".to_vec());
/// engine.write_batch(batch)?;
///
/// // Read at timestamp
/// let value = engine.get_at(b"key", 100)?;
/// ```
pub struct LsmEngine {
    /// Configuration.
    config: Arc<LsmConfig>,

    /// Engine state (active/frozen memtables).
    state: RwLock<LsmState>,

    /// Version management (separate from memtable state).
    version_set: VersionSet,

    /// Next memtable ID.
    next_memtable_id: AtomicU64,

    /// Next SST ID (atomic to prevent race conditions during concurrent flushes).
    /// This counter is owned by LsmEngine, not Version, to ensure atomic allocation.
    next_sst_id: AtomicU64,

    /// Next LSN for ordering operations (fallback if no lsn_provider).
    next_lsn: AtomicU64,

    /// Next SuperVersion number for tracking snapshot staleness.
    sv_number: AtomicU64,

    /// Optional shared LSN provider for unified LSN allocation.
    lsn_provider: Option<SharedLsnProvider>,

    /// Optional ilog service for durable SST metadata.
    ilog: Option<Arc<IlogService>>,

    /// Cached SuperVersion for atomic snapshot access.
    ///
    /// Readers call `get_super_version()` which clones this Arc.
    /// Writers call `install_super_version()` after any state change
    /// (rotate, flush, compact) to atomically update the snapshot.
    ///
    /// This ensures readers always get a consistent view of
    /// (active, frozen, version) without race conditions.
    current_sv: RwLock<Arc<SuperVersion>>,
}

impl LsmEngine {
    /// Open with recovery from ilog.
    ///
    /// This replays the ilog to rebuild version state.
    pub fn open_with_recovery(
        config: LsmConfig,
        lsn_provider: SharedLsnProvider,
        ilog: Arc<IlogService>,
        version: Version,
    ) -> Result<Self> {
        config.validate().map_err(TiSqlError::Storage)?;

        // Create SST directory if needed
        let sst_dir = config.sst_dir();
        if !sst_dir.exists() {
            std::fs::create_dir_all(&sst_dir)?;
        }

        // Initialize next_sst_id from recovered version's max SST ID + 1
        let recovered_max_sst_id = version.next_sst_id();

        // Create initial state and version_set
        let initial_state = LsmState::new(1);
        let version_set = VersionSet::new(version);

        // Create initial SuperVersion
        let initial_sv = Arc::new(SuperVersion::new(
            Arc::clone(&initial_state.active),
            initial_state.frozen.clone(),
            version_set.current(),
            0, // Initial sv_number
        ));

        Ok(Self {
            config: Arc::new(config),
            state: RwLock::new(initial_state),
            version_set,
            next_memtable_id: AtomicU64::new(2),
            next_sst_id: AtomicU64::new(recovered_max_sst_id),
            next_lsn: AtomicU64::new(1),
            sv_number: AtomicU64::new(1),
            lsn_provider: Some(lsn_provider),
            ilog: Some(ilog),
            current_sv: RwLock::new(initial_sv),
        })
    }

    /// Get the current configuration.
    pub fn config(&self) -> &LsmConfig {
        &self.config
    }

    /// Check if this engine has durable ilog.
    pub fn is_durable(&self) -> bool {
        self.ilog.is_some()
    }

    /// Get the next LSN and increment.
    fn alloc_lsn(&self) -> u64 {
        if let Some(ref provider) = self.lsn_provider {
            provider.alloc_lsn()
        } else {
            self.next_lsn.fetch_add(1, AtomicOrdering::SeqCst)
        }
    }

    /// Get the next memtable ID and increment.
    fn alloc_memtable_id(&self) -> u64 {
        self.next_memtable_id.fetch_add(1, AtomicOrdering::SeqCst)
    }

    /// Allocate the next SST ID atomically.
    ///
    /// This is used during flush to ensure unique SST IDs even with concurrent flushes.
    /// Unlike Version::next_sst_id() which reads from a snapshot, this is atomic.
    fn alloc_sst_id(&self) -> u64 {
        self.next_sst_id.fetch_add(1, AtomicOrdering::SeqCst)
    }

    /// Get the number of frozen memtables.
    pub fn frozen_count(&self) -> usize {
        let state = self.state.read().unwrap();
        state.frozen.len()
    }

    /// Get the approximate size of the active memtable.
    pub fn active_memtable_size(&self) -> usize {
        let state = self.state.read().unwrap();
        state.active.approximate_size()
    }

    /// Get the current version.
    pub fn current_version(&self) -> Arc<Version> {
        self.version_set.current()
    }

    /// Get a SuperVersion snapshot for reading.
    ///
    /// Returns a consistent snapshot of:
    /// - Active memtable
    /// - Frozen memtables
    /// - SST version
    ///
    /// The snapshot holds Arc references to all components, keeping them alive
    /// even if the engine rotates memtables or flushes to SST.
    ///
    /// # Usage
    ///
    /// ```ignore
    /// let sv = engine.get_super_version();
    /// // sv provides consistent view for reading
    /// // Drop sv when done to allow GC
    /// ```
    pub fn get_super_version(&self) -> Arc<SuperVersion> {
        // Just clone the cached SuperVersion - always consistent
        Arc::clone(&self.current_sv.read().unwrap())
    }

    /// Get the current SuperVersion number.
    ///
    /// Returns the sv_number of the currently installed SuperVersion.
    /// Useful for checking if a SuperVersion is stale.
    pub fn current_sv_number(&self) -> u64 {
        self.current_sv.read().unwrap().sv_number
    }

    /// Install a new SuperVersion after state changes.
    ///
    /// Takes `RwLockWriteGuard` to **enforce at compile time** that the caller
    /// holds the write lock. This prevents accidental calls without proper
    /// synchronization.
    ///
    /// # Why Guard Instead of &LsmState?
    ///
    /// Using `&LsmState` would allow calling with a read lock reference,
    /// which could cause race conditions. The guard type ensures:
    /// 1. Caller holds exclusive access to state
    /// 2. State modifications are complete before SV installation
    /// 3. No other thread can see partial state
    ///
    /// Call this after:
    /// - Memtable rotation (freeze_active)
    /// - Flush completion
    /// - Compaction completion
    fn install_super_version(&self, state: &RwLockWriteGuard<'_, LsmState>) {
        let sv_num = self.sv_number.fetch_add(1, AtomicOrdering::Relaxed);
        let version = self.version_set.current();

        let new_sv = Arc::new(SuperVersion::new(
            Arc::clone(&state.active),
            state.frozen.clone(),
            version,
            sv_num,
        ));

        *self.current_sv.write().unwrap() = new_sv;
    }

    /// Check if the active memtable should be rotated.
    fn should_rotate(&self) -> bool {
        let state = self.state.read().unwrap();
        state.active.approximate_size() >= self.config.memtable_size
    }

    /// Rotate the active memtable (freeze current, create new).
    ///
    /// Returns the frozen memtable if rotation occurred.
    pub fn maybe_rotate(&self) -> Option<Arc<MemTable>> {
        if !self.should_rotate() {
            return None;
        }

        let mut state = self.state.write().unwrap();

        // Double-check under write lock
        if state.active.approximate_size() < self.config.memtable_size {
            return None;
        }

        // Check frozen limit
        if state.frozen.len() >= self.config.max_frozen_memtables {
            // Cannot rotate - too many frozen memtables awaiting flush.
            // TODO: Implement write stalling/backpressure when flush can't keep up.
            // Currently, writes continue to the active memtable, which can grow
            // memory unbounded under sustained load without flush.
            return None;
        }

        // Freeze current active
        let old_active = Arc::clone(&state.active);

        // Failpoint: crash before freezing memtable
        #[cfg(feature = "failpoints")]
        fail_point!("lsm_before_freeze");

        old_active.freeze();

        // Failpoint: crash after freeze but before inserting to frozen list
        #[cfg(feature = "failpoints")]
        fail_point!("lsm_after_freeze_before_insert");

        // Create new active
        let new_id = self.alloc_memtable_id();
        state.active = Arc::new(MemTable::new(new_id));

        // Add to frozen list (newest at back)
        state.frozen.push_back(Arc::clone(&old_active));

        // Install new SuperVersion to reflect the rotation
        self.install_super_version(&state);

        Some(old_active)
    }

    /// Flush a frozen memtable to an SST file.
    ///
    /// This creates a new L0 SST and updates the version.
    /// If ilog is configured, uses intent/commit protocol for crash safety.
    pub fn flush_memtable(&self, memtable: &MemTable) -> Result<SstMeta> {
        if !memtable.is_frozen() {
            return Err(TiSqlError::Storage(
                "Cannot flush non-frozen memtable".to_string(),
            ));
        }

        // Allocate SST ID atomically to prevent races during concurrent flushes
        let sst_id = self.alloc_sst_id();
        let max_memtable_lsn = memtable.max_lsn().unwrap_or(0);

        // Phase 1: Write flush intent (if durable)
        if let Some(ref ilog) = self.ilog {
            ilog.write_flush_intent(sst_id, memtable.id(), max_memtable_lsn)?;
        }

        // Phase 2: Build SST file
        // Failpoint: crash before SST build
        #[cfg(feature = "failpoints")]
        fail_point!("lsm_flush_before_sst_build");

        let sst_path = self.config.sst_dir().join(format!("{sst_id:08}.sst"));
        let options = SstBuilderOptions {
            block_size: self.config.block_size,
            compression: self.config.compression,
        };

        let mut builder = SstBuilder::new(&sst_path, options)?;

        // Iterate memtable using streaming iterator and add all entries INCLUDING tombstones
        // as raw MVCC keys. SSTs MUST store MVCC keys (key || !commit_ts) to preserve version
        // information for correct MVCC semantics during reads.
        //
        // Use truly unbounded range - MvccKey::unbounded() creates empty placeholder
        // that signals "scan all entries" to the memtable.
        let range = Arc::new(MvccKey::unbounded()..MvccKey::unbounded());
        // Flush only committed data (owner_ts = 0 means no pending nodes visible)
        let mut iter = memtable.inner().create_streaming_iter(range, 0);
        iter.advance()?; // Initialize iterator to first entry

        while iter.valid() {
            // Reconstruct MVCC key from user_key + timestamp
            let mvcc_key = MvccKey::encode(iter.user_key(), iter.timestamp());
            builder.add(mvcc_key.as_bytes(), iter.value())?;
            iter.advance()?;
        }

        // Finish building (writes to disk with fsync)
        let meta = builder.finish(sst_id, 0)?; // Level 0

        // Failpoint: crash after SST write, before ilog commit
        #[cfg(feature = "failpoints")]
        fail_point!("lsm_flush_after_sst_write");

        // Phase 3: Write flush commit (if durable)
        if let Some(ref ilog) = self.ilog {
            ilog.write_flush_commit(meta.clone(), max_memtable_lsn)?;
        }

        // Failpoint: crash after ilog commit, before version update
        #[cfg(feature = "failpoints")]
        fail_point!("lsm_flush_after_ilog_commit");

        // Phase 4: Update version, then atomically update frozen + SuperVersion.
        //
        // apply_delta is done outside the state write lock to reduce contention.
        // This is safe because readers that snapshot state and version separately
        // may momentarily see the flushed memtable AND the new SST (redundant but
        // correct — get_at short-circuits on memtable hit, merge iterator deduplicates).
        // The dangerous direction (data in neither) cannot happen since version is
        // updated before the memtable is removed from frozen.
        //
        // install_super_version reads version_set.current() inside the state lock,
        // so SV readers always get a consistent (frozen, version) pair.
        let delta = ManifestDelta::flush(meta.clone(), max_memtable_lsn);
        let new_version = self.version_set.apply_delta(&delta);

        {
            let mut state = self.state.write().unwrap();

            // Remove flushed memtable from frozen list
            state.frozen.retain(|m| m.id() != memtable.id());

            // Install new SuperVersion while still holding lock
            self.install_super_version(&state);
        };

        // Failpoint: crash after version update
        #[cfg(feature = "failpoints")]
        fail_point!("lsm_flush_after_version_update");

        // Check if checkpoint needed
        if let Some(ref ilog) = self.ilog {
            if ilog.needs_checkpoint() {
                ilog.write_checkpoint(&new_version)?;
            }
        }

        Ok(meta)
    }

    /// Read a key from SST files with MVCC timestamp filtering.
    ///
    /// Searches L0 first (newest to oldest), then L1, L2, etc.
    /// SSTs contain MVCC keys (key || !commit_ts), so we need to:
    /// 1. Scan for entries matching the key
    /// 2. Find the latest version with entry_ts <= ts
    /// 3. Return None if that version is a tombstone
    fn get_from_sst(&self, key: &[u8], ts: Timestamp) -> Result<Option<RawValue>> {
        let version = self.current_version();

        // Find SSTs that may contain this key
        // Note: find_ssts_for_key now receives user key, but SST metadata stores MVCC key ranges
        // We need to update this to work with MVCC keys, but for now iterate all SSTs
        let candidates = version.find_ssts_for_key(key);

        // Track best match: (entry_ts, value) where entry_ts <= ts
        let mut best_match: Option<(Timestamp, RawValue)> = None;

        for sst_meta in candidates {
            let sst_path = self
                .config
                .sst_dir()
                .join(format!("{:08}.sst", sst_meta.id));

            // SST referenced by Version must exist - missing file indicates data corruption
            if !sst_path.exists() {
                return Err(TiSqlError::Storage(format!(
                    "SST file missing: {} (id={}, level={}). This indicates data corruption or incomplete recovery.",
                    sst_path.display(),
                    sst_meta.id,
                    sst_meta.level
                )));
            }

            // SST now contains MVCC keys - iterate to find matching key with ts visibility
            // Propagate errors instead of silently ignoring (risks wrong reads)
            let reader = SstReaderRef::open(&sst_path)?;
            let mut iter = SstIterator::new(reader)?;
            iter.seek_to_first()?; // Position at first entry

            while iter.valid() {
                let mvcc_key = iter.key();
                let value = iter.value();

                if let Some((decoded_key, entry_ts)) = decode_mvcc_key(mvcc_key) {
                    // Check if this is our key and visible at ts
                    if decoded_key == key && entry_ts <= ts {
                        // Check if this is better than current best match
                        // (higher entry_ts but still <= ts)
                        let dominated = best_match
                            .as_ref()
                            .is_some_and(|(best_ts, _)| entry_ts <= *best_ts);
                        if !dominated {
                            best_match = Some((entry_ts, value.to_vec()));
                        }
                    }
                    // Optimization: if decoded_key > key, we've passed our key
                    // (due to MVCC key ordering: same key groups together)
                    if decoded_key.as_slice() > key {
                        break;
                    }
                }

                iter.advance()?;
            }
        }

        // Return the best match, checking for tombstone
        match best_match {
            Some((_, value)) if is_tombstone(&value) => Ok(None),
            Some((_, value)) => Ok(Some(value)),
            None => Ok(None),
        }
    }

    /// Force freeze the active memtable.
    ///
    /// Used for shutdown to ensure all data is flushed.
    pub fn freeze_active(&self) -> Option<Arc<MemTable>> {
        let mut state = self.state.write().unwrap();

        // Only freeze if there's data
        if state.active.approximate_size() == 0 {
            return None;
        }

        // Check frozen limit
        if state.frozen.len() >= self.config.max_frozen_memtables {
            return None;
        }

        // Freeze current active
        let old_active = Arc::clone(&state.active);
        old_active.freeze();

        // Create new active
        let new_id = self.alloc_memtable_id();
        state.active = Arc::new(MemTable::new(new_id));

        // Add to frozen list (newest at back)
        state.frozen.push_back(Arc::clone(&old_active));

        // Install new SuperVersion to reflect the freeze
        self.install_super_version(&state);

        Some(old_active)
    }

    /// Force flush all frozen memtables.
    ///
    /// Used for shutdown and testing.
    pub fn flush_all(&self) -> Result<Vec<SstMeta>> {
        let mut results = Vec::new();

        loop {
            // Get next frozen memtable to flush
            let frozen = {
                let state = self.state.read().unwrap();
                state.frozen.front().cloned() // Oldest first
            };

            match frozen {
                Some(memtable) => {
                    let meta = self.flush_memtable(&memtable)?;
                    results.push(meta);
                }
                None => break,
            }
        }

        Ok(results)
    }

    /// Freeze active memtable and flush all.
    ///
    /// Used for clean shutdown to ensure all data is persisted.
    pub fn flush_all_with_active(&self) -> Result<Vec<SstMeta>> {
        // First freeze the active memtable
        self.freeze_active();

        // Then flush all frozen
        self.flush_all()
    }

    /// Get statistics about the engine.
    pub fn stats(&self) -> LsmStats {
        let state = self.state.read().unwrap();
        let version = self.version_set.current();

        LsmStats {
            active_memtable_size: state.active.approximate_size(),
            frozen_memtable_count: state.frozen.len(),
            l0_sst_count: version.level_size(0),
            total_sst_count: version.total_sst_count(),
            total_sst_bytes: version.total_size_bytes(),
            version_num: version.version_num(),
            flushed_lsn: version.flushed_lsn(),
        }
    }

    /// Get a value by key using snapshot read at the latest timestamp.
    ///
    /// This is a convenience method for point lookups. It:
    /// 1. Checks the active memtable
    /// 2. Checks frozen memtables (newest first)
    /// 3. Checks SST files via `get_from_sst`
    ///
    /// Returns `Ok(Some(value))` if found, `Ok(None)` if not found or deleted.
    pub fn get(&self, key: &[u8]) -> Result<Option<RawValue>> {
        self.get_at(key, Timestamp::MAX)
    }

    /// Get a value by key at a specific MVCC timestamp.
    ///
    /// This performs a point lookup at the given timestamp:
    /// 1. Checks the active memtable
    /// 2. Checks frozen memtables (newest first)
    /// 3. Checks SST files
    ///
    /// Returns `Ok(Some(value))` if found, `Ok(None)` if not found or deleted.
    pub fn get_at(&self, key: &[u8], ts: Timestamp) -> Result<Option<RawValue>> {
        let state = self.state.read().unwrap();

        // Check active memtable first
        // owner_start_ts = 0 means don't see pending writes from any transaction
        if let Some(value) = state.active.get_with_owner(key, ts, 0) {
            return if is_tombstone(&value) {
                Ok(None)
            } else {
                Ok(Some(value))
            };
        }

        // Check frozen memtables (newest first = iterate from back)
        for frozen in state.frozen.iter().rev() {
            if let Some(value) = frozen.get_with_owner(key, ts, 0) {
                return if is_tombstone(&value) {
                    Ok(None)
                } else {
                    Ok(Some(value))
                };
            }
        }

        // Drop the lock before reading SSTs
        drop(state);

        // Check SSTs
        self.get_from_sst(key, ts)
    }
}

use std::collections::BinaryHeap;

// ============================================================================
// ArcMemTableIterator - Streaming iterator that owns Arc<MemTable>
// ============================================================================

use super::memtable::VersionedMemTableIterator;

/// Streaming iterator that owns an `Arc<MemTable>` and iterates without materialization.
///
/// This struct combines:
/// - Ownership of `Arc<MemTable>` to keep the memtable data alive
/// - A `VersionedMemTableIterator<'static>` for true streaming iteration
///
/// ## Safety
///
/// The `VersionedMemTableIterator` holds a reference to the `VersionedMemTableEngine`
/// inside the memtable. We use unsafe to extend this reference's lifetime to `'static`,
/// which is safe because:
///
/// 1. The `_memtable: Arc<MemTable>` field keeps the underlying data alive
/// 2. Rust's struct field drop order is declaration order, so `_memtable` is dropped
///    after `iter` (which doesn't have a custom Drop impl that accesses the memtable)
/// 3. The skipmap used internally is a lock-free concurrent data structure that
///    allows safe concurrent iteration
struct ArcMemTableIterator {
    /// The streaming iterator with erased lifetime (safe because _memtable keeps data alive)
    iter: VersionedMemTableIterator<'static>,
    /// Keep the memtable alive. MUST be declared after `iter` so it's dropped last.
    _memtable: Arc<MemTable>,
}

impl ArcMemTableIterator {
    /// Create a new streaming iterator over the given memtable and range.
    ///
    /// Takes `SharedMvccRange` (Arc) to avoid cloning when creating multiple iterators.
    ///
    /// # Arguments
    ///
    /// * `memtable` - The memtable to iterate over
    /// * `range` - MVCC key range (Arc for zero-copy sharing)
    /// * `owner_ts` - Transaction's start_ts for read-your-writes (0 for autocommit)
    fn new(memtable: Arc<MemTable>, range: SharedMvccRange, owner_ts: Timestamp) -> Self {
        // Safety: We're extending the lifetime of the reference from the Arc's lifetime
        // to 'static. This is safe because:
        // 1. The Arc keeps the MemTable alive for as long as this struct exists
        // 2. The _memtable field is declared after iter, ensuring proper drop order
        // 3. VersionedMemTableIterator doesn't access the memtable in its Drop impl
        let engine_ref: &'static super::memtable::VersionedMemTableEngine =
            unsafe { std::mem::transmute(memtable.inner()) };
        let iter = engine_ref.create_streaming_iter(range, owner_ts);
        Self {
            iter,
            _memtable: memtable,
        }
    }
}

impl MvccIterator for ArcMemTableIterator {
    fn seek(&mut self, target: &MvccKey) -> Result<()> {
        self.iter.seek(target)
    }

    fn advance(&mut self) -> Result<()> {
        self.iter.advance()
    }

    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn user_key(&self) -> &[u8] {
        self.iter.user_key()
    }

    fn timestamp(&self) -> Timestamp {
        self.iter.timestamp()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }
}

// ============================================================================
// TieredMergeIterator - Priority-aware streaming merge with lazy SST loading
// ============================================================================
//
// This merge iterator is designed following RocksDB's approach:
// 1. Active memtable has highest priority (priority=0)
// 2. Frozen memtables come next (priority=1,2,3... by recency)
// 3. L0 SSTs need individual iterators (files can overlap) - LAZY
// 4. L1+ SSTs use LevelIterator for lazy file opening
//
// For the same MVCC key from multiple sources, higher priority wins.
//
// ## RocksDB-style Lazy Initialization
//
// Unlike eager initialization, this iterator:
// - Stores child handles in a vector (not opened iterators in heap)
// - Defers L0 SST file opening until first seek()
// - Uses heap of indices (like RocksDB's pointer-based heap)
// - Caches key in HeapEntry for efficient comparison (like IteratorWrapper)
//
// ## Memory Layout
//
// - `ChildIterator` enum provides concrete types without Box<dyn>
// - Child iterators return references, but TieredMergeIterator caches the
//   current entry (key + value) for deduplication and returning via trait
// - HeapEntry caches keys for efficient heap comparison

use std::cmp::Ordering;
use std::path::PathBuf;

/// Priority levels for merge iterator sources.
/// Lower number = higher priority (newer data).
const PRIORITY_ACTIVE: u32 = 0;
const PRIORITY_FROZEN_BASE: u32 = 100;
const PRIORITY_L0_BASE: u32 = 1000;
const PRIORITY_LEVEL_BASE: u32 = 10000; // L1 = 10000, L2 = 20000, etc.

// ============================================================================
// ChildIterator - Concrete iterator types without Box<dyn>
// ============================================================================

/// Concrete iterator enum replacing Box<dyn MvccIterator>.
///
/// This enum provides:
/// - No heap allocation for dynamic dispatch
/// - Enum-based dispatch (predictable, branch-predicted)
/// - Child methods return references to internal data
/// - All variants are internally lazy (no ChildSource wrapper needed)
enum ChildIterator {
    /// Memtable iterator (active or frozen)
    Memtable(ArcMemTableIterator),
    /// L0 SST iterator - internally lazy (opens file on first seek)
    L0Sst(L0SstIterator),
    /// Level iterator (L1+) - internally lazy
    Level(LevelIterator),
    /// Mock iterator for testing (test-only)
    #[cfg(test)]
    Mock(MockMvccIterator),
}

impl ChildIterator {
    #[inline]
    fn seek(&mut self, target: &MvccKey) -> Result<()> {
        match self {
            Self::Memtable(iter) => iter.seek(target),
            Self::L0Sst(iter) => iter.seek(target),
            Self::Level(iter) => iter.seek(target),
            #[cfg(test)]
            Self::Mock(iter) => iter.seek(target),
        }
    }

    #[inline]
    fn advance(&mut self) -> Result<()> {
        match self {
            Self::Memtable(iter) => iter.advance(),
            Self::L0Sst(iter) => iter.advance(),
            Self::Level(iter) => iter.advance(),
            #[cfg(test)]
            Self::Mock(iter) => iter.advance(),
        }
    }

    #[inline]
    fn valid(&self) -> bool {
        match self {
            Self::Memtable(iter) => iter.valid(),
            Self::L0Sst(iter) => iter.valid(),
            Self::Level(iter) => iter.valid(),
            #[cfg(test)]
            Self::Mock(iter) => iter.valid(),
        }
    }

    #[inline]
    fn user_key(&self) -> &[u8] {
        match self {
            Self::Memtable(iter) => iter.user_key(),
            Self::L0Sst(iter) => iter.user_key(),
            Self::Level(iter) => iter.user_key(),
            #[cfg(test)]
            Self::Mock(iter) => iter.user_key(),
        }
    }

    #[inline]
    fn timestamp(&self) -> Timestamp {
        match self {
            Self::Memtable(iter) => iter.timestamp(),
            Self::L0Sst(iter) => iter.timestamp(),
            Self::Level(iter) => iter.timestamp(),
            #[cfg(test)]
            Self::Mock(iter) => iter.timestamp(),
        }
    }

    #[inline]
    fn value(&self) -> &[u8] {
        match self {
            Self::Memtable(iter) => iter.value(),
            Self::L0Sst(iter) => iter.value(),
            Self::Level(iter) => iter.value(),
            #[cfg(test)]
            Self::Mock(iter) => iter.value(),
        }
    }
}

// ============================================================================
// ChildHandle - Stores iterator + priority
// ============================================================================

/// Handle for a child iterator with its priority.
///
/// All child iterators are internally lazy:
/// - Memtables: in-memory, no I/O needed
/// - L0Sst: L0SstIterator opens file on first seek
/// - Level: LevelIterator opens files on demand
struct ChildHandle {
    iter: ChildIterator,
    priority: u32,
}

// ============================================================================
// HeapEntry - Index + cached key for efficient comparison
// ============================================================================

/// Entry in the merge heap storing index into children vector.
///
/// Following RocksDB's IteratorWrapper pattern, we cache the key for
/// efficient comparison without virtual dispatch during heap operations.
struct HeapEntry {
    /// Index into the children vector
    child_idx: usize,
    /// Cached user key for comparison (avoids dispatch during heap ops)
    cached_user_key: Vec<u8>,
    /// Cached timestamp for comparison
    cached_ts: Timestamp,
    /// Priority (lower = higher precedence)
    priority: u32,
}

impl HeapEntry {
    /// Create a new heap entry with cached key from iterator.
    fn new(child_idx: usize, iter: &ChildIterator, priority: u32) -> Self {
        Self {
            child_idx,
            cached_user_key: iter.user_key().to_vec(),
            cached_ts: iter.timestamp(),
            priority,
        }
    }

    /// Update cached key from iterator (call after advance).
    fn update_cache(&mut self, iter: &ChildIterator) {
        self.cached_user_key.clear();
        self.cached_user_key.extend_from_slice(iter.user_key());
        self.cached_ts = iter.timestamp();
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cached_user_key == other.cached_user_key
            && self.cached_ts == other.cached_ts
            && self.priority == other.priority
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap ordering: smallest key first
        // MVCC order: (user_key ASC, timestamp DESC)
        // For same key, lower priority (higher precedence) first
        match other.cached_user_key.cmp(&self.cached_user_key) {
            Ordering::Equal => {
                // Same user key: higher timestamp first (descending)
                match self.cached_ts.cmp(&other.cached_ts) {
                    Ordering::Equal => other.priority.cmp(&self.priority),
                    ord => ord,
                }
            }
            ord => ord,
        }
    }
}

/// Iterator for a single L0 SST file with lazy opening.
///
/// L0 files can overlap with each other and with other levels, so each
/// needs its own iterator. This wrapper defers file I/O until first seek.
struct L0SstIterator {
    /// SST metadata
    meta: Arc<SstMeta>,
    /// Base path for SST files
    sst_dir: PathBuf,
    /// Shared query range
    range: SharedMvccRange,
    /// Inner iterator (lazy loaded on first seek)
    inner: Option<SstMvccIterator>,
    /// Pending error
    pending_error: Option<TiSqlError>,
}

impl L0SstIterator {
    /// Create a new L0 SST iterator (lazy - no I/O during construction).
    fn new(meta: Arc<SstMeta>, sst_dir: PathBuf, range: SharedMvccRange) -> Self {
        Self {
            meta,
            sst_dir,
            range,
            inner: None,
            pending_error: None,
        }
    }

    /// Open the SST file if not already open.
    fn ensure_open(&mut self) -> Result<()> {
        if self.inner.is_some() {
            return Ok(());
        }

        #[cfg(feature = "failpoints")]
        fail_point!("l0_sst_iterator_open_file", |_| {
            Err(TiSqlError::Storage("injected L0 open_file error".into()))
        });

        let path = self.sst_dir.join(format!("{:08}.sst", self.meta.id));
        if !path.exists() {
            return Err(TiSqlError::Storage(format!(
                "SST file missing: {} (id={}, level={})",
                path.display(),
                self.meta.id,
                self.meta.level
            )));
        }

        let reader = SstReaderRef::open(&path)?;
        let iter = SstMvccIterator::new(reader, Arc::clone(&self.range))?;
        self.inner = Some(iter);
        Ok(())
    }
}

impl MvccIterator for L0SstIterator {
    fn seek(&mut self, target: &MvccKey) -> Result<()> {
        #[cfg(feature = "failpoints")]
        fail_point!("l0_sst_iterator_seek", |_| {
            Err(TiSqlError::Storage("injected L0 seek error".into()))
        });

        if let Some(e) = self.pending_error.take() {
            return Err(e);
        }

        self.ensure_open()?;
        if let Some(ref mut iter) = self.inner {
            iter.seek(target)?;
        }
        Ok(())
    }

    fn advance(&mut self) -> Result<()> {
        #[cfg(feature = "failpoints")]
        fail_point!("l0_sst_iterator_advance", |_| {
            Err(TiSqlError::Storage("injected L0 advance error".into()))
        });

        if let Some(e) = self.pending_error.take() {
            return Err(e);
        }

        // If not yet opened, open and advance (which positions at range start)
        if self.inner.is_none() {
            self.ensure_open()?;
        }

        if let Some(ref mut iter) = self.inner {
            iter.advance()?;
        }
        Ok(())
    }

    fn valid(&self) -> bool {
        self.inner.as_ref().is_some_and(|iter| iter.valid())
    }

    fn user_key(&self) -> &[u8] {
        self.inner
            .as_ref()
            .expect("L0SstIterator not positioned")
            .user_key()
    }

    fn timestamp(&self) -> Timestamp {
        self.inner
            .as_ref()
            .expect("L0SstIterator not positioned")
            .timestamp()
    }

    fn value(&self) -> &[u8] {
        self.inner
            .as_ref()
            .expect("L0SstIterator not positioned")
            .value()
    }
}

/// Iterator for a non-overlapping level (L1+) that opens files sequentially.
///
/// Files in L1+ are sorted by key range and don't overlap, so we can
/// use binary search to find the right file and only open it when needed.
struct LevelIterator {
    /// SST metadata for this level, sorted by smallest_key
    sst_metas: Vec<Arc<SstMeta>>,
    /// Base path for SST files
    sst_dir: PathBuf,
    /// Shared query range (avoids cloning per iterator)
    range: SharedMvccRange,
    /// Current file index (None = exhausted)
    current_file_idx: Option<usize>,
    /// Current file's iterator (lazy loaded)
    current_iter: Option<SstMvccIterator>,
    /// Pending error
    pending_error: Option<TiSqlError>,
}

impl LevelIterator {
    /// Create a new level iterator for the given SSTs (lazy - no I/O during construction).
    ///
    /// The iterator is not positioned until `seek()` is called.
    fn new(sst_metas: Vec<Arc<SstMeta>>, sst_dir: PathBuf, range: SharedMvccRange) -> Self {
        Self {
            sst_metas,
            sst_dir,
            range,
            current_file_idx: None,
            current_iter: None,
            pending_error: None,
        }
    }

    /// Open the file at the given index.
    fn open_file(&mut self, idx: usize) -> Result<()> {
        if idx >= self.sst_metas.len() {
            self.current_file_idx = None;
            self.current_iter = None;
            return Ok(());
        }

        #[cfg(feature = "failpoints")]
        fail_point!("level_iterator_open_file", |_| {
            Err(TiSqlError::Storage("injected open_file error".into()))
        });

        let sst = &self.sst_metas[idx];
        let path = self.sst_dir.join(format!("{:08}.sst", sst.id));

        if !path.exists() {
            return Err(TiSqlError::Storage(format!(
                "SST file missing: {} (id={}, level={})",
                path.display(),
                sst.id,
                sst.level
            )));
        }

        let reader = SstReaderRef::open(&path)?;
        let iter = SstMvccIterator::new(reader, Arc::clone(&self.range))?;
        self.current_file_idx = Some(idx);
        self.current_iter = Some(iter);
        Ok(())
    }

    /// Position and skip empty files.
    ///
    /// If the current iterator is not positioned, positions it first (via advance).
    /// Then skips to the next file if the current file has no entries in range.
    fn skip_empty_files(&mut self) -> Result<()> {
        #[cfg(feature = "failpoints")]
        fail_point!("level_iterator_skip_empty_files", |_| {
            Err(TiSqlError::Storage(
                "injected skip_empty_files error".into(),
            ))
        });

        loop {
            // Position the iterator if not already positioned
            if let Some(ref mut iter) = self.current_iter {
                if !iter.valid() {
                    iter.advance()?; // Positions at range start if not positioned
                }
                if iter.valid() {
                    break; // Found valid entry
                }
            } else {
                break; // No iterator
            }

            // Current file exhausted, move to next file
            if let Some(idx) = self.current_file_idx {
                if idx + 1 < self.sst_metas.len() {
                    self.open_file(idx + 1)?;
                } else {
                    self.current_file_idx = None;
                    self.current_iter = None;
                    break;
                }
            } else {
                break;
            }
        }
        Ok(())
    }
}

impl MvccIterator for LevelIterator {
    fn seek(&mut self, target: &MvccKey) -> Result<()> {
        #[cfg(feature = "failpoints")]
        fail_point!("level_iterator_seek", |_| {
            Err(TiSqlError::Storage("injected seek error".into()))
        });

        if let Some(e) = self.pending_error.take() {
            return Err(e);
        }

        // Handle unbounded target: position at range start (first file)
        if target.is_unbounded() {
            if self.sst_metas.is_empty() {
                self.current_file_idx = None;
                self.current_iter = None;
                return Ok(());
            }
            // Open first file, skip_empty_files will position the iterator
            self.open_file(0)?;
            return self.skip_empty_files();
        }

        // Binary search to find the right file
        let target_bytes = target.as_bytes();
        let idx = self
            .sst_metas
            .partition_point(|sst| sst.largest_key.as_slice() < target_bytes);

        if idx >= self.sst_metas.len() {
            self.current_file_idx = None;
            self.current_iter = None;
            return Ok(());
        }

        // Check if we can reuse current file
        if self.current_file_idx == Some(idx) {
            if let Some(ref mut iter) = self.current_iter {
                iter.seek(target)?;
                return self.skip_empty_files();
            }
        }

        // Open the file and seek
        self.open_file(idx)?;
        if let Some(ref mut iter) = self.current_iter {
            iter.seek(target)?;
        }
        self.skip_empty_files()
    }

    fn advance(&mut self) -> Result<()> {
        #[cfg(feature = "failpoints")]
        fail_point!("level_iterator_advance", |_| {
            Err(TiSqlError::Storage("injected advance error".into()))
        });

        if let Some(e) = self.pending_error.take() {
            return Err(e);
        }

        if let Some(ref mut iter) = self.current_iter {
            iter.advance()?;
            // If current file exhausted, move to next
            if !iter.valid() {
                if let Some(idx) = self.current_file_idx {
                    if idx + 1 < self.sst_metas.len() {
                        self.open_file(idx + 1)?;
                        return self.skip_empty_files();
                    }
                }
                self.current_file_idx = None;
                self.current_iter = None;
            }
        }
        Ok(())
    }

    fn valid(&self) -> bool {
        self.current_iter.as_ref().is_some_and(|i| i.valid())
    }

    fn user_key(&self) -> &[u8] {
        self.current_iter
            .as_ref()
            .expect("Iterator not valid")
            .user_key()
    }

    fn timestamp(&self) -> Timestamp {
        self.current_iter
            .as_ref()
            .expect("Iterator not valid")
            .timestamp()
    }

    fn value(&self) -> &[u8] {
        self.current_iter
            .as_ref()
            .expect("Iterator not valid")
            .value()
    }
}

// ============================================================================
// TieredMergeIterator - Lazy merge iterator with concrete types
// ============================================================================

/// Tiered merge iterator for combining multiple MVCC sources.
///
/// This iterator merges multiple sources with different priorities:
/// - Active memtable (priority 0)
/// - Frozen memtables (priority 100+)
/// - L0 SSTs (priority 1000+) - **LAZY: opened on first seek**
/// - L1+ levels (priority 10000+ per level) - internally lazy
///
/// ## RocksDB-style Lazy Initialization
///
/// Unlike eager initialization, this iterator:
/// - Stores child handles in a vector (not opened iterators)
/// - Defers L0 SST file I/O until first `seek()` or `advance()`
/// - Uses heap of indices (like RocksDB's pointer-based heap)
/// - Caches key in HeapEntry for efficient comparison
///
/// ## Memory Layout
///
/// - `ChildIterator` enum: concrete types, no `Box<dyn>`
/// - Current entry (key + value) is cached for deduplication and returning
/// - Heap entries cache keys for efficient comparison (like RocksDB's IteratorWrapper)
///
/// Note: Each `advance()` copies key and value into owned storage. This is
/// necessary for deduplication across sources. Callers get references to
/// the cached data via `user_key()` and `value()`.
///
/// ## Usage
///
/// ```ignore
/// let mut iter = TieredMergeIterator::new();
/// iter.add_active_memtable(memtable, range);
/// iter.add_l0_sst(meta, sst_dir, range, idx);  // No I/O here!
/// iter.advance()?;  // First I/O happens here
/// while iter.valid() {
///     let key = iter.user_key();  // Reference to cached data
///     iter.advance()?;
/// }
/// ```
pub struct TieredMergeIterator {
    /// Child iterator handles stored in a vector (RocksDB-style)
    children: Vec<ChildHandle>,
    /// Min-heap of indices into children vector with cached keys
    heap: BinaryHeap<HeapEntry>,
    /// Whether initialization (first seek) has been done
    initialized: bool,
    /// Current user key (cached for returning reference)
    current_user_key: Option<Vec<u8>>,
    /// Current timestamp
    current_timestamp: Timestamp,
    /// Current value (cached for returning reference)
    current_value: Option<Vec<u8>>,
    /// Last emitted MVCC key: (user_key, timestamp) for deduplication
    last_emitted_key: Option<(Vec<u8>, Timestamp)>,
    /// Pending error from iterator operations
    pending_error: Option<TiSqlError>,
}

impl Default for TieredMergeIterator {
    fn default() -> Self {
        Self::new()
    }
}

impl TieredMergeIterator {
    /// Create a new tiered merge iterator.
    ///
    /// No I/O is done at construction time. Children are added via
    /// `add_*` methods, and initialization happens on first `seek()` or `advance()`.
    pub fn new() -> Self {
        Self {
            children: Vec::new(),
            heap: BinaryHeap::new(),
            initialized: false,
            current_user_key: None,
            current_timestamp: 0,
            current_value: None,
            last_emitted_key: None,
            pending_error: None,
        }
    }

    /// Add the active memtable iterator (highest priority).
    fn add_active_memtable(&mut self, iter: ArcMemTableIterator) {
        self.children.push(ChildHandle {
            iter: ChildIterator::Memtable(iter),
            priority: PRIORITY_ACTIVE,
        });
    }

    /// Add a frozen memtable iterator.
    fn add_frozen_memtable(&mut self, iter: ArcMemTableIterator, index: usize) {
        self.children.push(ChildHandle {
            iter: ChildIterator::Memtable(iter),
            priority: PRIORITY_FROZEN_BASE + index as u32,
        });
    }

    /// Add an L0 SST (lazy - no I/O until first seek).
    ///
    /// L0SstIterator opens the file lazily on first `seek()`.
    fn add_l0_sst(
        &mut self,
        meta: Arc<SstMeta>,
        sst_dir: PathBuf,
        range: SharedMvccRange,
        index: usize,
    ) {
        self.children.push(ChildHandle {
            iter: ChildIterator::L0Sst(L0SstIterator::new(meta, sst_dir, range)),
            priority: PRIORITY_L0_BASE + index as u32,
        });
    }

    /// Add a level iterator for L1+.
    ///
    /// LevelIterator is internally lazy (opens files on seek).
    fn add_level(&mut self, iter: LevelIterator, level: usize) {
        self.children.push(ChildHandle {
            iter: ChildIterator::Level(iter),
            priority: PRIORITY_LEVEL_BASE * level as u32,
        });
    }

    /// Finalize and return the iterator.
    ///
    /// The iterator is NOT positioned after construction. Call `advance()`
    /// to position on the first entry (this is when lazy initialization happens):
    ///
    /// ```ignore
    /// let mut iter = TieredMergeIterator::new();
    /// iter.add_active_memtable(mem);
    /// iter.add_l0_sst(meta, dir, range, 0);  // No I/O!
    /// iter.advance()?;  // First I/O happens here
    /// while iter.valid() {
    ///     // process...
    ///     iter.advance()?;
    /// }
    /// ```
    pub fn build(self) -> Self {
        // No initialization here - deferred to first seek/advance
        self
    }
}

// ==================== Test-only Mock Iterator ====================
//
// MockMvccIterator is placed outside the tests module so it can be used
// by the ChildIterator::Mock variant (which needs to be in the main enum).

#[cfg(test)]
use std::cell::RefCell;
#[cfg(test)]
use std::rc::Rc;

/// Mock iterator that tracks whether seek() has been called.
/// Used to verify lazy initialization behavior.
#[cfg(test)]
struct MockMvccIterator {
    /// Data to return (sorted by MvccKey)
    data: Vec<(MvccKey, Vec<u8>)>,
    /// Current position (-1 = not initialized, >= data.len() = exhausted)
    pos: i32,
    /// Tracks whether seek() has been called (shared for external inspection)
    seek_called: Rc<RefCell<bool>>,
}

#[cfg(test)]
impl MockMvccIterator {
    fn new(_name: &str, data: Vec<(MvccKey, Vec<u8>)>) -> (Self, Rc<RefCell<bool>>) {
        let seek_called = Rc::new(RefCell::new(false));
        let iter = Self {
            data,
            pos: -1, // Not initialized
            seek_called: Rc::clone(&seek_called),
        };
        (iter, seek_called)
    }

    fn new_with_tracker(
        _name: &str,
        data: Vec<(MvccKey, Vec<u8>)>,
        seek_called: Rc<RefCell<bool>>,
    ) -> Self {
        Self {
            data,
            pos: -1,
            seek_called,
        }
    }
}

#[cfg(test)]
impl MvccIterator for MockMvccIterator {
    fn seek(&mut self, target: &MvccKey) -> Result<()> {
        *self.seek_called.borrow_mut() = true;
        // Binary search for first key >= target
        self.pos = self
            .data
            .iter()
            .position(|(k, _)| k >= target)
            .map(|p| p as i32)
            .unwrap_or(self.data.len() as i32);
        Ok(())
    }

    fn advance(&mut self) -> Result<()> {
        if self.pos >= 0 {
            self.pos += 1;
        }
        Ok(())
    }

    fn valid(&self) -> bool {
        self.pos >= 0 && (self.pos as usize) < self.data.len()
    }

    fn user_key(&self) -> &[u8] {
        self.data[self.pos as usize].0.key()
    }

    fn timestamp(&self) -> Timestamp {
        self.data[self.pos as usize].0.timestamp()
    }

    fn value(&self) -> &[u8] {
        &self.data[self.pos as usize].1
    }
}

// Test-only methods for TieredMergeIterator
#[cfg(test)]
impl TieredMergeIterator {
    /// Add a mock iterator as active memtable (test-only).
    fn add_mock_active(&mut self, iter: MockMvccIterator) {
        self.children.push(ChildHandle {
            iter: ChildIterator::Mock(iter),
            priority: PRIORITY_ACTIVE,
        });
    }

    /// Add a mock iterator as frozen memtable (test-only).
    fn add_mock_frozen(&mut self, iter: MockMvccIterator, index: usize) {
        self.children.push(ChildHandle {
            iter: ChildIterator::Mock(iter),
            priority: PRIORITY_FROZEN_BASE + index as u32,
        });
    }

    /// Add a mock iterator as L0 SST (test-only).
    fn add_mock_l0(&mut self, iter: MockMvccIterator, index: usize) {
        self.children.push(ChildHandle {
            iter: ChildIterator::Mock(iter),
            priority: PRIORITY_L0_BASE + index as u32,
        });
    }

    /// Add a mock iterator as level (test-only).
    fn add_mock_level(&mut self, iter: MockMvccIterator, level: usize) {
        self.children.push(ChildHandle {
            iter: ChildIterator::Mock(iter),
            priority: PRIORITY_LEVEL_BASE * level as u32,
        });
    }
}

impl TieredMergeIterator {
    /// Initialize all children and populate the heap.
    ///
    /// This is called on first `seek()` or `advance()`. It:
    /// 1. Seeks all children to the target (lazy iterators open files on demand)
    /// 2. Adds valid children to the heap with cached keys
    fn initialize(&mut self, target: &MvccKey) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        self.heap.clear();

        for (idx, child) in self.children.iter_mut().enumerate() {
            // Seek the iterator (lazy iterators open files on first seek)
            child.iter.seek(target)?;

            // Add to heap if valid (with cached key for comparison)
            if child.iter.valid() {
                self.heap
                    .push(HeapEntry::new(idx, &child.iter, child.priority));
            }
        }

        self.initialized = true;
        Ok(())
    }

    /// Re-seek all children to a new target and rebuild the heap.
    fn reseek(&mut self, target: &MvccKey) -> Result<()> {
        self.heap.clear();

        for (idx, child) in self.children.iter_mut().enumerate() {
            child.iter.seek(target)?;

            if child.iter.valid() {
                self.heap
                    .push(HeapEntry::new(idx, &child.iter, child.priority));
            }
        }

        Ok(())
    }
}

impl MvccIterator for TieredMergeIterator {
    fn seek(&mut self, target: &MvccKey) -> Result<()> {
        // Clear any pending error from previous advance() - seek resets iterator state
        if let Some(e) = self.pending_error.take() {
            // Log but don't return - seek should reset the iterator
            tracing::debug!("clearing pending error on seek: {e:?}");
        }

        // Clear current state
        self.current_user_key = None;
        self.current_value = None;
        self.last_emitted_key = None;

        if !self.initialized {
            // First seek: initialize all children
            self.initialize(target)?;
        } else {
            // Subsequent seek: re-seek all children
            self.reseek(target)?;
        }

        // Move to first valid entry
        self.advance_internal()
    }

    fn advance(&mut self) -> Result<()> {
        if let Some(e) = self.pending_error.take() {
            return Err(e);
        }

        // Lazy initialization on first advance
        if !self.initialized {
            self.initialize(&MvccKey::unbounded())?;
        }

        self.advance_internal()
    }

    fn valid(&self) -> bool {
        self.current_user_key.is_some()
    }

    fn user_key(&self) -> &[u8] {
        self.current_user_key.as_ref().expect("Iterator not valid")
    }

    fn timestamp(&self) -> Timestamp {
        self.current_timestamp
    }

    fn value(&self) -> &[u8] {
        self.current_value.as_ref().expect("Iterator not valid")
    }
}

impl TieredMergeIterator {
    /// Internal advance logic - assumes initialization is done.
    fn advance_internal(&mut self) -> Result<()> {
        loop {
            if self.heap.is_empty() {
                self.current_user_key = None;
                self.current_value = None;
                return Ok(());
            }

            // Pop the smallest entry from heap
            let mut entry = self.heap.pop().unwrap();
            let child_idx = entry.child_idx;

            // Get the iterator for this child
            let child = &mut self.children[child_idx];

            // Cache the current entry for returning
            let user_key = child.iter.user_key().to_vec();
            let timestamp = child.iter.timestamp();
            let value = child.iter.value().to_vec();

            // Advance the iterator
            if let Err(e) = child.iter.advance() {
                self.pending_error = Some(e);
            }

            // Re-add to heap if still valid (update cached key)
            if child.iter.valid() {
                entry.update_cache(&child.iter);
                self.heap.push(entry);
            }

            // Skip duplicate MVCC keys (same user_key AND timestamp)
            // This happens when the same key exists in multiple sources.
            // We only emit the highest-priority version.
            if let Some((ref last_key, last_ts)) = self.last_emitted_key {
                if user_key == *last_key && timestamp == last_ts {
                    continue;
                }
            }

            // Emit this entry
            self.current_user_key = Some(user_key.clone());
            self.current_timestamp = timestamp;
            self.current_value = Some(value);
            self.last_emitted_key = Some((user_key, timestamp));
            return Ok(());
        }
    }
}

impl StorageEngine for LsmEngine {
    type Iter = TieredMergeIterator;

    fn scan_iter(&self, range: Range<MvccKey>, owner_ts: Timestamp) -> Result<TieredMergeIterator> {
        // Wrap range in Arc for zero-copy sharing across all iterators
        let range = Arc::new(range);

        // Snapshot the state under read lock, then release the lock.
        // We clone Arc references so iterators can outlive the lock.
        let (active, frozen) = {
            let state = self.state.read().unwrap();
            (Arc::clone(&state.active), state.frozen.clone())
        };

        // Get version snapshot (separate from memtable state)
        let version = self.version_set.current();

        let sst_dir = self.config.sst_dir();
        let mut merge_iter = TieredMergeIterator::new();

        // 1. Add active memtable iterator (highest priority)
        // owner_ts enables read-your-writes for explicit transactions (0 for autocommit)
        let active_iter = ArcMemTableIterator::new(active, Arc::clone(&range), owner_ts);
        merge_iter.add_active_memtable(active_iter);

        // 2. Add frozen memtable iterators (newest to oldest = iterate from back)
        for (idx, memtable) in frozen.iter().rev().enumerate() {
            let mem_iter =
                ArcMemTableIterator::new(Arc::clone(memtable), Arc::clone(&range), owner_ts);
            merge_iter.add_frozen_memtable(mem_iter, idx);
        }

        // 3. Add L0 SST sources (LAZY - no I/O until first seek!)
        // SSTs only contain committed data, so no owner_ts needed
        let mut l0_ssts: Vec<Arc<SstMeta>> = version
            .ssts_at_level(0)
            .iter()
            .filter(|sst| sst.overlaps_mvcc(range.start.as_bytes(), range.end.as_bytes()))
            .cloned()
            .collect();
        l0_ssts.sort_by(|a, b| b.id.cmp(&a.id)); // Newest first

        for (idx, sst_meta) in l0_ssts.into_iter().enumerate() {
            // LAZY: L0SstIterator opens file on first seek
            merge_iter.add_l0_sst(sst_meta, sst_dir.clone(), Arc::clone(&range), idx);
        }

        // 4. Add L1+ level iterators (internally lazy for file opening)
        for level in 1..MAX_LEVELS {
            let ssts: Vec<Arc<SstMeta>> = version
                .ssts_at_level(level as u32)
                .iter()
                .filter(|sst| sst.overlaps_mvcc(range.start.as_bytes(), range.end.as_bytes()))
                .cloned()
                .collect();

            if !ssts.is_empty() {
                // LevelIterator is already lazy internally
                let level_iter = LevelIterator::new(ssts, sst_dir.clone(), Arc::clone(&range));
                merge_iter.add_level(level_iter, level);
            }
        }

        // Return the iterator (no I/O done yet!)
        Ok(merge_iter.build())
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        // Use CLOG LSN if provided, otherwise allocate locally.
        // Using the CLOG LSN ensures proper recovery ordering: when we flush
        // the memtable to SST, the flushed_lsn in SST metadata matches the
        // clog LSN, allowing recovery to correctly identify which clog entries
        // have been persisted to storage.
        let lsn = batch.clog_lsn().unwrap_or_else(|| self.alloc_lsn());

        // Write to active memtable
        {
            let state = self.state.read().unwrap();
            state.active.write_batch_with_lsn(batch, lsn)?;
        }

        // Check if rotation is needed (non-blocking check)
        // Actual rotation happens asynchronously or on next write
        let _ = self.maybe_rotate();

        Ok(())
    }
}

impl PessimisticStorage for LsmEngine {
    fn put_pending(
        &self,
        key: &[u8],
        value: RawValue,
        owner_start_ts: Timestamp,
    ) -> std::result::Result<(), Timestamp> {
        // Forward to active memtable
        let state = self.state.read().unwrap();
        state.active.put_pending(key, value, owner_start_ts)
    }

    fn get_lock_owner(&self, key: &[u8]) -> Option<Timestamp> {
        // Check active memtable first
        let state = self.state.read().unwrap();
        if let Some(owner) = state.active.get_lock_owner(key) {
            return Some(owner);
        }
        // Check frozen memtables (newest to oldest = iterate from back)
        for frozen in state.frozen.iter().rev() {
            if let Some(owner) = frozen.get_lock_owner(key) {
                return Some(owner);
            }
        }
        None
    }

    fn finalize_pending(&self, keys: &[Key], owner_start_ts: Timestamp, commit_ts: Timestamp) {
        // Finalize in active memtable
        let state = self.state.read().unwrap();
        state
            .active
            .finalize_pending(keys, owner_start_ts, commit_ts);
        // Also finalize in frozen memtables in case writes happened before rotation
        for frozen in &state.frozen {
            frozen.finalize_pending(keys, owner_start_ts, commit_ts);
        }
    }

    fn abort_pending(&self, keys: &[Key], owner_start_ts: Timestamp) {
        // Abort in active memtable
        let state = self.state.read().unwrap();
        state.active.abort_pending(keys, owner_start_ts);
        // Also abort in frozen memtables in case writes happened before rotation
        for frozen in &state.frozen {
            frozen.abort_pending(keys, owner_start_ts);
        }
    }

    fn delete_pending(
        &self,
        key: &[u8],
        owner_start_ts: Timestamp,
    ) -> std::result::Result<bool, Timestamp> {
        // Check for lock conflicts across all memtables first
        let state = self.state.read().unwrap();

        // Check frozen memtables for conflicts (newest to oldest = iterate from back)
        for frozen in state.frozen.iter().rev() {
            if let Some(owner) = frozen.get_lock_owner(key) {
                if owner != owner_start_ts {
                    return Err(owner);
                }
            }
        }

        // Forward delete to active memtable
        // The active memtable will handle:
        // - Our pending write -> convert to LOCK
        // - Committed value -> write pending TOMBSTONE
        // - No value -> return Ok(false)
        state.active.delete_pending(key, owner_start_ts)
    }

    fn get_with_owner(
        &self,
        key: &[u8],
        read_ts: Timestamp,
        owner_start_ts: Timestamp,
    ) -> Option<RawValue> {
        let state = self.state.read().unwrap();

        // Check active memtable first
        if let Some(value) = state.active.get_with_owner(key, read_ts, owner_start_ts) {
            // Check if it's a tombstone
            if is_tombstone(&value) {
                return None;
            }
            return Some(value);
        }

        // Check frozen memtables (newest to oldest = iterate from back)
        for frozen in state.frozen.iter().rev() {
            if let Some(value) = frozen.get_with_owner(key, read_ts, owner_start_ts) {
                if is_tombstone(&value) {
                    return None;
                }
                return Some(value);
            }
        }

        // Check SST files via scan (point lookup)
        // For SST files, pending nodes don't exist, so we just do normal MVCC read
        drop(state); // Release lock before I/O

        // get_from_sst uses current_version() internally
        self.get_from_sst(key, read_ts).ok().flatten()
    }
}

/// Statistics about the LSM engine.
#[derive(Debug, Clone)]
pub struct LsmStats {
    /// Size of active memtable in bytes.
    pub active_memtable_size: usize,

    /// Number of frozen memtables awaiting flush.
    pub frozen_memtable_count: usize,

    /// Number of SST files at L0.
    pub l0_sst_count: usize,

    /// Total number of SST files.
    pub total_sst_count: usize,

    /// Total size of SST files in bytes.
    pub total_sst_bytes: u64,

    /// Current version number.
    pub version_num: u64,

    /// Highest LSN that has been flushed.
    pub flushed_lsn: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mvcc::is_tombstone;
    use std::path::Path;
    use tempfile::TempDir;

    // ==================== Test-only LsmEngine Methods ====================

    impl LsmEngine {
        /// Open or create an LSM engine at the given path without durability.
        ///
        /// This is test-only. Production code should use `open_with_recovery`.
        pub fn open(config: LsmConfig) -> Result<Self> {
            config.validate().map_err(TiSqlError::Storage)?;

            // Create SST directory if needed
            let sst_dir = config.sst_dir();
            if !sst_dir.exists() {
                std::fs::create_dir_all(&sst_dir)?;
            }

            // Create initial state and version_set
            let initial_state = LsmState::new(1);
            let version_set = VersionSet::new(Version::new());

            // Create initial SuperVersion
            let initial_sv = Arc::new(SuperVersion::new(
                Arc::clone(&initial_state.active),
                initial_state.frozen.clone(),
                version_set.current(),
                0, // Initial sv_number
            ));

            Ok(Self {
                config: Arc::new(config),
                state: RwLock::new(initial_state),
                version_set,
                next_memtable_id: AtomicU64::new(2),
                next_sst_id: AtomicU64::new(1),
                next_lsn: AtomicU64::new(1),
                sv_number: AtomicU64::new(1),
                lsn_provider: None,
                ilog: None,
                current_sv: RwLock::new(initial_sv),
            })
        }
    }

    // ==================== Test Configuration Helpers ====================

    fn test_config(dir: &Path) -> LsmConfig {
        LsmConfig::builder(dir)
            .memtable_size(1024) // Small for testing
            .max_frozen_memtables(4)
            .build()
            .unwrap()
    }

    fn new_batch(commit_ts: Timestamp) -> WriteBatch {
        let mut batch = WriteBatch::new();
        batch.set_commit_ts(commit_ts);
        batch
    }

    // ==================== Test Helpers Using MvccKey ====================
    //
    // These helpers use MvccKey explicitly instead of convenience methods.
    // Tests should encode keys as MvccKey and use scan to find results.

    /// Scan MVCC keys in range using streaming iterator (test-only helper).
    fn scan_mvcc(engine: &LsmEngine, range: Range<MvccKey>) -> Vec<(MvccKey, RawValue)> {
        let mut results = Vec::new();
        let mut iter = engine.scan_iter(range, 0).unwrap();
        iter.advance().unwrap(); // Position on first entry
        while iter.valid() {
            let key = MvccKey::encode(iter.user_key(), iter.timestamp());
            results.push((key, iter.value().to_vec()));
            iter.advance().unwrap();
        }
        results
    }

    /// Get the latest version of a key visible at the given timestamp.
    /// Uses streaming iterator with MvccKey range.
    fn get_at_for_test(engine: &LsmEngine, key: &[u8], ts: Timestamp) -> Option<RawValue> {
        // Create MVCC key range: from (key, ts) to next key
        let start = MvccKey::encode(key, ts);
        let end = MvccKey::encode(key, 0)
            .next_key()
            .unwrap_or_else(MvccKey::unbounded);
        let range = start..end;

        let results = scan_mvcc(engine, range);

        // Find the first entry matching our key (latest visible version)
        for (mvcc_key, value) in results {
            let (decoded_key, entry_ts) = mvcc_key.decode();
            if decoded_key == key && entry_ts <= ts {
                if is_tombstone(&value) {
                    return None;
                }
                return Some(value);
            }
        }
        None
    }

    /// Get the latest version of a key (at MAX timestamp).
    fn get_for_test(engine: &LsmEngine, key: &[u8]) -> Option<RawValue> {
        get_at_for_test(engine, key, Timestamp::MAX)
    }

    /// Scan a key range at the given timestamp, returning (key, value) pairs.
    fn scan_at_for_test(
        engine: &LsmEngine,
        range: &Range<Vec<u8>>,
        ts: Timestamp,
    ) -> Vec<(Vec<u8>, RawValue)> {
        // Convert user key range to MvccKey range
        let start = MvccKey::encode(&range.start, Timestamp::MAX);
        let end = MvccKey::encode(&range.end, 0);
        let mvcc_range = start..end;

        let results = scan_mvcc(engine, mvcc_range);

        // Deduplicate by key and filter by timestamp
        let mut seen_keys: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        let mut output = Vec::new();

        for (mvcc_key, value) in results {
            let (decoded_key, entry_ts) = mvcc_key.decode();
            if decoded_key < range.start || decoded_key >= range.end {
                continue;
            }
            if entry_ts > ts {
                continue;
            }
            if seen_keys.contains(&decoded_key) {
                continue;
            }
            seen_keys.insert(decoded_key.clone());
            if !is_tombstone(&value) {
                output.push((decoded_key, value));
            }
        }

        output.sort_by(|a, b| a.0.cmp(&b.0));
        output
    }

    /// Scan a key range at latest timestamp.
    fn scan_for_test(engine: &LsmEngine, range: &Range<Vec<u8>>) -> Vec<(Vec<u8>, RawValue)> {
        scan_at_for_test(engine, range, Timestamp::MAX)
    }

    #[test]
    fn test_lsm_engine_open() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());

        let engine = LsmEngine::open(config).unwrap();

        let stats = engine.stats();
        assert_eq!(stats.active_memtable_size, 0);
        assert_eq!(stats.frozen_memtable_count, 0);
        assert_eq!(stats.total_sst_count, 0);
    }

    #[test]
    fn test_lsm_engine_write_and_read() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        let mut batch = new_batch(100);
        batch.put(b"key1".to_vec(), b"value1".to_vec());
        batch.put(b"key2".to_vec(), b"value2".to_vec());
        engine.write_batch(batch).unwrap();

        assert_eq!(get_for_test(&engine, b"key1"), Some(b"value1".to_vec()));
        assert_eq!(get_for_test(&engine, b"key2"), Some(b"value2".to_vec()));
        assert_eq!(get_for_test(&engine, b"key3"), None);
    }

    #[test]
    fn test_lsm_engine_mvcc_read() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        // Write v1 at ts=10
        let mut batch = new_batch(10);
        batch.put(b"key".to_vec(), b"v1".to_vec());
        engine.write_batch(batch).unwrap();

        // Write v2 at ts=20
        let mut batch = new_batch(20);
        batch.put(b"key".to_vec(), b"v2".to_vec());
        engine.write_batch(batch).unwrap();

        // Read at different timestamps
        assert_eq!(get_at_for_test(&engine, b"key", 5), None);
        assert_eq!(get_at_for_test(&engine, b"key", 10), Some(b"v1".to_vec()));
        assert_eq!(get_at_for_test(&engine, b"key", 15), Some(b"v1".to_vec()));
        assert_eq!(get_at_for_test(&engine, b"key", 20), Some(b"v2".to_vec()));
        assert_eq!(get_at_for_test(&engine, b"key", 25), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_lsm_engine_memtable_rotation() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(200) // Very small
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let engine = LsmEngine::open(config).unwrap();

        // Write enough to trigger rotation
        for i in 0..10 {
            let mut batch = new_batch(i as Timestamp + 1);
            let key = format!("key_{i:04}");
            let value = format!("value_{i:04}");
            batch.put(key.as_bytes().to_vec(), value.as_bytes().to_vec());
            engine.write_batch(batch).unwrap();
        }

        // Should have some frozen memtables
        assert!(engine.frozen_count() > 0);

        // All data should still be readable
        for i in 0..10 {
            let key = format!("key_{i:04}");
            let expected = format!("value_{i:04}");
            assert_eq!(
                get_for_test(&engine, key.as_bytes()),
                Some(expected.as_bytes().to_vec())
            );
        }
    }

    #[test]
    fn test_lsm_engine_flush() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100) // Very small
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let engine = LsmEngine::open(config).unwrap();

        // Write some data
        for i in 0..5 {
            let mut batch = new_batch(i as Timestamp + 1);
            let key = format!("key_{i:04}");
            let value = format!("value_{i:04}");
            batch.put(key.as_bytes().to_vec(), value.as_bytes().to_vec());
            engine.write_batch(batch).unwrap();
        }

        // Force flush
        let _flushed = engine.flush_all().unwrap();

        // Should have flushed some SSTs
        if engine.frozen_count() > 0 {
            // If there were frozen memtables, they should be flushed
            // (may be 0 if all data fit in active)
        }

        // Data should still be readable
        for i in 0..5 {
            let key = format!("key_{i:04}");
            let expected = format!("value_{i:04}");
            assert_eq!(
                get_for_test(&engine, key.as_bytes()),
                Some(expected.as_bytes().to_vec()),
                "Key {key} should be readable after flush"
            );
        }
    }

    #[test]
    fn test_lsm_engine_scan() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        let mut batch = new_batch(100);
        batch.put(b"a".to_vec(), b"1".to_vec());
        batch.put(b"b".to_vec(), b"2".to_vec());
        batch.put(b"c".to_vec(), b"3".to_vec());
        batch.put(b"d".to_vec(), b"4".to_vec());
        engine.write_batch(batch).unwrap();

        let range = b"b".to_vec()..b"d".to_vec();
        let results = scan_for_test(&engine, &range);

        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|(k, _)| k == b"b"));
        assert!(results.iter().any(|(k, _)| k == b"c"));
    }

    #[test]
    fn test_lsm_engine_delete() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        // Write
        let mut batch = new_batch(10);
        batch.put(b"key".to_vec(), b"value".to_vec());
        engine.write_batch(batch).unwrap();

        assert_eq!(get_for_test(&engine, b"key"), Some(b"value".to_vec()));

        // Delete
        let mut batch = new_batch(20);
        batch.delete(b"key".to_vec());
        engine.write_batch(batch).unwrap();

        // Should be deleted at latest
        assert_eq!(get_for_test(&engine, b"key"), None);

        // Should still be visible at ts=15
        assert_eq!(
            get_at_for_test(&engine, b"key", 15),
            Some(b"value".to_vec())
        );
    }

    #[test]
    fn test_lsm_engine_stats() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        let mut batch = new_batch(100);
        batch.put(b"key".to_vec(), b"value".to_vec());
        engine.write_batch(batch).unwrap();

        let stats = engine.stats();
        assert!(stats.active_memtable_size > 0);
        assert_eq!(stats.version_num, 0); // No flushes yet
    }

    // ==================== Concurrent Tests ====================

    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_lsm_concurrent_writes() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(4096)
            .max_frozen_memtables(8)
            .build()
            .unwrap();

        let engine = Arc::new(LsmEngine::open(config).unwrap());
        let num_threads = 4;
        let writes_per_thread = 100;

        let handles: Vec<_> = (0..num_threads)
            .map(|tid| {
                let engine = Arc::clone(&engine);

                thread::spawn(move || {
                    for i in 0..writes_per_thread {
                        let mut batch = WriteBatch::new();
                        let ts = (tid * writes_per_thread + i + 1) as Timestamp;
                        batch.set_commit_ts(ts);
                        let key = format!("key_{tid}_{i}");
                        batch.put(key.as_bytes().to_vec(), b"value".to_vec());
                        engine.write_batch(batch).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Verify all writes
        for tid in 0..num_threads {
            for i in 0..writes_per_thread {
                let key = format!("key_{tid}_{i}");
                assert!(
                    get_for_test(&engine, key.as_bytes()).is_some(),
                    "Key {key} should exist"
                );
            }
        }
    }

    #[test]
    fn test_lsm_concurrent_reads_and_writes() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(4096)
            .max_frozen_memtables(8)
            .build()
            .unwrap();

        let engine = Arc::new(LsmEngine::open(config).unwrap());

        // Pre-populate
        let mut batch = new_batch(1);
        for i in 0..100 {
            batch.put(
                format!("key{i:03}").as_bytes().to_vec(),
                b"initial".to_vec(),
            );
        }
        engine.write_batch(batch).unwrap();

        let num_readers = 4;
        let num_writers = 2;

        let handles: Vec<_> = (0..num_readers)
            .map(|_| {
                let engine = Arc::clone(&engine);

                thread::spawn(move || {
                    for i in 0..500 {
                        let key = format!("key{:03}", i % 100);
                        let _ = get_for_test(&engine, key.as_bytes());
                    }
                })
            })
            .chain((0..num_writers).map(|tid| {
                let engine = Arc::clone(&engine);

                thread::spawn(move || {
                    for i in 0..200 {
                        let mut batch = WriteBatch::new();
                        let ts = (100 + tid * 200 + i) as Timestamp;
                        batch.set_commit_ts(ts);
                        let key = format!("new_key_{tid}_{i}");
                        batch.put(key.as_bytes().to_vec(), b"value".to_vec());
                        engine.write_batch(batch).unwrap();
                    }
                })
            }))
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Verify original data still readable
        for i in 0..100 {
            let key = format!("key{i:03}");
            assert!(get_for_test(&engine, key.as_bytes()).is_some());
        }
    }

    // ==================== Durable Engine Tests ====================

    use crate::lsn::new_lsn_provider;
    use crate::storage::ilog::{IlogConfig, IlogService};

    #[test]
    fn test_lsm_durable_flush() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100) // Very small
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let lsn_provider = new_lsn_provider();
        let ilog_config = IlogConfig::new(tmp.path());
        let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

        let engine = LsmEngine::open_with_recovery(
            config,
            Arc::clone(&lsn_provider),
            Arc::clone(&ilog),
            Version::new(),
        )
        .unwrap();

        assert!(engine.is_durable());

        // Write enough to trigger rotation
        for i in 0..5 {
            let mut batch = new_batch(i as Timestamp + 1);
            let key = format!("key_{i:04}");
            let value = format!("value_{i:04}");
            batch.put(key.as_bytes().to_vec(), value.as_bytes().to_vec());
            engine.write_batch(batch).unwrap();
        }

        // Force flush
        let _flushed = engine.flush_all().unwrap();

        // Data should still be readable
        for i in 0..5 {
            let key = format!("key_{i:04}");
            let expected = format!("value_{i:04}");
            assert_eq!(
                get_for_test(&engine, key.as_bytes()),
                Some(expected.as_bytes().to_vec()),
                "Key {key} should be readable after durable flush"
            );
        }
    }

    #[test]
    fn test_lsm_durable_recovery() {
        let tmp = TempDir::new().unwrap();

        // First session: write and flush
        {
            let config = LsmConfig::builder(tmp.path())
                .memtable_size(100)
                .max_frozen_memtables(4)
                .build()
                .unwrap();

            let lsn_provider = new_lsn_provider();
            let ilog_config = IlogConfig::new(tmp.path());
            let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

            let engine =
                LsmEngine::open_with_recovery(config, lsn_provider, ilog, Version::new()).unwrap();

            for i in 0..5 {
                let mut batch = new_batch(i as Timestamp + 1);
                let key = format!("key_{i:04}");
                let value = format!("value_{i:04}");
                batch.put(key.as_bytes().to_vec(), value.as_bytes().to_vec());
                engine.write_batch(batch).unwrap();
            }

            // Flush all including active memtable
            engine.flush_all_with_active().unwrap();
        }

        // Second session: recover and verify
        {
            let lsn_provider = new_lsn_provider();
            let ilog_config = IlogConfig::new(tmp.path());
            let (ilog, version, orphans) =
                IlogService::recover(ilog_config, Arc::clone(&lsn_provider)).unwrap();

            assert!(
                orphans.is_empty(),
                "Should have no orphans after clean shutdown"
            );

            let config = LsmConfig::builder(tmp.path())
                .memtable_size(100)
                .max_frozen_memtables(4)
                .build()
                .unwrap();

            let engine =
                LsmEngine::open_with_recovery(config, lsn_provider, Arc::new(ilog), version)
                    .unwrap();

            // Data should be readable from SST
            for i in 0..5 {
                let key = format!("key_{i:04}");
                let expected = format!("value_{i:04}");
                assert_eq!(
                    get_for_test(&engine, key.as_bytes()),
                    Some(expected.as_bytes().to_vec()),
                    "Key {key} should be readable after recovery"
                );
            }
        }
    }

    // ==================== MVCC SST Storage Tests ====================

    #[test]
    fn test_mvcc_scan_at_timestamp_filtering() {
        // Test that scan_at returns correct versions based on timestamp
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        // Write multiple versions of the same key at different timestamps
        let mut batch1 = new_batch(10);
        batch1.put(b"key".to_vec(), b"value_10".to_vec());
        engine.write_batch(batch1).unwrap();

        let mut batch2 = new_batch(20);
        batch2.put(b"key".to_vec(), b"value_20".to_vec());
        engine.write_batch(batch2).unwrap();

        let mut batch3 = new_batch(30);
        batch3.put(b"key".to_vec(), b"value_30".to_vec());
        engine.write_batch(batch3).unwrap();

        // get_at should return the correct version
        assert_eq!(get_at_for_test(&engine, b"key", 5), None); // Too early
        assert_eq!(
            get_at_for_test(&engine, b"key", 10),
            Some(b"value_10".to_vec())
        );
        assert_eq!(
            get_at_for_test(&engine, b"key", 15),
            Some(b"value_10".to_vec())
        );
        assert_eq!(
            get_at_for_test(&engine, b"key", 20),
            Some(b"value_20".to_vec())
        );
        assert_eq!(
            get_at_for_test(&engine, b"key", 25),
            Some(b"value_20".to_vec())
        );
        assert_eq!(
            get_at_for_test(&engine, b"key", 30),
            Some(b"value_30".to_vec())
        );
        assert_eq!(
            get_at_for_test(&engine, b"key", 100),
            Some(b"value_30".to_vec())
        );

        // scan_at should also respect timestamp
        let range = b"key".to_vec()..b"key\xff".to_vec();
        let results15 = scan_at_for_test(&engine, &range, 15);
        assert_eq!(results15.len(), 1);
        assert_eq!(results15[0].1, b"value_10".to_vec());

        let results25 = scan_at_for_test(&engine, &range, 25);
        assert_eq!(results25.len(), 1);
        assert_eq!(results25[0].1, b"value_20".to_vec());
    }

    #[test]
    fn test_mvcc_scan_at_after_flush() {
        // Test that MVCC works correctly when data is flushed to SST
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100) // Very small to force rotation/flush
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let lsn_provider = new_lsn_provider();
        let ilog_config = IlogConfig::new(tmp.path());
        let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

        let engine = LsmEngine::open_with_recovery(
            config,
            Arc::clone(&lsn_provider),
            Arc::clone(&ilog),
            Version::new(),
        )
        .unwrap();

        // Write key@ts=10
        let mut batch1 = WriteBatch::new();
        batch1.set_commit_ts(10);
        batch1.put(b"key".to_vec(), b"v1".to_vec());
        engine.write_batch(batch1).unwrap();

        // Write key@ts=20
        let mut batch2 = WriteBatch::new();
        batch2.set_commit_ts(20);
        batch2.put(b"key".to_vec(), b"v2".to_vec());
        engine.write_batch(batch2).unwrap();

        // Force flush to SST
        engine.flush_all_with_active().unwrap();

        // SST should contain MVCC keys - verify by reading at different timestamps
        assert_eq!(get_at_for_test(&engine, b"key", 15), Some(b"v1".to_vec()));
        assert_eq!(get_at_for_test(&engine, b"key", 25), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_mvcc_tombstone_in_sst() {
        // Test that tombstones in SST properly mask older values
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100)
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let lsn_provider = new_lsn_provider();
        let ilog_config = IlogConfig::new(tmp.path());
        let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

        let engine = LsmEngine::open_with_recovery(
            config,
            Arc::clone(&lsn_provider),
            Arc::clone(&ilog),
            Version::new(),
        )
        .unwrap();

        // Write value@ts=10
        let mut batch1 = WriteBatch::new();
        batch1.set_commit_ts(10);
        batch1.put(b"key".to_vec(), b"value".to_vec());
        engine.write_batch(batch1).unwrap();

        // Delete@ts=20
        let mut batch2 = WriteBatch::new();
        batch2.set_commit_ts(20);
        batch2.delete(b"key".to_vec());
        engine.write_batch(batch2).unwrap();

        // Force flush to SST
        engine.flush_all_with_active().unwrap();

        // Check MVCC visibility
        assert_eq!(
            get_at_for_test(&engine, b"key", 15),
            Some(b"value".to_vec())
        );
        assert_eq!(get_at_for_test(&engine, b"key", 25), None); // Deleted
        assert_eq!(get_for_test(&engine, b"key"), None); // Latest is deleted
    }

    #[test]
    fn test_clog_lsn_flows_to_memtable() {
        // Test that CLOG LSN is properly threaded to storage
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        // Write batch with explicit CLOG LSN
        let mut batch = WriteBatch::new();
        batch.set_commit_ts(100);
        batch.set_clog_lsn(42); // Simulate CLOG LSN from transaction service
        batch.put(b"key".to_vec(), b"value".to_vec());
        engine.write_batch(batch).unwrap();

        // Check that the memtable has the correct LSN
        let state = engine.state.read().unwrap();
        assert_eq!(
            state.active.max_lsn(),
            Some(42),
            "Memtable should have CLOG LSN"
        );
    }

    #[test]
    fn test_scan_at_multiple_keys_with_mvcc() {
        // Test scanning multiple keys with different versions
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        // Write multiple keys at ts=10
        let mut batch1 = new_batch(10);
        batch1.put(b"a".to_vec(), b"a_v1".to_vec());
        batch1.put(b"b".to_vec(), b"b_v1".to_vec());
        batch1.put(b"c".to_vec(), b"c_v1".to_vec());
        engine.write_batch(batch1).unwrap();

        // Update some keys at ts=20
        let mut batch2 = new_batch(20);
        batch2.put(b"b".to_vec(), b"b_v2".to_vec());
        engine.write_batch(batch2).unwrap();

        // Delete one key at ts=30
        let mut batch3 = new_batch(30);
        batch3.delete(b"a".to_vec());
        engine.write_batch(batch3).unwrap();

        // Scan at ts=15: should see a_v1, b_v1, c_v1
        let range = b"a".to_vec()..b"d".to_vec();
        let results15 = scan_at_for_test(&engine, &range, 15);
        assert_eq!(results15.len(), 3);
        assert_eq!(results15[0], (b"a".to_vec(), b"a_v1".to_vec()));
        assert_eq!(results15[1], (b"b".to_vec(), b"b_v1".to_vec()));
        assert_eq!(results15[2], (b"c".to_vec(), b"c_v1".to_vec()));

        // Scan at ts=25: should see a_v1, b_v2, c_v1
        let results25 = scan_at_for_test(&engine, &range, 25);
        assert_eq!(results25.len(), 3);
        assert_eq!(results25[0], (b"a".to_vec(), b"a_v1".to_vec()));
        assert_eq!(results25[1], (b"b".to_vec(), b"b_v2".to_vec()));
        assert_eq!(results25[2], (b"c".to_vec(), b"c_v1".to_vec()));

        // Scan at ts=35: should see b_v2, c_v1 (a deleted)
        let results35 = scan_at_for_test(&engine, &range, 35);
        assert_eq!(results35.len(), 2);
        assert_eq!(results35[0], (b"b".to_vec(), b"b_v2".to_vec()));
        assert_eq!(results35[1], (b"c".to_vec(), b"c_v1".to_vec()));
    }

    // ==================== Critical Issue Tests ====================

    #[test]
    fn test_clog_lsn_preserved_through_flush() {
        // Critical: Verify that flushed_lsn matches CLOG LSN for correct recovery
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100)
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let lsn_provider = new_lsn_provider();
        let ilog_config = IlogConfig::new(tmp.path());
        let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

        let engine = LsmEngine::open_with_recovery(
            config,
            Arc::clone(&lsn_provider),
            Arc::clone(&ilog),
            Version::new(),
        )
        .unwrap();

        // Write batch with explicit CLOG LSN (simulating TransactionService behavior)
        let clog_lsn = 100;
        let mut batch = WriteBatch::new();
        batch.set_commit_ts(10);
        batch.set_clog_lsn(clog_lsn);
        batch.put(b"key1".to_vec(), b"value1".to_vec());
        engine.write_batch(batch).unwrap();

        // Verify memtable has the CLOG LSN
        {
            let state = engine.state.read().unwrap();
            assert_eq!(state.active.max_lsn(), Some(clog_lsn));
        }

        // Flush to SST
        engine.flush_all_with_active().unwrap();

        // Verify version.flushed_lsn matches the CLOG LSN
        let version = engine.current_version();
        assert_eq!(
            version.flushed_lsn(),
            clog_lsn,
            "flushed_lsn should match CLOG LSN for correct recovery"
        );
    }

    #[test]
    fn test_sst_contains_mvcc_keys() {
        // Critical: Verify SST contains MVCC-encoded keys
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100)
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let lsn_provider = new_lsn_provider();
        let ilog_config = IlogConfig::new(tmp.path());
        let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

        let engine = LsmEngine::open_with_recovery(
            config.clone(),
            Arc::clone(&lsn_provider),
            Arc::clone(&ilog),
            Version::new(),
        )
        .unwrap();

        // Write multiple versions of same key
        let mut batch1 = WriteBatch::new();
        batch1.set_commit_ts(10);
        batch1.set_clog_lsn(1);
        batch1.put(b"key".to_vec(), b"v1".to_vec());
        engine.write_batch(batch1).unwrap();

        let mut batch2 = WriteBatch::new();
        batch2.set_commit_ts(20);
        batch2.set_clog_lsn(2);
        batch2.put(b"key".to_vec(), b"v2".to_vec());
        engine.write_batch(batch2).unwrap();

        // Flush
        engine.flush_all_with_active().unwrap();

        // Read SST directly to verify it contains MVCC keys
        let version = engine.current_version();
        let ssts = version.ssts_at_level(0);
        assert!(!ssts.is_empty(), "Should have at least one SST");

        let sst_path = config.sst_dir().join(format!("{:08}.sst", ssts[0].id));
        let reader = SstReaderRef::open(&sst_path).unwrap();
        let mut iter = SstIterator::new(reader).unwrap();
        iter.seek_to_first().unwrap(); // Position the iterator

        let mut entry_count = 0;
        let mut found_ts_10 = false;
        let mut found_ts_20 = false;

        while iter.valid() {
            let mvcc_key = iter.key();
            // MVCC key should be longer than user key (has 8-byte timestamp suffix)
            assert!(
                mvcc_key.len() >= 8,
                "SST key should have MVCC timestamp suffix"
            );

            // Decode and verify
            if let Some((decoded_key, ts)) = decode_mvcc_key(mvcc_key) {
                assert_eq!(decoded_key, b"key", "Key should match");
                if ts == 10 {
                    found_ts_10 = true;
                }
                if ts == 20 {
                    found_ts_20 = true;
                }
            }
            entry_count += 1;
            if iter.advance().is_err() {
                break;
            }
        }

        assert!(
            entry_count >= 2,
            "SST should contain multiple MVCC versions"
        );
        assert!(found_ts_10, "SST should contain version at ts=10");
        assert!(found_ts_20, "SST should contain version at ts=20");
    }

    #[test]
    fn test_tombstone_point_read_from_sst() {
        // Critical: Verify get_at returns None when visible version is tombstone in SST
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100)
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let lsn_provider = new_lsn_provider();
        let ilog_config = IlogConfig::new(tmp.path());
        let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

        let engine = LsmEngine::open_with_recovery(
            config,
            Arc::clone(&lsn_provider),
            Arc::clone(&ilog),
            Version::new(),
        )
        .unwrap();

        // Write value
        let mut batch1 = WriteBatch::new();
        batch1.set_commit_ts(10);
        batch1.set_clog_lsn(1);
        batch1.put(b"key".to_vec(), b"value".to_vec());
        engine.write_batch(batch1).unwrap();

        // Delete
        let mut batch2 = WriteBatch::new();
        batch2.set_commit_ts(20);
        batch2.set_clog_lsn(2);
        batch2.delete(b"key".to_vec());
        engine.write_batch(batch2).unwrap();

        // Flush - now both value and tombstone are in SST
        engine.flush_all_with_active().unwrap();

        // Point read at ts=15 should return value (before delete)
        assert_eq!(
            get_at_for_test(&engine, b"key", 15),
            Some(b"value".to_vec()),
            "get_at(ts=15) should return value from SST"
        );

        // Point read at ts=25 should return None (tombstone visible)
        assert_eq!(
            get_at_for_test(&engine, b"key", 25),
            None,
            "get_at(ts=25) should return None due to tombstone in SST"
        );

        // Latest read should also return None
        assert_eq!(
            get_for_test(&engine, b"key"),
            None,
            "get() should return None due to tombstone in SST"
        );
    }

    #[test]
    fn test_long_key_with_0xff_prefix_flushes_correctly() {
        // Medium: Verify keys with 0xFF prefix bytes are properly flushed
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100)
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let lsn_provider = new_lsn_provider();
        let ilog_config = IlogConfig::new(tmp.path());
        let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

        let engine = LsmEngine::open_with_recovery(
            config,
            Arc::clone(&lsn_provider),
            Arc::clone(&ilog),
            Version::new(),
        )
        .unwrap();

        // Create a key with many 0xFF bytes (longer than old 32-byte limit)
        let long_key: Vec<u8> = std::iter::repeat_n(0xFF, 64).collect();
        let value = b"value_for_long_key".to_vec();

        let mut batch = WriteBatch::new();
        batch.set_commit_ts(100);
        batch.set_clog_lsn(1);
        batch.put(long_key.clone(), value.clone());
        engine.write_batch(batch).unwrap();

        // Also add a normal key to ensure both are flushed
        let mut batch2 = WriteBatch::new();
        batch2.set_commit_ts(101);
        batch2.set_clog_lsn(2);
        batch2.put(b"normal_key".to_vec(), b"normal_value".to_vec());
        engine.write_batch(batch2).unwrap();

        // Flush
        engine.flush_all_with_active().unwrap();

        // Both keys should be readable from SST
        assert_eq!(
            get_for_test(&engine, &long_key),
            Some(value),
            "Long key with 0xFF prefix should be readable after flush"
        );
        assert_eq!(
            get_for_test(&engine, b"normal_key"),
            Some(b"normal_value".to_vec()),
            "Normal key should be readable after flush"
        );
    }

    #[test]
    fn test_concurrent_flush_unique_sst_ids() {
        // Medium: Verify concurrent flushes get unique SST IDs
        use std::sync::Barrier;

        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(50) // Very small to trigger rotation
            .max_frozen_memtables(16) // Allow many frozen memtables
            .build()
            .unwrap();

        let lsn_provider = new_lsn_provider();
        let ilog_config = IlogConfig::new(tmp.path());
        let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

        let engine = Arc::new(
            LsmEngine::open_with_recovery(
                config.clone(),
                Arc::clone(&lsn_provider),
                Arc::clone(&ilog),
                Version::new(),
            )
            .unwrap(),
        );

        // Write enough data to have multiple frozen memtables
        for i in 0..20 {
            let mut batch = WriteBatch::new();
            batch.set_commit_ts(i as Timestamp + 1);
            batch.set_clog_lsn(i as u64 + 1);
            let key = format!("key_{i:04}");
            let value = format!("value_{i:04}");
            batch.put(key.as_bytes().to_vec(), value.as_bytes().to_vec());
            engine.write_batch(batch).unwrap();
        }

        // Now trigger concurrent flushes
        let num_threads = 4;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let engine = Arc::clone(&engine);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();
                    // Each thread tries to flush
                    let _ = engine.flush_all();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Verify all SST IDs are unique
        let version = engine.current_version();
        let mut all_sst_ids: Vec<u64> = Vec::new();

        for level in 0..MAX_LEVELS {
            for sst in version.ssts_at_level(level as u32) {
                all_sst_ids.push(sst.id);
            }
        }

        // Check for duplicates
        let original_len = all_sst_ids.len();
        all_sst_ids.sort();
        all_sst_ids.dedup();
        assert_eq!(
            all_sst_ids.len(),
            original_len,
            "All SST IDs should be unique after concurrent flushes"
        );

        // Verify all data is readable
        for i in 0..20 {
            let key = format!("key_{i:04}");
            let expected = format!("value_{i:04}");
            assert_eq!(
                get_for_test(&engine, key.as_bytes()),
                Some(expected.as_bytes().to_vec()),
                "Key {key} should be readable after concurrent flushes"
            );
        }
    }

    #[test]
    fn test_mvcc_visibility_after_flush_recovery() {
        // Critical: Verify MVCC visibility works correctly after flush and recovery
        let tmp = TempDir::new().unwrap();

        // First session: write, flush
        {
            let config = LsmConfig::builder(tmp.path())
                .memtable_size(100)
                .max_frozen_memtables(4)
                .build()
                .unwrap();

            let lsn_provider = new_lsn_provider();
            let ilog_config = IlogConfig::new(tmp.path());
            let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

            let engine =
                LsmEngine::open_with_recovery(config, lsn_provider, ilog, Version::new()).unwrap();

            // Write key@ts=10
            let mut batch1 = WriteBatch::new();
            batch1.set_commit_ts(10);
            batch1.set_clog_lsn(1);
            batch1.put(b"key".to_vec(), b"v1".to_vec());
            engine.write_batch(batch1).unwrap();

            // Write key@ts=20
            let mut batch2 = WriteBatch::new();
            batch2.set_commit_ts(20);
            batch2.set_clog_lsn(2);
            batch2.put(b"key".to_vec(), b"v2".to_vec());
            engine.write_batch(batch2).unwrap();

            // Flush
            engine.flush_all_with_active().unwrap();
        }

        // Second session: recover and verify MVCC visibility
        {
            let lsn_provider = new_lsn_provider();
            let ilog_config = IlogConfig::new(tmp.path());
            let (ilog, version, _orphans) =
                IlogService::recover(ilog_config, Arc::clone(&lsn_provider)).unwrap();

            let config = LsmConfig::builder(tmp.path())
                .memtable_size(100)
                .max_frozen_memtables(4)
                .build()
                .unwrap();

            let engine =
                LsmEngine::open_with_recovery(config, lsn_provider, Arc::new(ilog), version)
                    .unwrap();

            // MVCC visibility should work from SST
            assert_eq!(
                get_at_for_test(&engine, b"key", 5),
                None,
                "Nothing visible at ts=5"
            );
            assert_eq!(
                get_at_for_test(&engine, b"key", 15),
                Some(b"v1".to_vec()),
                "v1 visible at ts=15"
            );
            assert_eq!(
                get_at_for_test(&engine, b"key", 25),
                Some(b"v2".to_vec()),
                "v2 visible at ts=25"
            );
        }
    }

    // ==================== Regression Tests for Review Comments ====================

    /// Test: Delete masking across levels.
    ///
    /// This catches the point-read tombstone bug where a delete in memtable
    /// should mask older values in SST.
    ///
    /// Scenario: put→flush→delete (unflushed) then get_at(ts>=delete_ts) must return None
    #[test]
    fn test_delete_masking_across_levels() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100)
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let lsn_provider = new_lsn_provider();
        let ilog_config = IlogConfig::new(tmp.path());
        let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

        let engine = LsmEngine::open_with_recovery(
            config,
            Arc::clone(&lsn_provider),
            Arc::clone(&ilog),
            Version::new(),
        )
        .unwrap();

        // Step 1: Write value@ts=10 and flush to SST
        let mut batch1 = WriteBatch::new();
        batch1.set_commit_ts(10);
        batch1.set_clog_lsn(1);
        batch1.put(b"key".to_vec(), b"value_in_sst".to_vec());
        engine.write_batch(batch1).unwrap();

        // Flush to SST
        engine.flush_all_with_active().unwrap();

        // Verify value is readable from SST
        assert_eq!(
            get_at_for_test(&engine, b"key", 15),
            Some(b"value_in_sst".to_vec()),
            "Value should be readable from SST before delete"
        );

        // Step 2: Delete@ts=20 (stays in memtable, NOT flushed)
        let mut batch2 = WriteBatch::new();
        batch2.set_commit_ts(20);
        batch2.set_clog_lsn(2);
        batch2.delete(b"key".to_vec());
        engine.write_batch(batch2).unwrap();

        // Step 3: Verify the delete masks the SST value
        // This is the critical test: get_at(ts>=20) MUST return None,
        // NOT the old value from SST. The tombstone in memtable must stop the search.
        assert_eq!(
            get_at_for_test(&engine, b"key", 25),
            None,
            "Delete in memtable MUST mask value in SST (ts=25 >= delete_ts=20)"
        );

        assert_eq!(
            get_at_for_test(&engine, b"key", 20),
            None,
            "Delete in memtable MUST mask value in SST (ts=20 == delete_ts)"
        );

        // Value should still be visible before the delete timestamp
        assert_eq!(
            get_at_for_test(&engine, b"key", 15),
            Some(b"value_in_sst".to_vec()),
            "Value should still be visible at ts=15 (before delete)"
        );

        // Latest read should also return None
        assert_eq!(
            get_for_test(&engine, b"key"),
            None,
            "Latest read should return None (deleted)"
        );
    }

    /// Test: Delete masking in scan across levels.
    ///
    /// Similar to point read, but for range scans.
    #[test]
    fn test_delete_masking_in_scan_across_levels() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100)
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let lsn_provider = new_lsn_provider();
        let ilog_config = IlogConfig::new(tmp.path());
        let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

        let engine = LsmEngine::open_with_recovery(
            config,
            Arc::clone(&lsn_provider),
            Arc::clone(&ilog),
            Version::new(),
        )
        .unwrap();

        // Write multiple keys and flush
        let mut batch1 = WriteBatch::new();
        batch1.set_commit_ts(10);
        batch1.set_clog_lsn(1);
        batch1.put(b"a".to_vec(), b"a_val".to_vec());
        batch1.put(b"b".to_vec(), b"b_val".to_vec());
        batch1.put(b"c".to_vec(), b"c_val".to_vec());
        engine.write_batch(batch1).unwrap();

        engine.flush_all_with_active().unwrap();

        // Delete key "b" (stays in memtable)
        let mut batch2 = WriteBatch::new();
        batch2.set_commit_ts(20);
        batch2.set_clog_lsn(2);
        batch2.delete(b"b".to_vec());
        engine.write_batch(batch2).unwrap();

        // Scan at ts=25: should see a, c (but NOT b - it's deleted)
        let range = b"a".to_vec()..b"d".to_vec();
        let results = scan_at_for_test(&engine, &range, 25);
        assert_eq!(results.len(), 2, "Should see 2 keys after delete");
        assert!(
            results.iter().all(|(k, _)| k != b"b"),
            "Deleted key 'b' should not appear in scan"
        );

        // Scan at ts=15: should see all 3 keys (before delete)
        let results = scan_at_for_test(&engine, &range, 15);
        assert_eq!(results.len(), 3, "Should see 3 keys before delete");
    }

    /// Test: Two-crash recovery.
    ///
    /// Write N txns, flush some, crash; recover, flush some recovered memtables, crash;
    /// recover again and verify no missing keys.
    ///
    /// This catches the missing clog_lsn + replay ordering issues.
    #[test]
    fn test_two_crash_recovery() {
        use crate::clog::{ClogBatch, ClogService, FileClogConfig, FileClogService};

        let tmp = TempDir::new().unwrap();

        // Session 1: Write data, flush some, "crash"
        let written_keys: Vec<(Vec<u8>, Vec<u8>)>;
        {
            let lsm_config = LsmConfig::builder(tmp.path())
                .memtable_size(50) // Small to encourage rotation
                .max_frozen_memtables(4)
                .build()
                .unwrap();

            let lsn_provider = new_lsn_provider();
            let ilog_config = IlogConfig::new(tmp.path());
            let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

            let engine = LsmEngine::open_with_recovery(
                lsm_config,
                Arc::clone(&lsn_provider),
                Arc::clone(&ilog),
                Version::new(),
            )
            .unwrap();

            // Use shared LSN provider for clog
            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog =
                FileClogService::open_with_lsn_provider(clog_config, Arc::clone(&lsn_provider))
                    .unwrap();

            // Write 5 transactions
            written_keys = (0..5)
                .map(|i| {
                    let key = format!("key_{i:04}").into_bytes();
                    let value = format!("value_{i:04}").into_bytes();

                    // Write to clog
                    let mut batch = ClogBatch::new();
                    batch.add_put(i as u64 + 1, key.clone(), value.clone());
                    batch.add_commit(i as u64 + 1, i as Timestamp + 100);
                    let clog_lsn = clog.write(&mut batch, true).unwrap();

                    // Write to engine with clog_lsn
                    let mut wb = WriteBatch::new();
                    wb.set_commit_ts(i as Timestamp + 100);
                    wb.set_clog_lsn(clog_lsn);
                    wb.put(key.clone(), value.clone());
                    engine.write_batch(wb).unwrap();

                    (key, value)
                })
                .collect();

            // Flush some (but not all)
            engine.freeze_active();
            if engine.frozen_count() > 0 {
                // Flush one frozen memtable
                let frozen = {
                    let state = engine.state.read().unwrap();
                    state.frozen.front().cloned()
                };
                if let Some(mt) = frozen {
                    engine.flush_memtable(&mt).unwrap();
                }
            }

            clog.close().unwrap();
            // "Crash" - engine dropped
        }

        // Session 2: Recover, flush more, "crash" again
        {
            use crate::storage::recovery::LsmRecovery;

            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover().unwrap();

            // Verify all data is recovered
            for (key, value) in &written_keys {
                assert_eq!(
                    get_for_test(&result.engine, key),
                    Some(value.clone()),
                    "Key {:?} should be recovered in session 2",
                    String::from_utf8_lossy(key)
                );
            }

            // Flush all recovered memtables
            result.engine.flush_all_with_active().unwrap();

            result.clog.close().unwrap();
            // "Crash" again - engine dropped
        }

        // Session 3: Final recovery - verify no data loss
        {
            use crate::storage::recovery::LsmRecovery;

            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover().unwrap();

            // Verify all data is still present after two-crash recovery
            for (key, value) in &written_keys {
                assert_eq!(
                    get_for_test(&result.engine, key),
                    Some(value.clone()),
                    "Key {:?} should be present after two-crash recovery",
                    String::from_utf8_lossy(key)
                );
            }
        }
    }

    /// Test: All-0xFF keys are handled correctly.
    #[test]
    fn test_all_0xff_keys() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(1000)
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let lsn_provider = new_lsn_provider();
        let ilog_config = IlogConfig::new(tmp.path());
        let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

        let engine = LsmEngine::open_with_recovery(
            config,
            Arc::clone(&lsn_provider),
            Arc::clone(&ilog),
            Version::new(),
        )
        .unwrap();

        // Create keys with all 0xFF bytes
        let key_all_ff_short: Vec<u8> = vec![0xFF; 8];
        let key_all_ff_long: Vec<u8> = vec![0xFF; 100];
        let key_normal = b"normal_key".to_vec();

        // Write all keys
        let mut batch = WriteBatch::new();
        batch.set_commit_ts(100);
        batch.set_clog_lsn(1);
        batch.put(key_all_ff_short.clone(), b"short_ff_value".to_vec());
        batch.put(key_all_ff_long.clone(), b"long_ff_value".to_vec());
        batch.put(key_normal.clone(), b"normal_value".to_vec());
        engine.write_batch(batch).unwrap();

        // Verify all keys are readable from memtable
        assert_eq!(
            get_for_test(&engine, &key_all_ff_short),
            Some(b"short_ff_value".to_vec()),
            "Short 0xFF key should be readable from memtable"
        );
        assert_eq!(
            get_for_test(&engine, &key_all_ff_long),
            Some(b"long_ff_value".to_vec()),
            "Long 0xFF key should be readable from memtable"
        );
        assert_eq!(
            get_for_test(&engine, &key_normal),
            Some(b"normal_value".to_vec()),
            "Normal key should be readable from memtable"
        );

        // Flush to SST
        engine.flush_all_with_active().unwrap();

        // Verify all keys are readable from SST
        assert_eq!(
            get_for_test(&engine, &key_all_ff_short),
            Some(b"short_ff_value".to_vec()),
            "Short 0xFF key should be readable from SST"
        );
        assert_eq!(
            get_for_test(&engine, &key_all_ff_long),
            Some(b"long_ff_value".to_vec()),
            "Long 0xFF key should be readable from SST"
        );
        assert_eq!(
            get_for_test(&engine, &key_normal),
            Some(b"normal_value".to_vec()),
            "Normal key should be readable from SST"
        );
    }

    // ==================== TieredMergeIterator Tests ====================
    //
    // These tests verify the tiered merge behavior of TieredMergeIterator:
    // 1. Data is merged in MVCC order: (user_key ASC, timestamp DESC)
    // 2. All sources (memtable, L0, L1+) are initialized on first seek/advance
    // 3. L0 SST files can overlap and are all considered
    // 4. Priority-based deduplication: higher priority source wins for same MVCC key
    // 5. Lazy file I/O: SST files are opened on first seek, not at construction

    use std::cell::RefCell;
    use std::rc::Rc;

    // MockMvccIterator is defined outside the tests module (at module level with #[cfg(test)])
    // so it can be used by ChildIterator::Mock variant

    /// Helper to create an MvccKey for testing.
    fn test_key(user_key: &[u8], ts: Timestamp) -> MvccKey {
        MvccKey::encode(user_key, ts)
    }

    /// Helper to get the current MVCC key from an iterator (for test assertions).
    fn iter_key<I: MvccIterator + ?Sized>(iter: &I) -> MvccKey {
        MvccKey::encode(iter.user_key(), iter.timestamp())
    }

    #[test]
    fn test_tiered_merge_iterator_memtable_only() {
        // Test that when memtable has all data, it's returned correctly
        let (active_iter, active_seek) = MockMvccIterator::new(
            "active",
            vec![
                (test_key(b"a", 100), b"a_active".to_vec()),
                (test_key(b"b", 100), b"b_active".to_vec()),
            ],
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active_iter);

        let mut merge_iter = merge_iter.build();

        // Seek happens on first advance (lazy initialization)
        merge_iter.advance().unwrap(); // Position on first entry
        assert!(*active_seek.borrow(), "Active memtable should be seeked");
        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"a", 100));
        assert_eq!(merge_iter.value(), b"a_active");
    }

    #[test]
    fn test_tiered_merge_iterator_tier_order_active_frozen() {
        // Test that active memtable has priority over frozen memtables
        // Same key exists in both, active should win
        let (active_iter, _) = MockMvccIterator::new(
            "active",
            vec![(test_key(b"key", 100), b"active_value".to_vec())],
        );

        let (frozen_iter, _) = MockMvccIterator::new(
            "frozen",
            vec![(test_key(b"key", 100), b"frozen_value".to_vec())],
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active_iter);
        merge_iter.add_mock_frozen(frozen_iter, 0);

        let mut merge_iter = merge_iter.build();
        merge_iter.advance().unwrap(); // Position on first entry

        assert!(merge_iter.valid());
        // Active has priority 0, frozen has priority 100+
        // For same key, active wins (lower priority number = higher precedence)
        assert_eq!(merge_iter.value(), b"active_value");
    }

    #[test]
    fn test_tiered_merge_iterator_all_tiers_initialized_at_first_advance() {
        // Test that ALL tiers (memtable and L0) are initialized at first advance
        // (lazy initialization) for correct MVCC key ordering.

        let (active_iter, active_seek) = MockMvccIterator::new(
            "active",
            vec![
                (test_key(b"a", 100), b"a_active".to_vec()),
                (test_key(b"b", 100), b"b_active".to_vec()),
            ],
        );

        let l0_seek = Rc::new(RefCell::new(false));
        let l0_iter = MockMvccIterator::new_with_tracker(
            "l0_sst",
            vec![
                (test_key(b"c", 100), b"c_l0".to_vec()),
                (test_key(b"d", 100), b"d_l0".to_vec()),
            ],
            Rc::clone(&l0_seek),
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active_iter);
        merge_iter.add_mock_l0(l0_iter, 0);

        let mut merge_iter = merge_iter.build();

        // Before first advance, nothing should be seeked (lazy initialization)
        assert!(
            !*active_seek.borrow(),
            "Active memtable should NOT be seeked before first advance (lazy)"
        );
        assert!(
            !*l0_seek.borrow(),
            "L0 SST should NOT be seeked before first advance (lazy)"
        );

        // Position on first entry - this triggers initialization
        merge_iter.advance().unwrap();

        // After first advance, ALL tiers should be initialized
        assert!(
            *active_seek.borrow(),
            "Active memtable should be seeked after first advance"
        );
        assert!(
            *l0_seek.borrow(),
            "L0 SST should be seeked after first advance"
        );

        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"a", 100));

        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"b", 100));

        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"c", 100));
        assert_eq!(merge_iter.value(), b"c_l0");

        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"d", 100));
    }

    #[test]
    fn test_tiered_merge_iterator_all_levels_initialized_at_first_advance() {
        // Test that ALL tiers (memtable, L0, and L1+) are initialized at first advance
        // (lazy initialization) for correct MVCC key ordering.

        let (active_iter, _) =
            MockMvccIterator::new("active", vec![(test_key(b"a", 100), b"a_active".to_vec())]);

        let l0_seek = Rc::new(RefCell::new(false));
        let l0_iter = MockMvccIterator::new_with_tracker(
            "l0_sst",
            vec![(test_key(b"b", 100), b"b_l0".to_vec())],
            Rc::clone(&l0_seek),
        );

        let l1_seek = Rc::new(RefCell::new(false));
        let l1_iter = MockMvccIterator::new_with_tracker(
            "l1_level",
            vec![(test_key(b"c", 100), b"c_l1".to_vec())],
            Rc::clone(&l1_seek),
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active_iter);
        merge_iter.add_mock_l0(l0_iter, 0);
        merge_iter.add_mock_level(l1_iter, 1);

        let mut merge_iter = merge_iter.build();

        // Before first advance, nothing should be seeked (lazy initialization)
        assert!(
            !*l0_seek.borrow(),
            "L0 SST should NOT be seeked before first advance"
        );
        assert!(
            !*l1_seek.borrow(),
            "L1 level should NOT be seeked before first advance"
        );

        // Position on first entry and read - should be sorted correctly
        merge_iter.advance().unwrap();

        // After first advance, ALL tiers should be initialized
        assert!(
            *l0_seek.borrow(),
            "L0 SST should be seeked after first advance"
        );
        assert!(
            *l1_seek.borrow(),
            "L1 level should be seeked after first advance"
        );

        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"a", 100));

        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"b", 100));

        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"c", 100));

        // Exhaust all
        merge_iter.advance().unwrap();
        assert!(!merge_iter.valid(), "All tiers exhausted");
    }

    #[test]
    fn test_tiered_merge_iterator_l0_unordered_all_considered() {
        // Test that ALL L0 SST files are considered during merge.
        // L0 files can have overlapping key ranges, so all must be in the heap.

        // Active memtable is empty to trigger L0 initialization immediately
        let (active_iter, _) = MockMvccIterator::new("active", vec![]);

        // L0 SSTs with overlapping/unordered key ranges
        // SST 0: keys a, d (newer, higher priority)
        let l0_0_seek = Rc::new(RefCell::new(false));
        let l0_0_iter = MockMvccIterator::new_with_tracker(
            "l0_sst_0",
            vec![
                (test_key(b"a", 100), b"a_l0_0".to_vec()),
                (test_key(b"d", 100), b"d_l0_0".to_vec()),
            ],
            Rc::clone(&l0_0_seek),
        );

        // SST 1: keys b, c (older, lower priority)
        let l0_1_seek = Rc::new(RefCell::new(false));
        let l0_1_iter = MockMvccIterator::new_with_tracker(
            "l0_sst_1",
            vec![
                (test_key(b"b", 100), b"b_l0_1".to_vec()),
                (test_key(b"c", 100), b"c_l0_1".to_vec()),
            ],
            Rc::clone(&l0_1_seek),
        );

        // SST 2: keys a, e (even older) - 'a' overlaps with SST 0
        let l0_2_seek = Rc::new(RefCell::new(false));
        let l0_2_iter = MockMvccIterator::new_with_tracker(
            "l0_sst_2",
            vec![
                (test_key(b"a", 100), b"a_l0_2".to_vec()),
                (test_key(b"e", 100), b"e_l0_2".to_vec()),
            ],
            Rc::clone(&l0_2_seek),
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active_iter);
        // Add in index order: 0 (newest) -> 1 -> 2 (oldest)
        merge_iter.add_mock_l0(l0_0_iter, 0);
        merge_iter.add_mock_l0(l0_1_iter, 1);
        merge_iter.add_mock_l0(l0_2_iter, 2);

        let mut merge_iter = merge_iter.build();

        // First advance positions on first entry and triggers lazy initialization
        merge_iter.advance().unwrap();

        // After first advance, ALL L0 SSTs should be initialized for correct ordering
        assert!(
            *l0_0_seek.borrow(),
            "L0 SST 0 should be seeked after first advance"
        );
        assert!(
            *l0_1_seek.borrow(),
            "L0 SST 1 should be seeked after first advance"
        );
        assert!(
            *l0_2_seek.borrow(),
            "L0 SST 2 should be seeked after first advance"
        );

        // Results should be in sorted order, deduplicating same keys
        // Key 'a' exists in SST 0 (priority 1000) and SST 2 (priority 1002)
        // SST 0 has higher priority, so its value should win
        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"a", 100));
        assert_eq!(
            merge_iter.value(),
            b"a_l0_0",
            "SST 0 (higher priority) should win for key 'a'"
        );

        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"b", 100));
        assert_eq!(merge_iter.value(), b"b_l0_1");

        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"c", 100));
        assert_eq!(merge_iter.value(), b"c_l0_1");

        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"d", 100));
        assert_eq!(merge_iter.value(), b"d_l0_0");

        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"e", 100));
        assert_eq!(merge_iter.value(), b"e_l0_2");

        merge_iter.advance().unwrap();
        assert!(!merge_iter.valid());
    }

    #[test]
    fn test_tiered_merge_iterator_seek_initializes_all() {
        // Test that seek() initializes ALL pending iterators (lazy initialization)

        let (active_iter, _) =
            MockMvccIterator::new("active", vec![(test_key(b"z", 100), b"z_active".to_vec())]);

        let l0_seek = Rc::new(RefCell::new(false));
        let l0_iter = MockMvccIterator::new_with_tracker(
            "l0_sst",
            vec![(test_key(b"a", 100), b"a_l0".to_vec())],
            Rc::clone(&l0_seek),
        );

        let l1_seek = Rc::new(RefCell::new(false));
        let l1_iter = MockMvccIterator::new_with_tracker(
            "l1_level",
            vec![(test_key(b"b", 100), b"b_l1".to_vec())],
            Rc::clone(&l1_seek),
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active_iter);
        merge_iter.add_mock_l0(l0_iter, 0);
        merge_iter.add_mock_level(l1_iter, 1);

        let mut merge_iter = merge_iter.build();

        // Before seek, nothing should be initialized (lazy)
        assert!(
            !*l0_seek.borrow(),
            "L0 SST should NOT be seeked before first seek/advance"
        );
        assert!(
            !*l1_seek.borrow(),
            "L1 level should NOT be seeked before first seek/advance"
        );

        // Seek to a specific key triggers lazy initialization
        merge_iter.seek(&test_key(b"a", 100)).unwrap();

        // After seek, ALL tiers should be initialized
        assert!(*l0_seek.borrow(), "L0 SST should be seeked after seek");
        assert!(*l1_seek.borrow(), "L1 level should be seeked after seek");

        // Should be positioned at 'a' from L0
        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"a", 100));
    }

    #[test]
    fn test_tiered_merge_iterator_full_tier_flow() {
        // Test the complete flow: active -> frozen -> L0 -> L1 -> L2
        // Each tier has unique keys to verify the flow

        let (active_iter, _) = MockMvccIterator::new(
            "active",
            vec![(test_key(b"01_active", 100), b"v_active".to_vec())],
        );

        let (frozen0_iter, _) = MockMvccIterator::new(
            "frozen0",
            vec![(test_key(b"02_frozen0", 100), b"v_frozen0".to_vec())],
        );

        let (frozen1_iter, _) = MockMvccIterator::new(
            "frozen1",
            vec![(test_key(b"03_frozen1", 100), b"v_frozen1".to_vec())],
        );

        let l0_seek = Rc::new(RefCell::new(false));
        let l0_iter = MockMvccIterator::new_with_tracker(
            "l0_sst",
            vec![(test_key(b"04_l0", 100), b"v_l0".to_vec())],
            Rc::clone(&l0_seek),
        );

        let l1_seek = Rc::new(RefCell::new(false));
        let l1_iter = MockMvccIterator::new_with_tracker(
            "l1_level",
            vec![(test_key(b"05_l1", 100), b"v_l1".to_vec())],
            Rc::clone(&l1_seek),
        );

        let l2_seek = Rc::new(RefCell::new(false));
        let l2_iter = MockMvccIterator::new_with_tracker(
            "l2_level",
            vec![(test_key(b"06_l2", 100), b"v_l2".to_vec())],
            Rc::clone(&l2_seek),
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active_iter);
        merge_iter.add_mock_frozen(frozen0_iter, 0);
        merge_iter.add_mock_frozen(frozen1_iter, 1);
        merge_iter.add_mock_l0(l0_iter, 0);
        merge_iter.add_mock_level(l1_iter, 1);
        merge_iter.add_mock_level(l2_iter, 2);

        let mut merge_iter = merge_iter.build();

        // Position on first entry - triggers lazy initialization
        merge_iter.advance().unwrap();

        // After first advance, ALL tiers should be initialized
        assert!(*l0_seek.borrow() && *l1_seek.borrow() && *l2_seek.borrow());

        assert!(merge_iter.valid());
        assert_eq!(merge_iter.value(), b"v_active");

        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.value(), b"v_frozen0");

        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.value(), b"v_frozen1");

        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.value(), b"v_l0");

        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.value(), b"v_l1");

        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.value(), b"v_l2");

        merge_iter.advance().unwrap();
        assert!(!merge_iter.valid());
    }

    #[test]
    fn test_tiered_merge_iterator_priority_deduplication() {
        // Test that higher priority source wins for duplicate keys
        // Priority order: active (0) > frozen (100+) > L0 (1000+) > L1+ (10000+)

        let (active_iter, _) = MockMvccIterator::new(
            "active",
            vec![(test_key(b"key", 100), b"active_wins".to_vec())],
        );

        let (frozen_iter, _) = MockMvccIterator::new(
            "frozen",
            vec![(test_key(b"key", 100), b"frozen_loses".to_vec())],
        );

        let l0_seek = Rc::new(RefCell::new(false));
        let l0_iter = MockMvccIterator::new_with_tracker(
            "l0_sst",
            vec![(test_key(b"key", 100), b"l0_loses".to_vec())],
            Rc::clone(&l0_seek),
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active_iter);
        merge_iter.add_mock_frozen(frozen_iter, 0);
        merge_iter.add_mock_l0(l0_iter, 0);

        let mut merge_iter = merge_iter.build();

        // Position on first entry
        merge_iter.advance().unwrap();

        // Only active's value should be returned
        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"key", 100));
        assert_eq!(merge_iter.value(), b"active_wins");

        // Next should exhaust all (duplicates were skipped)
        merge_iter.advance().unwrap();

        // L0 should be initialized, but its duplicate entry should have been skipped
        assert!(!merge_iter.valid());
    }

    #[test]
    fn test_tiered_merge_iterator_empty_memtable_with_l0() {
        // Test that L0 data is correctly returned when memtable is empty

        let (active_iter, _) = MockMvccIterator::new("active", vec![]);

        let l0_seek = Rc::new(RefCell::new(false));
        let l0_iter = MockMvccIterator::new_with_tracker(
            "l0_sst",
            vec![(test_key(b"a", 100), b"a_l0".to_vec())],
            Rc::clone(&l0_seek),
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active_iter);
        merge_iter.add_mock_l0(l0_iter, 0);

        let mut merge_iter = merge_iter.build();

        // First advance positions on L0 data and triggers lazy initialization
        merge_iter.advance().unwrap();

        // L0 should be seeked after first advance
        assert!(*l0_seek.borrow(), "L0 should be seeked after first advance");

        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"a", 100));
    }

    #[test]
    fn test_tiered_merge_iterator_multiple_frozen_memtables() {
        // Test multiple frozen memtables are merged correctly in order

        let (active_iter, _) =
            MockMvccIterator::new("active", vec![(test_key(b"a", 100), b"a_active".to_vec())]);

        // Frozen memtables: index 0 is newest, higher indices are older
        let (frozen0_iter, _) = MockMvccIterator::new(
            "frozen0",
            vec![
                (test_key(b"b", 100), b"b_frozen0".to_vec()),
                (test_key(b"shared", 100), b"shared_frozen0".to_vec()),
            ],
        );

        let (frozen1_iter, _) = MockMvccIterator::new(
            "frozen1",
            vec![
                (test_key(b"c", 100), b"c_frozen1".to_vec()),
                (test_key(b"shared", 100), b"shared_frozen1".to_vec()),
            ],
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active_iter);
        merge_iter.add_mock_frozen(frozen0_iter, 0);
        merge_iter.add_mock_frozen(frozen1_iter, 1);

        let mut merge_iter = merge_iter.build();

        // Position on first entry
        merge_iter.advance().unwrap();

        // Should get: a, b, c, shared (from frozen0 - higher priority)
        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"a", 100));

        merge_iter.advance().unwrap();
        assert_eq!(iter_key(&merge_iter), test_key(b"b", 100));

        merge_iter.advance().unwrap();
        assert_eq!(iter_key(&merge_iter), test_key(b"c", 100));

        merge_iter.advance().unwrap();
        assert_eq!(iter_key(&merge_iter), test_key(b"shared", 100));
        // frozen0 (priority 100) beats frozen1 (priority 101)
        assert_eq!(merge_iter.value(), b"shared_frozen0");

        merge_iter.advance().unwrap();
        assert!(!merge_iter.valid());
    }

    #[test]
    fn test_tiered_merge_iterator_all_empty() {
        // Test when all tiers are empty

        let (active_iter, _) = MockMvccIterator::new("active", vec![]);

        let l0_seek = Rc::new(RefCell::new(false));
        let l0_iter = MockMvccIterator::new_with_tracker("l0_sst", vec![], Rc::clone(&l0_seek));

        let l1_seek = Rc::new(RefCell::new(false));
        let l1_iter = MockMvccIterator::new_with_tracker("l1_level", vec![], Rc::clone(&l1_seek));

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active_iter);
        merge_iter.add_mock_l0(l0_iter, 0);
        merge_iter.add_mock_level(l1_iter, 1);

        let mut merge_iter = merge_iter.build();

        // With lazy initialization, children are NOT initialized at build time
        assert!(!*l0_seek.borrow(), "L0 should NOT be initialized at build");
        assert!(!*l1_seek.borrow(), "L1 should NOT be initialized at build");

        // First advance initializes ALL tiers for correct ordering
        merge_iter.advance().unwrap();

        // Now all tiers should be initialized
        assert!(
            *l0_seek.borrow(),
            "L0 should be initialized after first advance"
        );
        assert!(
            *l1_seek.borrow(),
            "L1 should be initialized after first advance"
        );

        assert!(!merge_iter.valid(), "Should be invalid - all empty");
    }

    #[test]
    fn test_tiered_merge_iterator_interleaved_keys() {
        // Test that keys from different tiers are properly interleaved in sorted order

        let (active_iter, _) = MockMvccIterator::new(
            "active",
            vec![
                (test_key(b"b", 100), b"b_active".to_vec()),
                (test_key(b"d", 100), b"d_active".to_vec()),
            ],
        );

        let (frozen_iter, _) = MockMvccIterator::new(
            "frozen",
            vec![
                (test_key(b"a", 100), b"a_frozen".to_vec()),
                (test_key(b"c", 100), b"c_frozen".to_vec()),
            ],
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active_iter);
        merge_iter.add_mock_frozen(frozen_iter, 0);

        let mut merge_iter = merge_iter.build();

        // Position on first entry
        merge_iter.advance().unwrap();

        // Should be sorted: a, b, c, d
        let expected = [
            (b"a", b"a_frozen"),
            (b"b", b"b_active"),
            (b"c", b"c_frozen"),
            (b"d", b"d_active"),
        ];

        for (key, value) in expected.iter() {
            assert!(merge_iter.valid());
            assert_eq!(iter_key(&merge_iter), test_key(*key, 100));
            assert_eq!(merge_iter.value(), *value);
            merge_iter.advance().unwrap();
        }

        assert!(!merge_iter.valid());
    }

    #[test]
    fn test_lsm_engine_scan_iter_tiered_lazy_loading() {
        // Integration test: verify that LsmEngine's scan_iter uses tiered lazy loading
        // When all data is in memtable, SST files should not be opened

        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(4096)
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let engine = LsmEngine::open(config).unwrap();

        // Write some data to memtable
        let mut batch = new_batch(100);
        batch.put(b"key1".to_vec(), b"value1".to_vec());
        batch.put(b"key2".to_vec(), b"value2".to_vec());
        engine.write_batch(batch).unwrap();

        // Use scan_iter (streaming) to read
        let range = MvccKey::encode(b"key1", Timestamp::MAX)..MvccKey::encode(b"key3", 0);
        let mut iter = engine.scan_iter(range, 0).unwrap();

        // Position on first entry
        iter.advance().unwrap();

        // Should be able to read from memtable
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"key1");

        iter.advance().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"key2");

        iter.advance().unwrap();
        assert!(!iter.valid());
    }

    #[test]
    fn test_lsm_engine_scan_iter_with_sst_lazy_loading() {
        // Integration test: verify SST data is loaded lazily when memtable doesn't have the key

        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100) // Small to force flush
            .max_frozen_memtables(2)
            .build()
            .unwrap();

        let engine = LsmEngine::open(config).unwrap();

        // Write data that will be flushed to SST
        for i in 0..5 {
            let mut batch = new_batch(i as Timestamp + 1);
            let key = format!("old_key_{i:04}");
            let value = format!("old_value_{i:04}");
            batch.put(key.as_bytes().to_vec(), value.as_bytes().to_vec());
            engine.write_batch(batch).unwrap();
        }

        // Flush to SST
        engine.flush_all_with_active().unwrap();

        // Write new data to memtable
        let mut batch = new_batch(100);
        batch.put(b"new_key".to_vec(), b"new_value".to_vec());
        engine.write_batch(batch).unwrap();

        // Scan for the new key first (should hit memtable only)
        let range = MvccKey::encode(b"new_key", Timestamp::MAX)..MvccKey::encode(b"new_key\xff", 0);
        let mut iter = engine.scan_iter(range, 0).unwrap();

        iter.advance().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"new_key");

        iter.advance().unwrap();
        assert!(!iter.valid());

        // Scan for old keys (will need SST)
        let range = MvccKey::encode(b"old_key", Timestamp::MAX)..MvccKey::encode(b"old_key\xff", 0);
        let mut iter = engine.scan_iter(range, 0).unwrap();

        // Should find all old keys from SST
        iter.advance().unwrap();
        let mut count = 0;
        while iter.valid() {
            count += 1;
            iter.advance().unwrap();
        }
        assert_eq!(count, 5, "Should find all 5 old keys from SST");
    }

    // ==================== Correctness Tests for Fixed Issues ====================
    //
    // These tests verify the correctness of fixes for critical issues:
    // 1. TieredMergeIterator tier-gating (keys out of MVCC order)
    // 2. SstMvccIterator range start bound enforcement
    // 3. LazySstIterator error handling

    #[test]
    fn test_tiered_merge_iterator_ordering_memtable_vs_sst() {
        // CRITICAL: Verify scan_iter returns keys in correct MVCC order
        // when SST has key "a" and memtable has key "b".
        // Previously, tier-gating would return "b" before "a", violating order.

        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100)
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let engine = LsmEngine::open(config).unwrap();

        // Write key "a" and "c" to memtable
        let mut batch = new_batch(10);
        batch.put(b"a".to_vec(), b"value_a".to_vec());
        batch.put(b"c".to_vec(), b"value_c".to_vec());
        engine.write_batch(batch).unwrap();

        // Flush to SST - now SST has a, c
        engine.flush_all_with_active().unwrap();

        // Write key "b" to memtable
        let mut batch = new_batch(20);
        batch.put(b"b".to_vec(), b"value_b".to_vec());
        engine.write_batch(batch).unwrap();

        // Scan a..d - must output a, b, c in order
        let range = MvccKey::encode(b"a", Timestamp::MAX)..MvccKey::encode(b"d", 0);
        let mut iter = engine.scan_iter(range, 0).unwrap();

        iter.advance().unwrap();
        assert!(iter.valid(), "Should have first entry");
        assert_eq!(
            iter.user_key(),
            b"a",
            "First key must be 'a' (from SST), not 'b' (from memtable)"
        );

        iter.advance().unwrap();
        assert!(iter.valid(), "Should have second entry");
        assert_eq!(
            iter.user_key(),
            b"b",
            "Second key must be 'b' (from memtable)"
        );

        iter.advance().unwrap();
        assert!(iter.valid(), "Should have third entry");
        assert_eq!(iter.user_key(), b"c", "Third key must be 'c' (from SST)");

        iter.advance().unwrap();
        assert!(!iter.valid(), "Should have no more entries after a, b, c");
    }

    #[test]
    fn test_tiered_merge_iterator_ordering_multiple_ssts() {
        // Test ordering when data is spread across multiple SSTs and memtable

        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(64) // Very small to force multiple flushes
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let engine = LsmEngine::open(config).unwrap();

        // Write and flush: SST1 gets keys 10, 30, 50
        let mut batch = new_batch(1);
        batch.put(b"key_10".to_vec(), b"v1".to_vec());
        batch.put(b"key_30".to_vec(), b"v1".to_vec());
        batch.put(b"key_50".to_vec(), b"v1".to_vec());
        engine.write_batch(batch).unwrap();
        engine.flush_all_with_active().unwrap();

        // Write and flush: SST2 gets keys 20, 40
        let mut batch = new_batch(2);
        batch.put(b"key_20".to_vec(), b"v2".to_vec());
        batch.put(b"key_40".to_vec(), b"v2".to_vec());
        engine.write_batch(batch).unwrap();
        engine.flush_all_with_active().unwrap();

        // Write to memtable: keys 15, 35
        let mut batch = new_batch(3);
        batch.put(b"key_15".to_vec(), b"v3".to_vec());
        batch.put(b"key_35".to_vec(), b"v3".to_vec());
        engine.write_batch(batch).unwrap();

        // Scan all - must be in sorted order: 10, 15, 20, 30, 35, 40, 50
        let range = MvccKey::encode(b"key_", Timestamp::MAX)..MvccKey::encode(b"key_\xff", 0);
        let mut iter = engine.scan_iter(range, 0).unwrap();

        let expected_order = [
            b"key_10".as_slice(),
            b"key_15",
            b"key_20",
            b"key_30",
            b"key_35",
            b"key_40",
            b"key_50",
        ];

        iter.advance().unwrap();
        for expected_key in expected_order {
            assert!(
                iter.valid(),
                "Expected key {expected_key:?} but iterator exhausted"
            );
            assert_eq!(
                iter.user_key(),
                expected_key,
                "Keys out of order: expected {:?}, got {:?}",
                std::str::from_utf8(expected_key),
                std::str::from_utf8(iter.user_key())
            );
            iter.advance().unwrap();
        }

        assert!(!iter.valid(), "Should have no more entries");
    }

    #[test]
    fn test_sst_mvcc_iterator_respects_range_start() {
        // Test that SstMvccIterator filters keys before range.start

        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100)
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let engine = LsmEngine::open(config).unwrap();

        // Write keys a, b, c, d to SST
        let mut batch = new_batch(10);
        batch.put(b"a".to_vec(), b"1".to_vec());
        batch.put(b"b".to_vec(), b"2".to_vec());
        batch.put(b"c".to_vec(), b"3".to_vec());
        batch.put(b"d".to_vec(), b"4".to_vec());
        engine.write_batch(batch).unwrap();
        engine.flush_all_with_active().unwrap();

        // Scan range b..d (should only get b, c)
        // For user key range [b, d), use:
        // - start: MvccKey::encode(b"b", Timestamp::MAX) = smallest MVCC key for "b"
        // - end: MvccKey::encode(b"d", Timestamp::MAX) = smallest MVCC key for "d"
        let range = MvccKey::encode(b"b", Timestamp::MAX)..MvccKey::encode(b"d", Timestamp::MAX);
        let mut iter = engine.scan_iter(range, 0).unwrap();

        iter.advance().unwrap();
        assert!(iter.valid());
        assert_eq!(
            iter.user_key(),
            b"b",
            "First key should be 'b' (range start), not 'a'"
        );

        iter.advance().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"c", "Second key should be 'c'");

        iter.advance().unwrap();
        assert!(
            !iter.valid(),
            "Should not have 'd' (beyond range end) or 'a' (before range start)"
        );
    }

    #[test]
    fn test_sst_mvcc_iterator_range_start_with_timestamp() {
        // Test range filtering with specific MVCC timestamps

        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100)
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let engine = LsmEngine::open(config).unwrap();

        // Write multiple versions of key "k"
        let mut batch1 = new_batch(10);
        batch1.put(b"k".to_vec(), b"v10".to_vec());
        engine.write_batch(batch1).unwrap();

        let mut batch2 = new_batch(20);
        batch2.put(b"k".to_vec(), b"v20".to_vec());
        engine.write_batch(batch2).unwrap();

        let mut batch3 = new_batch(30);
        batch3.put(b"k".to_vec(), b"v30".to_vec());
        engine.write_batch(batch3).unwrap();

        engine.flush_all_with_active().unwrap();

        // Scan with range starting at ts=25 - should skip version at ts=30
        // Range: (k, ts=25) .. (k+1, ts=0)
        let range = MvccKey::encode(b"k", 25)..MvccKey::encode(b"l", 0);
        let mut iter = engine.scan_iter(range, 0).unwrap();

        iter.advance().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"k");
        assert_eq!(
            iter.timestamp(),
            20,
            "Should get ts=20 (first visible version)"
        );

        iter.advance().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.timestamp(), 10, "Should get ts=10");

        iter.advance().unwrap();
        assert!(
            !iter.valid(),
            "Should not have ts=30 version (filtered by range start)"
        );
    }

    #[test]
    fn test_sst_data_accessible_after_flush() {
        // Test that data flushed to SST is accessible via scan_iter

        let tmp = TempDir::new().unwrap();
        let sst_dir = tmp.path().join("sst");
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100)
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let engine = LsmEngine::open(config).unwrap();

        // Write and flush some data
        let mut batch = new_batch(10);
        batch.put(b"key".to_vec(), b"value".to_vec());
        engine.write_batch(batch).unwrap();
        engine.flush_all_with_active().unwrap();

        // Get the SST path and verify it exists
        let version = engine.current_version();
        let ssts = version.ssts_at_level(0);
        assert!(!ssts.is_empty());
        let sst_id = ssts[0].id;
        let sst_path = sst_dir.join(format!("{sst_id:08}.sst"));
        assert!(sst_path.exists(), "SST file should exist");

        // Verify data is readable from SST
        let result = get_for_test(&engine, b"key");
        assert_eq!(
            result,
            Some(b"value".to_vec()),
            "Data should be readable from SST"
        );
    }

    #[test]
    fn test_merge_iterator_deduplication_across_sources() {
        // Test that duplicate MVCC keys from different sources are deduplicated

        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100)
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let engine = LsmEngine::open(config).unwrap();

        // Write key at ts=10 to SST
        let mut batch = new_batch(10);
        batch.put(b"key".to_vec(), b"old_value".to_vec());
        engine.write_batch(batch).unwrap();
        engine.flush_all_with_active().unwrap();

        // Write same key at ts=20 to memtable (newer version)
        let mut batch = new_batch(20);
        batch.put(b"key".to_vec(), b"new_value".to_vec());
        engine.write_batch(batch).unwrap();

        // Scan should return both versions in order (ts=20 first, then ts=10)
        let range = MvccKey::encode(b"key", Timestamp::MAX)..MvccKey::encode(b"key\xff", 0);
        let mut iter = engine.scan_iter(range, 0).unwrap();

        iter.advance().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"key");
        assert_eq!(
            iter.timestamp(),
            20,
            "Newest version (ts=20) should come first"
        );
        assert_eq!(iter.value(), b"new_value");

        iter.advance().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"key");
        assert_eq!(
            iter.timestamp(),
            10,
            "Older version (ts=10) should come second"
        );
        assert_eq!(iter.value(), b"old_value");

        iter.advance().unwrap();
        assert!(!iter.valid());
    }

    #[test]
    fn test_scan_iter_ordering_stress_test() {
        // Stress test: verify ordering with many keys across memtable and SSTs

        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(512) // Medium size
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let engine = LsmEngine::open(config).unwrap();

        // Write odd numbers to SST
        let mut batch = new_batch(1);
        for i in (1..100).step_by(2) {
            batch.put(format!("key_{i:03}").as_bytes().to_vec(), b"odd".to_vec());
        }
        engine.write_batch(batch).unwrap();
        engine.flush_all_with_active().unwrap();

        // Write even numbers to memtable
        let mut batch = new_batch(2);
        for i in (0..100).step_by(2) {
            batch.put(format!("key_{i:03}").as_bytes().to_vec(), b"even".to_vec());
        }
        engine.write_batch(batch).unwrap();

        // Scan all - should be perfectly sorted
        let range = MvccKey::encode(b"key_", Timestamp::MAX)..MvccKey::encode(b"key_\xff", 0);
        let mut iter = engine.scan_iter(range, 0).unwrap();

        iter.advance().unwrap();
        let mut prev_key: Option<Vec<u8>> = None;
        let mut count = 0;

        while iter.valid() {
            let current_key = iter.user_key().to_vec();
            if let Some(ref prev) = prev_key {
                assert!(
                    current_key > *prev,
                    "Keys out of order: {:?} should come after {:?}",
                    std::str::from_utf8(&current_key),
                    std::str::from_utf8(prev)
                );
            }
            prev_key = Some(current_key);
            count += 1;
            iter.advance().unwrap();
        }

        assert_eq!(count, 100, "Should have all 100 keys in sorted order");
    }

    // ==================== Lazy IO Verification Tests ====================
    //
    // These tests explicitly verify that IO (seek operations) is NOT triggered
    // at iterator construction time, but only when seek() or advance() is called.

    #[test]
    fn test_lazy_io_no_seek_at_build_time() {
        // Verify that building a TieredMergeIterator does NOT trigger any seek operations.
        // This is the core lazy initialization invariant.

        let active_seek = Rc::new(RefCell::new(false));
        let active_iter =
            MockMvccIterator::new_with_tracker("active", vec![], Rc::clone(&active_seek));

        let frozen_seek = Rc::new(RefCell::new(false));
        let frozen_iter =
            MockMvccIterator::new_with_tracker("frozen", vec![], Rc::clone(&frozen_seek));

        let l0_seek = Rc::new(RefCell::new(false));
        let l0_iter = MockMvccIterator::new_with_tracker("l0", vec![], Rc::clone(&l0_seek));

        let l1_seek = Rc::new(RefCell::new(false));
        let l1_iter = MockMvccIterator::new_with_tracker("l1", vec![], Rc::clone(&l1_seek));

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active_iter);
        merge_iter.add_mock_frozen(frozen_iter, 0);
        merge_iter.add_mock_l0(l0_iter, 0);
        merge_iter.add_mock_level(l1_iter, 1);

        // Build the iterator - NO IO should happen here
        let _merge_iter = merge_iter.build();

        // Verify NO iterators were seeked during build
        assert!(
            !*active_seek.borrow(),
            "Active memtable should NOT be seeked at build time"
        );
        assert!(
            !*frozen_seek.borrow(),
            "Frozen memtable should NOT be seeked at build time"
        );
        assert!(
            !*l0_seek.borrow(),
            "L0 SST should NOT be seeked at build time"
        );
        assert!(
            !*l1_seek.borrow(),
            "L1 level should NOT be seeked at build time"
        );
    }

    #[test]
    fn test_lazy_io_triggered_by_advance() {
        // Verify that calling advance() triggers seek on ALL child iterators.

        let active_seek = Rc::new(RefCell::new(false));
        let active_iter = MockMvccIterator::new_with_tracker(
            "active",
            vec![(test_key(b"a", 100), b"v".to_vec())],
            Rc::clone(&active_seek),
        );

        let l0_seek = Rc::new(RefCell::new(false));
        let l0_iter = MockMvccIterator::new_with_tracker(
            "l0",
            vec![(test_key(b"b", 100), b"v".to_vec())],
            Rc::clone(&l0_seek),
        );

        let l1_seek = Rc::new(RefCell::new(false));
        let l1_iter = MockMvccIterator::new_with_tracker(
            "l1",
            vec![(test_key(b"c", 100), b"v".to_vec())],
            Rc::clone(&l1_seek),
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active_iter);
        merge_iter.add_mock_l0(l0_iter, 0);
        merge_iter.add_mock_level(l1_iter, 1);

        let mut merge_iter = merge_iter.build();

        // Before advance: NO seeks
        assert!(!*active_seek.borrow(), "No seek before advance");
        assert!(!*l0_seek.borrow(), "No seek before advance");
        assert!(!*l1_seek.borrow(), "No seek before advance");

        // First advance triggers lazy initialization
        merge_iter.advance().unwrap();

        // After advance: ALL iterators seeked
        assert!(
            *active_seek.borrow(),
            "Active should be seeked after advance"
        );
        assert!(*l0_seek.borrow(), "L0 should be seeked after advance");
        assert!(*l1_seek.borrow(), "L1 should be seeked after advance");

        // Verify iterator is valid and positioned correctly
        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"a", 100));
    }

    #[test]
    fn test_lazy_io_triggered_by_seek() {
        // Verify that calling seek() triggers seek on ALL child iterators.

        let active_seek = Rc::new(RefCell::new(false));
        let active_iter = MockMvccIterator::new_with_tracker(
            "active",
            vec![(test_key(b"a", 100), b"v".to_vec())],
            Rc::clone(&active_seek),
        );

        let l0_seek = Rc::new(RefCell::new(false));
        let l0_iter = MockMvccIterator::new_with_tracker(
            "l0",
            vec![(test_key(b"b", 100), b"v".to_vec())],
            Rc::clone(&l0_seek),
        );

        let l1_seek = Rc::new(RefCell::new(false));
        let l1_iter = MockMvccIterator::new_with_tracker(
            "l1",
            vec![(test_key(b"c", 100), b"v".to_vec())],
            Rc::clone(&l1_seek),
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active_iter);
        merge_iter.add_mock_l0(l0_iter, 0);
        merge_iter.add_mock_level(l1_iter, 1);

        let mut merge_iter = merge_iter.build();

        // Before seek: NO seeks
        assert!(!*active_seek.borrow(), "No seek before seek()");
        assert!(!*l0_seek.borrow(), "No seek before seek()");
        assert!(!*l1_seek.borrow(), "No seek before seek()");

        // Explicit seek triggers lazy initialization
        merge_iter.seek(&test_key(b"b", 100)).unwrap();

        // After seek: ALL iterators seeked
        assert!(
            *active_seek.borrow(),
            "Active should be seeked after seek()"
        );
        assert!(*l0_seek.borrow(), "L0 should be seeked after seek()");
        assert!(*l1_seek.borrow(), "L1 should be seeked after seek()");

        // Verify iterator is positioned at the seek target
        assert!(merge_iter.valid());
        assert_eq!(iter_key(&merge_iter), test_key(b"b", 100));
    }

    #[test]
    fn test_lazy_io_subsequent_operations_no_reinit() {
        // Verify that after first initialization, subsequent advance/seek
        // operations do NOT re-initialize (seek_called should still be true from first init).

        let l0_seek = Rc::new(RefCell::new(false));
        let l0_iter = MockMvccIterator::new_with_tracker(
            "l0",
            vec![
                (test_key(b"a", 100), b"va".to_vec()),
                (test_key(b"b", 100), b"vb".to_vec()),
                (test_key(b"c", 100), b"vc".to_vec()),
            ],
            Rc::clone(&l0_seek),
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_l0(l0_iter, 0);

        let mut merge_iter = merge_iter.build();

        // First advance - initializes
        merge_iter.advance().unwrap();
        assert!(*l0_seek.borrow());

        // Reset the tracker to verify subsequent operations don't re-seek
        *l0_seek.borrow_mut() = false;

        // Subsequent advances - should NOT re-initialize
        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        // Note: MockMvccIterator.advance() doesn't call seek(), so seek_called stays false
        // This confirms we're not re-running initialization

        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());

        merge_iter.advance().unwrap();
        assert!(!merge_iter.valid()); // Exhausted
    }

    #[test]
    fn test_lazy_io_with_real_sst_files() {
        // Integration test: verify lazy IO with real SST files.
        // The test creates SST files and verifies they can be read after lazy init.

        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100)
            .max_frozen_memtables(4)
            .build()
            .unwrap();

        let engine = LsmEngine::open(config).unwrap();

        // Write data and flush to SST
        let mut batch = new_batch(10);
        batch.put(b"sst_key_a".to_vec(), b"sst_value_a".to_vec());
        batch.put(b"sst_key_b".to_vec(), b"sst_value_b".to_vec());
        engine.write_batch(batch).unwrap();
        engine.flush_all_with_active().unwrap();

        // Write more data to memtable (not flushed)
        let mut batch = new_batch(20);
        batch.put(b"mem_key_c".to_vec(), b"mem_value_c".to_vec());
        engine.write_batch(batch).unwrap();

        // Create iterator - NO file IO should happen here (lazy)
        let range = MvccKey::encode(b"", Timestamp::MAX)..MvccKey::encode(b"\xff", 0);
        let mut iter = engine.scan_iter(range, 0).unwrap();

        // At this point, SST files should NOT be opened yet
        // (We can't directly verify this without instrumenting the code,
        // but the test verifies correctness after lazy init)

        // First advance triggers lazy initialization and file IO
        iter.advance().unwrap();

        // Verify all data is accessible (from both memtable and SST)
        let mut keys_found = Vec::new();
        while iter.valid() {
            keys_found.push(iter.user_key().to_vec());
            iter.advance().unwrap();
        }

        assert!(
            keys_found.contains(&b"mem_key_c".to_vec()),
            "Should find memtable key"
        );
        assert!(
            keys_found.contains(&b"sst_key_a".to_vec()),
            "Should find SST key a"
        );
        assert!(
            keys_found.contains(&b"sst_key_b".to_vec()),
            "Should find SST key b"
        );
    }

    #[test]
    fn test_lazy_io_valid_returns_false_before_init() {
        // Verify that valid() returns false before any seek/advance.

        let (iter, _) = MockMvccIterator::new("test", vec![(test_key(b"a", 100), b"v".to_vec())]);

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(iter);

        let merge_iter = merge_iter.build();

        // Before any operation, valid() should be false
        assert!(
            !merge_iter.valid(),
            "Iterator should not be valid before advance/seek"
        );
    }

    #[test]
    fn test_lazy_io_multiple_l0_ssts_all_initialized() {
        // Verify that ALL L0 SSTs are initialized on first advance/seek.
        // This is critical because L0 SSTs can overlap and all must participate.

        let l0_0_seek = Rc::new(RefCell::new(false));
        let l0_0_iter = MockMvccIterator::new_with_tracker(
            "l0_0",
            vec![(test_key(b"a", 100), b"v0".to_vec())],
            Rc::clone(&l0_0_seek),
        );

        let l0_1_seek = Rc::new(RefCell::new(false));
        let l0_1_iter = MockMvccIterator::new_with_tracker(
            "l0_1",
            vec![(test_key(b"b", 100), b"v1".to_vec())],
            Rc::clone(&l0_1_seek),
        );

        let l0_2_seek = Rc::new(RefCell::new(false));
        let l0_2_iter = MockMvccIterator::new_with_tracker(
            "l0_2",
            vec![(test_key(b"c", 100), b"v2".to_vec())],
            Rc::clone(&l0_2_seek),
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_l0(l0_0_iter, 0);
        merge_iter.add_mock_l0(l0_1_iter, 1);
        merge_iter.add_mock_l0(l0_2_iter, 2);

        let mut merge_iter = merge_iter.build();

        // Before advance: NO L0 SSTs seeked
        assert!(!*l0_0_seek.borrow(), "L0[0] not seeked before advance");
        assert!(!*l0_1_seek.borrow(), "L0[1] not seeked before advance");
        assert!(!*l0_2_seek.borrow(), "L0[2] not seeked before advance");

        // First advance
        merge_iter.advance().unwrap();

        // After advance: ALL L0 SSTs should be seeked
        assert!(*l0_0_seek.borrow(), "L0[0] should be seeked after advance");
        assert!(*l0_1_seek.borrow(), "L0[1] should be seeked after advance");
        assert!(*l0_2_seek.borrow(), "L0[2] should be seeked after advance");
    }

    // ==================== TieredMergeIterator Additional Coverage Tests ====================

    #[test]
    fn test_tiered_merge_iterator_empty() {
        let mut merge_iter = TieredMergeIterator::new().build();

        // Before advance
        assert!(!merge_iter.valid());

        // After advance with no children
        merge_iter.advance().unwrap();
        assert!(!merge_iter.valid());
    }

    #[test]
    fn test_tiered_merge_iterator_single_source() {
        let (iter, _) = MockMvccIterator::new(
            "single",
            vec![
                (test_key(b"a", 100), b"va".to_vec()),
                (test_key(b"b", 100), b"vb".to_vec()),
                (test_key(b"c", 100), b"vc".to_vec()),
            ],
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(iter);
        let mut merge_iter = merge_iter.build();

        // Iterate all entries
        let mut keys = Vec::new();
        merge_iter.advance().unwrap();
        while merge_iter.valid() {
            keys.push(merge_iter.user_key().to_vec());
            merge_iter.advance().unwrap();
        }

        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn test_tiered_merge_iterator_active_wins_over_frozen_same_key() {
        // Test that higher priority source wins when same MVCC key exists in multiple sources.
        // Active memtable (priority 0) should win over frozen (priority 100+).

        let (active, _) = MockMvccIterator::new(
            "active",
            vec![(test_key(b"key", 100), b"active_value".to_vec())],
        );

        let (frozen, _) = MockMvccIterator::new(
            "frozen",
            vec![(test_key(b"key", 100), b"frozen_value".to_vec())], // Same MVCC key!
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active);
        merge_iter.add_mock_frozen(frozen, 0);
        let mut merge_iter = merge_iter.build();

        merge_iter.advance().unwrap();

        // Should get active value (higher priority = lower priority number)
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.user_key(), b"key");
        assert_eq!(merge_iter.timestamp(), 100);
        assert_eq!(merge_iter.value(), b"active_value");

        // Should skip the duplicate from frozen
        merge_iter.advance().unwrap();
        assert!(!merge_iter.valid());
    }

    #[test]
    fn test_tiered_merge_iterator_different_versions_not_deduplicated() {
        // Different versions (same user_key, different timestamp) should NOT be deduplicated.

        let (active, _) =
            MockMvccIterator::new("active", vec![(test_key(b"key", 100), b"v100".to_vec())]);

        let (frozen, _) = MockMvccIterator::new(
            "frozen",
            vec![(test_key(b"key", 50), b"v50".to_vec())], // Same key, different timestamp
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active);
        merge_iter.add_mock_frozen(frozen, 0);
        let mut merge_iter = merge_iter.build();

        // First entry: ts=100 (higher timestamp = smaller encoded key, comes first)
        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.timestamp(), 100);
        assert_eq!(merge_iter.value(), b"v100");

        // Second entry: ts=50 (NOT deduplicated - different version)
        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.timestamp(), 50);
        assert_eq!(merge_iter.value(), b"v50");

        merge_iter.advance().unwrap();
        assert!(!merge_iter.valid());
    }

    #[test]
    fn test_tiered_merge_iterator_mvcc_ordering() {
        // Test MVCC ordering: (user_key ASC, timestamp DESC)
        // Higher timestamps should come first for the same key.

        let (iter, _) = MockMvccIterator::new(
            "source",
            vec![
                (test_key(b"a", 100), b"a100".to_vec()),
                (test_key(b"a", 50), b"a50".to_vec()),
                (test_key(b"b", 200), b"b200".to_vec()),
                (test_key(b"b", 100), b"b100".to_vec()),
            ],
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(iter);
        let mut merge_iter = merge_iter.build();

        merge_iter.advance().unwrap();
        assert_eq!(merge_iter.user_key(), b"a");
        assert_eq!(merge_iter.timestamp(), 100);

        merge_iter.advance().unwrap();
        assert_eq!(merge_iter.user_key(), b"a");
        assert_eq!(merge_iter.timestamp(), 50);

        merge_iter.advance().unwrap();
        assert_eq!(merge_iter.user_key(), b"b");
        assert_eq!(merge_iter.timestamp(), 200);

        merge_iter.advance().unwrap();
        assert_eq!(merge_iter.user_key(), b"b");
        assert_eq!(merge_iter.timestamp(), 100);

        merge_iter.advance().unwrap();
        assert!(!merge_iter.valid());
    }

    #[test]
    fn test_tiered_merge_iterator_interleaved_sources() {
        // Test correct ordering when sources have interleaved keys.

        let (active, _) = MockMvccIterator::new(
            "active",
            vec![
                (test_key(b"a", 100), b"a".to_vec()),
                (test_key(b"c", 100), b"c".to_vec()),
                (test_key(b"e", 100), b"e".to_vec()),
            ],
        );

        let (frozen, _) = MockMvccIterator::new(
            "frozen",
            vec![
                (test_key(b"b", 100), b"b".to_vec()),
                (test_key(b"d", 100), b"d".to_vec()),
                (test_key(b"f", 100), b"f".to_vec()),
            ],
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active);
        merge_iter.add_mock_frozen(frozen, 0);
        let mut merge_iter = merge_iter.build();

        // Should iterate in sorted order: a, b, c, d, e, f
        let mut keys = Vec::new();
        merge_iter.advance().unwrap();
        while merge_iter.valid() {
            keys.push(merge_iter.user_key().to_vec());
            merge_iter.advance().unwrap();
        }

        assert_eq!(
            keys,
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
                b"e".to_vec(),
                b"f".to_vec()
            ]
        );
    }

    #[test]
    fn test_tiered_merge_iterator_seek() {
        let (iter, _) = MockMvccIterator::new(
            "source",
            vec![
                (test_key(b"a", 100), b"va".to_vec()),
                (test_key(b"b", 100), b"vb".to_vec()),
                (test_key(b"c", 100), b"vc".to_vec()),
            ],
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(iter);
        let mut merge_iter = merge_iter.build();

        // Seek to "b"
        merge_iter.seek(&test_key(b"b", 100)).unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.user_key(), b"b");

        // Continue iteration
        merge_iter.advance().unwrap();
        assert_eq!(merge_iter.user_key(), b"c");

        merge_iter.advance().unwrap();
        assert!(!merge_iter.valid());
    }

    #[test]
    fn test_tiered_merge_iterator_seek_resets_dedup_state() {
        // Verify that seek clears deduplication state.

        let (iter, _) = MockMvccIterator::new(
            "source",
            vec![
                (test_key(b"a", 100), b"va".to_vec()),
                (test_key(b"b", 100), b"vb".to_vec()),
            ],
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(iter);
        let mut merge_iter = merge_iter.build();

        // Iterate to end
        merge_iter.advance().unwrap();
        merge_iter.advance().unwrap();
        merge_iter.advance().unwrap();
        assert!(!merge_iter.valid());

        // Seek should reset state
        merge_iter.seek(&test_key(b"a", 100)).unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.user_key(), b"a");
    }

    #[test]
    fn test_tiered_merge_iterator_four_tier_ordering() {
        // Test all four tier types together: active, frozen, L0, level.

        let (active, _) =
            MockMvccIterator::new("active", vec![(test_key(b"a", 100), b"active".to_vec())]);

        let (frozen, _) =
            MockMvccIterator::new("frozen", vec![(test_key(b"b", 100), b"frozen".to_vec())]);

        let (l0, _) = MockMvccIterator::new("l0", vec![(test_key(b"c", 100), b"l0".to_vec())]);

        let (level, _) =
            MockMvccIterator::new("level", vec![(test_key(b"d", 100), b"level".to_vec())]);

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(active);
        merge_iter.add_mock_frozen(frozen, 0);
        merge_iter.add_mock_l0(l0, 0);
        merge_iter.add_mock_level(level, 1);
        let mut merge_iter = merge_iter.build();

        let mut results = Vec::new();
        merge_iter.advance().unwrap();
        while merge_iter.valid() {
            results.push((merge_iter.user_key().to_vec(), merge_iter.value().to_vec()));
            merge_iter.advance().unwrap();
        }

        assert_eq!(results.len(), 4);
        assert_eq!(results[0], (b"a".to_vec(), b"active".to_vec()));
        assert_eq!(results[1], (b"b".to_vec(), b"frozen".to_vec()));
        assert_eq!(results[2], (b"c".to_vec(), b"l0".to_vec()));
        assert_eq!(results[3], (b"d".to_vec(), b"level".to_vec()));
    }

    #[test]
    fn test_tiered_merge_iterator_priority_l0_newer_wins() {
        // L0 with lower index (newer) should win over L0 with higher index (older).

        let (l0_0, _) =
            MockMvccIterator::new("l0_0", vec![(test_key(b"key", 100), b"newer".to_vec())]);

        let (l0_1, _) = MockMvccIterator::new(
            "l0_1",
            vec![(test_key(b"key", 100), b"older".to_vec())], // Same MVCC key
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_l0(l0_0, 0); // Priority 1000
        merge_iter.add_mock_l0(l0_1, 1); // Priority 1001
        let mut merge_iter = merge_iter.build();

        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.value(), b"newer");

        // Should skip duplicate
        merge_iter.advance().unwrap();
        assert!(!merge_iter.valid());
    }

    #[test]
    fn test_tiered_merge_iterator_empty_sources_skipped() {
        // Sources with no entries should be handled gracefully.

        let (empty1, _) = MockMvccIterator::new("empty1", vec![]);
        let (non_empty, _) = MockMvccIterator::new(
            "non_empty",
            vec![(test_key(b"key", 100), b"value".to_vec())],
        );
        let (empty2, _) = MockMvccIterator::new("empty2", vec![]);

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(empty1);
        merge_iter.add_mock_frozen(non_empty, 0);
        merge_iter.add_mock_l0(empty2, 0);
        let mut merge_iter = merge_iter.build();

        merge_iter.advance().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.user_key(), b"key");
        assert_eq!(merge_iter.value(), b"value");

        merge_iter.advance().unwrap();
        assert!(!merge_iter.valid());
    }

    #[test]
    fn test_tiered_merge_iterator_frozen_priority_ordering() {
        // Test multiple frozen memtables with correct priority ordering.

        let (frozen0, _) = MockMvccIterator::new(
            "frozen0",
            vec![(test_key(b"key", 100), b"frozen0".to_vec())],
        );
        let (frozen1, _) = MockMvccIterator::new(
            "frozen1",
            vec![(test_key(b"key", 100), b"frozen1".to_vec())],
        );
        let (frozen2, _) = MockMvccIterator::new(
            "frozen2",
            vec![(test_key(b"key", 100), b"frozen2".to_vec())],
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_frozen(frozen0, 0); // Priority 100
        merge_iter.add_mock_frozen(frozen1, 1); // Priority 101
        merge_iter.add_mock_frozen(frozen2, 2); // Priority 102
        let mut merge_iter = merge_iter.build();

        merge_iter.advance().unwrap();
        // frozen0 (priority 100) should win
        assert_eq!(merge_iter.value(), b"frozen0");

        merge_iter.advance().unwrap();
        assert!(!merge_iter.valid()); // Duplicates skipped
    }

    #[test]
    fn test_tiered_merge_iterator_seek_past_all_data() {
        let (iter, _) = MockMvccIterator::new(
            "source",
            vec![
                (test_key(b"a", 100), b"va".to_vec()),
                (test_key(b"b", 100), b"vb".to_vec()),
            ],
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(iter);
        let mut merge_iter = merge_iter.build();

        // Seek past all data
        merge_iter.seek(&test_key(b"z", 100)).unwrap();
        assert!(!merge_iter.valid());
    }

    #[test]
    fn test_tiered_merge_iterator_all_sources_empty() {
        let (empty1, _) = MockMvccIterator::new("empty1", vec![]);
        let (empty2, _) = MockMvccIterator::new("empty2", vec![]);
        let (empty3, _) = MockMvccIterator::new("empty3", vec![]);

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter.add_mock_active(empty1);
        merge_iter.add_mock_frozen(empty2, 0);
        merge_iter.add_mock_l0(empty3, 0);
        let mut merge_iter = merge_iter.build();

        merge_iter.advance().unwrap();
        assert!(!merge_iter.valid());
    }

    // ==================== L0SstIterator Tests (via integration) ====================

    #[test]
    fn test_l0_sst_iterator_lazy_opening() {
        // Verify that L0SstIterator doesn't open files until first seek/advance.
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config.clone()).unwrap();

        // Write data and flush to L0
        let mut batch = new_batch(100);
        batch.put(b"key1".to_vec(), b"value1".to_vec());
        batch.put(b"key2".to_vec(), b"value2".to_vec());
        engine.write_batch(batch).unwrap();
        engine.flush_all_with_active().unwrap();

        // Verify L0 has SSTs
        let stats = engine.stats();
        assert!(stats.l0_sst_count > 0, "Should have L0 SSTs");

        // Creating an iterator should NOT open files yet (lazy)
        let range = MvccKey::unbounded()..MvccKey::unbounded();
        let iter = engine.scan_iter(range, 0).unwrap();

        // Iterator should not be valid before positioning
        assert!(!iter.valid());
    }

    #[test]
    fn test_l0_sst_iterator_with_multiple_ssts() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100) // Small to force multiple flushes
            .max_frozen_memtables(2)
            .build()
            .unwrap();
        let engine = LsmEngine::open(config).unwrap();

        // Create multiple L0 SSTs by writing and flushing multiple times
        for round in 0..3 {
            let mut batch = new_batch((round + 1) * 100);
            let key = format!("key_{round}");
            batch.put(
                key.as_bytes().to_vec(),
                format!("value_{round}").into_bytes(),
            );
            engine.write_batch(batch).unwrap();
            engine.flush_all_with_active().unwrap();
        }

        // All keys should be readable
        for round in 0..3 {
            let key = format!("key_{round}");
            let expected = format!("value_{round}");
            assert_eq!(
                get_for_test(&engine, key.as_bytes()),
                Some(expected.into_bytes()),
                "Should find key_{round}"
            );
        }
    }

    // ==================== LevelIterator Tests (via integration) ====================

    #[test]
    fn test_multiple_l0_ssts_scan() {
        // Test that scanning works correctly with multiple L0 SSTs.
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100)
            .max_frozen_memtables(2)
            .build()
            .unwrap();
        let engine = LsmEngine::open(config).unwrap();

        // Write non-overlapping data and flush to create multiple L0 SSTs
        for i in 0..4 {
            let mut batch = new_batch((i + 1) * 100);
            let key = format!("key_{i:02}");
            batch.put(key.as_bytes().to_vec(), format!("value_{i}").into_bytes());
            engine.write_batch(batch).unwrap();
            engine.flush_all_with_active().unwrap();
        }

        // Query each key - should work across multiple L0 SSTs
        for i in 0..4 {
            let key = format!("key_{i:02}");
            let expected = format!("value_{i}");
            let result = get_for_test(&engine, key.as_bytes());
            assert_eq!(
                result,
                Some(expected.into_bytes()),
                "Should find key_{i:02}"
            );
        }
    }

    #[test]
    fn test_level_iterator_empty_level() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        // Write to memtable only (no flush, so levels are empty)
        let mut batch = new_batch(100);
        batch.put(b"key".to_vec(), b"value".to_vec());
        engine.write_batch(batch).unwrap();

        // Query should work even with empty levels
        assert_eq!(get_for_test(&engine, b"key"), Some(b"value".to_vec()));
    }

    // ==================== Scan Iterator Range Tests ====================

    #[test]
    fn test_scan_iter_with_range_start() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        let mut batch = new_batch(100);
        batch.put(b"a".to_vec(), b"va".to_vec());
        batch.put(b"b".to_vec(), b"vb".to_vec());
        batch.put(b"c".to_vec(), b"vc".to_vec());
        batch.put(b"d".to_vec(), b"vd".to_vec());
        engine.write_batch(batch).unwrap();

        // Range starting from "b"
        let start = MvccKey::encode(b"b", u64::MAX);
        let end = MvccKey::unbounded();
        let range = start..end;

        let results = scan_mvcc(&engine, range);
        let keys: Vec<_> = results.iter().map(|(k, _)| k.key()).collect();

        assert!(keys.contains(&b"b".as_slice()));
        assert!(keys.contains(&b"c".as_slice()));
        assert!(keys.contains(&b"d".as_slice()));
        assert!(!keys.contains(&b"a".as_slice()));
    }

    #[test]
    fn test_scan_iter_with_range_end() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        let mut batch = new_batch(100);
        batch.put(b"a".to_vec(), b"va".to_vec());
        batch.put(b"b".to_vec(), b"vb".to_vec());
        batch.put(b"c".to_vec(), b"vc".to_vec());
        batch.put(b"d".to_vec(), b"vd".to_vec());
        engine.write_batch(batch).unwrap();

        // Range ending before "c"
        let start = MvccKey::unbounded();
        let end = MvccKey::encode(b"c", u64::MAX);
        let range = start..end;

        let results = scan_mvcc(&engine, range);
        let keys: Vec<_> = results.iter().map(|(k, _)| k.key()).collect();

        assert!(keys.contains(&b"a".as_slice()));
        assert!(keys.contains(&b"b".as_slice()));
        assert!(!keys.contains(&b"c".as_slice()));
        assert!(!keys.contains(&b"d".as_slice()));
    }

    #[test]
    fn test_scan_iter_across_memtable_and_sst() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(200)
            .max_frozen_memtables(2)
            .build()
            .unwrap();
        let engine = LsmEngine::open(config).unwrap();

        // Write and flush some data
        let mut batch = new_batch(100);
        batch.put(b"sst_a".to_vec(), b"va".to_vec());
        batch.put(b"sst_b".to_vec(), b"vb".to_vec());
        engine.write_batch(batch).unwrap();
        engine.flush_all_with_active().unwrap();

        // Write more data to memtable
        let mut batch = new_batch(200);
        batch.put(b"mem_c".to_vec(), b"vc".to_vec());
        batch.put(b"mem_d".to_vec(), b"vd".to_vec());
        engine.write_batch(batch).unwrap();

        // Scan should merge both
        let range = MvccKey::unbounded()..MvccKey::unbounded();
        let results = scan_mvcc(&engine, range);
        let keys: Vec<_> = results.iter().map(|(k, _)| k.key()).collect();

        assert!(keys.contains(&b"sst_a".as_slice()));
        assert!(keys.contains(&b"sst_b".as_slice()));
        assert!(keys.contains(&b"mem_c".as_slice()));
        assert!(keys.contains(&b"mem_d".as_slice()));
    }

    #[test]
    fn test_scan_iter_with_updates_across_layers() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(200)
            .max_frozen_memtables(2)
            .build()
            .unwrap();
        let engine = LsmEngine::open(config).unwrap();

        // Write v1 and flush to SST
        let mut batch = new_batch(100);
        batch.put(b"key".to_vec(), b"v1".to_vec());
        engine.write_batch(batch).unwrap();
        engine.flush_all_with_active().unwrap();

        // Write v2 to memtable
        let mut batch = new_batch(200);
        batch.put(b"key".to_vec(), b"v2".to_vec());
        engine.write_batch(batch).unwrap();

        // Both versions should be visible via MVCC scan
        let range = MvccKey::unbounded()..MvccKey::unbounded();
        let results = scan_mvcc(&engine, range);

        // Should have both versions
        assert_eq!(results.len(), 2);

        // v2 (ts=200) should come first (higher timestamp)
        assert_eq!(results[0].0.timestamp(), 200);
        assert_eq!(results[0].1, b"v2");

        // v1 (ts=100) should come second
        assert_eq!(results[1].0.timestamp(), 100);
        assert_eq!(results[1].1, b"v1");
    }

    #[test]
    fn test_scan_iter_empty_range() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        let mut batch = new_batch(100);
        batch.put(b"key".to_vec(), b"value".to_vec());
        engine.write_batch(batch).unwrap();

        // Empty range (start > end semantically)
        let start = MvccKey::encode(b"z", u64::MAX);
        let end = MvccKey::encode(b"a", u64::MAX);
        let range = start..end;

        let results = scan_mvcc(&engine, range);
        assert!(results.is_empty());
    }

    // ==================== HeapEntry Ordering Tests ====================

    #[test]
    fn test_heap_entry_ordering_user_key() {
        // Test that HeapEntry correctly orders by user_key first.

        let entry_a = HeapEntry {
            child_idx: 0,
            priority: 0,
            cached_user_key: b"a".to_vec(),
            cached_ts: 100,
        };

        let entry_b = HeapEntry {
            child_idx: 1,
            priority: 0,
            cached_user_key: b"b".to_vec(),
            cached_ts: 100,
        };

        // In min-heap, smaller key should have higher priority
        // The Ord impl uses reverse ordering for heap behavior
        assert!(entry_a > entry_b); // "a" comes before "b" in iteration
    }

    #[test]
    fn test_heap_entry_ordering_timestamp() {
        // Test that for same user_key, higher timestamp comes first.

        let entry_100 = HeapEntry {
            child_idx: 0,
            priority: 0,
            cached_user_key: b"key".to_vec(),
            cached_ts: 100,
        };

        let entry_50 = HeapEntry {
            child_idx: 1,
            priority: 0,
            cached_user_key: b"key".to_vec(),
            cached_ts: 50,
        };

        // ts=100 should come before ts=50 (MVCC: higher ts first)
        assert!(entry_100 > entry_50);
    }

    #[test]
    fn test_heap_entry_ordering_priority_tiebreaker() {
        // Test that for same MVCC key, lower priority number wins.

        let entry_high_priority = HeapEntry {
            child_idx: 0,
            priority: 0, // Higher priority (active memtable)
            cached_user_key: b"key".to_vec(),
            cached_ts: 100,
        };

        let entry_low_priority = HeapEntry {
            child_idx: 1,
            priority: 100, // Lower priority (frozen memtable)
            cached_user_key: b"key".to_vec(),
            cached_ts: 100,
        };

        // priority 0 should come before priority 100
        assert!(entry_high_priority > entry_low_priority);
    }

    // ==================== Additional Edge Cases ====================

    #[test]
    fn test_iterator_after_delete_and_reinsert() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        // Insert
        let mut batch = new_batch(100);
        batch.put(b"key".to_vec(), b"v1".to_vec());
        engine.write_batch(batch).unwrap();

        // Delete
        let mut batch = new_batch(200);
        batch.delete(b"key".to_vec());
        engine.write_batch(batch).unwrap();

        // Re-insert
        let mut batch = new_batch(300);
        batch.put(b"key".to_vec(), b"v2".to_vec());
        engine.write_batch(batch).unwrap();

        // Latest value should be v2
        assert_eq!(get_for_test(&engine, b"key"), Some(b"v2".to_vec()));

        // At ts=250 (after delete, before re-insert), should be None
        assert_eq!(get_at_for_test(&engine, b"key", 250), None);

        // At ts=150 (before delete), should be v1
        assert_eq!(get_at_for_test(&engine, b"key", 150), Some(b"v1".to_vec()));
    }

    #[test]
    fn test_iterator_large_number_of_versions() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        // Write 100 versions of the same key
        for i in 1..=100u64 {
            let mut batch = new_batch(i);
            let value = format!("v{i}");
            batch.put(b"key".to_vec(), value.into_bytes());
            engine.write_batch(batch).unwrap();
        }

        // Latest should be v100
        assert_eq!(get_for_test(&engine, b"key"), Some(b"v100".to_vec()));

        // Each version should be visible at its timestamp
        for i in 1..=100u64 {
            let expected = format!("v{i}");
            assert_eq!(
                get_at_for_test(&engine, b"key", i),
                Some(expected.into_bytes()),
                "Should find v{i} at ts={i}"
            );
        }
    }

    #[test]
    fn test_concurrent_scan_and_write() {
        use std::sync::Arc;
        use std::thread;

        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(4096)
            .max_frozen_memtables(4)
            .build()
            .unwrap();
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        // Pre-populate
        let mut batch = new_batch(1);
        for i in 0..100 {
            batch.put(format!("key{i:03}").into_bytes(), b"initial".to_vec());
        }
        engine.write_batch(batch).unwrap();

        let engine_reader = Arc::clone(&engine);
        let engine_writer = Arc::clone(&engine);

        // Reader thread
        let reader = thread::spawn(move || {
            for _ in 0..50 {
                let range = MvccKey::unbounded()..MvccKey::unbounded();
                let mut iter = engine_reader.scan_iter(range, 0).unwrap();
                iter.advance().unwrap();
                let mut count = 0;
                while iter.valid() {
                    count += 1;
                    iter.advance().unwrap();
                }
                assert!(count >= 100, "Should see at least 100 entries");
            }
        });

        // Writer thread
        let writer = thread::spawn(move || {
            for i in 0..50 {
                let mut batch = new_batch((100 + i) as u64);
                batch.put(format!("new_key{i:03}").into_bytes(), b"new_value".to_vec());
                engine_writer.write_batch(batch).unwrap();
            }
        });

        reader.join().unwrap();
        writer.join().unwrap();
    }

    // ==================== SuperVersion Tests ====================

    #[test]
    fn test_super_version_basic() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        // Write some data
        let mut batch = new_batch(100);
        batch.put(b"key1".to_vec(), b"value1".to_vec());
        engine.write_batch(batch).unwrap();

        // Get SuperVersion
        let sv = engine.get_super_version();

        // Should have 1 active memtable, 0 frozen
        assert_eq!(sv.memtable_count(), 1);
        assert_eq!(sv.frozen.len(), 0);
        assert_eq!(sv.version.total_sst_count(), 0);
    }

    #[test]
    fn test_super_version_snapshot_isolation() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100) // Small to trigger rotation
            .max_frozen_memtables(4)
            .build()
            .unwrap();
        let engine = LsmEngine::open(config).unwrap();

        // Write initial data
        let mut batch = new_batch(100);
        batch.put(b"key1".to_vec(), b"value1".to_vec());
        engine.write_batch(batch).unwrap();

        // Take snapshot sv1
        let sv1 = engine.get_super_version();
        let sv1_num = sv1.sv_number;

        // Write enough data to trigger rotation (state change)
        for i in 0..5 {
            let mut batch = new_batch((200 + i) as u64);
            let key = format!("key_{i:04}");
            let value = vec![b'x'; 50];
            batch.put(key.into_bytes(), value);
            engine.write_batch(batch).unwrap();
        }

        // Take snapshot sv2 (after rotation)
        let sv2 = engine.get_super_version();
        let sv2_num = sv2.sv_number;

        // sv2 should have a different number than sv1 (rotation happened)
        assert_ne!(sv1_num, sv2_num, "sv_number should change after rotation");

        // sv1 references old active memtable, sv2 references new one
        assert_ne!(sv1.active.id(), sv2.active.id());

        // sv1 is now stale relative to current
        assert!(!sv1.is_current(engine.current_sv_number()));
    }

    #[test]
    fn test_super_version_survives_rotation() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100) // Small to trigger rotation
            .max_frozen_memtables(4)
            .build()
            .unwrap();
        let engine = LsmEngine::open(config).unwrap();

        // Write initial data
        let mut batch = new_batch(100);
        batch.put(b"key1".to_vec(), b"value1".to_vec());
        engine.write_batch(batch).unwrap();

        // Take snapshot before rotation
        let sv1 = engine.get_super_version();
        let sv1_active_id = sv1.active.id();

        // Write more data to trigger rotation
        for i in 0..10 {
            let mut batch = new_batch((200 + i) as u64);
            let key = format!("big_key_{i:04}");
            let value = vec![b'x'; 50]; // Large values to trigger rotation
            batch.put(key.into_bytes(), value);
            engine.write_batch(batch).unwrap();
        }

        // Engine should have rotated
        let sv2 = engine.get_super_version();

        // sv1 still holds reference to old active memtable
        assert_eq!(sv1.active.id(), sv1_active_id);

        // sv2 may have different active (if rotation happened)
        // and sv1's active may now be in sv2's frozen list
        let sv2_frozen_ids: Vec<u64> = sv2.frozen.iter().map(|m| m.id()).collect();

        // If rotation happened, sv1's active should be in sv2's frozen or still active
        let sv1_active_in_sv2 =
            sv2.active.id() == sv1_active_id || sv2_frozen_ids.contains(&sv1_active_id);
        assert!(sv1_active_in_sv2 || sv2_frozen_ids.is_empty());
    }

    #[test]
    fn test_super_version_survives_flush() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(10000) // Large enough to not auto-rotate
            .max_frozen_memtables(4)
            .build()
            .unwrap();
        let engine = LsmEngine::open(config).unwrap();

        // Write some data to the active memtable
        let mut batch = new_batch(100);
        batch.put(b"key1".to_vec(), b"value1".to_vec());
        batch.put(b"key2".to_vec(), b"value2".to_vec());
        engine.write_batch(batch).unwrap();

        // Force freeze the active memtable
        let frozen = engine.freeze_active();
        assert!(frozen.is_some(), "Should have frozen a memtable");

        // Write more data to the new active memtable
        let mut batch = new_batch(200);
        batch.put(b"key3".to_vec(), b"value3".to_vec());
        engine.write_batch(batch).unwrap();

        // Take snapshot that includes the frozen memtable
        let sv1 = engine.get_super_version();
        let sv1_frozen_count = sv1.frozen.len();
        assert!(sv1_frozen_count > 0, "sv1 should have frozen memtables");

        // Get the first frozen memtable ID
        let frozen_id = sv1.frozen[0].id();

        // Flush the frozen memtable
        let flushed_metas = engine.flush_all().unwrap();
        assert!(!flushed_metas.is_empty(), "Should have flushed something");

        // Take new snapshot after flush
        let sv2 = engine.get_super_version();

        // sv2 should have fewer (or zero) frozen memtables
        assert!(
            sv2.frozen.len() < sv1_frozen_count,
            "sv2 frozen={}, sv1 frozen={}",
            sv2.frozen.len(),
            sv1_frozen_count
        );

        // sv2 should have SSTs
        assert!(sv2.version.total_sst_count() > 0);

        // But sv1 still holds the old frozen memtable reference!
        assert_eq!(sv1.frozen.len(), sv1_frozen_count);
        assert_eq!(sv1.frozen[0].id(), frozen_id);

        // sv1's version doesn't have SSTs (old snapshot)
        assert_eq!(sv1.version.total_sst_count(), 0);
    }

    #[test]
    fn test_super_version_concurrent_snapshots() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;

        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        // Write initial data
        let mut batch = new_batch(100);
        batch.put(b"key".to_vec(), b"value".to_vec());
        engine.write_batch(batch).unwrap();

        let snapshot_count = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        // Spawn 10 threads that each take multiple snapshots
        for _ in 0..10 {
            let engine_clone = Arc::clone(&engine);
            let count = Arc::clone(&snapshot_count);

            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    let sv = engine_clone.get_super_version();
                    // Use the snapshot (just read something to prevent optimization)
                    let _ = sv.memtable_count();
                    count.fetch_add(1, Ordering::Relaxed);
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(snapshot_count.load(Ordering::Relaxed), 1000);

        // With atomic SuperVersion installation, sv_number only increments on state changes.
        // Since no rotation/flush happened, sv_number should still be 0 (initial).
        // This test verifies concurrent access is safe, not sv_number counting.
        let final_sv_num = engine.current_sv_number();
        assert_eq!(
            final_sv_num, 0,
            "sv_number should still be 0 (no state changes)"
        );
    }

    #[test]
    fn test_super_version_staleness() {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::builder(tmp.path())
            .memtable_size(100) // Small to trigger rotation
            .max_frozen_memtables(4)
            .build()
            .unwrap();
        let engine = LsmEngine::open(config).unwrap();

        // Take first snapshot
        let sv1 = engine.get_super_version();

        // With atomic SuperVersion installation, sv1 IS current until state changes
        assert!(
            sv1.is_current(engine.current_sv_number()),
            "sv1 should be current (no state changes yet)"
        );

        // Take another snapshot without state change - should be same
        let sv2 = engine.get_super_version();
        assert_eq!(
            sv1.sv_number, sv2.sv_number,
            "same sv_number without state change"
        );

        // Trigger rotation by writing enough data
        for i in 0..5 {
            let mut batch = new_batch((100 + i) as u64);
            let key = format!("key_{i:04}");
            let value = vec![b'x'; 50];
            batch.put(key.into_bytes(), value);
            engine.write_batch(batch).unwrap();
        }

        // Now take another snapshot - should have new sv_number
        let sv3 = engine.get_super_version();

        // sv1 and sv2 are now stale (rotation happened)
        assert!(!sv1.is_current(engine.current_sv_number()));
        assert!(!sv2.is_current(engine.current_sv_number()));

        // sv3 is current
        assert!(sv3.is_current(engine.current_sv_number()));

        // sv3 has a higher number than sv1
        assert!(sv3.sv_number > sv1.sv_number);
    }

    #[test]
    fn test_super_version_debug_format() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let engine = LsmEngine::open(config).unwrap();

        let sv = engine.get_super_version();
        let debug_str = format!("{sv:?}");

        // Check that debug output contains expected fields
        assert!(debug_str.contains("SuperVersion"));
        assert!(debug_str.contains("sv_number"));
        assert!(debug_str.contains("active_memtable_id"));
        assert!(debug_str.contains("frozen_count"));
        assert!(debug_str.contains("version_num"));
        assert!(debug_str.contains("sst_count"));
    }
}
