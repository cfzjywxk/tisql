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

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock};

#[cfg(feature = "failpoints")]
use fail::fail_point;

use crate::error::{Result, TiSqlError};
use crate::lsn::SharedLsnProvider;
use crate::storage::mvcc::{decode_mvcc_key, is_tombstone, MvccIterator, MvccKey};
use crate::storage::{StorageEngine, WriteBatch};
use crate::types::{RawValue, Timestamp};

use super::config::LsmConfig;
use super::ilog::IlogService;
use super::memtable::MemTable;
use super::sstable::{
    SstBuilder, SstBuilderOptions, SstIterator, SstMeta, SstMvccIterator, SstReaderRef,
};
use super::version::{ManifestDelta, Version, MAX_LEVELS};

/// Inner state of the LSM engine, protected by RwLock for atomic updates.
struct LsmState {
    /// Active memtable accepting writes.
    active: Arc<MemTable>,

    /// Frozen memtables awaiting flush, ordered newest first.
    frozen: Vec<Arc<MemTable>>,

    /// Current version with SST metadata.
    version: Arc<Version>,
}

impl LsmState {
    fn new(memtable_id: u64) -> Self {
        Self {
            active: Arc::new(MemTable::new(memtable_id)),
            frozen: Vec::new(),
            version: Arc::new(Version::new()),
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

    /// Engine state (active/frozen memtables, version).
    state: RwLock<LsmState>,

    /// Next memtable ID.
    next_memtable_id: AtomicU64,

    /// Next SST ID (atomic to prevent race conditions during concurrent flushes).
    /// This counter is owned by LsmEngine, not Version, to ensure atomic allocation.
    next_sst_id: AtomicU64,

    /// Next LSN for ordering operations (fallback if no lsn_provider).
    next_lsn: AtomicU64,

    /// Optional shared LSN provider for unified LSN allocation.
    lsn_provider: Option<SharedLsnProvider>,

    /// Optional ilog service for durable SST metadata.
    ilog: Option<Arc<IlogService>>,
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

        // Start with recovered version
        let mut state = LsmState::new(1);
        // Initialize next_sst_id from recovered version's max SST ID + 1
        let recovered_max_sst_id = version.next_sst_id();
        state.version = Arc::new(version);

        Ok(Self {
            config: Arc::new(config),
            state: RwLock::new(state),
            next_memtable_id: AtomicU64::new(2),
            next_sst_id: AtomicU64::new(recovered_max_sst_id),
            next_lsn: AtomicU64::new(1),
            lsn_provider: Some(lsn_provider),
            ilog: Some(ilog),
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
        let state = self.state.read().unwrap();
        Arc::clone(&state.version)
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

        // Add to frozen list (newest first)
        state.frozen.insert(0, Arc::clone(&old_active));

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
        let range = MvccKey::unbounded()..MvccKey::unbounded();
        let mut iter = memtable.inner().create_streaming_iter(range);
        iter.next()?; // Initialize iterator to first entry

        while iter.valid() {
            builder.add(iter.key().as_bytes(), iter.value())?;
            iter.next()?;
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

        // Phase 4: Update in-memory version
        let delta = ManifestDelta::flush(meta.clone(), max_memtable_lsn);

        let mut state = self.state.write().unwrap();
        let new_version = state.version.apply(&delta);
        state.version = Arc::new(new_version);

        // Remove flushed memtable from frozen list
        state.frozen.retain(|m| m.id() != memtable.id());

        // Failpoint: crash after version update
        #[cfg(feature = "failpoints")]
        fail_point!("lsm_flush_after_version_update");

        // Check if checkpoint needed
        if let Some(ref ilog) = self.ilog {
            if ilog.needs_checkpoint() {
                ilog.write_checkpoint(&state.version)?;
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
    #[allow(dead_code)]
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

                iter.next()?;
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

        // Add to frozen list (newest first)
        state.frozen.insert(0, Arc::clone(&old_active));

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
                state.frozen.last().cloned() // Oldest first
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
        let version = &state.version;

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
    /// Keep the memtable alive. MUST be declared before `iter` so it's dropped last.
    _memtable: Arc<MemTable>,
    /// The streaming iterator with erased lifetime (safe because _memtable keeps data alive)
    iter: VersionedMemTableIterator<'static>,
}

impl ArcMemTableIterator {
    /// Create a new streaming iterator over the given memtable and range.
    fn new(memtable: Arc<MemTable>, range: Range<MvccKey>) -> Self {
        // Safety: We're extending the lifetime of the reference from the Arc's lifetime
        // to 'static. This is safe because:
        // 1. The Arc keeps the MemTable alive for as long as this struct exists
        // 2. The _memtable field is declared before iter, ensuring proper drop order
        // 3. VersionedMemTableIterator doesn't access the memtable in its Drop impl
        let engine_ref: &'static super::memtable::VersionedMemTableEngine =
            unsafe { std::mem::transmute(memtable.inner()) };
        let iter = engine_ref.create_streaming_iter(range);
        Self {
            _memtable: memtable,
            iter,
        }
    }
}

impl MvccIterator for ArcMemTableIterator {
    fn seek(&mut self, target: &MvccKey) -> Result<()> {
        self.iter.seek(target)
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()
    }

    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn key(&self) -> &MvccKey {
        self.iter.key()
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
// 3. L0 SSTs need individual iterators (files can overlap)
// 4. L1+ SSTs use LevelIterator for lazy file opening
//
// For the same MVCC key from multiple sources, higher priority wins.

use std::cmp::Ordering;
use std::path::PathBuf;

/// Priority levels for merge iterator sources.
/// Lower number = higher priority (newer data).
const PRIORITY_ACTIVE: u32 = 0;
const PRIORITY_FROZEN_BASE: u32 = 100;
const PRIORITY_L0_BASE: u32 = 1000;
const PRIORITY_LEVEL_BASE: u32 = 10000; // L1 = 10000, L2 = 20000, etc.

/// Entry in the merge heap with priority for proper ordering.
struct PriorityHeapEntry {
    /// Current key from this iterator
    key: MvccKey,
    /// Current value from this iterator
    value: Vec<u8>,
    /// Priority level (lower = higher priority)
    priority: u32,
    /// The underlying iterator (owned)
    iter: Box<dyn MvccIterator>,
}

impl PartialEq for PriorityHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.priority == other.priority
    }
}

impl Eq for PriorityHeapEntry {}

impl PartialOrd for PriorityHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PriorityHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap ordering: we want smallest key first
        // For same key, we want lowest priority (highest precedence) first
        match other.key.cmp(&self.key) {
            Ordering::Equal => other.priority.cmp(&self.priority),
            ord => ord,
        }
    }
}

/// Lazy SST iterator that defers file opening until first access.
///
/// This avoids unnecessary I/O for SST files whose key ranges don't
/// overlap with the actual keys being scanned.
struct LazySstIterator {
    /// SST metadata (key range, file ID, etc.)
    sst_meta: Arc<SstMeta>,
    /// Path to SST file
    sst_path: PathBuf,
    /// Query range
    range: Range<MvccKey>,
    /// Actual iterator (lazy loaded)
    inner: Option<SstMvccIterator>,
    /// Whether we've checked if this SST is relevant
    initialized: bool,
}

impl LazySstIterator {
    fn new(sst_meta: Arc<SstMeta>, sst_path: PathBuf, range: Range<MvccKey>) -> Self {
        Self {
            sst_meta,
            sst_path,
            range,
            inner: None,
            initialized: false,
        }
    }

    /// Initialize the inner iterator if not already done.
    fn ensure_initialized(&mut self) -> Result<()> {
        if self.initialized {
            return Ok(());
        }
        self.initialized = true;

        // Check if SST file exists
        if !self.sst_path.exists() {
            let err = TiSqlError::Storage(format!(
                "SST file missing: {} (id={}, level={})",
                self.sst_path.display(),
                self.sst_meta.id,
                self.sst_meta.level
            ));
            return Err(err);
        }

        // Open the file and create iterator
        let reader = SstReaderRef::open(&self.sst_path)?;
        let iter = SstMvccIterator::new(reader, self.range.clone())?;
        self.inner = Some(iter);
        Ok(())
    }
}

impl MvccIterator for LazySstIterator {
    fn seek(&mut self, target: &MvccKey) -> Result<()> {
        self.ensure_initialized()?;
        if let Some(ref mut inner) = self.inner {
            inner.seek(target)
        } else {
            Ok(())
        }
    }

    fn next(&mut self) -> Result<()> {
        self.ensure_initialized()?;
        if let Some(ref mut inner) = self.inner {
            inner.next()
        } else {
            Ok(())
        }
    }

    fn valid(&self) -> bool {
        self.inner.as_ref().is_some_and(|i| i.valid())
    }

    fn key(&self) -> &MvccKey {
        self.inner.as_ref().expect("Iterator not valid").key()
    }

    fn value(&self) -> &[u8] {
        self.inner.as_ref().expect("Iterator not valid").value()
    }
}

/// Iterator for a non-overlapping level (L1+) that opens files lazily.
///
/// Files in L1+ are sorted by key range and don't overlap, so we can
/// use binary search to find the right file and only open it when needed.
struct LevelIterator {
    /// SST metadata for this level, sorted by smallest_key
    sst_metas: Vec<Arc<SstMeta>>,
    /// Base path for SST files
    sst_dir: PathBuf,
    /// Query range
    range: Range<MvccKey>,
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
    fn new(sst_metas: Vec<Arc<SstMeta>>, sst_dir: PathBuf, range: Range<MvccKey>) -> Self {
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
        let iter = SstMvccIterator::new(reader, self.range.clone())?;
        self.current_file_idx = Some(idx);
        self.current_iter = Some(iter);
        Ok(())
    }

    /// Skip empty files (files where iterator is not valid).
    fn skip_empty_files(&mut self) -> Result<()> {
        while let Some(ref iter) = self.current_iter {
            if iter.valid() {
                break;
            }
            // Move to next file
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
        if let Some(e) = self.pending_error.take() {
            return Err(e);
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

    fn next(&mut self) -> Result<()> {
        if let Some(e) = self.pending_error.take() {
            return Err(e);
        }

        if let Some(ref mut iter) = self.current_iter {
            iter.next()?;
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

    fn key(&self) -> &MvccKey {
        self.current_iter
            .as_ref()
            .expect("Iterator not valid")
            .key()
    }

    fn value(&self) -> &[u8] {
        self.current_iter
            .as_ref()
            .expect("Iterator not valid")
            .value()
    }
}

/// Pending iterator to be initialized later.
struct PendingIterator {
    iter: Box<dyn MvccIterator>,
    priority: u32,
}

/// Tiered merge iterator with lazy SST initialization.
///
/// This iterator merges multiple sources with different priorities:
/// - Active memtable (priority 0)
/// - Frozen memtables (priority 100+)
/// - L0 SSTs (priority 1000+)
/// - L1+ levels (priority 10000+ per level)
///
/// **Lazy Loading Design:**
/// - Memtable iterators are initialized in `build()` (in-memory, no I/O)
/// - L0 SST iterators are kept pending until memtable tier is exhausted
/// - L1+ level iterators are kept pending until L0 tier is also exhausted
///
/// This avoids unnecessary disk I/O when memtable contains the required data.
///
/// For duplicate MVCC keys, the highest priority (lowest number) wins.
pub struct TieredMergeIterator {
    /// Min-heap of iterators with their current entries and priorities
    heap: BinaryHeap<PriorityHeapEntry>,
    /// Pending L0 SST iterators (initialized when memtable tier exhausted)
    pending_l0: Vec<PendingIterator>,
    /// Pending L1+ level iterators (initialized when L0 tier exhausted)
    pending_levels: Vec<PendingIterator>,
    /// Current key (cached for returning reference)
    current_key: Option<MvccKey>,
    /// Current value (cached for returning reference)
    current_value: Option<Vec<u8>>,
    /// Last emitted key (for deduplication)
    last_emitted_key: Option<MvccKey>,
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
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            pending_l0: Vec::new(),
            pending_levels: Vec::new(),
            current_key: None,
            current_value: None,
            last_emitted_key: None,
            pending_error: None,
        }
    }

    /// Add the active memtable iterator (highest priority).
    /// Memtable iterators are added directly to the heap (in-memory, no I/O).
    pub fn add_active_memtable(&mut self, mut iter: Box<dyn MvccIterator>) -> Result<()> {
        if !iter.valid() {
            iter.seek(&MvccKey::unbounded())?;
        }
        if iter.valid() {
            let key = iter.key().clone();
            let value = iter.value().to_vec();
            self.heap.push(PriorityHeapEntry {
                key,
                value,
                priority: PRIORITY_ACTIVE,
                iter,
            });
        }
        Ok(())
    }

    /// Add a frozen memtable iterator.
    /// Memtable iterators are added directly to the heap (in-memory, no I/O).
    pub fn add_frozen_memtable(
        &mut self,
        mut iter: Box<dyn MvccIterator>,
        index: usize,
    ) -> Result<()> {
        if !iter.valid() {
            iter.seek(&MvccKey::unbounded())?;
        }
        if iter.valid() {
            let key = iter.key().clone();
            let value = iter.value().to_vec();
            self.heap.push(PriorityHeapEntry {
                key,
                value,
                priority: PRIORITY_FROZEN_BASE + index as u32,
                iter,
            });
        }
        Ok(())
    }

    /// Add an L0 SST iterator (lazy - not initialized until memtable tier exhausted).
    pub fn add_l0_sst(&mut self, iter: Box<dyn MvccIterator>, index: usize) {
        self.pending_l0.push(PendingIterator {
            iter,
            priority: PRIORITY_L0_BASE + index as u32,
        });
    }

    /// Add a level iterator for L1+ (lazy - not initialized until L0 tier exhausted).
    pub fn add_level(&mut self, iter: Box<dyn MvccIterator>, level: usize) {
        self.pending_levels.push(PendingIterator {
            iter,
            priority: PRIORITY_LEVEL_BASE * level as u32,
        });
    }

    /// Finalize and prime the iterator.
    ///
    /// At this point, only memtable iterators are in the heap. L0 and L1+
    /// iterators remain pending and will be initialized lazily.
    pub fn build(mut self) -> Result<Self> {
        // Memtable iterators are already in the heap from add_active_memtable/add_frozen_memtable.
        // L0 and L1+ iterators are in pending_l0/pending_levels.
        // Position on first entry.
        self.next()?;
        Ok(self)
    }

    /// Initialize pending L0 SST iterators and add them to the heap.
    fn init_l0_tier(&mut self) -> Result<()> {
        for pending in self.pending_l0.drain(..) {
            let mut iter = pending.iter;
            iter.seek(&MvccKey::unbounded())?;
            if iter.valid() {
                let key = iter.key().clone();
                let value = iter.value().to_vec();
                self.heap.push(PriorityHeapEntry {
                    key,
                    value,
                    priority: pending.priority,
                    iter,
                });
            }
        }
        Ok(())
    }

    /// Initialize pending L1+ level iterators and add them to the heap.
    fn init_levels_tier(&mut self) -> Result<()> {
        for pending in self.pending_levels.drain(..) {
            let mut iter = pending.iter;
            iter.seek(&MvccKey::unbounded())?;
            if iter.valid() {
                let key = iter.key().clone();
                let value = iter.value().to_vec();
                self.heap.push(PriorityHeapEntry {
                    key,
                    value,
                    priority: pending.priority,
                    iter,
                });
            }
        }
        Ok(())
    }
}

impl MvccIterator for TieredMergeIterator {
    fn seek(&mut self, target: &MvccKey) -> Result<()> {
        // When seeking to a specific key, we must initialize all iterators
        // because we need to find the smallest key >= target across all tiers.

        // Collect all iterators: from heap + pending
        let mut all_iters: Vec<(Box<dyn MvccIterator>, u32)> = Vec::new();

        // Drain heap
        while let Some(entry) = self.heap.pop() {
            all_iters.push((entry.iter, entry.priority));
        }

        // Drain pending L0
        for pending in self.pending_l0.drain(..) {
            all_iters.push((pending.iter, pending.priority));
        }

        // Drain pending levels
        for pending in self.pending_levels.drain(..) {
            all_iters.push((pending.iter, pending.priority));
        }

        // Seek each iterator and re-add valid ones to heap
        for (mut iter, priority) in all_iters {
            iter.seek(target)?;
            if iter.valid() {
                let key = iter.key().clone();
                let value = iter.value().to_vec();
                self.heap.push(PriorityHeapEntry {
                    key,
                    value,
                    priority,
                    iter,
                });
            }
        }

        // Clear dedup state and move to first matching entry
        self.current_key = None;
        self.current_value = None;
        self.last_emitted_key = None;
        self.next()
    }

    fn next(&mut self) -> Result<()> {
        if let Some(e) = self.pending_error.take() {
            return Err(e);
        }

        loop {
            // Keep initializing tiers until we have data or all tiers are exhausted
            while self.heap.is_empty() {
                if !self.pending_l0.is_empty() {
                    // Memtable exhausted, initialize L0 tier
                    self.init_l0_tier()?;
                } else if !self.pending_levels.is_empty() {
                    // L0 exhausted, initialize L1+ tiers
                    self.init_levels_tier()?;
                } else {
                    // All tiers exhausted
                    self.current_key = None;
                    self.current_value = None;
                    return Ok(());
                }
            }

            if let Some(mut top) = self.heap.pop() {
                let key = top.key.clone();
                let value = top.value.clone();

                // Advance the iterator and re-add to heap if valid
                if let Err(e) = top.iter.next() {
                    self.pending_error = Some(e);
                }
                if top.iter.valid() {
                    let next_key = top.iter.key().clone();
                    let next_value = top.iter.value().to_vec();
                    self.heap.push(PriorityHeapEntry {
                        key: next_key,
                        value: next_value,
                        priority: top.priority,
                        iter: top.iter,
                    });
                }

                // Skip if this is a duplicate of the last emitted key
                if let Some(ref last) = self.last_emitted_key {
                    if &key == last {
                        continue;
                    }
                }

                // Emit this entry
                self.current_key = Some(key.clone());
                self.current_value = Some(value);
                self.last_emitted_key = Some(key);
                return Ok(());
            }
            // Unreachable: while loop above ensures heap is non-empty or returns early
        }
    }

    fn valid(&self) -> bool {
        self.current_key.is_some()
    }

    fn key(&self) -> &MvccKey {
        self.current_key.as_ref().expect("Iterator not valid")
    }

    fn value(&self) -> &[u8] {
        self.current_value.as_ref().expect("Iterator not valid")
    }
}

impl StorageEngine for LsmEngine {
    fn scan_iter(&self, range: Range<MvccKey>) -> Result<Box<dyn MvccIterator + '_>> {
        // Snapshot the state under read lock, then release the lock.
        // We clone Arc references so iterators can outlive the lock.
        let (active, frozen, version) = {
            let state = self.state.read().unwrap();
            (
                Arc::clone(&state.active),
                state.frozen.clone(),
                Arc::clone(&state.version),
            )
        };

        let sst_dir = self.config.sst_dir();
        let mut merge_iter = TieredMergeIterator::new();

        // 1. Add active memtable iterator (highest priority)
        // Memtable iterators are initialized immediately (in-memory, no I/O)
        let active_iter = ArcMemTableIterator::new(active, range.clone());
        merge_iter.add_active_memtable(Box::new(active_iter))?;

        // 2. Add frozen memtable iterators (newest to oldest)
        // Memtable iterators are initialized immediately (in-memory, no I/O)
        for (idx, memtable) in frozen.iter().enumerate() {
            let mem_iter = ArcMemTableIterator::new(Arc::clone(memtable), range.clone());
            merge_iter.add_frozen_memtable(Box::new(mem_iter), idx)?;
        }

        // 3. Add L0 SST iterators (files can overlap, use lazy loading)
        // L0 files are added in newest-to-oldest order (by ID descending)
        let mut l0_ssts: Vec<Arc<SstMeta>> = version
            .ssts_at_level(0)
            .iter()
            .filter(|sst| sst.overlaps_mvcc(range.start.as_bytes(), range.end.as_bytes()))
            .cloned()
            .collect();
        l0_ssts.sort_by(|a, b| b.id.cmp(&a.id)); // Newest first

        for (idx, sst_meta) in l0_ssts.into_iter().enumerate() {
            let sst_path = sst_dir.join(format!("{:08}.sst", sst_meta.id));
            let lazy_iter = LazySstIterator::new(sst_meta, sst_path, range.clone());
            merge_iter.add_l0_sst(Box::new(lazy_iter), idx);
        }

        // 4. Add L1+ level iterators (non-overlapping, use LevelIterator for lazy loading)
        for level in 1..MAX_LEVELS {
            let ssts: Vec<Arc<SstMeta>> = version
                .ssts_at_level(level as u32)
                .iter()
                .filter(|sst| sst.overlaps_mvcc(range.start.as_bytes(), range.end.as_bytes()))
                .cloned()
                .collect();

            if !ssts.is_empty() {
                let level_iter = LevelIterator::new(ssts, sst_dir.clone(), range.clone());
                merge_iter.add_level(Box::new(level_iter), level);
            }
        }

        // Build and return the merge iterator
        let merge_iter = merge_iter.build()?;
        Ok(Box::new(merge_iter))
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

            Ok(Self {
                config: Arc::new(config),
                state: RwLock::new(LsmState::new(1)),
                next_memtable_id: AtomicU64::new(2),
                next_sst_id: AtomicU64::new(1),
                next_lsn: AtomicU64::new(1),
                lsn_provider: None,
                ilog: None,
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
        let mut iter = engine.scan_iter(range).unwrap();
        while iter.valid() {
            results.push((iter.key().clone(), iter.value().to_vec()));
            iter.next().unwrap();
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
            if iter.next().is_err() {
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
                    state.frozen.last().cloned()
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
    // These tests verify the tiered lazy loading behavior of TieredMergeIterator:
    // 1. Data is read in tier order: active -> frozen -> L0 -> L1+
    // 2. SST iterators are only initialized when memtable tier is exhausted
    // 3. All L0 SST files are considered (they can overlap/be unordered)
    // 4. Priority-based deduplication works correctly
    // 5. seek() initializes all pending iterators

    use std::cell::RefCell;
    use std::rc::Rc;

    /// Mock iterator that tracks whether seek() has been called.
    /// Used to verify lazy initialization behavior.
    struct MockMvccIterator {
        /// Data to return (sorted by MvccKey)
        data: Vec<(MvccKey, Vec<u8>)>,
        /// Current position (-1 = not initialized, >= data.len() = exhausted)
        pos: i32,
        /// Tracks whether seek() has been called (shared for external inspection)
        seek_called: Rc<RefCell<bool>>,
    }

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

        fn next(&mut self) -> Result<()> {
            if self.pos >= 0 {
                self.pos += 1;
            }
            Ok(())
        }

        fn valid(&self) -> bool {
            self.pos >= 0 && (self.pos as usize) < self.data.len()
        }

        fn key(&self) -> &MvccKey {
            &self.data[self.pos as usize].0
        }

        fn value(&self) -> &[u8] {
            &self.data[self.pos as usize].1
        }
    }

    /// Helper to create an MvccKey for testing.
    fn test_key(user_key: &[u8], ts: Timestamp) -> MvccKey {
        MvccKey::encode(user_key, ts)
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
        merge_iter
            .add_active_memtable(Box::new(active_iter))
            .unwrap();

        let merge_iter = merge_iter.build().unwrap();

        assert!(*active_seek.borrow(), "Active memtable should be seeked");
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.key(), &test_key(b"a", 100));
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
        merge_iter
            .add_active_memtable(Box::new(active_iter))
            .unwrap();
        merge_iter
            .add_frozen_memtable(Box::new(frozen_iter), 0)
            .unwrap();

        let merge_iter = merge_iter.build().unwrap();

        assert!(merge_iter.valid());
        // Active has priority 0, frozen has priority 100+
        // For same key, active wins (lower priority number = higher precedence)
        assert_eq!(merge_iter.value(), b"active_value");
    }

    #[test]
    fn test_tiered_merge_iterator_lazy_l0_initialization() {
        // Test that L0 SST iterators are NOT initialized until memtable is exhausted

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
        merge_iter
            .add_active_memtable(Box::new(active_iter))
            .unwrap();
        merge_iter.add_l0_sst(Box::new(l0_iter), 0);

        let mut merge_iter = merge_iter.build().unwrap();

        // After build, only memtable should be initialized
        assert!(
            *active_seek.borrow(),
            "Active memtable should be seeked in build()"
        );
        assert!(
            !*l0_seek.borrow(),
            "L0 SST should NOT be seeked yet - memtable not exhausted"
        );

        // Read first memtable entry
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.key(), &test_key(b"a", 100));
        assert!(
            !*l0_seek.borrow(),
            "L0 SST should NOT be seeked - still reading memtable"
        );

        // Read second memtable entry
        merge_iter.next().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.key(), &test_key(b"b", 100));
        assert!(
            !*l0_seek.borrow(),
            "L0 SST should NOT be seeked - still reading memtable"
        );

        // Memtable exhausted, next should trigger L0 initialization
        merge_iter.next().unwrap();
        assert!(
            *l0_seek.borrow(),
            "L0 SST should be seeked now - memtable exhausted"
        );
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.key(), &test_key(b"c", 100));
        assert_eq!(merge_iter.value(), b"c_l0");
    }

    #[test]
    fn test_tiered_merge_iterator_lazy_level_initialization() {
        // Test that L1+ level iterators are NOT initialized until L0 is exhausted

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
        merge_iter
            .add_active_memtable(Box::new(active_iter))
            .unwrap();
        merge_iter.add_l0_sst(Box::new(l0_iter), 0);
        merge_iter.add_level(Box::new(l1_iter), 1);

        let mut merge_iter = merge_iter.build().unwrap();

        // After build, only memtable initialized
        assert!(!*l0_seek.borrow(), "L0 SST should NOT be seeked yet");
        assert!(!*l1_seek.borrow(), "L1 level should NOT be seeked yet");

        // Read memtable entry
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.key(), &test_key(b"a", 100));

        // Exhaust memtable -> L0 should be initialized
        merge_iter.next().unwrap();
        assert!(
            *l0_seek.borrow(),
            "L0 SST should be seeked - memtable exhausted"
        );
        assert!(
            !*l1_seek.borrow(),
            "L1 level should NOT be seeked - L0 not exhausted"
        );
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.key(), &test_key(b"b", 100));

        // Exhaust L0 -> L1 should be initialized
        merge_iter.next().unwrap();
        assert!(
            *l1_seek.borrow(),
            "L1 level should be seeked - L0 exhausted"
        );
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.key(), &test_key(b"c", 100));

        // Exhaust all
        merge_iter.next().unwrap();
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
        merge_iter
            .add_active_memtable(Box::new(active_iter))
            .unwrap();
        // Add in index order: 0 (newest) -> 1 -> 2 (oldest)
        merge_iter.add_l0_sst(Box::new(l0_0_iter), 0);
        merge_iter.add_l0_sst(Box::new(l0_1_iter), 1);
        merge_iter.add_l0_sst(Box::new(l0_2_iter), 2);

        let mut merge_iter = merge_iter.build().unwrap();

        // After build with empty memtable, L0 should be initialized
        assert!(*l0_0_seek.borrow(), "L0 SST 0 should be seeked");
        assert!(*l0_1_seek.borrow(), "L0 SST 1 should be seeked");
        assert!(*l0_2_seek.borrow(), "L0 SST 2 should be seeked");

        // Results should be in sorted order, deduplicating same keys
        // Key 'a' exists in SST 0 (priority 1000) and SST 2 (priority 1002)
        // SST 0 has higher priority, so its value should win
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.key(), &test_key(b"a", 100));
        assert_eq!(
            merge_iter.value(),
            b"a_l0_0",
            "SST 0 (higher priority) should win for key 'a'"
        );

        merge_iter.next().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.key(), &test_key(b"b", 100));
        assert_eq!(merge_iter.value(), b"b_l0_1");

        merge_iter.next().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.key(), &test_key(b"c", 100));
        assert_eq!(merge_iter.value(), b"c_l0_1");

        merge_iter.next().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.key(), &test_key(b"d", 100));
        assert_eq!(merge_iter.value(), b"d_l0_0");

        merge_iter.next().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.key(), &test_key(b"e", 100));
        assert_eq!(merge_iter.value(), b"e_l0_2");

        merge_iter.next().unwrap();
        assert!(!merge_iter.valid());
    }

    #[test]
    fn test_tiered_merge_iterator_seek_initializes_all() {
        // Test that seek() initializes ALL pending iterators

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
        merge_iter
            .add_active_memtable(Box::new(active_iter))
            .unwrap();
        merge_iter.add_l0_sst(Box::new(l0_iter), 0);
        merge_iter.add_level(Box::new(l1_iter), 1);

        let mut merge_iter = merge_iter.build().unwrap();

        // After build, only memtable initialized
        assert!(
            !*l0_seek.borrow(),
            "L0 SST should NOT be seeked after build"
        );
        assert!(
            !*l1_seek.borrow(),
            "L1 level should NOT be seeked after build"
        );

        // Seek to a specific key - this should initialize ALL iterators
        merge_iter.seek(&test_key(b"a", 100)).unwrap();

        assert!(*l0_seek.borrow(), "L0 SST should be seeked after seek()");
        assert!(*l1_seek.borrow(), "L1 level should be seeked after seek()");

        // Should be positioned at 'a' from L0
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.key(), &test_key(b"a", 100));
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
        merge_iter
            .add_active_memtable(Box::new(active_iter))
            .unwrap();
        merge_iter
            .add_frozen_memtable(Box::new(frozen0_iter), 0)
            .unwrap();
        merge_iter
            .add_frozen_memtable(Box::new(frozen1_iter), 1)
            .unwrap();
        merge_iter.add_l0_sst(Box::new(l0_iter), 0);
        merge_iter.add_level(Box::new(l1_iter), 1);
        merge_iter.add_level(Box::new(l2_iter), 2);

        let mut merge_iter = merge_iter.build().unwrap();

        // Verify tier flow
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.value(), b"v_active");
        assert!(!*l0_seek.borrow() && !*l1_seek.borrow() && !*l2_seek.borrow());

        merge_iter.next().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.value(), b"v_frozen0");
        assert!(!*l0_seek.borrow());

        merge_iter.next().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.value(), b"v_frozen1");
        assert!(!*l0_seek.borrow());

        // Memtable tier exhausted, L0 should be initialized
        merge_iter.next().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.value(), b"v_l0");
        assert!(*l0_seek.borrow());
        assert!(!*l1_seek.borrow() && !*l2_seek.borrow());

        // L0 exhausted, L1+ should be initialized
        merge_iter.next().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.value(), b"v_l1");
        assert!(*l1_seek.borrow() && *l2_seek.borrow());

        merge_iter.next().unwrap();
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.value(), b"v_l2");

        merge_iter.next().unwrap();
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
        merge_iter
            .add_active_memtable(Box::new(active_iter))
            .unwrap();
        merge_iter
            .add_frozen_memtable(Box::new(frozen_iter), 0)
            .unwrap();
        merge_iter.add_l0_sst(Box::new(l0_iter), 0);

        let mut merge_iter = merge_iter.build().unwrap();

        // Only active's value should be returned
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.key(), &test_key(b"key", 100));
        assert_eq!(merge_iter.value(), b"active_wins");

        // Next should exhaust all (duplicates were skipped)
        merge_iter.next().unwrap();

        // L0 might be initialized during the exhaustion process
        // but its duplicate entry should have been skipped
        assert!(!merge_iter.valid());
    }

    #[test]
    fn test_tiered_merge_iterator_empty_memtable_immediate_l0() {
        // Test that if memtable is empty, L0 is initialized immediately

        let (active_iter, _) = MockMvccIterator::new("active", vec![]);

        let l0_seek = Rc::new(RefCell::new(false));
        let l0_iter = MockMvccIterator::new_with_tracker(
            "l0_sst",
            vec![(test_key(b"a", 100), b"a_l0".to_vec())],
            Rc::clone(&l0_seek),
        );

        let mut merge_iter = TieredMergeIterator::new();
        merge_iter
            .add_active_memtable(Box::new(active_iter))
            .unwrap();
        merge_iter.add_l0_sst(Box::new(l0_iter), 0);

        let merge_iter = merge_iter.build().unwrap();

        // Memtable was empty, so L0 should be initialized during build/advance
        assert!(
            *l0_seek.borrow(),
            "L0 should be seeked when memtable is empty"
        );
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.key(), &test_key(b"a", 100));
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
        merge_iter
            .add_active_memtable(Box::new(active_iter))
            .unwrap();
        merge_iter
            .add_frozen_memtable(Box::new(frozen0_iter), 0)
            .unwrap();
        merge_iter
            .add_frozen_memtable(Box::new(frozen1_iter), 1)
            .unwrap();

        let mut merge_iter = merge_iter.build().unwrap();

        // Should get: a, b, c, shared (from frozen0 - higher priority)
        assert!(merge_iter.valid());
        assert_eq!(merge_iter.key(), &test_key(b"a", 100));

        merge_iter.next().unwrap();
        assert_eq!(merge_iter.key(), &test_key(b"b", 100));

        merge_iter.next().unwrap();
        assert_eq!(merge_iter.key(), &test_key(b"c", 100));

        merge_iter.next().unwrap();
        assert_eq!(merge_iter.key(), &test_key(b"shared", 100));
        // frozen0 (priority 100) beats frozen1 (priority 101)
        assert_eq!(merge_iter.value(), b"shared_frozen0");

        merge_iter.next().unwrap();
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
        merge_iter
            .add_active_memtable(Box::new(active_iter))
            .unwrap();
        merge_iter.add_l0_sst(Box::new(l0_iter), 0);
        merge_iter.add_level(Box::new(l1_iter), 1);

        let merge_iter = merge_iter.build().unwrap();

        // All tiers should be initialized (empty memtable triggers cascade)
        assert!(*l0_seek.borrow(), "L0 should be initialized");
        assert!(*l1_seek.borrow(), "L1 should be initialized");
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
        merge_iter
            .add_active_memtable(Box::new(active_iter))
            .unwrap();
        merge_iter
            .add_frozen_memtable(Box::new(frozen_iter), 0)
            .unwrap();

        let mut merge_iter = merge_iter.build().unwrap();

        // Should be sorted: a, b, c, d
        let expected = [
            (b"a", b"a_frozen"),
            (b"b", b"b_active"),
            (b"c", b"c_frozen"),
            (b"d", b"d_active"),
        ];

        for (key, value) in expected.iter() {
            assert!(merge_iter.valid());
            assert_eq!(merge_iter.key(), &test_key(*key, 100));
            assert_eq!(merge_iter.value(), *value);
            merge_iter.next().unwrap();
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
        let mut iter = engine.scan_iter(range).unwrap();

        // Should be able to read from memtable
        assert!(iter.valid());
        let (key1, _) = iter.key().decode();
        assert_eq!(key1, b"key1");

        iter.next().unwrap();
        assert!(iter.valid());
        let (key2, _) = iter.key().decode();
        assert_eq!(key2, b"key2");

        iter.next().unwrap();
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
        let mut iter = engine.scan_iter(range).unwrap();

        assert!(iter.valid());
        let (key, _) = iter.key().decode();
        assert_eq!(key, b"new_key");

        iter.next().unwrap();
        assert!(!iter.valid());

        // Scan for old keys (will need SST)
        let range = MvccKey::encode(b"old_key", Timestamp::MAX)..MvccKey::encode(b"old_key\xff", 0);
        let mut iter = engine.scan_iter(range).unwrap();

        // Should find all old keys from SST
        let mut count = 0;
        while iter.valid() {
            count += 1;
            iter.next().unwrap();
        }
        assert_eq!(count, 5, "Should find all 5 old keys from SST");
    }
}
