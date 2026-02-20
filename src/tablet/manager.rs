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

//! Tablet manager skeleton for phase-2 tablet separation.
//!
//! Phase-2 keeps the storage data path single-tablet equivalent while
//! introducing tablet lifecycle ownership and directory inventory.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::catalog::types::{Lsn, Timestamp};
use crate::inner_table::catalog_loader::CatalogCache;
use crate::log_info;
use crate::log_warn;
use crate::lsn::SharedLsnProvider;
use crate::tablet::commit_reservations::{CommitLsnReservations, CommitReservationStats};
use crate::tablet::CacheSuite;
use crate::util::error::{Result, TiSqlError};

use super::router::{route_index_to_tablet, route_table_to_tablet, TabletId};
use super::{CompactionScheduler, FlushScheduler, IlogTruncateStats, TabletEngine, Version};

/// One tablet checkpoint capture result.
pub struct TabletCheckpointCapture {
    pub tablet_id: TabletId,
    pub version: Arc<Version>,
    pub checkpoint_lsn: Lsn,
}

/// Global shared-clog boundary computed from all mounted tablet caps.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct GlobalLogGcBoundary {
    /// Min checkpointed flushed_lsn across tablets.
    pub checkpoint_flushed_cap: Lsn,
    /// Min `min_unflushed_lsn - 1` across tablets with unflushed data.
    pub unflushed_cap: Option<Lsn>,
    /// Global shared reservation cap (`min_reserved_lsn - 1`).
    pub reservation_cap: Option<Lsn>,
    /// Global per-tablet in-flight cap (`min_in_flight_lsn - 1`).
    pub inflight_cap: Option<Lsn>,
    /// Final safe shared-clog truncation boundary.
    pub safe_lsn: Lsn,
}

/// One tablet-ilog truncation result.
#[derive(Debug)]
pub struct TabletIlogTruncateCapture {
    pub tablet_id: TabletId,
    pub checkpoint_lsn: Lsn,
    pub stats: IlogTruncateStats,
}

struct TabletWorkerSet {
    flush: FlushScheduler,
    compaction: CompactionScheduler,
}

/// Derive desired tablet inventory from loaded catalog metadata.
///
/// Rules:
/// - always include the shared system tablet
/// - include dedicated tablets only for user tables / user local indexes
/// - inner/system objects remain pinned to `TabletId::System`
pub fn derive_tablet_inventory(cache: &CatalogCache) -> BTreeSet<TabletId> {
    let mut tablets = BTreeSet::new();
    tablets.insert(TabletId::System);

    for table in cache.tables.values() {
        let table_tablet = route_table_to_tablet(table.id());
        tablets.insert(table_tablet);

        for index in table.indexes() {
            let index_tablet = route_index_to_tablet(table.id(), index.id());
            tablets.insert(index_tablet);
        }
    }

    tablets
}

/// Tablet lifecycle owner.
///
/// Phase-2 keeps one mounted system tablet but starts tracking desired tablet
/// inventory and tablet directories for user objects.
pub struct TabletManager {
    data_dir: PathBuf,
    tablets: RwLock<BTreeMap<TabletId, Arc<TabletEngine>>>,
    desired_tablets: RwLock<BTreeSet<TabletId>>,
    background_runtime: RwLock<Option<tokio::runtime::Handle>>,
    tablet_workers: RwLock<BTreeMap<TabletId, TabletWorkerSet>>,
    cache_suite: RwLock<Option<Arc<CacheSuite>>>,
    lsn_provider: SharedLsnProvider,
    commit_reservations: CommitLsnReservations,
}

impl TabletManager {
    /// Create manager with a mounted system tablet.
    ///
    /// This also creates `data/tablets/system` and records compatibility mode
    /// when older flat layout (`data/sst`, `data/ilog`) is detected.
    pub fn new(
        data_dir: impl Into<PathBuf>,
        lsn_provider: SharedLsnProvider,
        system_tablet: Arc<TabletEngine>,
    ) -> Result<Self> {
        let data_dir = data_dir.into();
        let manager = Self {
            data_dir,
            tablets: RwLock::new(BTreeMap::from([(TabletId::System, system_tablet)])),
            desired_tablets: RwLock::new(BTreeSet::from([TabletId::System])),
            background_runtime: RwLock::new(None),
            tablet_workers: RwLock::new(BTreeMap::new()),
            cache_suite: RwLock::new(None),
            lsn_provider,
            commit_reservations: CommitLsnReservations::new(),
        };
        manager.prepare_tablet_layout()?;
        Ok(manager)
    }

    /// Root directory for tablet subdirectories.
    pub fn tablets_root(&self) -> PathBuf {
        self.data_dir.join("tablets")
    }

    /// Canonical directory for one tablet.
    pub fn tablet_dir(&self, tablet_id: TabletId) -> PathBuf {
        self.tablets_root().join(tablet_id.dir_name())
    }

    /// Ensure a tablet directory exists.
    pub fn ensure_tablet_dir(&self, tablet_id: TabletId) -> Result<PathBuf> {
        let dir = self.tablet_dir(tablet_id);
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
        }
        Ok(dir)
    }

    /// Mounted system tablet (always present in phase-2).
    pub fn system_tablet(&self) -> Arc<TabletEngine> {
        self.tablets
            .read()
            .get(&TabletId::System)
            .cloned()
            .expect("system tablet must exist")
    }

    /// Lookup mounted tablet by id.
    pub fn get_tablet(&self, tablet_id: TabletId) -> Option<Arc<TabletEngine>> {
        self.tablets.read().get(&tablet_id).cloned()
    }

    /// Snapshot of mounted tablets.
    pub fn all_tablets(&self) -> Vec<(TabletId, Arc<TabletEngine>)> {
        self.tablets
            .read()
            .iter()
            .map(|(id, tablet)| (*id, Arc::clone(tablet)))
            .collect()
    }

    /// Shared LSN provider used by all tablets in this manager.
    pub fn shared_lsn_provider(&self) -> SharedLsnProvider {
        Arc::clone(&self.lsn_provider)
    }

    /// Bind background runtime and start per-tablet workers for currently mounted tablets.
    pub fn bind_background_runtime(&self, handle: &tokio::runtime::Handle) -> Result<()> {
        *self.background_runtime.write() = Some(handle.clone());
        let tablets = self.all_tablets();
        for (tablet_id, tablet) in tablets {
            self.start_tablet_workers(tablet_id, tablet)?;
        }
        Ok(())
    }

    /// Stop all per-tablet flush/compaction workers.
    pub fn stop_background_workers(&self) {
        let mut workers = self.tablet_workers.write();
        let drained = std::mem::take(&mut *workers);
        for (_, worker_set) in drained {
            worker_set.flush.stop();
            worker_set.compaction.stop();
        }
    }

    /// Shutdown all manifest writers for mounted tablets.
    pub fn shutdown_manifest_writers(&self) {
        for (_, tablet) in self.all_tablets() {
            tablet.shutdown_manifest_writer();
        }
    }

    /// Close non-system tablet ilogs, flushing pending durable records.
    pub fn close_non_system_tablet_ilogs(&self) -> Result<()> {
        for (tablet_id, tablet) in self.all_tablets() {
            if tablet_id == TabletId::System {
                continue;
            }
            tablet.close_ilog()?;
        }
        Ok(())
    }

    /// Shutdown non-system tablet ilog writer channels before runtime drop.
    pub fn shutdown_non_system_tablet_ilogs(&self) {
        for (tablet_id, tablet) in self.all_tablets() {
            if tablet_id == TabletId::System {
                continue;
            }
            tablet.shutdown_ilog();
        }
    }

    /// Flush all mounted tablets (including active memtables).
    pub fn flush_all_with_active(&self) -> Result<()> {
        for (_, tablet) in self.all_tablets() {
            tablet.reset_aborted_flush_states();
            tablet.flush_all_with_active()?;
        }
        Ok(())
    }

    /// Whether mounted tablets are exactly `{system}`.
    pub fn has_only_system_tablet_mounted(&self) -> bool {
        let tablets = self.tablets.read();
        tablets.len() == 1 && tablets.contains_key(&TabletId::System)
    }

    /// Whether a mounted tablet currently has background workers running.
    pub fn has_running_workers(&self, tablet_id: TabletId) -> bool {
        self.tablet_workers
            .read()
            .get(&tablet_id)
            .map(|workers| workers.flush.is_running() && workers.compaction.is_running())
            .unwrap_or(false)
    }

    /// Number of tablets with worker sets.
    pub fn worker_tablet_count(&self) -> usize {
        self.tablet_workers.read().len()
    }

    /// Register desired tablets and materialize canonical directories.
    ///
    /// In phase-2 this does not mount new engines yet; only system tablet is
    /// mounted while directory inventory is prepared for later phases.
    pub fn register_desired_tablets<I>(&self, tablet_ids: I) -> Result<()>
    where
        I: IntoIterator<Item = TabletId>,
    {
        let ids: Vec<TabletId> = tablet_ids.into_iter().collect();
        for id in &ids {
            self.ensure_tablet_dir(*id)?;
        }

        let mut desired = self.desired_tablets.write();
        for id in ids {
            desired.insert(id);
        }
        Ok(())
    }

    /// Derive desired inventory from catalog and ensure corresponding dirs.
    pub fn register_catalog_inventory(&self, cache: &CatalogCache) -> Result<Vec<TabletId>> {
        let mut inventory = derive_tablet_inventory(cache);
        inventory.extend(self.discover_existing_tablet_dirs_for_recovery()?);
        self.register_desired_tablets(inventory.iter().copied())?;
        Ok(inventory.into_iter().collect())
    }

    /// Whether a tablet directory already contains durable state worth
    /// recovering/mounting (ilog file or SST files).
    pub fn has_persisted_tablet_state(&self, tablet_id: TabletId) -> Result<bool> {
        let dir = self.tablet_dir(tablet_id);
        let ilog_path = dir.join("ilog").join("tisql.ilog");
        if ilog_path.exists() {
            return Ok(true);
        }

        let sst_dir = dir.join("sst");
        if !sst_dir.exists() {
            return Ok(false);
        }

        for entry in std::fs::read_dir(&sst_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            if entry.path().extension().is_some_and(|ext| ext == "sst") {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Snapshot of desired tablet IDs (mounted + not-yet-mounted).
    pub fn desired_tablets(&self) -> Vec<TabletId> {
        self.desired_tablets.read().iter().copied().collect()
    }

    /// Insert one mounted tablet engine (future phases).
    pub fn insert_tablet(&self, tablet_id: TabletId, tablet: Arc<TabletEngine>) -> Result<()> {
        self.ensure_tablet_dir(tablet_id)?;
        if let Some(cache_suite) = self.cache_suite.read().as_ref() {
            tablet.set_cache_suite(Arc::clone(cache_suite));
        }
        self.desired_tablets.write().insert(tablet_id);
        self.tablets.write().insert(tablet_id, Arc::clone(&tablet));
        self.start_tablet_workers(tablet_id, tablet)?;
        Ok(())
    }

    /// Bind one shared cache suite and apply to all currently mounted tablets.
    pub fn bind_cache_suite(&self, suite: Arc<CacheSuite>) {
        *self.cache_suite.write() = Some(Arc::clone(&suite));
        for (_, tablet) in self.all_tablets() {
            tablet.set_cache_suite(Arc::clone(&suite));
        }
    }

    /// Remove a mounted tablet and stop its per-tablet workers.
    ///
    /// To avoid dropping unflushed data, this flushes pending memtables before
    /// unmounting the tablet.
    ///
    /// System tablet is never removed.
    pub fn remove_tablet(&self, tablet_id: TabletId) -> Result<Option<Arc<TabletEngine>>> {
        if tablet_id == TabletId::System {
            return Ok(None);
        }

        let tablet = match self.get_tablet(tablet_id) {
            Some(tablet) => tablet,
            None => {
                self.desired_tablets.write().remove(&tablet_id);
                return Ok(None);
            }
        };

        self.stop_tablet_workers(tablet_id);
        tablet.reset_aborted_flush_states();
        tablet.flush_all_with_active()?;
        tablet.shutdown_manifest_writer();
        tablet.close_ilog()?;
        tablet.shutdown_ilog();

        self.desired_tablets.write().remove(&tablet_id);
        let removed = self.tablets.write().remove(&tablet_id);
        Ok(removed)
    }

    /// Capture one tablet checkpoint via its manifest writer.
    pub async fn checkpoint_and_capture_tablet(
        &self,
        tablet_id: TabletId,
    ) -> Result<TabletCheckpointCapture> {
        let tablet = self.get_tablet(tablet_id).ok_or_else(|| {
            crate::util::error::TiSqlError::Storage(format!("tablet {tablet_id:?} not mounted"))
        })?;
        let (version, checkpoint_lsn) = tablet.checkpoint_and_capture_manifest().await?;
        Ok(TabletCheckpointCapture {
            tablet_id,
            version,
            checkpoint_lsn,
        })
    }

    /// Capture checkpoints for all mounted tablets.
    pub async fn checkpoint_and_capture_all_tablets(&self) -> Result<Vec<TabletCheckpointCapture>> {
        let tablets = self.all_tablets();
        let mut captures = Vec::with_capacity(tablets.len());
        // Phase-4 keeps this sequential for deterministic ordering/simpler
        // failure handling. Phase-5 may parallelize to reduce tail latency.
        for (tablet_id, tablet) in tablets {
            let (version, checkpoint_lsn) = tablet.checkpoint_and_capture_manifest().await?;
            captures.push(TabletCheckpointCapture {
                tablet_id,
                version,
                checkpoint_lsn,
            });
        }
        captures.sort_by_key(|capture| capture.tablet_id);
        Ok(captures)
    }

    /// Compute shared-clog GC boundary using global-min caps across tablets.
    pub fn compute_global_log_gc_boundary_with_caps(
        &self,
        captures: &[TabletCheckpointCapture],
    ) -> Result<GlobalLogGcBoundary> {
        if captures.is_empty() {
            return Err(TiSqlError::Storage(
                "cannot compute global log-GC boundary without tablet checkpoints".into(),
            ));
        }

        let checkpoint_flushed_cap = captures
            .iter()
            .map(|capture| capture.version.flushed_lsn())
            .min()
            .expect("captures is non-empty");

        let mut unflushed_cap: Option<Lsn> = None;
        for capture in captures {
            let tablet = self.get_tablet(capture.tablet_id).ok_or_else(|| {
                TiSqlError::Storage(format!(
                    "tablet {:?} missing while computing global log-GC boundary",
                    capture.tablet_id
                ))
            })?;
            if let Some(min_unflushed) = tablet.min_unflushed_lsn() {
                let cap = min_unflushed.saturating_sub(1);
                unflushed_cap = Some(unflushed_cap.map_or(cap, |cur| cur.min(cap)));
            }
        }

        let reservation_cap = self.min_reserved_lsn().map(|lsn| lsn.saturating_sub(1));
        let inflight_cap = self.min_in_flight_lsn().map(|lsn| lsn.saturating_sub(1));

        let mut safe_lsn = checkpoint_flushed_cap;
        if let Some(cap) = unflushed_cap {
            safe_lsn = safe_lsn.min(cap);
        }
        if let Some(cap) = reservation_cap {
            safe_lsn = safe_lsn.min(cap);
        }
        if let Some(cap) = inflight_cap {
            safe_lsn = safe_lsn.min(cap);
        }

        Ok(GlobalLogGcBoundary {
            checkpoint_flushed_cap,
            unflushed_cap,
            reservation_cap,
            inflight_cap,
            safe_lsn,
        })
    }

    /// Truncate each mounted tablet's ilog before that tablet's checkpoint_lsn.
    pub async fn truncate_tablet_ilogs_before(
        &self,
        captures: &[TabletCheckpointCapture],
    ) -> Result<Vec<TabletIlogTruncateCapture>> {
        let mut truncates = Vec::with_capacity(captures.len());
        for capture in captures {
            let tablet = self.get_tablet(capture.tablet_id).ok_or_else(|| {
                TiSqlError::Storage(format!(
                    "tablet {:?} missing while truncating tablet ilog",
                    capture.tablet_id
                ))
            })?;
            let stats = tablet.truncate_ilog_before(capture.checkpoint_lsn).await?;
            truncates.push(TabletIlogTruncateCapture {
                tablet_id: capture.tablet_id,
                checkpoint_lsn: capture.checkpoint_lsn,
                stats,
            });
        }
        truncates.sort_by_key(|entry| entry.tablet_id);
        Ok(truncates)
    }

    /// Global min flushed_lsn across mounted tablets.
    pub fn min_flushed_lsn(&self) -> Option<Lsn> {
        self.tablets
            .read()
            .values()
            .map(|tablet| tablet.current_version().flushed_lsn())
            .min()
    }

    /// Global min in-flight LSN across mounted tablets.
    ///
    /// Conservative fallback: returns `None` when no tablet currently reports
    /// in-flight writes.
    pub fn min_in_flight_lsn(&self) -> Option<Lsn> {
        self.tablets
            .read()
            .values()
            .filter_map(|tablet| tablet.min_in_flight_lsn())
            .min()
    }

    /// Global min reserved LSN from manager-level shared reservation tracker.
    ///
    /// Conservative fallback: returns `None` when there is no active
    /// reservation.
    pub fn min_reserved_lsn(&self) -> Option<Lsn> {
        self.commit_reservations.min_reserved_lsn()
    }

    /// Allocate and reserve a commit LSN at manager scope (shared clog).
    pub fn alloc_and_reserve_commit_lsn(&self, txn_start_ts: Timestamp) -> Lsn {
        self.commit_reservations
            .alloc_and_reserve(txn_start_ts, self.lsn_provider.as_ref())
    }

    /// Release manager-level commit reservation.
    pub fn release_commit_lsn(&self, txn_start_ts: Timestamp) -> Option<Lsn> {
        self.commit_reservations.release(txn_start_ts)
    }

    /// Check whether `(txn_start_ts, lsn)` is currently reserved.
    pub fn is_commit_lsn_reserved(&self, txn_start_ts: Timestamp, lsn: Lsn) -> bool {
        self.commit_reservations.is_reserved(txn_start_ts, lsn)
    }

    /// Reservation metrics snapshot.
    pub fn commit_reservation_stats(&self) -> CommitReservationStats {
        self.commit_reservations.stats()
    }

    fn start_tablet_workers(&self, tablet_id: TabletId, tablet: Arc<TabletEngine>) -> Result<()> {
        let handle = match self.background_runtime.read().as_ref() {
            Some(handle) => handle.clone(),
            None => return Ok(()),
        };

        let mut workers = self.tablet_workers.write();
        if workers.contains_key(&tablet_id) {
            return Ok(());
        }

        tablet.start_manifest_writer(&handle)?;

        let flush = FlushScheduler::new(Arc::clone(&tablet));
        flush.start(&handle);

        let compaction = CompactionScheduler::new(Arc::clone(&tablet));
        compaction.start(&handle);
        tablet.set_compaction_notify(compaction.notifier());

        workers.insert(tablet_id, TabletWorkerSet { flush, compaction });
        Ok(())
    }

    fn stop_tablet_workers(&self, tablet_id: TabletId) {
        if let Some(workers) = self.tablet_workers.write().remove(&tablet_id) {
            workers.flush.stop();
            workers.compaction.stop();
        }
    }

    fn prepare_tablet_layout(&self) -> Result<()> {
        let tablets_root = self.tablets_root();
        if !tablets_root.exists() && legacy_flat_layout_detected(&self.data_dir) {
            log_info!(
                "Detected legacy single-layout storage under {:?}; mapping to system tablet layout at {:?}",
                self.data_dir,
                tablets_root
            );
        }

        std::fs::create_dir_all(&tablets_root)?;
        self.ensure_tablet_dir(TabletId::System)?;
        Ok(())
    }

    /// Discover tablet directories currently present on disk.
    pub fn discover_existing_tablet_dirs_for_recovery(&self) -> Result<BTreeSet<TabletId>> {
        let mut discovered = BTreeSet::new();
        for tablet_id in self.discover_existing_tablet_dirs()? {
            if self.has_persisted_tablet_state(tablet_id)? {
                discovered.insert(tablet_id);
            } else if tablet_id != TabletId::System {
                // Fresh system dir without ilog/SST state is expected before first durability write.
                log_warn!(
                    "Ignoring empty/orphan tablet dir {:?}: no ilog or SST state",
                    self.tablet_dir(tablet_id)
                );
            }
        }
        Ok(discovered)
    }

    fn discover_existing_tablet_dirs(&self) -> Result<BTreeSet<TabletId>> {
        let mut discovered = BTreeSet::new();
        let root = self.tablets_root();
        if !root.exists() {
            return Ok(discovered);
        }

        for entry in std::fs::read_dir(&root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }

            let name = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => {
                    log_warn!("Ignoring non-utf8 tablet dir entry under {:?}", root);
                    continue;
                }
            };

            match TabletId::from_dir_name(&name) {
                Ok(id) if is_supported_discovered_tablet(id) => {
                    discovered.insert(id);
                }
                Ok(id) => {
                    log_warn!(
                        "Ignoring tablet dir {:?} because it maps to system space in v1 ({id})",
                        entry.path()
                    );
                }
                Err(e) => {
                    log_warn!("Ignoring unrecognized tablet dir {:?}: {}", entry.path(), e);
                }
            }
        }

        Ok(discovered)
    }
}

fn legacy_flat_layout_detected(data_dir: &Path) -> bool {
    dir_has_entries(&data_dir.join("sst"))
}

fn dir_has_entries(path: &Path) -> bool {
    match std::fs::read_dir(path) {
        Ok(mut entries) => entries.next().is_some(),
        Err(_) => false,
    }
}

fn is_supported_discovered_tablet(id: TabletId) -> bool {
    match id {
        TabletId::System => true,
        TabletId::Table { table_id } => {
            matches!(route_table_to_tablet(table_id), TabletId::Table { .. })
        }
        TabletId::LocalIndex { table_id, index_id } => matches!(
            route_index_to_tablet(table_id, index_id),
            TabletId::LocalIndex { .. }
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{IndexDef, TableDef};
    use crate::inner_table::core_tables::{ALL_TABLE_TABLE_ID, USER_TABLE_ID_START};
    use crate::lsn::new_lsn_provider;
    use crate::tablet::memtable::FlushState;
    use crate::tablet::{IlogConfig, IlogService, LsmConfig, StorageEngine, Version, WriteBatch};
    use std::collections::HashMap;
    use tempfile::TempDir;
    use tokio::time::{timeout, Duration};

    fn open_test_system_tablet(dir: &Path) -> Arc<TabletEngine> {
        Arc::new(TabletEngine::open(LsmConfig::new(dir)).unwrap())
    }

    fn open_durable_tablet(
        dir: &Path,
        lsn_provider: crate::lsn::SharedLsnProvider,
    ) -> Arc<TabletEngine> {
        let io = crate::io::IoService::new_for_test(32).unwrap();
        let ilog = Arc::new(
            IlogService::open_with_thread(IlogConfig::new(dir), Arc::clone(&lsn_provider)).unwrap(),
        );
        Arc::new(
            TabletEngine::open_with_recovery(
                LsmConfig::new(dir),
                lsn_provider,
                ilog,
                Version::new(),
                io,
            )
            .unwrap(),
        )
    }

    fn count_sst_files(dir: &Path) -> usize {
        std::fs::read_dir(dir.join("sst"))
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "sst"))
            .count()
    }

    fn empty_cache() -> CatalogCache {
        CatalogCache {
            schemas: HashMap::new(),
            schema_names: HashMap::new(),
            tables: HashMap::new(),
            table_id_map: HashMap::new(),
        }
    }

    #[test]
    fn test_manager_system_tablet_always_exists() {
        let dir = TempDir::new().unwrap();
        let manager = TabletManager::new(
            dir.path(),
            new_lsn_provider(),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();
        assert!(manager.get_tablet(TabletId::System).is_some());
        assert_eq!(manager.system_tablet().current_version().version_num(), 0);
    }

    #[test]
    fn test_manager_get_nonexistent_tablet() {
        let dir = TempDir::new().unwrap();
        let manager = TabletManager::new(
            dir.path(),
            new_lsn_provider(),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();
        assert!(manager
            .get_tablet(TabletId::Table { table_id: 9_999 })
            .is_none());
    }

    #[test]
    fn test_manager_all_tablets() {
        let dir = TempDir::new().unwrap();
        let manager = TabletManager::new(
            dir.path(),
            new_lsn_provider(),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();
        let tablets = manager.all_tablets();
        assert_eq!(tablets.len(), 1);
        assert_eq!(tablets[0].0, TabletId::System);
    }

    #[test]
    fn test_manager_commit_reservations_global() {
        let dir = TempDir::new().unwrap();
        let manager = TabletManager::new(
            dir.path(),
            new_lsn_provider(),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();
        let lsn = manager.alloc_and_reserve_commit_lsn(101);
        assert!(manager.is_commit_lsn_reserved(101, lsn));
        assert_eq!(manager.min_reserved_lsn(), Some(lsn));
        assert_eq!(manager.release_commit_lsn(101), Some(lsn));
        assert!(!manager.is_commit_lsn_reserved(101, lsn));
        assert_eq!(manager.min_reserved_lsn(), None);
    }

    #[test]
    fn test_manager_min_flushed_lsn_single_tablet() {
        let dir = TempDir::new().unwrap();
        let system = open_test_system_tablet(dir.path());
        let expected = system.current_version().flushed_lsn();
        let manager = TabletManager::new(dir.path(), new_lsn_provider(), system).unwrap();
        assert_eq!(manager.min_flushed_lsn(), Some(expected));
    }

    #[test]
    fn test_manager_global_min_in_flight() {
        let dir = TempDir::new().unwrap();
        let system = open_test_system_tablet(dir.path());
        let expected = system.min_in_flight_lsn();
        let manager = TabletManager::new(dir.path(), new_lsn_provider(), system).unwrap();
        assert_eq!(manager.min_in_flight_lsn(), expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase5_gc_boundary_min_across_two_tablets() {
        let dir = TempDir::new().unwrap();
        let lsn_provider = new_lsn_provider();

        let system_dir = dir.path().join("tablets").join("system");
        std::fs::create_dir_all(&system_dir).unwrap();
        let system = open_durable_tablet(&system_dir, Arc::clone(&lsn_provider));
        let manager = TabletManager::new(dir.path(), Arc::clone(&lsn_provider), system).unwrap();

        let tablet_a = TabletId::Table {
            table_id: USER_TABLE_ID_START + 500,
        };
        let tablet_b = TabletId::Table {
            table_id: USER_TABLE_ID_START + 501,
        };
        let engine_a =
            open_durable_tablet(&manager.tablet_dir(tablet_a), Arc::clone(&lsn_provider));
        let engine_b =
            open_durable_tablet(&manager.tablet_dir(tablet_b), Arc::clone(&lsn_provider));
        manager
            .insert_tablet(tablet_a, Arc::clone(&engine_a))
            .unwrap();
        manager
            .insert_tablet(tablet_b, Arc::clone(&engine_b))
            .unwrap();

        let mut wb_a = WriteBatch::new();
        wb_a.set_commit_ts(11);
        wb_a.set_clog_lsn(11);
        wb_a.put(b"a".to_vec(), b"va".to_vec());
        engine_a.write_batch(wb_a).unwrap();
        engine_a.flush_all_with_active().unwrap();

        let mut wb_b = WriteBatch::new();
        wb_b.set_commit_ts(21);
        wb_b.set_clog_lsn(21);
        wb_b.put(b"b".to_vec(), b"vb".to_vec());
        engine_b.write_batch(wb_b).unwrap();

        let captures = manager.checkpoint_and_capture_all_tablets().await.unwrap();
        let boundary = manager
            .compute_global_log_gc_boundary_with_caps(&captures)
            .unwrap();

        let slow_cap = engine_b.min_unflushed_lsn().unwrap().saturating_sub(1);
        assert!(
            boundary.safe_lsn <= slow_cap,
            "safe_lsn={} must be <= slow tablet cap={slow_cap}",
            boundary.safe_lsn
        );
        assert!(
            boundary.safe_lsn <= boundary.checkpoint_flushed_cap,
            "safe_lsn={} must be <= checkpoint cap={}",
            boundary.safe_lsn,
            boundary.checkpoint_flushed_cap
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase5_gc_boundary_reservation_cap_global() {
        let dir = TempDir::new().unwrap();
        let lsn_provider = new_lsn_provider();

        let system_dir = dir.path().join("tablets").join("system");
        std::fs::create_dir_all(&system_dir).unwrap();
        let system = open_durable_tablet(&system_dir, Arc::clone(&lsn_provider));
        let manager = TabletManager::new(dir.path(), Arc::clone(&lsn_provider), system).unwrap();

        let lsn = manager.alloc_and_reserve_commit_lsn(123);
        let captures = manager.checkpoint_and_capture_all_tablets().await.unwrap();
        let boundary = manager
            .compute_global_log_gc_boundary_with_caps(&captures)
            .unwrap();

        assert_eq!(boundary.reservation_cap, Some(lsn.saturating_sub(1)));
        assert!(
            boundary.safe_lsn <= lsn.saturating_sub(1),
            "safe_lsn={} must respect reservation cap {}",
            boundary.safe_lsn,
            lsn.saturating_sub(1)
        );
        assert_eq!(manager.release_commit_lsn(123), Some(lsn));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase5_gc_boundary_all_caps_combined() {
        let dir = TempDir::new().unwrap();
        let lsn_provider = new_lsn_provider();

        let system_dir = dir.path().join("tablets").join("system");
        std::fs::create_dir_all(&system_dir).unwrap();
        let system = open_durable_tablet(&system_dir, Arc::clone(&lsn_provider));
        let manager = TabletManager::new(dir.path(), Arc::clone(&lsn_provider), system).unwrap();

        let tablet = TabletId::Table {
            table_id: USER_TABLE_ID_START + 510,
        };
        let engine = open_durable_tablet(&manager.tablet_dir(tablet), Arc::clone(&lsn_provider));
        manager.insert_tablet(tablet, Arc::clone(&engine)).unwrap();

        let mut wb = WriteBatch::new();
        wb.set_commit_ts(41);
        wb.set_clog_lsn(41);
        wb.put(b"k".to_vec(), b"v".to_vec());
        engine.write_batch(wb).unwrap();

        let reserved = manager.alloc_and_reserve_commit_lsn(404);
        let captures = manager.checkpoint_and_capture_all_tablets().await.unwrap();
        let boundary = manager
            .compute_global_log_gc_boundary_with_caps(&captures)
            .unwrap();
        let unflushed_cap = engine.min_unflushed_lsn().unwrap().saturating_sub(1);
        let reservation_cap = reserved.saturating_sub(1);
        let expected_upper = unflushed_cap.min(reservation_cap);
        assert!(
            boundary.safe_lsn <= expected_upper,
            "safe_lsn={} must be <= min(unflushed_cap={unflushed_cap}, reservation_cap={reservation_cap})",
            boundary.safe_lsn
        );
        assert_eq!(manager.release_commit_lsn(404), Some(reserved));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase5_truncate_tablet_ilogs_before_all_tablets() {
        let dir = TempDir::new().unwrap();
        let lsn_provider = new_lsn_provider();

        let system_dir = dir.path().join("tablets").join("system");
        std::fs::create_dir_all(&system_dir).unwrap();
        let system = open_durable_tablet(&system_dir, Arc::clone(&lsn_provider));
        let manager = TabletManager::new(dir.path(), Arc::clone(&lsn_provider), system).unwrap();

        let tablet_a = TabletId::Table {
            table_id: USER_TABLE_ID_START + 520,
        };
        let tablet_b = TabletId::Table {
            table_id: USER_TABLE_ID_START + 521,
        };
        manager
            .insert_tablet(
                tablet_a,
                open_durable_tablet(&manager.tablet_dir(tablet_a), Arc::clone(&lsn_provider)),
            )
            .unwrap();
        manager
            .insert_tablet(
                tablet_b,
                open_durable_tablet(&manager.tablet_dir(tablet_b), Arc::clone(&lsn_provider)),
            )
            .unwrap();

        for (tablet_id, ts, lsn) in [
            (TabletId::System, 31, 31),
            (tablet_a, 32, 32),
            (tablet_b, 33, 33),
        ] {
            let tablet = manager.get_tablet(tablet_id).unwrap();
            let mut wb = WriteBatch::new();
            wb.set_commit_ts(ts);
            wb.set_clog_lsn(lsn);
            wb.put(
                format!("k_{ts}").into_bytes(),
                format!("v_{ts}").into_bytes(),
            );
            tablet.write_batch(wb).unwrap();
        }

        let captures = manager.checkpoint_and_capture_all_tablets().await.unwrap();
        let truncates = manager
            .truncate_tablet_ilogs_before(&captures)
            .await
            .unwrap();

        assert_eq!(
            truncates.len(),
            captures.len(),
            "every captured tablet should truncate its ilog once"
        );
        assert!(
            truncates.iter().any(|t| t.tablet_id == TabletId::System),
            "system tablet truncate stats must be present"
        );
        assert!(
            truncates.iter().any(|t| t.tablet_id == tablet_a),
            "tablet A truncate stats must be present"
        );
        assert!(
            truncates.iter().any(|t| t.tablet_id == tablet_b),
            "tablet B truncate stats must be present"
        );
    }

    #[test]
    fn test_register_catalog_inventory_excludes_inner_system_tables() {
        let dir = TempDir::new().unwrap();
        let manager = TabletManager::new(
            dir.path(),
            new_lsn_provider(),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();

        let mut cache = empty_cache();
        let mut user_table = TableDef::new(
            USER_TABLE_ID_START,
            "t_user".to_string(),
            "default".to_string(),
            vec![],
            vec![],
        );
        user_table.add_index(IndexDef::new(2001, "idx_c1".to_string(), vec![1], false));
        cache.tables.insert(
            ("default".to_string(), "t_user".to_string()),
            user_table.clone(),
        );
        cache.table_id_map.insert(
            user_table.id(),
            ("default".to_string(), "t_user".to_string()),
        );

        // Inner/system table should stay pinned to system tablet.
        let inner_table = TableDef::new(
            ALL_TABLE_TABLE_ID,
            "__all_table".to_string(),
            "__tisql_inner".to_string(),
            vec![],
            vec![],
        );
        cache.tables.insert(
            ("__tisql_inner".to_string(), "__all_table".to_string()),
            inner_table.clone(),
        );
        cache.table_id_map.insert(
            inner_table.id(),
            ("__tisql_inner".to_string(), "__all_table".to_string()),
        );

        let inventory = manager.register_catalog_inventory(&cache).unwrap();
        let inventory_set: BTreeSet<_> = inventory.into_iter().collect();
        assert!(inventory_set.contains(&TabletId::System));
        assert!(inventory_set.contains(&TabletId::Table {
            table_id: USER_TABLE_ID_START
        }));
        assert!(inventory_set.contains(&TabletId::LocalIndex {
            table_id: USER_TABLE_ID_START,
            index_id: 2001
        }));
        assert!(!inventory_set.contains(&TabletId::Table {
            table_id: ALL_TABLE_TABLE_ID
        }));

        // In phase-2 only the system tablet is mounted; user tablet dirs are pre-created.
        assert!(manager.get_tablet(TabletId::System).is_some());
        assert!(manager
            .get_tablet(TabletId::Table {
                table_id: USER_TABLE_ID_START
            })
            .is_none());
        assert!(manager
            .tablet_dir(TabletId::Table {
                table_id: USER_TABLE_ID_START
            })
            .exists());
        assert!(manager
            .tablet_dir(TabletId::LocalIndex {
                table_id: USER_TABLE_ID_START,
                index_id: 2001
            })
            .exists());
    }

    #[test]
    fn test_register_catalog_inventory_discovers_existing_tablet_dirs() {
        let dir = TempDir::new().unwrap();
        let manager = TabletManager::new(
            dir.path(),
            new_lsn_provider(),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();

        let user_tablet = TabletId::Table {
            table_id: USER_TABLE_ID_START + 7,
        };
        let user_index_tablet = TabletId::LocalIndex {
            table_id: USER_TABLE_ID_START + 7,
            index_id: 333,
        };
        manager.ensure_tablet_dir(user_tablet).unwrap();
        manager.ensure_tablet_dir(user_index_tablet).unwrap();
        std::fs::create_dir_all(manager.tablet_dir(user_tablet).join("ilog")).unwrap();
        std::fs::create_dir_all(manager.tablet_dir(user_index_tablet).join("ilog")).unwrap();
        std::fs::write(
            manager
                .tablet_dir(user_tablet)
                .join("ilog")
                .join("tisql.ilog"),
            b"stub",
        )
        .unwrap();
        std::fs::write(
            manager
                .tablet_dir(user_index_tablet)
                .join("ilog")
                .join("tisql.ilog"),
            b"stub",
        )
        .unwrap();

        let inventory = manager.register_catalog_inventory(&empty_cache()).unwrap();
        let inventory: BTreeSet<_> = inventory.into_iter().collect();
        assert!(inventory.contains(&TabletId::System));
        assert!(inventory.contains(&user_tablet));
        assert!(inventory.contains(&user_index_tablet));

        let desired = manager.desired_tablets();
        assert!(desired.contains(&user_tablet));
        assert!(desired.contains(&user_index_tablet));
    }

    #[test]
    fn test_register_catalog_inventory_ignores_invalid_or_system_space_dirs() {
        let dir = TempDir::new().unwrap();
        let manager = TabletManager::new(
            dir.path(),
            new_lsn_provider(),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();

        let tablets_root = manager.tablets_root();
        std::fs::create_dir_all(tablets_root.join("garbage")).unwrap();
        std::fs::create_dir_all(tablets_root.join(format!("t_{ALL_TABLE_TABLE_ID}"))).unwrap();
        std::fs::create_dir_all(tablets_root.join(format!("i_{ALL_TABLE_TABLE_ID}_9"))).unwrap();
        // Empty user tablet dir should be ignored unless it has persisted state.
        std::fs::create_dir_all(tablets_root.join(format!("t_{}", USER_TABLE_ID_START + 55)))
            .unwrap();

        let inventory = manager.register_catalog_inventory(&empty_cache()).unwrap();
        let inventory: BTreeSet<_> = inventory.into_iter().collect();
        assert_eq!(inventory, BTreeSet::from([TabletId::System]));

        let desired = manager.desired_tablets();
        assert_eq!(desired, vec![TabletId::System]);
    }

    #[test]
    fn test_legacy_flat_layout_maps_to_system_tablet_dir() {
        let dir = TempDir::new().unwrap();
        let legacy_sst = dir.path().join("sst");
        std::fs::create_dir_all(&legacy_sst).unwrap();
        std::fs::write(legacy_sst.join("1.sst"), b"stub").unwrap();

        let manager = TabletManager::new(
            dir.path(),
            new_lsn_provider(),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();

        let system_dir = manager.tablet_dir(TabletId::System);
        assert!(system_dir.exists());
        assert!(legacy_sst.exists());
    }

    // ==================== Phase-2 QA Tests ====================

    /// T2.2: insert_tablet adds a mounted tablet and registers it in desired_tablets.
    #[test]
    fn test_insert_tablet_mounts_and_registers() {
        let dir = TempDir::new().unwrap();
        let lsn = new_lsn_provider();
        let manager = TabletManager::new(
            dir.path(),
            Arc::clone(&lsn),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();

        let user_tablet_id = TabletId::Table {
            table_id: USER_TABLE_ID_START + 42,
        };
        let user_engine =
            Arc::new(TabletEngine::open(LsmConfig::new(dir.path().join("user42"))).unwrap());

        manager
            .insert_tablet(user_tablet_id, Arc::clone(&user_engine))
            .unwrap();

        // Mounted: get_tablet returns Some
        assert!(
            manager.get_tablet(user_tablet_id).is_some(),
            "inserted tablet should be retrievable via get_tablet"
        );
        // all_tablets includes both system and user
        let all = manager.all_tablets();
        assert_eq!(all.len(), 2);
        // desired_tablets includes the new tablet
        let desired = manager.desired_tablets();
        assert!(desired.contains(&user_tablet_id));
        // Directory was created
        assert!(manager.tablet_dir(user_tablet_id).exists());
    }

    /// T2.2: register_desired_tablets is additive and idempotent.
    #[test]
    fn test_register_desired_tablets_idempotent() {
        let dir = TempDir::new().unwrap();
        let manager = TabletManager::new(
            dir.path(),
            new_lsn_provider(),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();

        let tid = TabletId::Table {
            table_id: USER_TABLE_ID_START + 1,
        };

        // Register once
        manager.register_desired_tablets([tid]).unwrap();
        assert!(manager.desired_tablets().contains(&tid));
        assert!(manager.tablet_dir(tid).exists());

        // Register same tablet again — no error, no duplication
        manager.register_desired_tablets([tid]).unwrap();
        let desired = manager.desired_tablets();
        assert_eq!(
            desired.iter().filter(|&&t| t == tid).count(),
            1,
            "desired_tablets should not duplicate on re-registration"
        );
    }

    /// T2.2: ensure_tablet_dir is idempotent — calling twice succeeds.
    #[test]
    fn test_ensure_tablet_dir_idempotent() {
        let dir = TempDir::new().unwrap();
        let manager = TabletManager::new(
            dir.path(),
            new_lsn_provider(),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();

        let tid = TabletId::LocalIndex {
            table_id: USER_TABLE_ID_START,
            index_id: 999,
        };
        let path1 = manager.ensure_tablet_dir(tid).unwrap();
        let path2 = manager.ensure_tablet_dir(tid).unwrap();
        assert_eq!(path1, path2);
        assert!(path1.exists());
    }

    /// T2.2: tablet_dir produces canonical paths matching TabletId::dir_name.
    #[test]
    fn test_tablet_dir_path_correctness() {
        let dir = TempDir::new().unwrap();
        let manager = TabletManager::new(
            dir.path(),
            new_lsn_provider(),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();

        assert_eq!(
            manager.tablet_dir(TabletId::System),
            dir.path().join("tablets").join("system")
        );
        assert_eq!(
            manager.tablet_dir(TabletId::Table {
                table_id: USER_TABLE_ID_START
            }),
            dir.path()
                .join("tablets")
                .join(format!("t_{USER_TABLE_ID_START}"))
        );
        assert_eq!(
            manager.tablet_dir(TabletId::LocalIndex {
                table_id: USER_TABLE_ID_START,
                index_id: 77
            }),
            dir.path()
                .join("tablets")
                .join(format!("i_{USER_TABLE_ID_START}_77"))
        );
    }

    /// T2.5: commit_reservation_stats reflects alloc/release counters.
    #[test]
    fn test_commit_reservation_stats_counters() {
        let dir = TempDir::new().unwrap();
        let manager = TabletManager::new(
            dir.path(),
            new_lsn_provider(),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();

        let stats_before = manager.commit_reservation_stats();
        assert_eq!(stats_before.active, 0);
        assert_eq!(stats_before.total_allocated, 0);
        assert_eq!(stats_before.total_released, 0);

        let _lsn1 = manager.alloc_and_reserve_commit_lsn(200);
        let _lsn2 = manager.alloc_and_reserve_commit_lsn(201);

        let stats_mid = manager.commit_reservation_stats();
        assert_eq!(stats_mid.active, 2);
        assert_eq!(stats_mid.total_allocated, 2);

        manager.release_commit_lsn(200);

        let stats_after = manager.commit_reservation_stats();
        assert_eq!(stats_after.active, 1);
        assert_eq!(stats_after.total_released, 1);

        manager.release_commit_lsn(201);

        let stats_final = manager.commit_reservation_stats();
        assert_eq!(stats_final.active, 0);
        assert_eq!(stats_final.total_allocated, 2);
        assert_eq!(stats_final.total_released, 2);
    }

    /// T2.5: multiple reservations — min_reserved_lsn returns the smallest.
    #[test]
    fn test_multiple_reservations_min_ordering() {
        let dir = TempDir::new().unwrap();
        let manager = TabletManager::new(
            dir.path(),
            new_lsn_provider(),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();

        let lsn1 = manager.alloc_and_reserve_commit_lsn(300);
        let lsn2 = manager.alloc_and_reserve_commit_lsn(301);
        assert!(lsn1 < lsn2, "LSNs should be monotonically increasing");
        assert_eq!(
            manager.min_reserved_lsn(),
            Some(lsn1),
            "min_reserved_lsn should return the earlier reservation"
        );

        // Release the smaller → min advances to second
        manager.release_commit_lsn(300);
        assert_eq!(
            manager.min_reserved_lsn(),
            Some(lsn2),
            "after releasing first, min should advance to second"
        );

        manager.release_commit_lsn(301);
        assert_eq!(manager.min_reserved_lsn(), None);
    }

    /// T2.2: tablets_root returns the canonical tablets/ subdirectory.
    #[test]
    fn test_tablets_root_created_on_init() {
        let dir = TempDir::new().unwrap();
        let _manager = TabletManager::new(
            dir.path(),
            new_lsn_provider(),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();

        assert!(
            dir.path().join("tablets").exists(),
            "tablets root should be created by TabletManager::new"
        );
        assert!(
            dir.path().join("tablets").join("system").exists(),
            "system tablet dir should be created by TabletManager::new"
        );
    }

    /// T2.2: derive_tablet_inventory with multiple user tables and indexes
    /// produces distinct tablet IDs per table and per index.
    #[test]
    fn test_derive_inventory_multiple_user_tables() {
        let mut cache = empty_cache();

        // Two user tables with indexes
        for offset in [0u64, 1] {
            let tid = USER_TABLE_ID_START + offset;
            let name = format!("t_{offset}");
            let mut table = TableDef::new(tid, name.clone(), "default".to_string(), vec![], vec![]);
            table.add_index(IndexDef::new(
                5000 + offset,
                format!("idx_{offset}"),
                vec![1],
                false,
            ));
            cache
                .tables
                .insert(("default".to_string(), name.clone()), table);
            cache
                .table_id_map
                .insert(tid, ("default".to_string(), name));
        }

        let inventory = derive_tablet_inventory(&cache);

        // System + 2 table tablets + 2 index tablets = 5
        assert_eq!(inventory.len(), 5);
        assert!(inventory.contains(&TabletId::System));
        assert!(inventory.contains(&TabletId::Table {
            table_id: USER_TABLE_ID_START
        }));
        assert!(inventory.contains(&TabletId::Table {
            table_id: USER_TABLE_ID_START + 1
        }));
        assert!(inventory.contains(&TabletId::LocalIndex {
            table_id: USER_TABLE_ID_START,
            index_id: 5000
        }));
        assert!(inventory.contains(&TabletId::LocalIndex {
            table_id: USER_TABLE_ID_START + 1,
            index_id: 5001
        }));
    }

    /// T2.2: discover_existing_tablet_dirs skips non-directory entries (files).
    #[test]
    fn test_discover_skips_non_directory_entries() {
        let dir = TempDir::new().unwrap();
        let manager = TabletManager::new(
            dir.path(),
            new_lsn_provider(),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();

        // Create a regular file (not a directory) in tablets/
        let tablets_root = manager.tablets_root();
        std::fs::write(tablets_root.join("t_1000"), b"not a dir").unwrap();

        // Also create a valid user tablet dir
        let valid_tid = TabletId::Table {
            table_id: USER_TABLE_ID_START + 99,
        };
        std::fs::create_dir_all(tablets_root.join(valid_tid.dir_name())).unwrap();
        std::fs::create_dir_all(tablets_root.join(valid_tid.dir_name()).join("ilog")).unwrap();
        std::fs::write(
            tablets_root
                .join(valid_tid.dir_name())
                .join("ilog")
                .join("tisql.ilog"),
            b"stub",
        )
        .unwrap();

        let inventory = manager.register_catalog_inventory(&empty_cache()).unwrap();
        let inventory: BTreeSet<_> = inventory.into_iter().collect();

        // The file should be ignored; the valid dir should be discovered
        assert!(inventory.contains(&valid_tid));
        assert!(!inventory.contains(&TabletId::Table { table_id: 1000 }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase4_bind_runtime_starts_system_workers() {
        let dir = TempDir::new().unwrap();
        let manager = TabletManager::new(
            dir.path(),
            new_lsn_provider(),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();

        manager
            .bind_background_runtime(&tokio::runtime::Handle::current())
            .unwrap();
        assert!(manager.has_only_system_tablet_mounted());
        assert!(manager.has_running_workers(TabletId::System));
        assert_eq!(manager.worker_tablet_count(), 1);

        manager.stop_background_workers();
        assert!(!manager.has_running_workers(TabletId::System));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase4_insert_remove_tablet_workers_lifecycle() {
        let dir = TempDir::new().unwrap();
        let manager = TabletManager::new(
            dir.path(),
            new_lsn_provider(),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();
        manager
            .bind_background_runtime(&tokio::runtime::Handle::current())
            .unwrap();

        let user_tablet_id = TabletId::Table {
            table_id: USER_TABLE_ID_START + 55,
        };
        let user_engine = Arc::new(
            TabletEngine::open(LsmConfig::new(manager.tablet_dir(user_tablet_id))).unwrap(),
        );
        manager
            .insert_tablet(user_tablet_id, Arc::clone(&user_engine))
            .unwrap();
        assert!(!manager.has_only_system_tablet_mounted());

        let mut wb = WriteBatch::new();
        wb.set_commit_ts(7);
        wb.put(b"k".to_vec(), b"v".to_vec());
        user_engine.write_batch(wb).unwrap();
        user_engine.freeze_active();

        assert!(manager.has_running_workers(user_tablet_id));
        assert_eq!(manager.worker_tablet_count(), 2);

        let removed = manager.remove_tablet(user_tablet_id).unwrap();
        assert!(removed.is_some());
        let removed = removed.unwrap();
        assert!(
            removed.min_unflushed_lsn().is_none(),
            "remove_tablet must flush dirty memtables before unmount"
        );
        assert!(
            count_sst_files(&manager.tablet_dir(user_tablet_id)) > 0,
            "remove_tablet should persist frozen/active data before unmount"
        );
        assert!(!manager.has_running_workers(user_tablet_id));
        assert_eq!(manager.worker_tablet_count(), 1);
        assert!(manager.has_only_system_tablet_mounted());

        manager.stop_background_workers();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase4_remove_tablet_resets_in_progress_flush_and_persists_data() {
        let dir = TempDir::new().unwrap();
        let manager = TabletManager::new(
            dir.path(),
            new_lsn_provider(),
            open_test_system_tablet(dir.path()),
        )
        .unwrap();
        manager
            .bind_background_runtime(&tokio::runtime::Handle::current())
            .unwrap();

        let user_tablet_id = TabletId::Table {
            table_id: USER_TABLE_ID_START + 77,
        };
        let user_engine = Arc::new(
            TabletEngine::open(LsmConfig::new(manager.tablet_dir(user_tablet_id))).unwrap(),
        );
        manager
            .insert_tablet(user_tablet_id, Arc::clone(&user_engine))
            .unwrap();

        let mut wb = WriteBatch::new();
        wb.set_commit_ts(17);
        wb.put(b"remove_flush_key".to_vec(), b"remove_flush_val".to_vec());
        user_engine.write_batch(wb).unwrap();

        let frozen = user_engine
            .freeze_active()
            .expect("must create frozen memtable for flush-state simulation");
        frozen.set_flush_state(FlushState::Flushing);

        let removed = manager
            .remove_tablet(user_tablet_id)
            .unwrap()
            .expect("tablet should be removed");
        assert!(
            removed.min_unflushed_lsn().is_none(),
            "remove_tablet should recover interrupted flushing state and persist remaining data"
        );
        assert!(
            count_sst_files(&manager.tablet_dir(user_tablet_id)) > 0,
            "remove_tablet should flush frozen memtable after reset_aborted_flush_states"
        );
        assert!(!manager.has_running_workers(user_tablet_id));

        manager.stop_background_workers();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase4_checkpoint_capture_all_tablets_and_isolated_paths() {
        let dir = TempDir::new().unwrap();
        let lsn_provider = new_lsn_provider();

        let system_tablet_dir = dir.path().join("tablets").join("system");
        std::fs::create_dir_all(&system_tablet_dir).unwrap();
        let system_tablet = open_durable_tablet(&system_tablet_dir, Arc::clone(&lsn_provider));

        let manager =
            TabletManager::new(dir.path(), Arc::clone(&lsn_provider), system_tablet).unwrap();
        manager
            .bind_background_runtime(&tokio::runtime::Handle::current())
            .unwrap();

        let user_tablet_id = TabletId::Table {
            table_id: USER_TABLE_ID_START,
        };
        let user_tablet_dir = manager.tablet_dir(user_tablet_id);
        let user_tablet = open_durable_tablet(&user_tablet_dir, Arc::clone(&lsn_provider));
        manager.insert_tablet(user_tablet_id, user_tablet).unwrap();

        let mut wb1 = WriteBatch::new();
        wb1.set_commit_ts(10);
        wb1.put(b"sys_k".to_vec(), b"sys_v".to_vec());
        manager.system_tablet().write_batch(wb1).unwrap();

        let mut wb2 = WriteBatch::new();
        wb2.set_commit_ts(11);
        wb2.put(b"user_k".to_vec(), b"user_v".to_vec());
        manager
            .get_tablet(user_tablet_id)
            .expect("mounted user tablet")
            .write_batch(wb2)
            .unwrap();

        manager.flush_all_with_active().unwrap();

        let captures = manager.checkpoint_and_capture_all_tablets().await.unwrap();
        assert_eq!(captures.len(), 2);
        assert!(captures
            .iter()
            .any(|capture| capture.tablet_id == TabletId::System));
        assert!(captures
            .iter()
            .any(|capture| capture.tablet_id == user_tablet_id));

        let system_ilog = manager
            .tablet_dir(TabletId::System)
            .join("ilog")
            .join("tisql.ilog");
        let user_ilog = manager
            .tablet_dir(user_tablet_id)
            .join("ilog")
            .join("tisql.ilog");
        assert!(system_ilog.exists());
        assert!(user_ilog.exists());
        assert_ne!(system_ilog, user_ilog);

        let system_sst_dir = manager.tablet_dir(TabletId::System).join("sst");
        let user_sst_dir = manager.tablet_dir(user_tablet_id).join("sst");
        let system_sst_count = std::fs::read_dir(system_sst_dir)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .count();
        let user_sst_count = std::fs::read_dir(user_sst_dir)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .count();
        assert!(system_sst_count > 0);
        assert!(user_sst_count > 0);

        manager.stop_background_workers();
        manager.shutdown_manifest_writers();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase4_tablet_ilog_checkpoint_independent() {
        let dir = TempDir::new().unwrap();
        let lsn_provider = new_lsn_provider();

        let system_tablet_dir = dir.path().join("tablets").join("system");
        std::fs::create_dir_all(&system_tablet_dir).unwrap();
        let system_tablet = open_durable_tablet(&system_tablet_dir, Arc::clone(&lsn_provider));
        let manager =
            TabletManager::new(dir.path(), Arc::clone(&lsn_provider), system_tablet).unwrap();
        // Do not bind background workers in this test. We want deterministic
        // explicit concurrent flushes without scheduler races.

        let tablet_a = TabletId::Table {
            table_id: USER_TABLE_ID_START + 11,
        };
        let tablet_b = TabletId::Table {
            table_id: USER_TABLE_ID_START + 12,
        };
        manager
            .insert_tablet(
                tablet_a,
                open_durable_tablet(&manager.tablet_dir(tablet_a), Arc::clone(&lsn_provider)),
            )
            .unwrap();
        manager
            .insert_tablet(
                tablet_b,
                open_durable_tablet(&manager.tablet_dir(tablet_b), Arc::clone(&lsn_provider)),
            )
            .unwrap();

        let mut wba = WriteBatch::new();
        wba.set_commit_ts(101);
        wba.put(b"ka".to_vec(), b"va".to_vec());
        manager
            .get_tablet(tablet_a)
            .unwrap()
            .write_batch(wba)
            .unwrap();

        let mut wbb = WriteBatch::new();
        wbb.set_commit_ts(102);
        wbb.put(b"kb".to_vec(), b"vb".to_vec());
        manager
            .get_tablet(tablet_b)
            .unwrap()
            .write_batch(wbb)
            .unwrap();

        manager.flush_all_with_active().unwrap();

        // Stabilize tablet B's checkpoint state, then verify checkpointing A does
        // not append anything to B's ilog.
        manager
            .checkpoint_and_capture_tablet(tablet_b)
            .await
            .unwrap();
        let tablet_b_ilog = manager.tablet_dir(tablet_b).join("ilog").join("tisql.ilog");
        let b_size_before = std::fs::metadata(&tablet_b_ilog).unwrap().len();

        manager
            .checkpoint_and_capture_tablet(tablet_a)
            .await
            .unwrap();

        let b_size_after = std::fs::metadata(&tablet_b_ilog).unwrap().len();
        assert_eq!(
            b_size_before, b_size_after,
            "checkpointing tablet A must not mutate tablet B ilog"
        );

        manager.stop_background_workers();
        manager.shutdown_manifest_writers();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase4_tablet_ilog_truncate_independent() {
        let dir = TempDir::new().unwrap();
        let lsn_provider = new_lsn_provider();

        let system_tablet_dir = dir.path().join("tablets").join("system");
        std::fs::create_dir_all(&system_tablet_dir).unwrap();
        let system_tablet = open_durable_tablet(&system_tablet_dir, Arc::clone(&lsn_provider));
        let manager =
            TabletManager::new(dir.path(), Arc::clone(&lsn_provider), system_tablet).unwrap();
        manager
            .bind_background_runtime(&tokio::runtime::Handle::current())
            .unwrap();

        let tablet_a = TabletId::Table {
            table_id: USER_TABLE_ID_START + 13,
        };
        let tablet_b = TabletId::Table {
            table_id: USER_TABLE_ID_START + 14,
        };
        manager
            .insert_tablet(
                tablet_a,
                open_durable_tablet(&manager.tablet_dir(tablet_a), Arc::clone(&lsn_provider)),
            )
            .unwrap();
        manager
            .insert_tablet(
                tablet_b,
                open_durable_tablet(&manager.tablet_dir(tablet_b), Arc::clone(&lsn_provider)),
            )
            .unwrap();

        let mut wba = WriteBatch::new();
        wba.set_commit_ts(111);
        wba.put(b"ta".to_vec(), b"va".to_vec());
        manager
            .get_tablet(tablet_a)
            .unwrap()
            .write_batch(wba)
            .unwrap();

        let mut wbb = WriteBatch::new();
        wbb.set_commit_ts(112);
        wbb.put(b"tb".to_vec(), b"vb".to_vec());
        manager
            .get_tablet(tablet_b)
            .unwrap()
            .write_batch(wbb)
            .unwrap();

        manager.flush_all_with_active().unwrap();

        let capture_a = manager
            .checkpoint_and_capture_tablet(tablet_a)
            .await
            .unwrap();
        manager
            .checkpoint_and_capture_tablet(tablet_b)
            .await
            .unwrap();

        let tablet_a_ilog = manager.tablet_dir(tablet_a).join("ilog").join("tisql.ilog");
        let tablet_b_ilog = manager.tablet_dir(tablet_b).join("ilog").join("tisql.ilog");
        let a_size_before = std::fs::metadata(&tablet_a_ilog).unwrap().len();
        let b_size_before = std::fs::metadata(&tablet_b_ilog).unwrap().len();

        let a_tablet = manager.get_tablet(tablet_a).unwrap();
        let truncate_stats = a_tablet
            .truncate_ilog_before(capture_a.checkpoint_lsn)
            .await
            .unwrap();
        let a_size_after = std::fs::metadata(&tablet_a_ilog).unwrap().len();
        let b_size_after = std::fs::metadata(&tablet_b_ilog).unwrap().len();

        assert_eq!(truncate_stats.new_file_size, a_size_after);
        assert!(
            a_size_after <= a_size_before,
            "truncate should not grow tablet A ilog"
        );
        assert_eq!(
            b_size_before, b_size_after,
            "truncating tablet A ilog must not mutate tablet B ilog"
        );

        manager.stop_background_workers();
        manager.shutdown_manifest_writers();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_phase4_manifest_no_cross_contention_on_concurrent_flush() {
        let dir = TempDir::new().unwrap();
        let lsn_provider = new_lsn_provider();

        let system_tablet_dir = dir.path().join("tablets").join("system");
        std::fs::create_dir_all(&system_tablet_dir).unwrap();
        let system_tablet = open_durable_tablet(&system_tablet_dir, Arc::clone(&lsn_provider));
        let manager =
            TabletManager::new(dir.path(), Arc::clone(&lsn_provider), system_tablet).unwrap();
        manager
            .bind_background_runtime(&tokio::runtime::Handle::current())
            .unwrap();

        let tablet_a = TabletId::Table {
            table_id: USER_TABLE_ID_START + 41,
        };
        let tablet_b = TabletId::Table {
            table_id: USER_TABLE_ID_START + 42,
        };
        let engine_a =
            open_durable_tablet(&manager.tablet_dir(tablet_a), Arc::clone(&lsn_provider));
        let engine_b =
            open_durable_tablet(&manager.tablet_dir(tablet_b), Arc::clone(&lsn_provider));
        manager
            .insert_tablet(tablet_a, Arc::clone(&engine_a))
            .unwrap();
        manager
            .insert_tablet(tablet_b, Arc::clone(&engine_b))
            .unwrap();

        let mut wba = WriteBatch::new();
        wba.set_commit_ts(1001);
        wba.put(b"a".to_vec(), vec![b'a'; 128 * 1024]);
        engine_a.write_batch(wba).unwrap();
        engine_a.freeze_active();

        let mut wbb = WriteBatch::new();
        wbb.set_commit_ts(1002);
        wbb.put(b"b".to_vec(), vec![b'b'; 128 * 1024]);
        engine_b.write_batch(wbb).unwrap();
        engine_b.freeze_active();

        let flush_a = {
            let engine = Arc::clone(&engine_a);
            tokio::spawn(async move { engine.flush_all_with_active_async().await })
        };
        let flush_b = {
            let engine = Arc::clone(&engine_b);
            tokio::spawn(async move { engine.flush_all_with_active_async().await })
        };

        let (res_a, res_b) = timeout(Duration::from_secs(3), async {
            tokio::join!(flush_a, flush_b)
        })
        .await
        .expect("concurrent per-tablet flushes should not deadlock");

        assert!(!res_a.unwrap().unwrap().is_empty());
        assert!(!res_b.unwrap().unwrap().is_empty());
        assert!(count_sst_files(&manager.tablet_dir(tablet_a)) > 0);
        assert!(count_sst_files(&manager.tablet_dir(tablet_b)) > 0);

        manager.stop_background_workers();
        manager.shutdown_manifest_writers();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase4_shutdown_non_system_ilogs_keeps_system_tablet_alive() {
        let dir = TempDir::new().unwrap();
        let lsn_provider = new_lsn_provider();

        let system_tablet_dir = dir.path().join("tablets").join("system");
        std::fs::create_dir_all(&system_tablet_dir).unwrap();
        let system_tablet = open_durable_tablet(&system_tablet_dir, Arc::clone(&lsn_provider));

        let manager =
            TabletManager::new(dir.path(), Arc::clone(&lsn_provider), system_tablet).unwrap();
        manager
            .bind_background_runtime(&tokio::runtime::Handle::current())
            .unwrap();

        let user_tablet_id = TabletId::Table {
            table_id: USER_TABLE_ID_START + 1,
        };
        let user_tablet_dir = manager.tablet_dir(user_tablet_id);
        let user_tablet = open_durable_tablet(&user_tablet_dir, Arc::clone(&lsn_provider));
        manager.insert_tablet(user_tablet_id, user_tablet).unwrap();

        manager.close_non_system_tablet_ilogs().unwrap();
        manager.shutdown_non_system_tablet_ilogs();

        let system_checkpoint = manager
            .checkpoint_and_capture_tablet(TabletId::System)
            .await
            .unwrap();
        assert_eq!(system_checkpoint.tablet_id, TabletId::System);

        manager.stop_background_workers();
        manager.shutdown_manifest_writers();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase4_per_tablet_flush_scheduler_isolation() {
        let dir = TempDir::new().unwrap();
        let lsn_provider = new_lsn_provider();

        let system_tablet_dir = dir.path().join("tablets").join("system");
        std::fs::create_dir_all(&system_tablet_dir).unwrap();
        let system_tablet = open_durable_tablet(&system_tablet_dir, Arc::clone(&lsn_provider));
        let manager =
            TabletManager::new(dir.path(), Arc::clone(&lsn_provider), system_tablet).unwrap();
        manager
            .bind_background_runtime(&tokio::runtime::Handle::current())
            .unwrap();

        let tablet_a = TabletId::Table {
            table_id: USER_TABLE_ID_START + 21,
        };
        let tablet_b = TabletId::Table {
            table_id: USER_TABLE_ID_START + 22,
        };
        let engine_a =
            open_durable_tablet(&manager.tablet_dir(tablet_a), Arc::clone(&lsn_provider));
        let engine_b =
            open_durable_tablet(&manager.tablet_dir(tablet_b), Arc::clone(&lsn_provider));
        manager
            .insert_tablet(tablet_a, Arc::clone(&engine_a))
            .unwrap();
        manager
            .insert_tablet(tablet_b, Arc::clone(&engine_b))
            .unwrap();

        let mut wb = WriteBatch::new();
        wb.set_commit_ts(100);
        wb.put(b"a".to_vec(), vec![b'x'; 64 * 1024]);
        engine_a.write_batch(wb).unwrap();
        engine_a.freeze_active();

        timeout(Duration::from_secs(3), async {
            loop {
                if count_sst_files(&manager.tablet_dir(tablet_a)) > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("tablet A should flush in background");

        assert_eq!(
            count_sst_files(&manager.tablet_dir(tablet_b)),
            0,
            "tablet B should not flush when only tablet A rotates"
        );

        manager.stop_background_workers();
        manager.shutdown_manifest_writers();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_phase4_concurrent_checkpoint_capture_across_tablets() {
        let dir = TempDir::new().unwrap();
        let lsn_provider = new_lsn_provider();

        let system_tablet_dir = dir.path().join("tablets").join("system");
        std::fs::create_dir_all(&system_tablet_dir).unwrap();
        let system_tablet = open_durable_tablet(&system_tablet_dir, Arc::clone(&lsn_provider));
        let manager =
            TabletManager::new(dir.path(), Arc::clone(&lsn_provider), system_tablet).unwrap();
        manager
            .bind_background_runtime(&tokio::runtime::Handle::current())
            .unwrap();

        let tablet_a = TabletId::Table {
            table_id: USER_TABLE_ID_START + 31,
        };
        let tablet_b = TabletId::Table {
            table_id: USER_TABLE_ID_START + 32,
        };
        manager
            .insert_tablet(
                tablet_a,
                open_durable_tablet(&manager.tablet_dir(tablet_a), Arc::clone(&lsn_provider)),
            )
            .unwrap();
        manager
            .insert_tablet(
                tablet_b,
                open_durable_tablet(&manager.tablet_dir(tablet_b), Arc::clone(&lsn_provider)),
            )
            .unwrap();

        let mut wba = WriteBatch::new();
        wba.set_commit_ts(200);
        wba.put(b"ka".to_vec(), b"va".to_vec());
        manager
            .get_tablet(tablet_a)
            .unwrap()
            .write_batch(wba)
            .unwrap();

        let mut wbb = WriteBatch::new();
        wbb.set_commit_ts(201);
        wbb.put(b"kb".to_vec(), b"vb".to_vec());
        manager
            .get_tablet(tablet_b)
            .unwrap()
            .write_batch(wbb)
            .unwrap();

        manager.flush_all_with_active().unwrap();

        let captures = timeout(Duration::from_secs(2), async {
            tokio::join!(
                manager.checkpoint_and_capture_tablet(tablet_a),
                manager.checkpoint_and_capture_tablet(tablet_b),
            )
        })
        .await
        .expect("concurrent tablet checkpoints should complete");

        assert_eq!(captures.0.unwrap().tablet_id, tablet_a);
        assert_eq!(captures.1.unwrap().tablet_id, tablet_b);

        manager.stop_background_workers();
        manager.shutdown_manifest_writers();
    }
}
