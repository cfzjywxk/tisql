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
use crate::util::error::Result;

use super::router::{route_index_to_tablet, route_table_to_tablet, TabletId};
use super::TabletEngine;

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
        inventory.extend(self.discover_existing_tablet_dirs()?);
        self.register_desired_tablets(inventory.iter().copied())?;
        Ok(inventory.into_iter().collect())
    }

    /// Snapshot of desired tablet IDs (mounted + not-yet-mounted).
    pub fn desired_tablets(&self) -> Vec<TabletId> {
        self.desired_tablets.read().iter().copied().collect()
    }

    /// Insert one mounted tablet engine (future phases).
    pub fn insert_tablet(&self, tablet_id: TabletId, tablet: Arc<TabletEngine>) -> Result<()> {
        self.ensure_tablet_dir(tablet_id)?;
        self.desired_tablets.write().insert(tablet_id);
        self.tablets.write().insert(tablet_id, tablet);
        Ok(())
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
    use crate::tablet::LsmConfig;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn open_test_system_tablet(dir: &Path) -> Arc<TabletEngine> {
        Arc::new(TabletEngine::open(LsmConfig::new(dir)).unwrap())
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
}
