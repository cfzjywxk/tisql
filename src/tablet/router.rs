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

//! Tablet routing and tablet-id path codec helpers.
//!
//! Phase-1 routing is deterministic but behavior-preserving: callers can still
//! route all traffic to system in higher-level code. This module provides the
//! stable mapping rules and disk name codec used by later phases.

use crate::catalog::types::{IndexId, TableId};
use crate::inner_table::core_tables::USER_TABLE_ID_START;
use crate::util::error::{Result, TiSqlError};
use std::fmt;

#[cfg(test)]
use crate::util::codec::key::{decode_index_key, decode_table_id, is_index_key, is_record_key};

/// Stable tablet identifier used by routing and disk layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TabletId {
    /// Shared system/inner-table tablet.
    System,
    /// User table data tablet.
    Table { table_id: TableId },
    /// User local secondary index tablet.
    LocalIndex {
        table_id: TableId,
        index_id: IndexId,
    },
}

impl fmt::Display for TabletId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::System => f.write_str("system"),
            Self::Table { table_id } => write!(f, "t_{table_id}"),
            Self::LocalIndex { table_id, index_id } => write!(f, "i_{table_id}_{index_id}"),
        }
    }
}

impl TabletId {
    /// Canonical on-disk directory name for this tablet.
    pub fn dir_name(self) -> String {
        self.to_string()
    }

    /// Parse a canonical on-disk directory name into `TabletId`.
    pub fn from_dir_name(name: &str) -> Result<Self> {
        if name == "system" {
            return Ok(Self::System);
        }

        if let Some(raw_table_id) = name.strip_prefix("t_") {
            let table_id = parse_u64(raw_table_id, "table id", name)?;
            return Ok(Self::Table { table_id });
        }

        if let Some(raw) = name.strip_prefix("i_") {
            let mut parts = raw.split('_');
            let table = parts.next();
            let index = parts.next();
            let extra = parts.next();
            if table.is_none() || index.is_none() || extra.is_some() {
                return Err(TiSqlError::Codec(format!(
                    "invalid index tablet dir name: {name}"
                )));
            }

            let table_id = parse_u64(table.unwrap(), "table id", name)?;
            let index_id = parse_u64(index.unwrap(), "index id", name)?;
            return Ok(Self::LocalIndex { table_id, index_id });
        }

        Err(TiSqlError::Codec(format!(
            "invalid tablet dir name: {name}"
        )))
    }
}

/// True when `table_id` belongs to inner/system space.
#[inline]
pub fn is_system_table_id(table_id: TableId) -> bool {
    table_id < USER_TABLE_ID_START
}

/// Route a planned record-table target (no key decoding required).
///
/// This is the preferred upper-layer routing entrypoint once planner/executor
/// provide table metadata.
#[inline]
pub fn route_table_to_tablet(table_id: TableId) -> TabletId {
    if is_system_table_id(table_id) {
        TabletId::System
    } else {
        TabletId::Table { table_id }
    }
}

/// Route a planned local-index target (no key decoding required).
///
/// This is the preferred upper-layer routing entrypoint once planner/executor
/// provide index metadata.
#[inline]
pub fn route_index_to_tablet(table_id: TableId, index_id: IndexId) -> TabletId {
    if is_system_table_id(table_id) {
        TabletId::System
    } else {
        TabletId::LocalIndex { table_id, index_id }
    }
}

/// Test-only key decoding helper for routing compatibility checks.
///
/// Production paths should route by planner metadata via `route_table_to_tablet`
/// / `route_index_to_tablet`.
#[cfg(test)]
pub(crate) fn route_key_to_tablet(key: &[u8]) -> TabletId {
    let table_id = match decode_table_id(key) {
        Ok(id) => id,
        Err(_) => return TabletId::System,
    };

    if is_system_table_id(table_id) {
        return TabletId::System;
    }

    // `is_record_key` validates legacy/int-handle row keys. For common-handle
    // row keys, the handle payload can be shorter than that legacy minimum, so
    // we also accept the canonical record prefix marker directly.
    if is_record_key(key) || has_record_prefix(key) {
        return route_table_to_tablet(table_id);
    }

    if is_index_key(key) {
        return match decode_index_key(key) {
            Ok((decoded_table_id, index_id, _)) if decoded_table_id == table_id => {
                route_index_to_tablet(table_id, index_id)
            }
            _ => TabletId::System,
        };
    }

    TabletId::System
}

#[cfg(test)]
#[inline]
fn has_record_prefix(key: &[u8]) -> bool {
    // 't' (1 byte) + table_id (8 bytes) + "_r" (2 bytes)
    key.len() >= 11 && &key[9..11] == b"_r"
}

fn parse_u64(raw: &str, label: &str, full_name: &str) -> Result<u64> {
    raw.parse::<u64>().map_err(|_| {
        TiSqlError::Codec(format!(
            "invalid {label} in tablet dir name {full_name}: {raw}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::types::Value;
    use crate::inner_table::core_tables::{
        ALL_COLUMN_TABLE_ID, ALL_GC_DELETE_RANGE_TABLE_ID, ALL_INDEX_TABLE_ID, ALL_META_TABLE_ID,
        ALL_SCHEMA_TABLE_ID, ALL_TABLE_TABLE_ID, USER_TABLE_ID_START,
    };
    use crate::util::codec::key::{
        encode_index_seek_key, encode_record_key, encode_record_key_with_handle,
        encode_value_for_key, encode_values_for_key,
    };

    #[test]
    fn route_non_table_key_to_system() {
        assert_eq!(route_key_to_tablet(b"meta:key"), TabletId::System);
        assert_eq!(route_key_to_tablet(b""), TabletId::System);
    }

    #[test]
    fn route_planned_targets_without_key_decode() {
        assert_eq!(
            route_table_to_tablet(USER_TABLE_ID_START - 1),
            TabletId::System
        );
        assert_eq!(
            route_table_to_tablet(USER_TABLE_ID_START),
            TabletId::Table {
                table_id: USER_TABLE_ID_START,
            }
        );

        assert_eq!(
            route_index_to_tablet(USER_TABLE_ID_START - 1, 77),
            TabletId::System
        );
        assert_eq!(
            route_index_to_tablet(USER_TABLE_ID_START, 77),
            TabletId::LocalIndex {
                table_id: USER_TABLE_ID_START,
                index_id: 77,
            }
        );
    }

    #[test]
    fn route_system_table_keys_to_system_tablet() {
        let record_key = encode_record_key_with_handle(ALL_TABLE_TABLE_ID, 1);
        assert_eq!(route_key_to_tablet(&record_key), TabletId::System);

        let mut encoded_values = Vec::new();
        encode_value_for_key(&mut encoded_values, &Value::BigInt(7));
        let index_key = encode_index_seek_key(ALL_TABLE_TABLE_ID, 77, &encoded_values);
        assert_eq!(route_key_to_tablet(&index_key), TabletId::System);
    }

    #[test]
    fn route_boundary_around_user_table_id_start() {
        let system_boundary = USER_TABLE_ID_START - 1;

        let system_record_key = encode_record_key_with_handle(system_boundary, 1);
        assert_eq!(route_key_to_tablet(&system_record_key), TabletId::System);

        let user_record_key = encode_record_key_with_handle(USER_TABLE_ID_START, 1);
        assert_eq!(
            route_key_to_tablet(&user_record_key),
            TabletId::Table {
                table_id: USER_TABLE_ID_START,
            }
        );

        let system_index_key = encode_index_seek_key(system_boundary, 201, b"v");
        assert_eq!(route_key_to_tablet(&system_index_key), TabletId::System);

        let user_index_key = encode_index_seek_key(USER_TABLE_ID_START, 202, b"v");
        assert_eq!(
            route_key_to_tablet(&user_index_key),
            TabletId::LocalIndex {
                table_id: USER_TABLE_ID_START,
                index_id: 202,
            }
        );
    }

    #[test]
    fn route_user_table_record_and_index_keys() {
        let table_id = USER_TABLE_ID_START + 88;

        let record_key = encode_record_key_with_handle(table_id, 123);
        assert_eq!(
            route_key_to_tablet(&record_key),
            TabletId::Table { table_id }
        );

        let index_key = encode_index_seek_key(table_id, 2001, b"abc");
        assert_eq!(
            route_key_to_tablet(&index_key),
            TabletId::LocalIndex {
                table_id,
                index_id: 2001,
            }
        );
    }

    #[test]
    fn route_common_handle_record_key_to_table_tablet() {
        let table_id = USER_TABLE_ID_START + 90;
        // Short common-handle payload (5 bytes for INT) used by SQL primary key rows.
        let common_handle = encode_values_for_key(&[Value::Int(1)]);
        let record_key = encode_record_key(table_id, &common_handle);
        assert_eq!(
            route_key_to_tablet(&record_key),
            TabletId::Table { table_id }
        );
    }

    #[test]
    fn route_malformed_table_keys_fallback_to_system() {
        // Valid table id (user), but unknown kind marker `_x`.
        let mut unknown_marker = encode_record_key_with_handle(USER_TABLE_ID_START, 7);
        unknown_marker[9] = b'_';
        unknown_marker[10] = b'x';
        assert_eq!(route_key_to_tablet(&unknown_marker), TabletId::System);

        // `_i` marker but missing full index id payload.
        let mut short_index = encode_record_key_with_handle(USER_TABLE_ID_START, 9);
        short_index[9] = b'_';
        short_index[10] = b'i';
        short_index.truncate(13);
        assert_eq!(route_key_to_tablet(&short_index), TabletId::System);
    }

    #[test]
    fn tablet_id_dir_name_roundtrip() {
        let ids = [
            TabletId::System,
            TabletId::Table {
                table_id: USER_TABLE_ID_START,
            },
            TabletId::LocalIndex {
                table_id: USER_TABLE_ID_START,
                index_id: 2001,
            },
        ];

        for id in ids {
            let name = id.dir_name();
            let parsed = TabletId::from_dir_name(&name).unwrap();
            assert_eq!(parsed, id, "roundtrip mismatch for {name}");
        }
    }

    #[test]
    fn tablet_id_dir_name_rejects_invalid_inputs() {
        for invalid in ["", "sys", "t_", "t_x", "i_", "i_1", "i_1_", "i_1_2_3"] {
            assert!(TabletId::from_dir_name(invalid).is_err(), "input={invalid}");
        }
    }

    #[test]
    fn tablet_id_display_uses_canonical_names() {
        assert_eq!(TabletId::System.to_string(), "system");
        assert_eq!(
            TabletId::Table {
                table_id: USER_TABLE_ID_START,
            }
            .to_string(),
            format!("t_{USER_TABLE_ID_START}")
        );
        assert_eq!(
            TabletId::LocalIndex {
                table_id: USER_TABLE_ID_START,
                index_id: 2001,
            }
            .to_string(),
            format!("i_{USER_TABLE_ID_START}_2001")
        );
    }

    /// T1.5h: Verify `_r` routes to Table and `_i` routes to LocalIndex for same table_id.
    #[test]
    fn route_key_kind_discriminator() {
        let table_id = USER_TABLE_ID_START + 42;

        // `_r` key (record) → Table tablet
        let record_key = encode_record_key_with_handle(table_id, 1);
        assert_eq!(
            route_key_to_tablet(&record_key),
            TabletId::Table { table_id },
            "_r key should route to Table tablet"
        );

        // `_i` key (index) → LocalIndex tablet
        let mut encoded_values = Vec::new();
        encode_value_for_key(&mut encoded_values, &Value::BigInt(99));
        let index_key = encode_index_seek_key(table_id, 500, &encoded_values);
        assert_eq!(
            route_key_to_tablet(&index_key),
            TabletId::LocalIndex {
                table_id,
                index_id: 500,
            },
            "_i key should route to LocalIndex tablet"
        );
    }

    /// T1.5i: All 6 inner/system table IDs (1-6) must route to System.
    #[test]
    fn route_all_inner_table_ids_to_system() {
        let inner_table_ids = [
            ALL_META_TABLE_ID,
            ALL_SCHEMA_TABLE_ID,
            ALL_TABLE_TABLE_ID,
            ALL_COLUMN_TABLE_ID,
            ALL_INDEX_TABLE_ID,
            ALL_GC_DELETE_RANGE_TABLE_ID,
        ];

        for &tid in &inner_table_ids {
            // Record key for inner table → System
            let record_key = encode_record_key_with_handle(tid, 1);
            assert_eq!(
                route_key_to_tablet(&record_key),
                TabletId::System,
                "inner table record key (table_id={tid}) must route to System"
            );

            // Index key for inner table → System
            let index_key = encode_index_seek_key(tid, 1, b"v");
            assert_eq!(
                route_key_to_tablet(&index_key),
                TabletId::System,
                "inner table index key (table_id={tid}) must route to System"
            );

            // Metadata-first routing for inner table → System
            assert_eq!(
                route_table_to_tablet(tid),
                TabletId::System,
                "route_table_to_tablet({tid}) must return System"
            );
            assert_eq!(
                route_index_to_tablet(tid, 1),
                TabletId::System,
                "route_index_to_tablet({tid}, 1) must return System"
            );
        }
    }

    #[test]
    fn route_key_to_tablet_random_bytes_fallback_safety() {
        // Simple deterministic LCG so this remains CI-stable without external RNG crates.
        let mut seed: u64 = 0x1234_5678_9abc_def0;
        for len in 0..=64usize {
            for _ in 0..128usize {
                let mut key = Vec::with_capacity(len);
                for _ in 0..len {
                    seed = seed
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    key.push((seed >> 24) as u8);
                }

                let routed = route_key_to_tablet(&key);
                if key.first().copied() != Some(b't') {
                    assert_eq!(
                        routed,
                        TabletId::System,
                        "non-table prefix routed incorrectly for key={key:?}"
                    );
                }
            }
        }
    }
}
