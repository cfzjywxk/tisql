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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use dashmap::DashMap;

/// Logical source attached to an I/O operation for observability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum IoSource {
    ForegroundPointRead,
    ForegroundScan,
    Clog,
    Flush,
    Compaction,
    Manifest,
    Recovery,
    Other,
}

impl IoSource {
    pub fn is_background(self) -> bool {
        matches!(
            self,
            Self::Flush | Self::Compaction | Self::Manifest | Self::Recovery
        )
    }
}

impl Default for IoSource {
    fn default() -> Self {
        Self::Other
    }
}

/// I/O operation kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum IoOpKind {
    Read,
    Write,
    Fsync,
}

/// Source tag carried by one I/O request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct IoTraceTag {
    pub tablet_ns: u128,
    pub source: IoSource,
}

impl IoTraceTag {
    pub const fn new(tablet_ns: u128, source: IoSource) -> Self {
        Self { tablet_ns, source }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoCounterSnapshot {
    pub read_ops: u64,
    pub read_bytes: u64,
    pub write_ops: u64,
    pub write_bytes: u64,
    pub fsync_ops: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IoSnapshotEntry {
    pub tablet_ns: u128,
    pub source: IoSource,
    pub counters: IoCounterSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
struct IoKey {
    tablet_ns: u128,
    source: IoSource,
}

#[derive(Default)]
struct IoCounters {
    read_ops: AtomicU64,
    read_bytes: AtomicU64,
    write_ops: AtomicU64,
    write_bytes: AtomicU64,
    fsync_ops: AtomicU64,
}

impl IoCounters {
    fn record(&self, kind: IoOpKind, bytes: u64) {
        match kind {
            IoOpKind::Read => {
                self.read_ops.fetch_add(1, Ordering::Relaxed);
                self.read_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            IoOpKind::Write => {
                self.write_ops.fetch_add(1, Ordering::Relaxed);
                self.write_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            IoOpKind::Fsync => {
                self.fsync_ops.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn snapshot(&self) -> IoCounterSnapshot {
        IoCounterSnapshot {
            read_ops: self.read_ops.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            write_ops: self.write_ops.load(Ordering::Relaxed),
            write_bytes: self.write_bytes.load(Ordering::Relaxed),
            fsync_ops: self.fsync_ops.load(Ordering::Relaxed),
        }
    }
}

/// Process-wide I/O accountant keyed by `(tablet_ns, source)`.
pub struct IoAccountant {
    counters: DashMap<IoKey, Arc<IoCounters>>,
}

impl Default for IoAccountant {
    fn default() -> Self {
        Self {
            counters: DashMap::new(),
        }
    }
}

impl IoAccountant {
    pub fn global() -> &'static Self {
        static GLOBAL: OnceLock<IoAccountant> = OnceLock::new();
        GLOBAL.get_or_init(IoAccountant::default)
    }

    fn slot_for(&self, tag: IoTraceTag) -> Arc<IoCounters> {
        let key = IoKey {
            tablet_ns: tag.tablet_ns,
            source: tag.source,
        };
        if let Some(slot) = self.counters.get(&key) {
            return Arc::clone(slot.value());
        }

        let slot = Arc::new(IoCounters::default());
        Arc::clone(
            self.counters
                .entry(key)
                .or_insert_with(|| Arc::clone(&slot))
                .value(),
        )
    }

    pub fn record(&self, tag: IoTraceTag, kind: IoOpKind, bytes: u64) {
        self.slot_for(tag).record(kind, bytes);
    }

    pub fn snapshot(&self) -> Vec<IoSnapshotEntry> {
        let mut rows: Vec<IoSnapshotEntry> = self
            .counters
            .iter()
            .map(|entry| IoSnapshotEntry {
                tablet_ns: entry.key().tablet_ns,
                source: entry.key().source,
                counters: entry.value().snapshot(),
            })
            .collect();
        rows.sort_by_key(|row| (row.tablet_ns, row.source));
        rows
    }

    pub fn remove_tablet_ns(&self, tablet_ns: u128) -> usize {
        let keys: Vec<IoKey> = self
            .counters
            .iter()
            .filter_map(|entry| (entry.key().tablet_ns == tablet_ns).then_some(*entry.key()))
            .collect();
        let removed = keys.len();
        for key in keys {
            self.counters.remove(&key);
        }
        removed
    }

    #[cfg(test)]
    pub fn reset(&self) {
        self.counters.clear();
    }
}

pub fn record_io(tag: IoTraceTag, kind: IoOpKind, bytes: u64) {
    IoAccountant::global().record(tag, kind, bytes);
}

pub fn snapshot_io() -> Vec<IoSnapshotEntry> {
    IoAccountant::global().snapshot()
}

pub fn remove_io_namespace(tablet_ns: u128) -> usize {
    IoAccountant::global().remove_tablet_ns(tablet_ns)
}

#[cfg(test)]
pub fn reset_io_accounting() {
    IoAccountant::global().reset();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remove_tablet_ns_removes_only_target_namespace() {
        let accountant = IoAccountant::default();
        let ns_a = 11_u128;
        let ns_b = 22_u128;

        accountant.record(
            IoTraceTag::new(ns_a, IoSource::ForegroundPointRead),
            IoOpKind::Read,
            128,
        );
        accountant.record(IoTraceTag::new(ns_a, IoSource::Flush), IoOpKind::Write, 256);
        accountant.record(
            IoTraceTag::new(ns_b, IoSource::ForegroundScan),
            IoOpKind::Read,
            512,
        );

        let removed = accountant.remove_tablet_ns(ns_a);
        assert_eq!(removed, 2);

        let rows = accountant.snapshot();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tablet_ns, ns_b);
        assert_eq!(rows[0].source, IoSource::ForegroundScan);
        assert_eq!(rows[0].counters.read_ops, 1);
        assert_eq!(rows[0].counters.read_bytes, 512);
    }
}
