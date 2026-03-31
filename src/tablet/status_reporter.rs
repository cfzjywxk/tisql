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

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use tokio::sync::Notify;

use crate::io::{snapshot_io, IoCounterSnapshot, IoSnapshotEntry, IoSource};
use crate::{log_info, log_warn};

use super::cache::{
    BlockCacheNamespaceUsage, BlockCacheStats, ReaderCacheStats, RowCacheNamespaceUsage,
    RowCacheStats,
};
use super::{BackpressureState, CacheSuite, LsmStats, TabletId, TabletManager};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EngineStatusMetricRow {
    pub scope: String,
    pub tablet: String,
    pub metric: String,
    pub value: String,
}

#[derive(Debug, Clone, Default)]
pub struct EngineStatusSnapshot {
    pub ts_unix_ms: u64,
    pub collection_duration_us: u64,
    pub global: GlobalStatus,
    pub tablets: Vec<TabletStatus>,
}

#[derive(Debug, Clone, Default)]
pub struct GlobalStatus {
    pub tablet_count: usize,
    pub reported_tablets: usize,
    pub truncated_tablets: usize,
    pub total_active_memtable_bytes: usize,
    pub total_frozen_memtable_bytes: usize,
    pub total_frozen_memtable_count: usize,
    pub total_sst_count: usize,
    pub total_sst_bytes: u64,
    pub cache_delta: CacheDeltaStatus,
    pub io: IoStatus,
    pub flush_jobs: JobCounters,
    pub compaction_jobs: JobCounters,
}

#[derive(Debug, Clone)]
pub struct TabletStatus {
    pub tablet_id: TabletId,
    pub tablet_ns: u128,
    pub cache: CacheStatus,
    pub io: IoStatus,
    pub lsm: LsmStats,
    pub jobs: JobStatus,
    pub backpressure: BackpressureStatus,
}

#[derive(Debug, Clone, Default)]
pub struct CacheStatus {
    pub block: Option<BlockCacheStatus>,
    pub row: Option<RowCacheStatus>,
}

#[derive(Debug, Clone, Default)]
pub struct CacheDeltaStatus {
    pub block: Option<BlockCacheStats>,
    pub row: Option<RowCacheStats>,
    pub reader: Option<ReaderCacheStats>,
}

#[derive(Debug, Clone, Default)]
pub struct BlockCacheStatus {
    pub data_usage_bytes: u64,
    pub data_entries: u64,
    pub index_usage_bytes: u64,
    pub index_entries: u64,
    pub bloom_usage_bytes: u64,
    pub bloom_entries: u64,
}

impl BlockCacheStatus {
    pub fn total_usage_bytes(&self) -> u64 {
        self.data_usage_bytes + self.index_usage_bytes + self.bloom_usage_bytes
    }
}

#[derive(Debug, Clone, Default)]
pub struct RowCacheStatus {
    pub usage_bytes: u64,
    pub entries: u64,
}

#[derive(Debug, Clone, Default)]
pub struct IoStatus {
    pub by_source: Vec<IoSourceStatus>,
    pub foreground_ops: u64,
    pub foreground_bytes: u64,
    pub background_ops: u64,
    pub background_bytes: u64,
}

#[derive(Debug, Clone, Default)]
pub struct IoSourceStatus {
    pub source: IoSource,
    pub read_ops: u64,
    pub read_bytes: u64,
    pub write_ops: u64,
    pub write_bytes: u64,
    pub fsync_ops: u64,
}

impl IoSourceStatus {
    fn absorb(&mut self, counters: IoCounterSnapshot) {
        self.read_ops += counters.read_ops;
        self.read_bytes += counters.read_bytes;
        self.write_ops += counters.write_ops;
        self.write_bytes += counters.write_bytes;
        self.fsync_ops += counters.fsync_ops;
    }
}

#[derive(Debug, Clone, Default)]
pub struct JobStatus {
    pub flush: JobCounters,
    pub compaction: JobCounters,
}

#[derive(Debug, Clone, Default)]
pub struct JobCounters {
    pub in_progress: u64,
    pub completed: u64,
    pub failed: u64,
    pub pending: u64,
    pub last_error_ts_ms: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct BackpressureStatus {
    pub state: BackpressureState,
    pub slowdown_events: u64,
    pub stall_events: u64,
    pub frozen_slowdown_events: u64,
    pub frozen_cap_stall_events: u64,
    pub l0_stop_stall_events: u64,
    pub mem_pressure_soft_events: u64,
    pub mem_pressure_hard_events: u64,
    pub mem_pressure_soft_residency_ms: u64,
    pub mem_pressure_hard_residency_ms: u64,
    pub writer_wait_events: u64,
    pub writer_wake_events: u64,
    pub writer_wait_timeout_events: u64,
}

pub struct EngineStatusReporter {
    inner: Arc<EngineStatusReporterInner>,
    worker_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

struct EngineStatusReporterInner {
    tablet_manager: Arc<TabletManager>,
    cache_suite: Arc<CacheSuite>,
    interval: Duration,
    top_n_tablets: usize,
    shutdown: AtomicBool,
    notify: Notify,
    latest_snapshot: RwLock<Option<Arc<EngineStatusSnapshot>>>,
    previous_io_totals: parking_lot::Mutex<BTreeMap<(u128, IoSource), IoCounterSnapshot>>,
}

impl EngineStatusReporter {
    pub fn new(
        tablet_manager: Arc<TabletManager>,
        cache_suite: Arc<CacheSuite>,
        interval_secs: u64,
        top_n_tablets: usize,
    ) -> Self {
        Self {
            inner: Arc::new(EngineStatusReporterInner {
                tablet_manager,
                cache_suite,
                interval: Duration::from_secs(interval_secs),
                top_n_tablets,
                shutdown: AtomicBool::new(false),
                notify: Notify::new(),
                latest_snapshot: RwLock::new(None),
                previous_io_totals: parking_lot::Mutex::new(BTreeMap::new()),
            }),
            worker_handle: parking_lot::Mutex::new(None),
        }
    }

    pub fn interval(&self) -> Duration {
        self.inner.interval
    }

    pub fn start(&self, handle: &tokio::runtime::Handle) {
        let mut worker = self.worker_handle.lock();
        if worker.is_some() {
            return;
        }
        let inner = Arc::clone(&self.inner);
        *worker = Some(handle.spawn(async move {
            inner.run().await;
        }));
    }

    pub fn stop(&self) {
        self.inner.shutdown.store(true, Ordering::SeqCst);
        self.inner.notify.notify_one();
        if let Some(handle) = self.worker_handle.lock().take() {
            handle.abort();
        }
    }

    pub fn latest_snapshot(&self) -> Option<Arc<EngineStatusSnapshot>> {
        self.inner.latest_snapshot.read().clone()
    }

    pub fn collect_once(&self) -> Arc<EngineStatusSnapshot> {
        self.inner.collect_and_publish(true)
    }

    pub fn collect_once_quiet(&self) -> Arc<EngineStatusSnapshot> {
        self.inner.collect_and_publish(false)
    }
}

impl Drop for EngineStatusReporter {
    fn drop(&mut self) {
        self.stop();
    }
}

impl EngineStatusReporterInner {
    async fn run(&self) {
        // Publish one snapshot immediately for SQL query path.
        let _ = self.collect_and_publish(true);

        while !self.shutdown.load(Ordering::Relaxed) {
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep(self.interval) => {}
            }
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
            let _ = self.collect_and_publish(true);
        }
    }

    fn collect_and_publish(&self, emit_log: bool) -> Arc<EngineStatusSnapshot> {
        let io_entries = self.snapshot_io_delta();
        let snapshot = Arc::new(collect_snapshot_with_io_entries(
            self.tablet_manager.as_ref(),
            self.cache_suite.as_ref(),
            self.top_n_tablets,
            io_entries,
        ));
        *self.latest_snapshot.write() = Some(Arc::clone(&snapshot));
        if emit_log {
            log_snapshot(snapshot.as_ref());
        }
        snapshot
    }

    fn snapshot_io_delta(&self) -> Vec<IoSnapshotEntry> {
        let current_entries = snapshot_io();
        let mut previous = self.previous_io_totals.lock();
        let mut current_totals: BTreeMap<(u128, IoSource), IoCounterSnapshot> = BTreeMap::new();
        let mut deltas = Vec::with_capacity(current_entries.len());

        for entry in current_entries {
            let key = (entry.tablet_ns, entry.source);
            let previous_counters = previous.get(&key).copied().unwrap_or_default();
            current_totals.insert(key, entry.counters);

            let delta = diff_io_counters(entry.counters, previous_counters);
            if !io_counters_is_zero(delta) {
                deltas.push(IoSnapshotEntry {
                    tablet_ns: entry.tablet_ns,
                    source: entry.source,
                    counters: delta,
                });
            }
        }

        *previous = current_totals;
        deltas
    }
}

pub fn collect_snapshot(
    tablet_manager: &TabletManager,
    cache_suite: &CacheSuite,
    top_n_tablets: usize,
) -> EngineStatusSnapshot {
    collect_snapshot_with_io_entries(tablet_manager, cache_suite, top_n_tablets, snapshot_io())
}

fn collect_snapshot_with_io_entries(
    tablet_manager: &TabletManager,
    cache_suite: &CacheSuite,
    top_n_tablets: usize,
    io_entries: Vec<IoSnapshotEntry>,
) -> EngineStatusSnapshot {
    let begin = Instant::now();
    let ts_unix_ms = now_ms();
    let tablets = tablet_manager.all_tablets();
    let worker_statuses = tablet_manager.worker_statuses();
    let cache_delta = collect_cache_delta(cache_suite);
    let block_usage = collect_block_usage(cache_suite);
    let row_usage = collect_row_usage(cache_suite);
    let global_io = io_from_entries(&io_entries);

    let mut io_by_ns: BTreeMap<u128, Vec<IoSnapshotEntry>> = BTreeMap::new();
    for entry in io_entries {
        io_by_ns.entry(entry.tablet_ns).or_default().push(entry);
    }

    let mut tablet_rows: Vec<TabletStatus> = Vec::with_capacity(tablets.len());
    for (tablet_id, tablet) in tablets {
        let ns = tablet.tablet_cache_ns();
        let lsm = tablet.stats();
        let jobs = worker_statuses.get(&tablet_id).copied().unwrap_or_default();

        let cache = CacheStatus {
            block: block_usage.get(&ns).cloned().map(to_block_cache_status),
            row: row_usage.get(&ns).cloned().map(to_row_cache_status),
        };
        let io = io_from_entries(io_by_ns.get(&ns).map(Vec::as_slice).unwrap_or(&[]));
        let backpressure = BackpressureStatus {
            state: lsm.backpressure_state,
            slowdown_events: lsm.slowdown_events,
            stall_events: lsm.stall_events,
            frozen_slowdown_events: lsm.frozen_slowdown_events,
            frozen_cap_stall_events: lsm.frozen_stall_events,
            l0_stop_stall_events: lsm.l0_stop_stall_events,
            mem_pressure_soft_events: lsm.mem_pressure_soft_events,
            mem_pressure_hard_events: lsm.mem_pressure_hard_events,
            mem_pressure_soft_residency_ms: lsm.mem_pressure_soft_residency_ms,
            mem_pressure_hard_residency_ms: lsm.mem_pressure_hard_residency_ms,
            writer_wait_events: lsm.writer_wait_events,
            writer_wake_events: lsm.writer_wake_events,
            writer_wait_timeout_events: lsm.writer_wait_timeout_events,
        };
        let job_status = JobStatus {
            flush: JobCounters {
                in_progress: jobs.flush.in_progress,
                completed: jobs.flush.completed,
                failed: jobs.flush.failed,
                pending: tablet.frozen_count() as u64,
                last_error_ts_ms: jobs.flush.last_error_ts_ms,
            },
            compaction: JobCounters {
                in_progress: jobs.compaction.in_progress,
                completed: jobs.compaction.completed,
                failed: jobs.compaction.failed,
                pending: if tablet.has_pending_compaction() {
                    1
                } else {
                    0
                },
                last_error_ts_ms: jobs.compaction.last_error_ts_ms,
            },
        };
        tablet_rows.push(TabletStatus {
            tablet_id,
            tablet_ns: ns,
            cache,
            io,
            lsm,
            jobs: job_status,
            backpressure,
        });
    }

    let mut global = GlobalStatus {
        tablet_count: tablet_rows.len(),
        cache_delta,
        io: global_io,
        ..GlobalStatus::default()
    };
    for tablet in &tablet_rows {
        global.total_active_memtable_bytes += tablet.lsm.active_memtable_size;
        global.total_frozen_memtable_bytes += tablet.lsm.frozen_memtable_bytes;
        global.total_frozen_memtable_count += tablet.lsm.frozen_memtable_count;
        global.total_sst_count += tablet.lsm.total_sst_count;
        global.total_sst_bytes += tablet.lsm.total_sst_bytes;
        global.flush_jobs.in_progress += tablet.jobs.flush.in_progress;
        global.flush_jobs.completed += tablet.jobs.flush.completed;
        global.flush_jobs.failed += tablet.jobs.flush.failed;
        global.flush_jobs.pending += tablet.jobs.flush.pending;
        global.compaction_jobs.in_progress += tablet.jobs.compaction.in_progress;
        global.compaction_jobs.completed += tablet.jobs.compaction.completed;
        global.compaction_jobs.failed += tablet.jobs.compaction.failed;
        global.compaction_jobs.pending += tablet.jobs.compaction.pending;
    }

    tablet_rows.sort_by(|a, b| {
        b.lsm
            .total_sst_bytes
            .cmp(&a.lsm.total_sst_bytes)
            .then(a.tablet_id.cmp(&b.tablet_id))
    });
    if top_n_tablets > 0 && tablet_rows.len() > top_n_tablets {
        global.truncated_tablets = tablet_rows.len() - top_n_tablets;
        tablet_rows.truncate(top_n_tablets);
    }
    global.reported_tablets = tablet_rows.len();

    EngineStatusSnapshot {
        ts_unix_ms,
        collection_duration_us: begin.elapsed().as_micros() as u64,
        global,
        tablets: tablet_rows,
    }
}

fn to_block_cache_status(usage: BlockCacheNamespaceUsage) -> BlockCacheStatus {
    BlockCacheStatus {
        data_usage_bytes: usage.data_usage_bytes,
        data_entries: usage.data_entries,
        index_usage_bytes: usage.index_usage_bytes,
        index_entries: usage.index_entries,
        bloom_usage_bytes: usage.bloom_usage_bytes,
        bloom_entries: usage.bloom_entries,
    }
}

fn to_row_cache_status(usage: RowCacheNamespaceUsage) -> RowCacheStatus {
    RowCacheStatus {
        usage_bytes: usage.usage_bytes,
        entries: usage.entries,
    }
}

fn collect_block_usage(cache_suite: &CacheSuite) -> HashMap<u128, BlockCacheNamespaceUsage> {
    let mut map = HashMap::new();
    if let Some(block_cache) = cache_suite.block_cache() {
        for row in block_cache.namespace_usage_snapshot() {
            map.insert(row.ns, row);
        }
    }
    map
}

fn collect_row_usage(cache_suite: &CacheSuite) -> HashMap<u128, RowCacheNamespaceUsage> {
    let mut map = HashMap::new();
    if let Some(row_cache) = cache_suite.row_cache() {
        for row in row_cache.namespace_usage_snapshot() {
            map.insert(row.ns, row);
        }
    }
    map
}

fn collect_cache_delta(cache_suite: &CacheSuite) -> CacheDeltaStatus {
    CacheDeltaStatus {
        block: cache_suite
            .block_cache()
            .map(|cache| cache.snapshot_and_reset_delta()),
        row: cache_suite
            .row_cache()
            .map(|cache| cache.snapshot_and_reset_delta()),
        reader: cache_suite
            .reader_cache()
            .map(|cache| cache.snapshot_and_reset_delta()),
    }
}

fn io_from_entries(entries: &[IoSnapshotEntry]) -> IoStatus {
    let mut by_source_map: BTreeMap<IoSource, IoSourceStatus> = BTreeMap::new();
    let mut status = IoStatus::default();

    for entry in entries {
        let source_status = by_source_map
            .entry(entry.source)
            .or_insert_with(|| IoSourceStatus {
                source: entry.source,
                ..IoSourceStatus::default()
            });
        source_status.absorb(entry.counters);
    }

    status.by_source = by_source_map.into_values().collect();
    for source in &status.by_source {
        let ops = source.read_ops + source.write_ops;
        let bytes = source.read_bytes + source.write_bytes;
        if source.source.is_background() {
            status.background_ops += ops;
            status.background_bytes += bytes;
        } else {
            status.foreground_ops += ops;
            status.foreground_bytes += bytes;
        }
    }
    status
}

fn diff_io_counters(current: IoCounterSnapshot, previous: IoCounterSnapshot) -> IoCounterSnapshot {
    IoCounterSnapshot {
        read_ops: current.read_ops.saturating_sub(previous.read_ops),
        read_bytes: current.read_bytes.saturating_sub(previous.read_bytes),
        write_ops: current.write_ops.saturating_sub(previous.write_ops),
        write_bytes: current.write_bytes.saturating_sub(previous.write_bytes),
        fsync_ops: current.fsync_ops.saturating_sub(previous.fsync_ops),
    }
}

fn io_counters_is_zero(counters: IoCounterSnapshot) -> bool {
    counters.read_ops == 0
        && counters.read_bytes == 0
        && counters.write_ops == 0
        && counters.write_bytes == 0
        && counters.fsync_ops == 0
}

pub fn log_snapshot(snapshot: &EngineStatusSnapshot) {
    let block_delta = snapshot.global.cache_delta.block.unwrap_or_default();
    let reader_delta = snapshot.global.cache_delta.reader.unwrap_or_default();
    let row_delta = snapshot.global.cache_delta.row.unwrap_or_default();
    let fg_read_ops: u64 = snapshot
        .global
        .io
        .by_source
        .iter()
        .filter(|s| !s.source.is_background())
        .map(|s| s.read_ops)
        .sum();
    let fg_read_bytes: u64 = snapshot
        .global
        .io
        .by_source
        .iter()
        .filter(|s| !s.source.is_background())
        .map(|s| s.read_bytes)
        .sum();
    let bg_write_ops: u64 = snapshot
        .global
        .io
        .by_source
        .iter()
        .filter(|s| s.source.is_background())
        .map(|s| s.write_ops)
        .sum();
    let bg_write_bytes: u64 = snapshot
        .global
        .io
        .by_source
        .iter()
        .filter(|s| s.source.is_background())
        .map(|s| s.write_bytes)
        .sum();

    log_info!(
        "[engine-status] ts={} collect_us={} tablets={} reported={} cache_delta:block(h/m/i/e/x/r)={}/{}/{}/{}/{}/{} reader(h/m/i/e/x)={}/{}/{}/{}/{} row(h/m/i/e/x/r)={}/{}/{}/{}/{}/{} io:fg_read={}/{} bg_write={}/{}",
        snapshot.ts_unix_ms,
        snapshot.collection_duration_us,
        snapshot.global.tablet_count,
        snapshot.global.reported_tablets,
        block_delta.hits,
        block_delta.misses,
        block_delta.inserts,
        block_delta.evictions,
        block_delta.invalidations,
        block_delta.insert_rejects,
        reader_delta.hits,
        reader_delta.misses,
        reader_delta.inserts,
        reader_delta.evictions,
        reader_delta.invalidations,
        row_delta.hits,
        row_delta.misses,
        row_delta.inserts,
        row_delta.evictions,
        row_delta.invalidations,
        row_delta.insert_rejects,
        fg_read_ops,
        fg_read_bytes,
        bg_write_ops,
        bg_write_bytes
    );

    for tablet in &snapshot.tablets {
        log_info!(
            "[engine-status][tablet={}] cache:block={} row={} lsm:mem={} frozen={}/{} waiters={} flush:active={}/{} L0={} total_sst={} flush:run={} pend={} compact:run={} pend={} bp={:?}",
            tablet.tablet_id.dir_name(),
            tablet
                .cache
                .block
                .as_ref()
                .map_or(0, BlockCacheStatus::total_usage_bytes),
            tablet.cache.row.as_ref().map_or(0, |row| row.usage_bytes),
            tablet.lsm.active_memtable_size,
            tablet.lsm.frozen_memtable_count,
            tablet.lsm.frozen_memtable_bytes,
            tablet.lsm.frozen_waiters,
            tablet.lsm.flush_workers_active,
            tablet.lsm.flush_workers_limit,
            tablet.lsm.l0_sst_count,
            tablet.lsm.total_sst_bytes,
            tablet.jobs.flush.in_progress,
            tablet.jobs.flush.pending,
            tablet.jobs.compaction.in_progress,
            tablet.jobs.compaction.pending,
            tablet.backpressure.state
        );
    }

    if snapshot.collection_duration_us > 500_000 {
        log_warn!(
            "[engine-status] status collection slow: collect_us={}",
            snapshot.collection_duration_us
        );
    }
}

fn push_metric(
    rows: &mut Vec<EngineStatusMetricRow>,
    scope: &str,
    tablet: &str,
    metric: &str,
    value: impl ToString,
) {
    rows.push(EngineStatusMetricRow {
        scope: scope.to_string(),
        tablet: tablet.to_string(),
        metric: metric.to_string(),
        value: value.to_string(),
    });
}

pub fn snapshot_to_metric_rows(snapshot: &EngineStatusSnapshot) -> Vec<EngineStatusMetricRow> {
    let mut rows = Vec::new();
    let block_delta = snapshot.global.cache_delta.block.unwrap_or_default();
    let reader_delta = snapshot.global.cache_delta.reader.unwrap_or_default();
    let row_delta = snapshot.global.cache_delta.row.unwrap_or_default();
    push_metric(&mut rows, "global", "", "ts_unix_ms", snapshot.ts_unix_ms);
    push_metric(
        &mut rows,
        "global",
        "",
        "collection_duration_us",
        snapshot.collection_duration_us,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "tablet_count",
        snapshot.global.tablet_count,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "reported_tablets",
        snapshot.global.reported_tablets,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "total_sst_bytes",
        snapshot.global.total_sst_bytes,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "cache.block.delta.hits",
        block_delta.hits,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "cache.block.delta.misses",
        block_delta.misses,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "cache.block.delta.inserts",
        block_delta.inserts,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "cache.block.delta.evictions",
        block_delta.evictions,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "cache.block.delta.invalidations",
        block_delta.invalidations,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "cache.block.delta.insert_rejects",
        block_delta.insert_rejects,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "cache.reader.delta.hits",
        reader_delta.hits,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "cache.reader.delta.misses",
        reader_delta.misses,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "cache.reader.delta.inserts",
        reader_delta.inserts,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "cache.reader.delta.evictions",
        reader_delta.evictions,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "cache.reader.delta.invalidations",
        reader_delta.invalidations,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "cache.row.delta.hits",
        row_delta.hits,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "cache.row.delta.misses",
        row_delta.misses,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "cache.row.delta.inserts",
        row_delta.inserts,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "cache.row.delta.evictions",
        row_delta.evictions,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "cache.row.delta.invalidations",
        row_delta.invalidations,
    );
    push_metric(
        &mut rows,
        "global",
        "",
        "cache.row.delta.insert_rejects",
        row_delta.insert_rejects,
    );

    for source in &snapshot.global.io.by_source {
        let prefix = format!("io.{:?}", source.source);
        push_metric(
            &mut rows,
            "global",
            "",
            &format!("{prefix}.read_ops"),
            source.read_ops,
        );
        push_metric(
            &mut rows,
            "global",
            "",
            &format!("{prefix}.write_ops"),
            source.write_ops,
        );
        push_metric(
            &mut rows,
            "global",
            "",
            &format!("{prefix}.write_bytes"),
            source.write_bytes,
        );
        push_metric(
            &mut rows,
            "global",
            "",
            &format!("{prefix}.fsync_ops"),
            source.fsync_ops,
        );
    }

    for tablet in &snapshot.tablets {
        let name = tablet.tablet_id.dir_name();
        push_metric(&mut rows, "tablet", &name, "tablet_ns", tablet.tablet_ns);
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "lsm.active_memtable_size",
            tablet.lsm.active_memtable_size,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "lsm.frozen_memtable_count",
            tablet.lsm.frozen_memtable_count,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "lsm.frozen_waiters",
            tablet.lsm.frozen_waiters,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "lsm.flush_workers_active",
            tablet.lsm.flush_workers_active,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "lsm.flush_workers_limit",
            tablet.lsm.flush_workers_limit,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "lsm.l0_sst_count",
            tablet.lsm.l0_sst_count,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "lsm.total_sst_bytes",
            tablet.lsm.total_sst_bytes,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "backpressure.state",
            format!("{:?}", tablet.backpressure.state),
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "backpressure.slowdown_events",
            tablet.backpressure.slowdown_events,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "backpressure.stall_events",
            tablet.backpressure.stall_events,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "backpressure.frozen_slowdown_events",
            tablet.backpressure.frozen_slowdown_events,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "backpressure.frozen_cap_stall_events",
            tablet.backpressure.frozen_cap_stall_events,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "backpressure.l0_stop_stall_events",
            tablet.backpressure.l0_stop_stall_events,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "backpressure.mem_pressure_soft_events",
            tablet.backpressure.mem_pressure_soft_events,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "backpressure.mem_pressure_hard_events",
            tablet.backpressure.mem_pressure_hard_events,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "backpressure.mem_pressure_soft_residency_ms",
            tablet.backpressure.mem_pressure_soft_residency_ms,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "backpressure.mem_pressure_hard_residency_ms",
            tablet.backpressure.mem_pressure_hard_residency_ms,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "backpressure.writer_wait_events",
            tablet.backpressure.writer_wait_events,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "backpressure.writer_wake_events",
            tablet.backpressure.writer_wake_events,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "backpressure.writer_wait_timeout_events",
            tablet.backpressure.writer_wait_timeout_events,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "jobs.flush.in_progress",
            tablet.jobs.flush.in_progress,
        );
        push_metric(
            &mut rows,
            "tablet",
            &name,
            "jobs.compaction.in_progress",
            tablet.jobs.compaction.in_progress,
        );
        if let Some(block) = &tablet.cache.block {
            push_metric(
                &mut rows,
                "tablet",
                &name,
                "cache.block.total_usage_bytes",
                block.total_usage_bytes(),
            );
        }
        if let Some(row) = &tablet.cache.row {
            push_metric(
                &mut rows,
                "tablet",
                &name,
                "cache.row.usage_bytes",
                row.usage_bytes,
            );
        }
    }
    rows
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{reset_io_accounting, IoCounterSnapshot};
    use crate::lsn::new_lsn_provider;
    use crate::tablet::{CacheSuiteConfig, LsmConfig, TabletEngine};
    use std::path::Path;
    use tempfile::TempDir;
    use tokio::time::timeout;

    fn open_test_system_tablet(dir: &Path) -> Arc<TabletEngine> {
        Arc::new(TabletEngine::open(LsmConfig::new(dir)).unwrap())
    }

    fn metric_value(
        rows: &[EngineStatusMetricRow],
        scope: &str,
        tablet: &str,
        metric: &str,
    ) -> Option<String> {
        rows.iter()
            .find(|row| row.scope == scope && row.tablet == tablet && row.metric == metric)
            .map(|row| row.value.clone())
    }

    #[test]
    fn test_snapshot_to_metric_rows_and_log_snapshot_cover_metric_surface() {
        let dir = TempDir::new().unwrap();
        let engine = open_test_system_tablet(dir.path());
        let mut lsm = engine.stats();
        lsm.active_memtable_size = 11;
        lsm.frozen_memtable_count = 2;
        lsm.frozen_waiters = 4;
        lsm.flush_workers_active = 5;
        lsm.flush_workers_limit = 6;
        lsm.l0_sst_count = 7;
        lsm.total_sst_bytes = 99;

        let tablet_id = TabletId::Table { table_id: 42 };
        let tablet_name = tablet_id.dir_name();
        let snapshot = EngineStatusSnapshot {
            ts_unix_ms: 123,
            collection_duration_us: 600_001,
            global: GlobalStatus {
                tablet_count: 1,
                reported_tablets: 1,
                total_sst_bytes: 999,
                cache_delta: CacheDeltaStatus {
                    block: Some(BlockCacheStats {
                        hits: 1,
                        misses: 2,
                        inserts: 3,
                        evictions: 4,
                        invalidations: 5,
                        insert_rejects: 6,
                    }),
                    row: Some(RowCacheStats {
                        hits: 7,
                        misses: 8,
                        inserts: 9,
                        evictions: 10,
                        invalidations: 11,
                        insert_rejects: 12,
                    }),
                    reader: Some(ReaderCacheStats {
                        hits: 13,
                        misses: 14,
                        inserts: 15,
                        evictions: 16,
                        invalidations: 17,
                    }),
                },
                io: IoStatus {
                    by_source: vec![
                        IoSourceStatus {
                            source: IoSource::ForegroundPointRead,
                            read_ops: 18,
                            read_bytes: 19,
                            write_ops: 20,
                            write_bytes: 21,
                            fsync_ops: 22,
                        },
                        IoSourceStatus {
                            source: IoSource::Flush,
                            read_ops: 23,
                            read_bytes: 24,
                            write_ops: 25,
                            write_bytes: 26,
                            fsync_ops: 27,
                        },
                    ],
                    foreground_ops: 38,
                    foreground_bytes: 40,
                    background_ops: 48,
                    background_bytes: 50,
                },
                ..GlobalStatus::default()
            },
            tablets: vec![TabletStatus {
                tablet_id,
                tablet_ns: 456,
                cache: CacheStatus {
                    block: Some(BlockCacheStatus {
                        data_usage_bytes: 30,
                        data_entries: 1,
                        index_usage_bytes: 31,
                        index_entries: 2,
                        bloom_usage_bytes: 32,
                        bloom_entries: 3,
                    }),
                    row: Some(RowCacheStatus {
                        usage_bytes: 33,
                        entries: 4,
                    }),
                },
                io: IoStatus::default(),
                lsm,
                jobs: JobStatus {
                    flush: JobCounters {
                        in_progress: 34,
                        ..JobCounters::default()
                    },
                    compaction: JobCounters {
                        in_progress: 35,
                        ..JobCounters::default()
                    },
                },
                backpressure: BackpressureStatus {
                    state: BackpressureState::Slowdown,
                    slowdown_events: 36,
                    stall_events: 37,
                    frozen_slowdown_events: 38,
                    frozen_cap_stall_events: 39,
                    l0_stop_stall_events: 40,
                    mem_pressure_soft_events: 41,
                    mem_pressure_hard_events: 42,
                    mem_pressure_soft_residency_ms: 43,
                    mem_pressure_hard_residency_ms: 44,
                    writer_wait_events: 45,
                    writer_wake_events: 46,
                    writer_wait_timeout_events: 47,
                },
            }],
        };

        log_snapshot(&snapshot);
        let rows = snapshot_to_metric_rows(&snapshot);

        assert_eq!(
            metric_value(&rows, "global", "", "ts_unix_ms"),
            Some("123".to_string())
        );
        assert_eq!(
            metric_value(&rows, "global", "", "cache.block.delta.insert_rejects"),
            Some("6".to_string())
        );
        assert_eq!(
            metric_value(&rows, "global", "", "cache.reader.delta.invalidations"),
            Some("17".to_string())
        );
        assert_eq!(
            metric_value(&rows, "global", "", "cache.row.delta.insert_rejects"),
            Some("12".to_string())
        );
        assert_eq!(
            metric_value(&rows, "global", "", "io.ForegroundPointRead.read_ops"),
            Some("18".to_string())
        );
        assert_eq!(
            metric_value(&rows, "global", "", "io.Flush.write_bytes"),
            Some("26".to_string())
        );
        assert_eq!(
            metric_value(&rows, "tablet", &tablet_name, "tablet_ns"),
            Some("456".to_string())
        );
        assert_eq!(
            metric_value(&rows, "tablet", &tablet_name, "lsm.total_sst_bytes"),
            Some("99".to_string())
        );
        assert_eq!(
            metric_value(&rows, "tablet", &tablet_name, "backpressure.state"),
            Some("Slowdown".to_string())
        );
        assert_eq!(
            metric_value(
                &rows,
                "tablet",
                &tablet_name,
                "backpressure.writer_wait_timeout_events"
            ),
            Some("47".to_string())
        );
        assert_eq!(
            metric_value(&rows, "tablet", &tablet_name, "jobs.flush.in_progress"),
            Some("34".to_string())
        );
        assert_eq!(
            metric_value(&rows, "tablet", &tablet_name, "jobs.compaction.in_progress"),
            Some("35".to_string())
        );
        assert_eq!(
            metric_value(
                &rows,
                "tablet",
                &tablet_name,
                "cache.block.total_usage_bytes"
            ),
            Some("93".to_string())
        );
        assert_eq!(
            metric_value(&rows, "tablet", &tablet_name, "cache.row.usage_bytes"),
            Some("33".to_string())
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_reporter_lifecycle_collect_methods_publish_snapshot() {
        reset_io_accounting();

        let dir = TempDir::new().unwrap();
        let manager = Arc::new(
            TabletManager::new(
                dir.path(),
                new_lsn_provider(),
                open_test_system_tablet(dir.path()),
            )
            .unwrap(),
        );
        let cache_suite = Arc::new(CacheSuite::new(CacheSuiteConfig::default()));
        manager.bind_cache_suite(Arc::clone(&cache_suite));

        let reporter =
            EngineStatusReporter::new(Arc::clone(&manager), Arc::clone(&cache_suite), 1, 1);
        assert_eq!(reporter.interval(), Duration::from_secs(1));
        assert!(reporter.latest_snapshot().is_none());

        reporter.start(&tokio::runtime::Handle::current());
        reporter.start(&tokio::runtime::Handle::current());

        timeout(Duration::from_secs(5), async {
            while reporter.latest_snapshot().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reporter should publish an initial snapshot");

        let loud = reporter.collect_once();
        let quiet = reporter.collect_once_quiet();
        let latest = reporter
            .latest_snapshot()
            .expect("latest snapshot should exist");
        assert!(loud.global.tablet_count >= 1);
        assert!(quiet.global.reported_tablets >= 1);
        assert_eq!(
            latest.global.reported_tablets,
            quiet.global.reported_tablets
        );

        let delta = reporter.inner.snapshot_io_delta();
        assert!(delta.iter().all(
            |entry| io_counters_is_zero(entry.counters) || !io_counters_is_zero(entry.counters)
        ));

        let current = IoCounterSnapshot {
            read_ops: 9,
            read_bytes: 8,
            write_ops: 7,
            write_bytes: 6,
            fsync_ops: 5,
        };
        let previous = IoCounterSnapshot {
            read_ops: 4,
            read_bytes: 3,
            write_ops: 2,
            write_bytes: 1,
            fsync_ops: 1,
        };
        let diff = diff_io_counters(current, previous);
        assert_eq!(
            diff,
            IoCounterSnapshot {
                read_ops: 5,
                read_bytes: 5,
                write_ops: 5,
                write_bytes: 5,
                fsync_ops: 4,
            }
        );
        assert!(io_counters_is_zero(IoCounterSnapshot::default()));
        assert!(!io_counters_is_zero(diff));
        assert!(now_ms() > 0);

        reporter.stop();
    }
}
