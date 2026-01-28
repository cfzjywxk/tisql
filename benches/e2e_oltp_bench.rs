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

//! End-to-end OLTP benchmark simulating sysbench workloads.
//!
//! This benchmark tests the full MVCC transaction path (TxnService -> Storage)
//! with three different memtable implementations:
//! - CrossbeamMemTableEngine (default, lock-free with epoch-based GC)
//! - ArenaMemTableEngine (arena-based skip list)
//! - BTreeMemTableEngine (BTreeMap with RwLock, baseline)
//!
//! The benchmark simulates sysbench OLTP_RW workload:
//! - Point selects (70%)
//! - Non-indexed updates (10%)
//! - Deletes (10%)
//! - Inserts (10%)
//!
//! Run with: cargo bench --bench e2e_oltp_bench

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use tisql::storage::mvcc::is_tombstone;
use tisql::storage::{
    ArenaMemTableEngine, BTreeMemTableEngine, CrossbeamMemTableEngine, MvccKey, StorageEngine,
    WriteBatch,
};

// ============================================================================
// Benchmark Configuration
// ============================================================================

/// Number of rows to pre-populate for benchmarks
const TABLE_SIZE: usize = 100_000;
/// Size of each row's value (simulating typical row data)
const VALUE_SIZE: usize = 120;
/// Number of operations per benchmark iteration
const OPS_PER_ITER: usize = 10_000;

// OLTP_RW workload distribution (must sum to 100)
const PCT_POINT_SELECT: usize = 70;
const PCT_UPDATE: usize = 10;
const PCT_DELETE: usize = 10;
// PCT_INSERT is implicitly 10% (100 - 70 - 10 - 10)

/// Thread counts to benchmark
const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8];

// ============================================================================
// Helper Functions
// ============================================================================

/// Generate a table key (simulating TiDB format: t{table_id}_r{row_id})
fn make_key(table_id: u64, row_id: u64) -> Vec<u8> {
    format!("t{table_id:04}_r{row_id:012}").into_bytes()
}

/// Generate a value of specified size
fn make_value(row_id: u64, size: usize) -> Vec<u8> {
    let prefix = format!("row_{row_id:012}_");
    let mut value = prefix.into_bytes();
    while value.len() < size {
        value.push(b'x');
    }
    value.truncate(size);
    value
}

/// Point lookup using scan with MvccKey encoding (storage layer API)
fn get_at_via_scan<E: StorageEngine>(engine: &E, key: &[u8], ts: u64) -> Option<Vec<u8>> {
    let start = MvccKey::encode(key, ts);
    let end = MvccKey::encode(key, 0)
        .next_key()
        .unwrap_or_else(MvccKey::unbounded);

    let results = engine.scan(start..end).ok()?;

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

/// Populate the storage engine with initial data
fn populate<E: StorageEngine>(engine: &E, table_id: u64, num_rows: usize, ts_counter: &AtomicU64) {
    // Use batches of 1000 rows for efficiency
    const BATCH_SIZE: usize = 1000;

    for batch_start in (0..num_rows).step_by(BATCH_SIZE) {
        let mut batch = WriteBatch::new();
        let batch_end = (batch_start + BATCH_SIZE).min(num_rows);

        for row_id in batch_start..batch_end {
            let key = make_key(table_id, row_id as u64);
            let value = make_value(row_id as u64, VALUE_SIZE);
            batch.put(key, value);
        }

        let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
        batch.set_commit_ts(ts);
        engine.write_batch(batch).unwrap();
    }
}

// ============================================================================
// OLTP Workload Runner
// ============================================================================

/// Statistics for OLTP operations
#[derive(Default)]
struct OltpStats {
    point_selects: AtomicUsize,
    updates: AtomicUsize,
    deletes: AtomicUsize,
    inserts: AtomicUsize,
    errors: AtomicUsize,
    total_latency_ns: AtomicU64,
}

impl OltpStats {
    fn new() -> Self {
        Self::default()
    }

    fn total_ops(&self) -> usize {
        self.point_selects.load(Ordering::Relaxed)
            + self.updates.load(Ordering::Relaxed)
            + self.deletes.load(Ordering::Relaxed)
            + self.inserts.load(Ordering::Relaxed)
    }
}

/// Run OLTP workload on a storage engine
fn run_oltp_workload<E: StorageEngine>(
    engine: &E,
    table_id: u64,
    num_ops: usize,
    ts_counter: &AtomicU64,
    next_row_id: &AtomicU64,
    stats: &OltpStats,
    thread_id: usize,
) {
    // Simple pseudo-random number generator (deterministic per thread)
    let mut state: u64 = (thread_id as u64)
        .wrapping_mul(0x9E3779B97F4A7C15)
        .wrapping_add(1);
    let mut rng = || {
        state = state
            .wrapping_mul(0x5851F42D4C957F2D)
            .wrapping_add(0x14057B7EF767814F);
        state
    };

    for _ in 0..num_ops {
        let op_type = (rng() % 100) as usize;
        let start = Instant::now();

        if op_type < PCT_POINT_SELECT {
            // Point select
            let row_id = rng() % TABLE_SIZE as u64;
            let key = make_key(table_id, row_id);
            let ts = ts_counter.load(Ordering::Relaxed);
            let result = get_at_via_scan(engine, &key, ts);
            let _ = black_box(result);
            stats.point_selects.fetch_add(1, Ordering::Relaxed);
        } else if op_type < PCT_POINT_SELECT + PCT_UPDATE {
            // Update existing row
            let row_id = rng() % TABLE_SIZE as u64;
            let key = make_key(table_id, row_id);
            let value = make_value(row_id, VALUE_SIZE);

            let mut batch = WriteBatch::new();
            batch.put(key, value);
            let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
            batch.set_commit_ts(ts);

            if engine.write_batch(batch).is_ok() {
                stats.updates.fetch_add(1, Ordering::Relaxed);
            } else {
                stats.errors.fetch_add(1, Ordering::Relaxed);
            }
        } else if op_type < PCT_POINT_SELECT + PCT_UPDATE + PCT_DELETE {
            // Delete existing row (write tombstone)
            let row_id = rng() % TABLE_SIZE as u64;
            let key = make_key(table_id, row_id);

            let mut batch = WriteBatch::new();
            batch.delete(key);
            let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
            batch.set_commit_ts(ts);

            if engine.write_batch(batch).is_ok() {
                stats.deletes.fetch_add(1, Ordering::Relaxed);
            } else {
                stats.errors.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            // Insert new row
            let row_id = next_row_id.fetch_add(1, Ordering::SeqCst);
            let key = make_key(table_id, row_id);
            let value = make_value(row_id, VALUE_SIZE);

            let mut batch = WriteBatch::new();
            batch.put(key, value);
            let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
            batch.set_commit_ts(ts);

            if engine.write_batch(batch).is_ok() {
                stats.inserts.fetch_add(1, Ordering::Relaxed);
            } else {
                stats.errors.fetch_add(1, Ordering::Relaxed);
            }
        }

        let elapsed = start.elapsed().as_nanos() as u64;
        stats.total_latency_ns.fetch_add(elapsed, Ordering::Relaxed);
    }
}

// ============================================================================
// Benchmark Functions
// ============================================================================

fn bench_oltp_single_thread<E: StorageEngine + Default>(name: &str, c: &mut Criterion) {
    let mut group = c.benchmark_group(format!("oltp_single/{name}"));
    group.throughput(Throughput::Elements(OPS_PER_ITER as u64));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    let engine = E::default();
    let ts_counter = AtomicU64::new(1);
    let next_row_id = AtomicU64::new(TABLE_SIZE as u64);

    // Populate initial data
    populate(&engine, 1, TABLE_SIZE, &ts_counter);

    group.bench_function("oltp_rw", |b| {
        b.iter(|| {
            let stats = OltpStats::new();
            run_oltp_workload(
                &engine,
                1,
                OPS_PER_ITER,
                &ts_counter,
                &next_row_id,
                &stats,
                0,
            );
            black_box(stats.total_ops())
        })
    });

    group.finish();
}

fn bench_oltp_multi_thread<E: StorageEngine + Default + 'static>(name: &str, c: &mut Criterion) {
    let mut group = c.benchmark_group(format!("oltp_multi/{name}"));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    for &num_threads in THREAD_COUNTS {
        let ops_per_thread = OPS_PER_ITER / num_threads;
        group.throughput(Throughput::Elements((ops_per_thread * num_threads) as u64));

        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            &num_threads,
            |b, &num_threads| {
                // Create fresh engine for each benchmark
                let engine = Arc::new(E::default());
                let ts_counter = Arc::new(AtomicU64::new(1));
                let next_row_id = Arc::new(AtomicU64::new(TABLE_SIZE as u64));

                // Populate initial data
                populate(engine.as_ref(), 1, TABLE_SIZE, &ts_counter);

                b.iter(|| {
                    let stats = Arc::new(OltpStats::new());
                    let mut handles = Vec::with_capacity(num_threads);

                    for tid in 0..num_threads {
                        let engine = Arc::clone(&engine);
                        let ts_counter = Arc::clone(&ts_counter);
                        let next_row_id = Arc::clone(&next_row_id);
                        let stats = Arc::clone(&stats);

                        handles.push(std::thread::spawn(move || {
                            run_oltp_workload(
                                engine.as_ref(),
                                1,
                                ops_per_thread,
                                &ts_counter,
                                &next_row_id,
                                &stats,
                                tid,
                            );
                        }));
                    }

                    for h in handles {
                        h.join().unwrap();
                    }

                    black_box(stats.total_ops())
                })
            },
        );
    }

    group.finish();
}

// ============================================================================
// Criterion Groups
// ============================================================================

fn bench_crossbeam_single(c: &mut Criterion) {
    bench_oltp_single_thread::<CrossbeamMemTableEngine>("crossbeam", c);
}

fn bench_arena_single(c: &mut Criterion) {
    bench_oltp_single_thread::<ArenaMemTableEngine>("arena", c);
}

fn bench_btree_single(c: &mut Criterion) {
    bench_oltp_single_thread::<BTreeMemTableEngine>("btree", c);
}

fn bench_crossbeam_multi(c: &mut Criterion) {
    bench_oltp_multi_thread::<CrossbeamMemTableEngine>("crossbeam", c);
}

fn bench_arena_multi(c: &mut Criterion) {
    bench_oltp_multi_thread::<ArenaMemTableEngine>("arena", c);
}

fn bench_btree_multi(c: &mut Criterion) {
    bench_oltp_multi_thread::<BTreeMemTableEngine>("btree", c);
}

criterion_group!(
    single_thread,
    bench_crossbeam_single,
    bench_arena_single,
    bench_btree_single,
);

criterion_group!(
    multi_thread,
    bench_crossbeam_multi,
    bench_arena_multi,
    bench_btree_multi,
);

criterion_main!(single_thread, multi_thread);
