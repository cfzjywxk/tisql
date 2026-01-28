//! Comparison benchmarks for memtable implementations.
//!
//! Run with: cargo bench --bench memtable_impl_comparison
//!
//! This benchmark compares three MVCC memtable implementations:
//!
//! 1. **ArenaMemTableEngine** - Arena-based skip list
//! 2. **CrossbeamMemTableEngine** - Crossbeam epoch-based skip list
//! 3. **BTreeMemTableEngine** - BTreeMap with RwLock
//!
//! All benchmarks test the actual StorageEngine interface with MVCC semantics.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use tisql::storage::mvcc::is_tombstone;
use tisql::storage::{
    ArenaMemTableEngine, BTreeMemTableEngine, CrossbeamMemTableEngine, MvccKey, StorageEngine,
    WriteBatch,
};

// =============================================================================
// Test Parameters
// =============================================================================

/// Number of entries to pre-populate for read-heavy workloads
const PRE_POPULATE: usize = 50_000;

/// Operations per thread in concurrent benchmarks
const OPS_PER_THREAD: usize = 20_000;

// =============================================================================
// Helper Functions
// =============================================================================

/// Global timestamp counter for unique commit timestamps
static GLOBAL_TS: AtomicU64 = AtomicU64::new(1);

fn next_ts() -> u64 {
    GLOBAL_TS.fetch_add(1, Ordering::Relaxed)
}

fn reset_ts() {
    GLOBAL_TS.store(1, Ordering::Relaxed);
}

/// Pre-populate a storage engine with test data
fn populate_engine<E: StorageEngine>(engine: &E, count: usize) {
    for i in 0..count {
        let key = format!("key{i:08}");
        let value = format!("value{i:08}");
        let mut batch = WriteBatch::new();
        batch.set_commit_ts(next_ts());
        batch.put(key.as_bytes().to_vec(), value.as_bytes().to_vec());
        engine.write_batch(batch).unwrap();
    }
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

// =============================================================================
// Benchmark Groups
// =============================================================================

fn bench_write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_impl/write_only");

    for num_threads in [1, 2, 4, 8] {
        let total_ops = num_threads * OPS_PER_THREAD;
        group.throughput(Throughput::Elements(total_ops as u64));

        // Arena-based memtable
        group.bench_with_input(
            BenchmarkId::new("arena_memtable", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter_custom(|iters| {
                    let mut total_duration = std::time::Duration::ZERO;

                    for _ in 0..iters {
                        reset_ts();
                        let engine = ArenaMemTableEngine::new();
                        let barrier = Arc::new(Barrier::new(num_threads));

                        let start = std::time::Instant::now();

                        thread::scope(|s| {
                            for tid in 0..num_threads {
                                let engine = &engine;
                                let barrier = Arc::clone(&barrier);

                                s.spawn(move || {
                                    barrier.wait();
                                    for i in 0..OPS_PER_THREAD {
                                        let key = format!("key_{tid}_{i}");
                                        let value = format!("value_{tid}_{i}");
                                        let ts = next_ts();
                                        engine.put_at(key.as_bytes(), value.as_bytes(), ts);
                                    }
                                });
                            }
                        });

                        total_duration += start.elapsed();
                    }

                    total_duration
                });
            },
        );

        // Crossbeam-based memtable
        group.bench_with_input(
            BenchmarkId::new("crossbeam_memtable", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter_custom(|iters| {
                    let mut total_duration = std::time::Duration::ZERO;

                    for _ in 0..iters {
                        reset_ts();
                        let engine = CrossbeamMemTableEngine::new();
                        let barrier = Arc::new(Barrier::new(num_threads));

                        let start = std::time::Instant::now();

                        thread::scope(|s| {
                            for tid in 0..num_threads {
                                let engine = &engine;
                                let barrier = Arc::clone(&barrier);

                                s.spawn(move || {
                                    barrier.wait();
                                    for i in 0..OPS_PER_THREAD {
                                        let key = format!("key_{tid}_{i}");
                                        let value = format!("value_{tid}_{i}");
                                        let ts = next_ts();
                                        engine.put_at(key.as_bytes(), value.as_bytes(), ts);
                                    }
                                });
                            }
                        });

                        total_duration += start.elapsed();
                    }

                    total_duration
                });
            },
        );

        // BTreeMap-based memtable
        group.bench_with_input(
            BenchmarkId::new("btree_memtable", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter_custom(|iters| {
                    let mut total_duration = std::time::Duration::ZERO;

                    for _ in 0..iters {
                        reset_ts();
                        let engine = BTreeMemTableEngine::new();
                        let barrier = Arc::new(Barrier::new(num_threads));

                        let start = std::time::Instant::now();

                        thread::scope(|s| {
                            for tid in 0..num_threads {
                                let engine = &engine;
                                let barrier = Arc::clone(&barrier);

                                s.spawn(move || {
                                    barrier.wait();
                                    for i in 0..OPS_PER_THREAD {
                                        let key = format!("key_{tid}_{i}");
                                        let value = format!("value_{tid}_{i}");
                                        let ts = next_ts();
                                        engine.put_at(key.as_bytes(), value.as_bytes(), ts);
                                    }
                                });
                            }
                        });

                        total_duration += start.elapsed();
                    }

                    total_duration
                });
            },
        );
    }

    group.finish();
}

fn bench_read_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_impl/read_only");

    for num_threads in [1, 2, 4, 8] {
        let total_ops = num_threads * OPS_PER_THREAD;
        group.throughput(Throughput::Elements(total_ops as u64));

        // Arena-based memtable
        group.bench_with_input(
            BenchmarkId::new("arena_memtable", num_threads),
            &num_threads,
            |b, &num_threads| {
                reset_ts();
                let engine = ArenaMemTableEngine::new();
                populate_engine(&engine, PRE_POPULATE);
                let read_ts = next_ts(); // Read timestamp after all writes

                b.iter_custom(|iters| {
                    let mut total_duration = std::time::Duration::ZERO;

                    for _ in 0..iters {
                        let barrier = Arc::new(Barrier::new(num_threads));
                        let start = std::time::Instant::now();

                        thread::scope(|s| {
                            for _ in 0..num_threads {
                                let engine = &engine;
                                let barrier = Arc::clone(&barrier);

                                s.spawn(move || {
                                    barrier.wait();
                                    for i in 0..OPS_PER_THREAD {
                                        let key = format!("key{:08}", i % PRE_POPULATE);
                                        black_box(get_at_via_scan(engine, key.as_bytes(), read_ts));
                                    }
                                });
                            }
                        });

                        total_duration += start.elapsed();
                    }

                    total_duration
                });
            },
        );

        // Crossbeam-based memtable
        group.bench_with_input(
            BenchmarkId::new("crossbeam_memtable", num_threads),
            &num_threads,
            |b, &num_threads| {
                reset_ts();
                let engine = CrossbeamMemTableEngine::new();
                populate_engine(&engine, PRE_POPULATE);
                let read_ts = next_ts();

                b.iter_custom(|iters| {
                    let mut total_duration = std::time::Duration::ZERO;

                    for _ in 0..iters {
                        let barrier = Arc::new(Barrier::new(num_threads));
                        let start = std::time::Instant::now();

                        thread::scope(|s| {
                            for _ in 0..num_threads {
                                let engine = &engine;
                                let barrier = Arc::clone(&barrier);

                                s.spawn(move || {
                                    barrier.wait();
                                    for i in 0..OPS_PER_THREAD {
                                        let key = format!("key{:08}", i % PRE_POPULATE);
                                        black_box(get_at_via_scan(engine, key.as_bytes(), read_ts));
                                    }
                                });
                            }
                        });

                        total_duration += start.elapsed();
                    }

                    total_duration
                });
            },
        );

        // BTreeMap-based memtable
        group.bench_with_input(
            BenchmarkId::new("btree_memtable", num_threads),
            &num_threads,
            |b, &num_threads| {
                reset_ts();
                let engine = BTreeMemTableEngine::new();
                populate_engine(&engine, PRE_POPULATE);
                let read_ts = next_ts();

                b.iter_custom(|iters| {
                    let mut total_duration = std::time::Duration::ZERO;

                    for _ in 0..iters {
                        let barrier = Arc::new(Barrier::new(num_threads));
                        let start = std::time::Instant::now();

                        thread::scope(|s| {
                            for _ in 0..num_threads {
                                let engine = &engine;
                                let barrier = Arc::clone(&barrier);

                                s.spawn(move || {
                                    barrier.wait();
                                    for i in 0..OPS_PER_THREAD {
                                        let key = format!("key{:08}", i % PRE_POPULATE);
                                        black_box(get_at_via_scan(engine, key.as_bytes(), read_ts));
                                    }
                                });
                            }
                        });

                        total_duration += start.elapsed();
                    }

                    total_duration
                });
            },
        );
    }

    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_impl/mixed");

    let num_threads = 8;

    for (read_pct, write_pct) in [(50, 50), (90, 10), (10, 90)] {
        let label = format!("r{read_pct}w{write_pct}");
        let total_ops = num_threads * OPS_PER_THREAD;
        group.throughput(Throughput::Elements(total_ops as u64));

        // Arena-based memtable
        group.bench_with_input(
            BenchmarkId::new(format!("arena_memtable/{label}"), num_threads),
            &(read_pct, write_pct),
            |b, &(read_pct, _write_pct)| {
                reset_ts();
                let engine = ArenaMemTableEngine::new();
                populate_engine(&engine, PRE_POPULATE);
                let read_ts = next_ts();

                b.iter_custom(|iters| {
                    let mut total_duration = std::time::Duration::ZERO;

                    for _ in 0..iters {
                        let barrier = Arc::new(Barrier::new(num_threads));
                        let start = std::time::Instant::now();

                        thread::scope(|s| {
                            for tid in 0..num_threads {
                                let engine = &engine;
                                let barrier = Arc::clone(&barrier);

                                s.spawn(move || {
                                    barrier.wait();
                                    for i in 0..OPS_PER_THREAD {
                                        if (i % 100) < read_pct {
                                            let key = format!("key{:08}", i % PRE_POPULATE);
                                            black_box(get_at_via_scan(
                                                engine,
                                                key.as_bytes(),
                                                read_ts,
                                            ));
                                        } else {
                                            let key = format!("mixed_{tid}_{i}");
                                            let value = format!("value_{tid}_{i}");
                                            let ts = next_ts();
                                            engine.put_at(key.as_bytes(), value.as_bytes(), ts);
                                        }
                                    }
                                });
                            }
                        });

                        total_duration += start.elapsed();
                    }

                    total_duration
                });
            },
        );

        // Crossbeam-based memtable
        group.bench_with_input(
            BenchmarkId::new(format!("crossbeam_memtable/{label}"), num_threads),
            &(read_pct, write_pct),
            |b, &(read_pct, _write_pct)| {
                reset_ts();
                let engine = CrossbeamMemTableEngine::new();
                populate_engine(&engine, PRE_POPULATE);
                let read_ts = next_ts();

                b.iter_custom(|iters| {
                    let mut total_duration = std::time::Duration::ZERO;

                    for _ in 0..iters {
                        let barrier = Arc::new(Barrier::new(num_threads));
                        let start = std::time::Instant::now();

                        thread::scope(|s| {
                            for tid in 0..num_threads {
                                let engine = &engine;
                                let barrier = Arc::clone(&barrier);

                                s.spawn(move || {
                                    barrier.wait();
                                    for i in 0..OPS_PER_THREAD {
                                        if (i % 100) < read_pct {
                                            let key = format!("key{:08}", i % PRE_POPULATE);
                                            black_box(get_at_via_scan(
                                                engine,
                                                key.as_bytes(),
                                                read_ts,
                                            ));
                                        } else {
                                            let key = format!("mixed_{tid}_{i}");
                                            let value = format!("value_{tid}_{i}");
                                            let ts = next_ts();
                                            engine.put_at(key.as_bytes(), value.as_bytes(), ts);
                                        }
                                    }
                                });
                            }
                        });

                        total_duration += start.elapsed();
                    }

                    total_duration
                });
            },
        );

        // BTreeMap-based memtable
        group.bench_with_input(
            BenchmarkId::new(format!("btree_memtable/{label}"), num_threads),
            &(read_pct, write_pct),
            |b, &(read_pct, _write_pct)| {
                reset_ts();
                let engine = BTreeMemTableEngine::new();
                populate_engine(&engine, PRE_POPULATE);
                let read_ts = next_ts();

                b.iter_custom(|iters| {
                    let mut total_duration = std::time::Duration::ZERO;

                    for _ in 0..iters {
                        let barrier = Arc::new(Barrier::new(num_threads));
                        let start = std::time::Instant::now();

                        thread::scope(|s| {
                            for tid in 0..num_threads {
                                let engine = &engine;
                                let barrier = Arc::clone(&barrier);

                                s.spawn(move || {
                                    barrier.wait();
                                    for i in 0..OPS_PER_THREAD {
                                        if (i % 100) < read_pct {
                                            let key = format!("key{:08}", i % PRE_POPULATE);
                                            black_box(get_at_via_scan(
                                                engine,
                                                key.as_bytes(),
                                                read_ts,
                                            ));
                                        } else {
                                            let key = format!("mixed_{tid}_{i}");
                                            let value = format!("value_{tid}_{i}");
                                            let ts = next_ts();
                                            engine.put_at(key.as_bytes(), value.as_bytes(), ts);
                                        }
                                    }
                                });
                            }
                        });

                        total_duration += start.elapsed();
                    }

                    total_duration
                });
            },
        );
    }

    group.finish();
}

fn bench_scan_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_impl/scan");
    group.sample_size(20); // Scans are slower, use fewer samples

    let num_threads = 8;
    let scans_per_thread = 100;
    let scan_range_size = 1000;

    let total_ops = num_threads * scans_per_thread;
    group.throughput(Throughput::Elements(total_ops as u64));

    // Arena-based memtable
    group.bench_function("arena_memtable", |b| {
        reset_ts();
        let engine = ArenaMemTableEngine::new();
        populate_engine(&engine, PRE_POPULATE);
        let read_ts = next_ts();

        b.iter_custom(|iters| {
            let mut total_duration = std::time::Duration::ZERO;

            for _ in 0..iters {
                let barrier = Arc::new(Barrier::new(num_threads));
                let start = std::time::Instant::now();

                thread::scope(|s| {
                    for _ in 0..num_threads {
                        let engine = &engine;
                        let barrier = Arc::clone(&barrier);

                        s.spawn(move || {
                            barrier.wait();
                            for i in 0..scans_per_thread {
                                let start_idx = (i * 100) % (PRE_POPULATE - scan_range_size);
                                let start_key = format!("key{start_idx:08}");
                                let end_key = format!("key{:08}", start_idx + scan_range_size);
                                let start_mvcc = MvccKey::encode(start_key.as_bytes(), read_ts);
                                let end_mvcc = MvccKey::encode(end_key.as_bytes(), 0);
                                let results = engine.scan(start_mvcc..end_mvcc).unwrap();
                                black_box(results);
                            }
                        });
                    }
                });

                total_duration += start.elapsed();
            }

            total_duration
        });
    });

    // Crossbeam-based memtable
    group.bench_function("crossbeam_memtable", |b| {
        reset_ts();
        let engine = CrossbeamMemTableEngine::new();
        populate_engine(&engine, PRE_POPULATE);
        let read_ts = next_ts();

        b.iter_custom(|iters| {
            let mut total_duration = std::time::Duration::ZERO;

            for _ in 0..iters {
                let barrier = Arc::new(Barrier::new(num_threads));
                let start = std::time::Instant::now();

                thread::scope(|s| {
                    for _ in 0..num_threads {
                        let engine = &engine;
                        let barrier = Arc::clone(&barrier);

                        s.spawn(move || {
                            barrier.wait();
                            for i in 0..scans_per_thread {
                                let start_idx = (i * 100) % (PRE_POPULATE - scan_range_size);
                                let start_key = format!("key{start_idx:08}");
                                let end_key = format!("key{:08}", start_idx + scan_range_size);
                                let start_mvcc = MvccKey::encode(start_key.as_bytes(), read_ts);
                                let end_mvcc = MvccKey::encode(end_key.as_bytes(), 0);
                                let results = engine.scan(start_mvcc..end_mvcc).unwrap();
                                black_box(results);
                            }
                        });
                    }
                });

                total_duration += start.elapsed();
            }

            total_duration
        });
    });

    // BTreeMap-based memtable
    group.bench_function("btree_memtable", |b| {
        reset_ts();
        let engine = BTreeMemTableEngine::new();
        populate_engine(&engine, PRE_POPULATE);
        let read_ts = next_ts();

        b.iter_custom(|iters| {
            let mut total_duration = std::time::Duration::ZERO;

            for _ in 0..iters {
                let barrier = Arc::new(Barrier::new(num_threads));
                let start = std::time::Instant::now();

                thread::scope(|s| {
                    for _ in 0..num_threads {
                        let engine = &engine;
                        let barrier = Arc::clone(&barrier);

                        s.spawn(move || {
                            barrier.wait();
                            for i in 0..scans_per_thread {
                                let start_idx = (i * 100) % (PRE_POPULATE - scan_range_size);
                                let start_key = format!("key{start_idx:08}");
                                let end_key = format!("key{:08}", start_idx + scan_range_size);
                                let start_mvcc = MvccKey::encode(start_key.as_bytes(), read_ts);
                                let end_mvcc = MvccKey::encode(end_key.as_bytes(), 0);
                                let results = engine.scan(start_mvcc..end_mvcc).unwrap();
                                black_box(results);
                            }
                        });
                    }
                });

                total_duration += start.elapsed();
            }

            total_duration
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_write_throughput,
    bench_read_throughput,
    bench_mixed_workload,
    bench_scan_throughput,
);
criterion_main!(benches);
