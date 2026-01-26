//! Benchmarks for ArenaSkipList and ArenaMemTableEngine.
//!
//! Run with: cargo bench --bench memtable_bench
//!
//! These benchmarks measure the core performance characteristics of the
//! memtable subsystem, which is foundational to TiSQL's performance.

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use std::sync::{Arc, Barrier};
use std::thread;
use tisql::storage::arena_memtable::ArenaMemTableEngine;
use tisql::storage::skiplist::ArenaSkipList;
use tisql::storage::{StorageEngine, WriteBatch};
use tisql::util::arena::PageArena;

// =============================================================================
// ArenaSkipList Benchmarks
// =============================================================================

/// Benchmark sequential inserts into ArenaSkipList.
fn bench_skiplist_insert_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("skiplist/insert_sequential");

    for count in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                let arena = PageArena::with_defaults();
                let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);
                for i in 0..count {
                    black_box(list.insert(i, i * 10));
                }
                // arena and list dropped here together
            });
        });
    }

    group.finish();
}

/// Benchmark random inserts into ArenaSkipList.
fn bench_skiplist_insert_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("skiplist/insert_random");

    for count in [1_000, 10_000, 100_000] {
        // Pre-generate keys outside the benchmark loop
        let keys: Vec<i64> = (0..count)
            .map(|i| {
                let x = i as i64;
                x.wrapping_mul(2654435761) ^ (x >> 16)
            })
            .collect();

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &keys, |b, keys| {
            b.iter(|| {
                let arena = PageArena::with_defaults();
                let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);
                for &key in keys {
                    black_box(list.insert(key, key * 10));
                }
            });
        });
    }

    group.finish();
}

/// Benchmark point lookups in ArenaSkipList.
fn bench_skiplist_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("skiplist/get");

    for count in [1_000, 10_000, 100_000] {
        // Setup: create a pre-populated skiplist (leaked for 'static lifetime)
        let arena = Box::leak(Box::new(PageArena::with_defaults()));
        let list: &'static ArenaSkipList<i64, i64> = Box::leak(Box::new(ArenaSkipList::new(arena)));
        for i in 0..count {
            list.insert(i, i * 10);
        }

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::new("hit", count), &count, |b, &count| {
            b.iter(|| {
                for i in 0..count {
                    black_box(list.get(&i));
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("miss", count), &count, |b, &count| {
            b.iter(|| {
                for i in count..(count * 2) {
                    black_box(list.get(&i));
                }
            });
        });
    }

    group.finish();
}

/// Benchmark iteration over ArenaSkipList.
fn bench_skiplist_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("skiplist/iter");

    for count in [1_000, 10_000, 100_000] {
        let arena = Box::leak(Box::new(PageArena::with_defaults()));
        let list: &'static ArenaSkipList<i64, i64> = Box::leak(Box::new(ArenaSkipList::new(arena)));
        for i in 0..count {
            list.insert(i, i * 10);
        }

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let mut sum = 0i64;
                for (k, v) in list.iter() {
                    sum = sum.wrapping_add(*k).wrapping_add(*v);
                }
                black_box(sum)
            });
        });
    }

    group.finish();
}

/// Benchmark concurrent inserts into ArenaSkipList.
fn bench_skiplist_concurrent_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("skiplist/concurrent_insert");
    group.sample_size(50);

    for num_threads in [2, 4, 8] {
        let inserts_per_thread = 10_000;
        let total_inserts = num_threads * inserts_per_thread;

        group.throughput(Throughput::Elements(total_inserts as u64));
        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    // Create arena with stable address
                    let arena = Box::leak(Box::new(PageArena::with_defaults()));
                    let list: &'static ArenaSkipList<i64, i64> =
                        Box::leak(Box::new(ArenaSkipList::new(arena)));

                    let barrier = Arc::new(Barrier::new(num_threads));

                    let handles: Vec<_> = (0..num_threads)
                        .map(|tid| {
                            let barrier = Arc::clone(&barrier);
                            thread::spawn(move || {
                                barrier.wait();
                                for i in 0..inserts_per_thread {
                                    let key = (tid * inserts_per_thread + i) as i64;
                                    list.insert(key, key);
                                }
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }
                    // Note: Memory is leaked for simplicity in benchmarks
                });
            },
        );
    }

    group.finish();
}

/// Benchmark concurrent mixed read/write workload.
fn bench_skiplist_concurrent_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("skiplist/concurrent_mixed");
    group.sample_size(50);

    // Pre-populate with 100K entries
    let pre_populate = 100_000i64;
    let arena = Box::leak(Box::new(PageArena::with_defaults()));
    let list: &'static ArenaSkipList<i64, i64> = Box::leak(Box::new(ArenaSkipList::new(arena)));
    for i in 0..pre_populate {
        list.insert(i, i * 10);
    }

    for (readers, writers) in [(4, 4), (7, 1), (1, 7)] {
        let total_threads = readers + writers;
        let ops_per_thread = 10_000;

        group.throughput(Throughput::Elements(
            (total_threads * ops_per_thread) as u64,
        ));
        group.bench_with_input(
            BenchmarkId::new(format!("r{readers}_w{writers}"), total_threads),
            &(readers, writers),
            |b, &(readers, writers)| {
                b.iter(|| {
                    let barrier = Arc::new(Barrier::new(readers + writers));

                    let mut handles = Vec::new();

                    // Reader threads
                    for _ in 0..readers {
                        let barrier = Arc::clone(&barrier);
                        handles.push(thread::spawn(move || {
                            barrier.wait();
                            for i in 0..ops_per_thread {
                                let key = (i as i64 * 7) % pre_populate;
                                black_box(list.get(&key));
                            }
                        }));
                    }

                    // Writer threads (write to new keys beyond pre_populate)
                    for tid in 0..writers {
                        let barrier = Arc::clone(&barrier);
                        handles.push(thread::spawn(move || {
                            barrier.wait();
                            for i in 0..ops_per_thread {
                                let key = pre_populate + (tid * ops_per_thread + i) as i64;
                                list.insert(key, key);
                            }
                        }));
                    }

                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// ArenaMemTableEngine Benchmarks (MVCC Layer)
// =============================================================================

/// Generate a key with specified size.
fn make_key(id: u64, size: usize) -> Vec<u8> {
    let mut key = format!("key_{id:016}").into_bytes();
    key.resize(size, b'x');
    key
}

/// Generate a value with specified size.
fn make_value(id: u64, size: usize) -> Vec<u8> {
    let mut value = format!("value_{id:016}").into_bytes();
    value.resize(size, b'y');
    value
}

/// Benchmark memtable put operations.
fn bench_memtable_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/put");

    for count in [1_000, 10_000, 100_000] {
        // Pre-generate keys and values
        let keys: Vec<_> = (0..count).map(|i| make_key(i, 32)).collect();
        let values: Vec<_> = (0..count).map(|i| make_value(i, 128)).collect();

        group.throughput(Throughput::Elements(count));
        group.bench_with_input(
            BenchmarkId::from_parameter(count),
            &(&keys, &values),
            |b, &(keys, values)| {
                b.iter_batched(
                    ArenaMemTableEngine::new,
                    |engine| {
                        for (i, (key, value)) in keys.iter().zip(values.iter()).enumerate() {
                            engine.put_at(key, value, (i + 1) as u64);
                        }
                        engine
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark memtable get operations with MVCC.
fn bench_memtable_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/get");

    for count in [1_000, 10_000, 100_000] {
        // Pre-populate
        let engine = Box::leak(Box::new(ArenaMemTableEngine::new()));
        for i in 0..count {
            let key = make_key(i, 32);
            let value = make_value(i, 128);
            engine.put_at(&key, &value, i + 1);
        }

        let keys: Vec<_> = (0..count).map(|i| make_key(i, 32)).collect();
        let read_ts = count + 1;

        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::new("latest", count), &keys, |b, keys| {
            b.iter(|| {
                for key in keys.iter() {
                    black_box(engine.get_at(key, read_ts).unwrap());
                }
            });
        });

        // Benchmark reading at historical timestamp (middle of writes)
        let historical_ts = count / 2;
        group.bench_with_input(BenchmarkId::new("historical", count), &keys, |b, keys| {
            b.iter(|| {
                for key in keys.iter() {
                    black_box(engine.get_at(key, historical_ts).unwrap());
                }
            });
        });
    }

    group.finish();
}

/// Benchmark memtable scan operations.
fn bench_memtable_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/scan");

    for count in [1_000, 10_000, 100_000] {
        let engine = Box::leak(Box::new(ArenaMemTableEngine::new()));
        for i in 0..count {
            let key = make_key(i, 32);
            let value = make_value(i, 128);
            engine.put_at(&key, &value, 1);
        }

        group.throughput(Throughput::Elements(count));

        // Full scan
        let start = make_key(0, 32);
        let end = make_key(u64::MAX, 32);
        group.bench_with_input(
            BenchmarkId::new("full", count),
            &(&start, &end),
            |b, &(start, end)| {
                b.iter(|| {
                    let results: Vec<_> = engine
                        .scan(&(start.clone()..end.clone()))
                        .unwrap()
                        .collect();
                    black_box(results.len())
                });
            },
        );

        // Partial scan (10% of data)
        let partial_end = make_key(count / 10, 32);
        group.bench_with_input(
            BenchmarkId::new("partial_10pct", count),
            &(&start, &partial_end),
            |b, &(start, end)| {
                b.iter(|| {
                    let results: Vec<_> = engine
                        .scan(&(start.clone()..end.clone()))
                        .unwrap()
                        .collect();
                    black_box(results.len())
                });
            },
        );
    }

    group.finish();
}

/// Benchmark write batch operations.
fn bench_memtable_write_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/write_batch");

    for batch_size in [10, 100, 1000] {
        // Pre-generate batch data
        let batch_data: Vec<_> = (0..batch_size)
            .map(|i| (make_key(i, 32), make_value(i, 128)))
            .collect();

        group.throughput(Throughput::Elements(batch_size));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_data,
            |b, batch_data| {
                b.iter_batched(
                    || {
                        let engine = ArenaMemTableEngine::new();
                        let mut batch = WriteBatch::new();
                        batch.set_commit_ts(1);
                        for (key, value) in batch_data {
                            batch.put(key.clone(), value.clone());
                        }
                        (engine, batch)
                    },
                    |(engine, batch)| {
                        engine.write_batch(batch).unwrap();
                        engine
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark MVCC version chains (multiple versions of same key).
fn bench_memtable_mvcc_versions(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/mvcc_versions");

    for num_versions in [1, 10, 100] {
        let engine = Box::leak(Box::new(ArenaMemTableEngine::new()));
        let key = make_key(0, 32);

        // Create multiple versions of the same key
        for ts in 1..=num_versions {
            let value = make_value(ts, 128);
            engine.put_at(&key, &value, ts);
        }

        group.bench_with_input(
            BenchmarkId::new("read_latest", num_versions),
            &key,
            |b, key| {
                let ts = num_versions;
                b.iter(|| black_box(engine.get_at(key, ts).unwrap()));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("read_oldest", num_versions),
            &key,
            |b, key| {
                b.iter(|| black_box(engine.get_at(key, 1).unwrap()));
            },
        );
    }

    group.finish();
}

/// Benchmark concurrent memtable operations.
fn bench_memtable_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/concurrent");
    group.sample_size(50);

    // Pre-populate with 50K entries
    let pre_populate = 50_000u64;
    let engine: &'static ArenaMemTableEngine = Box::leak(Box::new(ArenaMemTableEngine::new()));
    for i in 0..pre_populate {
        let key = make_key(i, 32);
        let value = make_value(i, 128);
        engine.put_at(&key, &value, 1);
    }

    for num_threads in [2, 4, 8] {
        let ops_per_thread = 5_000;

        group.throughput(Throughput::Elements((num_threads * ops_per_thread) as u64));
        group.bench_with_input(
            BenchmarkId::new("mixed", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    let barrier = Arc::new(Barrier::new(num_threads));

                    let handles: Vec<_> = (0..num_threads)
                        .map(|tid| {
                            let barrier = Arc::clone(&barrier);
                            thread::spawn(move || {
                                barrier.wait();
                                for i in 0..ops_per_thread {
                                    if i % 4 == 0 {
                                        // 25% writes
                                        let key = make_key(
                                            pre_populate + (tid * ops_per_thread + i) as u64,
                                            32,
                                        );
                                        let value = make_value(i as u64, 128);
                                        let ts = (100 + tid * ops_per_thread + i) as u64;
                                        engine.put_at(&key, &value, ts);
                                    } else {
                                        // 75% reads
                                        let key = make_key((i as u64 * 7) % pre_populate, 32);
                                        black_box(engine.get_at(&key, u64::MAX).unwrap());
                                    }
                                }
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Arena Allocator Benchmarks
// =============================================================================

/// Benchmark arena allocation performance.
fn bench_arena_alloc(c: &mut Criterion) {
    let mut group = c.benchmark_group("arena/alloc");

    for size in [64, 256, 1024, 4096] {
        group.throughput(Throughput::Bytes(size as u64 * 10_000));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                PageArena::with_defaults,
                |arena| {
                    for _ in 0..10_000 {
                        black_box(arena.alloc(size, 8));
                    }
                    arena
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark concurrent arena allocation.
fn bench_arena_concurrent_alloc(c: &mut Criterion) {
    let mut group = c.benchmark_group("arena/concurrent_alloc");
    group.sample_size(50);

    for num_threads in [2, 4, 8] {
        let allocs_per_thread = 10_000;

        group.throughput(Throughput::Elements(
            (num_threads * allocs_per_thread) as u64,
        ));
        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter_batched(
                    || Arc::new(PageArena::with_defaults()),
                    |arena| {
                        let barrier = Arc::new(Barrier::new(num_threads));

                        let handles: Vec<_> = (0..num_threads)
                            .map(|_| {
                                let arena = Arc::clone(&arena);
                                let barrier = Arc::clone(&barrier);
                                thread::spawn(move || {
                                    barrier.wait();
                                    for i in 0..allocs_per_thread {
                                        let size = 64 + (i % 128);
                                        black_box(arena.alloc(size, 8));
                                    }
                                })
                            })
                            .collect();

                        for h in handles {
                            h.join().unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// =============================================================================
// Criterion Configuration
// =============================================================================

criterion_group!(
    skiplist_benches,
    bench_skiplist_insert_sequential,
    bench_skiplist_insert_random,
    bench_skiplist_get,
    bench_skiplist_iter,
    bench_skiplist_concurrent_insert,
    bench_skiplist_concurrent_mixed,
);

criterion_group!(
    memtable_benches,
    bench_memtable_put,
    bench_memtable_get,
    bench_memtable_scan,
    bench_memtable_write_batch,
    bench_memtable_mvcc_versions,
    bench_memtable_concurrent,
);

criterion_group!(
    arena_benches,
    bench_arena_alloc,
    bench_arena_concurrent_alloc,
);

criterion_main!(skiplist_benches, memtable_benches, arena_benches);
