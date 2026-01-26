//! Comparison benchmarks for different memtable implementations.
//!
//! Run with: cargo bench --bench memtable_comparison
//!
//! This benchmark compares three concurrent key-value storage approaches:
//!
//! 1. **ArenaSkipList** (TiSQL's implementation)
//!    - Lock-free skip list with arena allocation
//!    - No individual node deallocation (bulk cleanup)
//!    - Custom implementation optimized for MVCC workloads
//!
//! 2. **Crossbeam SkipMap**
//!    - Lock-free skip list with epoch-based GC
//!    - Individual node reclamation via crossbeam-epoch
//!    - Battle-tested, widely used in production
//!
//! 3. **BTreeMap + RwLock** (parking_lot)
//!    - Standard library BTreeMap with reader-writer lock
//!    - Good cache locality for reads
//!    - Write contention under high concurrency
//!
//! ## Key Differences
//!
//! | Implementation | Memory Management | Concurrent Writes | Memory Overhead |
//! |----------------|-------------------|-------------------|-----------------|
//! | ArenaSkipList  | Arena (bulk free) | Lock-free CAS     | Low (no refcnt) |
//! | Crossbeam      | Epoch-based GC    | Lock-free CAS     | Medium (epoch)  |
//! | BTreeMap+Lock  | Standard heap     | RwLock            | Low             |

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};
use std::thread;
use tisql::storage::skiplist::ArenaSkipList;
use tisql::util::arena::PageArena;

// =============================================================================
// Test Parameters
// =============================================================================

/// Number of entries to pre-populate for read-heavy workloads
const PRE_POPULATE: i64 = 50_000;

/// Operations per thread in concurrent benchmarks
const OPS_PER_THREAD: usize = 20_000;

// =============================================================================
// Wrapper Types for Uniform Interface
// =============================================================================

/// Trait for abstracting over different map implementations
trait ConcurrentMap: Send + Sync {
    fn insert(&self, key: i64, value: i64);
    fn get(&self, key: &i64) -> Option<i64>;
}

/// Wrapper for ArenaSkipList (requires leaked arena for 'static lifetime in benchmarks)
struct ArenaSkipListWrapper {
    list: &'static ArenaSkipList<'static, i64, i64>,
}

impl ConcurrentMap for ArenaSkipListWrapper {
    fn insert(&self, key: i64, value: i64) {
        self.list.insert(key, value);
    }

    fn get(&self, key: &i64) -> Option<i64> {
        self.list.get(key).copied()
    }
}

/// Wrapper for crossbeam SkipMap
struct CrossbeamSkipMapWrapper {
    map: SkipMap<i64, i64>,
}

impl ConcurrentMap for CrossbeamSkipMapWrapper {
    fn insert(&self, key: i64, value: i64) {
        self.map.insert(key, value);
    }

    fn get(&self, key: &i64) -> Option<i64> {
        self.map.get(key).map(|e| *e.value())
    }
}

/// Wrapper for BTreeMap with RwLock
struct BTreeMapWrapper {
    map: RwLock<BTreeMap<i64, i64>>,
}

impl ConcurrentMap for BTreeMapWrapper {
    fn insert(&self, key: i64, value: i64) {
        self.map.write().insert(key, value);
    }

    fn get(&self, key: &i64) -> Option<i64> {
        self.map.read().get(key).copied()
    }
}

// =============================================================================
// Benchmark: Concurrent Write-Only Workload
// =============================================================================

fn bench_concurrent_write_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("comparison/write_only");
    group.sample_size(30);

    for num_threads in [1, 2, 4, 8] {
        let total_ops = num_threads * OPS_PER_THREAD;
        group.throughput(Throughput::Elements(total_ops as u64));

        // ArenaSkipList
        group.bench_with_input(
            BenchmarkId::new("arena_skiplist", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    let arena = Box::leak(Box::new(PageArena::with_defaults()));
                    let list: &'static ArenaSkipList<i64, i64> =
                        Box::leak(Box::new(ArenaSkipList::new(arena)));
                    let wrapper = Arc::new(ArenaSkipListWrapper { list });

                    run_write_workload(wrapper, num_threads, OPS_PER_THREAD);
                });
            },
        );

        // Crossbeam SkipMap
        group.bench_with_input(
            BenchmarkId::new("crossbeam_skipmap", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    let wrapper = Arc::new(CrossbeamSkipMapWrapper {
                        map: SkipMap::new(),
                    });

                    run_write_workload(wrapper, num_threads, OPS_PER_THREAD);
                });
            },
        );

        // BTreeMap + RwLock
        group.bench_with_input(
            BenchmarkId::new("btreemap_rwlock", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    let wrapper = Arc::new(BTreeMapWrapper {
                        map: RwLock::new(BTreeMap::new()),
                    });

                    run_write_workload(wrapper, num_threads, OPS_PER_THREAD);
                });
            },
        );
    }

    group.finish();
}

fn run_write_workload<M: ConcurrentMap + 'static>(
    map: Arc<M>,
    num_threads: usize,
    ops_per_thread: usize,
) {
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|tid| {
            let map = Arc::clone(&map);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for i in 0..ops_per_thread {
                    let key = (tid * ops_per_thread + i) as i64;
                    map.insert(key, key);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

// =============================================================================
// Benchmark: Concurrent Read-Only Workload
// =============================================================================

fn bench_concurrent_read_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("comparison/read_only");
    group.sample_size(30);

    // Pre-populate data structures
    let arena = Box::leak(Box::new(PageArena::with_defaults()));
    let arena_list: &'static ArenaSkipList<i64, i64> =
        Box::leak(Box::new(ArenaSkipList::new(arena)));

    let crossbeam_map = SkipMap::new();
    let btree_map = BTreeMap::new();

    for i in 0..PRE_POPULATE {
        arena_list.insert(i, i * 10);
        crossbeam_map.insert(i, i * 10);
    }
    let btree_map = {
        let mut m = btree_map;
        for i in 0..PRE_POPULATE {
            m.insert(i, i * 10);
        }
        m
    };

    let arena_wrapper = Arc::new(ArenaSkipListWrapper { list: arena_list });
    let crossbeam_wrapper = Arc::new(CrossbeamSkipMapWrapper { map: crossbeam_map });
    let btree_wrapper = Arc::new(BTreeMapWrapper {
        map: RwLock::new(btree_map),
    });

    for num_threads in [1, 2, 4, 8] {
        let total_ops = num_threads * OPS_PER_THREAD;
        group.throughput(Throughput::Elements(total_ops as u64));

        // ArenaSkipList
        {
            let wrapper = Arc::clone(&arena_wrapper);
            group.bench_with_input(
                BenchmarkId::new("arena_skiplist", num_threads),
                &num_threads,
                move |b, &num_threads| {
                    let wrapper = Arc::clone(&wrapper);
                    b.iter(|| {
                        run_read_workload(Arc::clone(&wrapper), num_threads, OPS_PER_THREAD);
                    });
                },
            );
        }

        // Crossbeam SkipMap
        {
            let wrapper = Arc::clone(&crossbeam_wrapper);
            group.bench_with_input(
                BenchmarkId::new("crossbeam_skipmap", num_threads),
                &num_threads,
                move |b, &num_threads| {
                    let wrapper = Arc::clone(&wrapper);
                    b.iter(|| {
                        run_read_workload(Arc::clone(&wrapper), num_threads, OPS_PER_THREAD);
                    });
                },
            );
        }

        // BTreeMap + RwLock
        {
            let wrapper = Arc::clone(&btree_wrapper);
            group.bench_with_input(
                BenchmarkId::new("btreemap_rwlock", num_threads),
                &num_threads,
                move |b, &num_threads| {
                    let wrapper = Arc::clone(&wrapper);
                    b.iter(|| {
                        run_read_workload(Arc::clone(&wrapper), num_threads, OPS_PER_THREAD);
                    });
                },
            );
        }
    }

    group.finish();
}

fn run_read_workload<M: ConcurrentMap + 'static>(
    map: Arc<M>,
    num_threads: usize,
    ops_per_thread: usize,
) {
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let map = Arc::clone(&map);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for i in 0..ops_per_thread {
                    let key = (i as i64 * 7) % PRE_POPULATE;
                    black_box(map.get(&key));
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

// =============================================================================
// Benchmark: Concurrent Mixed Read/Write Workload
// =============================================================================

fn bench_concurrent_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("comparison/mixed");
    group.sample_size(30);

    // Test different read/write ratios with 8 threads total
    for (read_pct, write_pct) in [(50, 50), (90, 10), (10, 90)] {
        let num_threads = 8;
        let total_ops = num_threads * OPS_PER_THREAD;
        group.throughput(Throughput::Elements(total_ops as u64));

        let label = format!("r{read_pct}w{write_pct}");

        // ArenaSkipList
        group.bench_with_input(
            BenchmarkId::new(format!("arena_skiplist/{label}"), num_threads),
            &(read_pct, write_pct),
            |b, &(read_pct, _)| {
                b.iter(|| {
                    let arena = Box::leak(Box::new(PageArena::with_defaults()));
                    let list: &'static ArenaSkipList<i64, i64> =
                        Box::leak(Box::new(ArenaSkipList::new(arena)));

                    // Pre-populate
                    for i in 0..PRE_POPULATE {
                        list.insert(i, i * 10);
                    }

                    let wrapper = Arc::new(ArenaSkipListWrapper { list });
                    run_mixed_workload(wrapper, num_threads, OPS_PER_THREAD, read_pct);
                });
            },
        );

        // Crossbeam SkipMap
        group.bench_with_input(
            BenchmarkId::new(format!("crossbeam_skipmap/{label}"), num_threads),
            &(read_pct, write_pct),
            |b, &(read_pct, _)| {
                b.iter(|| {
                    let map = SkipMap::new();

                    // Pre-populate
                    for i in 0..PRE_POPULATE {
                        map.insert(i, i * 10);
                    }

                    let wrapper = Arc::new(CrossbeamSkipMapWrapper { map });
                    run_mixed_workload(wrapper, num_threads, OPS_PER_THREAD, read_pct);
                });
            },
        );

        // BTreeMap + RwLock
        group.bench_with_input(
            BenchmarkId::new(format!("btreemap_rwlock/{label}"), num_threads),
            &(read_pct, write_pct),
            |b, &(read_pct, _)| {
                b.iter(|| {
                    let mut map = BTreeMap::new();

                    // Pre-populate
                    for i in 0..PRE_POPULATE {
                        map.insert(i, i * 10);
                    }

                    let wrapper = Arc::new(BTreeMapWrapper {
                        map: RwLock::new(map),
                    });
                    run_mixed_workload(wrapper, num_threads, OPS_PER_THREAD, read_pct);
                });
            },
        );
    }

    group.finish();
}

fn run_mixed_workload<M: ConcurrentMap + 'static>(
    map: Arc<M>,
    num_threads: usize,
    ops_per_thread: usize,
    read_pct: usize,
) {
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|tid| {
            let map = Arc::clone(&map);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for i in 0..ops_per_thread {
                    // Determine if this op is a read or write based on percentage
                    let is_read = (i * 100 / ops_per_thread) < read_pct;

                    if is_read {
                        let key = (i as i64 * 7) % PRE_POPULATE;
                        black_box(map.get(&key));
                    } else {
                        let key = PRE_POPULATE + (tid * ops_per_thread + i) as i64;
                        map.insert(key, key);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

// =============================================================================
// Benchmark: Write Contention (Same Keys)
// =============================================================================

fn bench_write_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("comparison/contention");
    group.sample_size(30);

    // All threads write to a small set of keys (high contention)
    let hot_keys = 100i64;

    for num_threads in [2, 4, 8] {
        let total_ops = num_threads * OPS_PER_THREAD;
        group.throughput(Throughput::Elements(total_ops as u64));

        // ArenaSkipList
        group.bench_with_input(
            BenchmarkId::new("arena_skiplist", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    let arena = Box::leak(Box::new(PageArena::with_defaults()));
                    let list: &'static ArenaSkipList<i64, i64> =
                        Box::leak(Box::new(ArenaSkipList::new(arena)));
                    let wrapper = Arc::new(ArenaSkipListWrapper { list });

                    run_contention_workload(wrapper, num_threads, OPS_PER_THREAD, hot_keys);
                });
            },
        );

        // Crossbeam SkipMap
        group.bench_with_input(
            BenchmarkId::new("crossbeam_skipmap", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    let wrapper = Arc::new(CrossbeamSkipMapWrapper {
                        map: SkipMap::new(),
                    });

                    run_contention_workload(wrapper, num_threads, OPS_PER_THREAD, hot_keys);
                });
            },
        );

        // BTreeMap + RwLock
        group.bench_with_input(
            BenchmarkId::new("btreemap_rwlock", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    let wrapper = Arc::new(BTreeMapWrapper {
                        map: RwLock::new(BTreeMap::new()),
                    });

                    run_contention_workload(wrapper, num_threads, OPS_PER_THREAD, hot_keys);
                });
            },
        );
    }

    group.finish();
}

fn run_contention_workload<M: ConcurrentMap + 'static>(
    map: Arc<M>,
    num_threads: usize,
    ops_per_thread: usize,
    hot_keys: i64,
) {
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|tid| {
            let map = Arc::clone(&map);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for i in 0..ops_per_thread {
                    // All threads compete for the same small set of keys
                    let key = (tid as i64 * 17 + i as i64 * 31) % hot_keys;
                    map.insert(key, i as i64);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

// =============================================================================
// Benchmark: Sequential vs Random Access Pattern
// =============================================================================

fn bench_access_pattern(c: &mut Criterion) {
    let mut group = c.benchmark_group("comparison/access_pattern");
    group.sample_size(30);

    let num_threads = 4;
    let total_ops = num_threads * OPS_PER_THREAD;
    group.throughput(Throughput::Elements(total_ops as u64));

    // Pre-populate all implementations
    let arena = Box::leak(Box::new(PageArena::with_defaults()));
    let arena_list: &'static ArenaSkipList<i64, i64> =
        Box::leak(Box::new(ArenaSkipList::new(arena)));
    let crossbeam_map = SkipMap::new();
    let mut btree_map = BTreeMap::new();

    for i in 0..PRE_POPULATE {
        arena_list.insert(i, i * 10);
        crossbeam_map.insert(i, i * 10);
        btree_map.insert(i, i * 10);
    }

    let arena_wrapper = Arc::new(ArenaSkipListWrapper { list: arena_list });
    let crossbeam_wrapper = Arc::new(CrossbeamSkipMapWrapper { map: crossbeam_map });
    let btree_wrapper = Arc::new(BTreeMapWrapper {
        map: RwLock::new(btree_map),
    });

    // Sequential access pattern
    for (name, wrapper) in [
        (
            "arena_skiplist",
            arena_wrapper.clone() as Arc<dyn ConcurrentMap>,
        ),
        (
            "crossbeam_skipmap",
            crossbeam_wrapper.clone() as Arc<dyn ConcurrentMap>,
        ),
        (
            "btreemap_rwlock",
            btree_wrapper.clone() as Arc<dyn ConcurrentMap>,
        ),
    ] {
        group.bench_with_input(
            BenchmarkId::new(format!("{name}/sequential"), num_threads),
            &num_threads,
            |b, &num_threads| {
                let wrapper = Arc::clone(&wrapper);
                b.iter(|| {
                    run_sequential_read(Arc::clone(&wrapper), num_threads, OPS_PER_THREAD);
                });
            },
        );
    }

    // Random access pattern
    for (name, wrapper) in [
        (
            "arena_skiplist",
            arena_wrapper.clone() as Arc<dyn ConcurrentMap>,
        ),
        (
            "crossbeam_skipmap",
            crossbeam_wrapper.clone() as Arc<dyn ConcurrentMap>,
        ),
        (
            "btreemap_rwlock",
            btree_wrapper.clone() as Arc<dyn ConcurrentMap>,
        ),
    ] {
        group.bench_with_input(
            BenchmarkId::new(format!("{name}/random"), num_threads),
            &num_threads,
            |b, &num_threads| {
                let wrapper = Arc::clone(&wrapper);
                b.iter(|| {
                    run_random_read(Arc::clone(&wrapper), num_threads, OPS_PER_THREAD);
                });
            },
        );
    }

    group.finish();
}

fn run_sequential_read<M: ConcurrentMap + ?Sized + 'static>(
    map: Arc<M>,
    num_threads: usize,
    ops_per_thread: usize,
) {
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|tid| {
            let map = Arc::clone(&map);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let start = (tid * ops_per_thread) as i64 % PRE_POPULATE;
                for i in 0..ops_per_thread {
                    let key = (start + i as i64) % PRE_POPULATE;
                    black_box(map.get(&key));
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

fn run_random_read<M: ConcurrentMap + ?Sized + 'static>(
    map: Arc<M>,
    num_threads: usize,
    ops_per_thread: usize,
) {
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|tid| {
            let map = Arc::clone(&map);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for i in 0..ops_per_thread {
                    // Pseudo-random key using multiplicative hash
                    let key = ((tid * 0x9E3779B9 + i * 0x85EBCA6B) as i64).abs() % PRE_POPULATE;
                    black_box(map.get(&key));
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

// =============================================================================
// Criterion Configuration
// =============================================================================

// Core comparison benchmarks (reduced set to avoid memory issues from leaking)
criterion_group!(
    comparison_benches,
    bench_concurrent_write_only,
    bench_concurrent_read_only,
    bench_concurrent_mixed,
);

// Additional benchmarks (can be run separately)
criterion_group!(
    additional_benches,
    bench_write_contention,
    bench_access_pattern,
);

// Run core benchmarks by default
criterion_main!(comparison_benches);
