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

//! Sysbench-like OLTP benchmark tool for the VersionedMemTableEngine.
//!
//! This tool simulates sysbench OLTP_RW workload to benchmark the production memtable:
//!
//! - VersionedMemTableEngine (OceanBase-style user key + version chain)
//!
//! Usage:
//!     cargo run --release --bin sysbench-sim -- [OPTIONS]
//!
//! Example:
//!     cargo run --release --bin sysbench-sim -- --threads 8 --time 30 --tables 1 --table-size 100000

// Use jemalloc as the global allocator on Unix (like TiKV)
#[cfg(all(unix, feature = "jemalloc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;

use tisql::storage::mvcc::{is_tombstone, MvccKey};
use tisql::storage::{StorageEngine, VersionedMemTableEngine, WriteBatch};

// ============================================================================
// Command Line Arguments
// ============================================================================

#[derive(Parser, Debug, Clone)]
#[command(name = "sysbench-sim")]
#[command(about = "Sysbench-like OLTP benchmark for TiSQL memtable implementations")]
#[command(version = "1.0")]
struct Args {
    /// Number of threads to use
    #[arg(long, default_value_t = 4)]
    threads: usize,

    /// Test duration in seconds
    #[arg(long, default_value_t = 60)]
    time: u64,

    /// Number of tables
    #[arg(long, default_value_t = 1)]
    tables: u64,

    /// Number of rows per table
    #[arg(long, default_value_t = 100_000)]
    table_size: usize,

    /// Row size in bytes (approximate)
    #[arg(long, default_value_t = 120)]
    row_size: usize,

    /// Report interval in seconds
    #[arg(long, default_value_t = 1)]
    report_interval: u64,

    /// Percentage of point selects (default 70%)
    #[arg(long, default_value_t = 70)]
    point_select_pct: usize,

    /// Percentage of updates (default 10%)
    #[arg(long, default_value_t = 10)]
    update_pct: usize,

    /// Percentage of deletes (default 10%)
    #[arg(long, default_value_t = 10)]
    delete_pct: usize,

    /// Percentage of inserts (default 10%)
    #[arg(long, default_value_t = 10)]
    insert_pct: usize,
}

// ============================================================================
// Statistics
// ============================================================================

#[derive(Default)]
struct Stats {
    // Operation counts
    reads: AtomicUsize,
    writes: AtomicUsize,
    other: AtomicUsize,
    errors: AtomicUsize,

    // Detailed counts
    point_selects: AtomicUsize,
    updates: AtomicUsize,
    deletes: AtomicUsize,
    inserts: AtomicUsize,

    // Latency tracking (nanoseconds)
    total_latency_ns: AtomicU64,
    min_latency_ns: AtomicU64,
    max_latency_ns: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            min_latency_ns: AtomicU64::new(u64::MAX),
            ..Default::default()
        }
    }

    fn record_read(&self, latency_ns: u64) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.point_selects.fetch_add(1, Ordering::Relaxed);
        self.record_latency(latency_ns);
    }

    fn record_write(&self, op: &str, latency_ns: u64) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        match op {
            "update" => self.updates.fetch_add(1, Ordering::Relaxed),
            "delete" => self.deletes.fetch_add(1, Ordering::Relaxed),
            "insert" => self.inserts.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
        self.record_latency(latency_ns);
    }

    fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_latency(&self, latency_ns: u64) {
        self.total_latency_ns
            .fetch_add(latency_ns, Ordering::Relaxed);

        // Update min (CAS loop)
        let mut current_min = self.min_latency_ns.load(Ordering::Relaxed);
        while latency_ns < current_min {
            match self.min_latency_ns.compare_exchange_weak(
                current_min,
                latency_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_min = x,
            }
        }

        // Update max (CAS loop)
        let mut current_max = self.max_latency_ns.load(Ordering::Relaxed);
        while latency_ns > current_max {
            match self.max_latency_ns.compare_exchange_weak(
                current_max,
                latency_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }
    }

    fn total_txns(&self) -> usize {
        self.reads.load(Ordering::Relaxed) + self.writes.load(Ordering::Relaxed)
    }

    fn avg_latency_ms(&self) -> f64 {
        let total = self.total_txns();
        if total == 0 {
            return 0.0;
        }
        let total_ns = self.total_latency_ns.load(Ordering::Relaxed);
        (total_ns as f64 / total as f64) / 1_000_000.0
    }

    fn min_latency_ms(&self) -> f64 {
        let min = self.min_latency_ns.load(Ordering::Relaxed);
        if min == u64::MAX {
            return 0.0;
        }
        min as f64 / 1_000_000.0
    }

    fn max_latency_ms(&self) -> f64 {
        self.max_latency_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }
}

// ============================================================================
// Workload Generator
// ============================================================================

struct WorkloadConfig {
    tables: u64,
    table_size: usize,
    row_size: usize,
    point_select_pct: usize,
    update_pct: usize,
    delete_pct: usize,
    insert_pct: usize,
}

/// Generate a table key
fn make_key(table_id: u64, row_id: u64) -> Vec<u8> {
    format!("t{table_id:04}_r{row_id:012}").into_bytes()
}

/// Generate a row value
fn make_value(row_id: u64, size: usize) -> Vec<u8> {
    let prefix = format!("row_{row_id:012}_data_");
    let mut value = prefix.into_bytes();
    while value.len() < size {
        value.push(b'x');
    }
    value.truncate(size);
    value
}

/// Populate engine with initial data
fn populate<E: StorageEngine>(engine: &E, config: &WorkloadConfig, ts_counter: &AtomicU64) {
    println!(
        "Populating {} rows...",
        config.table_size * config.tables as usize
    );
    let start = Instant::now();

    const BATCH_SIZE: usize = 1000;

    for table_id in 1..=config.tables {
        for batch_start in (0..config.table_size).step_by(BATCH_SIZE) {
            let mut batch = WriteBatch::new();
            let batch_end = (batch_start + BATCH_SIZE).min(config.table_size);

            for row_id in batch_start..batch_end {
                let key = make_key(table_id, row_id as u64);
                let value = make_value(row_id as u64, config.row_size);
                batch.put(key, value);
            }

            let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
            batch.set_commit_ts(ts);
            engine.write_batch(batch).unwrap();
        }
    }

    let elapsed = start.elapsed();
    let total_rows = config.table_size * config.tables as usize;
    let rate = total_rows as f64 / elapsed.as_secs_f64();
    println!(
        "Populated {} rows in {:.2}s ({:.0} rows/sec)",
        total_rows,
        elapsed.as_secs_f64(),
        rate
    );
}

/// Run OLTP workload on a single thread
fn worker_thread<E: StorageEngine>(
    engine: &E,
    config: &WorkloadConfig,
    ts_counter: &AtomicU64,
    next_row_id: &AtomicU64,
    stats: &Stats,
    running: &AtomicBool,
    thread_id: usize,
) {
    // Simple PRNG state
    let mut state: u64 = (thread_id as u64)
        .wrapping_mul(0x9E3779B97F4A7C15)
        .wrapping_add(Instant::now().elapsed().as_nanos() as u64);
    let mut rng = || {
        state = state
            .wrapping_mul(0x5851F42D4C957F2D)
            .wrapping_add(0x14057B7EF767814F);
        state
    };

    while running.load(Ordering::Relaxed) {
        let table_id = (rng() % config.tables) + 1;
        let op_type = (rng() % 100) as usize;
        let start = Instant::now();

        if op_type < config.point_select_pct {
            // Point select using MvccKey scan_iter
            let row_id = rng() % config.table_size as u64;
            let key = make_key(table_id, row_id);
            let ts = ts_counter.load(Ordering::Relaxed);
            // Scan for the key at the given timestamp
            let seek_key = MvccKey::encode(&key, ts);
            let end_key = MvccKey::encode(&key, 0)
                .next_key()
                .unwrap_or_else(MvccKey::unbounded);
            let mut iter = engine.scan_iter(seek_key..end_key).unwrap();
            // Find the first visible entry
            let mut _found = false;
            while iter.valid() {
                let decoded_key = iter.user_key();
                let entry_ts = iter.timestamp();
                let value = iter.value();
                if decoded_key == key && entry_ts <= ts && !is_tombstone(value) {
                    _found = true;
                    break;
                }
                iter.next().unwrap();
            }
            stats.record_read(start.elapsed().as_nanos() as u64);
        } else if op_type < config.point_select_pct + config.update_pct {
            // Update
            let row_id = rng() % config.table_size as u64;
            let key = make_key(table_id, row_id);
            let value = make_value(row_id, config.row_size);

            let mut batch = WriteBatch::new();
            batch.put(key, value);
            let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
            batch.set_commit_ts(ts);

            if engine.write_batch(batch).is_ok() {
                stats.record_write("update", start.elapsed().as_nanos() as u64);
            } else {
                stats.record_error();
            }
        } else if op_type < config.point_select_pct + config.update_pct + config.delete_pct {
            // Delete
            let row_id = rng() % config.table_size as u64;
            let key = make_key(table_id, row_id);

            let mut batch = WriteBatch::new();
            batch.delete(key);
            let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
            batch.set_commit_ts(ts);

            if engine.write_batch(batch).is_ok() {
                stats.record_write("delete", start.elapsed().as_nanos() as u64);
            } else {
                stats.record_error();
            }
        } else {
            // Insert
            let row_id = next_row_id.fetch_add(1, Ordering::SeqCst);
            let key = make_key(table_id, row_id);
            let value = make_value(row_id, config.row_size);

            let mut batch = WriteBatch::new();
            batch.put(key, value);
            let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
            batch.set_commit_ts(ts);

            if engine.write_batch(batch).is_ok() {
                stats.record_write("insert", start.elapsed().as_nanos() as u64);
            } else {
                stats.record_error();
            }
        }
    }
}

// ============================================================================
// Benchmark Runner
// ============================================================================

fn run_benchmark<E: StorageEngine + Default + 'static>(engine_name: &str, args: &Args) {
    println!("\n{}", "=".repeat(70));
    println!("Engine: {engine_name}");
    println!("{}", "=".repeat(70));
    println!();

    let config = WorkloadConfig {
        tables: args.tables,
        table_size: args.table_size,
        row_size: args.row_size,
        point_select_pct: args.point_select_pct,
        update_pct: args.update_pct,
        delete_pct: args.delete_pct,
        insert_pct: args.insert_pct,
    };

    // Create engine
    let engine = Arc::new(E::default());
    let ts_counter = Arc::new(AtomicU64::new(1));
    let next_row_id = Arc::new(AtomicU64::new(
        (args.table_size * args.tables as usize) as u64,
    ));
    let stats = Arc::new(Stats::new());
    let running = Arc::new(AtomicBool::new(true));

    // Populate data
    populate(engine.as_ref(), &config, &ts_counter);

    println!();
    println!("Running benchmark:");
    println!("  Threads: {}", args.threads);
    println!("  Duration: {}s", args.time);
    println!("  Tables: {}", args.tables);
    println!("  Table size: {}", args.table_size);
    println!();

    // Start worker threads
    let mut handles = Vec::with_capacity(args.threads);
    let test_start = Instant::now();

    for tid in 0..args.threads {
        let engine = Arc::clone(&engine);
        let ts_counter = Arc::clone(&ts_counter);
        let next_row_id = Arc::clone(&next_row_id);
        let stats = Arc::clone(&stats);
        let running = Arc::clone(&running);
        let config = config.clone();

        handles.push(std::thread::spawn(move || {
            worker_thread(
                engine.as_ref(),
                &config,
                &ts_counter,
                &next_row_id,
                &stats,
                &running,
                tid,
            );
        }));
    }

    // Progress reporting
    let mut last_total = 0usize;
    let mut last_time = Instant::now();

    println!(
        "[ secs ] thds: {} tps: {:>10} reads: {:>10} writes: {:>10} latency (ms): {:>8}",
        args.threads, "tps", "reads", "writes", "avg"
    );
    println!("{}", "-".repeat(90));

    for sec in 1..=args.time {
        std::thread::sleep(Duration::from_secs(args.report_interval));

        let now = Instant::now();
        let elapsed = now.duration_since(last_time).as_secs_f64();
        let current_total = stats.total_txns();
        let tps = (current_total - last_total) as f64 / elapsed;

        println!(
            "[ {:>4}s ] thds: {:>3} tps: {:>10.0} reads: {:>10} writes: {:>10} latency (ms): {:>8.3}",
            sec,
            args.threads,
            tps,
            stats.reads.load(Ordering::Relaxed),
            stats.writes.load(Ordering::Relaxed),
            stats.avg_latency_ms(),
        );

        last_total = current_total;
        last_time = now;
    }

    // Stop workers
    running.store(false, Ordering::SeqCst);
    for h in handles {
        h.join().unwrap();
    }

    let total_time = test_start.elapsed();

    // Print final results
    println!();
    println!("{}", "=".repeat(70));
    println!("RESULTS FOR: {engine_name}");
    println!("{}", "=".repeat(70));
    println!();

    println!("SQL statistics:");
    println!("    queries performed:");
    println!(
        "        read:                            {:>12}",
        stats.point_selects.load(Ordering::Relaxed)
    );
    println!(
        "        write:                           {:>12}",
        stats.writes.load(Ordering::Relaxed)
    );
    println!(
        "            update:                      {:>12}",
        stats.updates.load(Ordering::Relaxed)
    );
    println!(
        "            delete:                      {:>12}",
        stats.deletes.load(Ordering::Relaxed)
    );
    println!(
        "            insert:                      {:>12}",
        stats.inserts.load(Ordering::Relaxed)
    );
    println!(
        "        other:                           {:>12}",
        stats.other.load(Ordering::Relaxed)
    );
    println!(
        "        total:                           {:>12}",
        stats.total_txns()
    );
    println!(
        "    transactions:                        {:>12} ({:.2} per sec.)",
        stats.total_txns(),
        stats.total_txns() as f64 / total_time.as_secs_f64()
    );
    println!(
        "    errors:                              {:>12}",
        stats.errors.load(Ordering::Relaxed)
    );

    println!();
    println!("General statistics:");
    println!(
        "    total time:                          {:>12.4}s",
        total_time.as_secs_f64()
    );
    println!(
        "    total number of events:              {:>12}",
        stats.total_txns()
    );

    println!();
    println!("Latency (ms):");
    println!(
        "         min:                            {:>12.3}",
        stats.min_latency_ms()
    );
    println!(
        "         avg:                            {:>12.3}",
        stats.avg_latency_ms()
    );
    println!(
        "         max:                            {:>12.3}",
        stats.max_latency_ms()
    );

    println!();
    println!("Threads fairness:");
    println!(
        "    events (avg/stddev):          {:.4}/{:.4}",
        stats.total_txns() as f64 / args.threads as f64,
        0.0
    );
    println!(
        "    execution time (avg/stddev):  {:.4}/{:.4}",
        total_time.as_secs_f64(),
        0.0
    );

    // Calculate throughput
    let tps = stats.total_txns() as f64 / total_time.as_secs_f64();
    let read_tps = stats.reads.load(Ordering::Relaxed) as f64 / total_time.as_secs_f64();
    let write_tps = stats.writes.load(Ordering::Relaxed) as f64 / total_time.as_secs_f64();

    println!();
    println!("Throughput:");
    println!("    transactions/sec (TPS):              {tps:>12.2}");
    println!("    reads/sec:                           {read_tps:>12.2}");
    println!("    writes/sec:                          {write_tps:>12.2}");
    println!();
}

impl Clone for WorkloadConfig {
    fn clone(&self) -> Self {
        Self {
            tables: self.tables,
            table_size: self.table_size,
            row_size: self.row_size,
            point_select_pct: self.point_select_pct,
            update_pct: self.update_pct,
            delete_pct: self.delete_pct,
            insert_pct: self.insert_pct,
        }
    }
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    let args = Args::parse();

    println!();
    println!("TiSQL Sysbench Simulator v1.0");
    println!("=============================");
    println!();
    println!("Configuration:");
    println!("  Threads:        {}", args.threads);
    println!("  Duration:       {}s", args.time);
    println!("  Tables:         {}", args.tables);
    println!("  Table size:     {} rows", args.table_size);
    println!("  Row size:       {} bytes", args.row_size);
    println!();
    println!("Workload distribution:");
    println!("  Point selects:  {}%", args.point_select_pct);
    println!("  Updates:        {}%", args.update_pct);
    println!("  Deletes:        {}%", args.delete_pct);
    println!("  Inserts:        {}%", args.insert_pct);

    // Validate percentages
    let total_pct = args.point_select_pct + args.update_pct + args.delete_pct + args.insert_pct;
    if total_pct != 100 {
        eprintln!("\nError: Workload percentages must sum to 100 (got {total_pct})");
        std::process::exit(1);
    }

    run_benchmark::<VersionedMemTableEngine>("VersionedMemTableEngine", &args);

    println!();
    println!("Benchmark complete!");
}
