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

// Use jemalloc as the global allocator on Unix (like TiKV)
// This provides better performance for concurrent workloads
#[cfg(all(unix, feature = "jemalloc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser as ClapParser;
use tisql::tablet::{
    DEFAULT_BLOOM_BITS_PER_KEY, DEFAULT_CACHE_TOTAL_RATIO, DEFAULT_L0_COMPACTION_TRIGGER,
    DEFAULT_L0_SLOWDOWN_TRIGGER, DEFAULT_L0_STOP_TRIGGER, DEFAULT_MAX_LEVELS,
    DEFAULT_READER_CACHE_MAX_ENTRIES, DEFAULT_READER_CACHE_SHARDS,
    DEFAULT_SCAN_FILL_CACHE_THRESHOLD_BLOCKS,
};
use tisql::util::LogLevel;
use tisql::{log_error, log_info};
use tisql::{
    Database, DatabaseConfig, MySqlServer, RuntimeThreadOverrides, RuntimeThreads,
    MYSQL_DEFAULT_PORT,
};

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> std::io::Result<&'static str> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;

    tokio::select! {
        _ = sigint.recv() => Ok("SIGINT"),
        _ = sigterm.recv() => Ok("SIGTERM"),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> std::io::Result<&'static str> {
    tokio::signal::ctrl_c().await?;
    Ok("Ctrl+C")
}

#[derive(ClapParser, Debug)]
#[command(name = "tisql")]
#[command(about = "TiSQL - A minimal SQL database in Rust")]
#[command(version = "0.1.0")]
struct Args {
    /// Host address to bind to
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    /// Port to listen on
    #[arg(short = 'P', long, default_value_t = MYSQL_DEFAULT_PORT)]
    port: u16,

    /// Data directory for persistence
    #[arg(short = 'D', long, default_value = "data")]
    data_dir: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short = 'L', long, default_value = "info")]
    log_level: String,

    /// Protocol runtime worker threads
    #[arg(long)]
    protocol_threads: Option<usize>,

    /// Query worker runtime threads
    #[arg(long)]
    worker_threads: Option<usize>,

    /// Background runtime threads
    #[arg(long)]
    bg_threads: Option<usize>,

    /// I/O runtime threads
    #[arg(long)]
    io_threads: Option<usize>,

    /// Per-tablet flush threads (auto-sized from CPU count when omitted)
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    flush_threads: Option<u32>,

    /// Enable adaptive encoded result batches (phase 3 optimization, experimental)
    #[arg(
        long,
        default_value_t = false,
        conflicts_with = "disable_encoded_result_batch"
    )]
    enable_encoded_result_batch: bool,

    /// Disable adaptive encoded result batches
    #[arg(
        long,
        default_value_t = false,
        conflicts_with = "enable_encoded_result_batch"
    )]
    disable_encoded_result_batch: bool,

    /// Enable point-get short path for eligible SELECT queries.
    #[arg(
        long,
        default_value_t = false,
        conflicts_with = "disable_point_get_short_path"
    )]
    enable_point_get_short_path: bool,

    /// Disable point-get short path for eligible SELECT queries.
    #[arg(
        long,
        default_value_t = false,
        conflicts_with = "enable_point_get_short_path"
    )]
    disable_point_get_short_path: bool,

    /// Enable bloom filter for SST point-lookups.
    #[arg(long, default_value_t = false, conflicts_with = "disable_bloom")]
    enable_bloom: bool,

    /// Disable bloom filter for SST point-lookups.
    #[arg(long, default_value_t = false, conflicts_with = "enable_bloom")]
    disable_bloom: bool,

    /// Bloom filter bits per key.
    #[arg(
        long,
        default_value_t = DEFAULT_BLOOM_BITS_PER_KEY,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    bloom_bits_per_key: u32,

    /// Enable shared block cache.
    #[arg(
        long,
        default_value_t = false,
        conflicts_with = "disable_shared_block_cache"
    )]
    enable_shared_block_cache: bool,

    /// Disable shared block cache.
    #[arg(
        long,
        default_value_t = false,
        conflicts_with = "enable_shared_block_cache"
    )]
    disable_shared_block_cache: bool,

    /// Enable reader cache.
    #[arg(long, default_value_t = false, conflicts_with = "disable_reader_cache")]
    enable_reader_cache: bool,

    /// Disable reader cache.
    #[arg(long, default_value_t = false, conflicts_with = "enable_reader_cache")]
    disable_reader_cache: bool,

    /// Enable row cache.
    #[arg(long, default_value_t = false, conflicts_with = "disable_row_cache")]
    enable_row_cache: bool,

    /// Disable row cache.
    #[arg(long, default_value_t = false, conflicts_with = "enable_row_cache")]
    disable_row_cache: bool,

    /// Enable cache-fill for short foreground scans.
    #[arg(
        long,
        default_value_t = false,
        conflicts_with = "disable_scan_fill_cache"
    )]
    enable_scan_fill_cache: bool,

    /// Disable cache-fill for foreground scans.
    #[arg(
        long,
        default_value_t = false,
        conflicts_with = "enable_scan_fill_cache"
    )]
    disable_scan_fill_cache: bool,

    /// Short-scan cache-fill threshold in estimated blocks.
    #[arg(
        long,
        default_value_t = DEFAULT_SCAN_FILL_CACHE_THRESHOLD_BLOCKS
    )]
    scan_fill_cache_threshold_blocks: usize,

    /// Fraction of machine RAM used as total cache budget.
    #[arg(long, default_value_t = DEFAULT_CACHE_TOTAL_RATIO)]
    cache_total_ratio: f64,

    /// Reader-cache entry cap.
    #[arg(
        long,
        default_value_t = DEFAULT_READER_CACHE_MAX_ENTRIES
    )]
    reader_cache_max_entries: usize,

    /// Reader-cache shards (0 = auto, or power-of-two in [1, 16]).
    #[arg(long, default_value_t = DEFAULT_READER_CACHE_SHARDS)]
    reader_cache_shards: usize,

    /// L0 file count that triggers compaction.
    #[arg(
        long,
        default_value_t = DEFAULT_L0_COMPACTION_TRIGGER as u32,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    l0_compaction_trigger: u32,

    /// L0 file count that starts write slowdown.
    #[arg(
        long,
        default_value_t = DEFAULT_L0_SLOWDOWN_TRIGGER as u32,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    l0_slowdown_trigger: u32,

    /// L0 file count that hard-stops writes.
    #[arg(
        long,
        default_value_t = DEFAULT_L0_STOP_TRIGGER as u32,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    l0_stop_trigger: u32,

    /// Number of LSM levels (kept intentionally small for per-tablet LSMs).
    #[arg(
        long,
        default_value_t = DEFAULT_MAX_LEVELS as u32,
        value_parser = clap::value_parser!(u32).range(2..)
    )]
    max_levels: u32,

    /// Engine status reporter interval in seconds (0 disables).
    #[arg(long, default_value_t = 60)]
    engine_status_report_interval_secs: u64,

    /// Maximum tablets emitted in periodic status reports (0 = all).
    #[arg(long, default_value_t = 20)]
    engine_status_top_n_tablets: usize,

    /// Group commit delay window in microseconds (0 disables delay).
    #[arg(long, default_value_t = 0)]
    group_commit_delay_us: u64,

    /// Skip group commit delay once ready batch reaches this size.
    #[arg(long, default_value_t = 16, value_parser = clap::value_parser!(u32).range(1..))]
    group_commit_no_delay_count: u32,

    /// Spin iterations before parking clog writer on empty buffer.
    #[arg(long, default_value_t = 0)]
    clog_spin_before_park_iters: u32,
}

fn main() {
    let args = Args::parse();

    let runtime_overrides = RuntimeThreadOverrides {
        protocol: args.protocol_threads,
        worker: args.worker_threads,
        background: args.bg_threads,
        io: args.io_threads,
    };
    let runtime_threads = runtime_overrides.apply(RuntimeThreads::detect());

    let addr: SocketAddr = format!("{}:{}", args.host, args.port)
        .parse()
        .expect("Invalid address");

    let encoded_result_batch = resolve_encoded_result_batch_setting(
        args.enable_encoded_result_batch,
        args.disable_encoded_result_batch,
    );
    let point_get_short_path = resolve_flag_setting(
        args.enable_point_get_short_path,
        args.disable_point_get_short_path,
        DatabaseConfig::default().enable_point_get_short_path,
    );
    let bloom_enabled = resolve_bloom_enabled_setting(args.enable_bloom, args.disable_bloom);
    let shared_block_cache_enabled = resolve_flag_setting(
        args.enable_shared_block_cache,
        args.disable_shared_block_cache,
        DatabaseConfig::default().shared_block_cache_enabled,
    );
    let reader_cache_enabled = resolve_flag_setting(
        args.enable_reader_cache,
        args.disable_reader_cache,
        DatabaseConfig::default().reader_cache_enabled,
    );
    let row_cache_enabled = resolve_flag_setting(
        args.enable_row_cache,
        args.disable_row_cache,
        DatabaseConfig::default().row_cache_enabled,
    );
    let scan_fill_cache = resolve_flag_setting(
        args.enable_scan_fill_cache,
        args.disable_scan_fill_cache,
        DatabaseConfig::default().scan_fill_cache,
    );

    // Open database with persistence
    let mut db_config = DatabaseConfig::with_data_dir(&args.data_dir)
        .with_runtime_threads(runtime_overrides)
        .with_encoded_result_batch(encoded_result_batch)
        .with_point_get_short_path(point_get_short_path)
        .with_bloom_enabled(bloom_enabled)
        .with_bloom_bits_per_key(args.bloom_bits_per_key)
        .with_shared_block_cache_enabled(shared_block_cache_enabled)
        .with_reader_cache_enabled(reader_cache_enabled)
        .with_row_cache_enabled(row_cache_enabled)
        .with_scan_fill_cache(scan_fill_cache)
        .with_scan_fill_cache_threshold_blocks(args.scan_fill_cache_threshold_blocks)
        .with_cache_total_ratio(args.cache_total_ratio)
        .with_reader_cache_max_entries(args.reader_cache_max_entries)
        .with_reader_cache_shards(args.reader_cache_shards)
        .with_l0_compaction_trigger(args.l0_compaction_trigger as usize)
        .with_l0_slowdown_trigger(args.l0_slowdown_trigger as usize)
        .with_l0_stop_trigger(args.l0_stop_trigger as usize)
        .with_max_levels(args.max_levels as usize)
        .with_engine_status_report_interval_secs(args.engine_status_report_interval_secs)
        .with_engine_status_top_n_tablets(args.engine_status_top_n_tablets)
        .with_group_commit_delay(Duration::from_micros(args.group_commit_delay_us))
        .with_group_commit_no_delay_count(args.group_commit_no_delay_count as usize)
        .with_clog_spin_before_park_iters(args.clog_spin_before_park_iters);
    if let Some(flush_threads) = args.flush_threads {
        db_config = db_config.with_flush_threads(flush_threads as usize);
    }
    let db = match Database::open(db_config) {
        Ok(db) => Arc::new(db),
        Err(e) => {
            eprintln!("Failed to open database: {e}");
            std::process::exit(1);
        }
    };

    let server = MySqlServer::new(Arc::clone(&db), addr);

    println!("TiSQL v0.1.0 - MySQL Protocol Server");
    println!("Data directory: {}", args.data_dir);
    println!("Listening on {}:{}", args.host, args.port);
    println!(
        "Runtime threads: protocol={}, worker={}, bg={}, io={}",
        runtime_threads.protocol,
        runtime_threads.worker,
        runtime_threads.background,
        runtime_threads.io
    );
    println!(
        "Encoded result batches: {}",
        if encoded_result_batch {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!(
        "Point-get short path: {}",
        if point_get_short_path {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!(
        "Bloom filter: {} (bits/key={})",
        if bloom_enabled { "enabled" } else { "disabled" },
        args.bloom_bits_per_key
    );
    println!(
        "Cache suite: block={}, reader={}, row={}, scan_fill={} (threshold_blocks={}, ratio={}, reader_cap={}, reader_shards={})",
        if shared_block_cache_enabled {
            "enabled"
        } else {
            "disabled"
        },
        if reader_cache_enabled {
            "enabled"
        } else {
            "disabled"
        },
        if row_cache_enabled {
            "enabled"
        } else {
            "disabled"
        },
        if scan_fill_cache {
            "enabled"
        } else {
            "disabled"
        },
        args.scan_fill_cache_threshold_blocks,
        args.cache_total_ratio,
        args.reader_cache_max_entries,
        args.reader_cache_shards
    );
    println!(
        "LSM flow control: levels={} l0(compact/slowdown/stop)=({}/{}/{})",
        args.max_levels, args.l0_compaction_trigger, args.l0_slowdown_trigger, args.l0_stop_trigger
    );
    println!(
        "Engine status reporter: interval={}s top_n={}",
        args.engine_status_report_interval_secs, args.engine_status_top_n_tablets
    );
    println!(
        "Clog writer tuning: delay_us={}, no_delay_count={}, spin_before_park_iters={}",
        args.group_commit_delay_us,
        args.group_commit_no_delay_count,
        args.clog_spin_before_park_iters
    );
    println!(
        "Connect with: mysql -h{} -P{} -uroot test",
        args.host, args.port
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(runtime_threads.protocol)
        .enable_all()
        .build()
        .expect("Failed to create protocol runtime");
    rt.block_on(async {
        // Initialize async logger within tokio runtime
        let log_level = LogLevel::parse(&args.log_level).unwrap_or(LogLevel::Info);
        tisql::util::init_logger(log_level);

        log_info!("TiSQL server starting on {}", addr);
        let server_result = server
            .run_until_shutdown(async {
                match wait_for_shutdown_signal().await {
                    Ok(signal) => {
                        log_info!("Received {signal}, starting graceful shutdown");
                    }
                    Err(e) => {
                        log_error!(
                            "Failed to wait for shutdown signal: {} (initiating shutdown anyway)",
                            e
                        );
                    }
                }
            })
            .await;

        if let Err(e) = db.close().await {
            log_error!("Database close error: {}", e);
            eprintln!("Database close error: {e}");
        }

        if let Err(e) = server_result {
            log_error!("Server error: {}", e);
            eprintln!("Server error: {e}");
        }
    });
}

fn resolve_encoded_result_batch_setting(enable: bool, disable: bool) -> bool {
    if enable {
        true
    } else if disable {
        false
    } else {
        DatabaseConfig::default().enable_encoded_result_batch
    }
}

fn resolve_bloom_enabled_setting(enable: bool, disable: bool) -> bool {
    resolve_flag_setting(enable, disable, DatabaseConfig::default().bloom_enabled)
}

fn resolve_flag_setting(enable: bool, disable: bool, default_value: bool) -> bool {
    if enable {
        true
    } else if disable {
        false
    } else {
        default_value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoded_result_batch_defaults_to_config() {
        let args = Args::try_parse_from(["tisql"]).unwrap();
        assert_eq!(
            resolve_encoded_result_batch_setting(
                args.enable_encoded_result_batch,
                args.disable_encoded_result_batch
            ),
            DatabaseConfig::default().enable_encoded_result_batch
        );
    }

    #[test]
    fn test_encoded_result_batch_enable_flag() {
        let args = Args::try_parse_from(["tisql", "--enable-encoded-result-batch"]).unwrap();
        assert!(resolve_encoded_result_batch_setting(
            args.enable_encoded_result_batch,
            args.disable_encoded_result_batch
        ));
    }

    #[test]
    fn test_encoded_result_batch_disable_flag() {
        let args = Args::try_parse_from(["tisql", "--disable-encoded-result-batch"]).unwrap();
        assert!(!resolve_encoded_result_batch_setting(
            args.enable_encoded_result_batch,
            args.disable_encoded_result_batch
        ));
    }

    #[test]
    fn test_encoded_result_batch_flags_conflict() {
        let parsed = Args::try_parse_from([
            "tisql",
            "--enable-encoded-result-batch",
            "--disable-encoded-result-batch",
        ]);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_point_get_short_path_defaults_to_config() {
        let args = Args::try_parse_from(["tisql"]).unwrap();
        assert_eq!(
            resolve_flag_setting(
                args.enable_point_get_short_path,
                args.disable_point_get_short_path,
                DatabaseConfig::default().enable_point_get_short_path
            ),
            DatabaseConfig::default().enable_point_get_short_path
        );
    }

    #[test]
    fn test_point_get_short_path_enable_flag() {
        let args = Args::try_parse_from(["tisql", "--enable-point-get-short-path"]).unwrap();
        assert!(resolve_flag_setting(
            args.enable_point_get_short_path,
            args.disable_point_get_short_path,
            false
        ));
    }

    #[test]
    fn test_point_get_short_path_disable_flag() {
        let args = Args::try_parse_from(["tisql", "--disable-point-get-short-path"]).unwrap();
        assert!(!resolve_flag_setting(
            args.enable_point_get_short_path,
            args.disable_point_get_short_path,
            true
        ));
    }

    #[test]
    fn test_point_get_short_path_flags_conflict() {
        let parsed = Args::try_parse_from([
            "tisql",
            "--enable-point-get-short-path",
            "--disable-point-get-short-path",
        ]);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_bloom_defaults_to_config() {
        let args = Args::try_parse_from(["tisql"]).unwrap();
        assert_eq!(
            resolve_bloom_enabled_setting(args.enable_bloom, args.disable_bloom),
            DatabaseConfig::default().bloom_enabled
        );
        assert_eq!(args.bloom_bits_per_key, DEFAULT_BLOOM_BITS_PER_KEY);
    }

    #[test]
    fn test_bloom_enable_flag() {
        let args = Args::try_parse_from(["tisql", "--enable-bloom"]).unwrap();
        assert!(resolve_bloom_enabled_setting(
            args.enable_bloom,
            args.disable_bloom
        ));
    }

    #[test]
    fn test_bloom_disable_flag() {
        let args = Args::try_parse_from(["tisql", "--disable-bloom"]).unwrap();
        assert!(!resolve_bloom_enabled_setting(
            args.enable_bloom,
            args.disable_bloom
        ));
    }

    #[test]
    fn test_bloom_flags_conflict() {
        let parsed = Args::try_parse_from(["tisql", "--enable-bloom", "--disable-bloom"]);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_bloom_bits_per_key_must_be_positive() {
        let parsed = Args::try_parse_from(["tisql", "--bloom-bits-per-key", "0"]);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_cache_flags_defaults_to_config() {
        let args = Args::try_parse_from(["tisql"]).unwrap();
        let defaults = DatabaseConfig::default();
        assert_eq!(
            resolve_flag_setting(
                args.enable_shared_block_cache,
                args.disable_shared_block_cache,
                defaults.shared_block_cache_enabled
            ),
            defaults.shared_block_cache_enabled
        );
        assert_eq!(
            resolve_flag_setting(
                args.enable_reader_cache,
                args.disable_reader_cache,
                defaults.reader_cache_enabled
            ),
            defaults.reader_cache_enabled
        );
        assert_eq!(
            resolve_flag_setting(
                args.enable_row_cache,
                args.disable_row_cache,
                defaults.row_cache_enabled
            ),
            defaults.row_cache_enabled
        );
        assert_eq!(
            resolve_flag_setting(
                args.enable_scan_fill_cache,
                args.disable_scan_fill_cache,
                defaults.scan_fill_cache
            ),
            defaults.scan_fill_cache
        );
        assert_eq!(
            args.scan_fill_cache_threshold_blocks,
            DEFAULT_SCAN_FILL_CACHE_THRESHOLD_BLOCKS
        );
        assert_eq!(args.cache_total_ratio, DEFAULT_CACHE_TOTAL_RATIO);
        assert_eq!(
            args.reader_cache_max_entries,
            DEFAULT_READER_CACHE_MAX_ENTRIES
        );
        assert_eq!(args.reader_cache_shards, DEFAULT_READER_CACHE_SHARDS);
        assert_eq!(
            args.l0_compaction_trigger,
            DEFAULT_L0_COMPACTION_TRIGGER as u32
        );
        assert_eq!(args.l0_slowdown_trigger, DEFAULT_L0_SLOWDOWN_TRIGGER as u32);
        assert_eq!(args.l0_stop_trigger, DEFAULT_L0_STOP_TRIGGER as u32);
        assert_eq!(args.max_levels, DEFAULT_MAX_LEVELS as u32);
        assert_eq!(
            args.engine_status_report_interval_secs,
            defaults.engine_status_report_interval_secs
        );
        assert_eq!(
            args.engine_status_top_n_tablets,
            defaults.engine_status_top_n_tablets
        );
        assert_eq!(args.group_commit_delay_us, 0);
        assert_eq!(args.group_commit_no_delay_count, 16);
        assert_eq!(args.clog_spin_before_park_iters, 0);
    }

    #[test]
    fn test_cache_enable_disable_flags() {
        let args = Args::try_parse_from([
            "tisql",
            "--enable-shared-block-cache",
            "--enable-reader-cache",
            "--enable-row-cache",
            "--enable-scan-fill-cache",
        ])
        .unwrap();
        assert!(resolve_flag_setting(
            args.enable_shared_block_cache,
            args.disable_shared_block_cache,
            false
        ));
        assert!(resolve_flag_setting(
            args.enable_reader_cache,
            args.disable_reader_cache,
            false
        ));
        assert!(resolve_flag_setting(
            args.enable_row_cache,
            args.disable_row_cache,
            false
        ));
        assert!(resolve_flag_setting(
            args.enable_scan_fill_cache,
            args.disable_scan_fill_cache,
            false
        ));
    }

    #[test]
    fn test_shared_block_cache_flags_conflict() {
        let parsed = Args::try_parse_from([
            "tisql",
            "--enable-shared-block-cache",
            "--disable-shared-block-cache",
        ]);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_lsm_flow_control_flags_parse() {
        let args = Args::try_parse_from([
            "tisql",
            "--l0-compaction-trigger",
            "12",
            "--l0-slowdown-trigger",
            "24",
            "--l0-stop-trigger",
            "36",
            "--max-levels",
            "4",
        ])
        .unwrap();
        assert_eq!(args.l0_compaction_trigger, 12);
        assert_eq!(args.l0_slowdown_trigger, 24);
        assert_eq!(args.l0_stop_trigger, 36);
        assert_eq!(args.max_levels, 4);
    }

    #[test]
    fn test_reader_cache_shards_flag_parse() {
        let args = Args::try_parse_from(["tisql", "--reader-cache-shards", "8"]).unwrap();
        assert_eq!(args.reader_cache_shards, 8);
    }

    #[test]
    fn test_flush_threads_flag_parse() {
        let args = Args::try_parse_from(["tisql", "--flush-threads", "3"]).unwrap();
        assert_eq!(args.flush_threads, Some(3));
    }

    #[test]
    fn test_flush_threads_flag_must_be_positive() {
        let parsed = Args::try_parse_from(["tisql", "--flush-threads", "0"]);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_group_commit_no_delay_count_must_be_positive() {
        let parsed = Args::try_parse_from(["tisql", "--group-commit-no-delay-count", "0"]);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_clog_spin_before_park_iters_flag_parse() {
        let args = Args::try_parse_from(["tisql", "--clog-spin-before-park-iters", "128"]).unwrap();
        assert_eq!(args.clog_spin_before_park_iters, 128);
    }
}
