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

use clap::Parser as ClapParser;
use tisql::util::LogLevel;
use tisql::{log_error, log_info};
use tisql::{
    Database, DatabaseConfig, MySqlServer, RuntimeThreadOverrides, RuntimeThreads,
    MYSQL_DEFAULT_PORT,
};

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

    let encoded_result_batch = if args.enable_encoded_result_batch {
        true
    } else if args.disable_encoded_result_batch {
        false
    } else {
        DatabaseConfig::default().enable_encoded_result_batch
    };

    // Open database with persistence
    let db_config = DatabaseConfig::with_data_dir(&args.data_dir)
        .with_runtime_threads(runtime_overrides)
        .with_encoded_result_batch(encoded_result_batch);
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

        if let Err(e) = server.run().await {
            log_error!("Server error: {}", e);
            eprintln!("Server error: {e}");
        }
    });
}
