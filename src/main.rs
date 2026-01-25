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

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser as ClapParser;
use tisql::util::LogLevel;
use tisql::{log_error, log_info};
use tisql::{Database, DatabaseConfig, MySqlServer, WorkerPool, WorkerPoolConfig, MYSQL_DEFAULT_PORT};

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

    /// Minimum number of worker threads for database operations
    #[arg(long, default_value_t = 4)]
    worker_min_threads: usize,

    /// Maximum number of worker threads for database operations (default: CPU count)
    #[arg(long)]
    worker_max_threads: Option<usize>,
}

fn main() {
    let args = Args::parse();

    let addr: SocketAddr = format!("{}:{}", args.host, args.port)
        .parse()
        .expect("Invalid address");

    // Create worker pool for database operations
    let worker_config = WorkerPoolConfig {
        name: "tisql-worker".to_string(),
        min_threads: args.worker_min_threads,
        max_threads: args.worker_max_threads.unwrap_or_else(num_cpus::get),
    };
    let worker_pool = Arc::new(WorkerPool::new(worker_config));

    // Open database with persistence
    let db_config = DatabaseConfig::with_data_dir(&args.data_dir);
    let db = match Database::open(db_config) {
        Ok(db) => Arc::new(db),
        Err(e) => {
            eprintln!("Failed to open database: {e}");
            std::process::exit(1);
        }
    };
    let server = MySqlServer::new(db, addr, worker_pool);

    println!("TiSQL v0.1.0 - MySQL Protocol Server");
    println!("Data directory: {}", args.data_dir);
    println!("Listening on {}:{}", args.host, args.port);
    println!(
        "Connect with: mysql -h{} -P{} -uroot test",
        args.host, args.port
    );

    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
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
