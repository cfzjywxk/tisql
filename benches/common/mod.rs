#![allow(dead_code)]

use std::path::Path;
use std::time::Duration;

use tempfile::TempDir;
use tisql::util::{init_logger_from_env, LogLevel};
use tisql::{Database, DatabaseConfig, RuntimeThreadOverrides};
use tokio::runtime::{Builder, Runtime};

pub fn bench_runtime() -> Runtime {
    let runtime = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("bench runtime should build");
    runtime.block_on(async {
        init_logger_from_env(LogLevel::Warn);
    });
    runtime
}

pub fn bench_db_config(path: &Path) -> DatabaseConfig {
    let mut config =
        DatabaseConfig::with_data_dir(path).with_runtime_threads(RuntimeThreadOverrides {
            protocol: Some(1),
            worker: Some(1),
            background: Some(1),
            io: Some(1),
        });
    config.engine_status_report_interval_secs = 0;
    config.group_commit_delay = Duration::ZERO;
    config.group_commit_no_delay_count = 1;
    config
}

pub fn open_bench_db() -> (TempDir, Database) {
    let dir = TempDir::new().expect("temp dir should build");
    let db = Database::open(bench_db_config(dir.path())).expect("database should open");
    (dir, db)
}

pub async fn seed_sql_table(db: &Database, table: &str, rows: usize) {
    db.execute_query(&format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, value INT)"
    ))
    .await
    .expect("table should be created");

    for id in 0..rows {
        db.execute_query(&format!("INSERT INTO {table} VALUES ({id}, {})", id * 10))
            .await
            .expect("seed insert should succeed");
    }
}
