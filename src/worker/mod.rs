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

// Worker pool module - separates database work from network IO using yatp
//
// Architecture:
// - Tokio runtime handles network IO (TCP accept, MySQL protocol)
// - yatp FuturePool handles CPU-bound database work (parse, bind, execute)
// - Communication via tokio::sync::oneshot channels

use std::sync::Arc;

use tokio::sync::oneshot;
use yatp::pool::Remote;
use yatp::task::future::TaskCell;

use crate::error::{Result, TiSqlError};
use crate::{Database, QueryResult};

/// Configuration for the worker thread pool
pub struct WorkerPoolConfig {
    /// Name prefix for worker threads (e.g., "tisql-worker")
    pub name: String,
    /// Minimum number of worker threads
    pub min_threads: usize,
    /// Maximum number of worker threads
    pub max_threads: usize,
}

impl Default for WorkerPoolConfig {
    fn default() -> Self {
        let cpus = num_cpus::get();
        Self {
            name: "tisql-worker".to_string(),
            min_threads: 4.min(cpus),
            max_threads: cpus,
        }
    }
}

/// Thread pool for executing database work off the network IO threads
pub struct WorkerPool {
    remote: Remote<TaskCell>,
    // Keep pool alive - dropping it shuts down the threads
    _pool: yatp::ThreadPool<TaskCell>,
}

impl WorkerPool {
    /// Create a new worker pool with the given configuration
    pub fn new(config: WorkerPoolConfig) -> Self {
        let pool = yatp::Builder::new(&config.name)
            .min_thread_count(config.min_threads)
            .max_thread_count(config.max_threads)
            .build_future_pool();

        let remote = pool.remote().clone();

        Self {
            remote,
            _pool: pool,
        }
    }

    /// Handle MySQL protocol query text on a worker thread.
    ///
    /// This method:
    /// 1. Creates a oneshot channel for the result
    /// 2. Spawns the database work onto the yatp pool
    /// 3. Awaits the result asynchronously (doesn't block tokio)
    pub async fn handle_mp_query(&self, db: Arc<Database>, query: String) -> Result<QueryResult> {
        let (tx, rx) = oneshot::channel();

        self.remote.spawn(async move {
            let result = db.handle_mp_query(&query);
            // Ignore send error - receiver may have been dropped if connection closed
            let _ = tx.send(result);
        });

        // Await the result from the worker thread
        rx.await
            .map_err(|_| TiSqlError::Internal("Worker task dropped".into()))?
    }
}
