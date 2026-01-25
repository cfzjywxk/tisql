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

// Protocol module - MySQL wire protocol implementation

pub mod mysql;

use std::net::SocketAddr;
use std::sync::Arc;

use opensrv_mysql::AsyncMysqlIntermediary;
use tokio::net::TcpListener;

use crate::worker::WorkerPool;
use crate::{log_debug, log_error, log_info, Database};
use mysql::MySqlBackend;

/// Default MySQL protocol port (TiDB style)
pub const MYSQL_DEFAULT_PORT: u16 = 4000;

/// MySQL protocol server
pub struct MySqlServer {
    db: Arc<Database>,
    addr: SocketAddr,
    worker_pool: Arc<WorkerPool>,
}

impl MySqlServer {
    /// Create a new MySQL server
    pub fn new(db: Arc<Database>, addr: SocketAddr, worker_pool: Arc<WorkerPool>) -> Self {
        Self {
            db,
            addr,
            worker_pool,
        }
    }

    /// Run the MySQL server
    pub async fn run(&self) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        log_info!("TiSQL server listening on {}", self.addr);

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let db = Arc::clone(&self.db);
            let worker_pool = Arc::clone(&self.worker_pool);

            log_info!("New connection from {}", peer_addr);

            tokio::spawn(async move {
                let backend = MySqlBackend::new(db, worker_pool);
                let session_id = backend.session_id();
                log_info!(
                    "Session {} established for connection from {}",
                    session_id,
                    peer_addr
                );

                // Split TCP stream into read and write halves
                let (r, w) = stream.into_split();
                if let Err(e) = AsyncMysqlIntermediary::run_on(backend, r, w).await {
                    log_error!(
                        "Session {} connection error from {}: {}",
                        session_id,
                        peer_addr,
                        e
                    );
                }
                log_info!("Session {} closed ({})", session_id, peer_addr);
            });
        }
    }
}
