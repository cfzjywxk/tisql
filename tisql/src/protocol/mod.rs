// Protocol module - MySQL wire protocol implementation

pub mod mysql;

use std::net::SocketAddr;
use std::sync::Arc;

use opensrv_mysql::AsyncMysqlIntermediary;
use tokio::net::TcpListener;

use crate::{log_debug, log_error, log_info, Database};
use mysql::MySqlBackend;

/// Default MySQL protocol port (TiDB style)
pub const MYSQL_DEFAULT_PORT: u16 = 4000;

/// MySQL protocol server
pub struct MySqlServer {
    db: Arc<Database>,
    addr: SocketAddr,
}

impl MySqlServer {
    /// Create a new MySQL server
    pub fn new(db: Arc<Database>, addr: SocketAddr) -> Self {
        Self { db, addr }
    }

    /// Run the MySQL server
    pub async fn run(&self) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        log_info!("TiSQL server listening on {}", self.addr);

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let db = Arc::clone(&self.db);

            log_info!("New connection from {}", peer_addr);

            tokio::spawn(async move {
                let backend = MySqlBackend::new(db);
                // Split TCP stream into read and write halves
                let (r, w) = stream.into_split();
                if let Err(e) = AsyncMysqlIntermediary::run_on(backend, r, w).await {
                    log_error!("Connection error from {}: {}", peer_addr, e);
                }
                log_debug!("Connection closed: {}", peer_addr);
            });
        }
    }
}
