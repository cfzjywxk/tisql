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
pub(crate) mod point_get_short;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::{future, future::Future};

use opensrv_mysql::AsyncMysqlIntermediary;
use tokio::net::TcpListener;
use tokio::task::JoinSet;

use crate::{log_error, log_info, Database};
use mysql::MySqlBackend;

/// Default MySQL protocol port (TiDB style)
pub const MYSQL_DEFAULT_PORT: u16 = 4000;

/// MySQL protocol server
pub struct MySqlServer {
    db: Arc<Database>,
    addr: SocketAddr,
}

fn is_transient_accept_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::ConnectionAborted
    ) || error.raw_os_error().is_some_and(is_transient_accept_errno)
}

#[cfg(unix)]
fn is_transient_accept_errno(errno: i32) -> bool {
    errno == libc::EMFILE
        || errno == libc::ENFILE
        || errno == libc::ENOBUFS
        || errno == libc::ENOMEM
}

#[cfg(not(unix))]
fn is_transient_accept_errno(_errno: i32) -> bool {
    false
}

impl MySqlServer {
    /// Create a new MySQL server.
    pub fn new(db: Arc<Database>, addr: SocketAddr) -> Self {
        Self { db, addr }
    }

    /// Run the MySQL server
    pub async fn run(&self) -> std::io::Result<()> {
        self.run_until_shutdown(future::pending::<()>()).await
    }

    /// Run the MySQL server until a shutdown signal future resolves.
    pub async fn run_until_shutdown<F>(&self, shutdown_signal: F) -> std::io::Result<()>
    where
        F: Future<Output = ()>,
    {
        let listener = TcpListener::bind(self.addr).await?;
        log_info!("TiSQL server listening on {}", self.addr);
        tokio::pin!(shutdown_signal);
        let mut sessions = JoinSet::new();

        let (result, graceful_shutdown) = loop {
            tokio::select! {
                _ = &mut shutdown_signal => {
                    log_info!("Shutdown signal received, stop accepting new MySQL connections");
                    break (Ok(()), true);
                }
                accept_result = listener.accept() => {
                    let (stream, peer_addr) = match accept_result {
                        Ok(v) => v,
                        Err(e) if is_transient_accept_error(&e) => {
                            log_error!("Transient listener accept error (will retry): {}", e);
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                        Err(e) => break (Err(e), false),
                    };
                    // Disable Nagle's algorithm — the MySQL protocol sends many small
                    // packets (column defs, row data, EOF markers).  Without this,
                    // Nagle + delayed-ACK interaction adds ~40 ms per round-trip.
                    if let Err(e) = stream.set_nodelay(true) {
                        log_error!("Failed to set TCP_NODELAY for {}: {}", peer_addr, e);
                        continue;
                    }
                    let db = Arc::clone(&self.db);

                    log_info!("New connection from {}", peer_addr);

                    sessions.spawn(async move {
                        let backend = MySqlBackend::new(db);
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
                Some(join_result) = sessions.join_next(), if !sessions.is_empty() => {
                    if let Err(join_err) = join_result {
                        if !join_err.is_cancelled() {
                            log_error!("MySQL session task join error: {}", join_err);
                        }
                    }
                }
            }
        };

        // Graceful shutdown waits for active sessions to end naturally so in-flight
        // transactions can finish and return a definitive result to clients.
        if graceful_shutdown && !sessions.is_empty() {
            log_info!(
                "Waiting for {} active MySQL sessions to exit",
                sessions.len()
            );
        }
        if !graceful_shutdown {
            sessions.abort_all();
        }
        while let Some(join_result) = sessions.join_next().await {
            if let Err(join_err) = join_result {
                if !join_err.is_cancelled() {
                    log_error!(
                        "MySQL session task join error during shutdown: {}",
                        join_err
                    );
                }
            }
        }

        log_info!("MySQL listener stopped");
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tablet::TabletId;
    use crate::{Database, DatabaseConfig, QueryResult};
    use std::collections::BTreeMap;
    use tokio::sync::oneshot;

    fn snapshot_flushed_lsn_by_tablet(db: &Database) -> BTreeMap<TabletId, u64> {
        let snapshot = db.engine_status_snapshot();
        let mut lsn_by_tablet = BTreeMap::new();

        for tablet in &snapshot.tablets {
            let engine = db
                .tablet_manager()
                .get_tablet(tablet.tablet_id)
                .expect("tablet in status snapshot must be mounted");
            let version_flushed = engine.current_version().flushed_lsn();
            assert_eq!(
                tablet.lsm.flushed_lsn, version_flushed,
                "status flushed_lsn must match engine version for tablet {:?}",
                tablet.tablet_id
            );
            lsn_by_tablet.insert(tablet.tablet_id, tablet.lsm.flushed_lsn);
        }

        lsn_by_tablet
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_graceful_shutdown_reopen_keeps_engine_status_lsn_consistent() {
        let dir = tempfile::tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path())
            .with_engine_status_report_interval_secs(0)
            .with_engine_status_top_n_tablets(0);

        let db = Arc::new(Database::open(config.clone()).unwrap());
        db.execute_query("CREATE TABLE shutdown_lsn (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        db.execute_query("INSERT INTO shutdown_lsn VALUES (1, 10)")
            .await
            .unwrap();
        db.execute_query("INSERT INTO shutdown_lsn VALUES (2, 20)")
            .await
            .unwrap();
        let lsn_before_shutdown = db.tablet_manager().shared_lsn_provider().current_lsn();

        let before_map = snapshot_flushed_lsn_by_tablet(db.as_ref());
        assert!(
            !before_map.is_empty(),
            "status snapshot should include at least system tablet"
        );

        let server = MySqlServer::new(
            Arc::clone(&db),
            "127.0.0.1:0".parse().expect("valid socket addr"),
        );
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            server
                .run_until_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let _ = shutdown_tx.send(());
        let server_result = server_task.await.expect("server task join should succeed");
        assert!(
            server_result.is_ok(),
            "server should stop cleanly after shutdown signal: {server_result:?}"
        );

        db.close().await.unwrap();
        drop(db);

        let db2 = Database::open(config).unwrap();
        match db2
            .execute_query("SELECT v FROM shutdown_lsn WHERE id = 2")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1);
                assert_eq!(data[0][0], "20");
            }
            other => panic!("expected row result after reopen, got {other:?}"),
        }
        let lsn_after_reopen_before_new_write =
            db2.tablet_manager().shared_lsn_provider().current_lsn();
        assert!(
            lsn_after_reopen_before_new_write >= lsn_before_shutdown,
            "LSN provider must not regress on reopen: before_shutdown={lsn_before_shutdown} after_reopen={lsn_after_reopen_before_new_write}"
        );

        db2.execute_query("INSERT INTO shutdown_lsn VALUES (3, 30)")
            .await
            .unwrap();
        let lsn_after_new_write = db2.tablet_manager().shared_lsn_provider().current_lsn();
        assert!(
            lsn_after_new_write > lsn_before_shutdown,
            "new write after reopen should advance LSN beyond pre-shutdown frontier: before_shutdown={lsn_before_shutdown} after_new_write={lsn_after_new_write}"
        );

        let after_map = snapshot_flushed_lsn_by_tablet(&db2);
        assert!(
            !after_map.is_empty(),
            "status snapshot after reopen should include mounted tablets"
        );
        for (tablet_id, before_lsn) in &before_map {
            let after_lsn = after_map
                .get(tablet_id)
                .copied()
                .expect("tablet from pre-shutdown snapshot should exist after reopen");
            assert!(
                after_lsn >= *before_lsn,
                "tablet {tablet_id:?} flushed_lsn regressed across graceful shutdown/reopen: before={before_lsn} after={after_lsn}"
            );
        }

        let min_from_snapshot = after_map
            .values()
            .copied()
            .min()
            .expect("snapshot map must have at least one tablet");
        assert_eq!(
            db2.tablet_manager().min_flushed_lsn(),
            Some(min_from_snapshot)
        );

        db2.close().await.unwrap();
    }
}
