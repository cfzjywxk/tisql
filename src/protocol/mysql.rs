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

// MySQL wire protocol implementation using opensrv-mysql
//
// Thin I/O shell: ALL database work (parse, bind, execute, txn control,
// SHOW) is dispatched to the worker runtime via dispatch_full_query.
// Protocol task only handles MySQL wire I/O, session state (SET/USE),
// and @@variables.

use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use opensrv_mysql::{
    AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind, InitWriter, OkResponse,
    ParamParser, QueryResultWriter, StatementMetaWriter, StatusFlags,
};
use tokio::io::AsyncWrite;

use crate::session::{ExecutionCtx, Session};
use crate::types::Value;
use crate::worker::{self, QueryResponse};
use crate::{log_debug, log_info, Database};

/// MySQL protocol backend for a single client connection.
pub struct MySqlBackend {
    db: Arc<Database>,
    session: Session,
}

impl MySqlBackend {
    pub fn new(db: Arc<Database>) -> Self {
        let session = Session::new();
        log_info!("Created session {} for new connection", session.id());
        Self { db, session }
    }

    pub fn session_id(&self) -> u64 {
        self.session.id()
    }

    fn make_ok_response(affected_rows: u64, last_insert_id: u64) -> OkResponse {
        OkResponse {
            header: 0,
            affected_rows,
            last_insert_id,
            status_flags: StatusFlags::SERVER_STATUS_AUTOCOMMIT,
            warnings: 0,
            info: String::new(),
            session_state_info: String::new(),
        }
    }
}

#[async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for MySqlBackend {
    type Error = io::Error;

    async fn on_init<'a>(
        &'a mut self,
        database: &'a str,
        writer: InitWriter<'a, W>,
    ) -> io::Result<()> {
        log_info!(
            "Session {} selected database: {}",
            self.session.id(),
            database
        );
        self.session.set_current_db(database);
        writer.ok().await
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        log_debug!("Session {} query: {}", self.session.id(), query);

        let query_lower = query.trim().to_lowercase();

        // SET — pure session state, no DB access
        if query_lower.starts_with("set ") {
            return results.completed(Self::make_ok_response(0, 0)).await;
        }

        // @@variables — pure computation, no DB access
        if query_lower.contains("@@") {
            return self.handle_system_variable(query, results).await;
        }

        // USE — session state update, no DB access
        if query_lower.starts_with("use ") {
            let db_name = query.trim()[4..].trim();
            let db_name = db_name.trim_matches('`').trim_end_matches(';').trim();
            log_info!(
                "Session {} selected database via USE query: {}",
                self.session.id(),
                db_name
            );
            self.session.set_current_db(db_name);
            return results.completed(Self::make_ok_response(0, 0)).await;
        }

        // Everything else → worker runtime
        self.dispatch_query(query, results).await
    }

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        log_debug!("Prepare: {}", query);
        info.reply(1, &[], &[]).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        _params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        log_debug!("Execute prepared statement: {}", id);
        results.completed(Self::make_ok_response(0, 0)).await
    }

    async fn on_close(&mut self, id: u32) {
        log_debug!("Close prepared statement: {}", id);
    }
}

impl MySqlBackend {
    // ========================================================================
    // Query Dispatch — single entry point for all DB commands
    // ========================================================================

    async fn dispatch_query<W: AsyncWrite + Send + Unpin>(
        &mut self,
        query: &str,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        let db = Arc::clone(&self.db);
        let worker_handle = db.worker_handle().cloned();
        let exec_ctx = ExecutionCtx::from_session(&self.session);
        let txn_ctx = self.session.take_current_txn();

        let response = worker::dispatch_full_query(
            worker_handle.as_ref(),
            db,
            query.to_string(),
            exec_ctx,
            txn_ctx,
        )
        .await;

        let result = match response {
            QueryResponse::Rows {
                columns,
                mut batch_rx,
                txn_ctx,
            } => {
                if let Some(ctx) = txn_ctx {
                    self.session.set_current_txn(ctx);
                }

                let cols: Vec<Column> = columns
                    .iter()
                    .map(|name| Column {
                        table: String::new(),
                        column: name.clone(),
                        coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                        colflags: ColumnFlags::empty(),
                    })
                    .collect();

                let mut rw = results.start(&cols).await?;

                // Receive row batches from the worker
                while let Some(batch_result) = batch_rx.recv().await {
                    match batch_result {
                        Ok(batch) => {
                            for row in &batch {
                                for val in row.iter() {
                                    write_value_col(&mut rw, val)?;
                                }
                                rw.end_row().await?;
                            }
                        }
                        Err(e) => {
                            // Update registry before early return on error
                            self.update_session_registry();
                            return rw
                                .finish_error(
                                    ErrorKind::ER_UNKNOWN_ERROR,
                                    &e.to_string().into_bytes(),
                                )
                                .await;
                        }
                    }
                }

                rw.finish().await
            }
            QueryResponse::Affected { count, txn_ctx } => {
                if let Some(ctx) = txn_ctx {
                    self.session.set_current_txn(ctx);
                }
                results.completed(Self::make_ok_response(count, 0)).await
            }
            QueryResponse::Ok { txn_ctx } => {
                if let Some(ctx) = txn_ctx {
                    self.session.set_current_txn(ctx);
                }
                results.completed(Self::make_ok_response(0, 0)).await
            }
            QueryResponse::Error { error, txn_ctx } => {
                if let Some(ctx) = txn_ctx {
                    self.session.set_current_txn(ctx);
                }
                results
                    .error(ErrorKind::ER_UNKNOWN_ERROR, error.to_string().as_bytes())
                    .await
            }
        };

        // Update session registry after every dispatched query
        self.update_session_registry();

        result
    }

    /// Update the session registry based on current transaction state.
    ///
    /// If an explicit transaction is active, register its start_ts.
    /// Otherwise, unregister this session to allow GC safe point advancement.
    fn update_session_registry(&self) {
        if self.session.has_active_txn() {
            if let Some(txn) = self.session.current_txn() {
                self.db
                    .session_registry()
                    .register(self.session.id(), txn.start_ts());
            }
        } else {
            self.db.session_registry().unregister(self.session.id());
        }
    }

    // ========================================================================
    // System Variable Handler — pure computation, no DB access
    // ========================================================================

    async fn handle_system_variable<W: AsyncWrite + Send + Unpin>(
        &self,
        query: &str,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        let query_lower = query.to_lowercase();

        let cols = vec![Column {
            table: String::new(),
            column: "Value".to_string(),
            coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
            colflags: ColumnFlags::empty(),
        }];

        let mut rw = results.start(&cols).await?;

        if query_lower.contains("@@version_comment") {
            rw.write_col("TiSQL")?;
        } else if query_lower.contains("@@version") {
            rw.write_col("8.0.32-TiSQL")?;
        } else if query_lower.contains("@@max_allowed_packet") {
            rw.write_col("67108864")?;
        } else if query_lower.contains("@@character_set")
            || query_lower.contains("@@collation")
            || query_lower.contains("character_set")
        {
            rw.write_col("utf8mb4")?;
        } else if query_lower.contains("@@session.auto_increment_increment") {
            rw.write_col("1")?;
        } else if query_lower.contains("@@session.tx_isolation")
            || query_lower.contains("@@transaction_isolation")
        {
            rw.write_col("REPEATABLE-READ")?;
        } else if query_lower.contains("@@session.tx_read_only")
            || query_lower.contains("@@transaction_read_only")
        {
            rw.write_col("0")?;
        } else if query_lower.contains("@@autocommit") {
            rw.write_col("1")?;
        } else if query_lower.contains("@@sql_mode") {
            rw.write_col("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION")?;
        } else if query_lower.contains("@@lower_case_table_names") {
            rw.write_col("0")?;
        } else {
            rw.write_col("")?;
        }

        rw.end_row().await?;
        rw.finish().await
    }
}

impl Drop for MySqlBackend {
    fn drop(&mut self) {
        // Unregister from session registry on connection close
        self.db.session_registry().unregister(self.session.id());
    }
}

// ============================================================================
// Typed Value Writing
// ============================================================================

fn write_value_col<W: AsyncWrite + Unpin>(
    rw: &mut opensrv_mysql::RowWriter<'_, W>,
    val: &Value,
) -> io::Result<()> {
    match val {
        Value::Null => rw.write_col(None::<i32>),
        Value::Boolean(b) => rw.write_col(if *b { "true" } else { "false" }),
        Value::TinyInt(v) => rw.write_col(v.to_string().as_str()),
        Value::SmallInt(v) => rw.write_col(v.to_string().as_str()),
        Value::Int(v) => rw.write_col(v.to_string().as_str()),
        Value::BigInt(v) => rw.write_col(v.to_string().as_str()),
        Value::Float(v) => rw.write_col(v.to_string().as_str()),
        Value::Double(v) => rw.write_col(v.to_string().as_str()),
        Value::Decimal(v) => rw.write_col(v.as_str()),
        Value::String(v) => rw.write_col(v.as_str()),
        Value::Bytes(v) => rw.write_col(format!("{v:?}").as_str()),
        Value::Date(v) => rw.write_col(format!("DATE({v})").as_str()),
        Value::Time(v) => rw.write_col(format!("TIME({v})").as_str()),
        Value::DateTime(v) => rw.write_col(format!("DATETIME({v})").as_str()),
        Value::Timestamp(v) => rw.write_col(format!("TIMESTAMP({v})").as_str()),
    }
}
