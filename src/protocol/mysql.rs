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
// Thin protocol layer: transaction control runs directly on the async task
// (CPU-only). Query execution is dispatched to a worker task via
// worker::dispatch_full_query which uses spawn_blocking for storage I/O.

use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use opensrv_mysql::{
    AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind, InitWriter, OkResponse,
    ParamParser, QueryResultWriter, StatementMetaWriter, StatusFlags,
};
use tokio::io::AsyncWrite;

use crate::session::{ExecutionCtx, Session};
use crate::types::{DataType, Value};
use crate::worker::{self, QueryDone, QueryResponse};
use crate::{log_debug, log_info, log_warn, Database};

// ============================================================================
// Statement Classification
// ============================================================================

/// Classifies SQL statements for protocol-level handling.
#[derive(Debug)]
enum StatementType {
    Begin { read_only: bool },
    Commit,
    Rollback,
    UseDatabase(String),
    Other,
}

fn classify_statement(sql: &str) -> StatementType {
    let sql_trimmed = sql.trim();
    let sql_upper = sql_trimmed.to_uppercase();

    if sql_upper.starts_with("BEGIN") || sql_upper.starts_with("START TRANSACTION") {
        let read_only = sql_upper.contains("READ ONLY");
        StatementType::Begin { read_only }
    } else if sql_upper.starts_with("COMMIT") {
        StatementType::Commit
    } else if sql_upper.starts_with("ROLLBACK") {
        StatementType::Rollback
    } else if sql_upper.starts_with("USE ") {
        let db_name = sql_trimmed[4..].trim();
        let db_name = db_name.trim_matches('`').trim_end_matches(';').trim();
        StatementType::UseDatabase(db_name.to_string())
    } else {
        StatementType::Other
    }
}

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

        // SHOW — catalog queries dispatched via spawn_blocking
        if query_lower.starts_with("show ") {
            return self.handle_show(query, results).await;
        }

        // @@variables — pure computation
        if query_lower.contains("@@") {
            return self.handle_system_variable(query, results).await;
        }

        match classify_statement(query) {
            StatementType::Begin { read_only } => self.handle_begin(read_only, results).await,
            StatementType::Commit => self.handle_commit(results).await,
            StatementType::Rollback => self.handle_rollback(results).await,
            StatementType::UseDatabase(db_name) => {
                log_info!(
                    "Session {} selected database via USE query: {}",
                    self.session.id(),
                    db_name
                );
                self.session.set_current_db(&db_name);
                results.completed(Self::make_ok_response(0, 0)).await
            }
            StatementType::Other => self.dispatch_query(query, results).await,
        }
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
    // Transaction Control — runs directly on async task (CPU-only, no I/O)
    // ========================================================================

    async fn handle_begin<W: AsyncWrite + Send + Unpin>(
        &mut self,
        read_only: bool,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        if self.session.has_active_txn() {
            return results
                .error(
                    ErrorKind::ER_UNKNOWN_ERROR,
                    b"Nested transactions are not supported. Use COMMIT or ROLLBACK first.",
                )
                .await;
        }

        // CPU-only: allocates TSO timestamp + creates TxnCtx
        match self.db.begin_explicit(read_only) {
            Ok(txn_ctx) => {
                log_debug!(
                    "Session {} started explicit transaction (txn_id={}, read_only={})",
                    self.session.id(),
                    txn_ctx.txn_id(),
                    read_only
                );
                self.session.set_current_txn(txn_ctx);
                results.completed(Self::make_ok_response(0, 0)).await
            }
            Err(e) => {
                results
                    .error(ErrorKind::ER_UNKNOWN_ERROR, e.to_string().as_bytes())
                    .await
            }
        }
    }

    async fn handle_commit<W: AsyncWrite + Send + Unpin>(
        &mut self,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        if let Some(ctx) = self.session.take_current_txn() {
            let txn_id = ctx.txn_id();
            // Async: yields during clog fsync
            match self.db.commit_async(ctx).await {
                Ok(info) => {
                    log_debug!(
                        "Session {} committed transaction (txn_id={}, commit_ts={})",
                        self.session.id(),
                        info.txn_id,
                        info.commit_ts
                    );
                    results.completed(Self::make_ok_response(0, 0)).await
                }
                Err(e) => {
                    log_warn!(
                        "Session {} failed to commit transaction (txn_id={}): {}",
                        self.session.id(),
                        txn_id,
                        e
                    );
                    results
                        .error(ErrorKind::ER_UNKNOWN_ERROR, e.to_string().as_bytes())
                        .await
                }
            }
        } else {
            results.completed(Self::make_ok_response(0, 0)).await
        }
    }

    async fn handle_rollback<W: AsyncWrite + Send + Unpin>(
        &mut self,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        if let Some(ctx) = self.session.take_current_txn() {
            let txn_id = ctx.txn_id();
            // CPU-only: releases in-memory locks and pending nodes
            match self.db.rollback(ctx) {
                Ok(()) => {
                    log_debug!(
                        "Session {} rolled back transaction (txn_id={})",
                        self.session.id(),
                        txn_id
                    );
                    results.completed(Self::make_ok_response(0, 0)).await
                }
                Err(e) => {
                    log_warn!(
                        "Session {} failed to rollback transaction (txn_id={}): {}",
                        self.session.id(),
                        txn_id,
                        e
                    );
                    results
                        .error(ErrorKind::ER_UNKNOWN_ERROR, e.to_string().as_bytes())
                        .await
                }
            }
        } else {
            results.completed(Self::make_ok_response(0, 0)).await
        }
    }

    // ========================================================================
    // Query Dispatch
    // ========================================================================

    async fn dispatch_query<W: AsyncWrite + Send + Unpin>(
        &mut self,
        query: &str,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        let db = Arc::clone(&self.db);
        let exec_ctx = ExecutionCtx::from_session(&self.session);
        let txn_ctx = self.session.take_current_txn();

        let response = worker::dispatch_full_query(db, query.to_string(), exec_ctx, txn_ctx).await;

        match response {
            QueryResponse::Rows {
                columns,
                mut batch_rx,
                done_rx,
            } => {
                let mut all_rows = Vec::new();
                while let Some(batch) = batch_rx.recv().await {
                    all_rows.push(batch);
                }

                let done = done_rx.await.unwrap_or(QueryDone::Error {
                    error: crate::error::TiSqlError::Internal("Worker task dropped".into()),
                    txn_ctx: None,
                });

                match done {
                    QueryDone::Success { txn_ctx } => {
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
                        for batch in all_rows {
                            for row in &batch {
                                for val in row.iter() {
                                    write_value_col(&mut rw, val)?;
                                }
                                rw.end_row().await?;
                            }
                        }
                        rw.finish().await
                    }
                    QueryDone::Error { error, txn_ctx } => {
                        if let Some(ctx) = txn_ctx {
                            self.session.set_current_txn(ctx);
                        }
                        results
                            .error(ErrorKind::ER_UNKNOWN_ERROR, error.to_string().as_bytes())
                            .await
                    }
                }
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
        }
    }

    // ========================================================================
    // SHOW and System Variable Handlers
    // ========================================================================

    async fn handle_show<W: AsyncWrite + Send + Unpin>(
        &self,
        query: &str,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        let query_lower = query.trim().to_lowercase();

        if query_lower.contains("databases") {
            // Catalog scan — storage I/O via spawn_blocking
            let db = Arc::clone(&self.db);
            let result = tokio::task::spawn_blocking(move || db.list_schemas()).await;

            match result {
                Ok(Ok(databases)) => {
                    let cols = vec![Column {
                        table: String::new(),
                        column: "Database".to_string(),
                        coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                        colflags: ColumnFlags::empty(),
                    }];
                    let mut rw = results.start(&cols).await?;
                    for db_name in databases {
                        rw.write_col(db_name.as_str())?;
                        rw.end_row().await?;
                    }
                    rw.finish().await
                }
                Ok(Err(e)) => {
                    results
                        .error(ErrorKind::ER_UNKNOWN_ERROR, e.to_string().as_bytes())
                        .await
                }
                Err(e) => {
                    results
                        .error(
                            ErrorKind::ER_UNKNOWN_ERROR,
                            format!("Internal error: {e}").as_bytes(),
                        )
                        .await
                }
            }
        } else if query_lower.contains("tables") {
            // Catalog scan — storage I/O via spawn_blocking
            let db = Arc::clone(&self.db);
            let database = self.session.current_db().to_string();
            let col_name = format!("Tables_in_{}", self.session.current_db());
            let result = tokio::task::spawn_blocking(move || db.list_tables(&database)).await;

            match result {
                Ok(Ok(tables)) => {
                    let cols = vec![Column {
                        table: String::new(),
                        column: col_name,
                        coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                        colflags: ColumnFlags::empty(),
                    }];
                    let mut rw = results.start(&cols).await?;
                    for table in tables {
                        rw.write_col(table.as_str())?;
                        rw.end_row().await?;
                    }
                    rw.finish().await
                }
                Ok(Err(e)) => {
                    results
                        .error(ErrorKind::ER_UNKNOWN_ERROR, e.to_string().as_bytes())
                        .await
                }
                Err(e) => {
                    results
                        .error(
                            ErrorKind::ER_UNKNOWN_ERROR,
                            format!("Internal error: {e}").as_bytes(),
                        )
                        .await
                }
            }
        } else if query_lower.contains("warnings") {
            let cols = vec![
                Column {
                    table: String::new(),
                    column: "Level".to_string(),
                    coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                    colflags: ColumnFlags::empty(),
                },
                Column {
                    table: String::new(),
                    column: "Code".to_string(),
                    coltype: ColumnType::MYSQL_TYPE_LONG,
                    colflags: ColumnFlags::empty(),
                },
                Column {
                    table: String::new(),
                    column: "Message".to_string(),
                    coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                    colflags: ColumnFlags::empty(),
                },
            ];
            let rw = results.start(&cols).await?;
            rw.finish().await
        } else if query_lower.contains("status") {
            let cols = vec![
                Column {
                    table: String::new(),
                    column: "Variable_name".to_string(),
                    coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                    colflags: ColumnFlags::empty(),
                },
                Column {
                    table: String::new(),
                    column: "Value".to_string(),
                    coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                    colflags: ColumnFlags::empty(),
                },
            ];
            let rw = results.start(&cols).await?;
            rw.finish().await
        } else {
            results
                .error(
                    ErrorKind::ER_UNKNOWN_ERROR,
                    format!("Unsupported SHOW command: {query}").as_bytes(),
                )
                .await
        }
    }

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

#[allow(dead_code)]
fn data_type_to_mysql(dt: &DataType) -> ColumnType {
    match dt {
        DataType::Boolean => ColumnType::MYSQL_TYPE_TINY,
        DataType::TinyInt => ColumnType::MYSQL_TYPE_TINY,
        DataType::SmallInt => ColumnType::MYSQL_TYPE_SHORT,
        DataType::Int => ColumnType::MYSQL_TYPE_LONG,
        DataType::BigInt => ColumnType::MYSQL_TYPE_LONGLONG,
        DataType::Float => ColumnType::MYSQL_TYPE_FLOAT,
        DataType::Double => ColumnType::MYSQL_TYPE_DOUBLE,
        DataType::Decimal { .. } => ColumnType::MYSQL_TYPE_DECIMAL,
        DataType::Char(_) => ColumnType::MYSQL_TYPE_STRING,
        DataType::Varchar(_) => ColumnType::MYSQL_TYPE_VAR_STRING,
        DataType::Text => ColumnType::MYSQL_TYPE_BLOB,
        DataType::Blob => ColumnType::MYSQL_TYPE_BLOB,
        DataType::Date => ColumnType::MYSQL_TYPE_DATE,
        DataType::Time => ColumnType::MYSQL_TYPE_TIME,
        DataType::DateTime => ColumnType::MYSQL_TYPE_DATETIME,
        DataType::Timestamp => ColumnType::MYSQL_TYPE_TIMESTAMP,
    }
}
