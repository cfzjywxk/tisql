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

use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use opensrv_mysql::{
    AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind, InitWriter, OkResponse,
    ParamParser, QueryResultWriter, StatementMetaWriter, StatusFlags,
};
use tokio::io::AsyncWrite;

use crate::session::{ExecutionCtx, Session};
use crate::types::DataType;
use crate::worker::{WorkerCommand, WorkerPool, WorkerResult};
use crate::{log_debug, log_info, log_warn, Database, QueryResult};

// ============================================================================
// Statement Classification
// ============================================================================

/// Classifies SQL statements for protocol-level handling.
///
/// Transaction control statements (BEGIN/COMMIT/ROLLBACK) and USE DATABASE
/// are handled directly at the protocol layer rather than being dispatched
/// to the worker pool. This ensures proper session state management.
#[derive(Debug)]
enum StatementType {
    /// BEGIN or START TRANSACTION
    Begin { read_only: bool },
    /// COMMIT
    Commit,
    /// ROLLBACK
    Rollback,
    /// USE database_name
    UseDatabase(String),
    /// All other statements - dispatch to worker pool
    Other,
}

/// Classifies a SQL statement for protocol-level handling.
///
/// This is a quick syntactic check, not a full parse. It handles:
/// - BEGIN / START TRANSACTION [READ ONLY]
/// - COMMIT
/// - ROLLBACK
/// - USE database_name
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
        // Extract database name: "USE dbname" or "USE `dbname`"
        let db_name = sql_trimmed[4..].trim();
        // Remove backticks and semicolon if present
        let db_name = db_name.trim_matches('`').trim_end_matches(';').trim();
        StatementType::UseDatabase(db_name.to_string())
    } else {
        StatementType::Other
    }
}

/// MySQL protocol backend that wraps our Database.
///
/// Each MySqlBackend represents a single client connection and holds:
/// - Reference to the shared Database
/// - Reference to the worker pool
/// - A Session for this connection (created on connection establishment)
///
/// The Session persists for the lifetime of the connection and is used
/// to create QueryCtx for each SQL statement execution.
pub struct MySqlBackend {
    db: Arc<Database>,
    worker_pool: Arc<WorkerPool>,
    /// Session for this connection - created when connection is established
    session: Session,
}

impl MySqlBackend {
    /// Create a new MySqlBackend for a client connection.
    ///
    /// This is called after successful MySQL protocol handshake,
    /// which means the connection phase is complete and we're entering
    /// the command phase. A new Session is created for this connection.
    pub fn new(db: Arc<Database>, worker_pool: Arc<WorkerPool>) -> Self {
        // Create a new session for this connection
        let session = Session::new();
        log_info!("Created session {} for new connection", session.id());

        Self {
            db,
            worker_pool,
            session,
        }
    }

    /// Get the session ID for this connection.
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

    /// Called when client connects and sends COM_INIT_DB.
    ///
    /// Updates the session's current database. This is also called
    /// when client sends USE database_name command.
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

    /// Called on COM_QUERY - direct SQL query.
    ///
    /// Transaction control statements (BEGIN/COMMIT/ROLLBACK) are handled
    /// directly at the protocol layer to ensure proper session state management.
    /// Other statements are dispatched to the worker pool with the current
    /// database and optional transaction context.
    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        log_debug!("Session {} query: {}", self.session.id(), query);

        // Handle some special MySQL client queries first
        let query_lower = query.trim().to_lowercase();

        // Handle SET statements (MySQL client sends these on connect)
        if query_lower.starts_with("set ") {
            return results.completed(Self::make_ok_response(0, 0)).await;
        }

        // Handle SHOW statements
        if query_lower.starts_with("show ") {
            return self.handle_show(query, results).await;
        }

        // Handle SELECT @@version, @@version_comment, etc.
        if query_lower.contains("@@") {
            return self.handle_system_variable(query, results).await;
        }

        // Classify the statement for protocol-level handling
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
            StatementType::Other => {
                // Dispatch to worker pool with current_db and optional TxnCtx
                self.dispatch_to_worker(query, results).await
            }
        }
    }

    /// Called on COM_STMT_PREPARE - prepare a statement
    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        log_debug!("Prepare: {}", query);

        // For now, we don't support prepared statements fully
        // Just return a simple statement with no parameters
        // This allows basic MySQL clients to work
        info.reply(1, &[], &[]).await
    }

    /// Called on COM_STMT_EXECUTE - execute prepared statement
    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        _params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        log_debug!("Execute prepared statement: {}", id);

        // For now, return empty result
        results.completed(Self::make_ok_response(0, 0)).await
    }

    /// Called on COM_STMT_CLOSE
    async fn on_close(&mut self, id: u32) {
        log_debug!("Close prepared statement: {}", id);
    }
}

impl MySqlBackend {
    // ========================================================================
    // Transaction Control (Protocol-Level)
    // ========================================================================

    /// Handle BEGIN / START TRANSACTION.
    ///
    /// Dispatched to the worker pool to keep tokio threads free.
    async fn handle_begin<W: AsyncWrite + Send + Unpin>(
        &mut self,
        read_only: bool,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        // Check if there's already an active transaction
        if self.session.has_active_txn() {
            return results
                .error(
                    ErrorKind::ER_UNKNOWN_ERROR,
                    b"Nested transactions are not supported. Use COMMIT or ROLLBACK first.",
                )
                .await;
        }

        let cmd = WorkerCommand::Begin { read_only };
        match self.worker_pool.dispatch(Arc::clone(&self.db), cmd).await {
            WorkerResult::Begin { txn_ctx } => {
                log_debug!(
                    "Session {} started explicit transaction (txn_id={}, read_only={})",
                    self.session.id(),
                    txn_ctx.txn_id(),
                    read_only
                );
                self.session.set_current_txn(txn_ctx);
                results.completed(Self::make_ok_response(0, 0)).await
            }
            WorkerResult::Error { error, .. } => {
                results
                    .error(ErrorKind::ER_UNKNOWN_ERROR, error.to_string().as_bytes())
                    .await
            }
            _ => {
                results
                    .error(ErrorKind::ER_UNKNOWN_ERROR, b"Unexpected worker result")
                    .await
            }
        }
    }

    /// Handle COMMIT.
    ///
    /// Dispatched to the worker pool to keep tokio threads free.
    async fn handle_commit<W: AsyncWrite + Send + Unpin>(
        &mut self,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        if let Some(ctx) = self.session.take_current_txn() {
            let txn_id = ctx.txn_id();
            let cmd = WorkerCommand::Commit { txn_ctx: ctx };
            match self.worker_pool.dispatch(Arc::clone(&self.db), cmd).await {
                WorkerResult::Committed { info } => {
                    log_debug!(
                        "Session {} committed transaction (txn_id={}, commit_ts={})",
                        self.session.id(),
                        info.txn_id,
                        info.commit_ts
                    );
                    results.completed(Self::make_ok_response(0, 0)).await
                }
                WorkerResult::Error { error, .. } => {
                    log_warn!(
                        "Session {} failed to commit transaction (txn_id={}): {}",
                        self.session.id(),
                        txn_id,
                        error
                    );
                    results
                        .error(ErrorKind::ER_UNKNOWN_ERROR, error.to_string().as_bytes())
                        .await
                }
                _ => {
                    results
                        .error(ErrorKind::ER_UNKNOWN_ERROR, b"Unexpected worker result")
                        .await
                }
            }
        } else {
            // No active transaction - COMMIT is a no-op (MySQL behavior)
            results.completed(Self::make_ok_response(0, 0)).await
        }
    }

    /// Handle ROLLBACK.
    ///
    /// Dispatched to the worker pool to keep tokio threads free.
    async fn handle_rollback<W: AsyncWrite + Send + Unpin>(
        &mut self,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        if let Some(ctx) = self.session.take_current_txn() {
            let txn_id = ctx.txn_id();
            let cmd = WorkerCommand::Rollback { txn_ctx: ctx };
            match self.worker_pool.dispatch(Arc::clone(&self.db), cmd).await {
                WorkerResult::RolledBack => {
                    log_debug!(
                        "Session {} rolled back transaction (txn_id={})",
                        self.session.id(),
                        txn_id
                    );
                    results.completed(Self::make_ok_response(0, 0)).await
                }
                WorkerResult::Error { error, .. } => {
                    log_warn!(
                        "Session {} failed to rollback transaction (txn_id={}): {}",
                        self.session.id(),
                        txn_id,
                        error
                    );
                    results
                        .error(ErrorKind::ER_UNKNOWN_ERROR, error.to_string().as_bytes())
                        .await
                }
                _ => {
                    results
                        .error(ErrorKind::ER_UNKNOWN_ERROR, b"Unexpected worker result")
                        .await
                }
            }
        } else {
            // No active transaction - ROLLBACK is a no-op (MySQL behavior)
            results.completed(Self::make_ok_response(0, 0)).await
        }
    }

    // ========================================================================
    // Worker Pool Dispatch
    // ========================================================================

    /// Dispatch a query to the worker pool.
    ///
    /// For explicit transactions, the TxnCtx is taken from the session,
    /// passed to the worker, and returned (potentially updated) after execution.
    ///
    /// Session variables are extracted into ExecutionCtx which provides
    /// read-only access during execution.
    async fn dispatch_to_worker<W: AsyncWrite + Send + Unpin>(
        &mut self,
        query: &str,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        let db = Arc::clone(&self.db);

        // Create execution context from session (read-only snapshot of session vars)
        let exec_ctx = ExecutionCtx::from_session(&self.session);

        // Take TxnCtx from session if there's an active explicit transaction
        let txn_ctx = self.session.take_current_txn();

        // Execute the query through the worker pool
        match self
            .worker_pool
            .handle_query(db, query.to_string(), exec_ctx, txn_ctx)
            .await
        {
            Ok((result, returned_txn_ctx)) => {
                // Put the TxnCtx back in session if it was returned
                if let Some(ctx) = returned_txn_ctx {
                    self.session.set_current_txn(ctx);
                }
                self.write_result(result, results).await
            }
            Err((e, returned_txn_ctx)) => {
                // On error, put the TxnCtx back in session (MySQL behavior: keep txn active)
                if let Some(ctx) = returned_txn_ctx {
                    self.session.set_current_txn(ctx);
                }
                results
                    .error(ErrorKind::ER_UNKNOWN_ERROR, e.to_string().as_bytes())
                    .await
            }
        }
    }

    // ========================================================================
    // Result Writing
    // ========================================================================

    /// Write query result to MySQL protocol
    async fn write_result<W: AsyncWrite + Send + Unpin>(
        &self,
        result: QueryResult,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        match result {
            QueryResult::Rows { columns, data } => {
                // Build column definitions
                let cols: Vec<Column> = columns
                    .iter()
                    .map(|name| Column {
                        table: String::new(),
                        column: name.clone(),
                        coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                        colflags: ColumnFlags::empty(),
                    })
                    .collect();

                // Start writing result set
                let mut rw = results.start(&cols).await?;

                // Write each row
                for row in &data {
                    for value in row {
                        rw.write_col(value.as_str())?;
                    }
                    rw.end_row().await?;
                }

                rw.finish().await
            }
            QueryResult::Affected(count) => {
                results.completed(Self::make_ok_response(count, 0)).await
            }
            QueryResult::Ok => results.completed(Self::make_ok_response(0, 0)).await,
        }
    }

    /// Handle SHOW statements
    async fn handle_show<W: AsyncWrite + Send + Unpin>(
        &self,
        query: &str,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        let query_lower = query.trim().to_lowercase();

        if query_lower.contains("databases") {
            // Dispatch to worker pool to avoid catalog lock on tokio thread
            let cmd = WorkerCommand::ShowDatabases;
            match self.worker_pool.dispatch(Arc::clone(&self.db), cmd).await {
                WorkerResult::Databases(databases) => {
                    let cols = vec![Column {
                        table: String::new(),
                        column: "Database".to_string(),
                        coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                        colflags: ColumnFlags::empty(),
                    }];

                    let mut rw = results.start(&cols).await?;
                    for db in databases {
                        rw.write_col(db.as_str())?;
                        rw.end_row().await?;
                    }
                    rw.finish().await
                }
                WorkerResult::Error { error, .. } => {
                    results
                        .error(ErrorKind::ER_UNKNOWN_ERROR, error.to_string().as_bytes())
                        .await
                }
                _ => {
                    results
                        .error(ErrorKind::ER_UNKNOWN_ERROR, b"Unexpected worker result")
                        .await
                }
            }
        } else if query_lower.contains("tables") {
            // Dispatch to worker pool to avoid catalog lock on tokio thread
            let cmd = WorkerCommand::ShowTables {
                database: self.session.current_db().to_string(),
            };
            let col_name = format!("Tables_in_{}", self.session.current_db());
            match self.worker_pool.dispatch(Arc::clone(&self.db), cmd).await {
                WorkerResult::Tables(tables) => {
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
                WorkerResult::Error { error, .. } => {
                    results
                        .error(ErrorKind::ER_UNKNOWN_ERROR, error.to_string().as_bytes())
                        .await
                }
                _ => {
                    results
                        .error(ErrorKind::ER_UNKNOWN_ERROR, b"Unexpected worker result")
                        .await
                }
            }
        } else if query_lower.contains("warnings") {
            // Return empty warnings
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
            // Return simple status
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
            // Unknown SHOW command
            results
                .error(
                    ErrorKind::ER_UNKNOWN_ERROR,
                    format!("Unsupported SHOW command: {query}").as_bytes(),
                )
                .await
        }
    }

    /// Handle system variable queries (@@version, etc.)
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

        // Return appropriate values for common system variables
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
            // Unknown variable - return empty string
            rw.write_col("")?;
        }

        rw.end_row().await?;
        rw.finish().await
    }
}

/// Convert our DataType to MySQL ColumnType
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
