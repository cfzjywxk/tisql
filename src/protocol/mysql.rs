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

use crate::types::DataType;
use crate::worker::WorkerPool;
use crate::{log_debug, log_info, Database, QueryResult};

/// MySQL protocol backend that wraps our Database
pub struct MySqlBackend {
    db: Arc<Database>,
    worker_pool: Arc<WorkerPool>,
    current_db: String,
}

impl MySqlBackend {
    pub fn new(db: Arc<Database>, worker_pool: Arc<WorkerPool>) -> Self {
        Self {
            db,
            worker_pool,
            current_db: "test".to_string(),
        }
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

    /// Called when client connects and sends COM_INIT_DB
    async fn on_init<'a>(
        &'a mut self,
        database: &'a str,
        writer: InitWriter<'a, W>,
    ) -> io::Result<()> {
        log_info!("Client selected database: {}", database);
        self.current_db = database.to_string();
        writer.ok().await
    }

    /// Called on COM_QUERY - direct SQL query
    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        log_debug!("Query: {}", query);

        // Handle some special MySQL client queries
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

        // Execute the query through the worker pool (off the network IO thread)
        let db = Arc::clone(&self.db);
        match self.worker_pool.handle_mp_query(db, query.to_string()).await {
            Ok(result) => self.write_result(result, results).await,
            Err(e) => {
                results
                    .error(ErrorKind::ER_UNKNOWN_ERROR, e.to_string().as_bytes())
                    .await
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
            let cols = vec![Column {
                table: String::new(),
                column: "Database".to_string(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            }];

            let mut rw = results.start(&cols).await?;
            rw.write_col("test")?;
            rw.end_row().await?;
            rw.write_col("information_schema")?;
            rw.end_row().await?;
            rw.finish().await
        } else if query_lower.contains("tables") {
            // Show tables in current database
            let cols = vec![Column {
                table: String::new(),
                column: format!("Tables_in_{}", self.current_db),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            }];

            let mut rw = results.start(&cols).await?;
            // Get tables from catalog
            if let Ok(tables) = self.db.list_tables() {
                for table in tables {
                    rw.write_col(table.as_str())?;
                    rw.end_row().await?;
                }
            }
            rw.finish().await
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
