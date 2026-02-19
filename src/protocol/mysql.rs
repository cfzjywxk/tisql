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

use crate::catalog::types::Value;
use crate::session::{ExecutionCtx, Session};
use crate::util::mysql_text::{
    format_date_canonical, format_datetime_canonical, format_time_canonical,
};
use crate::worker::{self, EncodedBatch, QueryResponse, ResultBatch};
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
                        Ok(batch) => match batch {
                            ResultBatch::Typed(batch) => {
                                for row in &batch.rows {
                                    for val in row.iter() {
                                        write_value_fast(&mut rw, val)?;
                                    }
                                    rw.end_row().await?;
                                }
                            }
                            ResultBatch::Encoded(batch) => {
                                if let Err(e) = write_encoded_batch(&mut rw, &batch).await {
                                    self.update_session_registry();
                                    return rw
                                        .finish_error(
                                            ErrorKind::ER_UNKNOWN_ERROR,
                                            &e.to_string().into_bytes(),
                                        )
                                        .await;
                                }
                            }
                        },
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

fn write_value_fast<W: AsyncWrite + Unpin>(
    rw: &mut opensrv_mysql::RowWriter<'_, W>,
    val: &Value,
) -> io::Result<()> {
    match val {
        Value::Null => rw.write_col(None::<i32>),
        Value::Boolean(b) => rw.write_col(if *b { "true" } else { "false" }),
        Value::TinyInt(v) => {
            let mut buf = itoa::Buffer::new();
            rw.write_col(buf.format(*v))
        }
        Value::SmallInt(v) => {
            let mut buf = itoa::Buffer::new();
            rw.write_col(buf.format(*v))
        }
        Value::Int(v) => {
            let mut buf = itoa::Buffer::new();
            rw.write_col(buf.format(*v))
        }
        Value::BigInt(v) => {
            let mut buf = itoa::Buffer::new();
            rw.write_col(buf.format(*v))
        }
        Value::Float(v) => {
            let mut buf = ryu::Buffer::new();
            rw.write_col(buf.format(*v))
        }
        Value::Double(v) => {
            let mut buf = ryu::Buffer::new();
            rw.write_col(buf.format(*v))
        }
        Value::Decimal(v) => rw.write_col(v.as_str()),
        Value::String(v) => rw.write_col(v.as_str()),
        Value::Bytes(v) => rw.write_col(v.as_slice()),
        Value::Date(v) => {
            let mut scratch = [0u8; 32];
            let n = format_date_canonical(*v, &mut scratch)?;
            rw.write_col(&scratch[..n])
        }
        Value::Time(v) => {
            let mut scratch = [0u8; 32];
            let n = format_time_canonical(*v, &mut scratch)?;
            rw.write_col(&scratch[..n])
        }
        Value::DateTime(v) => {
            let mut scratch = [0u8; 32];
            let n = format_datetime_canonical(*v, &mut scratch)?;
            rw.write_col(&scratch[..n])
        }
        Value::Timestamp(v) => {
            let mut scratch = [0u8; 32];
            let n = format_datetime_canonical(*v, &mut scratch)?;
            rw.write_col(&scratch[..n])
        }
    }
}

async fn write_encoded_batch<W: AsyncWrite + Unpin>(
    rw: &mut opensrv_mysql::RowWriter<'_, W>,
    batch: &EncodedBatch,
) -> io::Result<()> {
    let mut reader = BatchReader::new(&batch.bytes, batch.num_columns);
    let mut rows = 0usize;
    while let Some(mut row_reader) = reader.next_row()? {
        for _ in 0..batch.num_columns {
            match row_reader.next_col()? {
                Some(RawCol::Null) => rw.write_col(None::<i32>)?,
                Some(RawCol::Bytes(bytes)) => rw.write_col(bytes)?,
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Encoded row has fewer columns than expected",
                    ));
                }
            }
        }
        if row_reader.next_col()?.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Encoded row has more columns than expected",
            ));
        }
        rw.end_row().await?;
        rows += 1;
    }
    if rows != batch.num_rows {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Encoded batch row count mismatch: expected {}, decoded {}",
                batch.num_rows, rows
            ),
        ));
    }
    Ok(())
}

enum RawCol<'a> {
    Null,
    Bytes(&'a [u8]),
}

struct BatchReader<'a> {
    buf: &'a [u8],
    offset: usize,
    num_columns: usize,
}

impl<'a> BatchReader<'a> {
    fn new(buf: &'a [u8], num_columns: usize) -> Self {
        Self {
            buf,
            offset: 0,
            num_columns,
        }
    }

    fn next_row(&mut self) -> io::Result<Option<RowReader<'a>>> {
        if self.offset == self.buf.len() {
            return Ok(None);
        }
        if self.offset + 4 > self.buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Truncated encoded row header",
            ));
        }
        let row_len = u32::from_le_bytes([
            self.buf[self.offset],
            self.buf[self.offset + 1],
            self.buf[self.offset + 2],
            self.buf[self.offset + 3],
        ]) as usize;
        self.offset += 4;
        if self.offset + row_len > self.buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Truncated encoded row payload",
            ));
        }
        let row_payload = &self.buf[self.offset..self.offset + row_len];
        self.offset += row_len;
        Ok(Some(RowReader::new(row_payload, self.num_columns)))
    }
}

struct RowReader<'a> {
    row: &'a [u8],
    offset: usize,
    num_columns: usize,
    decoded_columns: usize,
}

impl<'a> RowReader<'a> {
    fn new(row: &'a [u8], num_columns: usize) -> Self {
        Self {
            row,
            offset: 0,
            num_columns,
            decoded_columns: 0,
        }
    }

    fn next_col(&mut self) -> io::Result<Option<RawCol<'a>>> {
        if self.offset == self.row.len() {
            return Ok(None);
        }
        if self.decoded_columns >= self.num_columns {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Encoded row has more columns than declared",
            ));
        }

        let first = self.row[self.offset];
        if first == 0xFB {
            self.offset += 1;
            self.decoded_columns += 1;
            return Ok(Some(RawCol::Null));
        }

        let (len, used) = decode_lenenc_int(&self.row[self.offset..])?;
        self.offset += used;
        let len = usize::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Length-encoded integer overflow",
            )
        })?;
        if self.offset + len > self.row.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Truncated encoded column payload",
            ));
        }
        let bytes = &self.row[self.offset..self.offset + len];
        self.offset += len;
        self.decoded_columns += 1;
        Ok(Some(RawCol::Bytes(bytes)))
    }
}

fn decode_lenenc_int(buf: &[u8]) -> io::Result<(u64, usize)> {
    if buf.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Empty buffer for length-encoded integer",
        ));
    }

    match buf[0] {
        x if x < 0xFB => Ok((x as u64, 1)),
        0xFC => {
            if buf.len() < 3 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Truncated lenenc u16",
                ));
            }
            Ok((u16::from_le_bytes([buf[1], buf[2]]) as u64, 3))
        }
        0xFD => {
            if buf.len() < 4 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Truncated lenenc u24",
                ));
            }
            let v = u32::from(buf[1]) | (u32::from(buf[2]) << 8) | (u32::from(buf[3]) << 16);
            Ok((v as u64, 4))
        }
        0xFE => {
            if buf.len() < 9 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Truncated lenenc u64",
                ));
            }
            Ok((
                u64::from_le_bytes([
                    buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8],
                ]),
                9,
            ))
        }
        0xFB => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "NULL marker is not valid lenenc integer",
        )),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid length-encoded integer marker",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_date_canonical_epoch() {
        let mut scratch = [0u8; 32];
        let n = format_date_canonical(0, &mut scratch).unwrap();
        assert_eq!(&scratch[..n], b"1970-01-01");
    }

    #[test]
    fn test_format_time_canonical_with_fraction() {
        let mut scratch = [0u8; 32];
        let n = format_time_canonical(
            12 * 3_600_000_000 + 34 * 60_000_000 + 56_789_000,
            &mut scratch,
        )
        .unwrap();
        assert_eq!(&scratch[..n], b"12:34:56.789000");
    }

    #[test]
    fn test_format_time_canonical_negative() {
        let mut scratch = [0u8; 32];
        let n = format_time_canonical(-3_600_000_000, &mut scratch).unwrap();
        assert_eq!(&scratch[..n], b"-01:00:00");
    }

    #[test]
    fn test_format_datetime_canonical_epoch() {
        let mut scratch = [0u8; 32];
        let n = format_datetime_canonical(0, &mut scratch).unwrap();
        assert_eq!(&scratch[..n], b"1970-01-01 00:00:00");
    }

    #[test]
    fn test_decode_lenenc_u24() {
        let (v, n) = decode_lenenc_int(&[0xFD, 0x56, 0x34, 0x12]).unwrap();
        assert_eq!(v, 0x12_34_56);
        assert_eq!(n, 4);
    }

    #[test]
    fn test_batch_reader_single_row() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&4u32.to_le_bytes());
        bytes.extend_from_slice(&[2, b'4', b'2', 0xFB]);

        let mut reader = BatchReader::new(&bytes, 2);
        let mut row = reader.next_row().unwrap().unwrap();
        match row.next_col().unwrap().unwrap() {
            RawCol::Bytes(v) => assert_eq!(v, b"42"),
            RawCol::Null => panic!("expected bytes column"),
        }
        assert!(matches!(row.next_col().unwrap().unwrap(), RawCol::Null));
        assert!(row.next_col().unwrap().is_none());
        assert!(reader.next_row().unwrap().is_none());
    }

    #[test]
    fn test_batch_reader_extra_column_rejected() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&4u32.to_le_bytes());
        bytes.extend_from_slice(&[1, b'a', 1, b'b']);

        let mut reader = BatchReader::new(&bytes, 1);
        let mut row = reader.next_row().unwrap().unwrap();
        assert!(row.next_col().unwrap().is_some());
        assert!(row.next_col().is_err());
    }
}
