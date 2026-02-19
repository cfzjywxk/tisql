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
                            for row in &batch.rows {
                                for val in row.iter() {
                                    write_value_fast(&mut rw, val)?;
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

fn format_date_canonical(days_since_epoch: i32, out: &mut [u8; 32]) -> io::Result<usize> {
    let (year, month, day) = civil_from_days(days_since_epoch as i64);
    let mut len = 0usize;
    append_year(out, &mut len, year)?;
    append_byte(out, &mut len, b'-')?;
    append_u32_padded(out, &mut len, month, 2)?;
    append_byte(out, &mut len, b'-')?;
    append_u32_padded(out, &mut len, day, 2)?;
    Ok(len)
}

fn format_time_canonical(micros: i64, out: &mut [u8; 32]) -> io::Result<usize> {
    let mut len = 0usize;
    let abs = micros.unsigned_abs();
    let total_secs = abs / 1_000_000;
    let frac = (abs % 1_000_000) as u32;
    let hours = total_secs / 3_600;
    let minutes = ((total_secs % 3_600) / 60) as u32;
    let seconds = (total_secs % 60) as u32;

    if micros < 0 {
        append_byte(out, &mut len, b'-')?;
    }

    if hours < 100 {
        append_u32_padded(out, &mut len, hours as u32, 2)?;
    } else {
        append_u64(out, &mut len, hours)?;
    }
    append_byte(out, &mut len, b':')?;
    append_u32_padded(out, &mut len, minutes, 2)?;
    append_byte(out, &mut len, b':')?;
    append_u32_padded(out, &mut len, seconds, 2)?;
    if frac != 0 {
        append_byte(out, &mut len, b'.')?;
        append_u32_padded(out, &mut len, frac, 6)?;
    }
    Ok(len)
}

fn format_datetime_canonical(micros_since_epoch: i64, out: &mut [u8; 32]) -> io::Result<usize> {
    let secs = micros_since_epoch.div_euclid(1_000_000);
    let frac = micros_since_epoch.rem_euclid(1_000_000) as u32;
    let days = secs.div_euclid(86_400);
    let sec_of_day = secs.rem_euclid(86_400) as u32;

    let (year, month, day) = civil_from_days(days);
    let hour = sec_of_day / 3_600;
    let minute = (sec_of_day % 3_600) / 60;
    let second = sec_of_day % 60;

    let mut len = 0usize;
    append_year(out, &mut len, year)?;
    append_byte(out, &mut len, b'-')?;
    append_u32_padded(out, &mut len, month, 2)?;
    append_byte(out, &mut len, b'-')?;
    append_u32_padded(out, &mut len, day, 2)?;
    append_byte(out, &mut len, b' ')?;
    append_u32_padded(out, &mut len, hour, 2)?;
    append_byte(out, &mut len, b':')?;
    append_u32_padded(out, &mut len, minute, 2)?;
    append_byte(out, &mut len, b':')?;
    append_u32_padded(out, &mut len, second, 2)?;
    if frac != 0 {
        append_byte(out, &mut len, b'.')?;
        append_u32_padded(out, &mut len, frac, 6)?;
    }
    Ok(len)
}

// Convert days since Unix epoch (1970-01-01) to civil date.
fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if m <= 2 { 1 } else { 0 };
    (year as i32, m as u32, d as u32)
}

fn append_year(out: &mut [u8], len: &mut usize, year: i32) -> io::Result<()> {
    if (0..=9_999).contains(&year) {
        append_u32_padded(out, len, year as u32, 4)
    } else {
        let mut buf = itoa::Buffer::new();
        append_bytes(out, len, buf.format(year).as_bytes())
    }
}

fn append_u64(out: &mut [u8], len: &mut usize, value: u64) -> io::Result<()> {
    let mut buf = itoa::Buffer::new();
    append_bytes(out, len, buf.format(value).as_bytes())
}

fn append_u32_padded(
    out: &mut [u8],
    len: &mut usize,
    mut value: u32,
    width: usize,
) -> io::Result<()> {
    let mut tmp = [b'0'; 10];
    let start = tmp.len().checked_sub(width).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid width for zero-padded integer",
        )
    })?;

    for idx in (start..tmp.len()).rev() {
        tmp[idx] = b'0' + (value % 10) as u8;
        value /= 10;
    }
    if value != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Zero-padded integer width overflow",
        ));
    }
    append_bytes(out, len, &tmp[start..])
}

fn append_byte(out: &mut [u8], len: &mut usize, b: u8) -> io::Result<()> {
    append_bytes(out, len, &[b])
}

fn append_bytes(out: &mut [u8], len: &mut usize, bytes: &[u8]) -> io::Result<()> {
    let remaining = out.len().saturating_sub(*len);
    if remaining < bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Temporal formatting exceeded scratch buffer",
        ));
    }
    out[*len..*len + bytes.len()].copy_from_slice(bytes);
    *len += bytes.len();
    Ok(())
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
}
