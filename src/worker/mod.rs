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

// Worker dispatch module — runs ALL database work on a dedicated worker runtime.
//
// Architecture:
// - Protocol layer is a thin I/O shell (TCP read/write, MySQL wire encoding)
// - ALL database work dispatched here: parse, bind, execute, txn control, SHOW
// - For read queries, worker pulls rows from the live operator tree and streams
//   batches to protocol via bounded mpsc channel — Execution stays on worker
// - For writes/DDL/txn control/SHOW, result returned via oneshot (no streaming)
// - Worker handle: if Database has a dedicated worker runtime, tasks are spawned
//   there; otherwise falls back to tokio::spawn (test path)

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

use crate::catalog::types::{Row, Value};
use crate::executor::ExecutionOutput;
use crate::session::ExecutionCtx;
use crate::transaction::TxnCtx;
use crate::util::error::TiSqlError;
use crate::util::mysql_text::{
    format_date_canonical, format_datetime_canonical, format_time_canonical,
};
use crate::Database;
use crate::{log_debug, log_warn};

/// Result batch transport between worker and protocol runtimes.
pub enum ResultBatch {
    Typed(TypedBatch),
    Encoded(EncodedBatch),
}

/// A typed row batch plus its byte-budget permit.
pub struct TypedBatch {
    pub rows: Vec<Row>,
    pub est_bytes: usize,
    _budget_permit: OwnedSemaphorePermit,
}

/// A pre-encoded row batch plus its byte-budget permit.
pub struct EncodedBatch {
    pub bytes: Vec<u8>,
    pub num_columns: usize,
    pub num_rows: usize,
    pub est_bytes: usize,
    _budget_permit: OwnedSemaphorePermit,
}

/// Target rows per emitted batch.
const TARGET_BATCH_ROWS: usize = 256;
/// Soft byte threshold per emitted batch.
const TARGET_BATCH_BYTES: usize = 256 * 1024;
/// Hard byte threshold per emitted batch.
const MAX_BATCH_BYTES: usize = 1024 * 1024;
/// Per-query result-memory budget.
const RESULT_BUDGET_BYTES: usize = 8 * 1024 * 1024;
/// Lower/upper bound for channel capacity derived from byte budget.
const MIN_CHANNEL_CAPACITY: usize = 4;
const MAX_CHANNEL_CAPACITY: usize = 64;
const CHANNEL_CAPACITY_FROM_BUDGET: usize = RESULT_BUDGET_BYTES.div_ceil(TARGET_BATCH_BYTES);

/// Encoded-fast sampling policy.
const SAMPLE_ROWS: usize = 32;
const MAX_ENCODED_ROW_BYTES: usize = 64 * 1024;
const NUMERIC_COLS_RATIO_THRESHOLD: f64 = 0.6;
const VARLEN_PAYLOAD_RATIO_THRESHOLD: f64 = 0.2;

static RESULT_PATH_MODE_TYPED_TOTAL: AtomicU64 = AtomicU64::new(0);
static RESULT_PATH_MODE_ENCODED_TOTAL: AtomicU64 = AtomicU64::new(0);
static RESULT_PATH_MODE_FALLBACK_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Bounded channel capacity (number of batches in flight).
const CHANNEL_CAPACITY: usize = if CHANNEL_CAPACITY_FROM_BUDGET < MIN_CHANNEL_CAPACITY {
    MIN_CHANNEL_CAPACITY
} else if CHANNEL_CAPACITY_FROM_BUDGET > MAX_CHANNEL_CAPACITY {
    MAX_CHANNEL_CAPACITY
} else {
    CHANNEL_CAPACITY_FROM_BUDGET
};

/// Response from query dispatch.
///
/// Read queries carry a `batch_rx` channel for streaming rows. Writes,
/// DDL, transaction control, and SHOW return results directly.
pub enum QueryResponse {
    /// Read query — receive row batches from the channel.
    Rows {
        columns: Vec<String>,
        batch_rx: mpsc::Receiver<Result<ResultBatch, TiSqlError>>,
        txn_ctx: Option<TxnCtx>,
    },
    /// Write operation (INSERT/UPDATE/DELETE).
    Affected { count: u64, txn_ctx: Option<TxnCtx> },
    /// DDL/session/txn control command (CREATE TABLE, BEGIN, COMMIT, ROLLBACK, etc.).
    Ok { txn_ctx: Option<TxnCtx> },
    /// Error before any streaming started.
    Error {
        error: TiSqlError,
        txn_ctx: Option<TxnCtx>,
    },
}

/// Dispatch a full query (parse + bind + execute) to the worker runtime.
///
/// ALL commands go through this function — SELECT, INSERT, DDL, BEGIN,
/// COMMIT, ROLLBACK, SHOW. The protocol layer is a thin I/O shell.
///
/// For read queries, the worker pulls rows and streams batches via mpsc.
/// For everything else, the result is returned directly via oneshot.
pub async fn dispatch_full_query(
    worker_handle: Option<&tokio::runtime::Handle>,
    db: Arc<Database>,
    sql: String,
    exec_ctx: ExecutionCtx,
    txn_ctx: Option<TxnCtx>,
) -> QueryResponse {
    let (response_tx, response_rx) = oneshot::channel::<QueryResponse>();

    let task = async move {
        // Check for SHOW commands (string-matched, not parsed)
        let sql_trimmed = sql.trim();
        let sql_lower = sql_trimmed.to_lowercase();
        if sql_lower.starts_with("show ") {
            let response = handle_show(&db, &sql_lower, &exec_ctx);
            let _ = response_tx.send(response);
            return;
        }

        // CPU: parse and bind
        let plan = match db.parse_and_bind(&sql, &exec_ctx) {
            Ok(plan) => plan,
            Err(e) => {
                let _ = response_tx.send(QueryResponse::Error { error: e, txn_ctx });
                return;
            }
        };

        let columns = if plan.is_read_query() {
            Some(plan.output_columns())
        } else {
            None
        };

        // Execute plan (async — iterators yield during SST I/O)
        let (exec_result, returned_ctx) = db.execute_plan(plan, &exec_ctx, txn_ctx).await;

        match (exec_result, returned_ctx) {
            (Ok(ExecutionOutput::Rows { exec, .. }), returned_ctx) => {
                // Create mpsc channel for row batches
                let (batch_tx, batch_rx) = mpsc::channel(CHANNEL_CAPACITY);
                let output_columns = columns.unwrap_or_default();
                let num_columns = output_columns.len();

                // Send the response with the channel receiver immediately
                let _ = response_tx.send(QueryResponse::Rows {
                    columns: output_columns,
                    batch_rx,
                    txn_ctx: returned_ctx,
                });

                // Pull rows from operator tree and stream batches
                stream_rows(
                    exec,
                    num_columns,
                    db.encoded_result_batch_enabled(),
                    batch_tx,
                )
                .await;
            }
            (Ok(ExecutionOutput::Affected { count }), ctx) => {
                let _ = response_tx.send(QueryResponse::Affected {
                    count,
                    txn_ctx: ctx,
                });
            }
            (Ok(ExecutionOutput::Ok), ctx) => {
                let _ = response_tx.send(QueryResponse::Ok { txn_ctx: ctx });
            }
            (Ok(ExecutionOutput::OkWithEffect(_)), ctx) => {
                // Effect already handled in Database::execute_plan
                let _ = response_tx.send(QueryResponse::Ok { txn_ctx: ctx });
            }
            (Err(e), ctx) => {
                let _ = response_tx.send(QueryResponse::Error {
                    error: e,
                    txn_ctx: ctx,
                });
            }
        }
    };

    // Spawn on worker runtime if available, otherwise on current runtime
    match worker_handle {
        Some(handle) => {
            handle.spawn(task);
        }
        None => {
            tokio::spawn(task);
        }
    }

    response_rx.await.unwrap_or(QueryResponse::Error {
        error: TiSqlError::Internal("Worker task dropped".into()),
        txn_ctx: None,
    })
}

/// Pull rows from the Execution handle and send batches through the channel.
///
/// Accumulates rows into row+byte bounded batches. On error, sends the error
/// through the channel and returns. On EOF, drops the sender (receiver gets None).
async fn stream_rows(
    mut exec: crate::executor::Execution,
    num_columns: usize,
    enable_encoded_batch: bool,
    batch_tx: mpsc::Sender<Result<ResultBatch, TiSqlError>>,
) {
    let result_budget = Arc::new(Semaphore::new(RESULT_BUDGET_BYTES));
    let mut state = StreamState::Sampling(SampleState::new());
    let mut typed_batch = Vec::with_capacity(TARGET_BATCH_ROWS);
    let mut typed_est_bytes = 0usize;
    let mut encoded_bytes = Vec::with_capacity(TARGET_BATCH_BYTES);
    let mut encoded_rows = 0usize;
    let mut encoded_est_bytes = 0usize;

    loop {
        match exec.next().await {
            Ok(Some(row)) => {
                let row_est_bytes = estimate_row_text_bytes(&row);
                match &mut state {
                    StreamState::Sampling(sample) => {
                        sample.observe(&row, row_est_bytes);
                        sample.rows.push(row);
                        if sample.rows.len() >= SAMPLE_ROWS {
                            let mode =
                                decide_stream_mode(sample, num_columns, enable_encoded_batch);
                            let sampled_rows = std::mem::take(&mut sample.rows);
                            state = StreamState::Active(mode);
                            for sampled in sampled_rows {
                                let sampled_est = estimate_row_text_bytes(&sampled);
                                let pushed = push_row_to_active_mode(
                                    mode,
                                    sampled,
                                    sampled_est,
                                    num_columns,
                                    &batch_tx,
                                    &result_budget,
                                    &mut typed_batch,
                                    &mut typed_est_bytes,
                                    &mut encoded_bytes,
                                    &mut encoded_rows,
                                    &mut encoded_est_bytes,
                                )
                                .await;
                                match pushed {
                                    Ok(true) => {}
                                    Ok(false) => return,
                                    Err(e) => {
                                        let _ = batch_tx.send(Err(e)).await;
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    StreamState::Active(mode) => {
                        let pushed = push_row_to_active_mode(
                            *mode,
                            row,
                            row_est_bytes,
                            num_columns,
                            &batch_tx,
                            &result_budget,
                            &mut typed_batch,
                            &mut typed_est_bytes,
                            &mut encoded_bytes,
                            &mut encoded_rows,
                            &mut encoded_est_bytes,
                        )
                        .await;
                        match pushed {
                            Ok(true) => {}
                            Ok(false) => return,
                            Err(e) => {
                                let _ = batch_tx.send(Err(e)).await;
                                return;
                            }
                        }
                    }
                };
            }
            Ok(None) => {
                if let StreamState::Sampling(sample) = &mut state {
                    // Small-result rule: if rows never exceed sample window, stay TypedFast.
                    RESULT_PATH_MODE_TYPED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    log_debug!(
                        "Result path mode=typed (small-result rows={}, sample_window={})",
                        sample.rows.len(),
                        SAMPLE_ROWS
                    );
                    let sampled_rows = std::mem::take(&mut sample.rows);
                    for sampled in sampled_rows {
                        let sampled_est = estimate_row_text_bytes(&sampled);
                        let pushed = push_row_to_active_mode(
                            StreamMode::Typed,
                            sampled,
                            sampled_est,
                            num_columns,
                            &batch_tx,
                            &result_budget,
                            &mut typed_batch,
                            &mut typed_est_bytes,
                            &mut encoded_bytes,
                            &mut encoded_rows,
                            &mut encoded_est_bytes,
                        )
                        .await;
                        match pushed {
                            Ok(true) => {}
                            Ok(false) => return,
                            Err(e) => {
                                let _ = batch_tx.send(Err(e)).await;
                                return;
                            }
                        }
                    }
                    state = StreamState::Active(StreamMode::Typed);
                }

                match state {
                    StreamState::Sampling(_) => {}
                    StreamState::Active(StreamMode::Typed) => {
                        match flush_typed_batch(
                            &batch_tx,
                            &result_budget,
                            &mut typed_batch,
                            &mut typed_est_bytes,
                        )
                        .await
                        {
                            Ok(true) => {}
                            Ok(false) => return,
                            Err(e) => {
                                let _ = batch_tx.send(Err(e)).await;
                                return;
                            }
                        }
                    }
                    StreamState::Active(StreamMode::Encoded) => {
                        match flush_encoded_batch(
                            &batch_tx,
                            &result_budget,
                            &mut encoded_bytes,
                            &mut encoded_rows,
                            &mut encoded_est_bytes,
                            num_columns,
                        )
                        .await
                        {
                            Ok(true) => {}
                            Ok(false) => return,
                            Err(e) => {
                                let _ = batch_tx.send(Err(e)).await;
                                return;
                            }
                        }
                    }
                }
                return; // EOF — drop sender, receiver gets None
            }
            Err(e) => {
                // Send error through the channel
                let _ = batch_tx.send(Err(e)).await;
                return;
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamMode {
    Typed,
    Encoded,
}

enum StreamState {
    Sampling(SampleState),
    Active(StreamMode),
}

struct SampleState {
    rows: Vec<Row>,
    total_cells: usize,
    numeric_cells: usize,
    total_payload_bytes: usize,
    varlen_payload_bytes: usize,
    max_row_est_bytes: usize,
}

impl SampleState {
    fn new() -> Self {
        Self {
            rows: Vec::with_capacity(SAMPLE_ROWS),
            total_cells: 0,
            numeric_cells: 0,
            total_payload_bytes: 0,
            varlen_payload_bytes: 0,
            max_row_est_bytes: 0,
        }
    }

    fn observe(&mut self, row: &Row, row_est_bytes: usize) {
        self.max_row_est_bytes = self.max_row_est_bytes.max(row_est_bytes);
        for val in row.iter() {
            self.total_cells = self.total_cells.saturating_add(1);
            if is_numeric_value(val) {
                self.numeric_cells = self.numeric_cells.saturating_add(1);
            }
            let payload = estimate_value_payload_bytes(val);
            self.total_payload_bytes = self.total_payload_bytes.saturating_add(payload);
            if is_varlen_value(val) {
                self.varlen_payload_bytes = self.varlen_payload_bytes.saturating_add(payload);
            }
        }
    }
}

fn decide_stream_mode(
    sample: &SampleState,
    num_columns: usize,
    enable_encoded_batch: bool,
) -> StreamMode {
    let numeric_cols_ratio = if sample.total_cells == 0 {
        0.0
    } else {
        sample.numeric_cells as f64 / sample.total_cells as f64
    };
    let varlen_payload_ratio = if sample.total_payload_bytes == 0 {
        0.0
    } else {
        sample.varlen_payload_bytes as f64 / sample.total_payload_bytes as f64
    };
    let encoded_candidate = num_columns > 0
        && numeric_cols_ratio >= NUMERIC_COLS_RATIO_THRESHOLD
        && varlen_payload_ratio <= VARLEN_PAYLOAD_RATIO_THRESHOLD
        && sample.max_row_est_bytes <= MAX_ENCODED_ROW_BYTES;

    let selected = if encoded_candidate && enable_encoded_batch {
        RESULT_PATH_MODE_ENCODED_TOTAL.fetch_add(1, Ordering::Relaxed);
        StreamMode::Encoded
    } else {
        RESULT_PATH_MODE_TYPED_TOTAL.fetch_add(1, Ordering::Relaxed);
        if encoded_candidate && !enable_encoded_batch {
            RESULT_PATH_MODE_FALLBACK_TOTAL.fetch_add(1, Ordering::Relaxed);
            log_warn!("Result-path encoded mode selected by heuristic but disabled by config; falling back to typed mode");
        }
        StreamMode::Typed
    };

    log_debug!(
        "Result path mode={:?} sample_rows={} numeric_ratio={:.3} varlen_ratio={:.3} max_row_est_bytes={}",
        selected,
        sample.rows.len(),
        numeric_cols_ratio,
        varlen_payload_ratio,
        sample.max_row_est_bytes
    );

    selected
}

#[allow(clippy::too_many_arguments)]
async fn push_row_to_active_mode(
    mode: StreamMode,
    row: Row,
    row_est_bytes: usize,
    num_columns: usize,
    batch_tx: &mpsc::Sender<Result<ResultBatch, TiSqlError>>,
    result_budget: &Arc<Semaphore>,
    typed_batch: &mut Vec<Row>,
    typed_est_bytes: &mut usize,
    encoded_bytes: &mut Vec<u8>,
    encoded_rows: &mut usize,
    encoded_est_bytes: &mut usize,
) -> Result<bool, TiSqlError> {
    match mode {
        StreamMode::Typed => {
            *typed_est_bytes = typed_est_bytes.saturating_add(row_est_bytes);
            typed_batch.push(row);
            if typed_batch.len() >= TARGET_BATCH_ROWS
                || *typed_est_bytes >= TARGET_BATCH_BYTES
                || *typed_est_bytes >= MAX_BATCH_BYTES
            {
                flush_typed_batch(batch_tx, result_budget, typed_batch, typed_est_bytes).await
            } else {
                Ok(true)
            }
        }
        StreamMode::Encoded => {
            encode_row_canonical(encoded_bytes, &row, num_columns)?;
            *encoded_rows = encoded_rows.saturating_add(1);
            *encoded_est_bytes = encoded_est_bytes.saturating_add(row_est_bytes);
            if *encoded_rows >= TARGET_BATCH_ROWS
                || encoded_bytes.len() >= TARGET_BATCH_BYTES
                || encoded_bytes.len() >= MAX_BATCH_BYTES
            {
                flush_encoded_batch(
                    batch_tx,
                    result_budget,
                    encoded_bytes,
                    encoded_rows,
                    encoded_est_bytes,
                    num_columns,
                )
                .await
            } else {
                Ok(true)
            }
        }
    }
}

async fn flush_typed_batch(
    batch_tx: &mpsc::Sender<Result<ResultBatch, TiSqlError>>,
    result_budget: &Arc<Semaphore>,
    batch: &mut Vec<Row>,
    batch_est_bytes: &mut usize,
) -> Result<bool, TiSqlError> {
    if batch.is_empty() {
        return Ok(true);
    }

    let est_bytes = (*batch_est_bytes).max(1);
    let permits = u32::try_from(est_bytes).map_err(|_| {
        TiSqlError::Internal(format!(
            "Batch estimated bytes {est_bytes} exceed semaphore permit range"
        ))
    })?;
    let permit = Arc::clone(result_budget)
        .acquire_many_owned(permits)
        .await
        .map_err(|_| TiSqlError::Internal("Result budget semaphore unexpectedly closed".into()))?;

    let rows = std::mem::replace(batch, Vec::with_capacity(TARGET_BATCH_ROWS));
    *batch_est_bytes = 0;
    let typed_batch = TypedBatch {
        rows,
        est_bytes,
        _budget_permit: permit,
    };

    if batch_tx
        .send(Ok(ResultBatch::Typed(typed_batch)))
        .await
        .is_err()
    {
        return Ok(false);
    }

    Ok(true)
}

async fn flush_encoded_batch(
    batch_tx: &mpsc::Sender<Result<ResultBatch, TiSqlError>>,
    result_budget: &Arc<Semaphore>,
    bytes: &mut Vec<u8>,
    num_rows: &mut usize,
    est_bytes_acc: &mut usize,
    num_columns: usize,
) -> Result<bool, TiSqlError> {
    if *num_rows == 0 {
        return Ok(true);
    }

    let payload = std::mem::take(bytes);
    let est_bytes = payload.len().max(1);
    let permits = u32::try_from(est_bytes).map_err(|_| {
        TiSqlError::Internal(format!(
            "Encoded batch bytes {est_bytes} exceed semaphore permit range"
        ))
    })?;
    let permit = Arc::clone(result_budget)
        .acquire_many_owned(permits)
        .await
        .map_err(|_| TiSqlError::Internal("Result budget semaphore unexpectedly closed".into()))?;

    let encoded_batch = EncodedBatch {
        bytes: payload,
        num_columns,
        num_rows: *num_rows,
        est_bytes,
        _budget_permit: permit,
    };

    *num_rows = 0;
    *est_bytes_acc = 0;
    bytes.reserve(TARGET_BATCH_BYTES);

    if batch_tx
        .send(Ok(ResultBatch::Encoded(encoded_batch)))
        .await
        .is_err()
    {
        return Ok(false);
    }

    Ok(true)
}

fn encode_row_canonical(
    out: &mut Vec<u8>,
    row: &Row,
    num_columns: usize,
) -> Result<(), TiSqlError> {
    if row.len() != num_columns {
        return Err(TiSqlError::Internal(format!(
            "Encoded row column mismatch: expected {}, got {}",
            num_columns,
            row.len()
        )));
    }

    let len_pos = out.len();
    out.extend_from_slice(&[0u8; 4]); // row_payload_len placeholder
    let row_start = out.len();
    for val in row.iter() {
        encode_col_canonical(out, val)?;
    }
    let payload_len = out.len().saturating_sub(row_start);
    let payload_len_u32 = u32::try_from(payload_len).map_err(|_| {
        TiSqlError::Internal(format!(
            "Encoded row payload too large: {payload_len} bytes"
        ))
    })?;
    out[len_pos..len_pos + 4].copy_from_slice(&payload_len_u32.to_le_bytes());
    Ok(())
}

fn encode_col_canonical(out: &mut Vec<u8>, val: &Value) -> Result<(), TiSqlError> {
    match val {
        Value::Null => {
            out.push(0xFB);
            Ok(())
        }
        Value::Boolean(b) => {
            let bytes = if *b {
                b"true".as_slice()
            } else {
                b"false".as_slice()
            };
            append_lenenc_bytes(out, bytes);
            Ok(())
        }
        Value::TinyInt(v) => {
            let mut buf = itoa::Buffer::new();
            append_lenenc_bytes(out, buf.format(*v).as_bytes());
            Ok(())
        }
        Value::SmallInt(v) => {
            let mut buf = itoa::Buffer::new();
            append_lenenc_bytes(out, buf.format(*v).as_bytes());
            Ok(())
        }
        Value::Int(v) => {
            let mut buf = itoa::Buffer::new();
            append_lenenc_bytes(out, buf.format(*v).as_bytes());
            Ok(())
        }
        Value::BigInt(v) => {
            let mut buf = itoa::Buffer::new();
            append_lenenc_bytes(out, buf.format(*v).as_bytes());
            Ok(())
        }
        Value::Float(v) => {
            let mut buf = ryu::Buffer::new();
            append_lenenc_bytes(out, buf.format(*v).as_bytes());
            Ok(())
        }
        Value::Double(v) => {
            let mut buf = ryu::Buffer::new();
            append_lenenc_bytes(out, buf.format(*v).as_bytes());
            Ok(())
        }
        Value::Decimal(v) | Value::String(v) => {
            append_lenenc_bytes(out, v.as_bytes());
            Ok(())
        }
        Value::Bytes(v) => {
            append_lenenc_bytes(out, v);
            Ok(())
        }
        Value::Date(v) => {
            let mut scratch = [0u8; 32];
            let n = format_date_canonical(*v, &mut scratch)
                .map_err(|e| TiSqlError::Internal(e.to_string()))?;
            append_lenenc_bytes(out, &scratch[..n]);
            Ok(())
        }
        Value::Time(v) => {
            let mut scratch = [0u8; 32];
            let n = format_time_canonical(*v, &mut scratch)
                .map_err(|e| TiSqlError::Internal(e.to_string()))?;
            append_lenenc_bytes(out, &scratch[..n]);
            Ok(())
        }
        Value::DateTime(v) | Value::Timestamp(v) => {
            let mut scratch = [0u8; 32];
            let n = format_datetime_canonical(*v, &mut scratch)
                .map_err(|e| TiSqlError::Internal(e.to_string()))?;
            append_lenenc_bytes(out, &scratch[..n]);
            Ok(())
        }
    }
}

fn append_lenenc_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    append_lenenc_int(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn append_lenenc_int(out: &mut Vec<u8>, v: u64) {
    if v < 251 {
        out.push(v as u8);
    } else if v < (1 << 16) {
        out.push(0xFC);
        out.extend_from_slice(&(v as u16).to_le_bytes());
    } else if v < (1 << 24) {
        out.push(0xFD);
        out.push((v & 0xFF) as u8);
        out.push(((v >> 8) & 0xFF) as u8);
        out.push(((v >> 16) & 0xFF) as u8);
    } else {
        out.push(0xFE);
        out.extend_from_slice(&v.to_le_bytes());
    }
}

fn is_numeric_value(val: &Value) -> bool {
    matches!(
        val,
        Value::TinyInt(_)
            | Value::SmallInt(_)
            | Value::Int(_)
            | Value::BigInt(_)
            | Value::Float(_)
            | Value::Double(_)
    )
}

fn is_varlen_value(val: &Value) -> bool {
    matches!(val, Value::Decimal(_) | Value::String(_) | Value::Bytes(_))
}

fn estimate_value_payload_bytes(val: &Value) -> usize {
    match val {
        Value::Null => 0,
        Value::Boolean(b) => {
            if *b {
                4
            } else {
                5
            }
        }
        Value::TinyInt(v) => {
            let mut buf = itoa::Buffer::new();
            buf.format(*v).len()
        }
        Value::SmallInt(v) => {
            let mut buf = itoa::Buffer::new();
            buf.format(*v).len()
        }
        Value::Int(v) => {
            let mut buf = itoa::Buffer::new();
            buf.format(*v).len()
        }
        Value::BigInt(v) => {
            let mut buf = itoa::Buffer::new();
            buf.format(*v).len()
        }
        Value::Float(v) => {
            let mut buf = ryu::Buffer::new();
            buf.format(*v).len()
        }
        Value::Double(v) => {
            let mut buf = ryu::Buffer::new();
            buf.format(*v).len()
        }
        Value::Decimal(v) | Value::String(v) => v.len(),
        Value::Bytes(v) => v.len(),
        Value::Date(_) => 10,
        Value::Time(v) => estimate_time_text_len(*v),
        Value::DateTime(v) | Value::Timestamp(v) => {
            let frac = v.rem_euclid(1_000_000);
            if frac == 0 {
                19
            } else {
                26
            }
        }
    }
}

fn estimate_row_text_bytes(row: &Row) -> usize {
    row.iter().fold(0usize, |acc, val| {
        acc.saturating_add(estimate_value_text_bytes(val))
    })
}

fn estimate_value_text_bytes(val: &Value) -> usize {
    match val {
        Value::Null => 1, // 0xFB
        Value::Boolean(b) => {
            let len = if *b { 4 } else { 5 }; // true/false
            lenenc_prefix_len(len).saturating_add(len)
        }
        Value::TinyInt(v) => {
            let mut buf = itoa::Buffer::new();
            let len = buf.format(*v).len();
            lenenc_prefix_len(len).saturating_add(len)
        }
        Value::SmallInt(v) => {
            let mut buf = itoa::Buffer::new();
            let len = buf.format(*v).len();
            lenenc_prefix_len(len).saturating_add(len)
        }
        Value::Int(v) => {
            let mut buf = itoa::Buffer::new();
            let len = buf.format(*v).len();
            lenenc_prefix_len(len).saturating_add(len)
        }
        Value::BigInt(v) => {
            let mut buf = itoa::Buffer::new();
            let len = buf.format(*v).len();
            lenenc_prefix_len(len).saturating_add(len)
        }
        Value::Float(v) => {
            let mut buf = ryu::Buffer::new();
            let len = buf.format(*v).len();
            lenenc_prefix_len(len).saturating_add(len)
        }
        Value::Double(v) => {
            let mut buf = ryu::Buffer::new();
            let len = buf.format(*v).len();
            lenenc_prefix_len(len).saturating_add(len)
        }
        Value::Decimal(v) | Value::String(v) => lenenc_prefix_len(v.len()).saturating_add(v.len()),
        Value::Bytes(v) => lenenc_prefix_len(v.len()).saturating_add(v.len()),
        Value::Date(_) => lenenc_prefix_len(10).saturating_add(10),
        Value::Time(v) => {
            let len = estimate_time_text_len(*v);
            lenenc_prefix_len(len).saturating_add(len)
        }
        Value::DateTime(v) | Value::Timestamp(v) => {
            let frac = v.rem_euclid(1_000_000);
            let len = if frac == 0 { 19 } else { 26 };
            lenenc_prefix_len(len).saturating_add(len)
        }
    }
}

fn estimate_time_text_len(micros: i64) -> usize {
    let abs = micros.saturating_abs() as u64;
    let total_secs = abs / 1_000_000;
    let hours = total_secs / 3600;
    let mut len = decimal_len_u64(hours).max(2) + 6; // HH:MM:SS
    if micros < 0 {
        len += 1; // '-'
    }
    if abs % 1_000_000 != 0 {
        len += 7; // ".ffffff"
    }
    len
}

fn decimal_len_u64(v: u64) -> usize {
    let mut buf = itoa::Buffer::new();
    buf.format(v).len()
}

fn lenenc_prefix_len(len: usize) -> usize {
    if len < 251 {
        1
    } else if len < (1 << 16) {
        3
    } else if len < (1 << 24) {
        4
    } else {
        9
    }
}

/// Handle SHOW commands directly on the worker (catalog scan).
fn handle_show(db: &Database, sql_lower: &str, exec_ctx: &ExecutionCtx) -> QueryResponse {
    if sql_lower.contains("databases") {
        match db.list_schemas() {
            Ok(databases) => {
                let rows: Vec<Row> = databases
                    .into_iter()
                    .map(|name| Row::new(vec![crate::catalog::types::Value::String(name)]))
                    .collect();
                make_show_response(vec!["Database".to_string()], rows)
            }
            Err(e) => QueryResponse::Error {
                error: e,
                txn_ctx: None,
            },
        }
    } else if sql_lower.contains("tables") {
        let database = &exec_ctx.current_db;
        let col_name = format!("Tables_in_{database}");
        match db.list_tables(database) {
            Ok(tables) => {
                let rows: Vec<Row> = tables
                    .into_iter()
                    .map(|name| Row::new(vec![crate::catalog::types::Value::String(name)]))
                    .collect();
                make_show_response(vec![col_name], rows)
            }
            Err(e) => QueryResponse::Error {
                error: e,
                txn_ctx: None,
            },
        }
    } else if sql_lower.contains("warnings") {
        make_show_response(
            vec![
                "Level".to_string(),
                "Code".to_string(),
                "Message".to_string(),
            ],
            vec![],
        )
    } else if sql_lower.contains("status") {
        make_show_response(
            vec!["Variable_name".to_string(), "Value".to_string()],
            vec![],
        )
    } else {
        QueryResponse::Error {
            error: TiSqlError::Internal(format!("Unsupported SHOW command: {sql_lower}")),
            txn_ctx: None,
        }
    }
}

/// Create a QueryResponse::Rows with all rows in a single batch.
fn make_show_response(columns: Vec<String>, rows: Vec<Row>) -> QueryResponse {
    let (batch_tx, batch_rx) = mpsc::channel(1);
    if !rows.is_empty() {
        // Send all rows as one batch (SHOW results are always small)
        let _ = batch_tx.try_send(make_typed_batch(rows));
    }
    // Drop sender immediately — receiver will get the batch then None
    drop(batch_tx);
    QueryResponse::Rows {
        columns,
        batch_rx,
        txn_ctx: None,
    }
}

fn make_typed_batch(rows: Vec<Row>) -> Result<ResultBatch, TiSqlError> {
    let est_bytes = rows
        .iter()
        .map(estimate_row_text_bytes)
        .fold(0usize, |acc, n| acc.saturating_add(n))
        .max(1);

    let permits = u32::try_from(est_bytes).map_err(|_| {
        TiSqlError::Internal(format!(
            "SHOW batch estimated bytes {est_bytes} exceed semaphore permit range"
        ))
    })?;
    let sem = Arc::new(Semaphore::new(est_bytes));
    let permit = Arc::clone(&sem)
        .try_acquire_many_owned(permits)
        .map_err(|_| TiSqlError::Internal("Failed to acquire SHOW batch result budget".into()))?;

    Ok(ResultBatch::Typed(TypedBatch {
        rows,
        est_bytes,
        _budget_permit: permit,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::types::Value;

    #[test]
    fn test_channel_capacity_from_budget_defaults() {
        assert_eq!(CHANNEL_CAPACITY, 32);
    }

    #[test]
    fn test_estimate_value_text_bytes_numeric_nonzero() {
        let int_len = estimate_value_text_bytes(&Value::Int(12345));
        let float_len = estimate_value_text_bytes(&Value::Double(std::f64::consts::PI));
        assert!(int_len > 0);
        assert!(float_len > 0);
    }

    #[test]
    fn test_estimate_time_text_len_fraction_and_sign() {
        assert_eq!(estimate_time_text_len(0), 8); // 00:00:00
        assert_eq!(estimate_time_text_len(-1_000_000), 9); // -00:00:01
        assert_eq!(estimate_time_text_len(1_234_567), 15); // 00:00:01.234567
    }

    #[test]
    fn test_encode_row_canonical_framing() {
        let row = Row::new(vec![
            Value::Int(42),
            Value::String("abc".to_string()),
            Value::Null,
        ]);
        let mut buf = Vec::new();
        encode_row_canonical(&mut buf, &row, 3).unwrap();
        assert!(buf.len() >= 4);
        let row_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        assert_eq!(row_len, buf.len() - 4);
    }

    #[test]
    fn test_decide_stream_mode_encoded_candidate() {
        let mut sample = SampleState::new();
        for i in 0..SAMPLE_ROWS {
            let row = Row::new(vec![
                Value::Int(i as i32),
                Value::BigInt(i as i64),
                Value::Double(i as f64),
                Value::TinyInt(i as i8),
            ]);
            let est = estimate_row_text_bytes(&row);
            sample.observe(&row, est);
            sample.rows.push(row);
        }
        let mode = decide_stream_mode(&sample, 4, true);
        assert_eq!(mode, StreamMode::Encoded);
    }

    #[test]
    fn test_decide_stream_mode_fallback_when_disabled() {
        let mut sample = SampleState::new();
        for i in 0..SAMPLE_ROWS {
            let row = Row::new(vec![Value::Int(i as i32), Value::Int(i as i32)]);
            let est = estimate_row_text_bytes(&row);
            sample.observe(&row, est);
            sample.rows.push(row);
        }
        let mode = decide_stream_mode(&sample, 2, false);
        assert_eq!(mode, StreamMode::Typed);
    }
}
