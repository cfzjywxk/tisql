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

//! File-based commit log service implementation.
//!
//! Inspired by TiKV's raft-engine with a simplified design:
//! - Single log file (no rotation for now)
//! - CRC32 checksums for each record
//! - Sequential append-only writes
//! - Full replay on recovery

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "failpoints")]
use fail::fail_point;

use crate::error::{Result, TiSqlError};
use crate::lsn::{AtomicLsnProvider, LsnProvider, SharedLsnProvider};
use crate::types::{Lsn, Timestamp, TxnId};
use crate::util::fs::{rename_durable, sync_dir};
use crate::{log_info, log_trace, log_warn};

use super::{ClogBatch, ClogEntry, ClogEntryRef, ClogFsyncFuture, ClogOpRef, ClogService};

/// File header magic bytes: "CLOG"
const FILE_MAGIC: &[u8; 4] = b"CLOG";

/// File format version
const FILE_VERSION: u32 = 1;

/// File header size in bytes (magic + version + reserved)
const FILE_HEADER_SIZE: usize = 16;

/// Record header size in bytes (type + length + checksum)
const RECORD_HEADER_SIZE: usize = 12;

/// Maximum record size (256 MB) to prevent OOM from corrupted length fields
const MAX_RECORD_SIZE: usize = 256 * 1024 * 1024;

/// Configuration for file-based commit log service
#[derive(Clone, Debug)]
pub struct FileClogConfig {
    /// Directory for commit log files
    pub clog_dir: PathBuf,
    /// Commit log file name
    pub clog_file: String,
}

impl Default for FileClogConfig {
    fn default() -> Self {
        Self {
            clog_dir: PathBuf::from("data"),
            clog_file: "tisql.clog".to_string(),
        }
    }
}

impl FileClogConfig {
    /// Create config with custom directory
    pub fn with_dir(dir: impl Into<PathBuf>) -> Self {
        Self {
            clog_dir: dir.into(),
            clog_file: "tisql.clog".to_string(),
        }
    }

    /// Get the full path to the commit log file
    pub fn clog_path(&self) -> PathBuf {
        self.clog_dir.join(&self.clog_file)
    }
}

/// Internal LSN provider - either shared or local
enum LsnProviderKind {
    /// Shared LSN provider (used when unified with ilog)
    Shared(SharedLsnProvider),
    /// Local LSN counter (standalone clog)
    Local(AtomicLsnProvider),
}

impl LsnProviderKind {
    fn alloc_lsn(&self) -> Lsn {
        match self {
            LsnProviderKind::Shared(p) => p.alloc_lsn(),
            LsnProviderKind::Local(p) => p.alloc_lsn(),
        }
    }

    fn current_lsn(&self) -> Lsn {
        match self {
            LsnProviderKind::Shared(p) => p.current_lsn(),
            LsnProviderKind::Local(p) => p.current_lsn(),
        }
    }
}

/// File-based commit log implementation
pub struct FileClogService {
    config: FileClogConfig,
    /// LSN provider (shared or local)
    lsn_provider: LsnProviderKind,
    /// Group commit writer for batched fsync.
    ///
    /// Concurrency is handled internally by the writer's 3-state machine
    /// (Active/Draining/Failed with Condvar). No outer Mutex needed.
    group_writer: super::group_commit::GroupCommitWriter,
    /// IoService for io_uring-backed writes.
    io_service: Arc<crate::io::IoService>,
    /// Runtime handle for spawning new GroupCommitWriter tasks (e.g. on clog rotation).
    io_handle: tokio::runtime::Handle,
}

impl FileClogService {
    /// Open or create a new commit log file (standalone mode with local LSN counter)
    pub fn open(
        config: FileClogConfig,
        io: Arc<crate::io::IoService>,
        io_handle: &tokio::runtime::Handle,
    ) -> Result<Self> {
        Self::open_internal(config, None, io, io_handle)
    }

    /// Open or create a new commit log file with a shared LSN provider.
    ///
    /// This is used when clog and ilog share a unified LSN space for
    /// consistent recovery ordering.
    pub fn open_with_lsn_provider(
        config: FileClogConfig,
        lsn_provider: SharedLsnProvider,
        io: Arc<crate::io::IoService>,
        io_handle: &tokio::runtime::Handle,
    ) -> Result<Self> {
        Self::open_internal(config, Some(lsn_provider), io, io_handle)
    }

    /// Internal open implementation
    fn open_internal(
        config: FileClogConfig,
        shared_provider: Option<SharedLsnProvider>,
        io: Arc<crate::io::IoService>,
        io_handle: &tokio::runtime::Handle,
    ) -> Result<Self> {
        // Ensure directory exists
        std::fs::create_dir_all(&config.clog_dir)?;

        let clog_path = config.clog_path();
        let file_exists = clog_path.exists();

        // Open file for read+write+create
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&clog_path)?;

        let mut max_lsn = 0u64;
        let mut valid_data_end: Option<u64> = None;

        if file_exists && file.metadata()?.len() > 0 {
            // Validate existing file header
            let mut reader = BufReader::new(&file);
            Self::validate_header(&mut reader)?;

            // Find max LSN from existing entries and track valid data length
            let (entries, valid_bytes) = Self::read_entries_with_valid_length(&mut reader)?;
            if let Some(last) = entries.last() {
                max_lsn = last.lsn;
            }

            // Calculate the total valid length: header + valid record bytes
            valid_data_end = Some(FILE_HEADER_SIZE as u64 + valid_bytes);

            drop(reader);
        } else {
            // Write header to new file
            let mut writer = BufWriter::new(&file);
            Self::write_header(&mut writer)?;
            writer.flush()?;
            // Fsync the file to make header durable
            file.sync_all()?;
            // Fsync parent directory to make file creation durable
            sync_dir(&config.clog_dir)?;
        }

        // Re-open for appending (seek to end)
        let file_for_write = OpenOptions::new().read(true).write(true).open(&clog_path)?;

        // If we detected corruption (file is larger than valid data), truncate it
        if let Some(valid_end) = valid_data_end {
            let actual_len = file_for_write.metadata()?.len();
            if actual_len > valid_end {
                log_warn!(
                    "Truncating corrupted clog tail: {} bytes -> {} bytes",
                    actual_len,
                    valid_end
                );
                file_for_write.set_len(valid_end)?;
                file_for_write.sync_all()?;
            }
        }

        // Close the std File, reopen as DmaFile for io_uring-backed writes
        drop(file_for_write);
        let dma_file = crate::io::DmaFile::open_append_buffered(&clog_path)
            .map_err(|e| TiSqlError::Internal(format!("Failed to open clog as DmaFile: {e}")))?;

        // Create LSN provider
        let lsn_provider = match shared_provider {
            Some(p) => {
                // Shared mode: ensure provider is at least at our max LSN + 1
                let current = p.current_lsn();
                if current <= max_lsn {
                    p.set_lsn(max_lsn + 1);
                }
                LsnProviderKind::Shared(p)
            }
            None => {
                // Standalone mode: use local counter
                LsnProviderKind::Local(AtomicLsnProvider::with_start(max_lsn + 1))
            }
        };

        log_info!(
            "Opened commit log file: {:?}, current_lsn={}",
            clog_path,
            lsn_provider.current_lsn()
        );

        let group_writer =
            super::group_commit::GroupCommitWriter::new(dma_file, Arc::clone(&io), io_handle);

        Ok(Self {
            config,
            lsn_provider,
            group_writer,
            io_service: io,
            io_handle: io_handle.clone(),
        })
    }

    /// Recover entries from commit log file (standalone mode)
    pub fn recover(
        config: FileClogConfig,
        io: Arc<crate::io::IoService>,
        io_handle: &tokio::runtime::Handle,
    ) -> Result<(Self, Vec<ClogEntry>)> {
        Self::recover_internal(config, None, io, io_handle)
    }

    /// Recover entries with a shared LSN provider.
    ///
    /// The shared provider's current LSN will be updated to be at least
    /// max(clog LSN) + 1 after recovery.
    pub fn recover_with_lsn_provider(
        config: FileClogConfig,
        lsn_provider: SharedLsnProvider,
        io: Arc<crate::io::IoService>,
        io_handle: &tokio::runtime::Handle,
    ) -> Result<(Self, Vec<ClogEntry>)> {
        Self::recover_internal(config, Some(lsn_provider), io, io_handle)
    }

    /// Internal recover implementation
    fn recover_internal(
        config: FileClogConfig,
        shared_provider: Option<SharedLsnProvider>,
        io: Arc<crate::io::IoService>,
        io_handle: &tokio::runtime::Handle,
    ) -> Result<(Self, Vec<ClogEntry>)> {
        let clog_path = config.clog_path();

        if !clog_path.exists() {
            // No commit log file, start fresh
            let service = Self::open_internal(config, shared_provider, io, io_handle)?;
            return Ok((service, vec![]));
        }

        // Read all entries
        let file = File::open(&clog_path)?;
        let mut reader = BufReader::new(file);
        Self::validate_header(&mut reader)?;
        let entries = Self::read_entries(&mut reader)?;

        log_info!(
            "Recovered {} commit log entries from {:?}",
            entries.len(),
            clog_path
        );

        // Open service at recovered position
        let service = Self::open_internal(config, shared_provider, io, io_handle)?;

        Ok((service, entries))
    }

    /// Read entries from clog starting from a given LSN.
    ///
    /// This is used during recovery to replay only unflushed transactions.
    pub fn read_from_lsn(&self, from_lsn: Lsn) -> Result<Vec<ClogEntry>> {
        let entries = self.read_all()?;
        Ok(entries.into_iter().filter(|e| e.lsn >= from_lsn).collect())
    }

    /// Get the maximum LSN in the clog file.
    ///
    /// Returns 0 if the clog is empty.
    pub fn max_lsn(&self) -> Result<Lsn> {
        let entries = self.read_all()?;
        Ok(entries.last().map(|e| e.lsn).unwrap_or(0))
    }

    /// Write file header
    fn write_header<W: Write>(writer: &mut W) -> Result<()> {
        writer.write_all(FILE_MAGIC)?;
        writer.write_all(&FILE_VERSION.to_le_bytes())?;
        // Reserved bytes (8 bytes to make 16 total)
        writer.write_all(&[0u8; 8])?;
        Ok(())
    }

    /// Validate file header
    fn validate_header<R: Read>(reader: &mut R) -> Result<()> {
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != FILE_MAGIC {
            return Err(TiSqlError::Internal(format!(
                "Invalid commit log file magic: {magic:?}"
            )));
        }

        let mut version_bytes = [0u8; 4];
        reader.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);
        if version != FILE_VERSION {
            return Err(TiSqlError::Internal(format!(
                "Unsupported commit log file version: {version}"
            )));
        }

        // Skip reserved bytes
        let mut reserved = [0u8; 8];
        reader.read_exact(&mut reserved)?;

        Ok(())
    }

    /// Read all entries from file (after header)
    fn read_entries<R: Read>(reader: &mut R) -> Result<Vec<ClogEntry>> {
        let (entries, _) = Self::read_entries_with_valid_length(reader)?;
        Ok(entries)
    }

    /// Read all entries from file (after header) and return the valid length.
    ///
    /// Returns (entries, bytes_read) where bytes_read is the number of bytes
    /// read from valid records (not including any corrupted tail).
    fn read_entries_with_valid_length<R: Read>(reader: &mut R) -> Result<(Vec<ClogEntry>, u64)> {
        let mut entries = Vec::new();
        let mut valid_bytes = 0u64;

        loop {
            match Self::read_record_with_size(reader) {
                Ok(Some((batch_entries, record_size))) => {
                    valid_bytes += record_size as u64;
                    entries.extend(batch_entries);
                }
                Ok(None) => {
                    // EOF
                    break;
                }
                Err(e) => {
                    // Commit log corruption or truncated write - stop here
                    log_warn!("Stopping commit log recovery at corrupted record: {}", e);
                    break;
                }
            }
        }

        Ok((entries, valid_bytes))
    }

    /// Read a single record from the commit log, returning entries and record size in bytes.
    fn read_record_with_size<R: Read>(reader: &mut R) -> Result<Option<(Vec<ClogEntry>, usize)>> {
        // Read record header
        let mut header = [0u8; RECORD_HEADER_SIZE];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None); // EOF
            }
            Err(e) => return Err(e.into()),
        }

        // Parse header: record_type (4) + length (4) + checksum (4)
        let record_type = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let length = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let checksum = u32::from_le_bytes(header[8..12].try_into().unwrap());

        // Only support entry records (type 1)
        if record_type != 1 {
            return Err(TiSqlError::Internal(format!(
                "Unknown record type: {record_type}"
            )));
        }

        // Guard against corrupted length field causing OOM
        if length > MAX_RECORD_SIZE {
            let max_size = MAX_RECORD_SIZE;
            return Err(TiSqlError::Internal(format!(
                "Record size {length} exceeds maximum {max_size} (possibly corrupted)"
            )));
        }

        // Read data
        let mut data = vec![0u8; length];
        reader.read_exact(&mut data)?;

        // Validate checksum
        let computed_checksum = crc32(&data);
        if computed_checksum != checksum {
            return Err(TiSqlError::Internal(format!(
                "Checksum mismatch: expected {checksum}, got {computed_checksum}"
            )));
        }

        // Deserialize entries
        let entries: Vec<ClogEntry> = bincode::deserialize(&data).map_err(|e| {
            TiSqlError::Internal(format!("Failed to deserialize commit log entries: {e}"))
        })?;

        // Total record size = header + data
        let record_size = RECORD_HEADER_SIZE + length;

        Ok(Some((entries, record_size)))
    }

    /// Serialize a record to bytes (header + data) without writing.
    fn serialize_record(entries: &[ClogEntry]) -> Result<Vec<u8>> {
        let data = bincode::serialize(entries).map_err(|e| {
            TiSqlError::Internal(format!("Failed to serialize commit log entries: {e}"))
        })?;

        Self::validate_and_frame(&data)
    }

    /// Serialize reference-based entries to bytes (header + data) without writing.
    fn serialize_record_refs(entries: &[ClogEntryRef<'_>]) -> Result<Vec<u8>> {
        let data = bincode::serialize(entries).map_err(|e| {
            TiSqlError::Internal(format!("Failed to serialize commit log entries: {e}"))
        })?;

        Self::validate_and_frame(&data)
    }

    /// Validate record size and frame with header (type + length + checksum).
    fn validate_and_frame(data: &[u8]) -> Result<Vec<u8>> {
        if data.len() > u32::MAX as usize {
            return Err(TiSqlError::Internal(format!(
                "Record size {} exceeds maximum {} (u32::MAX)",
                data.len(),
                u32::MAX
            )));
        }
        if data.len() > MAX_RECORD_SIZE {
            return Err(TiSqlError::Internal(format!(
                "Record size {} exceeds maximum {}",
                data.len(),
                MAX_RECORD_SIZE
            )));
        }

        let checksum = crc32(data);
        let record_type: u32 = 1;

        let mut buf = Vec::with_capacity(RECORD_HEADER_SIZE + data.len());
        buf.extend_from_slice(&record_type.to_le_bytes());
        buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf.extend_from_slice(data);

        Ok(buf)
    }

    /// Write a record to the commit log
    fn write_record<W: Write>(writer: &mut W, entries: &[ClogEntry]) -> Result<()> {
        // Serialize entries
        let data = bincode::serialize(entries).map_err(|e| {
            TiSqlError::Internal(format!("Failed to serialize commit log entries: {e}"))
        })?;

        // Guard against records larger than u32::MAX (length field is u32)
        if data.len() > u32::MAX as usize {
            return Err(TiSqlError::Internal(format!(
                "Record size {} exceeds maximum {} (u32::MAX)",
                data.len(),
                u32::MAX
            )));
        }

        // Also enforce our practical limit
        if data.len() > MAX_RECORD_SIZE {
            return Err(TiSqlError::Internal(format!(
                "Record size {} exceeds maximum {}",
                data.len(),
                MAX_RECORD_SIZE
            )));
        }

        // Compute checksum
        let checksum = crc32(&data);

        // Write header: record_type (4) + length (4) + checksum (4)
        let record_type: u32 = 1; // Entry record
        writer.write_all(&record_type.to_le_bytes())?;
        writer.write_all(&(data.len() as u32).to_le_bytes())?;
        writer.write_all(&checksum.to_le_bytes())?;

        // Write data
        writer.write_all(&data)?;

        Ok(())
    }
}

/// Result of preparing clog ops for writing: serialized bytes + end LSN.
struct PreparedBatch {
    record_bytes: Vec<u8>,
    end_lsn: Lsn,
    entry_count: usize,
}

impl FileClogService {
    /// Prepare pre-built ClogOpRef entries using caller-provided LSN.
    fn prepare_ops_with_lsn(
        &self,
        txn_id: TxnId,
        ops: &[ClogOpRef<'_>],
        commit_ts: Timestamp,
        txn_lsn: Lsn,
    ) -> Result<PreparedBatch> {
        let current_lsn = self.lsn_provider.current_lsn();
        if txn_lsn >= current_lsn {
            return Err(TiSqlError::Internal(format!(
                "write_ops_with_lsn expects pre-allocated lsn (< current_lsn): lsn={txn_lsn}, current_lsn={current_lsn}"
            )));
        }

        let entry_count = ops.len() + 1; // +1 for commit record
        let mut entries: Vec<super::ClogEntryRef<'_>> = Vec::with_capacity(entry_count);

        for op in ops {
            entries.push(super::ClogEntryRef {
                lsn: txn_lsn,
                txn_id,
                op: *op,
            });
        }

        // Add commit record
        let end_lsn = txn_lsn;
        entries.push(super::ClogEntryRef {
            lsn: end_lsn,
            txn_id,
            op: ClogOpRef::Commit { commit_ts },
        });

        let record_bytes = Self::serialize_record_refs(&entries)?;

        Ok(PreparedBatch {
            record_bytes,
            end_lsn,
            entry_count,
        })
    }
}

impl ClogService for FileClogService {
    fn write(&self, batch: &mut ClogBatch, sync: bool) -> Result<ClogFsyncFuture> {
        if batch.is_empty() {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(Ok(()));
            return Ok(ClogFsyncFuture::new(self.lsn_provider.current_lsn(), rx));
        }

        // V2.5 contract: write(ClogBatch) is single-transaction only.
        // Mixed-txn batches are rejected to keep per-txn LSN semantics strict.
        let first_txn = batch.entries[0].txn_id;
        if batch.entries.iter().any(|entry| entry.txn_id != first_txn) {
            return Err(TiSqlError::Internal(
                "ClogBatch::write requires a single txn_id; split mixed batches by transaction"
                    .to_string(),
            ));
        }

        // Per-transaction LSN assignment: all entries in this batch share one LSN.
        let txn_lsn = self.lsn_provider.alloc_lsn();
        for entry in &mut batch.entries {
            entry.lsn = txn_lsn;
        }

        // Serialize to bytes
        let record_bytes = Self::serialize_record(batch.entries())?;

        // Failpoint: crash before clog fsync
        #[cfg(feature = "failpoints")]
        fail_point!("clog_before_sync");

        // Submit to group commit writer (batches fsync with concurrent writers)
        let rx = self
            .group_writer
            .submit(record_bytes, sync)
            .map_err(|e| TiSqlError::Internal(format!("Clog group commit failed: {e}")))?;

        // Failpoint: crash after clog fsync
        #[cfg(feature = "failpoints")]
        fail_point!("clog_after_sync");

        log_trace!(
            "Wrote {} entries to commit log, lsn={}-{}",
            batch.len(),
            txn_lsn,
            txn_lsn
        );

        Ok(ClogFsyncFuture::new(txn_lsn, rx))
    }

    fn write_ops(
        &self,
        txn_id: TxnId,
        ops: &[ClogOpRef<'_>],
        commit_ts: Timestamp,
        sync: bool,
    ) -> Result<ClogFsyncFuture> {
        if ops.is_empty() {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(Ok(()));
            return Ok(ClogFsyncFuture::new(self.lsn_provider.current_lsn(), rx));
        }

        let txn_lsn = self.lsn_provider.alloc_lsn();
        self.write_ops_with_lsn(txn_id, ops, commit_ts, txn_lsn, sync)
    }

    fn write_ops_with_lsn(
        &self,
        txn_id: TxnId,
        ops: &[ClogOpRef<'_>],
        commit_ts: Timestamp,
        lsn: Lsn,
        sync: bool,
    ) -> Result<ClogFsyncFuture> {
        if ops.is_empty() {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(Ok(()));
            return Ok(ClogFsyncFuture::new(self.lsn_provider.current_lsn(), rx));
        }

        let prepared = self.prepare_ops_with_lsn(txn_id, ops, commit_ts, lsn)?;
        let end_lsn = prepared.end_lsn;

        #[cfg(feature = "failpoints")]
        fail_point!("clog_before_sync");

        let rx = self
            .group_writer
            .submit(prepared.record_bytes, sync)
            .map_err(|e| TiSqlError::Internal(format!("Clog group commit failed: {e}")))?;

        #[cfg(feature = "failpoints")]
        fail_point!("clog_after_sync");

        log_trace!(
            "Wrote {} entries to commit log (zero-copy ops), lsn=...-{}",
            prepared.entry_count,
            prepared.end_lsn
        );

        Ok(ClogFsyncFuture::new(end_lsn, rx))
    }

    fn sync(&self) -> Result<ClogFsyncFuture> {
        // Submit an empty write with sync=true to force a flush+fsync
        let rx = self
            .group_writer
            .submit(Vec::new(), true)
            .map_err(|e| TiSqlError::Internal(format!("Clog group commit sync failed: {e}")))?;
        Ok(ClogFsyncFuture::new(self.lsn_provider.current_lsn(), rx))
    }

    fn read_all(&self) -> Result<Vec<ClogEntry>> {
        let clog_path = self.config.clog_path();
        let file = File::open(&clog_path)?;
        let mut reader = BufReader::new(file);
        Self::validate_header(&mut reader)?;
        Self::read_entries(&mut reader)
    }

    fn current_lsn(&self) -> Lsn {
        self.lsn_provider.current_lsn()
    }

    async fn close(&self) -> Result<()> {
        // Force a final sync through the group writer, then it will be
        // cleanly shut down when dropped.
        let future = self.sync()?;
        future.await?;
        Ok(())
    }

    fn shutdown(&self) {
        self.group_writer.shutdown();
    }
}

// ============================================================================
// Clog Truncation / GC API
// ============================================================================

/// Statistics returned from clog truncation.
#[derive(Debug, Default)]
pub struct TruncateStats {
    /// Number of entries removed
    pub entries_removed: usize,
    /// Number of entries kept
    pub entries_kept: usize,
    /// Bytes freed
    pub bytes_freed: u64,
    /// New file size
    pub new_file_size: u64,
}

impl FileClogService {
    /// Truncate clog entries with lsn <= safe_lsn.
    ///
    /// This rewrites the clog file keeping only entries with lsn > safe_lsn.
    /// The safe_lsn is typically the flushed_lsn from the LSM engine's Version,
    /// indicating all entries up to that LSN are persisted in SST files.
    ///
    /// **Transaction atomicity**: Entries are grouped by `txn_id`. If ANY entry
    /// of a transaction has `lsn > safe_lsn`, ALL entries of that transaction
    /// are kept. This prevents orphaned Commit records without their Put/Delete
    /// records, which would cause data loss on recovery (recovery would see a
    /// Commit but no writes → skip the transaction).
    ///
    /// Uses async stop-drain-restart: stops the writer, drains in-flight
    /// requests, rewrites the file via `spawn_blocking`, then restarts
    /// the writer with the new file.
    ///
    /// # Safety
    ///
    /// Only call this when you're certain all entries with lsn <= safe_lsn
    /// have been durably persisted elsewhere (e.g., in SST files).
    pub async fn truncate_to(&self, safe_lsn: Lsn) -> Result<TruncateStats> {
        // Stop the writer and drain all in-flight requests.
        self.group_writer
            .stop_and_drain()
            .await
            .map_err(|e| TiSqlError::Internal(format!("Clog stop_and_drain failed: {e}")))?;

        let result: Result<TruncateStats> = async {
            let clog_path = self.config.clog_path();
            let old_size = std::fs::metadata(&clog_path).map(|m| m.len()).unwrap_or(0);

            // Read all entries (safe — writer is drained)
            let entries = self.read_all()?;

            // Compute the max LSN per transaction. A transaction's entries must be
            // kept or removed as a unit: individual per-entry LSN partitioning would
            // split Put(lsn=N) / Commit(lsn=N+K) when safe_lsn falls between them.
            let mut txn_max_lsn: std::collections::HashMap<u64, Lsn> =
                std::collections::HashMap::new();
            for entry in &entries {
                let max = txn_max_lsn.entry(entry.txn_id).or_insert(0);
                *max = (*max).max(entry.lsn);
            }

            // Partition by transaction: keep entire txn if its max LSN > safe_lsn.
            let (to_keep, to_remove): (Vec<_>, Vec<_>) = entries
                .into_iter()
                .partition(|e| txn_max_lsn[&e.txn_id] > safe_lsn);

            if to_remove.is_empty() {
                // Nothing to truncate — restart the writer
                let file = crate::io::DmaFile::open_append_buffered(&clog_path)
                    .map_err(|e| TiSqlError::Internal(format!("Failed to reopen clog: {e}")))?;
                self.group_writer.restart(
                    file,
                    Arc::clone(&self.io_service),
                    Some(&self.io_handle),
                );
                return Ok(TruncateStats {
                    entries_removed: 0,
                    entries_kept: to_keep.len(),
                    bytes_freed: 0,
                    new_file_size: old_size,
                });
            }

            // Rewrite kept entries via spawn_blocking (file I/O)
            let config_clone = self.config.clone();
            let (removed, kept) = tokio::task::spawn_blocking(move || -> Result<(usize, usize)> {
                let clog_path = config_clone.clog_path();
                let temp_path = clog_path.with_extension("clog.tmp");
                {
                    let temp_file = std::fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(&temp_path)?;
                    let mut temp_writer = BufWriter::new(temp_file);

                    Self::write_header(&mut temp_writer)?;
                    if !to_keep.is_empty() {
                        Self::write_record(&mut temp_writer, &to_keep)?;
                    }

                    temp_writer.flush()?;
                    temp_writer.get_ref().sync_data()?;
                }

                rename_durable(&temp_path, &clog_path)?;
                Ok((to_remove.len(), to_keep.len()))
            })
            .await
            .map_err(|e| TiSqlError::Internal(format!("Clog rewrite task panicked: {e}")))??;

            let file = crate::io::DmaFile::open_append_buffered(self.config.clog_path())
                .map_err(|e| TiSqlError::Internal(format!("Failed to reopen clog: {e}")))?;
            self.group_writer
                .restart(file, Arc::clone(&self.io_service), Some(&self.io_handle));

            let new_size = std::fs::metadata(self.config.clog_path())
                .map(|m| m.len())
                .unwrap_or(0);

            log_info!(
                "Truncated clog to safe_lsn={}: removed {} entries, kept {}",
                safe_lsn,
                removed,
                kept
            );

            Ok(TruncateStats {
                entries_removed: removed,
                entries_kept: kept,
                bytes_freed: old_size.saturating_sub(new_size),
                new_file_size: new_size,
            })
        }
        .await;

        if let Err(e) = &result {
            // stop_and_drain() moved writer to Draining; any error before restart
            // must transition to Failed so submitters return error instead of waiting.
            self.group_writer.set_failed(format!("{e}"));
        }

        result
    }

    /// Get the oldest LSN still in the clog.
    ///
    /// Returns 0 if the clog is empty.
    pub fn oldest_lsn(&self) -> Result<Lsn> {
        let entries = self.read_all()?;
        Ok(entries.first().map(|e| e.lsn).unwrap_or(0))
    }

    /// Get the clog file size in bytes.
    pub fn file_size(&self) -> Result<u64> {
        let clog_path = self.config.clog_path();
        let metadata = std::fs::metadata(&clog_path)?;
        Ok(metadata.len())
    }
}

/// Compute CRC32 checksum
fn crc32(data: &[u8]) -> u32 {
    // Simple CRC32 implementation (IEEE polynomial)
    let mut crc: u32 = 0xFFFFFFFF;
    for byte in data {
        crc ^= *byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clog::{ClogOp, ClogOpRef};
    use std::io::{Seek, SeekFrom};
    use tempfile::tempdir;

    fn make_test_io() -> Arc<crate::io::IoService> {
        crate::io::IoService::new_for_test(32).unwrap()
    }

    #[tokio::test]
    async fn test_open_and_write() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        let mut batch = ClogBatch::new();
        batch.add_put(1, b"key1".to_vec(), b"value1".to_vec());
        batch.add_put(1, b"key2".to_vec(), b"value2".to_vec());

        let lsn = service.write(&mut batch, true).unwrap().await.unwrap();
        assert_eq!(lsn, 1);

        service.close().await.unwrap();

        // Reopen and verify
        let service2 =
            FileClogService::open(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();
        assert_eq!(service2.current_lsn(), 2);

        let entries = service2.read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].lsn, 1);
        assert_eq!(entries[1].lsn, 1);
    }

    #[tokio::test]
    async fn test_recover() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        // Write some entries
        {
            let service = FileClogService::open(
                config.clone(),
                make_test_io(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap();
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"k1".to_vec(), b"v1".to_vec());
            batch.add_delete(1, b"k2".to_vec());
            batch.add_commit(1, 100);
            service.write(&mut batch, true).unwrap().await.unwrap();
            service.close().await.unwrap();
        }

        // Recover
        let (service, entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();
        assert_eq!(entries.len(), 3);

        match &entries[0].op {
            ClogOp::Put { key, value } => {
                assert_eq!(key, b"k1");
                assert_eq!(value, b"v1");
            }
            _ => panic!("Expected Put"),
        }

        match &entries[1].op {
            ClogOp::Delete { key } => {
                assert_eq!(key, b"k2");
            }
            _ => panic!("Expected Delete"),
        }

        match &entries[2].op {
            ClogOp::Commit { commit_ts } => {
                assert_eq!(*commit_ts, 100);
            }
            _ => panic!("Expected Commit"),
        }

        assert_eq!(service.current_lsn(), 2);
    }

    #[test]
    fn test_crc32() {
        let data = b"hello world";
        let checksum = crc32(data);
        // Known CRC32 value for "hello world"
        assert_eq!(checksum, 0x0D4A1185);
    }

    // ========================================================================
    // Crash Recovery Tests - simulate kill -9 scenarios
    // ========================================================================

    /// Test recovery with truncated record header (partial header write).
    /// Simulates crash during the 12-byte header write.
    #[tokio::test]
    async fn test_recover_truncated_header() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let clog_path = config.clog_path();

        // Write one valid record
        {
            let service = FileClogService::open(
                config.clone(),
                make_test_io(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap();
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"valid_key".to_vec(), b"valid_value".to_vec());
            batch.add_commit(1, 100);
            service.write(&mut batch, true).unwrap().await.unwrap();
            service.close().await.unwrap();
        }

        // Append partial header (6 bytes of a 12-byte header) to simulate crash
        {
            let mut file = OpenOptions::new().append(true).open(&clog_path).unwrap();
            // Partial record header: only 6 of 12 bytes
            file.write_all(&[1, 0, 0, 0, 50, 0]).unwrap();
            file.sync_all().unwrap();
        }

        // Recovery should skip the truncated header and recover valid entries
        let (service, entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();

        // Should recover the 2 valid entries (Put + Commit)
        assert_eq!(
            entries.len(),
            2,
            "Should recover valid entries before truncation"
        );
        assert_eq!(
            service.current_lsn(),
            2,
            "LSN should continue from valid entries"
        );
    }

    /// Test recovery with truncated record data (header complete, data partial).
    /// Simulates crash during the data write after header was written.
    #[tokio::test]
    async fn test_recover_truncated_data() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let clog_path = config.clog_path();

        // Write one valid record
        {
            let service = FileClogService::open(
                config.clone(),
                make_test_io(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap();
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"key1".to_vec(), b"value1".to_vec());
            batch.add_commit(1, 100);
            service.write(&mut batch, true).unwrap().await.unwrap();
            service.close().await.unwrap();
        }

        // Append a complete header but truncated data
        {
            let mut file = OpenOptions::new().append(true).open(&clog_path).unwrap();
            // Record header: type=1, length=100 (but we won't write 100 bytes), checksum=0
            let record_type: u32 = 1;
            let length: u32 = 100; // Claims 100 bytes of data
            let checksum: u32 = 0; // Wrong checksum
            file.write_all(&record_type.to_le_bytes()).unwrap();
            file.write_all(&length.to_le_bytes()).unwrap();
            file.write_all(&checksum.to_le_bytes()).unwrap();
            // Write only 10 bytes of the claimed 100
            file.write_all(&[0u8; 10]).unwrap();
            file.sync_all().unwrap();
        }

        // Recovery should skip the truncated record
        let (service, entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();

        assert_eq!(entries.len(), 2, "Should recover entries before truncation");
        assert_eq!(service.current_lsn(), 2);
    }

    /// Test recovery with corrupted checksum.
    /// Simulates data corruption (bit flip) after write.
    #[tokio::test]
    async fn test_recover_corrupted_checksum() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let clog_path = config.clog_path();

        // Write two valid records
        {
            let service = FileClogService::open(
                config.clone(),
                make_test_io(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap();

            // First batch
            let mut batch1 = ClogBatch::new();
            batch1.add_put(1, b"key1".to_vec(), b"value1".to_vec());
            batch1.add_commit(1, 100);
            service.write(&mut batch1, true).unwrap().await.unwrap();

            // Second batch
            let mut batch2 = ClogBatch::new();
            batch2.add_put(2, b"key2".to_vec(), b"value2".to_vec());
            batch2.add_commit(2, 200);
            service.write(&mut batch2, true).unwrap().await.unwrap();

            service.close().await.unwrap();
        }

        // Corrupt the checksum of the second record
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&clog_path)
                .unwrap();

            // Skip to the checksum of the second record
            // Header (16) + first record header (12) + first record data (variable)
            // We need to find where the second record starts

            // Read the file to find the structure
            file.seek(SeekFrom::Start(16)).unwrap(); // Skip file header

            // Read first record header
            let mut header = [0u8; 12];
            file.read_exact(&mut header).unwrap();
            let first_data_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as u64;

            // Skip first record data
            file.seek(SeekFrom::Current(first_data_len as i64)).unwrap();

            // Now at second record header - corrupt the checksum (offset 8-12 in header)
            let second_record_start = file.stream_position().unwrap();
            file.seek(SeekFrom::Start(second_record_start + 8)).unwrap();

            // Write corrupted checksum
            file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
            file.sync_all().unwrap();
        }

        // Recovery should stop at corrupted record
        let (service, entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();

        // Should only recover the first batch (2 entries)
        assert_eq!(entries.len(), 2, "Should stop at corrupted record");
        assert_eq!(service.current_lsn(), 2);

        // Verify we got the first batch
        match &entries[0].op {
            ClogOp::Put { key, value } => {
                assert_eq!(key, b"key1");
                assert_eq!(value, b"value1");
            }
            _ => panic!("Expected Put"),
        }
    }

    /// Test recovery after simulated crash with multiple valid batches.
    /// Verifies that all data before crash point is recovered.
    #[tokio::test]
    async fn test_recover_multiple_batches_before_crash() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let clog_path = config.clog_path();

        // Write multiple batches
        {
            let service = FileClogService::open(
                config.clone(),
                make_test_io(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap();

            for i in 1..=5 {
                let mut batch = ClogBatch::new();
                batch.add_put(
                    i,
                    format!("key{i}").into_bytes(),
                    format!("value{i}").into_bytes(),
                );
                batch.add_commit(i, i * 100);
                service.write(&mut batch, true).unwrap().await.unwrap();
            }
            service.close().await.unwrap();
        }

        // Append garbage to simulate partial write during crash
        {
            let mut file = OpenOptions::new().append(true).open(&clog_path).unwrap();
            file.write_all(b"GARBAGE_DATA_FROM_CRASH").unwrap();
            file.sync_all().unwrap();
        }

        // Recovery should get all 5 batches (10 entries)
        let (service, entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();

        assert_eq!(entries.len(), 10, "Should recover all valid entries");
        assert_eq!(service.current_lsn(), 6);

        // Verify all 5 puts are there
        let puts: Vec<_> = entries
            .iter()
            .filter(|e| matches!(e.op, ClogOp::Put { .. }))
            .collect();
        assert_eq!(puts.len(), 5);
    }

    /// Test that recovery can continue writing after crash.
    /// Simulates: write -> crash -> recover -> write more -> verify all data.
    #[tokio::test]
    async fn test_recover_and_continue_writing() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        // First session: write some data
        {
            let service = FileClogService::open(
                config.clone(),
                make_test_io(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap();
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"before_crash".to_vec(), b"v1".to_vec());
            batch.add_commit(1, 100);
            service.write(&mut batch, true).unwrap().await.unwrap();
            // Simulate crash - no close() call
        }

        // Second session: recover and write more
        let (service, entries) = FileClogService::recover(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();
        assert_eq!(entries.len(), 2);

        // Write more data after recovery
        let mut batch = ClogBatch::new();
        batch.add_put(2, b"after_crash".to_vec(), b"v2".to_vec());
        batch.add_commit(2, 200);
        service.write(&mut batch, true).unwrap().await.unwrap();
        service.close().await.unwrap();

        // Third session: verify all data
        let (_, all_entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();
        assert_eq!(
            all_entries.len(),
            4,
            "Should have entries from both sessions"
        );

        // Verify both puts are there
        let keys: Vec<_> = all_entries
            .iter()
            .filter_map(|e| match &e.op {
                ClogOp::Put { key, .. } => Some(key.clone()),
                _ => None,
            })
            .collect();
        assert!(keys.contains(&b"before_crash".to_vec()));
        assert!(keys.contains(&b"after_crash".to_vec()));
    }

    /// Test that corrupted tail is truncated before continuing writes.
    ///
    /// This test verifies the fix for a durability bug where:
    /// 1. Write valid data
    /// 2. Append garbage (simulating crash during write)
    /// 3. Recover (should see valid data only)
    /// 4. Write more data
    /// 5. Recover again - WITHOUT truncation fix, new data would be lost!
    ///
    /// The fix truncates the corrupted tail during open/recover, so subsequent
    /// writes are appended directly after the last valid record.
    #[tokio::test]
    async fn test_truncate_corrupted_tail_before_write() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let clog_path = config.clog_path();

        // Phase 1: Write initial valid data
        {
            let service = FileClogService::open(
                config.clone(),
                make_test_io(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap();
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"key1".to_vec(), b"value1".to_vec());
            batch.add_commit(1, 100);
            service.write(&mut batch, true).unwrap().await.unwrap();
            service.close().await.unwrap();
        }

        // Phase 2: Append garbage to simulate crash during write
        let garbage = b"GARBAGE_FROM_PARTIAL_WRITE";
        {
            let mut file = OpenOptions::new().append(true).open(&clog_path).unwrap();
            file.write_all(garbage).unwrap();
            file.sync_all().unwrap();
        }

        // Verify garbage was appended
        let file_size_with_garbage = std::fs::metadata(&clog_path).unwrap().len();

        // Phase 3: Recover and write more data
        let (service, entries) = FileClogService::recover(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();
        assert_eq!(entries.len(), 2, "Should recover 2 entries before garbage");

        // After recovery, garbage should be truncated
        let file_size_after_recover = std::fs::metadata(&clog_path).unwrap().len();
        assert!(
            file_size_after_recover < file_size_with_garbage,
            "File should be truncated: {file_size_after_recover} should be < {file_size_with_garbage}"
        );

        // Write new data after recovery
        let mut batch = ClogBatch::new();
        batch.add_put(2, b"key2".to_vec(), b"value2".to_vec());
        batch.add_commit(2, 200);
        service.write(&mut batch, true).unwrap().await.unwrap();
        service.close().await.unwrap();

        // Phase 4: Recover again and verify all data is present
        let (_, all_entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();
        assert_eq!(
            all_entries.len(),
            4,
            "Should have all 4 entries: 2 from phase 1 + 2 from phase 3"
        );

        // Verify both puts are there
        let keys: Vec<_> = all_entries
            .iter()
            .filter_map(|e| match &e.op {
                ClogOp::Put { key, .. } => Some(key.clone()),
                _ => None,
            })
            .collect();
        assert!(keys.contains(&b"key1".to_vec()), "key1 should be recovered");
        assert!(keys.contains(&b"key2".to_vec()), "key2 should be recovered");
    }

    /// Test recovery with empty file (only header, no records).
    /// Simulates crash immediately after file creation.
    #[tokio::test]
    async fn test_recover_empty_clog() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        // Create file but don't write any records
        {
            let service = FileClogService::open(
                config.clone(),
                make_test_io(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap();
            // Simulate crash before any writes
            service.close().await.unwrap();
        }

        // Recovery should succeed with empty entries
        let (service, entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();
        assert_eq!(entries.len(), 0);
        assert_eq!(service.current_lsn(), 1);
    }

    /// Test concurrent clog writes produce monotonic LSNs in file order.
    /// This verifies the fix for the LSN out-of-order issue where concurrent
    /// writers could produce entries with non-monotonic LSNs in the file.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_concurrent_writes_lsn_ordering() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = Arc::new(
            FileClogService::open(
                config.clone(),
                make_test_io(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap(),
        );

        let num_threads = 8;
        let writes_per_thread = 100;
        let barrier = Arc::new(Barrier::new(num_threads));

        let mut handles = Vec::new();
        for tid in 0..num_threads {
            let service = Arc::clone(&service);
            let barrier = Arc::clone(&barrier);

            handles.push(thread::spawn(move || {
                barrier.wait();

                for i in 0..writes_per_thread {
                    let mut batch = ClogBatch::new();
                    batch.add_put(
                        tid as u64,
                        format!("key_{tid}_{i}").into_bytes(),
                        format!("value_{tid}_{i}").into_bytes(),
                    );
                    crate::io::block_on_sync(service.write(&mut batch, false).unwrap()).unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        service.close().await.unwrap();

        // Recover and verify LSN ordering
        let (recovered_service, entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();

        // Verify all entries were written
        let expected_count = num_threads * writes_per_thread;
        assert_eq!(
            entries.len(),
            expected_count,
            "Expected {} entries, got {}",
            expected_count,
            entries.len()
        );

        // With group commit, file order may differ from LSN allocation order
        // because concurrent writers submit to a channel and the writer thread
        // drains them in arrival order. Verify all LSNs are unique and present.
        let mut lsns: Vec<u64> = entries.iter().map(|e| e.lsn).collect();
        lsns.sort();
        lsns.dedup();
        assert_eq!(lsns.len(), expected_count, "All LSNs should be unique");

        // LSNs should be contiguous 1..=expected_count
        assert_eq!(lsns[0], 1);
        assert_eq!(*lsns.last().unwrap(), expected_count as u64);

        // Verify current_lsn is max(lsn) + 1
        let max_lsn = entries.iter().map(|e| e.lsn).max().unwrap();
        assert_eq!(
            recovered_service.current_lsn(),
            max_lsn + 1,
            "current_lsn should be max(lsn) + 1"
        );
    }

    /// Stress test group commit with many concurrent transactions.
    ///
    /// Each transaction writes (Put + Commit) with sync=true. We verify all
    /// transactions are durably recovered with exactly one commit record each.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_group_commit_concurrent_transactions() {
        use std::collections::HashSet;
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = Arc::new(
            FileClogService::open(
                config.clone(),
                make_test_io(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap(),
        );

        let txn_count = 64u64;
        let mut handles = Vec::with_capacity(txn_count as usize);

        for txn_id in 1..=txn_count {
            let service = Arc::clone(&service);
            handles.push(tokio::spawn(async move {
                let mut batch = ClogBatch::new();
                batch.add_put(
                    txn_id,
                    format!("tx_key_{txn_id:04}").into_bytes(),
                    format!("tx_val_{txn_id:04}").into_bytes(),
                );
                batch.add_commit(txn_id, 10_000 + txn_id);
                service.write(&mut batch, true).unwrap().await.unwrap()
            }));
        }

        let mut end_lsns = Vec::with_capacity(txn_count as usize);
        for handle in handles {
            end_lsns.push(handle.await.unwrap());
        }
        end_lsns.sort_unstable();
        end_lsns.dedup();
        assert_eq!(
            end_lsns.len(),
            txn_count as usize,
            "Each transaction should have a unique end_lsn"
        );

        service.close().await.unwrap();

        let (_, entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();
        assert_eq!(
            entries.len(),
            (txn_count * 2) as usize,
            "Each transaction should produce Put + Commit"
        );

        let mut put_txn_ids = HashSet::new();
        let mut commit_txn_ids = HashSet::new();

        for entry in &entries {
            match &entry.op {
                ClogOp::Put { key, value } => {
                    let suffix = format!("{:04}", entry.txn_id);
                    assert!(
                        key.ends_with(suffix.as_bytes()),
                        "Put key should include txn suffix {suffix}: {key:?}"
                    );
                    assert!(
                        value.ends_with(suffix.as_bytes()),
                        "Put value should include txn suffix {suffix}: {value:?}"
                    );
                    put_txn_ids.insert(entry.txn_id);
                }
                ClogOp::Commit { commit_ts } => {
                    assert_eq!(
                        *commit_ts,
                        10_000 + entry.txn_id,
                        "Commit ts should match transaction-specific value"
                    );
                    commit_txn_ids.insert(entry.txn_id);
                }
                ClogOp::Delete { .. } | ClogOp::Rollback => {
                    panic!(
                        "Unexpected op in concurrent transaction test: {:?}",
                        entry.op
                    )
                }
            }
        }

        assert_eq!(
            put_txn_ids.len(),
            txn_count as usize,
            "Each transaction should have exactly one Put"
        );
        assert_eq!(
            commit_txn_ids.len(),
            txn_count as usize,
            "Each transaction should have exactly one Commit"
        );
    }

    /// Corruption in the middle of the log should truncate everything after the
    /// last valid prefix (including records that were valid before truncation).
    #[tokio::test]
    async fn test_recover_corrupted_middle_record_truncates_tail() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let clog_path = config.clog_path();

        // Write 3 valid records.
        {
            let service = FileClogService::open(
                config.clone(),
                make_test_io(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap();
            for i in 1..=3u64 {
                let mut batch = ClogBatch::new();
                batch.add_put(
                    i,
                    format!("k{i}").into_bytes(),
                    format!("v{i}").into_bytes(),
                );
                service.write(&mut batch, true).unwrap().await.unwrap();
            }
            service.close().await.unwrap();
        }

        let file_size_before = std::fs::metadata(&clog_path).unwrap().len();

        // Corrupt checksum of the second record header.
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&clog_path)
                .unwrap();

            file.seek(SeekFrom::Start(FILE_HEADER_SIZE as u64)).unwrap();
            let mut first_header = [0u8; RECORD_HEADER_SIZE];
            file.read_exact(&mut first_header).unwrap();
            let first_len = u32::from_le_bytes(first_header[4..8].try_into().unwrap()) as u64;

            let second_record_start =
                FILE_HEADER_SIZE as u64 + RECORD_HEADER_SIZE as u64 + first_len;
            file.seek(SeekFrom::Start(second_record_start + 8)).unwrap();
            file.write_all(&0xFFFF_FFFFu32.to_le_bytes()).unwrap();
            file.sync_all().unwrap();
        }

        // Recovery should keep only first record and truncate the rest.
        let (service, entries) = FileClogService::recover(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();
        assert_eq!(entries.len(), 1, "Only first valid record should survive");
        assert_eq!(entries[0].lsn, 1);

        let file_size_after_recover = std::fs::metadata(&clog_path).unwrap().len();
        assert!(
            file_size_after_recover < file_size_before,
            "File should be truncated after recovery of corrupted middle record"
        );

        // New writes should append from the valid prefix only.
        let mut batch = ClogBatch::new();
        batch.add_put(9, b"post_recover".to_vec(), b"ok".to_vec());
        let lsn = service.write(&mut batch, true).unwrap().await.unwrap();
        assert_eq!(
            lsn, 2,
            "LSN should continue from the first surviving record"
        );
        service.close().await.unwrap();

        let (_, recovered) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();
        assert_eq!(
            recovered.len(),
            2,
            "Recovered log should contain first original record + post-recovery record"
        );
        assert_eq!(recovered[0].lsn, 1);
        assert_eq!(recovered[1].lsn, 2);
    }

    /// Replaying recovery multiple times should be idempotent.
    ///
    /// Running recover() repeatedly must not duplicate entries, and new writes
    /// after recovery should append once.
    #[tokio::test]
    async fn test_recover_idempotent_replay() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        {
            let service = FileClogService::open(
                config.clone(),
                make_test_io(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap();
            for i in 1..=3u64 {
                let mut batch = ClogBatch::new();
                batch.add_put(
                    i,
                    format!("k{i}").into_bytes(),
                    format!("v{i}").into_bytes(),
                );
                batch.add_commit(i, 100 + i);
                service.write(&mut batch, true).unwrap().await.unwrap();
            }
            service.close().await.unwrap();
        }

        let (service1, entries1) = FileClogService::recover(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();
        let sig1 = bincode::serialize(&entries1).unwrap();
        service1.close().await.unwrap();

        let (service2, entries2) = FileClogService::recover(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();
        let sig2 = bincode::serialize(&entries2).unwrap();
        assert_eq!(
            sig1, sig2,
            "Repeated recover() should return identical entry stream"
        );

        let mut batch = ClogBatch::new();
        batch.add_put(99, b"k99".to_vec(), b"v99".to_vec());
        service2.write(&mut batch, true).unwrap().await.unwrap();
        service2.close().await.unwrap();

        let (_, entries3) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();
        assert_eq!(
            entries3.len(),
            entries2.len() + 1,
            "Post-recovery write should append exactly one new entry"
        );
        assert_eq!(
            bincode::serialize(&entries3[..entries2.len()]).unwrap(),
            sig2,
            "Prefix should remain unchanged after idempotent recover + append"
        );
    }

    // ========================================================================
    // Shared LSN Provider Tests
    // ========================================================================

    /// Test clog with shared LSN provider.
    /// Verifies that clog uses the shared provider for LSN allocation.
    #[tokio::test]
    async fn test_shared_lsn_provider() {
        use crate::lsn::new_lsn_provider;

        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let provider = new_lsn_provider();

        // Allocate some LSNs before opening clog
        assert_eq!(provider.alloc_lsn(), 1);
        assert_eq!(provider.alloc_lsn(), 2);

        // Open clog with shared provider
        let service = FileClogService::open_with_lsn_provider(
            config.clone(),
            provider.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Current LSN should be from provider
        assert_eq!(service.current_lsn(), 3);

        // Write to clog - should allocate LSN 3
        let mut batch = ClogBatch::new();
        batch.add_put(1, b"key1".to_vec(), b"value1".to_vec());
        let lsn = service.write(&mut batch, true).unwrap().await.unwrap();
        assert_eq!(lsn, 3);

        // Provider should have advanced
        assert_eq!(provider.current_lsn(), 4);

        // Write more
        let mut batch2 = ClogBatch::new();
        batch2.add_put(2, b"key2".to_vec(), b"value2".to_vec());
        let lsn2 = service.write(&mut batch2, true).unwrap().await.unwrap();
        assert_eq!(lsn2, 4);

        service.close().await.unwrap();

        // Recover with same provider
        let (recovered, entries) = FileClogService::recover_with_lsn_provider(
            config,
            provider.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].lsn, 3);
        assert_eq!(entries[1].lsn, 4);
        assert_eq!(recovered.current_lsn(), 5);
        assert_eq!(provider.current_lsn(), 5);
    }

    /// Test that shared provider is updated when clog has higher LSN.
    #[tokio::test]
    async fn test_shared_provider_updated_from_clog() {
        use crate::lsn::new_lsn_provider;

        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        // First: write to clog with standalone mode
        {
            let service = FileClogService::open(
                config.clone(),
                make_test_io(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap();
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"k1".to_vec(), b"v1".to_vec());
            batch.add_put(1, b"k2".to_vec(), b"v2".to_vec());
            batch.add_put(1, b"k3".to_vec(), b"v3".to_vec());
            service.write(&mut batch, true).unwrap().await.unwrap();
            // All entries share txn LSN=1.
            service.close().await.unwrap();
        }

        // Create fresh provider starting at 1
        let provider = new_lsn_provider();
        assert_eq!(provider.current_lsn(), 1);

        // Recover with shared provider - it should be updated to 2.
        let (recovered, entries) = FileClogService::recover_with_lsn_provider(
            config,
            provider.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(provider.current_lsn(), 2);
        assert_eq!(recovered.current_lsn(), 2);
    }

    /// Test read_from_lsn for partial replay during recovery.
    #[tokio::test]
    async fn test_read_from_lsn() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Write 5 entries with LSN 1-5
        for i in 1..=5 {
            let mut batch = ClogBatch::new();
            batch.add_put(
                i,
                format!("key{i}").into_bytes(),
                format!("val{i}").into_bytes(),
            );
            service.write(&mut batch, true).unwrap().await.unwrap();
        }

        // Read from LSN 3
        let entries = service.read_from_lsn(3).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].lsn, 3);
        assert_eq!(entries[1].lsn, 4);
        assert_eq!(entries[2].lsn, 5);

        // Read from LSN 1 (all entries)
        let all = service.read_from_lsn(1).unwrap();
        assert_eq!(all.len(), 5);

        // Read from LSN 6 (nothing)
        let none = service.read_from_lsn(6).unwrap();
        assert!(none.is_empty());

        service.close().await.unwrap();
    }

    /// Test max_lsn helper.
    #[tokio::test]
    async fn test_max_lsn() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Empty clog
        assert_eq!(service.max_lsn().unwrap(), 0);

        // Write some entries
        let mut batch = ClogBatch::new();
        batch.add_put(1, b"k1".to_vec(), b"v1".to_vec());
        batch.add_put(1, b"k2".to_vec(), b"v2".to_vec());
        service.write(&mut batch, true).unwrap().await.unwrap();

        assert_eq!(service.max_lsn().unwrap(), 1);

        service.close().await.unwrap();
    }

    // ========================================================================
    // Empty Batch Tests
    // ========================================================================

    /// Test write() with empty ClogBatch returns current LSN without writing.
    #[tokio::test]
    async fn test_write_empty_clog_batch() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Initial LSN is 1
        assert_eq!(service.current_lsn(), 1);

        // Write empty batch
        let mut empty_batch = ClogBatch::new();
        assert!(empty_batch.is_empty());
        let lsn = service
            .write(&mut empty_batch, true)
            .unwrap()
            .await
            .unwrap();

        // Should return current LSN without incrementing
        assert_eq!(lsn, 1);
        assert_eq!(service.current_lsn(), 1);

        // Verify nothing was written
        let entries = service.read_all().unwrap();
        assert!(entries.is_empty());

        service.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_write_rejects_mixed_txn_batch() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        let mut batch = ClogBatch::new();
        batch.add_put(1, b"k1".to_vec(), b"v1".to_vec());
        batch.add_put(2, b"k2".to_vec(), b"v2".to_vec());

        let err = match service.write(&mut batch, true) {
            Ok(_) => panic!("mixed-txn ClogBatch should be rejected"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("single txn_id"));

        // Verify that no entries were written.
        assert!(service.read_all().unwrap().is_empty());
        service.close().await.unwrap();
    }

    // ========================================================================
    // sync() and close() Tests
    // ========================================================================

    /// Test sync() flushes pending writes to disk.
    #[tokio::test]
    async fn test_sync() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Write without sync
        let mut batch = ClogBatch::new();
        batch.add_put(1, b"key".to_vec(), b"value".to_vec());
        service.write(&mut batch, false).unwrap().await.unwrap(); // sync=false

        // Explicitly sync
        service.sync().unwrap().await.unwrap();

        // Verify data is readable
        let entries = service.read_all().unwrap();
        assert_eq!(entries.len(), 1);

        service.close().await.unwrap();
    }

    /// Test close() properly flushes and syncs.
    #[tokio::test]
    async fn test_close() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        {
            let service = FileClogService::open(
                config.clone(),
                make_test_io(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap();
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"key".to_vec(), b"value".to_vec());
            service.write(&mut batch, false).unwrap().await.unwrap(); // No sync during write

            // Close should flush and sync
            service.close().await.unwrap();
        }

        // Verify data persisted after close
        let (_, entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();
        assert_eq!(entries.len(), 1);
    }

    // ========================================================================
    // oldest_lsn() and file_size() Tests
    // ========================================================================

    /// Test oldest_lsn() returns correct value.
    #[tokio::test]
    async fn test_oldest_lsn() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Empty clog
        assert_eq!(service.oldest_lsn().unwrap(), 0);

        // Write entries
        let mut batch = ClogBatch::new();
        batch.add_put(1, b"k1".to_vec(), b"v1".to_vec());
        batch.add_put(1, b"k2".to_vec(), b"v2".to_vec());
        batch.add_put(1, b"k3".to_vec(), b"v3".to_vec());
        service.write(&mut batch, true).unwrap().await.unwrap();

        // Oldest should be 1
        assert_eq!(service.oldest_lsn().unwrap(), 1);

        service.close().await.unwrap();
    }

    /// Test file_size() returns correct value.
    #[tokio::test]
    async fn test_file_size() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Initial file has just header (16 bytes)
        let initial_size = service.file_size().unwrap();
        assert_eq!(initial_size, FILE_HEADER_SIZE as u64);

        // Write some data
        let mut batch = ClogBatch::new();
        batch.add_put(1, b"key".to_vec(), b"value".to_vec());
        service.write(&mut batch, true).unwrap().await.unwrap();

        // File should be larger now
        let new_size = service.file_size().unwrap();
        assert!(new_size > initial_size);

        service.close().await.unwrap();
    }

    // ========================================================================
    // truncate_to() Tests
    // ========================================================================

    /// Test truncate_to() removes entries with lsn <= safe_lsn.
    #[tokio::test]
    async fn test_truncate_to() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Write 5 entries (LSN 1-5)
        for i in 1..=5 {
            let mut batch = ClogBatch::new();
            batch.add_put(
                i,
                format!("key{i}").into_bytes(),
                format!("val{i}").into_bytes(),
            );
            service.write(&mut batch, true).unwrap().await.unwrap();
        }

        // Verify we have 5 entries
        assert_eq!(service.read_all().unwrap().len(), 5);

        // Truncate entries with LSN <= 3
        let stats = service.truncate_to(3).await.unwrap();
        assert_eq!(stats.entries_removed, 3);
        assert_eq!(stats.entries_kept, 2);
        assert!(stats.bytes_freed > 0);

        // Verify only entries 4 and 5 remain
        let remaining = service.read_all().unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].lsn, 4);
        assert_eq!(remaining[1].lsn, 5);

        // Oldest LSN should now be 4
        assert_eq!(service.oldest_lsn().unwrap(), 4);

        service.close().await.unwrap();
    }

    /// Test truncate_to() with safe_lsn higher than all entries (removes all).
    #[tokio::test]
    async fn test_truncate_to_all() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Write 3 entries
        let mut batch = ClogBatch::new();
        batch.add_put(1, b"k1".to_vec(), b"v1".to_vec());
        batch.add_put(1, b"k2".to_vec(), b"v2".to_vec());
        batch.add_put(1, b"k3".to_vec(), b"v3".to_vec());
        service.write(&mut batch, true).unwrap().await.unwrap();

        // Truncate all (safe_lsn >= max LSN)
        let stats = service.truncate_to(10).await.unwrap();
        assert_eq!(stats.entries_removed, 3);
        assert_eq!(stats.entries_kept, 0);

        // Verify clog is empty (just header)
        let entries = service.read_all().unwrap();
        assert!(entries.is_empty());
        assert_eq!(service.oldest_lsn().unwrap(), 0);
        assert_eq!(service.file_size().unwrap(), FILE_HEADER_SIZE as u64);

        service.close().await.unwrap();
    }

    /// Test truncate_to() when nothing needs to be truncated.
    #[tokio::test]
    async fn test_truncate_to_none() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Write entries with LSN 1-3
        let mut batch = ClogBatch::new();
        batch.add_put(1, b"k1".to_vec(), b"v1".to_vec());
        batch.add_put(1, b"k2".to_vec(), b"v2".to_vec());
        batch.add_put(1, b"k3".to_vec(), b"v3".to_vec());
        service.write(&mut batch, true).unwrap().await.unwrap();

        let size_before = service.file_size().unwrap();

        // Truncate with safe_lsn = 0 (nothing to remove)
        let stats = service.truncate_to(0).await.unwrap();
        assert_eq!(stats.entries_removed, 0);
        assert_eq!(stats.entries_kept, 3);
        assert_eq!(stats.bytes_freed, 0);
        assert_eq!(stats.new_file_size, size_before);

        // All entries still present
        assert_eq!(service.read_all().unwrap().len(), 3);

        service.close().await.unwrap();
    }

    /// Test truncate_to() then continue writing.
    #[tokio::test]
    async fn test_truncate_then_write() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Write entries as separate transactions (mixed txn_id in one batch is
        // rejected by write()).
        for (txn_id, key, value) in [
            (1, b"k1".as_slice(), b"v1".as_slice()),
            (2, b"k2".as_slice(), b"v2".as_slice()),
            (3, b"k3".as_slice(), b"v3".as_slice()),
        ] {
            let mut batch = ClogBatch::new();
            batch.add_put(txn_id, key.to_vec(), value.to_vec());
            service.write(&mut batch, true).unwrap().await.unwrap();
        }

        // Truncate LSN 1-2 — txn 1 (max_lsn=1) and txn 2 (max_lsn=2) removed,
        // txn 3 (max_lsn=3 > 2) kept.
        service.truncate_to(2).await.unwrap();

        // Write more entries
        let mut batch2 = ClogBatch::new();
        batch2.add_put(4, b"k4".to_vec(), b"v4".to_vec());
        let lsn = service.write(&mut batch2, true).unwrap().await.unwrap();
        assert_eq!(lsn, 4); // LSN continues from 4

        // Verify entries
        let entries = service.read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].lsn, 3);
        assert_eq!(entries[1].lsn, 4);

        service.close().await.unwrap();
    }

    // ========================================================================
    // Transaction-boundary truncation tests
    // ========================================================================

    #[tokio::test]
    async fn test_write_ops_with_lsn_uses_preallocated_lsn() {
        use crate::lsn::new_lsn_provider;

        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let provider = new_lsn_provider();
        let txn_lsn = provider.alloc_lsn();
        assert_eq!(txn_lsn, 1);

        let service = FileClogService::open_with_lsn_provider(
            config,
            provider,
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        let ops = [ClogOpRef::Put {
            key: b"k1",
            value: b"v1",
        }];
        let written_lsn = service
            .write_ops_with_lsn(7, &ops, 100, txn_lsn, true)
            .unwrap()
            .await
            .unwrap();
        assert_eq!(written_lsn, txn_lsn);

        let entries = service.read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|entry| entry.lsn == txn_lsn));

        service.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_write_ops_with_lsn_rejects_unallocated_lsn() {
        use crate::lsn::new_lsn_provider;

        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let provider = new_lsn_provider();
        let service = FileClogService::open_with_lsn_provider(
            config,
            provider,
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        let ops = [ClogOpRef::Put {
            key: b"k1",
            value: b"v1",
        }];
        let err = match service.write_ops_with_lsn(7, &ops, 100, 1, true) {
            Ok(_) => panic!("write_ops_with_lsn should reject unallocated LSN"),
            Err(err) => format!("{err}"),
        };
        assert!(err.contains("pre-allocated lsn"), "unexpected error: {err}");
        assert!(service.read_all().unwrap().is_empty());

        service.close().await.unwrap();
    }

    /// Verify truncate_to respects transaction boundaries: when safe_lsn falls
    /// below a transaction LSN, the entire transaction is kept (not split).
    #[tokio::test]
    async fn test_truncate_preserves_transaction_atomicity() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Write a multi-entry transaction: all entries share one txn LSN.
        let ops = [
            ClogOpRef::Put {
                key: b"k1",
                value: b"v1",
            },
            ClogOpRef::Put {
                key: b"k2",
                value: b"v2",
            },
        ];
        let commit_lsn = service
            .write_ops(1, &ops, 100, true)
            .unwrap()
            .await
            .unwrap();
        assert_eq!(commit_lsn, 1);

        // Verify 3 entries written
        let entries = service.read_all().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].lsn, 1);
        assert_eq!(entries[1].lsn, 1);
        assert_eq!(entries[2].lsn, 1);

        // Truncate with safe_lsn=0: transaction is above boundary, so it must
        // be kept atomically.
        let stats = service.truncate_to(0).await.unwrap();
        assert_eq!(stats.entries_removed, 0, "entire txn should be kept");
        assert_eq!(stats.entries_kept, 3);

        // Verify all entries still present
        let remaining = service.read_all().unwrap();
        assert_eq!(remaining.len(), 3);
        assert_eq!(remaining[0].lsn, 1);
        assert_eq!(remaining[2].lsn, 1);

        service.close().await.unwrap();
    }

    /// Verify that a fully-below-safe_lsn transaction IS removed, while a
    /// partially-above transaction is kept intact.
    #[tokio::test]
    async fn test_truncate_mixed_transactions() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Transaction 1: Put + Commit at lsn=1.
        let ops1 = [ClogOpRef::Put {
            key: b"k1",
            value: b"v1",
        }];
        service
            .write_ops(1, &ops1, 100, true)
            .unwrap()
            .await
            .unwrap();

        // Transaction 2: all entries at lsn=2.
        let ops2 = [
            ClogOpRef::Put {
                key: b"k2",
                value: b"v2",
            },
            ClogOpRef::Put {
                key: b"k3",
                value: b"v3",
            },
            ClogOpRef::Put {
                key: b"k4",
                value: b"v4",
            },
        ];
        let commit_lsn = service
            .write_ops(2, &ops2, 200, true)
            .unwrap()
            .await
            .unwrap();
        assert_eq!(commit_lsn, 2);

        // Truncate at safe_lsn=1: T1 removed, T2 kept.
        let stats = service.truncate_to(1).await.unwrap();
        assert_eq!(stats.entries_removed, 2); // T1's Put + Commit
        assert_eq!(stats.entries_kept, 4); // T2's 3 Puts + Commit

        // Verify T2 is intact (all 4 entries)
        let remaining = service.read_all().unwrap();
        assert_eq!(remaining.len(), 4);
        assert_eq!(remaining[0].lsn, 2);
        assert_eq!(remaining[1].lsn, 2);
        assert_eq!(remaining[2].lsn, 2);
        assert_eq!(remaining[3].lsn, 2);
        // All entries belong to txn 2
        assert!(remaining.iter().all(|e| e.txn_id == 2));

        service.close().await.unwrap();
    }

    /// Verify that truncation at exactly the Commit LSN removes the entire txn.
    #[tokio::test]
    async fn test_truncate_at_commit_lsn_removes_entire_txn() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Transaction: all entries share one LSN.
        let ops = [
            ClogOpRef::Put {
                key: b"k1",
                value: b"v1",
            },
            ClogOpRef::Put {
                key: b"k2",
                value: b"v2",
            },
        ];
        service
            .write_ops(1, &ops, 100, true)
            .unwrap()
            .await
            .unwrap();

        // Truncate at safe_lsn=1 (equals txn LSN) → entire txn removed.
        let stats = service.truncate_to(1).await.unwrap();
        assert_eq!(stats.entries_removed, 3);
        assert_eq!(stats.entries_kept, 0);
        assert!(service.read_all().unwrap().is_empty());

        service.close().await.unwrap();
    }

    /// Verify no orphaned Commit entries after truncation with many transactions.
    #[tokio::test]
    async fn test_truncate_no_orphaned_commits() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Write 10 transactions, each with 2 Puts + 1 Commit (3 entries each).
        // Per-txn LSNs are T1=1, T2=2, ..., T10=10.
        for txn_id in 1..=10u64 {
            let ops = [
                ClogOpRef::Put {
                    key: b"ka",
                    value: b"va",
                },
                ClogOpRef::Put {
                    key: b"kb",
                    value: b"vb",
                },
            ];
            service
                .write_ops(txn_id, &ops, txn_id * 100, true)
                .unwrap()
                .await
                .unwrap();
        }

        // Verify 30 entries total
        assert_eq!(service.read_all().unwrap().len(), 30);

        // Truncate at safe_lsn=4: T1..T4 removed, T5..T10 kept.
        let stats = service.truncate_to(4).await.unwrap();
        assert_eq!(stats.entries_removed, 12); // T1-T4 (4 txns × 3 entries)
        assert_eq!(stats.entries_kept, 18); // T5-T10 (6 txns × 3 entries)

        // Verify no orphaned commits: every Commit entry must have preceding Puts
        let remaining = service.read_all().unwrap();
        let mut txn_has_writes: std::collections::HashMap<u64, bool> =
            std::collections::HashMap::new();
        for entry in &remaining {
            match &entry.op {
                ClogOp::Put { .. } | ClogOp::Delete { .. } => {
                    txn_has_writes.insert(entry.txn_id, true);
                }
                ClogOp::Commit { .. } => {
                    assert!(
                        txn_has_writes.get(&entry.txn_id).copied().unwrap_or(false),
                        "Orphaned Commit for txn_id={} at lsn={} — no preceding writes!",
                        entry.txn_id,
                        entry.lsn
                    );
                }
                _ => {}
            }
        }

        service.close().await.unwrap();
    }

    // ========================================================================
    // truncate_to() Drain Safety Tests
    // ========================================================================

    /// Test that truncate_to() drains pending group commit writes before
    /// reading the file. Without the drain barrier, writes enqueued on the
    /// writer queue but not yet processed by the writer loop could be
    /// lost when the file is rewritten.
    #[tokio::test]
    async fn test_truncate_to_drains_pending_writes() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Submit 10 writes without waiting for replies. Some may still be
        // queued when truncate_to runs.
        for i in 1..=10u64 {
            let mut batch = ClogBatch::new();
            batch.add_put(
                i,
                format!("key{i}").into_bytes(),
                format!("val{i}").into_bytes(),
            );
            let _future = service.write(&mut batch, true).unwrap();
            // Deliberately NOT awaiting _future
        }

        // Truncate immediately (keep all — safe_lsn=0)
        let stats = service.truncate_to(0).await.unwrap();

        // All 10 writes must survive: the drain barrier ensures pending
        // writes are flushed to disk before read_all().
        let entries = service.read_all().unwrap();
        assert_eq!(entries.len(), 10);
        assert_eq!(stats.entries_kept, 10);
        assert_eq!(stats.entries_removed, 0);

        service.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_truncate_to_read_error_sets_writer_failed() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        let mut batch = ClogBatch::new();
        batch.add_put(1, b"key".to_vec(), b"value".to_vec());
        service.write(&mut batch, true).unwrap().await.unwrap();

        std::fs::remove_file(config.clog_path()).unwrap();

        assert!(service.truncate_to(0).await.is_err());

        let err1 = service.group_writer.stop_and_drain().await.unwrap_err();
        let err2 = service.group_writer.stop_and_drain().await.unwrap_err();
        assert_eq!(err1, err2);
        assert!(!err1.contains("Already draining"));
    }

    // ========================================================================
    // append() Tests
    // ========================================================================

    /// Test append() default trait method.
    #[tokio::test]
    async fn test_append() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Use append (default implementation wraps write)
        let entry = ClogEntry {
            lsn: 0, // Will be assigned
            txn_id: 1,
            op: ClogOp::Put {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            },
        };
        let lsn = service.append(entry, true).unwrap().await.unwrap();
        assert_eq!(lsn, 1);

        // Append another
        let entry2 = ClogEntry {
            lsn: 0,
            txn_id: 1,
            op: ClogOp::Commit { commit_ts: 100 },
        };
        let lsn2 = service.append(entry2, true).unwrap().await.unwrap();
        assert_eq!(lsn2, 2);

        // Verify
        let entries = service.read_all().unwrap();
        assert_eq!(entries.len(), 2);

        service.close().await.unwrap();
    }

    // ========================================================================
    // ClogBatch Method Tests
    // ========================================================================

    /// Test ClogBatch methods: entries(), into_entries(), len(), is_empty().
    #[test]
    fn test_clog_batch_methods() {
        let mut batch = ClogBatch::new();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);

        batch.add_put(1, b"k1".to_vec(), b"v1".to_vec());
        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 1);

        batch.add_delete(1, b"k2".to_vec());
        batch.add_commit(1, 100);
        assert_eq!(batch.len(), 3);

        // Test entries() borrowing
        let entries = batch.entries();
        assert_eq!(entries.len(), 3);
        match &entries[0].op {
            ClogOp::Put { key, value } => {
                assert_eq!(key, b"k1");
                assert_eq!(value, b"v1");
            }
            _ => panic!("Expected Put"),
        }

        // Test into_entries() consuming
        let owned_entries = batch.into_entries();
        assert_eq!(owned_entries.len(), 3);
    }

    /// Test ClogBatch::add() directly with ClogEntry.
    #[test]
    fn test_clog_batch_add_direct() {
        let mut batch = ClogBatch::new();

        // Add entry directly
        let entry = ClogEntry {
            lsn: 0,
            txn_id: 42,
            op: ClogOp::Rollback,
        };
        batch.add(entry);

        assert_eq!(batch.len(), 1);
        match &batch.entries()[0].op {
            ClogOp::Rollback => {}
            _ => panic!("Expected Rollback"),
        }
    }

    // ========================================================================
    // FileClogConfig Tests
    // ========================================================================

    /// Test FileClogConfig::default().
    #[test]
    fn test_config_default() {
        let config = FileClogConfig::default();
        assert_eq!(config.clog_dir, PathBuf::from("data"));
        assert_eq!(config.clog_file, "tisql.clog");
        assert_eq!(config.clog_path(), PathBuf::from("data/tisql.clog"));
    }

    /// Test FileClogConfig::with_dir().
    #[test]
    fn test_config_with_dir() {
        let config = FileClogConfig::with_dir("/custom/path");
        assert_eq!(config.clog_dir, PathBuf::from("/custom/path"));
        assert_eq!(config.clog_file, "tisql.clog");
        assert_eq!(config.clog_path(), PathBuf::from("/custom/path/tisql.clog"));
    }

    // ========================================================================
    // Error Path Tests
    // ========================================================================

    /// Test recovery with invalid magic bytes.
    #[tokio::test]
    async fn test_invalid_magic() {
        let dir = tempdir().unwrap();
        let clog_path = dir.path().join("tisql.clog");

        // Write file with wrong magic
        {
            let mut file = std::fs::File::create(&clog_path).unwrap();
            file.write_all(b"XXXX").unwrap(); // Wrong magic
            file.write_all(&FILE_VERSION.to_le_bytes()).unwrap();
            file.write_all(&[0u8; 8]).unwrap();
            file.sync_all().unwrap();
        }

        let config = FileClogConfig::with_dir(dir.path());
        let result =
            FileClogService::open(config, make_test_io(), &tokio::runtime::Handle::current());

        match result {
            Err(e) => {
                let err = e.to_string();
                assert!(
                    err.contains("Invalid commit log file magic"),
                    "Unexpected error: {err}"
                );
            }
            Ok(_) => panic!("Expected error for invalid magic"),
        }
    }

    /// Test recovery with unsupported version.
    #[tokio::test]
    async fn test_unsupported_version() {
        let dir = tempdir().unwrap();
        let clog_path = dir.path().join("tisql.clog");

        // Write file with wrong version
        {
            let mut file = std::fs::File::create(&clog_path).unwrap();
            file.write_all(FILE_MAGIC).unwrap();
            file.write_all(&99u32.to_le_bytes()).unwrap(); // Wrong version
            file.write_all(&[0u8; 8]).unwrap();
            file.sync_all().unwrap();
        }

        let config = FileClogConfig::with_dir(dir.path());
        let result =
            FileClogService::open(config, make_test_io(), &tokio::runtime::Handle::current());

        match result {
            Err(e) => {
                let err = e.to_string();
                assert!(
                    err.contains("Unsupported commit log file version"),
                    "Unexpected error: {err}"
                );
            }
            Ok(_) => panic!("Expected error for unsupported version"),
        }
    }

    /// Test recovery with unknown record type.
    #[tokio::test]
    async fn test_unknown_record_type() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let clog_path = config.clog_path();

        // Create valid clog first
        {
            let service = FileClogService::open(
                config.clone(),
                make_test_io(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap();
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"key".to_vec(), b"value".to_vec());
            service.write(&mut batch, true).unwrap().await.unwrap();
            service.close().await.unwrap();
        }

        // Append a record with unknown type
        {
            let mut file = OpenOptions::new().append(true).open(&clog_path).unwrap();
            let record_type: u32 = 99; // Unknown type
            let length: u32 = 4;
            let data = [0u8; 4];
            let checksum = crc32(&data);

            file.write_all(&record_type.to_le_bytes()).unwrap();
            file.write_all(&length.to_le_bytes()).unwrap();
            file.write_all(&checksum.to_le_bytes()).unwrap();
            file.write_all(&data).unwrap();
            file.sync_all().unwrap();
        }

        // Recovery should stop at unknown record type
        let (service, entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();
        assert_eq!(entries.len(), 1); // Only the first valid entry
        assert_eq!(service.current_lsn(), 2);
    }

    /// Test recovery with record size exceeding MAX_RECORD_SIZE.
    #[tokio::test]
    async fn test_record_size_exceeds_max() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let clog_path = config.clog_path();

        // Create valid clog first
        {
            let service = FileClogService::open(
                config.clone(),
                make_test_io(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap();
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"key".to_vec(), b"value".to_vec());
            service.write(&mut batch, true).unwrap().await.unwrap();
            service.close().await.unwrap();
        }

        // Append a record with size > MAX_RECORD_SIZE
        {
            let mut file = OpenOptions::new().append(true).open(&clog_path).unwrap();
            let record_type: u32 = 1;
            let length: u32 = (MAX_RECORD_SIZE + 1) as u32; // Exceeds max
            let checksum: u32 = 0;

            file.write_all(&record_type.to_le_bytes()).unwrap();
            file.write_all(&length.to_le_bytes()).unwrap();
            file.write_all(&checksum.to_le_bytes()).unwrap();
            // Don't write actual data - just testing the check
            file.sync_all().unwrap();
        }

        // Recovery should stop at the oversized record
        let (service, entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(service.current_lsn(), 2);
    }

    // ========================================================================
    // Recovery from Non-Existent File
    // ========================================================================

    /// Test recover() when clog file doesn't exist.
    #[tokio::test]
    async fn test_recover_nonexistent_file() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        // Don't create any file - just recover
        let (service, entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();
        assert!(entries.is_empty());
        assert_eq!(service.current_lsn(), 1);
    }

    // ========================================================================
    // Directory Auto-Creation Tests
    // ========================================================================

    /// Test that open() creates directory if it doesn't exist.
    #[tokio::test]
    async fn test_auto_create_directory() {
        let dir = tempdir().unwrap();
        let nested_dir = dir.path().join("nested").join("path");
        let config = FileClogConfig::with_dir(&nested_dir);

        // Directory doesn't exist yet
        assert!(!nested_dir.exists());

        // Open should create it
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();
        assert!(nested_dir.exists());

        // Write and verify
        let mut batch = ClogBatch::new();
        batch.add_put(1, b"key".to_vec(), b"value".to_vec());
        service.write(&mut batch, true).unwrap().await.unwrap();
        service.close().await.unwrap();

        let (_, entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();
        assert_eq!(entries.len(), 1);
    }

    // ========================================================================
    // Rollback Operation Tests
    // ========================================================================

    /// Test Rollback operation serialization and recovery.
    #[tokio::test]
    async fn test_rollback_operation() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Write a put followed by rollback
        let mut batch = ClogBatch::new();
        batch.add_put(1, b"key".to_vec(), b"value".to_vec());
        batch.add(ClogEntry {
            lsn: 0,
            txn_id: 1,
            op: ClogOp::Rollback,
        });
        service.write(&mut batch, true).unwrap().await.unwrap();
        service.close().await.unwrap();

        // Recover and verify rollback
        let (_, entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();
        assert_eq!(entries.len(), 2);

        match &entries[1].op {
            ClogOp::Rollback => {}
            _ => panic!("Expected Rollback"),
        }
    }

    // ========================================================================
    // Mixed Operations in Single Batch
    // ========================================================================

    /// Test batch with multiple operation types.
    #[tokio::test]
    async fn test_mixed_operations_batch() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Create batch with all operation types
        let mut batch = ClogBatch::new();
        batch.add_put(1, b"key1".to_vec(), b"value1".to_vec());
        batch.add_put(1, b"key2".to_vec(), b"value2".to_vec());
        batch.add_delete(1, b"key_to_delete".to_vec());
        batch.add_put(1, b"key3".to_vec(), b"value3".to_vec());
        batch.add_commit(1, 1000);

        let lsn = service.write(&mut batch, true).unwrap().await.unwrap();
        assert_eq!(lsn, 1);

        service.close().await.unwrap();

        // Recover and verify order
        let (_, entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();
        assert_eq!(entries.len(), 5);

        // Verify order and types
        assert!(matches!(&entries[0].op, ClogOp::Put { key, .. } if key == b"key1"));
        assert!(matches!(&entries[1].op, ClogOp::Put { key, .. } if key == b"key2"));
        assert!(matches!(
            &entries[2].op,
            ClogOp::Delete { key } if key == b"key_to_delete"
        ));
        assert!(matches!(&entries[3].op, ClogOp::Put { key, .. } if key == b"key3"));
        assert!(matches!(
            &entries[4].op,
            ClogOp::Commit { commit_ts } if *commit_ts == 1000
        ));
    }

    // ========================================================================
    // Bincode Deserialization Error Path
    // ========================================================================

    /// Test recovery with corrupted data that passes CRC but fails deserialization.
    #[tokio::test]
    async fn test_corrupted_bincode_data() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let clog_path = config.clog_path();

        // Write valid data first
        {
            let service = FileClogService::open(
                config.clone(),
                make_test_io(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap();
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"key".to_vec(), b"value".to_vec());
            service.write(&mut batch, true).unwrap().await.unwrap();
            service.close().await.unwrap();
        }

        // Append data with valid CRC but invalid bincode format
        {
            let mut file = OpenOptions::new().append(true).open(&clog_path).unwrap();

            // Create some garbage data that won't deserialize as Vec<ClogEntry>
            let garbage_data = b"this is not valid bincode data for ClogEntry";
            let checksum = crc32(garbage_data);

            let record_type: u32 = 1;
            let length: u32 = garbage_data.len() as u32;

            file.write_all(&record_type.to_le_bytes()).unwrap();
            file.write_all(&length.to_le_bytes()).unwrap();
            file.write_all(&checksum.to_le_bytes()).unwrap();
            file.write_all(garbage_data).unwrap();
            file.sync_all().unwrap();
        }

        // Recovery should stop at the bad record
        let (service, entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();
        assert_eq!(entries.len(), 1); // Only the first valid entry
        assert_eq!(service.current_lsn(), 2);
    }

    // ========================================================================
    // TruncateStats Default Trait
    // ========================================================================

    /// Test TruncateStats Default implementation.
    #[test]
    fn test_truncate_stats_default() {
        let stats = TruncateStats::default();
        assert_eq!(stats.entries_removed, 0);
        assert_eq!(stats.entries_kept, 0);
        assert_eq!(stats.bytes_freed, 0);
        assert_eq!(stats.new_file_size, 0);
    }

    // ========================================================================
    // Debug and Clone Trait Coverage
    // ========================================================================

    /// Test Debug trait for ClogEntry, ClogOp, FileClogConfig.
    #[test]
    fn test_debug_traits() {
        // ClogEntry and ClogOp Debug
        let entry = ClogEntry {
            lsn: 1,
            txn_id: 42,
            op: ClogOp::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
        };
        let debug_str = format!("{entry:?}");
        assert!(debug_str.contains("ClogEntry"));
        assert!(debug_str.contains("42"));

        // ClogOp variants
        let op_put = ClogOp::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        let debug_put = format!("{op_put:?}");
        assert!(debug_put.contains("Put"));

        let op_delete = ClogOp::Delete { key: b"k".to_vec() };
        let debug_delete = format!("{op_delete:?}");
        assert!(debug_delete.contains("Delete"));

        let op_commit = ClogOp::Commit { commit_ts: 100 };
        let debug_commit = format!("{op_commit:?}");
        assert!(debug_commit.contains("Commit"));
        assert!(debug_commit.contains("100"));

        let op_rollback = ClogOp::Rollback;
        let debug_rollback = format!("{op_rollback:?}");
        assert!(debug_rollback.contains("Rollback"));

        // FileClogConfig Debug
        let config = FileClogConfig::default();
        let debug_config = format!("{config:?}");
        assert!(debug_config.contains("FileClogConfig"));
        assert!(debug_config.contains("data"));
    }

    /// Test Clone trait for ClogEntry and ClogOp.
    #[test]
    fn test_clone_traits() {
        let entry = ClogEntry {
            lsn: 1,
            txn_id: 42,
            op: ClogOp::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
        };

        let cloned = entry.clone();
        assert_eq!(cloned.lsn, entry.lsn);
        assert_eq!(cloned.txn_id, entry.txn_id);

        // Clone ClogOp variants
        let op_put = ClogOp::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        let cloned_put = op_put.clone();
        match cloned_put {
            ClogOp::Put { key, value } => {
                assert_eq!(key, b"k");
                assert_eq!(value, b"v");
            }
            _ => panic!("Expected Put"),
        }

        let op_rollback = ClogOp::Rollback;
        let cloned_rollback = op_rollback.clone();
        assert!(matches!(cloned_rollback, ClogOp::Rollback));

        // Clone config
        let config = FileClogConfig::default();
        let cloned_config = config.clone();
        assert_eq!(cloned_config.clog_dir, config.clog_dir);
        assert_eq!(cloned_config.clog_file, config.clog_file);
    }

    // ========================================================================
    // TruncateStats Debug Trait
    // ========================================================================

    /// Test TruncateStats Debug implementation.
    #[test]
    fn test_truncate_stats_debug() {
        let stats = TruncateStats {
            entries_removed: 5,
            entries_kept: 10,
            bytes_freed: 1024,
            new_file_size: 2048,
        };
        let debug_str = format!("{stats:?}");
        assert!(debug_str.contains("TruncateStats"));
        assert!(debug_str.contains("5"));
        assert!(debug_str.contains("10"));
        assert!(debug_str.contains("1024"));
        assert!(debug_str.contains("2048"));
    }

    // ========================================================================
    // Write Without Sync Tests
    // ========================================================================

    /// Test writes with sync=false still work and data is recoverable.
    #[tokio::test]
    async fn test_write_without_sync() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Write multiple batches without sync
        for i in 1..=5 {
            let mut batch = ClogBatch::new();
            batch.add_put(
                i,
                format!("key{i}").into_bytes(),
                format!("val{i}").into_bytes(),
            );
            service.write(&mut batch, false).unwrap().await.unwrap();
            // sync=false
        }

        // Close (which flushes and syncs)
        service.close().await.unwrap();

        // Verify data is recoverable
        let (_, entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();
        assert_eq!(entries.len(), 5);
    }

    // ========================================================================
    // Multiple Batches Recovery Test
    // ========================================================================

    /// Test recovery with many separate batches.
    #[tokio::test]
    async fn test_many_batches_recovery() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Write 20 separate batches
        for i in 1..=20 {
            let mut batch = ClogBatch::new();
            batch.add_put(
                i,
                format!("key{i}").into_bytes(),
                format!("val{i}").into_bytes(),
            );
            batch.add_commit(i, i * 100);
            service.write(&mut batch, true).unwrap().await.unwrap();
        }

        service.close().await.unwrap();

        // Recover and verify all batches
        let (service2, entries) = FileClogService::recover(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();
        assert_eq!(entries.len(), 40); // 20 puts + 20 commits
        assert_eq!(service2.current_lsn(), 21);

        // Verify first and last entries
        assert_eq!(entries[0].lsn, 1);
        assert_eq!(entries[39].lsn, 20);
    }

    // ========================================================================
    // Large Key/Value Test
    // ========================================================================

    /// Test with larger key and value sizes.
    #[tokio::test]
    async fn test_large_key_value() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = FileClogService::open(
            config.clone(),
            make_test_io(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Create large key and value (1KB each)
        let large_key: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
        let large_value: Vec<u8> = (0..1024).map(|i| ((i * 7) % 256) as u8).collect();

        let mut batch = ClogBatch::new();
        batch.add_put(1, large_key.clone(), large_value.clone());
        batch.add_commit(1, 100);
        service.write(&mut batch, true).unwrap().await.unwrap();
        service.close().await.unwrap();

        // Recover and verify
        let (_, entries) =
            FileClogService::recover(config, make_test_io(), &tokio::runtime::Handle::current())
                .unwrap();
        assert_eq!(entries.len(), 2);

        match &entries[0].op {
            ClogOp::Put { key, value } => {
                assert_eq!(key, &large_key);
                assert_eq!(value, &large_value);
            }
            _ => panic!("Expected Put"),
        }
    }

    // ========================================================================
    // ClogBatch Default Trait
    // ========================================================================

    /// Test ClogBatch Default implementation.
    #[test]
    fn test_clog_batch_default() {
        let batch: ClogBatch = Default::default();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
    }
}
