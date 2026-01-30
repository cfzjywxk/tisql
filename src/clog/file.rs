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
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Mutex;

#[cfg(feature = "failpoints")]
use fail::fail_point;

use crate::error::{Result, TiSqlError};
use crate::lsn::{AtomicLsnProvider, LsnProvider, SharedLsnProvider};
use crate::storage::{WriteBatch, WriteOp};
use crate::types::{Lsn, Timestamp, TxnId};
use crate::util::fs::{rename_durable, sync_dir};
use crate::{log_info, log_trace, log_warn};

use super::{ClogBatch, ClogEntry, ClogEntryRef, ClogOpRef, ClogService};

/// File header magic bytes: "CLOG"
const FILE_MAGIC: &[u8; 4] = b"CLOG";

/// File format version
const FILE_VERSION: u32 = 1;

/// File header size in bytes (magic + version + reserved)
#[allow(dead_code)]
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

    #[allow(dead_code)]
    fn set_lsn(&self, lsn: Lsn) {
        match self {
            LsnProviderKind::Shared(p) => p.set_lsn(lsn),
            LsnProviderKind::Local(p) => p.set_lsn(lsn),
        }
    }
}

/// File-based commit log implementation
pub struct FileClogService {
    config: FileClogConfig,
    /// LSN provider (shared or local)
    lsn_provider: LsnProviderKind,
    /// Commit log file writer (protected by mutex for thread safety)
    writer: Mutex<BufWriter<File>>,
}

impl FileClogService {
    /// Open or create a new commit log file (standalone mode with local LSN counter)
    pub fn open(config: FileClogConfig) -> Result<Self> {
        Self::open_internal(config, None)
    }

    /// Open or create a new commit log file with a shared LSN provider.
    ///
    /// This is used when clog and ilog share a unified LSN space for
    /// consistent recovery ordering.
    pub fn open_with_lsn_provider(
        config: FileClogConfig,
        lsn_provider: SharedLsnProvider,
    ) -> Result<Self> {
        Self::open_internal(config, Some(lsn_provider))
    }

    /// Internal open implementation
    fn open_internal(
        config: FileClogConfig,
        shared_provider: Option<SharedLsnProvider>,
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

        if file_exists && file.metadata()?.len() > 0 {
            // Validate existing file header
            let mut reader = BufReader::new(&file);
            Self::validate_header(&mut reader)?;

            // Find max LSN from existing entries
            let entries = Self::read_entries(&mut reader)?;
            if let Some(last) = entries.last() {
                max_lsn = last.lsn;
            }

            // Seek to end for appending
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
        let file = OpenOptions::new().read(true).write(true).open(&clog_path)?;

        let mut file_for_write = file;
        file_for_write.seek(SeekFrom::End(0))?;

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

        Ok(Self {
            config,
            lsn_provider,
            writer: Mutex::new(BufWriter::new(file_for_write)),
        })
    }

    /// Recover entries from commit log file (standalone mode)
    pub fn recover(config: FileClogConfig) -> Result<(Self, Vec<ClogEntry>)> {
        Self::recover_internal(config, None)
    }

    /// Recover entries with a shared LSN provider.
    ///
    /// The shared provider's current LSN will be updated to be at least
    /// max(clog LSN) + 1 after recovery.
    pub fn recover_with_lsn_provider(
        config: FileClogConfig,
        lsn_provider: SharedLsnProvider,
    ) -> Result<(Self, Vec<ClogEntry>)> {
        Self::recover_internal(config, Some(lsn_provider))
    }

    /// Internal recover implementation
    fn recover_internal(
        config: FileClogConfig,
        shared_provider: Option<SharedLsnProvider>,
    ) -> Result<(Self, Vec<ClogEntry>)> {
        let clog_path = config.clog_path();

        if !clog_path.exists() {
            // No commit log file, start fresh
            let service = Self::open_internal(config, shared_provider)?;
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
        let service = Self::open_internal(config, shared_provider)?;

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
        let mut entries = Vec::new();

        loop {
            match Self::read_record(reader) {
                Ok(Some(batch_entries)) => {
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

        Ok(entries)
    }

    /// Read a single record from the commit log
    fn read_record<R: Read>(reader: &mut R) -> Result<Option<Vec<ClogEntry>>> {
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

        Ok(Some(entries))
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

    /// Write a record from reference-based entries (zero-copy serialization).
    ///
    /// The on-disk format is identical to `write_record`, but this method
    /// serializes directly from borrowed data without requiring owned copies.
    fn write_record_refs<W: Write>(writer: &mut W, entries: &[ClogEntryRef<'_>]) -> Result<()> {
        // Serialize entries (bincode works with references via Serialize trait)
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

impl ClogService for FileClogService {
    fn write(&self, batch: &mut ClogBatch, sync: bool) -> Result<Lsn> {
        if batch.is_empty() {
            return Ok(self.lsn_provider.current_lsn());
        }

        // Acquire lock BEFORE assigning LSNs to ensure writes are ordered
        // This prevents concurrent writers from producing out-of-order LSNs in the file
        let mut writer = self.writer.lock().unwrap();

        // Assign LSNs inside the lock using the provider
        // Note: We allocate one LSN per entry from the shared provider
        let mut start_lsn = 0;
        for (i, entry) in batch.entries.iter_mut().enumerate() {
            let lsn = self.lsn_provider.alloc_lsn();
            if i == 0 {
                start_lsn = lsn;
            }
            entry.lsn = lsn;
        }
        let end_lsn = batch.entries.last().unwrap().lsn;

        // Write to file
        Self::write_record(&mut *writer, batch.entries())?;
        writer.flush()?;

        if sync {
            // Failpoint: crash before clog fsync
            #[cfg(feature = "failpoints")]
            fail_point!("clog_before_sync");

            writer.get_ref().sync_data()?;

            // Failpoint: crash after clog fsync
            #[cfg(feature = "failpoints")]
            fail_point!("clog_after_sync");
        }

        log_trace!(
            "Wrote {} entries to commit log, lsn={}-{}",
            batch.len(),
            start_lsn,
            end_lsn
        );

        Ok(end_lsn)
    }

    fn write_batch(
        &self,
        txn_id: TxnId,
        batch: &WriteBatch,
        commit_ts: Timestamp,
        sync: bool,
    ) -> Result<Lsn> {
        if batch.is_empty() {
            return Ok(self.lsn_provider.current_lsn());
        }

        // Count entries: one per op + one for commit record
        let entry_count = batch.len() + 1;

        // Acquire lock BEFORE assigning LSNs to ensure writes are ordered
        let mut writer = self.writer.lock().unwrap();

        // Allocate LSNs for all entries
        let mut lsns = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            lsns.push(self.lsn_provider.alloc_lsn());
        }
        let start_lsn = lsns[0];
        let end_lsn = *lsns.last().unwrap();

        // Build reference-based entries directly from WriteBatch
        // No cloning - we serialize directly from the borrowed data
        let mut entries: Vec<ClogEntryRef<'_>> = Vec::with_capacity(entry_count);
        let mut lsn_idx = 0;

        for (key, op) in batch.iter() {
            let entry = match op {
                WriteOp::Put { value } => ClogEntryRef {
                    lsn: lsns[lsn_idx],
                    txn_id,
                    op: ClogOpRef::Put { key, value },
                },
                WriteOp::Delete => ClogEntryRef {
                    lsn: lsns[lsn_idx],
                    txn_id,
                    op: ClogOpRef::Delete { key },
                },
            };
            entries.push(entry);
            lsn_idx += 1;
        }

        // Add commit record
        entries.push(ClogEntryRef {
            lsn: lsns[lsn_idx],
            txn_id,
            op: ClogOpRef::Commit { commit_ts },
        });

        // Write to file using reference-based serialization
        Self::write_record_refs(&mut *writer, &entries)?;
        writer.flush()?;

        if sync {
            #[cfg(feature = "failpoints")]
            fail_point!("clog_before_sync");

            writer.get_ref().sync_data()?;

            #[cfg(feature = "failpoints")]
            fail_point!("clog_after_sync");
        }

        log_trace!(
            "Wrote {} entries to commit log (zero-copy), lsn={}-{}",
            entry_count,
            start_lsn,
            end_lsn
        );

        Ok(end_lsn)
    }

    fn sync(&self) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        // Flush buffered data before fsync to ensure all writes are persisted
        writer.flush()?;
        writer.get_ref().sync_data()?;
        Ok(())
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

    fn close(&self) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.flush()?;
        writer.get_ref().sync_data()?;
        Ok(())
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
    /// # Safety
    ///
    /// Only call this when you're certain all entries with lsn <= safe_lsn
    /// have been durably persisted elsewhere (e.g., in SST files).
    pub fn truncate_to(&self, safe_lsn: Lsn) -> Result<TruncateStats> {
        let clog_path = self.config.clog_path();
        let old_size = std::fs::metadata(&clog_path).map(|m| m.len()).unwrap_or(0);

        // Read all entries
        let entries = self.read_all()?;

        // Partition entries
        let (to_keep, to_remove): (Vec<_>, Vec<_>) =
            entries.into_iter().partition(|e| e.lsn > safe_lsn);

        if to_remove.is_empty() {
            // Nothing to truncate
            return Ok(TruncateStats {
                entries_removed: 0,
                entries_kept: to_keep.len(),
                bytes_freed: 0,
                new_file_size: old_size,
            });
        }

        // Write kept entries to a temporary file
        let temp_path = clog_path.with_extension("clog.tmp");
        {
            let temp_file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temp_path)?;
            let mut temp_writer = BufWriter::new(temp_file);

            // Write header
            Self::write_header(&mut temp_writer)?;

            // Write kept entries
            if !to_keep.is_empty() {
                Self::write_record(&mut temp_writer, &to_keep)?;
            }

            temp_writer.flush()?;
            temp_writer.get_ref().sync_data()?;
        }

        // Atomically replace old file with new file, with directory fsync
        // to ensure the rename is durable across crashes
        rename_durable(&temp_path, &clog_path)?;

        // Reopen the writer
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&clog_path)?;

            // Seek to end for appending
            let mut file_for_write = file;
            file_for_write.seek(SeekFrom::End(0))?;

            let mut writer = self.writer.lock().unwrap();
            *writer = BufWriter::new(file_for_write);
        }

        let new_size = std::fs::metadata(&clog_path).map(|m| m.len()).unwrap_or(0);

        log_info!(
            "Truncated clog to safe_lsn={}: removed {} entries, kept {}",
            safe_lsn,
            to_remove.len(),
            to_keep.len()
        );

        Ok(TruncateStats {
            entries_removed: to_remove.len(),
            entries_kept: to_keep.len(),
            bytes_freed: old_size.saturating_sub(new_size),
            new_file_size: new_size,
        })
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
    use crate::clog::ClogOp;
    use tempfile::tempdir;

    #[test]
    fn test_open_and_write() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        let service = FileClogService::open(config.clone()).unwrap();

        let mut batch = ClogBatch::new();
        batch.add_put(1, b"key1".to_vec(), b"value1".to_vec());
        batch.add_put(1, b"key2".to_vec(), b"value2".to_vec());

        let lsn = service.write(&mut batch, true).unwrap();
        assert_eq!(lsn, 2);

        service.close().unwrap();

        // Reopen and verify
        let service2 = FileClogService::open(config).unwrap();
        assert_eq!(service2.current_lsn(), 3);

        let entries = service2.read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].lsn, 1);
        assert_eq!(entries[1].lsn, 2);
    }

    #[test]
    fn test_recover() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        // Write some entries
        {
            let service = FileClogService::open(config.clone()).unwrap();
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"k1".to_vec(), b"v1".to_vec());
            batch.add_delete(1, b"k2".to_vec());
            batch.add_commit(1, 100);
            service.write(&mut batch, true).unwrap();
            service.close().unwrap();
        }

        // Recover
        let (service, entries) = FileClogService::recover(config).unwrap();
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

        assert_eq!(service.current_lsn(), 4);
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
    #[test]
    fn test_recover_truncated_header() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let clog_path = config.clog_path();

        // Write one valid record
        {
            let service = FileClogService::open(config.clone()).unwrap();
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"valid_key".to_vec(), b"valid_value".to_vec());
            batch.add_commit(1, 100);
            service.write(&mut batch, true).unwrap();
            service.close().unwrap();
        }

        // Append partial header (6 bytes of a 12-byte header) to simulate crash
        {
            let mut file = OpenOptions::new().append(true).open(&clog_path).unwrap();
            // Partial record header: only 6 of 12 bytes
            file.write_all(&[1, 0, 0, 0, 50, 0]).unwrap();
            file.sync_all().unwrap();
        }

        // Recovery should skip the truncated header and recover valid entries
        let (service, entries) = FileClogService::recover(config).unwrap();

        // Should recover the 2 valid entries (Put + Commit)
        assert_eq!(
            entries.len(),
            2,
            "Should recover valid entries before truncation"
        );
        assert_eq!(
            service.current_lsn(),
            3,
            "LSN should continue from valid entries"
        );
    }

    /// Test recovery with truncated record data (header complete, data partial).
    /// Simulates crash during the data write after header was written.
    #[test]
    fn test_recover_truncated_data() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let clog_path = config.clog_path();

        // Write one valid record
        {
            let service = FileClogService::open(config.clone()).unwrap();
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"key1".to_vec(), b"value1".to_vec());
            batch.add_commit(1, 100);
            service.write(&mut batch, true).unwrap();
            service.close().unwrap();
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
        let (service, entries) = FileClogService::recover(config).unwrap();

        assert_eq!(entries.len(), 2, "Should recover entries before truncation");
        assert_eq!(service.current_lsn(), 3);
    }

    /// Test recovery with corrupted checksum.
    /// Simulates data corruption (bit flip) after write.
    #[test]
    fn test_recover_corrupted_checksum() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let clog_path = config.clog_path();

        // Write two valid records
        {
            let service = FileClogService::open(config.clone()).unwrap();

            // First batch
            let mut batch1 = ClogBatch::new();
            batch1.add_put(1, b"key1".to_vec(), b"value1".to_vec());
            batch1.add_commit(1, 100);
            service.write(&mut batch1, true).unwrap();

            // Second batch
            let mut batch2 = ClogBatch::new();
            batch2.add_put(2, b"key2".to_vec(), b"value2".to_vec());
            batch2.add_commit(2, 200);
            service.write(&mut batch2, true).unwrap();

            service.close().unwrap();
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
        let (service, entries) = FileClogService::recover(config).unwrap();

        // Should only recover the first batch (2 entries)
        assert_eq!(entries.len(), 2, "Should stop at corrupted record");
        assert_eq!(service.current_lsn(), 3);

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
    #[test]
    fn test_recover_multiple_batches_before_crash() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let clog_path = config.clog_path();

        // Write multiple batches
        {
            let service = FileClogService::open(config.clone()).unwrap();

            for i in 1..=5 {
                let mut batch = ClogBatch::new();
                batch.add_put(
                    i,
                    format!("key{i}").into_bytes(),
                    format!("value{i}").into_bytes(),
                );
                batch.add_commit(i, i * 100);
                service.write(&mut batch, true).unwrap();
            }
            service.close().unwrap();
        }

        // Append garbage to simulate partial write during crash
        {
            let mut file = OpenOptions::new().append(true).open(&clog_path).unwrap();
            file.write_all(b"GARBAGE_DATA_FROM_CRASH").unwrap();
            file.sync_all().unwrap();
        }

        // Recovery should get all 5 batches (10 entries)
        let (service, entries) = FileClogService::recover(config).unwrap();

        assert_eq!(entries.len(), 10, "Should recover all valid entries");
        assert_eq!(service.current_lsn(), 11);

        // Verify all 5 puts are there
        let puts: Vec<_> = entries
            .iter()
            .filter(|e| matches!(e.op, ClogOp::Put { .. }))
            .collect();
        assert_eq!(puts.len(), 5);
    }

    /// Test that recovery can continue writing after crash.
    /// Simulates: write -> crash -> recover -> write more -> verify all data.
    #[test]
    fn test_recover_and_continue_writing() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        // First session: write some data
        {
            let service = FileClogService::open(config.clone()).unwrap();
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"before_crash".to_vec(), b"v1".to_vec());
            batch.add_commit(1, 100);
            service.write(&mut batch, true).unwrap();
            // Simulate crash - no close() call
        }

        // Second session: recover and write more
        let (service, entries) = FileClogService::recover(config.clone()).unwrap();
        assert_eq!(entries.len(), 2);

        // Write more data after recovery
        let mut batch = ClogBatch::new();
        batch.add_put(2, b"after_crash".to_vec(), b"v2".to_vec());
        batch.add_commit(2, 200);
        service.write(&mut batch, true).unwrap();
        service.close().unwrap();

        // Third session: verify all data
        let (_, all_entries) = FileClogService::recover(config).unwrap();
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

    /// Test recovery with empty file (only header, no records).
    /// Simulates crash immediately after file creation.
    #[test]
    fn test_recover_empty_clog() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        // Create file but don't write any records
        {
            let service = FileClogService::open(config.clone()).unwrap();
            // Simulate crash before any writes
            service.close().unwrap();
        }

        // Recovery should succeed with empty entries
        let (service, entries) = FileClogService::recover(config).unwrap();
        assert_eq!(entries.len(), 0);
        assert_eq!(service.current_lsn(), 1);
    }

    /// Test concurrent clog writes produce monotonic LSNs in file order.
    /// This verifies the fix for the LSN out-of-order issue where concurrent
    /// writers could produce entries with non-monotonic LSNs in the file.
    #[test]
    fn test_concurrent_writes_lsn_ordering() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let service = Arc::new(FileClogService::open(config.clone()).unwrap());

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
                    service.write(&mut batch, false).unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        service.close().unwrap();

        // Recover and verify LSN ordering
        let (recovered_service, entries) = FileClogService::recover(config).unwrap();

        // Verify all entries were written
        let expected_count = num_threads * writes_per_thread;
        assert_eq!(
            entries.len(),
            expected_count,
            "Expected {} entries, got {}",
            expected_count,
            entries.len()
        );

        // Verify LSNs are monotonically increasing in file order
        for i in 1..entries.len() {
            assert!(
                entries[i].lsn > entries[i - 1].lsn,
                "LSNs not monotonic at index {}: {} <= {}",
                i,
                entries[i].lsn,
                entries[i - 1].lsn
            );
        }

        // Verify current_lsn is max(lsn) + 1
        let max_lsn = entries.iter().map(|e| e.lsn).max().unwrap();
        assert_eq!(
            recovered_service.current_lsn(),
            max_lsn + 1,
            "current_lsn should be max(lsn) + 1"
        );
    }

    // ========================================================================
    // Shared LSN Provider Tests
    // ========================================================================

    /// Test clog with shared LSN provider.
    /// Verifies that clog uses the shared provider for LSN allocation.
    #[test]
    fn test_shared_lsn_provider() {
        use crate::lsn::new_lsn_provider;

        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let provider = new_lsn_provider();

        // Allocate some LSNs before opening clog
        assert_eq!(provider.alloc_lsn(), 1);
        assert_eq!(provider.alloc_lsn(), 2);

        // Open clog with shared provider
        let service =
            FileClogService::open_with_lsn_provider(config.clone(), provider.clone()).unwrap();

        // Current LSN should be from provider
        assert_eq!(service.current_lsn(), 3);

        // Write to clog - should allocate LSN 3
        let mut batch = ClogBatch::new();
        batch.add_put(1, b"key1".to_vec(), b"value1".to_vec());
        let lsn = service.write(&mut batch, true).unwrap();
        assert_eq!(lsn, 3);

        // Provider should have advanced
        assert_eq!(provider.current_lsn(), 4);

        // Write more
        let mut batch2 = ClogBatch::new();
        batch2.add_put(2, b"key2".to_vec(), b"value2".to_vec());
        let lsn2 = service.write(&mut batch2, true).unwrap();
        assert_eq!(lsn2, 4);

        service.close().unwrap();

        // Recover with same provider
        let (recovered, entries) =
            FileClogService::recover_with_lsn_provider(config, provider.clone()).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].lsn, 3);
        assert_eq!(entries[1].lsn, 4);
        assert_eq!(recovered.current_lsn(), 5);
        assert_eq!(provider.current_lsn(), 5);
    }

    /// Test that shared provider is updated when clog has higher LSN.
    #[test]
    fn test_shared_provider_updated_from_clog() {
        use crate::lsn::new_lsn_provider;

        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        // First: write to clog with standalone mode
        {
            let service = FileClogService::open(config.clone()).unwrap();
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"k1".to_vec(), b"v1".to_vec());
            batch.add_put(1, b"k2".to_vec(), b"v2".to_vec());
            batch.add_put(1, b"k3".to_vec(), b"v3".to_vec());
            service.write(&mut batch, true).unwrap();
            // LSNs 1, 2, 3 are used
            service.close().unwrap();
        }

        // Create fresh provider starting at 1
        let provider = new_lsn_provider();
        assert_eq!(provider.current_lsn(), 1);

        // Recover with shared provider - it should be updated to 4
        let (recovered, entries) =
            FileClogService::recover_with_lsn_provider(config, provider.clone()).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(provider.current_lsn(), 4); // max(3) + 1
        assert_eq!(recovered.current_lsn(), 4);
    }

    /// Test read_from_lsn for partial replay during recovery.
    #[test]
    fn test_read_from_lsn() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        let service = FileClogService::open(config.clone()).unwrap();

        // Write 5 entries with LSN 1-5
        for i in 1..=5 {
            let mut batch = ClogBatch::new();
            batch.add_put(
                i,
                format!("key{i}").into_bytes(),
                format!("val{i}").into_bytes(),
            );
            service.write(&mut batch, true).unwrap();
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

        service.close().unwrap();
    }

    /// Test max_lsn helper.
    #[test]
    fn test_max_lsn() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());

        let service = FileClogService::open(config.clone()).unwrap();

        // Empty clog
        assert_eq!(service.max_lsn().unwrap(), 0);

        // Write some entries
        let mut batch = ClogBatch::new();
        batch.add_put(1, b"k1".to_vec(), b"v1".to_vec());
        batch.add_put(1, b"k2".to_vec(), b"v2".to_vec());
        service.write(&mut batch, true).unwrap();

        assert_eq!(service.max_lsn().unwrap(), 2);

        service.close().unwrap();
    }
}
