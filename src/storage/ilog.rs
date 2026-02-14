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

//! Index Log (ilog) service for SST metadata persistence.
//!
//! The ilog stores metadata about SST file operations (flush and compaction)
//! using an intent/commit protocol for crash safety. This is inspired by
//! OceanBase's ilog and RocksDB's MANIFEST.
//!
//! ## Record Types
//!
//! - `FlushIntent`: Written before creating an SST from memtable
//! - `FlushCommit`: Written after SST is successfully created
//! - `CompactIntent`: Written before starting compaction
//! - `CompactCommit`: Written after compaction SSTs are created
//! - `Checkpoint`: Periodic full snapshot of Version state
//!
//! ## Recovery Protocol
//!
//! On startup:
//! 1. Replay ilog from last checkpoint to rebuild Version
//! 2. Handle incomplete intents:
//!    - Incomplete FlushIntent: ignore (memtable data still in clog)
//!    - Incomplete CompactIntent: cleanup orphan SST files
//! 3. Continue with clog replay from flushed_lsn
//!
//! ## File Format
//!
//! Reuses the clog engine implementation but stores files in `data/ilog/`.
//! Each record has: type (4 bytes) + length (4 bytes) + checksum (4 bytes) + data.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

#[cfg(feature = "failpoints")]
use fail::fail_point;
use serde::{Deserialize, Serialize};

use crate::error::{Result, TiSqlError};
use crate::lsn::SharedLsnProvider;
use crate::types::Lsn;
use crate::util::fs::{create_dir_durable, rename_durable, sync_dir};
use crate::{log_info, log_trace, log_warn};

use super::sstable::SstMeta;
use super::version::Version;

/// File header magic bytes: "ILOG"
const FILE_MAGIC: &[u8; 4] = b"ILOG";

/// File format version
const FILE_VERSION: u32 = 1;

/// File header size in bytes (magic + version + reserved)
const FILE_HEADER_SIZE: usize = 16;

/// Record header size in bytes (type + length + checksum)
const RECORD_HEADER_SIZE: usize = 12;

/// Maximum record size (256 MB) to prevent OOM from corrupted length fields
const MAX_RECORD_SIZE: usize = 256 * 1024 * 1024;

/// Index log record types.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum IlogRecord {
    /// Intent to flush a memtable to SST.
    ///
    /// Written before starting the flush operation.
    /// Contains expected SST metadata.
    FlushIntent {
        /// LSN of this record
        lsn: Lsn,
        /// Expected SST ID
        sst_id: u64,
        /// Memtable ID being flushed
        memtable_id: u64,
    },

    /// Commit of a successful flush.
    ///
    /// Written after SST file is created and fsync'd.
    FlushCommit {
        /// LSN of this record
        lsn: Lsn,
        /// The created SST metadata
        sst_meta: SstMeta,
        /// Flushed LSN (up to which clog data is now in SST)
        flushed_lsn: Lsn,
    },

    /// Intent to start compaction.
    ///
    /// Written before starting compaction.
    /// Lists input SSTs and expected output SST IDs.
    CompactIntent {
        /// LSN of this record
        lsn: Lsn,
        /// Input SST IDs (will be deleted after commit)
        input_sst_ids: Vec<u64>,
        /// Expected output SST IDs (will be created)
        output_sst_ids: Vec<u64>,
        /// Target level for output
        target_level: u32,
    },

    /// Commit of a successful compaction.
    ///
    /// Written after all output SSTs are created and fsync'd.
    CompactCommit {
        /// LSN of this record
        lsn: Lsn,
        /// Input SSTs that were removed
        deleted_ssts: Vec<(u32, u64)>, // (level, sst_id)
        /// Output SSTs that were created
        new_ssts: Vec<SstMeta>,
    },

    /// Full Version checkpoint.
    ///
    /// Periodically written to enable faster recovery.
    /// Contains complete SST metadata for all levels.
    Checkpoint {
        /// LSN of this record
        lsn: Lsn,
        /// Full Version state
        version: VersionSnapshot,
        /// Checkpoint sequence number (for log truncation)
        checkpoint_seq: u64,
    },
}

impl IlogRecord {
    /// Get the LSN of this record.
    pub fn lsn(&self) -> Lsn {
        match self {
            IlogRecord::FlushIntent { lsn, .. } => *lsn,
            IlogRecord::FlushCommit { lsn, .. } => *lsn,
            IlogRecord::CompactIntent { lsn, .. } => *lsn,
            IlogRecord::CompactCommit { lsn, .. } => *lsn,
            IlogRecord::Checkpoint { lsn, .. } => *lsn,
        }
    }
}

/// Serializable Version snapshot for checkpoints.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct VersionSnapshot {
    /// SST files at each level (level 0 to MAX_LEVELS-1)
    pub levels: Vec<Vec<SstMeta>>,
    /// Next SST ID to allocate
    pub next_sst_id: u64,
    /// Version number
    pub version_num: u64,
    /// Highest LSN that has been flushed to SST
    pub flushed_lsn: Lsn,
}

impl From<&Version> for VersionSnapshot {
    fn from(v: &Version) -> Self {
        let mut levels = Vec::new();
        for level in 0..super::version::MAX_LEVELS {
            let ssts: Vec<SstMeta> = v
                .ssts_at_level(level as u32)
                .iter()
                .map(|m| (**m).clone())
                .collect();
            levels.push(ssts);
        }
        Self {
            levels,
            next_sst_id: v.next_sst_id(),
            version_num: v.version_num(),
            flushed_lsn: v.flushed_lsn(),
        }
    }
}

/// Configuration for ilog service.
#[derive(Clone, Debug)]
pub struct IlogConfig {
    /// Directory for ilog files
    pub ilog_dir: PathBuf,
    /// Ilog file name
    pub ilog_file: String,
    /// SST directory (for orphan cleanup)
    pub sst_dir: PathBuf,
    /// Checkpoint interval (number of records between checkpoints)
    pub checkpoint_interval: usize,
}

impl IlogConfig {
    /// Create config with data directory.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        Self {
            ilog_dir: data_dir.join("ilog"),
            ilog_file: "tisql.ilog".to_string(),
            sst_dir: data_dir.join("sst"),
            checkpoint_interval: 100,
        }
    }

    /// Get the full path to the ilog file.
    pub fn ilog_path(&self) -> PathBuf {
        self.ilog_dir.join(&self.ilog_file)
    }
}

/// Pending operation tracker for intent/commit matching.
#[derive(Default)]
struct PendingOps {
    /// Pending flush intents: sst_id
    flush_intents: HashSet<u64>,
    /// Pending compact intents: output_sst_ids
    compact_intents: Vec<Vec<u64>>,
}

/// Group writer runtime mode for recreating writer after ilog rewrite.
enum GroupWriterMode {
    Tokio(tokio::runtime::Handle),
    Thread,
}

/// Index log service for SST metadata persistence.
pub struct IlogService {
    config: IlogConfig,
    /// Shared LSN provider
    lsn_provider: SharedLsnProvider,
    /// Group commit writer for batched fsync.
    ///
    /// Wrapped in Mutex so the writer can be replaced if needed.
    /// The mutex is only held briefly to call `submit()` (channel send).
    group_writer: Mutex<crate::clog::GroupCommitWriter>,
    /// Group writer runtime mode (tokio handle or std::thread fallback).
    writer_mode: GroupWriterMode,
    /// Number of records since last checkpoint
    records_since_checkpoint: AtomicU64,
    /// Last checkpoint sequence
    last_checkpoint_seq: AtomicU64,
}

/// Statistics returned from ilog truncation.
#[derive(Debug, Default)]
pub struct IlogTruncateStats {
    /// Number of records removed
    pub records_removed: usize,
    /// Number of records kept
    pub records_kept: usize,
    /// Bytes freed
    pub bytes_freed: u64,
    /// New file size
    pub new_file_size: u64,
}

impl IlogService {
    /// Open or create an ilog service.
    pub fn open(
        config: IlogConfig,
        lsn_provider: SharedLsnProvider,
        io_handle: &tokio::runtime::Handle,
    ) -> Result<Self> {
        // Ensure directory exists with durable creation
        create_dir_durable(&config.ilog_dir)?;

        let ilog_path = config.ilog_path();
        let file_exists = ilog_path.exists();

        // Open file for read+write+create
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&ilog_path)?;

        let mut valid_data_end: Option<u64> = None;

        if file_exists && file.metadata()?.len() > 0 {
            // Validate existing file header and read records to find valid length
            let mut reader = BufReader::new(&file);
            Self::validate_header(&mut reader)?;

            // Read records and track valid data length
            let (_, valid_bytes) = Self::read_records_with_valid_length(&mut reader)?;

            // Calculate the total valid length: header + valid record bytes
            valid_data_end = Some(FILE_HEADER_SIZE as u64 + valid_bytes);

            drop(reader);
        } else {
            // Write header to new file
            let mut writer = BufWriter::new(&file);
            Self::write_header(&mut writer)?;
            writer.flush()?;
            // Sync file and directory to make creation durable
            file.sync_all()?;
            sync_dir(&config.ilog_dir)?;
        }

        // Re-open for appending
        let mut file_for_write = OpenOptions::new().read(true).write(true).open(&ilog_path)?;

        // If we detected corruption (file is larger than valid data), truncate it
        if let Some(valid_end) = valid_data_end {
            let actual_len = file_for_write.metadata()?.len();
            if actual_len > valid_end {
                log_warn!(
                    "Truncating corrupted ilog tail: {} bytes -> {} bytes",
                    actual_len,
                    valid_end
                );
                file_for_write.set_len(valid_end)?;
                file_for_write.sync_all()?;
            }
        }

        file_for_write.seek(SeekFrom::End(0))?;

        log_info!("Opened ilog file: {:?}", ilog_path);

        let group_writer =
            crate::clog::GroupCommitWriter::new(BufWriter::new(file_for_write), io_handle);

        Ok(Self {
            config,
            lsn_provider,
            group_writer: Mutex::new(group_writer),
            writer_mode: GroupWriterMode::Tokio(io_handle.clone()),
            records_since_checkpoint: AtomicU64::new(0),
            last_checkpoint_seq: AtomicU64::new(0),
        })
    }

    /// Recover from ilog file.
    ///
    /// Returns:
    /// - The ilog service
    /// - Recovered Version state
    /// - Set of incomplete intent SST IDs (for cleanup)
    pub fn recover(
        config: IlogConfig,
        lsn_provider: SharedLsnProvider,
        io_handle: &tokio::runtime::Handle,
    ) -> Result<(Self, Version, HashSet<u64>)> {
        let ilog_path = config.ilog_path();

        if !ilog_path.exists() {
            let service = Self::open(config, lsn_provider, io_handle)?;
            return Ok((service, Version::new(), HashSet::new()));
        }

        // Read all records
        let file = File::open(&ilog_path)?;
        let mut reader = BufReader::new(file);
        Self::validate_header(&mut reader)?;
        let records = Self::read_records(&mut reader)?;

        log_info!(
            "Recovered {} ilog records from {:?}",
            records.len(),
            ilog_path
        );

        // Rebuild Version from records
        let (version, orphan_ssts, max_lsn, last_checkpoint_seq) = Self::replay_records(&records)?;

        // Update LSN provider
        if max_lsn > 0 {
            let current = lsn_provider.current_lsn();
            if max_lsn >= current {
                lsn_provider.set_lsn(max_lsn + 1);
            }
        }

        // Open service
        let service = Self::open(config, Arc::clone(&lsn_provider), io_handle)?;
        service
            .last_checkpoint_seq
            .store(last_checkpoint_seq, Ordering::SeqCst);

        Ok((service, version, orphan_ssts))
    }

    /// Write a flush intent record.
    pub fn write_flush_intent(&self, sst_id: u64, memtable_id: u64) -> Result<Lsn> {
        let lsn = self.lsn_provider.alloc_lsn();
        let record = IlogRecord::FlushIntent {
            lsn,
            sst_id,
            memtable_id,
        };
        self.write_record(&record)?;

        // Failpoint: crash after flush intent write
        #[cfg(feature = "failpoints")]
        fail_point!("ilog_after_flush_intent");

        log_trace!("Wrote FlushIntent: lsn={}, sst_id={}", lsn, sst_id);
        Ok(lsn)
    }

    /// Write a flush commit record.
    pub fn write_flush_commit(&self, sst_meta: SstMeta, flushed_lsn: Lsn) -> Result<Lsn> {
        let lsn = self.lsn_provider.alloc_lsn();
        let record = IlogRecord::FlushCommit {
            lsn,
            sst_meta,
            flushed_lsn,
        };
        self.write_record(&record)?;
        log_trace!(
            "Wrote FlushCommit: lsn={}, flushed_lsn={}",
            lsn,
            flushed_lsn
        );
        Ok(lsn)
    }

    /// Write a compact intent record.
    pub fn write_compact_intent(
        &self,
        input_sst_ids: Vec<u64>,
        output_sst_ids: Vec<u64>,
        target_level: u32,
    ) -> Result<Lsn> {
        let lsn = self.lsn_provider.alloc_lsn();
        let record = IlogRecord::CompactIntent {
            lsn,
            input_sst_ids,
            output_sst_ids,
            target_level,
        };
        self.write_record(&record)?;
        log_trace!("Wrote CompactIntent: lsn={}", lsn);
        Ok(lsn)
    }

    /// Write a compact commit record.
    pub fn write_compact_commit(
        &self,
        deleted_ssts: Vec<(u32, u64)>,
        new_ssts: Vec<SstMeta>,
    ) -> Result<Lsn> {
        let lsn = self.lsn_provider.alloc_lsn();
        let record = IlogRecord::CompactCommit {
            lsn,
            deleted_ssts,
            new_ssts,
        };
        self.write_record(&record)?;
        log_trace!("Wrote CompactCommit: lsn={}", lsn);
        Ok(lsn)
    }

    /// Write a checkpoint record.
    pub fn write_checkpoint(&self, version: &Version) -> Result<Lsn> {
        let lsn = self.lsn_provider.alloc_lsn();
        let checkpoint_seq = self.last_checkpoint_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let record = IlogRecord::Checkpoint {
            lsn,
            version: VersionSnapshot::from(version),
            checkpoint_seq,
        };

        // Failpoint: crash mid checkpoint write (before actual write)
        #[cfg(feature = "failpoints")]
        fail_point!("ilog_checkpoint_mid_write");

        self.write_record(&record)?;
        self.records_since_checkpoint.store(0, Ordering::SeqCst);
        log_info!("Wrote Checkpoint: lsn={}, seq={}", lsn, checkpoint_seq);
        Ok(lsn)
    }

    /// Check if a checkpoint is needed based on record count.
    pub fn needs_checkpoint(&self) -> bool {
        self.records_since_checkpoint.load(Ordering::SeqCst) as usize
            >= self.config.checkpoint_interval
    }

    /// Sync the log to disk.
    pub fn sync(&self) -> Result<()> {
        let rx = self
            .group_writer
            .lock()
            .submit(Vec::new(), true)
            .map_err(|e| TiSqlError::Internal(format!("Ilog group commit sync failed: {e}")))?;
        self.wait_for_reply(rx)
    }

    /// Truncate ilog records with lsn < min_keep_lsn.
    ///
    /// Keeps only records at or after `min_keep_lsn` by rewriting the ilog
    /// file and durably replacing the old file.
    pub fn truncate_before(&self, min_keep_lsn: Lsn) -> Result<IlogTruncateStats> {
        // Serialize with append path and drain already-submitted writes.
        let mut group_writer = self.group_writer.lock();
        let barrier_rx = group_writer
            .submit(Vec::new(), true)
            .map_err(|e| TiSqlError::Internal(format!("Ilog group commit sync failed: {e}")))?;
        self.wait_for_reply(barrier_rx)?;

        let ilog_path = self.config.ilog_path();
        let old_size = std::fs::metadata(&ilog_path).map(|m| m.len()).unwrap_or(0);

        // Safe to read now: no concurrent appends and prior appends are durable.
        let records = self.read_all_records()?;
        let (to_keep, to_remove): (Vec<_>, Vec<_>) = records
            .into_iter()
            .partition(|record| record.lsn() >= min_keep_lsn);

        if to_remove.is_empty() {
            return Ok(IlogTruncateStats {
                records_removed: 0,
                records_kept: to_keep.len(),
                bytes_freed: 0,
                new_file_size: old_size,
            });
        }

        // Rewrite kept records into a temp file.
        let temp_path = ilog_path.with_extension("ilog.tmp");
        {
            let temp_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temp_path)?;
            let mut temp_writer = BufWriter::new(temp_file);
            Self::write_header(&mut temp_writer)?;
            for record in &to_keep {
                let framed = Self::encode_record(record)?;
                temp_writer.write_all(&framed)?;
            }
            temp_writer.flush()?;
            temp_writer.get_ref().sync_data()?;
        }

        rename_durable(&temp_path, &ilog_path)?;

        // Reopen append writer and swap group writer.
        let mut file_for_write = OpenOptions::new().read(true).write(true).open(&ilog_path)?;
        file_for_write.seek(SeekFrom::End(0))?;
        *group_writer = self.create_group_writer(BufWriter::new(file_for_write));

        let new_size = std::fs::metadata(&ilog_path).map(|m| m.len()).unwrap_or(0);
        log_info!(
            "Truncated ilog before min_keep_lsn={}: removed {} records, kept {}",
            min_keep_lsn,
            to_remove.len(),
            to_keep.len()
        );

        Ok(IlogTruncateStats {
            records_removed: to_remove.len(),
            records_kept: to_keep.len(),
            bytes_freed: old_size.saturating_sub(new_size),
            new_file_size: new_size,
        })
    }

    /// Get the ilog file size in bytes.
    pub fn file_size(&self) -> Result<u64> {
        let ilog_path = self.config.ilog_path();
        let metadata = std::fs::metadata(&ilog_path)?;
        Ok(metadata.len())
    }

    /// Clean up orphan SST files.
    ///
    /// Removes SST files that are not in the Version and were from incomplete operations.
    pub fn cleanup_orphan_ssts(
        &self,
        version: &Version,
        orphan_ids: &HashSet<u64>,
    ) -> Result<usize> {
        let mut cleaned = 0;

        for sst_id in orphan_ids {
            let sst_path = self.config.sst_dir.join(format!("{sst_id:08}.sst"));
            if sst_path.exists() {
                log_warn!("Removing orphan SST file: {:?}", sst_path);
                fs::remove_file(&sst_path)?;
                cleaned += 1;
            }
        }

        // Also scan for any SST files not in Version
        if self.config.sst_dir.exists() {
            for entry in fs::read_dir(&self.config.sst_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().map(|e| e == "sst").unwrap_or(false) {
                    if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                        if let Ok(sst_id) = filename.parse::<u64>() {
                            if !version.has_sst(sst_id) {
                                log_warn!("Removing orphan SST file (not in version): {:?}", path);
                                fs::remove_file(&path)?;
                                cleaned += 1;
                            }
                        }
                    }
                }
            }
        }

        if cleaned > 0 {
            log_info!("Cleaned up {} orphan SST files", cleaned);
        }

        Ok(cleaned)
    }

    /// Close the ilog service, flushing any pending writes.
    pub fn close(&self) -> Result<()> {
        self.sync()
    }

    /// Shutdown the group commit writer channel.
    ///
    /// Must be called before the io_runtime is dropped, otherwise the runtime
    /// drop blocks waiting for the writer loop (spawn_blocking task) to exit.
    pub fn shutdown(&self) {
        self.group_writer.lock().shutdown();
    }

    /// Open or create an ilog service using a plain `std::thread` for
    /// the group commit writer.
    ///
    /// Use this in test code and contexts where no tokio runtime handle
    /// is available. The group commit writer thread exits when the
    /// IlogService is dropped.
    pub fn open_with_thread(config: IlogConfig, lsn_provider: SharedLsnProvider) -> Result<Self> {
        // Ensure directory exists with durable creation
        create_dir_durable(&config.ilog_dir)?;

        let ilog_path = config.ilog_path();
        let file_exists = ilog_path.exists();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&ilog_path)?;

        let mut valid_data_end: Option<u64> = None;

        if file_exists && file.metadata()?.len() > 0 {
            let mut reader = BufReader::new(&file);
            Self::validate_header(&mut reader)?;
            let (_, valid_bytes) = Self::read_records_with_valid_length(&mut reader)?;
            valid_data_end = Some(FILE_HEADER_SIZE as u64 + valid_bytes);
            drop(reader);
        } else {
            let mut writer = BufWriter::new(&file);
            Self::write_header(&mut writer)?;
            writer.flush()?;
            file.sync_all()?;
            sync_dir(&config.ilog_dir)?;
        }

        let mut file_for_write = OpenOptions::new().read(true).write(true).open(&ilog_path)?;

        if let Some(valid_end) = valid_data_end {
            let actual_len = file_for_write.metadata()?.len();
            if actual_len > valid_end {
                log_warn!(
                    "Truncating corrupted ilog tail: {} bytes -> {} bytes",
                    actual_len,
                    valid_end
                );
                file_for_write.set_len(valid_end)?;
                file_for_write.sync_all()?;
            }
        }

        file_for_write.seek(SeekFrom::End(0))?;

        log_info!("Opened ilog file: {:?}", ilog_path);

        let group_writer =
            crate::clog::GroupCommitWriter::new_with_thread(BufWriter::new(file_for_write));

        Ok(Self {
            config,
            lsn_provider,
            group_writer: Mutex::new(group_writer),
            writer_mode: GroupWriterMode::Thread,
            records_since_checkpoint: AtomicU64::new(0),
            last_checkpoint_seq: AtomicU64::new(0),
        })
    }

    /// Recover from ilog file using a plain `std::thread` for the group
    /// commit writer.
    pub fn recover_with_thread(
        config: IlogConfig,
        lsn_provider: SharedLsnProvider,
    ) -> Result<(Self, Version, HashSet<u64>)> {
        let ilog_path = config.ilog_path();

        if !ilog_path.exists() {
            let service = Self::open_with_thread(config, lsn_provider)?;
            return Ok((service, Version::new(), HashSet::new()));
        }

        let file = File::open(&ilog_path)?;
        let mut reader = BufReader::new(file);
        Self::validate_header(&mut reader)?;
        let records = Self::read_records(&mut reader)?;

        log_info!(
            "Recovered {} ilog records from {:?}",
            records.len(),
            ilog_path
        );

        let (version, orphan_ssts, max_lsn, last_checkpoint_seq) = Self::replay_records(&records)?;

        if max_lsn > 0 {
            let current = lsn_provider.current_lsn();
            if max_lsn >= current {
                lsn_provider.set_lsn(max_lsn + 1);
            }
        }

        let service = Self::open_with_thread(config, Arc::clone(&lsn_provider))?;
        service
            .last_checkpoint_seq
            .store(last_checkpoint_seq, Ordering::SeqCst);

        Ok((service, version, orphan_ssts))
    }

    // ========== Private methods ==========

    fn create_group_writer(&self, writer: BufWriter<File>) -> crate::clog::GroupCommitWriter {
        match &self.writer_mode {
            GroupWriterMode::Tokio(handle) => crate::clog::GroupCommitWriter::new(writer, handle),
            GroupWriterMode::Thread => crate::clog::GroupCommitWriter::new_with_thread(writer),
        }
    }

    fn read_all_records(&self) -> Result<Vec<IlogRecord>> {
        let ilog_path = self.config.ilog_path();
        let file = File::open(&ilog_path)?;
        let mut reader = BufReader::new(file);
        Self::validate_header(&mut reader)?;
        Self::read_records(&mut reader)
    }

    fn write_record(&self, record: &IlogRecord) -> Result<()> {
        let buf = Self::encode_record(record)?;

        // Submit to group commit writer (batches fsync with concurrent writers)
        let rx = self
            .group_writer
            .lock()
            .submit(buf, true)
            .map_err(|e| TiSqlError::Internal(format!("Ilog group commit failed: {e}")))?;
        self.wait_for_reply(rx)?;

        // Failpoint: crash after fsync
        #[cfg(feature = "failpoints")]
        fail_point!("ilog_after_fsync");

        self.records_since_checkpoint.fetch_add(1, Ordering::SeqCst);

        Ok(())
    }

    fn encode_record(record: &IlogRecord) -> Result<Vec<u8>> {
        let data = bincode::serialize(record)
            .map_err(|e| TiSqlError::Internal(format!("Failed to serialize ilog record: {e}")))?;

        if data.len() > MAX_RECORD_SIZE {
            return Err(TiSqlError::Internal(format!(
                "Record size {} exceeds maximum allowed size {}",
                data.len(),
                MAX_RECORD_SIZE
            )));
        }

        let checksum = crc32(&data);
        let record_type: u32 = 1;
        let mut buf = Vec::with_capacity(RECORD_HEADER_SIZE + data.len());
        buf.extend_from_slice(&record_type.to_le_bytes());
        buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf.extend_from_slice(&data);
        Ok(buf)
    }

    /// Wait for group commit reply. Uses `.await` in async context,
    /// falls back to `block_on_sync` otherwise.
    fn wait_for_reply(
        &self,
        rx: tokio::sync::oneshot::Receiver<std::result::Result<(), String>>,
    ) -> Result<()> {
        crate::io::block_on_sync(rx)
            .map_err(|_| TiSqlError::Internal("Ilog writer dropped".into()))?
            .map_err(|e| TiSqlError::Internal(format!("Ilog group commit failed: {e}")))?;
        Ok(())
    }

    fn write_header<W: Write>(writer: &mut W) -> Result<()> {
        writer.write_all(FILE_MAGIC)?;
        writer.write_all(&FILE_VERSION.to_le_bytes())?;
        // Reserved bytes
        writer.write_all(&[0u8; 8])?;
        Ok(())
    }

    fn validate_header<R: Read>(reader: &mut R) -> Result<()> {
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != FILE_MAGIC {
            return Err(TiSqlError::Internal(format!(
                "Invalid ilog file magic: {magic:?}"
            )));
        }

        let mut version_bytes = [0u8; 4];
        reader.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);
        if version != FILE_VERSION {
            return Err(TiSqlError::Internal(format!(
                "Unsupported ilog version: {version}"
            )));
        }

        // Skip reserved bytes
        let mut reserved = [0u8; 8];
        reader.read_exact(&mut reserved)?;

        Ok(())
    }

    fn read_records<R: Read>(reader: &mut R) -> Result<Vec<IlogRecord>> {
        let (records, _) = Self::read_records_with_valid_length(reader)?;
        Ok(records)
    }

    /// Read all records from file (after header) and return the valid length.
    ///
    /// Returns (records, bytes_read) where bytes_read is the number of bytes
    /// read from valid records (not including any corrupted tail).
    fn read_records_with_valid_length<R: Read>(reader: &mut R) -> Result<(Vec<IlogRecord>, u64)> {
        let mut records = Vec::new();
        let mut valid_bytes = 0u64;

        loop {
            match Self::read_record_with_size(reader) {
                Ok(Some((record, record_size))) => {
                    valid_bytes += record_size as u64;
                    records.push(record);
                }
                Ok(None) => break,
                Err(e) => {
                    log_warn!("Stopping ilog recovery at corrupted record: {}", e);
                    break;
                }
            }
        }

        Ok((records, valid_bytes))
    }

    /// Read a single record from the ilog, returning record and size in bytes.
    fn read_record_with_size<R: Read>(reader: &mut R) -> Result<Option<(IlogRecord, usize)>> {
        let mut header = [0u8; RECORD_HEADER_SIZE];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        }

        let record_type = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let length = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let checksum = u32::from_le_bytes(header[8..12].try_into().unwrap());

        if record_type != 1 {
            return Err(TiSqlError::Internal(format!(
                "Unknown ilog record type: {record_type}"
            )));
        }

        if length > MAX_RECORD_SIZE {
            return Err(TiSqlError::Internal(format!(
                "Record size {length} exceeds maximum (possibly corrupted)"
            )));
        }

        let mut data = vec![0u8; length];
        reader.read_exact(&mut data)?;

        let computed_checksum = crc32(&data);
        if computed_checksum != checksum {
            return Err(TiSqlError::Internal(format!(
                "Checksum mismatch: expected {checksum}, got {computed_checksum}"
            )));
        }

        let record: IlogRecord = bincode::deserialize(&data)
            .map_err(|e| TiSqlError::Internal(format!("Failed to deserialize ilog record: {e}")))?;

        // Total record size = header + data
        let record_size = RECORD_HEADER_SIZE + length;

        Ok(Some((record, record_size)))
    }

    /// Replay records to rebuild Version.
    ///
    /// Returns (version, orphan_sst_ids, max_lsn, last_checkpoint_seq)
    fn replay_records(records: &[IlogRecord]) -> Result<(Version, HashSet<u64>, Lsn, u64)> {
        let mut version = Version::new();
        let mut pending = PendingOps::default();
        let mut orphan_ssts = HashSet::new();
        let mut max_lsn: Lsn = 0;
        let mut last_checkpoint_seq: u64 = 0;

        // Find last checkpoint and replay from there
        let start_idx = records
            .iter()
            .rposition(|r| matches!(r, IlogRecord::Checkpoint { .. }))
            .unwrap_or(0);

        for record in &records[start_idx..] {
            max_lsn = max_lsn.max(record.lsn());

            match record {
                IlogRecord::Checkpoint {
                    version: snapshot,
                    checkpoint_seq,
                    ..
                } => {
                    // Reset version to checkpoint state
                    version = Self::version_from_snapshot(snapshot);
                    pending = PendingOps::default();
                    last_checkpoint_seq = *checkpoint_seq;
                }

                IlogRecord::FlushIntent { sst_id, .. } => {
                    pending.flush_intents.insert(*sst_id);
                }

                IlogRecord::FlushCommit {
                    sst_meta,
                    flushed_lsn,
                    ..
                } => {
                    // Apply to version
                    let delta =
                        super::version::ManifestDelta::flush(sst_meta.clone(), *flushed_lsn);
                    version = version.apply(&delta);
                    pending.flush_intents.remove(&sst_meta.id);
                }

                IlogRecord::CompactIntent { output_sst_ids, .. } => {
                    pending.compact_intents.push(output_sst_ids.clone());
                }

                IlogRecord::CompactCommit {
                    deleted_ssts,
                    new_ssts,
                    ..
                } => {
                    // Apply to version
                    let delta = super::version::ManifestDelta {
                        new_ssts: new_ssts.iter().map(|m| Arc::new(m.clone())).collect(),
                        deleted_ssts: deleted_ssts.clone(),
                        flushed_lsn: None,
                        sequence: 0,
                    };
                    version = version.apply(&delta);

                    // Remove matched intent
                    if !pending.compact_intents.is_empty() {
                        pending.compact_intents.remove(0);
                    }
                }
            }
        }

        // Any remaining intents are orphans
        for sst_id in pending.flush_intents {
            orphan_ssts.insert(sst_id);
        }
        for output_ids in pending.compact_intents {
            for sst_id in output_ids {
                orphan_ssts.insert(sst_id);
            }
        }

        Ok((version, orphan_ssts, max_lsn, last_checkpoint_seq))
    }

    fn version_from_snapshot(snapshot: &VersionSnapshot) -> Version {
        use super::version::VersionBuilder;

        let mut builder = VersionBuilder::new();
        builder = builder
            .next_sst_id(snapshot.next_sst_id)
            .version_num(snapshot.version_num)
            .flushed_lsn(snapshot.flushed_lsn);

        for (level, ssts) in snapshot.levels.iter().enumerate() {
            for sst in ssts {
                builder = builder.add_sst_at_level(level as u32, Arc::new(sst.clone()));
            }
        }

        builder.build()
    }
}

/// Compute CRC32 checksum (same as clog).
fn crc32(data: &[u8]) -> u32 {
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
    use crate::lsn::new_lsn_provider;
    use std::path::Path;
    use tempfile::TempDir;

    fn test_config(dir: &Path) -> IlogConfig {
        IlogConfig::new(dir)
    }

    fn test_sst_meta(id: u64, level: u32) -> SstMeta {
        SstMeta {
            id,
            level,
            file_size: 1000,
            entry_count: 100,
            block_count: 10,
            smallest_key: vec![0],
            largest_key: vec![255],
            min_ts: 1,
            max_ts: 100,
            created_at: 0,
        }
    }

    #[tokio::test]
    async fn test_ilog_open() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();

        let service =
            IlogService::open(config, lsn_provider, &tokio::runtime::Handle::current()).unwrap();
        service.sync().unwrap();
    }

    #[tokio::test]
    async fn test_ilog_flush_intent_commit() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();
        let io_handle = tokio::runtime::Handle::current();

        let service =
            IlogService::open(config.clone(), Arc::clone(&lsn_provider), &io_handle).unwrap();

        // Write flush intent
        let intent_lsn = service.write_flush_intent(1, 100).unwrap();
        assert_eq!(intent_lsn, 1);

        // Write flush commit
        let meta = test_sst_meta(1, 0);
        let commit_lsn = service.write_flush_commit(meta, 50).unwrap();
        assert_eq!(commit_lsn, 2);

        service.sync().unwrap();

        // Recover and verify
        let lsn_provider2 = new_lsn_provider();
        let (_, version, orphans) =
            IlogService::recover(config, lsn_provider2, &io_handle).unwrap();

        assert!(orphans.is_empty(), "Should have no orphans");
        assert_eq!(version.level_size(0), 1);
        assert_eq!(version.flushed_lsn(), 50);
    }

    #[tokio::test]
    async fn test_ilog_incomplete_flush() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();
        let io_handle = tokio::runtime::Handle::current();

        let service =
            IlogService::open(config.clone(), Arc::clone(&lsn_provider), &io_handle).unwrap();

        // Write flush intent but no commit (simulating crash)
        service.write_flush_intent(1, 100).unwrap();
        service.sync().unwrap();

        // Recover
        let lsn_provider2 = new_lsn_provider();
        let (_, version, orphans) =
            IlogService::recover(config, lsn_provider2, &io_handle).unwrap();

        assert!(orphans.contains(&1), "SST 1 should be orphan");
        assert_eq!(version.level_size(0), 0, "No SST should be in version");
    }

    #[tokio::test]
    async fn test_ilog_compact_intent_commit() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();
        let io_handle = tokio::runtime::Handle::current();

        let service =
            IlogService::open(config.clone(), Arc::clone(&lsn_provider), &io_handle).unwrap();

        // First add some SSTs via flush
        let meta1 = test_sst_meta(1, 0);
        let meta2 = test_sst_meta(2, 0);
        service.write_flush_intent(1, 100).unwrap();
        service.write_flush_commit(meta1.clone(), 50).unwrap();
        service.write_flush_intent(2, 101).unwrap();
        service.write_flush_commit(meta2.clone(), 100).unwrap();

        // Now compact
        service
            .write_compact_intent(vec![1, 2], vec![3], 1)
            .unwrap();
        let meta3 = test_sst_meta(3, 1);
        service
            .write_compact_commit(vec![(0, 1), (0, 2)], vec![meta3])
            .unwrap();

        service.sync().unwrap();

        // Recover
        let lsn_provider2 = new_lsn_provider();
        let (_, version, orphans) =
            IlogService::recover(config, lsn_provider2, &io_handle).unwrap();

        assert!(orphans.is_empty());
        assert_eq!(
            version.level_size(0),
            0,
            "L0 should be empty after compaction"
        );
        assert_eq!(version.level_size(1), 1, "L1 should have 1 SST");
    }

    #[tokio::test]
    async fn test_ilog_incomplete_compact() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();
        let io_handle = tokio::runtime::Handle::current();

        let service =
            IlogService::open(config.clone(), Arc::clone(&lsn_provider), &io_handle).unwrap();

        // Add SSTs
        let meta1 = test_sst_meta(1, 0);
        service.write_flush_intent(1, 100).unwrap();
        service.write_flush_commit(meta1, 50).unwrap();

        // Start compact but don't commit
        service.write_compact_intent(vec![1], vec![2], 1).unwrap();
        service.sync().unwrap();

        // Recover
        let lsn_provider2 = new_lsn_provider();
        let (_, version, orphans) =
            IlogService::recover(config, lsn_provider2, &io_handle).unwrap();

        assert!(orphans.contains(&2), "Output SST 2 should be orphan");
        assert_eq!(version.level_size(0), 1, "Original SST should remain");
    }

    #[tokio::test]
    async fn test_ilog_checkpoint() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();
        let io_handle = tokio::runtime::Handle::current();

        let service =
            IlogService::open(config.clone(), Arc::clone(&lsn_provider), &io_handle).unwrap();

        // Add many flushes
        for i in 1..=10 {
            let meta = test_sst_meta(i, 0);
            service.write_flush_intent(i, 100 + i).unwrap();
            service.write_flush_commit(meta, i * 10).unwrap();
        }

        // Write checkpoint
        let lsn_provider2 = new_lsn_provider();
        let (_, version, _) =
            IlogService::recover(config.clone(), lsn_provider2, &io_handle).unwrap();

        let lsn_provider3 = new_lsn_provider();
        let service2 = IlogService::open(config.clone(), lsn_provider3, &io_handle).unwrap();
        service2.write_checkpoint(&version).unwrap();
        service2.sync().unwrap();

        // Recover from checkpoint
        let lsn_provider4 = new_lsn_provider();
        let (_, recovered_version, orphans) =
            IlogService::recover(config, lsn_provider4, &io_handle).unwrap();

        assert!(orphans.is_empty());
        assert_eq!(recovered_version.level_size(0), 10);
        assert_eq!(recovered_version.flushed_lsn(), 100);
    }

    #[tokio::test]
    async fn test_ilog_lsn_continuity() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();
        let io_handle = tokio::runtime::Handle::current();

        let service =
            IlogService::open(config.clone(), Arc::clone(&lsn_provider), &io_handle).unwrap();

        // Write some records
        service.write_flush_intent(1, 100).unwrap();
        let meta = test_sst_meta(1, 0);
        service.write_flush_commit(meta, 50).unwrap();
        service.sync().unwrap();

        // Recover
        let lsn_provider2 = new_lsn_provider();
        let (service2, _, _) =
            IlogService::recover(config.clone(), lsn_provider2.clone(), &io_handle).unwrap();

        // LSN should continue from where we left off
        assert!(lsn_provider2.current_lsn() >= 3);

        // New writes should have higher LSNs
        let new_lsn = service2.write_flush_intent(2, 101).unwrap();
        assert!(new_lsn >= 3);
    }

    #[tokio::test]
    async fn test_ilog_orphan_cleanup() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();
        let io_handle = tokio::runtime::Handle::current();

        // Create SST directory and orphan file
        fs::create_dir_all(&config.sst_dir).unwrap();
        let orphan_path = config.sst_dir.join("00000099.sst");
        fs::write(&orphan_path, b"orphan data").unwrap();
        assert!(orphan_path.exists());

        let service =
            IlogService::open(config.clone(), Arc::clone(&lsn_provider), &io_handle).unwrap();

        // Add a valid SST
        let meta = test_sst_meta(1, 0);
        service.write_flush_intent(1, 100).unwrap();
        service.write_flush_commit(meta, 50).unwrap();
        service.sync().unwrap();

        // Recover and get version
        let lsn_provider2 = new_lsn_provider();
        let (service2, version, _orphans) =
            IlogService::recover(config, lsn_provider2, &io_handle).unwrap();

        // Cleanup should remove orphan file
        let cleaned = service2
            .cleanup_orphan_ssts(&version, &HashSet::new())
            .unwrap();
        assert_eq!(cleaned, 1);
        assert!(!orphan_path.exists());
    }

    #[tokio::test]
    async fn test_ilog_needs_checkpoint() {
        let tmp = TempDir::new().unwrap();
        let mut config = test_config(tmp.path());
        config.checkpoint_interval = 5;
        let lsn_provider = new_lsn_provider();

        let service = IlogService::open(
            config,
            Arc::clone(&lsn_provider),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        assert!(!service.needs_checkpoint());

        // Write 5 records
        for i in 1..=5 {
            service.write_flush_intent(i, 100 + i).unwrap();
        }

        assert!(service.needs_checkpoint());
    }

    #[tokio::test]
    async fn test_ilog_truncate_before_none() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();
        let io_handle = tokio::runtime::Handle::current();

        let service =
            IlogService::open(config.clone(), Arc::clone(&lsn_provider), &io_handle).unwrap();
        service.write_flush_intent(1, 100).unwrap();
        service.write_flush_commit(test_sst_meta(1, 0), 50).unwrap();
        service.sync().unwrap();

        let old_size = service.file_size().unwrap();
        let stats = service.truncate_before(1).unwrap();
        assert_eq!(stats.records_removed, 0);
        assert_eq!(stats.records_kept, 2);
        assert_eq!(stats.bytes_freed, 0);
        assert_eq!(stats.new_file_size, old_size);

        let lsn_provider2 = new_lsn_provider();
        let (_, version, orphans) =
            IlogService::recover(config, lsn_provider2, &io_handle).unwrap();
        assert!(orphans.is_empty());
        assert_eq!(version.level_size(0), 1);
    }

    #[tokio::test]
    async fn test_ilog_truncate_before_all_older() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();
        let io_handle = tokio::runtime::Handle::current();

        let service =
            IlogService::open(config.clone(), Arc::clone(&lsn_provider), &io_handle).unwrap();
        service.write_flush_intent(1, 100).unwrap();
        service.write_flush_commit(test_sst_meta(1, 0), 50).unwrap();
        service.write_flush_intent(2, 101).unwrap();
        service
            .write_flush_commit(test_sst_meta(2, 0), 100)
            .unwrap();
        service.sync().unwrap();

        let stats = service.truncate_before(10).unwrap();
        assert_eq!(stats.records_removed, 4);
        assert_eq!(stats.records_kept, 0);
        assert_eq!(stats.new_file_size, FILE_HEADER_SIZE as u64);

        let lsn_provider2 = new_lsn_provider();
        let (_, version, orphans) =
            IlogService::recover(config, lsn_provider2, &io_handle).unwrap();
        assert!(orphans.is_empty());
        assert_eq!(version.level_size(0), 0);
    }

    #[tokio::test]
    async fn test_ilog_truncate_before_then_append_continuity() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();
        let io_handle = tokio::runtime::Handle::current();

        let service =
            IlogService::open(config.clone(), Arc::clone(&lsn_provider), &io_handle).unwrap();

        service.write_flush_intent(1, 100).unwrap();
        service.write_flush_commit(test_sst_meta(1, 0), 50).unwrap();
        let keep_from_lsn = service.write_flush_intent(2, 101).unwrap();
        service
            .write_flush_commit(test_sst_meta(2, 0), 100)
            .unwrap();
        service.sync().unwrap();

        let stats = service.truncate_before(keep_from_lsn).unwrap();
        assert_eq!(stats.records_removed, 2);
        assert_eq!(stats.records_kept, 2);

        let new_lsn = service.write_flush_intent(3, 102).unwrap();
        service
            .write_flush_commit(test_sst_meta(3, 0), 150)
            .unwrap();
        service.sync().unwrap();
        assert!(
            new_lsn > 4,
            "new writes should continue with higher lsn after truncation"
        );

        let lsn_provider2 = new_lsn_provider();
        let (_, version, orphans) =
            IlogService::recover(config, lsn_provider2, &io_handle).unwrap();
        assert!(orphans.is_empty());
        assert!(!version.has_sst(1));
        assert!(version.has_sst(2));
        assert!(version.has_sst(3));
    }

    #[test]
    fn test_ilog_record_lsn_all_variants() {
        // Test IlogRecord::lsn() for all record types
        let flush_intent = IlogRecord::FlushIntent {
            lsn: 100,
            sst_id: 1,
            memtable_id: 1,
        };
        assert_eq!(flush_intent.lsn(), 100);

        let flush_commit = IlogRecord::FlushCommit {
            lsn: 200,
            sst_meta: test_sst_meta(1, 0),
            flushed_lsn: 50,
        };
        assert_eq!(flush_commit.lsn(), 200);

        let compact_intent = IlogRecord::CompactIntent {
            lsn: 300,
            input_sst_ids: vec![1, 2],
            output_sst_ids: vec![3],
            target_level: 1,
        };
        assert_eq!(compact_intent.lsn(), 300);

        let compact_commit = IlogRecord::CompactCommit {
            lsn: 400,
            deleted_ssts: vec![(0, 1), (0, 2)],
            new_ssts: vec![test_sst_meta(3, 1)],
        };
        assert_eq!(compact_commit.lsn(), 400);

        let checkpoint = IlogRecord::Checkpoint {
            lsn: 500,
            version: VersionSnapshot {
                levels: vec![],
                next_sst_id: 1,
                version_num: 1,
                flushed_lsn: 50,
            },
            checkpoint_seq: 1,
        };
        assert_eq!(checkpoint.lsn(), 500);
    }

    #[test]
    fn test_version_snapshot_from_version() {
        use super::super::version::VersionBuilder;

        // Create a version with some SSTs
        let meta1 = Arc::new(test_sst_meta(1, 0));
        let meta2 = Arc::new(test_sst_meta(2, 0));
        let meta3 = Arc::new(test_sst_meta(3, 1));

        let version = VersionBuilder::new()
            .add_sst_at_level(0, meta1)
            .add_sst_at_level(0, meta2)
            .add_sst_at_level(1, meta3)
            .next_sst_id(4)
            .version_num(5)
            .flushed_lsn(100)
            .build();

        let snapshot = VersionSnapshot::from(&version);

        assert_eq!(snapshot.next_sst_id, 4);
        assert_eq!(snapshot.version_num, 5);
        assert_eq!(snapshot.flushed_lsn, 100);
        assert_eq!(snapshot.levels[0].len(), 2);
        assert_eq!(snapshot.levels[1].len(), 1);
    }

    #[tokio::test]
    async fn test_ilog_corrupted_header_magic() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());

        // Create a file with invalid magic
        std::fs::create_dir_all(&config.ilog_dir).unwrap();
        let path = config.ilog_path();
        std::fs::write(
            &path,
            b"BADM\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        .unwrap();

        let lsn_provider = new_lsn_provider();
        let result = IlogService::open(config, lsn_provider, &tokio::runtime::Handle::current());

        match result {
            Ok(_) => panic!("Expected error for invalid magic"),
            Err(e) => assert!(
                e.to_string().contains("Invalid ilog file magic"),
                "Error should mention invalid magic: {e}"
            ),
        }
    }

    #[tokio::test]
    async fn test_ilog_corrupted_header_version() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());

        // Create a file with unsupported version
        std::fs::create_dir_all(&config.ilog_dir).unwrap();
        let path = config.ilog_path();
        std::fs::write(
            &path,
            b"ILOG\x99\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        .unwrap();

        let lsn_provider = new_lsn_provider();
        let result = IlogService::open(config, lsn_provider, &tokio::runtime::Handle::current());

        match result {
            Ok(_) => panic!("Expected error for unsupported version"),
            Err(e) => assert!(
                e.to_string().contains("Unsupported ilog version"),
                "Error should mention unsupported version: {e}"
            ),
        }
    }

    #[tokio::test]
    async fn test_ilog_checksum_mismatch() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();
        let io_handle = tokio::runtime::Handle::current();

        // First write a valid record
        let service =
            IlogService::open(config.clone(), Arc::clone(&lsn_provider), &io_handle).unwrap();
        service.write_flush_intent(1, 100).unwrap();
        service.sync().unwrap();
        drop(service);

        // Corrupt the checksum in the file
        let path = config.ilog_path();
        let mut data = std::fs::read(&path).unwrap();
        // The checksum is at offset FILE_HEADER_SIZE + 8 (after type and length)
        let checksum_offset = FILE_HEADER_SIZE + 8;
        if data.len() > checksum_offset + 4 {
            data[checksum_offset] ^= 0xFF; // Flip bits
            std::fs::write(&path, &data).unwrap();
        }

        // Recovery should handle the corrupted record
        let lsn_provider2 = new_lsn_provider();
        let (_, version, _) = IlogService::recover(config, lsn_provider2, &io_handle).unwrap();

        // Version should be empty since the record was corrupted
        assert_eq!(version.level_size(0), 0);
    }

    #[tokio::test]
    async fn test_ilog_truncated_record() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();
        let io_handle = tokio::runtime::Handle::current();

        // Write a valid record
        let service =
            IlogService::open(config.clone(), Arc::clone(&lsn_provider), &io_handle).unwrap();
        service.write_flush_intent(1, 100).unwrap();
        let meta = test_sst_meta(1, 0);
        service.write_flush_commit(meta, 50).unwrap();
        service.sync().unwrap();
        drop(service);

        // Truncate the file mid-record (remove last few bytes)
        let path = config.ilog_path();
        let data = std::fs::read(&path).unwrap();
        let truncated_len = data.len() - 10; // Truncate 10 bytes
        std::fs::write(&path, &data[..truncated_len]).unwrap();

        // Recovery should handle truncation gracefully
        let lsn_provider2 = new_lsn_provider();
        let (_, version, orphans) =
            IlogService::recover(config, lsn_provider2, &io_handle).unwrap();

        // First record (flush intent) might be recovered, second (commit) likely corrupted
        // Either way, should not crash
        assert!(
            version.level_size(0) == 0 || orphans.contains(&1),
            "Should either have no SST or orphan for incomplete flush"
        );
    }

    #[tokio::test]
    async fn test_ilog_multiple_checkpoints_recovery() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();
        let io_handle = tokio::runtime::Handle::current();

        // Write records with multiple checkpoints
        let service =
            IlogService::open(config.clone(), Arc::clone(&lsn_provider), &io_handle).unwrap();

        // First batch of flushes
        for i in 1..=3 {
            let meta = test_sst_meta(i, 0);
            service.write_flush_intent(i, 100 + i).unwrap();
            service.write_flush_commit(meta, i * 10).unwrap();
        }

        // First checkpoint
        let lsn_provider2 = new_lsn_provider();
        let (_, version1, _) =
            IlogService::recover(config.clone(), lsn_provider2, &io_handle).unwrap();
        let lsn_provider3 = new_lsn_provider();
        let service2 = IlogService::open(config.clone(), lsn_provider3, &io_handle).unwrap();
        service2.write_checkpoint(&version1).unwrap();
        drop(service2);

        // Second batch of flushes
        let lsn_provider4 = new_lsn_provider();
        let service3 = IlogService::open(config.clone(), lsn_provider4, &io_handle).unwrap();
        for i in 4..=6 {
            let meta = test_sst_meta(i, 0);
            service3.write_flush_intent(i, 100 + i).unwrap();
            service3.write_flush_commit(meta, i * 10).unwrap();
        }

        // Second checkpoint
        let lsn_provider5 = new_lsn_provider();
        let (_, version2, _) =
            IlogService::recover(config.clone(), lsn_provider5, &io_handle).unwrap();
        let lsn_provider6 = new_lsn_provider();
        let service4 = IlogService::open(config.clone(), lsn_provider6, &io_handle).unwrap();
        service4.write_checkpoint(&version2).unwrap();
        drop(service4);

        // Final recovery should have all 6 SSTs
        let lsn_provider7 = new_lsn_provider();
        let (_, final_version, _) =
            IlogService::recover(config, lsn_provider7, &io_handle).unwrap();

        assert_eq!(
            final_version.level_size(0),
            6,
            "Should recover all 6 SSTs from last checkpoint"
        );
    }

    #[test]
    fn test_ilog_crc32_function() {
        // Test the crc32 function with known values
        assert_eq!(crc32(b""), 0x00000000);
        assert_eq!(crc32(b"hello"), 0x3610A686);
        assert_eq!(crc32(b"test"), 0xD87F7E0C);

        // Verify different inputs produce different checksums
        let crc1 = crc32(b"input1");
        let crc2 = crc32(b"input2");
        assert_ne!(crc1, crc2);
    }

    #[test]
    fn test_ilog_config_paths() {
        let config = IlogConfig::new("/data/test");

        assert_eq!(config.ilog_dir, std::path::PathBuf::from("/data/test/ilog"));
        assert_eq!(config.ilog_file, "tisql.ilog");
        assert_eq!(config.sst_dir, std::path::PathBuf::from("/data/test/sst"));
        assert_eq!(
            config.ilog_path(),
            std::path::PathBuf::from("/data/test/ilog/tisql.ilog")
        );
    }

    #[tokio::test]
    async fn test_ilog_close() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();
        let io_handle = tokio::runtime::Handle::current();

        let service =
            IlogService::open(config.clone(), Arc::clone(&lsn_provider), &io_handle).unwrap();
        service.write_flush_intent(1, 100).unwrap();

        // Close should succeed
        service.close().unwrap();

        // File should still be valid after close
        let lsn_provider2 = new_lsn_provider();
        let (_, _, orphans) = IlogService::recover(config, lsn_provider2, &io_handle).unwrap();
        assert!(orphans.contains(&1)); // Incomplete flush intent
    }

    #[tokio::test]
    async fn test_ilog_multiple_incomplete_compacts() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();
        let io_handle = tokio::runtime::Handle::current();

        let service =
            IlogService::open(config.clone(), Arc::clone(&lsn_provider), &io_handle).unwrap();

        // Multiple incomplete compact intents
        service.write_compact_intent(vec![1], vec![10], 1).unwrap();
        service
            .write_compact_intent(vec![2, 3], vec![20, 21], 1)
            .unwrap();
        service.sync().unwrap();

        let lsn_provider2 = new_lsn_provider();
        let (_, _, orphans) = IlogService::recover(config, lsn_provider2, &io_handle).unwrap();

        // All output SST IDs should be orphans
        assert!(orphans.contains(&10));
        assert!(orphans.contains(&20));
        assert!(orphans.contains(&21));
    }

    #[tokio::test]
    async fn test_ilog_reopen_and_append() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();
        let io_handle = tokio::runtime::Handle::current();

        // First session
        {
            let service =
                IlogService::open(config.clone(), Arc::clone(&lsn_provider), &io_handle).unwrap();
            service.write_flush_intent(1, 100).unwrap();
            let meta = test_sst_meta(1, 0);
            service.write_flush_commit(meta, 50).unwrap();
            service.sync().unwrap();
        }

        // Second session - append more
        {
            let lsn_provider2 = new_lsn_provider();
            let (service, _, _) =
                IlogService::recover(config.clone(), lsn_provider2, &io_handle).unwrap();
            service.write_flush_intent(2, 101).unwrap();
            let meta = test_sst_meta(2, 0);
            service.write_flush_commit(meta, 100).unwrap();
            service.sync().unwrap();
        }

        // Verify both records are present
        let lsn_provider3 = new_lsn_provider();
        let (_, version, _) = IlogService::recover(config, lsn_provider3, &io_handle).unwrap();

        assert_eq!(version.level_size(0), 2);
    }

    #[tokio::test]
    async fn test_ilog_empty_file_recovery() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());

        // Create just the header (empty ilog)
        std::fs::create_dir_all(&config.ilog_dir).unwrap();
        let mut header = Vec::new();
        header.extend_from_slice(FILE_MAGIC);
        header.extend_from_slice(&FILE_VERSION.to_le_bytes());
        header.extend_from_slice(&[0u8; 8]); // reserved
        std::fs::write(config.ilog_path(), &header).unwrap();

        let lsn_provider = new_lsn_provider();
        let (_, version, orphans) =
            IlogService::recover(config, lsn_provider, &tokio::runtime::Handle::current()).unwrap();

        assert!(orphans.is_empty());
        assert_eq!(version.level_size(0), 0);
    }

    #[test]
    fn test_ilog_version_snapshot_equality() {
        let snapshot1 = VersionSnapshot {
            levels: vec![vec![test_sst_meta(1, 0)]],
            next_sst_id: 2,
            version_num: 1,
            flushed_lsn: 100,
        };

        let snapshot2 = VersionSnapshot {
            levels: vec![vec![test_sst_meta(1, 0)]],
            next_sst_id: 2,
            version_num: 1,
            flushed_lsn: 100,
        };

        assert_eq!(snapshot1, snapshot2);
    }
}
