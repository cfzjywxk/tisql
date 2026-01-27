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
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::error::{Result, TiSqlError};
use crate::lsn::SharedLsnProvider;
use crate::types::Lsn;
use crate::{log_info, log_trace, log_warn};

use super::sstable::SstMeta;
use super::version::Version;

/// File header magic bytes: "ILOG"
const FILE_MAGIC: &[u8; 4] = b"ILOG";

/// File format version
const FILE_VERSION: u32 = 1;

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
        /// Max LSN in the memtable
        max_memtable_lsn: Lsn,
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
    /// Pending flush intents: sst_id -> (memtable_id, max_memtable_lsn)
    flush_intents: HashSet<u64>,
    /// Pending compact intents: output_sst_ids
    compact_intents: Vec<Vec<u64>>,
}

/// Index log service for SST metadata persistence.
pub struct IlogService {
    config: IlogConfig,
    /// Shared LSN provider
    lsn_provider: SharedLsnProvider,
    /// Log file writer
    writer: Mutex<BufWriter<File>>,
    /// Number of records since last checkpoint
    records_since_checkpoint: AtomicU64,
    /// Last checkpoint sequence
    last_checkpoint_seq: AtomicU64,
}

impl IlogService {
    /// Open or create an ilog service.
    pub fn open(config: IlogConfig, lsn_provider: SharedLsnProvider) -> Result<Self> {
        // Ensure directory exists
        fs::create_dir_all(&config.ilog_dir)?;

        let ilog_path = config.ilog_path();
        let file_exists = ilog_path.exists();

        // Open file for read+write+create
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&ilog_path)?;

        if file_exists && file.metadata()?.len() > 0 {
            // Validate existing file header
            let mut reader = BufReader::new(&file);
            Self::validate_header(&mut reader)?;
            drop(reader);
        } else {
            // Write header to new file
            let mut writer = BufWriter::new(&file);
            Self::write_header(&mut writer)?;
            writer.flush()?;
        }

        // Re-open for appending
        let file = OpenOptions::new().read(true).write(true).open(&ilog_path)?;
        let mut file_for_write = file;
        file_for_write.seek(SeekFrom::End(0))?;

        log_info!("Opened ilog file: {:?}", ilog_path);

        Ok(Self {
            config,
            lsn_provider,
            writer: Mutex::new(BufWriter::new(file_for_write)),
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
    ) -> Result<(Self, Version, HashSet<u64>)> {
        let ilog_path = config.ilog_path();

        if !ilog_path.exists() {
            let service = Self::open(config, lsn_provider)?;
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
        let service = Self::open(config, Arc::clone(&lsn_provider))?;
        service
            .last_checkpoint_seq
            .store(last_checkpoint_seq, Ordering::SeqCst);

        Ok((service, version, orphan_ssts))
    }

    /// Write a flush intent record.
    pub fn write_flush_intent(
        &self,
        sst_id: u64,
        memtable_id: u64,
        max_memtable_lsn: Lsn,
    ) -> Result<Lsn> {
        let lsn = self.lsn_provider.alloc_lsn();
        let record = IlogRecord::FlushIntent {
            lsn,
            sst_id,
            memtable_id,
            max_memtable_lsn,
        };
        self.write_record(&record)?;
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
        let mut writer = self.writer.lock().unwrap();
        writer.flush()?;
        writer.get_ref().sync_data()?;
        Ok(())
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
            let sst_path = self.config.sst_dir.join(format!("{:08}.sst", sst_id));
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

    // ========== Private methods ==========

    fn write_record(&self, record: &IlogRecord) -> Result<()> {
        let data = bincode::serialize(record)
            .map_err(|e| TiSqlError::Internal(format!("Failed to serialize ilog record: {e}")))?;

        let checksum = crc32(&data);

        let mut writer = self.writer.lock().unwrap();

        // Write header: record_type (4) + length (4) + checksum (4)
        let record_type: u32 = 1; // Entry record
        writer.write_all(&record_type.to_le_bytes())?;
        writer.write_all(&(data.len() as u32).to_le_bytes())?;
        writer.write_all(&checksum.to_le_bytes())?;
        writer.write_all(&data)?;
        writer.flush()?;
        writer.get_ref().sync_data()?;

        self.records_since_checkpoint.fetch_add(1, Ordering::SeqCst);

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
        let mut records = Vec::new();

        loop {
            match Self::read_record(reader) {
                Ok(Some(record)) => records.push(record),
                Ok(None) => break,
                Err(e) => {
                    log_warn!("Stopping ilog recovery at corrupted record: {}", e);
                    break;
                }
            }
        }

        Ok(records)
    }

    fn read_record<R: Read>(reader: &mut R) -> Result<Option<IlogRecord>> {
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

        Ok(Some(record))
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

    #[test]
    fn test_ilog_open() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();

        let service = IlogService::open(config, lsn_provider).unwrap();
        service.sync().unwrap();
    }

    #[test]
    fn test_ilog_flush_intent_commit() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();

        let service = IlogService::open(config.clone(), Arc::clone(&lsn_provider)).unwrap();

        // Write flush intent
        let intent_lsn = service.write_flush_intent(1, 100, 50).unwrap();
        assert_eq!(intent_lsn, 1);

        // Write flush commit
        let meta = test_sst_meta(1, 0);
        let commit_lsn = service.write_flush_commit(meta, 50).unwrap();
        assert_eq!(commit_lsn, 2);

        service.sync().unwrap();

        // Recover and verify
        let lsn_provider2 = new_lsn_provider();
        let (_, version, orphans) = IlogService::recover(config, lsn_provider2).unwrap();

        assert!(orphans.is_empty(), "Should have no orphans");
        assert_eq!(version.level_size(0), 1);
        assert_eq!(version.flushed_lsn(), 50);
    }

    #[test]
    fn test_ilog_incomplete_flush() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();

        let service = IlogService::open(config.clone(), Arc::clone(&lsn_provider)).unwrap();

        // Write flush intent but no commit (simulating crash)
        service.write_flush_intent(1, 100, 50).unwrap();
        service.sync().unwrap();

        // Recover
        let lsn_provider2 = new_lsn_provider();
        let (_, version, orphans) = IlogService::recover(config, lsn_provider2).unwrap();

        assert!(orphans.contains(&1), "SST 1 should be orphan");
        assert_eq!(version.level_size(0), 0, "No SST should be in version");
    }

    #[test]
    fn test_ilog_compact_intent_commit() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();

        let service = IlogService::open(config.clone(), Arc::clone(&lsn_provider)).unwrap();

        // First add some SSTs via flush
        let meta1 = test_sst_meta(1, 0);
        let meta2 = test_sst_meta(2, 0);
        service.write_flush_intent(1, 100, 50).unwrap();
        service.write_flush_commit(meta1.clone(), 50).unwrap();
        service.write_flush_intent(2, 101, 100).unwrap();
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
        let (_, version, orphans) = IlogService::recover(config, lsn_provider2).unwrap();

        assert!(orphans.is_empty());
        assert_eq!(
            version.level_size(0),
            0,
            "L0 should be empty after compaction"
        );
        assert_eq!(version.level_size(1), 1, "L1 should have 1 SST");
    }

    #[test]
    fn test_ilog_incomplete_compact() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();

        let service = IlogService::open(config.clone(), Arc::clone(&lsn_provider)).unwrap();

        // Add SSTs
        let meta1 = test_sst_meta(1, 0);
        service.write_flush_intent(1, 100, 50).unwrap();
        service.write_flush_commit(meta1, 50).unwrap();

        // Start compact but don't commit
        service.write_compact_intent(vec![1], vec![2], 1).unwrap();
        service.sync().unwrap();

        // Recover
        let lsn_provider2 = new_lsn_provider();
        let (_, version, orphans) = IlogService::recover(config, lsn_provider2).unwrap();

        assert!(orphans.contains(&2), "Output SST 2 should be orphan");
        assert_eq!(version.level_size(0), 1, "Original SST should remain");
    }

    #[test]
    fn test_ilog_checkpoint() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();

        let service = IlogService::open(config.clone(), Arc::clone(&lsn_provider)).unwrap();

        // Add many flushes
        for i in 1..=10 {
            let meta = test_sst_meta(i, 0);
            service.write_flush_intent(i, 100 + i, i * 10).unwrap();
            service.write_flush_commit(meta, i * 10).unwrap();
        }

        // Write checkpoint
        let lsn_provider2 = new_lsn_provider();
        let (_, version, _) = IlogService::recover(config.clone(), lsn_provider2).unwrap();

        let lsn_provider3 = new_lsn_provider();
        let service2 = IlogService::open(config.clone(), lsn_provider3).unwrap();
        service2.write_checkpoint(&version).unwrap();
        service2.sync().unwrap();

        // Recover from checkpoint
        let lsn_provider4 = new_lsn_provider();
        let (_, recovered_version, orphans) = IlogService::recover(config, lsn_provider4).unwrap();

        assert!(orphans.is_empty());
        assert_eq!(recovered_version.level_size(0), 10);
        assert_eq!(recovered_version.flushed_lsn(), 100);
    }

    #[test]
    fn test_ilog_lsn_continuity() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();

        let service = IlogService::open(config.clone(), Arc::clone(&lsn_provider)).unwrap();

        // Write some records
        service.write_flush_intent(1, 100, 50).unwrap();
        let meta = test_sst_meta(1, 0);
        service.write_flush_commit(meta, 50).unwrap();
        service.sync().unwrap();

        // Recover
        let lsn_provider2 = new_lsn_provider();
        let (service2, _, _) = IlogService::recover(config.clone(), lsn_provider2.clone()).unwrap();

        // LSN should continue from where we left off
        assert!(lsn_provider2.current_lsn() >= 3);

        // New writes should have higher LSNs
        let new_lsn = service2.write_flush_intent(2, 101, 100).unwrap();
        assert!(new_lsn >= 3);
    }

    #[test]
    fn test_ilog_orphan_cleanup() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let lsn_provider = new_lsn_provider();

        // Create SST directory and orphan file
        fs::create_dir_all(&config.sst_dir).unwrap();
        let orphan_path = config.sst_dir.join("00000099.sst");
        fs::write(&orphan_path, b"orphan data").unwrap();
        assert!(orphan_path.exists());

        let service = IlogService::open(config.clone(), Arc::clone(&lsn_provider)).unwrap();

        // Add a valid SST
        let meta = test_sst_meta(1, 0);
        service.write_flush_intent(1, 100, 50).unwrap();
        service.write_flush_commit(meta, 50).unwrap();
        service.sync().unwrap();

        // Recover and get version
        let lsn_provider2 = new_lsn_provider();
        let (service2, version, _orphans) = IlogService::recover(config, lsn_provider2).unwrap();

        // Cleanup should remove orphan file
        let cleaned = service2
            .cleanup_orphan_ssts(&version, &HashSet::new())
            .unwrap();
        assert_eq!(cleaned, 1);
        assert!(!orphan_path.exists());
    }

    #[test]
    fn test_ilog_needs_checkpoint() {
        let tmp = TempDir::new().unwrap();
        let mut config = test_config(tmp.path());
        config.checkpoint_interval = 5;
        let lsn_provider = new_lsn_provider();

        let service = IlogService::open(config, Arc::clone(&lsn_provider)).unwrap();

        assert!(!service.needs_checkpoint());

        // Write 5 records
        for i in 1..=5 {
            service.write_flush_intent(i, 100 + i, i * 10).unwrap();
        }

        assert!(service.needs_checkpoint());
    }
}
