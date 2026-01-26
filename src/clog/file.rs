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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::error::{Result, TiSqlError};
use crate::types::Lsn;
use crate::{log_info, log_trace, log_warn};

use super::{ClogBatch, ClogEntry, ClogService};

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

/// File-based commit log implementation
pub struct FileClogService {
    config: FileClogConfig,
    /// Current LSN (next to assign)
    current_lsn: AtomicU64,
    /// Commit log file writer (protected by mutex for thread safety)
    writer: Mutex<BufWriter<File>>,
}

impl FileClogService {
    /// Open or create a new commit log file
    pub fn open(config: FileClogConfig) -> Result<Self> {
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

        let mut current_lsn = 1;

        if file_exists && file.metadata()?.len() > 0 {
            // Validate existing file header
            let mut reader = BufReader::new(&file);
            Self::validate_header(&mut reader)?;

            // Find max LSN from existing entries
            let entries = Self::read_entries(&mut reader)?;
            if let Some(last) = entries.last() {
                current_lsn = last.lsn + 1;
            }

            // Seek to end for appending
            drop(reader);
        } else {
            // Write header to new file
            let mut writer = BufWriter::new(&file);
            Self::write_header(&mut writer)?;
            writer.flush()?;
        }

        // Re-open for appending (seek to end)
        let file = OpenOptions::new().read(true).write(true).open(&clog_path)?;

        let mut file_for_write = file;
        file_for_write.seek(SeekFrom::End(0))?;

        log_info!(
            "Opened commit log file: {:?}, current_lsn={}",
            clog_path,
            current_lsn
        );

        Ok(Self {
            config,
            current_lsn: AtomicU64::new(current_lsn),
            writer: Mutex::new(BufWriter::new(file_for_write)),
        })
    }

    /// Recover entries from commit log file
    pub fn recover(config: FileClogConfig) -> Result<(Self, Vec<ClogEntry>)> {
        let clog_path = config.clog_path();

        if !clog_path.exists() {
            // No commit log file, start fresh
            let service = Self::open(config)?;
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
        let service = Self::open(config)?;

        Ok((service, entries))
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
}

impl ClogService for FileClogService {
    fn write(&self, batch: &mut ClogBatch, sync: bool) -> Result<Lsn> {
        if batch.is_empty() {
            return Ok(self.current_lsn.load(Ordering::SeqCst));
        }

        // Acquire lock BEFORE assigning LSNs to ensure writes are ordered
        // This prevents concurrent writers from producing out-of-order LSNs in the file
        let mut writer = self.writer.lock().unwrap();

        // Assign LSNs inside the lock
        let start_lsn = self.current_lsn.load(Ordering::SeqCst);
        for (i, entry) in batch.entries.iter_mut().enumerate() {
            entry.lsn = start_lsn + i as u64;
        }
        let end_lsn = start_lsn + batch.len() as u64 - 1;

        // Write to file
        Self::write_record(&mut *writer, batch.entries())?;
        writer.flush()?;

        // Update current_lsn AFTER successful write
        self.current_lsn.store(end_lsn + 1, Ordering::SeqCst);

        if sync {
            writer.get_ref().sync_data()?;
        }

        log_trace!(
            "Wrote {} entries to commit log, lsn={}-{}",
            batch.len(),
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
        self.current_lsn.load(Ordering::SeqCst)
    }

    fn close(&self) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.flush()?;
        writer.get_ref().sync_data()?;
        Ok(())
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
}
