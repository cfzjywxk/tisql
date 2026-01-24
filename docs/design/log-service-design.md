# LogService Design for TiSQL

## Overview

This document describes the design of the LogService component for TiSQL, providing persistence capabilities through a Write-Ahead Log (WAL). The design is informed by research on OceanBase's PALF (Parallel Architecture Log Framework) and TiKV's Raft-Engine.

## Research Summary

### OceanBase PALF

PALF (Parallel Architecture Log Framework) is OceanBase's replicated WAL system designed for distributed databases. Key insights from the source code analysis:

**Architecture Components:**
- **LogHandler**: Primary interface between transaction layer and log service
- **LogSlidingWindow**: In-memory window managing uncommitted logs with Paxos consensus
- **LogEngine**: Central coordinator for storage, network, and I/O
- **LogStorage**: Physical disk storage with block management
- **ApplyService**: Bridges log replication and transaction execution

**Key Design Patterns:**
- Paxos-based replication with proposal IDs
- Dual mode: APPEND (primary) and RAW_WRITE (standby)
- Log grouping for batched I/O efficiency
- Async I/O with callback chains
- Accumulated checksums for batch validation

**Entry Format:**
```
LogGroupEntryHeader (batch):
  - Magic ("GR"), Version, Group Size
  - Proposal ID, Committed End LSN
  - Max SCN, Accumulated Checksum
  - Log ID, Flags, Header Checksum

LogEntryHeader (individual):
  - Magic ("LH"), Version, Log Size
  - SCN, Data Checksum, Flags
```

### TiKV Raft-Engine

Raft-Engine is a log-structured embedded storage engine inspired by BitCask. Key insights:

**Architecture Components:**
- **Engine**: Central coordinator exposing public API
- **LogBatch**: Atomic batch write container
- **MemTables**: Per-Raft-Group in-memory indexing
- **FilePipeLog**: Dual-queue storage (Append + Rewrite queues)
- **PurgeManager**: Collaborative garbage collection

**Key Design Patterns:**
- Sequential append-only writes (no random I/O)
- Partitioned memtables with router for concurrency
- Write batching with queue leader pattern
- Lazy two-phase garbage collection
- LZ4 compression support

**Performance Benefits:**
- 25-40% less write I/O than RocksDB
- 20% lower tail latency
- Minimal write amplification

## Proposed Architecture

### Component Hierarchy

```
┌─────────────────────────────────────────────────────────────────┐
│                        SQLEngine                                 │
│  (Database struct: parse → bind → plan → execute)               │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                     PartitionService                             │
│  (Logical partition management, key routing)                     │
│  • Map keys to partitions                                        │
│  • Future: distributed partition placement                       │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                   TransactionService                             │
│  (MVCC, isolation levels, commit/rollback)                       │
│  • Transaction lifecycle management                              │
│  • Conflict detection                                            │
│  • Visibility control                                            │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                      LogService                                  │
│  (Write-Ahead Log for durability)                                │
│  • Append log entries                                            │
│  • Sync to disk                                                  │
│  • Recovery replay                                               │
│  • Log compaction/GC                                             │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                     StateEngine                                  │
│  (Materialized state storage)                                    │
│  • Current: MemTableEngine (in-memory)                           │
│  • Future: LSM-based persistent storage                          │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Write Path:
  Client → SQLEngine → PartitionService → TransactionService
       → LogService.append() → LogService.sync()
       → StateEngine.apply() → Return to Client

Read Path:
  Client → SQLEngine → PartitionService → TransactionService
       → StateEngine.get() (with MVCC visibility check)
       → Return to Client

Recovery Path:
  Startup → LogService.recover()
       → Replay entries to StateEngine
       → Ready for queries
```

## LogService Design

### Core Traits

```rust
// src/log/mod.rs (enhanced)

/// Log Sequence Number - monotonically increasing position
pub type Lsn = u64;

/// Log entry for WAL
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub lsn: Lsn,
    pub txn_id: TxnId,
    pub partition_id: PartitionId,  // NEW: partition affinity
    pub op: LogOp,
    pub checksum: u32,              // NEW: CRC32 for integrity
}

/// Log operation types
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LogOp {
    Put { key: Key, value: RawValue },
    Delete { key: Key },
    Commit { commit_ts: Timestamp },
    Rollback,
    // Future: schema changes, partition operations
}

/// Batch of log entries for atomic append
pub struct LogBatch {
    entries: Vec<LogEntry>,
    sync_required: bool,
}

/// Callback for async append completion
pub trait AppendCallback: Send + 'static {
    fn on_success(&self, lsn: Lsn);
    fn on_failure(&self, error: TiSqlError);
}

/// Write-ahead log interface
pub trait WriteAheadLog: Send + Sync {
    /// Append single entry, returns LSN
    fn append(&self, entry: LogEntry) -> Result<Lsn>;

    /// Append batch atomically, returns last LSN
    fn append_batch(&self, batch: LogBatch) -> Result<Lsn>;

    /// Async append with callback (for high throughput)
    fn append_async(&self, batch: LogBatch, callback: Box<dyn AppendCallback>);

    /// Sync to disk up to LSN (durability guarantee)
    fn sync(&self, lsn: Lsn) -> Result<()>;

    /// Read entries from LSN (for recovery)
    fn read_from(&self, lsn: Lsn) -> Result<LogIterator>;

    /// Get current append position
    fn current_lsn(&self) -> Lsn;

    /// Get last synced position
    fn synced_lsn(&self) -> Lsn;

    /// Truncate log before LSN (after checkpoint)
    fn truncate_before(&self, lsn: Lsn) -> Result<()>;

    /// Close and flush all pending writes
    fn close(&self) -> Result<()>;
}
```

### Implementation: FileLogService

```rust
// src/log/file.rs

/// Configuration for file-based log service
pub struct FileLogConfig {
    /// Directory for log files
    pub log_dir: PathBuf,

    /// Maximum size per log segment (default: 64MB)
    pub segment_size: usize,

    /// Sync mode
    pub sync_mode: SyncMode,

    /// Write buffer size (default: 4MB)
    pub buffer_size: usize,

    /// Enable compression (LZ4)
    pub compression: bool,
}

pub enum SyncMode {
    /// Sync after every commit
    EveryCommit,
    /// Sync at interval (e.g., every 100ms)
    Periodic(Duration),
    /// Sync when buffer full
    OnBufferFull,
    /// No sync (fastest, least durable)
    None,
}

/// File-based WAL implementation
pub struct FileLogService {
    config: FileLogConfig,

    // Current state
    current_lsn: AtomicU64,
    synced_lsn: AtomicU64,

    // Active segment
    active_segment: Mutex<LogSegment>,

    // Write buffer for batching
    write_buffer: Mutex<WriteBuffer>,

    // Background sync worker
    sync_worker: Option<JoinHandle<()>>,
    sync_notify: Condvar,

    // Segment management
    segments: RwLock<Vec<SegmentMeta>>,
}

/// Individual log segment file
struct LogSegment {
    id: u64,
    file: File,
    size: usize,
    start_lsn: Lsn,
    end_lsn: Lsn,
}

/// Metadata for segment discovery
#[derive(Serialize, Deserialize)]
struct SegmentMeta {
    id: u64,
    filename: String,
    start_lsn: Lsn,
    end_lsn: Lsn,
    checksum: u32,
}
```

### On-Disk Format

```
Log Directory Structure:
  data/log/
  ├── manifest.json       # Segment metadata
  ├── segment_000001.log  # Log segments
  ├── segment_000002.log
  └── ...

Segment File Format:
  ┌─────────────────────────────────────────┐
  │ Segment Header (64 bytes)               │
  │  - Magic: "TSQL" (4 bytes)              │
  │  - Version: u32                         │
  │  - Segment ID: u64                      │
  │  - Start LSN: u64                       │
  │  - Created At: u64 (timestamp)          │
  │  - Reserved: 32 bytes                   │
  └─────────────────────────────────────────┘
  ┌─────────────────────────────────────────┐
  │ Entry Block (variable size)             │
  │  ┌───────────────────────────────────┐  │
  │  │ Block Header (16 bytes)           │  │
  │  │  - Entry Count: u32               │  │
  │  │  - Block Size: u32                │  │
  │  │  - Compression: u8                │  │
  │  │  - Checksum: u32                  │  │
  │  │  - Reserved: 3 bytes              │  │
  │  └───────────────────────────────────┘  │
  │  ┌───────────────────────────────────┐  │
  │  │ Entry 1                           │  │
  │  │  - LSN: u64                       │  │
  │  │  - TxnId: u64                     │  │
  │  │  - PartitionId: u64               │  │
  │  │  - OpType: u8                     │  │
  │  │  - KeyLen: u32, Key: bytes        │  │
  │  │  - ValueLen: u32, Value: bytes    │  │
  │  │  - Checksum: u32                  │  │
  │  └───────────────────────────────────┘  │
  │  │ Entry 2...N                       │  │
  └─────────────────────────────────────────┘
  │ More Entry Blocks...                    │
  └─────────────────────────────────────────┘
```

### Write Path Implementation

```rust
impl WriteAheadLog for FileLogService {
    fn append_batch(&self, mut batch: LogBatch) -> Result<Lsn> {
        // 1. Assign LSNs
        let start_lsn = self.current_lsn.fetch_add(
            batch.entries.len() as u64,
            Ordering::SeqCst
        );

        for (i, entry) in batch.entries.iter_mut().enumerate() {
            entry.lsn = start_lsn + i as u64;
            entry.checksum = crc32(&entry);
        }

        let end_lsn = start_lsn + batch.entries.len() as u64 - 1;

        // 2. Serialize and optionally compress
        let data = if self.config.compression {
            lz4_compress(&serialize(&batch.entries)?)
        } else {
            serialize(&batch.entries)?
        };

        // 3. Write to buffer
        {
            let mut buffer = self.write_buffer.lock();
            buffer.append(&data, end_lsn)?;

            // Flush if buffer full or sync required
            if buffer.should_flush() || batch.sync_required {
                self.flush_buffer(&mut buffer)?;
            }
        }

        // 4. Sync if required
        if batch.sync_required {
            self.sync(end_lsn)?;
        }

        Ok(end_lsn)
    }

    fn sync(&self, lsn: Lsn) -> Result<()> {
        // Ensure buffer is flushed
        {
            let mut buffer = self.write_buffer.lock();
            if buffer.max_lsn() >= lsn {
                self.flush_buffer(&mut buffer)?;
            }
        }

        // fsync the segment file
        {
            let segment = self.active_segment.lock();
            segment.file.sync_data()?;
        }

        // Update synced LSN
        self.synced_lsn.store(lsn, Ordering::SeqCst);

        Ok(())
    }
}
```

### Recovery Implementation

```rust
impl FileLogService {
    /// Recover from log directory
    pub fn recover(config: FileLogConfig) -> Result<(Self, Vec<LogEntry>)> {
        let log_dir = &config.log_dir;

        // 1. Load manifest
        let manifest_path = log_dir.join("manifest.json");
        let segments: Vec<SegmentMeta> = if manifest_path.exists() {
            serde_json::from_reader(File::open(&manifest_path)?)?
        } else {
            vec![]
        };

        // 2. Scan and validate segments
        let mut entries = Vec::new();
        let mut max_lsn = 0;

        for segment_meta in &segments {
            let segment_path = log_dir.join(&segment_meta.filename);
            let segment_entries = Self::read_segment(&segment_path)?;

            for entry in segment_entries {
                // Validate checksum
                if crc32(&entry) != entry.checksum {
                    return Err(TiSqlError::CorruptedLog(entry.lsn));
                }
                max_lsn = max_lsn.max(entry.lsn);
                entries.push(entry);
            }
        }

        // 3. Initialize service at recovered position
        let service = Self::new_at(config, max_lsn + 1, segments)?;

        Ok((service, entries))
    }

    fn read_segment(path: &Path) -> Result<Vec<LogEntry>> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read and validate header
        let header = SegmentHeader::read_from(&mut reader)?;
        if header.magic != SEGMENT_MAGIC {
            return Err(TiSqlError::InvalidSegment);
        }

        // Read entry blocks until EOF
        let mut entries = Vec::new();
        loop {
            match BlockHeader::read_from(&mut reader) {
                Ok(block_header) => {
                    let block_data = read_bytes(&mut reader, block_header.size)?;

                    // Validate block checksum
                    if crc32(&block_data) != block_header.checksum {
                        break; // Truncated write, stop here
                    }

                    // Decompress if needed
                    let data = if block_header.compression > 0 {
                        lz4_decompress(&block_data)?
                    } else {
                        block_data
                    };

                    // Deserialize entries
                    let block_entries: Vec<LogEntry> = deserialize(&data)?;
                    entries.extend(block_entries);
                }
                Err(_) => break, // EOF
            }
        }

        Ok(entries)
    }
}
```

## Integration with TransactionService

### Transaction Commit Flow

```rust
impl TransactionService {
    pub fn commit(&self, txn: &mut Transaction) -> Result<Timestamp> {
        // 1. Get commit timestamp
        let commit_ts = self.timestamp_oracle.get_ts();

        // 2. Build log batch from transaction writes
        let mut batch = LogBatch::new();
        for write in txn.writes() {
            batch.add(LogEntry {
                lsn: 0, // Assigned by LogService
                txn_id: txn.id(),
                partition_id: self.partition_service.get_partition(&write.key),
                op: write.op.clone(),
                checksum: 0,
            });
        }

        // 3. Add commit record
        batch.add(LogEntry {
            lsn: 0,
            txn_id: txn.id(),
            partition_id: 0, // Global
            op: LogOp::Commit { commit_ts },
            checksum: 0,
        });
        batch.set_sync_required(true);

        // 4. Append to log (durability)
        let commit_lsn = self.log_service.append_batch(batch)?;

        // 5. Apply to state engine
        for write in txn.writes() {
            match &write.op {
                LogOp::Put { key, value } => {
                    self.state_engine.put_versioned(key, value, commit_ts)?;
                }
                LogOp::Delete { key } => {
                    self.state_engine.delete_versioned(key, commit_ts)?;
                }
                _ => {}
            }
        }

        // 6. Mark transaction committed
        txn.set_committed(commit_ts, commit_lsn);

        Ok(commit_ts)
    }
}
```

### Startup and Recovery

```rust
impl Database {
    pub fn open(config: DatabaseConfig) -> Result<Self> {
        // 1. Recover LogService and get entries
        let (log_service, entries) = FileLogService::recover(config.log)?;

        // 2. Create state engine
        let state_engine = MemTableEngine::new();

        // 3. Replay log entries
        let mut committed_txns = HashSet::new();
        let mut pending_writes: HashMap<TxnId, Vec<LogEntry>> = HashMap::new();

        // First pass: identify committed transactions
        for entry in &entries {
            if let LogOp::Commit { .. } = entry.op {
                committed_txns.insert(entry.txn_id);
            }
        }

        // Second pass: apply committed writes
        for entry in entries {
            if committed_txns.contains(&entry.txn_id) {
                match entry.op {
                    LogOp::Put { key, value } => {
                        state_engine.put(&key, &value)?;
                    }
                    LogOp::Delete { key } => {
                        state_engine.delete(&key)?;
                    }
                    _ => {}
                }
            }
            // Uncommitted transactions are automatically rolled back
        }

        // 4. Build catalog from state
        let catalog = Self::rebuild_catalog(&state_engine)?;

        // 5. Create services
        let partition_service = PartitionService::new();
        let transaction_service = TransactionService::new(
            Arc::new(log_service),
            Arc::new(state_engine),
        );

        Ok(Self {
            catalog,
            partition_service,
            transaction_service,
            parser: Parser::new(),
            executor: SimpleExecutor::new(),
        })
    }
}
```

## Garbage Collection and Compaction

### Checkpoint and Truncation

```rust
impl LogService {
    /// Create checkpoint - safe truncation point
    pub fn checkpoint(&self) -> Result<Lsn> {
        // 1. Get current state snapshot LSN
        let snapshot_lsn = self.synced_lsn();

        // 2. Ensure all pending transactions are resolved
        // (committed or rolled back)

        // 3. Record checkpoint in manifest
        self.record_checkpoint(snapshot_lsn)?;

        Ok(snapshot_lsn)
    }

    /// Truncate log entries before checkpoint
    pub fn truncate_before(&self, lsn: Lsn) -> Result<()> {
        let mut segments = self.segments.write();

        // Find segments fully before LSN
        let to_remove: Vec<_> = segments
            .iter()
            .filter(|s| s.end_lsn < lsn)
            .map(|s| s.filename.clone())
            .collect();

        // Remove segment files
        for filename in &to_remove {
            let path = self.config.log_dir.join(filename);
            std::fs::remove_file(path)?;
        }

        // Update manifest
        segments.retain(|s| s.end_lsn >= lsn);
        self.save_manifest(&segments)?;

        Ok(())
    }
}
```

## Module Structure

```
src/
├── log/
│   ├── mod.rs           # Traits and types
│   ├── entry.rs         # LogEntry, LogOp, serialization
│   ├── batch.rs         # LogBatch implementation
│   ├── file.rs          # FileLogService implementation
│   ├── segment.rs       # Segment file management
│   ├── buffer.rs        # Write buffer
│   ├── recovery.rs      # Recovery logic
│   └── checkpoint.rs    # Checkpoint and GC
├── partition/
│   ├── mod.rs           # PartitionService trait
│   └── simple.rs        # Single-partition impl
├── transaction/
│   ├── mod.rs           # Transaction trait (existing)
│   ├── service.rs       # TransactionService
│   └── mvcc.rs          # MVCC implementation
└── ...
```

## Configuration

```rust
/// Database configuration
pub struct DatabaseConfig {
    /// Data directory
    pub data_dir: PathBuf,

    /// Log service configuration
    pub log: FileLogConfig,

    /// State engine type
    pub state_engine: StateEngineType,
}

impl Default for FileLogConfig {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from("data/log"),
            segment_size: 64 * 1024 * 1024, // 64MB
            sync_mode: SyncMode::EveryCommit,
            buffer_size: 4 * 1024 * 1024,   // 4MB
            compression: true,
        }
    }
}
```

## Implementation Phases

### Phase 1: Basic WAL
- [ ] Implement `FileLogService` with single segment
- [ ] Entry serialization with checksums
- [ ] Sync to disk
- [ ] Basic recovery (replay all entries)

### Phase 2: Segment Management
- [ ] Multiple segment files
- [ ] Segment rotation on size limit
- [ ] Manifest for segment tracking
- [ ] Recovery across segments

### Phase 3: Performance Optimization
- [ ] Write buffering
- [ ] LZ4 compression
- [ ] Async append with callbacks
- [ ] Batch group commit

### Phase 4: Integration
- [ ] Integrate with TransactionService
- [ ] MVCC visibility with log LSNs
- [ ] Checkpoint and truncation
- [ ] Graceful shutdown

### Phase 5: Advanced Features
- [ ] PartitionService for key routing
- [ ] Per-partition log streams (future)
- [ ] Distributed replication (future)

## References

1. **OceanBase PALF**: VLDB 2024 paper "PALF: Replicated Write-Ahead Logging for Distributed Databases"
2. **TiKV Raft-Engine**: https://github.com/tikv/raft-engine
3. **BitCask**: https://riak.com/assets/bitcask-intro.pdf
4. **TiDB Transaction Model**: https://docs.pingcap.com/tidb/stable/transaction-overview

## License

This project is licensed under the Apache License, Version 2.0. See [LICENSE](../../LICENSE) for details.
