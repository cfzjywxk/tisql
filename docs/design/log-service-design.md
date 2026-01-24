# LogService Design for TiSQL

## Overview

This document describes the design of the LogService component for TiSQL, providing persistence capabilities through a Write-Ahead Log (WAL). The design is based on TiKV's Raft-Engine patterns for local persistence, with raft-rs for future distributed replication.

## Design Principles

1. **Simple sequential log** - No complex sliding window or out-of-order commit
2. **Raft-Engine inspired persistence** - BitCask-style append-only log files
3. **Raft-rs for replication** - Standard Raft consensus when distributed
4. **Parallel apply ready** - Leave hooks for future parallel apply optimization

## Reference Implementations

### TiKV Raft-Engine

[Raft-Engine](https://github.com/tikv/raft-engine) is a log-structured embedded storage engine inspired by BitCask.

**Key Components:**
- **Engine**: Central coordinator exposing public API
- **LogBatch**: Atomic batch write container
- **MemTables**: Per-Raft-Group in-memory indexing (entry locations)
- **FilePipeLog**: Dual-queue storage (Append + Rewrite queues)
- **PurgeManager**: Lazy two-phase garbage collection

**Key Design Patterns:**
- Sequential append-only writes (no random I/O)
- Write batching with queue leader pattern
- Shared log stream across multiple Raft Groups
- Feedback-driven compaction (engine reports blocking entries)
- LZ4 compression support

**Why Raft-Engine over RocksDB:**
- 25-40% less write I/O (no LSM compaction)
- 20% lower tail latency
- Minimal write amplification
- No unnecessary key-sorting overhead

### TiKV Raft-rs

[Raft-rs](https://github.com/tikv/raft-rs) is the Raft consensus implementation used by TiKV.

**Integration Pattern:**
- `RawNode<S>` with Storage trait for log persistence
- `Unstable` for entries not yet persisted
- `Storage` trait for persisted entries

### Parallel Apply (Future)

Reference: [TiDB Hackathon 2022 - Parallel Apply](https://tanxinyu.work/2022-tidb-hackathon/)

**Key Insight:** Non-conflicting logs can be applied in parallel without dependency detection on the Leader side, because their key ranges must not overlap due to transaction semantics.

**Design Hooks for Future:**
- `region_id`/`partition_id` in log entries for routing
- Apply callback interface for parallel execution
- Term-based routing for consistency during leadership transitions

## Architecture

### Component Hierarchy

```
┌─────────────────────────────────────────────────────────────────┐
│                        SQLEngine                                 │
│  (Database struct: parse → bind → plan → execute)               │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                     PartitionService                             │
│  • Key-to-partition routing                                      │
│  • Future: distributed partition placement                       │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                   TransactionService                             │
│  • MVCC, isolation levels                                        │
│  • Conflict detection, visibility control                        │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                      LogService                                  │
│  • Append log entries (LogBatch)                                 │
│  • Sync to disk (fsync)                                          │
│  • Recovery replay                                               │
│  • Garbage collection                                            │
│  [Future: raft-rs integration for replication]                   │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                     StateEngine                                  │
│  • Current: MemTableEngine (in-memory)                           │
│  • Future: LSM-based persistent storage                          │
│  [Future: parallel apply from LogService]                        │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Write Path (Single Node):
  Client → SQLEngine → PartitionService → TransactionService
       → LogService.append_batch() → LogService.sync()
       → StateEngine.apply() → Return to Client

Write Path (Future Distributed):
  Client → SQLEngine → PartitionService → TransactionService
       → LogService.propose() → Raft consensus (raft-rs)
       → LogService.append() on majority → Commit
       → StateEngine.apply() → Return to Client

Read Path:
  Client → SQLEngine → PartitionService → TransactionService
       → StateEngine.get() (with MVCC visibility)
       → Return to Client

Recovery Path:
  Startup → LogService.recover()
       → Replay committed entries to StateEngine
       → Ready for queries
```

## LogService Design

### Core Types

```rust
// src/log/mod.rs

/// Log Sequence Number - monotonically increasing position
pub type Lsn = u64;

/// Index within a Raft log (for future raft-rs integration)
pub type RaftIndex = u64;

/// Log entry for WAL
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    /// Log sequence number (file position)
    pub lsn: Lsn,

    /// Transaction that made this change
    pub txn_id: TxnId,

    /// Partition/Region for routing (enables parallel apply)
    pub region_id: u64,

    /// The operation
    pub op: LogOp,
}

/// Log operation types
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LogOp {
    Put { key: Key, value: RawValue },
    Delete { key: Key },
    Commit { commit_ts: Timestamp },
    Rollback,
}

/// Batch of log entries for atomic append (like raft-engine's LogBatch)
pub struct LogBatch {
    entries: Vec<LogEntry>,
    compression: CompressionType,
}

impl LogBatch {
    pub fn new() -> Self { ... }

    pub fn add_entry(&mut self, region_id: u64, txn_id: TxnId, op: LogOp) { ... }

    pub fn add_put(&mut self, region_id: u64, txn_id: TxnId, key: Key, value: RawValue) {
        self.add_entry(region_id, txn_id, LogOp::Put { key, value });
    }

    pub fn add_delete(&mut self, region_id: u64, txn_id: TxnId, key: Key) {
        self.add_entry(region_id, txn_id, LogOp::Delete { key });
    }

    pub fn add_commit(&mut self, region_id: u64, txn_id: TxnId, commit_ts: Timestamp) {
        self.add_entry(region_id, txn_id, LogOp::Commit { commit_ts });
    }
}
```

### LogService Trait

```rust
/// Write-ahead log interface
pub trait LogService: Send + Sync {
    /// Append batch atomically, returns last LSN
    /// Similar to raft-engine's Engine::write()
    fn write(&self, batch: &mut LogBatch, sync: bool) -> Result<Lsn>;

    /// Read entries for a region starting from index
    /// Similar to raft-engine's fetch_entries_to()
    fn fetch_entries(&self, region_id: u64, from_lsn: Lsn) -> Result<Vec<LogEntry>>;

    /// Get first LSN for a region
    fn first_lsn(&self, region_id: u64) -> Option<Lsn>;

    /// Get last LSN for a region
    fn last_lsn(&self, region_id: u64) -> Option<Lsn>;

    /// Compact entries before LSN for a region
    /// Similar to raft-engine's compact_to()
    fn compact_to(&self, region_id: u64, lsn: Lsn) -> Result<()>;

    /// Purge old log files, returns regions with blocking entries
    /// Similar to raft-engine's purge_expired_files()
    fn purge_expired_files(&self) -> Result<Vec<u64>>;

    /// Sync all pending writes to disk
    fn sync(&self) -> Result<()>;
}
```

### FileLogService Implementation

Following raft-engine's design patterns:

```rust
// src/log/file.rs

pub struct FileLogConfig {
    /// Directory for log files
    pub log_dir: PathBuf,

    /// Target size per log file (default: 128MB)
    pub target_file_size: usize,

    /// Compression type
    pub compression: CompressionType,

    /// Purge threshold - trigger GC when this much space is reclaimable
    pub purge_threshold: usize,
}

/// File-based log service (raft-engine inspired)
pub struct FileLogService {
    config: FileLogConfig,

    /// Per-region memtables indexing entry locations
    memtables: RwLock<HashMap<u64, MemTable>>,

    /// Active log file for appends
    pipe_log: Mutex<PipeLog>,

    /// Global stats
    stats: LogStats,
}

/// In-memory index for a region's entries
struct MemTable {
    /// Entry LSN -> file location
    entries: BTreeMap<Lsn, FileLocation>,

    /// First and last LSN
    first_lsn: Lsn,
    last_lsn: Lsn,
}

/// Location of an entry in log files
#[derive(Clone, Copy)]
struct FileLocation {
    file_id: u64,
    offset: u64,
    len: u32,
}

/// Manages append-only log files
struct PipeLog {
    /// Active file for writing
    active_file: LogFile,

    /// All log files (id -> metadata)
    files: BTreeMap<u64, LogFileMeta>,

    /// Next file ID
    next_file_id: u64,
}

struct LogFile {
    id: u64,
    file: File,
    written: usize,
}

struct LogFileMeta {
    id: u64,
    path: PathBuf,
    first_lsn: Lsn,
    last_lsn: Lsn,
    size: usize,
}
```

### Write Path

```rust
impl LogService for FileLogService {
    fn write(&self, batch: &mut LogBatch, sync: bool) -> Result<Lsn> {
        // 1. Serialize batch (optionally compress)
        let data = batch.serialize(self.config.compression)?;

        // 2. Write to pipe log (may rotate file)
        let locations = {
            let mut pipe = self.pipe_log.lock();
            pipe.append(&data, &batch.entries)?
        };

        // 3. Update memtables
        {
            let mut memtables = self.memtables.write();
            for (entry, location) in batch.entries.iter().zip(locations) {
                let table = memtables
                    .entry(entry.region_id)
                    .or_insert_with(MemTable::new);
                table.insert(entry.lsn, location);
            }
        }

        // 4. Sync if requested
        if sync {
            self.sync()?;
        }

        Ok(batch.entries.last().map(|e| e.lsn).unwrap_or(0))
    }
}
```

### On-Disk Format

Similar to raft-engine's format:

```
Log Directory:
  data/log/
  ├── 000001.raftlog    # Log files with sequential IDs
  ├── 000002.raftlog
  ├── 000003.raftlog
  └── ...

Log File Format:
  ┌─────────────────────────────────────────┐
  │ File Header (32 bytes)                  │
  │  - Magic: "TLOG" (4 bytes)              │
  │  - Version: u32                         │
  │  - File ID: u64                         │
  │  - Created: u64 (timestamp)             │
  │  - Reserved: 8 bytes                    │
  └─────────────────────────────────────────┘
  ┌─────────────────────────────────────────┐
  │ Record 1                                │
  │  ┌───────────────────────────────────┐  │
  │  │ Record Header (12 bytes)          │  │
  │  │  - Type: u8 (Entries/Compact/etc) │  │
  │  │  - Compression: u8                │  │
  │  │  - Reserved: u16                  │  │
  │  │  - Length: u32                    │  │
  │  │  - Checksum: u32 (CRC32)          │  │
  │  └───────────────────────────────────┘  │
  │  ┌───────────────────────────────────┐  │
  │  │ Record Data (variable)            │  │
  │  │  [Serialized LogBatch entries]    │  │
  │  └───────────────────────────────────┘  │
  └─────────────────────────────────────────┘
  │ Record 2...N                            │
  └─────────────────────────────────────────┘
```

### Recovery

```rust
impl FileLogService {
    pub fn open(config: FileLogConfig) -> Result<Self> {
        let log_dir = &config.log_dir;
        std::fs::create_dir_all(log_dir)?;

        // 1. Discover log files
        let mut files: Vec<_> = std::fs::read_dir(log_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension() == Some("raftlog".as_ref()))
            .collect();
        files.sort_by_key(|e| e.path());

        // 2. Replay each file to rebuild memtables
        let mut memtables = HashMap::new();
        let mut file_metas = BTreeMap::new();

        for entry in files {
            let path = entry.path();
            let (file_id, entries, meta) = Self::replay_file(&path)?;

            for (lsn, region_id, location) in entries {
                let table = memtables
                    .entry(region_id)
                    .or_insert_with(MemTable::new);
                table.insert(lsn, location);
            }

            file_metas.insert(file_id, meta);
        }

        // 3. Open or create active file
        let next_file_id = file_metas.keys().last().map(|id| id + 1).unwrap_or(1);
        let active_file = LogFile::create(log_dir.join(format!("{:06}.raftlog", next_file_id)))?;

        Ok(Self {
            config,
            memtables: RwLock::new(memtables),
            pipe_log: Mutex::new(PipeLog {
                active_file,
                files: file_metas,
                next_file_id: next_file_id + 1,
            }),
            stats: LogStats::default(),
        })
    }

    fn replay_file(path: &Path) -> Result<(u64, Vec<(Lsn, u64, FileLocation)>, LogFileMeta)> {
        let mut file = File::open(path)?;
        let header = FileHeader::read_from(&mut file)?;

        let mut entries = Vec::new();
        let mut offset = FILE_HEADER_SIZE as u64;

        loop {
            match RecordHeader::read_from(&mut file) {
                Ok(record_header) => {
                    // Validate checksum
                    let data = read_bytes(&mut file, record_header.length as usize)?;
                    if crc32(&data) != record_header.checksum {
                        break; // Corrupted, stop here
                    }

                    // Decompress and parse
                    let batch_entries = LogBatch::deserialize(&data, record_header.compression)?;

                    for (i, entry) in batch_entries.iter().enumerate() {
                        let location = FileLocation {
                            file_id: header.file_id,
                            offset: offset + RECORD_HEADER_SIZE as u64,
                            len: record_header.length,
                        };
                        entries.push((entry.lsn, entry.region_id, location));
                    }

                    offset += RECORD_HEADER_SIZE as u64 + record_header.length as u64;
                }
                Err(_) => break, // EOF
            }
        }

        let meta = LogFileMeta {
            id: header.file_id,
            path: path.to_path_buf(),
            first_lsn: entries.first().map(|(lsn, _, _)| *lsn).unwrap_or(0),
            last_lsn: entries.last().map(|(lsn, _, _)| *lsn).unwrap_or(0),
            size: offset as usize,
        };

        Ok((header.file_id, entries, meta))
    }
}
```

### Garbage Collection

Following raft-engine's lazy two-phase approach:

```rust
impl LogService for FileLogService {
    fn compact_to(&self, region_id: u64, lsn: Lsn) -> Result<()> {
        // Logical compaction: remove entries from memtable
        let mut memtables = self.memtables.write();
        if let Some(table) = memtables.get_mut(&region_id) {
            table.entries.retain(|&entry_lsn, _| entry_lsn >= lsn);
            if let Some((&first, _)) = table.entries.first_key_value() {
                table.first_lsn = first;
            }
        }
        Ok(())
    }

    fn purge_expired_files(&self) -> Result<Vec<u64>> {
        // Find files where all entries have been compacted
        let memtables = self.memtables.read();
        let mut pipe = self.pipe_log.lock();

        // Find minimum LSN per file still referenced
        let mut file_min_lsn: HashMap<u64, Lsn> = HashMap::new();
        for table in memtables.values() {
            for location in table.entries.values() {
                let min = file_min_lsn.entry(location.file_id).or_insert(Lsn::MAX);
                // Track that this file still has live entries
                *min = (*min).min(location.file_id);
            }
        }

        // Delete files with no live entries (except active file)
        let active_id = pipe.active_file.id;
        let to_delete: Vec<_> = pipe.files
            .iter()
            .filter(|(id, _)| **id != active_id && !file_min_lsn.contains_key(id))
            .map(|(id, meta)| (*id, meta.path.clone()))
            .collect();

        for (id, path) in to_delete {
            std::fs::remove_file(&path)?;
            pipe.files.remove(&id);
        }

        // Return regions that are blocking GC (have old entries)
        let blocking_regions: Vec<u64> = memtables
            .iter()
            .filter(|(_, table)| {
                table.entries.values().any(|loc| {
                    pipe.files.get(&loc.file_id)
                        .map(|f| f.size > self.config.purge_threshold)
                        .unwrap_or(false)
                })
            })
            .map(|(region_id, _)| *region_id)
            .collect();

        Ok(blocking_regions)
    }
}
```

## Future: Raft-rs Integration

When distributed replication is needed, integrate with raft-rs:

```rust
// Future: src/log/raft.rs

use raft::{RawNode, Storage, Config};
use raft::eraftpb::{Entry, Snapshot, HardState, ConfState};

/// Raft storage backed by FileLogService
pub struct RaftLogStorage {
    region_id: u64,
    log_service: Arc<FileLogService>,

    /// Raft hard state (term, vote, commit)
    hard_state: HardState,

    /// Raft conf state (voters, learners)
    conf_state: ConfState,
}

impl Storage for RaftLogStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState {
            hard_state: self.hard_state.clone(),
            conf_state: self.conf_state.clone(),
        })
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> raft::Result<Vec<Entry>> {
        // Fetch from log service
        let entries = self.log_service.fetch_entries(self.region_id, low)?;
        // Convert LogEntry to raft Entry
        ...
    }

    fn term(&self, idx: u64) -> raft::Result<u64> { ... }
    fn first_index(&self) -> raft::Result<u64> { ... }
    fn last_index(&self) -> raft::Result<u64> { ... }
    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> { ... }
}

/// Raft node for a partition/region
pub struct RaftNode {
    raw_node: RawNode<RaftLogStorage>,
    log_service: Arc<FileLogService>,
    apply_sender: Sender<ApplyTask>,  // For parallel apply
}

impl RaftNode {
    /// Propose a write to the Raft group
    pub fn propose(&mut self, data: Vec<u8>) -> Result<()> {
        self.raw_node.propose(vec![], data)?;
        Ok(())
    }

    /// Drive Raft state machine
    pub fn tick(&mut self) {
        self.raw_node.tick();
    }

    /// Process ready state
    pub fn handle_ready(&mut self) -> Result<()> {
        if !self.raw_node.has_ready() {
            return Ok(());
        }

        let mut ready = self.raw_node.ready();

        // 1. Persist entries and hard state
        if !ready.entries().is_empty() {
            let mut batch = LogBatch::new();
            for entry in ready.entries() {
                // Convert raft Entry to LogEntry
                batch.add_raft_entry(self.region_id, entry);
            }
            self.log_service.write(&mut batch, true)?;
        }

        // 2. Send messages to peers
        for msg in ready.take_messages() {
            self.send_message(msg)?;
        }

        // 3. Apply committed entries
        for entry in ready.take_committed_entries() {
            self.apply_sender.send(ApplyTask {
                region_id: self.region_id,
                entry,
            })?;
        }

        // 4. Advance Raft
        let mut light_rd = self.raw_node.advance(ready);
        // Handle light ready...

        Ok(())
    }
}
```

## Future: Parallel Apply

Leave hooks for parallel apply optimization:

```rust
// Future: src/apply/mod.rs

/// Apply task for a committed entry
pub struct ApplyTask {
    pub region_id: u64,
    pub entry: LogEntry,
    pub callback: Option<Box<dyn ApplyCallback>>,
}

/// Parallel apply pool
pub struct ApplyPool {
    /// Per-region apply workers
    workers: HashMap<u64, ApplyWorker>,

    /// State engine for applying changes
    state_engine: Arc<dyn StateEngine>,
}

impl ApplyPool {
    /// Submit task for parallel apply
    /// Non-conflicting regions can be applied concurrently
    pub fn submit(&self, task: ApplyTask) {
        let worker = self.workers
            .entry(task.region_id)
            .or_insert_with(|| ApplyWorker::new(task.region_id));
        worker.submit(task);
    }
}

/// Worker for a specific region (maintains ordering within region)
struct ApplyWorker {
    region_id: u64,
    pending: VecDeque<ApplyTask>,
    applied_lsn: AtomicU64,
}
```

## Module Structure

```
src/
├── log/
│   ├── mod.rs           # LogService trait, LogEntry, LogBatch
│   ├── file.rs          # FileLogService implementation
│   ├── memtable.rs      # In-memory entry index
│   ├── pipe.rs          # PipeLog - file management
│   └── format.rs        # On-disk format, serialization
├── partition/
│   ├── mod.rs           # PartitionService trait
│   └── single.rs        # Single-partition impl (initial)
├── transaction/
│   ├── mod.rs           # Transaction trait (existing)
│   ├── service.rs       # TransactionService
│   └── mvcc.rs          # MVCC implementation
└── apply/               # Future: parallel apply
    ├── mod.rs
    └── pool.rs
```

## Configuration

```rust
impl Default for FileLogConfig {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from("data/log"),
            target_file_size: 128 * 1024 * 1024, // 128MB
            compression: CompressionType::Lz4,
            purge_threshold: 256 * 1024 * 1024,  // 256MB
        }
    }
}
```

## Implementation Phases

### Phase 1: Basic LogService
- [ ] LogEntry, LogBatch types
- [ ] FileLogService with single file
- [ ] Write path with fsync
- [ ] Recovery (replay on startup)

### Phase 2: Multi-File Management
- [ ] File rotation on size limit
- [ ] MemTable per region
- [ ] Basic GC (purge_expired_files)

### Phase 3: Performance
- [ ] LZ4 compression
- [ ] Write buffering
- [ ] Group commit

### Phase 4: Transaction Integration
- [ ] Integrate with TransactionService
- [ ] Commit/rollback logging
- [ ] Recovery with uncommitted txn handling

### Phase 5: Raft Integration (Future)
- [ ] Add raft-rs dependency
- [ ] RaftLogStorage impl
- [ ] RaftNode for consensus
- [ ] Message transport

### Phase 6: Parallel Apply (Future)
- [ ] ApplyPool with per-region workers
- [ ] Term-based routing
- [ ] Apply index tracking

## References

1. **TiKV Raft-Engine**: https://github.com/tikv/raft-engine
2. **TiKV Raft-rs**: https://github.com/tikv/raft-rs
3. **Raft-Engine Blog**: https://www.pingcap.com/blog/raft-engine-a-log-structured-embedded-storage-engine-for-multi-raft-logs-in-tikv/
4. **Parallel Apply**: https://tanxinyu.work/2022-tidb-hackathon/
5. **BitCask Paper**: https://riak.com/assets/bitcask-intro.pdf

## License

This project is licensed under the Apache License, Version 2.0. See [LICENSE](../../LICENSE) for details.
