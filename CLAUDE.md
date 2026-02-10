# TiSQL - Claude Code Notes

## Guidelines

- Do NOT push design documents to GitHub (use `docs/design/` locally, it's gitignored)
- Keep commits atomic and well-described
- **Run `make prepare` before pushing to GitHub** - this runs fmt, clippy, and all tests
- **Do not just fix issues** - clean up and refactor when necessary; remove unnecessary abstractions
- **Minimize `Box<dyn>` and lifetime annotations** - prefer concrete types, associated types, and simpler ownership patterns where possible
- **Keep `new()` constructors simple** - `new()` should only initialize fields, no I/O or seeking. For iterators, `new()` creates an unpositioned iterator; call `seek()` or `advance()` to position it
- **Correctness comes first** - always prioritize correct behavior over performance or elegance
- **Write performant code** - do not allocate arbitrarily; be mindful of allocations in hot paths
- **Use `unsafe` sparingly** - think carefully before using `unsafe`. Any `unsafe` code block must be accompanied by a correctness proof in comments

## Coding Guidelines

- **Prefer async over sync primitives** - TiSQL uses Rust async programming (tokio). Do NOT use synchronous utilities like `std::sync::Mutex`, `std::sync::Condvar`, or `std::thread::sleep` for coordination. Use their async equivalents (`tokio::sync::Mutex`, `tokio::sync::Notify`, `tokio::time::sleep`, etc.) instead
- **Minimize contention** - Avoid introducing lock contention in code. Prefer lock-free patterns (atomics, `Arc`), fine-grained locking, or message passing over coarse-grained mutexes. When locks are necessary, hold them for the shortest duration possible and never across `.await` points

---

## Project Overview

**TiSQL** is a TiDB-compatible SQL database in Rust, serving as a learning project for database internals with MySQL protocol compatibility.

- **Repository**: https://github.com/cfzjywxk/tisql
- **License**: Apache-2.0

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      MySQL Client                                │
└───────────────────────────────┬─────────────────────────────────┘
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  Protocol Layer: MySqlServer (opensrv-mysql) + Session          │
└───────────────────────────────┬─────────────────────────────────┘
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  SQL Layer: Parser → Binder → Executor (volcano-style)          │
└───────────────────────────────┬─────────────────────────────────┘
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  Transaction Layer: TxnService + ConcurrencyManager + TSO       │
└───────────────────────────────┬─────────────────────────────────┘
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  Storage Layer: LsmEngine (SST + MemTable) + Clog + Ilog        │
│                 [Unified LsnProvider for recovery ordering]     │
└─────────────────────────────────────────────────────────────────┘
```

### Key Design Principles

1. **Trait-based layering**: Each layer depends on traits, not concrete implementations
2. **TiDB/TiKV patterns**: MVCC key encoding, ConcurrencyManager, TSO
3. **OceanBase patterns**: Unified TxnService with TxnCtx context

---

## Module Structure

```
src/
├── lib.rs           # Public API: Database, traits, types
├── session/         # Session & QueryCtx (per-connection state)
├── sql/             # Parser, Binder, Plan (encapsulated)
├── executor/        # Volcano-style execution
├── transaction/     # TxnService, ConcurrencyManager
├── storage/         # LsmEngine, MemTable, SSTable (persistent storage)
├── lsn.rs           # LsnProvider trait for unified LSN allocation
├── tso/             # TsoService, LocalTso (timestamp oracle)
├── clog/            # FileClogService (commit log / WAL)
├── catalog/         # MemoryCatalog (schema metadata)
├── codec/           # TiDB-compatible key/value encoding
├── protocol/        # MySQL protocol handler
└── worker/          # yatp thread pool
```

---

## Testing & Running

```bash
# All tests
cargo test

# With failpoints (concurrency testing)
cargo test --features failpoints

# Run server
cargo run -- --data-dir ./data --port 4000

# Connect
mysql -h 127.0.0.1 -P 4000 -uroot
```

---

## Current Status

### Completed

- **SQL**: Parsing (sqlparser), binding, volcano-style execution
- **Protocol**: MySQL wire protocol (opensrv-mysql), session lifecycle
- **Transaction**: Unified TxnService with TxnCtx, 1PC with in-memory locks
- **Explicit Transactions**: BEGIN/COMMIT/ROLLBACK SQL support with pessimistic locking
- **MVCC**: Key encoding (`key || !commit_ts`), snapshot isolation
- **Durability**: FileClogService with crash recovery
- **Concurrency**: ConcurrencyManager (max_ts tracking), OceanBase-style pessimistic locking
- **Codec**: TiDB-compatible row/key encoding
- **Storage**: VersionedMemTableEngine (OceanBase-style) + LSM-tree with SST persistence
- **Memory**: jemalloc as default allocator (like TiKV)
- **Benchmarks**: sysbench-like OLTP benchmark tool

### Storage Layer

The storage layer uses `VersionedMemTableEngine` as the production memtable implementation:

| Component | Description |
|-----------|-------------|
| `VersionedMemTableEngine` | OceanBase-style design with user key stored once + version chain |
| `LsmEngine` | LSM-tree with memtable + SST levels for persistence |
| `StorageEngine` trait | Streaming-only interface via `scan_iter()` |

**Key Design:**
- Each user key stored once with linked list of versions (space efficient)
- Fast point lookups: seek to user key, traverse short version chain
- Better cache locality: all versions of a key are adjacent in memory
- Streaming iterators only (`scan_iter()`) - no materializing `scan()` method

Run benchmarks: `cargo run --release --bin sysbench-sim -- --threads 8 --time 60`

### LSM Storage Engine (Production Ready)

Persistent LSM-tree storage engine with crash recovery. **Now integrated as default storage backend.**

| Phase | Component | Status |
|-------|-----------|--------|
| 1 | SSTable (block format, builder, reader, iterator) | ✅ Complete |
| 2 | MemTable wrapper (LSN tracking), Version, LsmConfig | ✅ Complete |
| 3 | LsmEngine (main storage engine) | ✅ Complete |
| 4 | Compaction infrastructure (picker, merge iterator, executor) | ✅ Complete |
| 5 | Durability (ilog, intent/commit, recovery) | ✅ Complete |
| 6 | Unified LSN (clog + ilog share LsnProvider) | ✅ Complete |
| 7 | Integration with TxnService | ✅ Complete |
| 8 | Background flush/compaction workers + write flow control | 🔶 Partial (flush + write stall done, compaction workers pending) |
| 9 | Block cache for read performance | ⏳ Pending |
| 10 | Bloom filters for SST | ⏳ Pending |

**Storage Module Structure:**
```
src/storage/
├── mod.rs            # Public exports, StorageEngine trait
├── config.rs         # LsmConfig with builder pattern
├── version.rs        # Version management, ManifestDelta
├── version_set.rs    # VersionSet + SuperVersion for atomic snapshots
├── lsm.rs            # LsmEngine - main entry point
├── flush_scheduler.rs # Background flush worker
├── compaction.rs     # CompactionPicker, MergeIterator, CompactionExecutor
├── ilog.rs           # IlogService - SST metadata persistence
├── recovery.rs       # LsmRecovery - coordinated ilog+clog recovery
├── mvcc.rs           # MvccKey encoding, MvccIterator trait
├── memtable/         # VersionedMemTableEngine + MemTable wrapper
└── sstable/          # SST format (block, builder, reader, iterator)
```

**Recovery Sequence:**
```
1. Replay ilog → rebuild Version (SST metadata)
2. Get flushed_lsn from Version
3. Replay clog entries with lsn > flushed_lsn to memtable
4. Track max_commit_ts from ALL clog entries for TSO/ConcurrencyManager init
5. Cleanup orphan SST files from incomplete flush/compact
```

**Key Integration Details:**
- `DbStorage = LsmEngine` - LSM is now the default storage backend
- `Database::open()` uses `LsmRecovery` for coordinated recovery
- Tombstones (DELETE) are properly written to SST via `scan_all` method
- SST scan merges with memtable scan for consistent reads
- TSO initialized to `max_commit_ts + 1` after recovery

### Recent Changes

**Write Flow Control: L0 Slowdown + Frozen Memtable Stall (Feb 2026)**
- Added `Condvar`-based write stall mechanism to `LsmEngine`
- Three tiers of backpressure: L0 slowdown (linear delay 1ms→100ms), frozen memtable stall (Condvar block with 5s timeout), L0 stop (hard reject)
- `wait_if_stalled()` called from `write_batch()`, `put_pending()`, `delete_pending()`
- `notify_write_stall()` called from `flush_memtable()` and `do_compaction()` to wake stalled writers
- `finalize_pending`/`abort_pending` exempt from stalling (cleanup must not be delayed)
- Unit tests for delay interpolation, stall/unblock, timeout safety
- Integration test (`test_write_stall_e2e`) with concurrent writers + flush scheduler

**Atomic SuperVersion Installation (Feb 2026)**
- Implemented RocksDB/OceanBase-style atomic snapshot isolation
- Added `current_sv: RwLock<Arc<SuperVersion>>` for cached consistent snapshots
- `get_super_version()` returns `Arc<SuperVersion>` (lock-free after clone)
- `install_super_version(state: &RwLockWriteGuard)` enforces compile-time lock safety
- Flush holds state write lock during: version update + frozen removal + SV install
- sv_number only increments on state changes, not on every read
- Added `VersionSet` for separate version management with atomic delta application

**Background Flush Scheduler (Feb 2026)**
- Added `FlushScheduler` for automatic frozen memtable flushing
- Worker thread polls for frozen memtables and flushes them to SST
- Clean shutdown with in-progress flush completion
- Notification mechanism for immediate wake on rotation
- Test helpers: `wait_for_flush_count()`, `flush_count()`

**Storage Test Coverage Improvements (Feb 2026)**
- Added `LsmEngine::get()` and `get_at()` methods for point lookups (checks memtables first, then SSTs)
- Added L0 SST iterator fail points for error path testing:
  - `l0_sst_iterator_open_file` - SST file open errors
  - `l0_sst_iterator_seek` - Seek errors
  - `l0_sst_iterator_advance` - Advance errors
- Added 6 new fail point tests in `tests/storage_failpoint_test.rs`
- Total: 30 fail point tests, 700 library tests
- Coverage results:
  - `src/storage/lsm.rs`: 61.9% (314/507 lines)
  - `src/storage/config.rs`: 100% (92/92 lines)
  - `src/storage/memtable/mod.rs`: 100% (6/6 lines)
  - `src/storage/memtable/wrapper.rs`: 97.5% (77/79 lines)
  - `src/storage/recovery.rs`: 100% (79/79 lines)
  - Overall: 64.13% (4394/6852 lines)

**Explicit Transactions - SQL Layer Support (Feb 2026)**
- Added `LogicalPlan::Begin`, `Commit`, `Rollback` variants
- Session tracks active `TxnCtx` for explicit transactions
- `execute_with_session` for transaction-aware execution
- `Database::handle_mp_query_with_session_mut` for protocol layer
- 8 new tests for explicit transaction behavior
- **Read-your-writes**: Supported via `owner_ts` parameter in `scan_iter()` for explicit transactions

**OceanBase-Style Pessimistic Locking (Feb 2026)**
- `TxnStateCache` for centralized transaction state tracking
- `VersionNode` with pending/committed/aborted states
- `PessimisticStorage` trait with `put_pending`, `finalize_pending`, `abort_pending`
- Explicit transactions write pending nodes directly to storage
- Lock conflicts return `KeyIsLocked` error immediately

**VersionedMemTableEngine Test Coverage (Feb 2026)**
- Added 22 new unit tests covering iterator edge cases, boundary conditions, and Arc/lifetime safety
- Added 10 stress tests in `tests/memtable_stress_test.rs` for memory and concurrency stress testing
- Coverage gaps addressed: empty memtable iteration, large version chains (200+ versions), timestamp boundaries (0/MAX), Arc iterator outliving engine, binary keys/values, 1MB values
- Total test count: 44 unit tests + 10 stress tests (2 long-running ignored by default)

**Construct-Then-Position Pattern (Feb 2026)**
- Refactored `MergeIterator` (compaction) to follow the "keep `new()` simple" guideline
- `MergeIterator::new()` now only constructs child iterators; call `seek_to_first()` to position
- Replaced `Box<dyn MvccIterator>` with concrete enum types (`LazyMergeIteratorChild`) for lazy merge iterator
- All iterators now follow consistent pattern: `new()` → `seek()`/`advance()` → iterate

**Iterator Simplification (Jan 2026)**
- Fixed TieredMergeIterator tier-gating bug: all tiers (memtable, L0, L1+) now initialized at build time for correct MVCC key ordering
- Fixed SstMvccIterator range start bound checking in `is_in_range()`
- Removed `LazySstIterator` and `PendingIterator` abstractions - use `SstMvccIterator` directly
- Removed unsafe `Send/Sync` impl from `VersionedMemTableIterator`

**Read-Your-Writes Tests (Jan 2026)**
- Added comprehensive test coverage for `MvccScanIterator` buffer+storage merge:
  - Buffer-only, storage-only, interleaved keys
  - Delete operations (existing, non-existent, then put, after put)
  - Multiple updates to same key, range boundaries

### TODO

- [ ] Persist catalog (tables don't survive restart)
- [x] Explicit transactions (BEGIN/COMMIT/ROLLBACK) - basic support added
- [x] Read-your-writes for explicit transactions (via owner_ts in scan_iter)
- [ ] Index support
- [ ] Aggregations, JOINs, subqueries
- [ ] Log rotation and compaction
- [x] Background flush workers (FlushScheduler)
- [ ] Background compaction workers
- [ ] Block cache for SST read performance
- [ ] Bloom filters for SST point lookups

---

## Key Invariants

1. **commit_ts > any concurrent reader's start_ts** - Prevents "commit in the past" anomaly
2. **LSN ordering** - Clog records committed in ascending LSN order for recovery
3. **Lock before commit_ts** - Acquire locks before computing commit_ts
4. **max_ts tracking** - ConcurrencyManager updates max_ts on every read to ensure visibility
5. **Unified LSN** - Clog and ilog share the same LsnProvider for consistent recovery ordering

---

## Dependencies

| Crate | Purpose |
|-------|---------|
| sqlparser | SQL parsing |
| tokio | Async runtime |
| opensrv-mysql | MySQL protocol |
| yatp | Thread pool (TiKV) |
| tikv-jemallocator | jemalloc allocator (default) |
| fail | Fail point injection |
