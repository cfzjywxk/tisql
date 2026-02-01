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

//! Stress tests for VersionedMemTableEngine.
//!
//! These tests verify:
//! - Memory stress: large memtables, single key with many versions
//! - Concurrent stress: sustained load, high thread counts
//!
//! To run these tests:
//! ```
//! cargo test -p tisql --test memtable_stress_test
//! ```
//!
//! To run only the ignored long-running tests:
//! ```
//! cargo test -p tisql --test memtable_stress_test -- --ignored
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use tisql::storage::mvcc::{is_tombstone, MvccIterator, MvccKey};
use tisql::storage::{ArcVersionedMemTableIterator, VersionedMemTableEngine, WriteBatch};
use tisql::types::{RawValue, Timestamp};

// ==================== Test Helpers ====================

/// Write a key-value pair at the given timestamp.
fn put_at(engine: &VersionedMemTableEngine, key: &[u8], value: &[u8], ts: Timestamp) {
    let mut batch = WriteBatch::new();
    batch.put(key.to_vec(), value.to_vec());
    batch.set_commit_ts(ts);
    engine.write_batch(batch).unwrap();
}

/// Get the latest version of a key visible at the given timestamp.
fn get_at(engine: &VersionedMemTableEngine, key: &[u8], ts: Timestamp) -> Option<RawValue> {
    let start = MvccKey::encode(key, ts);
    let end = MvccKey::encode(key, 0)
        .next_key()
        .unwrap_or_else(MvccKey::unbounded);
    let range = Arc::new(start..end);

    let inner = engine.inner_arc();
    let mut iter = ArcVersionedMemTableIterator::new(inner, range);
    iter.advance().unwrap();

    while iter.valid() {
        if iter.user_key() == key && iter.timestamp() <= ts {
            let value = iter.value().to_vec();
            if is_tombstone(&value) {
                return None;
            }
            return Some(value);
        }
        iter.advance().unwrap();
    }
    None
}

/// Get the latest version of a key.
fn get(engine: &VersionedMemTableEngine, key: &[u8]) -> Option<RawValue> {
    get_at(engine, key, Timestamp::MAX)
}

// ============================================================================
// Memory Stress Tests
// ============================================================================

/// Test memtable with a single key having 10,000+ versions.
///
/// This stress tests the version chain implementation to ensure:
/// - Memory doesn't grow excessively
/// - Point lookups remain fast even with deep version chains
/// - Scanning works correctly
#[test]
fn test_single_key_many_versions() {
    let engine = VersionedMemTableEngine::new();
    let num_versions = 10_000;

    // Insert many versions of the same key
    for ts in 1..=num_versions {
        let value = format!("version_{ts:06}");
        put_at(&engine, b"hot_key", value.as_bytes(), ts);
    }

    // Verify counts
    assert_eq!(engine.len(), num_versions as usize);
    assert_eq!(engine.key_count(), 1);

    // Verify memory stats
    let stats = engine.memory_stats();
    assert_eq!(stats.entry_count, num_versions as usize);
    assert_eq!(stats.key_count, 1);

    // Verify point reads at various timestamps are fast
    let start = Instant::now();
    for ts in [1, 100, 1000, 5000, 10000] {
        let value = get_at(&engine, b"hot_key", ts);
        let expected = format!("version_{ts:06}");
        assert_eq!(value, Some(expected.into_bytes()));
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(100),
        "Point reads should be fast, took {elapsed:?}"
    );

    // Verify latest read
    let latest = get(&engine, b"hot_key");
    assert_eq!(
        latest,
        Some(format!("version_{num_versions:06}").into_bytes())
    );
}

/// Test memtable with many keys (100,000+ unique keys).
///
/// This stress tests the skiplist implementation to ensure:
/// - Memory scales linearly with key count
/// - Iteration order is maintained
/// - Random access remains O(log n)
#[test]
fn test_many_unique_keys() {
    let engine = VersionedMemTableEngine::new();
    let num_keys = 100_000;

    // Insert many unique keys
    for i in 0..num_keys {
        let key = format!("key_{i:08}");
        let value = format!("value_{i}");
        put_at(&engine, key.as_bytes(), value.as_bytes(), i as u64 + 1);
    }

    assert_eq!(engine.len(), num_keys);
    assert_eq!(engine.key_count(), num_keys);

    // Verify random access performance
    let start = Instant::now();
    let test_indices = [0, 1000, 10_000, 50_000, 99_999];
    for &i in &test_indices {
        let key = format!("key_{i:08}");
        let value = get(&engine, key.as_bytes());
        let expected = format!("value_{i}");
        assert_eq!(value, Some(expected.into_bytes()));
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(50),
        "Random access should be fast, took {elapsed:?}"
    );

    // Verify iteration order (sample first 100 keys)
    let range = Arc::new(MvccKey::unbounded()..MvccKey::unbounded());
    let inner = engine.inner_arc();
    let mut iter = ArcVersionedMemTableIterator::new(inner, range);
    iter.advance().unwrap();

    let mut count = 0;
    while iter.valid() && count < 100 {
        let expected_key = format!("key_{count:08}");
        assert_eq!(iter.user_key(), expected_key.as_bytes());
        iter.advance().unwrap();
        count += 1;
    }
    assert_eq!(count, 100);
}

/// Test with large values (1MB+ per value).
///
/// Verifies that large values don't cause issues with memory management
/// or value retrieval.
#[test]
fn test_large_values() {
    let engine = VersionedMemTableEngine::new();
    let num_entries = 10;
    let value_size = 1024 * 1024; // 1MB

    for i in 0..num_entries {
        let key = format!("large_key_{i}");
        let value = vec![(i % 256) as u8; value_size];
        put_at(&engine, key.as_bytes(), &value, i as u64 + 1);
    }

    assert_eq!(engine.len(), num_entries);

    // Verify each value
    for i in 0..num_entries {
        let key = format!("large_key_{i}");
        let value = get(&engine, key.as_bytes());
        assert!(value.is_some());
        let v = value.unwrap();
        assert_eq!(v.len(), value_size);
        assert!(v.iter().all(|&b| b == (i % 256) as u8));
    }
}

/// Test mixed key sizes and value sizes.
#[test]
fn test_mixed_sizes() {
    let engine = VersionedMemTableEngine::new();

    // Empty key and value
    put_at(&engine, b"", b"", 1);

    // Small key, large value
    let large_value = vec![b'x'; 100_000];
    put_at(&engine, b"s", &large_value, 2);

    // Large key, small value
    let large_key = vec![b'k'; 10_000];
    put_at(&engine, &large_key, b"v", 3);

    // Large key, large value
    let large_key2 = vec![b'K'; 10_000];
    let large_value2 = vec![b'V'; 100_000];
    put_at(&engine, &large_key2, &large_value2, 4);

    assert_eq!(engine.len(), 4);

    // Verify all entries
    assert_eq!(get(&engine, b""), Some(vec![]));
    assert_eq!(get(&engine, b"s"), Some(large_value));
    assert_eq!(get(&engine, &large_key), Some(b"v".to_vec()));
    assert_eq!(get(&engine, &large_key2), Some(large_value2));
}

// ============================================================================
// Concurrent Stress Tests
// ============================================================================

/// Test sustained concurrent writes from many threads.
///
/// Verifies:
/// - No data loss under heavy concurrent writes
/// - No panics or deadlocks
/// - All written data is readable
#[test]
fn test_sustained_concurrent_writes() {
    let engine = Arc::new(VersionedMemTableEngine::new());
    let num_threads = 8;
    let writes_per_thread = 10_000;
    let barrier = Arc::new(Barrier::new(num_threads));
    let ts_counter = Arc::new(AtomicU64::new(1));

    let mut handles = vec![];
    for thread_id in 0..num_threads {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        let ts_counter = Arc::clone(&ts_counter);

        let handle = thread::spawn(move || {
            barrier.wait();

            for i in 0..writes_per_thread {
                let key = format!("t{thread_id}_k{i:06}");
                let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
                put_at(&engine, key.as_bytes(), b"value", ts);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all writes
    assert_eq!(engine.len(), num_threads * writes_per_thread);
    assert_eq!(engine.key_count(), num_threads * writes_per_thread);

    // Sample verification
    for thread_id in 0..num_threads {
        for i in [0, writes_per_thread / 2, writes_per_thread - 1] {
            let key = format!("t{thread_id}_k{i:06}");
            let value = get(&engine, key.as_bytes());
            assert!(value.is_some(), "Key {key} should exist");
        }
    }
}

/// Test concurrent writes to the same key from multiple threads.
///
/// This stress tests the CAS-based version chain prepend operation.
/// Note: The memtable requires versions to be inserted in ascending timestamp order.
/// To test concurrent CAS operations correctly, we use a mutex to ensure atomic
/// get-timestamp-and-write operations, simulating proper transaction ordering.
#[test]
fn test_concurrent_writes_same_key() {
    use std::sync::Mutex;

    let engine = Arc::new(VersionedMemTableEngine::new());
    let num_threads = 16;
    let writes_per_thread = 1_000;
    let barrier = Arc::new(Barrier::new(num_threads));

    // Use a mutex to ensure atomic get-ts-and-write, simulating transaction ordering.
    // This is what the real transaction layer does (acquire lock, then get commit_ts).
    let ts_and_write_lock = Arc::new(Mutex::new(1u64));

    let mut handles = vec![];
    for _ in 0..num_threads {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        let ts_and_write_lock = Arc::clone(&ts_and_write_lock);

        let handle = thread::spawn(move || {
            barrier.wait();

            for _ in 0..writes_per_thread {
                // Atomic: get timestamp and write while holding lock
                let ts = {
                    let mut guard = ts_and_write_lock.lock().unwrap();
                    let ts = *guard;
                    *guard += 1;
                    let value = format!("v{ts}");
                    put_at(&engine, b"contended_key", value.as_bytes(), ts);
                    ts
                };
                let _ = ts; // Suppress unused warning
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // All versions should be present
    let total_versions = num_threads * writes_per_thread;
    assert_eq!(engine.len(), total_versions);
    assert_eq!(engine.key_count(), 1);

    // Latest value should be the highest timestamp
    let latest_ts = *ts_and_write_lock.lock().unwrap() - 1;
    let value = get(&engine, b"contended_key");
    let expected = format!("v{latest_ts}");
    assert_eq!(value, Some(expected.into_bytes()));
}

/// Test concurrent reads and writes.
///
/// Verifies MVCC correctness under concurrent access.
/// Each writer writes to a disjoint set of unique keys (never updating existing keys)
/// to avoid MVCC ordering violations.
#[test]
fn test_concurrent_reads_and_writes() {
    let engine = Arc::new(VersionedMemTableEngine::new());
    let num_writers = 4;
    let num_readers = 8;
    let duration = Duration::from_secs(2);
    let barrier = Arc::new(Barrier::new(num_writers + num_readers));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let ts_counter = Arc::new(AtomicU64::new(1001)); // Start after pre-populated data

    // Pre-populate some data (these keys won't be updated by writers)
    for i in 0..1000 {
        let key = format!("read_key_{i:04}");
        put_at(&engine, key.as_bytes(), b"initial", i as u64 + 1);
    }

    let writes_done = Arc::new(AtomicU64::new(0));
    let reads_done = Arc::new(AtomicU64::new(0));
    let read_errors = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // Spawn writers - each writer creates unique keys (no updates to existing)
    for writer_id in 0..num_writers {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        let stop_flag = Arc::clone(&stop_flag);
        let ts_counter = Arc::clone(&ts_counter);
        let writes_done = Arc::clone(&writes_done);

        let handle = thread::spawn(move || {
            barrier.wait();

            let mut local_write_count = 0u64;
            while !stop_flag.load(Ordering::Relaxed) {
                // Each writer creates unique keys: "w{writer_id}_k{local_count}"
                let key = format!("w{writer_id}_k{local_write_count}");
                let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
                put_at(&engine, key.as_bytes(), b"new_value", ts);
                local_write_count += 1;
                writes_done.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    // Spawn readers - read from pre-populated keys
    for reader_id in 0..num_readers {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        let stop_flag = Arc::clone(&stop_flag);
        let reads_done = Arc::clone(&reads_done);
        let read_errors = Arc::clone(&read_errors);

        let handle = thread::spawn(move || {
            barrier.wait();

            let mut local_reads = 0u64;
            let mut local_errors = 0u64;
            while !stop_flag.load(Ordering::Relaxed) {
                // Read pre-populated keys
                for offset in 0..10 {
                    let key_idx = (reader_id * 100 + offset) % 1000;
                    let key = format!("read_key_{key_idx:04}");
                    match get(&engine, key.as_bytes()) {
                        Some(_) => local_reads += 1,
                        None => local_errors += 1, // Should never happen
                    }
                }
            }
            reads_done.fetch_add(local_reads, Ordering::Relaxed);
            read_errors.fetch_add(local_errors, Ordering::Relaxed);
        });
        handles.push(handle);
    }

    // Run for duration
    thread::sleep(duration);
    stop_flag.store(true, Ordering::SeqCst);

    for handle in handles {
        handle.join().unwrap();
    }

    let total_writes = writes_done.load(Ordering::Relaxed);
    let total_reads = reads_done.load(Ordering::Relaxed);
    let total_errors = read_errors.load(Ordering::Relaxed);

    assert!(total_writes > 0, "Should have completed writes");
    assert!(total_reads > 0, "Should have completed reads");
    assert_eq!(total_errors, 0, "Should have no read errors");
}

/// Stress test with very high thread count.
#[test]
fn test_high_thread_count() {
    let engine = Arc::new(VersionedMemTableEngine::new());
    let num_threads = 64;
    let writes_per_thread = 100;
    let barrier = Arc::new(Barrier::new(num_threads));
    let ts_counter = Arc::new(AtomicU64::new(1));

    let mut handles = vec![];
    for thread_id in 0..num_threads {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        let ts_counter = Arc::clone(&ts_counter);

        let handle = thread::spawn(move || {
            barrier.wait();

            for i in 0..writes_per_thread {
                let key = format!("ht{thread_id}_k{i}");
                let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
                put_at(&engine, key.as_bytes(), b"value", ts);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(engine.len(), num_threads * writes_per_thread);
}

// ============================================================================
// Long-Running Stress Tests (ignored by default)
// ============================================================================

/// Extended stress test running for 30 seconds.
///
/// Run with: cargo test --test memtable_stress_test -- --ignored test_extended_stress
#[test]
#[ignore]
fn test_extended_stress() {
    let engine = Arc::new(VersionedMemTableEngine::new());
    let num_writers = 8;
    let num_readers = 16;
    let duration = Duration::from_secs(30);
    let barrier = Arc::new(Barrier::new(num_writers + num_readers));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let ts_counter = Arc::new(AtomicU64::new(1));

    let writes_done = Arc::new(AtomicU64::new(0));
    let reads_done = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // Writers - each writer writes unique keys to avoid MVCC ordering issues
    for writer_id in 0..num_writers {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        let stop_flag = Arc::clone(&stop_flag);
        let ts_counter = Arc::clone(&ts_counter);
        let writes_done = Arc::clone(&writes_done);

        let handle = thread::spawn(move || {
            barrier.wait();

            let mut local_writes = 0u64;
            while !stop_flag.load(Ordering::Relaxed) {
                // Each writer creates unique keys: "w{id}_k{count}"
                let key = format!("w{writer_id}_k{local_writes}");

                let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
                put_at(&engine, key.as_bytes(), b"stress_value", ts);
                local_writes += 1;

                // Occasional yield to reduce contention
                if local_writes % 1000 == 0 {
                    thread::yield_now();
                }
            }
            writes_done.fetch_add(local_writes, Ordering::Relaxed);
        });
        handles.push(handle);
    }

    // Readers with various access patterns
    for reader_id in 0..num_readers {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        let stop_flag = Arc::clone(&stop_flag);
        let reads_done = Arc::clone(&reads_done);

        let handle = thread::spawn(move || {
            barrier.wait();

            let mut local_reads = 0u64;
            let mut rng_state = reader_id as u64;

            while !stop_flag.load(Ordering::Relaxed) {
                // Simple LCG for pseudo-random access
                rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key_type = rng_state % 2;

                let key = match key_type {
                    0 => {
                        // Read from writer keys (may or may not exist yet)
                        let writer = rng_state % 8;
                        let idx = (rng_state / 8) % 10000;
                        format!("w{writer}_k{idx}")
                    }
                    _ => format!("nonexistent_{}", rng_state % 1000),
                };

                let _ = get(&engine, key.as_bytes());
                local_reads += 1;

                if local_reads % 1000 == 0 {
                    thread::yield_now();
                }
            }
            reads_done.fetch_add(local_reads, Ordering::Relaxed);
        });
        handles.push(handle);
    }

    // Run for duration
    thread::sleep(duration);
    stop_flag.store(true, Ordering::SeqCst);

    for handle in handles {
        handle.join().unwrap();
    }

    let total_writes = writes_done.load(Ordering::Relaxed);
    let total_reads = reads_done.load(Ordering::Relaxed);

    println!("Extended stress test: {total_writes} writes, {total_reads} reads in {duration:?}");
    println!(
        "Engine stats: {} entries, {} keys",
        engine.len(),
        engine.key_count()
    );

    assert!(total_writes > 100_000, "Should complete many writes");
    assert!(total_reads > 100_000, "Should complete many reads");
}

/// Memory pressure test with continuous allocation.
///
/// Run with: cargo test --test memtable_stress_test -- --ignored test_memory_pressure
#[test]
#[ignore]
fn test_memory_pressure() {
    let engine = Arc::new(VersionedMemTableEngine::new());
    let num_threads: usize = 4;
    let target_entries: usize = 1_000_000;
    let barrier = Arc::new(Barrier::new(num_threads));
    let ts_counter = Arc::new(AtomicU64::new(1));
    let entries_written = Arc::new(AtomicU64::new(0));

    let value = vec![b'x'; 1024]; // 1KB values

    let mut handles = vec![];
    for thread_id in 0..num_threads {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        let ts_counter = Arc::clone(&ts_counter);
        let entries_written = Arc::clone(&entries_written);
        let value = value.clone();

        let handle = thread::spawn(move || {
            barrier.wait();

            let entries_per_thread = target_entries / num_threads;
            for i in 0..entries_per_thread {
                let key = format!("mem_t{thread_id}_k{i:08}");
                let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
                put_at(&engine, key.as_bytes(), &value, ts);
                entries_written.fetch_add(1, Ordering::Relaxed);

                // Progress update every 100k entries
                if i > 0 && i % 100_000 == 0 {
                    let total = entries_written.load(Ordering::Relaxed);
                    let stats = engine.memory_stats();
                    println!(
                        "Progress: {} entries, {} keys",
                        stats.entry_count, stats.key_count
                    );
                    assert_eq!(total as usize, stats.entry_count, "Entry count mismatch");
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let stats = engine.memory_stats();
    println!(
        "Memory pressure test complete: {} entries, {} keys",
        stats.entry_count, stats.key_count
    );

    assert_eq!(stats.entry_count, target_entries);
    assert_eq!(stats.key_count, target_entries);

    // Sample verification
    let entries_per_thread = target_entries / num_threads;
    for sample in [
        0,
        target_entries / 4,
        target_entries / 2,
        target_entries - 1,
    ] {
        let thread_id = sample / entries_per_thread;
        let local_idx = sample % entries_per_thread;
        let key = format!("mem_t{thread_id}_k{local_idx:08}");
        let result = get(&engine, key.as_bytes());
        assert!(result.is_some(), "Key {key} should exist");
    }
}
