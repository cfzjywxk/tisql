use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::TempDir;
use tisql::testkit::{ClogBatch, ClogOpRef, FileClogConfig, FileClogService, IoService};
use tisql::{new_lsn_provider, ClogService};

mod common;

fn build_clog(runtime: &tokio::runtime::Runtime) -> (TempDir, Arc<FileClogService>) {
    let dir = TempDir::new().expect("temp dir should build");
    let io = IoService::new_for_test(32).expect("io service should open");
    let lsn = new_lsn_provider();
    let clog = Arc::new(
        FileClogService::open_with_lsn_provider(
            FileClogConfig::with_dir(dir.path()),
            lsn,
            io,
            runtime.handle(),
        )
        .expect("clog should open"),
    );
    (dir, clog)
}

fn seed_clog(runtime: &tokio::runtime::Runtime, clog: &FileClogService, rows: usize) {
    runtime.block_on(async {
        for txn_id in 1..=rows as u64 {
            let key = format!("seed-{txn_id}").into_bytes();
            let value = format!("value-{txn_id}").into_bytes();
            let ops = [ClogOpRef::Put {
                key: key.as_slice(),
                value: value.as_slice(),
            }];
            let future = clog
                .write_ops(txn_id, &ops, txn_id + 1, None, true)
                .await
                .expect("write_ops should succeed");
            future.await.expect("fsync should succeed");
        }
    });
}

fn bench_clog_service(c: &mut Criterion) {
    let runtime = common::bench_runtime();
    let (_dir, clog) = build_clog(&runtime);
    seed_clog(&runtime, clog.as_ref(), 128);

    let mut group = c.benchmark_group("clog_service");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(1));

    let next_txn_id = AtomicU64::new(10_000);
    group.bench_function("write_ops", |b| {
        b.iter(|| {
            let txn_id = next_txn_id.fetch_add(1, Ordering::Relaxed);
            let key = format!("bench-{txn_id}").into_bytes();
            let value = vec![b'x'; 32];
            let ops = [ClogOpRef::Put {
                key: key.as_slice(),
                value: value.as_slice(),
            }];
            let lsn = runtime.block_on(async {
                let future = clog
                    .write_ops(txn_id, &ops, txn_id + 1, None, true)
                    .await
                    .expect("write_ops should succeed");
                future.await.expect("fsync should succeed")
            });
            black_box(lsn);
        });
    });

    group.bench_function("read_all", |b| {
        b.iter(|| {
            let entries = clog.read_all().expect("read_all should succeed");
            black_box(entries.len());
        });
    });

    group.bench_function("batch_write", |b| {
        b.iter(|| {
            let txn_id = next_txn_id.fetch_add(1, Ordering::Relaxed);
            let mut batch = ClogBatch::new();
            batch.add_put(txn_id, format!("k{txn_id}").into_bytes(), vec![b'v'; 16]);
            batch.add_put(txn_id, format!("k{txn_id}-2").into_bytes(), vec![b'w'; 16]);
            batch.add_commit(txn_id, txn_id + 10);
            let lsn = runtime.block_on(async {
                let future = clog.write(&mut batch, true).expect("write should succeed");
                future.await.expect("fsync should succeed")
            });
            black_box(lsn);
        });
    });

    group.finish();
    runtime.block_on(async {
        clog.close().await.expect("clog should close cleanly");
    });
}

criterion_group!(benches, bench_clog_service);
criterion_main!(benches);
