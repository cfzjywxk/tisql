use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use tempfile::TempDir;
use tisql::catalog::types::Timestamp;
use tisql::tablet::mvcc::MvccKey;
use tisql::tablet::{MvccIterator, WriteBatch};
use tisql::testkit::{IlogConfig, IlogService, IoService, LsmConfig, LsmEngine, Version};
use tisql::{new_lsn_provider, StorageEngine};

mod common;

fn open_engine(dir: &std::path::Path) -> (tisql::SharedLsnProvider, LsmEngine) {
    let lsn = new_lsn_provider();
    let io = IoService::new_for_test(32).expect("io service should open");
    let ilog = Arc::new(
        IlogService::open_with_thread(IlogConfig::new(dir), Arc::clone(&lsn))
            .expect("ilog should open"),
    );
    let engine = LsmEngine::open_with_recovery(
        LsmConfig::new(dir),
        Arc::clone(&lsn),
        ilog,
        Version::new(),
        io,
    )
    .expect("engine should open");
    (lsn, engine)
}

fn seed_engine(engine: &LsmEngine, lsn: &tisql::SharedLsnProvider, rows: usize) {
    for id in 0..rows {
        let mut batch = WriteBatch::new();
        batch.put(
            format!("k{id:04}").into_bytes(),
            format!("v{id:04}").into_bytes(),
        );
        batch.set_commit_ts((id + 1) as Timestamp);
        batch.set_clog_lsn(lsn.alloc_lsn());
        engine
            .write_batch(batch)
            .expect("write batch should succeed");
    }
    engine
        .flush_all_with_active()
        .expect("flush_all_with_active should succeed");
}

fn bench_tablet_engine(c: &mut Criterion) {
    let runtime = common::bench_runtime();
    let dir = TempDir::new().expect("temp dir should build");
    let (lsn, engine) = open_engine(dir.path());
    seed_engine(&engine, &lsn, 1024);

    let mut group = c.benchmark_group("tablet_engine");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("point_get_sst", |b| {
        b.iter(|| {
            let value = runtime
                .block_on(engine.get(b"k0512"))
                .expect("get should succeed");
            black_box(value);
        });
    });

    group.bench_function("range_scan_sst", |b| {
        b.iter(|| {
            let rows = runtime.block_on(async {
                let start = MvccKey::encode(b"k0200", Timestamp::MAX);
                let end = MvccKey::encode(b"k0300", Timestamp::MAX);
                let mut iter = engine
                    .scan_iter(start..end, 0)
                    .expect("scan_iter should succeed");
                let mut rows = 0usize;
                iter.advance().await.expect("advance should succeed");
                while iter.valid() {
                    rows += 1;
                    iter.advance().await.expect("advance should succeed");
                }
                rows
            });
            black_box(rows);
        });
    });

    group.bench_function("write_batch_flush", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().expect("temp dir should build");
                let (lsn, engine) = open_engine(dir.path());
                let mut batch = WriteBatch::new();
                for id in 0..128 {
                    batch.put(
                        format!("bench-{id:04}").into_bytes(),
                        format!("value-{id:04}").into_bytes(),
                    );
                }
                batch.set_commit_ts(1);
                batch.set_clog_lsn(lsn.alloc_lsn());
                (dir, engine, batch)
            },
            |(_dir, engine, batch)| {
                engine
                    .write_batch(batch)
                    .expect("write batch should succeed");
                engine
                    .flush_all_with_active()
                    .expect("flush_all_with_active should succeed");
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_tablet_engine);
criterion_main!(benches);
