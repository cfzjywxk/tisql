use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::TempDir;
use tisql::testkit::{
    ConcurrencyManager, FileClogConfig, FileClogService, IlogConfig, IlogService, IoService,
    LocalTso, LsmConfig, LsmEngine, TabletTxnStorage, TransactionService, Version,
};
use tisql::{new_lsn_provider, ConflictWaitConfig, TxnScanCursor, TxnService};
use tokio::runtime::Runtime;

mod common;

fn build_txn_service(
    runtime: &Runtime,
) -> (
    TempDir,
    Arc<TransactionService<TabletTxnStorage<LsmEngine>, FileClogService, LocalTso>>,
) {
    let dir = TempDir::new().expect("temp dir should build");
    let lsn = new_lsn_provider();
    let io = IoService::new_for_test(32).expect("io service should open");
    let ilog = Arc::new(
        IlogService::open(
            IlogConfig::new(dir.path()),
            Arc::clone(&lsn),
            Arc::clone(&io),
            runtime.handle(),
        )
        .expect("ilog should open"),
    );
    let storage = Arc::new(
        LsmEngine::open_with_recovery(
            LsmConfig::new(dir.path()),
            Arc::clone(&lsn),
            ilog,
            Version::new(),
            Arc::clone(&io),
        )
        .expect("storage should open"),
    );
    let clog = Arc::new(
        FileClogService::open_with_lsn_provider(
            FileClogConfig::with_dir(dir.path()),
            Arc::clone(&lsn),
            io,
            runtime.handle(),
        )
        .expect("clog should open"),
    );
    let tso = Arc::new(LocalTso::new(1));
    let cm = Arc::new(ConcurrencyManager::new(0));
    let txn_storage = Arc::new(TabletTxnStorage::new(Arc::clone(&storage), Arc::clone(&cm)));
    let service = Arc::new(TransactionService::new_with_conflict_wait_config(
        txn_storage,
        clog,
        tso,
        cm,
        ConflictWaitConfig::default(),
    ));
    (dir, service)
}

fn seed_committed_rows(
    runtime: &Runtime,
    txn_service: &TransactionService<TabletTxnStorage<LsmEngine>, FileClogService, LocalTso>,
    table_id: u64,
    rows: usize,
) {
    runtime.block_on(async {
        let mut ctx = txn_service.begin(false).expect("begin should succeed");
        for id in 0..rows {
            txn_service
                .put(
                    &mut ctx,
                    table_id,
                    format!("k{id:04}").as_bytes(),
                    format!("v{id:04}").into_bytes(),
                )
                .await
                .expect("seed put should succeed");
        }
        txn_service
            .commit(ctx)
            .await
            .expect("seed commit should succeed");
    });
}

fn bench_transaction_service(c: &mut Criterion) {
    let runtime = common::bench_runtime();
    let (_dir, txn_service) = build_txn_service(&runtime);
    let table_id = 42_u64;
    seed_committed_rows(&runtime, txn_service.as_ref(), table_id, 256);

    let mut group = c.benchmark_group("transaction_service");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(1));

    let next_key = AtomicU64::new(10_000);
    group.bench_function("put_commit", |b| {
        b.iter(|| {
            let key_id = next_key.fetch_add(1, Ordering::Relaxed);
            let commit_info = runtime.block_on(async {
                let mut ctx = txn_service.begin(false).expect("begin should succeed");
                txn_service
                    .put(
                        &mut ctx,
                        table_id,
                        format!("bench-{key_id}").as_bytes(),
                        key_id.to_string().into_bytes(),
                    )
                    .await
                    .expect("put should succeed");
                txn_service
                    .commit(ctx)
                    .await
                    .expect("commit should succeed")
            });
            black_box(commit_info.commit_ts);
        });
    });

    group.bench_function("readonly_get", |b| {
        b.iter(|| {
            let value = runtime.block_on(async {
                let ctx = txn_service.begin(true).expect("begin should succeed");
                txn_service
                    .get(&ctx, table_id, b"k0128")
                    .await
                    .expect("get should succeed")
            });
            black_box(value);
        });
    });

    group.bench_function("range_scan", |b| {
        b.iter(|| {
            let rows = runtime.block_on(async {
                let ctx = txn_service.begin(true).expect("begin should succeed");
                let mut cursor = txn_service
                    .scan(&ctx, table_id, b"k0100".to_vec()..b"k0200".to_vec())
                    .expect("scan should succeed");
                let mut rows = 0usize;
                cursor.advance().await.expect("advance should succeed");
                while cursor.current().is_some() {
                    rows += 1;
                    cursor.advance().await.expect("advance should succeed");
                }
                rows
            });
            black_box(rows);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_transaction_service);
criterion_main!(benches);
