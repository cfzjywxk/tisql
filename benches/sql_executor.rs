use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use tisql::QueryResult;

mod common;

fn bench_sql_executor(c: &mut Criterion) {
    let runtime = common::bench_runtime();
    let (_dir, db) = common::open_bench_db();
    runtime.block_on(common::seed_sql_table(&db, "bench_sql", 1024));
    runtime.block_on(async {
        db.execute_query("CREATE TABLE bench_sql_insert (id INT PRIMARY KEY, value INT)")
            .await
            .expect("insert bench table should be created");
    });

    let mut group = c.benchmark_group("sql_executor");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("point_select", |b| {
        b.iter(|| {
            let result = runtime.block_on(async {
                db.execute_query("SELECT value FROM bench_sql WHERE id = 512")
                    .await
                    .expect("point select should succeed")
            });
            match result {
                QueryResult::Rows { data, .. } => black_box(data.len()),
                other => panic!("expected rows, got {other:?}"),
            }
        });
    });

    group.bench_function("range_scan", |b| {
        b.iter(|| {
            let result = runtime.block_on(async {
                db.execute_query(
                    "SELECT id, value FROM bench_sql WHERE id >= 256 AND id < 384 ORDER BY id",
                )
                .await
                .expect("range scan should succeed")
            });
            match result {
                QueryResult::Rows { data, .. } => black_box(data.len()),
                other => panic!("expected rows, got {other:?}"),
            }
        });
    });

    let next_id = AtomicU64::new(1);
    group.bench_function("insert_autocommit", |b| {
        b.iter(|| {
            let id = next_id.fetch_add(1, Ordering::Relaxed);
            let effect = runtime.block_on(async {
                db.execute_query(&format!(
                    "INSERT INTO bench_sql_insert VALUES ({}, {})",
                    id,
                    id * 10
                ))
                .await
                .expect("insert should succeed")
            });
            black_box(effect);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_sql_executor);
criterion_main!(benches);
