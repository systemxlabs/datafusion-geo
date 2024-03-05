use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::prelude::SessionContext;

mod util;

async fn geo_intersects(ctx: SessionContext, sql: &str) {
    #[cfg(feature = "geos")]
    {
        panic!("geo bench needs disabling geos feature flag")
    }
    let df = ctx.sql(sql).await.unwrap();
    let _ = df.collect().await.unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = util::create_tokio_runtime();
    let ctx = util::create_session_with_data();
    let sql = "select ST_Intersects(geom, ST_GeomFromText('POINT(10 11)')) from geom_table";
    c.bench_function(&format!("geo_bench with sql: {}", sql), |b| {
        b.to_async(&rt).iter(|| geo_intersects(ctx.clone(), sql))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
