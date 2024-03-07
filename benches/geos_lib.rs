use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::prelude::SessionContext;
use datafusion_expr::ScalarUDF;
use datafusion_geo::function::{GeomFromTextUdf, IntersectsUdf};

mod util;

async fn geos_computation(ctx: SessionContext, sql: &str) {
    #[cfg(not(feature = "geos"))]
    {
        panic!("geos bench needs enabling geos feature flag")
    }
    let df = ctx.sql(sql).await.unwrap();
    let _ = df.collect().await.unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = util::create_tokio_runtime();
    let ctx = util::create_session_with_data();
    ctx.register_udf(ScalarUDF::from(IntersectsUdf::new()));
    ctx.register_udf(ScalarUDF::from(GeomFromTextUdf::new()));
    let sql = "select ST_Intersects(geom, ST_GeomFromText('POINT(10 11)')) from geom_table";
    c.bench_function(&format!("geos_bench with sql: {}", sql), |b| {
        b.to_async(&rt).iter(|| geos_computation(ctx.clone(), sql))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
