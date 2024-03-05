use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_expr::ScalarUDF;
use datafusion_geo::function::{GeomFromWktUdf, IntersectsUdf};
use datafusion_geo::geo::GeometryArrayBuilder;
use geo::line_string;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub fn create_tokio_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap()
}

pub fn create_session_with_data() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "geom",
        DataType::Binary,
        true,
    )]));

    let mut batches = vec![];
    for i in 0..1000000 {
        let i = i as f64;
        let builder: GeometryArrayBuilder<i32> = vec![Some(line_string![
            (x: i, y: i + 1.0),
            (x: i + 2.0, y: i + 3.0),
            (x: i + 4.0, y: i + 5.0),
        ])]
        .as_slice()
        .into();
        let record = RecordBatch::try_new(schema.clone(), vec![Arc::new(builder.build())]).unwrap();
        batches.push(record);
    }
    let mem_table = MemTable::try_new(schema.clone(), vec![batches]).unwrap();

    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::from(IntersectsUdf::new()));
    ctx.register_udf(ScalarUDF::from(GeomFromWktUdf::new()));
    ctx.register_table("geom_table", Arc::new(mem_table))
        .unwrap();
    ctx
}
