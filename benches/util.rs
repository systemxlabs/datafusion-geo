use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_geo::geo::GeometryArrayBuilder;
use geo::line_string;
use geoarrow::array::WKBArray;
use geoarrow::trait_::IntoArrow;
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
    let mut geoarrow_batches = vec![];
    for i in 0..1000000 {
        let i = i as f64;
        let linestring = line_string![
            (x: i, y: i + 1.0),
            (x: i + 2.0, y: i + 3.0),
            (x: i + 4.0, y: i + 5.0),
        ];

        let builder: GeometryArrayBuilder<i32> = vec![Some(linestring.clone())].as_slice().into();
        let record = RecordBatch::try_new(schema.clone(), vec![Arc::new(builder.build())]).unwrap();
        batches.push(record);

        let wkb_arr: WKBArray<i32> = vec![Some(geo::Geometry::LineString(linestring))]
            .as_slice()
            .try_into()
            .unwrap();
        let geoarrow_record =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(wkb_arr.into_arrow())]).unwrap();
        geoarrow_batches.push(geoarrow_record);
    }
    let mem_table = MemTable::try_new(schema.clone(), vec![batches]).unwrap();
    let geoarrow_mem_table = MemTable::try_new(schema.clone(), vec![geoarrow_batches]).unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("geom_table", Arc::new(mem_table))
        .unwrap();
    ctx.register_table("geoarrow_table", Arc::new(geoarrow_mem_table))
        .unwrap();
    ctx
}
