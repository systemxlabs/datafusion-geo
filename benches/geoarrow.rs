#![allow(deprecated)]

use arrow_array::cast::downcast_array;
use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, Int64Array, StringArray};
use arrow_schema::DataType;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::functions::make_scalar_function;
use datafusion::prelude::SessionContext;
use datafusion_common::DataFusionError;
use datafusion_expr::{
    create_udf, ReturnTypeFunction, ScalarUDF, Signature, TypeSignature, Volatility,
};
use datafusion_geo::DFResult;
use geo::Intersects;
use geoarrow::array::WKBArray;
use geoarrow::geo_traits::GeometryTrait;
use geoarrow::trait_::GeometryArrayAccessor;
use geoarrow::GeometryArrayTrait;
use geozero::wkt::WktStr;
use geozero::{CoordDimensions, ToWkb};
use std::sync::Arc;

mod util;

async fn geoarrow_intersects(ctx: SessionContext, sql: &str) {
    let df = ctx.sql(sql).await.unwrap();
    let _ = df.collect().await.unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = util::create_tokio_runtime();
    let ctx = util::create_session_with_data();
    ctx.register_udf(geom_from_text());
    ctx.register_udf(intersects());

    let sql = "select ST_Intersects(geom, ST_GeomFromText('POINT(10 11)')) from geoarrow_table";
    c.bench_function(&format!("geoarrow_bench with sql: {}", sql), |b| {
        b.to_async(&rt)
            .iter(|| geoarrow_intersects(ctx.clone(), sql))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

pub fn geom_from_text() -> ScalarUDF {
    let st_geomfromtext = make_scalar_function(st_geomfromtext);

    let signature = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
        ],
        Volatility::Immutable,
    );
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Binary)));

    ScalarUDF::new(
        "st_geomfromtext",
        &signature,
        &return_type,
        &st_geomfromtext,
    )
}

fn st_geomfromtext(args: &[ArrayRef]) -> DFResult<Arc<dyn Array>> {
    let wkt_arr = args[0].as_ref();
    let wkt_arr = downcast_array::<StringArray>(wkt_arr);

    if args.len() == 2 {
        let srid_arr = args[1].as_ref();
        let srid_arr = downcast_array::<Int64Array>(srid_arr);

        let wkb_array: WKBArray<_> = wkt_arr
            .iter()
            .zip(srid_arr.iter())
            .map(|(wkt_opt, srid_opt)| {
                wkt_opt.and_then(|str| {
                    let wkt = WktStr(str);
                    wkt.to_ewkb(CoordDimensions::xy(), srid_opt.map(|a| a as i32))
                        .ok()
                })
            })
            .collect::<BinaryArray>()
            .into();

        Ok(wkb_array.into_array_ref())
    } else {
        let wkb_array: WKBArray<_> = wkt_arr
            .iter()
            .map(|opt| {
                opt.and_then(|str| {
                    let wkt = WktStr(str);
                    wkt.to_wkb(CoordDimensions::xy()).ok()
                })
            })
            .collect::<BinaryArray>()
            .into();

        Ok(wkb_array.into_array_ref())
    }
}

pub fn intersects() -> ScalarUDF {
    let intersects = |args: &[ArrayRef]| -> datafusion::error::Result<Arc<dyn Array>> {
        if args.len() != 2 {
            return Err(DataFusionError::Execution(
                "st_intersects must have only three args.".to_string(),
            ));
        }
        let Ok(wkb_array_a) = WKBArray::<i32>::try_from(&args[0] as &dyn Array) else {
            return Err(DataFusionError::Execution(
                "st_intersects input 0 can not convert to WKBArray<i32>.".to_string(),
            ));
        };

        let Ok(wkb_array_b) = WKBArray::<i32>::try_from(&args[1] as &dyn Array) else {
            return Err(DataFusionError::Execution(
                "st_intersects input 1 can not convert to WKBArray<i32>.".to_string(),
            ));
        };
        let result: BooleanArray = wkb_array_a
            .iter_geo()
            .zip(wkb_array_b.iter_geo())
            .map(|opt| match opt {
                (Some(geom_a), Some(geom_b)) => Some(geom_a.intersects(&geom_b)),
                _ => None,
            })
            .collect();
        Ok(Arc::new(result))
    };

    let translate = make_scalar_function(intersects);

    create_udf(
        "st_intersects",
        vec![DataType::Binary, DataType::Binary],
        Arc::new(DataType::Boolean),
        Volatility::Immutable,
        translate,
    )
}
