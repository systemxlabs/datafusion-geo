use crate::geo::GeometryArray;
use crate::DFResult;
use arrow_array::cast::AsArray;
use arrow_array::{Array, BooleanArray, GenericBinaryArray, OffsetSizeTrait};
use arrow_schema::DataType;
use datafusion_common::{internal_err, DataFusionError};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use rayon::prelude::*;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct IntersectsUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl IntersectsUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                2,
                vec![DataType::Binary, DataType::LargeBinary],
                Volatility::Immutable,
            ),
            aliases: vec!["st_intersects".to_string()],
        }
    }
}

impl ScalarUDFImpl for IntersectsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_Intersects"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let (arr0, arr1) = match (args[0].clone(), args[1].clone()) {
            (ColumnarValue::Array(arr0), ColumnarValue::Array(arr1)) => (arr0, arr1),
            (ColumnarValue::Array(arr0), ColumnarValue::Scalar(scalar)) => {
                (arr0.clone(), scalar.to_array_of_size(arr0.len())?)
            }
            (ColumnarValue::Scalar(scalar), ColumnarValue::Array(arr1)) => {
                (scalar.to_array_of_size(arr1.len())?, arr1)
            }
            (ColumnarValue::Scalar(scalar0), ColumnarValue::Scalar(scalar1)) => {
                (scalar0.to_array_of_size(1)?, scalar1.to_array_of_size(1)?)
            }
        };
        if arr0.len() != arr1.len() {
            return internal_err!("Two arrays length is not same");
        }
        match (arr0.data_type(), arr1.data_type()) {
            (DataType::Binary, DataType::Binary) => {
                let arr0 = arr0.as_binary::<i32>();
                let arr1 = arr1.as_binary::<i32>();
                intersects::<i32, i32>(arr0, arr1)
            }
            (DataType::LargeBinary, DataType::Binary) => {
                let arr0 = arr0.as_binary::<i64>();
                let arr1 = arr1.as_binary::<i32>();
                intersects::<i64, i32>(arr0, arr1)
            }
            (DataType::Binary, DataType::LargeBinary) => {
                let arr0 = arr0.as_binary::<i32>();
                let arr1 = arr1.as_binary::<i64>();
                intersects::<i32, i64>(arr0, arr1)
            }
            (DataType::LargeBinary, DataType::LargeBinary) => {
                let arr0 = arr0.as_binary::<i64>();
                let arr1 = arr1.as_binary::<i64>();
                intersects::<i64, i64>(arr0, arr1)
            }
            _ => unreachable!(),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

impl Default for IntersectsUdf {
    fn default() -> Self {
        Self::new()
    }
}

fn intersects<O: OffsetSizeTrait, F: OffsetSizeTrait>(
    arr0: &GenericBinaryArray<O>,
    arr1: &GenericBinaryArray<F>,
) -> DFResult<ColumnarValue> {
    let bool_vec = (0..arr0.geom_len())
        .into_par_iter()
        .map(|geom_index| {
            #[cfg(feature = "geos")]
            {
                use datafusion_common::internal_datafusion_err;
                use geos::Geom;
                match (arr0.geos_value(geom_index)?, arr1.geos_value(geom_index)?) {
                    (Some(geom0), Some(geom1)) => {
                        let result = geom0.intersects(&geom1).map_err(|e| {
                            internal_datafusion_err!("Failed to do intersects, error: {}", e)
                        })?;
                        Ok(Some(result))
                    }
                    _ => Ok(None),
                }
            }
            #[cfg(not(feature = "geos"))]
            {
                use geo::Intersects;
                match (arr0.geo_value(geom_index)?, arr1.geo_value(geom_index)?) {
                    (Some(geom0), Some(geom1)) => Ok(Some(geom0.intersects(&geom1))),
                    _ => Ok(None),
                }
            }
        })
        .collect::<DFResult<Vec<Option<bool>>>>()?;
    Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(bool_vec))))
}

#[cfg(test)]
mod tests {
    use crate::function::{GeomFromTextUdf, IntersectsUdf};
    use crate::geo::GeometryArrayBuilder;
    use arrow::util::pretty::pretty_format_batches;
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::SessionContext;
    use geo::line_string;
    use std::sync::Arc;

    #[tokio::test]
    async fn intersects() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeomFromTextUdf::new()));
        ctx.register_udf(ScalarUDF::from(IntersectsUdf::new()));
        let df = ctx
            .sql("select ST_Intersects(ST_GeomFromText('POINT(1 1)'), ST_GeomFromText('LINESTRING ( 1 1, 0 2 )'))")
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&df.collect().await.unwrap())
                .unwrap()
                .to_string(),
            "+-----------------------------------------------------------------------------------------------------+
| ST_Intersects(ST_GeomFromText(Utf8(\"POINT(1 1)\")),ST_GeomFromText(Utf8(\"LINESTRING ( 1 1, 0 2 )\"))) |
+-----------------------------------------------------------------------------------------------------+
| true                                                                                                |
+-----------------------------------------------------------------------------------------------------+"
        );
    }

    #[tokio::test]
    async fn intersects_table() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeomFromTextUdf::new()));
        ctx.register_udf(ScalarUDF::from(IntersectsUdf::new()));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "geom",
            DataType::Binary,
            true,
        )]));

        let mut linestrint_vec = vec![];
        for i in 0..3 {
            let i = i as f64;
            let linestring = line_string![
                (x: i, y: i + 1.0),
                (x: i + 2.0, y: i + 3.0),
                (x: i + 4.0, y: i + 5.0),
            ];
            linestrint_vec.push(Some(linestring));
        }
        let builder: GeometryArrayBuilder<i32> = linestrint_vec.as_slice().into();
        let record = RecordBatch::try_new(schema.clone(), vec![Arc::new(builder.build())]).unwrap();

        let mem_table =
            MemTable::try_new(schema.clone(), vec![vec![record.clone()], vec![record]]).unwrap();
        ctx.register_table("geom_table", Arc::new(mem_table))
            .unwrap();

        let df = ctx
            .sql("select ST_Intersects(geom, ST_GeomFromText('POINT(0 1)')) from geom_table")
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&df.collect().await.unwrap())
                .unwrap()
                .to_string(),
            "+--------------------------------------------------------------------+
| ST_Intersects(geom_table.geom,ST_GeomFromText(Utf8(\"POINT(0 1)\"))) |
+--------------------------------------------------------------------+
| true                                                               |
| false                                                              |
| false                                                              |
| true                                                               |
| false                                                              |
| false                                                              |
+--------------------------------------------------------------------+"
        );
    }
}
