use crate::geo::{GeometryArray, GeometryArrayBuilder};
use crate::DFResult;
use arrow_array::cast::AsArray;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use arrow_schema::DataType;
use datafusion_common::{internal_datafusion_err, internal_err, DataFusionError};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use geos::Geom;
use rayon::iter::IntoParallelIterator;
use rayon::prelude::*;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct SplitUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl SplitUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                2,
                vec![DataType::Binary, DataType::LargeBinary],
                Volatility::Immutable,
            ),
            aliases: vec!["st_split".to_string()],
        }
    }
}

impl ScalarUDFImpl for SplitUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_Split"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(arg_types[0].clone())
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
                split::<i32, i32>(arr0, arr1)
            }
            (DataType::LargeBinary, DataType::Binary) => {
                let arr0 = arr0.as_binary::<i64>();
                let arr1 = arr1.as_binary::<i32>();
                split::<i64, i32>(arr0, arr1)
            }
            (DataType::Binary, DataType::LargeBinary) => {
                let arr0 = arr0.as_binary::<i32>();
                let arr1 = arr1.as_binary::<i64>();
                split::<i32, i64>(arr0, arr1)
            }
            (DataType::LargeBinary, DataType::LargeBinary) => {
                let arr0 = arr0.as_binary::<i64>();
                let arr1 = arr1.as_binary::<i64>();
                split::<i64, i64>(arr0, arr1)
            }
            _ => unreachable!(),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

impl Default for SplitUdf {
    fn default() -> Self {
        Self::new()
    }
}

fn split<O: OffsetSizeTrait, F: OffsetSizeTrait>(
    arr0: &GenericBinaryArray<O>,
    arr1: &GenericBinaryArray<F>,
) -> DFResult<ColumnarValue> {
    let geom_vec = (0..arr0.geom_len())
        .into_par_iter()
        .map(
            |geom_index| match (arr0.geos_value(geom_index)?, arr1.geos_value(geom_index)?) {
                (Some(geom0), Some(geom1)) => {
                    let boundary = geom0.boundary().map_err(|e| {
                        internal_datafusion_err!("Failed to do boundary, error: {}", e)
                    })?;
                    let union = boundary.union(&geom1).map_err(|e| {
                        internal_datafusion_err!("Failed to do union, error: {}", e)
                    })?;
                    let (result, ..) = union.polygonize_full().map_err(|e| {
                        internal_datafusion_err!("Failed to do polygonize_full, error: {}", e)
                    })?;

                    Ok(Some(result))
                }
                _ => Ok(None),
            },
        )
        .collect::<DFResult<Vec<Option<geos::Geometry>>>>()?;
    let builder = GeometryArrayBuilder::<O>::from(geom_vec.as_slice());
    Ok(ColumnarValue::Array(Arc::new(builder.build())))
}

#[cfg(test)]
mod tests {
    use crate::function::{GeomFromTextUdf, SplitUdf};
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn split() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeomFromTextUdf::new()));
        ctx.register_udf(ScalarUDF::from(SplitUdf::new()));
        let df = ctx
            .sql("select ST_Split(ST_GeomFromText('LINESTRING ( 0 0, 1 1, 2 2 )'), ST_GeomFromText('POINT(1 1)'))")
            .await
            .unwrap();
        // FIXME fix split implementation
        // assert_eq!(
        //     pretty_format_batches(&df.collect().await.unwrap())
        //         .unwrap()
        //         .to_string(),
        //     ""
        // );
    }
}
