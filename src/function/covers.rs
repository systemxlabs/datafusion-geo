use crate::geo::GeometryArray;
use crate::DFResult;
use arrow_array::cast::AsArray;
use arrow_array::{BooleanArray, GenericBinaryArray, OffsetSizeTrait};
use arrow_schema::DataType;
use datafusion_common::{internal_datafusion_err, internal_err};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use geos::Geom;
use rayon::iter::IntoParallelIterator;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct CoversUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl CoversUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                2,
                vec![DataType::Binary, DataType::LargeBinary],
                Volatility::Immutable,
            ),
            aliases: vec!["st_covers".to_string()],
        }
    }
}

impl ScalarUDFImpl for CoversUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_Covers"
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
                covers::<i32, i32>(arr0, arr1)
            }
            (DataType::LargeBinary, DataType::Binary) => {
                let arr0 = arr0.as_binary::<i64>();
                let arr1 = arr1.as_binary::<i32>();
                covers::<i64, i32>(arr0, arr1)
            }
            (DataType::Binary, DataType::LargeBinary) => {
                let arr0 = arr0.as_binary::<i32>();
                let arr1 = arr1.as_binary::<i64>();
                covers::<i32, i64>(arr0, arr1)
            }
            (DataType::LargeBinary, DataType::LargeBinary) => {
                let arr0 = arr0.as_binary::<i64>();
                let arr1 = arr1.as_binary::<i64>();
                covers::<i64, i64>(arr0, arr1)
            }
            _ => unreachable!(),
        }
    }
}

impl Default for CoversUdf {
    fn default() -> Self {
        Self::new()
    }
}

fn covers<O: OffsetSizeTrait, F: OffsetSizeTrait>(
    arr0: &GenericBinaryArray<O>,
    arr1: &GenericBinaryArray<F>,
) -> DFResult<ColumnarValue> {
    let bool_vec = (0..arr0.geom_len())
        .into_par_iter()
        .map(
            |geom_index| match (arr0.geos_value(geom_index)?, arr1.geos_value(geom_index)?) {
                (Some(geom0), Some(geom1)) => {
                    let result = geom0.covers(&geom1).map_err(|e| {
                        internal_datafusion_err!("Failed to do covers, error: {}", e)
                    })?;
                    Ok(Some(result))
                }
                _ => Ok(None),
            },
        )
        .collect::<DFResult<Vec<Option<bool>>>>()?;
    Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(bool_vec))))
}

#[cfg(test)]
mod tests {
    use crate::function::{CoversUdf, GeomFromTextUdf};
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn covers() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeomFromTextUdf::new()));
        ctx.register_udf(ScalarUDF::from(CoversUdf::new()));
        let df = ctx
            .sql("select ST_Covers(ST_GeomFromText('LINESTRING ( 1 1, 0 2 )'), ST_GeomFromText('POINT(1 1)'))")
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&df.collect().await.unwrap())
                .unwrap()
                .to_string(),
            ""
        );
    }
}
