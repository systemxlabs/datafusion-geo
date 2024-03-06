use crate::geo::GeometryArray;
use crate::DFResult;
use arrow_array::cast::AsArray;
use arrow_array::{Array, BooleanArray, GenericBinaryArray, OffsetSizeTrait};
use arrow_schema::DataType;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
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
        let arr0 = args[0].clone().into_array(1)?;
        let arr1 = args[1].clone().into_array(1)?;
        match (arr0.data_type(), arr1.data_type()) {
            (DataType::Binary, DataType::Binary) => {
                let arr0 = arr0.as_binary::<i32>();
                let arr1 = arr1.as_binary::<i32>();
                let geom_len = std::cmp::min(arr0.geom_len(), arr1.geom_len());
                let mut bool_vec = vec![];
                for i in 0..geom_len {
                    bool_vec.push(intersects::<i32, i32>(arr0, arr1, i)?);
                }
                Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(bool_vec))))
            }
            (DataType::LargeBinary, DataType::Binary) => {
                let arr0 = arr0.as_binary::<i64>();
                let arr1 = arr1.as_binary::<i32>();
                let geom_len = std::cmp::min(arr0.geom_len(), arr1.geom_len());
                let mut bool_vec = vec![];
                for i in 0..geom_len {
                    bool_vec.push(intersects::<i64, i32>(arr0, arr1, i)?);
                }
                Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(bool_vec))))
            }
            (DataType::Binary, DataType::LargeBinary) => {
                let arr0 = arr0.as_binary::<i32>();
                let arr1 = arr1.as_binary::<i64>();
                let geom_len = std::cmp::min(arr0.geom_len(), arr1.geom_len());
                let mut bool_vec = vec![];
                for i in 0..geom_len {
                    bool_vec.push(intersects::<i32, i64>(arr0, arr1, i)?);
                }
                Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(bool_vec))))
            }
            (DataType::LargeBinary, DataType::LargeBinary) => {
                let arr0 = arr0.as_binary::<i64>();
                let arr1 = arr1.as_binary::<i64>();
                let geom_len = std::cmp::min(arr0.geom_len(), arr1.geom_len());
                let mut bool_vec = vec![];
                for i in 0..geom_len {
                    bool_vec.push(intersects::<i64, i64>(arr0, arr1, i)?);
                }
                Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(bool_vec))))
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
    geom_index: usize,
) -> DFResult<Option<bool>> {
    #[cfg(feature = "geos")]
    {
        use datafusion_common::DataFusionError;
        use geos::Geom;
        match (arr0.geos_value(geom_index)?, arr1.geos_value(geom_index)?) {
            (Some(geom0), Some(geom1)) => {
                let result = geom0.intersects(&geom1).map_err(|e| {
                    DataFusionError::Internal(format!("Failed to do intersects, error: {}", e))
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
}

#[cfg(test)]
mod tests {
    use crate::function::{GeomFromTextUdf, IntersectsUdf};
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::SessionContext;

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
}
