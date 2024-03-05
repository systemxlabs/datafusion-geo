use crate::geo::GeometryArray;
use crate::DFResult;
use arrow_array::cast::AsArray;
use arrow_array::{GenericBinaryArray, LargeStringArray, OffsetSizeTrait, StringArray};
use arrow_schema::DataType;
use datafusion_common::DataFusionError;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use geozero::{GeozeroGeometry, ToWkt};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct AsEwktUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl AsEwktUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary]),
                    TypeSignature::Exact(vec![DataType::LargeBinary]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["st_asewkt".to_string()],
        }
    }
}

impl ScalarUDFImpl for AsEwktUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_AsEWKT"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        match arg_types[0] {
            DataType::Binary => Ok(DataType::Utf8),
            DataType::LargeBinary => Ok(DataType::LargeUtf8),
            _ => unreachable!(),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let arr = args[0].clone().into_array(1)?;
        match args[0].data_type() {
            DataType::Binary => {
                let wkb_arr = arr.as_binary::<i32>();

                let mut wkt_vec = vec![];
                for i in 0..wkb_arr.geom_len() {
                    wkt_vec.push(to_ewkt::<i32>(wkb_arr, i)?);
                }

                Ok(ColumnarValue::Array(Arc::new(StringArray::from(wkt_vec))))
            }
            DataType::LargeBinary => {
                let wkb_arr = arr.as_binary::<i64>();

                let mut wkt_vec = vec![];
                for i in 0..wkb_arr.geom_len() {
                    wkt_vec.push(to_ewkt::<i64>(wkb_arr, i)?);
                }

                Ok(ColumnarValue::Array(Arc::new(LargeStringArray::from(
                    wkt_vec,
                ))))
            }
            _ => unreachable!(),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn to_ewkt<O: OffsetSizeTrait>(
    wkb_arr: &GenericBinaryArray<O>,
    geom_index: usize,
) -> DFResult<Option<String>> {
    let geom = wkb_arr.geos_value(geom_index)?;
    let wkt = match geom {
        Some(geom) => Some(geom.to_ewkt(geom.srid()).map_err(|_| {
            DataFusionError::Internal("Failed to convert geometry to ewkt".to_string())
        })?),
        None => None,
    };
    Ok(wkt)
}

impl Default for AsEwktUdf {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::function::{AsEwktUdf, GeomFromWktUdf};
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn as_ewkt() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeomFromWktUdf::new()));
        ctx.register_udf(ScalarUDF::from(AsEwktUdf::new()));
        let df = ctx
            .sql("select ST_AsEWKT(ST_GeomFromText('POINT(-71.064544 42.28787)', 4269))")
            .await
            .unwrap();
        let _ = df.clone().show().await;
        assert_eq!(
            pretty_format_batches(&df.collect().await.unwrap())
                .unwrap()
                .to_string(),
            ""
        );
    }
}
