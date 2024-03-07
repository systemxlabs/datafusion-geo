use crate::geo::GeometryArray;
use crate::DFResult;
use arrow_array::cast::AsArray;
use arrow_array::{GenericBinaryArray, LargeStringArray, OffsetSizeTrait, StringArray};
use arrow_schema::DataType;
use datafusion_common::{internal_datafusion_err, DataFusionError};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use geozero::ToWkt;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct AsTextUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl AsTextUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary]),
                    TypeSignature::Exact(vec![DataType::LargeBinary]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["st_astext".to_string()],
        }
    }
}

impl ScalarUDFImpl for AsTextUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_AsText"
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
                    wkt_vec.push(to_wkt::<i32>(wkb_arr, i)?);
                }

                Ok(ColumnarValue::Array(Arc::new(StringArray::from(wkt_vec))))
            }
            DataType::LargeBinary => {
                let wkb_arr = arr.as_binary::<i64>();

                let mut wkt_vec = vec![];
                for i in 0..wkb_arr.geom_len() {
                    wkt_vec.push(to_wkt::<i64>(wkb_arr, i)?);
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

fn to_wkt<O: OffsetSizeTrait>(
    wkb_arr: &GenericBinaryArray<O>,
    geom_index: usize,
) -> DFResult<Option<String>> {
    let geom = {
        #[cfg(feature = "geos")]
        {
            wkb_arr.geos_value(geom_index)?
        }
        #[cfg(not(feature = "geos"))]
        {
            wkb_arr.geo_value(geom_index)?
        }
    };
    let wkt = match geom {
        Some(geom) => Some(
            geom.to_wkt()
                .map_err(|_| internal_datafusion_err!("Failed to convert geometry to wkt"))?,
        ),
        None => None,
    };
    Ok(wkt)
}

impl Default for AsTextUdf {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::function::{AsTextUdf, GeomFromTextUdf};
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn as_text() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeomFromTextUdf::new()));
        ctx.register_udf(ScalarUDF::from(AsTextUdf::new()));
        let df = ctx
            .sql("select ST_AsText(ST_GeomFromText('POINT(-71.064544 42.28787)'))")
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&df.collect().await.unwrap())
                .unwrap()
                .to_string(),
            "+----------------------------------------------------------------+
| ST_AsText(ST_GeomFromText(Utf8(\"POINT(-71.064544 42.28787)\"))) |
+----------------------------------------------------------------+
| POINT(-71.064544 42.28787)                                     |
+----------------------------------------------------------------+"
        );
    }
}
