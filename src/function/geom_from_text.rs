use crate::geo::GeometryArrayBuilder;
use arrow_array::cast::AsArray;
use arrow_schema::DataType;
use datafusion_common::DataFusionError;
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use geozero::wkb::WkbDialect;
use geozero::{GeozeroGeometry, ToWkb};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct GeomFromWktUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl GeomFromWktUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["st_geomfromtext".to_string()],
        }
    }
}

impl ScalarUDFImpl for GeomFromWktUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_GeomFromText"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let srid = if args.len() == 2 {
            let ColumnarValue::Scalar(ScalarValue::Int64(Some(srid))) = args[1].clone() else {
                return Err(DataFusionError::Internal(
                    "The second arg should be int32".to_string(),
                ));
            };
            Some(srid as i32)
        } else {
            None
        };
        let arr = args[0].clone().into_array(1)?;
        let string_arr = arr.as_string::<i32>();

        let mut builder = GeometryArrayBuilder::<i32>::new(WkbDialect::Ewkb, 1);
        for value in string_arr.iter() {
            match value {
                None => builder.append_null(),
                Some(data) => {
                    let wkt = geozero::wkt::Wkt(data);
                    let ewkb = wkt.to_ewkb(wkt.dims(), srid.or(wkt.srid())).map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Failed to convert wkt to ewkb, error: {}",
                            e
                        ))
                    })?;
                    builder.append_wkb(Some(&ewkb))?;
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.build())))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

impl Default for GeomFromWktUdf {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::function::GeomFromWktUdf;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn geom_from_wkt() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeomFromWktUdf::new()));
        let df = ctx
            .sql("select ST_GeomFromText('POINT(-71.064544 42.28787)')")
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&df.collect().await.unwrap())
                .unwrap()
                .to_string(),
            "+-----------------------------------------------------+
| ST_GeomFromText(Utf8(\"POINT(-71.064544 42.28787)\")) |
+-----------------------------------------------------+
| 0101000000cb49287d21c451c0f0bf95ecd8244540          |
+-----------------------------------------------------+"
        );

        let df = ctx
            .sql("select ST_GeomFromText('POINT(-71.064544 42.28787)', 4269)")
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&df.collect().await.unwrap())
                .unwrap()
                .to_string(),
            "+-----------------------------------------------------------------+
| ST_GeomFromText(Utf8(\"POINT(-71.064544 42.28787)\"),Int64(4269)) |
+-----------------------------------------------------------------+
| 0101000020ad100000cb49287d21c451c0f0bf95ecd8244540              |
+-----------------------------------------------------------------+"
        );
    }
}
