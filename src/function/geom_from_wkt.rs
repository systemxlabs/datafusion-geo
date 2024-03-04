use crate::geo::GeometryArrayBuilder;
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
                    // TypeSignature::Exact(vec![
                    //     DataType::Utf8,
                    //     DataType::Int32,
                    // ]),
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
        match args[0].clone() {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(data))) => {
                let wkt = geozero::wkt::Wkt(data);
                let ewkb = wkt.to_ewkb(wkt.dims(), wkt.srid()).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Failed to convert wkt to ewkb, error: {}",
                        e
                    ))
                })?;
                let mut builder = GeometryArrayBuilder::<i32>::new(WkbDialect::Ewkb, 1);
                builder.append_wkb(Some(&ewkb))?;
                Ok(ColumnarValue::Array(Arc::new(builder.build())))
            }
            ColumnarValue::Array(_arr) => {
                todo!()
            }
            _ => unreachable!(),
        }
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
    }
}
