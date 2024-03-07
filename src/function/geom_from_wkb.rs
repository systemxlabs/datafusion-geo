use crate::geo::GeometryArrayBuilder;
use arrow_array::cast::AsArray;
use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_common::{internal_datafusion_err, internal_err, DataFusionError};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use geozero::wkb::WkbDialect;
use geozero::{GeozeroGeometry, ToWkb};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct GeomFromWkbUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl GeomFromWkbUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary]),
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["st_geomfromwkb".to_string()],
        }
    }
}

impl ScalarUDFImpl for GeomFromWkbUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_GeomFromWKB"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let srid = if args.len() == 2 {
            let ColumnarValue::Scalar(ScalarValue::Int64(Some(srid))) = &args[1] else {
                return internal_err!("The second arg should be int32");
            };
            Some(*srid as i32)
        } else {
            None
        };
        let arr = args[0].clone().into_array(1)?;
        let binary_arr = arr.as_binary::<i32>();

        let mut builder = GeometryArrayBuilder::<i32>::new(WkbDialect::Ewkb, 1);
        for value in binary_arr.iter() {
            match value {
                None => builder.append_null(),
                Some(data) => {
                    let wkb = geozero::wkb::Wkb(data);
                    let ewkb = wkb.to_ewkb(wkb.dims(), srid).map_err(|e| {
                        internal_datafusion_err!("Failed to convert wkb to ewkb, error: {}", e)
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

impl Default for GeomFromWkbUdf {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::function::geom_from_wkb::GeomFromWkbUdf;
    use crate::function::AsTextUdf;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn geom_from_wkb() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeomFromWkbUdf::new()));
        ctx.register_udf(ScalarUDF::from(AsTextUdf::new()));
        let df = ctx
            .sql("select ST_AsText(ST_GeomFromWKB(0x0101000000cb49287d21c451c0f0bf95ecd8244540))")
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&df.collect().await.unwrap())
                .unwrap()
                .to_string(),
            "+---------------------------------------------------------------------------------------------------------+
| ST_AsText(ST_GeomFromWKB(Binary(\"1,1,0,0,0,203,73,40,125,33,196,81,192,240,191,149,236,216,36,69,64\"))) |
+---------------------------------------------------------------------------------------------------------+
| POINT(-71.064544 42.28787)                                                                              |
+---------------------------------------------------------------------------------------------------------+"
        );
    }

    #[cfg(feature = "geos")]
    #[tokio::test]
    async fn geom_from_wkb_with_srid() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeomFromWkbUdf::new()));
        ctx.register_udf(ScalarUDF::from(crate::function::AsEwktUdf::new()));
        let df = ctx
            .sql("select ST_AsEWKT(ST_GeomFromWKB(0x0101000000cb49287d21c451c0f0bf95ecd8244540, 4269))")
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&df.collect().await.unwrap())
                .unwrap()
                .to_string(),
            "+---------------------------------------------------------------------------------------------------------------------+
| ST_AsEWKT(ST_GeomFromWKB(Binary(\"1,1,0,0,0,203,73,40,125,33,196,81,192,240,191,149,236,216,36,69,64\"),Int64(4269))) |
+---------------------------------------------------------------------------------------------------------------------+
| SRID=4269;POINT(-71.064544 42.28787)                                                                                |
+---------------------------------------------------------------------------------------------------------------------+"
        );
    }
}
