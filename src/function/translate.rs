use crate::geo::{GeometryArray, GeometryArrayBuilder};
use arrow_array::cast::AsArray;
use arrow_schema::DataType;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use geo::Translate;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct TranslateUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl TranslateUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Binary,
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::LargeBinary,
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["st_translate".to_string()],
        }
    }
}

impl ScalarUDFImpl for TranslateUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_Translate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let ColumnarValue::Scalar(ScalarValue::Float64(Some(x_offset))) = args[1] else {
            return Err(DataFusionError::Internal(
                "The second arg should be f64 scalar".to_string(),
            ));
        };
        let ColumnarValue::Scalar(ScalarValue::Float64(Some(y_offset))) = args[2] else {
            return Err(DataFusionError::Internal(
                "The third arg should be f64 scalar".to_string(),
            ));
        };

        match args[0].data_type() {
            DataType::Binary => {
                let arr = args[0].clone().into_array(1)?;
                let wkb_arr = arr.as_binary::<i32>();

                let mut geom_vec = vec![];
                for i in 0..wkb_arr.geom_len() {
                    geom_vec.push(
                        wkb_arr
                            .geo_value(i)?
                            .map(|geom| geom.translate(x_offset, y_offset)),
                    );
                }

                let builder: GeometryArrayBuilder<i32> = geom_vec.as_slice().into();
                Ok(ColumnarValue::Array(Arc::new(builder.build())))
            }
            DataType::LargeBinary => {
                let arr = args[0].clone().into_array(0)?;
                let wkb_arr = arr.as_binary::<i64>();

                let mut geom_vec = vec![];
                for i in 0..wkb_arr.geom_len() {
                    geom_vec.push(
                        wkb_arr
                            .geo_value(i)?
                            .map(|geom| geom.translate(x_offset, y_offset)),
                    );
                }
                let builder: GeometryArrayBuilder<i64> = geom_vec.as_slice().into();
                Ok(ColumnarValue::Array(Arc::new(builder.build())))
            }
            _ => unreachable!(),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

impl Default for TranslateUdf {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::function::{GeomFromWktUdf, TranslateUdf};
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn translate() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeomFromWktUdf::new()));
        ctx.register_udf(ScalarUDF::from(TranslateUdf::new()));
        let df = ctx
            .sql("select ST_Translate(ST_GeomFromText('POINT(-71.064544 42.28787)'), 1.0, 2.0)")
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&df.collect().await.unwrap())
                .unwrap()
                .to_string(),
            "+-----------------------------------------------------------------------------------------+
| ST_Translate(ST_GeomFromText(Utf8(\"POINT(-71.064544 42.28787)\")),Float64(1),Float64(2)) |
+-----------------------------------------------------------------------------------------+
| 020101000000cb49287d218451c0f0bf95ecd8244640                                            |
+-----------------------------------------------------------------------------------------+"
        );
    }
}
