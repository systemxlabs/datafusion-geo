use crate::geo::GeometryArray;
use arrow_array::cast::AsArray;
use arrow_array::{Array, Int32Array};
use arrow_schema::DataType;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use geozero::GeozeroGeometry;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct SridUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl SridUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Binary, DataType::LargeBinary],
                Volatility::Immutable,
            ),
            aliases: vec!["st_srid".to_string()],
        }
    }
}

impl ScalarUDFImpl for SridUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_SRID"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let arr = args[0].clone().into_array(1)?;
        match arr.data_type() {
            DataType::Binary => {
                let wkb_arr = arr.as_binary::<i32>();
                let mut srid_vec = vec![];
                for i in 0..wkb_arr.geom_len() {
                    srid_vec.push(wkb_arr.geos_value(i)?.and_then(|geom| geom.srid()));
                }
                Ok(ColumnarValue::Array(Arc::new(Int32Array::from(srid_vec))))
            }
            DataType::LargeBinary => {
                let wkb_arr = arr.as_binary::<i64>();
                let mut srid_vec = vec![];
                for i in 0..wkb_arr.geom_len() {
                    srid_vec.push(wkb_arr.geos_value(i)?.and_then(|geom| geom.srid()));
                }
                Ok(ColumnarValue::Array(Arc::new(Int32Array::from(srid_vec))))
            }
            _ => unreachable!(),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

impl Default for SridUdf {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::function::{GeomFromWktUdf, SridUdf};
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn srid() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeomFromWktUdf::new()));
        ctx.register_udf(ScalarUDF::from(SridUdf::new()));
        let df = ctx
            .sql("select ST_SRID(ST_GeomFromText('POINT(1 1)', 4269))")
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&df.collect().await.unwrap())
                .unwrap()
                .to_string(),
            "+----------------------------------------------------------+
| ST_SRID(ST_GeomFromText(Utf8(\"POINT(1 1)\"),Int64(4269))) |
+----------------------------------------------------------+
| 4269                                                     |
+----------------------------------------------------------+"
        );
    }
}
