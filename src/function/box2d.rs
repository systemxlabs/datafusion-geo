use crate::geo::{build_box2d_array, Box2d, GeometryArray};
use arrow_array::cast::AsArray;
use arrow_array::Array;
use arrow_schema::DataType;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use geo::BoundingRect;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct Box2dUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl Box2dUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Binary, DataType::LargeBinary],
                Volatility::Immutable,
            ),
            aliases: vec!["box2d".to_string()],
        }
    }
}

impl ScalarUDFImpl for Box2dUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "Box2D"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(Box2d::data_type())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let arr = args[0].clone().into_array(1)?;
        match arr.data_type() {
            DataType::Binary => {
                let wkb_arr = arr.as_binary::<i32>();
                let mut box2d_vec: Vec<Option<Box2d>> = vec![];
                for i in 0..wkb_arr.geom_len() {
                    box2d_vec.push(
                        wkb_arr
                            .geo_value(i)?
                            .and_then(|geom| geom.bounding_rect().map(Box2d::from)),
                    );
                }
                let arr = build_box2d_array(box2d_vec);
                Ok(ColumnarValue::Array(Arc::new(arr)))
            }
            DataType::LargeBinary => {
                let wkb_arr = arr.as_binary::<i64>();
                let mut box2d_vec: Vec<Option<Box2d>> = vec![];
                for i in 0..wkb_arr.geom_len() {
                    box2d_vec.push(
                        wkb_arr
                            .geo_value(i)?
                            .and_then(|geom| geom.bounding_rect().map(Box2d::from)),
                    );
                }
                let arr = build_box2d_array(box2d_vec);
                Ok(ColumnarValue::Array(Arc::new(arr)))
            }
            _ => unreachable!(),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

impl Default for Box2dUdf {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::function::box2d::Box2dUdf;
    use crate::function::GeomFromTextUdf;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn box2d() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeomFromTextUdf::new()));
        ctx.register_udf(ScalarUDF::from(Box2dUdf::new()));
        let df = ctx
            .sql("select Box2D(ST_GeomFromText('LINESTRING(1 2, 3 4, 5 6)'))")
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&df.collect().await.unwrap())
                .unwrap()
                .to_string(),
            "+-----------------------------------------------------------+
| Box2D(ST_GeomFromText(Utf8(\"LINESTRING(1 2, 3 4, 5 6)\"))) |
+-----------------------------------------------------------+
| {xmin: 1.0, ymin: 2.0, xmax: 5.0, ymax: 6.0}              |
+-----------------------------------------------------------+"
        );
    }
}
