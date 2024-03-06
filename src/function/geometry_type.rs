use crate::geo::GeometryArray;
use arrow_array::cast::AsArray;
use arrow_array::{Array, StringArray};
use arrow_schema::DataType;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct GeometryTypeUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl GeometryTypeUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Binary, DataType::LargeBinary],
                Volatility::Immutable,
            ),
            aliases: vec!["st_geometrytype".to_string()],
        }
    }
}

impl ScalarUDFImpl for GeometryTypeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_GeometryType"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let arr = args[0].clone().into_array(1)?;
        match arr.data_type() {
            DataType::Binary => {
                let wkb_arr = arr.as_binary::<i32>();
                let mut type_vec = vec![];
                for i in 0..wkb_arr.geom_len() {
                    type_vec.push(wkb_arr.geo_value(i)?.map(geometry_type));
                }
                Ok(ColumnarValue::Array(Arc::new(StringArray::from(type_vec))))
            }
            DataType::LargeBinary => {
                let wkb_arr = arr.as_binary::<i64>();
                let mut type_vec = vec![];
                for i in 0..wkb_arr.geom_len() {
                    type_vec.push(wkb_arr.geo_value(i)?.map(geometry_type));
                }
                Ok(ColumnarValue::Array(Arc::new(StringArray::from(type_vec))))
            }
            _ => unreachable!(),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

impl Default for GeometryTypeUdf {
    fn default() -> Self {
        Self::new()
    }
}

fn geometry_type(geom: geo::Geometry) -> &'static str {
    match geom {
        geo::Geometry::Point(_) => "ST_Point",
        geo::Geometry::Line(_) => "ST_Line",
        geo::Geometry::LineString(_) => "ST_LineString",
        geo::Geometry::Polygon(_) => "ST_Polygon",
        geo::Geometry::MultiPoint(_) => "ST_MultiPoint",
        geo::Geometry::MultiLineString(_) => "ST_MultiLineString",
        geo::Geometry::MultiPolygon(_) => "ST_MultiPolygon",
        geo::Geometry::GeometryCollection(_) => "ST_GeometryCollection",
        geo::Geometry::Rect(_) => "ST_Rect",
        geo::Geometry::Triangle(_) => "ST_Triangle",
    }
}

#[cfg(test)]
mod tests {
    use crate::function::geometry_type::GeometryTypeUdf;
    use crate::function::GeomFromTextUdf;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn geometry_type() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeomFromTextUdf::new()));
        ctx.register_udf(ScalarUDF::from(GeometryTypeUdf::new()));
        let df = ctx
            .sql("select ST_GeometryType(ST_GeomFromText('POINT(1 1)'))")
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&df.collect().await.unwrap())
                .unwrap()
                .to_string(),
            "+------------------------------------------------------+
| ST_GeometryType(ST_GeomFromText(Utf8(\"POINT(1 1)\"))) |
+------------------------------------------------------+
| ST_Point                                             |
+------------------------------------------------------+"
        );
    }
}
