use crate::geo::{Box2d, GeometryArray, GeometryArrayBuilder};
use crate::DFResult;
use arrow_array::cast::AsArray;
use arrow_array::{Array, GenericBinaryArray, OffsetSizeTrait, StructArray};
use arrow_schema::DataType;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use geo::{AffineOps, AffineTransform};
use geozero::wkb::WkbDialect;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct AsMVTGeomUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl AsMVTGeomUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary, Box2d::data_type()]),
                    TypeSignature::Exact(vec![DataType::LargeBinary, Box2d::data_type()]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["st_asmvtgeom".to_string()],
        }
    }
}

impl ScalarUDFImpl for AsMVTGeomUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_AsMVTGeom"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let arr = args[0].clone().into_array(1)?;
        let arr1 = args[1].clone().into_array(1)?;
        let box_arr = arr1.as_struct();
        match args[0].data_type() {
            DataType::Binary => {
                let wkb_arr = arr.as_binary::<i32>();
                Ok(ColumnarValue::Array(Arc::new(as_mvt_geom(
                    wkb_arr, box_arr,
                )?)))
            }
            DataType::LargeBinary => {
                let wkb_arr = arr.as_binary::<i64>();
                Ok(ColumnarValue::Array(Arc::new(as_mvt_geom(
                    wkb_arr, box_arr,
                )?)))
            }
            _ => unreachable!(),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn as_mvt_geom<O: OffsetSizeTrait>(
    wkb_arr: &GenericBinaryArray<O>,
    box_arr: &StructArray,
) -> DFResult<GenericBinaryArray<O>> {
    let mut builder = GeometryArrayBuilder::<O>::new(WkbDialect::Ewkb, wkb_arr.len());
    for i in 0..wkb_arr.geom_len() {
        let geom = wkb_arr.geo_value(i)?;
        let box2d = Box2d::value(box_arr, i)?.unwrap();

        match geom {
            Some(geom) => {
                let width = box2d.xmax - box2d.xmin;
                let height = box2d.ymax - box2d.ymin;
                let fx = 4096. / width;
                let fy = -4096. / height;

                let transform =
                    AffineTransform::new(fx, 0.0, -box2d.xmin * fx, 0.0, fy, -box2d.ymax * fy);

                let geom = geom.affine_transform(&transform);
                builder.append_geo_geometry(&Some(geom))?;
            }
            None => builder.append_null(),
        }
    }
    Ok(builder.build())
}

impl Default for AsMVTGeomUdf {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::function::as_mvt_geom::AsMVTGeomUdf;
    use crate::function::box2d::Box2dUdf;
    use crate::function::{AsTextUdf, GeomFromTextUdf};
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn as_mvt_geom() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeomFromTextUdf::new()));
        ctx.register_udf(ScalarUDF::from(AsTextUdf::new()));
        ctx.register_udf(ScalarUDF::from(AsMVTGeomUdf::new()));
        ctx.register_udf(ScalarUDF::from(Box2dUdf::new()));
        let df = ctx
            .sql("select ST_AsText(ST_AsMVTGeom(ST_GeomFromText('POLYGON ((0 0, 10 0, 10 5, 0 -5, 0 0))'), Box2D(ST_GeomFromText('LINESTRING(0 0, 4096 4096)'))))")
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&df.collect().await.unwrap())
                .unwrap()
                .to_string(),
            "+-----------------------------------------------------------------------------------------------------------------------------------------------------+
| ST_AsText(ST_AsMVTGeom(ST_GeomFromText(Utf8(\"POLYGON ((0 0, 10 0, 10 5, 0 -5, 0 0))\")),Box2D(ST_GeomFromText(Utf8(\"LINESTRING(0 0, 4096 4096)\"))))) |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+
| POLYGON((0 4096,10 4096,10 4091,0 4101,0 4096))                                                                                                     |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+"
        );
    }
}
