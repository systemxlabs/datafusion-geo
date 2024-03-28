use crate::geo::{GeometryArray, GeometryArrayBuilder};
use crate::DFResult;
use arrow_array::cast::AsArray;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use arrow_schema::DataType;
use datafusion_common::internal_datafusion_err;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use geos::Geom;
use geozero::wkb::WkbDialect;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct BoundaryUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl BoundaryUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary]),
                    TypeSignature::Exact(vec![DataType::LargeBinary]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["st_boundary".to_string()],
        }
    }
}

impl ScalarUDFImpl for BoundaryUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_Boundary"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let arr = args[0].clone().into_array(1)?;
        match args[0].data_type() {
            DataType::Binary => {
                let wkb_arr = arr.as_binary::<i32>();
                build_boundary_arr::<i32>(wkb_arr)
            }
            DataType::LargeBinary => {
                let wkb_arr = arr.as_binary::<i64>();
                build_boundary_arr::<i64>(wkb_arr)
            }
            _ => unreachable!(),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn build_boundary_arr<O: OffsetSizeTrait>(
    wkb_arr: &GenericBinaryArray<O>,
) -> DFResult<ColumnarValue> {
    let mut builder = GeometryArrayBuilder::<O>::new(WkbDialect::Ewkb, wkb_arr.geom_len());
    for i in 0..wkb_arr.geom_len() {
        builder.append_geos_geometry(&wkb_arr.geos_value(i)?.map(|geom| {
            geom.boundary()
                .map_err(|e| internal_datafusion_err!("Failed to call boundary, e: {}", e))?
        }))?;
    }

    Ok(ColumnarValue::Array(Arc::new(builder.build())))
}

#[cfg(test)]
mod tests {
    use crate::function::{BoundaryUdf, GeomFromTextUdf};
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn boundary() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeomFromTextUdf::new()));
        ctx.register_udf(ScalarUDF::from(BoundaryUdf::new()));
        let df = ctx
            .sql("SELECT ST_AsText(ST_Boundary(ST_GeomFromText('POLYGON((1 1,0 0, -1 1, 1 1))')));")
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&df.collect().await.unwrap())
                .unwrap()
                .to_string(),
            ""
        );
    }
}
