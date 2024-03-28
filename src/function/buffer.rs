use crate::function::AsGeoJsonUdf;
use crate::geo::{GeometryArray, GeometryArrayBuilder};
use crate::DFResult;
use arrow_array::cast::AsArray;
use arrow_array::{GenericBinaryArray, LargeStringArray, OffsetSizeTrait, StringArray};
use arrow_schema::DataType;
use datafusion_common::{internal_datafusion_err, internal_err, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use geos::Geom;
use geozero::wkb::WkbDialect;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct BufferUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl BufferUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Binary,
                        DataType::Float32,
                        DataType::Int32,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::LargeBinary,
                        DataType::Float32,
                        DataType::Int32,
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["st_buffer".to_string()],
        }
    }
}

impl ScalarUDFImpl for BufferUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_Buffer"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let arr = args[0].clone().into_array(1)?;
        let ColumnarValue::Scalar(ScalarValue::Float64(Some(width))) = args[1] else {
            return internal_err!("The second arg should be f64 scalar");
        };
        let ColumnarValue::Scalar(ScalarValue::Int32(Some(quadsegs))) = args[2] else {
            return internal_err!("The third arg should be i32 scalar");
        };

        match args[0].data_type() {
            DataType::Binary => {
                let wkb_arr = arr.as_binary::<i32>();
                build_buffer_arr(wkb_arr, width, quadsegs)
            }
            DataType::LargeBinary => {
                let wkb_arr = arr.as_binary::<i64>();
                build_buffer_arr(wkb_arr, width, quadsegs)
            }
            _ => unreachable!(),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn build_buffer_arr<O: OffsetSizeTrait>(
    wkb_arr: &GenericBinaryArray<O>,
    width: f64,
    quadsegs: i32,
) -> DFResult<ColumnarValue> {
    let mut builder = GeometryArrayBuilder::<O>::new(WkbDialect::Ewkb, wkb_arr.geom_len());
    for i in 0..wkb_arr.geom_len() {
        builder.append_geos_geometry(&wkb_arr.geos_value(i)?.map(|geom| {
            geom.buffer(width, quadsegs)
                .map_err(|e| internal_datafusion_err!("Failed to call buffer, e: {}", e))?
        }))?;
    }

    Ok(ColumnarValue::Array(Arc::new(builder.build())))
}

impl Default for BufferUdf {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::function::{BufferUdf, GeomFromTextUdf};
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn buffer() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeomFromTextUdf::new()));
        ctx.register_udf(ScalarUDF::from(BufferUdf::new()));
        let df = ctx
            .sql("SELECT ST_Buffer(ST_GeomFromText('POINT(100 90)'), 50.0, 8);")
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
