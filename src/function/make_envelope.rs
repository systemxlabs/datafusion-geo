use crate::function::IntersectsUdf;
use crate::geo::GeometryArrayBuilder;
use arrow_schema::DataType;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use geos::CoordSeq;
use geozero::wkb::WkbDialect;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct MakeEnvelopeUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl MakeEnvelopeUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["st_makeenvelope".to_string()],
        }
    }
}

impl ScalarUDFImpl for MakeEnvelopeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ST_MakeEnvelope"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let error = Err(DataFusionError::Internal(
            "The arg should be float64".to_string(),
        ));
        let ColumnarValue::Scalar(ScalarValue::Float64(Some(xmin))) = &args[0] else {
            return error;
        };
        let ColumnarValue::Scalar(ScalarValue::Float64(Some(ymin))) = &args[1] else {
            return error;
        };
        let ColumnarValue::Scalar(ScalarValue::Float64(Some(xmax))) = &args[2] else {
            return error;
        };
        let ColumnarValue::Scalar(ScalarValue::Float64(Some(ymax))) = &args[3] else {
            return error;
        };
        let srid = if args.len() == 5 {
            let ColumnarValue::Scalar(ScalarValue::Int64(Some(srid))) = &args[4] else {
                return Err(DataFusionError::Internal(
                    "The fifth arg should be int64".to_string(),
                ));
            };
            Some(*srid as i32)
        } else {
            None
        };
        let coords = CoordSeq::new_from_vec(&[
            &[xmin, ymin],
            &[xmin, ymax],
            &[xmax, ymax],
            &[xmax, ymin],
            &[xmin, ymin],
        ])
        .map_err(|_| DataFusionError::Internal("Failed to create coord req".to_string()))?;
        let exterior = geos::Geometry::create_line_string(coords)
            .map_err(|_| DataFusionError::Internal("Failed to create exterior".to_string()))?;
        let mut polygon = geos::Geometry::create_polygon(exterior, vec![])
            .map_err(|_| DataFusionError::Internal("Failed to create polygon".to_string()))?;
        let mut builder = if let Some(srid) = srid {
            polygon.set_srid(srid as usize);
            GeometryArrayBuilder::<i32>::new(WkbDialect::Ewkb, 1)
        } else {
            GeometryArrayBuilder::<i32>::new(WkbDialect::Wkb, 1)
        };
        builder.append_geos_geometry(&Some(polygon))?;
        let wkb_arr = builder.build();
        Ok(ColumnarValue::Array(Arc::new(wkb_arr)))
    }
}

impl Default for MakeEnvelopeUdf {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::function::{AsEwktUdf, MakeEnvelopeUdf};
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    #[ignore]
    async fn make_envelope() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(MakeEnvelopeUdf::new()));
        ctx.register_udf(ScalarUDF::from(AsEwktUdf::new()));
        let df = ctx
            .sql("select ST_AsEWKT(ST_MakeEnvelope(10, 10, 11, 11))")
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&df.collect().await.unwrap())
                .unwrap()
                .to_string(),
            ""
        );

        let df = ctx
            .sql("select ST_AsEWKT(ST_MakeEnvelope(10, 10, 11, 11, 4236))")
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
