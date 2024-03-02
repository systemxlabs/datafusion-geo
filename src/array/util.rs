use crate::array::{GeometryArray, PointArrayBuilder};
use crate::DFResult;
use arrow::array::OffsetSizeTrait;
use arrow::buffer::NullBuffer;
use datafusion::common::DataFusionError;

pub(crate) fn check_nulls(nulls: &Option<NullBuffer>, expected_len: usize) -> DFResult<()> {
    if let Some(nulls_buf) = &nulls {
        if nulls_buf.len() != expected_len {
            return Err(DataFusionError::Internal(
                "nulls mask length must match the number of values".to_string(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn build_geometry_array_from_geo<O: OffsetSizeTrait>(
    data: Vec<Option<geo::Geometry>>,
    array_type: &GeometryArray<O>,
) -> DFResult<GeometryArray<O>> {
    match array_type {
        GeometryArray::Point(_) => {
            let mut builder = PointArrayBuilder::new(data.len());
            for geom in data {
                match geom {
                    Some(geo::Geometry::Point(p)) => builder.push_geo_point(Some(p)),
                    None => builder.push_null(),
                    _ => {
                        return Err(DataFusionError::Internal(
                            "geo::Geometry is not point".to_string(),
                        ))
                    }
                }
            }
            Ok(GeometryArray::Point(builder.build()?))
        }
        GeometryArray::LineString(_) => todo!(),
        GeometryArray::Polygon(_) => todo!(),
        GeometryArray::MultiPoint(_) => todo!(),
        GeometryArray::MultiLineString(_) => todo!(),
        GeometryArray::MultiPolygon(_) => todo!(),
        GeometryArray::Mixed(_) => todo!(),
    }
}
