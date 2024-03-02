use crate::array::GeometryArray;
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
) -> GeometryArray<O> {
    match array_type {
        GeometryArray::Point(_) => {}
        GeometryArray::LineString(_) => {}
        GeometryArray::Polygon(_) => {}
        GeometryArray::MultiPoint(_) => {}
        GeometryArray::MultiLineString(_) => {}
        GeometryArray::MultiPolygon(_) => {}
        GeometryArray::Mixed(_) => {}
    }
    todo!()
}
