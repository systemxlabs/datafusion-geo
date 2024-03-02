use crate::array::util::build_geometry_array_from_geo;
use crate::array::{GeometryArray, GeometryArrayAccessor};
use crate::DFResult;
use arrow::array::OffsetSizeTrait;
use geo::Translate as _Translate;

pub trait Translate: Sized {
    fn translate(&self, x_offset: f64, y_offset: f64) -> DFResult<Self>;
}

impl<O: OffsetSizeTrait> Translate for GeometryArray<O> {
    fn translate(&self, x_offset: f64, y_offset: f64) -> DFResult<Self> {
        let data = self
            .iter_geo()
            .map(|geom| geom.map(|g| g.translate(x_offset, y_offset)))
            .collect::<Vec<Option<geo::Geometry>>>();
        build_geometry_array_from_geo(data, self)
    }
}
