mod linestring;
mod multilinestring;
mod multipoint;
mod multipolygon;
mod point;
mod polygon;
mod util;

pub use linestring::*;
pub use multilinestring::*;
pub use multipoint::*;
pub use multipolygon::*;
pub use point::*;
pub use polygon::*;

use crate::DFResult;
use arrow::array::OffsetSizeTrait;

pub trait GeometryScalarTrait {
    fn to_geo(&self) -> geo::Geometry;

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> DFResult<geos::Geometry<'static>>;
}

#[derive(Debug, Clone)]
pub enum GeometryScalar<'a, O: OffsetSizeTrait> {
    Point(Point<'a>),
    LineString(LineString<'a, O>),
    Polygon(Polygon<'a, O>),
    MultiPoint(MultiPoint<'a, O>),
    MultiLineString(MultiLineString<'a, O>),
    MultiPolygon(MultiPolygon<'a, O>),
}

impl<'a, O: OffsetSizeTrait> GeometryScalarTrait for GeometryScalar<'a, O> {
    fn to_geo(&self) -> geo::Geometry {
        match self {
            GeometryScalar::Point(v) => v.to_geo(),
            GeometryScalar::LineString(v) => v.to_geo(),
            GeometryScalar::Polygon(v) => v.to_geo(),
            GeometryScalar::MultiPoint(v) => v.to_geo(),
            GeometryScalar::MultiLineString(v) => v.to_geo(),
            GeometryScalar::MultiPolygon(v) => v.to_geo(),
        }
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> DFResult<geos::Geometry<'static>> {
        match self {
            GeometryScalar::Point(v) => v.to_geos(),
            GeometryScalar::LineString(v) => v.to_geos(),
            GeometryScalar::Polygon(v) => v.to_geos(),
            GeometryScalar::MultiPoint(v) => v.to_geos(),
            GeometryScalar::MultiLineString(v) => v.to_geos(),
            GeometryScalar::MultiPolygon(v) => v.to_geos(),
        }
    }
}
