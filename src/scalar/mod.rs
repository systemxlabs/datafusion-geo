mod multipoint;
mod point;

pub use multipoint::*;
pub use point::*;

use crate::DFResult;

pub trait GeometryScalarTrait {
    fn to_geo(&self) -> geo::Geometry;

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> DFResult<geos::Geometry>;
}
