mod linestring;
mod mixed;
mod multilinestring;
mod multipoint;
mod multipolygon;
mod point;
mod polygon;
pub mod util;

pub use linestring::*;
pub use mixed::*;
pub use mixed::*;
pub use multilinestring::*;
pub use multipoint::*;
pub use multipolygon::*;
pub use point::*;
pub use polygon::*;

use crate::scalar::{GeometryScalar, GeometryScalarTrait};
use crate::DFResult;
use arrow::array::OffsetSizeTrait;
use arrow::buffer::NullBuffer;
use strum::{EnumIter, IntoEnumIterator};

pub trait GeometryArrayTrait: Send + Sync {
    fn nulls(&self) -> Option<&NullBuffer>;

    fn len(&self) -> usize;

    #[inline]
    fn is_null(&self, i: usize) -> bool {
        self.nulls().map(|x| x.is_null(i)).unwrap_or(false)
    }
}

pub trait GeometryArrayAccessor<'a>: GeometryArrayTrait {
    fn value(&'a self, index: usize) -> DFResult<Option<impl GeometryScalarTrait>>;

    fn value_unchecked(&'a self, index: usize) -> Option<impl GeometryScalarTrait> {
        self.value(index).unwrap()
    }

    fn value_as_geo(&'a self, i: usize) -> DFResult<Option<geo::Geometry>> {
        let value = self.value(i)?;
        match value {
            Some(v) => Ok(Some(v.to_geo())),
            None => Ok(None),
        }
    }

    fn value_as_geo_unchecked(&'a self, i: usize) -> Option<geo::Geometry> {
        let value = self.value_unchecked(i);
        value.map(|v| v.to_geo())
    }

    #[cfg(feature = "geos")]
    fn value_as_geos(&'a self, i: usize) -> DFResult<Option<geos::Geometry<'static>>> {
        let value = self.value(i)?;
        match value {
            Some(v) => Ok(Some(v.to_geos()?)),
            None => Ok(None),
        }
    }

    fn iter(&'a self) -> impl ExactSizeIterator<Item = Option<impl GeometryScalarTrait>> + 'a {
        (0..self.len()).map(|i| self.value_unchecked(i))
    }

    fn iter_geo(&'a self) -> impl ExactSizeIterator<Item = Option<geo::Geometry>> + 'a {
        (0..self.len()).map(|i| self.value_as_geo_unchecked(i))
    }

    #[cfg(feature = "geos")]
    fn iter_geos(
        &'a self,
    ) -> impl ExactSizeIterator<Item = DFResult<Option<geos::Geometry<'static>>>> + 'a {
        (0..self.len()).map(|i| self.value_as_geos(i))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, EnumIter)]
pub enum GeometryType {
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    // GeometryCollection,
}

impl GeometryType {
    pub fn geo_type_id(&self) -> i8 {
        match self {
            GeometryType::Point => 1,
            GeometryType::LineString => 2,
            GeometryType::Polygon => 3,
            GeometryType::MultiPoint => 4,
            GeometryType::MultiLineString => 5,
            GeometryType::MultiPolygon => 6,
            // GeometryType::GeometryCollection => 7,
        }
    }

    pub fn find(type_id: i8) -> Option<GeometryType> {
        GeometryType::iter().find(|t| t.geo_type_id() == type_id)
    }
}

#[derive(Debug, Clone)]
pub enum GeometryArray<O: OffsetSizeTrait> {
    Point(PointArray),
    LineString(LineStringArray<O>),
    Polygon(PolygonArray<O>),
    MultiPoint(MultiPointArray<O>),
    MultiLineString(MultiLineStringArray<O>),
    MultiPolygon(MultiPolygonArray<O>),
    Mixed(MixedGeometryArray<O>),
}

impl<O: OffsetSizeTrait> GeometryArrayTrait for GeometryArray<O> {
    fn nulls(&self) -> Option<&NullBuffer> {
        match self {
            GeometryArray::Point(arr) => arr.nulls(),
            GeometryArray::LineString(arr) => arr.nulls(),
            GeometryArray::Polygon(arr) => arr.nulls(),
            GeometryArray::MultiPoint(arr) => arr.nulls(),
            GeometryArray::MultiLineString(arr) => arr.nulls(),
            GeometryArray::MultiPolygon(arr) => arr.nulls(),
            GeometryArray::Mixed(arr) => arr.nulls(),
        }
    }

    fn len(&self) -> usize {
        match self {
            GeometryArray::Point(arr) => arr.len(),
            GeometryArray::LineString(arr) => arr.len(),
            GeometryArray::Polygon(arr) => arr.len(),
            GeometryArray::MultiPoint(arr) => arr.len(),
            GeometryArray::MultiLineString(arr) => arr.len(),
            GeometryArray::MultiPolygon(arr) => arr.len(),
            GeometryArray::Mixed(arr) => arr.len(),
        }
    }
}

impl<'a, O: OffsetSizeTrait> GeometryArrayAccessor<'a> for GeometryArray<O> {
    fn value(&'a self, index: usize) -> DFResult<Option<GeometryScalar<'a, O>>> {
        match self {
            GeometryArray::Point(arr) => {
                let point = arr.value(index)?;
                Ok(point.map(|v| GeometryScalar::Point(v)))
            }
            GeometryArray::LineString(arr) => {
                let line_string = arr.value(index)?;
                Ok(line_string.map(|v| GeometryScalar::LineString(v)))
            }
            GeometryArray::Polygon(arr) => {
                let polygon = arr.value(index)?;
                Ok(polygon.map(|v| GeometryScalar::Polygon(v)))
            }
            GeometryArray::MultiPoint(arr) => {
                let multi_point = arr.value(index)?;
                Ok(multi_point.map(|v| GeometryScalar::MultiPoint(v)))
            }
            GeometryArray::MultiLineString(arr) => {
                let multi_linestring = arr.value(index)?;
                Ok(multi_linestring.map(|v| GeometryScalar::MultiLineString(v)))
            }
            GeometryArray::MultiPolygon(arr) => {
                let multi_polygon = arr.value(index)?;
                Ok(multi_polygon.map(|v| GeometryScalar::MultiPolygon(v)))
            }
            GeometryArray::Mixed(arr) => arr.value(index),
        }
    }
}
