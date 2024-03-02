mod geometry;
mod multipoint;
mod point;

use crate::DFResult;
use arrow::buffer::NullBuffer;
use crate::scalar::GeometryScalarTrait;

pub trait GeometryArrayTrait: Send + Sync {
    fn validity(&self) -> Option<&NullBuffer>;

    fn len(&self) -> usize;

    #[inline]
    fn is_null(&self, i: usize) -> bool {
        self.validity().map(|x| x.is_null(i)).unwrap_or(false)
    }
}

pub trait GeometryArrayAccessor<'a>: GeometryArrayTrait {
    type Item: GeometryScalarTrait;

    fn value(&'a self, index: usize) -> DFResult<Option<Self::Item>>;

    fn value_unchecked(&'a self, index: usize) -> Option<Self::Item> {
        self.value(index).unwrap()
    }

    fn value_as_geo(&'a self, i: usize) -> DFResult<Option<geo::Geometry>> {
        let value = self.value(i)?;
        match value {
            Some(v) => Ok(Some(v.to_geo())),
            None => Ok(None),
        }
    }

    #[cfg(feature = "geos")]
    fn value_as_geos(&'a self, i: usize) -> DFResult<Option<geos::Geometry>> {
        let value = self.value(i)?;
        match value {
            Some(v) => Ok(Some(v.to_geos()?)),
            None => Ok(None),
        }
    }

    fn iter(&'a self) -> impl ExactSizeIterator<Item = Option<Self::Item>> + 'a {
        (0..self.len()).map(|i| self.value_unchecked(i))
    }

    fn iter_geo(&'a self) -> impl ExactSizeIterator<Item = DFResult<Option<geo::Geometry>>> + 'a {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    #[cfg(feature = "geos")]
    fn iter_geos(&'a self) -> impl ExactSizeIterator<Item = DFResult<Option<geos::Geometry>>> + 'a {
        (0..self.len()).map(|i| self.value_as_geos(i))
    }
}
