use crate::buffer::CoordBuffer;
use crate::scalar::{GeometryScalarTrait, Point};
use crate::DFResult;
use arrow::array::OffsetSizeTrait;
use arrow::buffer::OffsetBuffer;
use datafusion::common::DataFusionError;
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub struct MultiPoint<'a, O: OffsetSizeTrait> {
    pub(crate) coords: Cow<'a, CoordBuffer>,
    pub(crate) geom_offsets: Cow<'a, OffsetBuffer<O>>,
    pub(crate) geom_index: usize,
    start_offset: usize,
    end_offset: usize,
}

impl<'a, O: OffsetSizeTrait> MultiPoint<'a, O> {
    pub fn try_new(
        coords: Cow<'a, CoordBuffer>,
        geom_offsets: Cow<'a, OffsetBuffer<O>>,
        geom_index: usize,
    ) -> DFResult<Self> {
        let Some(Some(start_offset)) = geom_offsets.get(geom_index).map(|f| f.to_usize()) else {
            return Err(DataFusionError::Internal(
                "Cannot get start_offset".to_string(),
            ));
        };
        let Some(Some(end_offset)) = geom_offsets.get(geom_index + 1).map(|f| f.to_usize()) else {
            return Err(DataFusionError::Internal(
                "Cannot get end_offset".to_string(),
            ));
        };
        Ok(Self {
            coords,
            geom_offsets,
            geom_index,
            start_offset,
            end_offset,
        })
    }

    pub fn num_points(&self) -> usize {
        self.end_offset - self.start_offset
    }

    pub fn point(&self, i: usize) -> Option<Point<'_>> {
        if i >= self.num_points() {
            None
        } else {
            Some(
                Point::try_new(self.coords.clone(), self.start_offset + i)
                    .expect("geom_index has been checked"),
            )
        }
    }

    pub fn points(&self) -> impl ExactSizeIterator<Item = Point> {
        (0..self.num_points()).map(|index| self.point(index).expect("index is valid"))
    }
}

impl<'a, O: OffsetSizeTrait> GeometryScalarTrait for MultiPoint<'a, O> {
    fn to_geo(&self) -> geo::Geometry {
        geo::Geometry::MultiPoint(self.into())
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> DFResult<geos::Geometry> {
        geos::Geometry::create_multipoint(
            self.points()
                .map(|point| point.to_geos())
                .collect::<DFResult<Vec<_>>>()?,
        )
        .map_err(|e| {
            DataFusionError::Internal("Cannot convert multipoint to geos Geometry".to_string())
        })
    }
}

impl<O: OffsetSizeTrait> From<MultiPoint<'_, O>> for geo::MultiPoint {
    fn from(value: MultiPoint<'_, O>) -> Self {
        (&value).into()
    }
}

impl<O: OffsetSizeTrait> From<&MultiPoint<'_, O>> for geo::MultiPoint {
    fn from(value: &MultiPoint<'_, O>) -> Self {
        geo::MultiPoint::new(value.points().map(|point| point.into()).collect())
    }
}
