use crate::buffer::CoordBuffer;
use crate::scalar::util::compute_start_end_offset;
use crate::scalar::{GeometryScalarTrait, Point};
use crate::DFResult;
use arrow::array::OffsetSizeTrait;
use arrow::buffer::OffsetBuffer;
use datafusion::common::DataFusionError;
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub struct LineString<'a, O: OffsetSizeTrait> {
    pub(crate) coords: Cow<'a, CoordBuffer>,
    pub(crate) geom_offsets: Cow<'a, OffsetBuffer<O>>,
    pub(crate) geom_index: usize,
    start_offset: usize,
    end_offset: usize,
}

impl<'a, O: OffsetSizeTrait> LineString<'a, O> {
    pub fn try_new(
        coords: Cow<'a, CoordBuffer>,
        geom_offsets: Cow<'a, OffsetBuffer<O>>,
        geom_index: usize,
    ) -> DFResult<Self> {
        let (start_offset, end_offset) =
            compute_start_end_offset(geom_offsets.clone(), geom_index)?;

        Ok(Self {
            coords,
            geom_offsets,
            geom_index,
            start_offset,
            end_offset,
        })
    }

    pub fn num_coords(&self) -> usize {
        self.end_offset - self.start_offset
    }

    pub fn coord(&self, i: usize) -> Option<Point<'_>> {
        if i >= self.num_coords() {
            None
        } else {
            Some(
                Point::try_new(self.coords.clone(), self.start_offset + i)
                    .expect("geom_index has been checked"),
            )
        }
    }

    pub fn coords(&self) -> impl ExactSizeIterator<Item = Point> {
        (0..self.num_coords()).map(|index| self.coord(index).expect("index is valid"))
    }
}

impl<'a, O: OffsetSizeTrait> GeometryScalarTrait for LineString<'a, O> {
    fn to_geo(&self) -> geo::Geometry {
        let coords: Vec<geo::Coord> = self.coords().map(|p| p.into()).collect();
        geo::Geometry::LineString(geo::LineString::new(coords))
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> DFResult<geos::Geometry> {
        let sliced_coords = self
            .coords
            .clone()
            .to_mut()
            .slice(self.start_offset, self.end_offset - self.start_offset)?;
        geos::Geometry::create_line_string(sliced_coords.try_into()?).map_err(|e| {
            DataFusionError::Internal("Cannot convert linestring to geos linestring".to_string())
        })
    }
}

impl<O: OffsetSizeTrait> From<LineString<'_, O>> for geo::LineString {
    fn from(value: LineString<'_, O>) -> Self {
        (&value).into()
    }
}

impl<O: OffsetSizeTrait> From<&LineString<'_, O>> for geo::LineString {
    fn from(value: &LineString<'_, O>) -> Self {
        let coords: Vec<geo::Coord> = value.coords().map(|p| p.into()).collect();
        geo::LineString::new(coords)
    }
}
