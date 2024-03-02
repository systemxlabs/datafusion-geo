use crate::buffer::CoordBuffer;
use crate::scalar::util::compute_start_end_offset;
use crate::scalar::{GeometryScalarTrait, LineString, Point};
use crate::DFResult;
use arrow::array::OffsetSizeTrait;
use arrow::buffer::OffsetBuffer;
use datafusion::common::DataFusionError;
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub struct MultiLineString<'a, O: OffsetSizeTrait> {
    pub(crate) coords: Cow<'a, CoordBuffer>,
    pub(crate) geom_offsets: Cow<'a, OffsetBuffer<O>>,
    pub(crate) ring_offsets: Cow<'a, OffsetBuffer<O>>,
    pub(crate) geom_index: usize,
    start_offset: usize,
    end_offset: usize,
}

impl<'a, O: OffsetSizeTrait> MultiLineString<'a, O> {
    pub fn try_new(
        coords: Cow<'a, CoordBuffer>,
        geom_offsets: Cow<'a, OffsetBuffer<O>>,
        ring_offsets: Cow<'a, OffsetBuffer<O>>,
        geom_index: usize,
    ) -> DFResult<Self> {
        let (start_offset, end_offset) =
            compute_start_end_offset(geom_offsets.clone(), geom_index)?;
        Ok(Self {
            coords,
            geom_offsets,
            ring_offsets,
            geom_index,
            start_offset,
            end_offset,
        })
    }

    pub fn num_lines(&self) -> usize {
        self.start_offset - self.end_offset
    }

    pub fn line(&self, i: usize) -> Option<LineString<O>> {
        if i >= self.num_lines() {
            None
        } else {
            Some(
                LineString::try_new(
                    self.coords.clone(),
                    self.ring_offsets.clone(),
                    self.start_offset + 1,
                )
                .expect("geom_index has been checked"),
            )
        }
    }

    pub fn lines(&self) -> impl ExactSizeIterator<Item = LineString<O>> {
        (0..self.num_lines()).map(|index| self.line(index).expect("index is valid"))
    }
}

impl<'a, O: OffsetSizeTrait> GeometryScalarTrait for MultiLineString<'a, O> {
    fn to_geo(&self) -> geo::Geometry {
        geo::Geometry::MultiLineString(geo::MultiLineString::new(
            self.lines().map(|line| line.into()).collect(),
        ))
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> DFResult<geos::Geometry<'static>> {
        geos::Geometry::create_multiline_string(
            self.lines()
                .map(|line| line.to_geos())
                .collect::<DFResult<Vec<_>>>()?,
        )
        .map_err(|e| {
            DataFusionError::Internal(
                "Cannot convert multilinestring to geos multilinestring".to_string(),
            )
        })
    }
}
