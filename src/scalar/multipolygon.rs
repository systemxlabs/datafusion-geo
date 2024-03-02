use crate::buffer::CoordBuffer;
use crate::scalar::util::compute_start_end_offset;
use crate::scalar::{GeometryScalarTrait, Polygon};
use crate::DFResult;
use arrow::array::OffsetSizeTrait;
use arrow::buffer::OffsetBuffer;
use datafusion::common::DataFusionError;
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub struct MultiPolygon<'a, O: OffsetSizeTrait> {
    pub(crate) coords: Cow<'a, CoordBuffer>,
    pub(crate) geom_offsets: Cow<'a, OffsetBuffer<O>>,
    pub(crate) polygon_offsets: Cow<'a, OffsetBuffer<O>>,
    pub(crate) ring_offsets: Cow<'a, OffsetBuffer<O>>,
    pub(crate) geom_index: usize,
    start_offset: usize,
    end_offset: usize,
}

impl<'a, O: OffsetSizeTrait> MultiPolygon<'a, O> {
    pub fn try_new(
        coords: Cow<'a, CoordBuffer>,
        geom_offsets: Cow<'a, OffsetBuffer<O>>,
        polygon_offsets: Cow<'a, OffsetBuffer<O>>,
        ring_offsets: Cow<'a, OffsetBuffer<O>>,
        geom_index: usize,
    ) -> DFResult<Self> {
        let (start_offset, end_offset) =
            compute_start_end_offset(geom_offsets.clone(), geom_index)?;
        Ok(Self {
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            geom_index,
            start_offset,
            end_offset,
        })
    }

    pub fn num_polygons(&self) -> usize {
        self.start_offset - self.end_offset
    }

    pub fn polygon(&self, i: usize) -> Option<Polygon<O>> {
        if i >= self.num_polygons() {
            None
        } else {
            Some(
                Polygon::try_new(
                    self.coords.clone(),
                    self.polygon_offsets.clone(),
                    self.ring_offsets.clone(),
                    self.start_offset + i,
                )
                .expect("geom_index has been checked"),
            )
        }
    }

    pub fn polygons(&self) -> impl ExactSizeIterator<Item = Polygon<O>> {
        (0..self.num_polygons()).map(|index| self.polygon(index).expect("index is valid"))
    }
}

impl<'a, O: OffsetSizeTrait> GeometryScalarTrait for MultiPolygon<'a, O> {
    fn to_geo(&self) -> geo::Geometry {
        geo::Geometry::MultiPolygon(geo::MultiPolygon::new(
            self.polygons().map(|polygon| polygon.into()).collect(),
        ))
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> DFResult<geos::Geometry> {
        geos::Geometry::create_multipolygon(
            self.polygons()
                .map(|polygons| polygons.to_geos())
                .collect::<DFResult<Vec<_>>>()?,
        )
        .map_err(|e| {
            DataFusionError::Internal(
                "Cannot convert multipolygon to geos multipolygon".to_string(),
            )
        })
    }
}
