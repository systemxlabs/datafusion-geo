use crate::buffer::CoordBuffer;
use crate::scalar::util::compute_start_end_offset;
use crate::scalar::{GeometryScalarTrait, LineString};
use crate::DFResult;
use arrow::array::OffsetSizeTrait;
use arrow::buffer::OffsetBuffer;
use datafusion::common::DataFusionError;
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub struct Polygon<'a, O: OffsetSizeTrait> {
    pub(crate) coords: Cow<'a, CoordBuffer>,
    pub(crate) geom_offsets: Cow<'a, OffsetBuffer<O>>,
    /// the first ring is exterior, others are interior
    pub(crate) ring_offsets: Cow<'a, OffsetBuffer<O>>,
    pub(crate) geom_index: usize,
    start_offset: usize,
    end_offset: usize,
}

impl<'a, O: OffsetSizeTrait> Polygon<'a, O> {
    pub fn try_new(
        coords: Cow<'a, CoordBuffer>,
        geom_offsets: Cow<'a, OffsetBuffer<O>>,
        ring_offsets: Cow<'a, OffsetBuffer<O>>,
        geom_index: usize,
    ) -> DFResult<Self> {
        let (start_offset, end_offset) =
            compute_start_end_offset(geom_offsets.clone(), geom_index)?;
        if start_offset == end_offset || ring_offsets.len() == 0 {
            return Err(DataFusionError::Internal(
                "polygon should not be empty".to_string(),
            ));
        }

        Ok(Self {
            coords,
            geom_offsets,
            ring_offsets,
            geom_index,
            start_offset,
            end_offset,
        })
    }

    fn exterior(&self) -> LineString<'_, O> {
        LineString::try_new(
            self.coords.clone(),
            self.ring_offsets.clone(),
            self.start_offset,
        )
        .expect("geom_index has been checked")
    }

    pub fn num_interiors(&self) -> usize {
        self.end_offset - self.start_offset - 1
    }

    pub fn interior(&self, i: usize) -> Option<LineString<'_, O>> {
        if i >= self.num_interiors() {
            None
        } else {
            Some(
                LineString::try_new(
                    self.coords.clone(),
                    self.ring_offsets.clone(),
                    self.start_offset + i + 1,
                )
                .expect("geom_index has been checked"),
            )
        }
    }

    pub fn interiors(&self) -> impl ExactSizeIterator<Item = LineString<O>> {
        (0..self.num_interiors()).map(|index| self.interior(index).expect("index is valid"))
    }
}

impl<'a, O: OffsetSizeTrait> GeometryScalarTrait for Polygon<'a, O> {
    fn to_geo(&self) -> geo::Geometry {
        let exterior: geo::LineString = self.exterior().into();
        let interiors: Vec<geo::LineString> =
            self.interiors().map(|interior| interior.into()).collect();
        geo::Geometry::Polygon(geo::Polygon::new(exterior, interiors))
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> DFResult<geos::Geometry<'static>> {
        let exterior = self.exterior().to_geos()?;
        let interiors = self
            .interiors()
            .map(|interior| interior.to_geos())
            .collect::<DFResult<Vec<_>>>()?;
        geos::Geometry::create_polygon(exterior, interiors).map_err(|e| {
            DataFusionError::Internal("Cannot convert polygon to geos polygon".to_string())
        })
    }
}

impl<O: OffsetSizeTrait> From<Polygon<'_, O>> for geo::Polygon {
    fn from(value: Polygon<'_, O>) -> Self {
        (&value).into()
    }
}

impl<O: OffsetSizeTrait> From<&Polygon<'_, O>> for geo::Polygon {
    fn from(value: &Polygon<'_, O>) -> Self {
        let exterior: geo::LineString = value.exterior().into();
        let interiors: Vec<geo::LineString> =
            value.interiors().map(|interior| interior.into()).collect();
        geo::Polygon::new(exterior, interiors)
    }
}
