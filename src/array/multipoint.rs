use crate::array::{GeometryArrayAccessor, GeometryArrayTrait};
use crate::buffer::CoordBuffer;
use crate::scalar::{GeometryScalarTrait, MultiPoint};
use crate::DFResult;
use arrow::array::OffsetSizeTrait;
use arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::error::DataFusionError;
use geo::Geometry;
use std::borrow::Cow;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MultiPointArray<O: OffsetSizeTrait> {
    pub(crate) coords: CoordBuffer,
    pub(crate) geom_offsets: OffsetBuffer<O>,
    pub(crate) validity: Option<NullBuffer>,
}

impl<O: OffsetSizeTrait> MultiPointArray<O> {
    pub fn try_new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<O>,
        validity: Option<NullBuffer>,
    ) -> DFResult<Self> {
        if let Some(validity) = validity.as_ref() {
            if validity.len() != geom_offsets.len() - 1 {
                return Err(DataFusionError::Internal(
                    "validity mask length must match the number of values".to_string(),
                ));
            }
        }

        if geom_offsets.last().unwrap().to_usize().unwrap() != coords.len() {
            return Err(DataFusionError::Internal(
                "largest geometry offset must match coords length".to_string(),
            ));
        }

        Ok(Self {
            coords,
            geom_offsets,
            validity,
        })
    }
}

impl<O: OffsetSizeTrait> GeometryArrayTrait for MultiPointArray<O> {
    fn validity(&self) -> Option<&NullBuffer> {
        self.validity.as_ref()
    }

    fn len(&self) -> usize {
        self.geom_offsets.len() - 1
    }
}

impl<'a, O: OffsetSizeTrait> GeometryArrayAccessor<'a> for MultiPointArray<O> {
    type Item = MultiPoint<'a, O>;

    fn value(&'a self, index: usize) -> DFResult<Option<Self::Item>> {
        if self.is_null(index) {
            return Ok(None);
        }
        let multipoint = MultiPoint::try_new(
            Cow::Borrowed(&self.coords),
            Cow::Borrowed(&self.geom_offsets),
            index,
        )?;
        Ok(Some(multipoint))
    }
}
