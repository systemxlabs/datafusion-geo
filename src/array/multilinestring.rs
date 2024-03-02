use crate::array::util::check_nulls;
use crate::array::{GeometryArrayAccessor, GeometryArrayTrait};
use crate::buffer::CoordBuffer;
use crate::scalar::{GeometryScalarTrait, MultiLineString, MultiPoint};
use crate::DFResult;
use arrow::array::OffsetSizeTrait;
use arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::error::DataFusionError;
use std::borrow::Cow;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MultiLineStringArray<O: OffsetSizeTrait> {
    pub(crate) coords: CoordBuffer,
    pub(crate) geom_offsets: OffsetBuffer<O>,
    pub(crate) ring_offsets: OffsetBuffer<O>,
    pub(crate) nulls: Option<NullBuffer>,
}

impl<O: OffsetSizeTrait> MultiLineStringArray<O> {
    pub fn try_new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<O>,
        ring_offsets: OffsetBuffer<O>,
        nulls: Option<NullBuffer>,
    ) -> DFResult<Self> {
        check_nulls(&nulls, geom_offsets.len() - 1)?;

        if ring_offsets.last().unwrap().to_usize().unwrap() != coords.len() {
            return Err(DataFusionError::Internal(
                "largest ring offset must match coords length".to_string(),
            ));
        }

        if geom_offsets.last().unwrap().to_usize().unwrap() != ring_offsets.len() - 1 {
            return Err(DataFusionError::Internal(
                "largest geometry offset must match ring offsets length".to_string(),
            ));
        }

        Ok(Self {
            coords,
            geom_offsets,
            ring_offsets,
            nulls,
        })
    }
}

impl<O: OffsetSizeTrait> GeometryArrayTrait for MultiLineStringArray<O> {
    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn len(&self) -> usize {
        self.geom_offsets.len() - 1
    }
}

impl<'a, O: OffsetSizeTrait> GeometryArrayAccessor<'a> for MultiLineStringArray<O> {
    fn value(&'a self, index: usize) -> DFResult<Option<MultiLineString<O>>> {
        if self.is_null(index) {
            return Ok(None);
        }
        let multi_linestring = MultiLineString::try_new(
            Cow::Borrowed(&self.coords),
            Cow::Borrowed(&self.geom_offsets),
            Cow::Borrowed(&self.ring_offsets),
            index,
        )?;
        Ok(Some(multi_linestring))
    }
}
