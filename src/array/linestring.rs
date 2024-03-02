use crate::array::util::check_nulls;
use crate::array::{GeometryArrayAccessor, GeometryArrayTrait};
use crate::buffer::CoordBuffer;
use crate::scalar::{GeometryScalarTrait, LineString};
use crate::DFResult;
use arrow::array::OffsetSizeTrait;
use arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::common::DataFusionError;
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub struct LineStringArray<O: OffsetSizeTrait> {
    pub(crate) coords: CoordBuffer,
    pub(crate) geom_offsets: OffsetBuffer<O>,
    pub(crate) nulls: Option<NullBuffer>,
}

impl<O: OffsetSizeTrait> LineStringArray<O> {
    pub fn try_new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<O>,
        nulls: Option<NullBuffer>,
    ) -> DFResult<Self> {
        check_nulls(&nulls, geom_offsets.len() - 1)?;

        if geom_offsets.last().unwrap().to_usize().unwrap() != coords.len() {
            return Err(DataFusionError::Internal(
                "largest geometry offset must match coords length".to_string(),
            ));
        }

        Ok(Self {
            coords,
            geom_offsets,
            nulls,
        })
    }
}

impl<O: OffsetSizeTrait> GeometryArrayTrait for LineStringArray<O> {
    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn len(&self) -> usize {
        self.geom_offsets.len() - 1
    }
}

impl<'a, O: OffsetSizeTrait> GeometryArrayAccessor<'a> for LineStringArray<O> {
    fn value(&'a self, index: usize) -> DFResult<Option<LineString<'a, O>>> {
        if self.is_null(index) {
            return Ok(None);
        }
        let line_string = LineString::try_new(
            Cow::Borrowed(&self.coords),
            Cow::Borrowed(&self.geom_offsets),
            index,
        )?;
        Ok(Some(line_string))
    }
}
