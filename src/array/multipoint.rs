use crate::array::util::check_nulls;
use crate::array::{GeometryArrayAccessor, GeometryArrayTrait, PointArray};
use crate::buffer::CoordBuffer;
use crate::scalar::MultiPoint;
use crate::DFResult;
use arrow::array::{ArrayRef, OffsetSizeTrait};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use datafusion::error::DataFusionError;
use std::borrow::Cow;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MultiPointArray<O: OffsetSizeTrait> {
    pub(crate) coords: CoordBuffer,
    pub(crate) geom_offsets: OffsetBuffer<O>,
    pub(crate) nulls: Option<NullBuffer>,
}

impl<O: OffsetSizeTrait> MultiPointArray<O> {
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

impl<O: OffsetSizeTrait> GeometryArrayTrait for MultiPointArray<O> {
    fn geo_type_id() -> i8 {
        4
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn len(&self) -> usize {
        self.geom_offsets.len() - 1
    }

    fn extension_name() -> &'static str {
        "geoarrow.multipoint"
    }

    fn data_type() -> DataType {
        let points_field = Field::new("points", PointArray::data_type(), false);
        match O::IS_LARGE {
            true => DataType::LargeList(Arc::new(points_field)),
            false => DataType::List(Arc::new(points_field)),
        }
    }

    fn into_arrow_array(self) -> ArrayRef {
        todo!()
    }
}

impl<'a, O: OffsetSizeTrait> GeometryArrayAccessor<'a> for MultiPointArray<O> {
    fn value(&'a self, index: usize) -> DFResult<Option<MultiPoint<'a, O>>> {
        if self.is_null(index) {
            return Ok(None);
        }
        let multi_point = MultiPoint::try_new(
            Cow::Borrowed(&self.coords),
            Cow::Borrowed(&self.geom_offsets),
            index,
        )?;
        Ok(Some(multi_point))
    }
}
