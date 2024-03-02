use crate::array::util::check_nulls;
use crate::array::{GeometryArrayAccessor, GeometryArrayTrait};
use crate::buffer::CoordBuffer;
use crate::scalar::Point;
use crate::DFResult;
use arrow::array::{Array, FixedSizeListArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub struct PointArray {
    pub(crate) coords: CoordBuffer,
    pub(crate) nulls: Option<NullBuffer>,
}

impl PointArray {
    pub fn try_new(coords: CoordBuffer, nulls: Option<NullBuffer>) -> DFResult<Self> {
        check_nulls(&nulls, coords.len())?;
        Ok(Self { coords, nulls })
    }
}

impl GeometryArrayTrait for PointArray {
    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn len(&self) -> usize {
        self.coords.len()
    }
}

impl<'a> GeometryArrayAccessor<'a> for PointArray {
    fn value(&'a self, index: usize) -> DFResult<Option<Point<'a>>> {
        if self.is_null(index) {
            Ok(None)
        } else {
            let point = Point::try_new(Cow::Borrowed(&self.coords), index)?;
            Ok(Some(point))
        }
    }
}

impl TryFrom<&FixedSizeListArray> for PointArray {
    type Error = DataFusionError;

    fn try_from(value: &FixedSizeListArray) -> Result<Self, Self::Error> {
        let coords: CoordBuffer = value.try_into()?;

        Self::try_new(coords, value.nulls().cloned())
    }
}

impl TryFrom<&dyn Array> for PointArray {
    type Error = DataFusionError;

    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type() {
            DataType::FixedSizeList(_, _) => {
                let arr = value.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                arr.try_into()
            }
            _ => Err(DataFusionError::Internal(
                "Invalid data type for PointArray".to_string(),
            )),
        }
    }
}
