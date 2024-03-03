use crate::array::util::check_nulls;
use crate::array::{GeometryArrayAccessor, GeometryArrayTrait};
use crate::buffer::{CoordBuffer, CoordBufferBuilder};
use crate::scalar::Point;
use crate::DFResult;
use arrow::array::{Array, ArrayRef, FixedSizeListArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType;
use arrow_buffer::NullBufferBuilder;
use datafusion::error::DataFusionError;
use std::borrow::Cow;
use std::sync::Arc;

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

    fn extension_name() -> &'static str {
        "geoarrow.point"
    }

    fn data_type() -> DataType {
        DataType::FixedSizeList(Arc::new(CoordBuffer::values_field()), 2)
    }

    fn into_arrow_array(self) -> ArrayRef {
        todo!()
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

#[derive(Debug)]
pub struct PointArrayBuilder {
    pub coords: CoordBufferBuilder,
    pub nulls: NullBufferBuilder,
}

impl PointArrayBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            coords: CoordBufferBuilder::new(capacity),
            nulls: NullBufferBuilder::new(capacity),
        }
    }

    pub fn push_geo_point(&mut self, value: Option<geo::Point>) {
        if let Some(value) = value {
            self.coords.push_xy(value.x(), value.y());
            self.nulls.append(true);
        } else {
            self.push_null()
        }
    }

    pub fn push_null(&mut self) {
        self.coords.push_xy(0., 0.);
        self.nulls.append(false);
    }

    pub fn build(self) -> DFResult<PointArray> {
        let coords = self.coords.build()?;
        let nulls = self.nulls.finish_cloned();
        PointArray::try_new(coords, nulls)
    }
}
