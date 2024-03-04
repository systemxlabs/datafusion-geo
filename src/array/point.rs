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
    fn geo_type_id() -> i8 {
        1
    }

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

impl From<&[Option<geo::Point>]> for PointArray {
    fn from(value: &[Option<geo::Point>]) -> Self {
        let mut builder = PointArrayBuilder::new(0);
        for p in value.iter() {
            builder.push_geo_point(p.clone());
        }
        builder.build()
    }
}

#[derive(Debug)]
pub struct PointArrayBuilder {
    coords: CoordBufferBuilder,
    nulls: NullBufferBuilder,
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

    pub fn build(self) -> PointArray {
        let coords = self.coords.build();
        let nulls = self.nulls.finish_cloned();
        PointArray::try_new(coords, nulls).expect("builder has checked")
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{GeometryArrayAccessor, GeometryArrayTrait, PointArray};
    use geo::point;

    #[test]
    fn test_point_array() {
        let p0 = point!(x: 0f64, y: 1f64);
        let p2 = point!(x: 2f64, y: 3f64);
        let arr: PointArray = vec![Some(p0), None, Some(p2)].as_slice().into();
        assert_eq!(arr.len(), 3);

        let mut iterator = arr.iter_geo();
        assert_eq!(iterator.next(), Some(Some(geo::Geometry::Point(p0))));
        assert_eq!(iterator.next(), Some(None));
        assert_eq!(iterator.next(), Some(Some(geo::Geometry::Point(p2))));
        assert_eq!(iterator.next(), None);
    }
}
