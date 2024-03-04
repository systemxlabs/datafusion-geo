use crate::DFResult;
use arrow::array::{FixedSizeListArray, Float64Array};
use arrow::buffer::{Buffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field};
use datafusion::common::DataFusionError;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct CoordBuffer {
    pub(crate) coords: ScalarBuffer<f64>,
}

impl CoordBuffer {
    pub fn try_new(coords: ScalarBuffer<f64>) -> DFResult<Self> {
        if coords.len() % 2 != 0 {
            return Err(DataFusionError::Internal(
                "x and y arrays must have the same length".to_string(),
            ));
        }
        Ok(Self { coords })
    }

    pub fn values_field() -> Field {
        Field::new("coord", DataType::Float64, false)
    }

    pub fn x(&self, i: usize) -> Option<f64> {
        self.coords.get(i * 2).cloned()
    }

    pub fn y(&self, i: usize) -> Option<f64> {
        self.coords.get(i * 2 + 1).cloned()
    }

    pub fn len(&self) -> usize {
        self.coords.len() / 2
    }

    pub(crate) fn slice(&self, offset: usize, length: usize) -> DFResult<Self> {
        if offset + length > self.len() {
            return Err(DataFusionError::Internal(
                "offset + length may not exceed length of array".to_string(),
            ));
        }
        Ok(Self {
            coords: self.coords.slice(offset * 2, length * 2),
        })
    }
}

impl From<CoordBuffer> for FixedSizeListArray {
    fn from(value: CoordBuffer) -> Self {
        FixedSizeListArray::new(
            Arc::new(CoordBuffer::values_field()),
            2,
            Arc::new(Float64Array::new(value.coords, None)),
            None,
        )
    }
}

impl TryFrom<&FixedSizeListArray> for CoordBuffer {
    type Error = DataFusionError;

    fn try_from(value: &FixedSizeListArray) -> Result<Self, Self::Error> {
        if value.value_length() != 2 {
            return Err(DataFusionError::Internal(
                "Expected this FixedSizeListArray to have size 2".to_string(),
            ));
        }

        if let Some(coord_array_values) = value.values().as_any().downcast_ref::<Float64Array>() {
            CoordBuffer::try_new(coord_array_values.values().clone())
        } else {
            Err(DataFusionError::Internal(
                "Cannot downcast FixedSizeListArray values to Float64Array".to_string(),
            ))
        }
    }
}

impl TryFrom<Vec<f64>> for CoordBuffer {
    type Error = DataFusionError;

    fn try_from(value: Vec<f64>) -> Result<Self, Self::Error> {
        Self::try_new(value.into())
    }
}

impl TryFrom<&[f64]> for CoordBuffer {
    type Error = DataFusionError;

    fn try_from(value: &[f64]) -> Result<Self, Self::Error> {
        Self::try_new(Buffer::from_slice_ref(value).into())
    }
}

#[cfg(feature = "geos")]
impl TryFrom<CoordBuffer> for geos::CoordSeq<'_> {
    type Error = DataFusionError;

    fn try_from(value: CoordBuffer) -> Result<Self, Self::Error> {
        geos::CoordSeq::new_from_buffer(&value.coords, value.len(), false, false).map_err(|e| {
            DataFusionError::Internal("Cannot convert coord buffer to geos CoordSeq".to_string())
        })
    }
}

impl From<&[geo::Coord]> for CoordBuffer {
    fn from(value: &[geo::Coord]) -> Self {
        let mut builder = CoordBufferBuilder::new(value.len());
        for coord in value {
            builder.push_geo_coord(coord);
        }
        builder.build()
    }
}

#[derive(Debug)]
pub struct CoordBufferBuilder {
    pub coords: Vec<f64>,
}

impl CoordBufferBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            coords: Vec::with_capacity(capacity * 2),
        }
    }

    pub fn len(&self) -> usize {
        self.coords.len() / 2
    }

    pub fn push_xy(&mut self, x: f64, y: f64) {
        self.coords.push(x);
        self.coords.push(y);
    }

    pub fn push_geo_coord(&mut self, coord: &geo::Coord) {
        self.coords.push(coord.x);
        self.coords.push(coord.y);
    }

    pub fn build(self) -> CoordBuffer {
        CoordBuffer::try_new(self.coords.into()).expect("builder has checked")
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::CoordBuffer;
    use geo::coord;

    #[test]
    pub fn test_coord_buffer() {
        let c0 = coord! {x: 0.0, y: 1.0 };
        let c1 = coord! {x: 1.0, y: 2.0 };
        let c2 = coord! {x: 2.0, y: 3.0 };
        let buffer: CoordBuffer = vec![c0, c1, c2].as_slice().into();

        assert_eq!(buffer.x(1), Some(c1.x));
        assert_eq!(buffer.y(2), Some(c2.y));

        let sliced_buffer = buffer.slice(0, 2).unwrap();

        assert_eq!(sliced_buffer.x(0), Some(c0.x));
        assert_eq!(sliced_buffer.y(0), Some(c0.y));
        assert_eq!(sliced_buffer.x(1), Some(c1.x));
        assert_eq!(sliced_buffer.y(1), Some(c1.y));
    }
}
