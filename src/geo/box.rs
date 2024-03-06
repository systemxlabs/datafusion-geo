use crate::DFResult;
use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, Float64Array, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use datafusion_common::{DataFusionError, ScalarValue};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Box2D {
    pub(crate) xmin: f64,
    pub(crate) ymin: f64,
    pub(crate) xmax: f64,
    pub(crate) ymax: f64,
}

impl Box2D {
    pub fn fields() -> Vec<Field> {
        vec![
            Field::new("xmin", DataType::Float64, false),
            Field::new("ymin", DataType::Float64, false),
            Field::new("xmax", DataType::Float64, false),
            Field::new("ymax", DataType::Float64, false),
        ]
    }
    pub fn data_type() -> DataType {
        DataType::Struct(Self::fields().into())
    }

    pub fn value(arr: &StructArray, index: usize) -> DFResult<Option<Box2D>> {
        if arr.data_type() != &Box2D::data_type() {
            return Err(DataFusionError::Internal(
                "StructArray data type is not matched".to_string(),
            ));
        }
        if index >= arr.len() || arr.is_null(index) {
            return Ok(None);
        }
        let scalar = ScalarValue::Struct(Arc::new(arr.slice(index, 1)));
        let box2d: Box2D = (&scalar).try_into()?;
        Ok(Some(box2d))
    }
}

impl TryFrom<&ScalarValue> for Box2D {
    type Error = DataFusionError;

    fn try_from(value: &ScalarValue) -> Result<Self, Self::Error> {
        if let ScalarValue::Struct(arr) = value {
            if arr.data_type() != &Box2D::data_type() {
                return Err(DataFusionError::Internal(
                    "ScalarValue data type is not matched".to_string(),
                ));
            }
            let xmin = arr.column(0).as_primitive::<Float64Type>().value(0);
            let ymin = arr.column(1).as_primitive::<Float64Type>().value(0);
            let xmax = arr.column(2).as_primitive::<Float64Type>().value(0);
            let ymax = arr.column(3).as_primitive::<Float64Type>().value(0);
            Ok(Box2D {
                xmin,
                ymin,
                xmax,
                ymax,
            })
        } else {
            Err(DataFusionError::Internal(
                "ScalarValue is not struct".to_string(),
            ))
        }
    }
}

impl From<geo::Rect> for Box2D {
    fn from(value: geo::Rect) -> Self {
        Self {
            xmin: value.min().x,
            ymin: value.min().y,
            xmax: value.max().x,
            ymax: value.max().y,
        }
    }
}

pub fn build_box2d_array(data: Vec<Option<Box2D>>) -> StructArray {
    let xmin_arr = Arc::new(Float64Array::from(
        data.iter()
            .map(|b| b.clone().map(|b| b.xmin))
            .collect::<Vec<_>>(),
    ));
    let ymin_arr = Arc::new(Float64Array::from(
        data.iter()
            .map(|b| b.clone().map(|b| b.ymin))
            .collect::<Vec<_>>(),
    ));
    let xmax_arr = Arc::new(Float64Array::from(
        data.iter()
            .map(|b| b.clone().map(|b| b.xmax))
            .collect::<Vec<_>>(),
    ));
    let ymax_arr = Arc::new(Float64Array::from(
        data.iter()
            .map(|b| b.clone().map(|b| b.ymax))
            .collect::<Vec<_>>(),
    ));
    let nulls: NullBuffer = data.iter().map(|b| b.is_some()).collect::<Vec<_>>().into();
    StructArray::try_new(
        Box2D::fields().into(),
        vec![xmin_arr, ymin_arr, xmax_arr, ymax_arr],
        Some(nulls),
    )
    .expect("data is valid")
}

#[cfg(test)]
mod tests {
    use crate::geo::r#box::{build_box2d_array, Box2D};
    use arrow_array::{Array, StructArray};

    #[test]
    fn box2d_array() {
        let box2d0 = Box2D {
            xmin: 1.0,
            ymin: 2.0,
            xmax: 3.0,
            ymax: 4.0,
        };
        let box2d2 = Box2D {
            xmin: 5.0,
            ymin: 6.0,
            xmax: 7.0,
            ymax: 8.0,
        };
        let arr: StructArray =
            build_box2d_array(vec![Some(box2d0.clone()), None, Some(box2d2.clone())]);
        assert_eq!(arr.len(), 3);

        assert_eq!(
            format!("{:?}", Box2D::value(&arr, 0).unwrap()),
            "Some(Box2D { xmin: 1.0, ymin: 2.0, xmax: 3.0, ymax: 4.0 })"
        );
        assert_eq!(format!("{:?}", Box2D::value(&arr, 1).unwrap()), "None");
        assert_eq!(
            format!("{:?}", Box2D::value(&arr, 2).unwrap()),
            "Some(Box2D { xmin: 5.0, ymin: 6.0, xmax: 7.0, ymax: 8.0 })"
        );
        assert_eq!(format!("{:?}", Box2D::value(&arr, 3).unwrap()), "None");
    }
}
