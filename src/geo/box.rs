use crate::DFResult;
use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, Float64Array, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use datafusion_common::{internal_err, DataFusionError, ScalarValue};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Box2d {
    pub(crate) xmin: f64,
    pub(crate) ymin: f64,
    pub(crate) xmax: f64,
    pub(crate) ymax: f64,
}

impl Box2d {
    pub fn new() -> Self {
        Self {
            xmin: f64::MAX,
            ymin: f64::MAX,
            xmax: f64::MIN,
            ymax: f64::MIN,
        }
    }
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

    pub fn value(arr: &StructArray, index: usize) -> DFResult<Option<Box2d>> {
        if arr.data_type() != &Box2d::data_type() {
            return internal_err!("StructArray data type is not matched");
        }
        if index >= arr.len() || arr.is_null(index) {
            return Ok(None);
        }
        let scalar = ScalarValue::Struct(Arc::new(arr.slice(index, 1)));
        let box2d: Box2d = (&scalar).try_into()?;
        Ok(Some(box2d))
    }
}

impl Default for Box2d {
    fn default() -> Self {
        Self::new()
    }
}

impl TryFrom<&ScalarValue> for Box2d {
    type Error = DataFusionError;

    fn try_from(value: &ScalarValue) -> Result<Self, Self::Error> {
        if let ScalarValue::Struct(arr) = value {
            if arr.data_type() != &Box2d::data_type() {
                return internal_err!("ScalarValue data type is not matched");
            }
            let xmin = arr.column(0).as_primitive::<Float64Type>().value(0);
            let ymin = arr.column(1).as_primitive::<Float64Type>().value(0);
            let xmax = arr.column(2).as_primitive::<Float64Type>().value(0);
            let ymax = arr.column(3).as_primitive::<Float64Type>().value(0);
            Ok(Box2d {
                xmin,
                ymin,
                xmax,
                ymax,
            })
        } else {
            internal_err!("ScalarValue is not struct")
        }
    }
}

impl From<Box2d> for ScalarValue {
    fn from(value: Box2d) -> Self {
        let arr = build_box2d_array(vec![Some(value)]);
        ScalarValue::Struct(Arc::new(arr))
    }
}

impl From<geo::Rect> for Box2d {
    fn from(value: geo::Rect) -> Self {
        Self {
            xmin: value.min().x,
            ymin: value.min().y,
            xmax: value.max().x,
            ymax: value.max().y,
        }
    }
}

#[cfg(feature = "geos")]
impl TryFrom<geos::Geometry<'_>> for Box2d {
    type Error = DataFusionError;

    fn try_from(value: geos::Geometry) -> Result<Self, Self::Error> {
        use datafusion_common::internal_datafusion_err;
        use geos::Geom;
        let xmin = value
            .get_x_min()
            .map_err(|_| internal_datafusion_err!("geom get_x_min failed"))?;
        let ymin = value
            .get_y_min()
            .map_err(|_| internal_datafusion_err!("geom get_y_min failed"))?;
        let xmax = value
            .get_x_max()
            .map_err(|_| internal_datafusion_err!("geom get_x_max failed"))?;
        let ymax = value
            .get_y_max()
            .map_err(|_| internal_datafusion_err!("geom get_y_max failed"))?;
        Ok(Box2d {
            xmin,
            ymin,
            xmax,
            ymax,
        })
    }
}

pub fn build_box2d_array(data: Vec<Option<Box2d>>) -> StructArray {
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
        Box2d::fields().into(),
        vec![xmin_arr, ymin_arr, xmax_arr, ymax_arr],
        Some(nulls),
    )
    .expect("data is valid")
}

#[cfg(test)]
mod tests {
    use crate::geo::r#box::{build_box2d_array, Box2d};
    use arrow_array::{Array, StructArray};

    #[test]
    fn box2d_array() {
        let box2d0 = Box2d {
            xmin: 1.0,
            ymin: 2.0,
            xmax: 3.0,
            ymax: 4.0,
        };
        let box2d2 = Box2d {
            xmin: 5.0,
            ymin: 6.0,
            xmax: 7.0,
            ymax: 8.0,
        };
        let arr: StructArray =
            build_box2d_array(vec![Some(box2d0.clone()), None, Some(box2d2.clone())]);
        assert_eq!(arr.len(), 3);

        assert_eq!(
            format!("{:?}", Box2d::value(&arr, 0).unwrap()),
            "Some(Box2d { xmin: 1.0, ymin: 2.0, xmax: 3.0, ymax: 4.0 })"
        );
        assert_eq!(format!("{:?}", Box2d::value(&arr, 1).unwrap()), "None");
        assert_eq!(
            format!("{:?}", Box2d::value(&arr, 2).unwrap()),
            "Some(Box2d { xmin: 5.0, ymin: 6.0, xmax: 7.0, ymax: 8.0 })"
        );
        assert_eq!(format!("{:?}", Box2d::value(&arr, 3).unwrap()), "None");
    }
}
