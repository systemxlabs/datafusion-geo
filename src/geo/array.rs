use crate::geo::dialect::decode_wkb_dialect;
use crate::geo::GeometryArrayBuilder;
use crate::DFResult;
use arrow::array::{Array, GenericByteArray, OffsetSizeTrait};
use arrow::datatypes::GenericBinaryType;
use datafusion::common::DataFusionError;
use geozero::wkb::{FromWkb, WkbDialect};

pub trait GeometryArray {
    fn dialect(&self) -> DFResult<WkbDialect>;

    fn wkb(&self, index: usize) -> Option<&[u8]>;

    fn geo_value(&self, index: usize) -> DFResult<Option<geo::Geometry>> {
        if let Some(wkb) = self.wkb(index) {
            let dialect = self.dialect()?;
            let mut rdr = std::io::Cursor::new(wkb);
            let value = geo::Geometry::from_wkb(&mut rdr, dialect).map_err(|e| {
                DataFusionError::Internal(format!("Failed to parse wkb, error: {}", e))
            })?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    #[cfg(feature = "geos")]
    fn geos_value(&self, index: usize) -> DFResult<Option<geos::Geometry>> {
        if let Some(wkb) = self.wkb(index) {
            let dialect = self.dialect()?;
            let mut rdr = std::io::Cursor::new(wkb);
            let value = geos::Geometry::from_wkb(&mut rdr, dialect).map_err(|e| {
                DataFusionError::Internal(format!("Failed to parse wkb, error: {}", e))
            })?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }
}

impl<O: OffsetSizeTrait> GeometryArray for GenericByteArray<GenericBinaryType<O>> {
    fn dialect(&self) -> DFResult<WkbDialect> {
        if let Some(b) = self.value_data().first() {
            decode_wkb_dialect(*b)
        } else {
            Err(DataFusionError::Internal(
                "Cannot get dialect as data is empty".to_string(),
            ))
        }
    }

    fn wkb(&self, index: usize) -> Option<&[u8]> {
        if index >= self.len() {
            return None;
        }
        if self.is_null(index) {
            return None;
        }
        Some(self.value(index))
    }
}

#[cfg(test)]
mod tests {
    use crate::geo::{GeometryArray, GeometryArrayBuilder};
    use arrow::array::Array;
    use geo::{line_string, point};

    #[test]
    fn point_array() {
        let p0 = point!(x: 0f64, y: 1f64);
        let p2 = point!(x: 2f64, y: 3f64);
        let builder: GeometryArrayBuilder<i32> = vec![Some(p0), None, Some(p2)].as_slice().into();
        let arr = builder.build();
        assert_eq!(arr.len(), 3);

        assert_eq!(arr.geo_value(0).unwrap(), Some(geo::Geometry::Point(p0)));
        assert_eq!(arr.geo_value(1).unwrap(), None);
        assert_eq!(arr.geo_value(2).unwrap(), Some(geo::Geometry::Point(p2)));
        assert_eq!(arr.geo_value(3).unwrap(), None);
    }

    #[test]
    fn linestring_array() {
        let ls0 = line_string![
            (x: 0., y: 1.),
            (x: 1., y: 2.)
        ];
        let ls2 = line_string![
            (x: 3., y: 4.),
            (x: 5., y: 6.)
        ];
        let builder: GeometryArrayBuilder<i32> = vec![Some(ls0.clone()), None, Some(ls2.clone())]
            .as_slice()
            .into();
        let arr = builder.build();
        assert_eq!(arr.len(), 3);

        assert_eq!(
            arr.geo_value(0).unwrap(),
            Some(geo::Geometry::LineString(ls0))
        );
        assert_eq!(arr.geo_value(1).unwrap(), None);
        assert_eq!(
            arr.geo_value(2).unwrap(),
            Some(geo::Geometry::LineString(ls2))
        );
        assert_eq!(arr.geo_value(3).unwrap(), None);
    }
}
