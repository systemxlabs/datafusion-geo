use crate::geo::dialect::decode_wkb_dialect;
use crate::DFResult;
use arrow_array::types::GenericBinaryType;
use arrow_array::{Array, GenericByteArray, OffsetSizeTrait};
use datafusion_common::{internal_datafusion_err, DataFusionError};
use geozero::wkb::FromWkb;

pub trait GeometryArray {
    fn geom_len(&self) -> usize;

    fn wkb(&self, geom_index: usize) -> Option<&[u8]>;

    fn geo_value(&self, geom_index: usize) -> DFResult<Option<geo::Geometry>> {
        if let Some(wkb) = self.wkb(geom_index) {
            let dialect = decode_wkb_dialect(wkb[0])?;
            let mut rdr = std::io::Cursor::new(&wkb[1..]);
            let value = geo::Geometry::from_wkb(&mut rdr, dialect)
                .map_err(|e| internal_datafusion_err!("Failed to parse wkb, error: {}", e))?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    #[cfg(feature = "geos")]
    fn geos_value(&self, geom_index: usize) -> DFResult<Option<geos::Geometry>> {
        if let Some(wkb) = self.wkb(geom_index) {
            let dialect = decode_wkb_dialect(wkb[0])?;
            let mut rdr = std::io::Cursor::new(&wkb[1..]);
            let value = geos::Geometry::from_wkb(&mut rdr, dialect)
                .map_err(|e| internal_datafusion_err!("Failed to parse wkb, error: {}", e))?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }
}

impl<O: OffsetSizeTrait> GeometryArray for GenericByteArray<GenericBinaryType<O>> {
    fn geom_len(&self) -> usize {
        self.len()
    }

    fn wkb(&self, geom_index: usize) -> Option<&[u8]> {
        if geom_index >= self.geom_len() || self.is_null(geom_index) {
            return None;
        }
        Some(self.value(geom_index))
    }
}

#[cfg(test)]
mod tests {
    use crate::geo::{GeometryArray, GeometryArrayBuilder};
    use geo::{line_string, point, polygon};

    #[test]
    fn point_array() {
        let p0 = point!(x: 0f64, y: 1f64);
        let p2 = point!(x: 2f64, y: 3f64);
        let builder: GeometryArrayBuilder<i32> = vec![Some(p0), None, Some(p2)].as_slice().into();
        let arr = builder.build();
        assert_eq!(arr.geom_len(), 3);

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
        assert_eq!(arr.geom_len(), 3);

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

    #[test]
    fn polygon_array() {
        let p0 = polygon![
            (x: -111., y: 45.),
            (x: -111., y: 41.),
            (x: -104., y: 41.),
            (x: -104., y: 45.),
        ];
        let p2 = polygon!(
            exterior: [
                (x: -111., y: 45.),
                (x: -111., y: 41.),
                (x: -104., y: 41.),
                (x: -104., y: 45.),
            ],
            interiors: [
                [
                    (x: -110., y: 44.),
                    (x: -110., y: 42.),
                    (x: -105., y: 42.),
                    (x: -105., y: 44.),
                ],
            ],
        );
        let builder: GeometryArrayBuilder<i32> = vec![Some(p0.clone()), None, Some(p2.clone())]
            .as_slice()
            .into();
        let arr = builder.build();
        assert_eq!(arr.geom_len(), 3);

        assert_eq!(arr.geo_value(0).unwrap(), Some(geo::Geometry::Polygon(p0)));
        assert_eq!(arr.geo_value(1).unwrap(), None);
        assert_eq!(arr.geo_value(2).unwrap(), Some(geo::Geometry::Polygon(p2)));
        assert_eq!(arr.geo_value(3).unwrap(), None);
    }

    #[test]
    fn multi_point_array() {
        let mp0 = geo::MultiPoint::new(vec![
            point!(
                x: 0., y: 1.
            ),
            point!(
                x: 1., y: 2.
            ),
        ]);
        let mp2 = geo::MultiPoint::new(vec![
            point!(
                x: 3., y: 4.
            ),
            point!(
                x: 5., y: 6.
            ),
        ]);
        let builder: GeometryArrayBuilder<i32> = vec![Some(mp0.clone()), None, Some(mp2.clone())]
            .as_slice()
            .into();
        let arr = builder.build();
        assert_eq!(arr.geom_len(), 3);

        assert_eq!(
            arr.geo_value(0).unwrap(),
            Some(geo::Geometry::MultiPoint(mp0))
        );
        assert_eq!(arr.geo_value(1).unwrap(), None);
        assert_eq!(
            arr.geo_value(2).unwrap(),
            Some(geo::Geometry::MultiPoint(mp2))
        );
        assert_eq!(arr.geo_value(3).unwrap(), None);
    }

    #[test]
    fn multi_line_string_array() {
        let ml0 = geo::MultiLineString::new(vec![line_string![
            (x: -111., y: 45.),
            (x: -111., y: 41.),
            (x: -104., y: 41.),
            (x: -104., y: 45.),
        ]]);
        let ml2 = geo::MultiLineString::new(vec![
            line_string![
                (x: -111., y: 45.),
                (x: -111., y: 41.),
                (x: -104., y: 41.),
                (x: -104., y: 45.),
            ],
            line_string![
                (x: -110., y: 44.),
                (x: -110., y: 42.),
                (x: -105., y: 42.),
                (x: -105., y: 44.),
            ],
        ]);

        let builder: GeometryArrayBuilder<i32> = vec![Some(ml0.clone()), None, Some(ml2.clone())]
            .as_slice()
            .into();
        let arr = builder.build();
        assert_eq!(arr.geom_len(), 3);

        assert_eq!(
            arr.geo_value(0).unwrap(),
            Some(geo::Geometry::MultiLineString(ml0))
        );
        assert_eq!(arr.geo_value(1).unwrap(), None);
        assert_eq!(
            arr.geo_value(2).unwrap(),
            Some(geo::Geometry::MultiLineString(ml2))
        );
        assert_eq!(arr.geo_value(3).unwrap(), None);
    }

    #[test]
    fn multi_polygon_array() {
        let mp0 = geo::MultiPolygon::new(vec![
            polygon![
                (x: -111., y: 45.),
                (x: -111., y: 41.),
                (x: -104., y: 41.),
                (x: -104., y: 45.),
            ],
            polygon!(
                exterior: [
                    (x: -111., y: 45.),
                    (x: -111., y: 41.),
                    (x: -104., y: 41.),
                    (x: -104., y: 45.),
                ],
                interiors: [
                    [
                        (x: -110., y: 44.),
                        (x: -110., y: 42.),
                        (x: -105., y: 42.),
                        (x: -105., y: 44.),
                    ],
                ],
            ),
        ]);
        let mp2 = geo::MultiPolygon::new(vec![
            polygon![
                (x: -111., y: 45.),
                (x: -111., y: 41.),
                (x: -104., y: 41.),
                (x: -104., y: 45.),
            ],
            polygon![
                (x: -110., y: 44.),
                (x: -110., y: 42.),
                (x: -105., y: 42.),
                (x: -105., y: 44.),
            ],
        ]);

        let builder: GeometryArrayBuilder<i32> = vec![Some(mp0.clone()), None, Some(mp2.clone())]
            .as_slice()
            .into();
        let arr = builder.build();
        assert_eq!(arr.geom_len(), 3);

        assert_eq!(
            arr.geo_value(0).unwrap(),
            Some(geo::Geometry::MultiPolygon(mp0))
        );
        assert_eq!(arr.geo_value(1).unwrap(), None);
        assert_eq!(
            arr.geo_value(2).unwrap(),
            Some(geo::Geometry::MultiPolygon(mp2))
        );
        assert_eq!(arr.geo_value(3).unwrap(), None);
    }
}
