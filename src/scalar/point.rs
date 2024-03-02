use crate::buffer::CoordBuffer;
use crate::scalar::GeometryScalarTrait;
use crate::DFResult;
use datafusion::common::DataFusionError;
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub struct Point<'a> {
    coords: Cow<'a, CoordBuffer>,
    geom_index: usize,
}

impl<'a> Point<'a> {
    pub fn try_new(coords: Cow<'a, CoordBuffer>, geom_index: usize) -> DFResult<Self> {
        if geom_index >= coords.len() {
            return Err(DataFusionError::Internal(format!(
                "geom_index {} is out of range of coords {}",
                geom_index,
                coords.len()
            )));
        }
        Ok(Self { coords, geom_index })
    }

    pub fn x(&self) -> f64 {
        self.coords
            .x(self.geom_index)
            .expect("geom_index has been checked")
    }

    pub fn y(&self) -> f64 {
        self.coords
            .y(self.geom_index)
            .expect("geom_index has been checked")
    }
}

impl<'a> GeometryScalarTrait for Point<'a> {
    fn to_geo(&self) -> geo::Geometry {
        geo::Geometry::Point(self.into())
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> DFResult<geos::Geometry> {
        let error = DataFusionError::Internal("Cannot convert point to geos Geometry".to_string());
        let mut coord_seq =
            geos::CoordSeq::new(1, geos::CoordDimensions::TwoD).map_err(|e| error)?;
        coord_seq.set_x(0, self.x()).map_err(|e| error)?;
        coord_seq.set_y(0, self.y()).map_err(|e| error)?;

        geos::Geometry::create_point(coord_seq).map_err(|e| error)
    }
}

impl From<Point<'_>> for geo::Point {
    fn from(value: Point<'_>) -> Self {
        (&value).into()
    }
}

impl From<&Point<'_>> for geo::Point {
    fn from(value: &Point<'_>) -> Self {
        geo::Point::new(value.x(), value.y())
    }
}

impl From<Point<'_>> for geo::Coord {
    fn from(value: Point<'_>) -> Self {
        (&value).into()
    }
}

impl From<&Point<'_>> for geo::Coord {
    fn from(value: &Point<'_>) -> Self {
        geo::Coord {
            x: value.x(),
            y: value.y(),
        }
    }
}
