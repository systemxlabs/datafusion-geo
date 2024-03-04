use crate::array::util::check_nulls;
use crate::array::{GeometryArrayAccessor, GeometryArrayTrait, PointArray};
use crate::buffer::{CoordBuffer, CoordBufferBuilder};
use crate::scalar::Polygon;
use crate::DFResult;
use arrow::array::{ArrayRef, OffsetSizeTrait};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use arrow_buffer::NullBufferBuilder;
use datafusion::error::DataFusionError;
use geo::CoordsIter;
use std::borrow::Cow;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PolygonArray<O: OffsetSizeTrait> {
    pub(crate) coords: CoordBuffer,
    pub(crate) geom_offsets: OffsetBuffer<O>,
    pub(crate) ring_offsets: OffsetBuffer<O>,
    pub(crate) nulls: Option<NullBuffer>,
}

impl<O: OffsetSizeTrait> PolygonArray<O> {
    pub fn try_new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<O>,
        ring_offsets: OffsetBuffer<O>,
        nulls: Option<NullBuffer>,
    ) -> DFResult<Self> {
        check_nulls(&nulls, geom_offsets.len() - 1)?;

        if ring_offsets.last().unwrap().to_usize().unwrap() != coords.len() {
            return Err(DataFusionError::Internal(
                "largest ring offset must match coords length".to_string(),
            ));
        }

        if geom_offsets.last().unwrap().to_usize().unwrap() != ring_offsets.len() - 1 {
            return Err(DataFusionError::Internal(
                "largest geometry offset must match ring offsets length".to_string(),
            ));
        }

        Ok(Self {
            coords,
            geom_offsets,
            ring_offsets,
            nulls,
        })
    }
}

impl<O: OffsetSizeTrait> GeometryArrayTrait for PolygonArray<O> {
    fn geo_type_id() -> i8 {
        3
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn len(&self) -> usize {
        self.geom_offsets.len() - 1
    }

    fn extension_name() -> &'static str {
        "geoarrow.polygon"
    }

    fn data_type() -> DataType {
        let vertices_field = Field::new("vertices", PointArray::data_type(), false);
        let rings_field = match O::IS_LARGE {
            true => Field::new_large_list("rings", vertices_field, true),
            false => Field::new_list("rings", vertices_field, true),
        };
        match O::IS_LARGE {
            true => DataType::LargeList(Arc::new(rings_field)),
            false => DataType::List(Arc::new(rings_field)),
        }
    }

    fn into_arrow_array(self) -> ArrayRef {
        todo!()
    }
}

impl<'a, O: OffsetSizeTrait> GeometryArrayAccessor<'a> for PolygonArray<O> {
    fn value(&'a self, index: usize) -> DFResult<Option<Polygon<O>>> {
        if self.is_null(index) {
            return Ok(None);
        }
        let polygon = Polygon::try_new(
            Cow::Borrowed(&self.coords),
            Cow::Borrowed(&self.geom_offsets),
            Cow::Borrowed(&self.ring_offsets),
            index,
        )?;
        Ok(Some(polygon))
    }
}

impl<O: OffsetSizeTrait> From<&[Option<geo::Polygon>]> for PolygonArray<O> {
    fn from(value: &[Option<geo::Polygon>]) -> Self {
        let mut builder = PolygonArrayBuilder::new(value.len());
        for polygon in value {
            builder.push_geo_polygon(polygon.clone());
        }
        builder.build()
    }
}

#[derive(Debug)]
pub struct PolygonArrayBuilder<O: OffsetSizeTrait> {
    pub(crate) coords: CoordBufferBuilder,
    pub(crate) geom_offsets: Vec<O>,
    pub(crate) ring_offsets: Vec<O>,
    pub(crate) nulls: NullBufferBuilder,
}

impl<O: OffsetSizeTrait> PolygonArrayBuilder<O> {
    pub fn new(capacity: usize) -> Self {
        Self {
            coords: CoordBufferBuilder::new(capacity),
            geom_offsets: Vec::with_capacity(capacity),
            ring_offsets: Vec::with_capacity(capacity),
            nulls: NullBufferBuilder::new(capacity),
        }
    }

    pub fn push_geo_polygon(&mut self, value: Option<geo::Polygon>) {
        if let Some(polygon) = value {
            let exterior_ring = polygon.exterior();
            for coord in exterior_ring.coords() {
                self.coords.push_geo_coord(&coord);
            }
            if let Some(last_offset) = self.ring_offsets.last() {
                let length = O::usize_as(exterior_ring.coords_count());
                let offset = *last_offset + length;
                self.ring_offsets.push(offset);
            } else {
                self.ring_offsets.push(O::zero());
            }

            // Total number of rings in this polygon
            if let Some(last_offset) = self.geom_offsets.last() {
                let length = O::usize_as(polygon.num_interior_rings());
                let offset = *last_offset + length;
                self.geom_offsets.push(offset);
            } else {
                self.geom_offsets.push(O::zero());
            }

            for interior_ring in polygon.interiors() {
                if let Some(last_offset) = self.ring_offsets.last() {
                    let length = O::usize_as(interior_ring.coords_count());
                    let offset = *last_offset + length;
                    self.ring_offsets.push(offset);
                } else {
                    self.ring_offsets.push(O::zero());
                }
                for coord in interior_ring.coords() {
                    self.coords.push_geo_coord(&coord);
                }
            }

            self.nulls.append(true);
        } else {
            self.push_null();
        }
    }

    pub fn push_null(&mut self) {
        let last_offset = self.geom_offsets.last().copied().unwrap_or(O::zero());
        self.geom_offsets.push(last_offset);
        self.nulls.append_null();
    }

    pub fn build(mut self) -> PolygonArray<O> {
        let coords = self.coords.build();
        self.ring_offsets.push(O::usize_as(coords.len()));
        let ring_offsets = OffsetBuffer::new(self.ring_offsets.into());
        self.geom_offsets.push(O::usize_as(ring_offsets.len() - 1));
        PolygonArray::try_new(
            coords,
            OffsetBuffer::new(self.geom_offsets.into()),
            ring_offsets,
            self.nulls.finish_cloned(),
        )
        .expect("builder has checked")
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{GeometryArrayAccessor, GeometryArrayTrait, PolygonArray};
    use geo::polygon;

    #[test]
    #[ignore]
    pub fn test_polygon_array() {
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
        let arr: PolygonArray<i64> = vec![Some(p0.clone()), None, Some(p2.clone())]
            .as_slice()
            .into();
        assert_eq!(arr.len(), 3);

        let mut iterator = arr.iter_geo();
        assert_eq!(iterator.next(), Some(Some(geo::Geometry::Polygon(p0))));
        assert_eq!(iterator.next(), Some(None));
        assert_eq!(iterator.next(), Some(Some(geo::Geometry::Polygon(p2))));
        assert_eq!(iterator.next(), None);
    }
}
