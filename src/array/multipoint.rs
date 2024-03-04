use crate::array::util::check_nulls;
use crate::array::{GeometryArrayAccessor, GeometryArrayTrait, PointArray};
use crate::buffer::{CoordBuffer, CoordBufferBuilder};
use crate::scalar::MultiPoint;
use crate::DFResult;
use arrow::array::{ArrayRef, OffsetSizeTrait};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use arrow_buffer::NullBufferBuilder;
use datafusion::error::DataFusionError;
use std::borrow::Cow;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MultiPointArray<O: OffsetSizeTrait> {
    pub(crate) coords: CoordBuffer,
    pub(crate) geom_offsets: OffsetBuffer<O>,
    pub(crate) nulls: Option<NullBuffer>,
}

impl<O: OffsetSizeTrait> MultiPointArray<O> {
    pub fn try_new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<O>,
        nulls: Option<NullBuffer>,
    ) -> DFResult<Self> {
        check_nulls(&nulls, geom_offsets.len() - 1)?;

        if geom_offsets.last().unwrap().to_usize().unwrap() != coords.len() {
            return Err(DataFusionError::Internal(
                "largest geometry offset must match coords length".to_string(),
            ));
        }

        Ok(Self {
            coords,
            geom_offsets,
            nulls,
        })
    }
}

impl<O: OffsetSizeTrait> GeometryArrayTrait for MultiPointArray<O> {
    fn geo_type_id() -> i8 {
        4
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn len(&self) -> usize {
        self.geom_offsets.len() - 1
    }

    fn extension_name() -> &'static str {
        "geoarrow.multipoint"
    }

    fn data_type() -> DataType {
        let points_field = Field::new("points", PointArray::data_type(), false);
        match O::IS_LARGE {
            true => DataType::LargeList(Arc::new(points_field)),
            false => DataType::List(Arc::new(points_field)),
        }
    }

    fn into_arrow_array(self) -> ArrayRef {
        todo!()
    }
}

impl<'a, O: OffsetSizeTrait> GeometryArrayAccessor<'a> for MultiPointArray<O> {
    fn value(&'a self, index: usize) -> DFResult<Option<MultiPoint<'a, O>>> {
        if self.is_null(index) {
            return Ok(None);
        }
        let multi_point = MultiPoint::try_new(
            Cow::Borrowed(&self.coords),
            Cow::Borrowed(&self.geom_offsets),
            index,
        )?;
        Ok(Some(multi_point))
    }
}

impl<O: OffsetSizeTrait> From<&[Option<geo::MultiPoint>]> for MultiPointArray<O> {
    fn from(value: &[Option<geo::MultiPoint>]) -> Self {
        let mut builder = MultiPointArrayBuilder::new(value.len());
        for mp in value {
            builder.push_geo_multipoint(mp.clone());
        }
        builder.build()
    }
}

pub struct MultiPointArrayBuilder<O: OffsetSizeTrait> {
    coords: CoordBufferBuilder,
    geom_offsets: Vec<O>,
    nulls: NullBufferBuilder,
}

impl<O: OffsetSizeTrait> MultiPointArrayBuilder<O> {
    pub fn new(capacity: usize) -> Self {
        Self {
            coords: CoordBufferBuilder::new(capacity),
            geom_offsets: Vec::with_capacity(capacity),
            nulls: NullBufferBuilder::new(capacity),
        }
    }

    pub fn push_geo_multipoint(&mut self, value: Option<geo::MultiPoint>) {
        if let Some(multipoint) = value {
            self.geom_offsets.push(O::usize_as(self.coords.len()));
            for point in multipoint.0.iter() {
                self.coords.push_xy(point.x(), point.y());
            }
            self.nulls.append(true);
        } else {
            self.push_null();
        }
    }

    pub fn push_null(&mut self) {
        self.geom_offsets.push(O::usize_as(self.coords.len()));
        self.nulls.append_null();
    }

    pub fn build(mut self) -> MultiPointArray<O> {
        let coords = self.coords.build();
        self.geom_offsets.push(O::usize_as(coords.len()));
        MultiPointArray::try_new(
            coords,
            OffsetBuffer::new(self.geom_offsets.into()),
            self.nulls.finish_cloned(),
        )
        .expect("builder has checked")
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{GeometryArrayAccessor, GeometryArrayTrait, MultiPointArray};

    #[test]
    fn test_multipoint_array() {
        let mp0 = geo::MultiPoint::new(vec![geo::Point::new(0.0, 1.0), geo::Point::new(2.0, 3.0)]);
        let mp2 = geo::MultiPoint::new(vec![geo::Point::new(4.0, 5.0), geo::Point::new(6.0, 7.0)]);

        let arr: MultiPointArray<i64> = vec![Some(mp0.clone()), None, Some(mp2.clone())]
            .as_slice()
            .into();
        assert_eq!(arr.len(), 3);

        let mut iterator = arr.iter_geo();
        assert_eq!(iterator.next(), Some(Some(geo::Geometry::MultiPoint(mp0))));
        assert_eq!(iterator.next(), Some(None));
        assert_eq!(iterator.next(), Some(Some(geo::Geometry::MultiPoint(mp2))));
        assert_eq!(iterator.next(), None);
    }
}
