use crate::array::util::check_nulls;
use crate::array::{GeometryArrayAccessor, GeometryArrayTrait, PointArray};
use crate::buffer::{CoordBuffer, CoordBufferBuilder};
use crate::scalar::LineString;
use crate::DFResult;
use arrow::array::{ArrayRef, OffsetSizeTrait};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use arrow_buffer::NullBufferBuilder;
use datafusion::common::DataFusionError;
use std::borrow::Cow;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct LineStringArray<O: OffsetSizeTrait> {
    pub(crate) coords: CoordBuffer,
    pub(crate) geom_offsets: OffsetBuffer<O>,
    pub(crate) nulls: Option<NullBuffer>,
}

impl<O: OffsetSizeTrait> LineStringArray<O> {
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

impl<O: OffsetSizeTrait> GeometryArrayTrait for LineStringArray<O> {
    fn geo_type_id() -> i8 {
        2
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn len(&self) -> usize {
        self.geom_offsets.len() - 1
    }

    fn extension_name() -> &'static str {
        "geoarrow.linestring"
    }

    fn data_type() -> DataType {
        let vertices_field = Field::new("vertices", PointArray::data_type(), false);
        match O::IS_LARGE {
            true => DataType::LargeList(Arc::new(vertices_field)),
            false => DataType::List(Arc::new(vertices_field)),
        }
    }

    fn into_arrow_array(self) -> ArrayRef {
        todo!()
    }
}

impl<'a, O: OffsetSizeTrait> GeometryArrayAccessor<'a> for LineStringArray<O> {
    fn value(&'a self, index: usize) -> DFResult<Option<LineString<'a, O>>> {
        if self.is_null(index) {
            return Ok(None);
        }
        let line_string = LineString::try_new(
            Cow::Borrowed(&self.coords),
            Cow::Borrowed(&self.geom_offsets),
            index,
        )?;
        Ok(Some(line_string))
    }
}

impl<O: OffsetSizeTrait> From<&[Option<geo::LineString>]> for LineStringArray<O> {
    fn from(value: &[Option<geo::LineString>]) -> Self {
        let mut builder = LineStringArrayBuilder::<O>::new(value.len());
        for ls in value {
            builder.push_geo_line_string(ls.clone());
        }
        builder.build()
    }
}

/// Builder
#[derive(Debug)]
pub struct LineStringArrayBuilder<O: OffsetSizeTrait> {
    coords: CoordBufferBuilder,
    geom_offsets: Vec<O>,
    nulls: NullBufferBuilder,
}

impl<O: OffsetSizeTrait> LineStringArrayBuilder<O> {
    pub fn new(capacity: usize) -> Self {
        Self {
            coords: CoordBufferBuilder::new(capacity),
            geom_offsets: Vec::with_capacity(capacity),
            nulls: NullBufferBuilder::new(capacity),
        }
    }

    pub fn push_geo_line_string(&mut self, value: Option<geo::LineString>) {
        use geo::CoordsIter;
        if let Some(line_string) = value {
            let offset = O::usize_as(self.coords.len());
            for coord in line_string.coords() {
                self.coords.push_geo_coord(coord);
            }
            self.geom_offsets.push(offset);
            self.nulls.append(true);
        } else {
            self.push_null()
        }
    }

    pub fn push_null(&mut self) {
        self.geom_offsets.push(O::usize_as(self.coords.len()));
        self.nulls.append_null();
    }

    pub fn build(mut self) -> LineStringArray<O> {
        let coords = self.coords.build();
        self.geom_offsets.push(O::usize_as(coords.len()));
        LineStringArray::try_new(
            coords,
            OffsetBuffer::new(self.geom_offsets.into()),
            self.nulls.finish_cloned(),
        )
        .expect("builder has checked")
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{GeometryArrayAccessor, GeometryArrayTrait, LineStringArray};
    use geo::line_string;

    #[test]
    fn test_line_string_array() {
        let ls0 = line_string![
            (x: 0., y: 1.),
            (x: 1., y: 2.)
        ];
        let ls2 = line_string![
            (x: 3., y: 4.),
            (x: 5., y: 6.)
        ];
        let arr: LineStringArray<i64> = vec![Some(ls0.clone()), None, Some(ls2.clone())]
            .as_slice()
            .into();
        assert_eq!(arr.len(), 3);

        let mut iterator = arr.iter_geo();
        assert_eq!(iterator.next(), Some(Some(geo::Geometry::LineString(ls0))));
        assert_eq!(iterator.next(), Some(None));
        assert_eq!(iterator.next(), Some(Some(geo::Geometry::LineString(ls2))));
        assert_eq!(iterator.next(), None);
    }
}
