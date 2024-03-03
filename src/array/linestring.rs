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

#[derive(Debug)]
pub struct LineStringArrayBuilder<O: OffsetSizeTrait> {
    pub(crate) coords: CoordBufferBuilder,
    pub(crate) geom_offsets: Vec<O>,
    pub(crate) nulls: NullBufferBuilder,
}

impl<O: OffsetSizeTrait> LineStringArrayBuilder<O> {
    pub fn new(capacity: usize) -> Self {
        todo!()
    }

    pub fn push_geo_line_string(&mut self, value: Option<geo::LineString>) {
        todo!()
    }

    pub fn push_null(&mut self) {
        self.nulls.append_null();
    }

    pub fn build(self) -> LineStringArray<O> {
        todo!()
    }
}
