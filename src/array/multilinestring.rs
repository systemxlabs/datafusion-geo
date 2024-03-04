use crate::array::util::check_nulls;
use crate::array::{GeometryArrayAccessor, GeometryArrayTrait, PointArray};
use crate::buffer::{CoordBuffer, CoordBufferBuilder};
use crate::scalar::MultiLineString;
use crate::DFResult;
use arrow::array::{ArrayRef, OffsetSizeTrait};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use arrow_buffer::NullBufferBuilder;
use datafusion::error::DataFusionError;
use std::borrow::Cow;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MultiLineStringArray<O: OffsetSizeTrait> {
    pub(crate) coords: CoordBuffer,
    pub(crate) geom_offsets: OffsetBuffer<O>,
    pub(crate) ring_offsets: OffsetBuffer<O>,
    pub(crate) nulls: Option<NullBuffer>,
}

impl<O: OffsetSizeTrait> MultiLineStringArray<O> {
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

impl<O: OffsetSizeTrait> GeometryArrayTrait for MultiLineStringArray<O> {
    fn geo_type_id() -> i8 {
        5
    }
    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn len(&self) -> usize {
        self.geom_offsets.len() - 1
    }

    fn extension_name() -> &'static str {
        "geoarrow.multilinestring"
    }

    fn data_type() -> DataType {
        let vertices_field = Field::new("vertices", PointArray::data_type(), false);
        let linestrings_field = match O::IS_LARGE {
            true => Field::new_large_list("linestrings", vertices_field, true),
            false => Field::new_list("linestrings", vertices_field, true),
        };
        match O::IS_LARGE {
            true => DataType::LargeList(Arc::new(linestrings_field)),
            false => DataType::List(Arc::new(linestrings_field)),
        }
    }

    fn into_arrow_array(self) -> ArrayRef {
        todo!()
    }
}

impl<'a, O: OffsetSizeTrait> GeometryArrayAccessor<'a> for MultiLineStringArray<O> {
    fn value(&'a self, index: usize) -> DFResult<Option<MultiLineString<O>>> {
        if self.is_null(index) {
            return Ok(None);
        }
        let multi_linestring = MultiLineString::try_new(
            Cow::Borrowed(&self.coords),
            Cow::Borrowed(&self.geom_offsets),
            Cow::Borrowed(&self.ring_offsets),
            index,
        )?;
        Ok(Some(multi_linestring))
    }
}

pub struct MultiLineStringArrayBuilder<O: OffsetSizeTrait> {
    coords: CoordBufferBuilder,
    geom_offsets: Vec<O>,
    ring_offsets: Vec<O>,
    nulls: NullBufferBuilder,
}

impl<O: OffsetSizeTrait> MultiLineStringArrayBuilder<O> {
    pub fn new(capacity: usize) -> Self {
        Self {
            coords: CoordBufferBuilder::new(capacity),
            geom_offsets: Vec::with_capacity(capacity),
            ring_offsets: Vec::with_capacity(capacity),
            nulls: NullBufferBuilder::new(capacity),
        }
    }

    pub fn push_geo_multi_line_string(&mut self, value: Option<geo::MultiLineString>) {
        if let Some(multi_line_string) = value {
            todo!()
        } else {
            self.push_null();
        }
    }

    pub fn push_null(&mut self) {
        self.geom_offsets.push(O::usize_as(self.ring_offsets.len()));
        self.nulls.append_null();
    }
}
