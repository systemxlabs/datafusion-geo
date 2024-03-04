use crate::geo::dialect::wkb_type_id;
use arrow::array::{GenericByteArray, OffsetSizeTrait, UInt8BufferBuilder};
use arrow::datatypes::GenericBinaryType;
use arrow_buffer::{BufferBuilder, NullBufferBuilder, OffsetBuffer};
use geozero::wkb::WkbDialect;
use geozero::{CoordDimensions, ToWkb};

pub struct GeometryArrayBuilder<O: OffsetSizeTrait> {
    value_builder: UInt8BufferBuilder,
    offsets_builder: BufferBuilder<O>,
    null_buffer_builder: NullBufferBuilder,
}

impl<O: OffsetSizeTrait> GeometryArrayBuilder<O> {
    pub fn new(dialect: WkbDialect, capacity: usize) -> Self {
        let type_id = wkb_type_id(dialect);

        let mut value_builder = UInt8BufferBuilder::new(capacity);
        value_builder.append(type_id);

        let mut offsets_builder = BufferBuilder::<O>::new(capacity + 1);
        offsets_builder.append(O::from_usize(value_builder.len()).unwrap());

        Self {
            value_builder,
            offsets_builder,
            null_buffer_builder: NullBufferBuilder::new(capacity),
        }
    }

    #[inline]
    pub fn append_wkb(&mut self, wkb: Option<&[u8]>) {
        if let Some(wkb) = wkb {
            self.value_builder.append_slice(wkb);
            self.null_buffer_builder.append(true);
            self.offsets_builder.append(self.next_offset());
        } else {
            self.append_null();
        }
    }

    #[inline]
    pub fn append_null(&mut self) {
        self.null_buffer_builder.append_null();
        self.offsets_builder.append(self.next_offset());
    }

    #[inline]
    fn next_offset(&self) -> O {
        O::from_usize(self.value_builder.len()).expect("array offset overflow")
    }

    pub fn build(mut self) -> GenericByteArray<GenericBinaryType<O>> {
        GenericByteArray::new(
            OffsetBuffer::<O>::new(self.offsets_builder.finish().into()),
            self.value_builder.finish(),
            self.null_buffer_builder.finish(),
        )
    }
}

impl<O: OffsetSizeTrait> From<&[Option<geo::Geometry>]> for GeometryArrayBuilder<O> {
    fn from(value: &[Option<geo::Geometry>]) -> Self {
        let mut builder = GeometryArrayBuilder::<O>::new(WkbDialect::Wkb, value.len());
        for geom in value {
            let wkb = geom.as_ref().map(|g| {
                g.to_wkb(CoordDimensions::xy())
                    .expect("geometry data is valid")
            });
            builder.append_wkb(wkb.as_ref().map(|w| w.as_slice()));
        }
        builder
    }
}

impl<O: OffsetSizeTrait> From<&[Option<geo::Point>]> for GeometryArrayBuilder<O> {
    fn from(value: &[Option<geo::Point>]) -> Self {
        let geo_vec = value
            .iter()
            .map(|p| p.map(|p| geo::Geometry::Point(p)))
            .collect::<Vec<_>>();
        geo_vec.as_slice().into()
    }
}

impl<O: OffsetSizeTrait> From<&[Option<geo::LineString>]> for GeometryArrayBuilder<O> {
    fn from(value: &[Option<geo::LineString>]) -> Self {
        let geo_vec = value
            .iter()
            .map(|ls| ls.clone().map(|ls| geo::Geometry::LineString(ls)))
            .collect::<Vec<_>>();
        geo_vec.as_slice().into()
    }
}
