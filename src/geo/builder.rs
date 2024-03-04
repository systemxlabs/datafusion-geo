use crate::geo::dialect::wkb_type_id;
use crate::DFResult;
use arrow_array::builder::UInt8BufferBuilder;
use arrow_array::types::GenericBinaryType;
use arrow_array::{GenericByteArray, OffsetSizeTrait};
use arrow_buffer::{BufferBuilder, NullBufferBuilder, OffsetBuffer};
use datafusion_common::DataFusionError;
use geozero::wkb::{FromWkb, WkbDialect};
use geozero::{GeozeroGeometry, ToWkb};

pub struct GeometryArrayBuilder<O: OffsetSizeTrait> {
    dialect: WkbDialect,
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
            dialect,
            value_builder,
            offsets_builder,
            null_buffer_builder: NullBufferBuilder::new(capacity),
        }
    }

    #[inline]
    pub fn append_wkb(&mut self, wkb: Option<&[u8]>) -> DFResult<()> {
        if let Some(wkb) = wkb {
            check_wkb(wkb, self.dialect)?;
            self.internal_append_wkb(wkb);
        } else {
            self.append_null();
        }
        Ok(())
    }

    #[inline]
    pub fn append_geo_geometry(&mut self, geom: &Option<geo::Geometry>) -> DFResult<()> {
        if let Some(geom) = geom {
            let wkb = geom
                .to_wkb_dialect(self.dialect, geom.dims(), geom.srid(), vec![])
                .map_err(|e| {
                    DataFusionError::Internal(format!("Failed to convert to wkb, error: {}", e))
                })?;
            self.internal_append_wkb(&wkb);
        } else {
            self.append_null();
        }
        Ok(())
    }

    #[cfg(feature = "geos")]
    #[inline]
    pub fn append_geos_geometry(&mut self, geom: &Option<geos::Geometry>) -> DFResult<()> {
        if let Some(geom) = geom {
            let wkb = geom
                .to_wkb_dialect(self.dialect, geom.dims(), geom.srid(), vec![])
                .map_err(|e| {
                    DataFusionError::Internal(format!("Failed to convert to wkb, error: {}", e))
                })?;
            self.internal_append_wkb(&wkb);
        } else {
            self.append_null();
        }
        Ok(())
    }

    #[inline]
    pub fn append_null(&mut self) {
        self.null_buffer_builder.append_null();
        self.offsets_builder.append(self.next_offset());
    }

    fn internal_append_wkb(&mut self, wkb: &[u8]) {
        self.value_builder.append_slice(wkb);
        self.null_buffer_builder.append(true);
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

fn check_wkb(wkb: &[u8], dialect: WkbDialect) -> DFResult<()> {
    let mut rdr = std::io::Cursor::new(wkb);
    #[cfg(feature = "geos")]
    {
        let _ = geos::Geometry::from_wkb(&mut rdr, dialect)
            .map_err(|e| DataFusionError::Internal(format!("Failed to parse wkb, error: {}", e)))?;
    }
    #[cfg(not(feature = "geos"))]
    {
        let _ = geo::Geometry::from_wkb(&mut rdr, dialect)
            .map_err(|e| DataFusionError::Internal(format!("Failed to parse wkb, error: {}", e)))?;
    }
    Ok(())
}

impl<O: OffsetSizeTrait> From<&[Option<geo::Geometry>]> for GeometryArrayBuilder<O> {
    fn from(value: &[Option<geo::Geometry>]) -> Self {
        let mut builder = GeometryArrayBuilder::<O>::new(WkbDialect::Wkb, value.len());
        for geom in value {
            builder
                .append_geo_geometry(geom)
                .expect("geometry data is valid");
        }
        builder
    }
}

impl<O: OffsetSizeTrait> From<&[Option<geo::Point>]> for GeometryArrayBuilder<O> {
    fn from(value: &[Option<geo::Point>]) -> Self {
        let geo_vec = value
            .iter()
            .map(|p| p.map(geo::Geometry::Point))
            .collect::<Vec<_>>();
        geo_vec.as_slice().into()
    }
}

impl<O: OffsetSizeTrait> From<&[Option<geo::LineString>]> for GeometryArrayBuilder<O> {
    fn from(value: &[Option<geo::LineString>]) -> Self {
        let geo_vec = value
            .iter()
            .map(|ls| ls.clone().map(geo::Geometry::LineString))
            .collect::<Vec<_>>();
        geo_vec.as_slice().into()
    }
}

impl<O: OffsetSizeTrait> From<&[Option<geo::Polygon>]> for GeometryArrayBuilder<O> {
    fn from(value: &[Option<geo::Polygon>]) -> Self {
        let geo_vec = value
            .iter()
            .map(|p| p.clone().map(geo::Geometry::Polygon))
            .collect::<Vec<_>>();
        geo_vec.as_slice().into()
    }
}

impl<O: OffsetSizeTrait> From<&[Option<geo::MultiPoint>]> for GeometryArrayBuilder<O> {
    fn from(value: &[Option<geo::MultiPoint>]) -> Self {
        let geo_vec = value
            .iter()
            .map(|mp| mp.clone().map(geo::Geometry::MultiPoint))
            .collect::<Vec<_>>();
        geo_vec.as_slice().into()
    }
}

impl<O: OffsetSizeTrait> From<&[Option<geo::MultiLineString>]> for GeometryArrayBuilder<O> {
    fn from(value: &[Option<geo::MultiLineString>]) -> Self {
        let geo_vec = value
            .iter()
            .map(|ml| ml.clone().map(geo::Geometry::MultiLineString))
            .collect::<Vec<_>>();
        geo_vec.as_slice().into()
    }
}

impl<O: OffsetSizeTrait> From<&[Option<geo::MultiPolygon>]> for GeometryArrayBuilder<O> {
    fn from(value: &[Option<geo::MultiPolygon>]) -> Self {
        let geo_vec = value
            .iter()
            .map(|mp| mp.clone().map(geo::Geometry::MultiPolygon))
            .collect::<Vec<_>>();
        geo_vec.as_slice().into()
    }
}

impl<O: OffsetSizeTrait> From<&[Option<geo::GeometryCollection>]> for GeometryArrayBuilder<O> {
    fn from(value: &[Option<geo::GeometryCollection>]) -> Self {
        let geo_vec = value
            .iter()
            .map(|gc| gc.clone().map(geo::Geometry::GeometryCollection))
            .collect::<Vec<_>>();
        geo_vec.as_slice().into()
    }
}
