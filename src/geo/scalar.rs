use crate::geo::buffer::WkbBuffer;
use crate::DFResult;
use arrow::array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use datafusion::common::DataFusionError;
use geozero::wkb::FromWkb;
use std::borrow::Cow;

pub struct Geometry<'a, O: OffsetSizeTrait> {
    pub(crate) wkb: Cow<'a, WkbBuffer>,
    pub(crate) geom_offsets: Cow<'a, OffsetBuffer<O>>,
    pub(crate) geom_index: usize,
    start_offset: usize,
    end_offset: usize,
}

impl<'a, O: OffsetSizeTrait> Geometry<'a, O> {
    pub fn try_new(
        wkb: Cow<'a, WkbBuffer>,
        geom_offsets: Cow<'a, OffsetBuffer<O>>,
        geom_index: usize,
    ) -> DFResult<Self> {
        let Some(Some(start_offset)) = geom_offsets.get(geom_index).map(|f| f.to_usize()) else {
            return Err(DataFusionError::Internal(
                "Cannot get start_offset".to_string(),
            ));
        };
        let Some(Some(end_offset)) = geom_offsets.get(geom_index + 1).map(|f| f.to_usize()) else {
            return Err(DataFusionError::Internal(
                "Cannot get end_offset".to_string(),
            ));
        };

        Ok(Self {
            wkb,
            geom_offsets,
            geom_index,
            start_offset,
            end_offset,
        })
    }

    pub fn to_geo(&self) -> DFResult<geo::Geometry> {
        let dialect = self.wkb.dialect()?;
        let sliced_bytes = self.wkb.data().slice(self.start_offset..self.end_offset);
        let mut rdr = std::io::Cursor::new(sliced_bytes.as_ref());
        geo::Geometry::from_wkb(&mut rdr, dialect)
            .map_err(|e| DataFusionError::Internal(format!("Failed to parse wkb, e: {}", e)))
    }

    #[cfg(feature = "geos")]
    pub fn to_geos(&self) -> DFResult<geos::Geometry> {
        let dialect = self.wkb.dialect()?;
        let sliced_bytes = self.wkb.data().slice(self.start_offset..self.end_offset);
        let mut rdr = std::io::Cursor::new(sliced_bytes.as_ref());
        geos::Geometry::from_wkb(&mut rdr, dialect)
            .map_err(|e| DataFusionError::Internal(format!("Failed to parse wkb, e: {}", e)))
    }
}
