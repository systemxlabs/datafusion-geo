use crate::DFResult;
use bytes::{Bytes, BytesMut};
use datafusion::error::DataFusionError;
use geozero::wkb::WkbDialect;

#[derive(Debug, Clone, PartialEq)]
pub struct WkbBuffer {
    data: Bytes,
}

pub fn wkb_type_id(dialect: WkbDialect) -> u8 {
    match dialect {
        WkbDialect::Wkb => 1,
        WkbDialect::Ewkb => 2,
        WkbDialect::Geopackage => 3,
        WkbDialect::MySQL => 4,
        WkbDialect::SpatiaLite => 5,
    }
}

pub fn decode_wkb_dialect(type_id: u8) -> DFResult<WkbDialect> {
    if type_id == wkb_type_id(WkbDialect::Wkb) {
        Ok(WkbDialect::Wkb)
    } else if type_id == wkb_type_id(WkbDialect::Ewkb) {
        Ok(WkbDialect::Ewkb)
    } else if type_id == wkb_type_id(WkbDialect::Geopackage) {
        Ok(WkbDialect::Geopackage)
    } else if type_id == wkb_type_id(WkbDialect::MySQL) {
        Ok(WkbDialect::MySQL)
    } else if type_id == wkb_type_id(WkbDialect::SpatiaLite) {
        Ok(WkbDialect::SpatiaLite)
    } else {
        Err(DataFusionError::Internal(format!(
            "Cannot decode WkbDialect from {}",
            type_id
        )))
    }
}

impl WkbBuffer {
    pub fn try_new(wkb_data: Bytes, dialect: WkbDialect) -> DFResult<Self> {
        let type_id = wkb_type_id(dialect);
        let mut bytes = BytesMut::from(vec![type_id].as_slice());
        bytes.extend(wkb_data);
        Ok(Self {
            data: bytes.freeze(),
        })
    }

    pub fn dialect(&self) -> DFResult<WkbDialect> {
        if let Some(b) = self.data.first() {
            decode_wkb_dialect(*b)
        } else {
            Err(DataFusionError::Internal(
                "Cannot get dialect as data is empty".to_string(),
            ))
        }
    }

    pub fn data(&self) -> Bytes {
        self.data.slice(1..)
    }

    pub fn len(&self) -> usize {
        self.data.len() - 1
    }
}
