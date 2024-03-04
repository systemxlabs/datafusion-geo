use crate::DFResult;
use datafusion::common::DataFusionError;
use geozero::wkb::WkbDialect;

pub(crate) fn wkb_type_id(dialect: WkbDialect) -> u8 {
    match dialect {
        WkbDialect::Wkb => 1,
        WkbDialect::Ewkb => 2,
        WkbDialect::Geopackage => 3,
        WkbDialect::MySQL => 4,
        WkbDialect::SpatiaLite => 5,
    }
}

pub(crate) fn decode_wkb_dialect(type_id: u8) -> DFResult<WkbDialect> {
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
