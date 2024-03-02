use crate::DFResult;
use arrow::array::OffsetSizeTrait;
use arrow::buffer::OffsetBuffer;
use datafusion::common::DataFusionError;
use std::borrow::Cow;

pub(crate) fn compute_start_end_offset<O: OffsetSizeTrait>(
    geom_offsets: Cow<OffsetBuffer<O>>,
    geom_index: usize,
) -> DFResult<(usize, usize)> {
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
    Ok((start_offset, end_offset))
}
