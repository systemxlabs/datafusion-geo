use crate::geo::buffer::WkbBuffer;
use crate::geo::scalar::Geometry;
use crate::DFResult;
use arrow::array::{ArrayRef, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use datafusion::common::DataFusionError;
use std::borrow::Cow;
use arrow::datatypes::DataType;

pub struct GeometryArrayV2<O: OffsetSizeTrait> {
    wkb: WkbBuffer,
    geom_offsets: OffsetBuffer<O>,
    nulls: Option<NullBuffer>,
}

impl<O: OffsetSizeTrait> GeometryArrayV2<O> {
    pub fn try_new(
        wkb: WkbBuffer,
        geom_offsets: OffsetBuffer<O>,
        nulls: Option<NullBuffer>,
    ) -> DFResult<Self> {
        let _ = wkb.dialect()?;
        if let Some(nulls_buf) = &nulls {
            if nulls_buf.len() != geom_offsets.len() - 1 {
                return Err(DataFusionError::Internal(
                    "nulls mask length must match the number of values".to_string(),
                ));
            }
        }
        if geom_offsets.last().unwrap().to_usize().unwrap() != wkb.len() {
            return Err(DataFusionError::Internal(
                "largest geometry offset must match coords length".to_string(),
            ));
        }

        Ok(Self {
            wkb,
            geom_offsets,
            nulls,
        })
    }


    pub fn len(&self) -> usize {
        self.geom_offsets.len() - 1
    }

    pub fn is_null(&self, index: usize) -> bool {
        self.nulls
            .as_ref()
            .map(|x| x.is_null(index))
            .unwrap_or(false)
    }

    pub fn get(&self, index: usize) -> Option<Geometry<O>> {
        if index >= self.len() {
            return None;
        }
        if self.is_null(index) {
            return None;
        }
        let geometry = Geometry::try_new(
            Cow::Borrowed(&self.wkb),
            Cow::Borrowed(&self.geom_offsets),
            index,
        )
        .expect("index has been checked");
        Some(geometry)
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = Option<Geometry<O>>> {
        (0..self.len()).map(|i| self.get(i))
    }

    pub fn data_type() -> DataType {
        match O::IS_LARGE {
            true => DataType::LargeBinary,
            false => DataType::Binary,
        }
    }

    pub fn into_arrow_array(self) -> ArrayRef {
        todo!()
    }
}
