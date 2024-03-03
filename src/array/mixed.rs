use crate::array::linestring::LineStringArray;
use crate::array::multipoint::MultiPointArray;
use crate::array::point::PointArray;
use crate::array::polygon::PolygonArray;
use crate::array::util::check_nulls;
use crate::array::{
    GeometryArrayAccessor, GeometryArrayTrait, MultiLineStringArray, MultiPolygonArray,
};
use crate::scalar::GeometryScalar;
use crate::DFResult;
use arrow::array::{ArrayRef, OffsetSizeTrait};
use arrow::buffer::{NullBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
use datafusion::error::DataFusionError;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct MixedGeometryArray<O: OffsetSizeTrait> {
    pub(crate) type_ids: ScalarBuffer<i8>,
    pub(crate) offsets: ScalarBuffer<i32>,
    pub(crate) nulls: Option<NullBuffer>,

    pub(crate) points: Option<PointArray>,
    pub(crate) line_strings: Option<LineStringArray<O>>,
    pub(crate) polygons: Option<PolygonArray<O>>,
    pub(crate) multi_points: Option<MultiPointArray<O>>,
    pub(crate) multi_line_strings: Option<MultiLineStringArray<O>>,
    pub(crate) multi_polygons: Option<MultiPolygonArray<O>>,
}

impl<O: OffsetSizeTrait> MixedGeometryArray<O> {
    pub fn try_new(
        type_ids: ScalarBuffer<i8>,
        offsets: ScalarBuffer<i32>,
        nulls: Option<NullBuffer>,
        points: Option<PointArray>,
        line_strings: Option<LineStringArray<O>>,
        polygons: Option<PolygonArray<O>>,
        multi_points: Option<MultiPointArray<O>>,
        multi_line_strings: Option<MultiLineStringArray<O>>,
        multi_polygons: Option<MultiPolygonArray<O>>,
    ) -> DFResult<Self> {
        if type_ids.len() != offsets.len() {
            return Err(DataFusionError::Internal(
                "type_ids buffer and offsets buffer must have same length".to_string(),
            ));
        }
        check_nulls(&nulls, type_ids.len())?;
        Ok(Self {
            type_ids,
            offsets,
            nulls,
            points,
            line_strings,
            polygons,
            multi_points,
            multi_line_strings,
            multi_polygons,
        })
    }
}

impl<O: OffsetSizeTrait> GeometryArrayTrait for MixedGeometryArray<O> {
    fn geo_type_id() -> i8 {
        8
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn len(&self) -> usize {
        self.type_ids.len()
    }

    fn extension_name() -> &'static str {
        "geoarrow.geometry"
    }

    fn data_type() -> DataType {
        let fields: Vec<Field> = vec![
            Field::new("geometry", PointArray::data_type(), true).with_metadata(HashMap::from([(
                "ARROW:extension:name".to_string(),
                PointArray::extension_name().to_string(),
            )])),
            Field::new("geometry", LineStringArray::<O>::data_type(), true).with_metadata(
                HashMap::from([(
                    "ARROW:extension:name".to_string(),
                    LineStringArray::<O>::extension_name().to_string(),
                )]),
            ),
            Field::new("geometry", PolygonArray::<O>::data_type(), true).with_metadata(
                HashMap::from([(
                    "ARROW:extension:name".to_string(),
                    PolygonArray::<O>::extension_name().to_string(),
                )]),
            ),
            Field::new("geometry", MultiPointArray::<O>::data_type(), true).with_metadata(
                HashMap::from([(
                    "ARROW:extension:name".to_string(),
                    MultiPointArray::<O>::extension_name().to_string(),
                )]),
            ),
            Field::new("geometry", MultiLineStringArray::<O>::data_type(), true).with_metadata(
                HashMap::from([(
                    "ARROW:extension:name".to_string(),
                    MultiLineStringArray::<O>::extension_name().to_string(),
                )]),
            ),
            Field::new("geometry", MultiPolygonArray::<O>::data_type(), true).with_metadata(
                HashMap::from([(
                    "ARROW:extension:name".to_string(),
                    MultiPolygonArray::<O>::extension_name().to_string(),
                )]),
            ),
        ];
        let type_ids = vec![
            PointArray::geo_type_id(),
            LineStringArray::<O>::geo_type_id(),
            PolygonArray::<O>::geo_type_id(),
            MultiPointArray::<O>::geo_type_id(),
            MultiLineStringArray::<O>::geo_type_id(),
            MultiPolygonArray::<O>::geo_type_id(),
        ];

        let union_fields = UnionFields::new(type_ids, fields);
        DataType::Union(union_fields, UnionMode::Dense)
    }

    fn into_arrow_array(self) -> ArrayRef {
        todo!()
    }
}

impl<'a, O: OffsetSizeTrait> GeometryArrayAccessor<'a> for MixedGeometryArray<O> {
    fn value(&'a self, index: usize) -> DFResult<Option<GeometryScalar<'a, O>>> {
        let Some(geo_type_id) = self.type_ids.get(index) else {
            return Ok(None);
        };
        let offset = self.offsets[index] as usize;

        if *geo_type_id == PointArray::geo_type_id() {
            if let Some(point_arr) = &self.points {
                let point = point_arr.value(offset)?;
                Ok(point.map(|v| GeometryScalar::Point(v)))
            } else {
                Err(DataFusionError::Internal("point array is none".to_string()))
            }
        } else if *geo_type_id == LineStringArray::<O>::geo_type_id() {
            if let Some(linestring_arr) = &self.line_strings {
                let line_string = linestring_arr.value(offset)?;
                Ok(line_string.map(|v| GeometryScalar::LineString(v)))
            } else {
                Err(DataFusionError::Internal(
                    "linestring array is none".to_string(),
                ))
            }
        } else if *geo_type_id == PolygonArray::<O>::geo_type_id() {
            if let Some(polygon_arr) = &self.polygons {
                let polygon = polygon_arr.value(offset)?;
                Ok(polygon.map(|v| GeometryScalar::Polygon(v)))
            } else {
                Err(DataFusionError::Internal(
                    "polygon array is none".to_string(),
                ))
            }
        } else if *geo_type_id == MultiPointArray::<O>::geo_type_id() {
            if let Some(multipoint_arr) = &self.multi_points {
                let multi_point = multipoint_arr.value(offset)?;
                Ok(multi_point.map(|v| GeometryScalar::MultiPoint(v)))
            } else {
                Err(DataFusionError::Internal(
                    "multipoint array is none".to_string(),
                ))
            }
        } else if *geo_type_id == MultiLineStringArray::<O>::geo_type_id() {
            if let Some(multilinestring_arr) = &self.multi_line_strings {
                let multi_linestring = multilinestring_arr.value(offset)?;
                Ok(multi_linestring.map(|v| GeometryScalar::MultiLineString(v)))
            } else {
                Err(DataFusionError::Internal(
                    "multilinestring array is none".to_string(),
                ))
            }
        } else if *geo_type_id == MultiPolygonArray::<O>::geo_type_id() {
            if let Some(multipolygon_arr) = &self.multi_polygons {
                let multi_polygon = multipolygon_arr.value(offset)?;
                Ok(multi_polygon.map(|v| GeometryScalar::MultiPolygon(v)))
            } else {
                Err(DataFusionError::Internal(
                    "multipolygon array is none".to_string(),
                ))
            }
        } else {
            Err(DataFusionError::Internal(
                "Invalid geometry type id".to_string(),
            ))
        }
    }
}
