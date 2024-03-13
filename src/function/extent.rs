use crate::geo::{Box2d, GeometryArray};
use crate::DFResult;
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, GenericBinaryArray, OffsetSizeTrait};
use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use geo::BoundingRect;
use std::any::Any;

// TODO add aliases after datafusion 37.0 released
#[derive(Debug)]
pub struct ExtentUdaf {
    signature: Signature,
}

impl ExtentUdaf {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Binary, DataType::LargeBinary],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for ExtentUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        // uadf not support alias
        "st_extent"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(Box2d::data_type())
    }

    fn accumulator(&self, _arg: &DataType) -> datafusion_common::Result<Box<dyn Accumulator>> {
        Ok(Box::new(ExtentAccumulator::new()))
    }

    fn state_type(&self, _return_type: &DataType) -> datafusion_common::Result<Vec<DataType>> {
        Ok(vec![Box2d::data_type()])
    }
}

impl Default for ExtentUdaf {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct ExtentAccumulator {
    box2d: Box2d,
}

impl ExtentAccumulator {
    pub fn new() -> Self {
        Self {
            box2d: Box2d::new(),
        }
    }
}

impl Accumulator for ExtentAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = &values[0];
        match arr.data_type() {
            DataType::Binary => {
                let wkb_arr = arr.as_binary::<i32>();
                let box2d = compute_extent::<i32>(wkb_arr)?;
                self.box2d = compute_bounding_box2d(self.box2d.clone(), box2d);
            }
            DataType::LargeBinary => {
                let wkb_arr = arr.as_binary::<i64>();
                let box2d = compute_extent::<i64>(wkb_arr)?;
                self.box2d = compute_bounding_box2d(self.box2d.clone(), box2d);
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        Ok(self.box2d.clone().into())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        Ok(vec![self.box2d.clone().into()])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        let arr = &states[0];
        (0..arr.len()).try_for_each(|index| {
            let v = states
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            if let ScalarValue::Struct(arr) = &v[0] {
                if let Some(box2d) = Box2d::value(arr, 0)? {
                    self.box2d = compute_bounding_box2d(self.box2d.clone(), box2d);
                }
            } else {
                unreachable!("")
            }
            Ok(())
        })
    }
}

fn compute_extent<O: OffsetSizeTrait>(arr: &GenericBinaryArray<O>) -> DFResult<Box2d> {
    let mut box2d = Box2d::new();
    for i in 0..arr.geom_len() {
        if let Some(value) = arr
            .geo_value(i)?
            .and_then(|geom| geom.bounding_rect().map(Box2d::from))
        {
            box2d = compute_bounding_box2d(box2d, value);
        }
    }
    Ok(box2d)
}

fn compute_bounding_box2d(b0: Box2d, b1: Box2d) -> Box2d {
    let xmin = b0.xmin.min(b1.xmin);
    let ymin = b0.ymin.min(b1.ymin);
    let xmax = b0.xmax.max(b1.xmax);
    let ymax = b0.ymax.max(b1.ymax);
    Box2d {
        xmin,
        ymin,
        xmax,
        ymax,
    }
}

#[cfg(test)]
mod tests {
    use crate::function::extent::ExtentUdaf;
    use crate::geo::GeometryArrayBuilder;
    use arrow::util::pretty::pretty_format_batches;
    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::AggregateUDF;
    use geo::line_string;
    use std::sync::Arc;

    #[tokio::test]
    async fn extent() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("geom", DataType::Binary, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        let mut linestrint_vec = vec![];
        for i in 0..4 {
            let i = i as f64;
            let linestring = line_string![
                (x: i, y: i + 1.0),
                (x: i + 2.0, y: i + 3.0),
                (x: i + 4.0, y: i + 5.0),
            ];
            linestrint_vec.push(Some(linestring));
        }
        let builder: GeometryArrayBuilder<i32> = linestrint_vec.as_slice().into();

        let record = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(builder.build()),
                Arc::new(StringArray::from(vec!["a", "a", "b", "b"])),
            ],
        )
        .unwrap();

        let mem_table = MemTable::try_new(schema.clone(), vec![vec![record]]).unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("geom_table", Arc::new(mem_table))
            .unwrap();
        ctx.register_udaf(AggregateUDF::from(ExtentUdaf::new()));
        let df = ctx
            .sql("select ST_Extent(geom), name from geom_table group by name order by name")
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&df.collect().await.unwrap())
                .unwrap()
                .to_string(),
            "+----------------------------------------------+------+
| st_extent(geom_table.geom)                   | name |
+----------------------------------------------+------+
| {xmin: 0.0, ymin: 1.0, xmax: 5.0, ymax: 6.0} | a    |
| {xmin: 2.0, ymin: 3.0, xmax: 7.0, ymax: 8.0} | b    |
+----------------------------------------------+------+"
        );
    }
}
