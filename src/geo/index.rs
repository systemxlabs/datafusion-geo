use crate::geo::{Box2d, GeometryArray};
use crate::DFResult;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use geo::BoundingRect;
use rstar::{RTree, RTreeObject, AABB};

#[derive(Clone, Debug)]
pub struct GeoGeometry(geo::Geometry);

impl RTreeObject for GeoGeometry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let box2d: Box2d = if let Some(rect) = self.0.bounding_rect() {
            rect.into()
        } else {
            Box2d::new()
        };
        AABB::from_corners([box2d.xmin, box2d.ymin], [box2d.xmax, box2d.ymax])
    }
}

pub fn build_rtree_index<O: OffsetSizeTrait>(
    wkb_arr: GenericBinaryArray<O>,
) -> DFResult<RTree<GeoGeometry>> {
    let mut geom_vec = vec![];
    for i in 0..wkb_arr.geom_len() {
        if let Some(geom) = wkb_arr.geo_value(i)? {
            geom_vec.push(GeoGeometry(geom));
        }
    }
    Ok(RTree::bulk_load(geom_vec))
}

#[cfg(test)]
mod tests {
    use crate::geo::index::build_rtree_index;
    use crate::geo::GeometryArrayBuilder;
    use geo::line_string;
    use rstar::AABB;

    #[test]
    fn rtree_index() {
        let ls0 = line_string![
            (x: 0., y: 0.),
            (x: 1., y: 1.)
        ];
        let ls2 = line_string![
            (x: 0., y: 0.),
            (x: -1., y: -1.)
        ];
        let builder: GeometryArrayBuilder<i32> = vec![Some(ls0), None, Some(ls2)].as_slice().into();
        let wkb_arr = builder.build();

        let index = build_rtree_index(wkb_arr).unwrap();

        let elements = index.locate_in_envelope(&AABB::from_corners([0., 0.], [0.5, 0.5]));
        assert_eq!(elements.count(), 0);
        let elements = index.locate_in_envelope(&AABB::from_corners([0., 0.], [1., 1.]));
        assert_eq!(elements.count(), 1);
        let elements = index.locate_in_envelope(&AABB::from_corners([-1., -1.], [1., 1.]));
        assert_eq!(elements.count(), 2);
        let elements = index.locate_in_envelope(&AABB::from_corners([-2., -2.], [2., 2.]));
        assert_eq!(elements.count(), 2);
    }
}
