# datafusion-geo
![License](https://img.shields.io/badge/license-MIT-blue.svg)
[![Crates.io](https://img.shields.io/crates/v/datafusion-geo.svg)](https://crates.io/crates/datafusion-geo)

Add geo functionality extension to datafusion query engine.


**Goals**
1. Support multiple wkb dialects
2. Provide DataFusion user defined functions 
3. Prefer using geos library if feature flag enabled

## Useful Links
1. Ewkb format: https://github.com/postgis/postgis/blob/master/doc/ZMSgeoms.txt
2. PostGIS functions: https://postgis.net/docs/manual-dev/PostGIS_Special_Functions_Index.html