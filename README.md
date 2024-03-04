# datafusion-geo 
Add geo functionality extension to datafusion query engine.

A lot of code from [geoarrow-rs](https://github.com/geoarrow/geoarrow-rs) but
1. Simplified many abstractions and removed a lot of irrelevant code
2. Mainly for PostGIS users
3. Provide DataFusion user defined functions 

## Reference
1. EWKB: https://github.com/postgis/postgis/blob/master/doc/ZMSgeoms.txt