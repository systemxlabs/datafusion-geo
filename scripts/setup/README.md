# Build support

```
cargo install pixi
pixi install
```

```bash
# export GDAL_HOME="$(pwd)/.pixi/envs/default"
export LD_LIBRARY_PATH="$(pwd)/.pixi/envs/default/lib:$LD_LIBRARY_PATH"
export GEOS_LIB_DIR="$(pwd)/.pixi/envs/default/lib:$GEOS_LIB_DIR"
export GEOS_VERSION=3.12.1
export PKG_CONFIG_PATH="$(pwd)/.pixi/envs/default/lib/pkgconfig:$PKG_CONFIG_PATH"
```