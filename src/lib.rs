mod algorithm;
mod array;
pub(crate) mod buffer;
pub(crate) mod scalar;

pub type DFResult<T> = datafusion::error::Result<T>;
