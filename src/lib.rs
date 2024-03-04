pub mod array;
pub(crate) mod buffer;
pub mod function;
mod geo;
pub mod scalar;

pub type DFResult<T> = datafusion::error::Result<T>;
