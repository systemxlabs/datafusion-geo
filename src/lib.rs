pub mod array;
pub(crate) mod buffer;
pub mod function;
pub mod scalar;

pub type DFResult<T> = datafusion::error::Result<T>;
