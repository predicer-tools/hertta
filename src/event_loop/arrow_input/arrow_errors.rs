use arrow::error::ArrowError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataConversionError {
    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),

    #[error("Invalid input data: {0}")]
    InvalidInput(String),

    // You can add more specific errors as needed
    #[error("Unexpected error: {0}")]
    _Unexpected(String),

    #[error("Empty or default input found for crucial parameters")]
    _EmptyOrDefaultInput,
}
