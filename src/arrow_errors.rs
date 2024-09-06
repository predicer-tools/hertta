
use arrow::error::ArrowError;
use thiserror::Error;
use std::io;

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

#[derive(Error, Debug)]
pub enum FileReadError {
    #[error("File operation error during {operation}: {source}")]
    IoError {
        operation: String,
        #[source]
        source: io::Error,
    },

    #[error("error parsing the file: {0}")]
    Parse(#[from] serde_yaml::Error),
}

impl FileReadError {
    pub fn _open_error(err: io::Error) -> Self {
        FileReadError::IoError {
            operation: "open".to_string(),
            source: err,
        }
    }

    pub fn _read_error(err: io::Error) -> Self {
        FileReadError::IoError {
            operation: "read".to_string(),
            source: err,
        }
    }
}