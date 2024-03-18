use std::fmt::{self};
use warp::reject::Reject;
use std::error::Error;
use arrow::error::ArrowError;
use serde::{Serialize, Deserialize};

/// A custom error type for representing errors from Julia.
///
/// This struct is used to encapsulate errors that occur within the Julia environment when
/// interfacing with Rust. 
///
#[derive(Debug)]
pub struct JuliaError(pub String);

impl fmt::Display for JuliaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Julia Error: {}", self.0)
    }
}

impl std::error::Error for JuliaError {}
impl Reject for JuliaError {}

/// Represents errors that occur during the control and execution of various processes.
///
/// This struct encapsulates errors specific to process control and execution, providing a 
/// consistent way to handle such errors in Rust code. It is useful for signaling issues that 
/// arise during operations like controlling the devices etc.
///

#[derive(Debug)]
pub struct ProcessControlError(pub String);

impl fmt::Display for ProcessControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Custom format for displaying the error
        write!(f, "Process Control Error: {}", self.0)
    }
}

impl std::error::Error for ProcessControlError {}

#[derive(Debug)]
pub enum ModelDataError {
    Network(reqwest::Error),
    Parsing(serde_yaml::Error),
}

impl fmt::Display for ModelDataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            ModelDataError::Network(ref err) => write!(f, "Network Error: {}", err),
            ModelDataError::Parsing(ref err) => write!(f, "Parsing Error: {}", err),
        }
    }
}

impl Error for ModelDataError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {
            ModelDataError::Network(ref err) => Some(err),
            ModelDataError::Parsing(ref err) => Some(err),
        }
    }
}

impl From<reqwest::Error> for ModelDataError {
    fn from(err: reqwest::Error) -> ModelDataError {
        ModelDataError::Network(err)
    }
}

impl From<serde_yaml::Error> for ModelDataError {
    fn from(err: serde_yaml::Error) -> ModelDataError {
        ModelDataError::Parsing(err)
    }
}

/// Represents errors that can occur during a POST request.
///
/// This struct is used to handle and encapsulate errors that might arise while making POST
/// requests, such as network issues, invalid responses, or other HTTP-related errors.
/// It is tailored to provide detailed error information specific to POST requests in a
/// web communication or API context.
///
/// # Arguments
///
/// - `f`: A mutable reference to a `fmt::Formatter`, used for writing the formatted string.
///
/// # Returns
///
/// Returns a `fmt::Result` which is `Ok` on successful formatting, or contains an error otherwise.
///
#[derive(Debug)]
pub struct PostRequestError(pub String);

impl fmt::Display for PostRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Custom format for displaying the error
        write!(f, "POST Request Error: {}", self.0)
    }
}

impl std::error::Error for PostRequestError {}

// Optional: Implementing `From` for reqwest's error type to enable easy conversion.
impl From<reqwest::Error> for PostRequestError {
    fn from(err: reqwest::Error) -> Self {
        PostRequestError(format!("HTTP request error: {}", err))
    }
}

#[derive(Debug)]
pub struct TaskError {
    message: String,
}

impl TaskError {
    pub fn _new(msg: &str) -> TaskError {
        TaskError { message: msg.to_string() }
    }
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for TaskError {}

/// Custom error for handling errors specific to parsing weather data.
#[derive(Debug)]
pub struct WeatherDataError {
    details: String,
}

impl fmt::Display for WeatherDataError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

#[derive(Debug)]
pub struct TimeDataParseError(String);

impl TimeDataParseError {
    pub fn new(msg: &str) -> Self {
        TimeDataParseError(msg.to_owned())
    }
}

impl std::fmt::Display for TimeDataParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for TimeDataParseError {}
unsafe impl Send for TimeDataParseError {}

#[derive(Debug, Serialize, Deserialize)]
pub enum CustomError {
    IoError(String),
    OtherError(String),
    ArrowError(String),
    // Add other error variants as needed
}

impl fmt::Display for CustomError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CustomError::IoError(ref cause) => write!(f, "IO Error: {}", cause),
            CustomError::OtherError(ref cause) => write!(f, "Other Error: {}", cause),
            CustomError::ArrowError(ref cause) => write!(f, "Arrow Error: {}", cause),
            // Handle other variants as needed
        }
    }
}

impl Error for CustomError {}

impl From<std::io::Error> for CustomError {
    fn from(error: std::io::Error) -> Self {
        CustomError::IoError(error.to_string())
    }
}

impl From<ArrowError> for CustomError {
    fn from(error: ArrowError) -> Self {
        CustomError::ArrowError(error.to_string())
    }
}
