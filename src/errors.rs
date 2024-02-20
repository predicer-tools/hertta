use std::fmt;
use warp::reject::Reject;
use std::error::Error;

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
    pub fn new(msg: &str) -> TaskError {
        TaskError { message: msg.to_string() }
    }
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for TaskError {}