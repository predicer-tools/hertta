use std::fmt;


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