use std::fmt;

#[derive(Debug)]
pub struct JuliaError(pub String);

impl fmt::Display for JuliaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Julia Error: {}", self.0)
    }
}

impl std::error::Error for JuliaError {}

/// Represents errors that occur during the control and execution of various processes.
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