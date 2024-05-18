

mod utilities;
mod input_data;
mod errors;
mod event_loop;
mod arrow_input;
mod julia_process;
mod arrow_test_data;

use julia_process::JuliaProcess;
use std::env;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, AUTHORIZATION};
use serde_json::json;
//use tokio::time::{self, Duration, Instant};
use warp::Filter;
use serde::{Deserialize, Serialize};
use serde_json;
use tokio::sync::mpsc;
use std::num::NonZeroUsize;
use jlrs::prelude::*;
use jlrs::error::JlrsError;
use tokio::task::JoinHandle;
//use std::fmt;
use reqwest::Client;
use tokio::sync::Mutex;
use std::sync::Arc;
use std::fs;
use std::net::SocketAddr;
use std::fs::File;
use std::io::Write;
use std::collections::HashMap;
use base64::{encode, decode};
use std::error::Error;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;
use std::net::TcpStream;
use bincode;
use std::path::PathBuf;
use std::io::Read;
use errors::FileReadError;

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::io::BufWriter;
use arrow::datatypes::{Field, Schema, DataType};
use arrow::array::{StringArray, ArrayRef};

//use std::time::{SystemTime, UNIX_EPOCH};
//use tokio::join;
//use warp::reject::Reject;
//use chrono::{Utc, Duration as ChronoDuration, TimeZone, Timelike};

/// This function is used to make post request to Home Assistant in the Hertta development phase
/// 
/// Sends a POST request to control light brightness.
///
/// This asynchronous function sends a POST request to a given URL to control the brightness of a specified light entity.
/// It includes necessary headers for content type and authorization and sends a JSON payload containing the entity ID and desired brightness level.
///
/// # Parameters
/// - `url`: The URL of the light control service.
/// - `entity_id`: The identifier of the light entity to be controlled.
/// - `token`: Authentication token required for the POST request.
/// - `brightness`: Desired brightness level.
///
/// # Returns
/// Returns `Ok(())` if the POST request is successfully sent and processed.
/// Returns `PostRequestError` if any error occurs during the request construction, sending, or processing.
///
/// # Errors
/// - Errors in header construction or during the sending of the request are converted to `PostRequestError`.
/// - Errors in the response status (e.g., non-successful HTTP status codes) are also converted to `PostRequestError`.
///
async fn _make_post_request(url: &str, entity_id: &str, token: &str, brightness: f64) -> Result<(), errors::PostRequestError> {
    
    // Check if the token is valid for HTTP headers
    if !utilities::_is_valid_http_header_value(token) {
        return Err(errors::PostRequestError("Invalid token header".to_string()));
    }
    
    // Construct the request headers, including content type and authorization.
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    
    // Handle the creation of the token header safely, converting any error into a `PostRequestError`.
    let token_header = HeaderValue::from_str(&format!("{}", token))
        .map_err(|e| errors::PostRequestError(format!("Invalid token header: {}", e)))?;
    headers.insert(AUTHORIZATION, token_header);

    // Construct the JSON payload with the light entity's ID and the desired brightness level.
    let payload = json!({
        "entity_id": entity_id,
        "brightness": brightness,
    });
    
    // Send the POST request using the constructed headers and payload.
    let client = Client::new();
    let response = client
        .post(url)
        .headers(headers)
        .json(&payload)
        .send()
        .await
        .map_err(errors::PostRequestError::from)?;

    // Check the response status, converting any error (e.g., non-2XX status) into a `PostRequestError`.
    if let Err(err) = response.error_for_status() {
        eprintln!("Error making POST request: {:?}", err);
        return Err(errors::PostRequestError::from(err));
    }

    Ok(())
}

/// Initializes the Julia runtime with an asynchronous interface.
///
/// This function sets up a new Julia runtime environment using the Tokio async runtime.
/// It configures the runtime with a specified channel capacity and starts it asynchronously.
/// 
/// # Errors
/// Returns `JuliaError` if the channel capacity is set to an invalid value or if the
/// runtime fails to start.
///
/// # Safety
/// This function contains unsafe code that assumes correct usage of the Julia runtime API.
/// Channel capacity not tested yet.
///
fn init_julia_runtime() -> Result<(AsyncJulia<Tokio>, JoinHandle<Result<(), Box<JlrsError>>>), errors::JuliaError> {
    unsafe {
        // Create a new runtime builder for Julia, using the Tokio async runtime.
        RuntimeBuilder::new()
            .async_runtime::<Tokio>()
             // Set the channel capacity for the runtime. An error is returned if the capacity is invalid.
            .channel_capacity(NonZeroUsize::new(4).ok_or(errors::JuliaError("Invalid channel capacity".to_string()))?)
            // Attempt to start the runtime asynchronously. Errors during startup are captured and returned.
            .start_async::<1>()
            .map_err(|e| errors::JuliaError(format!("Could not init Julia: {:?}", e)))
    }
}

/// Shuts down the Julia runtime and waits for the completion of its associated tasks.
///
/// This function drops the Julia runtime object to initiate its shutdown and then waits for
/// the completion of tasks associated with it, handling any errors that may occur.
///
/// # Parameters
/// - `julia`: The Julia runtime object to be shut down.
/// - `handle`: The join handle associated with a potentially running task in the runtime.
///
/// # Returns
/// Returns `Ok(())` if the runtime shuts down successfully and all tasks complete without error.
/// Returns `JuliaError` if any error occurs during task completion or runtime shutdown.
///
/// # Errors
/// - Returns `JuliaError` if the task running in the Julia runtime exits with an error or if the
///   thread associated with the runtime panics or encounters a similar issue.
///
async fn _shutdown_julia_runtime(julia: AsyncJulia<Tokio>, handle: JoinHandle<Result<(), Box<JlrsError>>>) -> Result<(), errors::JuliaError> {
    // Dropping `julia` to shut down the runtime
    std::mem::drop(julia);

    // Await the handle and handle any errors
    match handle.await {
        Ok(Ok(())) => Ok(()), // Both thread execution and task were successful
        Ok(Err(e)) => Err(errors::JuliaError(format!("Julia task exited with an error: {:?}", e))), 
        Err(e) => Err(errors::JuliaError(format!("Join handle failed: {:?}", e))),
    }
}



pub fn start_hass_backend_server() {

    Command::new("python")
    .arg("hass_backend/server.py") // Replace with the actual path to your Python file
    .spawn() // Spawns the command as a new process, not blocking the current thread
    .expect("HASS backend server failed to start");

}

pub fn start_weather_forecast_server() {

    // Start Python server 2
    Command::new("python")
    .arg("forecasts/weather_forecast.py") // Replace with the actual path to your Python file
    .spawn() // Spawns the command as a new process, not blocking the current thread
    .expect("Weather forecast failed to start");

}

pub fn start_julia_server_and_send_data(julia_script_path: &str, server_address: &str, data: HashMap<String, Vec<u8>>) -> Result<(), Box<dyn Error>> {
    let mut child = Command::new("julia")
        .arg(julia_script_path)
        .arg("--server")
        .arg(format!("--address={}", server_address))
        .spawn()?;

    // Attempt to connect to the server with retries
    let mut attempts = 0;
    let max_attempts = 10;
    let delay = Duration::from_secs(1);
    let mut connected = false;

    while attempts < max_attempts && !connected {
        match TcpStream::connect(server_address) {
            Ok(_) => {
                println!("Successfully connected to the server.");
                connected = true;
            },
            Err(e) => {
                println!("Attempt {} failed: {}", attempts + 1, e);
                thread::sleep(delay);
            }
        }
        attempts += 1;
    }

    if !connected {
        return Err("Failed to connect to the server within the maximum number of attempts.".into());
    }

    // If connected, proceed to serialize, encode, and send data
    let serialized_data = bincode::serialize(&data)?;
    let encoded_data = encode(&serialized_data);
    let full_message = format!("data:{}", encoded_data);
    let mut stream = TcpStream::connect(server_address)?;
    stream.write_all(encoded_data.as_bytes())?;

    // Ensure the child process is not abruptly terminated, affecting cleanup
    // You might want to add logic to gracefully shut down the server if needed
    // child.kill()?;

    Ok(())
}

pub fn read_yaml_file(file_path: &str) -> Result<input_data::InputData, FileReadError> {
    // Attempt to open the file and handle errors specifically related to file opening
    let mut file = File::open(file_path).map_err(FileReadError::open_error)?;

    // Read the contents of the file into a string, handling errors related to reading
    let mut contents = String::new();
    file.read_to_string(&mut contents).map_err(FileReadError::read_error)?;

    // Attempt to parse the YAML content into the expected data structure
    let input_data: input_data::InputData = serde_yaml::from_str(&contents)
        .map_err(FileReadError::Parse)?;

    // If all steps are successful, return the data
    Ok(input_data)
}

pub fn send_data_to_julia(server_address: &str, batches: &HashMap<String, RecordBatch>) -> Result<(), Box<dyn Error>> {
    let stream = TcpStream::connect(server_address)?;
    let writer = BufWriter::new(stream);

    // Assume that "setup" batch is what we want to send. Validate its existence.
    if let Some(batch) = batches.get("setup") {
        let mut arrow_writer = StreamWriter::try_new(writer, &batch.schema())?;
        arrow_writer.write(batch)?;
        arrow_writer.finish()?;
        println!("Data sent to Julia server.");
    } else {
        return Err("Batch 'setup' not found in the batches.".into());
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    

    // Path to the YAML file
    let yaml_file_path = "src/hertta_data.yaml";

    // Read the YAML data into `InputData`
    let input_data = read_yaml_file(yaml_file_path)?;

    // Create the record batches
    let batches = arrow_input::create_record_batches(&input_data)?;

    // Print the record batches
    arrow_input::print_record_batches(&batches)?;

    Ok(())
    
}




#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_init_julia_runtime_success() {
        let result = init_julia_runtime();
        assert!(result.is_ok());
    }

    /* 

    use mockito::{mock, server_url};
    use tokio;

    #[tokio::test]
    async fn test_make_post_request_light_success() {
        // Set up a mock server to simulate the Home Assistant endpoint
        let mock_server = mock("POST", "/some_endpoint")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body("{\"success\": true}")
            .create();

        // The mock server URL acts as a stand-in for the real Home Assistant URL
        let test_url = server_url() + "/some_endpoint";

        // Call the function with the mock server's URL
        let result = _make_post_request_light(&test_url, "entity1", "token123", 50.0).await;

        // Assert that the function returns Ok(())
        assert!(result.is_ok());

        // Confirm that the mock was called as expected
        mock_server.assert();
    }

    #[tokio::test]
    async fn test_invalid_token_header() {
    let result = _make_post_request_light("http://example.com", "entity1", "\u{7FFF}", 50.0).await;
    assert!(result.is_err()); // Ensure that an error is returned
    }

    #[tokio::test]
    async fn test_post_request_light_http_error() {
        let _mock = mock("POST", "/")
            .with_status(500) // Simulate an internal server error
            .create();

        let test_url = mockito::server_url();
        let result = _make_post_request_light(&test_url, "entity1", "token123", 50.0).await;
        assert!(result.is_err()); // The function should return an error
    }

    */


}

