

mod utilities;
mod input_data;
mod errors;
mod event_loop;
mod arrow_input;
mod julia_process;
mod arrow_test_data;
mod arrow_zmq;

use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, AUTHORIZATION};
use serde_json::json;
use serde_json;
use std::num::NonZeroUsize;
use tokio::task::JoinHandle;
use reqwest::Client;
use std::fs::File;
use std::collections::HashMap;
use std::error::Error;
use std::process::Command;
use std::net::TcpStream;
use std::io::Read;
use errors::FileReadError;

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::io::BufWriter;
use zmq;
use std::thread;
use std::env;
use std::time::Duration;

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    // Get the current working directory
    let current_dir = env::current_dir()?;

    // Print the current working directory
    println!("Current working directory: {:?}", current_dir);

    // Path to the YAML file
    let yaml_file_path = "src/hertta_data.yaml";

    // Read the YAML data into `InputData`
    let input_data = read_yaml_file(yaml_file_path)?;

    // Create and serialize record batches
    let serialized_batches = arrow_input::create_and_serialize_record_batches(&input_data)?;

    // Start the ZMQ server in a separate thread
    let server_handle = arrow_zmq::run_server();
    println!("Server started.");

    // Give the server some time to start
    thread::sleep(Duration::from_secs(2));

    // Start the Julia process
    match arrow_zmq::start_julia_local() {
        Ok(status) => {
            if status.success() {
                println!("Julia process executed successfully.");
            } else {
                eprintln!("Julia process failed with status: {:?}", status);
            }
        },
        Err(e) => {
            eprintln!("Failed to start Julia process: {:?}", e);
        }
    }

    // Join the server thread to ensure it exits cleanly
    server_handle.join().expect("Failed to join server thread");

    Ok(())

    
}

