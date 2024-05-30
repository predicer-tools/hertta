

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
use jlrs::prelude::*;
use jlrs::error::JlrsError;
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
fn _init_julia_runtime() -> Result<(AsyncJulia<Tokio>, JoinHandle<Result<(), Box<JlrsError>>>), errors::JuliaError> {
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

    // Spawn a separate thread for ZeroMQ
    let thread_join_handle = thread::Builder::new().name("Predicer data server".to_string()).spawn(move || {
        let zmq_context = zmq::Context::new();
        let responder = zmq_context.socket(zmq::REP).unwrap();
        assert!(responder.bind("tcp://*:5555").is_ok());
        let mut is_running = true;
        let receive_flags = 0;
        let send_flags = 0;
        while is_running {
            let request_result = responder.recv_string(receive_flags);
            match request_result {
                Ok(inner_result) => {
                    match inner_result {
                        Ok(command) => {
                            if let Some(buffer) = serialized_batches.get(&command) {
                                println!("Received request for {}", command);
                                responder.send(buffer, send_flags).unwrap();
                            } else if command == "Quit" {
                                println!("Received request to quit");
                                is_running = false;
                            } else {
                                println!("Received unknown command {}", command);
                            }
                        }
                        Err(_) => println!("Received absolute gibberish"),
                    }
                }
                Err(_) => println!("Failed to receive data")
            }
        }
    }).expect("failed to start server thread");

    // Path to the Julia script
    let julia_script_path = current_dir.join("Predicer/src/arrow_conversion.jl");

    // Run the Julia script
    let mut julia_command = Command::new("julia");
    julia_command.arg(julia_script_path.to_str().unwrap());
    julia_command.status().expect("Julia process failed to execute");

    // Wait for the ZeroMQ thread to complete
    thread_join_handle.join().expect("failed to join with server thread");

    Ok(())
    
}

