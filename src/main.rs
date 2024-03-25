

mod utilities;
mod input_data;
mod errors;
mod event_loop;
mod arrow_input;
mod julia_process;
mod arrow_test_data;

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
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::collections::HashMap;
use std::process::Command;
use base64::{encode, decode};
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {



    // Initialize the JuliaProcess
    let mut julia_process = julia_process::JuliaProcess::new("Predicer/src/arrow_test.jl")?;

    // Use the function to create base64 encoded Arrow data
    //let encoded_arrow_data = arrow_input::create_and_encode_inputdatasetup()?;
    //let encoded_arrow_data = arrow_input::create_and_encode_nodes()?;
    //let encoded_arrow_data = arrow_input::create_and_encode_process_topologys()?;
    //let encoded_arrow_data = arrow_input::create_and_encode_processes()?;
    //let encoded_arrow_data = arrow_input::create_and_encode_groups()?;
    //let encoded_arrow_data = arrow_input::create_and_encode_markets()?;
    //let encoded_arrow_data = arrow_input::create_and_encode_timeseries()?;
    let encoded_arrow_data = arrow_input::create_and_encode_node_inflows()?;

    // Send the encoded data to the Julia process
    julia_process.send_data(format!("data:{}\n", encoded_arrow_data).into_bytes())?;

    // Properly terminate the Julia process
    julia_process.terminate()?;

    Ok(())

    /* 
    
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let predicer_dir = args
        .get(1)
        .expect("First argument should be path to Predicer")
        .to_string();

    start_hass_backend_server();
    start_weather_forecasst_server();

    let options_path = "./src/options.json";
    //let options_path = "/data/options.json";

    let options_str = match fs::read_to_string(options_path) {
        Ok(content) => content,
        Err(err) => {
            eprintln!("Error reading options.json: {}", err);
            return;
        }
    };

    // Parse the options JSON string into an Options struct
    let options: Options = match serde_json::from_str(&options_str) {
        Ok(parsed_options) => parsed_options,
        Err(err) => {
            eprintln!("Error parsing options.json: {}", err);
            return;
        }
    };

    // Extract option data from the options.json file.
    let listen_ip = options.listen_ip.clone();
    let port = options.port.clone();

    let ip_port = format!("{}:{}", listen_ip, port);

    // Parse the combined string into a SocketAddr
    let ip_address: SocketAddr = ip_port.parse().unwrap();

    // Initialize the Julia runtime
    let (julia, handle) = match init_julia_runtime() {
        Ok((julia, handle)) => (julia, handle),
        Err(e) => {
            eprintln!("Failed to initialize Julia runtime: {:?}", e);
            return; // Exit the program if runtime couldn't start
        }
    };
    let julia = Arc::new(Mutex::new(julia));

    // Set up an mpsc channel for graceful shutdown
    let (shutdown_sender, mut shutdown_receiver) = mpsc::channel::<()>(1);

    // Define the asynchronous task to handle optimization
    let julia_clone = julia.clone();
    let predicer_dir_clone = predicer_dir.clone();
    

    let shutdown_route = {
        let shutdown_sender_clone = shutdown_sender.clone();
        warp::path!("shutdown")
            .and(warp::post())
            .map(move || {
                // Send a shutdown signal
                let _ = shutdown_sender_clone.try_send(());
                warp::reply::json(&"Server is shutting down")
            })
    };

    let routes = shutdown_route;

    // Start the Warp server and extract only the future part of the tuple
    let (_, _server_future) = warp::serve(routes)
    .bind_with_graceful_shutdown(ip_address, async move {
        shutdown_receiver.recv().await;
    });

    let event_loop_task = event_loop::event_loop(julia_clone, predicer_dir_clone);

    tokio::select! {
        _ = event_loop_task => {
            println!("Event loop has been shut down");
        },
        _ = tokio::signal::ctrl_c() => {
            println!("Ctrl+C pressed. Shutting down...");
            // Perform any necessary shutdown procedures here
        },
    }

    std::mem::drop(julia);
    handle.await
    .expect("Julia exited with an error")
    .expect("The runtime thread panicked");

    println!("Server has been shut down");

    */

    
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

