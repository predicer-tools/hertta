

mod predicer;
mod utilities;
mod input_data;
mod errors;
mod event_loop;

use std::env;
use hertta::julia_interface;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, AUTHORIZATION};
use serde_json::json;
use tokio::time::{self, Duration, Instant};
use warp::Filter;
use serde::{Deserialize, Serialize};
use serde_json;
use tokio::sync::mpsc;
use std::num::NonZeroUsize;
use jlrs::prelude::*;
use predicer::RunPredicer;
use jlrs::error::JlrsError;
use tokio::task::JoinHandle;
use std::fmt;
use reqwest::Client;
use tokio::sync::Mutex;
use std::sync::Arc;
use std::fs;
use std::net::SocketAddr;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::join;
use warp::reject::Reject;
use chrono::{Utc, Duration as ChronoDuration, TimeZone, Timelike};

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
    if !utilities::is_valid_http_header_value(token) {
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

/// Executes a specific task using the Julia runtime.
///
/// This asynchronous function sends a task to the Julia runtime for execution and awaits its result.
/// The task communication is managed through a one-shot channel.
///
/// # Parameters
/// - `julia`: Reference to the initialized AsyncJulia runtime.
///
/// # Returns
/// Returns `Ok(())` if the task is successfully executed, or `JuliaError` in case of any failure
/// during task dispatch or execution.
///
/// # Errors
/// - Returns `JuliaError` if there is an issue with the channel communication or if the Julia task execution fails.

async fn execute_task(julia: &AsyncJulia<Tokio>) -> Result<(), errors::JuliaError> {
    // Create a one-shot channel for task communication. `sender` is used to send the task result,
    // and `receiver` is used to await this result.
    let (sender, receiver) = tokio::sync::oneshot::channel();

    // Register and dispatch a task to the Julia runtime. The specific task type and parameters are
    // determined by `RunPredicer`. An error here would typically indicate issues in task registration or dispatch.
    julia
        .register_task::<RunPredicer, _>(sender)
        .dispatch_any()
        .await;

    // Await the task result from the receiver. If the receiver encounters a channel error,
    // it is converted to `JuliaError`.
    let task_result = receiver.await.map_err(|e| errors::JuliaError(format!("Channel error in executing Julia task: {:?}", e)))?;
    
    // If the task was received but resulted in an execution error, convert this error to `JuliaError`.
    // This typically indicates a failure within the task's logic or processing.
    task_result.map_err(|e| errors::JuliaError(format!("Task execution error: {:?}", e)))
}

/// Asynchronously sends a task to the Julia runtime for execution.
///
/// This function prepares a task with the provided input data and Predicer directory, then dispatches it
/// to the Julia runtime. The result of the task execution is communicated back through a one-shot channel.
///
/// # Parameters
/// - `julia`: Reference to the initialized AsyncJulia runtime.
/// - `data`: The input data for the task.
/// - `predicer_dir`: Predicer directory. 
/// - `sender`: A one-shot channel sender used to send the result of the task execution back.
///
/// # Returns
/// Returns `Ok(())` if the task is successfully dispatched to the runtime.
/// Returns `JuliaError` if the task fails to be dispatched.
///
/// # Errors
/// Returns `JuliaError` with a message indicating failure in dispatching the task to the runtime.

async fn send_task_to_runtime(
    julia: &AsyncJulia<Tokio>,
    data: input_data::InputData, 
    predicer_dir: String, 
    sender: tokio::sync::oneshot::Sender<Result<HashMap<String, predicer::ControlValues>, Box<JlrsError>>>,
) -> Result<(), errors::JuliaError> {

    // Dispatch a task with the provided data and directory to the Julia runtime.
    // Runs task "RunPredicer"
    let dispatch_result = julia
        .task(
            RunPredicer {
                data,
                predicer_dir,
            },
            sender,
        )
        .try_dispatch_any();
    
    // Handle the result of the dispatch attempt.
    match dispatch_result {
        Ok(()) => Ok(()), // Indicates successful dispatch of the task.
        Err(_dispatcher) => {
            // This branch handles the case where the task could not be dispatched.
            // An appropriate error message is returned wrapped in `JuliaError`.
            Err(errors::JuliaError("Failed to dispatch task in sending task to runtime".to_string()))
        }
    }
}

/// Asynchronously receives the result of a Julia task execution.
///
/// This function waits for the result of a task sent to the Julia runtime, which is received via a one-shot channel.
/// It handles both successful and erroneous results, converting them into appropriate Rust Result types.
///
/// # Parameters
/// - `receiver`: A one-shot channel receiver that awaits the result of the Julia task.
///
/// # Returns
/// - On success, returns a vector of tuples containing the task results.
/// - On failure, returns a `JuliaError` indicating the nature of the failure.
///
/// # Errors
/// - If the task execution itself fails, a `JuliaError` with the execution error message is returned.
/// - If there is an error in receiving the task result from the channel, a `JuliaError` indicating this failure is returned.
///
async fn receive_task_result(
    receiver: tokio::sync::oneshot::Receiver<Result<HashMap<String, predicer::ControlValues>, Box<JlrsError>>>,
) -> Result<HashMap<String, predicer::ControlValues>, errors::JuliaError> {
    match receiver.await {
        Ok(result) => match result {
            // The task result is successfully received from the channel.
            Ok(value) => 
            // The task executed successfully and returned a value.
            Ok(value),
            Err(e) => 
            // The task executed but resulted in an error. This error is converted to a `JuliaError`.
            Err(errors::JuliaError(format!("Task execution error: {:?}", e))),
        },
        Err(e) => 
        // An error occurred in receiving the result from the channel, which is converted to a `JuliaError`.
        Err(errors::JuliaError(format!("Failed to receive task result from channel in julia task: {:?}", e))),
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


/// Executes Run Predicer task using a Julia runtime.
///
/// This function handles the execution of optimization task by interfacing with a Julia runtime.
/// It involves sending Predicer task to the runtime, waiting for its completion, and retrieving the results.
///
/// # Parameters
/// - `julia`: An `Arc<Mutex<AsyncJulia<Tokio>>>` providing shared, thread-safe access to the Julia runtime.
/// - `data`: The input data required for the prediction task.
/// - `predicer_dir`: The directory path to optimization tool
///
/// # Returns
/// Returns a `Result` with a vector of tuples (String, f64) representing the prediction results,
/// or a `JuliaError` if any part of the process fails.
///
/// # Errors
/// - Errors can occur during task execution, sending the task to runtime, or receiving the task results.
/// - All errors are converted to `JuliaError` for a consistent error handling experience.
///
async fn run_predicer(
    julia: Arc<Mutex<AsyncJulia<Tokio>>>,
    data: input_data::InputData,
    predicer_dir: String,
) -> Result<HashMap<String, predicer::ControlValues>, errors::JuliaError> {

    // Acquire a lock on the Julia runtime.
    let julia_guard = julia.lock().await;

    // Attempt to execute the task using the Julia runtime.
    match execute_task(&*julia_guard).await {
        Ok(()) => println!("Task executed successfully"),
        Err(e) => eprintln!("Task execution failed: {}", e),
    }

    // Create a one-shot channel for task result communication.
    let (sender, receiver) = tokio::sync::oneshot::channel();

    // Send the prediction task to the runtime, handling any errors.
    if let Err(e) = send_task_to_runtime(&*julia_guard, data, predicer_dir, sender).await {
        eprintln!("Failed to send task to runtime: {}", e);
        return Err(e);
    }

    // Wait for and handle the task result.
    let result = match receive_task_result(receiver).await {
        Ok(value) => {
            println!("Results received.");
            value // value is of type Vec<(String, f64)>
        },
        Err(e) => {
            eprintln!("Error receiving task result: {}", e);
            return Err(errors::JuliaError(format!("Error receiving task result: {:?}", e))); // Updated error handling
        }
    };

    // Return the successful result.
    Ok(result)

}

async fn control_values_to_hass(result: Vec<(String, f64)>, entity_id: &str, server_1_url: &str) -> Result<(), reqwest::Error> {
    // Create JSON payload
    let json_payload = json!({
        "entity_id": entity_id,
        "control_values": result
    });

    // Create a client and make a POST request
    let client = reqwest::Client::new();
    client.post(server_1_url)
          .json(&json_payload)
          .send()
          .await?;

    Ok(())
}

// Example of Options for the development phase
// Configuration options saved into a json file in the addon data directory.
#[derive(Deserialize, Debug)]
struct Options {
	floor_area: i32,
	stories: i32,
	insulation_u_value: f32,
    listen_ip: String,
    port: String,
    hass_token: String,
}

pub fn write_to_json_file(data: &input_data::InputData, file_path: &str) -> Result<(), Box<dyn Error>> {
    // Serialize the data to JSON
    let json = serde_json::to_string_pretty(data)?;

    // Open a file in write mode
    let mut file = File::create(file_path)?;

    // Write the JSON data to the file
    file.write_all(json.as_bytes())?;

    Ok(())
}

pub fn serialize_device_control_values(
    device_control_values: &HashMap<String, Vec<(String, f64)>>,
) -> Result<String, Box<dyn Error>> {
    // Serialize the HashMap to a JSON string
    serde_json::to_string(device_control_values).map_err(|e| e.into())
}

#[derive(Serialize, Deserialize)]
struct OptimizationResult {
    data: f64,
}

#[tokio::main]
async fn main()  {

    
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let predicer_dir = args
        .get(1)
        .expect("First argument should be path to Predicer")
        .to_string();

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
    let (tx_optimization, mut rx_optimization) = mpsc::channel::<input_data::OptimizationData>(32);

    // Define the asynchronous task to handle optimization
    let julia_clone = julia.clone();
    let predicer_dir_clone = predicer_dir.clone();

    let optimization_task = tokio::spawn(async move {
        println!("Optimization task started and waiting for data...");
        while let Some(optimization_data) = rx_optimization.recv().await {
            println!("Received optimization data, running predicer...");
            
            match run_predicer(julia_clone.clone(), optimization_data.device_data.input_data.clone(), predicer_dir_clone.clone()).await {
                Ok(device_control_values) => {
                    // Process the results
                    println!("Optimization successful");
                },
                Err(error) => {
                    // Handle error
                    println!("An error occurred in optimization: {}", error);
                }
            }
        }
    });

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
    let (_, server_future) = warp::serve(routes)
    .bind_with_graceful_shutdown(ip_address, async move {
        shutdown_receiver.recv().await;
    });

    let event_loop_task = event_loop::event_loop(tx_optimization);

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

