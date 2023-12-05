
use std::env;
mod predicer;
mod utilities;
mod input_data;
use hertta::julia_interface;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, AUTHORIZATION};
use serde_json::json;
use tokio::time::{self, Duration};
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


#[derive(Debug)]
struct MyError(String);

impl fmt::Display for MyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Write the inner String of MyError to the provided formatter
        write!(f, "{}", self.0)
    }
}

impl warp::reject::Reject for MyError {}


fn _print_tuple_vector(vec: &Vec<(String, f64)>) {
    for (s, num) in vec {
        println!("{}: {}", s, num);
    }
}

async fn _make_post_request(url: &str, data: &str, token: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Construct the request headers
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("{}", token)).unwrap(),
    );

    // Construct the payload as a JSON object
    let payload = json!({
        "title": "REST Call Received",
        "message": format!("data: {}", data),
    });
	
    // Send the POST request
    let client = reqwest::Client::new();
    let response = client
        .post(url)
        .headers(headers)
        .json(&payload) // Use the correct json! macro
        .send()
        .await?;

    // Check the response status
    if let Err(err) = response.error_for_status() {
        eprintln!("Error making POST request: {:?}", err);
        return Err(Box::new(err));
    }

    Ok(())
}

async fn _make_post_request_light(url: &str, entity_id: &str, token: &str, brightness: f64) -> Result<(), Box<dyn std::error::Error>> {
    // Construct the request headers
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("{}", token)).unwrap(),
    );

    // Construct the payload as a JSON object
    let payload = json!({
        "entity_id": entity_id,
        "brightness": brightness,
    });
    
	
    // Send the POST request
    let client = reqwest::Client::new();
    let response = client
        .post(url)
        .headers(headers)
        .json(&payload) // Use the correct json! macro
        .send()
        .await?;

    // Check the response status
    if let Err(err) = response.error_for_status() {
        eprintln!("Error making POST request: {:?}", err);
        return Err(Box::new(err));
    }

    Ok(())
}


// Data structure for messaging between Home Assistant UI.
#[derive(Deserialize, Serialize, Debug)]
struct DataHass {
	entity_cat: i32,
	entity_id: String,
	data_type: i32,
	data_unit: String,
	data_str: String,
	data_int: i32,
	data_float: f32,
	data_bool: bool,
	date_time: String,
}

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

fn init_julia_runtime() -> Result<(AsyncJulia<Tokio>, JoinHandle<Result<(), Box<JlrsError>>>), MyError> {
    unsafe {
        RuntimeBuilder::new()
            .async_runtime::<Tokio>()
            .channel_capacity(NonZeroUsize::new(4).ok_or(MyError("Invalid channel capacity".to_string()))?)
            .start_async::<1>()
            .map_err(|e| MyError(format!("Could not init Julia: {:?}", e)))
    }
}

async fn execute_task(julia: &AsyncJulia<Tokio>) -> Result<(), MyError> {
    let (sender, receiver) = tokio::sync::oneshot::channel();

    julia
        .register_task::<RunPredicer, _>(sender)
        .dispatch_any()
        .await;

    let task_result = receiver.await.map_err(|e| MyError(format!("Channel error: {:?}", e)))?;
    task_result.map_err(|e| MyError(format!("Task execution error: {:?}", e)))
}

async fn send_task_to_runtime(
    julia: &AsyncJulia<Tokio>,
    data: input_data::InputData, // Replace with the actual type of your data
    predicer_dir: String, // Replace with the actual type of your predicer_dir
    sender: tokio::sync::oneshot::Sender<Result<Vec<(String, f64)>, Box<JlrsError>>>,
) -> Result<(), MyError> {

    let dispatch_result = julia
        .task(
            RunPredicer {
                data,
                predicer_dir,
            },
            sender,
        )
        .try_dispatch_any();

    match dispatch_result {
        Ok(()) => Ok(()),
        Err(_dispatcher) => {
            // Handle the error or retry
            // For example, you could log the error and return a custom MyError
            Err(MyError("Failed to dispatch task".to_string()))
        }
    }
}

async fn receive_task_result(
    receiver: tokio::sync::oneshot::Receiver<Result<Vec<(String, f64)>, Box<JlrsError>>>,
) -> Result<Vec<(String, f64)>, MyError> {
    match receiver.await {
        Ok(result) => match result {
            Ok(value) => Ok(value),
            Err(e) => Err(MyError(format!("Task execution error: {:?}", e))),
        },
        Err(e) => Err(MyError(format!("Failed to receive from channel: {:?}", e))),
    }
}

async fn send_light_commands(
    url: &str,
    entity_id: &str,
    hass_token: &str,
    brightness_values: &[f64], // Assuming brightness values are u8, adjust as needed
) -> Result<(), MyError> {
    for brightness in brightness_values.iter().take(2) {
        println!("Setting brightness to: {}", brightness);
        if let Err(err) = _make_post_request_light(url, entity_id, hass_token, *brightness).await {
            eprintln!("Error in making POST request for brightness {}: {:?}", brightness, err);
            // Decide how to handle the error: return or continue to the next iteration
        } else {
            println!("POST request successful for brightness: {}", brightness);
        }

        // Wait for 2 seconds before sending the next request
        println!("Waiting for 2 seconds before next request...");
        time::sleep(Duration::from_secs(2)).await;
    }
    Ok(())
}

async fn _shutdown_julia_runtime(julia: AsyncJulia<Tokio>, handle: JoinHandle<Result<(), Box<JlrsError>>>) -> Result<(), MyError> {
    // Dropping `julia` to shut down the runtime
    std::mem::drop(julia);

    // Await the handle and handle any errors
    match handle.await {
        Ok(Ok(())) => Ok(()), // Both thread execution and task were successful
        Ok(Err(e)) => Err(MyError(format!("Julia task exited with an error: {:?}", e))), // Task returned an error
        Err(e) => Err(MyError(format!("Join handle failed: {:?}", e))), // Thread panicked or similar issue
    }
}

use tokio::sync::Mutex;

async fn run_predicer(
    julia: Arc<Mutex<AsyncJulia<Tokio>>>,
    data: input_data::InputData,
    predicer_dir: String,
) -> Result<Vec<(String, f64)>, MyError> {

    let julia_guard = julia.lock().await;

    match execute_task(&*julia_guard).await {
        Ok(()) => println!("Task executed successfully"),
        Err(e) => eprintln!("Task execution failed: {}", e),
    }

    let (sender, receiver) = tokio::sync::oneshot::channel();

    if let Err(e) = send_task_to_runtime(&*julia_guard, data, predicer_dir, sender).await {
        eprintln!("Failed to send task to runtime: {}", e);
        return Err(e);
    }

    let result = match receive_task_result(receiver).await {
        Ok(value) => {
            println!("Results received.");
            value // value is of type Vec<(String, f64)>
        },
        Err(e) => {
            eprintln!("Error receiving task result: {}", e);
            return Err(MyError(format!("Error receiving task result: {:?}", e))); // Updated error handling
        }
    };

    utilities::_print_tuple_vector(&result);

    // Return the result
    Ok(result)


}


async fn change_brightness(
    values: Vec<(String, f64)>, 
    hass_token: String, 
    url: &str, 
    entity_id: &str
) -> Result<(), MyError> {
    // Transform values to brightness values
    let brightness_values: Vec<f64> = values.iter().map(|(_, value)| *value * 20.0).collect();
    println!("Brightness Values: {:?}", brightness_values);

    // Now passing a reference to Vec<f64> to send_light_commands
    let light_command = send_light_commands(url, entity_id, &hass_token, &brightness_values).await;
    
    match light_command {
        Ok(()) => {
            println!("Light command successful.");
            Ok(())
        },
        Err(e) => {
            eprintln!("Error in light commands: {}", e);
            return Err(MyError(format!("Error receiving task result: {:?}", e))); // Convert the error to a Box<dyn std::error::Error>
        },
    }
}



use std::sync::Arc;
use std::fs;
use std::net::SocketAddr;

// Import necessary modules and types (make sure they are correctly referenced)
// use jlrs::prelude::{AsyncJulia, JlrsError};
// use your_module::{input_data, run_predicer, MyError, init_julia_runtime};

#[tokio::main]
async fn main() {
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let predicer_dir = args
        .get(1)
        .expect("First argument should be path to Predicer")
        .to_string();

    //let options_path = "./src/options.json";
    let options_path = "/data/options.json";

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
	let _floor_area = &options.floor_area;
	let _stories = &options.stories;
	let _insulation_u_value = &options.insulation_u_value;
    let listen_ip = options.listen_ip.clone();
    let port = options.port.clone();
	let hass_token = options.hass_token.clone();
    let url = "http://192.168.1.171:8123/api/services/light/turn_on";
    let entity_id = "light.katto1";
	
	// Partially mask the hass token for printing.
	let _masked_token = if options.hass_token.len() > 4 {
		let last_part = &options.hass_token[options.hass_token.len() - 4..];
		let masked_part = "*".repeat(options.hass_token.len() - 4);
		format!("{}{}", masked_part, last_part)
	} else {
		// If the token is too short, just print it as is
		options.hass_token.clone()
	};

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

    // Define the route for handling POST requests to run the Julia task
    let my_route = {
        let julia = julia.clone();
        let predicer_dir = predicer_dir.clone();
        warp::path!("from_hass" / "post")
            .and(warp::post())
            .and(warp::body::json()) // Assuming you're receiving JSON data
            .map(move |data: input_data::HassData| { // Update the type of 'data' if needed
                // Clone shared resources
                let julia_clone = julia.clone();
                let predicer_dir_clone = predicer_dir.clone();
                let hass_token_clone = hass_token.clone();
                let url_clone = url.to_string();
                let entity_id_clone = entity_id.to_string();

                let data = input_data::create_data(data.init_temp);

                // Spawn an asynchronous task to run the Julia task
                tokio::spawn(async move {
                    // Call the function to run the Julia task with the provided data
                    match run_predicer(julia_clone, data, predicer_dir_clone).await {
                        Ok(result) => {
                            // Handle the successful result of the Julia task
                            println!("Julia task completed successfully: {:?}", result);
                            match change_brightness(result, hass_token_clone, &url_clone, &entity_id_clone).await {
                                Ok(_) => {
                                    // Handle the successful case here, if needed
                                    println!("change_brightness executed successfully");
                                }
                                Err(e) => {
                                    eprintln!("Error running change_brightness: {:?}", e);
                                }
                            }
                        }
                        Err(e) => {
                            // Handle any errors that occurred during the Julia task
                            eprintln!("Error running Julia task: {:?}", e);
                        }
                    }
                });

                // Respond to the request
                warp::reply::json(&"Request received, logic is running")
            })
    };

    // Define the route for triggering a graceful shutdown
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

    // Combine the routes
    let routes = my_route.or(shutdown_route);

    // Start the Warp server with graceful shutdown
    let server = {
        let (_, server) = warp::serve(routes)
            .bind_with_graceful_shutdown(ip_address, async move {
                shutdown_receiver.recv().await;
            });
        server
    };
    println!("Server started at {}", ip_address);

    // Run the server and listen for Ctrl+C
    tokio::select! {
        _ = server => {},
        _ = tokio::signal::ctrl_c() => {
            // Trigger shutdown if Ctrl+C is pressed
            let _ = shutdown_sender.send(());
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

    async fn test_julia_instance_functions() {

        //Init julia
        unsafe {
            RuntimeBuilder::new()
                .async_runtime::<Tokio>()
                .channel_capacity(NonZeroUsize::new(4).ok_or(MyError("Invalid channel capacity".to_string()))?)
                .start_async::<1>()
                .map_err(|e| MyError(format!("Could not init Julia: {:?}", e)));
        }

    }

    */


}

