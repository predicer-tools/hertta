mod arrow_errors;
mod arrow_input;
mod errors;
mod event_loop;
mod input_data;
mod settings;
mod utilities;

use chrono::{Duration as ChronoDuration, FixedOffset, Timelike, Utc};
use clap::Parser;
use input_data::{DataTable, InputData, OptimizationData};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use serde_json;
use serde_json::from_str;
use serde_json::json;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use warp::Filter;
use zmq;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct CommandLineArgs {
    #[arg(long, help = "write settings file and exit")]
    write_settings: bool,
}

fn write_default_settings_to_file(settings_file_path: &PathBuf) -> Result<(), Box<dyn Error>> {
    match settings_file_path.parent() {
        Some(settings_dir) => fs::create_dir_all(settings_dir)?,
        None => return Err("settings file should have a parent directory".into()),
    };
    let default_settings =
        settings::make_settings(&settings::map_from_environment_variables(), &PathBuf::new())?;
    let serialized_settings = toml::to_string_pretty(&default_settings)?;
    let mut settings_file = File::create(settings_file_path)?;
    settings_file.write_all(&serialized_settings.into_bytes())?;
    Ok(())
}

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
async fn _make_post_request(
    url: &str,
    entity_id: &str,
    token: &str,
    brightness: f64,
) -> Result<(), errors::PostRequestError> {
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

pub fn json_to_inputdata(json_file_path: &str) -> Result<input_data::InputData, Box<dyn Error>> {
    let mut file = File::open(json_file_path)?;
    let mut data = String::new();
    file.read_to_string(&mut data)?;

    let input_data: input_data::InputData = from_str(&data)?;
    Ok(input_data)
}

// Function to send serialized batches
pub fn send_serialized_batches(
    serialized_batches: &HashMap<String, Vec<u8>>,
    zmq_context: &zmq::Context,
) -> Result<(), Box<dyn Error>> {
    let push_socket = zmq_context.socket(zmq::PUSH)?;
    push_socket.connect("tcp://127.0.0.1:5555")?;

    for (key, batch) in serialized_batches {
        println!("Sending batch: {}", key); // Debug print
        push_socket.send(batch, 0)?;
    }

    Ok(())
}

pub fn find_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind to address");
    listener.local_addr().unwrap().port()
}

/*
pub fn receive_data(endpoint: &str, zmq_context: &zmq::Context, data_store: &Arc<Mutex<Vec<DataTable>>>) -> Result<(), Box<dyn Error>> {
    let receiver = zmq_context.socket(zmq::PULL)?;
    receiver.connect(endpoint)?;
    let flags = 0;

    let pull_result = receiver.recv_bytes(flags)?;
    let reader = StreamReader::try_new(pull_result.as_slice(), None).expect("Failed to construct Arrow reader");

    for record_batch_result in reader {
        let record_batch = record_batch_result.expect("Failed to read record batch");
        let data_table = DataTable::from_record_batch(&record_batch);

        let mut data_store = data_store.lock().unwrap();
        data_store.push(data_table);
    }

    Ok(())
}
*/

pub fn print_data_table(data_table: &DataTable) {
    // Print the column headers
    for column in &data_table.columns {
        print!("{:<20}", column);
    }
    println!();

    // Print a line separator
    for _ in &data_table.columns {
        print!("{:<20}", "--------------------");
    }
    println!();

    // Print the rows
    for row in &data_table.data {
        for cell in row {
            print!("{:<20}", cell);
        }
        println!();
    }
}

pub fn create_time_data() -> input_data::TimeData {
    // Get the current time and round it to the latest hour
    let start_time = Utc::now()
        .with_timezone(&FixedOffset::east_opt(0).unwrap())
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    let end_time = start_time + ChronoDuration::hours(12);

    // Generate a series of timestamps every 60 minutes
    let mut series = Vec::new();
    let mut current_time = start_time;
    while current_time <= end_time {
        series.push(current_time.format("%Y-%m-%dT%H:%M:%S").to_string());
        current_time = current_time + ChronoDuration::hours(1); // Adjust this duration to 60 minutes
    }

    input_data::TimeData {
        start_time,
        end_time,
        series,
    }
}

pub fn run_python_script() -> Result<(), Box<dyn Error>> {
    let status = Command::new("python").arg("input_data.py").status()?;

    if status.success() {
        println!("Python script executed successfully.");
    } else {
        println!("Python script failed to execute.");
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = CommandLineArgs::parse();
    if args.write_settings {
        let settings_file_path = settings::make_settings_file_path();
        write_default_settings_to_file(&settings_file_path)?;
        println!("Settings written to {}", settings_file_path.display());
        return Ok(());
    }
    let settings = settings::make_settings(
        &settings::map_from_environment_variables(),
        &settings::make_settings_file_path(),
    )?;
    if !settings.predicer_runner_project.is_empty() {
        fs::create_dir_all(Path::new(&settings.predicer_runner_project))?;
    }
    settings::validate_settings(&settings)?;
    // Define a route with query parameters for optimization
    let optimize_route = warp::path("optimize")
        .and(warp::post())
        .and(warp::query::<HashMap<String, String>>())
        .and(warp::body::json())
        .and_then(
            |params: HashMap<String, String>, input_data: InputData| async move {
                let fetch_time_data = params.get("fetch_time_data").map_or(false, |v| v == "true");
                let fetch_weather_data = params
                    .get("fetch_weather_data")
                    .map_or(false, |v| v == "true");
                let fetch_elec_data = params
                    .get("fetch_elec_data")
                    .map_or(false, |v| v == "elering" || v == "entsoe");
                let elec_price_source = params.get("fetch_elec_data").cloned();
                let country = params.get("country").cloned();
                let location = params.get("location").cloned();

                println!("Received optimization request with options:");
                println!("Fetch time data: {}", fetch_time_data);
                println!("Fetch weather data: {}", fetch_weather_data);
                println!("Fetch electricity data: {}", fetch_elec_data);
                println!("Electricity price source: {:?}", elec_price_source);
                println!("Country: {:?}", country);
                println!("Location: {:?}", location);

                // Create OptimizationData instance
                let optimization_data = OptimizationData {
                    fetch_weather_data,
                    fetch_elec_data,
                    fetch_time_data,
                    country,
                    location,
                    timezone: None,
                    elec_price_source: None,
                    model_data: Some(input_data),
                    time_data: None,
                    weather_data: None,
                    elec_price_data: None,
                    control_results: None,
                    input_data_batch: None,
                };

                let settings = settings::make_settings(
                    &settings::map_from_environment_variables(),
                    &settings::make_settings_file_path(),
                )
                .expect("failed to make settings");
                match settings::validate_settings(&settings) {
                    Ok(..) => (),
                    Err(error) => panic!("{}", error),
                };
                let (tx, rx) = mpsc::channel::<OptimizationData>(32);
                let tx = Arc::new(Mutex::new(tx));
                tokio::spawn(async move {
                    event_loop::event_loop(settings, rx).await;
                });

                let tx_clone = Arc::clone(&tx);
                tokio::spawn(async move {
                    let optimization_data = optimization_data.clone(); // Cloning the data to avoid ownership issues
                    let tx = tx_clone.lock().await;
                    if tx.send(optimization_data).await.is_err() {
                        eprintln!("Failed to send optimization data");
                    }
                });

                // Return a response
                Ok::<_, warp::Rejection>(warp::reply::json(&"Optimization started"))
            },
        );

    // Combine the routes
    let routes = warp::path("api").and(optimize_route);

    // Start the server
    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;

    Ok(())
}
