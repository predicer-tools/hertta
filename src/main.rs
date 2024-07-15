

mod utilities;
mod input_data;
mod errors;
mod event_loop;
mod arrow_input;

use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, AUTHORIZATION};
use serde_json::json;
use serde_json;
use std::num::NonZeroUsize;
use tokio::task::JoinHandle;
use reqwest::Client;
use std::collections::HashMap;
use std::error::Error;
use std::process::Command;
use std::net::TcpStream;
use errors::FileReadError;
use serde_json::error::Error as SerdeError;
use regex::Regex;

use arrow::ipc::writer::StreamWriter;
use arrow_ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use std::io::BufWriter;
use zmq;
use std::thread;
use std::env;
use std::time;
use std::process::ExitStatus;
use serde_json::Value;
use std::io::{self, Read, Write, BufRead, BufReader};
use std::fs::File;
use serde_json::from_str;
use arrow::array::{Array, StringArray, Float64Array, Int32Array, ArrayRef};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
use warp::Filter;
use tokio::io::{AsyncBufReadExt, BufReader as AsyncBufReader};
use chrono::{Timelike, FixedOffset, Utc, Duration as ChronoDuration};
use std::time::Duration;
use tokio::time::sleep;

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

pub fn json_to_inputdata(json_file_path: &str) -> Result<input_data::InputData, Box<dyn Error>> {
    let mut file = File::open(json_file_path)?;
    let mut data = String::new();
    file.read_to_string(&mut data)?;

    let input_data: input_data::InputData = from_str(&data)?;
    Ok(input_data)
}

// Function to send serialized batches
pub fn send_serialized_batches(serialized_batches: &HashMap<String, Vec<u8>>, zmq_context: &zmq::Context) -> Result<(), Box<dyn Error>> {
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

// Function to start the Julia process locally
pub fn start_julia_local() -> Result<ExitStatus, std::io::Error> {
    let push_port = find_available_port();
    std::env::set_var("PUSH_PORT", push_port.to_string());

    let mut julia_command = Command::new("C:\\Users\\enessi\\AppData\\Local\\Microsoft\\WindowsApps\\julia.exe");
    julia_command.arg("--project=C:\\users\\enessi\\Documents\\hertta-kaikki\\hertta-addon\\hertta");
    julia_command.arg("C:\\users\\enessi\\Documents\\hertta-kaikki\\hertta-addon\\hertta\\src\\Pr_ArrowConnection.jl");
    julia_command.status()  // Ensure this line returns the status
}

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


#[derive(Debug)]
pub struct DataTable {
    pub columns: Vec<String>,
    pub data: Vec<Vec<String>>,
}

impl DataTable {
    pub fn from_record_batch(batch: &RecordBatch) -> Self {
        let columns = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();

        let mut data = Vec::new();

        for row_index in 0..batch.num_rows() {
            let mut row = Vec::new();
            for column in batch.columns() {
                let value = column_value_to_string(column, row_index);
                row.push(value);
            }
            data.push(row);
        }

        DataTable { columns, data }
    }
}


pub fn column_value_to_string(column: &ArrayRef, row_index: usize) -> String {
    if column.is_null(row_index) {
        return "NULL".to_string();
    }

    match column.data_type() {
        arrow::datatypes::DataType::Utf8 => {
            let array = column.as_any().downcast_ref::<StringArray>().unwrap();
            array.value(row_index).to_string()
        }
        arrow::datatypes::DataType::Float64 => {
            let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
            array.value(row_index).to_string()
        }
        arrow::datatypes::DataType::Int32 => {
            let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
            array.value(row_index).to_string()
        }
        _ => "Unsupported type".to_string(),
    }
}

pub fn create_time_data() -> input_data::TimeData {
    // Get the current time and round it to the latest hour
    let start_time = Utc::now().with_timezone(&FixedOffset::east_opt(0).unwrap())
                                .with_minute(0).unwrap()
                                .with_second(0).unwrap()
                                .with_nanosecond(0).unwrap();
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {


    let (tx, rx) = mpsc::channel::<input_data::OptimizationData>(32);

    event_loop::event_loop(rx).await;

    let input_data = json_to_inputdata("src/building_e.json")?;

    let time_data = create_time_data();

    let optimization_data = input_data::OptimizationData {
        country: Some("fi".to_string()),
        location: Some("Hervanta".to_string()),
        timezone: None,
        elec_price_source: None,
        time_data: Some(time_data.clone()),
        weather_data: None,
        model_data: Some(input_data.clone()),
        elec_price_data: None,
        control_results: None,
        input_data_batch: None,
    };

    let data_instances = vec![
        optimization_data,
    ];

    for data in data_instances {
        tx.send(data).await.expect("Failed to send person");
    }

    //println!("Created OptimizationData for OPT command: {:?}", optimization_data);

    // Let the worker thread finish its work
    sleep(Duration::from_secs(50)).await;

    /* 

    println!("Serializing batches started");

    // Initialize the data store
    let data_store: Arc<Mutex<Vec<DataTable>>> = Arc::new(Mutex::new(Vec::new()));

    // Clone the data store for the server thread
    let data_store_clone = Arc::clone(&data_store);

    // Create and serialize record batches
    let serialized_batches = arrow_input::create_and_serialize_record_batches(&input_data);

   // Start the server in a separate thread
   let thread_join_handle = thread::Builder::new()
   .name("Predicer data server".to_string())
   .spawn(move || {
       // Setup ZMQ server
       let zmq_context = zmq::Context::new();
       let responder = zmq_context.socket(zmq::REP).unwrap();
       assert!(responder.bind(&format!("tcp://{}:5555", "*")).is_ok());

       let mut is_running = true;
       let receive_flags = 0;
       let send_flags = 0;

       // Server loop to handle requests
       while is_running {
           let request_result = responder.recv_string(receive_flags);
           match request_result {
               Ok(inner_result) => {
                   match inner_result {
                       Ok(command) => {
                           if command == "Hello" {
                               println!("Received Hello");

                               // Send all serialized batches
                               for (key, buffer) in &serialized_batches {
                                   println!("Sending batch: {}", key);
                                   if let Err(e) = responder.send(buffer, send_flags) {
                                       eprintln!("Failed to send batch {}: {:?}", key, e);
                                       is_running = false;
                                       break;
                                   }

                                   // Wait for acknowledgment from the client
                                   let ack_result = responder.recv_string(receive_flags);
                                   match ack_result {
                                       Ok(ack) => {
                                           println!("Received acknowledgment for batch: {}", key);
                                       }
                                       Err(e) => {
                                           eprintln!("Failed to receive acknowledgment: {:?}", e);
                                           is_running = false;
                                           break;
                                       }
                                   }
                               }

                               // Send END signal after all batches are sent
                               println!("Sending END signal");
                               if let Err(e) = responder.send("END", send_flags) {
                                   eprintln!("Failed to send END signal: {:?}", e);
                               }
                               
                           } else if command.starts_with("Take this!") {
                               let endpoint = command.strip_prefix("Take this! ").expect("cannot decipher endpoint");
                               responder.send("ready to receive", send_flags).expect("failed to confirm readiness for input");
                               //receive_data(endpoint, &zmq_context);
                               if let Err(e) = receive_data(endpoint, &zmq_context, &data_store_clone) {
                                eprintln!("Failed to receive data: {:?}", e);
                                }
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
               Err(e) => {
                   println!("Failed to receive data: {:?}", e);
                   thread::sleep(time::Duration::from_secs(1));
               }
           }
       }
   })
   .expect("failed to start server thread");


    // Start the Julia process
    match start_julia_local() {
        Ok(status) => {
            if !status.success() {
                eprintln!("Julia process failed with status: {:?}", status);
            }
        }
        Err(e) => {
            eprintln!("Failed to start Julia process: {:?}", e);
        }
    }

    // Wait for the server thread to finish
    let _ = thread_join_handle.join().expect("failed to join with server thread");

    // Lock the data store to access it
    let data_store = data_store.lock().unwrap();

    // Print the stored data tables using the print_data_table function
    for data_table in data_store.iter() {
        print_data_table(data_table);
    }

    */

    Ok(())
    
}

