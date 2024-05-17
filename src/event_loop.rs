use tokio::time::{self, Duration};
//use serde_json::json;
use tokio::sync::{mpsc};
use crate::errors;
use crate::input_data;
use std::error::Error;
use std::collections::HashMap;
use crate::input_data::{OptimizationData};
use serde_yaml;
use jlrs::prelude::*;
use jlrs::error::JlrsError;
use tokio::sync::Mutex;
use std::sync::Arc;


pub async fn event_loop(
    julia: Arc<Mutex<AsyncJulia<Tokio>>>,
    predicer_dir: String,
) {
    let (_tx_model, _rx_model) = mpsc::channel::<OptimizationData>(32);
    let (tx_time, rx_time) = mpsc::channel::<OptimizationData>(32);
    let (tx_weather, rx_weather) = mpsc::channel::<OptimizationData>(32);
    let (tx_elec, rx_elec) = mpsc::channel::<OptimizationData>(32);
    let (tx_update, rx_update) = mpsc::channel::<OptimizationData>(32);
    let (tx_optimization, rx_optimization) = mpsc::channel::<OptimizationData>(32);
    let (_tx_final, mut _rx_final) = mpsc::channel::<OptimizationData>(32);


    // Spawn the model data task
    tokio::spawn(async move {
        fetch_model_data_task(tx_time).await; // Pass to next task
    });

    // Spawn the create_time_data_task
    tokio::spawn(async move {
        create_time_data_task(rx_time, tx_weather).await; // Output passed to fetch_weather_data_task
    });

    // Spawn the weather data task to process and pass the data to the electricity price data task
    tokio::spawn(async move {
        fetch_weather_data_task(rx_weather, tx_elec).await; // Output sent to fetch_elec_price_task
    });

    // Spawn the electricity price data task to process and potentially finalize the data
    tokio::spawn(async move {
        fetch_elec_price_task(rx_elec, tx_update).await; // Final output sent to _tx_final
    });

    tokio::spawn(async move {
        // Adjust the function signature of update_model_data_task to accept tx_optimization
        update_model_data_task(rx_update, tx_optimization).await;
    });

    // Spawn the optimization task
    let julia_clone_for_optimization = Arc::clone(&julia);
    let predicer_dir_clone_for_optimization = predicer_dir.clone();
    tokio::spawn(async move {
        //optimization_task_logic(rx_optimization, julia_clone_for_optimization, predicer_dir_clone_for_optimization).await;
    });

    let mut interval = time::interval(Duration::from_secs(180));

    loop {
        interval.tick().await; // Wait for the next interval tick

        // Check if there's data to process
        while let Some(final_data) = _rx_final.recv().await {
            // Correctly call send_to_server_task with the received data
            if let Err(e) = send_to_server_task(final_data).await {
                eprintln!("Error while sending final optimization data to server: {}", e);
            }
        }
    }
}

/*
// Define a function for the optimization logic
async fn optimization_task_logic(
    mut rx_optimization: mpsc::Receiver<OptimizationData>,
    julia_clone: Arc<Mutex<AsyncJulia<Tokio>>>,
    predicer_dir_clone: String,
) {
    println!("Optimization task started and waiting for data...");
    while let Some(optimization_data) = rx_optimization.recv().await {

        if let Some(model_data) = optimization_data.model_data {
            match run_predicer(julia_clone.clone(), model_data.input_data.clone(), predicer_dir_clone.clone()).await {
                Ok(device_control_values) => {
                    println!("Optimization successful. Control values: {:?}", device_control_values);
                },
                Err(e) => {
                    println!("Optimization failed: {:?}", e);
                }
            }
        } else {
            println!("Optimization data is missing model_data. Skipping...");
        }
    }
}
*/
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
/// 
/* 
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
*/

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
/* 
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
*/
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
/* 
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
*/
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
/// 
/* 
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
*/
async fn update_model_data_task(mut rx: mpsc::Receiver<OptimizationData>, tx: mpsc::Sender<OptimizationData>) {
    while let Some(mut optimization_data) = rx.recv().await {
        
        // Update temporals to model data
        if let Err(e) = update_timeseries(&mut optimization_data) {
            eprintln!("Failed to update timeseries: {}", e);
            // Optionally, continue to attempt other updates even if one fails.
        }
        
        // Update indoor temperature
        if let Err(e) = update_interior_air_initial_state(&mut optimization_data) {
            eprintln!("Failed to update indoor air initial state: {}", e);
            // Optionally, continue to attempt other updates even if one fails.
        }

        // Update outdoor temp flow
        if let Err(e) = update_outside_node_inflow(&mut optimization_data) {
            eprintln!("Failed to update outside node inflow: {}", e);
            // Optionally, continue to attempt other updates even if one fails.
        }

        // Update elec prices
        match update_npe_market_prices(&mut optimization_data) {
            Ok(_) => (),
            Err(e) => eprintln!("Failed to update NPE market prices: {}", e),
        }

        if tx.send(optimization_data).await.is_err() {
            eprintln!("Failed to send updated OptimizationData");
        } else {
            println!("OptimizationData updated and sent successfully.");
        }
    }
}

fn update_timeseries(optimization_data: &mut OptimizationData) -> Result<(), &'static str> {
    // Check if time_data is available
    let time_data = optimization_data.time_data.as_ref().ok_or("Time data is not available.")?;
    
    // Check if model_data is available and update timeseries
    if let Some(model_data) = &mut optimization_data.model_data {
        model_data.input_data.temporals.t = time_data.series.clone();
        Ok(())
    } else {
        Err("Model data is not available.")
    }
}

/* 
fn update_gen_constraints(optimization_data: &mut OptimizationData)  -> Result<(), &'static str> {

    if let Some(time_data) = &optimization_data.time_data {

        let model_data = optimization_data.model_data.as_mut().ok_or("Model data is not available.")?;

    } else {
        // ModelData is None
        return Err("Time Data is not available.");
    }


}
*/

fn update_interior_air_initial_state(optimization_data: &mut OptimizationData) -> Result<(), &'static str> {
    // Check if sensor_data is Some
    if let Some(sensor_data) = &optimization_data.sensor_data {
        // Ensure model_data is available
        let model_data = optimization_data.model_data.as_mut().ok_or("Model data is not available.")?;
        let nodes = &mut model_data.input_data.nodes;

        // Find the sensor data for "interiorair"
        if let Some(interior_air_sensor) = sensor_data.iter().find(|s| s.sensor_name == "interiorair") {
            // Update the interiorair initial_state with the sensor temp
            if let Some(node) = nodes.get_mut("interiorair") {
                node.state.initial_state = interior_air_sensor.temp; // Correctly update the initial_state
                return Ok(());
            } else {
                // "interiorair" node not found
                return Err("InteriorAir node not found in nodes.");
            }
        } else {
            // "interiorair" sensor not found
            return Err("InteriorAir sensor not found in sensor data.");
        }
    } else {
        // sensor_data is None
        return Err("Sensor data is not available.");
    }
}

fn update_npe_market_prices(optimization_data: &mut OptimizationData) -> Result<(), &'static str> {
    // Check if ElectricityPriceData is Some
    if let Some(electricity_price_data) = &optimization_data.elec_price_data {
        // Ensure model_data is available and contains the "npe" market
        let model_data = optimization_data.model_data.as_mut().ok_or("Model data is not available.")?;
        let markets = &mut model_data.input_data.markets;

        // Find the market named "npe"
        if let Some(npe_market) = markets.get_mut("npe") {
            // Update the npe market's price with the electricity_price_data
            npe_market.price.ts_data = electricity_price_data.price_data.ts_data.clone();
            // Additionally, update the up_price and down_price based on electricity_price_data
            npe_market.up_price.ts_data = electricity_price_data.up_price_data.ts_data.clone();
            npe_market.down_price.ts_data = electricity_price_data.down_price_data.ts_data.clone();
            return Ok(());
        } else {
            // "npe" market not found
            return Err("NPE market not found in markets.");
        }
    } else {
        // ElectricityPriceData is None
        return Err("Electricity price data is not available.");
    }
}


async fn send_to_server_task(final_data: OptimizationData) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::new();
    let url = "http://127.0.0.1:8000/from_hertta/optimization_results"; // Your server URL

    match client.post(url)
        .json(&final_data)
        .send()
        .await {
            Ok(response) => {
                if response.status().is_success() {
                    //println!("Successfully sent final optimization data.");
                } else {
                    eprintln!("Failed to send final optimization data. Status: {}", response.status());
                }
            },
            Err(e) => eprintln!("Error sending request: {:?}", e),
    }

    Ok(())
}


async fn fetch_model_data_task(tx: mpsc::Sender<OptimizationData>) {
    let mut interval = time::interval(Duration::from_secs(10));

    loop {
        interval.tick().await;

        match fetch_model_data().await {
            Ok(optimization_data) => {
                // Update the token in the optimization data
                /* 
                if let Err(e) = update_token_in_optimization_data(&mut optimization_data).await {
                    eprintln!("Failed to update token in OptimizationData: {}", e);
                    // Optionally, handle the error, such as skipping this iteration, logging, etc.
                    continue;
                }
                */

                // Send the updated OptimizationData downstream
                if tx.send(optimization_data).await.is_err() {
                    eprintln!("Failed to send OptimizationData.");
                }
            },
            Err(e) => eprintln!("Failed to fetch or construct OptimizationData: {}", e),
        }
    }
}

/* 
async fn update_token_in_optimization_data(optimization_data: &mut OptimizationData) -> Result<(), io::Error> {
    let token = fs::read_to_string("config/token.txt")?;

    if let Some(ref mut elec_price_source) = optimization_data.elec_price_source {
        elec_price_source.token = Some(token);
    } else {
        // Optionally handle the case where elec_price_source is None
        optimization_data.elec_price_source = Some(ElecPriceSource {
            api_source: String::new(), // Or some default value
            token: Some(token),
            country: None,
            bidding_in_domain: None,
            bidding_out_domain: None,
        });
    }

    Ok(())
}
*/

async fn create_time_data_task(
    mut rx: mpsc::Receiver<OptimizationData>,
    tx: mpsc::Sender<OptimizationData>,
) {
    while let Some(mut optimization_data) = rx.recv().await {
        if let Some(timezone_str) = optimization_data.timezone.as_deref() {
            if let Ok((start_time, end_time)) = input_data::calculate_time_range(timezone_str, &optimization_data.temporals) {
                // Generate the series of timestamps based on the DateTime<FixedOffset> objects
                // Here, convert DateTime<FixedOffset> to strings only if necessary for the generate_hourly_timestamps function or any subsequent operation
                match input_data::generate_hourly_timestamps(start_time, end_time).await {
                    Ok(series) => {
                        // Update optimization_data.time_data with the new TimeData
                        // Here we keep start_time and end_time as DateTime<FixedOffset> objects

                        optimization_data.time_data = Some(input_data::TimeData {
                            start_time,
                            end_time,
                            series,
                        });
                        
                    },
                    Err(e) => {
                        eprintln!("Error generating hourly timestamps: {}", e);
                        continue;
                    }
                }
            } else {
                eprintln!("Error calculating time range.");
                continue;
            }
        } else {
            eprintln!("Timezone is missing from OptimizationData.");
            continue;
        }

        // Send the updated optimization data
        if tx.send(optimization_data).await.is_err() {
            eprintln!("Failed to send updated OptimizationData");
        }
    }
}


async fn fetch_weather_data_task(mut rx: mpsc::Receiver<OptimizationData>, tx: mpsc::Sender<OptimizationData>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));

    loop {
        tokio::select! {
            Some(mut optimization_data) = rx.recv() => {
                if let Some(time_data) = &optimization_data.time_data {
                    let location = optimization_data.location.clone().unwrap_or_default();

                    // Format start_time and end_time to the desired string representation
                    let start_time_formatted = time_data.start_time.format("%Y-%m-%dT%H:00:00Z").to_string();
                    let end_time_formatted = time_data.end_time.format("%Y-%m-%dT%H:00:00Z").to_string();

                    // Log the formatted times for debugging
                    //println!("Formatted start_time: {}", start_time_formatted);
                    //println!("Formatted end_time: {}", end_time_formatted);

                    // Pass formatted times as strings to fetch_weather_data
                    match fetch_weather_data(start_time_formatted, end_time_formatted, location).await {
                        Ok(weather_values) => {
                            //println!("Fetched weather data: {:?}", weather_values);

                            // Now that we have fetched weather values, create and update WeatherData
                            // Note: You must define the function create_and_update_weather_data if not already defined
                            create_and_update_weather_data(&mut optimization_data, &weather_values);
                        },
                        Err(e) => eprintln!("fetch_weather_data_task: Failed to fetch weather data: {}", e),
                    }

                    // Attempt to send the updated optimization data back
                    if tx.send(optimization_data).await.is_err() {
                        eprintln!("Failed to send updated optimization data");
                    }
                } else {
                    println!("fetch_weather_data_task: Time data is missing.");
                }
            },
            _ = interval.tick() => println!("Interval ticked, but no new optimization data received."),
            else => break,
        }
    }
}

pub fn update_outside_node_inflow(optimization_data: &mut OptimizationData) -> Result<(), &'static str> {
    // Check if weather_data is Some
    if let Some(weather_data) = &optimization_data.weather_data {
        // Ensure model_data is available and contains the "outside" node
        let model_data = optimization_data.model_data.as_mut().ok_or("Model data is not available.")?;
        let nodes = &mut model_data.input_data.nodes;

        // Find the node named "outside"
        if let Some(outside_node) = nodes.get_mut("outside") {
            // Check if the node is supposed to have inflow data
            if outside_node.is_inflow {
                // Update the outside node's inflow with the weather_data
                outside_node.inflow = weather_data.weather_data.clone(); // Assuming this is the TimeSeriesData you want to use
                return Ok(());
            } else {
                return Err("Outside node is not marked for inflow.");
            }
        } else {
            // "outside" node not found
            return Err("Outside node not found in nodes.");
        }
    } else {
        // weather_data is None
        return Err("Weather data is not available.");
    }
}

// Function to create TimeSeriesData from weather values and update the weather_data field in optimization_data
pub fn create_and_update_weather_data(optimization_data: &mut OptimizationData, values: &[f64]) {
    if let Some(time_data) = &optimization_data.time_data {
        // Extract series from TimeData
        let series = &time_data.series;

        // Use the extracted series and the provided values to pair them together
        let paired_series = pair_timeseries_with_values(series, values);

        // Generate TimeSeriesData from the paired series
        let ts_data = create_time_series_data_with_scenarios(paired_series);

        // Assuming you have the place information somewhere in optimization_data. If not, adjust as necessary.
        let place = optimization_data.location.clone().unwrap_or_default();

        // Update the weather_data field in optimization_data with the new TimeSeriesData and place
        optimization_data.weather_data = Some(input_data::WeatherData {
            place,
            weather_data: ts_data,
        });
    } else {
        eprintln!("TimeData is missing in optimization_data. Cannot update weather_data.");
    }
}

// Function to create TimeSeriesData with scenarios "s1" and "s2" using paired series
fn create_time_series_data_with_scenarios(paired_series: Vec<(String, f64)>) -> input_data::TimeSeriesData {
    // Create TimeSeries instances for scenarios "s1" and "s2" with the same series data
    let time_series_s1 = input_data::TimeSeries {
        scenario: "s1".to_string(),
        series: paired_series.clone(), // Clone to use the series data for both TimeSeries
    };

    let time_series_s2 = input_data::TimeSeries {
        scenario: "s2".to_string(),
        series: paired_series, // Use the original series data here
    };

    // Bundle the two TimeSeries into TimeSeriesData
    input_data::TimeSeriesData {
        ts_data: vec![time_series_s1, time_series_s2],
    }
}

pub fn pair_timeseries_with_values(series: &[String], values: &[f64]) -> Vec<(String, f64)> {
    series.iter().zip(values.iter())
        .map(|(timestamp, &value)| (timestamp.clone(), value))
        .collect()
}

pub fn _create_gen_constraints_timeseries(_optimization_data: &mut OptimizationData) {

}



async fn fetch_weather_data(start_time: String, end_time: String, place: String) -> Result<Vec<f64>, Box<dyn Error + Send>> {
    //println!("Fetching weather data for place: {}, start time: {}, end time: {}", place, start_time, end_time);

    let client = reqwest::Client::new();
    let url = format!("http://localhost:8001/get_weather_data?start_time={}&end_time={}&place={}", start_time, end_time, place);

    let response = client.get(&url).send().await.map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;

    //println!("Response status: {}", response.status());

    let response_body = response.text().await.map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;
    //println!("Raw response body: {}", response_body);

    match serde_json::from_str::<input_data::WeatherDataResponse>(&response_body) {
        Ok(fmi_data) => {
            //println!("Deserialized Weather Data: {:?}", fmi_data.weather_values);
            // Convert temperatures from Celsius to Kelvin
            let temperatures_in_kelvin: Vec<f64> = fmi_data.weather_values.iter().map(|&celsius| celsius + 273.15).collect();
            //println!("Temperatures in Kelvin: {:?}", temperatures_in_kelvin);
            Ok(temperatures_in_kelvin)
        },
        Err(e) => {
            eprintln!("Error deserializing response body: {}", e);
            Err(Box::new(e))
        },
    }
}

async fn fetch_electricity_prices(start_time: String, end_time: String, country: String) -> Result<Vec<f64>, Box<dyn Error + Send>> {
    //println!("Fetching electricity price data for country: {}, start time: {}, end_time: {}", country, start_time, end_time);

    let client = reqwest::Client::new();
    let url = format!("https://dashboard.elering.ee/api/nps/price?start={}&end={}", start_time, end_time);

    // Fetch the response
    let response = client.get(&url).header("accept", "*/*").send().await.map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;

    // Attempt to print the raw response text for debugging before parsing
    let response_text = response.text().await.map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;

    // Deserialize the response text into the custom ApiResponse structure
    let api_response: input_data::EleringData = serde_json::from_str(&response_text).map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;

    // Extract only the prices for the specified country and debug print the extracted segment
    let country_data = api_response.data.get(&country)
        .ok_or_else(|| Box::new(std::io::Error::new(std::io::ErrorKind::NotFound, "Country data not found")) as Box<dyn Error + Send>)?;

    // Convert the prices to the desired format and collect them
    let prices = country_data.iter()
        .map(|entry| entry.price * 1.0) // Convert from EUR/MWh to cents/kWh if needed
        .collect::<Vec<f64>>();

    Ok(prices)
}

pub fn create_and_update_elec_price_data(optimization_data: &mut OptimizationData, prices: Vec<f64>) {
    if let Some(time_data) = &optimization_data.time_data {
        let series = &time_data.series;

        // Pair the series with the provided prices
        let price_series = pair_timeseries_with_values(series, &prices);

        // Generating modified TimeSeriesData for original, up, and down price scenarios
        let original_ts_data = create_modified_price_series_data(&price_series, 1.0); // No modification for the original
        let up_price_ts_data = create_modified_price_series_data(&price_series, 1.1); // Increase by 10%
        let down_price_ts_data = create_modified_price_series_data(&price_series, 0.9); // Decrease by 10%

        // Assuming the country is defined somewhere within optimization_data, for example in the location field
        let country = optimization_data.location.clone().unwrap_or_default();

        // Constructing the ElectricityPriceData structure
        let electricity_price_data = input_data::ElectricityPriceData {
            country,
            price_data: original_ts_data,
            up_price_data: up_price_ts_data,
            down_price_data: down_price_ts_data,
        };

        // Updating the elec_price_data in optimization_data
        optimization_data.elec_price_data = Some(electricity_price_data);
    } else {
        eprintln!("TimeData is missing in optimization_data. Cannot update elec_price_data.");
    }
}

fn create_modified_price_series_data(
    original_series: &Vec<(String, f64)>,
    multiplier: f64,
) -> input_data::TimeSeriesData {
    let modified_series = original_series.iter()
        .map(|(timestamp, price)| (timestamp.clone(), price * multiplier))
        .collect::<Vec<(String, f64)>>();

    let time_series_s1 = input_data::TimeSeries {
        scenario: "s1".to_string(),
        series: modified_series.clone(), // Clone for s1
    };

    let time_series_s2 = input_data::TimeSeries {
        scenario: "s2".to_string(),
        series: modified_series, // Re-use for s2
    };

    input_data::TimeSeriesData {
        ts_data: vec![time_series_s1, time_series_s2],
    }
}

/* 
async fn fetch_entsoe_electricity_prices(
    start_time: String,
    end_time: String,
    country: String,
    token: String, // API token is now a parameter
) -> Result<input_data::ElectricityPriceData, Box<dyn Error + Send>> {
    println!("Fetching electricity price data for country: {}, start time: {}, end time: {}", country, start_time, end_time);

    let client = Client::new();
    let url = format!("https://transparency.entsoe.eu/api?documentType=A44&in_Domain={}&out_Domain={}&periodStart={}&periodEnd={}", 
                      country, country, start_time, end_time);

    let response = client.get(&url)
        .header("accept", "application/json")
        .header("X-API-Key", token) // Use your API key here
        .send().await
        .map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;

    let full_data: HashMap<String, serde_json::Value> = response.json().await
        .map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;

    // Assuming the API returns data in a format that needs parsing similar to your original function.
    // You will need to adjust the parsing logic based on the actual structure of the ENTSO-E API response.

    // Example placeholder for parsing logic. Replace with actual parsing based on the response structure.
    let electricity_price_data = input_data::ElectricityPriceData {
        country: country.clone(),
        // Placeholder for price data. Replace with actual data parsed from the response.
        price_data: vec![],
    };

    Ok(electricity_price_data)
}
*/


async fn fetch_elec_price_task(mut rx: mpsc::Receiver<OptimizationData>, tx: mpsc::Sender<OptimizationData>) {
    let mut interval = time::interval(tokio::time::Duration::from_secs(60));

    loop {
        interval.tick().await;

        if let Some(mut optimization_data) = rx.recv().await {
            if let (Some(country), Some(time_data)) = (&optimization_data.country, &optimization_data.time_data) {
                // Convert start_time and end_time to strings in the format "%Y-%m-%dT%H:00:00Z"
                let start_time_str = time_data.start_time.format("%Y-%m-%dT%H:00:00Z").to_string();
                let end_time_str = time_data.end_time.format("%Y-%m-%dT%H:00:00Z").to_string();

                match fetch_electricity_prices(start_time_str, end_time_str, country.clone()).await {
                    Ok(prices) => {
                        //println!("Fetched electricity prices for country '{}': {:?}", country, prices);
                        create_and_update_elec_price_data(&mut optimization_data, prices);
                        if tx.send(optimization_data).await.is_err() {
                            eprintln!("Failed to send updated optimization data");
                        }
                    },
                    Err(e) => {
                        eprintln!("Failed to fetch electricity price data for country '{}': {:?}", country, e);
                    },
                }
            } else {
                if optimization_data.country.is_none() {
                    eprintln!("Country data is missing.");
                }
                if optimization_data.time_data.is_none() {
                    eprintln!("Time data is missing.");
                }
            }
        } else {
            println!("Channel closed, stopping task.");
            break;
        }
    }
}

pub async fn fetch_model_data() -> Result<input_data::OptimizationData, errors::ModelDataError> {
    let url = "http://localhost:8000/to_hertta/model_data"; // Replace with the actual URL
    let client = reqwest::Client::new();
    
    let response = client.get(url)
        .send()
        .await
        .map_err(|e| {
            println!("Network request failed: {}", e);
            errors::ModelDataError::from(e) // Convert reqwest::Error to errors::ModelDataError
        })?;
    
    let text = response.text()
        .await
        .map_err(|e| {
            println!("Failed to get text from response: {}", e);
            errors::ModelDataError::from(e) // Convert reqwest::Error to errors::ModelDataError
        })?;
    
    let optimization_data = serde_yaml::from_str::<input_data::OptimizationData>(&text)
    .map_err(|e| {
        println!("Failed to parse YAML response: {}", e);
        errors::ModelDataError::from(e) // Convert serde_yaml::Error to errors::ModelDataError
    })?;

    Ok(optimization_data)
}


/* 
async fn run_bd_test_with_mock_data(mock_file_path: &str) -> Result<(), String> {
    let (tx, rx) = broadcast::channel(1);

    // Run the task with the mock function
    tokio::spawn(async move {
        fetch_building_data_task(tx, || mock_fetch_building_data(mock_file_path)).await;
    });

    // Check if data is received
    match rx.recv().await {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("Test failed: {:?}", e)),
    }
}

async fn mock_fetch_building_data(file_path: &str) -> Result<input_data::ModelData, Box<dyn Error + Send>> {
    match fs::read_to_string(file_path) {
        Ok(json_str) => match serde_json::from_str(&json_str) {
            Ok(building_data) => Ok(building_data),
            Err(e) => Err(Box::new(e)),
        },
        Err(e) => Err(Box::new(e)),
    }
}

#[tokio::test]
async fn test_scenario_success() {
    let mock_file_path = "tests/mocks/bd_success.json";
    if let Err(e) = run_bd_test_with_mock_data(mock_file_path).await {
        panic!("{}", e);
    }
}

#[tokio::test]
async fn test_scenario_failure() {
    let mock_file_path = "tests/mocks/bd_no_contains_reserves.json";
    if let Err(e) = run_bd_test_with_mock_data(mock_file_path).await {
        panic!("{}", e);
    }
}

*/

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio::time::{self, Duration};
    use tokio::fs::File as AsyncFile;
    use tokio::io::AsyncReadExt;
    use serde::{Deserialize, Serialize};
    use serde_yaml;
    use std::fs::File;
    use std::io::Read;
    use std::path::PathBuf;
    use tokio::runtime::Runtime;

    fn load_test_optimization_data_from_yaml(file_name: &str) -> Result<OptimizationData, Box<dyn std::error::Error>> {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("tests/mocks");
        d.push(file_name);

        let mut file = File::open(d)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let optimization_data: OptimizationData = serde_yaml::from_str(&contents)?;
        Ok(optimization_data)
    }

    #[test]
    fn test_update_interior_air_initial_state_success() {
        let mut optimization_data = load_test_optimization_data_from_yaml("update_interior_air_test_success.yaml")
            .expect("Failed to load test optimization data from YAML");

        // Accessing the model_data and then the nodes hashmap to check the original value
        let original_model_data = optimization_data.model_data.as_ref().expect("Model data is missing before update");
        let original_nodes = &original_model_data.input_data.nodes;

        if let Some(interior_air_node) = original_nodes.get("interiorair") {
            // Assert the original initial_state before the update
            assert_eq!(interior_air_node.state.initial_state, 298.15, "The original initial_state of the interiorair node should be 298.15");
        } else {
            panic!("InteriorAir node not found in nodes before update.");
        }

        // Perform the update
        let result = update_interior_air_initial_state(&mut optimization_data);
        assert!(result.is_ok());

        // Accessing the model_data again and then the nodes hashmap to check the updated value
        let updated_model_data = optimization_data.model_data.as_ref().expect("Model data is missing after update");
        let updated_nodes = &updated_model_data.input_data.nodes;

        // Now, we check the specific node for its updated initial_state
        if let Some(interior_air_node) = updated_nodes.get("interiorair") {
            // Assert the updated initial_state after the update
            assert_eq!(interior_air_node.state.initial_state, 296.0, "The initial_state of the interiorair node should be updated to 296.0");
        } else {
            panic!("InteriorAir node not found in nodes after update.");
        }
    }

    #[test]
    fn test_update_outside_node_inflow_success() {
        let mut optimization_data = load_test_optimization_data_from_yaml("update_outside_node_inflow_success.yaml")
            .expect("Failed to load test optimization data from YAML");

        // Pre-update check: Ensure "outside" node's inflow scenarios are as expected before update
        if let Some(outside_node) = optimization_data.model_data.as_ref().and_then(|md| md.input_data.nodes.get("outside")) {
            // Check that scenarios exist but have empty series
            for ts in &outside_node.inflow.ts_data {
                assert!(ts.series.is_empty(), "Scenario '{}' should initially have an empty series.", ts.scenario);
            }
        } else {
            panic!("Outside node not found in nodes before update.");
        }

        // Assuming update_outside_node_inflow is implemented to update the inflow based on weather_data
        let result = update_outside_node_inflow(&mut optimization_data);
        assert!(result.is_ok(), "Updating outside node's inflow failed.");

        // Post-update check: Verify "outside" node's inflow has been updated with non-empty series
        if let Some(outside_node) = optimization_data.model_data.as_ref().and_then(|md| md.input_data.nodes.get("outside")) {
            // Assert that the series for each scenario within inflow is now non-empty
            for ts in &outside_node.inflow.ts_data {
                assert!(!ts.series.is_empty(), "Scenario '{}' should not be empty after update.", ts.scenario);
                // Further checks can be added here to verify the content of the inflow matches expectations
            }
        } else {
            panic!("Outside node not found in nodes after update.");
        }
    }

    #[test]
    fn test_update_npe_market_prices_success() {
        let mut optimization_data = load_test_optimization_data_from_yaml("update_npe_market_prices_success.yaml")
            .expect("Failed to load test optimization data from YAML");

        // Pre-update check: Ensure "npe" market's price scenarios are as expected before update
        if let Some(npe_market) = optimization_data.model_data.as_ref().and_then(|md| md.input_data.markets.get("npe")) {
            // Check that scenarios exist but have empty series
            for ts in &npe_market.price.ts_data {
                assert!(ts.series.is_empty(), "Scenario '{}' in NPE market should initially have an empty series.", ts.scenario);
            }
        } else {
            panic!("NPE market not found in markets before update.");
        }

        // Perform the update
        let result = update_npe_market_prices(&mut optimization_data);
        assert!(result.is_ok(), "Updating NPE market's prices failed.");

        // Post-update check: Verify "npe" market's prices have been updated
        if let Some(npe_market) = optimization_data.model_data.as_ref().and_then(|md| md.input_data.markets.get("npe")) {
            // Assert that the series for each scenario within price is now non-empty
            for ts in &npe_market.price.ts_data {
                assert!(!ts.series.is_empty(), "Scenario '{}' in NPE market should not be empty after update.", ts.scenario);
                // Further checks can be added here to verify the content of the price matches expectations
            }
        } else {
            panic!("NPE market not found in markets after update.");
        }
    }

    #[test]
    fn test_create_modified_price_series_data() {
        // Setup test data
        let original_series = vec![
            ("2023-01-01T00:00:00Z".to_string(), 100.0),
            ("2023-01-01T01:00:00Z".to_string(), 200.0),
        ];
        let multiplier = 1.1; // Example multiplier

        // Call the function under test
        let result = create_modified_price_series_data(&original_series, multiplier);

        // Verify that the ts_data contains two scenarios "s1" and "s2"
        assert_eq!(result.ts_data.len(), 2, "There should be two scenarios in the result.");

        // Check that both scenarios ("s1" and "s2") have the series correctly modified
        for ts in &result.ts_data {
            assert_eq!(ts.series.len(), original_series.len(), "Each scenario should have the same number of series as the original.");

            for (i, (timestamp, price)) in ts.series.iter().enumerate() {
                assert_eq!(timestamp, &original_series[i].0, "Timestamps should match the original series.");
                let expected_price = original_series[i].1 * multiplier;
                assert!((price - expected_price).abs() < f64::EPSILON, "Prices should be correctly modified by the multiplier.");
            }
        }

        // Additionally, you can check specific scenarios if needed
        assert_eq!(result.ts_data[0].scenario, "s1", "First scenario should be 's1'.");
        assert_eq!(result.ts_data[1].scenario, "s2", "Second scenario should be 's2'.");
    }

    /* 
    #[tokio::test]
    async fn test_update_series_in_optimization_data() {
        // Mock the OptimizationData
        let mut optimization_data = load_test_optimization_data_from_yaml("time_data_generation_test.yaml")
            .expect("Failed to load optimization data from YAML");

        // Define start and end times
        let start_time = "2022-04-20T00:00:00+00:00";
        let end_time = "2022-04-20T03:00:00+00:00";

        // Perform the update
        update_series_in_optimization_data(&mut optimization_data, start_time, end_time)
            .await
            .expect("Failed to update series in optimization data");

        // Assert the series was updated as expected
        assert!(optimization_data.time_data.is_some(), "Time data should be present");
        let time_data = optimization_data.time_data.unwrap();
        assert_eq!(time_data.start_time, start_time);
        assert_eq!(time_data.end_time, end_time);
        assert_eq!(time_data.series.len(), 4); // Expecting 4 timestamps
        assert_eq!(time_data.series[0], "2022-04-20T00:00:00+00:00");
        // Further assertions can be made on the series if necessary
    }
    */


}
