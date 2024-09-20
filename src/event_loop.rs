use crate::arrow_input;
use crate::input_data;
use crate::input_data::OptimizationData;
use crate::settings::Settings;
use crate::utilities;
use arrow::ipc::reader::StreamReader;
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio::time::{self, Duration};
use zmq::Context;

pub async fn event_loop(settings: Settings, mut rx: mpsc::Receiver<input_data::OptimizationData>) {
    let (tx_weather, rx_weather) = mpsc::channel::<OptimizationData>(32);
    let (tx_elec, rx_elec) = mpsc::channel::<OptimizationData>(32);
    let (tx_update, rx_update) = mpsc::channel::<OptimizationData>(32);
    let (tx_batches, rx_batches) = mpsc::channel::<OptimizationData>(32);
    let (tx_optimization, rx_optimization) = mpsc::channel::<OptimizationData>(32);
    //let (_tx_final, mut _rx_final) = mpsc::channel::<OptimizationData>(32);

    // Spawn the weather data task to process and pass the data to the electricity price data task
    tokio::spawn(async move {
        fetch_weather_data_task(rx_weather, tx_elec).await; // Output sent to fetch_elec_price_task
    });

    // Spawn the electricity price data task to process and potentially finalize the data
    tokio::spawn(async move {
        fetch_elec_price_task(rx_elec, tx_update).await; // Final output sent to update_model_data_task
    });

    tokio::spawn(async move {
        // Adjust the function signature of update_model_data_task to accept tx_optimization
        update_model_data_task(rx_update, tx_batches).await;
    });

    // Spawn the data conversion task
    tokio::spawn(async move {
        data_conversion_task(rx_batches, tx_optimization).await;
    });

    // Spawn the optimization task
    tokio::spawn(async move {
        optimization_task(rx_optimization).await;
    });

    tokio::spawn(async move {
        while let Some(data) = rx.recv().await {
            if tx_weather.send(data).await.is_err() {
                eprintln!("Failed to send data to weather task");
            }

            // Simulate some work with a delay
            sleep(Duration::from_secs(1)).await;
        }
    });

    tokio::spawn(async move {
        match start_julia_local(
            &settings.julia_exec,
            &settings.predicer_runner_project,
            &settings.predicer_project,
            &settings.predicer_runner_script,
        )
        .await
        {
            Ok(status) => {
                if !status.success() {
                    eprintln!("Julia process failed with status: {:?}", status);
                }
            }
            Err(e) => {
                eprintln!("Failed to start Julia process: {:?}", e);
            }
        }
    });
}

use std::env;
use std::io;
use std::process::Command;
use std::process::ExitStatus;
use tokio::net::TcpListener;

pub async fn find_available_port() -> Result<u16, io::Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    Ok(port)
}

pub async fn start_julia_local(
    julia_exec: &String,
    predicer_runner_project: &String,
    predicer_project: &String,
    predicer_runner_script: &String,
) -> Result<ExitStatus, io::Error> {
    let push_port = find_available_port().await?;
    env::set_var("PUSH_PORT", push_port.to_string());
    println!("starting Julia from {}", julia_exec);
    println!("Predicer runner script in {}", predicer_runner_script);
    println!("using project in {}", predicer_runner_project);
    println!("using Predicer in {}", predicer_project);
    let status = Command::new(julia_exec)
        .arg(format!("--project={}", predicer_runner_project))
        .arg(predicer_runner_script)
        .arg(predicer_project)
        .status()?;
    Ok(status)
}

use std::thread;
pub async fn optimization_task(mut zmq_rx: mpsc::Receiver<OptimizationData>) {
    let zmq_context = zmq::Context::new();
    let responder = zmq_context.socket(zmq::REP).unwrap();
    assert!(responder.bind("tcp://*:5555").is_ok());

    let mut is_running = true;
    let receive_flags = 0;
    let send_flags = 0;

    // Initialize the data store
    let data_store: Arc<Mutex<Vec<input_data::DataTable>>> = Arc::new(Mutex::new(Vec::new()));

    // Server loop to handle requests
    while is_running {
        let request_result = responder.recv_string(receive_flags);
        match request_result {
            Ok(inner_result) => {
                match inner_result {
                    Ok(command) => {
                        if command == "Hello" {
                            println!("Received Hello");

                            if let Some(data) = zmq_rx.recv().await {
                                if let Some(batches) = data.input_data_batch {
                                    // Send all serialized batches
                                    for (key, buffer) in batches {
                                        println!("Sending batch: {}", key);
                                        if let Err(e) = responder.send(buffer, send_flags) {
                                            eprintln!("Failed to send batch {}: {:?}", key, e);
                                            is_running = false;
                                            break;
                                        }

                                        // Wait for acknowledgment from the client
                                        let ack_result = responder.recv_string(receive_flags);
                                        match ack_result {
                                            Ok(_ack) => {
                                                println!(
                                                    "Received acknowledgment for batch: {}",
                                                    key
                                                );
                                            }
                                            Err(e) => {
                                                eprintln!(
                                                    "Failed to receive acknowledgment: {:?}",
                                                    e
                                                );
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
                                }
                            }
                        } else if command.starts_with("Take this!") {
                            let endpoint = command
                                .strip_prefix("Take this! ")
                                .expect("cannot decipher endpoint");
                            responder
                                .send("ready to receive", send_flags)
                                .expect("failed to confirm readiness for input");
                            //receive_data(endpoint, &zmq_context);
                            if let Err(e) = receive_data(endpoint, &zmq_context, &data_store) {
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
}

pub async fn data_conversion_task(
    mut rx: mpsc::Receiver<OptimizationData>,
    tx: mpsc::Sender<OptimizationData>,
) {
    while let Some(optimization_data) = rx.recv().await {
        let optimization_data_arc = Arc::new(Mutex::new(optimization_data.clone()));

        let data_clone = Arc::clone(&optimization_data_arc);
        let check_handle: JoinHandle<()> = tokio::spawn(async move {
            utilities::check_ts_data_against_temporals(data_clone).await;
        });

        // Await the completion of the check task
        let _ = check_handle.await;

        // Lock the updated data and proceed with serialization
        let mut updated_data = optimization_data_arc.lock().await;

        // Print the updated data for debugging
        //println!("Updated OptimizationData: {:?}", *updated_data);

        if let Some(model_data) = &updated_data.model_data {
            match arrow_input::create_and_serialize_record_batches(model_data) {
                Ok(serialized_batches) => {
                    updated_data.input_data_batch = Some(serialized_batches);
                    if tx.send(updated_data.clone()).await.is_err() {
                        eprintln!("Failed to send converted OptimizationData");
                    } else {
                        println!("OptimizationData converted and sent successfully.");
                    }
                }
                Err(e) => {
                    eprintln!("Failed to serialize model data: {}", e);
                }
            }
        }
    }
}

pub fn receive_data(
    endpoint: &str,
    zmq_context: &zmq::Context,
    _data_store: &Arc<Mutex<Vec<input_data::DataTable>>>,
) -> Result<(), Box<dyn Error>> {
    let receiver = zmq_context.socket(zmq::PULL)?;
    receiver.connect(endpoint)?;
    let flags = 0;

    let pull_result = receiver.recv_bytes(flags)?;
    let reader = StreamReader::try_new(pull_result.as_slice(), None)
        .expect("Failed to construct Arrow reader");

    for record_batch_result in reader {
        let _record_batch = record_batch_result.expect("Failed to read record batch");
        //let data_table = input_data::DataTable::from_record_batch(&record_batch);

        //let mut data_store = data_store.lock().unwrap();
        //data_store.push(data_table);
    }

    Ok(())
}

async fn _send_serialized_batches(
    serialized_batches: &[(String, Vec<u8>)],
    zmq_context: &Context,
) -> Result<(), Box<dyn Error>> {
    let push_socket = zmq_context.socket(zmq::PUSH)?;
    push_socket.connect("tcp://127.0.0.1:5555")?;

    for (key, batch) in serialized_batches {
        println!("Sending batch: {}", key);
        push_socket.send(batch, 0)?;
    }

    Ok(())
}

async fn update_model_data_task(
    mut rx: mpsc::Receiver<OptimizationData>,
    tx: mpsc::Sender<OptimizationData>,
) {
    while let Some(mut optimization_data) = rx.recv().await {
        // Conditionally update temporals to model data
        if optimization_data.fetch_time_data {
            if let Err(e) = update_timeseries(&mut optimization_data) {
                eprintln!("Failed to update timeseries: {}", e);
                // Optionally, continue to attempt other updates even if one fails.
            }
        }

        // Conditionally update outdoor temp flow
        if optimization_data.fetch_weather_data {
            if let Err(e) = update_outside_node_inflow(&mut optimization_data) {
                eprintln!("Failed to update outside node inflow: {}", e);
                // Optionally, continue to attempt other updates even if one fails.
            }
        }

        // Conditionally update elec prices
        if optimization_data.fetch_elec_data {
            match update_npe_market_prices(&mut optimization_data) {
                Ok(_) => (),
                Err(e) => eprintln!("Failed to update NPE market prices: {}", e),
            }
        }

        // Send the updated OptimizationData to the next task
        if tx.send(optimization_data).await.is_err() {
            eprintln!("Failed to send updated OptimizationData to model data update");
        } else {
            println!("OptimizationData updated and sent successfully.");
        }
    }
}

fn update_timeseries(optimization_data: &mut OptimizationData) -> Result<(), &'static str> {
    // Check if time_data is available
    let time_data = optimization_data
        .time_data
        .as_ref()
        .ok_or("Time data is not available.")?;

    // Check if model_data is available and update timeseries
    if let Some(model_data) = &mut optimization_data.model_data {
        model_data.temporals.t = time_data.series.clone();
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

/*
pub fn update_interior_air_initial_state(optimization_data: &mut OptimizationData) -> Result<(), &'static str> {
    // Check if sensor_data is Some
    if let Some(sensor_data) = &optimization_data.sensor_data {
        // Ensure model_data is available
        let model_data = optimization_data.model_data.as_mut().ok_or("Model data is not available.")?;
        let nodes = &mut model_data.nodes;

        // Find the sensor data for "interiorair"
        if let Some(interior_air_sensor) = sensor_data.iter().find(|s| s.sensor_name == "interiorair") {
            // Update the interiorair initial_state with the sensor temp
            if let Some(node) = nodes.get_mut("interiorair") {
                if let Some(ref mut state) = node.state {
                    state.initial_state = interior_air_sensor.temp; // Correctly update the initial_state
                    return Ok(());
                } else {
                    return Err("InteriorAir node state is not available.");
                }
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
*/

pub fn update_npe_market_prices(
    optimization_data: &mut input_data::OptimizationData,
) -> Result<(), &'static str> {
    // Check if ElectricityPriceData is Some
    if let Some(electricity_price_data) = &optimization_data.elec_price_data {
        // Ensure model_data is available and contains the "npe" market
        let model_data = optimization_data
            .model_data
            .as_mut()
            .ok_or("Model data is not available.")?;
        let markets = &mut model_data.markets;

        // Find the market named "npe"
        if let Some(npe_market) = markets.get_mut("npe") {
            // Check and update the npe market's price with the electricity_price_data
            if let Some(price_data) = &electricity_price_data.price_data {
                npe_market.price.ts_data = price_data.ts_data.clone();
            }
            if let Some(up_price_data) = &electricity_price_data.up_price_data {
                npe_market.up_price.ts_data = up_price_data.ts_data.clone();
            }
            if let Some(down_price_data) = &electricity_price_data.down_price_data {
                npe_market.down_price.ts_data = down_price_data.ts_data.clone();
            }
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

/*
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
    */

async fn fetch_weather_data_task(
    mut rx: mpsc::Receiver<OptimizationData>,
    tx: mpsc::Sender<OptimizationData>,
) {
    loop {
        tokio::select! {
            Some(mut optimization_data) = rx.recv() => {
                if !optimization_data.fetch_weather_data {
                    // If fetch_weather_data is false, directly forward the data to the next task
                    if tx.send(optimization_data).await.is_err() {
                        eprintln!("Failed to send optimization data from weather task without processing");
                    }
                    continue;
                }

                if let Some(time_data) = &optimization_data.time_data {
                    let location = optimization_data.location.clone().unwrap_or_default();

                    // Format start_time and end_time to the desired string representation
                    let start_time_formatted = time_data.start_time.format("%Y-%m-%dT%H:00:00Z").to_string();
                    let end_time_formatted = time_data.end_time.format("%Y-%m-%dT%H:00:00Z").to_string();

                    // Fetch weather data using the formatted times
                    match fetch_weather_data(start_time_formatted, end_time_formatted, location).await {
                        Ok(weather_values) => {
                            // Create and update WeatherData
                            create_and_update_weather_data(&mut optimization_data, &weather_values);
                        },
                        Err(e) => eprintln!("fetch_weather_data_task: Failed to fetch weather data: {}", e),
                    }

                    // Attempt to send the updated optimization data back
                    if tx.send(optimization_data).await.is_err() {
                        eprintln!("Failed to send updated optimization data from weather task");
                    }
                } else {
                    eprintln!("fetch_weather_data_task: Time data is missing.");
                }
            },
            else => break,
        }
    }
}

pub fn update_outside_node_inflow(
    optimization_data: &mut OptimizationData,
) -> Result<(), &'static str> {
    // Check if weather_data is Some
    if let Some(weather_data) = &optimization_data.weather_data {
        // Ensure model_data is available and contains the "outside" node
        let model_data = optimization_data
            .model_data
            .as_mut()
            .ok_or("Model data is not available.")?;
        let nodes = &mut model_data.nodes;

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
fn create_time_series_data_with_scenarios(
    paired_series: BTreeMap<String, f64>,
) -> input_data::TimeSeriesData {
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

pub fn pair_timeseries_with_values(series: &[String], values: &[f64]) -> BTreeMap<String, f64> {
    series
        .iter()
        .zip(values.iter())
        .map(|(timestamp, &value)| (timestamp.clone(), value))
        .collect()
}

async fn fetch_weather_data(
    start_time: String,
    end_time: String,
    place: String,
) -> Result<Vec<f64>, Box<dyn std::error::Error + Send + Sync>> {
    //println!("Fetching weather data for place: {}, start time: {}, end time: {}", place, start_time, end_time);

    let client = reqwest::Client::new();
    let url = format!(
        "http://localhost:8001/get_weather_data?start_time={}&end_time={}&place={}",
        start_time, end_time, place
    );

    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    //println!("Response status: {}", response.status());

    let response_body = response
        .text()
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    //println!("Raw response body: {}", response_body);

    match serde_json::from_str::<input_data::WeatherDataResponse>(&response_body) {
        Ok(fmi_data) => {
            //println!("Deserialized Weather Data: {:?}", fmi_data.weather_values);
            // Convert temperatures from Celsius to Kelvin
            let temperatures_in_kelvin: Vec<f64> = fmi_data
                .weather_values
                .iter()
                .map(|&celsius| celsius + 273.15)
                .collect();
            //println!("Temperatures in Kelvin: {:?}", temperatures_in_kelvin);
            Ok(temperatures_in_kelvin)
        }
        Err(e) => {
            eprintln!("Error deserializing response body: {}", e);
            Err(Box::new(e))
        }
    }
}

async fn fetch_electricity_prices(
    start_time: String,
    end_time: String,
    country: String,
) -> Result<BTreeMap<String, f64>, Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::new();
    let url = format!(
        "https://dashboard.elering.ee/api/nps/price?start={}&end={}",
        start_time, end_time
    );

    // Fetch the response
    let response = client
        .get(&url)
        .header("accept", "application/json")
        .send()
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    let response_text = response
        .text()
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    // Deserialize the response text into a HashMap
    let api_response: HashMap<String, Vec<HashMap<String, f64>>> =
        serde_json::from_str(&response_text)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    // Extract only the prices for the specified country
    let country_data = api_response.get(&country).ok_or_else(|| {
        Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Country data not found",
        )) as Box<dyn std::error::Error + Send + Sync>
    })?;

    // Convert the prices to the desired format and collect them in BTreeMap
    let mut series = BTreeMap::new();
    for entry in country_data {
        if let (Some(&timestamp), Some(&price)) = (entry.get("timestamp"), entry.get("price")) {
            if let Some(datetime) = DateTime::<Utc>::from_timestamp(timestamp as i64, 0) {
                let timestamp_str = datetime.format("%Y-%m-%dT%H:00:00Z").to_string();
                series.insert(timestamp_str, price * 1.0); // Convert from EUR/MWh to cents/kWh if needed
            } else {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid timestamp",
                )));
            }
        }
    }

    Ok(series)
}

pub fn create_and_update_elec_price_data(
    optimization_data: &mut OptimizationData,
    price_series: &BTreeMap<String, f64>,
) {
    if let Some(_time_data) = &optimization_data.time_data {
        // Generating modified TimeSeriesData for original, up, and down price scenarios
        let original_ts_data = Some(create_modified_price_series_data(&price_series, 1.0)); // No modification for the original
        let up_price_ts_data = Some(create_modified_price_series_data(&price_series, 1.1)); // Increase by 10%
        let down_price_ts_data = Some(create_modified_price_series_data(&price_series, 0.9)); // Decrease by 10%

        // Assuming the country is defined somewhere within optimization_data, for example in the location field
        let country = optimization_data.location.clone().unwrap_or_default();

        // Constructing the ElectricityPriceData structure
        let electricity_price_data = input_data::ElectricityPriceData {
            api_source: Some("".to_string()),
            api_key: Some("".to_string()),
            country: Some(country),
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

pub fn create_modified_price_series_data(
    original_series: &BTreeMap<String, f64>,
    multiplier: f64,
) -> input_data::TimeSeriesData {
    // Create a modified series by multiplying each price by the multiplier
    let modified_series: BTreeMap<String, f64> = original_series
        .iter()
        .map(|(timestamp, price)| (timestamp.clone(), price * multiplier))
        .collect();

    // Create TimeSeries instances for scenarios "s1" and "s2" with the modified series
    let time_series_s1 = input_data::TimeSeries {
        scenario: "s1".to_string(),
        series: modified_series.clone(), // Clone for s1
    };

    let time_series_s2 = input_data::TimeSeries {
        scenario: "s2".to_string(),
        series: modified_series, // Re-use for s2
    };

    // Bundle the two TimeSeries into TimeSeriesData
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

async fn fetch_elec_price_task(
    mut rx: mpsc::Receiver<OptimizationData>,
    tx: mpsc::Sender<OptimizationData>,
) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));

    loop {
        interval.tick().await;

        if let Some(mut optimization_data) = rx.recv().await {
            if !optimization_data.fetch_elec_data {
                // If fetch_elec_data is false, directly forward the data to the next task
                if tx.send(optimization_data).await.is_err() {
                    eprintln!("Failed to send optimization data from elec task without processing");
                }
                continue;
            }

            if let (Some(country), Some(time_data)) =
                (&optimization_data.country, &optimization_data.time_data)
            {
                // Convert start_time and end_time to strings in the format "%Y-%m-%dT%H:00:00Z"
                let start_time_str = time_data
                    .start_time
                    .format("%Y-%m-%dT%H:00:00Z")
                    .to_string();
                let end_time_str = time_data.end_time.format("%Y-%m-%dT%H:00:00Z").to_string();

                match fetch_electricity_prices(start_time_str, end_time_str, country.clone()).await
                {
                    Ok(prices) => {
                        //println!("Fetched electricity prices for country '{}': {:?}", country, prices);
                        create_and_update_elec_price_data(&mut optimization_data, &prices);
                        if tx.send(optimization_data).await.is_err() {
                            eprintln!(
                                "Failed to send updated optimization data in electricity prices"
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "Failed to fetch electricity price data for country '{}': {:?}",
                            country, e
                        );
                    }
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
