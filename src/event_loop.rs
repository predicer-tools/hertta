
use tokio::time::{self, Duration};
//use serde_json::json;
use tokio::sync::{mpsc};
use crate::errors;
use crate::input_data;
use std::error::Error;
use crate::input_data::{OptimizationData};
use serde_yaml;
use tokio::sync::Mutex;
use std::sync::Arc;
use std::collections::BTreeMap;
use std::collections::HashMap;
use crate::arrow_input;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use std::sync::atomic::{AtomicBool, Ordering};
use reqwest::Client;
use tokio::time::sleep;
use zmq::Context;

#[derive(Debug)]
pub struct Person {
    pub name: String,
    pub age: u32,
}

pub async fn event_loop(mut rx: mpsc::Receiver<input_data::OptimizationData>) {

    let (tx_weather, rx_weather) = mpsc::channel::<OptimizationData>(32);
    let (tx_elec, rx_elec) = mpsc::channel::<OptimizationData>(32);
    let (tx_update, rx_update) = mpsc::channel::<OptimizationData>(32);
    let (tx_optimization, rx_optimization) = mpsc::channel::<OptimizationData>(32);
    let (_tx_final, mut _rx_final) = mpsc::channel::<OptimizationData>(32);

    // Spawn the weather data task to process and pass the data to the electricity price data task
    tokio::spawn(async move {
        println!("weather task");
        fetch_weather_data_task(rx_weather, tx_elec).await; // Output sent to fetch_elec_price_task
    });

    // Spawn the electricity price data task to process and potentially finalize the data
    tokio::spawn(async move {
        fetch_elec_price_task(rx_elec, tx_update).await; // Final output sent to update_model_data_task
    });

    tokio::spawn(async move {
        while let Some(data) = rx.recv().await {
            println!("Received: {:?}", data);
            if tx_weather.send(data).await.is_err() {
                eprintln!("Failed to send data to weather task");
            }

            // Simulate some work with a delay
            sleep(Duration::from_secs(1)).await;
        }
    });
}

pub async fn event_loop_old(mut rx_o: mpsc::Receiver<OptimizationData>) {
    let (tx_weather, rx_weather) = mpsc::channel::<OptimizationData>(32);
    let (tx_elec, rx_elec) = mpsc::channel::<OptimizationData>(32);
    let (tx_update, rx_update) = mpsc::channel::<OptimizationData>(32);
    let (tx_optimization, rx_optimization) = mpsc::channel::<OptimizationData>(32);
    let (_tx_final, mut _rx_final) = mpsc::channel::<OptimizationData>(32);

    println!("Event loop started.");

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
        update_model_data_task(rx_update, tx_optimization).await;
    });

    // Spawn the optimization task
    tokio::spawn(async move {
        optimization_task(rx_optimization, _tx_final).await;
    });

    loop {
        // Wait for a trigger with OptimizationData
        println!("Loop started.");

        tokio::select! {
            Some(optimization_data) = rx_o.recv() => {
                // Send the received OptimizationData to the next task in the pipeline
                println!("Received OptimizationData trigger.");
                if tx_weather.send(optimization_data).await.is_ok() {
                    println!("OptimizationData sent to weather data task.");
                } else {
                    eprintln!("Failed to send OptimizationData to tx_weather.");
                }
            }
            _ = time::sleep(Duration::from_secs(1)) => {
                // This is a small delay to check the stop signal periodically
            }
        }

        // Process final data
        while let Some(final_data) = _rx_final.recv().await {
            // Correctly call send_to_server_task with the received data
            println!("jee");
        }
    }
    println!("Event loop terminated.");
}


async fn send_serialized_batches(serialized_batches: &[(String, Vec<u8>)], zmq_context: &zmq::Context) -> Result<(), Box<dyn Error>> {
    let push_socket = zmq_context.socket(zmq::PUSH)?;
    push_socket.connect("tcp://127.0.0.1:5555")?;

    for (key, batch) in serialized_batches {
        println!("Sending batch: {}", key); // Debug print
        push_socket.send(batch, 0)?;
    }

    Ok(())
}

pub async fn receive_data(
    endpoint: &str,
    zmq_context: &Context,
    data_store: &Arc<Mutex<Vec<input_data::DataTable>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let receiver = zmq_context.socket(zmq::PULL)?;
    receiver.connect(endpoint)?;
    let flags = zmq::DONTWAIT;

    loop {
        match receiver.recv_bytes(flags) {
            Ok(pull_result) => {
                let reader = StreamReader::try_new(pull_result.as_slice(), None).expect("Failed to construct Arrow reader");
                for record_batch_result in reader {
                    let record_batch = Arc::new(record_batch_result.expect("Failed to read record batch"));
                    let data_table = input_data::DataTable::from_record_batch(Arc::clone(&record_batch));

                    let mut data_store = data_store.lock().await;
                    data_store.push(data_table);
                }
                break;
            },
            Err(e) => {
                if e.to_string().contains("Resource temporarily unavailable") {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                } else {
                    return Err(Box::new(e));
                }
            }
        }
    }

    Ok(())
}


async fn optimization_task(mut rx: mpsc::Receiver<OptimizationData>, tx: mpsc::Sender<OptimizationData>) {
    let zmq_context = zmq::Context::new();
    let data_store: Arc<tokio::sync::Mutex<Vec<input_data::DataTable>>> = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let endpoint = "tcp://127.0.0.1:5556"; // Assuming results are sent to this port

    while let Some(mut optimization_data) = rx.recv().await {
        // Create and serialize record batches
        if let Some(model_data) = &optimization_data.model_data {
            match arrow_input::create_and_serialize_record_batches(model_data) {
                Ok(serialized_batches) => {
                    // Send serialized batches to server
                    if let Err(e) = send_serialized_batches(&serialized_batches, &zmq_context).await {
                        eprintln!("Failed to send serialized batches: {}", e);
                        continue;
                    }

                    // Receive results from server
                    if let Err(e) = receive_data(endpoint, &zmq_context, &data_store).await {
                        eprintln!("Failed to receive results: {}", e);
                        continue;
                    }

                    // Update control_results in optimization_data
                    let data_store = data_store.lock().await;
                    optimization_data.control_results = Some(data_store.clone());
                }
                Err(e) => {
                    eprintln!("Failed to serialize model data: {}", e);
                    continue;
                }
            }
        }

        // Send the updated OptimizationData downstream
        if tx.send(optimization_data).await.is_err() {
            eprintln!("Failed to send updated OptimizationData");
        } else {
            println!("OptimizationData updated and sent successfully.");
        }
    }
}


async fn update_model_data_task(mut rx: mpsc::Receiver<OptimizationData>, tx: mpsc::Sender<OptimizationData>) {
    while let Some(mut optimization_data) = rx.recv().await {
        
        // Update temporals to model data
        if let Err(e) = update_timeseries(&mut optimization_data) {
            eprintln!("Failed to update timeseries: {}", e);
            // Optionally, continue to attempt other updates even if one fails.
        }
        /* 
        // Update indoor temperature
        if let Err(e) = update_interior_air_initial_state(&mut optimization_data) {
            eprintln!("Failed to update indoor air initial state: {}", e);
            // Optionally, continue to attempt other updates even if one fails.
        }
        */
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


fn update_npe_market_prices(optimization_data: &mut OptimizationData) -> Result<(), &'static str> {
    // Check if ElectricityPriceData is Some
    if let Some(electricity_price_data) = &optimization_data.elec_price_data {
        // Ensure model_data is available and contains the "npe" market
        let model_data = optimization_data.model_data.as_mut().ok_or("Model data is not available.")?;
        let markets = &mut model_data.markets;

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


async fn fetch_weather_data_task(mut rx: mpsc::Receiver<OptimizationData>, tx: mpsc::Sender<OptimizationData>) {
    loop {
        tokio::select! {
            Some(mut optimization_data) = rx.recv() => {
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
                    println!("fetch_weather_data_task: Time data is missing.");
                }
            },
            else => break,
        }
    }
}


pub fn update_outside_node_inflow(optimization_data: &mut OptimizationData) -> Result<(), &'static str> {
    // Check if weather_data is Some
    if let Some(weather_data) = &optimization_data.weather_data {
        // Ensure model_data is available and contains the "outside" node
        let model_data = optimization_data.model_data.as_mut().ok_or("Model data is not available.")?;
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
fn create_time_series_data_with_scenarios(paired_series: BTreeMap<String, f64>) -> input_data::TimeSeriesData {
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
    series.iter().zip(values.iter())
        .map(|(timestamp, &value)| (timestamp.clone(), value))
        .collect()
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
    let response = client.get(&url).header("accept", "").send().await.map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;

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

pub fn create_modified_price_series_data(
    original_series: &BTreeMap<String, f64>,
    multiplier: f64,
) -> input_data::TimeSeriesData {
    // Create a modified series by multiplying each price by the multiplier
    let modified_series: BTreeMap<String, f64> = original_series.iter()
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

    /* 
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
    */

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
    


}




