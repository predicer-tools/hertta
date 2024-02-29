use tokio::time::{self, Duration, Interval};
//use serde_json::json;
use tokio::sync::{mpsc, broadcast};
use crate::errors;
use crate::input_data;
use std::error::Error;
use chrono::{Utc, Duration as ChronoDuration, Timelike, DateTime, TimeZone, FixedOffset, NaiveDateTime};
use std::collections::HashMap;
use crate::input_data::{OptimizationData, ElectricityPriceData};
use serde_json::{self, Value};
use serde_yaml;
use chrono_tz::Tz;


pub async fn event_loop(tx_optimization: mpsc::Sender<OptimizationData>) {
    let (tx_model, rx_model) = mpsc::channel::<OptimizationData>(32);
    let (tx_time, mut rx_time) = mpsc::channel::<OptimizationData>(32);
    let (tx_weather, mut rx_weather) = mpsc::channel::<OptimizationData>(32);
    let (tx_elec, mut rx_elec) = mpsc::channel::<OptimizationData>(32);
    let (tx_final, mut rx_final) = mpsc::channel::<OptimizationData>(32);


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
        fetch_elec_price_task(rx_elec, tx_final).await; // Final output sent to tx_final
    });

    let mut interval = time::interval(Duration::from_secs(10));

    loop {
        interval.tick().await; // Wait for the next interval tick

        // Check if there's data to process
        while let Some(final_data) = rx_final.recv().await {
            println!("Received final processed OptimizationData: {:?}", final_data);
            // Correctly call send_to_server_task with the received data
            if let Err(e) = send_to_server_task(final_data).await {
                eprintln!("Error while sending final optimization data to server: {}", e);
            }
        }
    }
}

async fn update_model_data_task(mut rx: mpsc::Receiver<OptimizationData>, tx: mpsc::Sender<OptimizationData>) {
    while let Some(mut optimization_data) = rx.recv().await {
        
        
        //Update indoor temperature
        update_interior_air_initial_state(&mut optimization_data);

        //Update outdoor temp flow
        update_outside_node_inflow(&mut optimization_data);

        //Update elec prices
        // - Convert the data to TimeSeriesData
        // - Update the input_data.markets.npe

        if tx.send(optimization_data).await.is_err() {
            eprintln!("Failed to send updated OptimizationData");
        }
    }
}

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

fn update_outside_node_inflow(optimization_data: &mut OptimizationData) -> Result<(), &'static str> {
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

    println!("Sending final optimization data to the server.");
    match client.post(url)
        .json(&final_data)
        .send()
        .await {
            Ok(response) => {
                if response.status().is_success() {
                    println!("Successfully sent final optimization data.");
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
            Ok(mut optimization_data) => {
                // Update the token in the optimization data
                /* 
                if let Err(e) = update_token_in_optimization_data(&mut optimization_data).await {
                    eprintln!("Failed to update token in OptimizationData: {}", e);
                    // Optionally, handle the error, such as skipping this iteration, logging, etc.
                    continue;
                }
                */

                println!("OptimizationData successfully fetched or constructed");

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


async fn create_time_data_task(mut rx: mpsc::Receiver<OptimizationData>, tx: mpsc::Sender<OptimizationData>) {
    while let Some(mut optimization_data) = rx.recv().await {
        if let Some(timezone_str) = &optimization_data.timezone {
            // Attempt to parse the timezone string
            match timezone_str.parse::<Tz>() {
                Ok(timezone) => {
                    let now = Utc::now();
                    let now_timezone = now.with_timezone(&timezone);

                    let start_time = now_timezone.format("%Y-%m-%dT%H:%M:%SZ").to_string();

                    // Ensure that temporals and hours exist and are accessible
                    let hours_to_add = optimization_data.temporals.as_ref().map_or(12, |temporals| temporals.hours); //
                    let end_time = (now_timezone + chrono::Duration::hours(hours_to_add)).format("%Y-%m-%dT%H:%M:%SZ").to_string();

                    optimization_data.time_data = Some(input_data::TimeData { start_time, end_time });
                },
                Err(_) => {
                    eprintln!("Invalid timezone '{}', defaulting to UTC.", timezone_str);
                    // Handle the error as needed
                }
            }
        } else {
            eprintln!("Timezone is missing from OptimizationData.");
        }

        if tx.send(optimization_data).await.is_err() {
            eprintln!("Failed to send updated OptimizationData");
        }
    }
}


async fn fetch_weather_data_task(mut rx: mpsc::Receiver<OptimizationData>, tx: mpsc::Sender<OptimizationData>) {
    let mut interval = time::interval(Duration::from_secs(10));

    loop {
        tokio::select! {
            Some(mut optimization_data) = rx.recv() => { // Directly match against Some or None
                if let Some(time_data) = &optimization_data.time_data {
                    let location = optimization_data.location.clone().unwrap_or_else(|| "Default Location".to_string());
                    let start_time = time_data.start_time.clone();
                    let end_time = time_data.end_time.clone();

                    match fetch_weather_data(start_time, end_time, location).await {
                        Ok(weather_data) => {
                            optimization_data.weather_data = Some(weather_data);
                            if tx.send(optimization_data).await.is_err() {
                                eprintln!("Failed to send updated optimization data");
                            }
                        },
                        Err(e) => {
                            eprintln!("fetch_weather_data_task: Failed to fetch weather data: {:?}", e);
                        },
                    }
                } else {
                    println!("fetch_weather_data_task: Time data is missing.");
                }
            },
            _ = interval.tick() => {
                println!("Interval ticked, but no new optimization data received.");
            },
            else => break, // Handle the case when all channels are closed and no more messages will be received.
        }
    }
}


async fn fetch_weather_data(start_time: String, end_time: String, place: String) 
    -> Result<input_data::WeatherData, Box<dyn Error + Send>> {
    
    println!("Fetching weather data for place: {}, start time: {}, end time: {}", place, start_time, end_time);

    let client = reqwest::Client::new();
    let url = "http://localhost:8001/get_weather_data";
    let params = [("start_time", &start_time), ("end_time", &end_time), ("place", &place)];

    let response = client.get(url)
        .query(&params)
        .send()
        .await
        .map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;

    let series: Vec<(String, f64)> = response
        .json()
        .await
        .map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;

    // Create two TimeSeries instances with the same series data but different scenarios "s1" and "s2"
    let time_series_s1 = input_data::TimeSeries {
        scenario: "s1".to_string(),
        series: series.clone(), // Use clone to use the series data for both TimeSeries
    };
    
    let time_series_s2 = input_data::TimeSeries {
        scenario: "s2".to_string(),
        series, // This moves the original series data into the second TimeSeries
    };

    // Bundle the two TimeSeries into a TimeSeriesData
    let weather_ts = input_data::TimeSeriesData {
        ts_data: vec![time_series_s1, time_series_s2],
    };

    let weather_data = input_data::WeatherData {
        place,
        weather_data: weather_ts,
    };

    println!("Weather Data: {:?}", weather_data);
    Ok(weather_data)
}

async fn fetch_electricity_prices(start_time: String, end_time: String, country: String) 
    -> Result<input_data::ElectricityPriceData, Box<dyn Error + Send>> {
    
    println!("Fetching electricity price data for country: {}, start time: {}, end time: {}", country, start_time, end_time);

    let client = reqwest::Client::new();
    let url = format!("https://dashboard.elering.ee/api/nps/price?start={}&end={}", start_time, end_time);
    
    let response = client.get(&url).header("accept", "*/*").send().await
        .map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;

    // Assuming the API response directly maps to Vec<(String, f64)>
    let price_series: Vec<(String, f64)> = response.json().await
        .map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;

    // Create two TimeSeries instances with the same series data for scenarios "s1" and "s2"
    let time_series_s1 = input_data::TimeSeries {
        scenario: "s1".to_string(),
        series: price_series.clone(), // Clone since it's used again below
    };

    let time_series_s2 = input_data::TimeSeries {
        scenario: "s2".to_string(),
        series: price_series,
    };

    // Wrap these in TimeSeriesData
    let ts_data = input_data::TimeSeriesData {
        ts_data: vec![time_series_s1, time_series_s2],
    };

    // Create ElectricityPriceData with this TimeSeriesData
    let electricity_price_data = input_data::ElectricityPriceData {
        country: country.clone(),
        price_data: ts_data,
    };

    Ok(electricity_price_data)
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


async fn fetch_elec_price_task(mut rx: mpsc::Receiver<input_data::OptimizationData>, tx: mpsc::Sender<input_data::OptimizationData>) {
    let mut interval = time::interval(Duration::from_secs(60)); // Set up the interval right away

    println!("Fetch elec prices");

    loop {
        interval.tick().await; // Wait for the next interval tick before the next fetch

        if let Some(mut optimization_data) = rx.recv().await {
            // Ensure country and time_data are both present
            if let (Some(country), Some(time_data)) = (&optimization_data.country, &optimization_data.time_data) {
                let start_time = time_data.start_time.clone();
                let end_time = time_data.end_time.clone();

                match fetch_electricity_prices(start_time, end_time, country.clone()).await {
                    Ok(price_data) => {
                        // Update the electricity price data
                        optimization_data.elec_price_data = Some(price_data.clone()); // Assuming price_data can be cloned for printing
                        
                        // Print the fetched electricity price data
                        println!("Fetched electricity price data for country '{}': {:?}", country, price_data);

                        // Send updated data
                        tx.send(optimization_data).await.expect("Failed to send updated optimization data");
                    },
                    Err(e) => {
                        eprintln!("fetch_electricity_price_task: Failed to fetch electricity price data for country '{}': {:?}", country, e);
                    },
                }
            } else {
                if optimization_data.country.is_none() {
                    eprintln!("fetch_electricity_price_task: Country data is missing.");
                }
                if optimization_data.time_data.is_none() {
                    eprintln!("fetch_electricity_price_task: Time data is missing.");
                }
            }
        } else {
            println!("fetch_electricity_price_task: Channel closed, stopping task.");
            break; // Exit the loop if the channel is closed
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
    
    let mut optimization_data = serde_yaml::from_str::<input_data::OptimizationData>(&text)
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

    #[tokio::test]
    async fn test_fetch_electricity_prices() {
        let start_time = "2020-05-31T20:59:59.999Z".to_string();
        let end_time = "2020-06-30T20:59:59.999Z".to_string();
        let country = "fi".to_string();

        match fetch_electricity_prices(start_time, end_time, country).await {
            Ok(data) => {
                println!("Success! Data: {:?}", data);
                assert!(!data.price_data.ts_data.is_empty(), "Price data should not be empty");
            },
            Err(e) => {
                panic!("Test failed with error: {:?}", e);
            },
        }
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

}
