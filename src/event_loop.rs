use tokio::time::{self, Duration, Interval};
//use serde_json::json;
use tokio::sync::{mpsc, broadcast};
use crate::errors;
use crate::input_data;
use std::error::Error;
use chrono::{Utc, Duration as ChronoDuration, Timelike, DateTime, TimeZone};
use std::collections::HashMap;
use crate::input_data::{OptimizationData, ElectricityPriceData, ElectricityPricePoint};
use serde_json::{self, Value};

pub async fn event_loop(tx_optimization: mpsc::Sender<input_data::OptimizationData>) {
    // Broadcast channel for building data task
    let (tx_building, _) = broadcast::channel(32);
    let tx_building_for_task = tx_building.clone();

    // Channel for weather data task
    let (tx_weather, mut rx_weather) = mpsc::channel(32);

    // Channel for electricity price data task
    let (tx_elec_price, mut rx_elec_price) = mpsc::channel(32);

    let country = "fi".to_string(); // Example country

    // Spawn the building data task with the cloned sender
    tokio::spawn(async move {
        fetch_building_data_task(tx_building_for_task).await;
    });

    // Spawn the electricity price data task
    tokio::spawn(async move {
        fetch_electricity_price_task(tx_elec_price, country).await;
    });

    // Subscribe to the broadcast channel for weather data task
    let rx_building_weather = tx_building.subscribe();

    // Spawn the weather data task
    tokio::spawn(async move {
        fetch_weather_data_task(rx_building_weather, tx_weather).await;
    });

    // Subscribe to the broadcast channel for the event loop
    let mut rx_building = tx_building.subscribe();

    let mut last_building_data: Option<input_data::BuildingData> = None;
    let mut last_weather_data: Option<input_data::WeatherData> = None;
    let mut last_elec_price_data: Option<input_data::ElectricityPriceData> = None;
    let mut initial_data_received = false; 

    let mut interval: Option<Interval> = None; // Initialize interval as None

    loop {
        tokio::select! {
            Ok(building_data) = rx_building.recv() => {
                println!("Received predicer input data");
                last_building_data = Some(building_data);
                if last_weather_data.is_some() && last_elec_price_data.is_some() {
                    initial_data_received = true;
                }
            },
            Some(weather_data) = rx_weather.recv() => {
                println!("Received weather data");
                last_weather_data = Some(weather_data);
                if last_building_data.is_some() && last_elec_price_data.is_some() {
                    initial_data_received = true;
                }
            },
            Some(elec_price_data) = rx_elec_price.recv() => {
                println!("Received electricity price data");
                last_elec_price_data = Some(elec_price_data);
                if last_building_data.is_some() && last_weather_data.is_some() {
                    initial_data_received = true;
                }
            },
            _ = if initial_data_received {
                // Once initial data is received, use the interval
                Box::pin(async {
                    interval.as_mut().unwrap().tick().await;
                }) as Pin<Box<dyn Future<Output = ()>>>
            } else {
                // Before initial data, just sleep
                Box::pin(async {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }) as Pin<Box<dyn Future<Output = ()>>>
            } => {
                if let (Some(weather_data), Some(building_data), Some(elec_price_data)) = (&last_weather_data, &last_building_data, &last_elec_price_data) {
                    println!("Optimization task triggered.");
                    let optimization_data = OptimizationData {
                        weather_data: weather_data.clone(),
                        device_data: building_data.clone(),
                        elec_price_data: elec_price_data.clone(),
                    };
                    if let Err(e) = tx_optimization.send(optimization_data).await {
                        eprintln!("Failed to send optimization data: {:?}", e);
                    }
                }
            },
        }
    
        // Set up the interval after the first iteration
        if interval.is_none() && initial_data_received {
            interval = Some(time::interval(Duration::from_secs(180)));
        }
    }
}

async fn _update_input_data_task(mut rx: broadcast::Receiver<input_data::OptimizationData>, tx: mpsc::Sender<input_data::InputData>) {
    
    if let Ok(optimization_data) = rx.recv().await {

        //Update outside weather data
        let mut input_data = optimization_data.device_data.input_data.clone();
        let weather_data = input_data::convert_to_time_series(optimization_data.weather_data.weather_data.clone());
        input_data::update_outside_inflow(&mut input_data, weather_data);

        //Update elec prices


    }
}

async fn fetch_weather_data_task(mut rx: broadcast::Receiver<input_data::BuildingData>, tx: mpsc::Sender<input_data::WeatherData>) {
    let mut interval = None; // Initialize interval as None

    loop {
        // Wait for the next building data
        if let Ok(building_data) = rx.recv().await {

            // Start the interval after receiving the first building data
            if interval.is_none() {
                interval = Some(time::interval(Duration::from_secs(60)));
            }

            let now = Utc::now();
            let start_time = now.with_minute(0).unwrap().with_second(0).unwrap().with_nanosecond(0).unwrap();
            let end_time = start_time + ChronoDuration::hours(9);

            let start_time_str = start_time.format("%Y-%m-%d %H:%M:%S").to_string();
            let end_time_str = end_time.format("%Y-%m-%d %H:%M:%S").to_string();

            let place = building_data.place;

            let fetched_data = fetch_weather_data(start_time_str, end_time_str, place).await;

            match fetched_data {
                Ok(weather_data) => {
                    tx.send(weather_data).await.expect("Failed to send weather data");
                },
                Err(e) => {
                    eprintln!("fetch_weather_data_task: Failed to fetch weather data: {:?}", e);
                },
            }
        } else {
            println!("fetch_weather_data_task: No place received.");
        }

        // If an interval is active, wait for its tick
        if let Some(ref mut interval) = interval {
            interval.tick().await;
        }
    }
}

async fn fetch_weather_data(start_time: String, end_time: String, place: String) 
    -> Result<input_data::WeatherData, Box<dyn Error + Send>> {
    
    println!("Fetching weather data for place: {}, start time: {}, end time: {}", place, start_time, end_time);

    let client = reqwest::Client::new();
    let url = "http://localhost:8001/get_weather_data";
    let params = [("start_time", &start_time), ("end_time", &end_time), ("place", &place)];

    let response = match client.get(url).query(&params).send().await {
        Ok(res) => {
            res
        },
        Err(e) => {
            println!("Network request for weather data failed: {}", e);
            return Err(Box::new(errors::TaskError::new(&format!("Network request failed: {}", e))));
        },
    };

    match response.json::<input_data::WeatherData>().await {
        Ok(weather_data) => {
            Ok(weather_data)
        },
        Err(e) => {
            println!("Failed to parse weather data JSON response: {}", e);
            Err(Box::new(errors::TaskError::new(&format!("Failed to parse JSON response: {}", e))))
        },
    }
}

async fn fetch_electricity_prices(start_time: String, end_time: String, country: String) 
    -> Result<input_data::ElectricityPriceData, Box<dyn Error + Send>> {
    
    println!("Fetching electricity price data for country: {}, start time: {}, end time: {}", country, start_time, end_time);

    let client = reqwest::Client::new();
    let url = format!("https://dashboard.elering.ee/api/nps/price?start={}&end={}", start_time, end_time);
    
    let response = client.get(&url).header("accept", "*/*").send().await
        .map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;

    let full_data: HashMap<String, Value> = response.json().await
        .map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;

    if let Some(data) = full_data.get("data") {
        if let Some(country_data) = data.get(&country.to_lowercase()) {
            // Deserialize JSON into Vec<PricePoint>
            let raw_price_data: Vec<input_data::PricePoint> = serde_json::from_value(country_data.clone())
                .map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;

            // Parse the start_time as DateTime
            let start_datetime = DateTime::parse_from_rfc3339(&start_time)
                .map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;

            // Process and convert timestamps in price_data
            let processed_price_data: Vec<input_data::ElectricityPricePoint> = raw_price_data
                .iter()
                .enumerate()
                .map(|(i, price_point)| {
                    let new_timestamp = start_datetime + ChronoDuration::hours(i as i64);
                    input_data::ElectricityPricePoint {
                        timestamp: new_timestamp.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                        price: price_point.price,
                    }
                })
                .collect();

            let electricity_price_data = input_data::ElectricityPriceData {
                country: country.clone(),
                price_data: processed_price_data,
            };

            // Print the result before returning
            println!("Fetched Electricity Price Data: {:?}", electricity_price_data);

            Ok(electricity_price_data)
        } else {
            Err(Box::new(errors::TaskError::new(&format!("No data found for country: {}", country))))
        }
    } else {
        Err(Box::new(errors::TaskError::new("No data field found in response")))
    }
}


async fn fetch_electricity_price_task(
    tx: mpsc::Sender<input_data::ElectricityPriceData>, 
    country: String,
) {
    let mut interval = time::interval(Duration::from_secs(60)); // Fetch data every 60 seconds

    let now = Utc::now();
    let start_time = now.with_minute(0).unwrap().with_second(0).unwrap().with_nanosecond(0).unwrap();
    let end_time = start_time + ChronoDuration::hours(9);

    let start_time_str = start_time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let end_time_str = end_time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

    loop {
        let fetched_data = fetch_electricity_prices(start_time_str.to_string(), end_time_str.to_string(), country.clone()).await;

        match fetched_data {
            Ok(price_data) => {
                tx.send(price_data).await.expect("Failed to send electricity price data");
            },
            Err(e) => {
                eprintln!("fetch_electricity_price_task: Failed to fetch electricity price data: {:?}", e);
            },
        }

        interval.tick().await; // Wait for the next interval tick before next fetch
    }
}


pub async fn fetch_building_data() -> Result<input_data::BuildingData, Box<dyn Error + Send>> {

    let url = "http://localhost:8000/to_hertta/building_data"; // Replace with the actual URL
    let client = reqwest::Client::new();
    let response = match client.get(url).send().await {
        Ok(res) => {
            res
        },
        Err(e) => {
            println!("Network request failed: {}", e);
            return Err(Box::new(errors::TaskError::new(&format!("Network request failed: {}", e))));
        },
    };

    match response.json::<input_data::BuildingData>().await {
        Ok(building_data) => {
            Ok(building_data)
        },
        Err(e) => {
            println!("Failed to parse JSON response: {}", e);
            Err(Box::new(errors::TaskError::new(&format!("Failed to parse JSON response: {}", e))))
        },
    }
}

// Async function to fetch building data from another server
async fn fetch_building_data_task(tx: broadcast::Sender<input_data::BuildingData>) {
    let mut interval = None; // Initialize interval as None

    loop {

        // Start the interval after the first iteration
        if interval.is_none() {
            interval = Some(time::interval(Duration::from_secs(60)));
        }

        match fetch_building_data().await {
            Ok(building_data) => {
                if tx.send(building_data).is_err() {
                    eprintln!("fetch_building_data_task: Receiver dropped, stopping task.");
                    break;
                }
            },
            Err(e) => eprintln!("fetch_building_data_task: Failed to fetch building data: {:?}", e),
        }

        // If an interval is active, wait for its tick
        if let Some(ref mut interval) = interval {
            interval.tick().await;
        }
    }
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

async fn mock_fetch_building_data(file_path: &str) -> Result<input_data::BuildingData, Box<dyn Error + Send>> {
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

    #[tokio::test]
    async fn test_fetch_electricity_prices() {
        let start_time = "2020-05-31T20:59:59.999Z".to_string();
        let end_time = "2020-06-30T20:59:59.999Z".to_string();
        let country = "fi".to_string();

        match fetch_electricity_prices(start_time, end_time, country).await {
            Ok(data) => {
                println!("Success! Data: {:?}", data);
                assert!(!data.price_data.is_empty(), "Price data should not be empty");
            },
            Err(e) => {
                panic!("Test failed with error: {:?}", e);
            },
        }
    }

    #[tokio::test]
    async fn test_fetch_electricity_price_task() {
        // Set up a mock sender channel
        let (tx, mut rx) = mpsc::channel(1);
        
        // Mock country input
        let country = String::from("fi");

        // Start the task in a new task
        tokio::spawn(async move {
            fetch_electricity_price_task(tx, country).await;
        });

        // Mock wait time or trigger to simulate the passage of time or other conditions
        time::sleep(Duration::from_secs(1)).await;

        // Test the expected behavior
        match rx.recv().await {
            Some(price_data) => {
                // Assert conditions or inspect the `price_data` received
                assert!(!price_data.price_data.is_empty(), "Price data should not be empty");
                // ... more assertions as needed
            },
            None => panic!("No data received"),
        }
    }
}
