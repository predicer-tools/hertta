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
    
    
        // Assuming tasks complete quickly and you can await the final data directly
        if let Some(final_data) = rx_final.recv().await {
            println!("Received final processed OptimizationData: {:?}", final_data);
            // Process the received final data
        } else {
            eprintln!("Did not receive final processed data within the expected time frame.");
        }
    
        // The interval waits for the next tick, which can help to synchronize the cycle.
    }
}

async fn fetch_model_data_task(tx: mpsc::Sender<OptimizationData>) {
    let mut interval = time::interval(Duration::from_secs(10)); // Adjusted to fetch data every 10 seconds for this example

    loop {
        interval.tick().await; // Wait for the next interval tick before fetching new data

        match fetch_model_data().await {
            Ok(optimization_data) => {
                // Print the fetched OptimizationData
                println!("OptimizationData successfully fetched or constructed");
                
                // Send the fetched or constructed OptimizationData downstream
                if tx.send(optimization_data).await.is_err() {
                    eprintln!("Failed to send OptimizationData.");
                }
            },
            Err(e) => eprintln!("Failed to fetch or construct OptimizationData: {}", e),
        }
    }
}


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

    let response = match client.get(url).query(&params).send().await {
        Ok(res) => {
            res
        },
        Err(e) => {
            println!("Network request for weather data failed: {}", e);
            return Err(Box::new(errors::TaskError::new(&format!("Network request failed: {}", e))));
        },
    };

    // Get the response text as a String
    let response_text = response.text().await.unwrap_or_else(|_| "".to_string());

    // Parse JSON from the response text
    let mut weather_data: input_data::WeatherData = serde_json::from_str(&response_text)
    .map_err(|e| {
        println!("Failed to parse weather data JSON response: {}", e);
        Box::new(errors::TaskError::new(&format!("Failed to parse JSON response: {}", e))) as Box<dyn Error + Send>
    })?;

    // Parse start_time and end_time, and convert to DateTime<FixedOffset>
    let start_timestamp = match NaiveDateTime::parse_from_str(&start_time, "%Y-%m-%d %H:%M:%S") {
        Ok(ndt) => DateTime::<FixedOffset>::from_utc(ndt, FixedOffset::east(0)),
        Err(_) => Utc::now().with_timezone(&FixedOffset::east(0)), // Fallback to current time if parsing fails
    };

    let end_timestamp = match NaiveDateTime::parse_from_str(&end_time, "%Y-%m-%d %H:%M:%S") {
        Ok(ndt) => DateTime::<FixedOffset>::from_utc(ndt, FixedOffset::east(0)),
        Err(_) => Utc::now().with_timezone(&FixedOffset::east(0)), // Fallback to current time if parsing fails
    };

    let mut current_timestamp = start_timestamp;

    // Loop through the weather_data and replace the timestamp
    for time_point in &mut weather_data.weather_data {
        // Convert temperature
        time_point.value += 273.15;

        // Use generated timestamp, ensure it's formatted correctly
        let formatted_timestamp = current_timestamp.format("%Y-%m-%dT%H:%M:%S%z").to_string();
        time_point.timestamp = formatted_timestamp;

        // Increment current_timestamp by 1 hour
        current_timestamp = current_timestamp + ChronoDuration::hours(1);
        
        // Break the loop if the current_timestamp exceeds the end_timestamp
        if current_timestamp > end_timestamp {
            break;
        }
    }   

    // Print the updated weather data for debugging
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
            let processed_price_data: Vec<input_data::TimePoint> = raw_price_data
                .iter()
                .enumerate()
                .map(|(i, price_point)| {
                    let new_timestamp = start_datetime + ChronoDuration::hours(i as i64);
                    input_data::TimePoint {
                        timestamp: new_timestamp.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                        value: price_point.price,
                    }
                })
                .collect();

            let electricity_price_data = input_data::ElectricityPriceData {
                country: country.clone(),
                price_data: processed_price_data,
            };

            Ok(electricity_price_data)
        } else {
            Err(Box::new(errors::TaskError::new(&format!("No data found for country: {}", country))))
        }
    } else {
        Err(Box::new(errors::TaskError::new("No data field found in response")))
    }
}


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
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;
    use serde::{Deserialize, Serialize};
    use serde_yaml;

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
