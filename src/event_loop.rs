use tokio::time::{self, Duration, Instant};
use serde_json::json;
use tokio::sync::{mpsc, broadcast};
use crate::errors;
use crate::input_data;
use std::error::Error;
use chrono::{Utc, Duration as ChronoDuration, TimeZone, Timelike};
use std::collections::HashMap;

pub async fn event_loop() {

    // Broadcast channel for building data task
    let (tx_building, _) = broadcast::channel(32);
    // Clone the sender for the building data task
    let tx_building_for_task = tx_building.clone();

    // Channel for weather data task
    let (tx_weather, mut rx_weather) = mpsc::channel(32);
    // Channel for optimization task
    let (tx_optimization, mut rx_optimization) = mpsc::channel(32);

    // Spawn the building data task with the cloned sender
    tokio::spawn(async move {
        fetch_building_data_task(tx_building_for_task).await;
    });

    // Subscribe to the broadcast channel for weather data task
    let mut rx_building_weather = tx_building.subscribe();

    // Spawn the weather data task
    tokio::spawn(async move {
        fetch_weather_data_task(rx_building_weather, tx_weather).await;
    });

    // Subscribe to the broadcast channel for the event loop
    let mut rx_building = tx_building.subscribe();

    // Spawn the optimization task
    tokio::spawn(async move {
        run_optimization_task(rx_optimization).await;
    });

    // Variables to store the latest data
    let mut last_building_data: Option<input_data::BuildingData> = None;
    let mut last_weather_data: Option<input_data::WeatherData> = None;

    loop {
        tokio::select! {
            Ok(building_data) = rx_building.recv() => {
                last_building_data = Some(building_data);
            },
            Some(weather_data) = rx_weather.recv() => {
                last_weather_data = Some(weather_data);
            },
            // Periodic tasks in the event loop
            _ = tokio::time::sleep(Duration::from_secs(20)) => {
                // Check if both weather and building data are available
                if let (Some(weather_data), Some(building_data)) = (&last_weather_data, &last_building_data) {
                    println!("Optimization started.");
                    // Construct OptimizationData and send to optimization task
                    let optimization_data = input_data::OptimizationData {
                        weather_data: weather_data.clone(),
                        device_data: building_data.clone(),
                    };
                    if let Err(e) = tx_optimization.send(optimization_data).await {
                        eprintln!("Failed to send optimization data: {:?}", e);
                    }
                }
                println!("Event loop iteration");
            }
        }
    }
}

async fn run_optimization(optimization_data: input_data::OptimizationData) 
    -> Result<HashMap<String, Vec<(String, f64)>>, Box<dyn Error + Send>> {

    let client = reqwest::Client::new();
    let url = "http://localhost:8002/run_hertta/post";

    let response = match client.post(url)
                                .json(&optimization_data)
                                .send()
                                .await {
        Ok(res) => res,
        Err(e) => return Err(Box::new(errors::TaskError::new(&format!("Network request failed: {}", e)))),
    };

    if response.status().is_success() {
        match response.json::<HashMap<String, Vec<(String, f64)>>>().await {
            Ok(device_control_values) => Ok(device_control_values),
            Err(e) => Err(Box::new(errors::TaskError::new(&format!("Failed to parse JSON response: {}", e)))),
        }
    } else {
        Err(Box::new(errors::TaskError::new("Response returned unsuccessful status")))
    }
}

// Async function to run optimization and process results
async fn run_optimization_task(mut rx: mpsc::Receiver<input_data::OptimizationData>) {
    let client = reqwest::Client::new();

    while let Some(optimization_input) = rx.recv().await {
        match run_optimization(optimization_input).await {
            Ok(device_control_values) => {
                println!("Results received.")
                // Handle the successful optimization result
                // Send the results to another server
                /* 
                let url = "http://localhost:8000/from_hertta/optimization_results";

                match client.post(url)
                            .json(&device_control_values)
                            .send()
                            .await {
                    Ok(response) => {
                        if response.status().is_success() {
                            println!("Successfully sent optimization results");
                        } else {
                            eprintln!("Failed to send optimization results, received status: {}", response.status());
                        }
                    }
                    Err(e) => eprintln!("Failed to send optimization results: {:?}", e),
                }

                */
            },
            Err(e) => eprintln!("Optimization task failed: {:?}", e),
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
            println!("Received response from weather data server.");
            res
        },
        Err(e) => {
            println!("Network request for weather data failed: {}", e);
            return Err(Box::new(errors::TaskError::new(&format!("Network request failed: {}", e))));
        },
    };

    match response.json::<input_data::WeatherData>().await {
        Ok(weather_data) => {
            println!("Successfully parsed weather data.");
            Ok(weather_data)
        },
        Err(e) => {
            println!("Failed to parse weather data JSON response: {}", e);
            Err(Box::new(errors::TaskError::new(&format!("Failed to parse JSON response: {}", e))))
        },
    }
}

async fn fetch_weather_data_task(mut rx: broadcast::Receiver<input_data::BuildingData>, tx: mpsc::Sender<input_data::WeatherData>) {
    loop {
        let mut received_data = false;

        while let Ok(building_data) = rx.recv().await {
            received_data = true;
            println!("fetch_weather_data_task: Received place: {}", building_data.place);

            let now = Utc::now();
            let start_time = now.with_minute(0).unwrap().with_second(0).unwrap().with_nanosecond(0).unwrap();
            let end_time = start_time + ChronoDuration::hours(9);

            let start_time_str = start_time.format("%Y-%m-%d %H:%M:%S").to_string();
            let end_time_str = end_time.format("%Y-%m-%d %H:%M:%S").to_string();

            let place = building_data.place;

            println!("fetch_weather_data_task: Fetching weather data...");
            let fetched_data = fetch_weather_data(start_time_str, end_time_str, place).await;

            match fetched_data {
                Ok(weather_data) => {
                    println!("fetch_weather_data_task: Weather data fetched successfully.");
                    tx.send(weather_data).await.expect("Failed to send weather data");
                },
                Err(e) => {
                    eprintln!("fetch_weather_data_task: Failed to fetch weather data: {:?}", e);
                },
            }
        }

        if !received_data {
            println!("fetch_weather_data_task: No place received.");
        }

        // Additional code or sleep here if necessary to prevent tight loop
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

//Channel approach

// Async function to fetch building data from another server
async fn fetch_building_data_task(tx: broadcast::Sender<input_data::BuildingData>) {
    loop {
        println!("fetch_building_data_task: Attempting to fetch building data.");

        match fetch_building_data().await {
            Ok(building_data) => {
                println!("fetch_building_data_task: Building data fetched successfully.");
                if tx.send(building_data).is_err() {
                    eprintln!("fetch_building_data_task: Receiver dropped, stopping task.");
                    break;
                }
            },
            Err(e) => eprintln!("fetch_building_data_task: Failed to fetch building data: {:?}", e),
        }

        println!("fetch_building_data_task: Waiting for next interval...");
        time::sleep(Duration::from_secs(5)).await; // Adjust the interval as needed
    }
}
