use tokio::time::{self, Duration, Interval, Instant};
use serde_json::json;
use tokio::sync::{mpsc, broadcast};
use crate::errors;
use crate::input_data;
use std::error::Error;
use chrono::{Utc, Duration as ChronoDuration, TimeZone, Timelike};
use std::collections::HashMap;
use crate::input_data::{OptimizationData, BuildingData, WeatherData};

pub async fn event_loop(tx_optimization: mpsc::Sender<input_data::OptimizationData>) {
    // Broadcast channel for building data task
    let (tx_building, _) = broadcast::channel(32);
    let tx_building_for_task = tx_building.clone();

    // Channel for weather data task
    let (tx_weather, mut rx_weather) = mpsc::channel(32);

    // Spawn the building data task with the cloned sender
    tokio::spawn(async move {
        fetch_building_data_task(tx_building_for_task).await;
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

    let mut interval: Option<Interval> = None; // Initialize interval as None

    loop {
        tokio::select! {
            Ok(building_data) = rx_building.recv() => {
                println!("Received building data");
                last_building_data = Some(building_data);
            },
            Some(weather_data) = rx_weather.recv() => {
                println!("Received weather data");
                last_weather_data = Some(weather_data);
            },
            _ = if interval.is_none() {
                // First iteration, no interval
                Box::pin(async {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }) as Pin<Box<dyn Future<Output = ()>>>
            } else {
                // Subsequent iterations, 3-minute interval
                Box::pin(async {
                    interval.as_mut().unwrap().tick().await;
                }) as Pin<Box<dyn Future<Output = ()>>>
            } => {
                if let (Some(weather_data), Some(building_data)) = (&last_weather_data, &last_building_data) {
                    println!("Optimization task triggered.");
                    let optimization_data = OptimizationData {
                        weather_data: weather_data.clone(),
                        device_data: building_data.clone(),
                    };
                    if let Err(e) = tx_optimization.send(optimization_data).await {
                        eprintln!("Failed to send optimization data: {:?}", e);
                    }
                }
            },
            // Other arms of select...
        }
        
        // Set up the interval after the first iteration
        if interval.is_none() {
            interval = Some(time::interval(Duration::from_secs(180)));
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
    let mut interval = None; // Initialize interval as None

    loop {
        // Wait for the next building data
        if let Ok(building_data) = rx.recv().await {
            println!("fetch_weather_data_task: Received place: {}", building_data.place);

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
        } else {
            println!("fetch_weather_data_task: No place received.");
        }

        // If an interval is active, wait for its tick
        if let Some(ref mut interval) = interval {
            interval.tick().await;
        }
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
    let mut interval = None; // Initialize interval as None

    loop {
        println!("fetch_building_data_task: Attempting to fetch building data.");

        // Start the interval after the first iteration
        if interval.is_none() {
            interval = Some(time::interval(Duration::from_secs(60)));
        }

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

        // If an interval is active, wait for its tick
        if let Some(ref mut interval) = interval {
            interval.tick().await;
        }
    }
}
