use tokio::time::{self, Duration, Instant};
use serde_json::json;
use tokio::sync::mpsc;
use crate::errors;
use crate::input_data;
use std::error::Error;
use chrono::{Utc, Duration as ChronoDuration, TimeZone, Timelike};

pub async fn event_loop() {
    // Channel for building data task
    let (tx_building, mut rx_building) = mpsc::channel(32);
    // Channel for weather data task - to send BuildingData and receive WeatherData
    let (tx_weather_data, mut rx_weather_data) = mpsc::channel(32);

    // Spawn the building data task
    tokio::spawn(async move {
        fetch_building_data_task(tx_building).await;
    });

    // Spawn the weather data task
    tokio::spawn(async move {
        fetch_weather_data_task(rx_building, tx_weather_data).await;
    });

    loop {
        tokio::select! {
            // Handle incoming weather data
            Some(weather_data) = rx_weather_data.recv() => {
                // Process the weather data
                println!("Received weather data: {:?}", weather_data);
            },
            // Periodic tasks in the event loop
            _ = tokio::time::sleep(Duration::from_secs(60)) => {
                println!("Event loop iteration");
            }
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

async fn fetch_weather_data_task(mut rx: mpsc::Receiver<input_data::BuildingData>, tx: mpsc::Sender<input_data::WeatherData>) {
    loop {
        if let Some(building_data) = rx.recv().await {
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
        } else {
            println!("fetch_weather_data_task: No place received.");
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
async fn fetch_building_data_task(tx: mpsc::Sender<input_data::BuildingData>) {
    loop {
        println!("fetch_building_data_task: Attempting to fetch building data.");

        match fetch_building_data().await {
            Ok(building_data) => {
                println!("fetch_building_data_task: Building data fetched successfully.");
                if tx.send(building_data).await.is_err() {
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
