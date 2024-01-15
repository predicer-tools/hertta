mod predicer;
mod utilities;
mod input_data;
mod errors;

use std::env;
use hertta::julia_interface;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, AUTHORIZATION};
use serde_json::json;
use tokio::time::{self, Duration, Instant};
use warp::Filter;
use serde::{Deserialize, Serialize};
use serde_json;
use tokio::sync::mpsc;
use std::num::NonZeroUsize;
use jlrs::prelude::*;
use predicer::RunPredicer;
use jlrs::error::JlrsError;
use tokio::task::JoinHandle;
use std::fmt;
use reqwest::Client;
use tokio::sync::Mutex;
use std::sync::Arc;
use std::fs;
use std::net::SocketAddr;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::join;
use warp::reject::Reject;
use chrono::{Utc, Duration as ChronoDuration, TimeZone, Timelike};

pub fn run_event_loop() {

    let (tx_building, mut rx_building) = mpsc::channel(32);
    let (tx_weather, rx_weather) = mpsc::channel(32);
    let (tx_weather_data, mut rx_weather_data) = mpsc::channel(32);
    //let (tx_optimization, rx_optimization) = mpsc::channel(32);
    let (tx_shutdown, mut rx_shutdown) = mpsc::channel::<()>(1);

    println!("Spawning tasks...");

    let now = Utc::now();
    println!("Current UTC time: {}", now.to_string());

    let start_time = now.with_minute(0).unwrap().with_second(0).unwrap().with_nanosecond(0).unwrap();
    println!("Rounded down start time: {}", start_time.to_string());

    let end_time = start_time + ChronoDuration::hours(9);
    println!("End time (start time + 9 hours): {}", end_time.to_string());

    let start_time_str = start_time.format("%Y-%m-%d %H:%M:%S").to_string();
    let end_time_str = end_time.format("%Y-%m-%d %H:%M:%S").to_string();

    println!("Formatted start time: {}", start_time_str);
    println!("Formatted end time: {}", end_time_str);

    tokio::spawn(async {
        println!("fetch_building_data_task started.");
        fetch_building_data_task(tx_building).await;
        println!("fetch_building_data_task finished.");
    });

    
    tokio::spawn(async {
        println!("fetch_weather_data_task started.");
        fetch_weather_data_task(rx_weather, tx_weather_data).await;
        println!("fetch_weather_data_task finished.");
    });

    /* 
    tokio::spawn(async {
        println!("run_optimization_task started.");
        run_optimization_task(rx_optimization).await;
        println!("run_optimization_task finished.");
    });
*/
    let tx_shutdown_clone = tx_shutdown.clone();

    tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            let mut input = String::new();
            println!("Type 'quit' to shut down the server.");
            while let Ok(_) = std::io::stdin().read_line(&mut input) {
                if input.trim().eq_ignore_ascii_case("quit") {
                    let _ = tx_shutdown_clone.send(());
                    break;
                }
                input.clear();
            }
        })
        .await
        .expect("Failed to read user input");
    });

    let server = warp::serve(routes)
        .bind_with_graceful_shutdown(ip_address, async move {
            let mut rx_shutdown_for_server = tx_shutdown_for_server.subscribe();
            rx_shutdown_for_server.recv().await;
            println!("Server shutdown initiated.");
        });

    tokio::spawn(async move {
        server.await.expect("Server failed");
    });

    let mut task_interval = time::interval(Duration::from_secs(60));

    let listen_ip = "127.0.0.1";
    let port = "7001";
    let ip_port = format!("{}:{}", listen_ip, port);
    let ip_address: SocketAddr = ip_port.parse().unwrap();

    let mut first_iteration = true;
    loop {
        tokio::select! {
            _ = rx_shutdown.recv() => {
                println!("Shutdown signal received. Exiting loop.");
                break;
            }
            _ = async {
                println!("Main loop iteration started");
                
                if !first_iteration {
                    task_interval.tick().await;
                } else {
                    first_iteration = false;
                }
            
                if let Some(building_data) = rx_building.recv().await {
                    println!("Received building data: {:?}", building_data);
                    tx_weather.send(building_data.place.clone()).await.expect("Failed to send place to weather task");
                } else {
                    println!("No building data received.");
                }
            
                // Assuming rx_weather_data is another channel you're listening to
                if let Some(weather_data) = rx_weather_data.recv().await {
                    println!("Received weather data");
                    // Here you would handle the received weather data
                    // ...
                } else {
                    println!("No weather data received.");
                }

                println!("Main loop iteration ended");
            } => {}
        }
    }

}