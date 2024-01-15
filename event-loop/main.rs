use tokio::time::{self, Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::join;
use reqwest::Error;

#[tokio::main]
async fn main() {
    // Determine the next hour:00 start time from the current moment
    let now = Instant::now();
    let secs_to_next_hour = 3600 - (now.elapsed().as_secs() % 3600);
    let start_time = now + Duration::from_secs(secs_to_next_hour);

    // Set the interval to 10 minutes
    let mut interval = time::interval_at(start_time, Duration::from_secs(600));

    loop {
        interval.tick().await;

        // Run WeatherData and DeviceData tasks concurrently and await their results
        let (weather_result, device_result) = join!(
            fetch_weather_data(),
            fetch_device_data()
        );

        // Check if both WeatherData and DeviceData tasks were successful
        if let (Ok(weather_data), Ok(device_data)) = (weather_result, device_result) {
            // Run Optimization task directly with the fetched data
            match run_optimization(weather_data, device_data).await {
                Ok(opt_result) => {
                    // Process optimization result
                },
                Err(e) => eprintln!("Optimization task failed: {:?}", e),
            }
        } else {
            // Handle errors if WeatherData or DeviceData tasks failed
            if let Err(e) = weather_result {
                eprintln!("WeatherData task failed: {:?}", e);
            }
            if let Err(e) = device_result {
                eprintln!("DeviceData task failed: {:?}", e);
            }
        }

        // Optional: Add a break condition or signal handling if you want to stop the loop under certain conditions
    }
}

// Define your tasks as async functions or traits
async fn fetch_weather_data() -> Result<WeatherData, Box<dyn Error>> {
    // Implementation to fetch weather data
}

#[derive(Debug)]
struct DeviceData {
    // Define the fields based on the data structure you expect from the server
}

async fn fetch_device_data() -> Result<DeviceData, Box<dyn Error>> {
    // URL of the server to fetch data from
    let url = "http://example.com/api/device_data";

    // Send a GET request to the server
    let response = reqwest::get(url).await?;

    // Check if the response status is success
    if response.status().is_success() {
        // Parse the JSON response into your DeviceData structure
        let device_data = response.json::<DeviceData>().await?;
        Ok(device_data)
    } else {
        // If the response status is not success, return an error
        Err(Box::new(reqwest::Error::from(response.status())))
    }
}

async fn run_optimization(weather_data: WeatherData, device_data: DeviceData) -> Result<OptimizationResult, Box<dyn Error>> {
    // Implementation to run optimization using the input data
}
