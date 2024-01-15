use std::process::Command;
use std::str;
use serde_json::{Value, from_str};

pub fn get_weather_data(start_time: String, end_time: String, place: String) -> Vec<(String, f64)> {
    let python_script_path = "src/fmi_opendata.py";

    let output = Command::new("python")
        .arg(python_script_path)
        .arg(&start_time)
        .arg(&end_time)
        .arg(&place)
        .output()
        .expect("Failed to execute command");

    let output_str = match str::from_utf8(&output.stdout) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to read output: {}", e);
            return Vec::new(); // Return an empty vector on error
        }
    };

    let output_str = str::from_utf8(&output.stdout).unwrap();

    // Parse the JSON string into a Value object
    let value: Value = from_str(output_str).expect("JSON was not well-formatted");

    // Assuming the JSON structure is a map with one key (location name)
    // and the value is another map of timestamps to temperatures
    let mut temp_time_vec = Vec::new();
    if let Value::Object(outer_map) = value {
        if let Some((_, inner_map)) = outer_map.into_iter().next() {
            if let Value::Object(timestamp_map) = inner_map {
                for (time, temp_value) in timestamp_map {
                    if let Value::Number(num) = temp_value {
                        if let Some(temp) = num.as_f64() {
                            temp_time_vec.push((time, temp));
                        }
                    }
                }
            }
        }
    }

    // Print for debugging
    for (time, temperature) in &temp_time_vec {
        println!("Time: {}, Temperature: {}", time, temperature);
    }

    temp_time_vec
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Output, Command};

    /* 

    // Mock function to simulate Command::output
    fn mock_output() -> Output {
        let data = r#"
        [
            {"time": "2022-08-01T00:00:00+00:00", "temperature": 20.5},
            {"time": "2022-08-01T01:00:00+00:00", "temperature": 21.0}
        ]
        "#;

        Output {
            status: Command::new("true").status().unwrap(),
            stdout: data.as_bytes().to_vec(),
            stderr: Vec::new(),
        }
    }

    #[test]
    fn test_get_weather_data() {
        // Here we would mock the Command::new to return our mock_output
        // However, this requires more advanced setup and possibly third-party libraries for mocking

        // Assuming get_weather_data uses mock_output instead of Command::output
        // let weather_data = get_weather_data("2022-08-01 00:00".to_string(), "2022-08-02 00:00".to_string(), "61.4481,23.8521".to_string());

        // For demonstration, let's manually parse the mock data
        let output = mock_output();
        let output_str = str::from_utf8(&output.stdout).unwrap();
        let temperature_data: Vec<Value> = from_str(output_str).expect("JSON was not well-formatted");
        let temp_time_vec: Vec<(String, f64)> = temperature_data.iter()
            .map(|entry| {
                let time = entry["time"].as_str().unwrap().to_string();
                let temperature = entry["temperature"].as_f64().unwrap();
                (time, temperature)
            })
            .collect();

        assert_eq!(temp_time_vec.len(), 2);
        assert_eq!(temp_time_vec[0], ("2022-08-01T00:00:00+00:00".to_string(), 20.5));
        assert_eq!(temp_time_vec[1], ("2022-08-01T01:00:00+00:00".to_string(), 21.0));
    }

    */
}
