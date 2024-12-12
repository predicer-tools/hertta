use super::job_store::JobStore;
use super::jobs::{JobOutcome, JobStatus, WeatherForecastOutcome};
use crate::settings::Settings;
use crate::time_line_settings::TimeLineSettings;
use crate::TimeStamp;
use chrono::{DurationRound, NaiveDateTime, TimeDelta, Utc};
use serde_json::Value;
use std::process::Command;
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn start(
    job_id: i32,
    settings: Arc<Mutex<Settings>>,
    job_store: JobStore,
    time_line_settings: TimeLineSettings,
) {
    if job_store
        .set_job_status(job_id, Arc::new(JobStatus::InProgress))
        .await
        .is_err()
    {
        return;
    }
    let settings = settings.lock().await.clone();
    let location = settings.location;
    let place = match location {
        Some(location) => location.place,
        None => {
            job_store
                .set_job_status(
                    job_id,
                    Arc::new(JobStatus::Failed("location/place has not been set".into())),
                )
                .await
                .expect("setting job status should not fail");
            return;
        }
    };
    let start_time: TimeStamp = Utc::now().duration_trunc(TimeDelta::hours(1)).unwrap();
    let end_time = start_time + time_line_settings.duration().to_time_delta();
    match fetch_weather_data(
        &place,
        &start_time,
        &end_time,
        &time_line_settings.step().to_time_delta(),
        &settings.python_exec,
        &settings.weather_fetcher_script,
    ) {
        Ok(forecast) => {
            if job_store
                .set_job_status(
                    job_id,
                    Arc::new(JobStatus::Finished(JobOutcome::WeatherForecast(
                        forecast_to_outcome(forecast),
                    ))),
                )
                .await
                .is_err()
            {
                return;
            }
        }
        Err(error) => {
            job_store
                .set_job_status(job_id, Arc::new(JobStatus::Failed(error.into())))
                .await
                .expect("setting job status should not fail");
            return;
        }
    }
}

fn forecast_to_outcome(forecast: Vec<(TimeStamp, f64)>) -> WeatherForecastOutcome {
    let mut time_line = Vec::with_capacity(forecast.len());
    let mut temperatures = Vec::with_capacity(forecast.len());
    for (stamp, temperature) in forecast {
        time_line.push(stamp);
        temperatures.push(temperature);
    }
    WeatherForecastOutcome::new(time_line, temperatures)
}

fn parse_weather_fetcher_output(output: &str) -> Result<Vec<(TimeStamp, f64)>, String> {
    let parsed_json = serde_json::from_str(output)
        .or_else(|error| Err(format!("failed to parse output: {}", error)))?;
    if let Value::Array(time_series) = parsed_json {
        let mut forecast = Vec::with_capacity(time_series.len());
        for row in time_series {
            if let Value::Array(pair) = row {
                if pair.len() != 2 {
                    return Err("failed to parse time series".to_string());
                }
                if let Value::String(ref stamp) = pair[0] {
                    let time_stamp = match NaiveDateTime::parse_from_str(stamp, "%Y-%m-%dT%H:%M:%S")
                    {
                        Ok(date_time) => date_time.and_utc(),
                        Err(..) => {
                            return Err(format!("failed to parse stamp from string {}", stamp))
                        }
                    };
                    if let Value::Number(ref value) = pair[1] {
                        if let Some(temperature) = value.as_f64() {
                            forecast.push((time_stamp, temperature));
                        } else {
                            return Err("failed to convert temperature to float".to_string());
                        }
                    } else {
                        return Err("failed to parse temperature".to_string());
                    }
                } else {
                    return Err("failed to parse time stamp".to_string());
                }
            } else {
                return Err("failed to parse data pair in time series".to_string());
            }
        }
        Ok(forecast)
    } else {
        Err("failed to parse array from output".to_string())
    }
}

pub fn fetch_weather_data(
    place: &String,
    start_time: &TimeStamp,
    end_time: &TimeStamp,
    step: &TimeDelta,
    python_exec: &String,
    weather_data_script: &String,
) -> Result<Vec<(TimeStamp, f64)>, String> {
    let format_string = "%Y-%m-%dT%H:%M:%S";
    let mut command = Command::new(python_exec);
    command
        .arg(weather_data_script)
        .arg(start_time.format(&format_string).to_string())
        .arg(end_time.format(&format_string).to_string())
        .arg(format!("{}", step.num_minutes()))
        .arg(place);
    let output = match command.output() {
        Ok(bytes) => bytes,
        Err(error) => return Err(format!("Python failed: {}", error)),
    };
    if !output.status.success() {
        return Err("weather fetching returned non-zero exit status".into());
    }
    let output = match String::from_utf8(output.stdout) {
        Ok(json_out) => json_out,
        Err(..) => return Err("non-utf-8 characters in output".to_string()),
    };
    Ok(parse_weather_fetcher_output(&output)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    mod parse_weather_fetcher_output {
        use super::*;
        use chrono::TimeZone;
        #[test]
        fn parses_data_correctly() {
            let fetcher_output = r#"
                [
                        ["2024-11-08T11:00:00", 6.5],
                        ["2024-11-08T12:00:00", 6.6],
                        ["2024-11-08T13:00:00", 6.2],
                        ["2024-11-08T14:00:00", 5.9],
                        ["2024-11-08T15:00:00", 6.1],
                        ["2024-11-08T16:00:00", 6.3],
                        ["2024-11-08T17:00:00", 6.4],
                        ["2024-11-08T18:00:00", 6.7]
                ]"#;
            let weather_series = parse_weather_fetcher_output(fetcher_output)
                .expect("parsing output should not fail");
            let expected_time_stamps = [
                Utc.with_ymd_and_hms(2024, 11, 8, 11, 0, 0)
                    .single()
                    .unwrap(),
                Utc.with_ymd_and_hms(2024, 11, 8, 12, 0, 0)
                    .single()
                    .unwrap(),
                Utc.with_ymd_and_hms(2024, 11, 8, 13, 0, 0)
                    .single()
                    .unwrap(),
                Utc.with_ymd_and_hms(2024, 11, 8, 14, 0, 0)
                    .single()
                    .unwrap(),
                Utc.with_ymd_and_hms(2024, 11, 8, 15, 0, 0)
                    .single()
                    .unwrap(),
                Utc.with_ymd_and_hms(2024, 11, 8, 16, 0, 0)
                    .single()
                    .unwrap(),
                Utc.with_ymd_and_hms(2024, 11, 8, 17, 0, 0)
                    .single()
                    .unwrap(),
                Utc.with_ymd_and_hms(2024, 11, 8, 18, 0, 0)
                    .single()
                    .unwrap(),
            ];
            let expected_temperatures = [6.5, 6.6, 6.2, 5.9, 6.1, 6.3, 6.4, 6.7];
            assert_eq!(weather_series.len(), expected_temperatures.len());
            for (i, pair) in weather_series.iter().enumerate() {
                assert_eq!(pair.0, expected_time_stamps[i]);
                assert_eq!(pair.1, expected_temperatures[i]);
            }
        }
    }
}
