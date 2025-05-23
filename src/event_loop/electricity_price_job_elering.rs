use super::job_store::JobStore;
use super::jobs::{ElectricityPriceOutcome, JobOutcome, JobStatus};
use crate::settings::Settings;
use crate::time_line_settings::{TimeLineSettings, compute_timeline_start};
use crate::TimeStamp;
use chrono::{DateTime, Utc};
use serde_json::Value;
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
    let country = match location {
        Some(location) => location.country,
        None => {
            job_store
                .set_job_status(
                    job_id,
                    Arc::new(JobStatus::Failed(
                        "location/country has not been set".into(),
                    )),
                )
                .await
                .expect("setting job status should not fail");
            return;
        }
    };
    let start_time = compute_timeline_start(&time_line_settings);
    let end_time = start_time + time_line_settings.duration().to_time_delta();
    match fetch_electricity_prices_elering(&country, &start_time, &end_time).await {
        Ok(forecast) => {
            if job_store
                .set_job_status(
                    job_id,
                    Arc::new(JobStatus::Finished(JobOutcome::ElectricityPrice(
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
                .set_job_status(
                    job_id,
                    Arc::new(JobStatus::Failed(format!("{:?}", error).into())),
                )
                .await
                .expect("setting job status should not fail");
            return;
        }
    }
}

fn forecast_to_outcome(forecast: Vec<(TimeStamp, f64)>) -> ElectricityPriceOutcome {
    let mut time_line = Vec::with_capacity(forecast.len());
    let mut prices = Vec::with_capacity(forecast.len());
    for (stamp, temperature) in forecast {
        time_line.push(stamp);
        prices.push(temperature);
    }
    ElectricityPriceOutcome::new(time_line, prices)
}

pub async fn fetch_electricity_prices_elering(
    country: &String,
    start_time: &DateTime<Utc>,
    end_time: &DateTime<Utc>,
) -> Result<Vec<(TimeStamp, f64)>, Box<dyn std::error::Error + Send + Sync>> {
    println!(
        "[start_electricity_price_fetch] Entered start() for country {}", 
        country
    );
    let format_string = "%Y-%m-%dT%H:%M:%S%.3fZ";
    let start_time_str = start_time.format(&format_string).to_string();
    let end_time_str = end_time.format(&format_string).to_string();
    let client = reqwest::Client::new();
    let url = format!(
        "https://dashboard.elering.ee/api/nps/price?start={}&end={}",
        start_time_str, end_time_str
    );
    let response = client
        .get(&url)
        .header("accept", "application/json")
        .send()
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    let response_text = response
        .text()
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    Ok(parse_elering_response(
        &response_text,
        &as_elering_country(country)?,
    )?)
}

fn parse_elering_response(
    response_text: &str,
    elering_country: &str,
) -> Result<Vec<(TimeStamp, f64)>, String> {
    let response = serde_json::from_str(response_text)
        .or_else(|error| Err(format!("failed to parse response: {}", error)))?;
    let response_object = match response {
        Value::Object(object) => object,
        _ => return Err("unexpected response content".to_string()),
    };
    let success = match response_object
        .get("success")
        .ok_or("'success' field missing in response".to_string())?
    {
        Value::Bool(success) => success,
        _ => return Err("unexpected type for 'success' in response".to_string()),
    };
    if !success {
        return Err("electricity price query unsuccessful".to_string());
    }
    let data = match response_object
        .get("data")
        .ok_or("'data' field missing in response".to_string())?
    {
        Value::Object(data) => data,
        _ => return Err("unexpected type for 'data' in response".to_string()),
    };
    let price_data = match data.get(elering_country).ok_or(format!(
        "requested country '{}' not found in response",
        elering_country
    ))? {
        Value::Array(price_data) => price_data,
        _ => return Err("unexpected type for country data in response".to_string()),
    };
    let mut pairs = Vec::with_capacity(price_data.len());
    for element in price_data {
        pairs.push(elering_time_stamp_pair(element)?);
    }
    Ok(pairs)
}

fn as_elering_country(country: &str) -> Result<&str, String> {
    if country == "Estonia" {
        return Ok("ee");
    }
    if country == "Finland" {
        return Ok("fi");
    }
    if country == "Lithuania" {
        return Ok("lt");
    }
    if country == "Latvia" {
        return Ok("lv");
    }
    Err("unknown or unsupported country".to_string())
}

fn elering_time_stamp_pair(time_stamp_entry: &Value) -> Result<(TimeStamp, f64), String> {
    let stamp_object = match time_stamp_entry {
        Value::Object(stamp_object) => stamp_object,
        _ => return Err("unexpected type for price entry in response".to_string()),
    };
    let time_stamp = match stamp_object
        .get("timestamp")
        .ok_or("'timestamp' field missing in price entry")?
    {
        Value::Number(t) => t
            .as_i64()
            .ok_or("failed to parse time stamp as integer".to_string())?,
        _ => return Err("unexpected type for time stamp".to_string()),
    };
    let time_stamp = DateTime::from_timestamp(time_stamp, 0)
        .ok_or("failed to convert time stamp to date time".to_string())?;
    let price = match stamp_object
        .get("price")
        .ok_or("'price' field missing in price entry")?
    {
        Value::Number(x) => x
            .as_f64()
            .ok_or("failed to parse price as float".to_string())?,
        _ => return Err("unexpected type for price".to_string()),
    };
    Ok((time_stamp, price))
}

#[cfg(test)]
mod tests {
    use super::*;

    mod as_elering_country {
        use super::*;
        #[test]
        fn gives_correct_country_codes() -> Result<(), String> {
            assert_eq!(as_elering_country("Estonia")?, "ee");
            assert_eq!(as_elering_country("Finland")?, "fi");
            assert_eq!(as_elering_country("Lithuania")?, "lt");
            assert_eq!(as_elering_country("Latvia")?, "lv");
            Ok(())
        }
        #[test]
        fn fails_with_unknown_country() {
            if let Ok(..) = as_elering_country("Mordor") {
                panic!("call should have failed");
            }
        }
    }
    mod elering_time_stamp_pair {
        use super::*;
        use chrono::TimeZone;
        #[test]
        fn parses_time_stamp_entry() {
            let entry_json = "{\"timestamp\":1731927600,\"price\":85.0600}";
            let entry = serde_json::from_str(entry_json).expect("entry JSON should be parseable");
            let pair =
                elering_time_stamp_pair(&entry).expect("constructing a pair should not fail");
            let expected_date_time = Utc.with_ymd_and_hms(2024, 11, 18, 11, 00, 00).unwrap();
            assert_eq!(pair, (expected_date_time, 85.06));
        }
    }
    mod parse_elering_response {
        use super::*;
        use chrono::TimeZone;
        #[test]
        fn parses_response_succesfully() {
            let response_json = r#"
{
    "success":true,
    "data": {
        "fi":[{"timestamp":1731927600,"price":70.0500}]
    }
}"#;
            let time_series =
                parse_elering_response(response_json, "fi").expect("parsing should succeed");
            let expected_date_time = Utc.with_ymd_and_hms(2024, 11, 18, 11, 00, 00).unwrap();
            let expected_time_series = vec![(expected_date_time, 70.05)];
            assert_eq!(time_series, expected_time_series);
        }
    }
}