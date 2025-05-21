use super::job_store::JobStore;
use super::jobs::{ElectricityPriceOutcome, JobOutcome, JobStatus};
use crate::settings::Settings;
use crate::time_line_settings::{TimeLineSettings, compute_timeline_start};
use crate::TimeStamp;
use chrono::NaiveDateTime;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::process::Command; 

pub async fn start(
    job_id: i32,
    settings: Arc<Mutex<Settings>>,
    job_store: JobStore,
    time_line_settings: TimeLineSettings,
) {
    // 1. mark job as running --------------------------------------------------
    if job_store
        .set_job_status(job_id, Arc::new(JobStatus::InProgress))
        .await
        .is_err()
    {
        return;
    }

    // 2. snapshot settings ----------------------------------------------------
    let settings = settings.lock().await.clone();

    let country = match settings.location.as_ref().map(|l| &l.country) {
        Some(c) => c,
        None => {
            job_store
                .set_job_status(
                    job_id,
                    Arc::new(JobStatus::Failed("location/country has not been set".into())),
                )
                .await
                .ok();
            return;
        }
    };

    let api_token = match settings.entsoe_api_token.as_deref() {
        Some(t) => t,
        None => {
            job_store
                .set_job_status(
                    job_id,
                    Arc::new(JobStatus::Failed("ENTSO‑E API token has not been configured".into())),
                )
                .await
                .ok();
            return;
        }
    };

    // 3. compute time window --------------------------------------------------
    let start_time = compute_timeline_start(&time_line_settings);
    let end_time = start_time + time_line_settings.duration().to_time_delta();

    // 4. spawn the external fetcher ------------------------------------------
    match fetch_electricity_prices(
        country,
        api_token,
        &start_time,
        &end_time,
        &settings.python_exec,
        &settings.price_fetcher_script,
    )
    .await
    {
        Ok(prices) => {
            let outcome = prices_to_outcome(prices);
            let _ = job_store
                .set_job_status(
                    job_id,
                    Arc::new(JobStatus::Finished(JobOutcome::ElectricityPrice(outcome))),
                )
                .await;
        }
        Err(err) => {
            let _ = job_store
                .set_job_status(job_id, Arc::new(JobStatus::Failed(err.into())))
                .await;
        }
    }
}

fn prices_to_outcome(forecast: Vec<(TimeStamp, f64)>) -> ElectricityPriceOutcome {
    let mut time_line = Vec::with_capacity(forecast.len());
    let mut prices = Vec::with_capacity(forecast.len());
    for (stamp, price) in forecast {
        time_line.push(stamp);
        prices.push(price);
    }
    ElectricityPriceOutcome::new(time_line, prices)
}

fn parse_price_fetcher_output(output: &str) -> Result<Vec<(TimeStamp, f64)>, String> {
    let parsed_json = serde_json::from_str::<Value>(output)
        .map_err(|err| format!("failed to parse output: {}", err))?;

    match parsed_json {
        Value::Array(time_series) => {
            let mut data = Vec::with_capacity(time_series.len());
            for row in time_series {
                let pair = match row {
                    Value::Array(p) => p,
                    _ => return Err("failed to parse data pair in time series".into()),
                };

                if pair.len() != 2 {
                    return Err("failed to parse time series".into());
                }

                // 0. timestamp ────────────────────────────────────────────────┐
                let time_stamp = match &pair[0] {
                    Value::String(stamp) => {
                        match NaiveDateTime::parse_from_str(stamp, "%Y-%m-%dT%H:%M:%S") {
                            Ok(dt) => dt.and_utc(),
                            Err(_) => {
                                return Err(format!("failed to parse stamp from string {stamp}"));
                            }
                        }
                    }
                    _ => return Err("failed to parse time stamp".into()),
                };

                // 1. value ────────────────────────────────────────────────────┐
                let price = match &pair[1] {
                    Value::Number(n) => n
                        .as_f64()
                        .ok_or_else(|| "failed to convert price to float".to_string())?,
                    _ => return Err("failed to parse price".into()),
                };

                data.push((time_stamp, price));
            }
            Ok(data)
        }
        _ => Err("failed to parse array from output".into()),
    }
}

pub async fn fetch_electricity_prices(
    country_code: &str,
    api_token: &str,
    start_time: &TimeStamp,
    end_time: &TimeStamp,
    python_exec: &str,
    price_script: &str,
) -> Result<Vec<(TimeStamp, f64)>, String> {
    // helper expects "YYYY-MM-DD HH:MM" in UTC
    let cli_time_format = "%Y-%m-%d %H:%M";

    let output = Command::new(python_exec)
        .arg(price_script)
        .arg(start_time.format(cli_time_format).to_string())
        .arg(end_time.format(cli_time_format).to_string())
        .arg(country_code)
        .arg(api_token)
        .output()
        .await
        .map_err(|e| format!("Python failed: {e}"))?;

    if !output.status.success() {
        return Err("price fetcher returned non‑zero exit status".into());
    }

    let stdout = String::from_utf8(output.stdout)
        .map_err(|_| "non‑utf‑8 characters in output".to_string())?;

    parse_price_fetcher_output(&stdout)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    mod parse_price_fetcher_output {
        use super::*;

        #[test]
        fn parses_entsoe_data_correctly() {
            let fetcher_output = r#"[
                ["2025-05-21T03:00:00", 7.18],
                ["2025-05-21T04:00:00", 10.45],
                ["2025-05-21T05:00:00", 5.81]
            ]"#;

            let price_series = parse_price_fetcher_output(fetcher_output)
                .expect("parsing output should not fail");

            let expected_time_stamps = [
                Utc.with_ymd_and_hms(2025, 5, 21, 3, 0, 0).single().unwrap(),
                Utc.with_ymd_and_hms(2025, 5, 21, 4, 0, 0).single().unwrap(),
                Utc.with_ymd_and_hms(2025, 5, 21, 5, 0, 0).single().unwrap(),
            ];
            let expected_prices = [7.18, 10.45, 5.81];

            assert_eq!(price_series.len(), expected_prices.len());
            for (i, pair) in price_series.iter().enumerate() {
                assert_eq!(pair.0, expected_time_stamps[i]);
                assert!((pair.1 - expected_prices[i]).abs() < f64::EPSILON);
            }
        }
    }
}
