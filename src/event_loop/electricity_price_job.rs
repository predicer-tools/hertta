use super::job_store::JobStore;
use super::jobs::{ElectricityPriceOutcome, JobOutcome, JobStatus};
use crate::settings::Settings;
use crate::time_line_settings::{TimeLineSettings, compute_timeline_start};
use crate::TimeStamp;
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;
use quick_xml::{events::Event, reader::Reader, name::QName};
use std::collections::HashMap;
use std::borrow::Cow;

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
    match fetch_electricity_prices(&country, &start_time, &end_time).await {
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

pub async fn fetch_electricity_prices(
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

/// Map ISO-3166 country codes (or short names) → ENTSO-E bidding-zone EIC codes.
/// Extend this table as you need.
fn country_to_eic(country: &str) -> Result<&'static str, Box<dyn std::error::Error + Send + Sync>> {
    let map: HashMap<&str, &str> = [
        ("FI", "10YFI-1--------U"),
        ("SE1", "10Y1001A1001A44P"),
        ("SE2", "10Y1001A1001A45N"),
        ("SE3", "10Y1001A1001A46L"),
        ("SE4", "10Y1001A1001A47J"),
        ("EE", "10Y1001A1001A39I"),
        ("LV", "10Y1001A1001A51S"),
        ("LT", "10YLT-1001A0008Q"),
        ("DE", "10Y1001A1001A83F"), // Germany/LU common price zone
        ("DK1", "10YDK-1--------W"),
        ("DK2", "10YDK-2--------M"),
    ]
    .into_iter()
    .collect();

    map.get(country)
        .copied()
        .ok_or_else(|| format!("Unknown country/bidding zone: {country}").into())
}

pub async fn fetch_electricity_prices_entsoe(
    api_key:  &str,
    country:  &str,
    start:    &DateTime<Utc>,
    end:      &DateTime<Utc>,
) -> Result<Vec<(TimeStamp, f64)>, Box<dyn std::error::Error + Send + Sync>> {
    // ---- 1. Build request ----------------------------------------------------
    let eic          = country_to_eic(country)?;          // still your lookup
    let period_start = start.format("%Y%m%d%H%M").to_string();
    let period_end   = end  .format("%Y%m%d%H%M").to_string();

    let url = format!(
        "https://web-api.tp.entsoe.eu/api\
         ?securityToken={api_key}&documentType=A44\
         &in_Domain={eic}&out_Domain={eic}\
         &periodStart={period_start}&periodEnd={period_end}"
    );

    // ---- 2. Download XML ------------------------------------------------------
    let xml_bytes = reqwest::Client::new()
        .get(&url)
        .header("accept", "application/xml")
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;                                            // Vec<u8>

    let xml_text = std::str::from_utf8(&xml_bytes)
        .map_err(|e| format!("response not valid UTF-8: {e}"))?;  // &str

    // ---- 3. Parse & return ----------------------------------------------------
    parse_entsoe_response(xml_text)                         // Result<_, String>
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
}


pub fn parse_entsoe_response(
    xml: &str,
) -> Result<Vec<(DateTime<Utc>, f64)>, String> {
    // ── reader setup ─────────────────────────────────────────────────────
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);                    // newer API :contentReference[oaicite:1]{index=1}
    let mut buf = Vec::<u8>::new();

    // ── detect root element ──────────────────────────────────────────────
    let root = loop {
        match reader
            .read_event_into(&mut buf)
            .map_err(|e| format!("ill-formed XML: {e}"))?    // ✨ String
        {
            Event::Start(e) => break e.name().as_ref().to_vec(),
            Event::Eof      => return Err("empty XML document".into()),
            _               => buf.clear(),
        }
    };

    // ── acknowledgement branch ───────────────────────────────────────────
    if root == b"Acknowledgement_MarketDocument" {
        let mut code: Option<Cow<'_, str>> = None;
        let mut text: Option<Cow<'_, str>> = None;

        loop {
            match reader
                .read_event_into(&mut buf)
                .map_err(|e| format!("XML error in acknowledgement: {e}"))?  // ✨
            {
                Event::Start(tag) if tag.name().as_ref() == b"code" => {
                    code = Some(
                        reader
                            .read_text(QName(b"code"))
                            .map_err(|e| e.to_string())?,                  // ✨
                    );
                }
                Event::Start(tag) if tag.name().as_ref() == b"text" => {
                    text = Some(
                        reader
                            .read_text(QName(b"text"))
                            .map_err(|e| e.to_string())?,                  // ✨
                    );
                }
                Event::End(end) if end.name().as_ref() == b"Reason" => break,
                Event::Eof => break,
                _ => {}
            }
            buf.clear();
        }

        let msg = match (code.as_deref(), text.as_deref()) {
            (Some("B08"), _)        => "ENTSO-E: data not yet available (B08)".to_owned(),
            (Some(c), Some(t))      => format!("ENTSO-E acknowledgement {c}: {t}"),
            (Some(c), None)         => format!("ENTSO-E acknowledgement {c}"),
            _                       => "ENTSO-E sent acknowledgement".to_owned(),
        };
        return Err(msg);
    }

    // ── normal price document ────────────────────────────────────────────
    let mut series_start = None::<DateTime<Utc>>;
    let mut position     = None::<u32>;
    let mut out          = Vec::<(DateTime<Utc>, f64)>::new();

    loop {
        match reader
            .read_event_into(&mut buf)
            .map_err(|e| format!("XML parse error: {e}"))?                 // ✨
        {
            Event::Start(tag) if tag.name().as_ref() == b"timeInterval" => {
                let txt = reader
                    .read_text(QName(b"start"))
                    .map_err(|e| e.to_string())?;                          // ✨
                series_start = Some(
                    DateTime::parse_from_rfc3339(&txt)
                        .map_err(|e| e.to_string())?                       // ✨
                        .with_timezone(&Utc),
                );
            }
            Event::Start(tag) if tag.name().as_ref() == b"Point" => {
                position = None;
            }
            Event::Start(tag) if tag.name().as_ref() == b"position" => {
                let txt = reader
                    .read_text(QName(b"position"))
                    .map_err(|e| e.to_string())?;                          // ✨
                position = Some(txt.parse().map_err(|e| format!("bad position: {e}"))?);
            }
            Event::Start(tag) if tag.name().as_ref() == b"price.amount" => {
                let txt = reader
                    .read_text(QName(b"price.amount"))
                    .map_err(|e| e.to_string())?;                          // ✨
                let price = txt.parse::<f64>().map_err(|e| format!("bad price: {e}"))?;
                if let (Some(base), Some(pos)) = (series_start, position) {
                    let ts = base + Duration::hours((pos - 1) as i64);
                    out.push((ts, price));
                }
            }
            Event::Eof => break,
            _          => {}
        }
        buf.clear();
    }

    if out.is_empty() {
        Err("no price data found in document".into())
    } else {
        Ok(out)
    }
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
    use std::env;
    use dotenvy::dotenv;

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
    mod country_to_eic {
        use super::*;

        // Use the *same* error type as the function under test
        #[test]
        fn gives_correct_eic_codes() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            assert_eq!(country_to_eic("FI")?,  "10YFI-1--------U");
            assert_eq!(country_to_eic("SE1")?, "10Y1001A1001A44P");
            assert_eq!(country_to_eic("EE")?,  "10Y1001A1001A39I");
            Ok(())
        }

        #[test]
        fn fails_with_unknown_country() {
            assert!(country_to_eic("Mordor").is_err());
        }
    }

    // -------------------- live round-trip (ignored) ----------------------
    mod fetch_electricity_prices_entsoe {
        use super::*;
        use chrono::{TimeZone, Utc, Datelike};

        #[tokio::test]
        #[ignore]   // run with `cargo test -- --include-ignored`
        async fn pulls_one_day() {
            dotenv().ok();  // read ENTSOE_API_KEY from .env if present
            let api_key =
                env::var("ENTSOE_API_KEY").expect("Set ENTSOE_API_KEY to run this test");

            let yesterday   = Utc::now() - chrono::Duration::days(1);
            let date        = yesterday.date_naive();                       // NaiveDate
            let start = Utc
                .with_ymd_and_hms(date.year(), date.month(), date.day(), 0, 0, 0)
                .unwrap();
            let end   = start + chrono::Duration::days(1);

            let prices = fetch_electricity_prices_entsoe(&api_key, "FI", &start, &end)
                .await
                .expect("API call or XML parsing failed");

            // basic sanity
            assert_eq!(prices.len(), 24);
            let (first_ts, _) = prices.first().unwrap();
            assert_eq!(*first_ts, start);
        }
    }
}
