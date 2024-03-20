
use std::collections::HashMap;
use serde::{self, Serialize, Deserialize, Deserializer, Serializer};
use chrono::{DateTime, Duration as ChronoDuration, Utc, FixedOffset};
//use tokio::sync::mpsc;
//use std::fs::File;
use std::error::Error;
//use std::io::Write;
use chrono_tz::Tz;
use chrono::prelude::*;
//use std::io;
use crate::errors;
//use std::fmt::{self, Display, Formatter};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PricePoint {
    pub timestamp: i64,
    pub price: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Temporals {
    pub hours: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InputDataNew {
    pub temps: Vec<String>,
    pub setup: InputDataSetup,
    pub processes: HashMap<String, Process>,
    pub nodes: HashMap<String, Node>,
    pub node_diffusion: HashMap<String, NodeDiffusion>,
    pub node_delay: HashMap<String, NodeDelay>,
    pub node_histories: HashMap<String, NodeHistory>,
    pub markets: HashMap<String, Market>,
    pub groups: HashMap<String, Group>,
    pub scenarios: HashMap<String, f64>,
    pub reserve_type: HashMap<String, f64>,
    pub risk: HashMap<String, f64>,
    pub inflow_blocks: HashMap<String, f64>,
    pub gen_constraints: HashMap<String, GenConstraint>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InputData {
    pub timeseries: Vec<String>,
    pub contains_reserves: bool,
    pub contains_online: bool,
    pub contains_state: bool,
    pub contains_piecewise_eff: bool,
    pub contains_risk: bool,
    pub contains_delay: bool,
    pub contains_diffusion: bool,
    pub nodes: HashMap<String, Node>,
    pub processes: HashMap<String, Process>,
    pub markets: HashMap<String, Market>,
    pub groups: HashMap<String, Group>,
    pub gen_constraints: HashMap<String, GenConstraint>,
    pub node_diffusion: HashMap<String, NodeDiffusion>,
    pub node_delay: HashMap<String, NodeDelay>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InputDataSetup {
    pub contains_reserves: bool,
    pub contains_online: bool,
    pub contains_states: bool,
    pub contains_piecewise_eff: bool,
    pub contains_risk: bool,
    pub contains_diffusion: bool,
    pub contains_delay: bool,
    pub contains_markets: bool,
    pub reserve_realisation: bool,
    pub use_market_bids: bool,
    pub common_timesteps: i64,
    pub common_scenario_name: String,
    pub use_node_dummy_variables: bool,
    pub use_ramp_dummy_variables: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HassData {
    pub init_temp: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Process {
    pub name: String,
    pub group: String,
    pub delay: f64,
    pub is_cf: bool,
    pub is_cf_fix: bool,
    pub is_online: bool,
    pub is_res: bool,
    pub conversion: i64,
    pub eff: f64,
    pub load_min: f64,
    pub load_max: f64,
    pub start_cost: f64,
    pub min_online: f64,
    pub min_offline: f64,
    pub max_online: f64,
    pub max_offline: f64,
    pub initial_state: f64,
    pub topos: Vec<Topology>,
    pub eff_ops: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    pub name: String,
    pub is_commodity: bool,
    pub is_state: bool,
    pub is_res: bool,
    pub is_market: bool,
    pub is_inflow: bool,
    pub cost: TimeSeriesData,
    pub inflow: TimeSeriesData,
    pub state: State,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Market {
    pub name: String,
    pub m_type: String,
    pub node: String, //mikä tyyppi
    pub pgroup: String,
    pub direction: String,
    pub realisation: f64,
    pub reserve_type: String,
    pub is_bid: bool,
    pub is_limited: bool,
    pub min_bid: f64,
    pub max_bid: f64,
    pub fee: f64,
    pub price: TimeSeriesData,
    pub up_price: TimeSeriesData,
    pub down_price: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Group {
    pub name: String,
    pub g_type: String,
    pub entity: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeDiffusion {
    pub name: String,
    pub node1: String,
    pub node2: String,
    pub diff_coeff: f64,
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeDelay {
    pub name: String,
    pub node1: String,
    pub node2: String,
    pub delay: f64,
    pub min_flow: f64,
    pub max_flow: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Topology {
    pub source: String,
    pub sink: String,
    pub capacity: f64,
    pub vom_cost: f64,
    pub ramp_up: f64,
    pub ramp_down: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[derive(Default)]
pub struct State {
    pub in_max: f64,
    pub out_max: f64,
    pub state_loss_proportional: f64,
    pub state_max: f64,
    pub state_min: f64,
    pub initial_state: f64,
    pub is_temp: bool,
    pub t_e_conversion: f64,
    pub residual_value: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TimeSeriesData {
    pub ts_data: Vec<TimeSeries>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TimeSeries {
    pub scenario: String,
    pub series: Vec<(String, f64)>,
}

impl TimeSeries {
    pub fn new(scenario: String) -> TimeSeries {
        TimeSeries {
            scenario,
            series: Vec::new(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConFactor {
    pub var_type: String,
    pub flow: (String, String),
    pub data: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeHistory {
    pub node: String,
    pub steps: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GenConstraint {
    pub name: String,
    pub gc_type: String,
    pub is_setpoint: bool,
    pub penalty: f64,
    pub factors: Vec<ConFactor>,
    pub constant: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WeatherData {
    pub place: String,
    pub weather_data: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WeatherDataResponse {
    pub place: String,
    pub weather_values: Vec<f64>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ModelData {
    pub input_data: InputData,
}


// Serialization function for DateTime<FixedOffset>
fn serialize<S>(date: &DateTime<FixedOffset>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let s = date.to_rfc3339();
    serializer.serialize_str(&s)
}

// Deserialization function for DateTime<FixedOffset>
fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<FixedOffset>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    s.parse::<DateTime<FixedOffset>>().map_err(serde::de::Error::custom)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TimeData {
    #[serde(serialize_with = "serialize", deserialize_with = "deserialize")]
    pub start_time: DateTime<FixedOffset>,
    #[serde(serialize_with = "serialize", deserialize_with = "deserialize")]
    pub end_time: DateTime<FixedOffset>,
    pub series: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ElectricityPriceData {
    // Fields representing weather data
    pub country: String,
    pub price_data: TimeSeriesData,
    pub up_price_data: TimeSeriesData,
    pub down_price_data: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TimePoint {
    pub timestamp: String,
    pub value: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ElecPriceData {
    pub api_source: String,
    pub api_key: Option<String>,
    pub country: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ElecPriceSource {
    pub api_source: String,
    pub token: Option<String>,
    pub country: Option<String>,
    pub bidding_in_domain: Option<String>,
    pub bidding_out_domain: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SensorData {
    pub sensor_name: String,
    pub temp: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct OptimizationData {
    pub country: Option<String>,
    pub location: Option<String>,
    pub timezone: Option<String>,
    pub elec_price_source: Option<ElecPriceSource>,
    pub temporals: Option<Temporals>,
    pub time_data: Option<TimeData>,
    pub weather_data: Option<WeatherData>,
    pub model_data: Option<ModelData>,
    pub elec_price_data: Option<ElectricityPriceData>,
    pub sensor_data: Option<Vec<SensorData>>,
    pub control_results: Option<Vec<ControlData>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ControlData {
    pub device: String,
    pub control_data: Vec<TimePoint>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EleringData {
    pub success: bool,
    pub data: HashMap<String, Vec<PriceData>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PriceData {
    pub timestamp: i64, 
    pub price: f64,
}

pub fn calculate_time_range(timezone_str: &str, temporals: &Option<Temporals>) -> Result<(DateTime<FixedOffset>, DateTime<FixedOffset>), errors::TimeDataParseError> {
    let timezone: Tz = timezone_str.parse().map_err(|_| errors::TimeDataParseError::new("Invalid timezone string"))?;

    let now_utc = Utc::now();
    let now_in_timezone = now_utc.with_timezone(&timezone);

    // Get the FixedOffset from now_in_timezone
    let fixed_offset = FixedOffset::east_opt(now_in_timezone.offset().fix().local_minus_utc()).ok_or_else(|| errors::TimeDataParseError::new("Invalid FixedOffset"))?;

    let start_time = now_utc.with_timezone(&fixed_offset);
    let hours_to_add = temporals.as_ref().map_or(12, |t| t.hours as i64);
    let end_time = start_time + ChronoDuration::hours(hours_to_add);

    Ok((start_time, end_time))
}

pub async fn generate_hourly_timestamps(start_time: DateTime<FixedOffset>, end_time: DateTime<FixedOffset>) -> Result<Vec<String>, Box<dyn Error + Send>> {
    let mut current = start_time;
    let mut timestamps = Vec::new();

    // Loop to generate hourly timestamps between start_time and end_time
    while current <= end_time {
        // Format DateTime<FixedOffset> to "YYYY-MM-DDTHH:00:00±HH:MM"
        let formatted_timestamp = current.format("%Y-%m-%dT%H:00:00%:z").to_string();
        
        timestamps.push(formatted_timestamp);

        // Increment current time by one hour
        current = current + chrono::Duration::hours(1);
    }

    Ok(timestamps)
}

/* 
pub fn create_time_point(string: String, number: f64) -> (String, f64) {

    return (string, number)

}

pub fn add_time_point(ts_vec: &mut Vec<(String, f64)>, time_point: (String, f64)) {

    ts_vec.push(time_point);

}

pub fn add_time_serie(ts_data_vec: &mut Vec<TimeSeries>, time_series: TimeSeries) {

    ts_data_vec.push(time_series);

}

pub fn json_to_inputdata(json_data: &str) -> Result<InputData, serde_json::Error> {
    serde_json::from_str(json_data)
}
*/
/* 
pub fn _create_data(init_temp: f64) -> InputData {

    //Example time series

    let mut series1: Vec<(String, f64)> = Vec::new();
    let mut series2: Vec<(String, f64)> = Vec::new();

    let timepoint1 = create_time_point("Data1".to_string(), 0.0);
    let timepoint2 = create_time_point("Data2".to_string(), 0.0);

    add_time_point(&mut series1, timepoint1.clone());
    add_time_point(&mut series1, timepoint2.clone());
    add_time_point(&mut series2, timepoint1.clone());
    add_time_point(&mut series2, timepoint2.clone());

    let time_series1 = TimeSeries {
        scenario: "Scenario1".to_string(),
        series: series1,
    };

    let time_series2 = TimeSeries {
        scenario: "Scenario2".to_string(),
        series: series2,
    };

    // Step 2: Create a Vec<TimeSeries> containing the created TimeSeries instances
    let mut time_series_data_vec: Vec<TimeSeries> = Vec::new();
    add_time_serie(&mut time_series_data_vec, time_series1);
    add_time_serie(&mut time_series_data_vec, time_series2);


    // Step 3: Create a new TimeSeriesData instance with the Vec<TimeSeries>
    let time_series_data: TimeSeriesData = TimeSeriesData {
        ts_data: time_series_data_vec,
    };

    //Outside temperatures (time series)

    //These outside temperatures come from HASS, we need a function that takes data from HASS and put that timeserie in to a vec

    let outside_timeseries_s1: Vec<(String, f64)> = vec![
        ("2022-04-20T00:00:00+00:00".to_string(), 3.0),
        ("2022-04-20T01:00:00+00:00".to_string(), 0.0),
        ("2022-04-20T02:00:00+00:00".to_string(), 4.0),
        ("2022-04-20T03:00:00+00:00".to_string(), -1.0),
        ("2022-04-20T04:00:00+00:00".to_string(), 5.0),
        ("2022-04-20T05:00:00+00:00".to_string(), -4.0),
        ("2022-04-20T06:00:00+00:00".to_string(), -5.0),
        ("2022-04-20T07:00:00+00:00".to_string(), -2.0),
        ("2022-04-20T08:00:00+00:00".to_string(), 4.0),
        ("2022-04-20T09:00:00+00:00".to_string(), 0.0),
    ];

    let outside_timeseries_s2: Vec<(String, f64)> = vec![
        ("2022-04-20T00:00:00+00:00".to_string(), -2.0),
        ("2022-04-20T01:00:00+00:00".to_string(), 4.0),
        ("2022-04-20T02:00:00+00:00".to_string(), 4.0),
        ("2022-04-20T03:00:00+00:00".to_string(), -1.0),
        ("2022-04-20T04:00:00+00:00".to_string(), 1.0),
        ("2022-04-20T05:00:00+00:00".to_string(), -3.0),
        ("2022-04-20T06:00:00+00:00".to_string(), 0.0),
        ("2022-04-20T07:00:00+00:00".to_string(), -5.0),
        ("2022-04-20T08:00:00+00:00".to_string(), -3.0),
        ("2022-04-20T09:00:00+00:00".to_string(), -2.0),
    ];

    let outside_ts_s1 = TimeSeries {
        scenario: "s1".to_string(),
        series: outside_timeseries_s1,
    };

    let outside_ts_s2 = TimeSeries {
        scenario: "s2".to_string(),
        series: outside_timeseries_s2,
    };

    let mut outside_ts_vec: Vec<TimeSeries> = Vec::new();
    add_time_serie(&mut outside_ts_vec, outside_ts_s1);
    add_time_serie(&mut outside_ts_vec, outside_ts_s2);

    let outside_ts: TimeSeriesData = TimeSeriesData {
        ts_data: outside_ts_vec,
    };

    //Market prices (time series)

    let npe_timeseries_s1: Vec<(String, f64)> = vec![
        ("2022-04-20T00:00:00+00:00".to_string(), 18.0),
        ("2022-04-20T01:00:00+00:00".to_string(), 5.0),
        ("2022-04-20T02:00:00+00:00".to_string(), 8.0),
        ("2022-04-20T03:00:00+00:00".to_string(), 6.0),
        ("2022-04-20T04:00:00+00:00".to_string(), 19.0),
        ("2022-04-20T05:00:00+00:00".to_string(), 24.0),
        ("2022-04-20T06:00:00+00:00".to_string(), 24.0),
        ("2022-04-20T07:00:00+00:00".to_string(), 21.0),
        ("2022-04-20T08:00:00+00:00".to_string(), 20.0),
        ("2022-04-20T09:00:00+00:00".to_string(), 10.0),
    ];

    let npe_timeseries_s2: Vec<(String, f64)> = vec![
        ("2022-04-20T00:00:00+00:00".to_string(), 8.0),
        ("2022-04-20T01:00:00+00:00".to_string(), 4.0),
        ("2022-04-20T02:00:00+00:00".to_string(), 8.0),
        ("2022-04-20T03:00:00+00:00".to_string(), 2.0),
        ("2022-04-20T04:00:00+00:00".to_string(), 24.0),
        ("2022-04-20T05:00:00+00:00".to_string(), 2.0),
        ("2022-04-20T06:00:00+00:00".to_string(), 10.0),
        ("2022-04-20T07:00:00+00:00".to_string(), 16.0),
        ("2022-04-20T08:00:00+00:00".to_string(), 11.0),
        ("2022-04-20T09:00:00+00:00".to_string(), 12.0),
    ];

    let npe_ts_s1 = TimeSeries {
        scenario: "s1".to_string(),
        series: npe_timeseries_s1,
    };

    let npe_ts_s2 = TimeSeries {
        scenario: "s2".to_string(),
        series: npe_timeseries_s2,
    };

    let npe_ts_vec: Vec<TimeSeries> = vec![npe_ts_s1, npe_ts_s2];

    let npe_ts: TimeSeriesData = TimeSeriesData {
        ts_data: npe_ts_vec,
    };

    //Market up prices (time series)

    let npe_up_prices_s1: Vec<(String, f64)> = vec![
        ("2022-04-20T00:00:00+00:00".to_string(), 19.8),
        ("2022-04-20T01:00:00+00:00".to_string(), 5.5),
        ("2022-04-20T02:00:00+00:00".to_string(), 8.8),
        ("2022-04-20T03:00:00+00:00".to_string(), 6.6),
        ("2022-04-20T04:00:00+00:00".to_string(), 20.9),
        ("2022-04-20T05:00:00+00:00".to_string(), 26.4),
        ("2022-04-20T06:00:00+00:00".to_string(), 26.4),
        ("2022-04-20T07:00:00+00:00".to_string(), 23.1),
        ("2022-04-20T08:00:00+00:00".to_string(), 22.0),
        ("2022-04-20T09:00:00+00:00".to_string(), 11.0),
    ];

    let npe_up_prices_s2: Vec<(String, f64)> = vec![
        ("2022-04-20T00:00:00+00:00".to_string(), 8.8),
        ("2022-04-20T01:00:00+00:00".to_string(), 4.4),
        ("2022-04-20T02:00:00+00:00".to_string(), 8.8),
        ("2022-04-20T03:00:00+00:00".to_string(), 2.2),
        ("2022-04-20T04:00:00+00:00".to_string(), 26.4),
        ("2022-04-20T05:00:00+00:00".to_string(), 2.2),
        ("2022-04-20T06:00:00+00:00".to_string(), 11.0),
        ("2022-04-20T07:00:00+00:00".to_string(), 17.6),
        ("2022-04-20T08:00:00+00:00".to_string(), 12.1),
        ("2022-04-20T09:00:00+00:00".to_string(), 13.2),
    ];

    let npe_up_s1 = TimeSeries {
        scenario: "s1".to_string(),
        series: npe_up_prices_s1,
    };

    let npe_up_s2 = TimeSeries {
        scenario: "s2".to_string(),
        series: npe_up_prices_s2,
    };

    let npe_up_vec: Vec<TimeSeries> = vec![npe_up_s1, npe_up_s2];

    let npe_up_ts: TimeSeriesData = TimeSeriesData {
        ts_data: npe_up_vec,
    };

    //Market down prices (time series)

    let npe_down_prices_s1: Vec<(String, f64)> = vec![
        ("2022-04-20T00:00:00+00:00".to_string(), 16.2),
        ("2022-04-20T01:00:00+00:00".to_string(), 4.5),
        ("2022-04-20T02:00:00+00:00".to_string(), 7.2),
        ("2022-04-20T03:00:00+00:00".to_string(), 5.4),
        ("2022-04-20T04:00:00+00:00".to_string(), 17.1),
        ("2022-04-20T05:00:00+00:00".to_string(), 21.6),
        ("2022-04-20T06:00:00+00:00".to_string(), 21.6),
        ("2022-04-20T07:00:00+00:00".to_string(), 18.9),
        ("2022-04-20T08:00:00+00:00".to_string(), 18.0),
        ("2022-04-20T09:00:00+00:00".to_string(), 9.0),
    ];

    let npe_down_prices_s2: Vec<(String, f64)> = vec![
        ("2022-04-20T00:00:00+00:00".to_string(), 7.2),
        ("2022-04-20T01:00:00+00:00".to_string(), 3.6),
        ("2022-04-20T02:00:00+00:00".to_string(), 7.2),
        ("2022-04-20T03:00:00+00:00".to_string(), 1.8),
        ("2022-04-20T04:00:00+00:00".to_string(), 21.6),
        ("2022-04-20T05:00:00+00:00".to_string(), 1.8),
        ("2022-04-20T06:00:00+00:00".to_string(), 9.0),
        ("2022-04-20T07:00:00+00:00".to_string(), 14.4),
        ("2022-04-20T08:00:00+00:00".to_string(), 9.9),
        ("2022-04-20T09:00:00+00:00".to_string(), 10.8),
    ];

    let npe_down_s1 = TimeSeries {
        scenario: "s1".to_string(),
        series: npe_down_prices_s1,
    };

    let npe_down_s2 = TimeSeries {
        scenario: "s2".to_string(),
        series: npe_down_prices_s2,
    };

    let npe_down_vec: Vec<TimeSeries> = vec![npe_down_s1, npe_down_s2];

    let npe_down_ts: TimeSeriesData = TimeSeriesData {
        ts_data: npe_down_vec,
    };

    //Gen constraints time series

    let c_interiorair_up_s1: Vec<(String, f64)> = vec![
        ("2022-04-20T00:00:00+00:00".to_string(), 298.15),
        ("2022-04-20T01:00:00+00:00".to_string(), 298.15),
        ("2022-04-20T02:00:00+00:00".to_string(), 298.15),
        ("2022-04-20T03:00:00+00:00".to_string(), 298.15),
        ("2022-04-20T04:00:00+00:00".to_string(), 298.15),
        ("2022-04-20T05:00:00+00:00".to_string(), 298.15),
        ("2022-04-20T06:00:00+00:00".to_string(), 298.15),
        ("2022-04-20T07:00:00+00:00".to_string(), 298.15),
        ("2022-04-20T08:00:00+00:00".to_string(), 298.15),
        ("2022-04-20T09:00:00+00:00".to_string(), 298.15),
    ];

    let c_interiorair_up_s2: Vec<(String, f64)> = vec![
        ("2022-04-20T00:00:00+00:00".to_string(), 298.15),
        ("2022-04-20T01:00:00+00:00".to_string(), 298.15),
        ("2022-04-20T02:00:00+00:00".to_string(), 298.15),
        ("2022-04-20T03:00:00+00:00".to_string(), 298.15),
        ("2022-04-20T04:00:00+00:00".to_string(), 298.15),
        ("2022-04-20T05:00:00+00:00".to_string(), 298.15),
        ("2022-04-20T06:00:00+00:00".to_string(), 298.15),
        ("2022-04-20T07:00:00+00:00".to_string(), 298.15),
        ("2022-04-20T08:00:00+00:00".to_string(), 298.15),
        ("2022-04-20T09:00:00+00:00".to_string(), 298.15),
    ];

    let c_interiorair_down_s1: Vec<(String, f64)> = vec![
        ("2022-04-20T00:00:00+00:00".to_string(), 292.15),
        ("2022-04-20T01:00:00+00:00".to_string(), 292.15),
        ("2022-04-20T02:00:00+00:00".to_string(), 292.15),
        ("2022-04-20T03:00:00+00:00".to_string(), 292.15),
        ("2022-04-20T04:00:00+00:00".to_string(), 292.15),
        ("2022-04-20T05:00:00+00:00".to_string(), 292.15),
        ("2022-04-20T06:00:00+00:00".to_string(), 292.15),
        ("2022-04-20T07:00:00+00:00".to_string(), 292.15),
        ("2022-04-20T08:00:00+00:00".to_string(), 292.15),
        ("2022-04-20T09:00:00+00:00".to_string(), 292.15),
    ];

    let c_interiorair_down_s2: Vec<(String, f64)> = vec![
        ("2022-04-20T00:00:00+00:00".to_string(), 292.15),
        ("2022-04-20T01:00:00+00:00".to_string(), 292.15),
        ("2022-04-20T02:00:00+00:00".to_string(), 292.15),
        ("2022-04-20T03:00:00+00:00".to_string(), 292.15),
        ("2022-04-20T04:00:00+00:00".to_string(), 292.15),
        ("2022-04-20T05:00:00+00:00".to_string(), 292.15),
        ("2022-04-20T06:00:00+00:00".to_string(), 292.15),
        ("2022-04-20T07:00:00+00:00".to_string(), 292.15),
        ("2022-04-20T08:00:00+00:00".to_string(), 292.15),
        ("2022-04-20T09:00:00+00:00".to_string(), 292.15),
    ];

    let interiorair_up_s1 = TimeSeries {
        scenario: "s1".to_string(),
        series: c_interiorair_up_s1, 
    };

    let interiorair_up_s2 = TimeSeries {
        scenario: "s2".to_string(),
        series: c_interiorair_up_s2,
    };

    let interiorair_down_s1 = TimeSeries {
        scenario: "s1".to_string(),
        series: c_interiorair_down_s1, 
    };

    let interiorair_down_s2 = TimeSeries {
        scenario: "s2".to_string(),
        series: c_interiorair_down_s2,
    };

    let gc_interiorair_up_vec: Vec<TimeSeries> = vec![interiorair_up_s1, interiorair_up_s2];
    let gc_interiorair_down_vec: Vec<TimeSeries> = vec![interiorair_down_s1, interiorair_down_s2];

    let interiorair_up_ts: TimeSeriesData = TimeSeriesData {
        ts_data: gc_interiorair_up_vec,
    };

    let interiorair_down_ts: TimeSeriesData = TimeSeriesData {
        ts_data: gc_interiorair_down_vec,
    };

    //Creating node_diffusion

    let diffusion_1 = NodeDiffusion {
        name: String::from("diffusion_1"),
        node1: String::from("interiorair"),
        node2: String::from("buildingenvelope"),
        diff_coeff: 0.5,
    };

    let diffusion_2 = NodeDiffusion {
        name: String::from("diffusion_2"),
        node1: String::from("buildingenvelope"),
        node2: String::from("outside"),
        diff_coeff: 0.4,
    };

    //Creating node_delay

    let delay_1 = NodeDelay {
        name: String::from("delay_1"),
        node1: String::from("dh1"),
        node2: String::from("dh2"),
        delay: 2.0,
        min_flow: 0.0,
        max_flow: 20.0,
    };

    //Creating state

    let interiorair_state = State {

        in_max: 1.0e10,
        out_max: 1.0e10,
        state_loss_proportional: 0.0,
        state_max: 308.15,
        state_min: 273.15,
        initial_state: init_temp.clone(),
        is_temp: true,
        t_e_conversion: 0.5,
        residual_value: 0.0,

    };

    //Creating nodes

    let _interiorair = Node {
        name: String::from("interiorair"),
        is_commodity: false,
        is_state: true,
        is_res: false,
        is_market: false,
        is_inflow: false,
        cost: time_series_data.clone(),
        inflow: time_series_data.clone(),
        state: interiorair_state,
    };

    let building_envelope_state = State {

        in_max: 1.0e10,
        out_max: 1.0e10,
        state_loss_proportional: 0.0,
        state_max: 308.15,
        state_min: 238.15,
        initial_state: 273.15,
        is_temp: true,
        t_e_conversion: 1.0,
        residual_value: 0.0,

    };

    let _building_envelope_state = State {

        in_max: 1.0e10,
        out_max: 1.0e10,
        state_loss_proportional: 0.0,
        state_max: 308.15,
        state_min: 238.15,
        initial_state: 273.15,
        is_temp: true,
        t_e_conversion: 1.0,
        residual_value: 0.0,
    };

    let _building_envelope = Node {
        name: String::from("buildingenvelope"),
        is_commodity: false,
        is_state: true,
        is_res: false,
        is_market: false,
        is_inflow: false,
        cost: time_series_data.clone(),
        inflow: time_series_data.clone(),
        state: building_envelope_state,
    };

    let outside_state = State {

        in_max: 1.0e10,
        out_max: 1.0e10,
        state_loss_proportional: 0.0,
        state_max: 308.15,
        state_min: 238.15,
        initial_state: 268.15,
        is_temp: true,
        t_e_conversion: 1000000000.0,
        residual_value: 0.0,

    };

    let _outside = Node {
        name: String::from("outside"),
        is_commodity: false,
        is_state: true,
        is_res: false,
        is_market: false,
        is_inflow: true,
        cost: time_series_data.clone(),
        inflow: outside_ts.clone(),
        state: outside_state,
    };

    let empty_state: State = Default::default();

    let _electricitygrid = Node {
        name: String::from("electricitygrid"),
        is_commodity: false,
        is_state: false,
        is_res: false,
        is_market: false,
        is_inflow: false,
        cost: time_series_data.clone(),
        inflow: time_series_data.clone(),
        state: empty_state,
    };

    let _node_history_1 = NodeHistory {
        node: String::from("electricitygrid"),
        steps: time_series_data.clone(),
    };

    let mut _nodes: HashMap<String, Node> = HashMap::new();
    let mut _node_diffusion: HashMap<String, NodeDiffusion> = HashMap::new();
    let mut _node_delay: HashMap<String, NodeDelay> = HashMap::new();

    _nodes.insert(_interiorair.name.clone(), _interiorair.clone());
    _nodes.insert(_building_envelope.name.clone(), _building_envelope.clone());
    _nodes.insert(_outside.name.clone(), _outside.clone());
    _nodes.insert(_electricitygrid.name.clone(), _electricitygrid.clone());

    _node_diffusion.insert(diffusion_1.name.clone(), diffusion_1.clone());
    _node_diffusion.insert(diffusion_2.name.clone(), diffusion_2.clone());

    _node_delay.insert(delay_1.name.clone(), delay_1.clone());

    let mut _processes: HashMap<String, Process> = HashMap::new();

    //Creating topology for processes

    let topology1 = Topology {
        source: String::from("electricitygrid"),
        sink: String::from("electricheater"),
        capacity: 7.5,
        vom_cost: 0.0,
        ramp_up: 1.0,
        ramp_down: 1.0,
    };

    let topology2 = Topology {
        source: String::from("electricheater"),
        sink: String::from("interiorair"),
        capacity: 7.5,
        vom_cost: 0.0,
        ramp_up: 1.0,
        ramp_down: 1.0,
    };

    let topo_vec: Vec<Topology> = vec![topology1, topology2];

    //Creating process

    let process_vec: Vec<String> = vec![("eff_ops".to_string())];

    let _electricheater1 = Process {
        name: String::from("electricheater"),
        group: String::from("p1"),
        delay: 0.0,
        is_cf: false,
        is_cf_fix: false,
        is_online: false,
        is_res: false,
        conversion: 1, //1,2 tai 3
        eff: 1.0,
        load_min: 0.0,
        load_max: 1.0,
        start_cost: 0.0,
        min_online: 0.0,
        min_offline: 0.0,
        max_online: 0.0,
        max_offline: 0.0,
        initial_state: 0.0,
        topos: topo_vec.clone(),
        eff_ops: process_vec.clone(),
    };

    _processes.insert(_electricheater1.name.clone(), _electricheater1.clone());

    let mut _markets: HashMap<String, Market> = HashMap::new();
    let mut _groups: HashMap<String, Group> = HashMap::new();
    let mut _genconstraints: HashMap<String, GenConstraint> = HashMap::new();

    let _npe = Market {
        name: String::from("npe"),
        m_type: String::from("energy"),
        node: String::from("electricitygrid"),
        pgroup: String::from("p1"),
        direction: String::from("none"),
        realisation: 0.0,
        reserve_type: String::from("none"),
        is_bid: true,
        is_limited: false,
        min_bid: 0.0,
        max_bid: 0.0,
        fee: 0.0,
        price: npe_ts.clone(),
        up_price: npe_up_ts.clone(),
        down_price: npe_down_ts.clone(),
    };

    _markets.insert(_npe.name.clone(), _npe.clone());

    let _p1 = Group {
        name: String::from("p1"),
        g_type: String::from("process"),
        entity: String::from("electricheater"),
    };

    _groups.insert(_p1.name.clone(), _p1.clone());

    let interiorair_up_cf = ConFactor {
        var_type: String::from("state"),
        flow: (String::from("interiorair"), String::from("")),
        data: interiorair_up_ts.clone(),
    };

    let interiorair_up_cf_vec: Vec<ConFactor> = vec![interiorair_up_cf];

    let interiorair_down_cf = ConFactor {
        var_type: String::from("state"),
        flow: (String::from("interiorair"), String::from("")),
        data: interiorair_down_ts.clone(),
    };

    let interiorair_down_cf_vec: Vec<ConFactor> = vec![interiorair_down_cf];

    let _c_interiorair_up = GenConstraint {
        name: String::from("c_interiorair_up"),
        gc_type: String::from("st"),
        is_setpoint: true,
        penalty: 1000.0,
        factors: interiorair_up_cf_vec.clone(),
        constant: time_series_data.clone(),
    };

    let _c_interiorair_down = GenConstraint {
        name: String::from("c_interiorair_down"),
        gc_type: String::from("gt"),
        is_setpoint: true,
        penalty: 1000.0,
        factors: interiorair_down_cf_vec.clone(),
        constant: time_series_data.clone(),
    };

    _genconstraints.insert(_c_interiorair_up.name.clone(), _c_interiorair_up.clone());
    _genconstraints.insert(_c_interiorair_down.name.clone(), _c_interiorair_down.clone());

    let mut _solution: Vec<(String, f64)> = Vec::new();
    
    
     
    let data = InputData {
        contains_reserves: false,
        contains_online: false,
        contains_state: true,
        contains_piecewise_eff: false,
        contains_risk: false,
        contains_delay: false,
        contains_diffusion: true,
        nodes: _nodes,
        processes: _processes,
        markets: _markets,
        groups: _groups,
        gen_constraints: _genconstraints,
        node_diffusion: _node_diffusion,
        node_delay: _node_delay,
    };

    return data

}
*/

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};


}

