
use std::collections::HashMap;
use serde::{self, Serialize, Deserialize, Deserializer, Serializer};
use chrono::{DateTime, Duration as ChronoDuration, Utc, FixedOffset};
use std::collections::BTreeMap;
//use tokio::sync::mpsc;
//use std::fs::File;
use std::error::Error;
//use std::io::Write;
use chrono_tz::Tz;
use chrono::prelude::*;
//use std::io;
use crate::errors;
//use std::fmt::{self, Display, Formatter};
use arrow::record_batch::RecordBatch;
use arrow::array::{Array, StringArray, Float64Array, Int32Array, ArrayRef};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataTable {
    pub columns: Vec<String>,
    pub data: Vec<Vec<String>>,
}

unsafe impl Send for DataTable {}
unsafe impl Sync for DataTable {}

impl DataTable {
    pub fn from_record_batch(batch: Arc<RecordBatch>) -> Self {
        let columns = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();

        let mut data = Vec::new();

        for row_index in 0..batch.num_rows() {
            let mut row = Vec::new();
            for column in batch.columns() {
                let value = column_value_to_string(column, row_index);
                row.push(value);
            }
            data.push(row);
        }

        DataTable { columns, data }
    }
}

pub fn column_value_to_string(column: &ArrayRef, row_index: usize) -> String {
    if column.is_null(row_index) {
        return "NULL".to_string();
    }

    match column.data_type() {
        arrow::datatypes::DataType::Utf8 => {
            let array = column.as_any().downcast_ref::<StringArray>().unwrap();
            array.value(row_index).to_string()
        }
        arrow::datatypes::DataType::Float64 => {
            let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
            array.value(row_index).to_string()
        }
        arrow::datatypes::DataType::Int32 => {
            let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
            array.value(row_index).to_string()
        }
        _ => "Unsupported type".to_string(),
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PricePoint {
    pub timestamp: i64,
    pub price: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TemporalsHours {
    pub hours: i64,

}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InputData {
    pub temporals: Temporals,
    pub setup: InputDataSetup,
    pub processes: HashMap<String, Process>,
    pub nodes: HashMap<String, Node>,
    pub node_diffusion: Vec<NodeDiffusion>,
    pub node_delay: Vec<(String, String, f64, f64, f64)>,
    pub node_histories: HashMap<String, NodeHistory>,
    pub markets: HashMap<String, Market>,
    pub groups: HashMap<String, Group>,
    pub scenarios: BTreeMap<String, f64>,
    pub reserve_type: HashMap<String, f64>,
    pub risk: HashMap<String, f64>,
    pub inflow_blocks: HashMap<String, InflowBlock>,
    pub bid_slots: HashMap<String, BidSlot>,
    pub gen_constraints: HashMap<String, GenConstraint>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Temporals {
    pub t: Vec<String>,
    pub dtf: f64,
    pub is_variable_dt: bool,
    pub variable_dt: Vec<(String, f64)>,
    pub ts_format: String, 
    
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
    pub node_dummy_variable_cost: f64,
    pub ramp_dummy_variable_cost: f64,
}

//INITIAL_STATE MUUTTUNUT
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Process {
    pub name: String,
    pub groups: Vec<String>,
    pub conversion: i64,
    pub is_cf: bool,
    pub is_cf_fix: bool,
    pub is_online: bool,
    pub is_res: bool,
    pub eff: f64,
    pub load_min: f64,
    pub load_max: f64,
    pub start_cost: f64,
    pub min_online: f64,
    pub min_offline: f64,
    pub max_online: f64,
    pub max_offline: f64,
    pub initial_state: bool,
    pub is_scenario_independent: bool,
    pub topos: Vec<Topology>,
    pub cf: TimeSeriesData,
    pub eff_ts: TimeSeriesData,
    pub eff_ops: Vec<String>,
    pub eff_fun: Vec<(f64,f64)>
}

//STATE MUUTTUNUT
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    pub name: String,
    pub groups: Vec<String>,
    pub is_commodity: bool,
    pub is_market: bool,
    pub is_state: bool,
    pub is_res: bool,
    pub is_inflow: bool,
    pub state: Option<State>,
    pub cost: TimeSeriesData,
    pub inflow: TimeSeriesData,
}

//MUUTTUNUT, NAME POISTUI, COEFFICIENT MUUTTUI
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeDiffusion {
    pub node1: String,
    pub node2: String,
    pub coefficient: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeHistory {
    pub node: String,
    pub steps: TimeSeriesData,
}

//MUUTTUNUT, LISÄTTY reserve_activation_price, realisation -> timeseriesdata
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Market {
    pub name: String,
    pub var_type: String,
    pub node: String, 
    pub processgroup: String,
    pub direction: String,
    pub realisation: TimeSeriesData,
    pub reserve_type: String,
    pub is_bid: bool,
    pub is_limited: bool,
    pub min_bid: f64,
    pub max_bid: f64,
    pub fee: f64,
    pub price: TimeSeriesData,
    pub up_price: TimeSeriesData,
    pub down_price: TimeSeriesData,
    pub reserve_activation_price: TimeSeriesData,
    pub fixed: Vec<(String, f64)>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Group {
    pub name: String,
    pub var_type: String,
    pub members: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InflowBlock {
    pub name: String,
    pub node: String,
    pub start_time: String,
    pub data: TimeSeriesData,
}

//UUSI
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BidSlot {
    pub market: String,
    pub time_steps: Vec<String>,
    pub slots: Vec<String>,
    pub prices: BTreeMap<(String, String), f64>,
    pub market_price_allocation: BTreeMap<(String, String), (String, String)>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GenConstraint {
    pub name: String,
    pub var_type: String,
    pub is_setpoint: bool,
    pub penalty: f64,
    pub factors: Vec<ConFactor>,
    pub constant: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Topology {
    pub source: String,
    pub sink: String,
    pub capacity: f64,
    pub vom_cost: f64,
    pub ramp_up: f64,
    pub ramp_down: f64,
    pub initial_load: f64,
    pub initial_flow: f64,
    pub cap_ts: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct State {
    pub in_max: f64,
    pub out_max: f64,
    pub state_loss_proportional: f64,
    pub state_max: f64,
    pub state_min: f64,
    pub initial_state: f64,
    pub is_scenario_independent: bool,
    pub is_temp: bool,
    pub t_e_conversion: f64,
    pub residual_value: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TimeSeriesData {
    pub ts_data: Vec<TimeSeries>,
}

//MUUTETTU, VEC->BTreeMap
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TimeSeries {
    pub scenario: String,
    pub series: BTreeMap<String, f64>,
}

// Implement a function to create the TimeSeries
impl TimeSeries {
    fn new(scenario: String, series: BTreeMap<String, f64>) -> TimeSeries {
        TimeSeries { scenario, series }
    }
}

//MUUTTUNUT, NIMI -> var_tuple
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConFactor {
    pub var_type: String,
    pub var_tuple: (String, String),
    pub data: TimeSeriesData,
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
    pub temporals: Option<TemporalsHours>,
    pub time_data: Option<TimeData>,
    pub weather_data: Option<WeatherData>,
    pub model_data: Option<InputData>,
    pub elec_price_data: Option<ElectricityPriceData>,
    pub control_results: Option<Vec<DataTable>>,
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

pub fn calculate_time_range(timezone_str: &str, temporals: &Option<TemporalsHours>) -> Result<(DateTime<FixedOffset>, DateTime<FixedOffset>), errors::TimeDataParseError> {
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