
use crate::errors;
use crate::utilities;

use std::collections::HashMap;
use serde::{self, Serialize, Deserialize, Deserializer, Serializer};
use chrono::{DateTime, FixedOffset};
use std::collections::BTreeMap;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use serde::de::{self, MapAccess, Visitor};
use std::fmt;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct OptimizationData {
    pub fetch_weather_data: bool,
    pub fetch_elec_data: bool,
    pub fetch_time_data: bool,
    pub country: Option<String>,
    pub location: Option<String>,
    pub timezone: Option<String>,
    pub elec_price_source: Option<ElecPriceSource>,
    pub model_data: Option<InputData>,
    pub time_data: Option<TimeData>,
    pub weather_data: Option<WeatherData>,
    pub elec_price_data: Option<ElectricityPriceData>,
    pub control_results: Option<Vec<DataTable>>,
    pub input_data_batch: Option<Vec<(String, Vec<u8>)>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct InputData {
    pub temporals: Temporals,
    pub setup: InputDataSetup,
    pub processes: HashMap<String, Process>,
    pub nodes: BTreeMap<String, Node>,
    pub node_diffusion: Vec<NodeDiffusion>,
    pub node_delay: Vec<(String, String, f64, f64, f64)>,
    pub node_histories: HashMap<String, NodeHistory>,
    pub markets: BTreeMap<String, Market>,
    pub groups: HashMap<String, Group>,
    pub scenarios: BTreeMap<String, f64>,
    pub reserve_type: HashMap<String, f64>,
    pub risk: HashMap<String, f64>,
    pub inflow_blocks: BTreeMap<String, InflowBlock>,
    pub bid_slots: HashMap<String, BidSlot>,
    pub gen_constraints: HashMap<String, GenConstraint>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Temporals {
    pub t: Vec<String>,
    pub dtf: f64,
    pub is_variable_dt: bool,
    pub variable_dt: Vec<(String, f64)>,
    pub ts_format: String, 
    
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
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

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
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

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
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

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct NodeDiffusion {
    pub node1: String,
    pub node2: String,
    pub coefficient: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct NodeHistory {
    pub node: String,
    pub steps: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Market {
    pub name: String,
    pub m_type: String,
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

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Group {
    pub name: String,
    pub g_type: String,
    pub members: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct InflowBlock {
    pub name: String,
    pub node: String,
    pub start_time: String,
    pub data: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BidSlot {
    pub market: String,
    pub time_steps: Vec<String>,
    pub slots: Vec<String>,
    #[serde(deserialize_with = "deserialize_prices")]
    pub prices: BTreeMap<(String, String), f64>,
    #[serde(deserialize_with = "deserialize_market_price_allocation")]
    pub market_price_allocation: BTreeMap<(String, String), (String, String)>,
}

fn deserialize_prices<'de, D>(deserializer: D) -> Result<BTreeMap<(String, String), f64>, D::Error>
where
    D: Deserializer<'de>,
{
    struct PricesVisitor;

    impl<'de> Visitor<'de> for PricesVisitor {
        type Value = BTreeMap<(String, String), f64>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a map with string keys formatted as tuple (String, String) and float values")
        }

        fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            let mut map = BTreeMap::new();

            while let Some((key, value)) = access.next_entry::<String, f64>()? {
                let key = key
                    .trim_matches(|c| c == '(' || c == ')')
                    .split(", ")
                    .map(String::from)
                    .collect::<Vec<_>>();
                if key.len() != 2 {
                    return Err(de::Error::custom("invalid key format for prices"));
                }
                map.insert((key[0].clone(), key[1].clone()), value);
            }

            Ok(map)
        }
    }

    deserializer.deserialize_map(PricesVisitor)
}

fn deserialize_market_price_allocation<'de, D>(deserializer: D) -> Result<BTreeMap<(String, String), (String, String)>, D::Error>
where
    D: Deserializer<'de>,
{
    struct MarketPriceAllocationVisitor;

    impl<'de> Visitor<'de> for MarketPriceAllocationVisitor {
        type Value = BTreeMap<(String, String), (String, String)>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a map with string keys formatted as tuple (String, String) and tuple (String, String) values")
        }

        fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            let mut map = BTreeMap::new();

            while let Some((key, value)) = access.next_entry::<String, Vec<String>>()? {
                let key = key
                    .trim_matches(|c| c == '(' || c == ')')
                    .split(", ")
                    .map(String::from)
                    .collect::<Vec<_>>();
                if key.len() != 2 || value.len() != 2 {
                    return Err(de::Error::custom("invalid key format for market_price_allocation"));
                }
                map.insert(
                    (key[0].clone(), key[1].clone()),
                    (value[0].clone(), value[1].clone()),
                );
            }

            Ok(map)
        }
    }

    deserializer.deserialize_map(MarketPriceAllocationVisitor)
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct GenConstraint {
    pub name: String,
    pub gc_type: String,
    pub is_setpoint: bool,
    pub penalty: f64,
    pub factors: Vec<ConFactor>,
    pub constant: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
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

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TimeSeriesData {
    pub ts_data: Vec<TimeSeries>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TimeSeries {
    pub scenario: String,
    pub series: BTreeMap<String, f64>,
}

impl TimeSeriesData {
    pub fn from_temporals(temporals_t: &[String], scenario: String) -> Self {
        let mut series = BTreeMap::new();
        for time in temporals_t {
            series.insert(time.clone(), 1.0);
        }
        let time_series = TimeSeries {
            scenario,
            series,
        };
        TimeSeriesData {
            ts_data: vec![time_series],
        }
    }
}


// Implement a function to create the TimeSeries
impl TimeSeries {
    fn new(scenario: String, series: BTreeMap<String, f64>) -> TimeSeries {
        TimeSeries { scenario, series }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ConFactor {
    pub var_type: String,
    pub var_tuple: (String, String),
    pub data: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
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
                let value = utilities::column_value_to_string(column, row_index);
                row.push(value);
            }
            data.push(row);
        }

        DataTable { columns, data }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct WeatherData {
    pub place: String,
    pub weather_data: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Debug, Default)]
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

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct TimeData {
    #[serde(serialize_with = "serialize", deserialize_with = "deserialize")]
    pub start_time: DateTime<FixedOffset>,
    #[serde(serialize_with = "serialize", deserialize_with = "deserialize")]
    pub end_time: DateTime<FixedOffset>,
    pub series: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ElectricityPriceData {
    pub api_source: Option<String>,
    pub api_key: Option<String>,
    pub country: Option<String>,
    pub price_data: Option<TimeSeriesData>,
    pub up_price_data: Option<TimeSeriesData>,
    pub down_price_data: Option<TimeSeriesData>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ElecPriceData {
    pub api_source: String,
    pub api_key: Option<String>,
    pub country: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct EleringData {
    pub success: bool,
    pub data: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ElecPriceSource {
    pub api_source: String,
    pub token: Option<String>,
    pub country: Option<String>,
    pub bidding_in_domain: Option<String>,
    pub bidding_out_domain: Option<String>,
}