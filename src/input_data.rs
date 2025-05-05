use crate::{TimeLine, TimeStamp};
use chrono::DateTime;
use hertta_derive::Name;
use juniper::{GraphQLEnum, GraphQLObject};
use serde::de::{self, MapAccess, Visitor};
use serde::{self, Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;
use std::fmt::{self, Display};
use indexmap::IndexMap;

pub trait Name {
    fn name(&self) -> &String;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InputData {
    pub temporals: Temporals,
    pub setup: InputDataSetup,
    pub processes: IndexMap<String, Process>,
    pub nodes: IndexMap<String, Node>,
    pub node_diffusion: Vec<NodeDiffusion>,
    pub node_delay: Vec<(String, String, f64, f64, f64)>,
    pub node_histories: IndexMap<String, NodeHistory>,
    pub markets: IndexMap<String, Market>,
    pub groups: IndexMap<String, Group>,
    pub scenarios: IndexMap<String, f64>,
    pub reserve_type: IndexMap<String, f64>,
    pub risk: IndexMap<String, f64>,
    pub inflow_blocks: IndexMap<String, InflowBlock>,
    pub bid_slots: IndexMap<String, BidSlot>,
    pub gen_constraints: IndexMap<String, GenConstraint>,
}

fn check_forecastable_series(
    forecastable: &Forecastable,
    temporals_t: &[TimeStamp],
    context: &str,
) -> Result<(), String> {
    match forecastable {
        Forecastable::TimeSeriesData(ref series) => {
            for ts_data in &series.ts_data {
                check_series(&ts_data, temporals_t, context)?;
            }
        }
        Forecastable::Forecast(_) => {
            return Err(format!(
                "{} data has not been replaced forecasted time series",
                context
            ))
        }
    };
    Ok(())
}

fn check_series(
    ts_data: &TimeSeries,
    temporals_t: &[TimeStamp],
    context: &str,
) -> Result<(), String> {
    let series_keys: TimeLine = ts_data.series.keys().cloned().collect();
    if series_keys != *temporals_t {
        println!("Mismatch in {}: {:?}", context, ts_data.scenario);
        println!("Expected: {:?}", temporals_t);
        println!("Found: {:?}", series_keys);
        return Err(format!(
            "time series mismatch in {}, scenario {}",
            context, ts_data.scenario
        ));
    }
    Ok(())
}

impl InputData {
    pub fn check_ts_data_against_temporals(&self) -> Result<(), String> {
        let temporals_t = &self.temporals.t;
        for (process_name, process) in &self.processes {
            for ts_data in &process.cf.ts_data {
                check_series(&ts_data, temporals_t, process_name)?;
            }
            for ts_data in &process.eff_ts.ts_data {
                check_series(&ts_data, temporals_t, process_name)?;
            }
            for topology in &process.topos {
                for ts_data in &topology.cap_ts.ts_data {
                    check_series(&ts_data, temporals_t, process_name)?;
                }
            }
        }
        for (node_name, node) in &self.nodes {
            for ts_data in &node.cost.ts_data {
                check_series(&ts_data, temporals_t, node_name)?;
            }
            check_forecastable_series(&node.inflow, temporals_t, node_name)?;
        }
        for node_diffusion in &self.node_diffusion {
            for ts_data in &node_diffusion.coefficient.ts_data {
                check_series(
                    &ts_data,
                    temporals_t,
                    &format!(
                        "diffusion {}-{}",
                        node_diffusion.node1, node_diffusion.node2
                    ),
                )?;
            }
        }
        for (market_name, market) in &self.markets {
            for ts_data in &market.realisation.ts_data {
                check_series(&ts_data, temporals_t, market_name)?;
            }
            check_forecastable_series(&market.price, temporals_t, market_name)?;
            check_forecastable_series(&market.up_price, temporals_t, market_name)?;
            check_forecastable_series(&market.down_price, temporals_t, market_name)?;
            for ts_data in &market.reserve_activation_price.ts_data {
                check_series(&ts_data, temporals_t, market_name)?;
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Temporals {
    pub t: TimeLine,
    pub dtf: f64,
    pub variable_dt: Option<Vec<(String, f64)>>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Name)]
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
    pub eff_fun: Vec<(f64, f64)>,
}

#[derive(Clone, Debug, Deserialize, GraphQLObject, Name, PartialEq, Serialize)]
pub struct Forecast {
    name: String,
}

impl Forecast {
    pub fn new(name: String) -> Self {
        Forecast { name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum Forecastable {
    Forecast(Forecast),
    TimeSeriesData(TimeSeriesData),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Name)]
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
    pub inflow: Forecastable,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct NodeDiffusion {
    pub node1: String,
    pub node2: String,
    pub coefficient: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct NodeHistory {
    pub node: String,
    pub steps: TimeSeriesData,
}

impl Name for NodeHistory {
    fn name(&self) -> &String {
        &self.node
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Name)]
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
    pub price: Forecastable,
    pub up_price: Forecastable,
    pub down_price: Forecastable,
    pub reserve_activation_price: TimeSeriesData,
    pub fixed: Vec<(String, f64)>,
}

#[derive(Clone, Debug, Deserialize, GraphQLEnum, PartialEq, Serialize)]
pub enum GroupType {
    Node,
    Process,
}

impl Display for GroupType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GroupType::Node => write!(f, "node"),
            GroupType::Process => write!(f, "process"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, GraphQLObject, Name, PartialEq, Serialize)]
pub struct Group {
    pub name: String,
    pub g_type: GroupType,
    pub members: Vec<String>,
}

impl Group {
    pub fn new(name: String, group_type: GroupType) -> Self {
        Group {
            name,
            g_type: group_type,
            members: Vec::new(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Name)]
pub struct InflowBlock {
    pub name: String,
    pub node: String,
    pub start_time: TimeStamp,
    pub data: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct BidSlot {
    pub market: String,
    pub time_steps: TimeLine,
    pub slots: Vec<String>,
    #[serde(deserialize_with = "deserialize_prices")]
    pub prices: BTreeMap<(TimeStamp, String), f64>,
    #[serde(deserialize_with = "deserialize_market_price_allocation")]
    pub market_price_allocation: BTreeMap<(String, TimeStamp), (String, String)>,
}

pub fn parse_time_stamp(key: &str) -> Result<TimeStamp, String> {
    DateTime::parse_from_rfc3339(key.trim_matches(|c| c == '"'))
        .and_then(|ts| Ok(ts.to_utc()))
        .map_err(|error| format!("invalid time format {} for prices: {}", key, error))
}

pub fn deserialize_prices<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<(TimeStamp, String), f64>, D::Error>
where
    D: Deserializer<'de>,
{
    struct PricesVisitor;

    impl<'de> Visitor<'de> for PricesVisitor {
        type Value = BTreeMap<(TimeStamp, String), f64>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a map with (time stamp, string) formatted as tuple (String, String) keys and float values")
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
                let time_stamp =
                    parse_time_stamp(&key[0]).map_err(|error| de::Error::custom(error))?;
                map.insert(
                    (time_stamp, key[1].trim_matches(|c| c == '"').to_string()),
                    value,
                );
            }
            Ok(map)
        }
    }
    deserializer.deserialize_map(PricesVisitor)
}

pub fn deserialize_market_price_allocation<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<(String, TimeStamp), (String, String)>, D::Error>
where
    D: Deserializer<'de>,
{
    struct MarketPriceAllocationVisitor;
    impl<'de> Visitor<'de> for MarketPriceAllocationVisitor {
        type Value = BTreeMap<(String, TimeStamp), (String, String)>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a map with (string, time stamp) formatted as tuple (String, String) and tuple (String, String) values")
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
                    return Err(de::Error::custom(
                        "invalid key format for market_price_allocation",
                    ));
                }
                let time_stamp =
                    parse_time_stamp(&key[1]).map_err(|error| de::Error::custom(error))?;
                map.insert(
                    (key[0].trim_matches(|c| c == '"').to_string(), time_stamp),
                    (value[0].clone(), value[1].clone()),
                );
            }
            Ok(map)
        }
    }
    deserializer.deserialize_map(MarketPriceAllocationVisitor)
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Name)]
pub struct GenConstraint {
    pub name: String,
    pub gc_type: String,
    pub is_setpoint: bool,
    pub penalty: f64,
    pub factors: Vec<ConFactor>,
    pub constant: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
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

#[derive(Clone, Debug, Default, Deserialize, GraphQLObject, PartialEq, Serialize)]
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

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct TimeSeriesData {
    pub ts_data: Vec<TimeSeries>,
}

impl From<Vec<TimeSeries>> for TimeSeriesData {
    fn from(value: Vec<TimeSeries>) -> Self {
        TimeSeriesData { ts_data: value }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct TimeSeries {
    pub scenario: String,
    pub series: BTreeMap<TimeStamp, f64>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct ConFactor {
    pub var_type: String,
    pub var_tuple: (String, String),
    pub data: TimeSeriesData,
}
