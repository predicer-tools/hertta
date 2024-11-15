use chrono::{DateTime, FixedOffset};
use serde::de::{self, MapAccess, Visitor};
use serde::{self, Deserialize, Deserializer, Serialize};
use std::fmt;
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct OptimizationData {
    pub fetch_elec_data: bool,
    pub model_data: Option<InputData>,
    pub time_data: Option<Vec<DateTime<FixedOffset>>>,
    pub weather_data: Option<Vec<TimeSeries>>,
    pub elec_price_data: Option<ElectricityPriceData>,
    pub input_data_batch: Option<Vec<(String, Vec<u8>)>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct InputData {
    pub temporals: Temporals,
    pub setup: InputDataSetup,
    pub processes: BTreeMap<String, Process>,
    pub nodes: BTreeMap<String, Node>,
    pub node_diffusion: Vec<NodeDiffusion>,
    pub node_delay: Vec<(String, String, f64, f64, f64)>,
    pub node_histories: BTreeMap<String, NodeHistory>,
    pub markets: BTreeMap<String, Market>,
    pub groups: BTreeMap<String, Group>,
    pub scenarios: BTreeMap<String, f64>,
    pub reserve_type: BTreeMap<String, f64>,
    pub risk: BTreeMap<String, f64>,
    pub inflow_blocks: BTreeMap<String, InflowBlock>,
    pub bid_slots: BTreeMap<String, BidSlot>,
    pub gen_constraints: BTreeMap<String, GenConstraint>,
}

pub fn find_input_node_names<'a>(nodes: impl Iterator<Item = &'a Node>) -> Vec<String> {
    nodes
        .filter(|node| {
            !node.is_commodity
                && !node.is_market
                && !node.is_state
                && !node.is_res
                && !node.is_inflow
        })
        .map(|node| node.name.clone())
        .collect()
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Temporals {
    pub t: Vec<DateTime<FixedOffset>>,
    pub dtf: f64, // in hours
    pub variable_dt: Option<Vec<(String, f64)>>,
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
    pub eff_fun: Vec<(f64, f64)>,
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
    pub start_time: DateTime<FixedOffset>,
    pub data: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BidSlot {
    pub market: String,
    pub time_steps: Vec<DateTime<FixedOffset>>,
    pub slots: Vec<String>,
    #[serde(deserialize_with = "deserialize_prices")]
    pub prices: BTreeMap<(DateTime<FixedOffset>, String), f64>,
    #[serde(deserialize_with = "deserialize_market_price_allocation")]
    pub market_price_allocation: BTreeMap<(String, DateTime<FixedOffset>), (String, String)>,
}

pub fn parse_time_stamp(key: &str) -> Result<DateTime<FixedOffset>, String> {
    DateTime::parse_from_rfc3339(key.trim_matches(|c| c == '"'))
        .map_err(|error| format!("invalid time format {} for prices: {}", key, error))
}

pub fn deserialize_prices<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<(DateTime<FixedOffset>, String), f64>, D::Error>
where
    D: Deserializer<'de>,
{
    struct PricesVisitor;

    impl<'de> Visitor<'de> for PricesVisitor {
        type Value = BTreeMap<(DateTime<FixedOffset>, String), f64>;

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
) -> Result<BTreeMap<(String, DateTime<FixedOffset>), (String, String)>, D::Error>
where
    D: Deserializer<'de>,
{
    struct MarketPriceAllocationVisitor;
    impl<'de> Visitor<'de> for MarketPriceAllocationVisitor {
        type Value = BTreeMap<(String, DateTime<FixedOffset>), (String, String)>;

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
    pub series: BTreeMap<DateTime<FixedOffset>, f64>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ConFactor {
    pub var_type: String,
    pub var_tuple: (String, String),
    pub data: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct WeatherData {
    pub weather_data: TimeSeriesData,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ElectricityPriceData {
    pub price_data: Option<TimeSeriesData>,
    pub up_price_data: Option<TimeSeriesData>,
    pub down_price_data: Option<TimeSeriesData>,
}

#[cfg(test)]
mod tests {
    use super::*;
    mod find_input_node_names {
        use super::*;
        #[test]
        fn no_nodes_means_no_names() {
            let names = find_input_node_names(Vec::<Node>::new().iter());
            assert!(names.is_empty());
        }
        #[test]
        fn test_non_input_nodes_are_filtered() {
            let commodity_nodes = vec![Node {
                name: "commodity".to_string(),
                is_commodity: true,
                ..Node::default()
            }];
            assert!(find_input_node_names(commodity_nodes.iter()).is_empty());
            let market_nodes = vec![Node {
                name: "market".to_string(),
                is_market: true,
                ..Node::default()
            }];
            assert!(find_input_node_names(market_nodes.iter()).is_empty());
            let state_nodes = vec![Node {
                name: "state".to_string(),
                is_state: true,
                ..Node::default()
            }];
            assert!(find_input_node_names(state_nodes.iter()).is_empty());
            let res_nodes = vec![Node {
                name: "res".to_string(),
                is_res: true,
                ..Node::default()
            }];
            assert!(find_input_node_names(res_nodes.iter()).is_empty());
            let inflow_nodes = vec![Node {
                name: "inflow".to_string(),
                is_inflow: true,
                ..Node::default()
            }];
            assert!(find_input_node_names(inflow_nodes.iter()).is_empty());
        }
        #[test]
        fn true_input_node_gets_found() {
            let input_nodes = vec![Node {
                name: "input".to_string(),
                ..Node::default()
            }];
            let input_nodes = find_input_node_names(input_nodes.iter());
            assert_eq!(input_nodes.len(), 1);
            assert_eq!(input_nodes[0], "input");
        }
    }
}
