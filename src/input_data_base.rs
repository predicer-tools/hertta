use chrono::{DateTime, FixedOffset};
use serde::{self, Deserialize, Serialize};
use std::collections::BTreeMap;
use crate::input_data;
use input_data::TimeSeriesData;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BaseInputData {
    pub setup: BaseInputDataSetup,
    pub processes: BTreeMap<String, BaseProcess>,
    pub nodes: BTreeMap<String, BaseNode>,
    pub node_diffusion: Vec<BaseNodeDiffusion>,
    pub node_delay: Vec<(String, String, f64, f64, f64)>,
    pub node_histories: BTreeMap<String, BaseNodeHistory>,
    pub markets: BTreeMap<String, BaseMarket>,
    pub groups: BTreeMap<String, BaseGroup>,
    pub scenarios: BTreeMap<String, f64>,
    pub reserve_type: BTreeMap<String, f64>,
    pub risk: BTreeMap<String, f64>,
    pub inflow_blocks: BTreeMap<String, BaseInflowBlock>,
    pub bid_slots: BTreeMap<String, BaseBidSlot>,
    pub gen_constraints: BTreeMap<String, BaseGenConstraint>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BaseInputDataSetup {
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
pub struct BaseProcess {
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
    pub topos: Vec<BaseTopology>,
    pub cf: f64,
    pub eff_ts: f64,
    pub eff_ops: Vec<String>,
    pub eff_fun: Vec<(f64, f64)>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BaseNode {
    pub name: String,
    pub groups: Vec<String>,
    pub is_commodity: bool,
    pub is_market: bool,
    pub is_state: bool,
    pub is_res: bool,
    pub is_inflow: bool,
    pub state: Option<BaseState>,
    pub cost: f64, //leave as TimeSeriesData for now, not used in add-on
    pub inflow: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BaseNodeDiffusion {
    pub node1: String,
    pub node2: String,
    pub coefficient: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BaseNodeHistory {
    pub node: String,
    pub steps: f64, 
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BaseMarket {
    pub name: String,
    pub m_type: String,
    pub node: String,
    pub processgroup: String,
    pub direction: String,
    pub realisation: f64, 
    pub reserve_type: String,
    pub is_bid: bool,
    pub is_limited: bool,
    pub min_bid: f64,
    pub max_bid: f64,
    pub fee: f64,
    pub price: f64, 
    pub up_price: f64, 
    pub down_price: f64, 
    pub reserve_activation_price: f64, 
    pub fixed: Vec<(String, f64)>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BaseGroup {
    pub name: String,
    pub g_type: String,
    pub members: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BaseInflowBlock {
    pub name: String,
    pub node: String,
    pub start_time: DateTime<FixedOffset>,
    pub data: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BaseBidSlot {
    pub market: String,
    pub time_steps: Vec<DateTime<FixedOffset>>,
    pub slots: Vec<String>,
    #[serde(deserialize_with = "input_data::deserialize_prices")]
    pub prices: BTreeMap<(DateTime<FixedOffset>, String), f64>,
    #[serde(deserialize_with = "input_data::deserialize_market_price_allocation")]
    pub market_price_allocation: BTreeMap<(String, DateTime<FixedOffset>), (String, String)>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BaseGenConstraint {
    pub name: String,
    pub gc_type: String,
    pub is_setpoint: bool,
    pub penalty: f64,
    pub factors: Vec<BaseConFactor>,
    pub constant: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BaseTopology {
    pub source: String,
    pub sink: String,
    pub capacity: f64,
    pub vom_cost: f64,
    pub ramp_up: f64,
    pub ramp_down: f64,
    pub initial_load: f64,
    pub initial_flow: f64,
    pub cap_ts: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BaseState {
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
pub struct BaseConFactor {
    pub var_type: String,
    pub var_tuple: (String, String),
    pub data: f64,
}