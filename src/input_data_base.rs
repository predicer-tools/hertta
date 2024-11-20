use crate::input_data::{
    ConFactor, GenConstraint, Group, InflowBlock, InputData, InputDataSetup, Market, Name, Node,
    NodeDiffusion, NodeHistory, Process, State, Temporals, TimeSeries, TimeSeriesData, Topology,
};
use crate::{TimeLine, TimeStamp};
use hertta_derive::Name;
use serde::{self, Deserialize, Serialize};
use std::collections::BTreeMap;

pub trait ExpandToTimeSeries {
    type Expanded;
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<String>,
    ) -> Self::Expanded;
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BaseInputData {
    pub setup: InputDataSetup,
    pub processes: BTreeMap<String, BaseProcess>,
    pub nodes: BTreeMap<String, BaseNode>,
    pub node_diffusion: Vec<BaseNodeDiffusion>,
    pub node_delay: Vec<(String, String, f64, f64, f64)>,
    pub node_histories: BTreeMap<String, BaseNodeHistory>,
    pub markets: BTreeMap<String, BaseMarket>,
    pub groups: BTreeMap<String, Group>,
    pub scenarios: BTreeMap<String, f64>,
    pub reserve_type: BTreeMap<String, f64>,
    pub risk: BTreeMap<String, f64>,
    pub inflow_blocks: BTreeMap<String, BaseInflowBlock>,
    pub gen_constraints: BTreeMap<String, BaseGenConstraint>,
}

impl ExpandToTimeSeries for BaseInputData {
    type Expanded = InputData;
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<String>,
    ) -> Self::Expanded {
        InputData {
            temporals: make_temporals(time_line),
            setup: self.setup.clone(),
            processes: self
                .processes
                .iter()
                .map(|(name, process)| {
                    (
                        name.clone(),
                        process.expand_to_time_series(time_line, scenarios),
                    )
                })
                .collect(),
            nodes: self
                .nodes
                .iter()
                .map(|(name, node)| {
                    (
                        name.clone(),
                        node.expand_to_time_series(time_line, scenarios),
                    )
                })
                .collect(),
            node_diffusion: self
                .node_diffusion
                .iter()
                .map(|diffusion| diffusion.expand_to_time_series(time_line, scenarios))
                .collect(),
            node_delay: self.node_delay.clone(),
            node_histories: self
                .node_histories
                .iter()
                .map(|(name, history)| {
                    (
                        name.clone(),
                        history.expand_to_time_series(time_line, scenarios),
                    )
                })
                .collect(),
            markets: self
                .markets
                .iter()
                .map(|(name, market)| {
                    (
                        name.clone(),
                        market.expand_to_time_series(time_line, scenarios),
                    )
                })
                .collect(),
            groups: self.groups.clone(),
            scenarios: self.scenarios.clone(),
            reserve_type: self.reserve_type.clone(),
            risk: self.risk.clone(),
            inflow_blocks: self
                .inflow_blocks
                .iter()
                .map(|(name, block)| {
                    (
                        name.clone(),
                        block.expand_to_time_series(time_line, scenarios),
                    )
                })
                .collect(),
            bid_slots: BTreeMap::new(),
            gen_constraints: self
                .gen_constraints
                .iter()
                .map(|(name, constraint)| {
                    (
                        name.clone(),
                        constraint.expand_to_time_series(time_line, scenarios),
                    )
                })
                .collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Name)]
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

impl ExpandToTimeSeries for BaseProcess {
    type Expanded = Process;
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<String>,
    ) -> Self::Expanded {
        Process {
            name: self.name.clone(),
            groups: self.groups.clone(),
            conversion: self.conversion,
            is_cf: self.is_cf,
            is_cf_fix: self.is_cf_fix,
            is_online: self.is_online,
            is_res: self.is_res,
            eff: self.eff,
            load_min: self.load_min,
            load_max: self.load_max,
            start_cost: self.start_cost,
            min_online: self.min_online,
            min_offline: self.min_offline,
            max_online: self.max_online,
            max_offline: self.max_offline,
            initial_state: self.initial_state,
            is_scenario_independent: self.is_scenario_independent,
            topos: self
                .topos
                .iter()
                .map(|topology| topology.expand_to_time_series(time_line, scenarios))
                .collect(),
            cf: to_time_series(self.cf, time_line, scenarios),
            eff_ts: to_time_series(self.eff_ts, time_line, scenarios),
            eff_ops: self.eff_ops.clone(),
            eff_fun: self.eff_fun.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Name)]
pub struct BaseNode {
    pub name: String,
    pub groups: Vec<String>,
    pub is_commodity: bool,
    pub is_market: bool,
    pub is_state: bool,
    pub is_res: bool,
    pub is_inflow: bool,
    pub state: Option<State>,
    pub cost: f64,
    pub inflow: f64,
}

impl ExpandToTimeSeries for BaseNode {
    type Expanded = Node;
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<String>,
    ) -> Self::Expanded {
        Node {
            name: self.name.clone(),
            groups: self.groups.clone(),
            is_commodity: self.is_commodity,
            is_market: self.is_market,
            is_state: self.is_state,
            is_res: self.is_res,
            is_inflow: self.is_inflow,
            state: self.state.clone(),
            cost: to_time_series(self.cost, time_line, scenarios),
            inflow: to_time_series(self.inflow, time_line, scenarios),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BaseNodeDiffusion {
    pub node1: String,
    pub node2: String,
    pub coefficient: f64,
}

impl ExpandToTimeSeries for BaseNodeDiffusion {
    type Expanded = NodeDiffusion;

    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<String>,
    ) -> Self::Expanded {
        NodeDiffusion {
            node1: self.node1.clone(),
            node2: self.node2.clone(),
            coefficient: to_time_series(self.coefficient, &time_line, scenarios),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BaseNodeHistory {
    pub node: String,
    pub steps: f64,
}

impl Name for BaseNodeHistory {
    fn name(&self) -> &String {
        &self.node
    }
}

impl ExpandToTimeSeries for BaseNodeHistory {
    type Expanded = NodeHistory;
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<String>,
    ) -> Self::Expanded {
        NodeHistory {
            node: self.node.clone(),
            steps: to_time_series(self.steps, time_line, scenarios),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Name)]
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

impl ExpandToTimeSeries for BaseMarket {
    type Expanded = Market;
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<String>,
    ) -> Self::Expanded {
        Market {
            name: self.name.clone(),
            m_type: self.m_type.clone(),
            node: self.node.clone(),
            processgroup: self.processgroup.clone(),
            direction: self.direction.clone(),
            realisation: to_time_series(self.realisation, time_line, scenarios),
            reserve_type: self.reserve_type.clone(),
            is_bid: self.is_bid,
            is_limited: self.is_limited,
            min_bid: self.min_bid,
            max_bid: self.max_bid,
            fee: self.fee,
            price: to_time_series(self.price, time_line, scenarios),
            up_price: to_time_series(self.up_price, time_line, scenarios),
            down_price: to_time_series(self.down_price, time_line, scenarios),
            reserve_activation_price: to_time_series(
                self.reserve_activation_price,
                time_line,
                scenarios,
            ),
            fixed: self.fixed.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Name)]
pub struct BaseInflowBlock {
    pub name: String,
    pub node: String,
    pub data: f64,
}

impl ExpandToTimeSeries for BaseInflowBlock {
    type Expanded = InflowBlock;
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<String>,
    ) -> Self::Expanded {
        InflowBlock {
            name: self.name.clone(),
            node: self.node.clone(),
            start_time: time_line[0].clone(),
            data: to_time_series(self.data, time_line, scenarios),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Name)]
pub struct BaseGenConstraint {
    pub name: String,
    pub gc_type: String,
    pub is_setpoint: bool,
    pub penalty: f64,
    pub factors: Vec<BaseConFactor>,
    pub constant: f64,
}

impl ExpandToTimeSeries for BaseGenConstraint {
    type Expanded = GenConstraint;
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<String>,
    ) -> Self::Expanded {
        GenConstraint {
            name: self.name.clone(),
            gc_type: self.gc_type.clone(),
            is_setpoint: self.is_setpoint,
            penalty: self.penalty,
            factors: self
                .factors
                .iter()
                .map(|factor| factor.expand_to_time_series(time_line, scenarios))
                .collect(),
            constant: to_time_series(self.constant, time_line, scenarios),
        }
    }
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

impl ExpandToTimeSeries for BaseTopology {
    type Expanded = Topology;
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<String>,
    ) -> Self::Expanded {
        Topology {
            source: self.source.clone(),
            sink: self.sink.clone(),
            capacity: self.capacity,
            vom_cost: self.vom_cost,
            ramp_up: self.ramp_up,
            ramp_down: self.ramp_down,
            initial_load: self.initial_load,
            initial_flow: self.initial_flow,
            cap_ts: to_time_series(self.cap_ts, time_line, scenarios),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BaseConFactor {
    pub var_type: String,
    pub var_tuple: (String, String),
    pub data: f64,
}

impl ExpandToTimeSeries for BaseConFactor {
    type Expanded = ConFactor;
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<String>,
    ) -> Self::Expanded {
        ConFactor {
            var_type: self.var_type.clone(),
            var_tuple: self.var_tuple.clone(),
            data: to_time_series(self.data, time_line, scenarios),
        }
    }
}

fn to_time_series(y: f64, time_line: &TimeLine, scenarios: &Vec<String>) -> TimeSeriesData {
    let single_series: BTreeMap<TimeStamp, f64> = time_line
        .iter()
        .map(|time_stamp| (time_stamp.clone(), y))
        .collect();
    TimeSeriesData {
        ts_data: scenarios
            .iter()
            .map(|scenario| TimeSeries {
                scenario: scenario.clone(),
                series: single_series.clone(),
            })
            .collect(),
    }
}

fn make_temporals(time_line: &TimeLine) -> Temporals {
    Temporals {
        t: time_line.clone(),
        dtf: (time_line[1] - time_line[0]).num_seconds() as f64 / 3600.0,
        variable_dt: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input_data::BidSlot;
    use chrono::{TimeZone, Utc};
    fn as_map<T: Name>(x: T) -> BTreeMap<String, T> {
        let mut map = BTreeMap::new();
        map.insert(x.name().clone(), x);
        map
    }
    #[test]
    fn expanding_input_data_works() {
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios = vec!["S1".to_string()];
        let setup = InputDataSetup::default();
        let base_topology = BaseTopology {
            source: "Source".to_string(),
            sink: "Sink".to_string(),
            capacity: 1.1,
            vom_cost: 1.2,
            ramp_up: 1.3,
            ramp_down: 1.4,
            initial_load: 1.5,
            initial_flow: 1.6,
            cap_ts: 1.7,
        };
        let base_process = BaseProcess {
            name: "Conversion".to_string(),
            groups: vec!["Group".to_string()],
            conversion: 23,
            is_cf: true,
            is_cf_fix: false,
            is_online: true,
            is_res: false,
            eff: 1.2,
            load_min: 1.3,
            load_max: 1.4,
            start_cost: 1.5,
            min_online: 1.6,
            min_offline: 1.7,
            max_online: 1.8,
            max_offline: 1.9,
            initial_state: true,
            is_scenario_independent: false,
            topos: vec![base_topology],
            cf: 2.0,
            eff_ts: 2.1,
            eff_ops: vec!["oops!".to_string()],
            eff_fun: vec![(2.2, 2.3)],
        };
        let process = base_process.expand_to_time_series(&time_line, &scenarios);
        let base_node = BaseNode {
            name: "East".to_string(),
            groups: vec!["Group".to_string()],
            is_commodity: true,
            is_market: false,
            is_state: true,
            is_res: false,
            is_inflow: true,
            state: None,
            cost: 1.1,
            inflow: 1.2,
        };
        let node = base_node.expand_to_time_series(&time_line, &scenarios);
        let base_node_diffusion = BaseNodeDiffusion {
            node1: "Node 1".to_string(),
            node2: "Node 2".to_string(),
            coefficient: -2.3,
        };
        let node_diffusion = base_node_diffusion.expand_to_time_series(&time_line, &scenarios);
        let node_delay = vec![("South".to_string(), "North".to_string(), 3.1, 3.2, 3.3)];
        let base_node_history = BaseNodeHistory {
            node: "South".to_string(),
            steps: 1.1,
        };
        let node_history = base_node_history.expand_to_time_series(&time_line, &scenarios);
        let base_market = BaseMarket {
            name: "Market".to_string(),
            m_type: "energy".to_string(),
            node: "North".to_string(),
            processgroup: "Group".to_string(),
            direction: "none".to_string(),
            realisation: 1.1,
            reserve_type: "not none".to_string(),
            is_bid: true,
            is_limited: false,
            min_bid: 1.2,
            max_bid: 1.3,
            fee: 1.4,
            price: 1.5,
            up_price: 1.6,
            down_price: 1.7,
            reserve_activation_price: 1.8,
            fixed: vec![("Fix".to_string(), 1.9)],
        };
        let market = base_market.expand_to_time_series(&time_line, &scenarios);
        let group = Group::default();
        let mut scenario_map = BTreeMap::new();
        scenario_map.insert("S1".to_string(), 1.0);
        let mut reserve_type = BTreeMap::new();
        reserve_type.insert("infinite".to_string(), 4.1);
        let mut risk = BTreeMap::new();
        risk.insert("high".to_string(), 0.99);
        let base_inflow_block = BaseInflowBlock {
            name: "Inflow".to_string(),
            node: "West".to_string(),
            data: 2.3,
        };
        let inflow_block = base_inflow_block.expand_to_time_series(&time_line, &scenarios);
        let base_con_factor = BaseConFactor {
            var_type: "state".to_string(),
            var_tuple: ("interior_air".to_string(), String::new()),
            data: 23.0,
        };
        let base_gen_constraint = BaseGenConstraint {
            name: "Constraint".to_string(),
            gc_type: "gt".to_string(),
            is_setpoint: true,
            penalty: 1.1,
            factors: vec![base_con_factor],
            constant: 1.2,
        };
        let gen_constraint = base_gen_constraint.expand_to_time_series(&time_line, &scenarios);
        let base = BaseInputData {
            setup: setup.clone(),
            processes: as_map(base_process),
            nodes: as_map(base_node),
            node_diffusion: vec![base_node_diffusion],
            node_delay: node_delay.clone(),
            node_histories: as_map(base_node_history),
            markets: as_map(base_market),
            groups: as_map(group.clone()),
            scenarios: scenario_map.clone(),
            reserve_type: reserve_type.clone(),
            risk: risk.clone(),
            inflow_blocks: as_map(base_inflow_block),
            gen_constraints: as_map(base_gen_constraint),
        };
        let input_data = base.expand_to_time_series(&time_line, &scenarios);
        let temporals = Temporals {
            t: time_line,
            dtf: 1.0,
            variable_dt: None,
        };
        assert_eq!(input_data.temporals, temporals);
        assert_eq!(input_data.setup, setup);
        assert_eq!(input_data.processes, as_map(process));
        assert_eq!(input_data.nodes, as_map(node));
        assert_eq!(input_data.node_diffusion, vec![node_diffusion]);
        assert_eq!(input_data.node_delay, node_delay);
        assert_eq!(input_data.node_histories, as_map(node_history));
        assert_eq!(input_data.markets, as_map(market));
        assert_eq!(input_data.groups, as_map(group));
        assert_eq!(input_data.scenarios, scenario_map);
        assert_eq!(input_data.reserve_type, reserve_type);
        assert_eq!(input_data.risk, risk);
        assert_eq!(input_data.inflow_blocks, as_map(inflow_block));
        assert_eq!(input_data.bid_slots, BTreeMap::<String, BidSlot>::new());
        assert_eq!(input_data.gen_constraints, as_map(gen_constraint));
    }
    #[test]
    fn expanding_process_works() {
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios = vec!["S1".to_string()];
        let base_topology = BaseTopology {
            source: "Source".to_string(),
            sink: "Sink".to_string(),
            capacity: 1.1,
            vom_cost: 1.2,
            ramp_up: 1.3,
            ramp_down: 1.4,
            initial_load: 1.5,
            initial_flow: 1.6,
            cap_ts: 1.7,
        };
        let topology = base_topology.expand_to_time_series(&time_line, &scenarios);
        let base = BaseProcess {
            name: "Conversion".to_string(),
            groups: vec!["Group".to_string()],
            conversion: 23,
            is_cf: true,
            is_cf_fix: false,
            is_online: true,
            is_res: false,
            eff: 1.2,
            load_min: 1.3,
            load_max: 1.4,
            start_cost: 1.5,
            min_online: 1.6,
            min_offline: 1.7,
            max_online: 1.8,
            max_offline: 1.9,
            initial_state: true,
            is_scenario_independent: false,
            topos: vec![base_topology],
            cf: 2.0,
            eff_ts: 2.1,
            eff_ops: vec!["oops!".to_string()],
            eff_fun: vec![(2.2, 2.3)],
        };
        let process = base.expand_to_time_series(&time_line, &scenarios);
        assert_eq!(process.name, "Conversion");
        assert_eq!(process.groups, vec!["Group".to_string()]);
        assert_eq!(process.conversion, 23);
        assert!(process.is_cf);
        assert!(!process.is_cf_fix);
        assert!(process.is_online);
        assert!(!process.is_res);
        assert_eq!(process.eff, 1.2);
        assert_eq!(process.load_min, 1.3);
        assert_eq!(process.load_max, 1.4);
        assert_eq!(process.start_cost, 1.5);
        assert_eq!(process.min_online, 1.6);
        assert_eq!(process.min_offline, 1.7);
        assert_eq!(process.max_online, 1.8);
        assert_eq!(process.max_offline, 1.9);
        assert!(process.initial_state);
        assert!(!process.is_scenario_independent);
        assert_eq!(process.topos, vec![topology]);
        assert_eq!(process.cf, to_time_series(2.0, &time_line, &scenarios));
        assert_eq!(process.eff_ts, to_time_series(2.1, &time_line, &scenarios));
        assert_eq!(process.eff_ops, vec!["oops!".to_string()]);
        assert_eq!(process.eff_fun, vec![(2.2, 2.3)]);
    }
    #[test]
    fn expanding_node_works() {
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios = vec!["S1".to_string()];
        let base = BaseNode {
            name: "East".to_string(),
            groups: vec!["Group".to_string()],
            is_commodity: true,
            is_market: false,
            is_state: true,
            is_res: false,
            is_inflow: true,
            state: None,
            cost: 1.1,
            inflow: 1.2,
        };
        let node = base.expand_to_time_series(&time_line, &scenarios);
        assert_eq!(node.name, "East");
        assert_eq!(node.groups, vec!["Group".to_string()]);
        assert!(node.is_commodity);
        assert!(!node.is_market);
        assert!(node.is_state);
        assert!(!node.is_res);
        assert!(node.is_inflow);
        assert!(node.state.is_none());
        assert_eq!(node.inflow, to_time_series(1.2, &time_line, &scenarios));
    }
    #[test]
    fn expanding_node_diffusion_works() {
        let base = BaseNodeDiffusion {
            node1: "Node 1".to_string(),
            node2: "Node 2".to_string(),
            coefficient: -2.3,
        };
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios = vec!["S1".to_string()];
        let node_diffusion = base.expand_to_time_series(&time_line, &scenarios);
        assert_eq!(node_diffusion.node1, "Node 1");
        assert_eq!(node_diffusion.node2, "Node 2");
        assert_eq!(
            node_diffusion.coefficient,
            to_time_series(-2.3, &time_line, &scenarios)
        );
    }
    #[test]
    fn expanding_node_history_works() {
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios = vec!["S1".to_string()];
        let base = BaseNodeHistory {
            node: "South".to_string(),
            steps: 1.1,
        };
        let node_history = base.expand_to_time_series(&time_line, &scenarios);
        assert_eq!(node_history.node, "South");
        assert_eq!(
            node_history.steps,
            to_time_series(1.1, &time_line, &scenarios)
        );
    }
    #[test]
    fn expanding_market_works() {
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios = vec!["S1".to_string()];
        let base = BaseMarket {
            name: "Market".to_string(),
            m_type: "energy".to_string(),
            node: "North".to_string(),
            processgroup: "Group".to_string(),
            direction: "none".to_string(),
            realisation: 1.1,
            reserve_type: "not none".to_string(),
            is_bid: true,
            is_limited: false,
            min_bid: 1.2,
            max_bid: 1.3,
            fee: 1.4,
            price: 1.5,
            up_price: 1.6,
            down_price: 1.7,
            reserve_activation_price: 1.8,
            fixed: vec![("Fix".to_string(), 1.9)],
        };
        let market = base.expand_to_time_series(&time_line, &scenarios);
        assert_eq!(market.name, "Market");
        assert_eq!(market.m_type, "energy");
        assert_eq!(market.node, "North");
        assert_eq!(market.processgroup, "Group");
        assert_eq!(market.direction, "none");
        assert_eq!(
            market.realisation,
            to_time_series(1.1, &time_line, &scenarios)
        );
        assert_eq!(market.reserve_type, "not none");
        assert!(market.is_bid);
        assert!(!market.is_limited);
        assert_eq!(market.min_bid, 1.2);
        assert_eq!(market.max_bid, 1.3);
        assert_eq!(market.fee, 1.4);
        assert_eq!(market.price, to_time_series(1.5, &time_line, &scenarios));
        assert_eq!(market.up_price, to_time_series(1.6, &time_line, &scenarios));
        assert_eq!(
            market.down_price,
            to_time_series(1.7, &time_line, &scenarios)
        );
        assert_eq!(
            market.reserve_activation_price,
            to_time_series(1.8, &time_line, &scenarios)
        );
        assert_eq!(market.fixed, vec![("Fix".to_string(), 1.9)])
    }
    #[test]
    fn expanding_inflow_block_works() {
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios = vec!["S1".to_string()];
        let base = BaseInflowBlock {
            name: "Inflow".to_string(),
            node: "West".to_string(),
            data: 2.3,
        };
        let inflow_block = base.expand_to_time_series(&time_line, &scenarios);
        assert_eq!(inflow_block.name, "Inflow");
        assert_eq!(inflow_block.node, "West");
        assert_eq!(inflow_block.start_time, time_line[0]);
        assert_eq!(
            inflow_block.data,
            to_time_series(2.3, &time_line, &scenarios)
        );
    }
    #[test]
    fn expanding_gen_constraing_works() {
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios = vec!["S1".to_string()];
        let base_con_factor = BaseConFactor {
            var_type: "state".to_string(),
            var_tuple: ("interior_air".to_string(), String::new()),
            data: 23.0,
        };
        let con_factor = base_con_factor.expand_to_time_series(&time_line, &scenarios);
        let base = BaseGenConstraint {
            name: "Constraint".to_string(),
            gc_type: "gt".to_string(),
            is_setpoint: true,
            penalty: 1.1,
            factors: vec![base_con_factor],
            constant: 1.2,
        };
        let gen_constraint = base.expand_to_time_series(&time_line, &scenarios);
        assert_eq!(gen_constraint.name, "Constraint");
        assert_eq!(gen_constraint.gc_type, "gt");
        assert!(gen_constraint.is_setpoint);
        assert_eq!(gen_constraint.penalty, 1.1);
        assert_eq!(gen_constraint.factors, vec![con_factor]);
        assert_eq!(
            gen_constraint.constant,
            to_time_series(1.2, &time_line, &scenarios)
        );
    }
    #[test]
    fn expanding_topology_works() {
        let base = BaseTopology {
            source: "Source".to_string(),
            sink: "Sink".to_string(),
            capacity: 1.1,
            vom_cost: 1.2,
            ramp_up: 1.3,
            ramp_down: 1.4,
            initial_load: 1.5,
            initial_flow: 1.6,
            cap_ts: 1.7,
        };
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios = vec!["S1".to_string()];
        let topology = base.expand_to_time_series(&time_line, &scenarios);
        assert_eq!(topology.source, "Source");
        assert_eq!(topology.sink, "Sink");
        assert_eq!(topology.capacity, 1.1);
        assert_eq!(topology.vom_cost, 1.2);
        assert_eq!(topology.ramp_up, 1.3);
        assert_eq!(topology.ramp_down, 1.4);
        assert_eq!(topology.initial_load, 1.5);
        assert_eq!(topology.initial_flow, 1.6);
        assert_eq!(topology.cap_ts, to_time_series(1.7, &time_line, &scenarios));
    }
    #[test]
    fn expanding_con_factor_works() {
        let base = BaseConFactor {
            var_type: "state".to_string(),
            var_tuple: ("interior_air".to_string(), String::new()),
            data: 23.0,
        };
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios = vec!["S1".to_string()];
        let con_factor = base.expand_to_time_series(&time_line, &scenarios);
        assert_eq!(con_factor.var_type, "state");
        assert_eq!(
            con_factor.var_tuple,
            ("interior_air".to_string(), "".to_string())
        );
        assert_eq!(
            con_factor.data,
            to_time_series(base.data, &time_line, &scenarios)
        );
    }
    #[test]
    fn make_temporals_works() {
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 45, 0)
                .unwrap()
                .into(),
        ];
        let temporals = make_temporals(&time_line);
        assert_eq!(temporals.t, time_line);
        assert_eq!(temporals.dtf, 0.75);
        assert!(temporals.variable_dt.is_none());
    }
    #[test]
    fn to_time_series_works() {
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios = vec!["S1".to_string()];
        let time_series = to_time_series(2.3, &time_line, &scenarios);
        assert_eq!(time_series.ts_data.len(), 1);
        assert_eq!(time_series.ts_data[0].scenario, "S1");
        let mut expected_series = BTreeMap::new();
        expected_series.insert(
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0)
                .unwrap()
                .fixed_offset(),
            2.3,
        );
        expected_series.insert(
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0)
                .unwrap()
                .fixed_offset(),
            2.3,
        );
        assert_eq!(time_series.ts_data[0].series, expected_series);
    }
}
