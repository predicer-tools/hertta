use crate::graphql::HerttaContext;
use crate::input_data::{
    ConFactor, Forecast, Forecastable, GenConstraint, Group, GroupType, InflowBlock, InputData,
    InputDataSetup, Market, Name, Node, NodeDiffusion, NodeHistory, Process, State, Temporals,
    TimeSeries, TimeSeriesData, Topology,
};
use crate::scenarios::Scenario;
use crate::time_line_settings::Duration;
use crate::{TimeLine, TimeStamp};
use hertta_derive::{Members, Name};
use juniper::{graphql_object, FieldResult, GraphQLEnum, GraphQLObject, GraphQLUnion, GraphQLInputObject};
use serde::{self, Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::convert::TryFrom;
use indexmap::IndexMap;

pub trait TypeName {
    fn type_name() -> &'static str;
}

#[derive(GraphQLInputObject, Clone, Debug, Default, Deserialize, Serialize)]
pub struct ValueInput {
    pub scenario: Option<String>, 
    pub constant: Option<f64>,  
    pub series: Option<Vec<f64>>, 
}

#[derive(GraphQLInputObject, Clone, Debug, Default, Deserialize, Serialize)]
pub struct ForecastValueInput {
    pub scenario: Option<String>,
    pub constant: Option<f64>,
    pub series: Option<Vec<f64>>,
    pub forecast: Option<String>,
}

#[derive(GraphQLObject, Clone, Debug, Deserialize, Serialize)]
pub struct ForecastValue {
    pub scenario: Option<String>,
    pub value: BaseForecastable,
}

impl TryFrom<ForecastValueInput> for ForecastValue {
    type Error = String;

    fn try_from(input: ForecastValueInput) -> Result<Self, Self::Error> {
        let scenario = input.scenario;

        let value = if let Some(forecast) = input.forecast {
            BaseForecastable::Forecast(Forecast::new(forecast))
        } else {
            match (input.constant, input.series) {
                (Some(c), None) => BaseForecastable::Constant(Constant { value: c }),
                (None, Some(series)) => BaseForecastable::FloatList(FloatList { values: series }),
                (Some(_), Some(_)) => {
                    return Err("ForecastValueInput cannot have both `constant` and `series` populated simultaneously.".to_string())
                }
                (None, None) => {
                    return Err("ForecastValueInput requires one of `forecast`, `constant`, or `series` to be provided.".to_string())
                }
            }
        };

        Ok(ForecastValue { scenario, value })
    }
}

#[derive(GraphQLObject, Clone, Debug, Deserialize, Serialize)]
pub struct Value {
    pub scenario: Option<String>, 
    pub value: SeriesValue,
}

impl TryFrom<ValueInput> for Value {
    type Error = String;

    fn try_from(input: ValueInput) -> Result<Self, Self::Error> {
        let scenario = input.scenario;

        let value = match (input.constant, input.series) {
            (Some(constant), None) => SeriesValue::Constant(Constant { value: constant }),
            (None, Some(series)) => SeriesValue::FloatList(FloatList { values: series }),
            (Some(_), Some(_)) => {
                return Err(
                    "ValueInput cannot have both `constant` and `series` populated simultaneously."
                        .to_string(),
                );
            }
            (None, None) => SeriesValue::Constant(Constant { value: 0.0 }),
        };

        Ok(Value { scenario, value })
    }
}

#[derive(Clone, Debug, Deserialize, GraphQLObject, Serialize)]
pub struct Constant {
    value: f64,
}

impl Constant {
    pub fn new(value: f64) -> Self {
        Constant { value }
    }

    pub fn value(&self) -> f64 {
        self.value
    }
}

impl From<f64> for Constant {
    fn from(value: f64) -> Self {
        Constant { value }
    }
}

impl From<&f64> for Constant {
    fn from(value: &f64) -> Self {
        Constant { value: *value }
    }
}

#[derive(Clone, Debug, Deserialize, GraphQLObject, Serialize)]
pub struct FloatList {
    values: Vec<f64>,
}

#[derive(Clone, Debug, GraphQLUnion, Deserialize, Serialize)]
pub enum SeriesValue {
    Constant(Constant),
    FloatList(FloatList),
}

fn convert_value_to_series(value: &Value, timeline: &TimeLine) -> Result<BTreeMap<TimeStamp, f64>, String> {
    match &value.value {
        SeriesValue::Constant(constant) => {
            let series: BTreeMap<TimeStamp, f64> = timeline
                .iter()
                .cloned()
                .map(|ts| (ts, constant.value))
                .collect();
            Ok(series)
        },
        SeriesValue::FloatList(float_list) => {
            if float_list.values.len() != timeline.len() {
                return Err(format!(
                    "time series mismatch in FloatList, expected length {}, found {}",
                    timeline.len(),
                    float_list.values.len()
                ));
            }
            let series: BTreeMap<TimeStamp, f64> = timeline
                .iter()
                .cloned()
                .zip(float_list.values.iter().cloned())
                .collect();
            Ok(series)
        },
    }
}

fn values_to_time_series_data(
    values: Vec<Value>, 
    scenarios: Vec<Scenario>, 
    timeline: TimeLine
) -> Result<TimeSeriesData, String> {
    if values.is_empty() {
        return Ok(TimeSeriesData { ts_data: Vec::new() });
    }

    let default_values: Vec<&Value> = values.iter()
        .filter(|v| v.scenario.is_none())
        .collect();

    if default_values.len() > 1 {
        return Err("Multiple default values found (scenario = None)".to_string());
    }
    
    let default_series_option = if let Some(default_value) = default_values.first() {
        Some(convert_value_to_series(default_value, &timeline)?)
    } else {
        None
    };

    let mut ts_vec: Vec<TimeSeries> = Vec::new();
    let mut scenarios_with_values: HashSet<String> = HashSet::new();

    for value in &values {
        if let Some(scenario_name) = &value.scenario {
            if !scenarios.iter().any(|s| s.name() == scenario_name) {
                return Err(format!(
                    "Scenario '{}' specified in Value does not exist in provided scenarios",
                    scenario_name
                ));
            }
            scenarios_with_values.insert(scenario_name.clone());
            let series = convert_value_to_series(value, &timeline)?;
            let ts = TimeSeries {
                scenario: scenario_name.clone(),
                series,
            };
            ts_vec.push(ts);
        }
    }

    if let Some(default_series) = default_series_option {
        for scenario in &scenarios {
            if !scenarios_with_values.contains(scenario.name().as_str()) {
                let ts = TimeSeries {
                    scenario: scenario.name().clone(),
                    series: default_series.clone(),
                };
                ts_vec.push(ts);
            }
        }
    } else {
        let missing_scenarios: Vec<String> = scenarios.iter()
            .filter(|s| !scenarios_with_values.contains(s.name().as_str()))
            .map(|s| s.name().clone())
            .collect();
        if !missing_scenarios.is_empty() {
            return Err(format!(
                "Missing default values for scenarios: {:?}",
                missing_scenarios
            ));
        }
    }
    
    let ts_data = TimeSeriesData::from(ts_vec);
    Ok(ts_data)
}

fn convert_forecast_value_to_series(
    fv: &ForecastValue,
    timeline: &TimeLine,
) -> Result<BTreeMap<TimeStamp, f64>, String> {
    match &fv.value {
        BaseForecastable::Constant(constant) => {
            let series: BTreeMap<TimeStamp, f64> = timeline
                .iter()
                .cloned()
                .map(|ts| (ts, constant.value))
                .collect();
            Ok(series)
        },
        BaseForecastable::FloatList(float_list) => {
            if float_list.values.len() != timeline.len() {
                return Err(format!(
                    "time series mismatch in FloatList, expected length {}, found {}",
                    timeline.len(),
                    float_list.values.len()
                ));
            }
            let series: BTreeMap<TimeStamp, f64> = timeline
                .iter()
                .cloned()
                .zip(float_list.values.iter().cloned())
                .collect();
            Ok(series)
        },
        BaseForecastable::Forecast(_forecast) => {
            Err("Cannot convert Forecast variant to time series".to_string())
        }
    }
}

pub fn forecast_values_to_time_series_data(
    forecast_values: &Vec<ForecastValue>,
    scenarios: &Vec<Scenario>,
    timeline: &TimeLine,
) -> Result<TimeSeriesData, String> {
    if forecast_values.is_empty() {
        return Ok(TimeSeriesData { ts_data: Vec::new() });
    }
    let default_values: Vec<&ForecastValue> = forecast_values
        .iter()
        .filter(|fv| fv.scenario.is_none())
        .collect();
    if default_values.len() > 1 {
        return Err("Multiple default forecast values found (scenario = None)".to_string());
    }
    let default_series_option: Option<BTreeMap<TimeStamp, f64>> =
        if let Some(default_value) = default_values.first() {
            Some(convert_forecast_value_to_series(default_value, timeline)?)
        } else {
            None
        };

    let mut ts_vec: Vec<TimeSeries> = Vec::new();
    let mut scenarios_with_values: HashSet<String> = HashSet::new();

    for fv in forecast_values.iter() {
        if fv.scenario.is_none() {
            let series = convert_forecast_value_to_series(fv, timeline)?;
            ts_vec.push(TimeSeries {
                scenario: "Default".to_string(),
                series,
            });
        } else {
            let scenario_name = fv.scenario.as_ref().unwrap();
            if !scenarios.iter().any(|s| s.name() == scenario_name) {
                return Err(format!(
                    "Scenario '{}' specified in ForecastValue does not exist in provided scenarios",
                    scenario_name
                ));
            }
            scenarios_with_values.insert(scenario_name.clone());
            let series = convert_forecast_value_to_series(fv, timeline)?;
            ts_vec.push(TimeSeries {
                scenario: scenario_name.clone(),
                series,
            });
        }
    }

    if let Some(default_series) = default_series_option {
        for scenario in scenarios.iter() {
            if !scenarios_with_values.contains(scenario.name().as_str()) {
                ts_vec.push(TimeSeries {
                    scenario: scenario.name().clone(),
                    series: default_series.clone(),
                });
            }
        }
    } else {
        for scenario in scenarios.iter() {
            if !scenarios_with_values.contains(scenario.name().as_str()) {
                return Err(format!("Missing forecast value for scenario {}", scenario.name()));
            }
        }
    }
    Ok(TimeSeriesData::from(ts_vec))
}

pub fn forecast_values_to_forecastable(
    forecast_values: &Vec<ForecastValue>,
    scenarios: &Vec<Scenario>,
    timeline: &TimeLine,
) -> Forecastable {

    if let Some(fv) = forecast_values.iter().find(|fv| {
        matches!(fv.value, BaseForecastable::Forecast(_))
    }) {
        if let BaseForecastable::Forecast(ref forecast) = fv.value {
            return Forecastable::Forecast(forecast.clone());
        }
    }

    let ts_data = forecast_values_to_time_series_data(forecast_values, scenarios, timeline)
        .unwrap_or_else(|err| {
            panic!("Error converting forecast values to time series data: {}", err)
        });
    Forecastable::TimeSeriesData(ts_data)
}

pub trait ExpandToTimeSeries {
    type Expanded;
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<Scenario>,
    ) -> Self::Expanded;
}

fn use_name_as_key<T: Clone + Name>(x: &T) -> (String, T) {
    (x.name().clone(), x.clone())
}

fn expand_and_use_name_as_key<T: ExpandToTimeSeries + Name>(
    x: &T,
    time_line: &TimeLine,
    scenarios: &Vec<Scenario>,
) -> (String, T::Expanded) {
    (
        x.name().clone(),
        x.expand_to_time_series(time_line, scenarios),
    )
}

pub trait GroupMember {
    fn groups(&self) -> &Vec<String>;
    fn groups_mut(&mut self) -> &mut Vec<String>;
}

#[derive(Clone, Debug, Default, Deserialize, GraphQLObject, Serialize)]
#[graphql(name = "InputData", description = "The model itself.", context = HerttaContext)]
pub struct BaseInputData {
    pub scenarios: Vec<Scenario>,
    pub setup: BaseInputDataSetup,
    pub processes: Vec<BaseProcess>,
    pub nodes: Vec<BaseNode>,
    pub node_diffusion: Vec<BaseNodeDiffusion>,
    pub node_delay: Vec<Delay>,
    pub node_histories: Vec<BaseNodeHistory>,
    pub markets: Vec<BaseMarket>,
    pub node_groups: Vec<NodeGroup>,
    pub process_groups: Vec<ProcessGroup>,
    pub reserve_type: Vec<ReserveType>,
    pub risk: Vec<Risk>,
    pub inflow_blocks: Vec<BaseInflowBlock>,
    pub gen_constraints: Vec<BaseGenConstraint>,
}

#[derive(Clone, Debug, Deserialize, GraphQLObject, Serialize)]
pub struct Series {
    pub scenario: String,
    pub durations: Vec<Duration>,
    pub values: Vec<f64>,
}

impl Series {
    fn to_time_series(&self, time_line: &TimeLine) -> TimeSeries {
        if self.values.is_empty() || time_line.is_empty() {
            return TimeSeries {
                scenario: self.scenario.clone(),
                series: BTreeMap::new(),
            };
        }
        let mut time_series: BTreeMap<TimeStamp, f64> = BTreeMap::new();
        let mut time_line_iter = time_line.iter();
        let mut epoch_start = time_line_iter.next().unwrap().clone();
        let mut value_iter = self.values.iter();
        let mut epoch_value = *value_iter.next().unwrap();
        time_series.insert(epoch_start.clone(), epoch_value);
        let mut duration_iter = self.durations.iter();
        let mut epoch_end = epoch_start + duration_iter.next().unwrap().to_time_delta();
        'collect_loop: for stamp in time_line_iter {
            while *stamp >= epoch_end {
                epoch_start = epoch_end;
                epoch_value = match value_iter.next() {
                    Some(value) => *value,
                    None => break 'collect_loop,
                };
                epoch_end = epoch_start + duration_iter.next().unwrap().to_time_delta();
            }
            time_series.insert(stamp.clone(), epoch_value);
        }
        TimeSeries {
            scenario: self.scenario.clone(),
            series: time_series,
        }
    }
    fn to_time_series_data(serieses: &Vec<Series>, time_line: &TimeLine) -> TimeSeriesData {
        let data = serieses
            .iter()
            .map(|s| s.to_time_series(time_line))
            .collect();
        TimeSeriesData { ts_data: data }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Delay {
    pub from_node: String,
    pub to_node: String,
    pub delay: f64,
    pub min_delay_flow: f64,
    pub max_delay_flow: f64,
}

#[graphql_object]
#[graphql(description = "Delay for connections between nodes.", context = HerttaContext)]
impl Delay {
    async fn from_node(&self, context: &HerttaContext) -> FieldResult<BaseNode> {
        let model = context.model().lock().await;
        find_node(&self.from_node, "from_node", &model.input_data.nodes)
    }
    async fn to_node(&self, context: &HerttaContext) -> FieldResult<BaseNode> {
        let model = context.model().lock().await;
        find_node(&self.to_node, "to_node", &model.input_data.nodes)
    }
    fn delay(&self) -> f64 {
        self.delay
    }
    fn min_delay_flow(&self) -> f64 {
        self.min_delay_flow
    }
    fn max_delay_flow(&self) -> f64 {
        self.max_delay_flow
    }
}

impl Delay {
    pub fn to_tuple(&self) -> (String, String, f64, f64, f64) {
        (
            self.from_node.clone(),
            self.to_node.clone(),
            self.delay,
            self.min_delay_flow,
            self.max_delay_flow,
        )
    }
}

#[derive(Clone, Debug, GraphQLObject, Deserialize, Serialize)]
pub struct ReserveType {
    pub name: String,
    pub ramp_rate: f64,
}

impl ReserveType {
    fn to_indexmap(reserve_types: &Vec<Self>) -> IndexMap<String, f64> {
        reserve_types
            .iter()
            .map(|reserve| (reserve.name.clone(), reserve.ramp_rate))
            .collect()
    }
}

#[derive(Clone, Debug, GraphQLObject, Deserialize, Serialize)]
pub struct Risk {
    pub parameter: String,
    pub value: f64,
}

impl Name for Risk {
    fn name(&self) -> &String {
        &self.parameter
    }
}

impl TypeName for Risk {
    fn type_name() -> &'static str {
        "risk"
    }
}

impl Risk {
    fn to_indexmap(risks: &Vec<Self>) -> IndexMap<String, f64> {
        risks
            .iter()
            .map(|risk| (risk.parameter.clone(), risk.value))
            .collect()
    }
}

impl BaseInputData {
    pub fn expand_to_time_series(&self, time_line: &TimeLine) -> InputData {
        let mut groups = Vec::with_capacity(self.node_groups.len() + self.process_groups.len());
        groups.extend(self.node_groups.iter().map(|g| Group::from(g)));
        groups.extend(self.process_groups.iter().map(|g| Group::from(g)));
        InputData {
            temporals: make_temporals(time_line),
            setup: self.setup.expand_to_time_series(time_line, &self.scenarios),
            processes: self
                .processes
                .iter()
                .map(|process| expand_and_use_name_as_key(process, time_line, &self.scenarios))
                .collect(),
            nodes: self
                .nodes
                .iter()
                .map(|node| expand_and_use_name_as_key(node, time_line, &self.scenarios))
                .collect(),
            node_diffusion: self
                .node_diffusion
                .iter()
                .map(|diffusion| diffusion.expand_to_time_series(time_line, &self.scenarios))
                .collect(),
            node_delay: self
                .node_delay
                .iter()
                .map(|delay| delay.to_tuple())
                .collect(),
            node_histories: self
                .node_histories
                .iter()
                .map(|history| expand_and_use_name_as_key(history, time_line, &self.scenarios))
                .collect(),
            markets: self
                .markets
                .iter()
                .map(|market| expand_and_use_name_as_key(market, time_line, &self.scenarios))
                .collect(),
            groups: groups.iter().map(|group| use_name_as_key(group)).collect(),
            scenarios: Scenario::to_indexmap(&self.scenarios),
            reserve_type: ReserveType::to_indexmap(&self.reserve_type),
            risk: Risk::to_indexmap(&self.risk),
            inflow_blocks: self
                .inflow_blocks
                .iter()
                .map(|block| expand_and_use_name_as_key(block, time_line, &self.scenarios))
                .collect(),
            bid_slots: IndexMap::new(),
            gen_constraints: self
                .gen_constraints
                .iter()
                .map(|constraint| {
                    expand_and_use_name_as_key(constraint, time_line, &self.scenarios)
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
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
    pub common_timesteps: i32,
    pub common_scenario_name: String,
    pub use_node_dummy_variables: bool,
    pub use_ramp_dummy_variables: bool,
    pub node_dummy_variable_cost: f64,
    pub ramp_dummy_variable_cost: f64,
}

impl ExpandToTimeSeries for BaseInputDataSetup {
    type Expanded = InputDataSetup;
    fn expand_to_time_series(
        &self,
        _time_line: &TimeLine,
        _scenarios: &Vec<Scenario>,
    ) -> Self::Expanded {
        InputDataSetup {
            contains_reserves: self.contains_reserves,
            contains_online: self.contains_online,
            contains_states: self.contains_states,
            contains_piecewise_eff: self.contains_piecewise_eff,
            contains_risk: self.contains_risk,
            contains_diffusion: self.contains_diffusion,
            contains_delay: self.contains_delay,
            contains_markets: self.contains_markets,
            reserve_realisation: self.reserve_realisation,
            use_market_bids: self.use_market_bids,
            common_timesteps: self.common_timesteps as i64,
            common_scenario_name: self.common_scenario_name.clone(),
            use_node_dummy_variables: self.use_node_dummy_variables,
            use_ramp_dummy_variables: self.use_ramp_dummy_variables,
            node_dummy_variable_cost: self.node_dummy_variable_cost,
            ramp_dummy_variable_cost: self.ramp_dummy_variable_cost,
        }
    }
}

#[graphql_object]
#[graphql(name = "InputDataSetup", context = HerttaContext)]
impl BaseInputDataSetup {
    fn contains_reserves(&self) -> bool {
        self.contains_reserves
    }
    fn contain_online(&self) -> bool {
        self.contains_online
    }
    fn contains_states(&self) -> bool {
        self.contains_states
    }
    fn contains_piecewise_eff(&self) -> bool {
        self.contains_piecewise_eff
    }
    fn contains_risk(&self) -> bool {
        self.contains_risk
    }
    fn contains_diffusion(&self) -> bool {
        self.contains_diffusion
    }
    fn contains_delay(&self) -> bool {
        self.contains_delay
    }
    fn contains_markets(&self) -> bool {
        self.contains_markets
    }
    fn reserve_realisation(&self) -> bool {
        self.reserve_realisation
    }
    fn use_market_bids(&self) -> bool {
        self.use_market_bids
    }
    fn common_time_steps(&self) -> i32 {
        self.common_timesteps
    }
    async fn common_scenario(&self, context: &HerttaContext) -> FieldResult<Scenario> {
        let model = context.model().lock().await;
        match model
            .input_data
            .scenarios
            .iter()
            .find(|&s| *s.name() == self.common_scenario_name)
        {
            Some(scenario) => Ok(scenario.clone()),
            None => Err(format!("scenario '{}' doesn't exist", self.common_scenario_name).into()),
        }
    }
    fn use_node_dummy_variables(&self) -> bool {
        self.use_node_dummy_variables
    }
    fn use_ramp_dummy_variables(&self) -> bool {
        self.use_ramp_dummy_variables
    }
    fn node_dummy_variable_cost(&self) -> f64 {
        self.node_dummy_variable_cost
    }
    fn ramp_dummy_variable_cost(&self) -> f64 {
        self.ramp_dummy_variable_cost
    }
}

#[derive(Clone, Copy, Debug, Deserialize, GraphQLEnum, Serialize)]
pub enum Conversion {
    Unit,
    Transfer,
    Market,
}

impl Conversion {
    fn to_input(&self) -> i64 {
        match self {
            Conversion::Unit => 1,
            Conversion::Transfer => 2,
            Conversion::Market => 3,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Name, Serialize)]
pub struct BaseProcess {
    pub name: String,
    pub groups: Vec<String>,
    pub conversion: Conversion,
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
    pub cf: Vec<Value>,
    pub eff_ts: Vec<Value>,
    pub eff_ops: Vec<String>,
    pub eff_fun: Vec<Point>,
}

impl GroupMember for BaseProcess {
    fn groups(&self) -> &Vec<String> {
        &self.groups
    }
    fn groups_mut(&mut self) -> &mut Vec<String> {
        &mut self.groups
    }
}

impl TypeName for BaseProcess {
    fn type_name() -> &'static str {
        "process"
    }
}

#[derive(Clone, Debug, Deserialize, GraphQLObject, Serialize)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

impl ExpandToTimeSeries for BaseProcess {
    type Expanded = Process;
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<Scenario>,
    ) -> Self::Expanded {
        Process {
            name: self.name.clone(),
            groups: self.groups.clone(),
            conversion: self.conversion.to_input(),
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
            cf: values_to_time_series_data(self.cf.clone(), scenarios.clone(), time_line.clone())
            .unwrap_or_else(|err| {
                panic!(
                    "Failed to convert 'cf' to TimeSeriesData for node '{}': {}",
                    self.name, err
                )
            }),
            eff_ts: values_to_time_series_data(self.eff_ts.clone(), scenarios.clone(), time_line.clone())
            .unwrap_or_else(|err| {
                panic!(
                    "Failed to convert 'eff_ts' to TimeSeriesData for node '{}': {}",
                    self.name, err
                )
            }),
            eff_ops: self.eff_ops.clone(),
            eff_fun: self
                .eff_fun
                .iter()
                .map(|point| (point.x, point.y))
                .collect(),
        }
    }
}

#[graphql_object]
#[graphql(name = "Process", context = HerttaContext)]
impl BaseProcess {
    fn name(&self) -> &String {
        &self.name
    }
    async fn groups(&self, context: &HerttaContext) -> Vec<ProcessGroup> {
        let model = context.model().lock().await;
        model
            .input_data
            .process_groups
            .iter()
            .filter(|&g| self.groups.iter().any(|g_name| *g_name == g.name))
            .cloned()
            .collect()
    }
    fn conversion(&self) -> Conversion {
        self.conversion
    }
    fn is_cf(&self) -> bool {
        self.is_cf
    }
    fn is_cf_fix(&self) -> bool {
        self.is_cf_fix
    }
    fn is_online(&self) -> bool {
        self.is_online
    }
    fn is_res(&self) -> bool {
        self.is_res
    }
    fn eff(&self) -> f64 {
        self.eff
    }
    fn load_min(&self) -> f64 {
        self.load_min
    }
    fn load_max(&self) -> f64 {
        self.load_max
    }
    fn start_cost(&self) -> f64 {
        self.start_cost
    }
    fn min_online(&self) -> f64 {
        self.min_online
    }
    fn min_offline(&self) -> f64 {
        self.min_offline
    }
    fn max_online(&self) -> f64 {
        self.max_online
    }
    fn max_offline(&self) -> f64 {
        self.max_offline
    }
    fn is_scenario_independent(&self) -> bool {
        self.is_scenario_independent
    }
    fn topos(&self) -> &Vec<BaseTopology> {
        &self.topos
    }
    fn cf(&self) -> &Vec<Value> {
        &self.cf
    }
    fn eff_ts(&self) -> &Vec<Value> {
        &self.eff_ts
    }
    fn eff_ops(&self) -> &Vec<String> {
        &self.eff_ops
    }
    fn eff_fun(&self) -> &Vec<Point> {
        &self.eff_fun
    }
}

impl BaseProcess {
    pub fn new(name: String, conversion: Conversion) -> Self {
        BaseProcess {
            name,
            groups: Vec::new(),
            conversion,
            is_cf: false,
            is_cf_fix: false,
            is_online: false,
            is_res: false,
            eff: 0.0,
            load_min: 0.0,
            load_max: 0.0,
            start_cost: 0.0,
            min_online: 0.0,
            min_offline: 0.0,
            max_online: 0.0,
            max_offline: 0.0,
            initial_state: false,
            is_scenario_independent: false,
            topos: Vec::new(),
            cf: Vec::new(),
            eff_ts: Vec::new(),
            eff_ops: Vec::new(),
            eff_fun: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, GraphQLUnion, Serialize)]
#[graphql(name = "Forecastable")]
pub enum BaseForecastable {
    Constant(Constant),
    FloatList(FloatList),
    Forecast(Forecast),
}

#[derive(Clone, Debug, Deserialize, Name, Serialize)]
pub struct BaseNode {
    pub name: String,
    pub groups: Vec<String>,
    pub is_commodity: bool,
    pub is_market: bool,
    pub is_res: bool,
    pub state: Option<State>,
    pub cost: Vec<Value>,
    pub inflow: Vec<ForecastValue>,
}

impl TypeName for BaseNode {
    fn type_name() -> &'static str {
        "node"
    }
}

impl ExpandToTimeSeries for BaseNode {
    type Expanded = Node;
    
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<Scenario>,
    ) -> Self::Expanded {
        Node {
            name: self.name.clone(),
            groups: self.groups.clone(),
            is_commodity: self.is_commodity,
            is_market: self.is_market,
            is_state: self.state.is_some(),
            is_res: self.is_res,
            is_inflow: !self.inflow.is_empty(),
            state: self.state.clone(),
            cost: values_to_time_series_data(self.cost.clone(), scenarios.clone(), time_line.clone())
                .unwrap_or_else(|err| {
                    panic!(
                        "Failed to convert 'cost' to TimeSeriesData for node '{}': {}",
                        self.name, err
                    )
                }),
            inflow: forecast_values_to_forecastable(&self.inflow, scenarios, time_line),
        }
    }
}

impl GroupMember for BaseNode {
    fn groups(&self) -> &Vec<String> {
        &self.groups
    }
    fn groups_mut(&mut self) -> &mut Vec<String> {
        &mut self.groups
    }
}

#[graphql_object]
#[graphql(name = "Node", context = HerttaContext)]
impl BaseNode {
    fn name(&self) -> &String {
        &self.name
    }
    async fn groups(&self, context: &HerttaContext) -> Vec<NodeGroup> {
        let model = context.model().lock().await;
        model
            .input_data
            .node_groups
            .iter()
            .filter(|&g| self.groups.iter().any(|g_name| *g_name == g.name))
            .cloned()
            .collect()
    }
    fn is_commodity(&self) -> bool {
        self.is_commodity
    }
    fn is_market(&self) -> bool {
        self.is_market
    }
    fn is_res(&self) -> bool {
        self.is_res
    }
    fn state(&self) -> &Option<State> {
        &self.state
    }
    fn cost(&self) -> &Vec<Value> {
        &self.cost
    }
    fn inflow(&self) -> &Vec<ForecastValue> {
        &self.inflow
    }
}

impl BaseNode {
    pub fn new(name: String) -> Self {
        BaseNode {
            name,
            groups: Vec::new(),
            is_commodity: false,
            is_market: false,
            is_res: false,
            state: None,
            cost: Vec::new(),
            inflow: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BaseNodeDiffusion {
    pub from_node: String,
    pub to_node: String,
    pub coefficient: Vec<Value>,
}

impl ExpandToTimeSeries for BaseNodeDiffusion {
    type Expanded = NodeDiffusion;

    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<Scenario>,
    ) -> Self::Expanded {
        NodeDiffusion {
            node1: self.from_node.clone(),
            node2: self.to_node.clone(),
            coefficient: if self.coefficient.is_empty() {
                panic!(
                    "No coefficient values provided for NodeDiffusion from '{}' to '{}'",
                    self.from_node, self.to_node
                );
            } else {
                values_to_time_series_data(self.coefficient.clone(), scenarios.clone(), time_line.clone())
                    .unwrap_or_else(|err| {
                        panic!(
                            "Failed to convert 'coefficient' to TimeSeriesData for NodeDiffusion from '{}' to '{}': {}",
                            self.from_node, self.to_node, err
                        )
                    })
            },
        }
    }
}

#[graphql_object]
#[graphql(name = "NodeDiffusion", context = HerttaContext)]
impl BaseNodeDiffusion {
    async fn from_node(&self, context: &HerttaContext) -> FieldResult<BaseNode> {
        let model = context.model().lock().await;
        find_node(&self.from_node, "from_node", &model.input_data.nodes)
    }
    async fn to_node(&self, context: &HerttaContext) -> FieldResult<BaseNode> {
        let model = context.model().lock().await;
        find_node(&self.to_node, "to_node", &model.input_data.nodes)
    }
    fn coefficient(&self) -> &Vec<Value> {
        &self.coefficient
    }
}

fn find_node(node_name: &String, node_type: &str, nodes: &Vec<BaseNode>) -> FieldResult<BaseNode> {
    match nodes.iter().find(|&n| n.name == *node_name) {
        Some(node) => Ok(node.clone()),
        None => Err(format!("{} '{}' doesn't exist", node_type, node_name).into()),
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BaseNodeHistory {
    pub node: String,
    pub steps: Vec<Series>,
}

impl Name for BaseNodeHistory {
    fn name(&self) -> &String {
        &self.node
    }
}

#[graphql_object]
#[graphql(name = "NodeHistory", context = HerttaContext)]
impl BaseNodeHistory {
    async fn node(&self, context: &HerttaContext) -> FieldResult<BaseNode> {
        let model = context.model().lock().await;
        find_node(&self.node, "node", &model.input_data.nodes)
    }
    fn steps(&self) -> &Vec<Series> {
        &self.steps
    }
}

impl ExpandToTimeSeries for BaseNodeHistory {
    type Expanded = NodeHistory;
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        _scenarios: &Vec<Scenario>,
    ) -> Self::Expanded {
        NodeHistory {
            node: self.node.clone(),
            steps: Series::to_time_series_data(&self.steps, time_line),
        }
    }
}

impl BaseNodeHistory {
    pub fn new(node_name: String) -> Self {
        BaseNodeHistory {
            node: node_name,
            steps: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, GraphQLEnum, Serialize)]
pub enum MarketType {
    Energy,
    Reserve,
}

impl MarketType {
    fn to_input(&self) -> String {
        match self {
            MarketType::Energy => "energy",
            MarketType::Reserve => "reserve",
        }
        .into()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, GraphQLEnum, Serialize)]
pub enum MarketDirection {
    Up,
    Down,
    UpDown,
    ResUp,
    ResDown,
}

impl MarketDirection {
    fn to_input(&self) -> String {
        match self {
            MarketDirection::Up => "up",
            MarketDirection::Down => "down",
            MarketDirection::UpDown => "up_down",
            MarketDirection::ResUp => "res_up",
            MarketDirection::ResDown => "res_down",
        }
        .into()
    }
}

#[derive(Clone, Debug, Deserialize, Name, Serialize)]
pub struct BaseMarket {
    pub name: String,
    pub m_type: MarketType,
    pub node: String,
    pub process_group: String,
    pub direction: Option<MarketDirection>,
    pub realisation: Vec<Value>,
    pub reserve_type: Option<String>,
    pub is_bid: bool,
    pub is_limited: bool,
    pub min_bid: f64,
    pub max_bid: f64,
    pub fee: f64,
    pub price: Vec<ForecastValue>,
    pub up_price: Vec<ForecastValue>,
    pub down_price: Vec<ForecastValue>,
    pub reserve_activation_price: Vec<Value>,
    pub fixed: Vec<MarketFix>,
}

impl TypeName for BaseMarket {
    fn type_name() -> &'static str {
        "market"
    }
}

#[derive(Clone, Debug, Deserialize, GraphQLObject, Serialize)]
pub struct MarketFix {
    pub name: String,
    pub factor: f64,
}

impl ExpandToTimeSeries for BaseMarket {
    type Expanded = Market;
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<Scenario>,
    ) -> Self::Expanded {
        Market {
            name: self.name.clone(),
            m_type: self.m_type.to_input(),
            node: self.node.clone(),
            processgroup: self.process_group.clone(),
            direction: match self.direction {
                Some(dir) => dir.to_input(),
                None => "none".to_string(),
            },
            realisation: values_to_time_series_data(
                self.realisation.clone(),
                scenarios.clone(),
                time_line.clone(),
            )
            .unwrap_or_else(|err| {
                panic!(
                    "Failed to convert 'realisation' to TimeSeriesData for market '{}': {}",
                    self.name, err
                )
            }),
            reserve_type: match self.reserve_type {
                Some(ref reserve_type) => reserve_type.clone(),
                None => "none".to_string(),
            },
            is_bid: self.is_bid,
            is_limited: self.is_limited,
            min_bid: self.min_bid,
            max_bid: self.max_bid,
            fee: self.fee,
            price: forecast_values_to_forecastable(&self.price, scenarios, time_line),
            up_price: forecast_values_to_forecastable(&self.up_price, scenarios, time_line),
            down_price: forecast_values_to_forecastable(&self.down_price, scenarios, time_line),
            reserve_activation_price: values_to_time_series_data(
                self.reserve_activation_price.clone(),
                scenarios.clone(),
                time_line.clone(),
            )
            .unwrap_or_else(|err| {
                panic!(
                    "Failed to convert 'reserve_activation_price' to TimeSeriesData for market '{}': {}",
                    self.name, err
                )
            }),
            fixed: self.fixed.iter().map(|fix| (fix.name.clone(), fix.factor)).collect(),
        }
    }
}


#[graphql_object]
#[graphql(name = "Market", context = HerttaContext)]
impl BaseMarket {
    fn name(&self) -> &String {
        &self.name
    }
    fn m_type(&self) -> MarketType {
        self.m_type
    }
    async fn node(&self, context: &HerttaContext) -> FieldResult<BaseNode> {
        let model = context.model().lock().await;
        find_node(&self.node, "node", &model.input_data.nodes)
    }
    async fn process_group(&self, context: &HerttaContext) -> FieldResult<ProcessGroup> {
        let model = context.model().lock().await;
        match model
            .input_data
            .process_groups
            .iter()
            .find(|&g| g.name == self.process_group)
        {
            Some(group) => Ok(group.clone()),
            None => Err(format!("process group '{}' doesn't exist", self.process_group).into()),
        }
    }
    fn direction(&self) -> Option<MarketDirection> {
        self.direction
    }
    fn realisation(&self) -> &Vec<Value> {
        &self.realisation
    }
    async fn reserve_type(&self, context: &HerttaContext) -> FieldResult<Option<ReserveType>> {
        let model = context.model().lock().await;
        if let Some(ref type_name) = self.reserve_type {
            if let Some(reserve_type) = model
                .input_data
                .reserve_type
                .iter()
                .find(|r| r.name == *type_name)
            {
                return Ok(Some(reserve_type.clone()));
            } else {
                return Err(format!("reserve type '{}' doesn't exist", type_name).into());
            }
        } else {
            return Ok(None);
        }
    }
    fn is_bid(&self) -> bool {
        self.is_bid
    }
    fn is_limited(&self) -> bool {
        self.is_limited
    }
    fn min_bid(&self) -> f64 {
        self.min_bid
    }
    fn max_bid(&self) -> f64 {
        self.max_bid
    }
    fn fee(&self) -> f64 {
        self.fee
    }
    fn price(&self) -> &Vec<ForecastValue> {
        &self.price
    }
    fn up_price(&self) -> &Vec<ForecastValue> {
        &self.up_price
    }
    fn down_price(&self) -> &Vec<ForecastValue> {
        &self.down_price
    }
    fn reserve_activation_price(&self) -> &Vec<Value> {
        &self.reserve_activation_price
    }
    fn fixed(&self) -> &Vec<MarketFix> {
        &self.fixed
    }
}

pub trait NamedGroup {
    fn new(name: String) -> Self;
}

pub trait Members {
    fn members(&self) -> &Vec<String>;
    fn members_mut(&mut self) -> &mut Vec<String>;
}

#[derive(Clone, Debug, Deserialize, Members, Name, PartialEq, Serialize)]
pub struct NodeGroup {
    pub name: String,
    pub members: Vec<String>,
}

impl TypeName for NodeGroup {
    fn type_name() -> &'static str {
        "node group"
    }
}

impl NamedGroup for NodeGroup {
    fn new(name: String) -> Self {
        NodeGroup {
            name,
            members: Vec::new(),
        }
    }
}

#[graphql_object]
#[graphql(context = HerttaContext)]
impl NodeGroup {
    fn name(&self) -> &String {
        &self.name
    }
    async fn members(&self, context: &HerttaContext) -> Vec<BaseNode> {
        let model = context.model().lock().await;
        model
            .input_data
            .nodes
            .iter()
            .filter(|&n| self.members.iter().any(|m| *m == n.name))
            .cloned()
            .collect()
    }
}

impl From<&NodeGroup> for Group {
    fn from(value: &NodeGroup) -> Self {
        Group {
            name: value.name.clone(),
            g_type: GroupType::Node,
            members: value.members.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Members, Name, PartialEq, Serialize)]
pub struct ProcessGroup {
    pub name: String,
    pub members: Vec<String>,
}

impl TypeName for ProcessGroup {
    fn type_name() -> &'static str {
        "process group"
    }
}

impl NamedGroup for ProcessGroup {
    fn new(name: String) -> Self {
        ProcessGroup {
            name,
            members: Vec::new(),
        }
    }
}

#[graphql_object]
#[graphql(context = HerttaContext)]
impl ProcessGroup {
    fn name(&self) -> &String {
        &self.name
    }
    async fn members(&self, context: &HerttaContext) -> Vec<BaseProcess> {
        let model = context.model().lock().await;
        model
            .input_data
            .processes
            .iter()
            .filter(|&n| self.members.iter().any(|m| *m == n.name))
            .cloned()
            .collect()
    }
}

impl From<&ProcessGroup> for Group {
    fn from(value: &ProcessGroup) -> Self {
        Group {
            name: value.name.clone(),
            g_type: GroupType::Process,
            members: value.members.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Name, Serialize)]
pub struct BaseInflowBlock {
    pub name: String,
    pub node: String,
    pub data: Vec<Value>,
}

impl ExpandToTimeSeries for BaseInflowBlock {
    type Expanded = InflowBlock;
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<Scenario>,
    ) -> Self::Expanded {
        InflowBlock {
            name: self.name.clone(),
            node: self.node.clone(),
            start_time: time_line[0].clone(),
            data: values_to_time_series_data(
                self.data.clone(),
                scenarios.clone(),
                time_line.clone(),
            )
            .unwrap_or_else(|err| {
                panic!(
                    "Failed to convert 'data' to TimeSeriesData for inflow block '{}': {}",
                    self.name, err
                )
            }),
        }
    }
}

#[graphql_object]
#[graphql(name = "InflowBlock", context = HerttaContext)]
impl BaseInflowBlock {
    fn name(&self) -> &String {
        &self.name
    }
    async fn node(&self, context: &HerttaContext) -> FieldResult<BaseNode> {
        let model = context.model().lock().await;
        find_node(&self.node, "node", &model.input_data.nodes)
    }
    fn data(&self) -> &Vec<Value> {
        &self.data
    }
}

#[derive(Clone, Copy, Debug, Deserialize, GraphQLEnum, Serialize)]
pub enum ConstraintType {
    LessThan,
    Equal,
    GreaterThan,
}

impl ConstraintType {
    fn to_input(&self) -> String {
        match self {
            ConstraintType::LessThan => "st",
            ConstraintType::Equal => "eq",
            ConstraintType::GreaterThan => "gt",
        }
        .into()
    }
}

#[derive(Clone, Debug, Deserialize, GraphQLObject, Name, Serialize)]
#[graphql(name = "GenConstraint", context = HerttaContext)]
pub struct BaseGenConstraint {
    pub name: String,
    pub gc_type: ConstraintType,
    pub is_setpoint: bool,
    pub penalty: f64,
    pub factors: Vec<BaseConFactor>,
    pub constant: Vec<Value>,
}

impl TypeName for BaseGenConstraint {
    fn type_name() -> &'static str {
        "generic constraint"
    }
}

impl ExpandToTimeSeries for BaseGenConstraint {
    type Expanded = GenConstraint;
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<Scenario>,
    ) -> Self::Expanded {
        GenConstraint {
            name: self.name.clone(),
            gc_type: self.gc_type.to_input(),
            is_setpoint: self.is_setpoint,
            penalty: self.penalty,
            factors: self
                .factors
                .iter()
                .map(|factor| factor.expand_to_time_series(time_line, scenarios))
                .collect(),
            constant: values_to_time_series_data(
                self.constant.clone(),
                scenarios.clone(),
                time_line.clone(),
            )
            .unwrap_or_else(|err| {
                panic!(
                    "Failed to convert 'constant' to TimeSeriesData for gen constraint '{}': {}",
                    self.name, err
                )
            }),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BaseTopology {
    pub source: String,
    pub sink: String,
    pub capacity: f64,
    pub vom_cost: f64,
    pub ramp_up: f64,
    pub ramp_down: f64,
    pub initial_load: f64,
    pub initial_flow: f64,
    pub cap_ts: Vec<Value>,
}

impl ExpandToTimeSeries for BaseTopology {
    type Expanded = Topology;
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<Scenario>,
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
            cap_ts: values_to_time_series_data(
                self.cap_ts.clone(),
                scenarios.clone(),
                time_line.clone(),
            )
            .unwrap_or_else(|err| {
                panic!(
                    "Failed to convert 'cap_ts' to TimeSeriesData for topology ({} -> {}): {}",
                    self.source, self.sink, err
                )
            }),
        }
    }
}

impl BaseTopology {
    pub fn new(source: String, sink: String) -> Self {
        BaseTopology {
            source,
            sink,
            capacity: 0.0,
            vom_cost: 0.0,
            ramp_up: 0.0,
            ramp_down: 0.0,
            initial_load: 0.0,
            initial_flow: 0.0,
            cap_ts: Vec::new(),
        }
    }
}

#[derive(GraphQLUnion)]
#[graphql(context = HerttaContext)]
pub enum NodeOrProcess {
    Node(BaseNode),
    Process(BaseProcess),
}

#[graphql_object]
#[graphql(name = "Topology", context = HerttaContext)]
impl BaseTopology {
    async fn source(&self, context: &HerttaContext) -> FieldResult<NodeOrProcess> {
        let model = context.model().lock().await;
        find_node_or_process(
            &self.source,
            "source",
            &model.input_data.nodes,
            &model.input_data.processes,
        )
    }
    async fn sink(&self, context: &HerttaContext) -> FieldResult<NodeOrProcess> {
        let model = context.model().lock().await;
        find_node_or_process(
            &self.sink,
            "sink",
            &model.input_data.nodes,
            &model.input_data.processes,
        )
    }
}

fn find_node_or_process(
    name: &str,
    entity_type: &str,
    nodes: &Vec<BaseNode>,
    processes: &Vec<BaseProcess>,
) -> FieldResult<NodeOrProcess> {
    if let Some(node) = nodes.iter().find(|&n| n.name == name) {
        return Ok(NodeOrProcess::Node(node.clone()));
    } else {
        return match processes.iter().find(|&p| p.name == name) {
            Some(process) => Ok(NodeOrProcess::Process(process.clone())),
            None => Err(format!("{} node or process '{}' doesn't exist", entity_type, name).into()),
        };
    }
}

#[derive(Clone, Copy, Debug, Deserialize, GraphQLEnum, PartialEq, Serialize)]
pub enum ConstraintFactorType {
    Flow,
    State,
    Online,
}

impl ConstraintFactorType {
    fn to_input(&self) -> String {
        match self {
            ConstraintFactorType::Flow => "flow",
            ConstraintFactorType::State => "state",
            ConstraintFactorType::Online => "online",
        }
        .into()
    }
}

#[derive(Clone, Debug, Deserialize, GraphQLObject, Serialize)]
#[graphql(name = "ConFactor", context = HerttaContext)]
pub struct BaseConFactor {
    pub var_type: ConstraintFactorType,
    pub var_tuple: VariableId,
    pub data: Vec<Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VariableId {
    pub entity: String,
    pub identifier: Option<String>,
}

impl ExpandToTimeSeries for BaseConFactor {
    type Expanded = ConFactor;
    fn expand_to_time_series(
        &self,
        time_line: &TimeLine,
        scenarios: &Vec<Scenario>,
    ) -> Self::Expanded {
        ConFactor {
            var_type: self.var_type.to_input(),
            var_tuple: (
                self.var_tuple.entity.clone(),
                self.var_tuple
                    .identifier
                    .as_ref()
                    .map_or("".into(), |s| s.clone()),
            ),
            data: values_to_time_series_data(
                self.data.clone(),
                scenarios.clone(),
                time_line.clone()
            ).unwrap_or_else(|err| {
                panic!("Failed to convert 'data' to TimeSeriesData for ConFactor '{}': {}", self.var_tuple.entity, err)
            }),
        }
    }
}

impl BaseConFactor {
    pub fn is_flow(&self) -> bool {
        match self.var_type {
            ConstraintFactorType::Flow => true,
            _ => false,
        }
    }
    pub fn is_state(&self) -> bool {
        match self.var_type {
            ConstraintFactorType::State => true,
            _ => false,
        }
    }
    pub fn is_online(&self) -> bool {
        match self.var_type {
            ConstraintFactorType::Online => true,
            _ => false,
        }
    }
}

#[graphql_object]
#[graphql(context = HerttaContext)]
impl VariableId {
    async fn entity(&self, context: &HerttaContext) -> FieldResult<NodeOrProcess> {
        let model = context.model().lock().await;
        find_node_or_process(
            &self.entity,
            "entity",
            &model.input_data.nodes,
            &model.input_data.processes,
        )
    }
    async fn identifier(&self, context: &HerttaContext) -> FieldResult<Option<BaseNode>> {
        let model = context.model().lock().await;
        if let Some(ref node_name) = self.identifier {
            return Ok(Some(find_node(
                node_name,
                "identifier",
                &model.input_data.nodes,
            )?));
        } else {
            return Ok(None);
        }
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
    use crate::input_data::{BidSlot, Group, GroupType};
    use chrono::{TimeZone, Utc};
    fn as_map<T: Name>(x: T) -> BTreeMap<String, T> {
        let mut map = BTreeMap::new();
        map.insert(x.name().clone(), x);
        map
    }
    fn as_indexmap<T: Name>(x: T) -> IndexMap<String, T> {
        let mut map = IndexMap::new();
        map.insert(x.name().clone(), x);
        map
    }
    fn to_time_series_data(y: f64, time_line: &TimeLine, scenarios: &Vec<Scenario>) -> TimeSeriesData {
        let single_series: BTreeMap<TimeStamp, f64> = time_line
            .iter()
            .map(|time_stamp| (time_stamp.clone(), y))
            .collect();
        TimeSeriesData {
            ts_data: scenarios
                .iter()
                .map(|scenario| TimeSeries {
                    scenario: scenario.name().clone(),
                    series: single_series.clone(),
                })
                .collect(),
        }
    }
    #[test]
    fn expanding_input_data_works() {
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios =
            vec![Scenario::new("S1", 1.0).expect("scenario construction should succeed")];
        let base_setup = BaseInputDataSetup::default();
        let setup = base_setup.expand_to_time_series(&time_line, &scenarios);
        let base_topology = BaseTopology {
            source: "Source".to_string(),
            sink: "Sink".to_string(),
            capacity: 1.1,
            vom_cost: 1.2,
            ramp_up: 1.3,
            ramp_down: 1.4,
            initial_load: 1.5,
            initial_flow: 1.6,
            cap_ts: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 1.7 }),
            }],
        };
        let base_process = BaseProcess {
            name: "Conversion".to_string(),
            groups: vec!["Group".to_string()],
            conversion: Conversion::Unit,
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
            cf: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 2.0 }),
            }],
            eff_ts: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 2.1 }),
            }],
            eff_ops: vec!["oops!".to_string()],
            eff_fun: vec![Point { x: 2.2, y: 2.3 }],
        };
        let process = base_process.expand_to_time_series(&time_line, &scenarios);
        let base_node = BaseNode {
            name: "East".to_string(),
            groups: vec!["Group".to_string()],
            is_commodity: true,
            is_market: false,
            is_res: false,
            state: None,
            cost: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 1.1 }),
            }],
            inflow: vec![ForecastValue {
                scenario: None,
                value: BaseForecastable::Constant(Constant { value: 1.2 }),
            }],
        };
        let node = base_node.expand_to_time_series(&time_line, &scenarios);
        let base_node_diffusion = BaseNodeDiffusion {
            from_node: "Node 1".to_string(),
            to_node: "Node 2".to_string(),
            coefficient: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: -2.3 }),
            }],
        };
        let node_diffusion = base_node_diffusion.expand_to_time_series(&time_line, &scenarios);
        let node_delay = vec![Delay {
            from_node: "South".to_string(),
            to_node: "North".to_string(),
            delay: 3.1,
            min_delay_flow: 3.2,
            max_delay_flow: 3.3,
        }];
        let base_node_history = BaseNodeHistory {
            node: "South".to_string(),
            steps: vec![Series {
                scenario: "S1".into(),
                durations: vec![
                    Duration::try_new(1, 0, 0).expect("constructing duration should succeed")
                ],
                values: vec![1.1],
            }],
        };
        let node_history = base_node_history.expand_to_time_series(&time_line, &scenarios);
        let base_market = BaseMarket {
            name: "Market".to_string(),
            m_type: MarketType::Energy,
            node: "North".to_string(),
            process_group: "Group".to_string(),
            direction: None,
            realisation: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 1.0 }),
            }],
            reserve_type: Some("not none".to_string()),
            is_bid: true,
            is_limited: false,
            min_bid: 1.2,
            max_bid: 1.3,
            fee: 1.4,
            price: vec![ForecastValue {
                scenario: None,
                value: BaseForecastable::Constant(1.5.into()),
            }],            
            up_price: vec![ForecastValue {
                scenario: None,
                value: BaseForecastable::Constant(1.6.into()),
            }],
            down_price: vec![ForecastValue {
                scenario: None,
                value: BaseForecastable::Constant(1.7.into()),
            }],
            reserve_activation_price: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 1.8 }),
            }],
            fixed: vec![MarketFix {
                name: "Fix".to_string(),
                factor: 1.9,
            }],
        };
        let market = base_market.expand_to_time_series(&time_line, &scenarios);
        let node_groups = vec![NodeGroup::new("The node club".into())];
        let process_groups = vec![ProcessGroup::new("The process club".into())];
        let scenarios =
            vec![Scenario::new("S1", 1.0).expect("constructing scenario should succeed")];
        let reserve_type = vec![ReserveType {
            name: "infinite".to_string(),
            ramp_rate: 4.1,
        }];
        let risk = vec![Risk {
            parameter: "high".to_string(),
            value: 0.99,
        }];
        let base_inflow_block = BaseInflowBlock {
            name: "Inflow".to_string(),
            node: "West".to_string(),
            data: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 2.3 }),
            }],
        };
        let inflow_block = base_inflow_block.expand_to_time_series(&time_line, &scenarios);
        let base_con_factor = BaseConFactor {
            var_type: ConstraintFactorType::State,
            var_tuple: VariableId {
                entity: "interior_air".to_string(),
                identifier: None,
            },
            data: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 23.0 }),
            }],
        };
        let base_gen_constraint = BaseGenConstraint {
            name: "Constraint".to_string(),
            gc_type: ConstraintType::GreaterThan,
            is_setpoint: true,
            penalty: 1.1,
            factors: vec![base_con_factor],
            constant: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 1.2 }),
            }],
        };
        let gen_constraint = base_gen_constraint.expand_to_time_series(&time_line, &scenarios);
        let base = BaseInputData {
            scenarios: scenarios,
            setup: base_setup,
            processes: vec![base_process],
            nodes: vec![base_node],
            node_diffusion: vec![base_node_diffusion],
            node_delay: node_delay,
            node_histories: vec![base_node_history],
            markets: vec![base_market],
            node_groups,
            process_groups,
            reserve_type: reserve_type,
            risk: risk,
            inflow_blocks: vec![base_inflow_block],
            gen_constraints: vec![base_gen_constraint],
        };
        let input_data = base.expand_to_time_series(&time_line);
        let temporals = Temporals {
            t: time_line,
            dtf: 1.0,
            variable_dt: None,
        };
        assert_eq!(input_data.temporals, temporals);
        assert_eq!(input_data.setup, setup);
        assert_eq!(input_data.processes, as_indexmap(process));
        assert_eq!(input_data.nodes, as_indexmap(node));
        assert_eq!(input_data.node_diffusion, vec![node_diffusion]);
        assert_eq!(
            input_data.node_delay,
            base.node_delay
                .iter()
                .map(|delay| delay.to_tuple())
                .collect::<Vec<_>>()
        );
        assert_eq!(input_data.node_histories, as_indexmap(node_history));
        assert_eq!(input_data.markets, as_indexmap(market));
        let mut groups = IndexMap::new();
        groups.insert(
            "The node club".to_string(),
            Group {
                name: "The node club".into(),
                g_type: GroupType::Node,
                members: Vec::new(),
            },
        );
        groups.insert(
            "The process club".to_string(),
            Group {
                name: "The process club".into(),
                g_type: GroupType::Process,
                members: Vec::new(),
            },
        );
        assert_eq!(input_data.groups, groups,);
        assert_eq!(input_data.scenarios, Scenario::to_indexmap(&base.scenarios));
        assert_eq!(
            input_data.reserve_type,
            ReserveType::to_indexmap(&base.reserve_type)
        );
        assert_eq!(input_data.risk, Risk::to_indexmap(&base.risk));
        assert_eq!(input_data.inflow_blocks, as_indexmap(inflow_block));
        assert_eq!(input_data.bid_slots, IndexMap::<String, BidSlot>::new());
        assert_eq!(input_data.gen_constraints, as_indexmap(gen_constraint));
    }
    #[test]
    fn expanding_process_works() {
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios =
            vec![Scenario::new("S1", 1.0).expect("scenario construction should succeed")];
        let base_topology = BaseTopology {
            source: "Source".to_string(),
            sink: "Sink".to_string(),
            capacity: 1.1,
            vom_cost: 1.2,
            ramp_up: 1.3,
            ramp_down: 1.4,
            initial_load: 1.5,
            initial_flow: 1.6,
            cap_ts: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 1.7 }),
            }],
        };
        let topology = base_topology.expand_to_time_series(&time_line, &scenarios);
        let base = BaseProcess {
            name: "Conversion".to_string(),
            groups: vec!["Group".to_string()],
            conversion: Conversion::Transfer,
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
            cf: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 2.0 }),
            }],
            eff_ts: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 2.1 }),
            }],
            eff_ops: vec!["oops!".to_string()],
            eff_fun: vec![Point { x: 2.2, y: 2.3 }],
        };
        let process = base.expand_to_time_series(&time_line, &scenarios);
        assert_eq!(process.name, "Conversion");
        assert_eq!(process.groups, vec!["Group".to_string()]);
        assert_eq!(process.conversion, 2);
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
        assert_eq!(process.cf, to_time_series_data(2.0, &time_line, &scenarios));
        assert_eq!(
            process.eff_ts,
            to_time_series_data(2.1, &time_line, &scenarios)
        );
        assert_eq!(process.eff_ops, vec!["oops!".to_string()]);
        assert_eq!(process.eff_fun, vec![(2.2, 2.3)]);
    }
    #[test]
    fn expanding_node_works() {
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios =
            vec![Scenario::new("S1", 1.0).expect("constructing scenario should succeed")];
        let base = BaseNode {
            name: "East".to_string(),
            groups: vec!["Group".to_string()],
            is_commodity: true,
            is_market: false,
            is_res: false,
            state: None,
            cost: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 1.1 }),
            }],
            inflow: vec![ForecastValue {
                scenario: None,
                value: BaseForecastable::Constant(Constant { value: 1.2 }),
            }],
        };
        let node = base.expand_to_time_series(&time_line, &scenarios);
        assert_eq!(node.name, "East");
        assert_eq!(node.groups, vec!["Group".to_string()]);
        assert!(node.is_commodity);
        assert!(!node.is_market);
        assert!(!node.is_state);
        assert!(!node.is_res);
        assert!(node.is_inflow);
        assert!(node.state.is_none());
        match node.inflow {
            Forecastable::Forecast(_) => panic!("inflow should not be temperature forecast"),
            Forecastable::TimeSeriesData(time_series_data) => assert_eq!(
                time_series_data,
                to_time_series_data(1.2, &time_line, &scenarios)
            ),
        };
    }
    #[test]
    fn expanding_node_diffusion_works() {
        let base = BaseNodeDiffusion {
            from_node: "Node 1".to_string(),
            to_node: "Node 2".to_string(),
            coefficient: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: -2.3 }),
            }],
        };
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios =
            vec![Scenario::new("S1", 1.0).expect("constructing scenario should succeed")];
        let node_diffusion = base.expand_to_time_series(&time_line, &scenarios);
        assert_eq!(node_diffusion.node1, "Node 1");
        assert_eq!(node_diffusion.node2, "Node 2");
        assert_eq!(
            node_diffusion.coefficient,
            to_time_series_data(-2.3, &time_line, &scenarios)
        );
    }
    #[test]
    fn expanding_node_history_works() {
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios =
            vec![Scenario::new("S1", 1.0).expect("constructing scenario should succeed")];
        let base = BaseNodeHistory {
            node: "South".to_string(),
            steps: vec![Series {
                scenario: "S1".into(),
                durations: vec![
                    Duration::try_new(1, 0, 0).expect("constructing duration should succeed")
                ],
                values: vec![1.1],
            }],
        };
        let node_history = base.expand_to_time_series(&time_line, &scenarios);
        assert_eq!(node_history.node, "South");
        assert_eq!(
            node_history.steps,
            TimeSeriesData {
                ts_data: vec![TimeSeries {
                    scenario: "S1".into(),
                    series: [(Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap(), 1.1)]
                        .into_iter()
                        .collect()
                }],
            }
        );
    }
    #[test]
    fn expanding_market_works() {
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios =
            vec![Scenario::new("S1", 1.0).expect("constructing scenario should succeed")];
        let base = BaseMarket {
            name: "Market".to_string(),
            m_type: MarketType::Energy,
            node: "North".to_string(),
            process_group: "Group".to_string(),
            direction: None,
            realisation: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 1.1 }),
            }],
            reserve_type: Some("not none".to_string()),
            is_bid: true,
            is_limited: false,
            min_bid: 1.2,
            max_bid: 1.3,
            fee: 1.4,
            price: vec![ForecastValue {
                scenario: None,
                value: BaseForecastable::Constant(1.5.into()),
            }],
            up_price: vec![ForecastValue {
                scenario: None,
                value: BaseForecastable::Constant(1.6.into()),
            }],
            down_price: vec![ForecastValue {
                scenario: None,
                value: BaseForecastable::Constant(1.7.into()),
            }],
            reserve_activation_price: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 1.8 }),
            }],
            fixed: vec![MarketFix {
                name: "Fix".to_string(),
                factor: 1.9,
            }],
        };
        let market = base.expand_to_time_series(&time_line, &scenarios);
        assert_eq!(market.name, "Market");
        assert_eq!(market.m_type, "energy");
        assert_eq!(market.node, "North");
        assert_eq!(market.processgroup, "Group");
        assert_eq!(market.direction, "none");
        assert_eq!(
            market.realisation,
            to_time_series_data(1.1, &time_line, &scenarios)
        );
        assert_eq!(market.reserve_type, "not none");
        assert!(market.is_bid);
        assert!(!market.is_limited);
        assert_eq!(market.min_bid, 1.2);
        assert_eq!(market.max_bid, 1.3);
        assert_eq!(market.fee, 1.4);
        assert_eq!(
            market.price,
            Forecastable::TimeSeriesData(to_time_series_data(1.5, &time_line, &scenarios))
        );
        assert_eq!(
            market.up_price,
            Forecastable::TimeSeriesData(to_time_series_data(1.6, &time_line, &scenarios))
        );
        assert_eq!(
            market.down_price,
            Forecastable::TimeSeriesData(to_time_series_data(1.7, &time_line, &scenarios))
        );
        assert_eq!(
            market.reserve_activation_price,
            to_time_series_data(1.8, &time_line, &scenarios)
        );
        assert_eq!(market.fixed, vec![("Fix".to_string(), 1.9)])
    }
    #[test]
    fn expanding_inflow_block_works() {
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios =
            vec![Scenario::new("S1", 1.0).expect("constructing scenario should succeed")];
        let base = BaseInflowBlock {
            name: "Inflow".to_string(),
            node: "West".to_string(),
            data: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 2.3 }),
            }],
        };
        let inflow_block = base.expand_to_time_series(&time_line, &scenarios);
        assert_eq!(inflow_block.name, "Inflow");
        assert_eq!(inflow_block.node, "West");
        assert_eq!(inflow_block.start_time, time_line[0]);
        assert_eq!(
            inflow_block.data,
            to_time_series_data(2.3, &time_line, &scenarios)
        );
    }
    #[test]
    fn expanding_gen_constraing_works() {
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios =
            vec![Scenario::new("S1", 1.0).expect("constructing scenario should succeed")];
        let base_con_factor = BaseConFactor {
            var_type: ConstraintFactorType::State,
            var_tuple: VariableId {
                entity: "interior_air".to_string(),
                identifier: None,
            },
            data: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 23.0 }),
            }],
        };
        let con_factor = base_con_factor.expand_to_time_series(&time_line, &scenarios);
        let base = BaseGenConstraint {
            name: "Constraint".to_string(),
            gc_type: ConstraintType::GreaterThan,
            is_setpoint: true,
            penalty: 1.1,
            factors: vec![base_con_factor],
            constant: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 1.2 }),
            }],
        };
        let gen_constraint = base.expand_to_time_series(&time_line, &scenarios);
        assert_eq!(gen_constraint.name, "Constraint");
        assert_eq!(gen_constraint.gc_type, "gt");
        assert!(gen_constraint.is_setpoint);
        assert_eq!(gen_constraint.penalty, 1.1);
        assert_eq!(gen_constraint.factors, vec![con_factor]);
        assert_eq!(
            gen_constraint.constant,
            to_time_series_data(1.2, &time_line, &scenarios)
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
            cap_ts: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 1.7 }),
            }],
        };
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios =
            vec![Scenario::new("S1", 1.0).expect("constructing scenario should succeed")];
        let topology = base.expand_to_time_series(&time_line, &scenarios);
        assert_eq!(topology.source, "Source");
        assert_eq!(topology.sink, "Sink");
        assert_eq!(topology.capacity, 1.1);
        assert_eq!(topology.vom_cost, 1.2);
        assert_eq!(topology.ramp_up, 1.3);
        assert_eq!(topology.ramp_down, 1.4);
        assert_eq!(topology.initial_load, 1.5);
        assert_eq!(topology.initial_flow, 1.6);
        assert_eq!(
            topology.cap_ts,
            to_time_series_data(1.7, &time_line, &scenarios)
        );
    }
    #[test]
    fn expanding_con_factor_works() {
        let base = BaseConFactor {
            var_type: ConstraintFactorType::State,
            var_tuple: VariableId {
                entity: "interior_air".to_string(),
                identifier: None,
            },
            data: vec![Value {
                scenario: None,
                value: SeriesValue::Constant(Constant { value: 23.0 }),
            }],
        };
        let time_line: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap().into(),
        ];
        let scenarios =
            vec![Scenario::new("S1", 1.0).expect("constructing scenario should succeed")];
        let con_factor = base.expand_to_time_series(&time_line, &scenarios);
        assert_eq!(con_factor.var_type, "state");
        assert_eq!(
            con_factor.var_tuple,
            ("interior_air".to_string(), "".to_string())
        );
        assert_eq!(
            con_factor.data,
            values_to_time_series_data(base.data.clone(), scenarios.clone(), time_line.clone()).unwrap()
        );
    }
    mod find_input_node_names {
        use super::*;
        #[test]
        fn no_nodes_means_no_names() {
            let names = find_input_node_names(Vec::<BaseNode>::new().iter());
            assert!(names.is_empty());
        }
        #[test]
        fn test_non_input_nodes_are_filtered() {
            let mut commodity_nodes = vec![BaseNode::new("commodity".to_string())];
            commodity_nodes[0].is_commodity = true;
            assert!(find_input_node_names(commodity_nodes.iter()).is_empty());
            let mut market_nodes = vec![BaseNode::new("market".to_string())];
            market_nodes[0].is_market = true;
            assert!(find_input_node_names(market_nodes.iter()).is_empty());
            let mut state_nodes = vec![BaseNode::new("state".to_string())];
            state_nodes[0].state = Some(State::default());
            assert!(find_input_node_names(state_nodes.iter()).is_empty());
            let mut res_nodes = vec![BaseNode::new("res".to_string())];
            res_nodes[0].is_res = true;
            assert!(find_input_node_names(res_nodes.iter()).is_empty());
            let mut inflow_nodes = vec![BaseNode::new("inflow".to_string())];
            inflow_nodes[0].inflow = vec![ForecastValue {
                scenario: None,
                value: BaseForecastable::Constant(Constant { value: 2.3 }),
            }];
            assert!(find_input_node_names(inflow_nodes.iter()).is_empty());
        }
        #[test]
        fn true_input_node_gets_found() {
            let input_nodes = vec![BaseNode::new("input".to_string())];
            let input_nodes = find_input_node_names(input_nodes.iter());
            assert_eq!(input_nodes.len(), 1);
            assert_eq!(input_nodes[0], "input");
        }
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
        let scenarios =
            vec![Scenario::new("S1", 1.0).expect("constructing scenario should succeed")];
        let time_series = to_time_series_data(2.3, &time_line, &scenarios);
        assert_eq!(time_series.ts_data.len(), 1);
        assert_eq!(time_series.ts_data[0].scenario, "S1");
        let mut expected_series = BTreeMap::new();
        expected_series.insert(Utc.with_ymd_and_hms(2024, 11, 19, 13, 0, 0).unwrap(), 2.3);
        expected_series.insert(Utc.with_ymd_and_hms(2024, 11, 19, 14, 0, 0).unwrap(), 2.3);
        assert_eq!(time_series.ts_data[0].series, expected_series);
    }
    mod series {
        use super::*;
        #[test]
        fn to_time_series_with_empty_series() {
            let series = Series {
                scenario: "S1".into(),
                durations: Vec::new(),
                values: Vec::new(),
            };
            let time_line = vec![
                Utc.with_ymd_and_hms(2024, 12, 18, 13, 0, 0).unwrap(),
                Utc.with_ymd_and_hms(2024, 12, 18, 14, 0, 0).unwrap(),
            ];
            let time_series = series.to_time_series(&time_line);
            assert_eq!(time_series.scenario, "S1");
            assert!(time_series.series.is_empty());
        }
        #[test]
        fn to_time_series_with_timeline_within_duration() {
            let series = Series {
                scenario: "S1".into(),
                durations: vec![Duration::try_new(24, 0, 0).unwrap()],
                values: vec![2.3],
            };
            let time_line = vec![
                Utc.with_ymd_and_hms(2024, 12, 18, 13, 0, 0).unwrap(),
                Utc.with_ymd_and_hms(2024, 12, 18, 14, 0, 0).unwrap(),
            ];
            let time_series = series.to_time_series(&time_line);
            assert_eq!(time_series.scenario, "S1");
            assert_eq!(time_series.series.len(), time_line.len());
            let mut expected_series = BTreeMap::new();
            for stamp in time_line {
                expected_series.insert(stamp, 2.3);
            }
            assert_eq!(time_series.series, expected_series)
        }
        #[test]
        fn to_time_series_with_timeline_longer_than_duration() {
            let series = Series {
                scenario: "S1".into(),
                durations: vec![Duration::try_new(1, 0, 0).unwrap()],
                values: vec![2.3],
            };
            let time_line = vec![
                Utc.with_ymd_and_hms(2024, 12, 18, 13, 0, 0).unwrap(),
                Utc.with_ymd_and_hms(2024, 12, 18, 14, 0, 0).unwrap(),
            ];
            let time_series = series.to_time_series(&time_line);
            assert_eq!(time_series.scenario, "S1");
            assert_eq!(time_series.series.len(), 1);
            let mut expected_series = BTreeMap::new();
            expected_series.insert(time_line[0], 2.3);
            assert_eq!(time_series.series, expected_series)
        }
        #[test]
        fn to_time_series_with_time_stamps_that_extend_over_multiple_durations() {
            let series = Series {
                scenario: "S1".into(),
                durations: vec![
                    Duration::try_new(0, 15, 0).unwrap(),
                    Duration::try_new(0, 30, 0).unwrap(),
                    Duration::try_new(0, 30, 0).unwrap(),
                ],
                values: vec![2.3, 3.2, 23.0],
            };
            let time_line = vec![
                Utc.with_ymd_and_hms(2024, 12, 18, 13, 0, 0).unwrap(),
                Utc.with_ymd_and_hms(2024, 12, 18, 14, 0, 0).unwrap(),
            ];
            let time_series = series.to_time_series(&time_line);
            assert_eq!(time_series.scenario, "S1");
            assert_eq!(time_series.series.len(), 2);
            let mut expected_series = BTreeMap::new();
            expected_series.insert(time_line[0], 2.3);
            expected_series.insert(time_line[1], 23.0);
            assert_eq!(time_series.series, expected_series)
        }
    }

    #[test]
    fn try_from_value_input_with_constant_only() {
        let input = ValueInput {
            scenario: Some("test_scenario".to_string()),
            constant: Some(42.0),
            series: None,
        };

        let value = Value::try_from(input).unwrap();
        assert_eq!(value.scenario, Some("test_scenario".to_string()));
        match value.value {
            SeriesValue::Constant(constant) => assert_eq!(constant.value, 42.0),
            _ => panic!("Expected SeriesValue::Constant"),
        }
    }
    #[test]
    fn try_from_value_input_with_series_only() {
        let input = ValueInput {
            scenario: Some("test_scenario".to_string()),
            constant: None,
            series: Some(vec![1.0, 2.0, 3.0]),
        };

        let value = Value::try_from(input).unwrap();
        assert_eq!(value.scenario, Some("test_scenario".to_string()));
        match value.value {
            SeriesValue::FloatList(float_list) => assert_eq!(float_list.values, vec![1.0, 2.0, 3.0]),
            _ => panic!("Expected SeriesValue::FloatList"),
        }
    }

    #[test]
    fn try_from_value_input_with_both_constant_and_series() {
        let input = ValueInput {
            scenario: Some("invalid_scenario".to_string()),
            constant: Some(42.0),
            series: Some(vec![1.0, 2.0, 3.0]),
        };

        let result = Value::try_from(input);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            "ValueInput cannot have both `constant` and `series` populated simultaneously."
        );
    }

    #[test]
    fn try_from_value_input_with_none_for_constant_and_series() {
        let input = ValueInput {
            scenario: None,
            constant: None,
            series: None,
        };

        let value = Value::try_from(input).unwrap();
        assert_eq!(value.scenario, None);
        match value.value {
            SeriesValue::Constant(constant) => assert_eq!(constant.value, 0.0),
            _ => panic!("Expected SeriesValue::Constant with default value 0.0"),
        }
    }
    type TimeLine = Vec<TimeStamp>;
    fn make_timeline(times: &[(i32, u32, u32, u32, u32, u32)]) -> TimeLine {
        times
            .iter()
            .map(|&(y, m, d, h, min, s)| Utc.with_ymd_and_hms(y, m, d, h, min, s).unwrap().into())
            .collect()
    }
    #[test]
    fn forecast_values_to_forecastable_returns_forecast_variant() {
        let fv = ForecastValue {
            scenario: None,
            value: BaseForecastable::Forecast(Forecast::new("MyForecast".to_string())),
        };
        let forecast_values = vec![fv];
        let scenarios = vec![Scenario::new("S1", 1.0).unwrap()];
        let timeline = make_timeline(&[(2025, 1, 1, 0, 0, 0), (2025, 1, 1, 1, 0, 0)]);
        let result = forecast_values_to_forecastable(&forecast_values, &scenarios, &timeline);
        match result {
            Forecastable::Forecast(f) => {
                assert_eq!(f.name(), "MyForecast");
            }
            _ => panic!("Expected Forecastable::Forecast"),
        }
    }
    #[test]
    #[should_panic(expected = "Cannot convert Forecast variant to time series")]
    fn forecast_values_to_time_series_data_errors_on_forecast_variant() {
        let fv = ForecastValue {
            scenario: Some("S1".to_string()),
            value: BaseForecastable::Forecast(Forecast::new("BadForecast".to_string())),
        };
        let forecast_values = vec![fv];
        let scenarios = vec![Scenario::new("S1", 1.0).unwrap()];
        let timeline = make_timeline(&[(2025, 1, 1, 0, 0, 0), (2025, 1, 1, 1, 0, 0)]);
        let _ = forecast_values_to_time_series_data(&forecast_values, &scenarios, &timeline).unwrap();
    }
    #[test]
    #[should_panic(expected = "Missing forecast value for scenario")]
    fn forecast_values_to_time_series_data_missing_default_error() {
        let fv = ForecastValue {
            scenario: Some("S1".to_string()),
            value: BaseForecastable::Constant(Constant { value: 10.0 }),
        };
        let forecast_values = vec![fv];
        let scenarios = vec![
            Scenario::new("S1", 1.0).unwrap(),
            Scenario::new("S2", 1.0).unwrap(),
        ];
        let timeline = make_timeline(&[(2025, 1, 1, 0, 0, 0), (2025, 1, 1, 1, 0, 0)]);
        let _ = forecast_values_to_time_series_data(&forecast_values, &scenarios, &timeline).unwrap();
    }
    #[test]
    #[should_panic(expected = "Multiple default forecast values found")]
    fn forecast_values_to_time_series_data_multiple_defaults_error() {
        let fv1 = ForecastValue {
            scenario: None,
            value: BaseForecastable::Constant(Constant { value: 10.0 }),
        };
        let fv2 = ForecastValue {
            scenario: None,
            value: BaseForecastable::Constant(Constant { value: 20.0 }),
        };
        let forecast_values = vec![fv1, fv2];
        let scenarios = vec![Scenario::new("S1", 1.0).unwrap()];
        let timeline = make_timeline(&[(2025, 1, 1, 0, 0, 0), (2025, 1, 1, 1, 0, 0)]);
        let _ = forecast_values_to_time_series_data(&forecast_values, &scenarios, &timeline).unwrap();
    }
    #[test]
    fn forecast_values_to_forecastable_returns_time_series_data() {
        let fv_default = ForecastValue {
            scenario: None,
            value: BaseForecastable::Constant(Constant { value: 42.0 }),
        };
        let fv_s1 = ForecastValue {
            scenario: Some("S1".to_string()),
            value: BaseForecastable::Constant(Constant { value: 10.0 }),
        };
        let forecast_values = vec![fv_default, fv_s1];
        let scenarios = vec![
            Scenario::new("S1", 1.0).unwrap(),
            Scenario::new("S2", 1.0).unwrap(),
        ];
        let timeline = make_timeline(&[(2025, 1, 1, 0, 0, 0), (2025, 1, 1, 1, 0, 0)]);
        let result = forecast_values_to_forecastable(&forecast_values, &scenarios, &timeline);
        match result {
            Forecastable::TimeSeriesData(ts_data) => {
                let ts_map: BTreeMap<_, _> = ts_data.ts_data.into_iter()
                    .map(|ts| (ts.scenario, ts.series))
                    .collect();
                let expected_s1: BTreeMap<TimeStamp, f64> = timeline
                    .iter()
                    .cloned()
                    .map(|ts| (ts, 10.0))
                    .collect();
                let expected_s2: BTreeMap<TimeStamp, f64> = timeline
                    .iter()
                    .cloned()
                    .map(|ts| (ts, 42.0))
                    .collect();
                assert_eq!(ts_map.get("S1").unwrap(), &expected_s1);
                assert_eq!(ts_map.get("S2").unwrap(), &expected_s2);
            }
            _ => panic!("Expected Forecastable::TimeSeriesData"),
        }
    }
    // Test A single default (scenario = None) constant value is applied to all scenarios.
    #[test]
    fn test_values_to_time_series_data_default_constant() {
        let timeline = make_timeline(&[
            (2025, 1, 1, 0, 0, 0),
            (2025, 1, 1, 1, 0, 0),
            (2025, 1, 1, 2, 0, 0),
        ]);
        let scenarios = vec![
            Scenario::new("S1", 1.0).expect("failed to create scenario"),
            Scenario::new("S2", 1.0).expect("failed to create scenario"),
        ];
        let value = Value {
            scenario: None,
            value: SeriesValue::Constant(Constant { value: 42.0 }),
        };
        let values = vec![value];

        let result = values_to_time_series_data(values, scenarios.clone(), timeline.clone());
        assert!(result.is_ok(), "Expected Ok result, got error: {:?}", result.err());
        let ts_data = result.unwrap();

        assert_eq!(
            ts_data.ts_data.len(),
            1 + scenarios.len(),
            "Expected default + one entry per scenario"
        );

        for ts in ts_data.ts_data {
            assert_eq!(ts.series.len(), timeline.len());
            for (&_ts, &v) in ts.series.iter() {
                assert_eq!(v, 42.0);
            }
            assert!(
                ts.scenario == "Default"
                    || scenarios.iter().any(|s| s.name() == &ts.scenario),
                "Unexpected scenario name: {}",
                ts.scenario
            );
        }
    }
    // Test: Two values provided with scenario-specific constant values.
    #[test]
    fn test_values_to_time_series_data_scenario_specific() {
        let timeline = make_timeline(&[(2025, 1, 1, 0, 0, 0), (2025, 1, 1, 1, 0, 0)]);
        let scenarios = vec![
            Scenario::new("S1", 1.0).expect("failed to create scenario"),
            Scenario::new("S2", 1.0).expect("failed to create scenario"),
        ];

        let value1 = Value {
            scenario: Some("S1".to_string()),
            value: SeriesValue::Constant(Constant { value: 10.0 }),
        };
        let value2 = Value {
            scenario: Some("S2".to_string()),
            value: SeriesValue::Constant(Constant { value: 20.0 }),
        };
        let values = vec![value1, value2];

        let result = values_to_time_series_data(values, scenarios.clone(), timeline.clone());
        assert!(result.is_ok());
        let ts_data = result.unwrap();
        assert_eq!(ts_data.ts_data.len(), 2);

        for ts in ts_data.ts_data {
            assert_eq!(ts.series.len(), timeline.len());
            match ts.scenario.as_str() {
                "S1" => {
                    for &v in ts.series.values() {
                        assert_eq!(v, 10.0);
                    }
                }
                "S2" => {
                    for &v in ts.series.values() {
                        assert_eq!(v, 20.0);
                    }
                }
                other => panic!("Unexpected scenario name: {}", other),
            }
        }
    }

    // Test: When more than one default value is provided, an error is returned.
    #[test]
    fn test_values_to_time_series_data_multiple_defaults_error() {
        let timeline = make_timeline(&[(2025, 1, 1, 0, 0, 0), (2025, 1, 1, 1, 0, 0)]);
        let scenarios = vec![Scenario::new("S1", 1.0).expect("failed to create scenario")];

        let value1 = Value {
            scenario: None,
            value: SeriesValue::Constant(Constant { value: 5.0 }),
        };
        let value2 = Value {
            scenario: None,
            value: SeriesValue::Constant(Constant { value: 10.0 }),
        };
        let values = vec![value1, value2];

        let result = values_to_time_series_data(values, scenarios, timeline);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            "Multiple default values found (scenario = None)".to_string()
        );
    }

    // Test: When a scenario-specific value refers to a scenario not in the provided list.
    #[test]
    fn test_values_to_time_series_data_nonexistent_scenario_error() {
        let timeline = make_timeline(&[(2025, 1, 1, 0, 0, 0), (2025, 1, 1, 1, 0, 0)]);
        let scenarios = vec![Scenario::new("S1", 1.0).expect("failed to create scenario")];

        let value = Value {
            scenario: Some("S2".to_string()),
            value: SeriesValue::Constant(Constant { value: 15.0 }),
        };
        let values = vec![value];

        let result = values_to_time_series_data(values, scenarios, timeline);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            "Scenario 'S2' specified in Value does not exist in provided scenarios".to_string()
        );
    }

    // Test: When a scenario-specific value is given for one scenario but not all scenarios are covered
    // and no default value is provided, an error is returned.
    #[test]
    fn test_values_to_time_series_data_missing_default_error() {
        let timeline = make_timeline(&[(2025, 1, 1, 0, 0, 0), (2025, 1, 1, 1, 0, 0)]);
        let scenarios = vec![
            Scenario::new("S1", 1.0).expect("failed to create scenario"),
            Scenario::new("S2", 1.0).expect("failed to create scenario"),
        ];

        let value = Value {
            scenario: Some("S1".to_string()),
            value: SeriesValue::Constant(Constant { value: 100.0 }),
        };
        let values = vec![value];

        let result = values_to_time_series_data(values, scenarios, timeline);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            "Missing default values for scenarios: [\"S2\"]".to_string()
        );
    }

    // Test: A value with a FloatList that matches the timeline length is converted correctly.
    #[test]
    fn test_values_to_time_series_data_float_list() {
        let timeline = make_timeline(&[
            (2025, 1, 1, 0, 0, 0),
            (2025, 1, 1, 1, 0, 0),
            (2025, 1, 1, 2, 0, 0),
        ]);
        let scenarios = vec![Scenario::new("S1", 1.0).expect("failed to create scenario")];

        let value = Value {
            scenario: Some("S1".to_string()),
            value: SeriesValue::FloatList(FloatList {
                values: vec![1.1, 2.2, 3.3],
            }),
        };
        let values = vec![value];

        let result = values_to_time_series_data(values, scenarios, timeline.clone());
        assert!(result.is_ok());
        let ts_data = result.unwrap();
        assert_eq!(ts_data.ts_data.len(), 1);
        let ts = &ts_data.ts_data[0];
        assert_eq!(ts.scenario, "S1");
        for (i, ts_stamp) in timeline.iter().enumerate() {
            assert_eq!(
                *ts.series.get(ts_stamp).expect("timestamp missing"),
                vec![1.1, 2.2, 3.3][i]
            );
        }
    }

    // Test: A FloatList value with a length mismatch relative to the timeline returns an error.
    #[test]
    fn test_values_to_time_series_data_float_list_length_mismatch() {
        let timeline = make_timeline(&[(2025, 1, 1, 0, 0, 0), (2025, 1, 1, 1, 0, 0)]);
        let scenarios = vec![Scenario::new("S1", 1.0).expect("failed to create scenario")];
        let value = Value {
            scenario: Some("S1".to_string()),
            value: SeriesValue::FloatList(FloatList { values: vec![1.0] }),
        };
        let values = vec![value];

        let result = values_to_time_series_data(values, scenarios, timeline);
        assert!(result.is_err());
        let expected_err = format!(
            "time series mismatch in FloatList, expected length {}, found {}",
            2, 1
        );
        assert_eq!(result.err().unwrap(), expected_err);
    }
}
