use super::delete;
use super::forecastable;
use super::{MaybeError, ValidationError, ValidationErrors};
use crate::input_data::Forecast;
use crate::input_data_base::{
    BaseForecastable, BaseGenConstraint, BaseInflowBlock, BaseMarket, BaseNode, BaseNodeDiffusion,
    BaseNodeHistory, BaseProcess, ConstraintFactorType, Delay, NodeGroup, ValueInput, Value, ForecastValue, ForecastValueInput
};
use crate::scenarios::Scenario;
use juniper::GraphQLInputObject;

#[derive(GraphQLInputObject)]
pub struct NewNode {
    name: String,
    is_commodity: bool,
    is_market: bool,
    is_res: bool,
    cost: Vec<ValueInput>,
    inflow: Vec<ForecastValueInput>,
}

impl NewNode {
    fn to_node(self, scenarios: &Vec<Scenario>) -> BaseNode {
        BaseNode {
            name: self.name,
            groups: Vec::new(),
            is_commodity: self.is_commodity,
            is_market: self.is_market,
            is_res: self.is_res,
            state: None,
            cost: self
            .cost
            .into_iter()
            .map(Value::try_from)
            .collect::<Result<Vec<Value>, _>>()
            .expect("Could not parse cost values"),
            inflow: forecastable::convert_forecast_value_inputs(self.inflow, scenarios),
        }
    }
}

#[derive(GraphQLInputObject)]
pub struct NodeUpdate {
    name: Option<String>,
    is_commodity: Option<bool>,
    is_market: Option<bool>,
    is_state: Option<bool>,
    is_res: Option<bool>,
    is_inflow: Option<bool>,
    cost: Option<f64>,
    inflow: Option<f64>,
}

pub fn create_node(
    node: NewNode,
    nodes: &mut Vec<BaseNode>,
    processes: &mut Vec<BaseProcess>,
    scenarios: &mut Vec<Scenario>,
) -> ValidationErrors {
    let errors = validate_node_creation(&node, nodes, processes);
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    nodes.push(node.to_node(scenarios));
    ValidationErrors::default()
}

fn validate_node_creation(
    node: &NewNode,
    nodes: &Vec<BaseNode>,
    processes: &Vec<BaseProcess>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if node.name.is_empty() {
        errors.push(ValidationError::new("name", "name is empty"));
    }
    if nodes.iter().any(|n| n.name == node.name) {
        errors.push(ValidationError::new(
            "name",
            "a node with the same name exists",
        ));
    }
    if processes.iter().any(|p| p.name == node.name) {
        errors.push(ValidationError::new(
            "name",
            "a process with the same name exists",
        ));
    }
    errors
}

pub fn connect_node_inflow_to_temperature_forecast(
    node_name: &str,
    forecast_name: String,
    forecast_type: String,
    nodes: &mut Vec<BaseNode>,
) -> MaybeError {
    let node = match nodes.iter_mut().find(|n| n.name == node_name) {
        Some(node) => node,
        None => return "no such node".into(),
    };
    node.inflow = vec![ForecastValue {
        scenario: None,
        value: BaseForecastable::Forecast(Forecast::new(forecast_name, forecast_type)),
    }];
    MaybeError::new_ok()
}

pub fn delete_node(
    name: &str,
    nodes: &mut Vec<BaseNode>,
    groups: &mut Vec<NodeGroup>,
    processes: &mut Vec<BaseProcess>,
    diffusions: &mut Vec<BaseNodeDiffusion>,
    delays: &mut Vec<Delay>,
    histories: &mut Vec<BaseNodeHistory>,
    markets: &mut Vec<BaseMarket>,
    inflow_blocks: &mut Vec<BaseInflowBlock>,
    constraints: &mut Vec<BaseGenConstraint>,
) -> MaybeError {
    let maybe_error = delete::delete_named(name, nodes);
    if maybe_error.is_error() {
        return maybe_error;
    }
    for group in groups {
        if let Some(position) = group.members.iter().position(|m| m == name) {
            group.members.swap_remove(position);
        }
    }
    for process in processes {
        process.topos.retain(|t| t.source != name && t.sink != name);
    }
    diffusions.retain(|d| d.from_node != name && d.to_node != name);
    delays.retain(|d| d.from_node != name && d.to_node != name);
    if let Some(position) = histories.iter().position(|h| h.node == name) {
        histories.swap_remove(position);
    }
    markets.retain(|m| m.node != name);
    inflow_blocks.retain(|i| i.node != name);
    for constraint in constraints {
        constraint.factors.retain(|f| match f.var_type {
            ConstraintFactorType::Flow => {
                f.var_tuple.identifier.as_ref().is_some_and(|n| n != name)
            }
            ConstraintFactorType::Online => true,
            ConstraintFactorType::State => f.var_tuple.entity == name,
        });
    }
    MaybeError::new_ok()
}


