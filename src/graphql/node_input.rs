use super::{ValidationError, ValidationErrors};
use crate::input_data_base::{BaseNode, BaseProcess};
use juniper::GraphQLInputObject;

#[derive(GraphQLInputObject)]
pub struct AddNodeInput {
    name: String,
    is_commodity: bool,
    is_market: bool,
    is_state: bool,
    is_res: bool,
    is_inflow: bool,
    cost: f64,
    inflow: f64,
}

impl AddNodeInput {
    fn to_node(self) -> BaseNode {
        BaseNode {
            name: self.name,
            groups: Vec::new(),
            is_commodity: self.is_commodity,
            is_market: self.is_market,
            is_state: self.is_state,
            is_res: self.is_res,
            is_inflow: self.is_inflow,
            state: None,
            cost: self.cost,
            inflow: self.inflow,
        }
    }
}

#[derive(GraphQLInputObject)]
pub struct NodeInput {
    name: Option<String>,
    is_commodity: Option<bool>,
    is_market: Option<bool>,
    is_state: Option<bool>,
    is_res: Option<bool>,
    is_inflow: Option<bool>,
    cost: Option<f64>,
    inflow: Option<f64>,
}

pub fn add_node(
    node: AddNodeInput,
    nodes: &mut Vec<BaseNode>,
    processes: &mut Vec<BaseProcess>,
) -> ValidationErrors {
    let errors = validate_node_to_add(&node, nodes, processes);
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    nodes.push(node.to_node());
    ValidationErrors::default()
}

fn validate_node_to_add(
    node: &AddNodeInput,
    nodes: &Vec<BaseNode>,
    processes: &Vec<BaseProcess>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if node.name.is_empty() {
        errors.push(ValidationError::new("name", "name is empty"));
    }
    if nodes.iter().find(|n| n.name == node.name).is_some() {
        errors.push(ValidationError::new(
            "name",
            "a node with the same name exists",
        ));
    }
    if processes.iter().find(|p| p.name == node.name).is_some() {
        errors.push(ValidationError::new(
            "name",
            "a process with the same name exists",
        ));
    }
    errors
}