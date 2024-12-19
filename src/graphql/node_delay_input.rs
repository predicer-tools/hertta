use super::{MaybeError, ValidationError, ValidationErrors};
use crate::input_data_base::{BaseNode, Delay};
use juniper::GraphQLInputObject;

#[derive(GraphQLInputObject)]
pub struct NewNodeDelay {
    from_node: String,
    to_node: String,
    delay: f64,
    min_delay_flow: f64,
    max_delay_flow: f64,
}

impl NewNodeDelay {
    fn to_delay(self) -> Delay {
        Delay {
            from_node: self.from_node,
            to_node: self.to_node,
            delay: self.delay,
            min_delay_flow: self.min_delay_flow,
            max_delay_flow: self.max_delay_flow,
        }
    }
}

pub fn create_node_delay(
    delay: NewNodeDelay,
    delays: &mut Vec<Delay>,
    nodes: &Vec<BaseNode>,
) -> ValidationErrors {
    let errors = validate_node_creation(&delay, delays, nodes);
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    delays.push(delay.to_delay());
    ValidationErrors::default()
}

fn validate_node_creation(
    delay: &NewNodeDelay,
    delays: &Vec<Delay>,
    nodes: &Vec<BaseNode>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if delay.from_node.is_empty() {
        errors.push(ValidationError::new("from_node", "node name is empty"));
    }
    if delay.to_node.is_empty() {
        errors.push(ValidationError::new("to_node", "node name is empty"));
    }
    if delay.from_node == delay.to_node {
        errors.push(ValidationError::new("from_node", "same as to_node"));
    }
    if !nodes.iter().any(|n| n.name == delay.from_node) {
        errors.push(ValidationError::new("from_node", "no such node"));
    }
    if !nodes.iter().any(|n| n.name == delay.to_node) {
        errors.push(ValidationError::new("to_node", "no such node"));
    }
    if delays
        .iter()
        .any(|d| d.from_node == delay.from_node && d.to_node == delay.to_node)
    {
        errors.push(ValidationError::new(
            "from_node",
            "a delay with same from_node and to_node exists",
        ));
    }
    if delay.min_delay_flow > delay.max_delay_flow {
        errors.push(ValidationError::new(
            "min_delay_flow",
            "greater than max_delay_flow",
        ))
    }
    errors
}

pub fn delete_node_delay(from_node: &str, to_node: &str, delays: &mut Vec<Delay>) -> MaybeError {
    let position = match delays
        .iter()
        .position(|d| d.from_node == from_node && d.to_node == to_node)
    {
        Some(position) => position,
        None => return "no such node delay".into(),
    };
    delays.swap_remove(position);
    MaybeError::new_ok()
}
