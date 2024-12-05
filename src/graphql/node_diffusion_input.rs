use super::{ValidationError, ValidationErrors};
use crate::input_data_base::{BaseNode, BaseNodeDiffusion};

fn to_node_diffusion(from_node: String, to_node: String, coefficient: f64) -> BaseNodeDiffusion {
    BaseNodeDiffusion {
        from_node,
        to_node,
        coefficient,
    }
}

pub fn create_node_diffusion(
    from_node: String,
    to_node: String,
    coefficient: f64,
    diffusions: &mut Vec<BaseNodeDiffusion>,
    nodes: &Vec<BaseNode>,
) -> ValidationErrors {
    let errors = validate_node_diffusion_creation(&from_node, &to_node, diffusions, nodes);
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    diffusions.push(to_node_diffusion(from_node, to_node, coefficient));
    ValidationErrors::default()
}

fn validate_node_diffusion_creation(
    from_node: &String,
    to_node: &String,
    diffusions: &Vec<BaseNodeDiffusion>,
    nodes: &Vec<BaseNode>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if from_node == to_node {
        errors.push(ValidationError::new(
            "from_node",
            "to node and from node are the same",
        ));
    }
    if nodes.iter().find(|n| n.name == *from_node).is_none() {
        errors.push(ValidationError::new("from_node", "no such node"));
    }
    if nodes.iter().find(|n| n.name == *to_node).is_none() {
        errors.push(ValidationError::new("to_node", "no such node"));
    }
    if diffusions
        .iter()
        .find(|d| d.from_node == *from_node && d.to_node == *to_node)
        .is_some()
    {
        errors.push(ValidationError::new(
            "from_node",
            "a node diffusion with the same to and from nodes exists",
        ));
    }
    errors
}
