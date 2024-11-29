use super::{ValidationError, ValidationErrors};
use crate::input_data_base::{BaseNode, BaseNodeDiffusion};
use juniper::GraphQLInputObject;

#[derive(GraphQLInputObject)]
pub struct AddNodeDiffusionInput {
    from_node: String,
    to_node: String,
    coefficient: f64,
}

impl AddNodeDiffusionInput {
    fn to_node_diffusion(self) -> BaseNodeDiffusion {
        BaseNodeDiffusion {
            from_node: self.from_node,
            to_node: self.to_node,
            coefficient: self.coefficient,
        }
    }
}

pub fn add_node_diffusion(
    diffusion: AddNodeDiffusionInput,
    diffusions: &mut Vec<BaseNodeDiffusion>,
    nodes: &Vec<BaseNode>,
) -> ValidationErrors {
    let errors = validate_node_diffusion(&diffusion, nodes);
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    diffusions.push(diffusion.to_node_diffusion());
    ValidationErrors::default()
}

fn validate_node_diffusion(
    diffusion: &AddNodeDiffusionInput,
    nodes: &Vec<BaseNode>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if diffusion.from_node == diffusion.to_node {
        errors.push(ValidationError::new(
            "from_node",
            "to node and from node are the same",
        ));
    }
    if nodes
        .iter()
        .find(|n| n.name == diffusion.from_node)
        .is_none()
    {
        errors.push(ValidationError::new("from_node", "no such node"));
    }
    if nodes.iter().find(|n| n.name == diffusion.to_node).is_none() {
        errors.push(ValidationError::new("to_node", "no such node"));
    }
    errors
}
