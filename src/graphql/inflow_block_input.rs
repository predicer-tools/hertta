use juniper::GraphQLInputObject;
use super::delete;
use super::{MaybeError, ValidationError, ValidationErrors};
use crate::input_data_base::{BaseInflowBlock, ValueInput, Value};

#[derive(GraphQLInputObject, Debug, Clone)]
pub struct NewInflowBlock {
    pub name: String,
    pub node: String,
    pub data: Vec<ValueInput>,
}

impl NewInflowBlock {
    pub fn to_inflow_block(self) -> BaseInflowBlock {
        BaseInflowBlock {
            name: self.name,
            node: self.node,
            data: self.data
            .into_iter()
            .map(Value::try_from)
            .collect::<Result<Vec<Value>, _>>()
            .expect("Could not parse cost values"),
        }
    }
}

pub fn create_inflow_block(inflow_block: NewInflowBlock, inflow_blocks: &mut Vec<BaseInflowBlock>) -> ValidationErrors {
    let errors = validate_inflow_block_creation(&inflow_block);
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    inflow_blocks.push(inflow_block.to_inflow_block());
    ValidationErrors::default()
}

fn validate_inflow_block_creation(inflow_block: &NewInflowBlock) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if inflow_block.name.is_empty() {
        errors.push(ValidationError::new("name", "name is empty"));
    }
    if inflow_block.node.is_empty() {
        errors.push(ValidationError::new("node", "node is empty"));
    }
    if inflow_block.data.is_empty() {
        errors.push(ValidationError::new("data", "data is empty"));
    }
    errors
}

pub fn _delete_inflow_block(parameter: &str, inflow_blocks: &mut Vec<BaseInflowBlock>) -> MaybeError {
    delete::delete_named(parameter, inflow_blocks)
}

