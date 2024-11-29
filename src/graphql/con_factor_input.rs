use super::{ValidationError, ValidationErrors};
use crate::input_data_base::{BaseConFactor, BaseGenConstraint, VariableId};
use juniper::{GraphQLInputObject, GraphQLObject, GraphQLUnion};

#[derive(GraphQLInputObject)]
pub struct AddConFactorInput {
    pub var_type: String,
    pub var_tuple: AddVariableIdInput,
    pub data: f64,
}

impl AddConFactorInput {
    fn to_con_factor(self) -> BaseConFactor {
        BaseConFactor {
            var_type: self.var_type,
            var_tuple: self.var_tuple.to_variable_id(),
            data: self.data,
        }
    }
}

#[derive(GraphQLInputObject)]
pub struct AddVariableIdInput {
    pub entity: String,
    pub identifier: String,
}

impl AddVariableIdInput {
    fn to_variable_id(self) -> VariableId {
        VariableId {
            entity: self.entity,
            identifier: self.identifier,
        }
    }
}

#[derive(GraphQLObject)]
pub struct ConFactorId {
    constraint_name: String,
    factor_index: i32,
}

impl ConFactorId {
    fn new(constraint_name: &str, factor_index: i32) -> Self {
        ConFactorId {
            constraint_name: String::from(constraint_name),
            factor_index,
        }
    }
}

#[derive(GraphQLUnion)]
pub enum AddConFactorResult {
    Ok(ConFactorId),
    Err(ValidationErrors),
}

pub fn add_con_factor_to_constraint(
    factor: AddConFactorInput,
    constraint_name: String,
    constraints: &mut Vec<BaseGenConstraint>,
) -> AddConFactorResult {
    let errors = validate_con_factor_to_add(&factor);
    if !errors.is_empty() {
        return AddConFactorResult::Err(ValidationErrors::from(errors));
    }
    let constraint = match constraints.iter_mut().find(|c| c.name == constraint_name) {
        Some(constraint) => constraint,
        None => {
            return AddConFactorResult::Err(ValidationErrors::from(vec![ValidationError::new(
                "constraint_name",
                "no such constraint",
            )]))
        }
    };
    constraint.factors.push(factor.to_con_factor());
    AddConFactorResult::Ok(ConFactorId::new(
        &constraint_name,
        (constraint.factors.len() - 1) as i32,
    ))
}

fn validate_con_factor_to_add(factor: &AddConFactorInput) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if factor.var_tuple.entity.is_empty() {
        errors.push(ValidationError::new("var_tuple.entity", "entity is empty"));
    }
    if ["v_flow", "v_state", "v_online"]
        .iter()
        .find(|t| **t == factor.var_type)
        .is_none()
    {
        errors.push(ValidationError::new(
            "var_type",
            "should be 'v_flow', 'v_state' or 'v_online'",
        ));
    }
    errors
}
