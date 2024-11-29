use super::{ValidationError, ValidationErrors};
use crate::input_data_base::BaseGenConstraint;
use juniper::GraphQLInputObject;

#[derive(GraphQLInputObject)]
pub struct AddGenConstraintInput {
    name: String,
    gc_type: String,
    is_setpoint: bool,
    penalty: f64,
    constant: f64,
}

impl AddGenConstraintInput {
    fn to_gen_constraint(self) -> BaseGenConstraint {
        BaseGenConstraint {
            name: self.name,
            gc_type: self.gc_type,
            is_setpoint: self.is_setpoint,
            penalty: self.penalty,
            factors: Vec::new(),
            constant: self.constant,
        }
    }
}

pub fn add_gen_constraint(
    constraint: AddGenConstraintInput,
    constraints: &mut Vec<BaseGenConstraint>,
) -> ValidationErrors {
    let errors = validate_gen_contraint_to_add(&constraint, constraints);
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    constraints.push(constraint.to_gen_constraint());
    ValidationErrors::default()
}

fn validate_gen_contraint_to_add(
    constraint: &AddGenConstraintInput,
    constraints: &Vec<BaseGenConstraint>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if constraint.name.is_empty() {
        errors.push(ValidationError::new("name", "name is empty"));
    }
    if constraints
        .iter()
        .find(|c| c.name == constraint.name)
        .is_some()
    {
        errors.push(ValidationError::new(
            "name",
            "a constraint with the same name exists",
        ));
    }
    if ["eq", "st", "gt"]
        .iter()
        .find(|c| **c == constraint.gc_type)
        .is_none()
    {
        errors.push(ValidationError::new(
            "gc_type",
            "should be 'eq', 'st' or 'gt'",
        ));
    }
    errors
}
