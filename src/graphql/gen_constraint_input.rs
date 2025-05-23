use super::delete;
use super::{MaybeError, ValidationError, ValidationErrors};
use crate::input_data_base::{BaseGenConstraint, ConstraintType, Value, ValueInput};
use juniper::GraphQLInputObject;

#[derive(GraphQLInputObject)]
pub struct NewGenConstraint {
    name: String,
    gc_type: ConstraintType,
    is_setpoint: bool,
    penalty: f64,
    constant: Vec<ValueInput>,
}

impl NewGenConstraint {
    fn to_gen_constraint(self) -> BaseGenConstraint {
        BaseGenConstraint {
            name: self.name,
            gc_type: self.gc_type,
            is_setpoint: self.is_setpoint,
            penalty: self.penalty,
            factors: Vec::new(),
            constant: self
            .constant
            .into_iter()
            .map(Value::try_from)
            .collect::<Result<Vec<Value>, _>>()
            .expect("Could not parse cost values"),
        }
    }
}

pub fn create_gen_constraint(
    constraint: NewGenConstraint,
    constraints: &mut Vec<BaseGenConstraint>,
) -> ValidationErrors {
    let errors = validate_gen_contraint_creation(&constraint, constraints);
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    constraints.push(constraint.to_gen_constraint());
    ValidationErrors::default()
}

fn validate_gen_contraint_creation(
    constraint: &NewGenConstraint,
    constraints: &Vec<BaseGenConstraint>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if constraint.name.is_empty() {
        errors.push(ValidationError::new("name", "name is empty"));
    }
    if constraints.iter().any(|c| c.name == constraint.name) {
        errors.push(ValidationError::new(
            "name",
            "a constraint with the same name exists",
        ));
    }
    errors
}

pub fn delete_gen_constraint(name: &str, constraints: &mut Vec<BaseGenConstraint>) -> MaybeError {
    delete::delete_named(name, constraints)
}
