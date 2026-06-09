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

#[derive(GraphQLInputObject)]
pub struct GenConstraintUpdate {
    gc_type: Option<ConstraintType>,
    is_setpoint: Option<bool>,
    penalty: Option<f64>,
    constant: Option<Vec<ValueInput>>,
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

pub fn update_gen_constraint(
    name: &str,
    update: GenConstraintUpdate,
    constraints: &mut Vec<BaseGenConstraint>,
) -> ValidationErrors {
    let constraint = match constraints.iter_mut().find(|constraint| constraint.name == name) {
        Some(constraint) => constraint,
        None => {
            return ValidationErrors::from(ValidationError::new("name", "no such constraint"));
        }
    };

    let constant = match update.constant {
        Some(values) => match values
            .into_iter()
            .map(Value::try_from)
            .collect::<Result<Vec<Value>, _>>()
        {
            Ok(values) => Some(values),
            Err(error) => {
                return ValidationErrors::from(ValidationError::new("constant", &error));
            }
        },
        None => None,
    };

    if let Some(gc_type) = update.gc_type {
        constraint.gc_type = gc_type;
    }
    if let Some(is_setpoint) = update.is_setpoint {
        constraint.is_setpoint = is_setpoint;
    }
    if let Some(penalty) = update.penalty {
        constraint.penalty = penalty;
    }
    if let Some(constant) = constant {
        constraint.constant = constant;
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    fn constraint() -> BaseGenConstraint {
        BaseGenConstraint {
            name: "temperature_min".into(),
            gc_type: ConstraintType::GreaterThan,
            is_setpoint: true,
            penalty: 15.0,
            factors: Vec::new(),
            constant: Vec::new(),
        }
    }

    #[test]
    fn update_gen_constraint_updates_provided_fields_only() {
        let mut constraints = vec![constraint()];
        let update = GenConstraintUpdate {
            gc_type: Some(ConstraintType::Equal),
            is_setpoint: None,
            penalty: Some(25.0),
            constant: None,
        };

        let errors = update_gen_constraint("temperature_min", update, &mut constraints);

        assert!(errors.errors.is_empty());
        assert!(matches!(constraints[0].gc_type, ConstraintType::Equal));
        assert!(constraints[0].is_setpoint);
        assert_eq!(constraints[0].penalty, 25.0);
    }

    #[test]
    fn update_gen_constraint_reports_missing_constraint() {
        let mut constraints = Vec::new();
        let update = GenConstraintUpdate {
            gc_type: None,
            is_setpoint: None,
            penalty: Some(25.0),
            constant: None,
        };

        let errors = update_gen_constraint("missing", update, &mut constraints);

        assert_eq!(errors.errors.len(), 1);
    }
}
