use super::{ValidationError, ValidationErrors};
use crate::input_data_base::Risk;
use juniper::GraphQLInputObject;

#[derive(GraphQLInputObject)]
pub struct NewRisk {
    parameter: String,
    value: f64,
}

impl NewRisk {
    fn to_risk(self) -> Risk {
        Risk {
            parameter: self.parameter,
            value: self.value,
        }
    }
}

pub fn create_risk(risk: NewRisk, risks: &mut Vec<Risk>) -> ValidationErrors {
    let errors = validate_risk_creation(&risk);
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    risks.push(risk.to_risk());
    ValidationErrors::default()
}

fn validate_risk_creation(risk: &NewRisk) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if risk.parameter.is_empty() {
        errors.push(ValidationError::new("parameter", "parameter is empty"));
    }
    errors
}
