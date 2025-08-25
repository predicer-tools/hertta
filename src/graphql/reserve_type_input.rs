use juniper::GraphQLInputObject;
use super::delete;
use super::{MaybeError, ValidationError, ValidationErrors};

use crate::input_data_base::ReserveType;

#[derive(GraphQLInputObject, Debug, Clone)]
pub struct NewReserveType {
    pub name: String,
    pub ramp_rate: f64,
}

impl NewReserveType {
pub fn to_reserve_type(self) -> ReserveType {
    ReserveType {
        name: self.name,
        ramp_rate: self.ramp_rate,
     }
    }
}

#[derive(GraphQLInputObject, Debug, Clone)]
pub struct ReserveTypeUpdate {
    pub name: Option<String>,
    pub ramp_rate: Option<f64>,
}

pub fn create_reserve_type(reserve_type: NewReserveType, reserve_types: &mut Vec<ReserveType>) -> ValidationErrors {
    let errors = validate_reserve_type_creation(&reserve_type);
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    reserve_types.push(reserve_type.to_reserve_type());
    ValidationErrors::default()
}

fn validate_reserve_type_creation(reserve_type: &NewReserveType) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if reserve_type.name.is_empty() {
        errors.push(ValidationError::new("name", "name is empty"));
    }
    errors
}

pub fn delete_reserve_type(parameter: &str, reserve_types: &mut Vec<ReserveType>) -> MaybeError {
    delete::delete_named(parameter, reserve_types)
}

