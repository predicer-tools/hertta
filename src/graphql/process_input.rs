use super::{ValidationError, ValidationErrors};
use crate::input_data_base::{BaseNode, BaseProcess};
use juniper::GraphQLInputObject;

#[derive(GraphQLInputObject)]
pub struct AddProcessInput {
    name: String,
    #[graphql(description = "Must be 'unit' or 'transport'.")]
    conversion: String,
    is_cf_fix: bool,
    is_online: bool,
    is_res: bool,
    eff: f64,
    load_min: f64,
    load_max: f64,
    start_cost: f64,
    min_online: f64,
    max_online: f64,
    min_offline: f64,
    max_offline: f64,
    initial_state: bool,
    is_scenario_independent: bool,
    cf: Option<f64>,
    eff_ts: Option<f64>,
}

impl AddProcessInput {
    fn to_process(self) -> BaseProcess {
        BaseProcess {
            name: self.name,
            groups: Vec::new(),
            conversion: self.conversion,
            is_cf: self.cf.is_some(),
            is_cf_fix: self.is_cf_fix,
            is_online: self.is_online,
            is_res: self.is_res,
            eff: self.eff,
            load_min: self.load_min,
            load_max: self.load_max,
            start_cost: self.start_cost,
            min_online: self.min_online,
            min_offline: self.min_offline,
            max_online: self.max_online,
            max_offline: self.max_offline,
            initial_state: self.initial_state,
            is_scenario_independent: self.is_scenario_independent,
            topos: Vec::new(),
            cf: self.cf.unwrap_or(0.0),
            eff_ts: self.eff_ts,
            eff_ops: Vec::new(),
            eff_fun: Vec::new(),
        }
    }
}

pub fn add_process(
    process: AddProcessInput,
    processes: &mut Vec<BaseProcess>,
    nodes: &mut Vec<BaseNode>,
) -> ValidationErrors {
    let errors = validate_process_to_add(&process, processes, nodes);
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    processes.push(process.to_process());
    ValidationErrors::default()
}

fn validate_process_to_add(
    process: &AddProcessInput,
    processes: &Vec<BaseProcess>,
    nodes: &Vec<BaseNode>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if process.name.is_empty() {
        errors.push(ValidationError::new("name", "name is empty"));
    }
    if processes.iter().find(|p| p.name == process.name).is_some() {
        errors.push(ValidationError::new(
            "name",
            "a process with the same name exists",
        ));
    }
    if nodes.iter().find(|n| n.name == process.name).is_some() {
        errors.push(ValidationError::new(
            "name",
            "a node with the same name exists",
        ));
    }
    if ["unit", "transport", "market"]
        .iter()
        .find(|conversion| **conversion == process.conversion)
        .is_none()
    {
        errors.push(ValidationError::new(
            "conversion",
            "should be 'unit', 'transport' or 'market'",
        ));
    }
    if process.load_min < 0.0 || process.load_min > 1.0 {
        errors.push(ValidationError::new("load_min", "should be in [0, 1]"))
    }
    if process.load_min > process.load_max {
        errors.push(ValidationError::new("load_min", "greater than load_max"));
    }
    if process.load_max < 0.0 || process.load_max > 1.0 {
        errors.push(ValidationError::new("load_max", "should be in [0, 1]"));
    }
    if process.min_offline > process.max_offline {
        errors.push(ValidationError::new(
            "min_offline",
            "greater than max_offline",
        ));
    }
    if process.min_online > process.max_online {
        errors.push(ValidationError::new(
            "min_online",
            "greater than max_online",
        ));
    }
    errors
}
