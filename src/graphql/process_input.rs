use super::delete;
use super::{MaybeError, ValidationError, ValidationErrors};
use crate::input_data_base::{
    BaseConFactor, BaseGenConstraint, BaseNode, BaseProcess, ConstraintFactorType, Conversion,
    ProcessGroup,
};
use juniper::GraphQLInputObject;

#[derive(GraphQLInputObject)]
pub struct NewProcess {
    name: String,
    conversion: Conversion,
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

impl NewProcess {
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

pub fn create_process(
    process: NewProcess,
    processes: &mut Vec<BaseProcess>,
    nodes: &mut Vec<BaseNode>,
) -> ValidationErrors {
    let errors = validate_process_creation(&process, processes, nodes);
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    processes.push(process.to_process());
    ValidationErrors::default()
}

fn validate_process_creation(
    process: &NewProcess,
    processes: &Vec<BaseProcess>,
    nodes: &Vec<BaseNode>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if process.name.is_empty() {
        errors.push(ValidationError::new("name", "name is empty"));
    }
    if processes.iter().any(|p| p.name == process.name) {
        errors.push(ValidationError::new(
            "name",
            "a process with the same name exists",
        ));
    }
    if nodes.iter().any(|n| n.name == process.name) {
        errors.push(ValidationError::new(
            "name",
            "a node with the same name exists",
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

pub fn delete_process(
    name: &str,
    processes: &mut Vec<BaseProcess>,
    groups: &mut Vec<ProcessGroup>,
    constraints: &mut Vec<BaseGenConstraint>,
) -> MaybeError {
    let maybe_error = delete::delete_named(name, processes);
    if maybe_error.is_error() {
        return maybe_error;
    }
    for group in groups {
        if let Some(process_position) = group.members.iter().position(|m| m == name) {
            group.members.swap_remove(process_position);
        }
    }
    for constraint in constraints {
        constraint.factors.retain(|c| !process_con_factor(c, name));
    }
    MaybeError::new_ok()
}

pub fn process_con_factor(con_factor: &BaseConFactor, process_name: &str) -> bool {
    match con_factor.var_type {
        ConstraintFactorType::Flow | ConstraintFactorType::Online => {
            con_factor.var_tuple.entity == process_name
        }
        ConstraintFactorType::State => false,
    }
}