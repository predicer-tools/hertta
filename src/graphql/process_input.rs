use super::delete;
use super::{MaybeError, ValidationError, ValidationErrors};
use crate::input_data_base::{
    BaseConFactor, BaseGenConstraint, BaseNode, BaseProcess, ConstraintFactorType, Conversion,
    ProcessGroup, ValueInput, Value,
};
use juniper::GraphQLInputObject;

#[derive(GraphQLInputObject)]
pub struct NewProcess {
    #[graphql(description = "Name of the process.")]
    name: String,
   #[graphql(description = "Indicates the type of the process. Options: Unit, Transfer, Market")]
    conversion: Conversion,
    #[graphql(description = "Indicates if the process has to match the capacity factor time series.")]
    is_cf_fix: bool,
    #[graphql(description = "Indicates if the process is an online/offline unit.")]
    is_online: bool,
    #[graphql(description = "Indicates if the process participates in reserve markets")]
    is_res: bool,
    #[graphql(description = "Process efficiency (total output / total input)")]
    eff: f64,
    #[graphql(description = "Minimum load of the process as a fraction of total capacity. Only for online processes")]
    load_min: f64,
    #[graphql(description = "Maximum load of the process as a fraction of total capacity. Only for online processes")]
    load_max: f64,
    #[graphql(description = "Cost of starting the unit, only for online processes.")]
    start_cost: f64,
    #[graphql(description = "Minimum time the process has to be online after start up.")]
    min_online: f64,
    #[graphql(description = "Maximum time the process can be online.")]
    max_online: f64,
    #[graphql(description = "Minimum time the process has to be offline during shut down.")]
    min_offline: f64,
    #[graphql(description = "Maximum time the process can be offline.")]
    max_offline: f64,
    #[graphql(description = "Initial state of the online unit (0 = offline, 1 = online).")]
    initial_state: bool,
    #[graphql(description = "If true, forces the online variable of the process to be equal in all scenarios.")]
    is_scenario_independent: bool,
    #[graphql(description = "Capacity factor time series for processes with cf functionality.")]
    cf: Vec<ValueInput>,
    #[graphql(description = "Value time series of the efficiency of processes")]
    eff_ts: Vec<ValueInput>,
}

impl NewProcess {
    fn to_process(self) -> BaseProcess {
        BaseProcess {
            name: self.name,
            groups: Vec::new(),
            conversion: self.conversion,
            is_cf: !self.cf.is_empty(),
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
            cf: self
            .cf
            .into_iter()
            .map(Value::try_from)
            .collect::<Result<Vec<Value>, _>>()
            .expect("Could not parse cost values"),
            eff_ts: self
            .eff_ts
            .into_iter()
            .map(Value::try_from)
            .collect::<Result<Vec<Value>, _>>()
            .expect("Could not parse cost values"),
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