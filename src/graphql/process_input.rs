use super::delete;
use super::{MaybeError, ValidationError, ValidationErrors};
use crate::input_data_base::{
    BaseConFactor, BaseGenConstraint, BaseNode, BaseProcess, ConstraintFactorType, Conversion,
    ProcessGroup, ValueInput, Value, PointInput,
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
    #[graphql(description = "Value time series of the efficiency of processes")]
    eff_ops_fun: Vec<PointInput>,
}

#[derive(GraphQLInputObject)]
pub struct ProcessUpdate {
    conversion: Option<Conversion>,
    is_cf_fix: Option<bool>,
    is_online: Option<bool>,
    is_res: Option<bool>,
    eff: Option<f64>,
    load_min: Option<f64>,
    load_max: Option<f64>,
    start_cost: Option<f64>,
    min_online: Option<f64>,
    max_online: Option<f64>,
    min_offline: Option<f64>,
    max_offline: Option<f64>,
    initial_state: Option<bool>,
    is_scenario_independent: Option<bool>,
    cf: Option<Vec<ValueInput>>,
    eff_ts: Option<Vec<ValueInput>>,
    eff_ops_fun: Option<Vec<PointInput>>,
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
            eff_ops_fun: self.eff_ops_fun.into_iter().map(Into::into).collect(),
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

pub fn update_process(name: &str, update: ProcessUpdate, processes: &mut Vec<BaseProcess>) -> ValidationErrors {
    let process = match processes.iter_mut().find(|process| process.name == name) {
        Some(process) => process,
        None => return ValidationErrors::from(ValidationError::new("name", "no such process")),
    };
    let load_min = update.load_min.unwrap_or(process.load_min);
    let load_max = update.load_max.unwrap_or(process.load_max);
    let min_online = update.min_online.unwrap_or(process.min_online);
    let max_online = update.max_online.unwrap_or(process.max_online);
    let min_offline = update.min_offline.unwrap_or(process.min_offline);
    let max_offline = update.max_offline.unwrap_or(process.max_offline);
    let mut errors = Vec::new();
    if !(0.0..=1.0).contains(&load_min) { errors.push(ValidationError::new("load_min", "should be in [0, 1]")); }
    if !(0.0..=1.0).contains(&load_max) { errors.push(ValidationError::new("load_max", "should be in [0, 1]")); }
    if load_min > load_max { errors.push(ValidationError::new("load_min", "greater than load_max")); }
    if min_online > max_online && min_online > 0.0 && max_online > 0.0 {
        errors.push(ValidationError::new("min_online", "greater than max_online"));
    }
    if min_offline > max_offline && min_offline > 0.0 && max_offline > 0.0 {
        errors.push(ValidationError::new("min_offline", "greater than max_offline"));
    }
    if !errors.is_empty() { return ValidationErrors::from(errors); }

    let cf = match convert_values(update.cf, "cf") { Ok(value) => value, Err(errors) => return errors };
    let eff_ts = match convert_values(update.eff_ts, "eff_ts") { Ok(value) => value, Err(errors) => return errors };
    if let Some(value) = update.conversion { process.conversion = value; }
    if let Some(value) = update.is_cf_fix { process.is_cf_fix = value; }
    if let Some(value) = update.is_online { process.is_online = value; }
    if let Some(value) = update.is_res { process.is_res = value; }
    if let Some(value) = update.eff { process.eff = value; }
    process.load_min = load_min;
    process.load_max = load_max;
    if let Some(value) = update.start_cost { process.start_cost = value; }
    process.min_online = min_online;
    process.max_online = max_online;
    process.min_offline = min_offline;
    process.max_offline = max_offline;
    if let Some(value) = update.initial_state { process.initial_state = value; }
    if let Some(value) = update.is_scenario_independent { process.is_scenario_independent = value; }
    if let Some(values) = cf { process.is_cf = !values.is_empty(); process.cf = values; }
    if let Some(values) = eff_ts { process.eff_ts = values; }
    if let Some(points) = update.eff_ops_fun { process.eff_ops_fun = points.into_iter().map(Into::into).collect(); }
    ValidationErrors::default()
}

fn convert_values(inputs: Option<Vec<ValueInput>>, field: &str) -> Result<Option<Vec<Value>>, ValidationErrors> {
    match inputs {
        Some(inputs) => inputs.into_iter().map(Value::try_from).collect::<Result<Vec<_>, _>>()
            .map(Some).map_err(|error| ValidationErrors::from(ValidationError::new(field, &error))),
        None => Ok(None),
    }
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
    if process.min_online > process.max_online
    && process.min_online > 0.0
    && process.max_online > 0.0
    {
        errors.push(ValidationError::new(
            "min_online",
            "greater than max_online",
        ));
    }
    if process.min_offline > process.max_offline
    && process.min_offline > 0.0
    && process.max_offline > 0.0
    {
        errors.push(ValidationError::new(
            "min_offline",
            "greater than max_offline",
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
