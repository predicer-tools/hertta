use super::{MaybeError, ValidationError, ValidationErrors};
use crate::input_data_base::{
    BaseConFactor, BaseGenConstraint, BaseNode, BaseProcess, ConstraintFactorType, VariableId,
};

pub fn create_flow_con_factor(
    factor: f64,
    constraint_name: String,
    process_name: String,
    source_or_sink_node_name: String,
    constraints: &mut Vec<BaseGenConstraint>,
    processes: &Vec<BaseProcess>,
) -> ValidationErrors {
    let constraint = match find_constraint(&constraint_name, constraints) {
        Ok(constraint) => constraint,
        Err(errors) => return errors,
    };
    let errors = validate_flow_con_factor_creation(
        &process_name,
        &source_or_sink_node_name,
        constraint,
        processes,
    );
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    let con_factor = BaseConFactor {
        var_type: ConstraintFactorType::Flow,
        var_tuple: VariableId {
            entity: process_name,
            identifier: Some(source_or_sink_node_name),
        },
        data: factor,
    };
    constraint.factors.push(con_factor);
    ValidationErrors::default()
}

fn find_constraint<'a>(
    constraint_name: &str,
    constraints: &'a mut Vec<BaseGenConstraint>,
) -> Result<&'a mut BaseGenConstraint, ValidationErrors> {
    let constraint = match constraints.iter_mut().find(|c| c.name == constraint_name) {
        Some(constraint) => constraint,
        None => {
            return Err(ValidationErrors::from(vec![ValidationError::new(
                "constraint_name",
                "no such constraint",
            )]))
        }
    };
    Ok(constraint)
}

fn validate_flow_con_factor_creation(
    process: &String,
    source_or_sink_node: &String,
    constraint: &BaseGenConstraint,
    processes: &Vec<BaseProcess>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if source_or_sink_node.is_empty() {
        errors.push(ValidationError::new(
            "source_or_sink_node_name",
            "name is empty",
        ));
    }
    if let Some(process) = processes.iter().find(|p| p.name == *process) {
        if process
            .topos
            .iter()
            .find(|t| t.source == *source_or_sink_node || t.sink == *source_or_sink_node)
            .is_none()
        {
            errors.push(ValidationError::new(
                "source_or_sink_node_name",
                "no such source or sink in process",
            ));
        }
    } else {
        errors.push(ValidationError::new("process_name", "no such process"));
    }
    if constraint
        .factors
        .iter()
        .find(|f| {
            f.var_type == ConstraintFactorType::Flow
                && f.var_tuple.entity == *process
                && f.var_tuple
                    .identifier
                    .as_ref()
                    .is_some_and(|i| i == source_or_sink_node)
        })
        .is_some()
    {
        errors.push(ValidationError::new(
            "constraint_name",
            "a flow constraint factor with same process and node exists",
        ));
    }
    errors
}

pub fn create_state_con_factor(
    factor: f64,
    constraint_name: String,
    node_name: String,
    constraints: &mut Vec<BaseGenConstraint>,
    nodes: &Vec<BaseNode>,
) -> ValidationErrors {
    let constraint = match find_constraint(&constraint_name, constraints) {
        Ok(constraint) => constraint,
        Err(errors) => return errors,
    };
    let errors = validate_state_con_factor_creation(&node_name, constraint, nodes);
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    let con_factor = BaseConFactor {
        var_type: ConstraintFactorType::State,
        var_tuple: VariableId {
            entity: node_name,
            identifier: None,
        },
        data: factor,
    };
    constraint.factors.push(con_factor);
    ValidationErrors::default()
}

fn validate_state_con_factor_creation(
    node: &String,
    constraint: &BaseGenConstraint,
    nodes: &Vec<BaseNode>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if node.is_empty() {
        errors.push(ValidationError::new("node_name", "name is empty"));
    }
    if nodes.iter().find(|n| n.name == *node).is_none() {
        errors.push(ValidationError::new("node_name", "no such node"));
    }
    if constraint
        .factors
        .iter()
        .find(|f| f.var_type == ConstraintFactorType::State && f.var_tuple.entity == *node)
        .is_some()
    {
        errors.push(ValidationError::new(
            "constraint_name",
            "a state constraint factor with same node exists",
        ));
    }
    errors
}

pub fn create_online_con_factor(
    factor: f64,
    constraint_name: String,
    process_name: String,
    constraints: &mut Vec<BaseGenConstraint>,
    processes: &Vec<BaseProcess>,
) -> ValidationErrors {
    let constraint = match find_constraint(&constraint_name, constraints) {
        Ok(constraint) => constraint,
        Err(errors) => return errors,
    };
    let errors = validate_online_con_factor_creation(&process_name, constraint, processes);
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    let con_factor = BaseConFactor {
        var_type: ConstraintFactorType::Online,
        var_tuple: VariableId {
            entity: process_name,
            identifier: None,
        },
        data: factor,
    };
    constraint.factors.push(con_factor);
    ValidationErrors::default()
}

fn validate_online_con_factor_creation(
    process: &String,
    constraint: &BaseGenConstraint,
    processes: &Vec<BaseProcess>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if process.is_empty() {
        errors.push(ValidationError::new("process_name", "name is empty"));
    }
    if processes.iter().find(|p| p.name == *process).is_none() {
        errors.push(ValidationError::new("process_name", "no such process"));
    }
    if constraint
        .factors
        .iter()
        .find(|f| f.var_type == ConstraintFactorType::Online && f.var_tuple.entity == *process)
        .is_some()
    {
        errors.push(ValidationError::new(
            "constraint_name",
            "an online constraint factor with same process exists",
        ));
    }
    errors
}

pub fn delete_flow_con_factor(
    constraint_name: &str,
    process_name: &str,
    source_or_sink_node_name: &str,
    constraints: &mut Vec<BaseGenConstraint>,
) -> MaybeError {
    delete_con_factor(
        constraint_name,
        |f| {
            f.is_flow()
                && f.var_tuple.entity == process_name
                && f.var_tuple
                    .identifier
                    .as_ref()
                    .is_some_and(|i| i == source_or_sink_node_name)
        },
        constraints,
    )
}

pub fn delete_state_con_factor(
    constraint_name: &str,
    node_name: &str,
    constraints: &mut Vec<BaseGenConstraint>,
) -> MaybeError {
    delete_con_factor(
        constraint_name,
        |f| f.is_state() && f.var_tuple.entity == node_name,
        constraints,
    )
}

pub fn delete_online_con_factor(
    constraint_name: &str,
    process_name: &str,
    constraints: &mut Vec<BaseGenConstraint>,
) -> MaybeError {
    delete_con_factor(
        constraint_name,
        |f| f.is_online() && f.var_tuple.entity == process_name,
        constraints,
    )
}

fn delete_con_factor<P: FnMut(&BaseConFactor) -> bool>(
    constraint_name: &str,
    mut predicate: P,
    constraints: &mut Vec<BaseGenConstraint>,
) -> MaybeError {
    let constraint = match constraints.iter_mut().find(|c| c.name == constraint_name) {
        Some(constraint) => constraint,
        None => return "no such constraint".into(),
    };
    if let Some(position) = constraint.factors.iter().position(|f| predicate(f)) {
        constraint.factors.swap_remove(position);
        return MaybeError::new_ok();
    } else {
        return "no such constraint factor".into();
    }
}
