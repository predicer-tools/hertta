use std::borrow::BorrowMut;

use super::{ValidationError, ValidationErrors};
use crate::input_data::State;
use crate::input_data_base::BaseNode;
use juniper::GraphQLInputObject;

#[derive(GraphQLInputObject)]
pub struct StateInput {
    in_max: f64,
    out_max: f64,
    state_loss_proportional: f64,
    state_min: f64,
    state_max: f64,
    initial_state: f64,
    is_scenario_independent: bool,
    is_temp: bool,
    t_e_conversion: f64,
    residual_value: f64,
}

impl StateInput {
    fn to_state(self) -> State {
        State {
            in_max: self.in_max,
            out_max: self.out_max,
            state_loss_proportional: self.state_loss_proportional,
            state_max: self.state_max,
            state_min: self.state_min,
            initial_state: self.initial_state,
            is_scenario_independent: self.is_scenario_independent,
            is_temp: self.is_temp,
            t_e_conversion: self.t_e_conversion,
            residual_value: self.residual_value,
        }
    }
}

pub fn set_state_for_node(
    node_name: &str,
    state: Option<StateInput>,
    nodes: &mut Vec<BaseNode>,
) -> ValidationErrors {
    if let Some(ref real_state) = state {
        let errors = validate_state_to_set(real_state);
        if !errors.is_empty() {
            return ValidationErrors::from(errors);
        }
    }
    let node = match nodes.iter_mut().find(|p| p.name == node_name) {
        Some(p) => p,
        None => {
            return ValidationErrors::from(ValidationError::new(node_name, "no such node"));
        }
    };
    if node.state.is_some() {
        return ValidationErrors::from(ValidationError::new(node_name, "node already has a state"));
    }
    node.state = state.and_then(|state| Some(state.to_state()));
    ValidationErrors::from(Vec::new())
}

fn validate_state_to_set(state: &StateInput) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    validate_state_min(state.state_min, state.state_max, &mut errors);
    validate_state_loss_proportional(state.state_loss_proportional, &mut errors);
    errors
}

fn validate_state_min(state_min: f64, state_max: f64, errors: &mut Vec<ValidationError>) {
    if state_min > state_max {
        errors.push(ValidationError::new("state_min", "greater than state_max"));
    }
}

fn validate_state_loss_proportional(state_loss: f64, errors: &mut Vec<ValidationError>) {
    if state_loss < 0.0 || state_loss > 1.0 {
        errors.push(ValidationError::new(
            "state_loss_proportional",
            "should be in [0, 1]",
        ));
    }
}

#[derive(GraphQLInputObject)]
pub struct StateUpdate {
    in_max: Option<f64>,
    out_max: Option<f64>,
    state_loss_proportional: Option<f64>,
    state_max: Option<f64>,
    state_min: Option<f64>,
    initial_state: Option<f64>,
    is_scenario_independent: Option<bool>,
    is_temp: Option<bool>,
    t_e_conversion: Option<f64>,
    residual_value: Option<f64>,
}

impl StateUpdate {
    fn update_state(self, state: &mut State) {
        optional_update(self.in_max, &mut state.in_max);
        optional_update(self.out_max, &mut state.out_max);
        optional_update(
            self.state_loss_proportional,
            &mut state.state_loss_proportional,
        );
        optional_update(self.state_max, &mut state.state_max);
        optional_update(self.state_min, &mut state.state_min);
        optional_update(self.initial_state, &mut state.initial_state);
        optional_update(
            self.is_scenario_independent,
            &mut state.is_scenario_independent,
        );
        optional_update(self.is_temp, &mut state.is_temp);
        optional_update(self.t_e_conversion, &mut state.t_e_conversion);
        optional_update(self.residual_value, &mut state.residual_value);
    }
}

fn optional_update<T>(source: Option<T>, target: &mut T) {
    if let Some(x) = source {
        *target = x;
    }
}

pub fn update_state_in_node(
    state: StateUpdate,
    node_name: String,
    nodes: &mut Vec<BaseNode>,
) -> ValidationErrors {
    let node = match nodes.iter_mut().find(|n| n.name == node_name) {
        Some(node) => node,
        None => {
            return ValidationErrors::from(vec![ValidationError::new("node_name", "no such node")])
        }
    };
    if let Some(node_state) = node.state.borrow_mut() {
        let errors = validate_state_to_update(&state, node_state);
        if !errors.is_empty() {
            return ValidationErrors::from(errors);
        }
        state.update_state(node_state);
    } else {
        return ValidationErrors::from(vec![ValidationError::new(
            "node_name",
            "node has no state",
        )]);
    }
    ValidationErrors::default()
}

fn validate_state_to_update(state_update: &StateUpdate, state: &State) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if state_update.state_min.is_some() || state_update.state_max.is_some() {
        let min = state_update.state_min.unwrap_or(state.state_min);
        let max = state_update.state_max.unwrap_or(state.state_max);
        validate_state_min(min, max, &mut errors);
    }
    if let Some(state_loss) = state_update.state_loss_proportional {
        validate_state_loss_proportional(state_loss, &mut errors);
    }
    errors
}
