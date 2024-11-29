use super::{ValidationError, ValidationErrors};
use crate::input_data::State;
use crate::input_data_base::BaseNode;
use juniper::GraphQLInputObject;

#[derive(GraphQLInputObject)]
pub struct SetStateInput {
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

impl SetStateInput {
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
    state: Option<SetStateInput>,
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
            return ValidationErrors::from(vec![ValidationError::new(node_name, "no such node")])
        }
    };
    node.state = state.and_then(|state| Some(state.to_state()));
    ValidationErrors::from(Vec::new())
}

fn validate_state_to_set(state: &SetStateInput) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if state.state_min > state.state_max {
        errors.push(ValidationError::new("state_min", "greater than state_max"));
    }
    if state.state_loss_proportional < 0.0 || state.state_loss_proportional > 1.0 {
        errors.push(ValidationError::new(
            "state_loss_proportional",
            "should be in [0, 1]",
        ));
    }
    errors
}
