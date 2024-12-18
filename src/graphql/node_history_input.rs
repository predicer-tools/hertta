use super::time_line_input::DurationInput;
use super::{MaybeError, ValidationError, ValidationErrors};
use crate::input_data_base::{BaseNode, BaseNodeHistory, Series};
use crate::scenarios::Scenario;
use juniper::GraphQLInputObject;

pub fn create_node_history(
    node_name: String,
    histories: &mut Vec<BaseNodeHistory>,
    nodes: &Vec<BaseNode>,
) -> MaybeError {
    if !nodes.iter().any(|n| n.name == node_name) {
        return "no such node".into();
    }
    histories.push(BaseNodeHistory::new(node_name));
    MaybeError::new_ok()
}

#[derive(GraphQLInputObject)]
pub struct NewSeries {
    scenario: String,
    durations: Vec<DurationInput>,
    values: Vec<f64>,
}

impl NewSeries {
    fn to_series(self) -> Result<Series, String> {
        let mut durations = Vec::with_capacity(self.durations.len());
        for duration in self.durations {
            durations.push(duration.to_duration()?);
        }
        Ok(Series {
            scenario: self.scenario,
            durations,
            values: self.values,
        })
    }
}

pub fn add_step_to_node_history(
    node_name: String,
    step: NewSeries,
    histories: &mut Vec<BaseNodeHistory>,
    scenarios: &Vec<Scenario>,
) -> ValidationErrors {
    let history = match histories.iter_mut().find(|h| h.node == node_name) {
        Some(history) => history,
        None => return ValidationError::new("node_name", "no history for node").into(),
    };
    let errors = validate_step_addition(&step, history, scenarios);
    if !errors.is_empty() {
        return errors.into();
    }
    let history_step = match step.to_series() {
        Ok(series) => series,
        Err(error) => return ValidationError::new("durations", &error).into(),
    };
    history.steps.push(history_step);
    ValidationErrors::default()
}

fn validate_step_addition(
    step: &NewSeries,
    history: &BaseNodeHistory,
    scenarios: &Vec<Scenario>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if !scenarios.iter().any(|s| *s.name() == step.scenario) {
        errors.push(ValidationError::new("scenario", "no such scenario"));
    }
    if history.steps.iter().any(|s| s.scenario == step.scenario) {
        errors.push(ValidationError::new(
            "scenario",
            "a step with same scenario exists",
        ));
    }
    if step.durations.len() != step.values.len() {
        errors.push(ValidationError::new(
            "durations",
            "length does not match values",
        ));
    }
    errors
}
