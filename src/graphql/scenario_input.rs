use super::delete;
use super::MaybeError;
use crate::input_data_base::BaseNodeHistory;
use crate::scenarios::Scenario;

pub fn create_scenario(name: String, weight: f64, scenarios: &mut Vec<Scenario>) -> MaybeError {
    if name.is_empty() {
        return "name is empty".into();
    }
    if scenarios.iter().any(|s| *s.name() == name) {
        return "a scenario with the same name exists".into();
    }
    let scenario = match Scenario::new(&name, weight) {
        Ok(scenario) => scenario,
        Err(error) => return error.into(),
    };
    scenarios.push(scenario);
    MaybeError::new_ok()
}

pub fn delete_scenario(
    name: &str,
    scenarios: &mut Vec<Scenario>,
    node_histories: &mut Vec<BaseNodeHistory>,
) -> MaybeError {
    let maybe_error = delete::delete_named(name, scenarios);
    if maybe_error.is_error() {
        return maybe_error;
    }
    for history in node_histories {
        history.steps.retain(|s| s.scenario != name);
    }
    MaybeError::new_ok()
}
