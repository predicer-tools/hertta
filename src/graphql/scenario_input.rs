use super::delete;
use super::MaybeError;
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

pub fn delete_scenario(name: &str, scenarios: &mut Vec<Scenario>) -> MaybeError {
    delete::delete_named(name, scenarios)
}
