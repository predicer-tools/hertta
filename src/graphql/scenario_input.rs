use super::MaybeError;
use crate::scenarios::Scenario;

pub fn create_scenario(name: String, weight: f64, scenarios: &mut Vec<Scenario>) -> MaybeError {
    if scenarios.iter().find(|s| *s.name() == name).is_some() {
        return "a scenario with the same name exists".into();
    }
    let scenario = match Scenario::new(&name, weight) {
        Ok(scenario) => scenario,
        Err(error) => return error.into(),
    };
    scenarios.push(scenario);
    MaybeError::new_ok()
}
