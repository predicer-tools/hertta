use crate::input_data::Name;
use crate::input_data_base::TypeName;
use hertta_derive::Name;
use juniper::GraphQLObject;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize, GraphQLObject, Name, Serialize)]
#[graphql(description = "Scenario for stochastics.")]
pub struct Scenario {
    #[graphql(description = "Scenario name.")]
    name: String,
    #[graphql(description = "Scenario weight.")]
    weight: f64,
}

impl Default for Scenario {
    fn default() -> Self {
        Scenario {
            name: "S1".to_string(),
            weight: 1.0,
        }
    }
}

impl TypeName for Scenario {
    fn type_name() -> &'static str {
        "scenario"
    }
}

impl Scenario {
    fn check_name(name: &str) -> Result<(), String> {
        if name.is_empty() {
            return Err("name is empty".to_string());
        }
        Ok(())
    }
    fn check_weight(weight: f64) -> Result<(), String> {
        if weight <= 0.0 {
            return Err("non-positive weight".to_string());
        }
        Ok(())
    }
    pub fn new(name: &str, weight: f64) -> Result<Self, String> {
        Self::check_name(name)?;
        Self::check_weight(weight)?;
        Ok(Scenario {
            name: String::from(name),
            weight,
        })
    }
    pub fn name(&self) -> &String {
        &self.name
    }
    pub fn set_name(&mut self, name: &str) -> Result<(), String> {
        Self::check_name(name)?;
        self.name = String::from(name);
        Ok(())
    }
    pub fn weight(&self) -> f64 {
        self.weight
    }
    pub fn set_weight(&mut self, weight: f64) -> Result<(), String> {
        Self::check_weight(weight)?;
        self.weight = weight;
        Ok(())
    }
    pub fn to_map(scenarios: &Vec<Self>) -> BTreeMap<String, f64> {
        let weight_sum: f64 = scenarios.iter().map(|s| s.weight).sum();
        scenarios
            .iter()
            .map(|s| (s.name.clone(), s.weight / weight_sum))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn new_scenario() {
        if let Ok(scenario) = Scenario::new("my scenario", 2.3) {
            assert_eq!(scenario.name, "my scenario");
            assert_eq!(scenario.weight, 2.3);
        } else {
            panic!("scenario with valid initial values should not fail");
        }
        if let Err(error) = Scenario::new("", 2.3) {
            assert_eq!(error, "name is empty");
        } else {
            panic!("scenario with empty name should fail");
        }
        if let Err(error) = Scenario::new("my scenario", -2.3) {
            assert_eq!(error, "non-positive weight");
        } else {
            panic!("scenario with negative weight sould fail");
        }
    }
    #[test]
    fn set_scenario_name() {
        let mut scenario = Scenario::default();
        scenario
            .set_name("acceptable name")
            .expect("non-empty name should not cause error");
        if let Err(error) = scenario.set_name("") {
            assert_eq!(error, "name is empty");
        } else {
            panic!("setting empty name should fail");
        }
    }
    #[test]
    fn set_scenario_weight() {
        let mut scenario = Scenario::default();
        scenario
            .set_weight(2.3)
            .expect("positive weight should be ok");
        if let Err(error) = scenario.set_weight(0.0) {
            assert_eq!(error, "non-positive weight");
        } else {
            panic!("zero weight should fail");
        }
        if let Err(error) = scenario.set_weight(-2.3) {
            assert_eq!(error, "non-positive weight");
        } else {
            panic!("negative weight should fail");
        }
    }
    #[test]
    fn test_to_map() {
        let mut scenarios = Vec::new();
        let mut expected_map = BTreeMap::new();
        assert_eq!(Scenario::to_map(&scenarios), expected_map);
        scenarios.push(Scenario::new("S1", 2.3).unwrap());
        expected_map.insert("S1".to_string(), 1.0);
        assert_eq!(Scenario::to_map(&scenarios), expected_map);
        scenarios.push(Scenario::new("S2", 3.3).unwrap());
        expected_map.insert("S1".to_string(), 2.3 / 5.6);
        expected_map.insert("S2".to_string(), 3.3 / 5.6);
        assert_eq!(Scenario::to_map(&scenarios), expected_map);
        assert_eq!(Scenario::to_map(&scenarios).values().sum::<f64>(), 1.0);
    }
}
