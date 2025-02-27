use crate::input_data_base::{ForecastValueInput, ForecastValue};
use crate::scenarios::Scenario;

pub fn convert_forecast_value_inputs(
    inputs: Vec<ForecastValueInput>,
    scenarios: &Vec<Scenario>,
) -> Vec<ForecastValue> {
    let mut converted: Vec<ForecastValue> = inputs
        .into_iter()
        .map(|inp| {
            ForecastValue::try_from(inp)
                .expect("Error converting ForecastValueInput")
        })
        .collect();

    let default_value: Option<ForecastValue> = {
        let mut iter = converted.iter().filter(|fv| fv.scenario.is_none());
        let first = iter.next().cloned();

        if iter.next().is_some() {
            panic!("Multiple default forecast values provided (scenario == None)");
        }
        first
    };

    if let Some(default_value) = default_value {
        let missing: Vec<String> = scenarios
            .iter()
            .filter(|scenario| {
                !converted.iter().any(|fv| {
                    fv.scenario
                        .as_ref()
                        .map_or(false, |s| s == scenario.name())
                })
            })
            .map(|scenario| scenario.name().clone())
            .collect();

        for scenario_name in missing {
            let mut new_fv = default_value.clone();
            new_fv.scenario = Some(scenario_name);
            converted.push(new_fv);
        }
    } else {
        for scenario in scenarios {
            if !converted.iter().any(|fv| {
                fv.scenario
                    .as_ref()
                    .map_or(false, |s| s == scenario.name())
            }) {
                panic!("Missing forecast value for scenario {}", scenario.name());
            }
        }
    }

    converted
}

