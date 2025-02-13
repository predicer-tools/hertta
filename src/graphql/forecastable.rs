use crate::input_data_base::{BaseForecastable, ForecastValueInput, ForecastValue};
use crate::scenarios::Scenario;

pub fn to_forecastable(constant: Option<f64>) -> Option<BaseForecastable> {
    constant.map(|value| BaseForecastable::Constant(value.into()))
}

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

    // Instead of collecting a Vec<&ForecastValue>, extract an owned default value.
    let default_value: Option<ForecastValue> = {
        let mut iter = converted.iter().filter(|fv| fv.scenario.is_none());
        let first = iter.next().cloned();
        // If there is another default, then panic.
        if iter.next().is_some() {
            panic!("Multiple default forecast values provided (scenario == None)");
        }
        first
    };

    if let Some(default_value) = default_value {
        // Collect missing scenario names from the provided scenarios.
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

        // For each missing scenario, clone the owned default and set its scenario.
        for scenario_name in missing {
            let mut new_fv = default_value.clone();
            new_fv.scenario = Some(scenario_name);
            converted.push(new_fv);
        }
    } else {
        // If no default is provided, every scenario must have an explicit forecast.
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

