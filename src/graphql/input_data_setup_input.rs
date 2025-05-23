use crate::input_data_base::BaseInputDataSetup;
use juniper::GraphQLInputObject;

use super::ValidationErrors;

#[derive(GraphQLInputObject)]
pub struct InputDataSetupInput {
    use_reserves: Option<bool>,
    contains_online: Option<bool>, //will be removed
    contains_states: Option<bool>, //will be removed
    contains_piecewise_eff: Option<bool>, //will be removed
    contains_risk: Option<bool>, //will be removed
    contains_diffusion: Option<bool>, //will be removed
    contains_delay: Option<bool>, //will be removed
    contains_markets: Option<bool>, //will be removed
    use_reserve_realisation: Option<bool>,
    use_market_bids: Option<bool>,
    common_timesteps: Option<i32>,
    common_scenario_name: Option<String>,
    use_node_dummy_variables: Option<bool>,
    use_ramp_dummy_variables: Option<bool>,
    node_dummy_variable_cost: Option<f64>, //check what to do with these
    ramp_dummy_variable_cost: Option<f64>, //ceheck what to do with these
}

impl InputDataSetupInput {
    fn update_input_data_setup(self, setup: &mut BaseInputDataSetup) {
        if let Some(flag) = self.use_reserves {
            setup.contains_reserves = flag;
        }
        if let Some(flag) = self.contains_online {
            setup.contains_online = flag;
        }
        if let Some(flag) = self.contains_states {
            setup.contains_states = flag;
        }
        if let Some(flag) = self.contains_piecewise_eff {
            setup.contains_piecewise_eff = flag;
        }
        if let Some(flag) = self.contains_risk {
            setup.contains_risk = flag;
        }
        if let Some(flag) = self.contains_diffusion {
            setup.contains_diffusion = flag;
        }
        if let Some(flag) = self.contains_delay {
            setup.contains_delay = flag;
        }
        if let Some(flag) = self.contains_markets {
            setup.contains_markets = flag;
        }
        if let Some(flag) = self.use_reserve_realisation {
            setup.reserve_realisation = flag;
        }
        if let Some(flag) = self.use_market_bids {
            setup.use_market_bids = flag;
        }
        if let Some(steps) = self.common_timesteps {
            setup.common_timesteps = steps.into();
        }
        if let Some(name) = self.common_scenario_name {
            setup.common_scenario_name = name;
        }
        if let Some(flag) = self.use_node_dummy_variables {
            setup.use_node_dummy_variables = flag;
        }
        if let Some(flag) = self.use_ramp_dummy_variables {
            setup.use_ramp_dummy_variables = flag;
        }
        if let Some(cost) = self.node_dummy_variable_cost {
            setup.node_dummy_variable_cost = cost;
        }
        if let Some(cost) = self.ramp_dummy_variable_cost {
            setup.ramp_dummy_variable_cost = cost;
        }
    }
}

pub fn update_input_data_setup(
    setup_update: InputDataSetupInput,
    setup: &mut BaseInputDataSetup,
) -> ValidationErrors {
    setup_update.update_input_data_setup(setup);
    ValidationErrors::from(Vec::new())
}
