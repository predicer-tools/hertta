use crate::input_data_base::BaseInputDataSetup;
use juniper::GraphQLInputObject;

use super::ValidationErrors;

#[derive(GraphQLInputObject)]
pub struct InputDataSetupInput {
    
    #[graphql(description = "Flag indicating whether market bids are used in the model.")]
    use_market_bids: Option<bool>,
    #[graphql(description = "Flag indicating whether reserves are used in the model. If set to false, no reserve functionalities are used.")]
    use_reserves: Option<bool>,
    #[graphql(description = "Indicates whether reserves can be realised. If set to false, no realisation occurs.")]
    use_reserve_realisation: Option<bool>,
    #[graphql(description = "Indicates if dummy variables should be used in the node balance equations.")]
    use_node_dummy_variables: Option<bool>,
    #[graphql(description = "Indicates if dummy variables should be used in the ramp balance equations.")]
    use_ramp_dummy_variables: Option<bool>,
    #[graphql(description = "Indicates the length of a common start, where the parameters and variable values are equal across all scenarios. Default is 0.")]
    common_timesteps: Option<i32>,
    #[graphql(description = "Name of the common start scenario, if it is used.")]
    common_scenario_name: Option<String>,
    node_dummy_variable_cost: Option<f64>,
    ramp_dummy_variable_cost: Option<f64>,
}

impl InputDataSetupInput {
    fn update_input_data_setup(self, setup: &mut BaseInputDataSetup) {
        if let Some(flag) = self.use_reserves {
            setup.use_reserves = flag;
        }
        if let Some(flag) = self.use_reserve_realisation {
            setup.use_reserve_realisation = flag;
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
