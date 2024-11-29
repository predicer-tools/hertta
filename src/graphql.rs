mod con_factor_input;
mod gen_constraint_input;
mod group_input;
mod input_data_setup_input;
mod market_input;
mod node_diffusion_input;
mod node_input;
mod process_input;
mod risk_input;
mod scenario_input;
mod state_input;
mod time_line_input;
mod topology_input;
pub mod types;

use crate::event_loop::{OptimizationState, OptimizationTask};
use crate::input_data_base::BaseInputData;
use crate::model::{self, Model};
use crate::settings::{LocationSettings, Settings};
use crate::status::Status;
use con_factor_input::{AddConFactorInput, AddConFactorResult};
use gen_constraint_input::AddGenConstraintInput;
use input_data_setup_input::InputDataSetupInput;
use juniper::{
    graphql_object, Context, EmptySubscription, FieldResult, GraphQLInputObject, GraphQLObject,
    GraphQLUnion, Nullable, RootNode,
};
use market_input::AddMarketInput;
use node_diffusion_input::AddNodeDiffusionInput;
use node_input::AddNodeInput;
use process_input::AddProcessInput;
use risk_input::AddRiskInput;
use state_input::SetStateInput;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use time_line_input::TimeLineInput;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::sync::Semaphore;
use topology_input::{AddTopologyInput, AddTopologyResult};

#[derive(Debug, GraphQLObject)]
struct ValidationError {
    field: String,
    message: String,
}

impl ValidationError {
    fn new(field: &str, message: &str) -> Self {
        ValidationError {
            field: String::from(field),
            message: String::from(message),
        }
    }
}

#[derive(Debug, Default, GraphQLObject)]
struct ValidationErrors {
    errors: Vec<ValidationError>,
}

impl From<Vec<ValidationError>> for ValidationErrors {
    fn from(value: Vec<ValidationError>) -> Self {
        ValidationErrors { errors: value }
    }
}

#[derive(GraphQLInputObject)]
#[graphql(description = "Location input.")]
struct LocationInput {
    #[graphql(description = "Country.")]
    country: Option<String>,
    #[graphql(description = "Place within the country.")]
    place: Option<String>,
}

#[derive(Default, GraphQLInputObject)]
struct SettingsInput {
    location: Nullable<LocationInput>,
}

#[derive(GraphQLUnion)]
enum SettingsResult {
    Ok(Settings),
    Err(ValidationErrors),
}

#[derive(GraphQLObject)]
struct StartOptimizationOutput {
    job_id: i32,
}

#[derive(GraphQLObject)]
struct StartOptimizationError {
    message: String,
}

#[derive(GraphQLUnion)]
enum StartOptimizationResult {
    Ok(StartOptimizationOutput),
    Err(StartOptimizationError),
}

#[derive(GraphQLObject)]
struct MaybeError {
    #[graphql(description = "Error message; if null, the operation succeeded.")]
    error: Option<String>,
}

impl From<&str> for MaybeError {
    fn from(value: &str) -> Self {
        MaybeError {
            error: Some(String::from(value)),
        }
    }
}

impl From<String> for MaybeError {
    fn from(value: String) -> Self {
        MaybeError { error: Some(value) }
    }
}

impl MaybeError {
    pub fn new_ok() -> Self {
        MaybeError { error: None }
    }
}

pub struct HerttaContext {
    settings: Arc<Mutex<Settings>>,
    model: Arc<Mutex<Model>>,
    job_id: Arc<Mutex<i32>>,
    optimize_permit: Arc<Semaphore>,
    tx_optimize: mpsc::Sender<(i32, OptimizationTask)>,
    rx_state: watch::Receiver<OptimizationState>,
}

impl Context for HerttaContext {}

impl HerttaContext {
    pub fn new(
        settings: &Arc<Mutex<Settings>>,
        model: &Arc<Mutex<Model>>,
        job_id: &Arc<Mutex<i32>>,
        optimize_permit: &Arc<Semaphore>,
        tx_optimize: mpsc::Sender<(i32, OptimizationTask)>,
        rx_state: watch::Receiver<OptimizationState>,
    ) -> Self {
        HerttaContext {
            settings: Arc::clone(settings),
            model: Arc::clone(model),
            job_id: Arc::clone(job_id),
            optimize_permit: Arc::clone(optimize_permit),
            tx_optimize,
            rx_state,
        }
    }
}

pub struct Query;

#[graphql_object]
#[graphql(context = HerttaContext)]
impl Query {
    fn api_version() -> &'static str {
        "0.10.0"
    }
    fn settings(context: &HerttaContext) -> FieldResult<Settings> {
        let settings = context.settings.lock().unwrap();
        Ok(settings.clone())
    }
    fn model(context: &HerttaContext) -> FieldResult<Model> {
        let model = context.model.lock().unwrap();
        Ok(model.clone())
    }
    fn status(context: &HerttaContext) -> Status {
        match *context.rx_state.borrow() {
            OptimizationState::Idle => return Status::new_idle(),
            OptimizationState::InProgress(job_id) => return Status::new_in_progress(job_id),
            OptimizationState::Finished(job_id) => return Status::new_finished(job_id),
            OptimizationState::Error(job_id, ref error) => return Status::new_error(job_id, error),
        }
    }
}

pub struct Mutation;

#[graphql_object]
#[graphql(context = HerttaContext)]
impl Mutation {
    async fn start_optimization(context: &HerttaContext) -> StartOptimizationResult {
        let current_job_id: i32;
        {
            let _permit = context.optimize_permit.acquire().await.unwrap();
            if let OptimizationState::InProgress(..) = *context.rx_state.borrow() {
                return StartOptimizationResult::Err(StartOptimizationError {
                    message: "optimization already underway".to_string(),
                });
            }
            {
                let mut mutable_id = context.job_id.lock().unwrap();
                current_job_id = *mutable_id;
                *mutable_id += 1;
            }
            if context
                .tx_optimize
                .send((current_job_id, OptimizationTask::Start))
                .await
                .is_err()
            {
                return StartOptimizationResult::Err(StartOptimizationError {
                    message: "failed to send task to event loop".to_string(),
                });
            }
        }
        StartOptimizationResult::Ok(StartOptimizationOutput {
            job_id: current_job_id,
        })
    }
    #[graphql(description = "Update model's time line.")]
    fn update_time_line(
        time_line_input: TimeLineInput,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model = context.model.lock().unwrap();
        time_line_input::update_time_line(time_line_input, &mut model.time_line)
    }
    #[graphql(description = "Add new scenario to model.")]
    fn add_scenario(name: String, weight: f64, context: &HerttaContext) -> MaybeError {
        let mut model = context.model.lock().unwrap();
        scenario_input::add_scenario(name, weight, &mut model.input_data.scenarios)
    }
    #[graphql(description = "Save the model on disk.")]
    fn save_model(context: &HerttaContext) -> MaybeError {
        let file_path = model::make_model_file_path();
        let model = context.model.lock().unwrap();
        let result = model::write_model_to_file(&model, &file_path).err();
        MaybeError { error: result }
    }
    #[graphql(description = "Clear input data from model.")]
    fn clear_input_data(context: &HerttaContext) -> MaybeError {
        let mut lock_guard = context.model.lock();
        let model = lock_guard.as_mut().unwrap();
        model.input_data = BaseInputData::default();
        MaybeError::new_ok()
    }
    #[graphql(description = "Update input data setup.")]
    fn update_input_data_setup(
        setup_update: InputDataSetupInput,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model = context.model.lock().unwrap();
        input_data_setup_input::update_input_data_setup(setup_update, &mut model.input_data.setup)
    }
    #[graphql(description = "Add new node group to model")]
    fn add_node_group(name: String, context: &HerttaContext) -> MaybeError {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        group_input::add_node_group(name, &mut model.input_data.groups)
    }
    #[graphql(description = "Add new process group to model")]
    fn add_process_group(name: String, context: &HerttaContext) -> MaybeError {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        group_input::add_process_group(name, &mut model.input_data.groups)
    }
    #[graphql(description = "Add new process to model.")]
    fn add_process(process: AddProcessInput, context: &HerttaContext) -> ValidationErrors {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        process_input::add_process(
            process,
            &mut model.input_data.processes,
            &mut model.input_data.nodes,
        )
    }
    #[graphql(description = "Add process to process group.")]
    fn add_process_to_group(
        process_name: String,
        group_name: String,
        context: &HerttaContext,
    ) -> MaybeError {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        group_input::add_to_group(
            process_name,
            group_name,
            &mut model.input_data.processes,
            &mut model.input_data.groups,
        )
    }
    #[graphql(description = "Add new topology to given process.")]
    fn add_topology(
        topology: AddTopologyInput,
        process_name: String,
        context: &HerttaContext,
    ) -> AddTopologyResult {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        topology_input::add_topology_to_process(
            &process_name,
            topology,
            &mut model.input_data.processes,
            &mut model.input_data.nodes,
        )
    }
    fn add_node(node: AddNodeInput, context: &HerttaContext) -> ValidationErrors {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        node_input::add_node(
            node,
            &mut model.input_data.nodes,
            &mut model.input_data.processes,
        )
    }
    #[graphql(description = "Add node to node group.")]
    fn add_node_to_group(
        node_name: String,
        group_name: String,
        context: &HerttaContext,
    ) -> MaybeError {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        group_input::add_to_group(
            node_name,
            group_name,
            &mut model.input_data.nodes,
            &mut model.input_data.groups,
        )
    }
    #[graphql(description = "Set state for node. Null clears the state.")]
    fn set_node_state(
        state: Option<SetStateInput>,
        node_name: String,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        state_input::set_state_for_node(&node_name, state, &mut model.input_data.nodes)
    }
    #[graphql(description = "Add diffusion for node.")]
    fn add_node_diffusion(
        diffusion: AddNodeDiffusionInput,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        node_diffusion_input::add_node_diffusion(
            diffusion,
            &mut model.input_data.node_diffusion,
            &model.input_data.nodes,
        )
    }
    #[graphql(description = "Add new market to model.")]
    fn add_market(market: AddMarketInput, context: &HerttaContext) -> ValidationErrors {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        market_input::add_market(
            market,
            &mut model.input_data.markets,
            &model.input_data.nodes,
            &model.input_data.groups,
        )
    }
    #[graphql(description = "Adds new risk to model.")]
    fn add_risk(risk: AddRiskInput, context: &HerttaContext) -> ValidationErrors {
        let mut model = context.model.lock().unwrap();
        risk_input::add_risk(risk, &mut model.input_data.risk)
    }
    #[graphql(description = "Add new generic constraint.")]
    fn add_gen_constraint(
        constraint: AddGenConstraintInput,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model = context.model.lock().unwrap();
        gen_constraint_input::add_gen_constraint(constraint, &mut model.input_data.gen_constraints)
    }
    #[graphql(description = "Add new constraint factor to generic constraint.")]
    fn add_con_factor_to_gen_constraint(
        factor: AddConFactorInput,
        constraint_name: String,
        context: &HerttaContext,
    ) -> AddConFactorResult {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        con_factor_input::add_con_factor_to_constraint(
            factor,
            constraint_name,
            &mut model.input_data.gen_constraints,
            &model.input_data.nodes,
            &model.input_data.processes,
        )
    }
    fn update_settings(settings_input: SettingsInput, context: &HerttaContext) -> SettingsResult {
        let errors = Vec::new();
        let mut settings = context.settings.lock().unwrap();
        match settings_input.location {
            Nullable::Some(ref location_input) => update_location(location_input, &mut settings),
            Nullable::ExplicitNull => settings.location = None,
            Nullable::ImplicitNull => (),
        }
        if !errors.is_empty() {
            return SettingsResult::Err(ValidationErrors::from(errors));
        }
        SettingsResult::Ok(settings.clone())
    }
}

fn update_location(input: &LocationInput, settings: &mut Settings) {
    let location = settings
        .location
        .get_or_insert_with(LocationSettings::default);
    if let Some(ref country) = input.country {
        location.country = country.clone();
    }
    if let Some(ref place) = input.place {
        location.place = place.clone();
    }
}

pub type Schema = RootNode<'static, Query, Mutation, EmptySubscription<HerttaContext>>;

#[cfg(test)]
mod tests {
    use super::*;

    fn default_context() -> HerttaContext {
        let settings = Arc::new(Mutex::new(Settings::default()));
        let model = Arc::new(Mutex::new(Model::default()));
        let job_id = Arc::new(Mutex::new(0));
        let optimize_permit = Arc::new(Semaphore::new(1));
        let (tx_optimize, _rx_optimize) = mpsc::channel::<(i32, OptimizationTask)>(1);
        let (_tx_state, rx_state) = watch::channel::<OptimizationState>(OptimizationState::Idle);
        HerttaContext::new(
            &settings,
            &model,
            &job_id,
            &optimize_permit,
            tx_optimize,
            rx_state,
        )
    }
    #[test]
    fn update_location_in_settings() {
        let context = default_context();
        let mut input = SettingsInput::default();
        let location_input = LocationInput {
            country: Some("Puurtila".to_string()),
            place: Some("Akun puoti".to_string()),
        };
        input.location = Nullable::Some(location_input);
        let output = Mutation::update_settings(input, &context);
        let location_output = match output {
            SettingsResult::Ok(settings) => settings.location.expect("location should be there"),
            SettingsResult::Err(..) => panic!("setting location should not fail"),
        };
        assert_eq!(location_output.country, "Puurtila".to_string());
        assert_eq!(location_output.place, "Akun puoti".to_string());
        {
            let settings = context.settings.lock().unwrap();
            assert_eq!(
                settings
                    .location
                    .as_ref()
                    .expect("location should be set")
                    .country,
                "Puurtila"
            );
            assert_eq!(
                settings
                    .location
                    .as_ref()
                    .expect("location should be set")
                    .place,
                "Akun puoti"
            );
        }
    }
}
