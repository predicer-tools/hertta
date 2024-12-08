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
mod status;
mod time_line_input;
mod topology_input;

use crate::event_loop::{OptimizationState, OptimizationTask};
use crate::input_data::Name;
use crate::input_data_base::{
    BaseGenConstraint, BaseInputData, BaseMarket, BaseNode, BaseNodeDiffusion, BaseProcess,
    GroupMember, Members, NodeGroup, ProcessGroup,
};
use crate::model::{self, Model};
use crate::scenarios::Scenario;
use crate::settings::{LocationSettings, Settings};
use gen_constraint_input::NewGenConstraint;
use input_data_setup_input::InputDataSetupUpdate;
use juniper::{
    graphql_object, Context, EmptySubscription, FieldResult, GraphQLInputObject, GraphQLObject,
    GraphQLUnion, Nullable, RootNode,
};
use market_input::NewMarket;
use node_input::NewNode;
use process_input::NewProcess;
use risk_input::NewRisk;
use state_input::{StateInput, StateUpdate};
use status::Status;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use time_line_input::TimeLineUpdate;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::sync::Semaphore;
use topology_input::NewTopology;

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

impl From<ValidationError> for ValidationErrors {
    fn from(value: ValidationError) -> Self {
        ValidationErrors {
            errors: vec![value],
        }
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

    pub fn model(&self) -> &Arc<Mutex<Model>> {
        &self.model
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
    fn gen_constraint(name: String, context: &HerttaContext) -> FieldResult<BaseGenConstraint> {
        let model = context.model.lock().unwrap();
        model
            .input_data
            .gen_constraints
            .iter()
            .find(|g| g.name == name)
            .map(|g| g.clone())
            .ok_or_else(|| "no such generic constraint".into())
    }
    fn node_group(name: String, context: &HerttaContext) -> FieldResult<NodeGroup> {
        let model = context.model.lock().unwrap();
        model
            .input_data
            .node_groups
            .iter()
            .find(|g| g.name == name)
            .map(|g| g.clone())
            .ok_or_else(|| "no such group".into())
    }
    fn nodes_in_group(name: String, context: &HerttaContext) -> FieldResult<Vec<BaseNode>> {
        let model = context.model.lock().unwrap();
        group_members(
            &model.input_data.node_groups,
            &name,
            &model.input_data.nodes,
        )
        .map_err(|error| error.into())
    }
    fn process_group(name: String, context: &HerttaContext) -> FieldResult<ProcessGroup> {
        let model = context.model.lock().unwrap();
        model
            .input_data
            .process_groups
            .iter()
            .find(|g| g.name == name)
            .map(|g| g.clone())
            .ok_or_else(|| "no such group".into())
    }
    fn processes_in_group(name: String, context: &HerttaContext) -> FieldResult<Vec<BaseProcess>> {
        let model = context.model.lock().unwrap();
        group_members(
            &model.input_data.process_groups,
            &name,
            &model.input_data.processes,
        )
        .map_err(|error| error.into())
    }
    fn market(name: String, context: &HerttaContext) -> FieldResult<BaseMarket> {
        let model = context.model.lock().unwrap();
        model
            .input_data
            .markets
            .iter()
            .find(|m| m.name == name)
            .map(|m| m.clone())
            .ok_or_else(|| "no such market".into())
    }
    fn node(name: String, context: &HerttaContext) -> FieldResult<BaseNode> {
        let model = context.model.lock().unwrap();
        model
            .input_data
            .nodes
            .iter()
            .find(|n| n.name == name)
            .map(|n| n.clone())
            .ok_or_else(|| "no such node".into())
    }
    #[graphql(description = "Return all groups the given node is member of.")]
    fn groups_for_node(name: String, context: &HerttaContext) -> FieldResult<Vec<NodeGroup>> {
        let model = context.model.lock().unwrap();
        member_groups(
            &model.input_data.nodes,
            &name,
            &model.input_data.node_groups,
        )
        .map_err(|error| error.into())
    }
    fn node_diffusion(
        from_node: String,
        to_node: String,
        context: &HerttaContext,
    ) -> FieldResult<BaseNodeDiffusion> {
        let model = context.model.lock().unwrap();
        model
            .input_data
            .node_diffusion
            .iter()
            .find(|n| n.from_node == from_node && n.to_node == to_node)
            .map(|n| n.clone())
            .ok_or_else(|| "no such node diffusion".into())
    }
    #[graphql(description = "Return all groups the given process is member of.")]
    fn groups_for_process(name: String, context: &HerttaContext) -> FieldResult<Vec<ProcessGroup>> {
        let model = context.model.lock().unwrap();
        member_groups(
            &model.input_data.processes,
            &name,
            &model.input_data.process_groups,
        )
        .map_err(|error| error.into())
    }
    fn process(name: String, context: &HerttaContext) -> FieldResult<BaseProcess> {
        let model = context.model.lock().unwrap();
        model
            .input_data
            .processes
            .iter()
            .find(|p| p.name == name)
            .map(|p| p.clone())
            .ok_or_else(|| "no such process".into())
    }
    fn scenario(name: String, context: &HerttaContext) -> FieldResult<Scenario> {
        let model = context.model.lock().unwrap();
        model
            .input_data
            .scenarios
            .iter()
            .find(|s| *s.name() == name)
            .map(|s| s.clone())
            .ok_or_else(|| "no such scenario".into())
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

fn group_members<G: Members + Name, M: Clone + GroupMember + Name>(
    groups: &Vec<G>,
    group_name: &str,
    candidates: &Vec<M>,
) -> Result<Vec<M>, String> {
    let group = groups
        .iter()
        .find(|&g| g.name() == group_name)
        .ok_or("no such group")?;
    let mut members = Vec::with_capacity(group.members().len());
    for member_name in group.members() {
        if let Some(member) = candidates.iter().find(|m| *m.name() == *member_name) {
            members.push(member.clone())
        } else {
            return Err(format!(
                "member {} '{}' does not exist",
                M::group_type().to_string(),
                member_name
            )
            .into());
        }
    }
    Ok(members)
}

fn member_groups<M: GroupMember + Name, G: Clone + Name>(
    items: &Vec<M>,
    member_name: &str,
    groups: &Vec<G>,
) -> Result<Vec<G>, String> {
    let member = items
        .iter()
        .find(|i| i.name() == member_name)
        .ok_or_else(|| format!("no such {}", M::group_type()))?;
    let mut groups_of_member = Vec::with_capacity(member.groups().len());
    for group_name in member.groups() {
        if let Some(group) = groups.iter().find(|&g| *g.name() == *group_name) {
            groups_of_member.push(group.clone());
        } else {
            return Err(format!(
                "{} group '{}' does not exist",
                M::group_type(),
                group_name
            ));
        }
    }
    Ok(groups_of_member)
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
        time_line_input: TimeLineUpdate,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model = context.model.lock().unwrap();
        time_line_input::update_time_line(time_line_input, &mut model.time_line)
    }

    #[graphql(description = "Create new scenario.")]
    fn create_scenario(name: String, weight: f64, context: &HerttaContext) -> MaybeError {
        let mut model = context.model.lock().unwrap();
        scenario_input::create_scenario(name, weight, &mut model.input_data.scenarios)
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
        setup_update: InputDataSetupUpdate,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model = context.model.lock().unwrap();
        input_data_setup_input::update_input_data_setup(setup_update, &mut model.input_data.setup)
    }

    #[graphql(description = "Create new node group")]
    fn create_node_group(name: String, context: &HerttaContext) -> MaybeError {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        group_input::create_node_group(name, &mut model.input_data.node_groups)
    }

    #[graphql(description = "Create new process group.")]
    fn create_process_group(name: String, context: &HerttaContext) -> MaybeError {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        group_input::create_process_group(name, &mut model.input_data.process_groups)
    }

    #[graphql(description = "Create new process.")]
    fn create_process(process: NewProcess, context: &HerttaContext) -> ValidationErrors {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        process_input::create_process(
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
            &process_name,
            &group_name,
            &mut model.input_data.processes,
            &mut model.input_data.process_groups,
        )
    }

    #[graphql(description = "Create new topology and add it to process.")]
    fn create_topology(
        topology: NewTopology,
        source_node_name: Option<String>,
        process_name: String,
        sink_node_name: Option<String>,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        topology_input::create_topology(
            process_name,
            source_node_name,
            sink_node_name,
            topology,
            &mut model.input_data.processes,
            &mut model.input_data.nodes,
        )
    }

    #[graphql(description = "Create new node.")]
    fn create_node(node: NewNode, context: &HerttaContext) -> ValidationErrors {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        node_input::create_node(
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
            &node_name,
            &group_name,
            &mut model.input_data.nodes,
            &mut model.input_data.node_groups,
        )
    }

    #[graphql(description = "Set state for node. Null clears the state.")]
    fn set_node_state(
        state: Option<StateInput>,
        node_name: String,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        state_input::set_state_for_node(&node_name, state, &mut model.input_data.nodes)
    }

    #[graphql(description = "Update state of a node. The state has to be set.")]
    fn update_node_state(
        state: StateUpdate,
        node_name: String,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        state_input::update_state_in_node(state, node_name, &mut model.input_data.nodes)
    }

    #[graphql(description = "Create new diffusion between nodes.")]
    fn create_node_diffusion(
        from_node: String,
        to_node: String,
        coefficient: f64,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        node_diffusion_input::create_node_diffusion(
            from_node,
            to_node,
            coefficient,
            &mut model.input_data.node_diffusion,
            &model.input_data.nodes,
        )
    }

    #[graphql(description = "Create new market.")]
    fn create_market(market: NewMarket, context: &HerttaContext) -> ValidationErrors {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        market_input::create_market(
            market,
            &mut model.input_data.markets,
            &model.input_data.nodes,
            &model.input_data.process_groups,
        )
    }

    #[graphql(description = "Create new risk.")]
    fn create_risk(risk: NewRisk, context: &HerttaContext) -> ValidationErrors {
        let mut model = context.model.lock().unwrap();
        risk_input::create_risk(risk, &mut model.input_data.risk)
    }

    #[graphql(description = "Create new generic constraint.")]
    fn create_gen_constraint(
        constraint: NewGenConstraint,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model = context.model.lock().unwrap();
        gen_constraint_input::create_gen_constraint(
            constraint,
            &mut model.input_data.gen_constraints,
        )
    }

    #[graphql(description = "Create new flow constraint factor and add it to generic constraint.")]
    fn create_flow_con_factor(
        factor: f64,
        constraint_name: String,
        process_name: String,
        source_or_sink_node_name: String,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        con_factor_input::create_flow_con_factor(
            factor,
            constraint_name,
            process_name,
            source_or_sink_node_name,
            &mut model.input_data.gen_constraints,
            &model.input_data.processes,
        )
    }

    #[graphql(description = "Create new state constraint factor and add it to generic constraint.")]
    fn create_state_con_factor(
        factor: f64,
        constraint_name: String,
        node_name: String,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        con_factor_input::create_state_con_factor(
            factor,
            constraint_name,
            node_name,
            &mut model.input_data.gen_constraints,
            &model.input_data.nodes,
        )
    }

    #[graphql(
        description = "Create new online constraint factor and add it to generic constraint."
    )]
    fn create_online_con_factor(
        factor: f64,
        constraint_name: String,
        process_name: String,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model_ref = context.model.lock().unwrap();
        let model = model_ref.deref_mut();
        con_factor_input::create_online_con_factor(
            factor,
            constraint_name,
            process_name,
            &mut model.input_data.gen_constraints,
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
