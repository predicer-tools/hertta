mod con_factor_input;
mod delete;
mod gen_constraint_input;
mod group_input;
mod input_data_setup_input;
mod job_status;
mod market_input;
mod node_delay_input;
mod node_diffusion_input;
mod node_history_input;
mod node_input;
mod process_input;
mod risk_input;
mod scenario_input;
mod state_input;
mod time_line_input;
mod topology_input;

use crate::event_loop;
use crate::event_loop::job_store::JobStore;
use crate::event_loop::jobs::{self, Job, JobOutcome, JobStatus, NewJob};
use crate::input_data::Name;
use crate::input_data_base::{
    BaseConFactor, BaseGenConstraint, BaseInputData, BaseMarket, BaseNode, BaseNodeDiffusion,
    BaseProcess, GroupMember, Members, NodeGroup, ProcessGroup, TypeName,
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
use node_delay_input::NewNodeDelay;
use node_history_input::NewSeries;
use node_input::NewNode;
use process_input::NewProcess;
use risk_input::NewRisk;
use state_input::{StateInput, StateUpdate};
use std::ops::DerefMut;
use std::sync::Arc;
use time_line_input::TimeLineUpdate;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
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
struct Error {
    message: String,
}

impl From<&str> for Error {
    fn from(value: &str) -> Self {
        Error {
            message: String::from(value),
        }
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Error { message: value }
    }
}

#[derive(GraphQLObject)]
struct MaybeError {
    #[graphql(description = "Error message; if null, the operation succeeded.")]
    message: Option<String>,
}

impl From<&str> for MaybeError {
    fn from(value: &str) -> Self {
        MaybeError {
            message: Some(String::from(value)),
        }
    }
}

impl From<String> for MaybeError {
    fn from(value: String) -> Self {
        MaybeError {
            message: Some(value),
        }
    }
}

impl MaybeError {
    pub fn new_ok() -> Self {
        MaybeError { message: None }
    }
    pub fn is_error(&self) -> bool {
        self.message.is_some()
    }
}

pub struct HerttaContext {
    settings: Arc<Mutex<Settings>>,
    job_store: JobStore,
    model: Arc<Mutex<Model>>,
    job_sender: mpsc::Sender<NewJob>,
}

impl Context for HerttaContext {}

impl HerttaContext {
    pub fn new(
        settings: Arc<Mutex<Settings>>,
        job_store: JobStore,
        model: Arc<Mutex<Model>>,
        job_sender: mpsc::Sender<NewJob>,
    ) -> Self {
        HerttaContext {
            settings,
            job_store,
            model,
            job_sender,
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
    async fn settings(context: &HerttaContext) -> FieldResult<Settings> {
        let settings = context.settings.lock().await;
        Ok(settings.clone())
    }
    async fn model(context: &HerttaContext) -> FieldResult<Model> {
        let model = context.model.lock().await;
        Ok(model.clone())
    }
    async fn gen_constraint(
        name: String,
        context: &HerttaContext,
    ) -> FieldResult<BaseGenConstraint> {
        let model = context.model.lock().await;
        model
            .input_data
            .gen_constraints
            .iter()
            .find(|g| g.name == name)
            .map(|g| g.clone())
            .ok_or_else(|| "no such generic constraint".into())
    }
    async fn node_group(name: String, context: &HerttaContext) -> FieldResult<NodeGroup> {
        let model = context.model.lock().await;
        model
            .input_data
            .node_groups
            .iter()
            .find(|g| g.name == name)
            .map(|g| g.clone())
            .ok_or_else(|| "no such group".into())
    }
    async fn nodes_in_group(name: String, context: &HerttaContext) -> FieldResult<Vec<BaseNode>> {
        let model = context.model.lock().await;
        group_members(
            &model.input_data.node_groups,
            &name,
            &model.input_data.nodes,
        )
        .map_err(|error| error.into())
    }
    async fn process_group(name: String, context: &HerttaContext) -> FieldResult<ProcessGroup> {
        let model = context.model.lock().await;
        model
            .input_data
            .process_groups
            .iter()
            .find(|g| g.name == name)
            .map(|g| g.clone())
            .ok_or_else(|| "no such group".into())
    }
    async fn processes_in_group(
        name: String,
        context: &HerttaContext,
    ) -> FieldResult<Vec<BaseProcess>> {
        let model = context.model.lock().await;
        group_members(
            &model.input_data.process_groups,
            &name,
            &model.input_data.processes,
        )
        .map_err(|error| error.into())
    }
    async fn market(name: String, context: &HerttaContext) -> FieldResult<BaseMarket> {
        let model = context.model.lock().await;
        model
            .input_data
            .markets
            .iter()
            .find(|m| m.name == name)
            .map(|m| m.clone())
            .ok_or_else(|| "no such market".into())
    }
    async fn node(name: String, context: &HerttaContext) -> FieldResult<BaseNode> {
        let model = context.model.lock().await;
        model
            .input_data
            .nodes
            .iter()
            .find(|n| n.name == name)
            .map(|n| n.clone())
            .ok_or_else(|| "no such node".into())
    }
    #[graphql(description = "Return all groups the given node is member of.")]
    async fn groups_for_node(name: String, context: &HerttaContext) -> FieldResult<Vec<NodeGroup>> {
        let model = context.model.lock().await;
        member_groups(
            &model.input_data.nodes,
            &name,
            &model.input_data.node_groups,
        )
        .map_err(|error| error.into())
    }
    async fn node_diffusion(
        from_node: String,
        to_node: String,
        context: &HerttaContext,
    ) -> FieldResult<BaseNodeDiffusion> {
        let model = context.model.lock().await;
        model
            .input_data
            .node_diffusion
            .iter()
            .find(|n| n.from_node == from_node && n.to_node == to_node)
            .map(|n| n.clone())
            .ok_or_else(|| "no such node diffusion".into())
    }
    #[graphql(description = "Return all groups the given process is member of.")]
    async fn groups_for_process(
        name: String,
        context: &HerttaContext,
    ) -> FieldResult<Vec<ProcessGroup>> {
        let model = context.model.lock().await;
        member_groups(
            &model.input_data.processes,
            &name,
            &model.input_data.process_groups,
        )
        .map_err(|error| error.into())
    }
    async fn process(name: String, context: &HerttaContext) -> FieldResult<BaseProcess> {
        let model = context.model.lock().await;
        model
            .input_data
            .processes
            .iter()
            .find(|p| p.name == name)
            .map(|p| p.clone())
            .ok_or_else(|| "no such process".into())
    }

    async fn con_factors_for_process(
        name: String,
        context: &HerttaContext,
    ) -> FieldResult<Vec<BaseConFactor>> {
        let model = context.model.lock().await;
        let mut factors = Vec::new();
        for constraint in &model.input_data.gen_constraints {
            factors.extend(
                constraint
                    .factors
                    .iter()
                    .filter(|&f| process_input::process_con_factor(f, &name))
                    .cloned(),
            );
        }
        Ok(factors)
    }

    async fn scenario(name: String, context: &HerttaContext) -> FieldResult<Scenario> {
        let model = context.model.lock().await;
        model
            .input_data
            .scenarios
            .iter()
            .find(|s| *s.name() == name)
            .map(|s| s.clone())
            .ok_or_else(|| "no such scenario".into())
    }
    async fn job_status(
        job_id: i32,
        context: &HerttaContext,
    ) -> FieldResult<job_status::JobStatus> {
        let status = context
            .job_store
            .job_status(job_id)
            .await
            .ok_or("no such job")?;
        match *status {
            jobs::JobStatus::Queued => Ok(job_status::JobStatus::new_queued()),
            jobs::JobStatus::InProgress => Ok(job_status::JobStatus::new_in_progress()),
            jobs::JobStatus::Failed(ref failure) => {
                Ok(job_status::JobStatus::new_failed(failure.message().clone()))
            }
            jobs::JobStatus::Finished(..) => Ok(job_status::JobStatus::new_finished()),
        }
    }
    async fn job_outcome(job_id: i32, context: &HerttaContext) -> FieldResult<JobOutcome> {
        let job_status = context
            .job_store
            .job_status(job_id)
            .await
            .ok_or("no such job")?;
        match *job_status {
            JobStatus::Finished(ref outcome) => Ok(outcome.clone()),
            _ => Err("job not finished".into()),
        }
    }
}

fn group_members<G: Members + Name, M: Clone + GroupMember + Name + TypeName>(
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
                M::type_name().to_string(),
                member_name
            )
            .into());
        }
    }
    Ok(members)
}

fn member_groups<M: GroupMember + Name + TypeName, G: Clone + Name>(
    items: &Vec<M>,
    member_name: &str,
    groups: &Vec<G>,
) -> Result<Vec<G>, String> {
    let member = items
        .iter()
        .find(|i| i.name() == member_name)
        .ok_or_else(|| format!("no such {}", M::type_name()))?;
    let mut groups_of_member = Vec::with_capacity(member.groups().len());
    for group_name in member.groups() {
        if let Some(group) = groups.iter().find(|&g| *g.name() == *group_name) {
            groups_of_member.push(group.clone());
        } else {
            return Err(format!(
                "{} group '{}' does not exist",
                M::type_name(),
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
    #[graphql(description = "Start optimization job. Return job ID.")]
    async fn start_optimization(context: &HerttaContext) -> FieldResult<i32> {
        let job_id =
            event_loop::start_job(Job::Optimization, &context.job_store, &context.job_sender)
                .await?;
        Ok(job_id)
    }
    #[graphql(description = "Start electricity price fetch job. Return job ID.")]
    async fn start_electricity_price_fetch(context: &HerttaContext) -> FieldResult<i32> {
        let job_id = event_loop::start_job(
            Job::ElectricityPrice,
            &context.job_store,
            &context.job_sender,
        )
        .await?;
        Ok(job_id)
    }

    #[graphql(description = "Start weather forecast job. Return job ID.")]
    async fn start_weather_forecast_fetch(context: &HerttaContext) -> FieldResult<i32> {
        let job_id = event_loop::start_job(
            Job::WeatherForecast,
            &context.job_store,
            &context.job_sender,
        )
        .await?;
        Ok(job_id)
    }

    #[graphql(description = "Update model's time line.")]
    async fn update_time_line(
        time_line_input: TimeLineUpdate,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model = context.model.lock().await;
        time_line_input::update_time_line(time_line_input, &mut model.time_line)
    }

    #[graphql(description = "Create new scenario.")]
    async fn create_scenario(name: String, weight: f64, context: &HerttaContext) -> MaybeError {
        let mut model = context.model.lock().await;
        scenario_input::create_scenario(name, weight, &mut model.input_data.scenarios)
    }

    #[graphql(description = "Delete a scenario and all items that depend on that scenario.")]
    async fn delete_scenario(name: String, context: &HerttaContext) -> MaybeError {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        scenario_input::delete_scenario(
            &name,
            &mut model.input_data.scenarios,
            &mut model.input_data.node_histories,
        )
    }

    #[graphql(description = "Save the model on disk.")]
    async fn save_model(context: &HerttaContext) -> MaybeError {
        let file_path = model::make_model_file_path();
        let model = context.model.lock().await;
        let result = model::write_model_to_file(&model, &file_path).err();
        MaybeError { message: result }
    }

    #[graphql(description = "Clear input data from model.")]
    async fn clear_input_data(context: &HerttaContext) -> MaybeError {
        let mut lock_guard = context.model.lock().await;
        let model = lock_guard.deref_mut();
        model.input_data = BaseInputData::default();
        MaybeError::new_ok()
    }

    #[graphql(description = "Update input data setup.")]
    async fn update_input_data_setup(
        setup_update: InputDataSetupUpdate,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model = context.model.lock().await;
        input_data_setup_input::update_input_data_setup(setup_update, &mut model.input_data.setup)
    }

    #[graphql(description = "Create new node group")]
    async fn create_node_group(name: String, context: &HerttaContext) -> MaybeError {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        group_input::create_node_group(
            name,
            &mut model.input_data.node_groups,
            &model.input_data.process_groups,
        )
    }

    #[graphql(description = "Create new process group.")]
    async fn create_process_group(name: String, context: &HerttaContext) -> MaybeError {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        group_input::create_process_group(
            name,
            &mut model.input_data.process_groups,
            &model.input_data.node_groups,
        )
    }

    async fn delete_group(name: String, context: &HerttaContext) -> MaybeError {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        group_input::delete_group(
            &name,
            &mut model.input_data.node_groups,
            &mut model.input_data.process_groups,
        )
    }
    #[graphql(description = "Create new process.")]
    async fn create_process(process: NewProcess, context: &HerttaContext) -> ValidationErrors {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        process_input::create_process(
            process,
            &mut model.input_data.processes,
            &mut model.input_data.nodes,
        )
    }

    #[graphql(description = "Add process to process group.")]
    async fn add_process_to_group(
        process_name: String,
        group_name: String,
        context: &HerttaContext,
    ) -> MaybeError {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        group_input::add_to_group(
            &process_name,
            &group_name,
            &mut model.input_data.processes,
            &mut model.input_data.process_groups,
        )
    }

    #[graphql(description = "Delete a process and all items that depend on that process.")]
    async fn delete_process(name: String, context: &HerttaContext) -> MaybeError {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        process_input::delete_process(
            &name,
            &mut model.input_data.processes,
            &mut model.input_data.process_groups,
            &mut model.input_data.gen_constraints,
        )
    }

    #[graphql(description = "Create new topology and add it to process.")]
    async fn create_topology(
        topology: NewTopology,
        source_node_name: Option<String>,
        process_name: String,
        sink_node_name: Option<String>,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model_ref = context.model.lock().await;
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

    async fn delete_topology(
        source_node_name: Option<String>,
        process_name: String,
        sink_node_name: Option<String>,
        context: &HerttaContext,
    ) -> MaybeError {
        let mut model = context.model.lock().await;
        topology_input::delete_topology(
            &process_name,
            &source_node_name,
            &sink_node_name,
            &mut model.input_data.processes,
        )
    }

    #[graphql(description = "Create new node.")]
    async fn create_node(node: NewNode, context: &HerttaContext) -> ValidationErrors {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        node_input::create_node(
            node,
            &mut model.input_data.nodes,
            &mut model.input_data.processes,
        )
    }

    #[graphql(description = "Add node to node group.")]
    async fn add_node_to_group(
        node_name: String,
        group_name: String,
        context: &HerttaContext,
    ) -> MaybeError {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        group_input::add_to_group(
            &node_name,
            &group_name,
            &mut model.input_data.nodes,
            &mut model.input_data.node_groups,
        )
    }

    #[graphql(description = "Set state for node. Null clears the state.")]
    async fn set_node_state(
        state: Option<StateInput>,
        node_name: String,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        state_input::set_state_for_node(&node_name, state, &mut model.input_data.nodes)
    }

    #[graphql(description = "Update state of a node. The state has to be set.")]
    async fn update_node_state(
        state: StateUpdate,
        node_name: String,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        state_input::update_state_in_node(state, node_name, &mut model.input_data.nodes)
    }

    async fn connect_node_inflow_to_temperature_forecast(
        node_name: String,
        forecast_name: String,
        context: &HerttaContext,
    ) -> MaybeError {
        let mut model = context.model.lock().await;
        node_input::connect_node_inflow_to_temperature_forecast(
            &node_name,
            forecast_name,
            &mut model.input_data.nodes,
        )
    }

    #[graphql(description = "Delete a node and all items that depend on that node.")]
    async fn delete_node(name: String, context: &HerttaContext) -> MaybeError {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        node_input::delete_node(
            &name,
            &mut model.input_data.nodes,
            &mut model.input_data.node_groups,
            &mut model.input_data.processes,
            &mut model.input_data.node_diffusion,
            &mut model.input_data.node_delay,
            &mut model.input_data.node_histories,
            &mut model.input_data.markets,
            &mut model.input_data.inflow_blocks,
            &mut model.input_data.gen_constraints,
        )
    }
    #[graphql(description = "Create new diffusion between nodes.")]
    async fn create_node_diffusion(
        from_node: String,
        to_node: String,
        coefficient: f64,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        node_diffusion_input::create_node_diffusion(
            from_node,
            to_node,
            coefficient,
            &mut model.input_data.node_diffusion,
            &model.input_data.nodes,
        )
    }

    async fn delete_node_diffusion(
        from_node: String,
        to_node: String,
        context: &HerttaContext,
    ) -> MaybeError {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        node_diffusion_input::delete_node_diffusion(
            &from_node,
            &to_node,
            &mut model.input_data.node_diffusion,
        )
    }

    async fn create_node_delay(delay: NewNodeDelay, context: &HerttaContext) -> ValidationErrors {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        node_delay_input::create_node_delay(
            delay,
            &mut model.input_data.node_delay,
            &model.input_data.nodes,
        )
    }

    async fn delete_node_delay(
        from_node: String,
        to_node: String,
        context: &HerttaContext,
    ) -> MaybeError {
        let mut model = context.model.lock().await;
        node_delay_input::delete_node_delay(&from_node, &to_node, &mut model.input_data.node_delay)
    }

    async fn create_node_history(node_name: String, context: &HerttaContext) -> ValidationErrors {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        node_history_input::create_node_history(
            node_name,
            &mut model.input_data.node_histories,
            &model.input_data.nodes,
        )
    }

    async fn delete_node_history(node_name: String, context: &HerttaContext) -> MaybeError {
        let mut model = context.model.lock().await;
        node_history_input::delete_node_history(&node_name, &mut model.input_data.node_histories)
    }

    async fn add_step_to_node_history(
        node_name: String,
        step: NewSeries,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        node_history_input::add_step_to_node_history(
            node_name,
            step,
            &mut model.input_data.node_histories,
            &model.input_data.scenarios,
        )
    }

    async fn clear_node_history_steps(node_name: String, context: &HerttaContext) -> MaybeError {
        let mut model = context.model.lock().await;
        node_history_input::clear_node_history_steps(
            &node_name,
            &mut model.input_data.node_histories,
        )
    }

    #[graphql(description = "Create new market.")]
    async fn create_market(market: NewMarket, context: &HerttaContext) -> ValidationErrors {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        market_input::create_market(
            market,
            &mut model.input_data.markets,
            &model.input_data.nodes,
            &model.input_data.process_groups,
        )
    }

    async fn delete_market(name: String, context: &HerttaContext) -> MaybeError {
        let mut model = context.model.lock().await;
        market_input::delete_market(&name, &mut model.input_data.markets)
    }

    #[graphql(description = "Create new risk.")]
    async fn create_risk(risk: NewRisk, context: &HerttaContext) -> ValidationErrors {
        let mut model = context.model.lock().await;
        risk_input::create_risk(risk, &mut model.input_data.risk)
    }

    async fn delete_risk(parameter: String, context: &HerttaContext) -> MaybeError {
        let mut model = context.model.lock().await;
        risk_input::delete_risk(&parameter, &mut model.input_data.risk)
    }

    #[graphql(description = "Create new generic constraint.")]
    async fn create_gen_constraint(
        constraint: NewGenConstraint,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model = context.model.lock().await;
        gen_constraint_input::create_gen_constraint(
            constraint,
            &mut model.input_data.gen_constraints,
        )
    }

    async fn delete_gen_constraint(name: String, context: &HerttaContext) -> MaybeError {
        let mut model = context.model.lock().await;
        gen_constraint_input::delete_gen_constraint(&name, &mut model.input_data.gen_constraints)
    }

    #[graphql(description = "Create new flow constraint factor and add it to generic constraint.")]
    async fn create_flow_con_factor(
        factor: f64,
        constraint_name: String,
        process_name: String,
        source_or_sink_node_name: String,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model_ref = context.model.lock().await;
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

    async fn delete_flow_con_factor(
        constraint_name: String,
        process_name: String,
        source_or_sink_node_name: String,
        context: &HerttaContext,
    ) -> MaybeError {
        let mut model = context.model.lock().await;
        con_factor_input::delete_flow_con_factor(
            &constraint_name,
            &process_name,
            &source_or_sink_node_name,
            &mut model.input_data.gen_constraints,
        )
    }

    #[graphql(description = "Create new state constraint factor and add it to generic constraint.")]
    async fn create_state_con_factor(
        factor: f64,
        constraint_name: String,
        node_name: String,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        con_factor_input::create_state_con_factor(
            factor,
            constraint_name,
            node_name,
            &mut model.input_data.gen_constraints,
            &model.input_data.nodes,
        )
    }

    async fn delete_state_con_factor(
        constraint_name: String,
        node_name: String,
        context: &HerttaContext,
    ) -> MaybeError {
        let mut model = context.model.lock().await;
        con_factor_input::delete_state_con_factor(
            &constraint_name,
            &node_name,
            &mut model.input_data.gen_constraints,
        )
    }

    #[graphql(
        description = "Create new online constraint factor and add it to generic constraint."
    )]
    async fn create_online_con_factor(
        factor: f64,
        constraint_name: String,
        process_name: String,
        context: &HerttaContext,
    ) -> ValidationErrors {
        let mut model_ref = context.model.lock().await;
        let model = model_ref.deref_mut();
        con_factor_input::create_online_con_factor(
            factor,
            constraint_name,
            process_name,
            &mut model.input_data.gen_constraints,
            &model.input_data.processes,
        )
    }

    async fn delete_online_con_factor(
        constraint_name: String,
        process_name: String,
        context: &HerttaContext,
    ) -> MaybeError {
        let mut model = context.model.lock().await;
        con_factor_input::delete_online_con_factor(
            &constraint_name,
            &process_name,
            &mut model.input_data.gen_constraints,
        )
    }

    async fn update_settings(
        settings_input: SettingsInput,
        context: &HerttaContext,
    ) -> SettingsResult {
        let errors = Vec::new();
        let mut settings = context.settings.lock().await;
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
        let job_store = JobStore::default();
        let model = Arc::new(Mutex::new(Model::default()));
        let (tx_optimize, _rx_optimize) = mpsc::channel::<NewJob>(1);
        HerttaContext::new(settings, job_store, model, tx_optimize)
    }
    #[tokio::test]
    async fn update_location_in_settings() {
        let context = default_context();
        let mut input = SettingsInput::default();
        let location_input = LocationInput {
            country: Some("Puurtila".to_string()),
            place: Some("Akun puoti".to_string()),
        };
        input.location = Nullable::Some(location_input);
        let output = Mutation::update_settings(input, &context).await;
        let location_output = match output {
            SettingsResult::Ok(settings) => settings.location.expect("location should be there"),
            SettingsResult::Err(..) => panic!("setting location should not fail"),
        };
        assert_eq!(location_output.country, "Puurtila".to_string());
        assert_eq!(location_output.place, "Akun puoti".to_string());
        {
            let settings = context.settings.lock().await;
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
