pub mod types;

use crate::event_loop::{OptimizationState, OptimizationTask};
use crate::input_data_base::BaseInputData;
use crate::model::{self, Model};
use crate::settings::{LocationSettings, Settings};
use crate::status::Status;
use crate::time_line_settings::{Duration, TimeLineSettings};
use juniper::{
    graphql_object, Context, EmptySubscription, FieldResult, GraphQLInputObject, GraphQLObject,
    GraphQLUnion, Nullable, RootNode,
};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::sync::Semaphore;

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

#[derive(Debug, GraphQLObject)]
struct ValidationErrors {
    errors: Vec<ValidationError>,
}

impl From<Vec<ValidationError>> for ValidationErrors {
    fn from(value: Vec<ValidationError>) -> Self {
        ValidationErrors { errors: value }
    }
}

#[derive(GraphQLInputObject)]
struct DurationInput {
    hours: i32,
    minutes: i32,
    seconds: i32,
}

#[derive(GraphQLInputObject)]
struct TimeLineInput {
    duration: Option<DurationInput>,
    step: Option<DurationInput>,
}

#[derive(Default, GraphQLInputObject)]
struct ModelInput {
    time_line: Option<TimeLineInput>,
}

#[derive(GraphQLUnion)]
enum ModelResult {
    Ok(Model),
    Err(ValidationErrors),
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
    fn update_model(model_input: ModelInput, context: &HerttaContext) -> ModelResult {
        let mut errors = Vec::new();
        let mut model = context.model.lock().unwrap();
        if let Some(ref time_line_input) = model_input.time_line {
            update_time_line(time_line_input, &mut model.time_line, &mut errors);
        }
        if !errors.is_empty() {
            return ModelResult::Err(ValidationErrors::from(errors));
        }
        ModelResult::Ok(model.clone())
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
        MaybeError { error: None }
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

fn update_time_line(
    input: &TimeLineInput,
    time_line: &mut TimeLineSettings,
    errors: &mut Vec<ValidationError>,
) {
    if let Some(ref duration_input) = input.duration {
        duration_from_input(
            duration_input,
            |d| time_line.set_duration(d),
            "duration",
            errors,
        );
    }
    if let Some(ref step_input) = input.step {
        duration_from_input(step_input, |d| time_line.set_step(d), "step", errors);
    }
}

fn duration_from_input<F>(
    input: &DurationInput,
    set_duration: F,
    field: &str,
    errors: &mut Vec<ValidationError>,
) where
    F: FnOnce(Duration) -> Result<(), String>,
{
    match Duration::try_new(input.hours, input.minutes, input.seconds) {
        Ok(duration) => match set_duration(duration) {
            Ok(..) => (),
            Err(error) => errors.push(ValidationError::new(field, &error)),
        },
        Err(error) => errors.push(ValidationError::new(field, &error)),
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
    use chrono::TimeDelta;

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
    #[test]
    fn update_time_line_in_model() {
        let time_line_input = TimeLineInput {
            duration: Some(DurationInput {
                hours: 13,
                minutes: 0,
                seconds: 0,
            }),
            step: Some(DurationInput {
                hours: 0,
                minutes: 45,
                seconds: 0,
            }),
        };
        let input = ModelInput {
            time_line: Some(time_line_input),
        };
        let context = default_context();
        let output = match Mutation::update_model(input, &context) {
            ModelResult::Ok(time_line) => time_line,
            ModelResult::Err(..) => panic!("setting time line should not fail"),
        };
        assert_eq!(
            output.time_line.duration().to_time_delta(),
            TimeDelta::hours(13)
        );
        assert_eq!(
            output.time_line.step().to_time_delta(),
            TimeDelta::minutes(45)
        );
        {
            let model = context.model.lock().unwrap();
            assert_eq!(
                model.time_line.duration().to_time_delta(),
                TimeDelta::hours(13)
            );
            assert_eq!(
                model.time_line.step().to_time_delta(),
                TimeDelta::minutes(45)
            );
        }
    }

    #[test]
    fn setting_negative_duration_causes_error() {
        let time_line_input = TimeLineInput {
            duration: Some(DurationInput {
                hours: -13,
                minutes: 0,
                seconds: 0,
            }),
            step: Some(DurationInput {
                hours: 0,
                minutes: 45,
                seconds: 0,
            }),
        };
        let input = ModelInput {
            time_line: Some(time_line_input),
        };
        let context = default_context();
        match Mutation::update_model(input, &context) {
            ModelResult::Ok(..) => panic!("negative duration should cause error"),
            ModelResult::Err(errors) => {
                assert_eq!(errors.errors.len(), 1);
                assert_eq!(errors.errors[0].field, "duration");
                assert_eq!(errors.errors[0].message, "hours should be non-negative");
            }
        };
        let model = context.model.lock().unwrap();
        let vanilla_time_line = TimeLineSettings::default();
        assert_eq!(model.time_line.duration(), vanilla_time_line.duration());
        assert_eq!(
            model.time_line.step().to_time_delta(),
            TimeDelta::minutes(45)
        );
    }

    #[test]
    fn setting_negative_step_causes_error() {
        let time_line_input = TimeLineInput {
            duration: Some(DurationInput {
                hours: 13,
                minutes: 0,
                seconds: 0,
            }),
            step: Some(DurationInput {
                hours: 0,
                minutes: -45,
                seconds: 0,
            }),
        };
        let input = ModelInput {
            time_line: Some(time_line_input),
        };

        let context = default_context();
        match Mutation::update_model(input, &context) {
            ModelResult::Ok(..) => panic!("negative step should cause error"),
            ModelResult::Err(errors) => {
                assert_eq!(errors.errors.len(), 1);
                assert_eq!(errors.errors[0].field, "step");
                assert_eq!(errors.errors[0].message, "minutes should be non-negative");
            }
        };
        let model = context.model.lock().unwrap();
        assert_eq!(
            model.time_line.duration().to_time_delta(),
            TimeDelta::hours(13)
        );
        let vanilla_time_line = TimeLineSettings::default();
        assert_eq!(model.time_line.step(), vanilla_time_line.step());
    }
    #[test]
    fn setting_step_longer_than_duration_causes_error() {
        let time_line_input = TimeLineInput {
            duration: Some(DurationInput {
                hours: 13,
                minutes: 0,
                seconds: 0,
            }),
            step: Some(DurationInput {
                hours: 45,
                minutes: 0,
                seconds: 0,
            }),
        };
        let input = ModelInput {
            time_line: Some(time_line_input),
        };
        let context = default_context();
        match Mutation::update_model(input, &context) {
            ModelResult::Ok(..) => panic!("too long step should cause error"),
            ModelResult::Err(errors) => {
                assert_eq!(errors.errors.len(), 1);
                assert_eq!(errors.errors[0].field, "step");
                assert_eq!(
                    errors.errors[0].message,
                    "time line step should not exceed duration"
                );
            }
        };
        let model = context.model.lock().unwrap();
        assert_eq!(
            model.time_line.duration().to_time_delta(),
            TimeDelta::hours(13)
        );
        let vanilla_time_line = TimeLineSettings::default();
        assert_eq!(model.time_line.step(), vanilla_time_line.step());
    }
}
