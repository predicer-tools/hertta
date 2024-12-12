use clap::Parser;
use hertta::event_loop;
use hertta::event_loop::job_store::JobStore;
use hertta::event_loop::jobs::NewJob;
use hertta::graphql::{HerttaContext, Mutation, Query, Schema};
use hertta::model::{self, Model};
use hertta::settings::{self, Settings};
use juniper::{EmptySubscription, RootNode};
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::marker::{Send, Sync};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use warp::Filter;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct CommandLineArgs {
    #[arg(long, help = "write settings file and exit")]
    write_settings: bool,
    #[arg(long, help = "print GraphQL schema in JSON and exit")]
    print_schema: bool,
}

fn write_default_settings_to_file(settings_file_path: &PathBuf) -> Result<(), Box<dyn Error>> {
    match settings_file_path.parent() {
        Some(settings_dir) => fs::create_dir_all(settings_dir)?,
        None => return Err("settings file should have a parent directory".into()),
    };
    let default_settings =
        settings::make_settings(&settings::map_from_environment_variables(), &PathBuf::new())?;
    let serialized_settings = toml::to_string_pretty(&default_settings)?;
    let mut settings_file = File::create(settings_file_path)?;
    settings_file.write_all(&serialized_settings.into_bytes())?;
    Ok(())
}

fn inject_clone<T: Clone + Send + Sync + 'static>(x: T) -> warp::filters::BoxedFilter<(T,)> {
    warp::any().map(move || x.clone()).boxed()
}

fn write_settings_file() -> Result<(), Box<dyn Error>> {
    let settings_file_path = settings::make_settings_file_path();
    write_default_settings_to_file(&settings_file_path)?;
    println!("Settings written to {}", settings_file_path.display());
    Ok(())
}

fn print_schema_json() -> Result<(), Box<dyn Error>> {
    let schema = RootNode::new(Query, Mutation, EmptySubscription::<HerttaContext>::new());
    let schema_definition = schema.as_sdl();
    println!("{}", schema_definition);
    Ok(())
}

fn get_model() -> Model {
    let file_path = model::make_model_file_path();
    if file_path.is_file() {
        match model::read_model_from_file(&file_path) {
            Ok(model) => return model,
            Err(error) => println!("{}; using default model instead", error),
        }
    }
    model::Model::default()
}

fn spawn_event_loop(
    settings: Arc<Mutex<Settings>>,
    job_store: JobStore,
    model: Arc<Mutex<Model>>,
    job_receiver: mpsc::Receiver<NewJob>,
) {
    tokio::spawn(async move {
        event_loop::event_loop(settings, job_store, model, job_receiver).await;
    });
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = CommandLineArgs::parse();
    if args.write_settings {
        write_settings_file()?;
        return Ok(());
    }
    if args.print_schema {
        print_schema_json()?;
        return Ok(());
    }
    let settings = Arc::new(Mutex::new(settings::make_settings(
        &settings::map_from_environment_variables(),
        &settings::make_settings_file_path(),
    )?));
    {
        let settings = settings.lock().await;
        if !settings.predicer_runner_project.is_empty() {
            fs::create_dir_all(Path::new(&settings.predicer_runner_project))?;
        }
        settings::validate_settings(&settings)?;
    }
    let (job_sender, job_receiver) = mpsc::channel::<NewJob>(32);
    let job_store = JobStore::default();
    let model = Arc::new(Mutex::new(get_model()));
    spawn_event_loop(
        Arc::clone(&settings),
        job_store.clone(),
        Arc::clone(&model),
        job_receiver,
    );
    let schema = Arc::new(Schema::new(Query, Mutation, EmptySubscription::new()));
    let graphql_route = warp::path("graphql").and(juniper_warp::make_graphql_filter(
        schema,
        warp::any()
            .and(inject_clone(settings))
            .and(inject_clone(job_store))
            .and(inject_clone(model))
            .and(inject_clone(job_sender))
            .map(|settings_clone, job_store_clone, model_clone, job_sender| {
                HerttaContext::new(settings_clone, job_store_clone, model_clone, job_sender)
            }),
    ));
    let server_handle = warp::serve(graphql_route).run(([127, 0, 0, 1], 3030));
    server_handle.await;
    Ok(())
}
