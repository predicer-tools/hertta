use clap::Parser;
use hertta::event_loop::{self, OptimizationState, ResultData};
use hertta::graphql::{HerttaContext, Mutation, Query, Schema};
use hertta::input_data::{InputData, OptimizationData};
use hertta::settings;
use juniper::{EmptySubscription, RootNode};
use serde::Serialize;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::marker::{Send, Sync};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::sync::Semaphore;
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

async fn send_optimization_data_to_event_loop(
    job_id: usize,
    params: HashMap<String, String>,
    input_data: InputData,
    tx_optimize: mpsc::Sender<(usize, OptimizationData)>,
) -> Result<(), String> {
    let fetch_elec_data = params
        .get("fetch_elec_data")
        .map_or(false, |v| v == "elering" || v == "entsoe");
    let optimization_data = OptimizationData {
        fetch_elec_data,
        model_data: Some(input_data),
        time_data: None,
        weather_data: None,
        elec_price_data: None,
        input_data_batch: None,
    };
    if tx_optimize.send((job_id, optimization_data)).await.is_err() {
        return Err("Failed to send optimization data".to_string());
    }
    Ok(())
}

fn inject_clone<T: Clone + Send + Sync + 'static>(x: T) -> warp::filters::BoxedFilter<(T,)> {
    warp::any().map(move || x.clone()).boxed()
}

#[derive(Serialize)]
struct JobId {
    job_id: usize,
}

#[derive(Serialize)]
struct IdleReply {
    status: String,
}

impl IdleReply {
    fn new() -> Self {
        IdleReply {
            status: "idle".to_string(),
        }
    }
}

#[derive(Serialize)]
struct InProgressReply {
    status: String,
    job_id: usize,
}

impl InProgressReply {
    fn new(job_id: usize) -> Self {
        InProgressReply {
            status: "in_progress".to_string(),
            job_id,
        }
    }
}

#[derive(Serialize)]
struct FinishedReply<'a> {
    status: String,
    job_id: usize,
    results: &'a ResultData,
}

impl<'a> FinishedReply<'a> {
    fn new(job_id: usize, results: &'a ResultData) -> Self {
        FinishedReply {
            status: "finished".to_string(),
            job_id,
            results,
        }
    }
}

#[derive(Serialize)]
struct ErrorReply {
    status: String,
    job_id: usize,
    message: String,
}

impl ErrorReply {
    fn new(job_id: usize, message: &str) -> Self {
        ErrorReply {
            status: "error".to_string(),
            job_id,
            message: message.to_string(),
        }
    }
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
        let settings = settings.lock().unwrap();
        if !settings.predicer_runner_project.is_empty() {
            fs::create_dir_all(Path::new(&settings.predicer_runner_project))?;
        }
        settings::validate_settings(&settings)?;
    }
    let (tx_optimize, rx_optimize) = mpsc::channel::<(usize, OptimizationData)>(1);
    let (tx_state, rx_state) = watch::channel::<OptimizationState>(OptimizationState::Idle);
    let settings_clone = Arc::clone(&settings);
    tokio::spawn(async move {
        event_loop::event_loop(settings_clone, rx_optimize, tx_state).await;
    });
    let optimize_permit = Arc::new(Semaphore::new(1));
    let job_id = Arc::new(Mutex::new(0usize));
    let optimize_route = warp::path("api")
        .and(warp::path("optimize"))
        .and(warp::post())
        .and(warp::query::<HashMap<String, String>>())
        .and(warp::body::json())
        .and(inject_clone(optimize_permit))
        .and(inject_clone(job_id))
        .and(inject_clone(rx_state.clone()))
        .and(inject_clone(tx_optimize))
        .and_then(
            |params: HashMap<String, String>,
             input_data: InputData,
             optimize_permit: Arc<Semaphore>,
             job_id: Arc<Mutex<usize>>,
             rx_state: watch::Receiver<OptimizationState>,
             tx_optimize: mpsc::Sender<(usize, OptimizationData)>| async move {
                let current_job_id: usize;
                {
                    let _permit = optimize_permit.acquire().await.unwrap();
                    if let OptimizationState::InProgress(..) = *rx_state.borrow() {
                        return Ok::<_, warp::Rejection>(warp::reply::with_status(
                            warp::reply::json(&()),
                            warp::http::StatusCode::SERVICE_UNAVAILABLE,
                        ));
                    }
                    {
                        let mut mutable_id = job_id.lock().unwrap();
                        current_job_id = *mutable_id;
                        *mutable_id += 1;
                    }
                    tokio::spawn(async move {
                        send_optimization_data_to_event_loop(
                            current_job_id,
                            params,
                            input_data,
                            tx_optimize,
                        )
                        .await
                        .unwrap();
                    });
                }
                Ok::<_, warp::Rejection>(warp::reply::with_status(
                    warp::reply::json(&JobId {
                        job_id: current_job_id,
                    }),
                    warp::http::StatusCode::OK,
                ))
            },
        );
    let progress_route = warp::path("api")
        .and(warp::path("progress"))
        .and(warp::get())
        .and(inject_clone(rx_state.clone()))
        .and_then(|rx_state: watch::Receiver<OptimizationState>| async move {
            match *rx_state.borrow() {
                OptimizationState::Idle => {
                    Ok::<_, warp::Rejection>(warp::reply::json(&IdleReply::new()))
                }
                OptimizationState::InProgress(job_id) => {
                    Ok::<_, warp::Rejection>(warp::reply::json(&InProgressReply::new(job_id)))
                }
                OptimizationState::Finished(job_id, ref result_data) => Ok::<_, warp::Rejection>(
                    warp::reply::json(&FinishedReply::new(job_id, &result_data)),
                ),
                OptimizationState::Error(job_id, ref error) => {
                    Ok::<_, warp::Rejection>(warp::reply::json(&ErrorReply::new(job_id, &error)))
                }
            }
        });
    let schema = Arc::new(Schema::new(Query, Mutation, EmptySubscription::new()));
    let settings_clone = Arc::clone(&settings);
    let graphql_route = warp::path("graphql").and(juniper_warp::make_graphql_filter(
        schema,
        warp::any().map(move || HerttaContext::new(&settings_clone)),
    ));
    let routes = optimize_route.or(progress_route).or(graphql_route);
    let server_handle = warp::serve(routes).run(([127, 0, 0, 1], 3030));
    server_handle.await;
    Ok(())
}
