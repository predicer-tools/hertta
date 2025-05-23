mod arrow_input;
mod electricity_price_job_elering;
mod electricity_price_job_entsoe;
pub mod job_store;
pub mod jobs;
mod optimization_job;
mod time_series;
mod utilities;
mod weather_forecast_job;

use crate::input_data::{TimeSeries, TimeSeriesData};
use crate::input_data_base::{BaseInputData, BaseForecastable};
use crate::model::Model;
use crate::settings::Settings;
use crate::time_line_settings::TimeLineSettings;
use crate::TimeLine;
use job_store::JobStore;
use jobs::{Job, NewJob};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use crate::event_loop::jobs::{JobStatus, JobOutcome, ElectricityPriceOutcome, WeatherForecastOutcome};

pub struct OptimizationData {
    pub input_data: BaseInputData,
    pub time_data: Option<TimeLine>,
    pub weather_data: Option<WeatherData>,
    pub elec_price_data: Option<ElectricityPriceData>,
}

impl OptimizationData {
    fn with_input_data(input_data: BaseInputData) -> Self {
        OptimizationData {
            input_data: input_data,
            time_data: None,
            weather_data: None,
            elec_price_data: None,
        }
    }
}

type WeatherData = Vec<TimeSeries>;

#[derive(Clone, Debug, Default)]
pub struct ElectricityPriceData {
    pub price_data: Option<TimeSeriesData>,
}

pub async fn start_job(
    job: Job,
    job_store: &JobStore,
    job_sender: &mpsc::Sender<NewJob>,
) -> Result<i32, String> {
    let job_id = job_store.create_queued_job().await;
    if job_sender.send(NewJob::new(job_id, job)).await.is_err() {
        return Err("failed to send job to event loop".into());
    }
    Ok(job_id)
}

pub async fn event_loop(
    settings: Arc<Mutex<Settings>>,
    job_store: JobStore,
    model: Arc<Mutex<Model>>,
    mut message_receiver: mpsc::Receiver<NewJob>,
) {
    while let Some(new_job) = message_receiver.recv().await {
        match new_job.job() {
            Job::ElectricityPrice => {
                let (mut found_valid, mut invalid_names) = (false, Vec::<String>::new());
                let (mut has_elering, mut has_entsoe) = (false, false);
                {
                    let model_guard = model.lock().await;
                    for market in &model_guard.input_data.markets {
                        for fv in &market.price {
                            if let BaseForecastable::Forecast(f) = &fv.value {
                                if f.f_type() == "electricity" {
                                    match f.name() {
                                        "ELERING" => { has_elering = true; found_valid = true; }
                                        "ENTSOE"  => { has_entsoe  = true; found_valid = true; }
                                        other     => invalid_names.push(other.to_owned()),
                                    }
                                }
                            }
                        }
                    }
                }
                if !invalid_names.is_empty() {
                    let msg = format!(
                        "Electricity forecast(s) with unsupported name(s): {}",
                        invalid_names.join(", ")
                    );
                    let _ = job_store
                        .set_job_status(
                            new_job.job_id(),
                            Arc::new(JobStatus::Failed(msg.into())),
                        )
                        .await;
                    continue;
                }

                if !found_valid {
                    println!(
                        "[event_loop] No valid electricity price forecasts found for job {}; skipping fetch",
                        new_job.job_id()
                    );
                    let _ = job_store
                        .set_job_status(
                            new_job.job_id(),
                            Arc::new(JobStatus::Finished(JobOutcome::ElectricityPrice(
                                ElectricityPriceOutcome::new(vec![], vec![]),
                            ))),
                        )
                        .await;
                    continue;
                }

                start_electricity_price_fetch(
                    new_job.job_id(),
                    Arc::clone(&settings),
                    job_store.clone(),
                    model.lock().await.time_line.clone(),
                    has_elering,
                    has_entsoe,
                )
                .await;
            }

            Job::Optimization => {
                start_optimization(
                    new_job.job_id(),
                    Arc::clone(&settings),
                    job_store.clone(),
                    Arc::clone(&model),
                )
                .await;
            }

            Job::WeatherForecast => {
                let has_weather_forecast = {
                    let model_guard = model.lock().await;
                    model_guard
                        .input_data
                        .nodes
                        .iter()
                        .any(|node| {
                            node.inflow.iter().any(|fv| match &fv.value {
                                BaseForecastable::Forecast(f) if f.name() == "FMI" => true,
                                _ => false,
                            })
                        })
                };

                if !has_weather_forecast {
                    let _ = job_store
                        .set_job_status(
                            new_job.job_id(),
                            Arc::new(JobStatus::Finished(
                                JobOutcome::WeatherForecast(
                                    WeatherForecastOutcome::new(vec![], vec![]),
                                ),
                            )),
                        )
                        .await;
                    continue;
                }

                start_weather_forecast_fetch(
                    new_job.job_id(),
                    Arc::clone(&settings),
                    job_store.clone(),
                    model.lock().await.time_line.clone(),
                )
                .await;
            }
        };
    }
}

async fn start_electricity_price_fetch(
    job_id: i32,
    settings: Arc<Mutex<Settings>>,
    job_store: JobStore,
    time_line_settings: TimeLineSettings,
    fetch_elering: bool,
    fetch_entsoe: bool,
) {
    tokio::spawn(async move {
        if fetch_elering {
            electricity_price_job_elering::start(
                job_id,
                Arc::clone(&settings),
                job_store.clone(),
                time_line_settings.clone(),
            )
            .await;
        }

        if fetch_entsoe {
            electricity_price_job_entsoe::start(
                job_id,
                settings,
                job_store,
                time_line_settings,
            )
            .await;
        }
    });
}

async fn start_optimization(
    job_id: i32,
    settings: Arc<Mutex<Settings>>,
    job_store: JobStore,
    model: Arc<Mutex<Model>>,
) {
    tokio::spawn(async move { optimization_job::start(job_id, settings, job_store, model).await });
}

async fn start_weather_forecast_fetch(
    job_id: i32,
    settings: Arc<Mutex<Settings>>,
    job_store: JobStore,
    time_line_settings: TimeLineSettings,
) {
    tokio::spawn(async move {
        weather_forecast_job::start(job_id, settings, job_store, time_line_settings).await
    });
}
