mod arrow_input;
mod electricity_price_job;
pub mod job_store;
pub mod jobs;
mod optimization_job;
mod time_series;
mod utilities;
mod weather_forecast_job;

use crate::input_data::{TimeSeries, TimeSeriesData};
use crate::input_data_base::BaseInputData;
use crate::model::Model;
use crate::settings::Settings;
use crate::time_line_settings::TimeLineSettings;
use crate::TimeLine;
use job_store::JobStore;
use jobs::{Job, NewJob};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

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
    pub up_price_data: Option<TimeSeriesData>,
    pub down_price_data: Option<TimeSeriesData>,
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
                start_electricity_price_fetch(
                    new_job.job_id(),
                    Arc::clone(&settings),
                    job_store.clone(),
                    model.lock().await.time_line.clone(),
                )
                .await
            }
            Job::Optimization => {
                start_optimization(
                    new_job.job_id(),
                    Arc::clone(&settings),
                    job_store.clone(),
                    Arc::clone(&model),
                )
                .await
            }
            Job::WeatherForecast => {
                start_weather_forecast_fetch(
                    new_job.job_id(),
                    Arc::clone(&settings),
                    job_store.clone(),
                    model.lock().await.time_line.clone(),
                )
                .await
            }
        };
    }
}

async fn start_electricity_price_fetch(
    job_id: i32,
    settings: Arc<Mutex<Settings>>,
    job_store: JobStore,
    time_line_settings: TimeLineSettings,
) {
    tokio::spawn(async move {
        electricity_price_job::start(job_id, settings, job_store, time_line_settings).await
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
