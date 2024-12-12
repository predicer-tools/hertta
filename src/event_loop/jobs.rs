use super::optimization_job::ControlSignal;
use crate::TimeLine;
use juniper::{GraphQLObject, GraphQLUnion};

pub enum Job {
    Optimization,
    WeatherForecast,
}

pub struct NewJob {
    job_id: i32,
    job: Job,
}

impl NewJob {
    pub fn new(job_id: i32, job: Job) -> Self {
        NewJob { job_id, job }
    }
    pub fn job_id(&self) -> i32 {
        self.job_id
    }
    pub fn job(&self) -> &Job {
        &self.job
    }
}

pub enum JobStatus {
    Queued,
    InProgress,
    Failed(JobFailure),
    Finished(JobOutcome),
}

pub struct JobFailure {
    message: String,
}

impl From<String> for JobFailure {
    fn from(value: String) -> Self {
        JobFailure { message: value }
    }
}

impl From<&str> for JobFailure {
    fn from(value: &str) -> Self {
        JobFailure {
            message: String::from(value),
        }
    }
}

impl JobFailure {
    pub fn message(&self) -> &String {
        &self.message
    }
}

#[derive(Clone, GraphQLUnion)]
pub enum JobOutcome {
    Optimization(OptimizationOutcome),
    WeatherForecast(WeatherForecastOutcome),
}

#[derive(Clone, GraphQLObject)]
pub struct OptimizationOutcome {
    time: TimeLine,
    control_signals: Vec<ControlSignal>,
}

impl OptimizationOutcome {
    pub fn new(time: TimeLine, control_signals: Vec<ControlSignal>) -> Self {
        OptimizationOutcome {
            time,
            control_signals,
        }
    }
}

#[derive(Clone, GraphQLObject)]
pub struct WeatherForecastOutcome {
    time: TimeLine,
    temperature: Vec<f64>,
}

impl WeatherForecastOutcome {
    pub fn new(time: TimeLine, temperature: Vec<f64>) -> Self {
        WeatherForecastOutcome { time, temperature }
    }
}
