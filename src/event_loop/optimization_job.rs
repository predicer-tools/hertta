use super::arrow_input;
use super::electricity_price_job;
use super::job_store::JobStore;
use super::jobs::{JobOutcome, JobStatus, OptimizationOutcome};
use super::time_series;
use super::utilities;
use super::weather_forecast_job;
use super::{ElectricityPriceData, OptimizationData, WeatherData};
use crate::input_data::{InputData, Market, TimeSeries, TimeSeriesData};
use crate::input_data_base::{self, BaseInputData, BaseProcess};
use crate::model::Model;
use crate::scenarios::Scenario;
use crate::settings::{LocationSettings, Settings};
use crate::time_line_settings::TimeLineSettings;
use crate::{TimeLine, TimeStamp};
use arrow::array::timezone::Tz;
use arrow::array::{self, Array};
use arrow::datatypes::{DataType, Float64Type, TimeUnit, TimestampMillisecondType};
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;
use chrono::{DateTime, DurationRound, TimeDelta, Utc};
use juniper::GraphQLObject;
use std::collections::BTreeMap;
use std::error::Error;
use std::process::{Command, ExitStatus};
use std::sync::Arc;
use tokio::io;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use zmq::{Context, Socket};

const PREDICER_SEND_FLAGS: i32 = 0;
const PREDICER_RECEIVE_FLAGS: i32 = 0;

#[derive(Clone, GraphQLObject)]
pub struct ControlSignal {
    pub name: String,
    pub signal: Vec<f64>,
}

pub async fn start(
    job_id: i32,
    settings: Arc<Mutex<Settings>>,
    job_store: JobStore,
    model: Arc<Mutex<Model>>,
) {
    if job_store
        .set_job_status(job_id, Arc::new(JobStatus::InProgress))
        .await
        .is_err()
    {
        return;
    }
    let settings_snapshot = settings.lock().await.clone();
    let model_snapshot = model.lock().await.clone();
    let optimization_data = OptimizationData::with_input_data(model_snapshot.input_data);
    let start_time: TimeStamp = Utc::now().duration_trunc(TimeDelta::hours(1)).unwrap();
    let control_processes = match expected_control_processes(&optimization_data.input_data) {
        Ok(names) => names,
        Err(error) => {
            let _ = job_store
                .set_job_status(job_id, Arc::new(JobStatus::Failed(error.into())))
                .await;
            return;
        }
    };
    let mut zmq_port = settings_snapshot.predicer_port;
    if zmq_port == 0 {
        zmq_port = match find_available_port().await {
            Ok(port) => port,
            Err(..) => {
                let _ = job_store
                    .set_job_status(
                        job_id,
                        Arc::new(JobStatus::Failed(
                            "failed to find available ZMQ port".into(),
                        )),
                    )
                    .await;
                return;
            }
        }
    }
    let (tx_time_line, rx_time_line) = oneshot::channel::<OptimizationData>();
    let (tx_weather, rx_weather) = oneshot::channel::<OptimizationData>();
    let (tx_elec, rx_elec) = oneshot::channel::<OptimizationData>();
    let (tx_update, rx_update) = oneshot::channel::<OptimizationData>();
    let (tx_batches, rx_batches) = oneshot::channel::<InputData>();
    let (tx_optimization, rx_optimization) = oneshot::channel::<Vec<(String, Vec<u8>)>>();
    let time_line_settings_clone = model_snapshot.time_line.clone();
    let start_time_clone = start_time.clone();
    let update_time_line_handle = tokio::spawn(async move {
        update_time_line_task(
            start_time_clone,
            &time_line_settings_clone,
            rx_time_line,
            tx_weather,
        )
        .await
    });
    let location_snapshot = match settings_snapshot.location {
        Some(location) => location.clone(),
        None => {
            let _ = job_store
                .set_job_status(
                    job_id,
                    Arc::new(JobStatus::Failed(
                        "cannot fetch weather data: no location set".into(),
                    )),
                )
                .await;
            return;
        }
    };
    let location_clone = location_snapshot.clone();
    let time_line_settings_clone = model_snapshot.time_line.clone();
    let python_exec_clone = settings_snapshot.python_exec.clone();
    let weather_fetcher_script_clone = settings_snapshot.weather_fetcher_script.clone();
    let fetch_weather_data_handle = tokio::spawn(async move {
        fetch_weather_data_task(
            &location_clone,
            &time_line_settings_clone,
            &python_exec_clone,
            &weather_fetcher_script_clone,
            rx_weather,
            tx_elec,
        )
        .await
    });
    let location_clone = location_snapshot.clone();
    let scenarios_clone = optimization_data.input_data.scenarios.clone();
    let fetch_electricity_price_handle = tokio::spawn(async move {
        fetch_electricity_price_task(&location_clone, &scenarios_clone, rx_elec, tx_update).await
    });
    let update_model_data_handle =
        tokio::spawn(async move { generate_model_task(rx_update, tx_batches).await });
    let data_conversion_handle =
        tokio::spawn(async move { data_conversion_task(rx_batches, tx_optimization).await });
    let optimization_handle =
        tokio::spawn(async move { optimization_task(rx_optimization, zmq_port).await });
    if tx_time_line.send(optimization_data).is_err() {
        let _ = job_store
            .set_job_status(
                job_id,
                Arc::new(JobStatus::Failed(
                    "failed to send initial data to optimization pipeline".into(),
                )),
            )
            .await;
        return;
    }
    let julia_exec_clone = settings_snapshot.julia_exec.clone();
    let predicer_runner_project_clone = settings_snapshot.predicer_runner_project.clone();
    let predicer_project_clone = settings_snapshot.predicer_project.clone();
    let predicer_runner_script_clone = settings_snapshot.predicer_runner_script.clone();
    tokio::spawn(async move {
        match start_julia_local(
            &julia_exec_clone,
            &predicer_runner_project_clone,
            &predicer_project_clone,
            &predicer_runner_script_clone,
            zmq_port,
        )
        .await
        {
            Ok(status) => {
                if !status.success() {
                    eprintln!("Julia process failed with status: {:?}", status);
                }
            }
            Err(e) => {
                eprintln!("Failed to start Julia process: {:?}", e);
            }
        }
    });
    if let Err(error) = tokio::try_join!(
        flatten_handle(update_time_line_handle),
        flatten_handle(fetch_weather_data_handle),
        flatten_handle(fetch_electricity_price_handle),
        flatten_handle(update_model_data_handle),
        flatten_handle(data_conversion_handle)
    ) {
        let _ = job_store
            .set_job_status(job_id, Arc::new(JobStatus::Failed(error.into())))
            .await;
        return;
    }
    match optimization_handle.await.unwrap() {
        Ok(results) => match results.get("v_flow") {
            Some(result_batch) => {
                let time_stamps = match time_stamps_from_result_batch(&result_batch) {
                    Ok(stamps) => stamps,
                    Err(error) => {
                        let _ = job_store
                            .set_job_status(job_id, Arc::new(JobStatus::Failed(error.into())))
                            .await;
                        return;
                    }
                };
                let control_data =
                    match controls_from_result_batch(&result_batch, control_processes) {
                        Ok(d) => d,
                        Err(error) => {
                            let _ = job_store
                                .set_job_status(job_id, Arc::new(JobStatus::Failed(error.into())))
                                .await;
                            return;
                        }
                    };
                let result_data = OptimizationOutcome::new(time_stamps, control_data);
                let _ = job_store.set_job_status(
                    job_id,
                    Arc::new(JobStatus::Finished(JobOutcome::Optimization(result_data))),
                );
            }
            None => {
                let message = "no v_flow in result batch".to_string();
                let _ = job_store
                    .set_job_status(job_id, Arc::new(JobStatus::Failed(message.into())))
                    .await;
                return;
            }
        },
        Err(error) => {
            let _ = job_store
                .set_job_status(job_id, Arc::new(JobStatus::Failed(error.into())))
                .await;
            return;
        }
    }
}

async fn flatten_handle(handle: JoinHandle<Result<(), String>>) -> Result<(), String> {
    match handle.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error),
        Err(..) => Err("handling failed".to_string()),
    }
}

async fn find_available_port() -> Result<u16, io::Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    Ok(port)
}

fn expected_control_processes(model_data: &BaseInputData) -> Result<Vec<ProcessInfo>, String> {
    let process_names = processes_and_column_prefixes(
        &mut model_data.processes.iter(),
        &input_data_base::find_input_node_names(model_data.nodes.iter()),
    );
    let first_scenario_name = match model_data.scenarios.first() {
        Some(scenario) => scenario.name().clone(),
        None => {
            return Err("no scenarios in model data".to_string());
        }
    };
    Ok(process_names
        .iter()
        .map(|name| ProcessInfo {
            name: name.0.clone(),
            column_name: format!("{}_{}", name.1, first_scenario_name),
        })
        .collect())
}

struct ProcessInfo {
    name: String,
    column_name: String,
}

fn processes_and_column_prefixes<'a>(
    processes: impl Iterator<Item = &'a BaseProcess>,
    input_node_names: &Vec<String>,
) -> Vec<(String, String)> {
    let mut process_names = Vec::new();
    for process in processes {
        for topology in &process.topos {
            if input_node_names.contains(&topology.source) && process.name == topology.sink {
                let prefix = format!("{}_{}_{}", &process.name, &topology.source, &process.name);
                process_names.push((process.name.clone(), prefix));
            }
        }
    }
    process_names
}

fn time_stamps_from_result_batch(batch: &RecordBatch) -> Result<Vec<DateTime<Utc>>, String> {
    match batch.column_by_name("t") {
        Some(time_stamp_column) => match time_stamp_column.data_type() {
            DataType::Timestamp(TimeUnit::Millisecond, Some(time_zone)) => Ok(
                convert_time_stamp_column_to_vec(time_stamp_column, time_zone),
            ),
            unexpected_type => {
                let message = format!(
                    "unexpected data type in 't' column: '{}'",
                    unexpected_type.to_string()
                );
                Err(message)
            }
        },
        None => {
            let message = "no 't' column in v_flow".to_string();
            Err(message)
        }
    }
}

fn convert_time_stamp_column_to_vec(column: &dyn Array, time_zone: &str) -> Vec<DateTime<Utc>> {
    let time_stamp_array = array::as_primitive_array::<TimestampMillisecondType>(column);
    let mut time_stamps_in_vec = Vec::with_capacity(time_stamp_array.len());
    let tz: Tz = time_zone.parse().unwrap();
    for i in 0..time_stamp_array.len() {
        let stamp = time_stamp_array.value_as_datetime_with_tz(i, tz).unwrap();
        time_stamps_in_vec.push(stamp.with_timezone(&Utc));
    }
    time_stamps_in_vec
}

fn controls_from_result_batch(
    batch: &RecordBatch,
    control_processes: Vec<ProcessInfo>,
) -> Result<Vec<ControlSignal>, String> {
    let mut control_data = Vec::with_capacity(control_processes.len());
    for control_process in control_processes {
        match batch.column_by_name(&control_process.column_name) {
            Some(column) => match column.data_type() {
                DataType::Float64 => {
                    let float_array = array::as_primitive_array::<Float64Type>(column);
                    let data_as_vec = float_array
                        .into_iter()
                        .map(|x| x.unwrap())
                        .collect::<Vec<f64>>();
                    control_data.push(ControlSignal {
                        name: control_process.name,
                        signal: data_as_vec,
                    });
                }
                unexpected_type => {
                    let message = format!(
                        "unexpected data type in '{}' column: '{}'",
                        control_process.column_name,
                        unexpected_type.to_string()
                    );
                    return Err(message);
                }
            },
            None => {
                let message = format!("no '{}' column in v_flow", control_process.column_name);
                return Err(message);
            }
        }
    }
    Ok(control_data)
}

async fn start_julia_local(
    julia_exec: &String,
    predicer_runner_project: &String,
    predicer_project: &String,
    predicer_runner_script: &String,
    zmq_port: u16,
) -> Result<ExitStatus, io::Error> {
    let status = Command::new(julia_exec)
        .arg(format!("--project={}", predicer_runner_project))
        .arg(predicer_runner_script)
        .arg(predicer_project)
        .arg(zmq_port.to_string())
        .status()?;
    Ok(status)
}

fn abort_julia_process(reply_socket: Socket) -> Result<(), String> {
    let request_result = reply_socket.recv_string(PREDICER_RECEIVE_FLAGS);
    match request_result {
        Ok(inner_result) => match inner_result {
            Ok(..) => {
                if let Err(error) = reply_socket.send("Abort", PREDICER_SEND_FLAGS) {
                    return Err(format!(
                        "failed to send Abort to Predicer: {}",
                        error.message()
                    ));
                }
            }
            Err(_) => {
                return Err(format!(
                    "received absolute gibberish while aborting Predicer"
                ))
            }
        },
        Err(e) => {
            return Err(format!(
                "failed to receive data while aborting Predicer: {:?}",
                e
            ));
        }
    }
    Ok(())
}

async fn update_time_line_task(
    start_time: TimeStamp,
    time_line: &TimeLineSettings,
    rx: oneshot::Receiver<OptimizationData>,
    tx: oneshot::Sender<OptimizationData>,
) -> Result<(), String> {
    if let Ok(mut optimization_data) = rx.await {
        optimization_data.time_data = Some(time_series::make_time_data(
            start_time,
            time_line.step().to_time_delta(),
            time_line.duration().to_time_delta(),
        ));
        if tx.send(optimization_data).is_err() {
            return Err("update_time_line_task: failed to send output data".to_string());
        }
        Ok(())
    } else {
        Err("update_time_line_taks: input channel failure".to_string())
    }
}

async fn optimization_task(
    rx: oneshot::Receiver<Vec<(String, Vec<u8>)>>,
    zmq_port: u16,
) -> Result<BTreeMap<String, RecordBatch>, String> {
    let zmq_context: Context = Context::new();
    let reply_socket = zmq_context.socket(zmq::REP).unwrap();
    assert!(reply_socket.bind(&format!("tcp://*:{}", zmq_port)).is_ok());
    let mut is_running = true;
    let mut result =
        Err("optimization_task: Predicer process didn't send any results at all".to_string());
    if let Ok(data) = rx.await {
        while is_running {
            let request_result = reply_socket.recv_string(PREDICER_RECEIVE_FLAGS);
            match request_result {
                Ok(inner_result) => match inner_result {
                    Ok(command) => {
                        if command == "Hello" {
                            if let Err(error) = send_predicer_batches(&reply_socket, &data) {
                                return Err(format!(
                                    "optimization_task: failed to send batches {:?}",
                                    error
                                ));
                            }
                        } else if command == "Ready to receive?" {
                            send_acknowledgement(&reply_socket)
                                .expect("failed to confirm readiness for input");
                            result = match receive_predicer_results(&reply_socket) {
                                Ok(optimization_result) => Ok(optimization_result),
                                Err(error) => {
                                    return Err(format!(
                                        "optimization_task: failed to receive data: {:?}",
                                        error
                                    ))
                                }
                            };
                            is_running = false;
                        } else if command == "Failed" {
                            return Err("optimization_task: Predicer process failed".into());
                        } else {
                            return Err(format!(
                                "optimization_task: received unknown command {}",
                                command
                            ));
                        }
                    }
                    Err(_) => {
                        return Err(format!("optimization_task: received absolute gibberish"))
                    }
                },
                Err(e) => {
                    return Err(format!(
                        "optimization_task: failed to receive data: {:?}",
                        e
                    ));
                }
            }
        }
        result
    } else {
        if let Err(abort_error) = abort_julia_process(reply_socket) {
            return Err(format!("optimization_task: {}", abort_error));
        }
        return Err("optimization_task: failed to get data for the optimization task".to_string());
    }
}

async fn data_conversion_task(
    rx: oneshot::Receiver<InputData>,
    tx: oneshot::Sender<Vec<(String, Vec<u8>)>>,
) -> Result<(), String> {
    if let Ok(input_data) = rx.await {
        match arrow_input::create_and_serialize_record_batches(&input_data) {
            Ok(serialized_batches) => {
                if tx.send(serialized_batches).is_err() {
                    return Err(
                        "data_conversion_task: failed to send converted OptimizationData"
                            .to_string(),
                    );
                }
            }
            Err(e) => {
                return Err(format!(
                    "data_conversion_task: failed to serialize model data: {}",
                    e
                ));
            }
        }
        Ok(())
    } else {
        Err("data_conversion_task: input channel closed".to_string())
    }
}

fn send_acknowledgement(socket: &Socket) -> Result<(), Box<dyn Error>> {
    Ok(socket.send("Ok", PREDICER_SEND_FLAGS)?)
}

fn receive_acknowledgement(socket: &Socket) -> Result<(), Box<dyn Error>> {
    match socket.recv_string(PREDICER_RECEIVE_FLAGS)? {
        Ok(request) => {
            if request != "Ok" {
                return Err("unknown reply".into());
            }
            Ok(())
        }
        Err(..) => return Err("failed to decode acknowledgement".into()),
    }
}

fn send_predicer_batches(
    socket: &Socket,
    batches: &Vec<(String, Vec<u8>)>,
) -> Result<(), Box<dyn Error>> {
    for (key, buffer) in batches {
        send_single_predicer_batch(&socket, &key, &buffer)?;
    }
    socket.send("End", PREDICER_SEND_FLAGS)?;
    Ok(())
}

fn send_single_predicer_batch(
    socket: &Socket,
    key: &str,
    buffer: &Vec<u8>,
) -> Result<(), Box<dyn Error>> {
    socket.send(&format!("Receive {}", key), PREDICER_SEND_FLAGS)?;
    receive_acknowledgement(socket)?;
    socket.send(buffer, PREDICER_SEND_FLAGS)?;
    receive_acknowledgement(socket)?;
    Ok(())
}

fn receive_predicer_results(
    socket: &Socket,
) -> Result<BTreeMap<String, RecordBatch>, Box<dyn Error>> {
    let mut data = BTreeMap::<String, RecordBatch>::new();
    loop {
        let request = socket
            .recv_string(PREDICER_RECEIVE_FLAGS)?
            .expect("failed to decode received bytes");
        if request == "End" {
            send_acknowledgement(&socket)?;
            break;
        } else if request.starts_with("Receive ") {
            let key = match request.split_once(' ') {
                Some((_, key)) => key,
                None => return Err(format!("key missing from Receive request").into()),
            };
            send_acknowledgement(socket)?;
            let batch = receive_single_predicer_result(socket)?;
            data.insert(key.to_string(), batch);
        } else {
            return Err(format!("unknown request {}", request).into());
        }
    }
    Ok(data)
}

fn receive_single_predicer_result(socket: &Socket) -> Result<RecordBatch, Box<dyn Error>> {
    let blob = socket.recv_bytes(PREDICER_RECEIVE_FLAGS)?;
    send_acknowledgement(socket)?;
    let reader =
        StreamReader::try_new(blob.as_slice(), None).expect("Failed to construct Arrow reader");
    let mut batches = Vec::<RecordBatch>::new();
    for read_result in reader {
        match read_result {
            Ok(batch) => batches.push(batch),
            Err(error) => return Err(Box::new(error)),
        };
    }
    if batches.len() != 1 {
        return Err(format!("expected a single record batch, got {}", batches.len()).into());
    }
    Ok(batches.pop().expect("batches should have an element"))
}

async fn generate_model_task(
    rx: oneshot::Receiver<OptimizationData>,
    tx: oneshot::Sender<InputData>,
) -> Result<(), String> {
    if let Ok(mut optimization_data) = rx.await {
        let time_line = optimization_data
            .time_data
            .as_ref()
            .ok_or("generate_model_task: didn't receive time data".to_string())?;
        let mut input_data = optimization_data
            .input_data
            .expand_to_time_series(time_line);
        let weather_data = optimization_data.weather_data.take().ok_or(
            "update_model_data_task: weather data missing in optimization data".to_string(),
        )?;
        if let Err(e) = update_outside_node_inflow(&mut input_data, weather_data) {
            return Err(format!(
                "update_model_data_task: failed to update outside node inflow: {}",
                e
            ));
        }
        let electricity_price_data = optimization_data.elec_price_data.take().ok_or(
            "update_model_data_task: electricity price data missing in optimization data"
                .to_string(),
        )?;
        if let Err(e) = update_npe_market_prices(&mut input_data.markets, electricity_price_data) {
            return Err(format!(
                "update_model_data_task: failed to update NPE market prices: {}",
                e
            ));
        }
        input_data.check_ts_data_against_temporals()?;
        if tx.send(input_data).is_err() {
            return Err("update_model_data_task: failed to send generated input data".to_string());
        }
        return Ok(());
    }
    Err("update_model_data_task: input channel closed".to_string())
}

fn update_npe_market_prices(
    markets: &mut BTreeMap<String, Market>,
    electricity_price_data: ElectricityPriceData,
) -> Result<(), String> {
    let electricity_market_name = "npe";
    if let Some(npe_market) = markets.get_mut(electricity_market_name) {
        if let Some(price_data) = &electricity_price_data.price_data {
            npe_market.price.ts_data = price_data.ts_data.clone();
        }
        if let Some(up_price_data) = &electricity_price_data.up_price_data {
            npe_market.up_price.ts_data = up_price_data.ts_data.clone();
        }
        if let Some(down_price_data) = &electricity_price_data.down_price_data {
            npe_market.down_price.ts_data = down_price_data.ts_data.clone();
        }
        return Ok(());
    } else {
        return Err(format!(
            "electricity market '{}' not found in markets",
            electricity_market_name
        ));
    }
}

fn update_outside_node_inflow(
    input_data: &mut InputData,
    weather_data: WeatherData,
) -> Result<(), String> {
    let outside_node_name = "outside";
    let nodes = &mut input_data.nodes;
    let outside_node = nodes
        .get_mut(outside_node_name)
        .ok_or("outside node not found in nodes".to_string())?;
    if !outside_node.is_inflow {
        return Err("outside node is not marked for inflow".to_string());
    }
    outside_node.inflow.ts_data = weather_data;
    Ok(())
}

// Function to create TimeSeriesData from weather values and update the weather_data field in optimization_data
fn update_weather_data(
    time_data: &TimeLine,
    values: &[f64],
    scenarios: &Vec<Scenario>,
) -> Vec<TimeSeries> {
    let paired_series = pair_timeseries_with_values(time_data, values);
    create_time_series_data_with_scenarios(paired_series, scenarios)
}

// Function to create TimeSeriesData
fn create_time_series_data_with_scenarios(
    paired_series: BTreeMap<TimeStamp, f64>,
    scenarios: &Vec<Scenario>,
) -> Vec<TimeSeries> {
    let mut series = Vec::with_capacity(scenarios.len());
    for scenario in scenarios {
        series.push(TimeSeries {
            scenario: scenario.name().clone(),
            series: paired_series.clone(),
        });
    }
    series
}

fn pair_timeseries_with_values(series: &[TimeStamp], values: &[f64]) -> BTreeMap<TimeStamp, f64> {
    series
        .iter()
        .zip(values.iter())
        .map(|(timestamp, &value)| (timestamp.clone(), value))
        .collect()
}

pub async fn fetch_weather_data_task(
    location: &LocationSettings,
    time_line_settings: &TimeLineSettings,
    python_exec: &String,
    weather_fetcher_script: &String,
    rx: oneshot::Receiver<OptimizationData>,
    tx: oneshot::Sender<OptimizationData>,
) -> Result<(), String> {
    if let Ok(mut optimization_data) = rx.await {
        let time_line = optimization_data
            .time_data
            .as_ref()
            .ok_or("fetch_weather_data_task: did not receive time data".to_string())?;
        let start_time = time_line.first().ok_or("empty time line".to_string())?;
        let end_time = time_line.last().ok_or("empty time line".to_string())?;
        match weather_forecast_job::fetch_weather_data(
            &location.place,
            &start_time,
            &end_time,
            &time_line_settings.step().to_time_delta(),
            python_exec,
            weather_fetcher_script,
        ) {
            Ok(weather_values) => {
                let series = optimization_data
                    .time_data
                    .as_ref()
                    .ok_or("series missing from time data".to_string())?;
                if let Err(error) =
                    utilities::check_stamps_match(&weather_values, &time_line, "weather forecast")
                {
                    return Err(error);
                }
                let values =
                    time_series::extract_values_from_pairs_checked(&weather_values, &series)
                        .or_else(|e| Err(format!("fetch_weather_data: {}", e)))?;
                optimization_data.weather_data = Some(update_weather_data(
                    series,
                    &values,
                    &optimization_data.input_data.scenarios,
                ));
            }
            Err(e) => {
                return Err(format!(
                    "fetch_weather_data_task: failed to fetch weather data: {}",
                    e
                ))
            }
        }
        return match tx.send(optimization_data) {
            Ok(..) => Ok(()),
            Err(..) => {
                Err("fetch_weather_data_task: failed to forward optimization data".to_string())
            }
        };
    } else {
        return Err("fetch_weather_data_task: input channel closed".to_string());
    }
}

fn create_and_update_elec_price_data(
    optimization_data: &mut OptimizationData,
    price_series: &BTreeMap<TimeStamp, f64>,
    scenarios: &Vec<Scenario>,
) -> Result<(), String> {
    if let Some(_time_data) = &optimization_data.time_data {
        let original_ts_data = Some(create_modified_price_series_data(
            price_series,
            1.0,
            scenarios,
        ));
        let up_price_ts_data = Some(create_modified_price_series_data(
            price_series,
            1.1,
            scenarios,
        ));
        let down_price_ts_data = Some(create_modified_price_series_data(
            price_series,
            0.9,
            scenarios,
        ));
        let electricity_price_data = ElectricityPriceData {
            price_data: original_ts_data,
            up_price_data: up_price_ts_data,
            down_price_data: down_price_ts_data,
        };
        optimization_data.elec_price_data = Some(electricity_price_data);
    } else {
        return Err(
            "time data is missing in optimization_data; cannot update elec_price_data".to_string(),
        );
    }
    Ok(())
}

fn create_modified_price_series_data(
    original_series: &BTreeMap<TimeStamp, f64>,
    multiplier: f64,
    scenarios: &Vec<Scenario>,
) -> TimeSeriesData {
    let modified_series: BTreeMap<TimeStamp, f64> = original_series
        .iter()
        .map(|(timestamp, price)| (timestamp.clone(), price * multiplier))
        .collect();
    let mut scenario_time_series = Vec::with_capacity(scenarios.len());
    for scenario in scenarios {
        let time_series = TimeSeries {
            scenario: scenario.name().clone(),
            series: modified_series.clone(),
        };
        scenario_time_series.push(time_series);
    }
    TimeSeriesData {
        ts_data: scenario_time_series,
    }
}

async fn fetch_electricity_price_task(
    location: &LocationSettings,
    scenarios: &Vec<Scenario>,
    rx: oneshot::Receiver<OptimizationData>,
    tx: oneshot::Sender<OptimizationData>,
) -> Result<(), String> {
    if let Ok(mut optimization_data) = rx.await {
        let time_line = optimization_data
            .time_data
            .as_ref()
            .ok_or("fetch_electricity_price_task: didn't receive time data")?;
        let start_time = time_line.first().ok_or("time line is empty")?;
        let end_time = (*time_line.last().ok_or("time line is empty")? - TimeDelta::hours(1))
            .duration_trunc(TimeDelta::hours(1))
            .unwrap();
        match electricity_price_job::fetch_electricity_prices(
            &location.country,
            start_time,
            &end_time,
        )
        .await
        {
            Ok(prices) => {
                let prices = fit_prices_to_time_line(&prices, time_line)?;
                if let Err(error) =
                    utilities::check_stamps_match(&prices, time_line, "electricity prices")
                {
                    return Err(error);
                }
                let prices = prices.iter().map(|pair| (pair.0.clone(), pair.1)).collect();
                create_and_update_elec_price_data(&mut optimization_data, &prices, scenarios)?;
                if tx.send(optimization_data).is_err() {
                    return Err("fetch_electricity_price_task: failed to send updated optimization data in electricity prices".to_string());
                }
            }
            Err(e) => {
                return Err(format!(
                    "fetch_electricity_price_task: failed to fetch electricity price data: {}",
                    e
                ));
            }
        }
        return Ok(());
    } else {
        return Err("fetch_electricity_price_task: input channel closed".to_string());
    }
}

fn fit_prices_to_time_line(
    prices: &Vec<(TimeStamp, f64)>,
    time_line: &TimeLine,
) -> Result<Vec<(TimeStamp, f64)>, String> {
    let mut fitted = Vec::with_capacity(time_line.len());
    if prices.is_empty() {
        return Err("electricity prices should have at least one time stamp".into());
    }
    if prices[0].0 != time_line[0] {
        return Err("first electricity price time stamp mismatches with time line start".into());
    }
    let mut price_iter = prices.iter();
    let mut current_price = price_iter.next().unwrap().1;
    let (mut next_time_stamp, mut next_price) = price_iter.next().unwrap();
    for stamp in time_line {
        if *stamp == next_time_stamp {
            current_price = next_price;
            (next_time_stamp, next_price) = match price_iter.next() {
                Some((stamp, price)) => (*stamp, *price),
                None => (
                    *time_line.last().unwrap() + TimeDelta::hours(1),
                    current_price,
                ),
            };
        }
        fitted.push((stamp.clone(), current_price));
    }
    Ok(fitted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int8Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::net::TcpListener as SyncTcpListener;
    use std::thread;

    fn find_available_port_sync() -> Result<u16, io::Error> {
        let listener = SyncTcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        Ok(port)
    }
    fn make_serialized_record_batch(data: Vec<i8>) -> Vec<u8> {
        let array = Int8Array::from(data);
        let schema = Schema::new(vec![Field::new("number", DataType::Int8, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
        arrow_input::serialize_batch_to_buffer(&batch).expect("failed to serialize batch")
    }
    mod send_predicer_batches {
        use super::*;
        #[test]
        fn sends_end_message_last() -> Result<(), Box<dyn Error>> {
            let zmq_context: Context = Context::new();
            let request_socket = zmq_context
                .socket(zmq::REQ)
                .expect("failed to create request socket");
            let port: u16 = find_available_port_sync()?;
            let join_handle = thread::spawn(move || {
                let reply_socket = zmq_context
                    .socket(zmq::REP)
                    .expect("failed to create reply socket");
                reply_socket
                    .bind(&format!("tcp://*:{}", port))
                    .expect("failed to bind reply socket");
                assert_eq!(
                    reply_socket
                        .recv_string(0)
                        .expect("failed to receive hello")
                        .expect("failed to decode hello"),
                    "Hello"
                );
                send_predicer_batches(&reply_socket, &Vec::<(String, Vec<u8>)>::new())
                    .expect("failed to send batches");
            });
            request_socket
                .connect(&format!("tcp://127.0.0.1:{}", port))
                .expect("failed to connect request socket");
            request_socket.send("Hello", PREDICER_SEND_FLAGS)?;
            let end_message = request_socket
                .recv_string(PREDICER_RECEIVE_FLAGS)?
                .expect("failed to decode key");
            assert_eq!(end_message, "End");
            join_handle
                .join()
                .expect("failed to join the sender thread");
            Ok(())
        }
    }
    mod send_single_predicer_batch {
        use super::*;
        #[test]
        fn sending_single_record_batch_works() -> Result<(), Box<dyn Error>> {
            let zmq_context: Context = Context::new();
            let request_socket = zmq_context
                .socket(zmq::REQ)
                .expect("failed to create request socket");
            let port: u16 = find_available_port_sync()?;
            let join_handle = thread::spawn(move || {
                let reply_socket = zmq_context
                    .socket(zmq::REP)
                    .expect("failed to create reply socket");
                reply_socket
                    .bind(&format!("tcp://*:{}", port))
                    .expect("failed to bind reply socket");
                assert_eq!(
                    reply_socket
                        .recv_string(0)
                        .expect("failed to receive hello")
                        .expect("failed to decode hello"),
                    "Hello"
                );
                send_single_predicer_batch(&reply_socket, "my key", &vec![23])
                    .expect("failed to send batch");
            });
            request_socket
                .connect(&format!("tcp://127.0.0.1:{}", port))
                .expect("failed to connect request socket");
            request_socket.send("Hello", PREDICER_SEND_FLAGS)?;
            let key = request_socket
                .recv_string(PREDICER_RECEIVE_FLAGS)?
                .expect("failed to decode key");
            assert_eq!(key, "Receive my key");
            send_acknowledgement(&request_socket).expect("failed to send acknowledgement");
            let buffer = request_socket
                .recv_bytes(PREDICER_RECEIVE_FLAGS)
                .expect("failed to receive bytes");
            assert_eq!(buffer, vec![23]);
            send_acknowledgement(&request_socket).expect("failed to send acknowledgement");
            join_handle
                .join()
                .expect("failed to join the sender thread");
            Ok(())
        }
    }
    mod receive_predicer_results {
        use super::*;
        #[test]
        fn end_message_stops_receiving() -> Result<(), Box<dyn Error>> {
            let zmq_context: Context = Context::new();
            let request_socket = zmq_context
                .socket(zmq::REQ)
                .expect("failed to create request socket");
            let port: u16 = find_available_port_sync()?;
            let join_handle = thread::spawn(move || {
                let reply_socket = zmq_context
                    .socket(zmq::REP)
                    .expect("failed to create reply socket");
                reply_socket
                    .bind(&format!("tcp://*:{}", port))
                    .expect("failed to bind reply socket");
                let data_store =
                    receive_predicer_results(&reply_socket).expect("failed to receive batches");
                data_store
            });
            request_socket
                .connect(&format!("tcp://127.0.0.1:{}", port))
                .expect("failed to connect request socket");
            request_socket
                .send("End", PREDICER_SEND_FLAGS)
                .expect("failed to hail replier");
            let data_store = join_handle.join().expect("failed to join replier thread");
            assert!(data_store.is_empty());
            Ok(())
        }
        #[test]
        fn receive_single_record_batch_works() -> Result<(), Box<dyn Error>> {
            let zmq_context: Context = Context::new();
            let request_socket = zmq_context
                .socket(zmq::REQ)
                .expect("failed to create request socket");
            let port: u16 = find_available_port_sync()?;
            let join_handle = thread::spawn(move || {
                let reply_socket = zmq_context
                    .socket(zmq::REP)
                    .expect("failed to create reply socket");
                reply_socket
                    .bind(&format!("tcp://*:{}", port))
                    .expect("failed to bind reply socket");
                let data_store =
                    receive_predicer_results(&reply_socket).expect("failed to receive batches");
                data_store
            });
            request_socket
                .connect(&format!("tcp://127.0.0.1:{}", port))
                .expect("failed to connect request socket");
            request_socket
                .send("Receive my key", PREDICER_SEND_FLAGS)
                .expect("failed to hail replier");
            let serialized_batch = make_serialized_record_batch(vec![-1, 0, 1]);
            let ok = request_socket
                .recv_string(PREDICER_RECEIVE_FLAGS)?
                .expect("failed to decode  acknowledgement");
            assert_eq!(ok, "Ok");
            request_socket
                .send(serialized_batch, PREDICER_SEND_FLAGS)
                .expect("failed to send record batch");
            let ok = request_socket
                .recv_string(PREDICER_RECEIVE_FLAGS)?
                .expect("failed to decode acknowledgement");
            assert_eq!(ok, "Ok");
            request_socket
                .send("End", PREDICER_SEND_FLAGS)
                .expect("failed to send end message");
            let data_store = join_handle.join().expect("failed to join replier thread");
            assert!(!data_store.is_empty());
            let record_batch = match data_store.get("my key") {
                Some(batch) => batch,
                None => return Err("expected key does not exist".into()),
            };
            assert_eq!(record_batch.schema().fields().len(), 1);
            assert_eq!(record_batch.schema().fields()[0].name(), "number");
            let number_column = match record_batch.column_by_name("number") {
                Some(column) => column,
                None => return Err("numbers not found".into()),
            };
            assert_eq!(number_column.len(), 3);
            assert_eq!(
                number_column
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .expect("failed to downcast array")
                    .values(),
                &[-1, 0, 1]
            );
            Ok(())
        }
    }
    mod receive_single_predicer_result {
        use super::*;
        #[test]
        fn receiving_record_batch_works() -> Result<(), Box<dyn Error>> {
            let zmq_context: Context = Context::new();
            let request_socket = zmq_context
                .socket(zmq::REQ)
                .expect("failed to create request socket");
            let port: u16 = find_available_port_sync()?;
            let join_handle = thread::spawn(move || {
                let reply_socket = zmq_context
                    .socket(zmq::REP)
                    .expect("failed to create reply socket");
                reply_socket
                    .bind(&format!("tcp://*:{}", port))
                    .expect("failed to bind reply socket");
                receive_single_predicer_result(&reply_socket).expect("error when receiving batch")
            });
            request_socket
                .connect(&format!("tcp://127.0.0.1:{}", port))
                .expect("failed to connect request socket");
            let serialized_batch = make_serialized_record_batch(vec![-1, 0, 1]);
            request_socket.send(serialized_batch, PREDICER_SEND_FLAGS)?;
            let acknowledgement = request_socket
                .recv_string(PREDICER_RECEIVE_FLAGS)?
                .expect("failed to decode acknowledgement");
            assert_eq!(acknowledgement, "Ok");
            let record_batch = join_handle.join().expect("failed to join replier thread");
            assert_eq!(record_batch.schema().fields().len(), 1);
            assert_eq!(record_batch.schema().fields()[0].name(), "number");
            let number_column = match record_batch.column_by_name("number") {
                Some(column) => column,
                None => return Err("numbers not found".into()),
            };
            assert_eq!(number_column.len(), 3);
            assert_eq!(
                number_column
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .expect("failed to downcast array")
                    .values(),
                &[-1, 0, 1]
            );
            Ok(())
        }
    }
    mod processes_and_column_prefixes {
        use super::*;
        use crate::input_data_base::{BaseTopology, Conversion};
        #[test]
        fn column_prefix_gets_generated() {
            let mut process = BaseProcess::new("control_process".to_string(), Conversion::Unit);
            process.topos.push(BaseTopology::new(
                "input_node".to_string(),
                "control_process".to_string(),
            ));
            let processes = vec![process];
            let input_node_names = vec!["input_node".to_string()];
            let process_names = processes_and_column_prefixes(processes.iter(), &input_node_names);
            assert_eq!(process_names.len(), 1);
            assert_eq!(process_names[0].0, "control_process");
            assert_eq!(
                process_names[0].1,
                "control_process_input_node_control_process"
            );
        }
    }
    mod fit_prices_to_time_line {
        use chrono::TimeZone;

        use super::*;
        #[test]
        fn test_last_price_is_extended_until_end_of_time_line() {
            let prices = [
                (Utc.with_ymd_and_hms(2024, 12, 4, 11, 0, 0), 2.2),
                (Utc.with_ymd_and_hms(2024, 12, 4, 12, 0, 0), 2.3),
            ]
            .iter()
            .map(|(s, p)| (s.unwrap(), *p))
            .collect();
            let time_line = [
                Utc.with_ymd_and_hms(2024, 12, 4, 11, 0, 0),
                Utc.with_ymd_and_hms(2024, 12, 4, 11, 30, 0),
                Utc.with_ymd_and_hms(2024, 12, 4, 12, 0, 0),
                Utc.with_ymd_and_hms(2024, 12, 4, 12, 30, 0),
                Utc.with_ymd_and_hms(2024, 12, 4, 13, 0, 0),
                Utc.with_ymd_and_hms(2024, 12, 4, 13, 30, 0),
            ]
            .iter()
            .map(|s| s.unwrap())
            .collect();
            let fitted_prices =
                fit_prices_to_time_line(&prices, &time_line).expect("fitting should not fail");
            assert_eq!(fitted_prices.len(), time_line.len());
            let expected_prices: Vec<(TimeStamp, f64)> = [
                (Utc.with_ymd_and_hms(2024, 12, 4, 11, 0, 0), 2.2),
                (Utc.with_ymd_and_hms(2024, 12, 4, 11, 30, 0), 2.2),
                (Utc.with_ymd_and_hms(2024, 12, 4, 12, 0, 0), 2.3),
                (Utc.with_ymd_and_hms(2024, 12, 4, 12, 30, 0), 2.3),
                (Utc.with_ymd_and_hms(2024, 12, 4, 13, 0, 0), 2.3),
                (Utc.with_ymd_and_hms(2024, 12, 4, 13, 30, 0), 2.3),
            ]
            .iter()
            .map(|(s, p)| (s.unwrap(), *p))
            .collect();
            assert_eq!(fitted_prices, expected_prices);
        }
    }
}