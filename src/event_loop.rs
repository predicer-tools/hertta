mod arrow_input;
mod time_series;

use crate::input_data::{self, InputData, Market, TimeSeries, TimeSeriesData};
use crate::input_data_base::{self, BaseInputData, BaseProcess};
use crate::model::Model;
use crate::scenarios::Scenario;
use crate::settings::{LocationSettings, Settings};
use crate::time_line_settings::TimeLineSettings;
use crate::{TimeLine, TimeStamp};
use arrow::array::timezone::Tz;
use arrow::array::types::{Float64Type, TimestampMillisecondType};
use arrow::array::{self, Array, RecordBatch};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::ipc::reader::StreamReader;
use chrono::round::DurationRound;
use chrono::{DateTime, NaiveDateTime, TimeDelta, Utc};
use juniper::GraphQLObject;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::error::Error;
use std::io;
use std::iter;
use std::process::{Command, ExitStatus};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use zmq::{Context, Socket};

const PREDICER_SEND_FLAGS: i32 = 0;
const PREDICER_RECEIVE_FLAGS: i32 = 0;

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

#[derive(GraphQLObject)]
pub struct ControlSignal {
    pub name: String,
    pub signal: Vec<f64>,
}

#[derive(GraphQLObject)]
pub struct ResultData {
    pub job_id: i32,
    pub t: Vec<DateTime<Utc>>,
    pub controls: Vec<ControlSignal>,
}

pub enum OptimizationState {
    Idle,
    InProgress(i32),
    Finished(i32),
    Error(i32, String),
}

pub enum OptimizationTask {
    Start,
    Stop,
}

pub async fn event_loop(
    vanilla_settings: Arc<Mutex<Settings>>,
    model: Arc<Mutex<Model>>,
    result_data: Arc<Mutex<Option<ResultData>>>,
    mut rx_input: mpsc::Receiver<(i32, OptimizationTask)>,
    tx_state: watch::Sender<OptimizationState>,
) {
    while let Some((job_id, task)) = rx_input.recv().await {
        if let OptimizationTask::Stop = task {
            break;
        }
        tx_state
            .send(OptimizationState::InProgress(job_id))
            .unwrap();
        let settings_snapshot = vanilla_settings.lock().unwrap().clone();
        let model_snapshot = model.lock().unwrap().clone();
        let optimization_data = OptimizationData::with_input_data(model_snapshot.input_data);
        let start_time: TimeStamp = Utc::now().duration_trunc(TimeDelta::hours(1)).unwrap();
        let control_processes = match expected_control_processes(&optimization_data.input_data) {
            Ok(names) => names,
            Err(error) => {
                tx_state
                    .send(OptimizationState::Error(job_id, error))
                    .unwrap();
                continue;
            }
        };
        let mut zmq_port = settings_snapshot.predicer_port;
        if zmq_port == 0 {
            zmq_port = match find_available_port().await {
                Ok(port) => port,
                Err(..) => {
                    tx_state
                        .send(OptimizationState::Error(
                            job_id,
                            "failed to find available ZMQ port".to_string(),
                        ))
                        .unwrap();
                    continue;
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
                tx_state
                    .send(OptimizationState::Error(
                        job_id,
                        "cannot fetch weather data: no location set".to_string(),
                    ))
                    .unwrap();
                continue;
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
            fetch_electricity_price_task(&location_clone, &scenarios_clone, rx_elec, tx_update)
                .await
        });
        let update_model_data_handle =
            tokio::spawn(async move { generate_model_task(rx_update, tx_batches).await });
        let data_conversion_handle =
            tokio::spawn(async move { data_conversion_task(rx_batches, tx_optimization).await });
        let optimization_handle =
            tokio::spawn(async move { optimization_task(rx_optimization, zmq_port).await });
        if tx_time_line.send(optimization_data).is_err() {
            tx_state
                .send(OptimizationState::Error(
                    job_id,
                    "failed to send initial data to optimization pipeline".to_string(),
                ))
                .unwrap();
            continue;
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
            tx_state
                .send(OptimizationState::Error(job_id, error))
                .unwrap();
            continue;
        }
        match optimization_handle.await.unwrap() {
            Ok(results) => match results.get("v_flow") {
                Some(result_batch) => {
                    let time_stamps = match time_stamps_from_result_batch(&result_batch) {
                        Ok(stamps) => stamps,
                        Err(error) => {
                            tx_state
                                .send(OptimizationState::Error(job_id, error))
                                .unwrap();
                            continue;
                        }
                    };
                    let control_data =
                        match controls_from_result_batch(&result_batch, control_processes) {
                            Ok(d) => d,
                            Err(error) => {
                                tx_state
                                    .send(OptimizationState::Error(job_id, error))
                                    .unwrap();
                                continue;
                            }
                        };
                    result_data.lock().as_mut().unwrap().replace(ResultData {
                        job_id: job_id,
                        t: time_stamps,
                        controls: control_data,
                    });
                    tx_state.send(OptimizationState::Finished(job_id)).unwrap();
                }
                None => {
                    let message = "no v_flow in result batch".to_string();
                    tx_state
                        .send(OptimizationState::Error(job_id, message))
                        .unwrap();
                    continue;
                }
            },
            Err(error) => {
                tx_state
                    .send(OptimizationState::Error(job_id, error))
                    .unwrap();
                continue;
            }
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

async fn find_available_port() -> Result<u16, io::Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    Ok(port)
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

fn check_stamps_match(
    checkable_series: &Vec<(TimeStamp, f64)>,
    time_line: &TimeLine,
    context: &str,
) -> Result<(), String> {
    if checkable_series.len() != time_line.len() {
        return Err(format!("{} length doesn't match time line", context));
    }
    for (forecast_pair, time_stamp) in iter::zip(checkable_series, time_line) {
        if forecast_pair.0 != *time_stamp {
            return Err(format!("{} time stamp doesn't match time line", context));
        }
    }
    Ok(())
}

async fn fetch_weather_data_task(
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
        match fetch_weather_data(
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
                    check_stamps_match(&weather_values, &time_line, "weather forecast")
                {
                    return Err(error);
                }
                let values =
                    time_series::extract_values_from_pairs_checked(&weather_values, &series)
                        .or_else(|e| Err(format!("fetch_weather_data: {}", e)))?;
                optimization_data.weather_data = Some(update_weather_data(series, &values));
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
fn update_weather_data(time_data: &TimeLine, values: &[f64]) -> Vec<TimeSeries> {
    let paired_series = pair_timeseries_with_values(time_data, values);
    create_time_series_data_with_scenarios(paired_series)
}

// Function to create TimeSeriesData with scenarios "s1" and "s2" using paired series
fn create_time_series_data_with_scenarios(
    paired_series: BTreeMap<TimeStamp, f64>,
) -> Vec<TimeSeries> {
    let time_series_s1 = input_data::TimeSeries {
        scenario: "s1".to_string(),
        series: paired_series.clone(),
    };
    let time_series_s2 = input_data::TimeSeries {
        scenario: "s2".to_string(),
        series: paired_series,
    };
    return vec![time_series_s1, time_series_s2];
}

fn pair_timeseries_with_values(series: &[TimeStamp], values: &[f64]) -> BTreeMap<TimeStamp, f64> {
    series
        .iter()
        .zip(values.iter())
        .map(|(timestamp, &value)| (timestamp.clone(), value))
        .collect()
}

#[derive(Deserialize)]
pub struct WeatherDataResponse {
    pub place: String,
    pub weather_values: Vec<f64>,
}

fn parse_weather_fetcher_output(output: &str) -> Result<Vec<(TimeStamp, f64)>, String> {
    let parsed_json = serde_json::from_str(output)
        .or_else(|error| Err(format!("failed to parse output: {}", error)))?;
    if let Value::Array(time_series) = parsed_json {
        let mut forecast = Vec::with_capacity(time_series.len());
        for row in time_series {
            if let Value::Array(pair) = row {
                if pair.len() != 2 {
                    return Err("failed to parse time series".to_string());
                }
                if let Value::String(ref stamp) = pair[0] {
                    let time_stamp = match NaiveDateTime::parse_from_str(stamp, "%Y-%m-%dT%H:%M:%S")
                    {
                        Ok(date_time) => date_time.and_utc(),
                        Err(..) => {
                            return Err(format!("failed to parse stamp from string {}", stamp))
                        }
                    };
                    if let Value::Number(ref value) = pair[1] {
                        if let Some(temperature) = value.as_f64() {
                            forecast.push((time_stamp, temperature));
                        } else {
                            return Err("failed to convert temperature to float".to_string());
                        }
                    } else {
                        return Err("failed to parse temperature".to_string());
                    }
                } else {
                    return Err("failed to parse time stamp".to_string());
                }
            } else {
                return Err("failed to parse data pair in time series".to_string());
            }
        }
        Ok(forecast)
    } else {
        Err("failed to parse array from output".to_string())
    }
}

fn fetch_weather_data(
    place: &String,
    start_time: &TimeStamp,
    end_time: &TimeStamp,
    step: &TimeDelta,
    python_exec: &String,
    weather_data_script: &String,
) -> Result<Vec<(TimeStamp, f64)>, String> {
    let format_string = "%Y-%m-%dT%H:%M:%S";
    let mut command = Command::new(python_exec);
    command
        .arg(weather_data_script)
        .arg(start_time.format(&format_string).to_string())
        .arg(end_time.format(&format_string).to_string())
        .arg(format!("{}", step.num_minutes()))
        .arg(place);
    let output = match command.output() {
        Ok(bytes) => bytes,
        Err(error) => return Err(format!("Python failed: {}", error)),
    };
    if !output.status.success() {
        return Err("weather fetching returned non-zero exit status".into());
    }
    let output = match String::from_utf8(output.stdout) {
        Ok(json_out) => json_out,
        Err(..) => return Err("non-utf-8 characters in output".to_string()),
    };
    Ok(parse_weather_fetcher_output(&output)?)
}

async fn fetch_electricity_prices(
    start_time: String,
    end_time: String,
    country: &String,
) -> Result<Vec<(TimeStamp, f64)>, Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::new();
    let url = format!(
        "https://dashboard.elering.ee/api/nps/price?start={}&end={}",
        start_time, end_time
    );
    let response = client
        .get(&url)
        .header("accept", "application/json")
        .send()
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    let response_text = response
        .text()
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    Ok(parse_elering_response(
        &response_text,
        &as_elering_country(country)?,
    )?)
}

fn parse_elering_response(
    response_text: &str,
    elering_country: &str,
) -> Result<Vec<(TimeStamp, f64)>, String> {
    let response = serde_json::from_str(response_text)
        .or_else(|error| Err(format!("failed to parse response: {}", error)))?;
    let response_object = match response {
        Value::Object(object) => object,
        _ => return Err("unexpected response content".to_string()),
    };
    let success = match response_object
        .get("success")
        .ok_or("'success' field missing in response".to_string())?
    {
        Value::Bool(success) => success,
        _ => return Err("unexpected type for 'success' in response".to_string()),
    };
    if !success {
        return Err("electricity price query unsuccessful".to_string());
    }
    let data = match response_object
        .get("data")
        .ok_or("'data' field missing in response".to_string())?
    {
        Value::Object(data) => data,
        _ => return Err("unexpected type for 'data' in response".to_string()),
    };
    let price_data = match data.get(elering_country).ok_or(format!(
        "requested country '{}' not found in response",
        elering_country
    ))? {
        Value::Array(price_data) => price_data,
        _ => return Err("unexpected type for country data in response".to_string()),
    };
    let mut pairs = Vec::with_capacity(price_data.len());
    for element in price_data {
        pairs.push(elering_time_stamp_pair(element)?);
    }
    Ok(pairs)
}

fn as_elering_country(country: &str) -> Result<&str, String> {
    if country == "Estonia" {
        return Ok("ee");
    }
    if country == "Finland" {
        return Ok("fi");
    }
    if country == "Lithuania" {
        return Ok("lt");
    }
    if country == "Latvia" {
        return Ok("lv");
    }
    Err("unknown or unsupported country".to_string())
}

fn elering_time_stamp_pair(time_stamp_entry: &Value) -> Result<(TimeStamp, f64), String> {
    let stamp_object = match time_stamp_entry {
        Value::Object(stamp_object) => stamp_object,
        _ => return Err("unexpected type for price entry in response".to_string()),
    };
    let time_stamp = match stamp_object
        .get("timestamp")
        .ok_or("'timestamp' field missing in price entry")?
    {
        Value::Number(t) => t
            .as_i64()
            .ok_or("failed to parse time stamp as integer".to_string())?,
        _ => return Err("unexpected type for time stamp".to_string()),
    };
    let time_stamp = DateTime::from_timestamp(time_stamp, 0)
        .ok_or("failed to convert time stamp to date time".to_string())?;
    let price = match stamp_object
        .get("price")
        .ok_or("'price' field missing in price entry")?
    {
        Value::Number(x) => x
            .as_f64()
            .ok_or("failed to parse price as float".to_string())?,
        _ => return Err("unexpected type for price".to_string()),
    };
    Ok((time_stamp, price))
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
        let format_string = "%Y-%m-%dT%H:%M:%S%.3fZ";
        let start_time = time_line.first().ok_or("time line is empty")?;
        let start_time_str = start_time.format(&format_string).to_string();
        let end_time = (*time_line.last().ok_or("time line is empty")? - TimeDelta::hours(1))
            .duration_trunc(TimeDelta::hours(1))
            .unwrap();
        let end_time_str = end_time.format(&format_string).to_string();
        match fetch_electricity_prices(start_time_str, end_time_str, &location.country).await {
            Ok(prices) => {
                let prices = fit_prices_to_time_line(&prices, time_line)?;
                if let Err(error) = check_stamps_match(&prices, time_line, "electricity prices") {
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
        use crate::input_data_base::BaseTopology;
        #[test]
        fn column_prefix_gets_generated() {
            let processes = vec![BaseProcess {
                name: "control_process".to_string(),
                topos: vec![BaseTopology {
                    source: "input_node".to_string(),
                    sink: "control_process".to_string(),
                    ..BaseTopology::default()
                }],
                ..BaseProcess::default()
            }];
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
    mod parse_weather_fetcher_output {
        use super::*;
        use chrono::TimeZone;
        #[test]
        fn parses_data_correctly() {
            let fetcher_output = r#"
                [
                        ["2024-11-08T11:00:00", 6.5],
                        ["2024-11-08T12:00:00", 6.6],
                        ["2024-11-08T13:00:00", 6.2],
                        ["2024-11-08T14:00:00", 5.9],
                        ["2024-11-08T15:00:00", 6.1],
                        ["2024-11-08T16:00:00", 6.3],
                        ["2024-11-08T17:00:00", 6.4],
                        ["2024-11-08T18:00:00", 6.7]
                ]"#;
            let weather_series = parse_weather_fetcher_output(fetcher_output)
                .expect("parsing output should not fail");
            let expected_time_stamps = [
                Utc.with_ymd_and_hms(2024, 11, 8, 11, 0, 0)
                    .single()
                    .unwrap(),
                Utc.with_ymd_and_hms(2024, 11, 8, 12, 0, 0)
                    .single()
                    .unwrap(),
                Utc.with_ymd_and_hms(2024, 11, 8, 13, 0, 0)
                    .single()
                    .unwrap(),
                Utc.with_ymd_and_hms(2024, 11, 8, 14, 0, 0)
                    .single()
                    .unwrap(),
                Utc.with_ymd_and_hms(2024, 11, 8, 15, 0, 0)
                    .single()
                    .unwrap(),
                Utc.with_ymd_and_hms(2024, 11, 8, 16, 0, 0)
                    .single()
                    .unwrap(),
                Utc.with_ymd_and_hms(2024, 11, 8, 17, 0, 0)
                    .single()
                    .unwrap(),
                Utc.with_ymd_and_hms(2024, 11, 8, 18, 0, 0)
                    .single()
                    .unwrap(),
            ];
            let expected_temperatures = [6.5, 6.6, 6.2, 5.9, 6.1, 6.3, 6.4, 6.7];
            assert_eq!(weather_series.len(), expected_temperatures.len());
            for (i, pair) in weather_series.iter().enumerate() {
                assert_eq!(pair.0, expected_time_stamps[i]);
                assert_eq!(pair.1, expected_temperatures[i]);
            }
        }
    }
    mod as_elering_country {
        use super::*;
        #[test]
        fn gives_correct_country_codes() -> Result<(), String> {
            assert_eq!(as_elering_country("Estonia")?, "ee");
            assert_eq!(as_elering_country("Finland")?, "fi");
            assert_eq!(as_elering_country("Lithuania")?, "lt");
            assert_eq!(as_elering_country("Latvia")?, "lv");
            Ok(())
        }
        #[test]
        fn fails_with_unknown_country() {
            if let Ok(..) = as_elering_country("Mordor") {
                panic!("call should have failed");
            }
        }
    }
    mod elering_time_stamp_pair {
        use super::*;
        use chrono::TimeZone;
        #[test]
        fn parses_time_stamp_entry() {
            let entry_json = "{\"timestamp\":1731927600,\"price\":85.0600}";
            let entry = serde_json::from_str(entry_json).expect("entry JSON should be parseable");
            let pair =
                elering_time_stamp_pair(&entry).expect("constructing a pair should not fail");
            let expected_date_time = Utc.with_ymd_and_hms(2024, 11, 18, 11, 00, 00).unwrap();
            assert_eq!(pair, (expected_date_time, 85.06));
        }
    }
    mod parse_elering_response {
        use super::*;
        use chrono::TimeZone;
        #[test]
        fn parses_response_succesfully() {
            let response_json = r#"
{
    "success":true,
    "data": {
        "fi":[{"timestamp":1731927600,"price":70.0500}]
    }
}"#;
            let time_series =
                parse_elering_response(response_json, "fi").expect("parsing should succeed");
            let expected_date_time = Utc.with_ymd_and_hms(2024, 11, 18, 11, 00, 00).unwrap();
            let expected_time_series = vec![(expected_date_time, 70.05)];
            assert_eq!(time_series, expected_time_series);
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
