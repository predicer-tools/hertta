use crate::arrow_input;
use crate::input_data;
use crate::input_data::{InputData, OptimizationData, Process};
use crate::settings::Settings;
use crate::utilities;
use arrow::array::timezone::Tz;
use arrow::array::types::{Float64Type, TimestampMillisecondType};
use arrow::array::{self, Array, RecordBatch};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::ipc::reader::StreamReader;
use chrono::{DateTime, FixedOffset, Utc};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::io;
use std::process::{Command, ExitStatus};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use zmq::{Context, Socket};

const PREDICER_SEND_FLAGS: i32 = 0;
const PREDICER_RECEIVE_FLAGS: i32 = 0;

#[derive(Serialize)]
pub struct ResultData {
    t: Vec<DateTime<Tz>>,
    controls: BTreeMap<String, Vec<f64>>,
}

pub enum OptimizationState {
    Idle,
    InProgress(usize),
    Finished(usize, ResultData),
    Error(usize, String),
}

pub async fn event_loop(
    settings: Arc<Settings>,
    mut rx_input: mpsc::Receiver<(usize, OptimizationData)>,
    tx_state: watch::Sender<OptimizationState>,
) {
    while let Some((job_id, data)) = rx_input.recv().await {
        tx_state
            .send(OptimizationState::InProgress(job_id))
            .unwrap();
        let control_processes = match expected_control_processes(&data.model_data) {
            Ok(names) => names,
            Err(error) => {
                tx_state
                    .send(OptimizationState::Error(job_id, error))
                    .unwrap();
                continue;
            }
        };
        let mut zmq_port = settings.predicer_port;
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
        let (tx_weather, rx_weather) = oneshot::channel::<OptimizationData>();
        let (tx_elec, rx_elec) = oneshot::channel::<OptimizationData>();
        let (tx_update, rx_update) = oneshot::channel::<OptimizationData>();
        let (tx_batches, rx_batches) = oneshot::channel::<OptimizationData>();
        let (tx_optimization, rx_optimization) = oneshot::channel::<OptimizationData>();
        let fetch_weather_data_handle =
            tokio::spawn(async move { fetch_weather_data_task(rx_weather, tx_elec).await });
        let fetch_electricity_price_handle =
            tokio::spawn(async move { fetch_electricity_price_task(rx_elec, tx_update).await });
        let update_model_data_handle =
            tokio::spawn(async move { update_model_data_task(rx_update, tx_batches).await });
        let data_conversion_handle =
            tokio::spawn(async move { data_conversion_task(rx_batches, tx_optimization).await });
        let optimization_handle =
            tokio::spawn(async move { optimization_task(rx_optimization, zmq_port).await });
        if tx_weather.send(data).is_err() {
            tx_state
                .send(OptimizationState::Error(
                    job_id,
                    "failed to send initial data to optimization pipeline".to_string(),
                ))
                .unwrap();
            continue;
        }
        let settings_clone = Arc::clone(&settings);
        tokio::spawn(async move {
            match start_julia_local(
                &settings_clone.julia_exec,
                &settings_clone.predicer_runner_project,
                &settings_clone.predicer_project,
                &settings_clone.predicer_runner_script,
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
                    tx_state
                        .send(OptimizationState::Finished(
                            job_id,
                            ResultData {
                                t: time_stamps,
                                controls: control_data,
                            },
                        ))
                        .unwrap();
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
    processes: impl Iterator<Item = &'a Process>,
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

fn expected_control_processes(model_data: &Option<InputData>) -> Result<Vec<ProcessInfo>, String> {
    match model_data {
        Some(ref model_data) => Ok({
            let process_names = processes_and_column_prefixes(
                &mut model_data.processes.values(),
                &input_data::find_input_node_names(model_data.nodes.values()),
            );
            let first_scenario_name = match model_data.scenarios.keys().next() {
                Some(name) => name.clone(),
                None => {
                    return Err("no scenarios in model data".to_string());
                }
            };
            process_names
                .iter()
                .map(|name| ProcessInfo {
                    name: name.0.clone(),
                    column_name: format!("{}_{}", name.1, first_scenario_name),
                })
                .collect()
        }),
        None => Err("no model data in optimization data".to_string()),
    }
}

fn time_stamps_from_result_batch(batch: &RecordBatch) -> Result<Vec<DateTime<Tz>>, String> {
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

fn convert_time_stamp_column_to_vec(column: &dyn Array, time_zone: &str) -> Vec<DateTime<Tz>> {
    let time_stamp_array = array::as_primitive_array::<TimestampMillisecondType>(column);
    let mut time_stamps_in_vec = Vec::with_capacity(time_stamp_array.len());
    let tz: Tz = time_zone.parse().unwrap();
    for i in 0..time_stamp_array.len() {
        let stamp = time_stamp_array.value_as_datetime_with_tz(i, tz).unwrap();
        time_stamps_in_vec.push(stamp.into());
    }
    time_stamps_in_vec
}

fn controls_from_result_batch(
    batch: &RecordBatch,
    control_processes: Vec<ProcessInfo>,
) -> Result<BTreeMap<String, Vec<f64>>, String> {
    let mut control_data = BTreeMap::new();
    for control_process in control_processes {
        match batch.column_by_name(&control_process.column_name) {
            Some(column) => match column.data_type() {
                DataType::Float64 => {
                    let float_array = array::as_primitive_array::<Float64Type>(column);
                    let data_as_vec = float_array
                        .into_iter()
                        .map(|x| x.unwrap())
                        .collect::<Vec<f64>>();
                    control_data.insert(control_process.name, data_as_vec);
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

async fn optimization_task(
    rx: oneshot::Receiver<OptimizationData>,
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
                            if let Some(ref batches) = data.input_data_batch {
                                if let Err(error) = send_predicer_batches(&reply_socket, &batches) {
                                    return Err(format!(
                                        "optimization_task: failed to send batches {:?}",
                                        error
                                    ));
                                }
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
    rx: oneshot::Receiver<OptimizationData>,
    tx: oneshot::Sender<OptimizationData>,
) -> Result<(), String> {
    if let Ok(optimization_data) = rx.await {
        let optimization_data_arc = Arc::new(Mutex::new(optimization_data.clone()));

        let data_clone = Arc::clone(&optimization_data_arc);
        let check_handle: JoinHandle<()> = tokio::spawn(async move {
            utilities::check_ts_data_against_temporals(data_clone).await;
        });
        check_handle.await.expect("time series data check failed");
        let mut updated_data = optimization_data_arc.lock().await;
        if let Some(model_data) = &updated_data.model_data {
            match arrow_input::create_and_serialize_record_batches(model_data) {
                Ok(serialized_batches) => {
                    updated_data.input_data_batch = Some(serialized_batches);
                    if tx.send(updated_data.clone()).is_err() {
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
        }
    }
    Ok(())
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

async fn update_model_data_task(
    rx: oneshot::Receiver<OptimizationData>,
    tx: oneshot::Sender<OptimizationData>,
) -> Result<(), String> {
    if let Ok(mut optimization_data) = rx.await {
        if optimization_data.fetch_time_data {
            if let Err(e) = update_timeseries(&mut optimization_data) {
                return Err(format!(
                    "update_model_data_task: failed to update timeseries: {}",
                    e
                ));
            }
        }
        if optimization_data.fetch_weather_data {
            if let Err(e) = update_outside_node_inflow(&mut optimization_data) {
                return Err(format!(
                    "update_model_data_task: failed to update outside node inflow: {}",
                    e
                ));
            }
        }
        if optimization_data.fetch_elec_data {
            if let Err(e) = update_npe_market_prices(&mut optimization_data) {
                return Err(format!(
                    "update_model_data_task: failed to update NPE market prices: {}",
                    e
                ));
            }
        }
        if tx.send(optimization_data).is_err() {
            return Err(
                "update_model_data_task: failed to send updated OptimizationData".to_string(),
            );
        }
        return Ok(());
    }
    Err("update_model_data_task: input channel closed".to_string())
}

fn update_timeseries(optimization_data: &mut OptimizationData) -> Result<(), &'static str> {
    let time_data = optimization_data
        .time_data
        .as_ref()
        .ok_or("Time data is not available.")?;
    if let Some(model_data) = &mut optimization_data.model_data {
        model_data.temporals.t = time_data.series.clone();
        Ok(())
    } else {
        Err("Model data is not available.")
    }
}

fn update_npe_market_prices(
    optimization_data: &mut input_data::OptimizationData,
) -> Result<(), &'static str> {
    if let Some(electricity_price_data) = &optimization_data.elec_price_data {
        let model_data = optimization_data
            .model_data
            .as_mut()
            .ok_or("Model data is not available.")?;
        let markets = &mut model_data.markets;
        if let Some(npe_market) = markets.get_mut("npe") {
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
            return Err("NPE market not found in markets.");
        }
    } else {
        return Err("Electricity price data is not available.");
    }
}

async fn fetch_weather_data_task(
    rx: oneshot::Receiver<OptimizationData>,
    tx: oneshot::Sender<OptimizationData>,
) -> Result<(), String> {
    if let Ok(mut optimization_data) = rx.await {
        if optimization_data.fetch_weather_data {
            if let Some(time_data) = &optimization_data.time_data {
                let location = optimization_data.location.clone().unwrap_or_default();
                let format_string = "%Y-%m-%dT%H:00:00Z";
                let start_time_formatted = time_data.start_time.format(&format_string).to_string();
                let end_time_formatted = time_data.end_time.format(&format_string).to_string();
                match fetch_weather_data(start_time_formatted, end_time_formatted, location).await {
                    Ok(weather_values) => {
                        create_and_update_weather_data(&mut optimization_data, &weather_values);
                    }
                    Err(e) => {
                        return Err(format!(
                            "fetch_weather_data_task: failed to fetch weather data: {}",
                            e
                        ))
                    }
                }
            } else {
                return Err("fetch_weather_data_task: time data is missing".to_string());
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
    optimization_data: &mut OptimizationData,
) -> Result<(), &'static str> {
    // Check if weather_data is Some
    if let Some(weather_data) = &optimization_data.weather_data {
        // Ensure model_data is available and contains the "outside" node
        let model_data = optimization_data
            .model_data
            .as_mut()
            .ok_or("Model data is not available.")?;
        let nodes = &mut model_data.nodes;

        // Find the node named "outside"
        if let Some(outside_node) = nodes.get_mut("outside") {
            // Check if the node is supposed to have inflow data
            if outside_node.is_inflow {
                // Update the outside node's inflow with the weather_data
                outside_node.inflow = weather_data.weather_data.clone(); // Assuming this is the TimeSeriesData you want to use
                return Ok(());
            } else {
                return Err("Outside node is not marked for inflow.");
            }
        } else {
            // "outside" node not found
            return Err("Outside node not found in nodes.");
        }
    } else {
        // weather_data is None
        return Err("Weather data is not available.");
    }
}

// Function to create TimeSeriesData from weather values and update the weather_data field in optimization_data
fn create_and_update_weather_data(optimization_data: &mut OptimizationData, values: &[f64]) {
    if let Some(time_data) = &optimization_data.time_data {
        // Extract series from TimeData
        let series = &time_data.series;

        // Use the extracted series and the provided values to pair them together
        let paired_series = pair_timeseries_with_values(series, values);

        // Generate TimeSeriesData from the paired series
        let ts_data = create_time_series_data_with_scenarios(paired_series);

        // Assuming you have the place information somewhere in optimization_data. If not, adjust as necessary.
        let place = optimization_data.location.clone().unwrap_or_default();

        // Update the weather_data field in optimization_data with the new TimeSeriesData and place
        optimization_data.weather_data = Some(input_data::WeatherData {
            place,
            weather_data: ts_data,
        });
    } else {
        eprintln!("TimeData is missing in optimization_data. Cannot update weather_data.");
    }
}

// Function to create TimeSeriesData with scenarios "s1" and "s2" using paired series
fn create_time_series_data_with_scenarios(
    paired_series: BTreeMap<DateTime<FixedOffset>, f64>,
) -> input_data::TimeSeriesData {
    // Create TimeSeries instances for scenarios "s1" and "s2" with the same series data
    let time_series_s1 = input_data::TimeSeries {
        scenario: "s1".to_string(),
        series: paired_series.clone(), // Clone to use the series data for both TimeSeries
    };

    let time_series_s2 = input_data::TimeSeries {
        scenario: "s2".to_string(),
        series: paired_series, // Use the original series data here
    };

    // Bundle the two TimeSeries into TimeSeriesData
    input_data::TimeSeriesData {
        ts_data: vec![time_series_s1, time_series_s2],
    }
}

fn pair_timeseries_with_values(
    series: &[DateTime<FixedOffset>],
    values: &[f64],
) -> BTreeMap<DateTime<FixedOffset>, f64> {
    series
        .iter()
        .zip(values.iter())
        .map(|(timestamp, &value)| (timestamp.clone(), value))
        .collect()
}

async fn fetch_weather_data(
    start_time: String,
    end_time: String,
    place: String,
) -> Result<Vec<f64>, Box<dyn std::error::Error + Send + Sync>> {
    //println!("Fetching weather data for place: {}, start time: {}, end time: {}", place, start_time, end_time);

    let client = reqwest::Client::new();
    let url = format!(
        "http://localhost:8001/get_weather_data?start_time={}&end_time={}&place={}",
        start_time, end_time, place
    );

    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    //println!("Response status: {}", response.status());

    let response_body = response
        .text()
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    //println!("Raw response body: {}", response_body);

    match serde_json::from_str::<input_data::WeatherDataResponse>(&response_body) {
        Ok(fmi_data) => {
            //println!("Deserialized Weather Data: {:?}", fmi_data.weather_values);
            // Convert temperatures from Celsius to Kelvin
            let temperatures_in_kelvin: Vec<f64> = fmi_data
                .weather_values
                .iter()
                .map(|&celsius| celsius + 273.15)
                .collect();
            //println!("Temperatures in Kelvin: {:?}", temperatures_in_kelvin);
            Ok(temperatures_in_kelvin)
        }
        Err(e) => {
            eprintln!("Error deserializing response body: {}", e);
            Err(Box::new(e))
        }
    }
}

async fn fetch_electricity_prices(
    start_time: String,
    end_time: String,
    country: String,
) -> Result<BTreeMap<DateTime<FixedOffset>, f64>, Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::new();
    let url = format!(
        "https://dashboard.elering.ee/api/nps/price?start={}&end={}",
        start_time, end_time
    );

    // Fetch the response
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

    // Deserialize the response text into a HashMap
    let api_response: HashMap<String, Vec<HashMap<String, f64>>> =
        serde_json::from_str(&response_text)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    // Extract only the prices for the specified country
    let country_data = api_response.get(&country).ok_or_else(|| {
        Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Country data not found",
        )) as Box<dyn std::error::Error + Send + Sync>
    })?;

    // Convert the prices to the desired format and collect them in BTreeMap
    let mut series = BTreeMap::<DateTime<FixedOffset>, f64>::new();
    for entry in country_data {
        if let (Some(&timestamp), Some(&price)) = (entry.get("timestamp"), entry.get("price")) {
            if let Some(datetime) = DateTime::<Utc>::from_timestamp(timestamp as i64, 0) {
                series.insert(datetime.into(), price * 1.0);
            } else {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid timestamp",
                )));
            }
        }
    }
    Ok(series)
}

pub fn create_and_update_elec_price_data(
    optimization_data: &mut OptimizationData,
    price_series: &BTreeMap<DateTime<FixedOffset>, f64>,
) {
    if let Some(_time_data) = &optimization_data.time_data {
        // Generating modified TimeSeriesData for original, up, and down price scenarios
        let original_ts_data = Some(create_modified_price_series_data(price_series, 1.0));
        let up_price_ts_data = Some(create_modified_price_series_data(price_series, 1.1));
        let down_price_ts_data = Some(create_modified_price_series_data(price_series, 0.9));

        // Assuming the country is defined somewhere within optimization_data, for example in the location field
        let country = optimization_data.location.clone().unwrap_or_default();

        // Constructing the ElectricityPriceData structure
        let electricity_price_data = input_data::ElectricityPriceData {
            api_source: Some(String::new()),
            api_key: Some(String::new()),
            country: Some(country),
            price_data: original_ts_data,
            up_price_data: up_price_ts_data,
            down_price_data: down_price_ts_data,
        };

        // Updating the elec_price_data in optimization_data
        optimization_data.elec_price_data = Some(electricity_price_data);
    } else {
        eprintln!("TimeData is missing in optimization_data. Cannot update elec_price_data.");
    }
}

pub fn create_modified_price_series_data(
    original_series: &BTreeMap<DateTime<FixedOffset>, f64>,
    multiplier: f64,
) -> input_data::TimeSeriesData {
    // Create a modified series by multiplying each price by the multiplier
    let modified_series: BTreeMap<DateTime<FixedOffset>, f64> = original_series
        .iter()
        .map(|(timestamp, price)| (timestamp.clone(), price * multiplier))
        .collect();

    // Create TimeSeries instances for scenarios "s1" and "s2" with the modified series
    let time_series_s1 = input_data::TimeSeries {
        scenario: "s1".to_string(),
        series: modified_series.clone(), // Clone for s1
    };

    let time_series_s2 = input_data::TimeSeries {
        scenario: "s2".to_string(),
        series: modified_series, // Re-use for s2
    };

    // Bundle the two TimeSeries into TimeSeriesData
    input_data::TimeSeriesData {
        ts_data: vec![time_series_s1, time_series_s2],
    }
}

async fn fetch_electricity_price_task(
    rx: oneshot::Receiver<OptimizationData>,
    tx: oneshot::Sender<OptimizationData>,
) -> Result<(), String> {
    if let Ok(mut optimization_data) = rx.await {
        if optimization_data.fetch_elec_data {
            if let (Some(country), Some(time_data)) =
                (&optimization_data.country, &optimization_data.time_data)
            {
                let format_string = "%Y-%m-%dT%H:00:00Z";
                let start_time_str = time_data.start_time.format(&format_string).to_string();
                let end_time_str = time_data.end_time.format(&format_string).to_string();
                match fetch_electricity_prices(start_time_str, end_time_str, country.clone()).await
                {
                    Ok(prices) => {
                        create_and_update_elec_price_data(&mut optimization_data, &prices);
                        if tx.send(optimization_data).is_err() {
                            return Err("fetch_electricity_price_task: failed to send updated optimization data in electricity prices".to_string());
                        }
                    }
                    Err(e) => {
                        return Err(format!(
                            "fetch_electricity_price_task: failed to fetch electricity price data for country '{}': {:?}",
                            country, e
                        ));
                    }
                }
            } else {
                if optimization_data.country.is_none() {
                    return Err("fetch_electricity_price_task: country data is missing".to_string());
                }
                if optimization_data.time_data.is_none() {
                    return Err("fetch_electricity_price_task: time data is missing".to_string());
                }
            }
        } else if tx.send(optimization_data).is_err() {
            return Err(
                "fetch_electricity_price_task: failed to forward optimization data".to_string(),
            );
        }
        return Ok(());
    } else {
        return Err("fetch_electricity_price_task: input channel closed".to_string());
    }
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
        use crate::input_data::Topology;
        #[test]
        fn column_prefix_gets_generated() {
            let processes = vec![Process {
                name: "control_process".to_string(),
                topos: vec![Topology {
                    source: "input_node".to_string(),
                    sink: "control_process".to_string(),
                    ..Topology::default()
                }],
                ..Process::default()
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
}
