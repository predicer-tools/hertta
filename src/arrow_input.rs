use arrow::array::{StringArray, Float64Array, Int32Array, Int64Array, BooleanArray, ArrayRef, Array};
use arrow::{datatypes::{DataType, Field, Schema}, error::ArrowError, record_batch::RecordBatch};
use std::sync::Arc;
use arrow::error::Result as ArrowResult;
use crate::input_data;
use crate::arrow_test_data;
use std::collections::HashMap;
use std::error::Error;
use arrow::ipc::writer::StreamWriter;
use base64::{encode};
use serde::{Serialize, Deserialize};
use crate::input_data::InputData;
use std::fs::File;
use std::io::Read;
use bincode;
use std::path::Path;
use chrono::{NaiveDate, NaiveDateTime};

// This function creates a HashMap of binary serialized RecordBatches.
pub fn create_example_arrow_data() -> Result<HashMap<String, Vec<u8>>, Box<dyn Error>> {
    let mut example_data = HashMap::new();

    // List of batches to create and serialize
    let batches = vec![
        "setup", "nodes", "processes", "groups", "process_topology", "node_diffusion",
        "node_history", "node_delay", "inflow_blocks", "markets", "reserve_realisation",
        "scenarios", "efficiencies", "reserve_type", "risk", "cap_ts", "gen_constraint",
        "constraints", "cf", "inflow", "market_prices", "price", "eff_ts", "fixed_ts", "balance_prices",
    ];

    for name in batches {
        match create_and_batch_node_delay() {
            Ok(batch) => {
                let mut writer = Vec::new();
                {
                    // Introduce a new scope for stream_writer
                    let mut stream_writer = StreamWriter::try_new(&mut writer, &batch.schema())?;
                    stream_writer.write(&batch)?;
                    stream_writer.finish()?;
                    // stream_writer is dropped here at the end of the scope
                }
                // Now it's safe to move writer as stream_writer no longer exists
                example_data.insert(name.to_string(), writer);
            },
            Err(e) => return Err(e),
        }
    }

    Ok(example_data)
}

pub fn serialize_batch_and_encode_to_base64(batch: &RecordBatch) -> Result<String, Box<dyn Error>> {
    // Serialize the RecordBatch to a Vec<u8>
    let arrow_data: Vec<u8> = serialize_record_batch_to_vec(batch)?;

    // Encode the Vec<u8> into a base64 String
    let encoded_arrow_data: String = encode(&arrow_data);

    // Return the base64 encoded string
    Ok(encoded_arrow_data)
}

// Define the new function
pub fn create_and_batch_node_diffusion() -> Result<RecordBatch, Box<dyn Error>> {
    // Create a test instance of InputDataSetup
    let node_diffusion = arrow_test_data::create_test_node_diffusion();

    // Convert the InputDataSetup to a RecordBatch
    let batch: RecordBatch = node_diffusion_to_arrow(&node_diffusion)?;

    Ok(batch)
}

pub fn create_and_batch_reserve_type() -> Result<RecordBatch, Box<dyn Error>> {
    // Create a test instance of InputDataSetup
    let reserve_type = arrow_test_data::create_test_reserve_type();

    // Convert the InputDataSetup to a RecordBatch
    let batch: RecordBatch = reserve_type_to_arrow(&reserve_type)?;

    Ok(batch)
}

pub fn create_and_batch_node_delay() -> Result<RecordBatch, Box<dyn Error>> {
    // Create a test instance of InputDataSetup
    let node_delay = arrow_test_data::create_test_node_delay_data();

    // Convert the InputDataSetup to a RecordBatch
    let batch: RecordBatch = node_delays_to_arrow(&node_delay)?;

    Ok(batch)
}

pub fn create_and_batch_market_realisation() -> Result<RecordBatch, Box<dyn Error>> {
    // Create a test instance of InputDataSetup
    let markets_map = arrow_test_data::create_test_markets_hashmap();

    // Convert the InputDataSetup to a RecordBatch
    let batch: RecordBatch = market_realisation_to_arrow(&markets_map)?;

    Ok(batch)
}

pub fn create_and_batch_node_history() -> Result<RecordBatch, Box<dyn Error>> {
    // Create a test instance of InputDataSetup
    let node_history = arrow_test_data::create_example_node_histories();

    // Convert the InputDataSetup to a RecordBatch
    let batch: RecordBatch = node_histories_to_arrow(&node_history)?;

    Ok(batch)
}

// Define the new function
pub fn create_and_encode_inputdatasetup() -> Result<String, Box<dyn Error>> {
    // Create a test instance of InputDataSetup
    let setup = arrow_test_data::create_test_inputdatasetup();

    // Convert the InputDataSetup to a RecordBatch
    let batch: RecordBatch = inputdatasetup_to_arrow(&setup)?;

    // Serialize the RecordBatch to a Vec<u8>
    let arrow_data: Vec<u8> = serialize_record_batch_to_vec(&batch)?;

    // Encode the Vec<u8> into a base64 String
    let encoded_arrow_data: String = encode(&arrow_data);

    // Return the base64 encoded string
    Ok(encoded_arrow_data)
}

pub fn create_and_encode_risk() -> Result<String, Box<dyn Error>> {
    // Create a test instance of Risk
    let risk = arrow_test_data::create_test_risk_data();

    // Convert the InputDataSetup to a RecordBatch
    let batch: RecordBatch = risk_to_arrow(&risk)?;

    // Serialize the RecordBatch to a Vec<u8>
    let arrow_data: Vec<u8> = serialize_record_batch_to_vec(&batch)?;

    // Encode the Vec<u8> into a base64 String
    let encoded_arrow_data: String = encode(&arrow_data);

    // Return the base64 encoded string
    Ok(encoded_arrow_data)
}

// Define the new function
pub fn create_and_encode_nodes() -> Result<String, Box<dyn Error>> {
    // Create a test instance of InputDataSetup
    let nodes = arrow_test_data::create_test_nodes_hashmap();

    // Convert the InputDataSetup to a RecordBatch
    let batch: RecordBatch = nodes_to_arrow(&nodes)?;

    // Serialize the RecordBatch to a Vec<u8>
    let arrow_data: Vec<u8> = serialize_record_batch_to_vec(&batch)?;

    // Encode the Vec<u8> into a base64 String
    let encoded_arrow_data: String = encode(&arrow_data);

    // Return the base64 encoded string
    Ok(encoded_arrow_data)
}

pub fn create_and_encode_node_inflows() -> Result<String, Box<dyn Error>> {
    // Create a test instance of InputDataSetup
    let nodes = arrow_test_data::create_test_nodes_hashmap();

    // Convert the InputDataSetup to a RecordBatch
    let batch: RecordBatch = nodes_inflow_to_arrow(&nodes)?;

    // Serialize the RecordBatch to a Vec<u8>
    let arrow_data: Vec<u8> = serialize_record_batch_to_vec(&batch)?;

    // Encode the Vec<u8> into a base64 String
    let encoded_arrow_data: String = encode(&arrow_data);

    // Return the base64 encoded string
    Ok(encoded_arrow_data)
}

// Define the new function
pub fn create_and_encode_processes() -> Result<String, Box<dyn Error>> {
    // Create a test instance of InputDataSetup
    let processes = arrow_test_data::create_test_processes_hashmap();

    // Convert the InputDataSetup to a RecordBatch
    let batch: RecordBatch = processes_to_arrow(&processes)?;

    // Serialize the RecordBatch to a Vec<u8>
    let arrow_data: Vec<u8> = serialize_record_batch_to_vec(&batch)?;

    // Encode the Vec<u8> into a base64 String
    let encoded_arrow_data: String = encode(&arrow_data);

    // Return the base64 encoded string
    Ok(encoded_arrow_data)
}

// Define the new function
pub fn create_and_encode_process_eff_ops() -> Result<String, Box<dyn Error>> {
    // Create a test instance of InputDataSetup
    let processes = arrow_test_data::create_test_processes_hashmap();

    // Convert the InputDataSetup to a RecordBatch
    let batch: RecordBatch = processes_eff_ops_to_arrow(&processes)?;

    // Serialize the RecordBatch to a Vec<u8>
    let arrow_data: Vec<u8> = serialize_record_batch_to_vec(&batch)?;

    // Encode the Vec<u8> into a base64 String
    let encoded_arrow_data: String = encode(&arrow_data);

    // Return the base64 encoded string
    Ok(encoded_arrow_data)
}

// Define the new function
pub fn create_and_encode_process_topologys() -> Result<String, Box<dyn Error>> {
    // Create a test instance of InputDataSetup
    let processes = arrow_test_data::create_test_process_topologys_hashmap();

    // Convert the InputDataSetup to a RecordBatch
    let batch: RecordBatch = process_topos_to_arrow(&processes)?;

    // Serialize the RecordBatch to a Vec<u8>
    let arrow_data: Vec<u8> = serialize_record_batch_to_vec(&batch)?;

    // Encode the Vec<u8> into a base64 String
    let encoded_arrow_data: String = encode(&arrow_data);

    // Return the base64 encoded string
    Ok(encoded_arrow_data)
}

pub fn create_and_encode_groups() -> Result<String, Box<dyn Error>> {
    // Create a test instance of InputDataSetup
    let groups = arrow_test_data::create_test_groups_hashmap();

    // Convert the InputDataSetup to a RecordBatch
    let batch: RecordBatch = groups_to_arrow(&groups)?;

    // Serialize the RecordBatch to a Vec<u8>
    let arrow_data: Vec<u8> = serialize_record_batch_to_vec(&batch)?;

    // Encode the Vec<u8> into a base64 String
    let encoded_arrow_data: String = encode(&arrow_data);

    // Return the base64 encoded string
    Ok(encoded_arrow_data)
}

pub fn create_and_encode_markets() -> Result<String, Box<dyn Error>> {
    // Create a test instance of InputDataSetup
    let markets = arrow_test_data::create_test_markets_hashmap();

    // Convert the InputDataSetup to a RecordBatch
    let batch: RecordBatch = markets_to_arrow(&markets)?;

    // Serialize the RecordBatch to a Vec<u8>
    let arrow_data: Vec<u8> = serialize_record_batch_to_vec(&batch)?;

    // Encode the Vec<u8> into a base64 String
    let encoded_arrow_data: String = encode(&arrow_data);

    // Return the base64 encoded string
    Ok(encoded_arrow_data)
}

pub fn create_and_encode_timeseries() -> Result<String, Box<dyn Error>> {
    // Create a test instance of InputDataSetup
    let timeseries = arrow_test_data::create_test_timeseries();

    // Convert the InputDataSetup to a RecordBatch
    let batch: RecordBatch = timeseries_to_arrow(timeseries)?;

    // Serialize the RecordBatch to a Vec<u8>
    let arrow_data: Vec<u8> = serialize_record_batch_to_vec(&batch)?;

    // Encode the Vec<u8> into a base64 String
    let encoded_arrow_data: String = encode(&arrow_data);

    // Return the base64 encoded string
    Ok(encoded_arrow_data)
}

pub fn create_and_encode_scenarios() -> Result<String, Box<dyn Error>> {
    // Create a test instance of InputDataSetup
    let scenarios = arrow_test_data::create_test_scenarios();

    // Convert the InputDataSetup to a RecordBatch
    let batch: RecordBatch = scenarios_to_arrow(&scenarios)?;

    // Serialize the RecordBatch to a Vec<u8>
    let arrow_data: Vec<u8> = serialize_record_batch_to_vec(&batch)?;

    // Encode the Vec<u8> into a base64 String
    let encoded_arrow_data: String = encode(&arrow_data);

    // Return the base64 encoded string
    Ok(encoded_arrow_data)
}

pub fn risk_to_arrow(risk: &HashMap<String, f64>) -> Result<RecordBatch, ArrowError> {
    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("parameter", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]);

    // Initialize vectors to hold the data
    let mut parameters: Vec<String> = Vec::new();
    let mut values: Vec<f64> = Vec::new();

    // Populate the vectors from the HashMap
    for (key, &value) in risk.iter() {
        parameters.push(key.clone());
        values.push(value);
    }

    // Create Arrow arrays from the vectors
    let parameter_array: ArrayRef = Arc::new(StringArray::from(parameters));
    let value_array: ArrayRef = Arc::new(Float64Array::from(values));

    // Create the RecordBatch using these arrays and the schema
    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![parameter_array, value_array],
    );

    record_batch
}

pub fn reserve_type_to_arrow(reserve_type: &HashMap<String, f64>) -> Result<RecordBatch, ArrowError> {
    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("type", DataType::Utf8, false),
        Field::new("ramp_factor", DataType::Float64, false),
    ]);

    // Initialize vectors to hold the data
    let mut types: Vec<String> = Vec::new();
    let mut ramp_factors: Vec<f64> = Vec::new();

    // Populate the vectors from the HashMap
    for (key, &value) in reserve_type.iter() {
        types.push(key.clone());
        ramp_factors.push(value);
    }

    // Create Arrow arrays from the vectors
    let type_array: ArrayRef = Arc::new(StringArray::from(types));
    let ramp_factor_array: ArrayRef = Arc::new(Float64Array::from(ramp_factors));

    // Create the RecordBatch using these arrays and the schema
    RecordBatch::try_new(
        Arc::new(schema),
        vec![type_array, ramp_factor_array],
    )
}

pub fn market_realisation_to_arrow(markets: &HashMap<String, input_data::MarketNew>) -> Result<RecordBatch, ArrowError> {
    // First, collect all scenario names across all markets
    let mut scenario_names = HashMap::new();
    for market in markets.values() {
        for scenario in market.realisation.keys() {
            scenario_names.insert(scenario.clone(), ());
        }
    }
    let scenario_names: Vec<String> = scenario_names.into_keys().collect();

    // Define the schema dynamically based on the scenario names
    let mut fields: Vec<Field> = vec![Field::new("reserve_product", DataType::Utf8, false)];
    fields.extend(scenario_names.iter().map(|name| Field::new(name, DataType::Float64, false)));
    
    // Initialize vectors to hold the data
    let mut market_names: Vec<String> = Vec::new();
    let mut scenario_values: Vec<Vec<f64>> = vec![vec![]; scenario_names.len()];

    // Populate the vectors from the HashMap
    for market in markets.values() {
        market_names.push(market.name.clone());
        for (i, scenario_name) in scenario_names.iter().enumerate() {
            scenario_values[i].push(*market.realisation.get(scenario_name).unwrap_or(&0.0));
        }
    }

    // Create Arrow arrays from the vectors
    let market_names_array: ArrayRef = Arc::new(StringArray::from(market_names));
    let mut columns: Vec<ArrayRef> = vec![market_names_array];
    for values in scenario_values {
        columns.push(Arc::new(Float64Array::from(values)) as ArrayRef);
    }

    let schema = Arc::new(Schema::new(fields));

    // Create the RecordBatch using these arrays and the schema
    RecordBatch::try_new(schema, columns)
}

pub fn node_delays_to_arrow(node_delays: &HashMap<String, input_data::NodeDelay>) -> Result<RecordBatch, ArrowError> {
    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("node1", DataType::Utf8, false),
        Field::new("node2", DataType::Utf8, false),
        Field::new("delay_t", DataType::Float64, false),
        Field::new("min_flow", DataType::Float64, false),
        Field::new("max_flow", DataType::Float64, false),
    ]);

    // Initialize vectors to hold the data
    let mut node1s: Vec<String> = Vec::new();
    let mut node2s: Vec<String> = Vec::new();
    let mut delays: Vec<f64> = Vec::new();
    let mut min_flows: Vec<f64> = Vec::new();
    let mut max_flows: Vec<f64> = Vec::new();

    // Populate the vectors from the HashMap
    for node_delay in node_delays.values() {
        node1s.push(node_delay.node1.clone());
        node2s.push(node_delay.node2.clone());
        delays.push(node_delay.delay);
        min_flows.push(node_delay.min_flow);
        max_flows.push(node_delay.max_flow);
    }

    // Create Arrow arrays from the vectors
    let node1_array: ArrayRef = Arc::new(StringArray::from(node1s));
    let node2_array: ArrayRef = Arc::new(StringArray::from(node2s));
    let delay_array: ArrayRef = Arc::new(Float64Array::from(delays));
    let min_flow_array: ArrayRef = Arc::new(Float64Array::from(min_flows));
    let max_flow_array: ArrayRef = Arc::new(Float64Array::from(max_flows));

    // Create the RecordBatch using these arrays and the schema
    RecordBatch::try_new(
        Arc::new(schema),
        vec![node1_array, node2_array, delay_array, min_flow_array, max_flow_array],
    )
}

pub fn node_histories_to_arrow(node_histories: &HashMap<String, input_data::NodeHistory>) -> ArrowResult<RecordBatch> {
    // Define the schema for the Arrow RecordBatch dynamically
    let mut fields = vec![Field::new("t", DataType::Int32, false)];
    let mut columns: Vec<ArrayRef> = Vec::new();

    // We'll need a column for 't' values
    let mut ts_values: Vec<i32> = Vec::new();

    // Placeholder to hold time series data
    let mut string_columns_data = HashMap::new();
    let mut float_columns_data = HashMap::new();

    // Populate the columns from the NodeHistory HashMap
    for (node_name, node_history) in node_histories {
        for ts in &node_history.steps.ts_data {
            let column_name_string = format!("{},t,{}", node_name, ts.scenario);
            let column_name_float = format!("{},{}", node_name, ts.scenario);

            fields.push(Field::new(&column_name_string, DataType::Utf8, false));
            fields.push(Field::new(&column_name_float, DataType::Float64, false));

            for (t, value) in &ts.series {
                string_columns_data
                    .entry(column_name_string.clone())
                    .or_insert_with(Vec::new)
                    .push(t.clone());
                float_columns_data
                    .entry(column_name_float.clone())
                    .or_insert_with(Vec::new)
                    .push(*value);
            }
        }
    }

    // Add 't' column which is just a sequence of integers
    for i in 0..string_columns_data.values().next().unwrap().len() as i32 {
        ts_values.push(i + 1);
    }
    columns.push(Arc::new(Int32Array::from(ts_values)) as ArrayRef);

    // Add the string and float columns to the RecordBatch
    for (key, val) in string_columns_data {
        columns.push(Arc::new(StringArray::from(val)) as ArrayRef);
    }
    for (key, val) in float_columns_data {
        columns.push(Arc::new(Float64Array::from(val)) as ArrayRef);
    }

    let schema = Arc::new(Schema::new(fields));

    // Create the RecordBatch using these arrays and the schema
    RecordBatch::try_new(schema, columns)
}

// Function to convert a HashMap<String, NodeDiffusion> to an Arrow RecordBatch
pub fn node_diffusion_to_arrow(
    node_diffusions: &HashMap<String, input_data::NodeDiffusion>,
) -> Result<RecordBatch, ArrowError> {
    // Define the schema fields based on the NodeDiffusion structure
    let fields = vec![
        Field::new("node1", DataType::Utf8, false),
        Field::new("node2", DataType::Utf8, false),
        Field::new("diff_coeff", DataType::Float64, false),
    ];

    // Prepare columns for the RecordBatch
    let mut node1_values: Vec<String> = Vec::new();
    let mut node2_values: Vec<String> = Vec::new();
    let mut diff_coeff_values: Vec<f64> = Vec::new();

    // Fill the columns with data from the HashMap
    for node_diffusion in node_diffusions.values() {
        node1_values.push(node_diffusion.node1.clone());
        node2_values.push(node_diffusion.node2.clone());
        diff_coeff_values.push(node_diffusion.diff_coeff);
    }

    // Create Arrow arrays for each column
    let node1_array: ArrayRef = Arc::new(StringArray::from(node1_values));
    let node2_array: ArrayRef = Arc::new(StringArray::from(node2_values));
    let diff_coeff_array: ArrayRef = Arc::new(Float64Array::from(diff_coeff_values));

    // Collect arrays into a vector
    let columns: Vec<ArrayRef> = vec![node1_array, node2_array, diff_coeff_array];

    // Create the schema for the RecordBatch
    let schema = Arc::new(Schema::new(fields));

    // Create the RecordBatch
    RecordBatch::try_new(schema, columns)
}

pub fn serialize_record_batch_to_vec(batch: &RecordBatch) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        // Place the writer in a scoped block
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
        // `writer` gets dropped here, at the end of the scoped block, releasing the borrow on `buf`
    }
    // Now that the writer is dropped, it's safe to move `buf`
    println!("Buffer size after serialization: {}", buf.len());
    Ok(buf)
}

pub fn create_arrow_data_buffer() -> Result<Vec<u8>, Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let id_values = vec![1, 2, 3, 4];
    let name_values = vec!["Alice", "Bob", "Cindy", "David"];
    let ids = Int32Array::from(id_values);
    let names = StringArray::from(name_values);

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(names)])?;

    let mut buffer: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
    }

    Ok(buffer)
}

// This function converts a HashMap<String, Node> of inflow TimeSeriesData to an Arrow RecordBatch
pub fn nodes_inflow_to_arrow(nodes: &HashMap<String, input_data::NodeNew>) -> Result<RecordBatch, ArrowError> {
    let mut fields = vec![Field::new("t", DataType::Utf8, false)];
    let mut columns: Vec<ArrayRef> = Vec::new();
    
    // Assume all nodes have the same timestamps
    if let Some((_, first_node)) = nodes.iter().next() {
        let timestamps: Vec<String> = first_node.inflow.ts_data[0].series.iter().map(|(t, _)| t.clone()).collect();
        let timestamp_array = Arc::new(StringArray::from(timestamps)) as ArrayRef;
        columns.push(timestamp_array);
    }

    for (node_name, node) in nodes {
        for ts in &node.inflow.ts_data {
            let column_name = format!("{},{}", node_name, ts.scenario);
            fields.push(Field::new(&column_name, DataType::Float64, false));
    
            let values: Vec<f64> = ts.series.iter().map(|(_, v)| *v).collect();
            let value_array = Arc::new(Float64Array::from(values)) as ArrayRef;
            columns.push(value_array);
        }
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}

// Function to convert eff_ops of Process to an Arrow RecordBatch
pub fn processes_eff_ops_to_arrow(processes: &HashMap<String, input_data::ProcessNew>) -> Result<RecordBatch, ArrowError> {
    let mut fields: Vec<Field> = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();

    // Create a column for each possible index in eff_ops, assuming all Process have the same number of operations
    let max_ops = processes.values().map(|p| p.eff_ops.len()).max().unwrap_or(0);
    for i in 1..=max_ops {
        let column_name = i.to_string();
        fields.push(Field::new(&column_name, DataType::Float64, true)); // true for nullable
    }

    for (process_name, process) in processes {
        // Process name is used as part of the row identifier
        let row_name = format!("{},op", process_name);
        let mut ops_values: Vec<Option<f64>> = Vec::new();

        for op in &process.eff_ops {
            // Attempt to parse the string as f64. If it fails, use None which will be converted to null in the RecordBatch
            let value = op.parse::<f64>().ok();
            ops_values.push(value);
        }

        // Make all rows the same length by filling in with None
        while ops_values.len() < max_ops {
            ops_values.push(None);
        }

        let ops_array = Arc::new(Float64Array::from(ops_values)) as ArrayRef;
        columns.push(ops_array);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}

// Convert HashMap<String, MarketNew> to RecordBatch
pub fn markets_to_arrow(markets: &HashMap<String, input_data::MarketNew>) -> Result<RecordBatch, ArrowError> {
    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("market", DataType::Utf8, false),
        Field::new("m_type", DataType::Utf8, false),
        Field::new("node", DataType::Utf8, false),
        Field::new("processgroup", DataType::Utf8, false),
        Field::new("direction", DataType::Utf8, false),
        Field::new("reserve_type", DataType::Utf8, false),
        Field::new("is_bid", DataType::Boolean, false),
        Field::new("is_limited", DataType::Boolean, false),
        Field::new("min_bid", DataType::Float64, false),
        Field::new("max_bid", DataType::Float64, false),
        Field::new("fee", DataType::Float64, false),
    ]);

    // Initialize vectors to hold market data
    let mut markets_vec: Vec<String> = Vec::new();
    let mut m_types: Vec<String> = Vec::new();
    let mut nodes: Vec<String> = Vec::new();
    let mut processgroups: Vec<String> = Vec::new();
    let mut directions: Vec<String> = Vec::new();
    let mut reserve_types: Vec<String> = Vec::new();
    let mut is_bids: Vec<bool> = Vec::new();
    let mut is_limiteds: Vec<bool> = Vec::new();
    let mut min_bids: Vec<f64> = Vec::new();
    let mut max_bids: Vec<f64> = Vec::new();
    let mut fees: Vec<f64> = Vec::new();

    for (market_name, market) in markets {
        markets_vec.push(market.name.clone());
        m_types.push(market.m_type.clone());
        nodes.push(market.node.clone());
        processgroups.push(market.pgroup.clone());
        directions.push(market.direction.clone());
        reserve_types.push(market.reserve_type.clone());
        is_bids.push(market.is_bid);
        is_limiteds.push(market.is_limited);
        min_bids.push(market.min_bid);
        max_bids.push(market.max_bid);
        fees.push(market.fee);
    }

    // Create arrays from the vectors
    let markets_array = Arc::new(StringArray::from(markets_vec)) as ArrayRef;
    let m_types_array = Arc::new(StringArray::from(m_types)) as ArrayRef;
    let nodes_array = Arc::new(StringArray::from(nodes)) as ArrayRef;
    let processgroups_array = Arc::new(StringArray::from(processgroups)) as ArrayRef;
    let directions_array = Arc::new(StringArray::from(directions)) as ArrayRef;
    let reserve_types_array = Arc::new(StringArray::from(reserve_types)) as ArrayRef;
    let is_bids_array = Arc::new(BooleanArray::from(is_bids)) as ArrayRef;
    let is_limiteds_array = Arc::new(BooleanArray::from(is_limiteds)) as ArrayRef;
    let min_bids_array = Arc::new(Float64Array::from(min_bids)) as ArrayRef;
    let max_bids_array = Arc::new(Float64Array::from(max_bids)) as ArrayRef;
    let fees_array = Arc::new(Float64Array::from(fees)) as ArrayRef;

    // Now you can create the RecordBatch using these arrays
    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            markets_array,
            m_types_array,
            nodes_array,
            processgroups_array,
            directions_array,
            reserve_types_array,
            is_bids_array,
            is_limiteds_array,
            min_bids_array,
            max_bids_array,
            fees_array,
        ],
    );

    record_batch

}

// Function to convert scenarios HashMap into an Arrow RecordBatch
pub fn scenarios_to_arrow(scenarios: &HashMap<String, f64>) -> Result<RecordBatch, ArrowError> {
    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("probability", DataType::Float64, false),
    ]);

    // Initialize vectors to hold scenario data
    let mut names: Vec<String> = Vec::new();
    let mut probabilities: Vec<f64> = Vec::new();

    // Populate vectors with data from the HashMap
    for (name, probability) in scenarios {
        names.push(name.clone());
        probabilities.push(*probability);
    }

    // Create arrays from the vectors
    let names_array = Arc::new(StringArray::from(names)) as ArrayRef;
    let probabilities_array = Arc::new(Float64Array::from(probabilities)) as ArrayRef;

    // Create the RecordBatch using these arrays
    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![names_array, probabilities_array],
    )?;

    Ok(record_batch)
}

// Convert HashMap<String, ProcessTopology> to RecordBatch
pub fn process_topos_to_arrow(process_topologys: &HashMap<String, input_data::ProcessTopology>) -> Result<RecordBatch, ArrowError> {
    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("process", DataType::Utf8, false),
        Field::new("source_sink", DataType::Utf8, false),
        Field::new("node", DataType::Utf8, false),
        Field::new("conversion_coeff", DataType::Float64, false),
        Field::new("capacity", DataType::Float64, false),
        Field::new("vom_cost", DataType::Float64, false),
        Field::new("ramp_up", DataType::Float64, false),
        Field::new("ramp_down", DataType::Float64, false),
        Field::new("initial_load", DataType::Float64, false),
        Field::new("initial_flow", DataType::Float64, false),
    ]);

    let mut processes: Vec<String> = Vec::new();
    let mut source_sinks: Vec<String> = Vec::new();
    let mut nodes: Vec<String> = Vec::new();
    let mut conversion_coeffs: Vec<f64> = Vec::new();
    let mut capacities: Vec<f64> = Vec::new();
    let mut vom_costs: Vec<f64> = Vec::new();
    let mut ramp_ups: Vec<f64> = Vec::new();
    let mut ramp_downs: Vec<f64> = Vec::new();
    let mut initial_loads: Vec<f64> = Vec::new();
    let mut initial_flows: Vec<f64> = Vec::new();

    for (process_name, process) in process_topologys {
        processes.push(process.process.clone());
        source_sinks.push(process.source_sink.clone());
        nodes.push(process.node.clone());
        conversion_coeffs.push(process.conversion_coeff);
        capacities.push(process.capacity);
        vom_costs.push(process.vom_cost);
        ramp_ups.push(process.ramp_up);
        ramp_downs.push(process.ramp_down);
        initial_loads.push(process.initial_load);
        initial_flows.push(process.initial_flow);
    }    

    // Create arrays from the vectors
    let processes_array = Arc::new(StringArray::from(processes)) as ArrayRef;
    let source_sinks_array = Arc::new(StringArray::from(source_sinks)) as ArrayRef;
    let nodes_array = Arc::new(StringArray::from(nodes)) as ArrayRef;
    let conversion_coeffs_array = Arc::new(Float64Array::from(conversion_coeffs)) as ArrayRef;
    let capacities_array = Arc::new(Float64Array::from(capacities)) as ArrayRef;
    let vom_costs_array = Arc::new(Float64Array::from(vom_costs)) as ArrayRef;
    let ramp_ups_array = Arc::new(Float64Array::from(ramp_ups)) as ArrayRef;
    let ramp_downs_array = Arc::new(Float64Array::from(ramp_downs)) as ArrayRef;
    let initial_loads_array = Arc::new(Float64Array::from(initial_loads)) as ArrayRef;
    let initial_flows_array = Arc::new(Float64Array::from(initial_flows)) as ArrayRef;

    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            processes_array,
            source_sinks_array,
            nodes_array,
            conversion_coeffs_array,
            capacities_array,
            vom_costs_array,
            ramp_ups_array,
            ramp_downs_array,
            initial_loads_array,
            initial_flows_array,
        ],
    );    

    record_batch
}

// Convert HashMap<String, ProcessNew> to RecordBatch
pub fn processes_to_arrow(processes: &HashMap<String, input_data::ProcessNew>) -> Result<RecordBatch, ArrowError> {
    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("is_cf", DataType::Boolean, false),
        Field::new("is_cf_fix", DataType::Boolean, false),
        Field::new("is_online", DataType::Boolean, false),
        Field::new("is_res", DataType::Boolean, false),
        Field::new("conversion", DataType::Int64, false),
        Field::new("eff", DataType::Float64, false),
        Field::new("load_min", DataType::Float64, false),
        Field::new("load_max", DataType::Float64, false),
        Field::new("start_cost", DataType::Float64, false),
        Field::new("min_online", DataType::Float64, false),
        Field::new("min_offline", DataType::Float64, false),
        Field::new("max_online", DataType::Float64, false),
        Field::new("max_offline", DataType::Float64, false),
        Field::new("initial_state", DataType::Float64, false),
        Field::new("scenario_independent_online", DataType::Float64, false),
        Field::new("delay", DataType::Float64, false),
    ]);

    // Initialize vectors to hold process data
    let mut names: Vec<String> = Vec::new();
    let mut is_cfs: Vec<bool> = Vec::new();
    let mut is_cf_fixes: Vec<bool> = Vec::new();
    let mut is_onlines: Vec<bool> = Vec::new();
    let mut is_reses: Vec<bool> = Vec::new();
    let mut conversions: Vec<i64> = Vec::new();
    let mut effs: Vec<f64> = Vec::new();
    let mut load_mins: Vec<f64> = Vec::new();
    let mut load_maxs: Vec<f64> = Vec::new();
    let mut start_costs: Vec<f64> = Vec::new();
    let mut min_onlines: Vec<f64> = Vec::new();
    let mut min_offlines: Vec<f64> = Vec::new();
    let mut max_onlines: Vec<f64> = Vec::new();
    let mut max_offlines: Vec<f64> = Vec::new();
    let mut initial_states: Vec<f64> = Vec::new();
    let mut scenario_independent_onlines: Vec<f64> = Vec::new();
    let mut delays: Vec<f64> = Vec::new();

    for (name, process) in processes {
        names.push(process.name.clone());
        is_cfs.push(process.is_cf);
        is_cf_fixes.push(process.is_cf_fix);
        is_onlines.push(process.is_online);
        is_reses.push(process.is_res);
        conversions.push(process.conversion);
        effs.push(process.eff);
        load_mins.push(process.load_min);
        load_maxs.push(process.load_max);
        start_costs.push(process.start_cost);
        min_onlines.push(process.min_online);
        min_offlines.push(process.min_offline);
        max_onlines.push(process.max_online);
        max_offlines.push(process.max_offline);
        initial_states.push(process.initial_state);
        scenario_independent_onlines.push(process.scenario_independent_online);
        delays.push(process.delay);
    }

    // Create arrays from the vectors
    let names_array = Arc::new(StringArray::from(names)) as ArrayRef;
    let is_cfs_array = Arc::new(BooleanArray::from(is_cfs)) as ArrayRef;
    let is_cf_fixes_array = Arc::new(BooleanArray::from(is_cf_fixes)) as ArrayRef;
    let is_onlines_array = Arc::new(BooleanArray::from(is_onlines)) as ArrayRef;
    let is_reses_array = Arc::new(BooleanArray::from(is_reses)) as ArrayRef;
    let conversions_array = Arc::new(Int64Array::from(conversions)) as ArrayRef;
    let effs_array = Arc::new(Float64Array::from(effs)) as ArrayRef;
    let load_mins_array = Arc::new(Float64Array::from(load_mins)) as ArrayRef;
    let load_maxs_array = Arc::new(Float64Array::from(load_maxs)) as ArrayRef;
    let start_costs_array = Arc::new(Float64Array::from(start_costs)) as ArrayRef;
    let min_onlines_array = Arc::new(Float64Array::from(min_onlines)) as ArrayRef;
    let min_offlines_array = Arc::new(Float64Array::from(min_offlines)) as ArrayRef;
    let max_onlines_array = Arc::new(Float64Array::from(max_onlines)) as ArrayRef;
    let max_offlines_array = Arc::new(Float64Array::from(max_offlines)) as ArrayRef;
    let initial_states_array = Arc::new(Float64Array::from(initial_states)) as ArrayRef;
    let scenario_independent_onlines_array = Arc::new(Float64Array::from(scenario_independent_onlines)) as ArrayRef;
    let delays_array = Arc::new(Float64Array::from(delays)) as ArrayRef;

    // Now you can create the RecordBatch using these arrays
    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            names_array,
            is_cfs_array,
            is_cf_fixes_array,
            is_onlines_array,
            is_reses_array,
            conversions_array,
            effs_array,
            load_mins_array,
            load_maxs_array,
            start_costs_array,
            min_onlines_array,
            min_offlines_array,
            max_onlines_array,
            max_offlines_array,
            initial_states_array,
            scenario_independent_onlines_array,
            delays_array,
        ],
    );

    record_batch

}

// Convert HashMap<String, Node> to RecordBatch
pub fn nodes_to_arrow(nodes: &HashMap<String, input_data::NodeNew>) -> Result<RecordBatch, ArrowError> {
    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("node", DataType::Utf8, false),
        Field::new("is_commodity", DataType::Boolean, false),
        Field::new("is_state", DataType::Boolean, false),
        Field::new("is_res", DataType::Boolean, false),
        Field::new("is_market", DataType::Boolean, false),
        Field::new("is_inflow", DataType::Boolean, false),
        Field::new("state_max", DataType::Float64, false),
        Field::new("state_min", DataType::Float64, false),
        Field::new("in_max", DataType::Float64, false),
        Field::new("out_max", DataType::Float64, false),
        Field::new("initial_state", DataType::Float64, false),
        Field::new("state_loss_proportional", DataType::Float64, false),
        Field::new("scenario_independent_state", DataType::Boolean, false),
        Field::new("is_temp", DataType::Boolean, false),
        Field::new("t_e_conversion", DataType::Float64, false),
        Field::new("residual_value", DataType::Float64, false),
    ]);

    let mut names: Vec<String> = Vec::new();
    let mut is_commodity: Vec<bool> = Vec::new();
    let mut is_state: Vec<bool> = Vec::new();
    let mut is_res: Vec<bool> = Vec::new();
    let mut is_market: Vec<bool> = Vec::new();
    let mut is_inflow: Vec<bool> = Vec::new();
    let mut state_maxs: Vec<f64> = Vec::new();
    let mut state_mins: Vec<f64> = Vec::new();
    let mut in_maxs: Vec<f64> = Vec::new();
    let mut out_maxs: Vec<f64> = Vec::new();
    let mut initial_states: Vec<f64> = Vec::new();
    let mut state_loss_proportionals: Vec<f64> = Vec::new();
    let mut scenario_independent_states: Vec<bool> = Vec::new();
    let mut is_temps: Vec<bool> = Vec::new();
    let mut t_e_conversions: Vec<f64> = Vec::new();
    let mut residual_values: Vec<f64> = Vec::new();

    for (node_name, node) in nodes {
        names.push(node_name.clone());
        is_commodity.push(node.is_commodity);
        is_state.push(node.is_state);
        is_res.push(node.is_res);
        is_market.push(node.is_market);
        is_inflow.push(node.is_inflow);
        state_maxs.push(node.state.state_max);
        state_mins.push(node.state.state_min);
        in_maxs.push(node.state.in_max);
        out_maxs.push(node.state.out_max);
        initial_states.push(node.state.initial_state);
        state_loss_proportionals.push(node.state.state_loss_proportional);
        scenario_independent_states.push(node.state.scenario_independent_state); // Adjust if you have a specific field for this
        is_temps.push(node.state.is_temp);
        t_e_conversions.push(node.state.t_e_conversion);
        residual_values.push(node.state.residual_value);
    }

    // Create arrays from the vectors
    let names_array = Arc::new(StringArray::from(names)) as ArrayRef;
    let is_commodity_array = Arc::new(BooleanArray::from(is_commodity)) as ArrayRef;
    let is_state_array = Arc::new(BooleanArray::from(is_state)) as ArrayRef;
    let is_res_array = Arc::new(BooleanArray::from(is_res)) as ArrayRef;
    let is_market_array = Arc::new(BooleanArray::from(is_market)) as ArrayRef;
    let is_inflow_array = Arc::new(BooleanArray::from(is_inflow)) as ArrayRef;
    let state_maxs_array = Arc::new(Float64Array::from(state_maxs)) as ArrayRef;
    let state_mins_array = Arc::new(Float64Array::from(state_mins)) as ArrayRef;
    let in_maxs_array = Arc::new(Float64Array::from(in_maxs)) as ArrayRef;
    let out_maxs_array = Arc::new(Float64Array::from(out_maxs)) as ArrayRef;
    let initial_states_array = Arc::new(Float64Array::from(initial_states)) as ArrayRef;
    let state_loss_proportionals_array = Arc::new(Float64Array::from(state_loss_proportionals)) as ArrayRef;
    let scenario_independent_states_array = Arc::new(BooleanArray::from(scenario_independent_states)) as ArrayRef;
    let is_temps_array = Arc::new(BooleanArray::from(is_temps)) as ArrayRef;
    let t_e_conversions_array = Arc::new(Float64Array::from(t_e_conversions)) as ArrayRef;
    let residual_values_array = Arc::new(Float64Array::from(residual_values)) as ArrayRef;


    // Now you can create the RecordBatch using these arrays
    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            names_array,
            is_commodity_array,
            is_state_array,
            is_res_array,
            is_market_array,
            is_inflow_array,
            state_maxs_array,
            state_mins_array,
            in_maxs_array,
            out_maxs_array,
            initial_states_array,
            state_loss_proportionals_array,
            scenario_independent_states_array,
            is_temps_array,
            t_e_conversions_array,
            residual_values_array,
        ],
    );

    record_batch
}

pub fn inputdatasetup_to_arrow(setup: &input_data::InputDataSetup) -> Result<RecordBatch, ArrowError> {
    println!("Starting inputdatasetup_to_arrow function.");

    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("parameter", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, true), // Using Utf8 for all values for simplicity
    ]);
    println!("Schema defined.");

    // Prepare data
    let parameters = vec![
        "contains_reserves", "contains_online", "contains_states", "contains_piecewise_eff",
        "contains_risk", "contains_diffusion", "contains_delay", "contains_markets",
        "reserve_realisation", "use_market_bids", "common_timesteps", "common_scenario_name",
        "use_node_dummy_variables", "use_ramp_dummy_variables",
    ];
    
    println!("Parameters prepared.");

    let values: Vec<String> = vec![
        setup.contains_reserves.to_string(), setup.contains_online.to_string(), setup.contains_states.to_string(),
        setup.contains_piecewise_eff.to_string(), setup.contains_risk.to_string(), setup.contains_diffusion.to_string(),
        setup.contains_delay.to_string(), setup.contains_markets.to_string(), setup.reserve_realisation.to_string(),
        setup.use_market_bids.to_string(), setup.common_timesteps.to_string(), setup.common_scenario_name.clone(),
        setup.use_node_dummy_variables.to_string(), setup.use_ramp_dummy_variables.to_string(),
    ];

    println!("Values prepared.");

    /* 
    // Debug print to check the first few parameters and values
    for i in 0..parameters.len().min(5) { // Just print the first 5 for brevity
        println!("{}: {}", parameters[i], values[i]);
    }
    */

    // Create arrays
    let parameter_array = Arc::new(StringArray::from(parameters)) as ArrayRef;
    let value_array = Arc::new(StringArray::from(values)) as ArrayRef;

    //println!("Arrays created.");

    // Attempt to create a RecordBatch
    match RecordBatch::try_new(Arc::new(schema), vec![parameter_array, value_array]) {
        Ok(batch) => {
            println!("RecordBatch successfully created.");
            Ok(batch)
        },
        Err(e) => {
            println!("Error creating RecordBatch: {:?}", e);
            Err(e)
        }
    }
}

// Function to convert HashMap<String, GroupNew> to RecordBatch
pub fn groups_to_arrow(groups: &HashMap<String, input_data::GroupNew>) -> Result<RecordBatch, ArrowError> {
    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("g_type", DataType::Utf8, false),
        Field::new("entity", DataType::Utf8, false),
        Field::new("group", DataType::Utf8, false),
    ]);

    // Initialize vectors to hold group data
    let mut names: Vec<String> = Vec::new();
    let mut g_types: Vec<String> = Vec::new();
    let mut entities: Vec<String> = Vec::new();
    let mut groups_vec: Vec<String> = Vec::new();

    for (_, group) in groups {
        names.push(group.name.clone());
        g_types.push(group.g_type.clone());
        entities.push(group.entity.clone());
        groups_vec.push(group.group.clone());
    }

    // Create arrays from the vectors
    let names_array = Arc::new(StringArray::from(names)) as ArrayRef;
    let g_types_array = Arc::new(StringArray::from(g_types)) as ArrayRef;
    let entities_array = Arc::new(StringArray::from(entities)) as ArrayRef;
    let groups_array = Arc::new(StringArray::from(groups_vec)) as ArrayRef;

    // Now you can create the RecordBatch using these arrays
    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            names_array,
            g_types_array,
            entities_array,
            groups_array,
        ],
    );

    record_batch
}

// Function to convert Vec<String> of timeseries data to RecordBatch
pub fn timeseries_to_arrow(timeseries: Vec<String>) -> Result<RecordBatch, ArrowError> {
    // Define the schema for the Arrow RecordBatch with a single column of strings
    let schema = Schema::new(vec![
        Field::new("t", DataType::Utf8, false),
    ]);

    // Create a StringArray from the timeseries vector
    let timeseries_array = Arc::new(StringArray::from(timeseries)) as ArrayRef;

    // Create the RecordBatch
    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![timeseries_array],
    );

    record_batch
}

pub fn vec_to_record_batch(timeseries: Vec<String>) -> ArrowResult<RecordBatch> {
    // Convert the Vec<String> to a StringArray
    let array: ArrayRef = Arc::new(StringArray::from(timeseries));

    // Define the schema of the record batch. This schema only has one field.
    let schema = Schema::new(vec![
        Field::new("timeseries", DataType::Utf8, false),
    ]);

    // Create the record batch
    let batch = RecordBatch::try_new(Arc::new(schema), vec![array])?;

    Ok(batch)
}

pub fn bool_to_record_batch(value: bool, field_name: &str) -> ArrowResult<RecordBatch> {
    // Convert the bool value to a BooleanArray
    let array: ArrayRef = Arc::new(BooleanArray::from(vec![value]));

    // Define the schema of the record batch. This schema only has one field.
    let schema = Schema::new(vec![
        Field::new(field_name, DataType::Boolean, false),
    ]);

    // Create the record batch
    let batch = RecordBatch::try_new(Arc::new(schema), vec![array])?;

    Ok(batch)
}

/// Converts a State struct to an Arrow RecordBatch, including a node_id for each record.
pub fn state_to_record_batch_with_node_id(state: input_data::State, node_id: i32) -> ArrowResult<RecordBatch> {
    // Define the schema, now including a field for the node_id
    let schema = Schema::new(vec![
        Field::new("node_id", DataType::Int32, false),
        Field::new("in_max", DataType::Float64, false),
        Field::new("out_max", DataType::Float64, false),
        Field::new("state_loss_proportional", DataType::Float64, false),
        Field::new("state_max", DataType::Float64, false),
        Field::new("state_min", DataType::Float64, false),
        Field::new("initial_state", DataType::Float64, false),
        Field::new("is_temp", DataType::Boolean, false),
        Field::new("t_e_conversion", DataType::Float64, false),
        Field::new("residual_value", DataType::Float64, false),
    ]);

    // Arrays for the State fields
    let node_id_array = Int32Array::from(vec![node_id]); // Correctly use Int32Array for node_id
    let in_max_array = Float64Array::from(vec![state.in_max]);
    let out_max_array = Float64Array::from(vec![state.out_max]);
    let state_loss_proportional_array = Float64Array::from(vec![state.state_loss_proportional]);
    let state_max_array = Float64Array::from(vec![state.state_max]);
    let state_min_array = Float64Array::from(vec![state.state_min]);
    let initial_state_array = Float64Array::from(vec![state.initial_state]);
    let is_temp_array = BooleanArray::from(vec![state.is_temp]);
    let t_e_conversion_array = Float64Array::from(vec![state.t_e_conversion]);
    let residual_value_array = Float64Array::from(vec![state.residual_value]);

    // Include the node_id array in the list of arrays for the RecordBatch
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(node_id_array),
        Arc::new(in_max_array),
        Arc::new(out_max_array),
        Arc::new(state_loss_proportional_array),
        Arc::new(state_max_array),
        Arc::new(state_min_array),
        Arc::new(initial_state_array),
        Arc::new(is_temp_array),
        Arc::new(t_e_conversion_array),
        Arc::new(residual_value_array),
    ];

    RecordBatch::try_new(Arc::new(schema), arrays)
}

/// Converts TimeSeriesData to an Arrow RecordBatch, including a node_id for each record.
pub fn time_series_data_to_record_batch_with_node_id(ts_data: input_data::TimeSeriesData, node_id: i32) -> ArrowResult<RecordBatch> {
    // Flatten the data
    let mut scenarios = Vec::new();
    let mut dates = Vec::new();
    let mut values = Vec::new();
    let mut node_ids = Vec::new(); // Vector to hold the node_id for each record
    
    for ts in ts_data.ts_data {
        for (date, value) in ts.series {
            scenarios.push(ts.scenario.clone());
            dates.push(date);
            values.push(value);
            node_ids.push(node_id); // Associate each record with the node_id
        }
    }

    // Create Arrow arrays for each column, including the node_id column
    let scenario_array: ArrayRef = Arc::new(StringArray::from(scenarios));
    let date_array: ArrayRef = Arc::new(StringArray::from(dates));
    let value_array: ArrayRef = Arc::new(Float64Array::from(values));
    let node_id_array: ArrayRef = Arc::new(Int32Array::from(node_ids)); // Array for node_ids

    // Define the schema, now including a field for the node_id
    let schema = Schema::new(vec![
        Field::new("node_id", DataType::Int32, false),
        Field::new("scenario", DataType::Utf8, false),
        Field::new("date", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]);

    // Create the RecordBatch with the node_id column included
    RecordBatch::try_new(Arc::new(schema), vec![node_id_array, scenario_array, date_array, value_array])
}

/* 
pub fn convert_test_input_data_to_record_batch(input_data: &TestInputData) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let node_keys: Vec<String> = input_data.nodes.keys().cloned().collect();
    let node_names: Vec<String> = input_data.nodes.values().map(|v| v.name.clone()).collect();
    let node_values: Vec<i32> = input_data.nodes.values().map(|v| v.value).collect();

    // Metadata is repeated for each node to keep the table size consistent
    let metadata: Vec<String> = vec![input_data.metadata.clone(); input_data.nodes.len()];

    let node_keys_array: ArrayRef = Arc::new(StringArray::from(node_keys));
    let node_names_array: ArrayRef = Arc::new(StringArray::from(node_names));
    let node_values_array: ArrayRef = Arc::new(Int32Array::from(node_values));
    let metadata_array: ArrayRef = Arc::new(StringArray::from(metadata));

    let fields = vec![
        Field::new("node_key", DataType::Utf8, false),
        Field::new("node_name", DataType::Utf8, false),
        Field::new("node_value", DataType::Int32, false),
        Field::new("metadata", DataType::Utf8, false),
    ];
    let schema = Arc::new(Schema::new(fields));

    let record_batch = RecordBatch::try_new(schema, vec![node_keys_array, node_names_array, node_values_array, metadata_array])?;

    Ok(record_batch)
}
*/

pub fn read_input_data_from_yaml<P: AsRef<Path>>(path: P) -> Result<InputData, Box<dyn Error>> {
    // Open the file
    let mut file = File::open(path)?;
    
    // Read the contents into a string
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    
    // Deserialize the YAML string into InputData
    let input_data: InputData = serde_yaml::from_str(&contents)?;
    
    Ok(input_data)
}


/* 
pub fn nodes_to_record_batches(nodes: HashMap<String, input_data::Node>) -> ArrowResult<Vec<RecordBatch>> {
    let mut batches = Vec::new();

    for (index, (key, node)) in nodes.iter().enumerate() {
        let node_id = index as i32; // Example ID generation
        let name_array: ArrayRef = Arc::new(StringArray::from(vec![node.name.clone()]));

        // Directly convert boolean fields to BooleanArray here, rather than using bool_to_record_batch
        let is_commodity_array: ArrayRef = Arc::new(BooleanArray::from(vec![node.is_commodity]));
        let is_state_array: ArrayRef = Arc::new(BooleanArray::from(vec![node.is_state]));
        let is_res_array: ArrayRef = Arc::new(BooleanArray::from(vec![node.is_res]));
        let is_market_array: ArrayRef = Arc::new(BooleanArray::from(vec![node.is_market]));
        let is_inflow_array: ArrayRef = Arc::new(BooleanArray::from(vec![node.is_inflow]));

        let cost_batch = time_series_data_to_record_batch(node.cost.clone(), node_id)?;
        let inflow_batch = time_series_data_to_record_batch(node.inflow.clone(), node_id)?;
        let state_batch = state_to_record_batch(node.state.clone(), node_id)?;

        batches.push(cost_batch);
        batches.push(inflow_batch);
        batches.push(state_batch);

        // Define the schema for the node
        let schema = Schema::new(vec![
            Field::new("node_id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("is_commodity", DataType::Boolean, false),
            Field::new("is_state", DataType::Boolean, false),
            Field::new("is_res", DataType::Boolean, false),
            Field::new("is_market", DataType::Boolean, false),
            Field::new("is_inflow", DataType::Boolean, false),
            // Add other fields as necessary...
        ]);

        // Create the RecordBatch for the node
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(vec![node_id])), name_array, is_commodity_array /*, other arrays */],
        )?;

        batches.push(batch);
    }

    Ok(batches)
}
*/

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, Array, Float64Array, BooleanArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow::error::Result as ArrowResult;
    use std::sync::Arc;

    #[test]
    pub fn test_time_series_data_to_record_batch() {
        // Setup sample TimeSeriesData
        let ts_data = input_data::TimeSeriesData {
            ts_data: vec![
                input_data::TimeSeries {
                    scenario: "Scenario A".to_string(),
                    series: vec![
                        ("2021-01-01".to_string(), 100.0),
                        ("2021-01-02".to_string(), 110.0),
                    ],
                },
                input_data::TimeSeries {
                    scenario: "Scenario B".to_string(),
                    series: vec![
                        ("2021-01-01".to_string(), 200.0),
                        ("2021-01-02".to_string(), 210.0),
                    ],
                },
            ],
        };

        // Convert TimeSeriesData to RecordBatch
        let batch = time_series_data_to_record_batch(ts_data).expect("Failed to create RecordBatch");

        // Validate schema
        assert_eq!(batch.schema().fields().len(), 3, "Schema should have three fields");
        assert_eq!(batch.schema().fields()[0].as_ref(), &Field::new("scenario", DataType::Utf8, false), "Field schema does not match expected for 'scenario'");
        assert_eq!(batch.schema().fields()[1].as_ref(), &Field::new("date", DataType::Utf8, false), "Field schema does not match expected for 'date'");
        assert_eq!(batch.schema().fields()[2].as_ref(), &Field::new("value", DataType::Float64, false), "Field schema does not match expected for 'value'");

        // Validate data
        // Scenario column
        let scenario_col = batch.column(0);
        let scenario_col = scenario_col.as_any().downcast_ref::<StringArray>().expect("Failed to downcast");
        assert_eq!(scenario_col.value(0), "Scenario A");
        assert_eq!(scenario_col.value(2), "Scenario B");

        // Date column
        let date_col = batch.column(1);
        let date_col = date_col.as_any().downcast_ref::<StringArray>().expect("Failed to downcast");
        assert_eq!(date_col.value(0), "2021-01-01");
        assert_eq!(date_col.value(1), "2021-01-02");

        // Value column
        let value_col = batch.column(2);
        let value_col = value_col.as_any().downcast_ref::<Float64Array>().expect("Failed to downcast");
        assert_eq!(value_col.value(0), 100.0);
        assert_eq!(value_col.value(3), 210.0);
    }

    #[test]
    pub fn test_vec_to_record_batch() {
        // Setup the test input: a Vec<String> of timeseries data
        let timeseries_data = vec!["2021-01-01".to_string(), "2021-01-02".to_string(), "2021-01-03".to_string()];
        
        // Call the function to test
        let batch_result = vec_to_record_batch(timeseries_data.clone()).expect("Failed to create RecordBatch");
        
        // Check that the schema is correct
        assert_eq!(batch_result.schema().fields().len(), 1, "Schema should have exactly one field");
        assert_eq!(batch_result.schema().fields()[0].as_ref(), &Field::new("timeseries", DataType::Utf8, false), "Field schema does not match expected");
        
        // Check the data
        let column = batch_result.column(0);
        let column = column.as_any().downcast_ref::<StringArray>().expect("Failed to downcast to StringArray");
        assert_eq!(column.len(), timeseries_data.len(), "Column length does not match input data length");
        
        // Check each value
        for (i, value) in timeseries_data.iter().enumerate() {
            assert_eq!(column.value(i), *value, "Column value does not match input data value at index {}", i);
        }
    }

    #[test]
    pub fn test_bool_to_record_batch() {
        // Example usage of the test
        let field_name = "contains_online";
        let batch = bool_to_record_batch(true, field_name).expect("Failed to create RecordBatch");

        // Verify schema is correct
        assert_eq!(batch.schema().fields().len(), 1, "Schema should have exactly one field");
        assert_eq!(batch.schema().fields()[0].as_ref(), &Field::new(field_name, DataType::Boolean, false), "Field schema does not match expected");

        // Verify data
        let column = batch.column(0);
        let column = column.as_any().downcast_ref::<BooleanArray>().expect("Failed to downcast to BooleanArray");
        assert_eq!(column.len(), 1, "Column length should be 1");
        assert_eq!(column.value(0), true, "Column value does not match input value");
    }


    #[test]
    pub fn test_state_to_record_batch() {
        // Create a sample State instance
        let state_sample = input_data::State {
            in_max: 100.0,
            out_max: 80.0,
            state_loss_proportional: 0.01,
            state_max: 120.0,
            state_min: 20.0,
            initial_state: 50.0,
            is_temp: true,
            t_e_conversion: 0.95,
            residual_value: 10.0,
        };

        // Convert the State instance to a RecordBatch
        let batch = state_to_record_batch(state_sample).expect("Failed to create RecordBatch");

        // Check that the schema is correct
        let expected_schema = Schema::new(vec![
            Field::new("in_max", DataType::Float64, false),
            Field::new("out_max", DataType::Float64, false),
            Field::new("state_loss_proportional", DataType::Float64, false),
            Field::new("state_max", DataType::Float64, false),
            Field::new("state_min", DataType::Float64, false),
            Field::new("initial_state", DataType::Float64, false),
            Field::new("is_temp", DataType::Boolean, false),
            Field::new("t_e_conversion", DataType::Float64, false),
            Field::new("residual_value", DataType::Float64, false),
        ]);

        assert_eq!(batch.schema().as_ref(), &expected_schema);

        // Check the data in each column
        let check_float_column = |idx: usize, expected_val: f64| {
            let col = batch.column(idx);
            let col = col.as_any().downcast_ref::<Float64Array>().expect("Failed to downcast");
            assert_eq!(col.value(0), expected_val);
        };

        let check_bool_column = |idx: usize, expected_val: bool| {
            let col = batch.column(idx);
            let col = col.as_any().downcast_ref::<BooleanArray>().expect("Failed to downcast");
            assert_eq!(col.value(0), expected_val);
        };

        check_float_column(0, 100.0);
        check_float_column(1, 80.0);
        check_float_column(2, 0.01);
        check_float_column(3, 120.0);
        check_float_column(4, 20.0);
        check_float_column(5, 50.0);
        check_bool_column(6, true);
        check_float_column(7, 0.95);
        check_float_column(8, 10.0);
    }

    #[test]
    pub fn test_time_series_to_record_batch() {
        // Setup sample TimeSeries data
        let sample_ts = input_data::TimeSeries {
            scenario: "Base".to_string(),
            series: vec![
                ("2021-01-01".to_string(), 100.0),
                ("2021-01-02".to_string(), 110.0),
            ],
        };
    
        // Convert TimeSeries to RecordBatch
        let batch = time_series_to_record_batch(sample_ts).expect("Failed to create RecordBatch");
    
        // Validate schema
        assert_eq!(batch.schema().fields().len(), 2, "Schema should have two fields");
        assert_eq!(batch.schema().fields()[0].as_ref(), &Field::new("date", DataType::Utf8, false), "Field schema does not match expected for 'date'");
        assert_eq!(batch.schema().fields()[1].as_ref(), &Field::new("value", DataType::Float64, false), "Field schema does not match expected for 'value'");
    
        // Validate data
        let date_col = batch.column(0);
        let date_col = date_col.as_any().downcast_ref::<StringArray>().expect("Failed to downcast");
        assert_eq!(date_col.value(0), "2021-01-01");
        assert_eq!(date_col.value(1), "2021-01-02");
    
        let value_col = batch.column(1);
        let value_col = value_col.as_any().downcast_ref::<Float64Array>().expect("Failed to downcast");
        assert_eq!(value_col.value(0), 100.0);
        assert_eq!(value_col.value(1), 110.0);
    }    
}