use arrow::array::{StringArray, Float64Array, Int32Array, BooleanArray, ArrayRef, Array};
use arrow::{datatypes::{DataType, Field, Schema}, error::ArrowError, record_batch::RecordBatch};
use std::sync::Arc;
use arrow::error::Result as ArrowResult;
use crate::input_data;
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

// Define the new function
pub fn create_and_encode_inputdatasetup() -> Result<String, Box<dyn Error>> {
    // Create a test instance of InputDataSetup
    let setup = create_test_inputdatasetup();

    // Convert the InputDataSetup to a RecordBatch
    let batch: RecordBatch = inputdatasetup_to_arrow(&setup)?;

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
    let nodes = create_test_nodes_hashmap();

    // Convert the InputDataSetup to a RecordBatch
    let batch: RecordBatch = nodes_to_arrow(&nodes)?;

    // Serialize the RecordBatch to a Vec<u8>
    let arrow_data: Vec<u8> = serialize_record_batch_to_vec(&batch)?;

    // Encode the Vec<u8> into a base64 String
    let encoded_arrow_data: String = encode(&arrow_data);

    // Return the base64 encoded string
    Ok(encoded_arrow_data)
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestNode {
    pub name: String,
    pub value: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestInputData {
    pub nodes: HashMap<String, TestNode>,
    pub metadata: String,
}

pub fn create_example_input_data() -> TestInputData {
    let nodes = HashMap::from([
        ("node1".to_string(), TestNode { name: "Node One".to_string(), value: 100 }),
        ("node2".to_string(), TestNode { name: "Node Two".to_string(), value: 200 }),
    ]);
    TestInputData {
        nodes,
        metadata: "Example Metadata".to_string(),
    }
}

// Function to create a test instance of InputDataSetup
pub fn create_test_inputdatasetup() -> input_data::InputDataSetup {
    input_data::InputDataSetup {
        contains_reserves: true,
        contains_online: true,
        contains_states: false,
        contains_piecewise_eff: true,
        contains_risk: false,
        contains_diffusion: true,
        contains_delay: false,
        contains_markets: true,
        reserve_realisation: true,
        use_market_bids: true,
        common_timesteps: 10,
        common_scenario_name: "TestScenario".to_string(),
        use_node_dummy_variables: true,
        use_ramp_dummy_variables: false,
    }
}

pub fn create_test_nodes_hashmap() -> HashMap<String, input_data::NodeNew> {
    let mut nodes: HashMap<String, input_data::NodeNew> = HashMap::new();

    let node1_state = create_statenew();
    let node2_state = create_statenew();

    // Example nodes
    let node1 = input_data::NodeNew {
        name: "Node1".to_string(),
        is_commodity: true,
        is_state: false,
        is_res: false,
        is_market: false,
        is_inflow: true,
        state: node1_state.clone(),
    };

    let node2 = input_data::NodeNew {
        name: "Node2".to_string(),
        is_commodity: false,
        is_state: true,
        is_res: false,
        is_market: false,
        is_inflow: false,
        state: node2_state.clone(),
    };

    // Insert nodes into the hashmap
    nodes.insert(node1.name.clone(), node1);
    nodes.insert(node2.name.clone(), node2);

    nodes
}

pub fn create_statenew() -> input_data::StateNew {
    // Initialize default values for StateNew
    input_data::StateNew {
        state_max: 100.0,
        state_min: 0.0,
        in_max: 50.0,
        out_max: 50.0,
        initial_state: 25.0,
        state_loss_proportional: 0.1,
        scenario_independent_state: true,
        is_temp: false,
        t_e_conversion: 0.8,
        residual_value: 10.0,
    }
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