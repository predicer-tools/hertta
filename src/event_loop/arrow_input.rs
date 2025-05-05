mod arrow_errors;

use crate::input_data;
use crate::input_data::{Forecastable, InputData, Market, TimeSeriesData};
use crate::{TimeLine, TimeStamp};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMillisecondArray, UnionArray,
};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit, UnionFields};
use arrow::{error::ArrowError, record_batch::RecordBatch};
use arrow_errors::DataConversionError;
use arrow_ipc::writer::StreamWriter;
use std::collections::BTreeSet;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use indexmap::IndexMap;

// Function to create and serialize multiple RecordBatches
pub fn create_and_serialize_record_batches(
    input_data: &InputData,
) -> Result<Vec<(String, Vec<u8>)>, Box<dyn std::error::Error + Send + Sync>> {
    let batches = create_record_batches(input_data)?;
    let mut serialized_batches = Vec::new();

    for (key, batch) in batches {
        let buffer = serialize_batch_to_buffer(&batch)?;
        serialized_batches.push((key, buffer));
    }

    Ok(serialized_batches)
}

// Function to create RecordBatches (implement your Arrow conversion functions)
fn create_record_batches(
    input_data: &InputData,
) -> Result<Vec<(String, RecordBatch)>, Box<dyn std::error::Error + Send + Sync>> {
    let mut batches = Vec::new();

    batches.push(("temps".to_string(), temps_to_arrow(&input_data)?));
    batches.push(("setup".to_string(), inputdatasetup_to_arrow(&input_data)?));
    batches.push(("nodes".to_string(), nodes_to_arrow(&input_data)?));
    batches.push(("processes".to_string(), processes_to_arrow(&input_data)?));
    batches.push(("groups".to_string(), groups_to_arrow(&input_data)?));
    batches.push((
        "process_topology".to_string(),
        process_topos_to_arrow(&input_data)?,
    ));
    batches.push((
        "node_history".to_string(),
        node_histories_to_arrow(&input_data)?,
    ));
    batches.push(("node_delay".to_string(), node_delays_to_arrow(&input_data)?));
    batches.push((
        "node_diffusion".to_string(),
        node_diffusion_to_arrow(&input_data)?,
    ));
    batches.push((
        "inflow_blocks".to_string(),
        inflow_blocks_to_arrow(&input_data)?,
    ));
    batches.push(("markets".to_string(), markets_to_arrow(&input_data)?));
    batches.push((
        "reserve_realisation".to_string(),
        market_realisation_to_arrow(&input_data)?,
    ));
    batches.push((
        "reserve_activation_price".to_string(),
        market_reserve_activation_price_to_arrow(&input_data)?,
    ));
    batches.push(("scenarios".to_string(), scenarios_to_arrow(&input_data)?));
    batches.push((
        "efficiencies".to_string(),
        processes_eff_fun_to_arrow(&input_data)?,
    ));
    batches.push((
        "reserve_type".to_string(),
        reserve_type_to_arrow(&input_data)?,
    ));
    batches.push(("risk".to_string(), risk_to_arrow(&input_data)?));
    batches.push(("cap_ts".to_string(), processes_cap_to_arrow(&input_data)?));
    batches.push((
        "gen_constraint".to_string(),
        gen_constraints_to_arrow(&input_data)?,
    ));
    batches.push((
        "constraints".to_string(),
        constraints_to_arrow(&input_data)?,
    ));
    batches.push(("bid_slots".to_string(), bid_slots_to_arrow(&input_data)?));
    batches.push(("cf".to_string(), processes_cf_to_arrow(&input_data)?));
    batches.push(("inflow".to_string(), nodes_inflow_to_arrow(&input_data)?));
    batches.push((
        "market_prices".to_string(),
        market_price_to_arrow(&input_data)?,
    ));
    batches.push((
        "price".to_string(),
        nodes_commodity_price_to_arrow(&input_data)?,
    ));
    batches.push(("eff_ts".to_string(), processes_eff_to_arrow(&input_data)?));
    batches.push(("fixed_ts".to_string(), market_fixed_to_arrow(&input_data)?));
    batches.push((
        "balance_prices".to_string(),
        market_balance_price_to_arrow(&input_data)?,
    ));

    Ok(batches)
}

// Function to serialize the batch to a buffer
pub fn serialize_batch_to_buffer(
    batch: &RecordBatch,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let schema = batch.schema();
    let schema_ref: &Schema = schema.as_ref();
    let mut buffer: Vec<u8> = Vec::new();
    {
        let mut stream_writer = StreamWriter::try_new(&mut buffer, schema_ref)?;
        stream_writer.write(batch)?;
        stream_writer.finish()?;
    } // `stream_writer` is dropped here
    Ok(buffer)
}

fn make_timestamp_field() -> Field {
    Field::new("t", DataType::Timestamp(TimeUnit::Millisecond, None), false)
}

fn times_stamp_array_from_temporal_stamps(stamps: &TimeLine) -> Arc<TimestampMillisecondArray> {
    Arc::new(stamps.iter().map(|s| Some(s.timestamp_millis())).collect())
}

fn temps_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let fields = vec![make_timestamp_field()];
    let schema = Arc::new(Schema::new(fields));
    let stamp_column = times_stamp_array_from_temporal_stamps(&input_data.temporals.t) as ArrayRef;
    let columns = vec![stamp_column];
    RecordBatch::try_new(schema, columns)
}

fn inputdatasetup_to_arrow(input_data: &InputData) -> Result<RecordBatch, DataConversionError> {
    let setup = &input_data.setup;
    let parameters = vec![
        "use_reserves",        // boolean index 0
        "contains_online",          // boolean index 1
        "contains_states",          // boolean index 2
        "contains_piecewise_eff",   // boolean index 3
        "contains_risk",            // boolean index 4
        "contains_diffusion",       // boolean index 5
        "contains_delay",           // boolean index 6
        "contains_markets",         // boolean index 7
        "use_reserve_realisation",  // boolean index 8
        "use_market_bids",          // boolean index 9
        "common_timesteps",         // int index 0
        "common_scenario_name",     // string index 0
        "use_node_dummy_variables", // boolean index 10
        "use_ramp_dummy_variables", // boolean index 11
        "node_dummy_variable_cost", // float index 0
        "ramp_dummy_variable_cost", // float index 1
    ];

    let bool_array = BooleanArray::from(vec![
        setup.contains_reserves,
        setup.contains_online,
        setup.contains_states,
        setup.contains_piecewise_eff,
        setup.contains_risk,
        setup.contains_diffusion,
        setup.contains_delay,
        setup.contains_markets,
        setup.reserve_realisation,
        setup.use_market_bids,
        setup.use_node_dummy_variables,
        setup.use_ramp_dummy_variables,
    ]);
    
    let float_array = Float64Array::from(vec![
        setup.node_dummy_variable_cost,
        setup.ramp_dummy_variable_cost,
    ]);
    
    let int_array = Int64Array::from(vec![setup.common_timesteps]);
    
    let scenario_name = if setup.common_scenario_name.is_empty() {
        "missing".to_string()
    } else {
        setup.common_scenario_name.clone()
    };
    let str_array = StringArray::from(vec![scenario_name]);

    let union_fields = [
        (0, Arc::new(Field::new("bool", DataType::Boolean, false))),
        (1, Arc::new(Field::new("float", DataType::Float64, false))),
        (2, Arc::new(Field::new("int", DataType::Int64, false))),
        (3, Arc::new(Field::new("str", DataType::Utf8, false))),
    ]
    .into_iter()
    .collect::<UnionFields>();

    // Booleans: 0, Floats: 1, Int: 2, String: 3.
    let type_ids = vec![
        0, // use_reserves
        0, // contains_online
        0, // contains_states
        0, // contains_piecewise_eff
        0, // contains_risk
        0, // contains_diffusion
        0, // contains_delay
        0, // contains_markets
        0, // reserve_realisation
        0, // use_market_bids
        2, // common_timesteps
        3, // common_scenario_name
        0, // use_node_dummy_variables
        0, // use_ramp_dummy_variables
        1, // node_dummy_variable_cost
        1, // ramp_dummy_variable_cost
    ];
    let offsets = vec![
        0,  // use_reserves -> bool[0]
        1,  // contains_online -> bool[1]
        2,  // contains_states -> bool[2]
        3,  // contains_piecewise_eff -> bool[3]
        4,  // contains_risk -> bool[4]
        5,  // contains_diffusion -> bool[5]
        6,  // contains_delay -> bool[6]
        7,  // contains_markets -> bool[7]
        8,  // reserve_realisation -> bool[8]
        9,  // use_market_bids -> bool[9]
        0,  // common_timesteps -> int[0]
        0,  // common_scenario_name -> str[0]
        10, // use_node_dummy_variables -> bool[10]
        11, // use_ramp_dummy_variables -> bool[11]
        0,  // node_dummy_variable_cost -> float[0]
        1,  // ramp_dummy_variable_cost -> float[1]
    ];
    
    let type_ids = ScalarBuffer::from(type_ids);
    let offsets = ScalarBuffer::from(offsets);
    
    let children = vec![
        Arc::new(bool_array) as ArrayRef,
        Arc::new(float_array),
        Arc::new(int_array),
        Arc::new(str_array),
    ];
    let values = UnionArray::try_new(union_fields, type_ids, Some(offsets), children)
        .unwrap();

    if parameters.len() != values.len() {
        return Err(DataConversionError::InvalidInput(
            "Mismatched parameters and values lengths".to_string(),
        ));
    }
    let parameter_array = Arc::new(StringArray::from(parameters)) as ArrayRef;
    let value_array = Arc::new(values) as ArrayRef;
    let schema = Schema::new(vec![
        Field::new("parameter", DataType::Utf8, false),
        Field::new("value", value_array.data_type().clone(), true),
    ]);
    RecordBatch::try_new(Arc::new(schema), vec![parameter_array, value_array])
        .map_err(DataConversionError::from)
}

//What are the default values if State is None?
//How to add the node.groups here?
fn nodes_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let nodes = &input_data.nodes;
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
    let size = nodes.len();
    let mut names: Vec<String> = Vec::with_capacity(size);
    let mut is_commodity: Vec<bool> = Vec::with_capacity(size);
    let mut is_state: Vec<bool> = Vec::with_capacity(size);
    let mut is_res: Vec<bool> = Vec::with_capacity(size);
    let mut is_market: Vec<bool> = Vec::with_capacity(size);
    let mut is_inflow: Vec<bool> = Vec::with_capacity(size);
    let mut state_maxs: Vec<f64> = Vec::with_capacity(size);
    let mut state_mins: Vec<f64> = Vec::with_capacity(size);
    let mut in_maxs: Vec<f64> = Vec::with_capacity(size);
    let mut out_maxs: Vec<f64> = Vec::with_capacity(size);
    let mut initial_states: Vec<f64> = Vec::with_capacity(size);
    let mut state_loss_proportionals: Vec<f64> = Vec::with_capacity(size);
    let mut scenario_independent_states: Vec<bool> = Vec::with_capacity(size);
    let mut is_temps: Vec<bool> = Vec::with_capacity(size);
    let mut t_e_conversions: Vec<f64> = Vec::with_capacity(size);
    let mut residual_values: Vec<f64> = Vec::with_capacity(size);
    for (node_name, node) in nodes.iter() {
        names.push(node_name.clone());
        is_commodity.push(node.is_commodity);
        is_state.push(node.is_state);
        is_res.push(node.is_res);
        is_market.push(node.is_market);
        is_inflow.push(node.is_inflow);

        if let Some(state) = &node.state {
            state_maxs.push(state.state_max);
            state_mins.push(state.state_min);
            in_maxs.push(state.in_max);
            out_maxs.push(state.out_max);
            initial_states.push(state.initial_state);
            state_loss_proportionals.push(state.state_loss_proportional);
            scenario_independent_states.push(state.is_scenario_independent);
            is_temps.push(state.is_temp);
            t_e_conversions.push(state.t_e_conversion);
            residual_values.push(state.residual_value);
        } else {
            state_maxs.push(0.0);
            state_mins.push(0.0);
            in_maxs.push(0.0);
            out_maxs.push(0.0);
            initial_states.push(0.0);
            state_loss_proportionals.push(0.0);
            scenario_independent_states.push(false);
            is_temps.push(false);
            t_e_conversions.push(1.0);
            residual_values.push(0.0);
        }
    }
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
    let state_loss_proportionals_array =
        Arc::new(Float64Array::from(state_loss_proportionals)) as ArrayRef;
    let scenario_independent_states_array =
        Arc::new(BooleanArray::from(scenario_independent_states)) as ArrayRef;
    let is_temps_array = Arc::new(BooleanArray::from(is_temps)) as ArrayRef;
    let t_e_conversions_array = Arc::new(Float64Array::from(t_e_conversions)) as ArrayRef;
    let residual_values_array = Arc::new(Float64Array::from(residual_values)) as ArrayRef;
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
    )?;
    Ok(record_batch)
}

fn processes_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let processes = &input_data.processes;
    let contains_delay = input_data.setup.contains_delay;

    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("process", DataType::Utf8, false),
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
        Field::new("initial_state", DataType::Boolean, false),
        Field::new("scenario_independent_online", DataType::Boolean, false),
        Field::new("delay", DataType::Boolean, false), // New column for delay
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
    let mut initial_states: Vec<bool> = Vec::new();
    let mut scenario_independent_onlines: Vec<bool> = Vec::new();
    let mut delays: Vec<bool> = Vec::new(); // New vector for delays

    for (process_name, process) in processes.iter() {
        names.push(process_name.clone());
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
        scenario_independent_onlines.push(process.is_scenario_independent);
        delays.push(contains_delay); // Set delay value based on input_data.setup.contains_delay
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
    let initial_states_array = Arc::new(BooleanArray::from(initial_states)) as ArrayRef;
    let scenario_independent_onlines_array =
        Arc::new(BooleanArray::from(scenario_independent_onlines)) as ArrayRef;
    let delays_array = Arc::new(BooleanArray::from(delays)) as ArrayRef; // New array for delays

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
            delays_array, // Add delays array to the record batch
        ],
    )?;

    Ok(record_batch)
}

// Function to convert HashMap<String, Group> to RecordBatch
fn groups_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let groups = &input_data.groups;

    let schema = Schema::new(vec![
        Field::new("group_type", DataType::Utf8, false),
        Field::new("entity", DataType::Utf8, false),
        Field::new("group", DataType::Utf8, false),
    ]);

    let mut row_count: usize = 0;
    for group in groups.values() {
        row_count += group.members.len()
    }
    let mut types: Vec<String> = Vec::with_capacity(row_count);
    let mut entities: Vec<String> = Vec::with_capacity(row_count);
    let mut group_names: Vec<String> = Vec::with_capacity(row_count);
    for group in groups.values() {
        for member in &group.members {
            types.push(group.g_type.to_string());
            entities.push(member.clone());
            group_names.push(group.name.clone());
        }
    }
    let types_array = Arc::new(StringArray::from(types)) as ArrayRef;
    let entities_array = Arc::new(StringArray::from(entities)) as ArrayRef;
    let group_names_array = Arc::new(StringArray::from(group_names)) as ArrayRef;

    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![types_array, entities_array, group_names_array],
    )?;

    Ok(record_batch)
}

fn process_topos_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let process_topologys = &input_data.processes;

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

    // Extract data from the HashMap
    for (process_name, process) in process_topologys {
        for topo in &process.topos {
            // Add row for source if the process name is not the same as the source
            if process.name != topo.source {
                processes.push(process_name.clone());
                source_sinks.push("source".to_string());
                nodes.push(topo.source.clone());
                conversion_coeffs.push(1.0); // Example placeholder value for `conversion_coeff`
                capacities.push(topo.capacity);
                vom_costs.push(topo.vom_cost);
                ramp_ups.push(topo.ramp_up);
                ramp_downs.push(topo.ramp_down);
                initial_loads.push(topo.initial_load);
                initial_flows.push(topo.initial_flow);
            }

            // Add row for sink if the process name is not the same as the sink
            if process.name != topo.sink {
                processes.push(process_name.clone());
                source_sinks.push("sink".to_string());
                nodes.push(topo.sink.clone());
                conversion_coeffs.push(1.0); // Example placeholder value for `conversion_coeff`
                capacities.push(topo.capacity);
                vom_costs.push(topo.vom_cost);
                ramp_ups.push(topo.ramp_up);
                ramp_downs.push(topo.ramp_down);
                initial_loads.push(topo.initial_load);
                initial_flows.push(topo.initial_flow);
            }
        }
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

    // Create the RecordBatch using these arrays
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

fn node_diffusion_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let node_diffusions = &input_data.node_diffusion;
    let temporals_t = &input_data.temporals.t;
    let mut fields = vec![make_timestamp_field()];
    let mut columns: Vec<ArrayRef> =
        vec![times_stamp_array_from_temporal_stamps(temporals_t) as ArrayRef];
    if node_diffusions.is_empty() {
        let schema = Schema::new(fields);
        return RecordBatch::try_new(Arc::new(schema), columns);
    }
    let mut float_columns_data: HashMap<String, Vec<f64>> = HashMap::new();
    for node_diffusion in node_diffusions {
        for ts in &node_diffusion.coefficient.ts_data {
            check_timestamps_match(temporals_t, &ts)?;
            let column_name = format!(
                "{},{},{}",
                node_diffusion.node1, node_diffusion.node2, ts.scenario
            );
            fields.push(Field::new(&column_name, DataType::Float64, true));
            let mut column_data = vec![f64::NAN; temporals_t.len()];
            for (timestamp, value) in &ts.series {
                if let Some(pos) = temporals_t.iter().position(|t| t == timestamp) {
                    column_data[pos] = *value;
                }
            }
            if column_data.iter().any(|&x| !x.is_nan()) {
                float_columns_data.insert(column_name, column_data);
            }
        }
    }
    for (_key, val) in float_columns_data {
        columns.push(Arc::new(Float64Array::from(val)) as ArrayRef);
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}

// Function to convert node histories to Arrow RecordBatch
fn node_histories_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let node_histories = &input_data.node_histories;
    let mut fields = vec![Field::new("t", DataType::Int32, false)];
    let mut columns: Vec<ArrayRef> = Vec::new();
    let mut time_stamp_columns_data: BTreeMap<String, TimeLine> = BTreeMap::new();
    let mut float_columns_data: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    let mut total_rows = 0;
    for node_history in node_histories.values() {
        for ts in &node_history.steps.ts_data {
            let series_len = ts.series.len();
            total_rows = total_rows.max(series_len);
        }
    }
    let mut running_number = Vec::with_capacity(total_rows);
    for i in 0..total_rows {
        running_number.push(i as i32 + 1);
    }
    columns.push(Arc::new(Int32Array::from(running_number)) as ArrayRef);
    for node_history in node_histories.values() {
        for ts in &node_history.steps.ts_data {
            let column_name_time_stamp = format!("{},t,{}", node_history.node, ts.scenario);
            let column_name_float = format!("{},{}", node_history.node, ts.scenario);
            fields.push(Field::new(
                &column_name_time_stamp,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ));
            fields.push(Field::new(&column_name_float, DataType::Float64, false));
            let mut timestamps = Vec::<TimeStamp>::with_capacity(total_rows);
            let mut values = Vec::<f64>::with_capacity(total_rows);
            for (timestamp, value) in ts.series.iter() {
                timestamps.push(timestamp.clone());
                values.push(*value);
            }
            time_stamp_columns_data.insert(column_name_time_stamp, timestamps);
            float_columns_data.insert(column_name_float, values);
        }
    }
    for node_history in node_histories.values() {
        for ts in &node_history.steps.ts_data {
            let column_name_time_stamp = format!("{},t,{}", node_history.node, ts.scenario);
            let column_name_float = format!("{},{}", node_history.node, ts.scenario);
            if let Some(val) = time_stamp_columns_data.get(&column_name_time_stamp) {
                columns.push(times_stamp_array_from_temporal_stamps(val) as ArrayRef);
            }
            if let Some(val) = float_columns_data.get(&column_name_float) {
                columns.push(Arc::new(Float64Array::from(val.clone())) as ArrayRef);
            }
        }
    }
    let schema = Arc::new(Schema::new(fields));
    let record_batch = RecordBatch::try_new(schema, columns)?;
    Ok(record_batch)
}

fn node_delays_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let node_delays = &input_data.node_delay;
    let schema = Schema::new(vec![
        Field::new("node1", DataType::Utf8, false),
        Field::new("node2", DataType::Utf8, false),
        Field::new("delay_t", DataType::Float64, false),
        Field::new("min_flow", DataType::Float64, false),
        Field::new("max_flow", DataType::Float64, false),
    ]);
    if node_delays.is_empty() {
        let node1_array: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
        let node2_array: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
        let delay_array: ArrayRef = Arc::new(Float64Array::from(Vec::<f64>::new()));
        let min_flow_array: ArrayRef = Arc::new(Float64Array::from(Vec::<f64>::new()));
        let max_flow_array: ArrayRef = Arc::new(Float64Array::from(Vec::<f64>::new()));
        return RecordBatch::try_new(
            Arc::new(schema),
            vec![
                node1_array,
                node2_array,
                delay_array,
                min_flow_array,
                max_flow_array,
            ],
        );
    }
    let mut node1s: Vec<String> = Vec::new();
    let mut node2s: Vec<String> = Vec::new();
    let mut delays: Vec<f64> = Vec::new();
    let mut min_flows: Vec<f64> = Vec::new();
    let mut max_flows: Vec<f64> = Vec::new();
    for (node1, node2, delay, min_flow, max_flow) in node_delays {
        node1s.push(node1.clone());
        node2s.push(node2.clone());
        delays.push(*delay);
        min_flows.push(*min_flow);
        max_flows.push(*max_flow);
    }
    let node1_array: ArrayRef = Arc::new(StringArray::from(node1s));
    let node2_array: ArrayRef = Arc::new(StringArray::from(node2s));
    let delay_array: ArrayRef = Arc::new(Float64Array::from(delays));
    let min_flow_array: ArrayRef = Arc::new(Float64Array::from(min_flows));
    let max_flow_array: ArrayRef = Arc::new(Float64Array::from(max_flows));
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            node1_array,
            node2_array,
            delay_array,
            min_flow_array,
            max_flow_array,
        ],
    )
}

fn inflow_blocks_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let inflow_blocks = &input_data.inflow_blocks;
    if inflow_blocks.is_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::Int64, false)]));
        let running_number_for_t: Vec<i64> = (1..=10).collect();
        let t_array: ArrayRef = Arc::new(Int64Array::from(running_number_for_t));
        return RecordBatch::try_new(schema, vec![t_array]);
    }
    let mut scenario_names = BTreeMap::new(); // Use BTreeMap for consistent lexicographical ordering
    let mut common_timestamps: Option<TimeLine> = None;
    // Collect all unique scenario names and ensure timestamps are consistent across all inflow blocks
    for block in inflow_blocks.values() {
        for ts in &block.data.ts_data {
            scenario_names.insert(ts.scenario.clone(), ()); // BTreeMap will maintain lexicographical order
            let timestamps: TimeLine = ts.series.keys().cloned().collect();
            if let Some(ref common_ts) = common_timestamps {
                if *common_ts != timestamps {
                    return Err(ArrowError::ComputeError(
                        "Inconsistent timestamps across inflow blocks".to_string(),
                    ));
                }
            } else {
                common_timestamps = Some(timestamps);
            }
        }
    }

    let scenario_names: Vec<String> = scenario_names.keys().cloned().collect(); // Extract scenario names in lexicographical order
    let common_timestamps = common_timestamps
        .ok_or_else(|| ArrowError::ComputeError("No timestamps found".to_string()))?;

    // Create the schema with the `t` column and additional columns based on inflow blocks
    let mut fields: Vec<Field> = vec![Field::new("t", DataType::Int64, false)]; // `t` as running number

    for block in inflow_blocks.values() {
        // Add the block name, node (e.g., "b1,dh_sto") column
        fields.push(Field::new(
            &format!("{},{}", block.name, block.node),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ));

        // Add scenario columns (e.g., "b1,s1", "b1,s2", ...) in lexicographical order
        for scenario in &scenario_names {
            fields.push(Field::new(
                &format!("{},{}", block.name, scenario),
                DataType::Float64,
                false,
            ));
        }
    }

    let running_number_for_t: Vec<i64> = (1..=common_timestamps.len() as i64).collect();

    let mut start_times: BTreeMap<String, TimeLine> = BTreeMap::new(); // Use BTreeMap to preserve ordering
    let mut scenario_values: BTreeMap<String, BTreeMap<String, Vec<f64>>> = BTreeMap::new(); // Use BTreeMap to preserve ordering

    // Initialize data structures for each block and scenario
    for block in inflow_blocks.values() {
        start_times
            .entry(block.name.clone())
            .or_insert_with(Vec::new);

        let mut scenario_map = BTreeMap::new(); // Use BTreeMap for scenarios to preserve order
        for scenario in &scenario_names {
            scenario_map.insert(scenario.clone(), vec![]);
        }
        scenario_values
            .entry(block.name.clone())
            .or_insert(scenario_map);
    }

    // Populate the vectors for each inflow block and its scenarios
    for block in inflow_blocks.values() {
        let start_time_vec = start_times
            .get_mut(&block.name)
            .expect("block.name not found in start_times");

        let scenario_map = scenario_values
            .get_mut(&block.name)
            .expect("block.name not found in scenario_values");

        for t in &common_timestamps {
            start_time_vec.push(t.clone());

            for ts in &block.data.ts_data {
                if let Some(value) = ts.series.get(t) {
                    scenario_map
                        .get_mut(&ts.scenario)
                        .expect("Scenario not found in scenario_map")
                        .push(*value);
                } else {
                    return Err(ArrowError::ComputeError(format!(
                        "Timeseries mismatch for timestamp {} in temporal data",
                        t
                    )));
                }
            }
        }
    }

    // Create Arrow arrays from the vectors
    let t_array: ArrayRef = Arc::new(Int64Array::from(running_number_for_t));
    let mut columns: Vec<ArrayRef> = vec![t_array];

    for block in inflow_blocks.values() {
        // Create column for `block.name,block.node` (e.g., "b1,dh_sto")
        let start_times_array: ArrayRef = times_stamp_array_from_temporal_stamps(
            &start_times.remove(&block.name).unwrap_or_else(Vec::new),
        );
        columns.push(start_times_array);

        // Add scenario columns (e.g., "b1,s1", "b1,s2", ...) in lexicographical order
        for scenario in &scenario_names {
            let values = scenario_values
                .get(&block.name)
                .expect("block.name not found in scenario_values")
                .get(scenario)
                .expect("Scenario not found in scenario_map")
                .clone();
            columns.push(Arc::new(Float64Array::from(values)) as ArrayRef);
        }
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}

fn markets_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let markets = &input_data.markets;

    let schema = Schema::new(vec![
        Field::new("market", DataType::Utf8, false),
        Field::new("market_type", DataType::Utf8, false),
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

    for market in markets.values() {
        markets_vec.push(market.name.clone());
        m_types.push(market.m_type.clone());
        nodes.push(market.node.clone());
        processgroups.push(market.processgroup.clone());
        directions.push(market.direction.clone());
        reserve_types.push(market.reserve_type.clone());
        is_bids.push(market.is_bid);
        is_limiteds.push(market.is_limited);
        min_bids.push(market.min_bid);
        max_bids.push(market.max_bid);
        fees.push(market.fee);
    }

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

fn sort_unique_market_and_scenario_names(
    markets: &IndexMap<String, Market>,
) -> (Vec<String>, Vec<String>) {
    // Use BTreeSet to ensure lexicographical order for market and scenario names
    let mut scenario_names: BTreeSet<String> = BTreeSet::new();
    let mut market_names: BTreeSet<String> = BTreeSet::new();
    for market in markets.values() {
        for ts in &market.realisation.ts_data {
            scenario_names.insert(ts.scenario.clone());
        }
        market_names.insert(market.name.clone());
    }
    let scenario_names: Vec<String> = scenario_names.into_iter().collect();
    let market_names: Vec<String> = market_names.into_iter().collect();
    (market_names, scenario_names)
}

fn schema_fields_from_scenarios(
    market_names: &Vec<String>,
    scenario_names: &Vec<String>,
) -> Vec<Field> {
    let mut fields: Vec<Field> = vec![Field::new(
        "t",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )];
    for market in market_names {
        for scenario in scenario_names {
            fields.push(Field::new(
                &format!("{},{}", market, scenario),
                DataType::Float64,
                true,
            ));
        }
    }
    fields
}

fn market_realisation_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let markets = &input_data.markets;
    let temporals = &input_data.temporals;
    let (market_names, scenario_names) = sort_unique_market_and_scenario_names(markets);
    let fields = schema_fields_from_scenarios(&market_names, &scenario_names);
    let mut scenario_values: BTreeMap<String, BTreeMap<String, BTreeMap<TimeStamp, Option<f64>>>> =
        BTreeMap::new();
    let has_realisation_data = markets
        .values()
        .any(|market| !market.realisation.ts_data.is_empty());
    if !has_realisation_data {
        for market in &market_names {
            let mut market_map = BTreeMap::new();
            for scenario in &scenario_names {
                let mut value_map = BTreeMap::new();
                for timestamp in &temporals.t {
                    value_map.insert(timestamp.clone(), None);
                }
                market_map.insert(scenario.clone(), value_map);
            }
            scenario_values.insert(market.clone(), market_map);
        }
    } else {
        for market in markets.values() {
            for ts in &market.realisation.ts_data {
                check_timestamps_match(&temporals.t, ts)?;
                for (timestamp, value) in &ts.series {
                    let entry = scenario_values
                        .entry(market.name.clone())
                        .or_insert_with(BTreeMap::new)
                        .entry(ts.scenario.clone())
                        .or_insert_with(BTreeMap::new);
                    entry.insert(timestamp.clone(), Some(*value));
                }
            }
        }
    }

    // Ensure all scenario_values have entries for all timestamps
    for market in &market_names {
        let market_map = scenario_values
            .entry(market.clone())
            .or_insert_with(BTreeMap::new);
        for scenario in &scenario_names {
            let scenario_map = market_map
                .entry(scenario.clone())
                .or_insert_with(BTreeMap::new);
            for timestamp in &temporals.t {
                scenario_map.entry(timestamp.clone()).or_insert(None);
            }
        }
    }
    let timestamps_array: ArrayRef = times_stamp_array_from_temporal_stamps(&temporals.t);
    let mut columns: Vec<ArrayRef> = vec![timestamps_array];
    for market in market_names {
        for scenario in &scenario_names {
            let values = temporals
                .t
                .iter()
                .map(|timestamp| {
                    scenario_values[&market][scenario]
                        .get(timestamp)
                        .cloned()
                        .unwrap_or(None)
                })
                .collect::<Vec<Option<f64>>>();
            columns.push(Arc::new(Float64Array::from(values)) as ArrayRef);
        }
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}

fn market_reserve_activation_price_to_arrow(
    input_data: &input_data::InputData,
) -> Result<RecordBatch, ArrowError> {
    let markets = &input_data.markets;
    let temporals = &input_data.temporals;
    let (market_names, scenario_names) = sort_unique_market_and_scenario_names(markets);
    let fields = schema_fields_from_scenarios(&market_names, &scenario_names);
    let mut scenario_values: BTreeMap<String, BTreeMap<String, BTreeMap<TimeStamp, Option<f64>>>> =
        BTreeMap::new();

    // Handle the case where there are no reserve_activation_price data
    let has_reserve_activation_price_data = markets
        .values()
        .any(|market| !market.reserve_activation_price.ts_data.is_empty());
    if !has_reserve_activation_price_data {
        // Initialize scenario values with None
        for market in &market_names {
            let mut market_map = BTreeMap::new();
            for scenario in &scenario_names {
                let mut value_map = BTreeMap::new();
                for timestamp in &temporals.t {
                    value_map.insert(timestamp.clone(), None);
                }
                market_map.insert(scenario.clone(), value_map);
            }
            scenario_values.insert(market.clone(), market_map);
        }
    } else {
        // Collect timestamps and populate scenario values
        for market in markets.values() {
            for ts in &market.reserve_activation_price.ts_data {
                check_timestamps_match(&temporals.t, ts)?;
                for (timestamp, value) in &ts.series {
                    let entry = scenario_values
                        .entry(market.name.clone())
                        .or_insert_with(BTreeMap::new)
                        .entry(ts.scenario.clone())
                        .or_insert_with(BTreeMap::new);
                    entry.insert(timestamp.clone(), Some(*value));
                }
            }
        }
    }

    // Ensure all scenario_values have entries for all timestamps
    for market in &market_names {
        let market_map = scenario_values
            .entry(market.clone())
            .or_insert_with(BTreeMap::new);
        for scenario in &scenario_names {
            let scenario_map = market_map
                .entry(scenario.clone())
                .or_insert_with(BTreeMap::new);
            for timestamp in &temporals.t {
                scenario_map.entry(timestamp.clone()).or_insert(None);
            }
        }
    }

    // Create Arrow arrays from the vectors
    let timestamps_array: ArrayRef = times_stamp_array_from_temporal_stamps(&temporals.t);
    let mut columns: Vec<ArrayRef> = vec![timestamps_array];
    for market in &market_names {
        for scenario in &scenario_names {
            let values = temporals
                .t
                .iter()
                .map(|timestamp| {
                    scenario_values[market][scenario]
                        .get(timestamp)
                        .cloned()
                        .unwrap_or(None)
                })
                .collect::<Vec<Option<f64>>>();
            columns.push(Arc::new(Float64Array::from(values)) as ArrayRef);
        }
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}

fn scenarios_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let scenarios = &input_data.scenarios;

    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("probability", DataType::Float64, false),
    ]);

    // Initialize vectors to hold scenario data
    let mut names: Vec<String> = Vec::new();
    let mut probabilities: Vec<f64> = Vec::new();

    // Populate vectors with data from the BTreeMap
    for (name, probability) in scenarios.iter() {
        names.push(name.clone());
        probabilities.push(*probability);
    }

    // Create arrays from the vectors
    let names_array = Arc::new(StringArray::from(names)) as ArrayRef;
    let probabilities_array = Arc::new(Float64Array::from(probabilities)) as ArrayRef;

    // Check if both columns have the same length
    let names_len = names_array.len();
    let probabilities_len = probabilities_array.len();
    if names_len != probabilities_len {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Mismatched column lengths: 'name' ({}), 'probability' ({})",
            names_len, probabilities_len
        )));
    }

    // Create the RecordBatch using these arrays
    let record_batch =
        RecordBatch::try_new(Arc::new(schema), vec![names_array, probabilities_array])?;

    Ok(record_batch)
}

fn processes_eff_fun_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let processes = &input_data.processes;

    // Determine the maximum length of eff_fun across all processes that actually have data
    let max_length = processes
        .values()
        .filter(|p| !p.eff_fun.is_empty()) // Only consider processes with actual efficiency data
        .map(|p| p.eff_fun.len())
        .max()
        .unwrap_or(0);

    // Create the schema with dynamic columns based on the maximum length
    let mut fields: Vec<Field> = vec![Field::new("process", DataType::Utf8, false)];
    for i in 1..=max_length {
        fields.push(Field::new(&format!("{}", i), DataType::Float64, true));
    }

    // Initialize the column data
    let mut process_column: Vec<String> = Vec::new();
    let mut column_data: Vec<Vec<Option<f64>>> = vec![Vec::new(); max_length];

    if max_length > 0 {
        // Construct columns for operation points and efficiency values
        for (process_name, process) in processes {
            // Only add rows for processes that have data in `eff_fun`
            if !process.eff_fun.is_empty() {
                process_column.push(format!("{},op", process_name));
                process_column.push(format!("{},eff", process_name));

                for i in 0..max_length {
                    if i < process.eff_fun.len() {
                        let (op_point, eff_value) = process.eff_fun[i];
                        column_data[i].push(Some(op_point));
                        column_data[i].push(Some(eff_value));
                    } else {
                        column_data[i].push(None); // For op
                        column_data[i].push(None); // For eff
                    }
                }
            }
        }
    }

    // Create an Arrow column for the 'process' field
    let process_array = Arc::new(StringArray::from(process_column)) as ArrayRef;
    let mut columns: Vec<ArrayRef> = vec![process_array];

    // Create Arrow columns for each tuple index in eff_fun
    for col in column_data {
        let arrow_col = Arc::new(Float64Array::from(col)) as ArrayRef;
        columns.push(arrow_col);
    }

    // Create the schema from the fields
    let schema = Arc::new(Schema::new(fields));

    // Construct and return the RecordBatch
    RecordBatch::try_new(schema, columns)
}

fn reserve_type_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let reserve_type = &input_data.reserve_type;
    let schema = Schema::new(vec![
        Field::new("reserve_type", DataType::Utf8, false),
        Field::new("ramp_factor", DataType::Float64, false),
    ]);
    let mut types: Vec<String> = Vec::with_capacity(reserve_type.len());
    let mut ramp_factors: Vec<f64> = Vec::with_capacity(reserve_type.len());
    for (key, &value) in reserve_type.iter() {
        types.push(key.clone());
        ramp_factors.push(value);
    }
    let type_array: ArrayRef = Arc::new(StringArray::from(types));
    let ramp_factor_array: ArrayRef = Arc::new(Float64Array::from(ramp_factors));
    RecordBatch::try_new(Arc::new(schema), vec![type_array, ramp_factor_array])
}

fn risk_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    // Extract risk data from input_data
    let risk = &input_data.risk;

    // Check if the risk data is empty
    if risk.is_empty() {
        // Return an empty RecordBatch with no schema and no columns
        let schema = Arc::new(Schema::empty());
        let columns: Vec<ArrayRef> = vec![];
        return RecordBatch::try_new(schema, columns);
    }

    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("parameter", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]);

    // Initialize vectors to hold the data
    let mut parameters: Vec<String> = Vec::new();
    let mut values: Vec<f64> = Vec::new();

    // Populate the vectors from the BTreeMap
    for (key, &value) in risk.iter() {
        parameters.push(key.clone());
        values.push(value);
    }

    // Create Arrow arrays from the vectors
    let parameter_array: ArrayRef = Arc::new(StringArray::from(parameters));
    let value_array: ArrayRef = Arc::new(Float64Array::from(values));

    // Create the RecordBatch using these arrays and the schema
    let record_batch = RecordBatch::try_new(Arc::new(schema), vec![parameter_array, value_array])?;

    Ok(record_batch)
}

fn processes_cap_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let temporals = &input_data.temporals;
    let processes = &input_data.processes;

    let mut fields: Vec<Field> = vec![Field::new(
        "t",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )];
    let mut columns: Vec<ArrayRef> = vec![];
    let mut column_data: BTreeMap<String, Vec<Option<f64>>> = BTreeMap::new();

    let mut has_ts_data = false;

    for (process_name, process) in processes {
        if process.eff_ts.ts_data.is_empty() {
            continue;
        }
        for time_series in &process.eff_ts.ts_data {
            check_timestamps_match(&temporals.t, time_series)?;
        }
        has_ts_data = true;
        for topology in &process.topos {
            if topology.cap_ts.ts_data.is_empty() {
                continue;
            }
            let flow = if topology.sink == *process_name {
                &topology.source
            } else if topology.source == *process_name {
                &topology.sink
            } else {
                continue;
            };
            for time_series in &topology.cap_ts.ts_data {
                check_timestamps_match(&temporals.t, time_series)?;
                let column_name = format!("{},{},{}", process_name, flow, time_series.scenario);
                fields.push(Field::new(&column_name, DataType::Float64, true));

                let series_data: Vec<Option<f64>> = time_series
                    .series
                    .iter()
                    .map(|(_, value)| Some(*value))
                    .collect();

                if !series_data.is_empty() {
                    column_data.insert(column_name, series_data);
                }
            }
        }
    }
    let timestamp_column = times_stamp_array_from_temporal_stamps(&temporals.t) as ArrayRef;
    columns.push(timestamp_column);

    // Check if we have any data to insert
    if !has_ts_data || column_data.is_empty() {
        // Return a RecordBatch with only the t column
        let schema = Arc::new(Schema::new(fields));
        return RecordBatch::try_new(schema, columns);
    }

    // Create Arrow columns from the collected data, preserving order
    for data in column_data.values() {
        columns.push(Arc::new(Float64Array::from(data.clone())) as ArrayRef);
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}

fn gen_constraints_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let temporals = &input_data.temporals;
    let gen_constraints = &input_data.gen_constraints;

    let mut fields: Vec<Field> = Vec::new();
    let mut column_data: BTreeMap<String, Vec<Option<f64>>> = BTreeMap::new();

    fields.push(Field::new(
        "t",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    ));

    // Handle the constant TimeSeriesData for each GenConstraint
    for (constraint_name, gen_constraint) in gen_constraints.iter() {
        for ts in &gen_constraint.constant.ts_data {
            check_timestamps_match(&temporals.t, ts)?;
            let col_name = format!("{},{}", constraint_name, ts.scenario);
            fields.push(Field::new(&col_name, DataType::Float64, true));
            let series_data: Vec<Option<f64>> =
                ts.series.iter().map(|(_, value)| Some(*value)).collect();
            if !series_data.is_empty() {
                column_data.insert(col_name, series_data);
            }
        }
    }

    // Handle the ConFactor data
    for (constraint_name, gen_constraint) in gen_constraints.iter() {
        for factor in &gen_constraint.factors {
            for ts in &factor.data.ts_data {
                check_timestamps_match(&temporals.t, ts)?;
                let var_tuple_component = if factor.var_tuple.1.is_empty() {
                    factor.var_tuple.0.clone()
                } else {
                    format!("{},{}", factor.var_tuple.0, factor.var_tuple.1)
                };
                let col_name = format!(
                    "{},{},{}",
                    constraint_name, var_tuple_component, ts.scenario
                );
                fields.push(Field::new(&col_name, DataType::Float64, true));
                let series_data: Vec<Option<f64>> =
                    ts.series.iter().map(|(_, value)| Some(*value)).collect();
                if !series_data.is_empty() {
                    column_data.insert(col_name, series_data);
                }
            }
        }
    }
    let mut columns: Vec<ArrayRef> = Vec::new();
    columns.push(times_stamp_array_from_temporal_stamps(&temporals.t) as ArrayRef);
    if column_data.is_empty() {
        let schema = Arc::new(Schema::new(fields));
        return RecordBatch::try_new(schema, columns);
    }

    // Create other columns from the collected column_data
    for field in &fields[1..] {
        // Skip the timestamp field
        if let Some(col_data) = column_data.remove(field.name()) {
            let col_array = Float64Array::from(col_data);
            columns.push(Arc::new(col_array) as ArrayRef);
        }
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}

fn constraints_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let gen_constraints = &input_data.gen_constraints;

    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("operator", DataType::Utf8, false),
        Field::new("is_setpoint", DataType::Boolean, false),
        Field::new("penalty", DataType::Float64, false),
    ]);

    // Initialize vectors to hold the data
    let mut names: Vec<String> = Vec::new();
    let mut types: Vec<String> = Vec::new();
    let mut is_setpoints: Vec<bool> = Vec::new();
    let mut penalties: Vec<f64> = Vec::new();

    // Populate the vectors from the HashMap
    for (_key, gen_constraint) in gen_constraints.iter() {
        names.push(gen_constraint.name.clone());
        types.push(gen_constraint.gc_type.clone());
        is_setpoints.push(gen_constraint.is_setpoint);
        penalties.push(gen_constraint.penalty);
    }

    // Create Arrow arrays from the vectors
    let name_array: ArrayRef = Arc::new(StringArray::from(names));
    let type_array: ArrayRef = Arc::new(StringArray::from(types));
    let is_setpoint_array: ArrayRef = Arc::new(BooleanArray::from(is_setpoints));
    let penalty_array: ArrayRef = Arc::new(Float64Array::from(penalties));

    // Create the RecordBatch using these arrays and the schema
    RecordBatch::try_new(
        Arc::new(schema),
        vec![name_array, type_array, is_setpoint_array, penalty_array],
    )
}

fn bid_slots_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let bid_slots = &input_data.bid_slots;
    let temporals = &input_data.temporals;
    let timestamps = if let Some(bid_slot) = bid_slots.values().next() {
        &bid_slot.time_steps
    } else {
        &temporals.t
    };
    let mut has_ts_data = false;
    for bid_slot in bid_slots.values() {
        if !bid_slot.prices.is_empty() {
            has_ts_data = true;
            break;
        }
    }
    let mut fields: Vec<Field> = vec![Field::new(
        "t",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )];
    let mut columns: Vec<ArrayRef> = vec![times_stamp_array_from_temporal_stamps(&timestamps)];
    if !has_ts_data {
        let schema = Arc::new(Schema::new(fields));
        return RecordBatch::try_new(schema, columns);
    }
    for (market, bid_slot) in bid_slots {
        for slot in &bid_slot.slots {
            let column_name = format!("{},{}", market, slot);
            fields.push(Field::new(&column_name, DataType::Float64, true));
            let mut column_values: Vec<Option<f64>> = Vec::with_capacity(timestamps.len());
            for time in timestamps {
                let key = (time.clone(), slot.clone());
                let value = bid_slot.prices.get(&key).copied();
                column_values.push(value);
            }
            columns.push(Arc::new(Float64Array::from(column_values)) as ArrayRef);
        }
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}

fn processes_cf_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let temporals = &input_data.temporals;
    let processes = &input_data.processes;
    let mut fields: Vec<Field> = vec![Field::new(
        "t",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )];
    let mut columns: Vec<ArrayRef> = Vec::new();
    let mut has_ts_data = false;
    for process in processes.values() {
        if !process.cf.ts_data.is_empty() {
            has_ts_data = true;
            for time_series in &process.cf.ts_data {
                check_timestamps_match(&temporals.t, time_series)?;
            }
        }
    }
    let timestamp_column = times_stamp_array_from_temporal_stamps(&temporals.t);
    columns.push(timestamp_column as ArrayRef);
    if !has_ts_data {
        let schema = Arc::new(Schema::new(fields.clone()));
        return RecordBatch::try_new(schema, columns);
    }
    for process in processes.values() {
        for ts in &process.cf.ts_data {
            fields.push(Field::new(
                &format!("{},{}", process.name, ts.scenario),
                DataType::Float64,
                true,
            ));
        }
    }

    // Collect data for each time series and add it to the columns
    for process in processes.values() {
        for ts in &process.cf.ts_data {
            // Match timestamps to values
            let column_values: Vec<Option<f64>> = temporals
                .t
                .iter()
                .map(|t| ts.series.get(t).copied())
                .collect();

            // Add the data column
            columns.push(Arc::new(Float64Array::from(column_values)) as ArrayRef);
        }
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}

fn market_fixed_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let markets = &input_data.markets;

    // Gather all timestamps and sort them
    let mut all_timestamps: Vec<String> = markets
        .values()
        .flat_map(|market| market.fixed.iter().map(|(t, _)| t.clone()))
        .collect();
    all_timestamps.sort();
    all_timestamps.dedup();

    // Check if there are any timestamps collected
    if all_timestamps.is_empty() {
        // If no data, return an empty RecordBatch with only the t column
        let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::Utf8, false)]));
        let empty_array: ArrayRef = Arc::new(StringArray::from(Vec::<String>::new()));
        return RecordBatch::try_new(schema, vec![empty_array]);
    }

    // Initialize a vector for each market
    let mut columns_data: HashMap<String, Vec<Option<f64>>> = HashMap::new();
    for market in markets.values() {
        let mut market_data: Vec<Option<f64>> = vec![None; all_timestamps.len()];
        for (t, value) in &market.fixed {
            if let Some(pos) = all_timestamps.iter().position(|timestamp| timestamp == t) {
                market_data[pos] = Some(*value);
            }
        }
        columns_data.insert(market.name.clone(), market_data);
    }

    // Create the schema
    let mut fields: Vec<Field> = vec![Field::new("t", DataType::Utf8, false)];
    fields.extend(
        markets
            .keys()
            .map(|name| Field::new(name, DataType::Float64, true)),
    );

    // Create the columns
    let timestamp_array: ArrayRef = Arc::new(StringArray::from(all_timestamps));
    let mut columns: Vec<ArrayRef> = vec![timestamp_array];
    for market_name in markets.keys() {
        let column_data = columns_data
            .remove(market_name)
            .expect("Market data should be present");
        let column_array: ArrayRef = Arc::new(Float64Array::from(column_data));
        columns.push(column_array);
    }

    let schema = Arc::new(Schema::new(fields));

    // Create the RecordBatch using these arrays and the schema
    RecordBatch::try_new(schema, columns)
}

fn try_forecastable_to_time_series_data(forecastable: &Forecastable) -> Option<&TimeSeriesData> {
    match forecastable {
        Forecastable::TimeSeriesData(ref ts_data) => Some(ts_data),
        _ => None,
    }
}

fn market_price_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let temporals_t = &input_data.temporals.t;
    if temporals_t.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Temporals timestamps are empty".to_string(),
        ));
    }
    let mut unique_timestamps: BTreeSet<TimeStamp> = BTreeSet::new();
    for market in input_data.markets.values() {
        let price = try_forecastable_to_time_series_data(&market.price).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "{} market price has not been replaced by forecasted time series",
                market.name
            ))
        })?;
        for time_series in &price.ts_data {
            check_timestamps_match(temporals_t, &time_series)?;
            for timestamp in time_series.series.keys() {
                unique_timestamps.insert(timestamp.clone());
            }
        }
    }

    let sorted_timestamps: TimeLine = temporals_t.clone();

    // If there are no unique timestamps, return an empty RecordBatch
    if sorted_timestamps.is_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]));
        let timestamp_array: ArrayRef = times_stamp_array_from_temporal_stamps(&sorted_timestamps);
        let arrays = vec![timestamp_array];
        return RecordBatch::try_new(schema, arrays);
    }

    // Initialize column data
    let mut columns: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    for market in input_data.markets.values() {
        let price = try_forecastable_to_time_series_data(&market.price).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "{} market price has not been replaced by forecasted time series",
                market.name
            ))
        })?;
        for data in &price.ts_data {
            let scenario = &data.scenario;
            let column_name = format!("{},{}", market.name, scenario);
            let mut column_data = vec![f64::NAN; sorted_timestamps.len()];

            for (timestamp, value) in &data.series {
                if let Some(pos) = sorted_timestamps.iter().position(|t| t == timestamp) {
                    column_data[pos] = *value;
                }
            }

            // Only add the column if it contains any non-NaN values
            if column_data.iter().any(|&x| !x.is_nan()) {
                columns.insert(column_name, column_data);
            }
        }
    }

    // Prepare the schema and arrays
    let mut fields = vec![Field::new(
        "t",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )];
    let timestamp_array: ArrayRef = times_stamp_array_from_temporal_stamps(&sorted_timestamps);
    let mut arrays = vec![timestamp_array];

    // BTreeMap will naturally iterate columns in lexicographical order
    for (name, data) in columns {
        fields.push(Field::new(&name, DataType::Float64, true));
        let array: ArrayRef = Arc::new(Float64Array::from(data));
        arrays.push(array);
    }

    // Create the RecordBatch
    let schema = Arc::new(Schema::new(fields));
    let record_batch = RecordBatch::try_new(schema, arrays)?;
    Ok(record_batch)
}

fn market_balance_price_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let temporals_t = &input_data.temporals.t;
    if temporals_t.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Temporals timestamps are empty".to_string(),
        ));
    }
    // Collect all timestamps and initialize columns using BTreeMap to ensure order
    let mut columns: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    if temporals_t.is_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]));
        let timestamp_array: ArrayRef = times_stamp_array_from_temporal_stamps(&temporals_t);
        let arrays = vec![timestamp_array];
        return RecordBatch::try_new(schema, arrays);
    }

    // Initialize column data for up and down prices
    for market in input_data.markets.values().filter(|m| m.m_type == "energy") {
        for (label, forecastable_price) in
            [("up", &market.up_price), ("dw", &market.down_price)].iter()
        {
            let price_data =
                try_forecastable_to_time_series_data(&forecastable_price).ok_or_else(|| {
                    ArrowError::InvalidArgumentError(format!(
                        "{} market {} price has not been replaced by forecasted time series",
                        market.name, label
                    ))
                })?;
            for data in &price_data.ts_data {
                check_timestamps_match(temporals_t, data)?;
                let column_name = format!("{},{},{}", market.name, label, data.scenario);
                let mut column_data = vec![f64::NAN; temporals_t.len()];

                for (timestamp, value) in &data.series {
                    if let Some(pos) = temporals_t.iter().position(|t| t == timestamp) {
                        column_data[pos] = *value;
                    }
                }

                // Only add the column if it contains any non-NaN values
                if column_data.iter().any(|&x| !x.is_nan()) {
                    columns.insert(column_name, column_data);
                }
            }
        }
    }
    let mut fields = vec![Field::new(
        "t",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )];
    let timestamp_array: ArrayRef = times_stamp_array_from_temporal_stamps(&temporals_t);
    let mut arrays = vec![timestamp_array];
    for (name, data) in columns {
        fields.push(Field::new(&name, DataType::Float64, true));
        let array: ArrayRef = Arc::new(Float64Array::from(data));
        arrays.push(array);
    }
    let schema = Arc::new(Schema::new(fields));
    let record_batch = RecordBatch::try_new(schema, arrays)?;
    Ok(record_batch)
}

// This function converts a HashMap<String, Node> of inflow TimeSeriesData to an Arrow RecordBatch
fn nodes_inflow_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let temporals_t = &input_data.temporals.t;
    let nodes = &input_data.nodes;
    if temporals_t.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "temporals timestamps are empty".to_string(),
        ));
    }
    let mut fields = vec![Field::new(
        "t",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )];
    let mut columns: Vec<ArrayRef> = Vec::new();
    let timestamps = temporals_t.clone();
    let common_length = timestamps.len();
    let timestamp_array = times_stamp_array_from_temporal_stamps(&timestamps) as ArrayRef;
    columns.push(timestamp_array);
    for (node_name, node) in nodes {
        let inflow = try_forecastable_to_time_series_data(&node.inflow).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "{} inflow has not been replaced by forecasted temperature",
                node_name
            ))
        })?;
        if inflow.ts_data.is_empty() || inflow.ts_data.iter().all(|ts| ts.series.is_empty()) {
            continue;
        }
        for ts in &inflow.ts_data {
            if ts.series.is_empty() {
                continue;
            }
            check_timestamps_match(temporals_t, ts)?;
            let column_name = format!("{},{}", node_name, ts.scenario);
            fields.push(Field::new(&column_name, DataType::Float64, true));
            let values: Vec<Option<f64>> = timestamps
                .iter()
                .map(|t| ts.series.get(t).copied())
                .collect();
            if values.len() != common_length {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "inconsistent data length for node '{}', scenario '{}': expected {}, got {}",
                    node_name,
                    ts.scenario,
                    common_length,
                    values.len()
                )));
            }
            let value_array = Arc::new(Float64Array::from(values)) as ArrayRef;
            columns.push(value_array);
        }
    }
    let schema = Arc::new(Schema::new(fields));
    let record_batch = RecordBatch::try_new(schema, columns)?;
    Ok(record_batch)
}

fn nodes_commodity_price_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let temporals_t = &input_data.temporals.t;
    let nodes = &input_data.nodes;

    // Ensure temporals_t is not empty
    if temporals_t.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Temporals timestamps are empty".to_string(),
        ));
    }

    // Check if the timestamps in node cost data match temporals_t
    for node in nodes.values().filter(|n| n.is_commodity) {
        for time_series in &node.cost.ts_data {
            check_timestamps_match(temporals_t, time_series)?;
        }
    }

    let mut fields = vec![Field::new(
        "t",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )];
    let mut columns: Vec<ArrayRef> = Vec::new();

    // First column from Temporals
    let timestamp_array = times_stamp_array_from_temporal_stamps(&temporals_t) as ArrayRef;
    columns.push(timestamp_array);

    // Check the common length from temporals
    let common_length = temporals_t.len();

    // Process only the nodes where `is_commodity` is true
    for (node_name, node) in nodes.iter().filter(|(_, n)| n.is_commodity) {
        for ts in &node.cost.ts_data {
            if ts.series.is_empty() {
                continue; // Skip empty time series
            }

            let column_name = format!("{},{}", node_name, ts.scenario);
            fields.push(Field::new(&column_name, DataType::Float64, true));

            let values: Vec<Option<f64>> =
                ts.series.iter().map(|(_, value)| Some(*value)).collect();
            if values.len() != common_length {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Inconsistent data length for node '{}', scenario '{}': expected {}, got {}",
                    node_name,
                    ts.scenario,
                    common_length,
                    values.len()
                )));
            }

            let value_array = Arc::new(Float64Array::from(values)) as ArrayRef;
            columns.push(value_array);
        }
    }

    let schema = Arc::new(Schema::new(fields));
    let record_batch = RecordBatch::try_new(schema, columns)?;

    Ok(record_batch)
}

fn processes_eff_to_arrow(input_data: &InputData) -> Result<RecordBatch, ArrowError> {
    let processes = &input_data.processes;
    let temporals_t = &input_data.temporals.t;
    if temporals_t.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Temporals timestamps are empty".to_string(),
        ));
    }
    for process in processes.values() {
        for time_series in &process.eff_ts.ts_data {
            check_timestamps_match(temporals_t, time_series)?;
        }
    }
    let mut fields: Vec<Field> = vec![Field::new(
        "t",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )];
    let mut columns: Vec<ArrayRef> = Vec::new();

    // Create the timestamp column from Temporals
    let timestamp_array = times_stamp_array_from_temporal_stamps(&temporals_t) as ArrayRef;
    columns.push(timestamp_array);

    let common_length = temporals_t.len();

    // Group and sort the scenarios across all processes
    let mut scenario_groups: BTreeMap<String, Vec<(String, Vec<f64>)>> = BTreeMap::new();
    for (process_name, process) in processes {
        for time_series in &process.eff_ts.ts_data {
            if time_series.series.is_empty() {
                continue; // Skip empty time series
            }

            let values: Vec<f64> = time_series.series.iter().map(|(_, v)| *v).collect();
            if values.len() != common_length {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Inconsistent data length for process '{}', scenario '{}': expected {}, got {}",
                    process_name,
                    time_series.scenario,
                    common_length,
                    values.len()
                )));
            }

            scenario_groups
                .entry(time_series.scenario.clone())
                .or_insert_with(Vec::new)
                .push((process_name.clone(), values));
        }
    }

    // Iterate over the sorted scenario groups and add columns accordingly
    for (scenario, proc_values) in scenario_groups {
        for (process_name, values) in proc_values {
            let column_name = format!("{},{}", process_name, scenario);
            fields.push(Field::new(&column_name, DataType::Float64, true));
            let value_array = Arc::new(Float64Array::from(values)) as ArrayRef;
            columns.push(value_array);
        }
    }
    let schema = Arc::new(Schema::new(fields));
    let record_batch = RecordBatch::try_new(schema, columns)?;
    Ok(record_batch)
}

// Function to check timestamps match
fn check_timestamps_match(
    temporals_t: &TimeLine,
    time_series: &input_data::TimeSeries,
) -> Result<(), ArrowError> {
    if temporals_t.len() != time_series.series.len() {
        return Err(ArrowError::InvalidArgumentError(
            "time stamp count differs from temporals.t".to_string(),
        ));
    }
    for (i, stamp) in time_series.series.keys().enumerate() {
        if *stamp != temporals_t[i] {
            return Err(ArrowError::InvalidArgumentError(
                "timestamps do not match temporals.t".to_string(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::timezone::Tz;
    use arrow::array::{Array, Float64Array, Int32Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use chrono::offset::Utc;
    use chrono::TimeZone;
    use std::fs::File;
    use std::io::BufReader;
    use std::path::PathBuf;
    use std::str::FromStr;

    fn load_test_data() -> InputData {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests/predicer/predicer_all.json");
        let file = File::open(path).expect("Failed to open test file");
        let reader = BufReader::new(file);
        serde_json::from_reader(reader).expect("Failed to parse JSON")
    }

    fn print_batches(batches: &[RecordBatch]) {
        for batch in batches {
            let schema = batch.schema();
            // Print header
            for field in schema.fields() {
                print!("{:<20}\t", field.name());
            }
            println!();

            // Print rows
            for row in 0..batch.num_rows() {
                for column in batch.columns() {
                    let value = if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                        array.value(row).to_string()
                    } else if let Some(array) = column.as_any().downcast_ref::<BooleanArray>() {
                        array.value(row).to_string()
                    } else if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
                        array.value(row).to_string()
                    } else if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
                        array.value(row).to_string()
                    } else if let Some(array) =
                        column.as_any().downcast_ref::<TimestampMillisecondArray>()
                    {
                        array.value_as_datetime(row).unwrap().to_string()
                    } else {
                        "N/A".to_string()
                    };
                    print!("{:<20}\t", value);
                }
                println!();
            }
        }
    }

    fn assert_boolean_column(array: &BooleanArray, expected_values: &[bool], column_name: &str) {
        for (i, expected) in expected_values.iter().enumerate() {
            assert_eq!(
                array.value(i),
                *expected,
                "Mismatch at row {}, column '{}'",
                i,
                column_name
            );
        }
    }

    fn assert_float64_column(array: &Float64Array, expected_values: &[f64], column_name: &str) {
        for (i, expected) in expected_values.iter().enumerate() {
            assert_eq!(
                array.value(i),
                *expected,
                "Mismatch at row {}, column '{}'",
                i,
                column_name
            );
        }
    }

    fn assert_int64_column(array: &Int64Array, expected_values: &[i64], column_name: &str) {
        for (i, expected) in expected_values.iter().enumerate() {
            assert_eq!(
                array.value(i),
                *expected,
                "Mismatch at row {}, column '{}'",
                i,
                column_name
            );
        }
    }

    fn assert_string_column(array: &StringArray, expected_values: &[&str], column_name: &str) {
        for (i, expected) in expected_values.iter().enumerate() {
            assert_eq!(
                array.value(i),
                *expected,
                "Mismatch at row {}, column '{}'",
                i,
                column_name
            );
        }
    }

    fn assert_time_stamp_column(
        array: &TimestampMillisecondArray,
        expected_values: &[TimeStamp],
        column_name: &str,
    ) {
        for (i, expected) in expected_values.iter().enumerate() {
            assert_eq!(
                array
                    .value_as_datetime_with_tz(
                        i,
                        Tz::from_str(&expected.timezone().to_string())
                            .expect("parsing a machine created timezone string should not fail")
                    )
                    .unwrap(),
                *expected,
                "Mismatch at row {}, column '{}'",
                i,
                column_name
            );
        }
    }

    #[test]
    fn test_processes_to_arrow() {
        let input_data = load_test_data();

        // Print all processes in the BTreeMap
        for (process_name, process) in &input_data.processes {
            println!("Process Name: {}", process_name);
            println!("Process: {:?}", process);
        }

        // Convert processes to Arrow RecordBatch
        let record_batch =
            processes_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Print the RecordBatch for debugging
        let batches = vec![record_batch.clone()];
        print_batches(&batches);

        // Expected result DataFrame
        let expected_process_names = vec![
            "dh_source_out",
            "dh_sto_charge",
            "dh_sto_discharge",
            "dh_tra",
            "hp1",
            "ngchp",
            "p2x1",
            "pv1",
        ];
        let expected_is_cf = vec![false, false, false, false, false, false, false, true];
        let expected_is_cf_fix = vec![false, false, false, false, false, false, false, true];
        let expected_is_online = vec![false, false, false, false, false, true, false, false];
        let expected_is_res = vec![false, false, false, false, true, true, true, false];
        let expected_conversion = vec![2, 1, 1, 2, 1, 1, 1, 1];
        let expected_eff = vec![1.0, 0.99, 0.99, 0.99, 3.0, 0.9, 0.7, 1.0];
        let expected_load_min = vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.3, 0.0, 0.0];
        let expected_load_max = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let expected_start_cost = vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let expected_min_online = vec![0.0, 0.0, 0.0, 0.0, 0.0, 4.0, 0.0, 0.0];
        let expected_min_offline = vec![0.0, 0.0, 0.0, 0.0, 0.0, 3.0, 0.0, 0.0];
        let expected_max_online = vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let expected_max_offline = vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let expected_initial_state = vec![false, false, false, false, false, true, false, false];
        let expected_scenario_independent_online =
            vec![false, false, false, false, false, true, false, false];
        let expected_delay = vec![false, false, false, false, false, false, false, false];

        // Assert process names
        let process_names_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (i, expected) in expected_process_names.iter().enumerate() {
            assert_eq!(
                process_names_array.value(i),
                *expected,
                "Mismatch at row {}, column 'process'",
                i
            );
        }

        // Assert other columns
        assert_boolean_column(
            record_batch
                .column(1)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &expected_is_cf,
            "is_cf",
        );
        assert_boolean_column(
            record_batch
                .column(2)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &expected_is_cf_fix,
            "is_cf_fix",
        );
        assert_boolean_column(
            record_batch
                .column(3)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &expected_is_online,
            "is_online",
        );
        assert_boolean_column(
            record_batch
                .column(4)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &expected_is_res,
            "is_res",
        );
        assert_int64_column(
            record_batch
                .column(5)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &expected_conversion,
            "conversion",
        );
        assert_float64_column(
            record_batch
                .column(6)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap(),
            &expected_eff,
            "eff",
        );
        assert_float64_column(
            record_batch
                .column(7)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap(),
            &expected_load_min,
            "load_min",
        );
        assert_float64_column(
            record_batch
                .column(8)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap(),
            &expected_load_max,
            "load_max",
        );
        assert_float64_column(
            record_batch
                .column(9)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap(),
            &expected_start_cost,
            "start_cost",
        );
        assert_float64_column(
            record_batch
                .column(10)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap(),
            &expected_min_online,
            "min_online",
        );
        assert_float64_column(
            record_batch
                .column(11)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap(),
            &expected_min_offline,
            "min_offline",
        );
        assert_float64_column(
            record_batch
                .column(12)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap(),
            &expected_max_online,
            "max_online",
        );
        assert_float64_column(
            record_batch
                .column(13)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap(),
            &expected_max_offline,
            "max_offline",
        );
        assert_boolean_column(
            record_batch
                .column(14)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &expected_initial_state,
            "initial_state",
        );
        assert_boolean_column(
            record_batch
                .column(15)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &expected_scenario_independent_online,
            "scenario_independent_online",
        );
        assert_boolean_column(
            record_batch
                .column(16)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &expected_delay,
            "delay",
        );
    }

    #[test]
    fn test_groups_to_arrow() {
        let input_data = load_test_data();

        // Print all groups in the BTreeMap
        for (group_name, group) in &input_data.groups {
            println!("Group Name: {}", group_name);
            println!("Group: {:?}", group);
        }

        // Convert groups to Arrow RecordBatch
        let record_batch = groups_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Print the RecordBatch for debugging
        let batches = vec![record_batch.clone()];
        print_batches(&batches);

        // Expected result DataFrame
        let expected_types = vec!["node", "process", "process", "process"];
        let expected_entities = vec!["elc", "ngchp", "hp1", "p2x1"];
        let expected_group_names = vec!["elc_res", "p1", "p1", "p1"];

        // Helper function to assert string column values
        fn assert_string_column(array: &StringArray, expected_values: &[&str], column_name: &str) {
            for (i, expected) in expected_values.iter().enumerate() {
                assert_eq!(
                    array.value(i),
                    *expected,
                    "Mismatch at row {}, column '{}'",
                    i,
                    column_name
                );
            }
        }

        // Assert columns
        let types_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_string_column(types_array, &expected_types, "type");

        let entities_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_string_column(entities_array, &expected_entities, "entity");

        let group_names_array = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_string_column(group_names_array, &expected_group_names, "group");
    }

    #[test]
    fn test_process_topos_to_arrow() {
        // Load the test input data
        let input_data = load_test_data();

        // Convert the input data to a RecordBatch
        let record_batch =
            process_topos_to_arrow(&input_data).expect("Failed to create RecordBatch");

        // Use the existing print_batches function to print the generated Arrow table
        print_batches(&[record_batch.clone()]);

        // Expected values
        let expected_processes = vec![
            "dh_source_out",
            "dh_sto_charge",
            "dh_sto_charge",
            "dh_sto_discharge",
            "dh_sto_discharge",
            "dh_tra",
            "dh_tra",
            "hp1",
            "hp1",
            "ngchp",
            "ngchp",
            "ngchp",
            "p2x1",
            "p2x1",
            "pv1",
        ];

        let expected_source_sinks = vec![
            "sink", "source", "sink", "source", "sink", "source", "sink", "source", "sink",
            "source", "sink", "sink", "source", "sink", "sink",
        ];

        let expected_nodes = vec![
            "dh", "dh", "dh_sto", "dh_sto", "dh", "dh", "dh2", "elc", "dh", "ng", "dh", "elc",
            "elc", "h2", "elc",
        ];

        let expected_conversion_coeffs = vec![1.0; 15];
        let expected_capacities = vec![
            1000.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 5.0, 15.0, 20.0, 10.0, 8.0, 10.0, 7.0, 5.0,
        ];
        let expected_vom_costs = vec![
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 15.0, 0.5, 3.0, 0.0, 0.0, 15.0, 1.0, 0.5,
        ];
        let expected_ramp_ups = vec![
            1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.5, 0.5, 0.5, 0.5, 0.5, 1.0, 1.0, 1.0,
        ];
        let expected_ramp_downs = vec![
            1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.5, 0.5, 0.5, 0.5, 0.5, 1.0, 1.0, 1.0,
        ];
        let expected_initial_loads = vec![0.6; 15];
        let expected_initial_flows = vec![0.6; 15];

        // Get columns from RecordBatch
        let processes_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let source_sinks_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let nodes_array = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let conversion_coeffs_array = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let capacities_array = record_batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let vom_costs_array = record_batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let ramp_ups_array = record_batch
            .column(6)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let ramp_downs_array = record_batch
            .column(7)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let initial_loads_array = record_batch
            .column(8)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let initial_flows_array = record_batch
            .column(9)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // Assertions for each column
        assert_eq!(processes_array.len(), expected_processes.len());
        for (i, expected_value) in expected_processes.iter().enumerate() {
            assert_eq!(
                processes_array.value(i),
                *expected_value,
                "Mismatch at row {} in 'process' column",
                i
            );
        }

        assert_eq!(source_sinks_array.len(), expected_source_sinks.len());
        for (i, expected_value) in expected_source_sinks.iter().enumerate() {
            assert_eq!(
                source_sinks_array.value(i),
                *expected_value,
                "Mismatch at row {} in 'source_sink' column",
                i
            );
        }

        assert_eq!(nodes_array.len(), expected_nodes.len());
        for (i, expected_value) in expected_nodes.iter().enumerate() {
            assert_eq!(
                nodes_array.value(i),
                *expected_value,
                "Mismatch at row {} in 'node' column",
                i
            );
        }

        assert_float64_column(
            conversion_coeffs_array,
            &expected_conversion_coeffs,
            "conversion_coeff",
        );
        assert_float64_column(capacities_array, &expected_capacities, "capacity");
        assert_float64_column(vom_costs_array, &expected_vom_costs, "vom_cost");
        assert_float64_column(ramp_ups_array, &expected_ramp_ups, "ramp_up");
        assert_float64_column(ramp_downs_array, &expected_ramp_downs, "ramp_down");
        assert_float64_column(initial_loads_array, &expected_initial_loads, "initial_load");
        assert_float64_column(initial_flows_array, &expected_initial_flows, "initial_flow");
    }

    #[test]
    fn test_node_histories_to_arrow() {
        let input_data = load_test_data(); // Assuming this loads data similar to what was described

        // Print all node histories in the BTreeMap
        for (node_name, node_history) in &input_data.node_histories {
            println!("Node Name: {}", node_name);
            println!("Node History: {:?}", node_history);
        }

        // Convert node histories to Arrow RecordBatch
        let record_batch =
            node_histories_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Print the RecordBatch for debugging
        let batches = vec![record_batch.clone()];
        print_batches(&batches);

        // Expected result DataFrame
        let expected_t_values = vec![1, 2];
        let expected_timestamp_s1: TimeLine = vec![
            Utc.with_ymd_and_hms(2022, 4, 20, 0, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 1, 0, 0).unwrap().into(),
        ];
        let expected_value_s1 = vec![1.0, 1.0];
        let expected_timestamp_s2: TimeLine = vec![
            Utc.with_ymd_and_hms(2022, 4, 20, 0, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 1, 0, 0).unwrap().into(),
        ];
        let expected_value_s2 = vec![1.0, 1.0];
        let expected_timestamp_s3: TimeLine = vec![
            Utc.with_ymd_and_hms(2022, 4, 20, 0, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 1, 0, 0).unwrap().into(),
        ];
        let expected_value_s3 = vec![1.0, 1.0];

        // Assert columns
        let t_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for (i, expected) in expected_t_values.iter().enumerate() {
            assert_eq!(
                t_array.value(i),
                *expected,
                "Mismatch at row {}, column 't'",
                i
            );
        }

        let timestamp_s1_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_time_stamp_column(timestamp_s1_array, &expected_timestamp_s1, "dh_source,t,s1");

        let value_s1_array = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(value_s1_array, &expected_value_s1, "dh_source,s1");

        let timestamp_s2_array = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_time_stamp_column(timestamp_s2_array, &expected_timestamp_s2, "dh_source,t,s2");

        let value_s2_array = record_batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(value_s2_array, &expected_value_s2, "dh_source,s2");

        let timestamp_s3_array = record_batch
            .column(5)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_time_stamp_column(timestamp_s3_array, &expected_timestamp_s3, "dh_source,t,s3");

        let value_s3_array = record_batch
            .column(6)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(value_s3_array, &expected_value_s3, "dh_source,s3");
    }

    #[test]
    fn test_node_delays_to_arrow() {
        // Load the test input data
        let input_data = load_test_data();

        // Convert the node delays to a RecordBatch
        let record_batch = node_delays_to_arrow(&input_data).expect("Failed to create RecordBatch");

        // Print the generated Arrow table using the provided print_batches function
        print_batches(&[record_batch.clone()]);

        // Expected values
        let expected_node1 = vec!["delay_source"];
        let expected_node2 = vec!["dh_source"];
        let expected_delay_t = vec![2.0];
        let expected_min_flow = vec![0.0];
        let expected_max_flow = vec![1000.0];

        // Get columns from the RecordBatch
        let node1_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let node2_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let delay_t_array = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let min_flow_array = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let max_flow_array = record_batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // Validate the 'node1' column
        for (i, expected_value) in expected_node1.iter().enumerate() {
            assert_eq!(
                node1_array.value(i),
                *expected_value,
                "Mismatch at row {} in 'node1' column",
                i
            );
        }

        // Validate the 'node2' column
        for (i, expected_value) in expected_node2.iter().enumerate() {
            assert_eq!(
                node2_array.value(i),
                *expected_value,
                "Mismatch at row {} in 'node2' column",
                i
            );
        }

        // Validate the 'delay_t' column
        for (i, expected_value) in expected_delay_t.iter().enumerate() {
            assert_eq!(
                delay_t_array.value(i),
                *expected_value,
                "Mismatch at row {} in 'delay_t' column",
                i
            );
        }

        // Validate the 'min_flow' column
        for (i, expected_value) in expected_min_flow.iter().enumerate() {
            assert_eq!(
                min_flow_array.value(i),
                *expected_value,
                "Mismatch at row {} in 'min_flow' column",
                i
            );
        }

        // Validate the 'max_flow' column
        for (i, expected_value) in expected_max_flow.iter().enumerate() {
            assert_eq!(
                max_flow_array.value(i),
                *expected_value,
                "Mismatch at row {} in 'max_flow' column",
                i
            );
        }
    }

    #[test]
    fn test_node_diffusion_to_arrow() {
        let input_data = load_test_data(); // Assuming this loads data similar to what was described

        // Print all node diffusions for debugging
        for node_diffusion in &input_data.node_diffusion {
            println!("Node Diffusion: {:?}", node_diffusion);
        }

        // Convert node diffusion to Arrow RecordBatch
        let record_batch =
            node_diffusion_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Print the RecordBatch for debugging
        let batches = vec![record_batch.clone()];
        print_batches(&batches);

        // Expected result DataFrame
        let expected_t_values: TimeLine = vec![
            Utc.with_ymd_and_hms(2022, 4, 20, 0, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 1, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 2, 0, 0).unwrap().into(),
        ];
        let expected_value_s1 = vec![0.02, 0.02, 0.02];
        let expected_value_s2 = vec![0.02, 0.02, 0.02];
        let expected_value_s3 = vec![0.02, 0.02, 0.02];

        // Assert 't' column
        let t_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_time_stamp_column(t_array, &expected_t_values, "t");

        // Assert 'dh_sto,ambience,s1' column
        let value_s1_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(value_s1_array, &expected_value_s1, "dh_sto,ambience,s1");

        // Assert 'dh_sto,ambience,s2' column
        let value_s2_array = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(value_s2_array, &expected_value_s2, "dh_sto,ambience,s2");

        // Assert 'dh_sto,ambience,s3' column
        let value_s3_array = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(value_s3_array, &expected_value_s3, "dh_sto,ambience,s3");
    }

    #[test]
    fn test_inflow_blocks_to_arrow() {
        let input_data = load_test_data(); // Load the test data from the provided JSON file.

        // Print all inflow blocks for debugging
        for inflow_block in &input_data.inflow_blocks {
            println!("Inflow Block: {:?}", inflow_block);
        }

        // Convert inflow blocks to Arrow RecordBatch
        let record_batch =
            inflow_blocks_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Print the RecordBatch for debugging
        let batches = vec![record_batch.clone()];
        print_batches(&batches);

        // Expected result DataFrame
        let expected_t_values = vec![1, 2, 3];
        let expected_b1_dh_sto: TimeLine = vec![
            Utc.with_ymd_and_hms(2022, 4, 20, 0, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 1, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 2, 0, 0).unwrap().into(),
        ];
        let expected_b1_s1 = vec![-2.0, 1.0, 2.0];
        let expected_b1_s2 = vec![-3.0, 2.0, 2.0];
        let expected_b1_s3 = vec![-4.0, 3.0, 2.0];

        // Assert 't' column (running numbers)
        let t_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_int64_column(
            t_array,
            &expected_t_values
                .iter()
                .map(|&x| x as i64)
                .collect::<Vec<i64>>(),
            "t",
        );

        // Assert 'b1,dh_sto' column (timestamps)
        let b1_dh_sto_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_time_stamp_column(b1_dh_sto_array, &expected_b1_dh_sto, "b1,dh_sto");

        // Assert 'b1,s1' column
        let b1_s1_array = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(b1_s1_array, &expected_b1_s1, "b1,s1");

        // Assert 'b1,s2' column
        let b1_s2_array = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(b1_s2_array, &expected_b1_s2, "b1,s2");

        // Assert 'b1,s3' column
        let b1_s3_array = record_batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(b1_s3_array, &expected_b1_s3, "b1,s3");
    }

    #[test]
    fn test_markets_to_arrow() {
        let input_data = load_test_data(); // Load the test data from the provided JSON file.

        // Print all markets for debugging
        for market in &input_data.markets {
            println!("Market: {:?}", market);
        }

        // Convert markets to Arrow RecordBatch
        let record_batch = markets_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Print the RecordBatch for debugging
        let batches = vec![record_batch.clone()];
        print_batches(&batches);

        // Expected result DataFrame based on the lexicographical order from BTreeMap
        let expected_market = vec!["fcr_up", "npe"];
        let expected_market_type = vec!["reserve", "energy"];
        let expected_node = vec!["elc_res", "elc"];
        let expected_processgroup = vec!["p1", "p1"];
        let expected_direction = vec!["res_up", "none"];
        let expected_reserve_type = vec!["slow", "none"];
        let expected_is_bid = vec![true, true];
        let expected_is_limited = vec![false, false];
        let expected_min_bid = vec![0.0, 0.0];
        let expected_max_bid = vec![0.0, 0.0];
        let expected_fee = vec![0.0, 0.0];

        // Assert columns in the correct order
        let market_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_string_column(market_array, &expected_market, "market");

        let market_type_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_string_column(market_type_array, &expected_market_type, "market_type");

        let node_array = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_string_column(node_array, &expected_node, "node");

        let processgroup_array = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_string_column(processgroup_array, &expected_processgroup, "processgroup");

        let direction_array = record_batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_string_column(direction_array, &expected_direction, "direction");

        let reserve_type_array = record_batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_string_column(reserve_type_array, &expected_reserve_type, "reserve_type");

        let is_bid_array = record_batch
            .column(6)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_boolean_column(is_bid_array, &expected_is_bid, "is_bid");

        let is_limited_array = record_batch
            .column(7)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_boolean_column(is_limited_array, &expected_is_limited, "is_limited");

        let min_bid_array = record_batch
            .column(8)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(min_bid_array, &expected_min_bid, "min_bid");

        let max_bid_array = record_batch
            .column(9)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(max_bid_array, &expected_max_bid, "max_bid");

        let fee_array = record_batch
            .column(10)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(fee_array, &expected_fee, "fee");
    }

    #[test]
    fn test_market_realisation_to_arrow() {
        let input_data = load_test_data(); // Load the test data from the provided JSON file.

        // Print all markets for debugging
        for market in &input_data.markets {
            println!("Market: {:?}", market);
        }

        // Convert market realisation data to Arrow RecordBatch
        let record_batch =
            market_realisation_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Print the RecordBatch for debugging
        let batches = vec![record_batch.clone()];
        print_batches(&batches);

        // Expected result DataFrame
        let expected_t_values: TimeLine = vec![
            Utc.with_ymd_and_hms(2022, 4, 20, 0, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 1, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 2, 0, 0).unwrap().into(),
        ];
        let expected_fcr_up_s1 = vec![0.27, 0.27, 0.27];
        let expected_fcr_up_s2 = vec![0.27, 0.27, 0.29];
        let expected_fcr_up_s3 = vec![0.27, 0.27, 0.31];

        // Assert 't' column (timestamps)
        let t_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_time_stamp_column(t_array, &expected_t_values, "t");

        // Assert 'fcr_up,s1' column
        assert_eq!(
            record_batch
                .schema()
                .fields
                .find("fcr_up,s1")
                .expect("expected field fcr_up,s1 not found")
                .0,
            1
        );
        let fcr_up_s1_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(fcr_up_s1_array, &expected_fcr_up_s1, "fcr_up,s1");

        // Assert 'fcr_up,s2' column
        let fcr_up_s2_array = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(fcr_up_s2_array, &expected_fcr_up_s2, "fcr_up,s2");

        // Assert 'fcr_up,s3' column
        let fcr_up_s3_array = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(fcr_up_s3_array, &expected_fcr_up_s3, "fcr_up,s3");
    }

    #[test]
    fn test_market_reserve_activation_price_to_arrow() {
        let input_data = load_test_data(); // Assuming this loads data similar to what was described

        // Convert reserve activation prices to Arrow RecordBatch
        let record_batch = market_reserve_activation_price_to_arrow(&input_data)
            .expect("Failed to convert to RecordBatch");

        // Print the RecordBatch for debugging
        let batches = vec![record_batch.clone()];
        print_batches(&batches);

        // Expected result DataFrame
        let expected_t_values: TimeLine = vec![
            Utc.with_ymd_and_hms(2022, 4, 20, 0, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 1, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 2, 0, 0).unwrap().into(),
        ];
        let expected_fcr_up_s1 = vec![1.0, 2.0, 3.0];
        let expected_fcr_up_s2 = vec![1.0, 2.0, 3.0];
        let expected_fcr_up_s3 = vec![1.0, 2.0, 3.0];

        // Assert 't' column (timestamps)
        let t_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_time_stamp_column(t_array, &expected_t_values, "t");

        // Assert 'fcr_up,s1' column
        let fcr_up_s1_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(fcr_up_s1_array, &expected_fcr_up_s1, "fcr_up,s1");

        // Assert 'fcr_up,s2' column
        let fcr_up_s2_array = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(fcr_up_s2_array, &expected_fcr_up_s2, "fcr_up,s2");

        // Assert 'fcr_up,s3' column
        let fcr_up_s3_array = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(fcr_up_s3_array, &expected_fcr_up_s3, "fcr_up,s3");
    }

    #[test]
    fn test_scenarios_to_arrow() {
        let input_data = load_test_data(); // Load the test data from the provided JSON file.

        // Convert scenarios to Arrow RecordBatch
        let record_batch =
            scenarios_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Print the RecordBatch for debugging
        let batches = vec![record_batch.clone()];
        print_batches(&batches);

        // Expected result DataFrame
        let expected_names = vec!["s1", "s2", "s3"];
        let expected_probabilities = vec![0.4, 0.3, 0.3];

        // Assert 'name' column
        let name_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_string_column(name_array, &expected_names, "name");

        // Assert 'probability' column
        let probability_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(probability_array, &expected_probabilities, "probability");
    }

    #[test]
    fn test_processes_eff_fun_to_arrow() {
        let input_data = load_test_data(); // Load the test data from the provided JSON file.

        // Convert processes efficiency functions to Arrow RecordBatch
        let record_batch =
            processes_eff_fun_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Print the RecordBatch for debugging
        let batches = vec![record_batch.clone()];
        print_batches(&batches);

        // Expected result DataFrame
        let expected_process = vec!["ngchp,op", "ngchp,eff"];
        let expected_column_1 = vec![0.4, 0.8];
        let expected_column_2 = vec![0.6, 0.78];
        let expected_column_3 = vec![0.8, 0.75];
        let expected_column_4 = vec![0.9, 0.7];
        let expected_column_5 = vec![1.0, 0.65];

        // Assert 'process' column
        let process_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_string_column(process_array, &expected_process, "process");

        // Assert each dynamic column for efficiency function
        let column_1 = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_1, &expected_column_1, "1");

        let column_2 = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_2, &expected_column_2, "2");

        let column_3 = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_3, &expected_column_3, "3");

        let column_4 = record_batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_4, &expected_column_4, "4");

        let column_5 = record_batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_5, &expected_column_5, "5");
    }

    #[test]
    fn test_reserve_type_to_arrow() {
        let input_data = load_test_data(); // Load the test data from the provided JSON file.

        // Convert reserve types to Arrow RecordBatch
        let record_batch =
            reserve_type_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Print the RecordBatch for debugging
        let batches = vec![record_batch.clone()];
        print_batches(&batches);

        // Expected result DataFrame
        let expected_reserve_type = vec!["slow"];
        let expected_ramp_factor = vec![1.0];

        // Assert 'reserve_type' column
        let reserve_type_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_string_column(reserve_type_array, &expected_reserve_type, "reserve_type");

        // Assert 'ramp_factor' column
        let ramp_factor_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(ramp_factor_array, &expected_ramp_factor, "ramp_factor");
    }

    #[test]
    fn test_risk_to_arrow() {
        let input_data = load_test_data(); // Load the test data from the provided JSON file.

        // Convert risk data to Arrow RecordBatch
        let record_batch = risk_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Print the RecordBatch for debugging
        let batches = vec![record_batch.clone()];
        print_batches(&batches);

        // Expected result DataFrame
        let expected_parameters = vec!["alfa", "beta"];
        let expected_values = vec![0.1, 0.2];

        // Assert 'parameter' column
        let parameter_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_string_column(parameter_array, &expected_parameters, "parameter");

        // Assert 'value' column
        let value_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(value_array, &expected_values, "value");
    }

    #[test]
    fn test_processes_cap_to_arrow() {
        let input_data = load_test_data(); // Load the test data from the provided JSON file.

        // Convert processes cap data to Arrow RecordBatch
        let record_batch =
            processes_cap_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Print the RecordBatch for debugging
        let batches = vec![record_batch.clone()];
        print_batches(&batches);

        // Expected result DataFrame
        let expected_t: TimeLine = vec![
            Utc.with_ymd_and_hms(2022, 4, 20, 0, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 1, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 2, 0, 0).unwrap().into(),
        ];

        let expected_hp1_elc_s1 = vec![5.0, 4.285714285714286, 4.285714285714286];
        let expected_hp1_elc_s2 = vec![5.0, 4.285714285714286, 4.285714285714286];
        let expected_hp1_elc_s3 = vec![5.0, 4.28571428571429, 4.28571428571429];

        // Assert 't' column
        let t_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_time_stamp_column(t_array, &expected_t, "t");

        // Assert 'hp1,elc,s1' column
        let hp1_elc_s1_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(hp1_elc_s1_array, &expected_hp1_elc_s1, "hp1,elc,s1");

        // Assert 'hp1,elc,s2' column
        let hp1_elc_s2_array = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(hp1_elc_s2_array, &expected_hp1_elc_s2, "hp1,elc,s2");

        // Assert 'hp1,elc,s3' column
        let hp1_elc_s3_array = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(hp1_elc_s3_array, &expected_hp1_elc_s3, "hp1,elc,s3");
    }

    #[test]
    fn test_gen_constraints_to_arrow() {
        let input_data = load_test_data(); // Load the test data from the provided JSON file.

        // Convert gen constraints to Arrow RecordBatch
        let record_batch =
            gen_constraints_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Print the RecordBatch for debugging
        let batches = vec![record_batch.clone()];
        print_batches(&batches);

        // Expected result DataFrame
        let expected_t_values: TimeLine = vec![
            Utc.with_ymd_and_hms(2022, 4, 20, 0, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 1, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 2, 0, 0).unwrap().into(),
        ];
        let expected_c1_s1 = vec![0.0, 0.0, 0.0];
        let expected_c1_s2 = vec![0.0, 0.0, 0.0];
        let expected_c1_s3 = vec![0.0, 0.0, 0.0];
        let expected_c1_ngchp_elc_s1 = vec![1.0, 1.0, 1.0];
        let expected_c1_ngchp_elc_s2 = vec![1.0, 1.0, 1.0];
        let expected_c1_ngchp_elc_s3 = vec![1.0, 1.0, 1.0];
        let expected_c1_ngchp_dh_s1 = vec![-0.8, -0.8, -0.8];
        let expected_c1_ngchp_dh_s2 = vec![-0.8, -0.8, -0.8];
        let expected_c1_ngchp_dh_s3 = vec![-0.8, -0.8, -0.8];
        let expected_c2_dh_sto_s1 = vec![300.0, 300.0, 300.0];
        let expected_c2_dh_sto_s2 = vec![300.0, 300.0, 300.0];
        let expected_c2_dh_sto_s3 = vec![300.0, 300.0, 300.0];
        let expected_c3_dh_sto_s1 = vec![256.0, 256.0, 256.0];
        let expected_c3_dh_sto_s2 = vec![256.0, 256.0, 256.0];
        let expected_c3_dh_sto_s3 = vec![256.0, 256.0, 256.0];

        // Assert 't' column
        let t_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_time_stamp_column(t_array, &expected_t_values, "t");

        // Assert other columns in lexicographical order
        let column_c1_s1 = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_c1_s1, &expected_c1_s1, "c1,s1");

        let column_c1_s2 = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_c1_s2, &expected_c1_s2, "c1,s2");

        let column_c1_s3 = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_c1_s3, &expected_c1_s3, "c1,s3");

        let column_c1_ngchp_elc_s1 = record_batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(
            column_c1_ngchp_elc_s1,
            &expected_c1_ngchp_elc_s1,
            "c1,ngchp,elc,s1",
        );

        let column_c1_ngchp_elc_s2 = record_batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(
            column_c1_ngchp_elc_s2,
            &expected_c1_ngchp_elc_s2,
            "c1,ngchp,elc,s2",
        );

        let column_c1_ngchp_elc_s3 = record_batch
            .column(6)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(
            column_c1_ngchp_elc_s3,
            &expected_c1_ngchp_elc_s3,
            "c1,ngchp,elc,s3",
        );

        let column_c1_ngchp_dh_s1 = record_batch
            .column(7)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(
            column_c1_ngchp_dh_s1,
            &expected_c1_ngchp_dh_s1,
            "c1,ngchp,dh,s1",
        );

        let column_c1_ngchp_dh_s2 = record_batch
            .column(8)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(
            column_c1_ngchp_dh_s2,
            &expected_c1_ngchp_dh_s2,
            "c1,ngchp,dh,s2",
        );

        let column_c1_ngchp_dh_s3 = record_batch
            .column(9)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(
            column_c1_ngchp_dh_s3,
            &expected_c1_ngchp_dh_s3,
            "c1,ngchp,dh,s3",
        );

        let column_c2_dh_sto_s1 = record_batch
            .column(10)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_c2_dh_sto_s1, &expected_c2_dh_sto_s1, "c2,dh_sto,s1");

        let column_c2_dh_sto_s2 = record_batch
            .column(11)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_c2_dh_sto_s2, &expected_c2_dh_sto_s2, "c2,dh_sto,s2");

        let column_c2_dh_sto_s3 = record_batch
            .column(12)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_c2_dh_sto_s3, &expected_c2_dh_sto_s3, "c2,dh_sto,s3");

        let column_c3_dh_sto_s1 = record_batch
            .column(13)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_c3_dh_sto_s1, &expected_c3_dh_sto_s1, "c3,dh_sto,s1");

        let column_c3_dh_sto_s2 = record_batch
            .column(14)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_c3_dh_sto_s2, &expected_c3_dh_sto_s2, "c3,dh_sto,s2");

        let column_c3_dh_sto_s3 = record_batch
            .column(15)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_c3_dh_sto_s3, &expected_c3_dh_sto_s3, "c3,dh_sto,s3");
    }

    #[test]
    fn test_constraints_to_arrow() {
        let input_data = load_test_data(); // Load the test data from the provided JSON file.

        // Convert constraints to Arrow RecordBatch
        let record_batch =
            constraints_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Expected result DataFrame
        let expected_names = vec!["c1", "c2", "c3"];
        let expected_operators = vec!["eq", "st", "gt"];
        let expected_is_setpoints = vec![false, true, true];
        let expected_penalties = vec![0.0, 1.0, 1.0];

        // Assert 'name' column
        let name_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_string_column(name_array, &expected_names, "name");

        // Assert 'operator' column
        let operator_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_string_column(operator_array, &expected_operators, "operator");

        // Assert 'is_setpoint' column
        let is_setpoint_array = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_boolean_column(is_setpoint_array, &expected_is_setpoints, "is_setpoint");

        // Assert 'penalty' column
        let penalty_array = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(penalty_array, &expected_penalties, "penalty");
    }

    #[test]
    fn test_bid_slots_to_arrow() {
        let input_data = load_test_data();
        let record_batch =
            bid_slots_to_arrow(&input_data).expect("Failed to convert to RecordBatch");
        let expected_t_values: TimeLine = vec![
            Utc.with_ymd_and_hms(2022, 4, 20, 0, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 1, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 2, 0, 0).unwrap().into(),
        ];
        let expected_npe_p0 = vec![45.0, 58.0, 55.0];
        let expected_npe_p1 = vec![45.0, 58.0, 65.0];
        let expected_npe_p2 = vec![50.0, 62.0, 88.0];
        let expected_npe_p3 = vec![50.0, 62.0, 91.0];
        let t_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_time_stamp_column(t_array, &expected_t_values, "t");

        // Assert other columns (in the expected order)
        let column_npe_p0 = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_npe_p0, &expected_npe_p0, "npe,p0");

        let column_npe_p1 = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_npe_p1, &expected_npe_p1, "npe,p1");

        let column_npe_p2 = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_npe_p2, &expected_npe_p2, "npe,p2");

        let column_npe_p3 = record_batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_npe_p3, &expected_npe_p3, "npe,p3");
    }

    #[test]
    fn test_processes_cf_to_arrow() {
        let input_data = load_test_data(); // Load the test data from the provided JSON file.

        // Convert processes capacity factor to Arrow RecordBatch
        let record_batch =
            processes_cf_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Expected result DataFrame
        let expected_t_values: TimeLine = vec![
            Utc.with_ymd_and_hms(2022, 4, 20, 0, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 1, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 2, 0, 0).unwrap().into(),
        ];
        let expected_pv1_s1_s2_s3 = vec![0.0, 0.4, 0.5];

        // Assert 't' column
        let t_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_time_stamp_column(t_array, &expected_t_values, "t");

        // Assert the capacity factor column for 'pv1,s1,s2,s3'
        let column_pv1_s1_s2_s3 = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(column_pv1_s1_s2_s3, &expected_pv1_s1_s2_s3, "pv1,s1,s2,s3");
    }

    #[test]
    fn test_market_price_to_arrow() {
        let input_data = load_test_data();
        let record_batch =
            market_price_to_arrow(&input_data).expect("Failed to convert to RecordBatch");
        let expected_t_values: TimeLine = vec![
            Utc.with_ymd_and_hms(2022, 4, 20, 0, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 1, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 2, 0, 0).unwrap().into(),
        ];
        let expected_fcr_up_s1 = vec![4.0, 56.0, 44.0];
        let expected_fcr_up_s2 = vec![4.0, 56.0, 52.8];
        let expected_fcr_up_s3 = vec![4.0, 56.0, 55.0];
        let expected_npe_s1 = vec![48.0, 60.0, 58.0];
        let expected_npe_s2 = vec![48.0, 60.0, 87.0];
        let expected_npe_s3 = vec![48.0, 60.0, 89.0];

        // Assert 't' column
        let t_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_time_stamp_column(t_array, &expected_t_values, "t");

        // Assert 'fcr_up,s1' column
        let fcr_up_s1_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(fcr_up_s1_array, &expected_fcr_up_s1, "fcr_up,s1");

        // Assert 'fcr_up,s2' column
        let fcr_up_s2_array = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(fcr_up_s2_array, &expected_fcr_up_s2, "fcr_up,s2");

        // Assert 'fcr_up,s3' column
        let fcr_up_s3_array = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(fcr_up_s3_array, &expected_fcr_up_s3, "fcr_up,s3");

        // Assert 'npe,s1' column
        let npe_s1_array = record_batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(npe_s1_array, &expected_npe_s1, "npe,s1");

        // Assert 'npe,s2' column
        let npe_s2_array = record_batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(npe_s2_array, &expected_npe_s2, "npe,s2");

        // Assert 'npe,s3' column
        let npe_s3_array = record_batch
            .column(6)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(npe_s3_array, &expected_npe_s3, "npe,s3");
    }

    #[test]
    fn test_nodes_commodity_price_to_arrow() {
        let input_data = load_test_data(); // Load the test data from the provided JSON file.

        // Convert nodes commodity price data to Arrow RecordBatch
        let record_batch =
            nodes_commodity_price_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Expected result DataFrame
        let expected_t_values: TimeLine = vec![
            Utc.with_ymd_and_hms(2022, 4, 20, 0, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 1, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 2, 0, 0).unwrap().into(),
        ];
        let expected_ng_all = vec![12.0, 12.0, 12.0];

        // Assert 't' column
        let t_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_time_stamp_column(t_array, &expected_t_values, "t");

        // Assert 'ng,ALL' column
        let ng_all_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(ng_all_array, &expected_ng_all, "ng,ALL");
    }

    #[test]
    fn test_processes_eff_to_arrow() {
        let input_data = load_test_data(); // Load the test data from the provided JSON file.

        // Convert process efficiency data to Arrow RecordBatch
        let record_batch =
            processes_eff_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Expected result DataFrame
        let expected_t_values: TimeLine = vec![
            Utc.with_ymd_and_hms(2022, 4, 20, 0, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 1, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 2, 0, 0).unwrap().into(),
        ];
        let expected_hp1_s1 = vec![3.0, 3.5, 3.5];
        let expected_hp1_s2 = vec![3.0, 3.5, 3.5];
        let expected_hp1_s3 = vec![3.0, 3.5, 3.5];
        let t_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_time_stamp_column(t_array, &expected_t_values, "t");
        let hp1_s1_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(hp1_s1_array, &expected_hp1_s1, "hp1,s1");

        // Assert 'hp1,s2' column
        let hp1_s2_array = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(hp1_s2_array, &expected_hp1_s2, "hp1,s2");

        // Assert 'hp1,s3' column
        let hp1_s3_array = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(hp1_s3_array, &expected_hp1_s3, "hp1,s3");
    }

    //TÄHÄN TEST_MARKET_FIXED_TO_ARROW KUNHAN SAADAAN DATAA

    #[test]
    fn test_market_balance_price_to_arrow() {
        let input_data = load_test_data(); // Load the test data from the provided JSON file.

        // Convert market balance price data to Arrow RecordBatch
        let record_batch =
            market_balance_price_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Expected result DataFrame with columns in BTreeMap lexicographical order
        let expected_t_values: TimeLine = vec![
            Utc.with_ymd_and_hms(2022, 4, 20, 0, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 1, 0, 0).unwrap().into(),
            Utc.with_ymd_and_hms(2022, 4, 20, 2, 0, 0).unwrap().into(),
        ];
        let expected_npe_dw_s1 = vec![43.2, 54.0, 52.2];
        let expected_npe_dw_s2 = vec![43.2, 54.0, 78.3];
        let expected_npe_dw_s3 = vec![43.2, 54.0, 78.3];
        let expected_npe_up_s1 = vec![52.8, 66.0, 63.8];
        let expected_npe_up_s2 = vec![52.8, 66.0, 95.7];
        let expected_npe_up_s3 = vec![52.8, 66.0, 95.7];

        // Assert 't' column
        let t_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_time_stamp_column(t_array, &expected_t_values, "t");

        // Assert 'npe,dw,s1' column
        let npe_dw_s1_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(npe_dw_s1_array, &expected_npe_dw_s1, "npe,dw,s1");

        // Assert 'npe,dw,s2' column
        let npe_dw_s2_array = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(npe_dw_s2_array, &expected_npe_dw_s2, "npe,dw,s2");

        // Assert 'npe,dw,s3' column
        let npe_dw_s3_array = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(npe_dw_s3_array, &expected_npe_dw_s3, "npe,dw,s3");

        // Assert 'npe,up,s1' column
        let npe_up_s1_array = record_batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(npe_up_s1_array, &expected_npe_up_s1, "npe,up,s1");

        // Assert 'npe,up,s2' column
        let npe_up_s2_array = record_batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(npe_up_s2_array, &expected_npe_up_s2, "npe,up,s2");

        // Assert 'npe,up,s3' column
        let npe_up_s3_array = record_batch
            .column(6)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_float64_column(npe_up_s3_array, &expected_npe_up_s3, "npe,up,s3");
    }

    #[test]
    fn test_inputdatasetup_to_arrow() {
        let input_data = load_test_data();
        let record_batch =
            inputdatasetup_to_arrow(&input_data).expect("Failed to convert to RecordBatch");

        // Expected result DataFrame
        let expected_parameters = vec![
            "use_market_bids",
            "use_reserves",
            "use_reserve_realisation",
            "use_node_dummy_variables",
            "use_ramp_dummy_variables",
            "node_dummy_variable_cost",
            "ramp_dummy_variable_cost",
            "common_timesteps",
            "common_scenario_name",
        ];
        // Assert parameter values
        let batches = vec![record_batch];
        let parameter_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (i, expected) in expected_parameters.iter().enumerate() {
            assert_eq!(parameter_array.value(i), *expected);
        }

        // Assert value values
        let expected_bools = vec![true, true, true, true, true];
        let expected_floats = vec![10000.0, 10000.0];
        let expected_ints = vec![2];
        let expected_strings = vec!["s_all".to_string()];
        let value_array = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        let mut base = 0;
        for (i, expected) in expected_bools.iter().enumerate() {
            let value = value_array
                .value(base + i)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0);
            assert_eq!(value, *expected);
        }
        base += expected_bools.len();
        for (i, expected) in expected_floats.iter().enumerate() {
            let value = value_array
                .value(base + i)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0);
            assert_eq!(value, *expected);
        }
        base += expected_floats.len();
        for (i, expected) in expected_ints.iter().enumerate() {
            let value = value_array
                .value(base + i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            assert_eq!(value, *expected);
        }
        base += expected_ints.len();
        for (i, expected) in expected_strings.iter().enumerate() {
            let inner_array = value_array.value(base + i);
            let value = inner_array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0);
            assert_eq!(value, *expected);
        }
    }
}
