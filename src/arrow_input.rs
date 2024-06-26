use arrow::array::{StringArray, Float64Array, Int32Array, Int64Array, BooleanArray, ArrayRef, Array};
use arrow::{datatypes::{DataType, Field, Schema, SchemaRef}, error::ArrowError, record_batch::RecordBatch};
use std::sync::Arc;
use crate::input_data;
use crate::errors;
use std::collections::{HashMap, BTreeMap};
use std::error::Error;
//use base64::encode;
use std::collections::HashSet;
use errors::{DataConversionError};
use prettytable::{Table, Row, Cell};
use arrow_ipc::writer::StreamWriter;
use crate::errors::FileReadError;

pub fn print_record_batches(batches: &HashMap<String, RecordBatch>) -> Result<(), Box<dyn Error>> {
    for (name, batch) in batches {
        println!("Batch: {}", name);

        // Create a PrettyTable table
        let mut table = Table::new();
        
        // Add the header
        let header: Vec<Cell> = batch.schema().fields().iter()
            .map(|field| Cell::new(&field.name()))
            .collect();
        table.add_row(Row::new(header));

        // Determine the number of rows in the batch
        let num_rows = batch.num_rows();

        // Iterate over each row
        for row_idx in 0..num_rows {
            let mut row = Vec::new();
            
            // Iterate over each column
            for col_idx in 0..batch.num_columns() {
                let column = batch.column(col_idx);
                
                let cell_value = match column.data_type() {
                    DataType::Utf8 => {
                        let string_array = column.as_any().downcast_ref::<StringArray>().unwrap();
                        string_array.value(row_idx).to_string()
                    },
                    DataType::Float64 => {
                        let float_array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                        if float_array.is_null(row_idx) {
                            "NULL".to_string()
                        } else {
                            float_array.value(row_idx).to_string()
                        }
                    },
                    DataType::Boolean => {
                        let bool_array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                        if bool_array.is_null(row_idx) {
                            "NULL".to_string()
                        } else {
                            bool_array.value(row_idx).to_string()
                        }
                    },
                    _ => "Unsupported data type".to_string()
                };

                row.push(Cell::new(&cell_value));
            }

            table.add_row(Row::new(row));
        }

        table.printstd();
        println!("\n");
    }
    Ok(())
}

// Function to create and serialize multiple RecordBatches
pub fn create_and_serialize_record_batches(
    input_data: &input_data::InputData,
) -> Result<Vec<(String, Vec<u8>)>, Box<dyn Error>> {
    let batches = create_record_batches(input_data)?;
    let mut serialized_batches = Vec::new();

    for (key, batch) in batches {
        let buffer = serialize_batch_to_buffer(&batch)?;
        serialized_batches.push((key, buffer));
    }

    Ok(serialized_batches)
}

// Function to create RecordBatches (implement your Arrow conversion functions)
pub fn create_record_batches(
    input_data: &input_data::InputData,
) -> Result<Vec<(String, RecordBatch)>, Box<dyn Error>> {
    let mut batches = Vec::new();
    
    batches.push(("temps".to_string(), temps_to_arrow(&input_data)?));
    batches.push(("setup".to_string(), inputdatasetup_to_arrow(&input_data)?));
    batches.push(("nodes".to_string(), nodes_to_arrow(&input_data)?));
    batches.push(("processes".to_string(), processes_to_arrow(&input_data)?));
    batches.push(("groups".to_string(), groups_to_arrow(&input_data)?));
    batches.push(("process_topology".to_string(), process_topos_to_arrow(&input_data)?));
    batches.push(("node_history".to_string(), node_histories_to_arrow(&input_data)?));
    batches.push(("node_delay".to_string(), node_delays_to_arrow(&input_data)?));
    batches.push(("node_diffusion".to_string(), node_diffusion_to_arrow(&input_data)?));
    batches.push(("inflow_blocks".to_string(), inflow_blocks_to_arrow(&input_data)?));
    batches.push(("markets".to_string(), markets_to_arrow(&input_data)?));
    batches.push(("reserve_realisation".to_string(), market_realisation_to_arrow(&input_data)?));
    batches.push(("reserve_activation_price".to_string(), market_reserve_activation_price_to_arrow(&input_data)?));
    batches.push(("scenarios".to_string(), scenarios_to_arrow(&input_data)?));
    batches.push(("efficiencies".to_string(), processes_eff_fun_to_arrow(&input_data)?));
    batches.push(("reserve_type".to_string(), reserve_type_to_arrow(&input_data)?));
    batches.push(("risk".to_string(), risk_to_arrow(&input_data)?));
    batches.push(("cap_ts".to_string(), processes_cap_to_arrow(&input_data)?));
    batches.push(("gen_constraint".to_string(), gen_constraints_to_arrow(&input_data)?));
    batches.push(("constraints".to_string(), constraints_to_arrow(&input_data)?));
    batches.push(("bid_slots".to_string(), bid_slots_to_arrow(&input_data)?));
    batches.push(("cf".to_string(), processes_cf_to_arrow(&input_data)?));
    batches.push(("inflow".to_string(), nodes_inflow_to_arrow(&input_data)?));
    batches.push(("market_prices".to_string(), market_price_to_arrow(&input_data)?));
    batches.push(("price".to_string(), nodes_commodity_price_to_arrow(&input_data)?));
    batches.push(("eff_ts".to_string(), processes_eff_to_arrow(&input_data)?));
    batches.push(("fixed_ts".to_string(), market_fixed_to_arrow(&input_data)?));
    batches.push(("balance_prices".to_string(), market_balance_price_to_arrow(&input_data)?));

    Ok(batches)
}

// Function to serialize the batch to a buffer
pub fn serialize_batch_to_buffer(batch: &RecordBatch) -> Result<Vec<u8>, Box<dyn Error>> {
    let schema = batch.schema();
    let schema_ref: &Schema = schema.as_ref(); // Dereference the Arc<Schema>
    let mut buffer: Vec<u8> = Vec::new();
    {
        let mut stream_writer = StreamWriter::try_new(&mut buffer, schema_ref)?;
        stream_writer.write(batch)?;
        stream_writer.finish()?;
    } // `stream_writer` is dropped here
    Ok(buffer)
}

pub fn temps_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let temporals = &input_data.temporals;

    // Define the schema for the RecordBatch
    let fields = vec![Field::new("t", DataType::Utf8, false)];
    let schema = Arc::new(Schema::new(fields));

    // Create the StringArray from temporals.t
    let t_values: Vec<&str> = temporals.t.iter().map(|s| s.as_str()).collect();
    let t_array = StringArray::from(t_values);
    let t_column: ArrayRef = Arc::new(t_array);

    // Create the RecordBatch
    RecordBatch::try_new(schema, vec![t_column])
}

pub fn inputdatasetup_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, DataConversionError> {
    let setup = &input_data.setup;

    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("parameter", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, true),
    ]);

    // Prepare data
    let mut parameters = Vec::new();
    let mut values = Vec::new();

    parameters.push("use_market_bids");
    if setup.contains_markets {
        values.push("1".to_string());
    } else {
        values.push("0".to_string());
    }

    parameters.push("use_reserves");
    if setup.contains_reserves {
        values.push("1".to_string());
    } else {
        values.push("0".to_string());
    }

    parameters.push("use_reserve_realisation");
    if setup.reserve_realisation {
        values.push("1".to_string());
    } else {
        values.push("0".to_string());
    }

    parameters.push("use_node_dummy_variables");
    if setup.use_node_dummy_variables {
        values.push("1".to_string());
    } else {
        values.push("0".to_string());
    }

    parameters.push("use_ramp_dummy_variables");
    if setup.use_ramp_dummy_variables {
        values.push("1".to_string());
    } else {
        values.push("0".to_string());
    }

    parameters.push("node_dummy_variable_cost");
    if setup.node_dummy_variable_cost != 0.0 {
        values.push(setup.node_dummy_variable_cost.to_string());
    } else {
        values.push("0.0".to_string());
    }

    parameters.push("ramp_dummy_variable_cost");
    if setup.ramp_dummy_variable_cost != 0.0 {
        values.push(setup.ramp_dummy_variable_cost.to_string());
    } else {
        values.push("0.0".to_string());
    }

    // Always include common_timesteps
    parameters.push("common_timesteps");
    values.push(setup.common_timesteps.to_string());

    // Include common_scenario_name with a default value if it's empty
    parameters.push("common_scenario_name");
    if setup.common_scenario_name.is_empty() {
        values.push("missing".to_string());
    } else {
        values.push(setup.common_scenario_name.clone());
    }

    // Check for logical errors in input before creating arrays
    if parameters.len() != values.len() {
        return Err(DataConversionError::InvalidInput("Mismatched parameters and values lengths".to_string()));
    }

    let parameter_array = Arc::new(StringArray::from(parameters)) as ArrayRef;
    let value_array = Arc::new(StringArray::from(values)) as ArrayRef;

    RecordBatch::try_new(Arc::new(schema), vec![parameter_array, value_array])
        .map_err(DataConversionError::from)
}

//What are the default values if State is None?
//How to add the node.groups here?
pub fn nodes_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let nodes = &input_data.nodes;

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
        Field::new("T_E_conversion", DataType::Float64, false),
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
            // Provide default values for the fields in case state is None
            state_maxs.push(0.0);
            state_mins.push(0.0);
            in_maxs.push(0.0);
            out_maxs.push(0.0);
            initial_states.push(0.0);
            state_loss_proportionals.push(0.0);
            scenario_independent_states.push(false);
            is_temps.push(false);
            t_e_conversions.push(0.0);
            residual_values.push(0.0);
        }
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

pub fn processes_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
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

    for process in processes.values() {
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
    let scenario_independent_onlines_array = Arc::new(BooleanArray::from(scenario_independent_onlines)) as ArrayRef;
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
    );

    record_batch
}

// Function to convert HashMap<String, Group> to RecordBatch
pub fn groups_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let groups = &input_data.groups;

    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("type", DataType::Utf8, false),
        Field::new("entity", DataType::Utf8, false),
        Field::new("group", DataType::Utf8, false),
    ]);

    // Initialize vectors to hold group data
    let mut types: Vec<String> = Vec::new();
    let mut entities: Vec<String> = Vec::new();
    let mut group_names: Vec<String> = Vec::new();

    // Process each group
    for (_, group) in groups.iter() {
        for member in &group.members {
            types.push(group.var_type.clone());
            entities.push(member.clone());
            group_names.push(group.name.clone());
        }
    }

    // Create arrays from the vectors
    let types_array = Arc::new(StringArray::from(types)) as ArrayRef;
    let entities_array = Arc::new(StringArray::from(entities)) as ArrayRef;
    let group_names_array = Arc::new(StringArray::from(group_names)) as ArrayRef;

    // Create the RecordBatch using these arrays
    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            types_array,
            entities_array,
            group_names_array,
        ],
    );

    record_batch
}

// Convert HashMap<String, ProcessTopology> to RecordBatch
pub fn process_topos_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let process_topologys = &input_data.processes;

    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("process", DataType::Utf8, false),
        Field::new("source_sink", DataType::Utf8, false),
        Field::new("node", DataType::Utf8, false),
        Field::new("conversion_coeff", DataType::Float64, false),
        Field::new("capacity", DataType::Float64, false),
        Field::new("VOM_cost", DataType::Float64, false),
        Field::new("ramp_up", DataType::Float64, false),
        Field::new("ramp_down", DataType::Float64, false),
        Field::new("initial_load", DataType::Float64, false),
        Field::new("initial_flow", DataType::Float64, false),
    ]);

    // Initialize vectors to hold process data
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
            processes.push(process_name.clone());

            // Determine the `source_sink` value and `node` value based on process name
            let (source_sink, node) = if process.name == topo.sink {
                ("source".to_string(), topo.source.clone())
            } else {
                ("sink".to_string(), topo.sink.clone())
            };

            source_sinks.push(source_sink);
            nodes.push(node);
            conversion_coeffs.push(1.0); // Example placeholder value for `conversion_coeff`
            capacities.push(topo.capacity);
            vom_costs.push(topo.vom_cost);
            ramp_ups.push(topo.ramp_up);
            ramp_downs.push(topo.ramp_down);
            initial_loads.push(topo.initial_load);
            initial_flows.push(topo.initial_flow);
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


pub fn node_diffusion_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let node_diffusions = &input_data.node_diffusion;
    let temporals_t = &input_data.temporals.t;

    // If node_diffusions is empty, create a dataframe with just the timestamps
    if node_diffusions.is_empty() {
        let t_array: ArrayRef = Arc::new(StringArray::from(temporals_t.clone()));
        let schema = Schema::new(vec![Field::new("t", DataType::Utf8, false)]);
        return RecordBatch::try_new(Arc::new(schema), vec![t_array]);
    }

    // Define fields for the schema dynamically
    let mut fields = vec![Field::new("t", DataType::Utf8, false)];
    let mut columns: Vec<ArrayRef> = Vec::new();

    // Placeholder to hold time series data
    let mut float_columns_data: HashMap<String, Vec<f64>> = HashMap::new();

    // Add 't' column
    let t_column: Vec<String> = temporals_t.clone();
    columns.push(Arc::new(StringArray::from(t_column.clone())) as ArrayRef);

    // Populate the columns from the NodeDiffusion Vec
    for node_diffusion in node_diffusions {
        for ts in &node_diffusion.coefficient.ts_data {
            let column_name = format!("{},{},{}", node_diffusion.node1, node_diffusion.node2, ts.scenario);

            fields.push(Field::new(&column_name, DataType::Float64, true));

            // Initialize column data with NaNs
            let mut column_data = vec![f64::NAN; temporals_t.len()];

            for (timestamp, value) in &ts.series {
                if let Some(pos) = temporals_t.iter().position(|t| t == timestamp) {
                    column_data[pos] = *value;
                }
            }

            // Only add the column if it contains any non-NaN values
            if column_data.iter().any(|&x| !x.is_nan()) {
                float_columns_data.insert(column_name, column_data);
            }
        }

        // Check if the timestamps match
        check_timestamps_match(temporals_t, &node_diffusion.coefficient.ts_data)?;
    }

    // Add the float columns to the RecordBatch
    for (key, val) in float_columns_data {
        columns.push(Arc::new(Float64Array::from(val)) as ArrayRef);
    }

    let schema = Arc::new(Schema::new(fields));

    // Create the RecordBatch using these arrays and the schema
    RecordBatch::try_new(schema, columns)
}


// Function to convert node histories to Arrow RecordBatch
pub fn node_histories_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let node_histories = &input_data.node_histories;
    let temporals_t = &input_data.temporals.t;

    // Check if the timestamps match
    for (_, node_history) in node_histories {
        check_timestamps_match(temporals_t, &node_history.steps.ts_data)?;
    }

    // Define the schema for the Arrow RecordBatch dynamically
    let mut fields = vec![Field::new("t", DataType::Utf8, false)];
    let mut columns: Vec<ArrayRef> = Vec::new();

    // Placeholder to hold time series data
    let mut string_columns_data: HashMap<String, Vec<String>> = HashMap::new();
    let mut float_columns_data: HashMap<String, Vec<f64>> = HashMap::new();

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

            // If ts_data is empty, use temporals_t
            if ts.series.is_empty() {
                for t in temporals_t {
                    string_columns_data
                        .entry(column_name_string.clone())
                        .or_insert_with(Vec::new)
                        .push(t.clone());
                }
            }
        }
    }

    // If node_histories is empty, create a column from temporals_t
    if node_histories.is_empty() {
        for t in temporals_t {
            string_columns_data
                .entry("t".to_string())
                .or_insert_with(Vec::new)
                .push(t.clone());
        }
    }

    // Add 't' column
    if let Some(t_values) = string_columns_data.get("t") {
        columns.push(Arc::new(StringArray::from(t_values.clone())) as ArrayRef);
    } else {
        // If 't' column doesn't exist in string_columns_data, create it from temporals_t
        columns.push(Arc::new(StringArray::from(temporals_t.clone())) as ArrayRef);
    }

    // Add the string and float columns to the RecordBatch
    for (key, val) in string_columns_data {
        if key != "t" {
            columns.push(Arc::new(StringArray::from(val.clone())) as ArrayRef);
        }
    }
    for val in float_columns_data.values() {
        columns.push(Arc::new(Float64Array::from(val.clone())) as ArrayRef);
    }

    // Update the schema to match the columns
    if node_histories.is_empty() {
        fields = vec![Field::new("t", DataType::Utf8, false)];
    }

    let schema = Arc::new(Schema::new(fields));

    // Create the RecordBatch using these arrays and the schema
    let record_batch = RecordBatch::try_new(schema, columns)?;

    Ok(record_batch)
}


pub fn node_delays_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let node_delays = &input_data.node_delay;

    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("node1", DataType::Utf8, false),
        Field::new("node2", DataType::Utf8, false),
        Field::new("delay_t", DataType::Float64, false),
        Field::new("min_flow", DataType::Float64, false),
        Field::new("max_flow", DataType::Float64, false),
    ]);

    // Check if node_delays is empty
    if node_delays.is_empty() {
        // Create empty Arrow arrays
        let node1_array: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
        let node2_array: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
        let delay_array: ArrayRef = Arc::new(Float64Array::from(Vec::<f64>::new()));
        let min_flow_array: ArrayRef = Arc::new(Float64Array::from(Vec::<f64>::new()));
        let max_flow_array: ArrayRef = Arc::new(Float64Array::from(Vec::<f64>::new()));

        // Create the RecordBatch using these arrays and the schema
        return RecordBatch::try_new(
            Arc::new(schema),
            vec![node1_array, node2_array, delay_array, min_flow_array, max_flow_array],
        );
    }

    // Initialize vectors to hold the data
    let mut node1s: Vec<String> = Vec::new();
    let mut node2s: Vec<String> = Vec::new();
    let mut delays: Vec<f64> = Vec::new();
    let mut min_flows: Vec<f64> = Vec::new();
    let mut max_flows: Vec<f64> = Vec::new();

    // Populate the vectors from the node_delays
    for (node1, node2, delay, min_flow, max_flow) in node_delays {
        node1s.push(node1.clone());
        node2s.push(node2.clone());
        delays.push(*delay);
        min_flows.push(*min_flow);
        max_flows.push(*max_flow);
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


pub fn inflow_blocks_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    // Extract inflow_blocks and temporals from input_data
    let inflow_blocks = &input_data.inflow_blocks;

    // Check if inflow_blocks is empty
    if inflow_blocks.is_empty() {
        // Create a schema for the empty inflow_blocks scenario
        let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::Int32, false)]));

        // Create a vector with values from 1 to 10
        let t_values: Vec<i32> = (1..=10).collect();

        // Create an Arrow array from the vector
        let t_array: ArrayRef = Arc::new(Int32Array::from(t_values));

        // Create the RecordBatch using the array and the schema
        return RecordBatch::try_new(schema, vec![t_array]);
    }

    // Original processing for non-empty inflow_blocks
    let temporals = &input_data.temporals;

    // Collect all unique scenario names across all inflow blocks
    let mut scenario_names = HashMap::new();
    for block in inflow_blocks.values() {
        for ts in &block.data.ts_data {
            scenario_names.insert(ts.scenario.clone(), ());
        }
    }
    let scenario_names: Vec<String> = scenario_names.into_keys().collect();

    // Define the schema dynamically based on the scenario names
    let mut fields: Vec<Field> = vec![Field::new("t", DataType::Utf8, false)];
    for block in inflow_blocks.values() {
        fields.push(Field::new(&format!("{}, {}", block.name, block.node), DataType::Utf8, false));
        for scenario in &scenario_names {
            fields.push(Field::new(&format!("{}, {}", block.name, scenario), DataType::Float64, false));
        }
    }

    // Initialize data structures for each column
    let t_values: Vec<String> = temporals.t.clone();
    let mut start_times: HashMap<String, Vec<String>> = HashMap::new();
    let mut scenario_values: HashMap<String, Vec<Vec<f64>>> = HashMap::new();
    for block in inflow_blocks.values() {
        start_times.insert(block.name.clone(), vec![]);
        scenario_values.insert(block.name.clone(), vec![vec![]; scenario_names.len()]);
    }

    // Populate the vectors from the HashMap
    for block in inflow_blocks.values() {
        for ts in &block.data.ts_data {
            if ts.series.len() != temporals.t.len() {
                return Err(ArrowError::ComputeError("Timeseries length mismatch".to_string()));
            }
        }

        let start_time_vec = start_times.get_mut(&block.name).unwrap();
        let scenario_vecs = scenario_values.get_mut(&block.name).unwrap();
        for t in &temporals.t {
            start_time_vec.push(block.start_time.clone());

            for (j, ts) in block.data.ts_data.iter().enumerate() {
                if !ts.series.contains_key(t) {
                    return Err(ArrowError::ComputeError("Timeseries mismatch in temporal data".to_string()));
                }
                scenario_vecs[j].push(ts.series[t]);
            }
        }
    }

    // Create Arrow arrays from the vectors
    let t_array: ArrayRef = Arc::new(StringArray::from(t_values));
    let mut columns: Vec<ArrayRef> = vec![t_array];

    for block in inflow_blocks.values() {
        let start_times_array: ArrayRef = Arc::new(StringArray::from(start_times.remove(&block.name).unwrap_or_else(Vec::new)));
        columns.push(start_times_array);

        for values in scenario_values.remove(&block.name).unwrap_or_else(Vec::new) {
            columns.push(Arc::new(Float64Array::from(values)) as ArrayRef);
        }
    }

    // Create the schema
    let schema = Arc::new(Schema::new(fields));

    // Create the RecordBatch using these arrays and the schema
    RecordBatch::try_new(schema, columns)
}

pub fn markets_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let markets = &input_data.markets;

    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("market", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("node", DataType::Utf8, false),
        Field::new("processgroup", DataType::Utf8, false),
        Field::new("direction", DataType::Utf8, false),
        Field::new("realisation", DataType::Boolean, false), // New column for realisation
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
    let mut realisations: Vec<bool> = Vec::new(); // New vector for realisation
    let mut reserve_types: Vec<String> = Vec::new();
    let mut is_bids: Vec<bool> = Vec::new();
    let mut is_limiteds: Vec<bool> = Vec::new();
    let mut min_bids: Vec<f64> = Vec::new();
    let mut max_bids: Vec<f64> = Vec::new();
    let mut fees: Vec<f64> = Vec::new();

    for market in markets.values() {
        markets_vec.push(market.name.clone());
        m_types.push(market.var_type.clone());
        nodes.push(market.node.clone());
        processgroups.push(market.processgroup.clone());
        directions.push(market.direction.clone());
        realisations.push(!market.realisation.ts_data.is_empty()); // Check if realisation has time series data
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
    let realisations_array = Arc::new(BooleanArray::from(realisations)) as ArrayRef; // New array for realisation
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
            realisations_array, // Add realisation array to the RecordBatch
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

pub fn market_realisation_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let markets = &input_data.markets;
    let temporals = &input_data.temporals;

    // First, collect all unique scenario names across all markets
    let mut scenario_names = HashSet::new();
    for market in markets.values() {
        for ts in &market.realisation.ts_data {
            scenario_names.insert(ts.scenario.clone());
        }
    }
    let scenario_names: Vec<String> = scenario_names.into_iter().collect();

    // Define the schema dynamically based on the scenario names
    let mut fields: Vec<Field> = vec![Field::new("t", DataType::Utf8, false)];
    fields.extend(scenario_names.iter().map(|name| Field::new(name, DataType::Float64, true)));

    // Initialize a vector for the timestamps and data columns
    let mut timestamps: Vec<String> = Vec::new();
    let mut scenario_values: HashMap<String, Vec<Option<f64>>> = HashMap::new();

    // Handle the case where there are no realisation data
    let has_realisation_data = markets.values().any(|market| !market.realisation.ts_data.is_empty());
    if !has_realisation_data {
        timestamps = temporals.t.clone();
        // Initialize scenario values with None
        for scenario in &scenario_names {
            scenario_values.insert(scenario.clone(), vec![None; timestamps.len()]);
        }
    } else {
        // Collect timestamps and populate scenario values
        for market in markets.values() {
            for ts in &market.realisation.ts_data {
                for (timestamp, value) in &ts.series {
                    timestamps.push(timestamp.clone());
                    scenario_values.entry(ts.scenario.clone()).or_insert_with(|| vec![None; timestamps.len()]).push(Some(*value));
                }
            }
        }
        timestamps.sort();
        timestamps.dedup();
    }

    // Create Arrow arrays from the vectors
    let timestamps_array: ArrayRef = Arc::new(StringArray::from(timestamps));
    let mut columns: Vec<ArrayRef> = vec![timestamps_array];
    for scenario in scenario_names {
        let values = scenario_values.remove(&scenario).unwrap_or_else(|| vec![None; columns[0].len()]);
        columns.push(Arc::new(Float64Array::from(values)) as ArrayRef);
    }

    // Create the schema
    let schema = Arc::new(Schema::new(fields));

    // Create the RecordBatch using these arrays and the schema
    RecordBatch::try_new(schema, columns)
}

pub fn market_reserve_activation_price_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let markets = &input_data.markets;
    let temporals = &input_data.temporals;

    // First, collect all unique scenario names across all markets
    let mut scenario_names = HashSet::new();
    for market in markets.values() {
        for ts in &market.reserve_activation_price.ts_data {
            scenario_names.insert(ts.scenario.clone());
        }
    }
    let scenario_names: Vec<String> = scenario_names.into_iter().collect();

    // Define the schema dynamically based on the scenario names
    let mut fields: Vec<Field> = vec![Field::new("t", DataType::Utf8, false)];
    fields.extend(scenario_names.iter().map(|name| Field::new(name, DataType::Float64, true)));

    // Initialize a vector for the timestamps and data columns
    let mut timestamps: Vec<String> = Vec::new();
    let mut scenario_values: HashMap<String, Vec<Option<f64>>> = HashMap::new();

    // Handle the case where there are no reserve_activation_price data
    let has_reserve_activation_price_data = markets.values().any(|market| !market.reserve_activation_price.ts_data.is_empty());
    if (!has_reserve_activation_price_data) {
        timestamps = temporals.t.clone();
        // Initialize scenario values with None
        for scenario in &scenario_names {
            scenario_values.insert(scenario.clone(), vec![None; timestamps.len()]);
        }
    } else {
        // Collect timestamps and populate scenario values
        for market in markets.values() {
            for ts in &market.reserve_activation_price.ts_data {
                for (timestamp, value) in &ts.series {
                    timestamps.push(timestamp.clone());
                    scenario_values.entry(ts.scenario.clone()).or_insert_with(|| vec![None; timestamps.len()]).push(Some(*value));
                }
            }
        }
        timestamps.sort();
        timestamps.dedup();
    }

    // Create Arrow arrays from the vectors
    let timestamps_array: ArrayRef = Arc::new(StringArray::from(timestamps));
    let mut columns: Vec<ArrayRef> = vec![timestamps_array];
    for scenario in scenario_names {
        let values = scenario_values.remove(&scenario).unwrap_or_else(|| vec![None; columns[0].len()]);
        columns.push(Arc::new(Float64Array::from(values)) as ArrayRef);
    }

    // Create the schema
    let schema = Arc::new(Schema::new(fields));

    // Create the RecordBatch using these arrays and the schema
    RecordBatch::try_new(schema, columns)
}


pub fn scenarios_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
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
    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![names_array, probabilities_array],
    )?;

    Ok(record_batch)
}

pub fn processes_eff_fun_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let processes = &input_data.processes;

    // Determine the maximum length of eff_fun across all processes
    let max_length = processes.values()
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

pub fn reserve_type_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let reserve_type = &input_data.reserve_type;

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

pub fn risk_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
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

pub fn processes_cap_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {

    let temporals = &input_data.temporals;
    let processes = &input_data.processes;

    let mut fields: Vec<Field> = vec![Field::new("t", DataType::Utf8, false)];
    let mut columns: Vec<ArrayRef> = vec![];
    let mut column_data: HashMap<String, Vec<Option<f64>>> = HashMap::new();

    let mut has_ts_data = false;

    for (process_name, process) in processes {
        if process.eff_ts.ts_data.is_empty() {
            continue;
        }

        has_ts_data = true;

        // Check if the timestamps match temporals.t
        check_timestamps_match(&temporals.t, &process.eff_ts.ts_data)?;

        for topology in &process.topos {
            let flow = if topology.sink == *process_name {
                &topology.source
            } else if topology.source == *process_name {
                &topology.sink
            } else {
                continue;
            };

            for time_series in &process.eff_ts.ts_data {
                let column_name = format!("{},{},{}", process_name, flow, time_series.scenario);
                fields.push(Field::new(&column_name, DataType::Float64, true));

                let series_data: Vec<Option<f64>> = time_series.series.iter().map(|(_, value)| Some(*value)).collect();

                if !series_data.is_empty() {
                    column_data.insert(column_name, series_data);
                }
            }
        }
    }

    // Create the timestamp column
    let timestamp_column = Arc::new(StringArray::from(temporals.t.clone())) as ArrayRef;
    columns.push(timestamp_column);

    // Check if we have any data to insert
    if !has_ts_data || column_data.is_empty() {
        // Return a RecordBatch with only the t column
        let schema = Arc::new(Schema::new(fields));
        return RecordBatch::try_new(schema, columns);
    }

    // Create Arrow columns from the collected data
    for data in column_data.values() {
        columns.push(Arc::new(Float64Array::from(data.clone())) as ArrayRef);
    }

    // Create the schema from the field definitions
    let schema = Arc::new(Schema::new(fields));

    // Construct and return the RecordBatch
    RecordBatch::try_new(schema, columns)
}

pub fn gen_constraints_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {

    let temporals = &input_data.temporals;
    let gen_constraints = &input_data.gen_constraints;
    
    let mut fields: Vec<Field> = Vec::new();
    let mut column_data: HashMap<String, Vec<Option<f64>>> = HashMap::new();
    let timestamps: Vec<Option<String>> = temporals.t.iter().map(|t| Some(t.clone())).collect();

    fields.push(Field::new("t", DataType::Utf8, false)); // Timestamp column is not nullable

    // Handle the constant TimeSeriesData for each GenConstraint
    for (constraint_name, gen_constraint) in gen_constraints.iter() {
        for ts in &gen_constraint.constant.ts_data {
            let col_name = format!("{},{}", constraint_name, ts.scenario);
            fields.push(Field::new(&col_name, DataType::Float64, true));
            let series_data: Vec<Option<f64>> = ts.series.iter().map(|(_, value)| Some(*value)).collect();

            // Check if the series data timestamps match temporals.t
            if ts.series.len() != temporals.t.len() {
                return Err(ArrowError::ComputeError("Timeseries length mismatch".to_string()));
            }
            for (i, (timestamp, _)) in ts.series.iter().enumerate() {
                if &temporals.t[i] != timestamp {
                    return Err(ArrowError::ComputeError("Timeseries mismatch in temporal data".to_string()));
                }
            }

            if !series_data.is_empty() {
                column_data.insert(col_name, series_data);
            }
        }
    }

    // Handle the ConFactor data
    for (constraint_name, gen_constraint) in gen_constraints.iter() {
        for factor in &gen_constraint.factors {
            for ts in &factor.data.ts_data {
                let var_tuple_component = if factor.var_tuple.1.is_empty() {
                    factor.var_tuple.0.clone()
                } else {
                    format!("{},{}", factor.var_tuple.0, factor.var_tuple.1)
                };
                let col_name = format!("{},{},{}", constraint_name, var_tuple_component, ts.scenario);
                fields.push(Field::new(&col_name, DataType::Float64, true));
                let series_data: Vec<Option<f64>> = ts.series.iter().map(|(_, value)| Some(*value)).collect();

                // Check if the series data timestamps match temporals.t
                if ts.series.len() != temporals.t.len() {
                    return Err(ArrowError::ComputeError("Timeseries length mismatch".to_string()));
                }
                for (i, (timestamp, _)) in ts.series.iter().enumerate() {
                    if &temporals.t[i] != timestamp {
                        return Err(ArrowError::ComputeError("Timeseries mismatch in temporal data".to_string()));
                    }
                }

                if !series_data.is_empty() {
                    column_data.insert(col_name, series_data);
                }
            }
        }
    }

    // Initialize the columns vector
    let mut columns: Vec<ArrayRef> = Vec::new();

    // Check if we have any data to insert
    if column_data.is_empty() {
        let schema = Arc::new(Schema::new(fields));
        return RecordBatch::try_new(schema, columns);
    }

    // Create the timestamp column
    let timestamp_column: ArrayRef = Arc::new(StringArray::from(timestamps.iter().flatten().cloned().collect::<Vec<String>>()));
    columns.push(timestamp_column);

    // Create other columns from the collected column_data
    for field in &fields[1..] { // Skip the timestamp field
        if let Some(col_data) = column_data.remove(field.name()) {
            let col_array = Float64Array::from(col_data);
            columns.push(Arc::new(col_array) as ArrayRef);
        }
    }

    let schema = Arc::new(Schema::new(fields));

    // Create the RecordBatch using these arrays and the schema
    RecordBatch::try_new(schema, columns)
}

pub fn constraints_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
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
        types.push(gen_constraint.var_type.clone());
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

pub fn bid_slots_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let bid_slots = &input_data.bid_slots;
    let temporals = &input_data.temporals;

    // Define the schema for the Arrow RecordBatch
    let schema = Schema::new(vec![
        Field::new("t", DataType::Utf8, false),
    ]);

    // Initialize a vector to hold the timestamps
    let mut timestamps: Vec<String> = Vec::new();

    // Check if bid_slots has data
    let has_bid_slots = !bid_slots.is_empty();

    if has_bid_slots {
        // If there are bid_slots, collect the time_steps from each BidSlot
        for bid_slot in bid_slots.values() {
            timestamps.extend(bid_slot.time_steps.clone());
        }
    } else {
        // If there are no bid_slots, use the timestamps from temporals.t
        timestamps = temporals.t.clone();
    }

    // Create an Arrow array from the timestamps
    let timestamps_array: ArrayRef = Arc::new(StringArray::from(timestamps)) as ArrayRef;

    // Create the RecordBatch using the array and the schema
    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![timestamps_array],
    )?;

    Ok(record_batch)
}

pub fn processes_cf_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {

    let temporals = &input_data.temporals;
    let processes = &input_data.processes;

    let mut fields: Vec<Field> = vec![Field::new("t", DataType::Utf8, false)];
    let mut columns: Vec<ArrayRef> = vec![];

    let mut has_ts_data = false;

    // Validate timestamps in each process
    for process in processes.values() {
        if !process.cf.ts_data.is_empty() {
            has_ts_data = true;
            check_timestamps_match(&temporals.t, &process.cf.ts_data)?;
        }
    }

    // If no ts_data was found, return a RecordBatch with only the t column
    if !has_ts_data {
        let schema = Arc::new(Schema::new(fields.clone()));
        let t_column: ArrayRef = Arc::new(StringArray::from(temporals.t.clone())) as ArrayRef;
        return RecordBatch::try_new(schema, vec![t_column]);
    }

    // Extract the temporals.t as timestamps
    let timestamps = temporals.t.clone();

    // Validate and collect timestamps, and create fields for each time series
    for process in processes.values() {
        for ts in &process.cf.ts_data {
            fields.push(Field::new(&format!("{},{}", process.name, ts.scenario), DataType::Float64, true));
        }
    }

    // Create the timestamp column
    let timestamp_column = StringArray::from(timestamps.clone());
    columns.push(Arc::new(timestamp_column) as ArrayRef);

    // Collect data for each time series and add it to the columns
    for process in processes.values() {
        for ts in &process.cf.ts_data {
            // Match timestamps to values
            let column_values: Vec<Option<f64>> = timestamps
                .iter()
                .map(|t| {
                    ts.series
                        .get(t) // Use get to directly retrieve the value associated with the key
                        .copied() // Convert Option<&f64> to Option<f64>
                })
                .collect();

            // Add the data column
            columns.push(Arc::new(Float64Array::from(column_values)) as ArrayRef);
        }
    }

    // Create the schema from the fields
    let schema = Arc::new(Schema::new(fields));

    // Construct and return the RecordBatch
    RecordBatch::try_new(schema, columns)
}

pub fn market_fixed_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let markets = &input_data.markets;

    // Gather all timestamps and sort them
    let mut all_timestamps: Vec<String> = markets.values()
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
    fields.extend(markets.keys().map(|name| Field::new(name, DataType::Float64, true)));

    // Create the columns
    let timestamp_array: ArrayRef = Arc::new(StringArray::from(all_timestamps));
    let mut columns: Vec<ArrayRef> = vec![timestamp_array];
    for market_name in markets.keys() {
        let column_data = columns_data.remove(market_name).expect("Market data should be present");
        let column_array: ArrayRef = Arc::new(Float64Array::from(column_data));
        columns.push(column_array);
    }

    let schema = Arc::new(Schema::new(fields));

    // Create the RecordBatch using these arrays and the schema
    RecordBatch::try_new(schema, columns)
}

pub fn market_price_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let temporals_t = &input_data.temporals.t;

    // Ensure temporals_t is not empty
    if temporals_t.is_empty() {
        return Err(ArrowError::InvalidArgumentError("Temporals timestamps are empty".to_string()));
    }

    // Check if the timestamps in market price data match temporals_t
    for market in input_data.markets.values() {
        check_timestamps_match(temporals_t, &market.price.ts_data)?;
    }

    // Collect all timestamps and initialize columns
    let mut columns: HashMap<String, Vec<f64>> = HashMap::new();
    let mut unique_timestamps: HashSet<String> = HashSet::new();

    for market in input_data.markets.values() {
        for data in &market.price.ts_data {
            for (timestamp, _) in &data.series {
                unique_timestamps.insert(timestamp.clone());
            }
        }
    }

    let sorted_timestamps: Vec<String> = temporals_t.clone();

    // If there are no unique timestamps, return an empty RecordBatch
    if sorted_timestamps.is_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::Utf8, false)]));
        let timestamp_array: ArrayRef = Arc::new(StringArray::from(sorted_timestamps));
        let arrays = vec![timestamp_array];
        return RecordBatch::try_new(schema, arrays);
    }

    // Initialize column data
    for market in input_data.markets.values() {
        for data in &market.price.ts_data {
            let scenario = &data.scenario;
            let column_name = format!("{},{}", market.name, scenario); // Updated column name format
            let mut column_data = vec![f64::NAN; sorted_timestamps.len()]; // Fill with NaN for missing data

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

    // Sort columns by scenario to ensure order: first all s1, then s2, then s3, etc.
    let mut column_names: Vec<String> = columns.keys().cloned().collect();
    column_names.sort_by(|a, b| {
        let a_parts: Vec<&str> = a.split(',').collect();
        let b_parts: Vec<&str> = b.split(',').collect();
        a_parts[1].cmp(&b_parts[1]).then_with(|| a_parts[0].cmp(&b_parts[0]))
    });

    // Prepare the schema and arrays
    let mut fields = vec![Field::new("t", DataType::Utf8, false)];
    let timestamp_array: ArrayRef = Arc::new(StringArray::from(sorted_timestamps));
    let mut arrays = vec![timestamp_array];

    for name in column_names {
        fields.push(Field::new(&name, DataType::Float64, true));
        let array: ArrayRef = Arc::new(Float64Array::from(columns.get(&name).unwrap().clone()));
        arrays.push(array);
    }

    // Create the RecordBatch
    let schema = Arc::new(Schema::new(fields));
    let record_batch = RecordBatch::try_new(schema, arrays)?;
    Ok(record_batch)
}

pub fn market_balance_price_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let temporals_t = &input_data.temporals.t;

    // Ensure temporals_t is not empty
    if temporals_t.is_empty() {
        return Err(ArrowError::InvalidArgumentError("Temporals timestamps are empty".to_string()));
    }

    // Check if the timestamps in market price data match temporals_t
    for market in input_data.markets.values().filter(|m| m.var_type == "energy") {
        check_timestamps_match(temporals_t, &market.up_price.ts_data)?;
        check_timestamps_match(temporals_t, &market.down_price.ts_data)?;
    }

    // Collect all timestamps and initialize columns
    let mut columns: HashMap<String, Vec<f64>> = HashMap::new();
    let mut unique_timestamps: HashSet<String> = HashSet::new();

    for market in input_data.markets.values().filter(|m| m.var_type == "energy") {
        for data in [&market.up_price, &market.down_price].iter() {
            for ts_data in &data.ts_data {
                for (timestamp, _) in &ts_data.series {
                    unique_timestamps.insert(timestamp.clone());
                }
            }
        }
    }

    let sorted_timestamps: Vec<String> = temporals_t.clone();

    // If there are no unique timestamps, return an empty RecordBatch
    if sorted_timestamps.is_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::Utf8, false)]));
        let timestamp_array: ArrayRef = Arc::new(StringArray::from(sorted_timestamps));
        let arrays = vec![timestamp_array];
        return RecordBatch::try_new(schema, arrays);
    }

    // Initialize column data for up and down prices
    for market in input_data.markets.values().filter(|m| m.var_type == "energy") {
        for (label, price_data) in [("up", &market.up_price), ("dw", &market.down_price)].iter() {
            for data in &price_data.ts_data {
                let column_name = format!("{},{},{}", market.name, label, data.scenario);
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
    }

    // Sort columns by scenario to ensure order: first all s1, then s2, then s3, etc.
    let mut column_names: Vec<String> = columns.keys().cloned().collect();
    column_names.sort_by(|a, b| {
        let a_parts: Vec<&str> = a.split(',').collect();
        let b_parts: Vec<&str> = b.split(',').collect();
        a_parts[2].cmp(&b_parts[2]).then_with(|| a_parts[0].cmp(&b_parts[0]))
    });

    // Prepare the schema and arrays
    let mut fields = vec![Field::new("t", DataType::Utf8, false)];
    let timestamp_array: ArrayRef = Arc::new(StringArray::from(sorted_timestamps));
    let mut arrays = vec![timestamp_array];

    for name in column_names {
        fields.push(Field::new(&name, DataType::Float64, true));
        let array: ArrayRef = Arc::new(Float64Array::from(columns.get(&name).unwrap().clone()));
        arrays.push(array);
    }

    // Create the RecordBatch
    let schema = Arc::new(Schema::new(fields));
    let record_batch = RecordBatch::try_new(schema, arrays)?;
    Ok(record_batch)
}


// This function converts a HashMap<String, Node> of inflow TimeSeriesData to an Arrow RecordBatch
pub fn nodes_inflow_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let temporals_t = &input_data.temporals.t;
    let nodes = &input_data.nodes;

    // Ensure temporals_t is not empty
    if temporals_t.is_empty() {
        return Err(ArrowError::InvalidArgumentError("Temporals timestamps are empty".to_string()));
    }

    let mut fields = vec![Field::new("t", DataType::Utf8, false)];
    let mut columns: Vec<ArrayRef> = Vec::new();

    // Determine the common length using the timestamps of the temporals
    let timestamps = temporals_t.clone();
    let common_length = timestamps.len();
    let timestamp_array = Arc::new(StringArray::from(timestamps.clone())) as ArrayRef;
    columns.push(timestamp_array);

    // Collect inflow data for each node and scenario
    for (node_name, node) in nodes {
        if node.inflow.ts_data.is_empty() || node.inflow.ts_data.iter().all(|ts| ts.series.is_empty()) {
            continue;
        }

        // Check if the timestamps in node inflow data match temporals_t
        if let Err(e) = check_timestamps_match(temporals_t, &node.inflow.ts_data) {
            println!(
                "Timestamp mismatch in node '{}'. Error: {}",
                node_name, e
            );
            return Err(e);
        }

        for ts in &node.inflow.ts_data {
            if ts.series.is_empty() {
                continue;
            }

            let column_name = format!("{},{}", node_name, ts.scenario);
            fields.push(Field::new(&column_name, DataType::Float64, true));

            // Prepare values and ensure they have the same length as the timestamps
            let values: Vec<Option<f64>> = timestamps.iter().map(|t| {
                ts.series.get(t).copied()
            }).collect();

            if values.len() != common_length {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Inconsistent data length for node '{}', scenario '{}': expected {}, got {}",
                    node_name, ts.scenario, common_length, values.len()
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

pub fn nodes_commodity_price_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let temporals_t = &input_data.temporals.t;
    let nodes = &input_data.nodes;

    // Ensure temporals_t is not empty
    if temporals_t.is_empty() {
        return Err(ArrowError::InvalidArgumentError("Temporals timestamps are empty".to_string()));
    }

    // Check if the timestamps in node cost data match temporals_t
    for node in nodes.values().filter(|n| n.is_commodity) {
        check_timestamps_match(temporals_t, &node.cost.ts_data)?;
    }

    let mut fields = vec![Field::new("t", DataType::Utf8, false)];
    let mut columns: Vec<ArrayRef> = Vec::new();

    // First column from Temporals
    let timestamp_array = Arc::new(StringArray::from(temporals_t.clone())) as ArrayRef;
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

            let values: Vec<Option<f64>> = ts.series.iter().map(|(_, value)| Some(*value)).collect();
            if values.len() != common_length {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Inconsistent data length for node '{}', scenario '{}': expected {}, got {}",
                    node_name, ts.scenario, common_length, values.len()
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

// Function to convert eff_ops of Process to an Arrow RecordBatch
pub fn _processes_eff_ops_to_arrow(
    processes: &HashMap<String, input_data::Process>,
) -> Result<RecordBatch, ArrowError> {
    let mut fields: Vec<Field> = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();

    // Calculate the maximum number of operations across all processes
    let max_ops = processes.values()
                           .map(|p| p.eff_ops.len())
                           .max()
                           .unwrap_or(0);
    
    for i in 0..max_ops {
        let column_name = format!("op{}", i+1);
        fields.push(Field::new(&column_name, DataType::Float64, true));
    }

    // Create a data column for each operation index
    for i in 0..max_ops {
        let mut column_values: Vec<Option<f64>> = Vec::new();

        for process in processes.values() {
            if i < process.eff_ops.len() {
                match process.eff_ops[i].parse::<f64>() {
                    Ok(num) => column_values.push(Some(num)),
                    Err(_) => column_values.push(None),
                }
            } else {
                column_values.push(None); // Pad with None if the process has fewer ops than max_ops
            }
        }

        let float_array = Arc::new(Float64Array::from(column_values)) as ArrayRef;
        columns.push(float_array);
    }

    if columns.len() != fields.len() {
        println!("Mismatch error: {} columns for {} fields", columns.len(), fields.len());
        return Err(ArrowError::InvalidArgumentError(format!(
            "number of columns({}) must match number of fields({}) in schema",
            columns.len(),
            fields.len()
        )));
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}

pub fn processes_eff_to_arrow(input_data: &input_data::InputData) -> Result<RecordBatch, ArrowError> {
    let processes = &input_data.processes;
    let temporals_t = &input_data.temporals.t;

    // Ensure temporals_t is not empty
    if temporals_t.is_empty() {
        return Err(ArrowError::InvalidArgumentError("Temporals timestamps are empty".to_string()));
    }

    // Check if the timestamps in process efficiency data match temporals_t
    for process in processes.values() {
        check_timestamps_match(temporals_t, &process.eff_ts.ts_data)?;
    }

    let mut fields: Vec<Field> = vec![Field::new("t", DataType::Utf8, false)];
    let mut columns: Vec<ArrayRef> = Vec::new();

    // Create the timestamp column from Temporals
    let timestamp_array = Arc::new(StringArray::from(temporals_t.clone())) as ArrayRef;
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
                    process_name, time_series.scenario, common_length, values.len()
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

    // Construct the schema from the fields
    let schema = Arc::new(Schema::new(fields));

    // Construct the record batch
    let record_batch = RecordBatch::try_new(schema, columns)?;

    Ok(record_batch)
}


// Function to convert Vec<String> of timeseries data to RecordBatch
pub fn _timeseries_to_arrow(timeseries: Vec<String>) -> Result<RecordBatch, ArrowError> {
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

// Function to check timestamps match
pub fn check_timestamps_match(
    temporals_t: &Vec<String>,
    ts_data: &Vec<input_data::TimeSeries>,
) -> Result<(), ArrowError> {
    for ts in ts_data {
        let ts_timestamps: Vec<String> = ts.series.keys().cloned().collect();
        if ts_timestamps != *temporals_t {
            return Err(ArrowError::InvalidArgumentError("Timestamps do not match temporals.t".to_string()));
        }
    }
    Ok(())
}

#[derive(Debug)]
pub struct DataFrame {
    schema: SchemaRef,
    columns: HashMap<String, ArrayRef>,
}

impl DataFrame {
    pub fn new(batch: &RecordBatch) -> Self {
        let schema = batch.schema();
        let mut columns = HashMap::new();
        for i in 0..batch.num_columns() {
            let field = schema.field(i);
            columns.insert(field.name().clone(), batch.column(i).clone());
        }
        DataFrame {
            schema,
            columns,
        }
    }

    pub fn print(&self) {
        println!("Schema: {:?}", self.schema);
        for (name, array) in &self.columns {
            println!("Column: {}", name);
            match array.data_type() {
                arrow::datatypes::DataType::Float64 => {
                    let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    for i in 0..arr.len() {
                        println!("  {}", arr.value(i));
                    }
                }
                arrow::datatypes::DataType::Utf8 => {
                    let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                    for i in 0..arr.len() {
                        println!("  {}", arr.value(i));
                    }
                }
                arrow::datatypes::DataType::Int32 => {
                    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                    for i in 0..arr.len() {
                        println!("  {}", arr.value(i));
                    }
                }
                arrow::datatypes::DataType::Int64 => {
                    let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    for i in 0..arr.len() {
                        println!("  {}", arr.value(i));
                    }
                }
                arrow::datatypes::DataType::Boolean => {
                    let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                    for i in 0..arr.len() {
                        println!("  {}", arr.value(i));
                    }
                }
                _ => println!("Unsupported data type"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml;
    use std::fs::File;
    use std::io::Read;
    use std::path::PathBuf;
    use std::env;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use arrow::array::{StringArray, ArrayRef};

    pub fn read_yaml_file(file_path: &str) -> Result<input_data::InputData, FileReadError> {
        let mut file = File::open(file_path).map_err(FileReadError::open_error)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents).map_err(FileReadError::read_error)?;
    
        match serde_yaml::from_str(&contents) {
            Ok(data) => Ok(data),
            Err(e) => {
                // Log or handle the detailed parsing error message here
                eprintln!("Failed to parse YAML content: {}", e);
                Err(FileReadError::Parse(e))
            }
        }
    }

    #[test]
    fn test_gen_constraints_to_arrow() {
        // Define the path to the mock data file
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests/mocks/arrow_tests/full_data.yaml");
        assert!(path.exists(), "Test file does not exist at the expected path");

        // Read the mock input data from the YAML file
        let input_data = read_yaml_file::<InputData>(path.to_str().unwrap()).expect("Should read the YAML file correctly");

        // Call the function
        let result = gen_constraints_to_arrow(&input_data);

        // Check and print the result
        match result {
            Ok(record_batch) => {
                println!("Schema: {:?}", record_batch.schema());
                for column in record_batch.columns() {
                    println!("{:?}", column);
                }
            }
            Err(e) => {
                eprintln!("Error creating Arrow table: {:?}", e);
            }
        }
    }

    #[test]
    fn test_input_datasetup_to_arrow() {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests/mocks/arrow_tests/full_data.yaml");
        assert!(path.exists(), "Test file does not exist at the expected path");

        let input_data = read_yaml_file(path.to_str().unwrap()).expect("Should read the YAML file correctly");

        // Dynamic Schema Construction
        let schema = Schema::new(vec![
            Field::new("parameter", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]);

        // Include all fields and check for non-empty 'common_scenario_name' to include it
        let mut parameters = vec![
            "contains_reserves", "contains_online", "contains_states", "contains_piecewise_eff",
            "contains_risk", "contains_diffusion", "contains_delay", "contains_markets",
            "reserve_realisation", "use_market_bids", "common_timesteps",
            "use_node_dummy_variables", "use_ramp_dummy_variables", "common_scenario_name"
        ];

        let mut values = vec![
            input_data.setup.contains_reserves.to_string(), input_data.setup.contains_online.to_string(),
            input_data.setup.contains_states.to_string(), input_data.setup.contains_piecewise_eff.to_string(),
            input_data.setup.contains_risk.to_string(), input_data.setup.contains_diffusion.to_string(),
            input_data.setup.contains_delay.to_string(), input_data.setup.contains_markets.to_string(),
            input_data.setup.reserve_realisation.to_string(), input_data.setup.use_market_bids.to_string(),
            input_data.setup.common_timesteps.to_string(), input_data.setup.use_node_dummy_variables.to_string(),
            input_data.setup.use_ramp_dummy_variables.to_string(), input_data.setup.common_scenario_name.clone()
        ];

        let parameter_array = Arc::new(StringArray::from(parameters)) as ArrayRef;
        let value_array = Arc::new(StringArray::from(values)) as ArrayRef;
        let expected_record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![parameter_array, value_array]
        ).unwrap();

        // Test the function
        let result = inputdatasetup_to_arrow(&input_data).expect("Conversion should succeed");

        // Validate the output
        assert_eq!(result.num_columns(), expected_record_batch.num_columns());
        assert_eq!(result.num_rows(), expected_record_batch.num_rows());

        for (i, column) in result.columns().iter().enumerate() {
            let expected_column = expected_record_batch.column(i).as_any().downcast_ref::<StringArray>().unwrap();
            let result_column = column.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(result_column, expected_column, "Column data does not match");
        }
    }

    #[test]
    fn test_nodes_to_arrow() {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests/mocks/arrow_tests/full_data.yaml");
        assert!(path.exists(), "Test file does not exist at the expected path");

        let input_data = read_yaml_file(path.to_str().unwrap()).expect("Should read the YAML file correctly");

        let batch = nodes_to_arrow(&input_data).expect("Conversion to arrow should succeed");

        // Define expected schema and order of the fields
        let expected_fields = vec![
            "node", "is_commodity", "is_state", "is_res", "is_market", "is_inflow",
            "state_max", "state_min", "in_max", "out_max", "initial_state",
            "state_loss_proportional", "scenario_independent_state", "is_temp", "T_E_conversion", "residual_value"
        ];

        let expected_schema = Schema::new(vec![
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
            Field::new("T_E_conversion", DataType::Float64, false),
            Field::new("residual_value", DataType::Float64, false),
        ]);

        // Verify schema and order of fields
        assert_eq!(batch.schema().fields().len(), expected_fields.len(), "Number of fields does not match");
        for (i, field) in batch.schema().fields().iter().enumerate() {
            assert_eq!(field.name(), expected_fields[i], "Field name or order does not match expected");
        }

        // Verify the content of the first node 'interiorair'
        let names_array = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let is_commodity_array = batch.column(1).as_any().downcast_ref::<BooleanArray>().unwrap();
        let is_state_array = batch.column(2).as_any().downcast_ref::<BooleanArray>().unwrap();

        // More fields as needed
        let initial_state_array = batch.column(10).as_any().downcast_ref::<Float64Array>().unwrap();
        let t_e_conversion_array = batch.column(14).as_any().downcast_ref::<Float64Array>().unwrap();

        // Check values specifically for 'interiorair'
        assert_eq!(names_array.value(0), "interiorair", "Node name mismatch");
        assert_eq!(is_commodity_array.value(0), false, "is_commodity mismatch for interiorair");
        assert_eq!(is_state_array.value(0), true, "is_state mismatch for interiorair");
        assert_eq!(initial_state_array.value(0), 296.15, "initial_state mismatch for interiorair");
        assert_eq!(t_e_conversion_array.value(0), 0.5, "t_e_conversion mismatch for interiorair");
    } 

    #[test]
    fn test_read_yaml_file_with_empty_data() {
        // Construct the path to the empty YAML file
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests/mocks/arrow_tests/empty_input_data.yaml"); // Adjust the path as necessary

        // Check if the file actually exists to avoid false negatives
        assert!(path.exists(), "Test file does not exist at the expected path");

        // Attempt to read and parse the empty YAML file
        let result = read_yaml_file(path.to_str().unwrap());

        // Check that the result is an error
        assert!(result.is_err(), "Expected an error for empty input data, but got Ok");
        
        // Optionally, to assert specific error types or messages:
        if let Err(e) = result {
            match e {
                FileReadError::Parse(parse_error) => {
                    println!("Received expected parsing error: {}", parse_error);
                },
                _ => panic!("Expected a parsing error, but got a different error"),
            }
        }
    }

    #[test]
    fn test_read_yaml_file_with_wrong_format() {
        // Construct the path to the YAML file with incorrect format
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests/mocks/arrow_tests/wrong_input_data.yaml"); // Adjust the path as necessary

        // Check if the file actually exists to avoid false negatives
        assert!(path.exists(), "Test file does not exist at the expected path");

        // Attempt to read and parse the YAML file with wrong format
        let result = read_yaml_file(path.to_str().unwrap());

        // Check that the result is an error
        assert!(result.is_err(), "Expected an error for wrongly formatted input data, but got Ok");
        
        // Optionally, to assert specific error types or messages:
        if let Err(e) = result {
            match e {
                FileReadError::Parse(parse_error) => {
                    println!("Received expected parsing error due to format issue: {}", parse_error);
                },
                _ => panic!("Expected a parsing error due to wrong format, but got a different type of error"),
            }
        }
    }
}