use crate::julia_interface;
use crate::utilities;
use crate::input_data;
use jlrs::prelude::*;
use std::collections::HashMap;
use jlrs::memory::target::frame;


pub struct RunPredicer {
    pub data: input_data::InputData,
    pub predicer_dir: String,
}

// Define the type for control values
pub type ControlValues = Vec<(String, f64)>;

#[async_trait(?Send)]
impl AsyncTask for RunPredicer {
    // Different tasks can return different results. If successful, this task returns an `f64`.
    type Output = HashMap<String, Vec<(String, f64)>>;
    type Affinity = DispatchAny;

    async fn run<'base>(&mut self, mut frame: AsyncGcFrame<'base>) -> JlrsResult<Self::Output> {

        unsafe { RuntimeBuilder::new().async_runtime::<Tokio>().start_async::<1>().expect("Could not init Julia") };
    
        unsafe {

            let jl_predicer_dir = JuliaString::new(&mut frame, self.predicer_dir.clone()).as_value();
            Value::eval_string(&mut frame, "using Pkg").unwrap();
            julia_interface::call(&frame, &["Pkg", "activate"], &[jl_predicer_dir]).unwrap();
            julia_interface::call(&frame, &["Pkg", "instantiate"], &[]).unwrap();
            Value::eval_string(&mut frame, "using Predicer").unwrap();

            let mut list: Vec<Value> = Vec::new();

            //Create processes
            
            let dict_processes = julia_interface::call(
                &mut frame,
                &["Predicer", "create_processes"],
                &[],
            )
            .into_jlrs_result();

            match dict_processes {
                Ok(processes_value) => {
                    create_processes(&mut frame, &self.data.processes, processes_value);
                    list.push(processes_value.clone());

                }
                Err(error) => println!("Error returning processes: {:?}", error),
            }

            //Create nodes

            let dict_nodes = julia_interface::call(
                &mut frame,
                &["Predicer", "create_nodes"],
                &[],
            )
            .into_jlrs_result();

            match dict_nodes {
                Ok(nodes_value) => {
                    create_nodes(&mut frame, &self.data.nodes, nodes_value);
                    list.push(nodes_value.clone());

                }
                Err(error) => println!("Error returning nodes: {:?}", error),
            }

            //Create node diffusion

            let dict_node_diffusion = julia_interface::call(
                &mut frame,
                &["Predicer", "create_node_diffusion_tuples"],
                &[],
            )
            .into_jlrs_result();

            match dict_node_diffusion {
                Ok(node_diffusion_value) => {
                    create_node_diffusion(&mut frame, self.data.node_diffusion.clone(), node_diffusion_value);
                    list.push(node_diffusion_value.clone());

                }
                Err(error) => println!("Error returning nodes: {:?}", error),
            }

            //Create node_delay

            let dict_node_delay = julia_interface::call(
                &mut frame,
                &["Predicer", "create_node_delay_tuples"],
                &[],
            )
            .into_jlrs_result();

            match dict_node_delay {
                Ok(node_delay_value) => {
                    create_node_delay(&mut frame, self.data.node_delay.clone(), node_delay_value);
                    list.push(node_delay_value.clone());

                }
                Err(error) => println!("Error returning nodes: {:?}", error),
            }

            //Create node histories

            let args1 = [];
            let node_histories = julia_interface::call(
                &mut frame,
                &["Predicer", "create_node_histories"],
                &args1,
            )
            .into_jlrs_result();

            match node_histories {
                Ok(nodehistory_value) => {
                    //list[4]
                    list.push(nodehistory_value);
                }
                Err(error) => println!("Error adding node histories: {:?}", error),
            }

            //Create markets

            let dict_markets = julia_interface::call(
                &mut frame,
                &["Predicer", "create_markets"],
                &[],
            )
            .into_jlrs_result();

            match dict_markets {
                Ok(markets_value) => {
                    create_markets(&mut frame, &self.data.markets, markets_value);
                    list.push(markets_value.clone());

                }
                Err(error) => println!("Error creating markets: {:?}", error),
            }

            //Create groups

            let dict_groups = julia_interface::call(
                &mut frame,
                &["Predicer", "create_groups"],
                &[],
            )
            .into_jlrs_result();

            match dict_groups {
                Ok(groups_value) => {
                    create_groups(&mut frame, &self.data.groups, groups_value);
                    list.push(groups_value.clone());

                }
                Err(error) => println!("Error creating markets: {:?}", error),
            }

            //Create gen constraints

            let dict_genconstraints = julia_interface::call(
                &mut frame,
                &["Predicer", "create_genconstraints"],
                &[],
            )
            .into_jlrs_result();

            match dict_genconstraints {
                Ok(genconstraints_value) => {
                    create_genconstraints(&mut frame, &self.data.gen_constraints, genconstraints_value);
                    list.push(genconstraints_value.clone());

                }
                Err(error) => println!("Error creating markets: {:?}", error),
            }

            //Create scenarios

            let r_scenarios: Vec<(String, f64)> =
                vec![(String::from("s1"), 0.5), (String::from("s2"), 0.5)];

            let j_scenarios =
                julia_interface::to_ordered_dict(frame.as_extended_target(), &r_scenarios)
                    .unwrap();

            //Create reserve type (empty)

            let r_reserve_type: Vec<(String, f64)> = Vec::new();

            let j_reserve_type =
                julia_interface::to_ordered_dict(frame.as_extended_target(), &r_reserve_type)
                    .unwrap();

            //Create risk

            let r_risk: Vec<(String, f64)> =
                vec![(String::from("alfa"), 0.1), (String::from("beta"), 0.0)];

            let j_risk =
                julia_interface::to_ordered_dict(frame.as_extended_target(), &r_risk).unwrap();

            //Create inflow blocks

            let args1 = [];
            let inflowblocks =
                julia_interface::call(&mut frame, &["Predicer", "create_inflowblocks"], &args1)
                    .into_jlrs_result();

            match inflowblocks {
                Ok(inflowblocks_value) => {
                    //list[8]
                    list.push(inflowblocks_value);
                }
                Err(error) => println!("Error adding inflowblocks: {:?}", error),
            }

            //Create boolean parameters for input data

            let j_contains_reserves = Value::new(&mut frame, self.data.contains_reserves);
            let j_contains_online = Value::new(&mut frame, self.data.contains_online);
            let j_contains_state = Value::new(&mut frame, self.data.contains_state);
            let j_contains_piecewise_eff = Value::new(&mut frame, self.data.contains_piecewise_eff);
            let j_contains_risk = Value::new(&mut frame, self.data.contains_risk);
            let j_contains_delay = Value::new(&mut frame, self.data.contains_delay);
            let j_contains_diffusion = Value::new(&mut frame, self.data.contains_diffusion);

            //Parameters for creating input data

            let i_args = [
                list[0], //processes
                list[1], //nodes
                list[2], //node diffusions
                list[3], //node delay
                list[4], //node histories
                list[5], //markets
                list[6], //groups
                list[7], //gengonstraints
                j_scenarios, //scenarios
                j_reserve_type, //reserve_type
                j_risk, //risk
                list[8], //inflowblocks
                j_contains_reserves, //contains reserves
                j_contains_online, //contains online
                j_contains_state, //contains state
                j_contains_piecewise_eff, //contains piecewise eff
                j_contains_risk, //contains risk
                j_contains_delay, //contains delay
                j_contains_diffusion, //contains diffusion
            ];

            //Create input data
            
            let _input_data = julia_interface::call(&mut frame, &["Predicer", "create_inputdata"], &i_args).into_jlrs_result();


            //Solve model


            // Define a HashMap to store device IDs and their corresponding control values
            let mut all_device_controls: HashMap<String, ControlValues> = HashMap::new();


            match _input_data {
                Ok(id_value) => {
                    let _generate_model_result = julia_interface::call(
                        &mut frame, 
                        &["Predicer", "solve_hertta"], 
                        &[id_value]
                    );

                    let ts_column = "t";
                    let ts_column_name = JuliaString::new(&mut frame, ts_column).as_value();
                    let scenario = "s1";

                    //Predicer data columns for fetching process control values 
                    // Define a HashMap to store process names and their corresponding data column names
                    let mut predicer_data_columns: HashMap<String, String> = HashMap::new();

                    // Iterate over processes and populate the HashMap
                    for (process_name, process) in &self.data.processes {
                        if let Some(data_column) = generate_data_column(process, scenario) {
                            predicer_data_columns.insert(process_name.clone(), data_column);
                        } else {
                            println!("Process '{}' does not have matching topology", process.name);
                        }
                    }        

                    match _generate_model_result {
                        Ok(df) => {

                            for (process_name, data_column_name) in predicer_data_columns.iter() {
                                let ts_vector_function = julia_interface::call(
                                    &mut frame, 
                                    &["Predicer", "extract_column_as_vector"], 
                                    &[df, ts_column_name]
                                );
                    
                                let ts_vector = match ts_vector_function {
                                    Ok(df) => julia_interface::make_rust_vector_string(&mut frame, &df),
                                    Err(error) => {
                                        println!("Error extracting time series: {:?}", error);
                                        continue; // Skip to the next iteration
                                    },
                                };

                                let j_data_column_name = JuliaString::new(&mut frame, data_column_name).as_value();
                    
                                let df_vector_function = julia_interface::call(
                                    &mut frame, 
                                    &["Predicer", "extract_column_as_vector"], 
                                    &[df, j_data_column_name]
                                );
                    
                                let data_vector = match df_vector_function {
                                    Ok(df) => julia_interface::make_rust_vector_f64(&mut frame, &df),
                                    Err(error) => {
                                        println!("Error extracting data: {:?}", error);
                                        continue; // Skip to the next iteration
                                    },
                                };

                                match utilities::combine_vectors(ts_vector, data_vector) {
                                    Ok(control_values_for_device) => {
                                        // Insert the control data for the device
                                        all_device_controls.insert(process_name.to_string(), control_values_for_device);
                                        
                                    },
                                    Err(e) => {
                                        // Handle the error
                                        println!("Error combining vectors: {}", e);
                                    }
                                }
                            
                            }
                               
                        }

                        Err(error) => println!("Error solving model: {:?}", error),   

                    }   
                }

                Err(error) => println!("Error solving model: {:?}", error),  

            }

            for (device_id, control_values) in &all_device_controls {
                println!("Device ID: {}", device_id);
                for (timestamp, value) in control_values {
                    println!("    Timestamp: {}, Value: {}", timestamp, value);
                }
            }            

            Ok(all_device_controls)
        }
    }
}

fn generate_data_column(process: &input_data::Process, scenario: &str) -> Option<String> {
    for topology in &process.topos {
        let data_column = format!("{}_{}_{}_{}", process.name, topology.source, process.name, scenario);
        return Some(data_column);
    }
    None
}
    
pub fn add_topology<'target, 'data>(frame: &mut frame::GcFrame<'target>, process: Value<'_, '_>, topos: &Vec<input_data::Topology>) {

    frame.scope(|mut frame| {

        for topo in topos {
            let t_source = JuliaString::new(&mut frame, &topo.source).as_value();
            let t_sink = JuliaString::new(&mut frame, &topo.sink).as_value();
            let t_capacity = Value::new(&mut frame, topo.capacity);
            let t_vom_cost = Value::new(&mut frame, topo.vom_cost);
            let t_ramp_up = Value::new(&mut frame, topo.ramp_up);
            let t_ramp_down = Value::new(&mut frame, topo.ramp_down);

            let _create_topology = julia_interface::call(
                &mut frame,
                &["Predicer", "create_topology"],
                &[t_source, t_sink, t_capacity, t_vom_cost, t_ramp_up, t_ramp_down],
            );

            match _create_topology {
                Ok(topology) => {
                    let _add_topology = julia_interface::call(
                        &mut frame, 
                        &["Predicer", "add_topology"], 
                        &[process, topology]
                    );
                }
                Err(error) => println!("Error adding topology to process: {:?}", error),
            }

        }

        Ok(())

    }).unwrap();

}

pub fn add_state<'target, 'data>(frame: &mut frame::GcFrame<'target>, node: Value<'_, '_>, value: &input_data::Node) {

    frame.scope(|mut frame| {

        //create state

        let s_in_max = Value::new(&mut frame, value.state.in_max);
        let s_out_max = Value::new(&mut frame, value.state.out_max);
        let s_state_loss_proportional = Value::new(&mut frame, value.state.state_loss_proportional);
        let s_state_max = Value::new(&mut frame, value.state.state_max);
        let s_state_min = Value::new(&mut frame, value.state.state_min);
        let s_initial_state = Value::new(&mut frame, value.state.initial_state);
        let s_is_temp = Value::new(&mut frame, value.state.is_temp);
        let s_t_e_conversion = Value::new(&mut frame, value.state.t_e_conversion);
        let s_residual_value = Value::new(&mut frame, value.state.residual_value);


        let _create_state = julia_interface::call(
            &mut frame,
            &["Predicer", "create_state"],
            &[s_in_max, s_out_max, s_state_loss_proportional, s_state_max, s_state_min, s_initial_state, s_is_temp, s_t_e_conversion, s_residual_value],
        );

        //add state to node

        match _create_state {
            Ok(state) => {
                let _add_state = julia_interface::call(
                    &mut frame,
                    &["Predicer", "add_state"],
                    &[node, state],
                );
            }
            Err(error) => println!("Error creating state: {:?}", error),
        }

        Ok(())

    }).unwrap();

}

pub fn create_groups<'target, 'data>(frame: &mut frame::GcFrame<'target>, groups: &HashMap<String, input_data::Group>, j_groups: Value<'_, '_>) {

    frame.scope(|mut frame| {

        //Creating groups

        for (key, value) in groups {
            // An extended target provides a target for the result we want to return and a frame for
            // temporary data.

            let d1 = JuliaString::new(&mut frame, key).as_value();
            let d2 = JuliaString::new(&mut frame, &value.g_type).as_value();
            let d3 = JuliaString::new(&mut frame, &value.entity).as_value();

            let g_args = [d1, d2, d3];

            let _group =
                julia_interface::call(&mut frame, &["Predicer", "create_group"], &g_args)
                    .into_jlrs_result();

            match _group {
                Ok(group_value) => {
                    let _add_to_groups_result = julia_interface::call(
                        &mut frame,
                        &["Predicer", "add_to_groups"],
                        &[group_value, j_groups],
                    );
                }
                Err(error) => println!("Error adding group to groups: {:?}", error),
            }
        }

        Ok(())

    }).unwrap();

}

pub fn add_timeseries<'target, 'data>(frame: &mut frame::GcFrame<'target>, julia_value: Value<'_, '_>, ts_data: &Vec<input_data::TimeSeries>, function: &str) {

    frame.scope(|mut frame| {

        let _create_timeseriesdata = julia_interface::call(
            &mut frame,
            &["Predicer", "create_timeseriesdata"],
            &[],
        );

        match _create_timeseriesdata {
            Ok(timeseriesdata) => {

                for _time_serie in ts_data {

                    let ts_scenario = JuliaString::new(&mut frame, &_time_serie.scenario).as_value();

                    let _create_timeseries = julia_interface::call(
                        &mut frame,
                        &["Predicer", "create_timeseries"],
                        &[ts_scenario],
                    );

                    match _create_timeseries {
                        Ok(timeserie) => {

                            for time_point in &_time_serie.series {

                                let j_timestamp = JuliaString::new(&mut frame, &time_point.0).as_value();
                                let j_value = Value::new(&mut frame, time_point.1);

                                let _make_time_point = julia_interface::call(
                                    &mut frame,
                                    &["Predicer", "make_time_point"],
                                    &[j_timestamp, j_value],
                                );

                                match _make_time_point {
                                    Ok(time_point) => {
                                        let _push_time_point = julia_interface::call(
                                            &mut frame,
                                            &["Predicer", "push_time_point"],
                                            &[timeserie, time_point],
                                        );
                                    }
                                    Err(error) => println!("Error creating time point: {:?}", error),
                                } 
                                
                            }

                            let _push_timeseries = julia_interface::call(
                                &mut frame,
                                &["Predicer", "push_timeseries"],
                                &[timeseriesdata, timeserie],
                            );
                            
                        }
                        Err(error) => println!("Error creating timeseries: {:?}", error),
                    }       
                }

                //use timeseries in function

                let _function = julia_interface::call(
                    &mut frame,
                    &["Predicer", function],
                    &[julia_value, timeseriesdata],
                );

            }
            Err(error) => println!("Error creating timeseriesdata: {:?}", error),
        }

        Ok(())

    }).unwrap();

}

pub fn create_node_diffusion<'target, 'data>(frame: &mut frame::GcFrame<'target>, node_diffusion: HashMap<String, input_data::NodeDiffusion>, j_node_diffusion_tuples: Value<'_, '_>) {

    frame.scope(|mut frame| {

        //Node diffusion

        for (_key, value) in &node_diffusion {
            // An extended target provides a target for the result we want to return and a frame for
            // temporary data.

            let nd_d1 = JuliaString::new(&mut frame, &value.node1).as_value();
            let nd_d2 = JuliaString::new(&mut frame, &value.node2).as_value();
            let nd_d3 = Value::new(&mut frame, value.diff_coeff);

            let _node_diffusion_tuple = julia_interface::call(
                &mut frame,
                &["Predicer", "create_node_diffusion_tuple"],
                &[nd_d1, nd_d2, nd_d3, j_node_diffusion_tuples],
            )
            .into_jlrs_result();
        }

        Ok(())

    }).unwrap();

}

pub fn create_node_delay<'target, 'data>(frame: &mut frame::GcFrame<'target>, node_delay: HashMap<String, input_data::NodeDelay>, j_node_delay_tuples: Value<'_, '_>) {

    frame.scope(|mut frame| {

        //Node delay

        for (_key, value) in &node_delay {
            // An extended target provides a target for the result we want to return and a frame for
            // temporary data.
        
            let nde_d1 = JuliaString::new(&mut frame, &value.node1).as_value();
            let nde_d2 = JuliaString::new(&mut frame, &value.node2).as_value();
            let nde_d3 = Value::new(&mut frame, value.delay);
            let nde_d4 = Value::new(&mut frame, value.min_flow);
            let nde_d5 = Value::new(&mut frame, value.max_flow);
        
            let _node_delay_tuple = julia_interface::call(
                &mut frame,
                &["Predicer", "create_node_delay_tuple"],
                &[nde_d1, nde_d2, nde_d3, nde_d4, nde_d5, j_node_delay_tuples],
            )
            .into_jlrs_result();
        }

        Ok(())

    }).unwrap();

}

pub fn create_confactors<'target, 'data>(frame: &mut frame::GcFrame<'target>, gc_value: Value<'_, '_>, factors: &Vec<input_data::ConFactor>) {

    frame.scope(|mut frame| {

        //Confactors

        for confactor in factors {

            let s1 = JuliaString::new(&mut frame, &confactor.flow.0).as_value();
            let s2 = JuliaString::new(&mut frame, &confactor.flow.1).as_value();
        
            let g_args = [s1, s2];
        
            //create vartuple
        
            let _create_vartuple =
            julia_interface::call(&mut frame, &["Predicer", "create_vartuple"], &g_args)
                .into_jlrs_result();
        
            match _create_vartuple {
                Ok(vartuple) => {
        
                    let vartype = JuliaString::new(&mut frame, &confactor.var_type).as_value();
        
                    //create confactor
        
                    let _create_confactor = julia_interface::call(
                        &mut frame,
                        &["Predicer", "create_confactor"],
                        &[vartype, vartuple],
                    );
                    match _create_confactor {
                        Ok(confactor_value) => {
        
                            //add confactor timeseries
        
                            let function = "add_ts_to_confactor";
                            add_timeseries(&mut frame, confactor_value, &confactor.data.ts_data, function);        
        
                            //add confactor to gen constraints
        
                            let _add_confactor_to_gc = julia_interface::call(
                                &mut frame,
                                &["Predicer", "add_confactor_to_gc"],
                                &[confactor_value,  gc_value],
                            );
                        }
                        Err(error) => println!("Error creating confactor: {:?}", error),
                    }
                }
                Err(error) => println!("Error creating vartuple: {:?}", error),
            }
        
        }

        Ok(())

    }).unwrap();

}

pub fn create_processes<'target, 'data>(frame: &mut frame::GcFrame<'target>, processes: &HashMap<String, input_data::Process>, j_processes: Value<'_, '_>) {

    frame.scope(|mut frame| {


        for (key, value) in processes {
            let p_name = JuliaString::new(&mut frame, key).as_value();
            let p_conversion = Value::new(&mut frame, value.conversion);
            let p_group = JuliaString::new(&mut frame, &value.group).as_value();
            let p_delay = Value::new(&mut frame, value.delay);


            let process_result = julia_interface::call(
                &mut frame,
                &["Predicer", "create_process"],
                &[p_name, p_conversion, p_delay],
            )
            .into_jlrs_result();

            match process_result {
                Ok(process) => {

                    add_topology(&mut frame, process, &value.topos); 

                    let _add_group_to_processes = julia_interface::call(
                        &mut frame,
                        &["Predicer", "add_group_to_process"],
                        &[process, p_group],
                    );

                    let _add_to_processes = julia_interface::call(
                        &mut frame, 
                        &["Predicer", "add_to_processes"], 
                        &[process, j_processes]
                    ).into_jlrs_result();

                }
                Err(error) => println!("Error creating process: {:?}", error),
            }
        }

        Ok(())

    }).unwrap();

}

pub fn create_markets<'target, 'data>(frame: &mut frame::GcFrame<'target>, markets: &HashMap<String, input_data::Market>, j_markets: Value<'_, '_>) {

    frame.scope(|mut frame| {

        //Markets

        for (key, value) in markets {
            // An extended target provides a target for the result we want to return and a frame for
            // temporary data.

            let d1 = JuliaString::new(&mut frame, key).as_value();
            let d2 = JuliaString::new(&mut frame, &value.m_type).as_value();
            let d3 = JuliaString::new(&mut frame, &value.node).as_value();
            let d4 = JuliaString::new(&mut frame, &value.pgroup).as_value();
            let d5 = JuliaString::new(&mut frame, &value.direction).as_value();
            let d6 = JuliaString::new(&mut frame, &value.reserve_type).as_value();
            let d7 = Value::new(&mut frame, value.is_bid);
            let d8 = Value::new(&mut frame, value.is_limited);
            let d9 = Value::new(&mut frame, value.min_bid);
            let d10 = Value::new(&mut frame, value.max_bid);
            let d11 = Value::new(&mut frame, value.fee);

            let m_args = [d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11];

            let _market =
                julia_interface::call(&mut frame, &["Predicer", "create_market"], &m_args)
                    .into_jlrs_result();

            match _market {
                Ok(market) => {

                    //ADD TIMESERIES TO MARKET

                    let function = "add_market_prices";
                    add_timeseries(&mut frame, market, &value.price.ts_data, function);

                    //Create market up prices

                    let function = "add_market_up_prices";
                    add_timeseries(&mut frame, market, &value.up_price.ts_data, function);

                    //Create market down prices

                    let function = "add_market_down_prices";
                    add_timeseries(&mut frame, market, &value.down_price.ts_data, function);

                    let _add_to_markets_result = julia_interface::call(
                        &mut frame,
                        &["Predicer", "add_to_markets"],
                        &[market, j_markets],
                    );
                }
                Err(error) => println!("Error adding market to markets2: {:?}", error),
            }
        }

        Ok(())

    }).unwrap();

}

pub fn create_nodes<'target, 'data>(frame: &mut frame::GcFrame<'target>, nodes: &HashMap<String, input_data::Node>, j_nodes: Value<'_, '_>) {

    frame.scope(|mut frame| {

        //Nodes

        for (key, value) in nodes {

            let n_name = JuliaString::new(&mut frame, key).as_value();
            let n_is_commodity = Value::new(&mut frame, value.is_commodity);
            let n_is_market = Value::new(&mut frame, value.is_market);
            let n_is_inflow = Value::new(&mut frame, value.is_inflow);
            let n_is_state = Value::new(&mut frame, value.is_state);

            let _create_node = julia_interface::call(
                &mut frame,
                &["Predicer", "create_node"],
                &[n_name, n_is_commodity, n_is_market, n_is_inflow, n_is_state],
            )
            .into_jlrs_result();

            match _create_node {
                Ok(node) => {

                    //create state

                    if value.is_state {

                        add_state(&mut frame, node, value);

                    }

                    //add inflow

                    if value.is_inflow {

                        let function = "add_inflow_to_node";
                        add_timeseries(&mut frame, node, &value.inflow.ts_data, function)

                    }

                    let _add_to_nodes_result = julia_interface::call(
                        &mut frame,
                        &["Predicer", "add_to_nodes"],
                        &[node, j_nodes],
                    );
                }
                Err(error) => println!("Error creating node: {:?}", error),
            }
        }

        Ok(())

    }).unwrap();

}

pub fn create_genconstraints<'target, 'data>(frame: &mut frame::GcFrame<'target>, gen_constraints: &HashMap<String, input_data::GenConstraint>, j_genconstraints: Value<'_, '_>) {

    frame.scope(|mut frame| {

        //Gen constraints

        for (key, value) in gen_constraints {
            // An extended target provides a target for the result we want to return and a frame for
            // temporary data.

            let d1 = JuliaString::new(&mut frame, key).as_value();
            let d2 = JuliaString::new(&mut frame, &value.gc_type).as_value();
            let d3 = Value::new(&mut frame, value.is_setpoint);
            let d4 = Value::new(&mut frame, value.penalty);

            let gc_args = [d1, d2, d3, d4];

            let _genconstraint = julia_interface::call(
                &mut frame,
                &["Predicer", "create_genconstraint"],
                &gc_args,
            )
            .into_jlrs_result();

            match _genconstraint {
                Ok(gc_value) => {

                    create_confactors(&mut frame, gc_value, &value.factors);


                    let _add_to_genconstraints_result = julia_interface::call(
                        &mut frame,
                        &["Predicer", "add_to_genconstraints"],
                        &[gc_value, j_genconstraints],
                    );
                }
                Err(error) => println!(
                    "Error adding gen constraint to gen_constraints: {:?}",
                    error
                ),
            }
        }

        Ok(())

    }).unwrap();

}    


