use crate::input_data;
use serde::{Serialize, Deserialize};
use crate::input_data::InputData;
use std::collections::HashMap;
use std::error::Error;
use chrono::{NaiveTime, NaiveDate};
use rand::Rng;

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

pub fn create_test_timeseries() -> Vec<String> {
    let hours = 0..24;
    let mut timeseries = Vec::new();

    for hour in hours {
        let time = format!("2022-04-20T{:02}:00:00", hour);
        timeseries.push(time);
    }

    timeseries
}

pub fn create_test_risk_data() -> HashMap<String, f64> {
    // Creating a new HashMap to store our test data
    let mut risk_data = HashMap::new();

    // Inserting test data into the HashMap
    // Each entry maps a String key to a f64 value representing the risk
    risk_data.insert("alfa".to_string(), 0.1);
    risk_data.insert("beta".to_string(), 0.2);

    // Returning the populated HashMap
    risk_data
}

pub fn create_test_scenarios() -> HashMap<String, f64> {
    let mut scenarios = HashMap::new();

    // Example scenarios with their probabilities
    scenarios.insert("s1".to_string(), 0.3);
    scenarios.insert("s2".to_string(), 0.5);
    scenarios.insert("s3".to_string(), 0.2);

    scenarios
}

// Function to create test NodeDiffusion data
pub fn create_test_node_diffusion() -> HashMap<String, input_data::NodeDiffusion> {
    let mut data = HashMap::new();

    // Insert test data into the HashMap
    data.insert("diffusion1".to_string(), input_data::NodeDiffusion {
        name: "Diffusion 1".to_string(),
        node1: "Node A".to_string(),
        node2: "Node B".to_string(),
        diff_coeff: 0.1,
    });
    
    data.insert("diffusion2".to_string(), input_data::NodeDiffusion {
        name: "Diffusion 2".to_string(),
        node1: "Node C".to_string(),
        node2: "Node D".to_string(),
        diff_coeff: 0.2,
    });

    // Add more NodeDiffusion instances as needed

    data
}

// Example function that creates a TimeSeriesData with example timeseries data
pub fn create_example_timeseries_data() -> input_data::TimeSeriesData {
    let scenarios = vec!["s1", "s2", "s3"];

    let mut ts_data = Vec::new();

    for scenario in scenarios {
        let mut series = Vec::new();

        // Create example time points and associated data
        for hour in 0..24 {
            let time = format!("2022-04-20T{:02}:00:00", hour);
            let value = (hour as f64) * 1.5; // Just an example value
            series.push((time, value));
        }

        ts_data.push(input_data::TimeSeries {
            scenario: scenario.to_string(),
            series,
        });
    }

    input_data::TimeSeriesData { ts_data }
}

// Function to create test NodeDelay data
pub fn create_test_node_delay_data() -> HashMap<String, input_data::NodeDelay> {
    let mut node_delays = HashMap::new();

    // Create a few NodeDelay instances
    node_delays.insert("delay1".to_string(), input_data::NodeDelay {
        name: "Delay between A and B".to_string(),
        node1: "Node A".to_string(),
        node2: "Node B".to_string(),
        delay: 1.5,
        min_flow: 0.0,
        max_flow: 10.0,
    });

    node_delays.insert("delay2".to_string(), input_data::NodeDelay {
        name: "Delay between C and D".to_string(),
        node1: "Node C".to_string(),
        node2: "Node D".to_string(),
        delay: 2.0,
        min_flow: 1.0,
        max_flow: 15.0,
    });

    // You can add more test data here if needed

    node_delays
}

// Function to create example TimeSeries
pub fn create_example_time_series(scenario: &str, data: Vec<(&str, f64)>) -> input_data::TimeSeries {
    input_data::TimeSeries {
        scenario: scenario.to_string(),
        series: data.into_iter().map(|(t, value)| (t.to_string(), value)).collect(),
    }
}

// Function to create TimeSeriesData
pub fn create_example_time_series_data(scenarios: Vec<input_data::TimeSeries>) -> input_data::TimeSeriesData {
    input_data::TimeSeriesData {
        ts_data: scenarios,
    }
}

// Function to create NodeHistory
pub fn create_example_node_history(node: &str, steps: input_data::TimeSeriesData) -> input_data::NodeHistory {
    input_data::NodeHistory {
        node: node.to_string(),
        steps,
    }
}

// Function to create the HashMap with NodeHistory data
pub fn create_example_node_histories() -> HashMap<String, input_data::NodeHistory> {
    let mut node_histories = HashMap::new();

    // Assuming we are only creating a single NodeHistory for this example
    let time_series = create_example_time_series("t", vec![
        ("20.4.2022 0:00", 3.0),
        ("20.4.2022 1:00", 4.0),
    ]);
    
    let time_series_data = create_example_time_series_data(vec![time_series]);

    let node_history = create_example_node_history("dh2", time_series_data);

    node_histories.insert("dh2".to_string(), node_history);

    node_histories
}

pub fn create_example_realisation() -> HashMap<String, f64> {
    let mut realisation = HashMap::new();

    // Inserting values into the hashmap
    // Please note: in Rust, decimal points are denoted by a dot, not a comma
    realisation.insert("reserve_product_s1".to_string(), 0.2);
    realisation.insert("reserve_product_s2".to_string(), 0.3);

    realisation
}

// Function to create a test HashMap for MarketNew
pub fn create_test_markets_hashmap() -> HashMap<String, input_data::MarketNew> {
    let mut markets: HashMap<String, input_data::MarketNew> = HashMap::new();

    let timeseries = create_timeseries();
    let realisation = create_example_realisation();

    let data: Vec<(String, f64)> = vec![
        ("Item1".to_string(), 10.5),
        ("Item2".to_string(), 20.75),
        ("Item3".to_string(), 30.0),
        ("Item4".to_string(), 40.25),
    ];

    // Example markets
    let market1 = input_data::MarketNew {
        name: "Market1".to_string(),
        m_type: "energy".to_string(),
        node: "Node1".to_string(),
        pgroup: "GroupA".to_string(),
        direction: "Supply".to_string(),
        realisation: realisation.clone(),
        reserve_type: "Primary".to_string(),
        is_bid: true,
        is_limited: false,
        min_bid: 10.0,
        max_bid: 100.0,
        fee: 1.0,
        price: timeseries.clone(),
        up_price: timeseries.clone(),
        down_price: timeseries.clone(),
        fixed: data.clone(),
    };

    let market2 = input_data::MarketNew {
        name: "Market2".to_string(),
        m_type: "Tertiary".to_string(),
        node: "Node2".to_string(),
        pgroup: "GroupB".to_string(),
        direction: "Demand".to_string(),
        realisation: realisation.clone(),
        reserve_type: "Secondary".to_string(),
        is_bid: false,
        is_limited: true,
        min_bid: 20.0,
        max_bid: 200.0,
        fee: 2.0,
        price: timeseries.clone(),
        up_price: timeseries.clone(),
        down_price: timeseries.clone(),
        fixed: data.clone(),
    };

    // Insert markets into the hashmap
    markets.insert(market1.name.clone(), market1);
    markets.insert(market2.name.clone(), market2);

    markets
}

pub fn create_test_reserve_type() -> HashMap<String, f64> {
    let mut reserve_type = HashMap::new();
    reserve_type.insert("fast".to_string(), 2.0);
    reserve_type
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

pub fn create_test_confactor() -> input_data::ConFactor {
    input_data::ConFactor {
        var_type: "ExampleType".to_string(),
        flow: ("Node1".to_string(), "".to_string()),
        data: create_timeseries(),
    }
}

pub fn create_test_genconstraints() -> HashMap<String, input_data::GenConstraint> {
    let mut gen_constraints_map = HashMap::new();
    
    let test_genconstraint = input_data::GenConstraint {
        name: "ExampleConstraintName".to_string(),
        gc_type: "ExampleConstraintType".to_string(),
        is_setpoint: false,
        penalty: 50.0,
        factors: vec![create_test_confactor()],
        constant: create_timeseries(),
    };
    
    // Insert the GenConstraint into the HashMap. The name of the GenConstraint is used as the key.
    gen_constraints_map.insert(test_genconstraint.name.clone(), test_genconstraint);
    
    gen_constraints_map
}

pub fn create_test_nodes_hashmap() -> HashMap<String, input_data::NodeNew> {
    let mut nodes: HashMap<String, input_data::NodeNew> = HashMap::new();

    let node1_state = create_statenew();
    let node2_state = create_statenew();
    let node3_state = create_statenew();
    let node1_timeseries = create_timeseries();
    let node2_timeseries = create_timeseries();
    let node3_timeseries = create_timeseries();

    // Example nodes
    let node1 = input_data::NodeNew {
        name: "node1".to_string(),
        is_commodity: true,
        is_state: false,
        is_res: false,
        is_market: false,
        is_inflow: true,
        cost: node1_timeseries.clone(),
        inflow: node1_timeseries.clone(),
        state: node1_state.clone(),
    };

    let node2 = input_data::NodeNew {
        name: "node2".to_string(),
        is_commodity: false,
        is_state: true,
        is_res: false,
        is_market: false,
        is_inflow: false,
        cost: node2_timeseries.clone(),
        inflow: node2_timeseries.clone(),
        state: node2_state.clone(),
    };

    let node3 = input_data::NodeNew {
        name: "node3".to_string(),
        is_commodity: false,
        is_state: true,
        is_res: false,
        is_market: false,
        is_inflow: false,
        cost: node3_timeseries.clone(),
        inflow: node3_timeseries.clone(),
        state: node2_state.clone(),
    };

    // Insert nodes into the hashmap
    nodes.insert(node1.name.clone(), node1);
    nodes.insert(node2.name.clone(), node2);
    nodes.insert(node3.name.clone(), node3);

    nodes
}

pub fn create_timeseries() -> input_data::TimeSeriesData {
    let scenarios = vec!["s1", "s2", "s3"];
    let base_date = NaiveDate::parse_from_str("20.4.2022", "%d.%m.%Y").unwrap();
    let mut ts_data = Vec::new();

    for scenario in scenarios {
        let mut series = Vec::new();

        for hour in 0..24 {
            let time = NaiveTime::from_hms(hour, 0, 0);
            let datetime = base_date.and_time(time);
            let timestamp = datetime.format("%d.%m.%Y %H:%M").to_string();
            let value = match hour {
                6 => -14.0,
                7 => -20.0,
                8 => -13.0,
                9 => -12.0,
                _ => -5.0, // Default value, you can insert your logic here
            };
            series.push((timestamp, value));
        }

        ts_data.push(input_data::TimeSeries {
            scenario: scenario.to_string(),
            series,
        });
    }

    input_data::TimeSeriesData { ts_data }
}

// Function to create a test HashMap for GroupNew
pub fn create_test_groups_hashmap() -> HashMap<String, input_data::GroupNew> {
    let mut groups: HashMap<String, input_data::GroupNew> = HashMap::new();

    // Example groups
    let group1 = input_data::GroupNew {
        name: "Group1".to_string(),
        g_type: "Type1".to_string(),
        entity: "Entity1".to_string(),
        group: "GroupA".to_string(),
    };

    let group2 = input_data::GroupNew {
        name: "Group2".to_string(),
        g_type: "Type2".to_string(),
        entity: "Entity2".to_string(),
        group: "GroupB".to_string(),
    };

    let group3 = input_data::GroupNew {
        name: "Group3".to_string(),
        g_type: "Type3".to_string(),
        entity: "Entity3".to_string(),
        group: "GroupC".to_string(),
    };

    // Insert groups into the hashmap
    groups.insert(group1.name.clone(), group1);
    groups.insert(group2.name.clone(), group2);
    groups.insert(group3.name.clone(), group3);

    groups
}

pub fn create_test_topologies_for_process(process_name: &str) -> Vec<input_data::TopologyNew> {
    let mut topologies: Vec<input_data::TopologyNew> = Vec::new();
    
    let time_series_data = create_timeseries(); // Placeholder function

    // Create a topology where the given process is the sink
    topologies.push(input_data::TopologyNew {
        source: "SomeOtherSource".to_string(),
        sink: process_name.to_string(),
        capacity: 20.0,
        vom_cost: 3.0,
        ramp_up: 0.5,
        ramp_down: 0.5,
        initial_load: 0.6,
        initial_flow: 0.6,
        cap_ts: time_series_data.clone(),
    });

    // Create a topology where the given process is the source
    topologies.push(input_data::TopologyNew {
        source: process_name.to_string(),
        sink: "SomeOtherSink".to_string(),
        capacity: 20.0,
        vom_cost: 3.0,
        ramp_up: 0.5,
        ramp_down: 0.5,
        initial_load: 0.6,
        initial_flow: 0.6,
        cap_ts: time_series_data,
    });

    topologies
}

pub fn create_test_eff_ops(num_ops: usize, min_val: f64, max_val: f64) -> Vec<String> {
    let mut rng = rand::thread_rng();
    (0..num_ops).map(|_| {
        // Generate a random f64 value within the specified range
        let val = rng.gen_range(min_val..max_val);
        // Convert the f64 value to a String
        format!("{:.2}", val)
    }).collect()
}

pub fn create_test_processes_hashmap() -> HashMap<String, input_data::ProcessNew> {
    let mut processes = HashMap::new();

    // Assuming create_test_eff_ops and create_timeseries are available and return the appropriate types
    let p1_eff_ops = create_test_eff_ops(10, 0.5, 1.0);
    let p2_eff_ops = create_test_eff_ops(10, 0.5, 1.0);
    
    let p1_cf_ts = create_timeseries(); // Assuming this creates TimeSeriesData
    let p1_eff_ts = create_timeseries(); // Assuming this creates TimeSeriesData

    let p2_cf_ts = create_timeseries(); // Assuming this creates TimeSeriesData
    let p2_eff_ts = create_timeseries(); // Assuming this creates TimeSeriesData

    let p1_topo = create_test_topologies_for_process("Process1");
    let p2_topo = create_test_topologies_for_process("Process2");

    // Create the ProcessNew instances
    let process1 = input_data::ProcessNew {
        name: "Process1".to_string(),
        groups: Vec::new(), // Assuming an empty Vec for groups
        conversion: 100,
        is_cf: true,
        is_cf_fix: false,
        is_online: true,
        is_res: false,
        eff: 0.9,
        load_min: 10.0,
        load_max: 100.0,
        start_cost: 500.0,
        min_online: 1.0,
        min_offline: 1.0,
        max_online: 24.0,
        max_offline: 24.0,
        initial_state: 0.0,
        is_scenario_independent: false, // Assuming a boolean value for this field
        topos: p1_topo.clone(), // Assuming an empty Vec for topos
        cf: p1_cf_ts,
        eff_ts: p1_eff_ts,
        eff_ops: p1_eff_ops.clone(),
        eff_fun: Vec::new(), // Assuming an empty Vec for eff_fun
    };

    let process2 = input_data::ProcessNew {
        name: "Process2".to_string(),
        groups: Vec::new(), // Assuming an empty Vec for groups
        conversion: 200,
        is_cf: false,
        is_cf_fix: true,
        is_online: false,
        is_res: true,
        eff: 0.8,
        load_min: 20.0,
        load_max: 200.0,
        start_cost: 1000.0,
        min_online: 2.0,
        min_offline: 2.0,
        max_online: 48.0,
        max_offline: 48.0,
        initial_state: 0.0,
        is_scenario_independent: false, // Assuming a boolean value for this field
        topos: p2_topo.clone(), // Assuming an empty Vec for topos
        cf: p2_cf_ts,
        eff_ts: p2_eff_ts,
        eff_ops: p2_eff_ops.clone(),
        eff_fun: Vec::new(), // Assuming an empty Vec for eff_fun
    };

    // Insert processes into the hashmap
    processes.insert(process1.name.clone(), process1);
    processes.insert(process2.name.clone(), process2);

    processes
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