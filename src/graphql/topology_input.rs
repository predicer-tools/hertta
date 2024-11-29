use super::{ValidationError, ValidationErrors};
use crate::input_data_base::{BaseNode, BaseProcess, BaseTopology};
use juniper::{GraphQLInputObject, GraphQLObject, GraphQLUnion};

#[derive(GraphQLInputObject)]
pub struct AddTopologyInput {
    #[graphql(description = "Source node name. Null if source is the process itself.")]
    pub source_node: Option<String>,
    #[graphql(description = "Sink node name. Null if sink is the process itself.")]
    pub sink_node: Option<String>,
    pub capacity: f64,
    pub vom_cost: f64,
    pub ramp_up: f64,
    pub ramp_down: f64,
    pub initial_load: f64,
    pub initial_flow: f64,
    pub cap_ts: f64,
}

impl AddTopologyInput {
    fn to_topology(self, topology_name: &str) -> BaseTopology {
        let source = self
            .source_node
            .unwrap_or_else(|| String::from(topology_name));
        let sink = self
            .sink_node
            .unwrap_or_else(|| String::from(topology_name));
        BaseTopology {
            source: source,
            sink: sink,
            capacity: self.capacity,
            vom_cost: self.vom_cost,
            ramp_up: self.ramp_up,
            ramp_down: self.ramp_down,
            initial_load: self.initial_load,
            initial_flow: self.initial_flow,
            cap_ts: self.cap_ts,
        }
    }
}

#[derive(GraphQLObject)]
pub struct TopologyId {
    process_name: String,
    topology_index: i32,
}

impl TopologyId {
    fn new(process_name: &str, topology_index: i32) -> Self {
        TopologyId {
            process_name: String::from(process_name),
            topology_index,
        }
    }
}

#[derive(GraphQLUnion)]
pub enum AddTopologyResult {
    Ok(TopologyId),
    Err(ValidationErrors),
}

pub fn add_topology_to_process(
    process_name: &str,
    topology: AddTopologyInput,
    processes: &mut Vec<BaseProcess>,
    nodes: &mut Vec<BaseNode>,
) -> AddTopologyResult {
    let errors = validate_topology_to_add(&topology, nodes);
    if !errors.is_empty() {
        return AddTopologyResult::Err(ValidationErrors::from(errors));
    }
    let process = match processes.iter_mut().find(|p| p.name == process_name) {
        Some(p) => p,
        None => {
            return AddTopologyResult::Err(ValidationErrors::from(vec![ValidationError::new(
                process_name,
                "no such process",
            )]))
        }
    };
    process.topos.push(topology.to_topology(process_name));
    AddTopologyResult::Ok(TopologyId::new(
        process_name,
        (process.topos.len() - 1) as i32,
    ))
}

fn validate_topology_to_add(
    topology: &AddTopologyInput,
    nodes: &Vec<BaseNode>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if topology.source_node.is_none() && topology.sink_node.is_none() {
        errors.push(ValidationError::new(
            "source_node",
            "source and sink nodes are both null",
        ));
    }
    if let Some(ref sink) = topology.sink_node {
        if nodes.iter().find(|n| n.name == *sink).is_none() {
            errors.push(ValidationError::new("sink_node", "no such node"));
        }
    }
    if let Some(ref source) = topology.source_node {
        if nodes.iter().find(|n| n.name == *source).is_none() {
            errors.push(ValidationError::new("source_node", "no such node"));
        }
    }
    if topology.source_node == topology.sink_node {
        errors.push(ValidationError::new(
            "source",
            "should be different from sink",
        ));
    }
    if topology.ramp_down < 0.0 || topology.ramp_down > 1.0 {
        errors.push(ValidationError::new("ramp_down", "should be in [0, 1]"))
    }
    if topology.ramp_up < 0.0 || topology.ramp_up > 1.0 {
        errors.push(ValidationError::new("ramp_up", "should be in [0, 1]"))
    }
    errors
}
