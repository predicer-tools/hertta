use super::{MaybeError, ValidationError, ValidationErrors};
use crate::input_data_base::{BaseNode, BaseProcess, BaseTopology};
use juniper::GraphQLInputObject;

#[derive(GraphQLInputObject)]
pub struct NewTopology {
    pub capacity: f64,
    pub vom_cost: f64,
    pub ramp_up: f64,
    pub ramp_down: f64,
    pub initial_load: f64,
    pub initial_flow: f64,
    pub cap_ts: Option<f64>,
}

impl NewTopology {
    fn to_topology(self, source: String, sink: String) -> BaseTopology {
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

pub fn create_topology(
    process_name: String,
    source_node_name: Option<String>,
    sink_node_name: Option<String>,
    topology: NewTopology,
    processes: &mut Vec<BaseProcess>,
    nodes: &mut Vec<BaseNode>,
) -> ValidationErrors {
    let process = match processes.iter_mut().find(|p| p.name == process_name) {
        Some(p) => p,
        None => {
            return ValidationErrors::from(vec![ValidationError::new(
                "process_name",
                "no such process",
            )]);
        }
    };
    let errors = validate_topology_creation(
        &topology,
        &source_node_name,
        &sink_node_name,
        &process,
        nodes,
    );
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    let source = source_node_name.unwrap_or_else(|| process_name.clone());
    let sink = sink_node_name.unwrap_or_else(|| process_name.clone());
    process.topos.push(topology.to_topology(source, sink));
    ValidationErrors::default()
}

fn validate_topology_creation(
    topology: &NewTopology,
    source_node: &Option<String>,
    sink_node: &Option<String>,
    process: &BaseProcess,
    nodes: &Vec<BaseNode>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if source_node.is_none() && sink_node.is_none() {
        errors.push(ValidationError::new(
            "source_node",
            "source and sink nodes are both null",
        ));
    }
    if let Some(ref sink) = sink_node {
        if nodes.iter().find(|n| n.name == *sink).is_none() {
            errors.push(ValidationError::new("sink_node", "no such node"));
        }
    }
    if let Some(ref source) = source_node {
        if nodes.iter().find(|n| n.name == *source).is_none() {
            errors.push(ValidationError::new("source_node", "no such node"));
        }
    }
    if source_node == sink_node {
        errors.push(ValidationError::new(
            "source_node",
            "should be different from sink",
        ));
    }
    let source = source_node.as_ref().unwrap_or(&process.name);
    let sink = sink_node.as_ref().unwrap_or(&process.name);
    if process
        .topos
        .iter()
        .find(|t| t.source == *source && t.sink == *sink)
        .is_some()
    {
        errors.push(ValidationError::new(
            "process_name",
            "a topolgy with the same source and sink exists",
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

pub fn delete_topology(
    process_name: &str,
    source_node_name: &Option<String>,
    sink_node_name: &Option<String>,

    processes: &mut Vec<BaseProcess>,
) -> MaybeError {
    let process = match processes.iter_mut().find(|p| p.name == process_name) {
        Some(process) => process,
        None => return "no such process".into(),
    };
    let source_name = source_node_name
        .as_ref()
        .map(|n| n.as_str())
        .unwrap_or(process_name);
    let sink_name = sink_node_name
        .as_ref()
        .map(|n| n.as_str())
        .unwrap_or(process_name);
    if let Some(position) = process
        .topos
        .iter()
        .position(|t| t.source == source_name && t.sink == sink_name)
    {
        process.topos.swap_remove(position);
    } else {
        return "no such topology".into();
    }
    MaybeError::new_ok()
}
