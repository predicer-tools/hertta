import requests
import json

# Example Topology
example_topology = {
    "source": "SourceNode",
    "sink": "SinkNode",
    "capacity": 100.0,
    "vom_cost": 5.0,
    "ramp_up": 50.0,
    "ramp_down": 50.0,
}

# Example Process
example_process = {
    "name": "Process1",
    "group": "GroupA",
    "delay": 1.0,
    "is_cf": True,
    "is_cf_fix": False,
    "is_online": True,
    "is_res": False,
    "conversion": 1,
    "eff": 0.9,
    "load_min": 10.0,
    "load_max": 100.0,
    "start_cost": 20.0,
    "min_online": 2.0,
    "min_offline": 1.0,
    "max_online": 24.0,
    "max_offline": 12.0,
    "initial_state": 1.0,
    "topos": [example_topology],  # List of Topology
    "eff_ops": ["eff_op1", "eff_op2"],  # Example efficiency operations
}

input_data = {
    "contains_reserves": True,
    "contains_online": False,
    "contains_state": True,
    "contains_piecewise_eff": False,
    "contains_risk": True,
    "contains_delay": False,
    "contains_diffusion": True,
    "nodes": {
        "node1": { 
            # Node details
        },
        # ... more nodes
    },
    "processes": {
        "process1": { 
            # Process details
        },
        # ... more processes
    },
    # ... more fields like markets, groups, etc.
}

# Convert to JSON
json_data = json.dumps(data)