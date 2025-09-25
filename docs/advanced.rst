Advanced Features
=================

Risk
----
Predicer models uncertainty by adding a Conditional Value at Risk (CVaR) term to the objective function.

Example::

  createRisk(risk: { parameter: "alfa", value: 0.95 }) { errors { field message } }
  createRisk(risk: { parameter: "beta", value: 0.1 }) { errors { field message } }

Inflow Blocks
-------------
Inflow blocks (or blocks) model flexibility actions like shifting inflow between timesteps.
Each block has a binary variable and a series of constants defining how much inflow is shifted at each timestep.
Blocks are linked to a specific node and scenario, and only one block may be active for a given node,
time and scenario.

Example of a block ``b1`` acting on node ``n1`` in scenario ``s1`` (tabular form)::

  t   b1,n1   b1,s1
  1   20.4.2022 1:00   6
  2   20.4.2022 2:00  -3
  3   20.4.2022 3:00  -2
  4   20.4.2022 4:00  -1

(The Hertta API does not currently support creating blocks via GraphQL.)

Node Diffusion
--------------
Node diffusion models energy flow between two storage nodes based on their state (e.g. heat transfer).
The flow is ``E = k * (T1 - T2)``, where ``k`` is the diffusion coefficient.

Example::

  createNodeDiffusion(
    newDiffusion: {
      fromNode: "Room1"
      toNode: "Room2"
      coefficient: [ { scenario: null, constant: 0.05, series: null } ]
    }
  ) { errors { field message } }

Node Delay
----------
A node delay represents a one-way delay between two nodes (e.g. water travel time between reservoirs).
Commodity and market nodes cannot participate in a delay.

Example::

  createNodeDelay(
    delay: {
      fromNode: "R1"
      toNode: "R2"
      delay: 3.0
      minDelayFlow: -1e9
      maxDelayFlow: 1e9
    }
  ) { errors { field message } }

Node Histories
--------------
When modelling delays, the downstream node needs a history defining inflow for the first ``d`` timesteps.

Example::

  createNodeHistory(nodeName: "R2") { errors { field message } }

  addStepToNodeHistory(
    nodeName: "R2",
    step: {
      scenario: "Scenario1"
      durations: [ { hours: 1, minutes: 0, seconds: 0 }, { hours: 1, minutes: 0, seconds: 0 } ]
      values: [3.0, 4.0]
    }
  ) { errors { field message } }

General Constraints
-------------------
General constraints restrict relationships between process flows, storage states or online variables.
They can be rigid (strict equality/inequality) or setpoint constraints with a penalty for deviations.

Example (less-than constraint + flow factors)::

  createGenConstraint(
    constraint: {
      name: "gt_c1"
      gcType: LESS_THAN
      isSetpoint: false
      penalty: 0.0
      constant: [ { scenario: null, constant: 0.0, series: null } ]
    }
  ) { errors { field message } }

  # Electricity flow factor
  createFlowConFactor(
    factor: [ { scenario: null, constant: 3.0, series: null } ],
    constraintName: "gt_c1",
    processName: "gas_turb",
    sourceOrSinkNodeName: "elc"
  )

  # Heat flow factor
  createFlowConFactor(
    factor: [ { scenario: null, constant: -1.0, series: null } ],
    constraintName: "gt_c1",
    processName: "gas_turb",
    sourceOrSinkNodeName: "heat"
  )

Troubleshooting
---------------
- **Permissions:** Use ``sudo`` where necessary on Linux/macOS.
- **PATH issues:** Ensure Cargo/Rust are available in your PATH.
- **Updating dependencies:** Pull changes with ``git pull`` and rebuild with ``cargo build --release``.
