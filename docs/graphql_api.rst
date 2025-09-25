GraphQL API
===========

This section explains how to build Predicer input data using Hertta’s GraphQL API.
Each subsection describes an object, its parameters, and provides examples.

TimeLineSettings
----------------
The timeline defines the resolution and length of your optimisation problem.
It is the same for all data objects, so the number of values in every time series
must match the number of timesteps.

Set the timeline with the ``updateTimeLine`` mutation.

Example (six-hour timeline, hourly steps)::

  updateTimeLine(
    timeLineInput: {
      duration: { hours: 6, minutes: 0, seconds: 0 }
      step: { hours: 1, minutes: 0, seconds: 0 }
    }
  ) {
    errors { field message }
  }

For a six-hour horizon starting at ``2025-04-20T00:00:00Z`` the resulting timestamps are::

  2025-04-20T00:00:00Z
  2025-04-20T01:00:00Z
  2025-04-20T02:00:00Z
  2025-04-20T03:00:00Z
  2025-04-20T04:00:00Z
  2025-04-20T05:00:00Z

InputDataSetup
--------------
Global settings that affect optimisation behaviour.
Create or edit via ``createInputDataSetup`` / ``updateInputDataSetup``.

Example (enable market bids and reserves)::

  createInputDataSetup(
    setupUpdate: {
      useMarketBids: true
      useReserves: true
      useReserveRealisation: false
      useNodeDummyVariables: true
      useRampDummyVariables: true
      commonTimesteps: 0
      commonScenarioName: null
      nodeDummyVariableCost: 1000000.0
      rampDummyVariableCost: 1000000.0
    }
  ) {
    errors { field message }
  }

Scenarios
---------
Scenarios represent different possible futures. Use ``createScenario`` to add them.

Example::

  createScenario(name: "scenarioA", weight: 1.0) { message }
  createScenario(name: "scenarioB", weight: 2.0) { message }

Nodes
-----
Nodes are fundamental building blocks in Predicer. Use ``createNode``.
Nodes may represent commodities, markets, reserves or storages.

Example 1: constant cost, no inflow::

  createNode(
    node: {
      name: "Node1"
      isCommodity: false
      isMarket: false
      isRes: false
      cost: [
        { scenario: null, constant: 24.0, series: null }
      ]
      inflow: []
    }
  ) {
    errors { field message }
  }

Example 2: cost series for a scenario::

  createNode(
    node: {
      name: "Node2"
      isCommodity: false
      isMarket: false
      isRes: false
      cost: [
        {
          scenario: "Scenario1"
          constant: null
          series: [10.0, 12.0, 14.0, 13.0, 11.0, 15.0]
        }
      ]
      inflow: []
    }
  ) {
    errors { field message }
  }

Example 3: inflow from an electricity price forecast::

  createNode(
    node: {
      name: "Node3"
      isCommodity: false
      isMarket: false
      isRes: false
      cost: []
      inflow: [
        {
          scenario: "Scenario1"
          constant: null
          series: null
          forecast: "ELERING"
          fType: "electricity"
        }
      ]
    }
  ) {
    errors { field message }
  }

Node state (storage)
--------------------
If a node has storage, set its state separately using ``setNodeState``::

  setNodeState(
    nodeName: "Node1"
    state: {
      inMax: 100.0
      outMax: 80.0
      stateLossProportional: 0.01
      stateMin: 20.0
      stateMax: 200.0
      initialState: 50.0
      isScenarioIndependent: true
      isTemp: false
      tEConversion: 1.0
      residualValue: 0.0
    }
  ) {
    errors { field message }
  }

Processes
---------
Processes convert or transfer commodities between nodes.
Use ``createProcess`` with a ``NewProcess`` input.

Example::

  createProcess(
    process: {
      name: "Process1"
      conversion: UNIT
      isCfFix: false
      isOnline: true
      isRes: false
      eff: 0.95
      loadMin: 0.3
      loadMax: 1.0
      startCost: 100.0
      minOnline: 2.0
      maxOnline: 6.0
      minOffline: 1.0
      maxOffline: 24.0
      initialState: false
      isScenarioIndependent: true
      cf: []
      effTs: []
    }
  ) {
    errors { field message }
  }

Groups
------
Groups allow you to bundle nodes or processes.

Example::

  # Create a process group
  createProcessGroup(name: "g1") { message }

  # Add a process to the group
  addProcessToGroup(processName: "Process1", groupName: "g1") { message }

  # Create a node group
  createNodeGroup(name: "ng1") { message }

  # Add a node to the group
  addNodeToGroup(nodeName: "Node1", groupName: "ng1") { message }

Markets
-------
Markets balance your model by allowing buying or selling of commodities.
Markets can be of energy or reserve type.

Example (energy market)::

  createMarket(
    market: {
      name: "el_market"
      mType: ENERGY
      node: "GridNode"
      processGroup: ""
      direction: null
      reserveType: null
      isBid: true
      isLimited: false
      minBid: 0.0
      maxBid: 0.0
      fee: 0.0
      price: [
        { scenario: null, constant: 50.0, series: null, forecast: null, fType: null }
      ]
      upPrice: []
      downPrice: []
      realisation: []
      reserveActivationPrice: []
    }
  ) {
    errors { field message }
  }

Connect market prices to an external forecast::

  connectMarketPricesToForecast(
    marketName: "el_market",
    forecastName: "ENTSOE",
    forecastType: "electricity"
  ) { message }

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
