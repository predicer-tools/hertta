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
