# Simulation package

Network simulation infrastructure for testing erasure-coded broadcast strategies against a gossipsub baseline. Supports configurable topologies derived from real Ethereum node distribution, two execution drivers (Shadow for scale, simnet for fast iteration), and a Python CLI for orchestration.

## Prerequisites

- **Python 3.12+** with **uv** for the `simctl` CLI
- **Go 1.25+** for building the simulation node
- **Shadow** for shadow mode simulations (see [shadow-rs.github.io](https://shadow-rs.github.io/docs/guide/install/))

## Quick start

```bash
# Install CLI (from sim/cli directory)
cd sim/cli && uv sync

# Generate default config
simctl init -o config.yaml

# Run simulation (driver is set in config: simulation.driver)
simctl run config.yaml

# Compare strategies with an experiment
simctl experiment init -o experiment.yaml
# Edit experiment.yaml to configure strategies...
simctl experiment run experiment.yaml --output-dir=results/
```

## Configuration

A single YAML schema drives both Go binaries and the Python CLI. The config has four sections: `simulation`, `strategy`, `workload`, and `topology`.

### Run config

See [`cli/configs/small_rs.yaml`](cli/configs/small_rs.yaml) for a minimal example.

```yaml
simulation:
  driver: shadow # shadow or simnet
  log_level: info # debug, info, warn, error
  bandwidth_log_frequency_ms: 100

strategy:
  name: RS # RS, RLNC, gossipsub
  data_shards: 16
  parity_shards: 16
  enable_bitmaps: true
  bitmap_threshold: 100

workload:
  num_messages: 10
  message_size: 100000 # bytes
  publish_wait_seconds: 10.0
  stop_time_minutes: 30.0

topology:
  generate: # Python generates topology before running Go
    num_nodes: 100
    degree: 8
    type: random # random or ring
    seed: 42
    super_node_fraction: 0.0
```

The `topology` section has two mutually exclusive modes:

- **`topology.generate`**: Python generates a topology JSON file from these parameters, saves it alongside the run config, and rewrites the config with `topology.file` pointing to the generated file before invoking Go.
- **`topology.file`**: Points directly to a pre-existing topology JSON file. Go reads this; it never sees `generate` parameters.

If Go encounters `topology.generate` without `topology.file`, it errors with a message directing you to run via `simctl`.

### Strategy parameters

Each strategy reads only its own fields; unknown fields are ignored.

RLNC strategies require the private `../ethp2p-extras` overlay linked with
`just link-rlnc` and Go commands run with `-tags rlnc`.

| Strategy  | Fields                                                                                                             |
| --------- | ------------------------------------------------------------------------------------------------------------------ |
| RS        | `data_shards`, `parity_shards`, `chunk_len`, `forward_multiplier`, `enable_bitmaps`, `bitmap_threshold`            |
| RLNC      | `num_chunks`, `num_chunks_per_generation`, `target_chunk_size`, `origin_redundancy`, `forward_multiplier`          |
| gossipsub | (none)                                                                                                             |

### Experiment config

Experiments share topology and workload across multiple strategy variations. See [`cli/configs/example_experiment.yaml`](cli/configs/example_experiment.yaml).

```yaml
name: strategy-comparison
description: "Compare RS, RLNC, and gossipsub"

simulation:
  driver: shadow
  log_level: info

topology:
  generate:
    num_nodes: 100
    degree: 8
    type: random
    seed: 42

workload:
  num_messages: 10
  message_size: 100000
  publish_wait_seconds: 10.0
  stop_time_minutes: 30.0

strategies:
  - name: RS
    data_shards: 16
    parity_shards: 16

  - name: RLNC
    num_chunks: 16

  - name: gossipsub
    publish_wait_seconds: 30.0
```

The topology is generated once and shared by all strategy runs within the experiment.

## Topology generation

Topology generation is handled by Python (`sim/cli/simctl/topology.py`) using real-world data from `sim/cli/data/`:

**`data/country_weights.json`** contains the relative frequency of Ethereum nodes per country, derived from network crawl data. Examples: US (5031), Germany (2241), France (1032), Finland (962). When generating a topology, each node is assigned a country via weighted random selection, so a 100-node topology reflects the actual geographic distribution of the Ethereum network.

**`data/country_latencies.json`** is a country-to-country latency matrix (measured round-trip times). After nodes are placed and connected into a graph, each edge's latency is looked up from this matrix based on the countries of the source and target nodes.

Bandwidth is assigned by role:

- Node 0 (block builder): 50 Mbps up / 100 Mbps down
- Super nodes (controlled by `super_node_fraction`): 1024 / 1024 Mbps
- Regular nodes: 50 / 50 Mbps

The generation algorithm:

1. Assigns countries and bandwidth to all nodes
2. Builds a spanning tree for connectivity (every node reachable)
3. Adds random edges until each node reaches the target degree
4. Looks up edge latencies from the country matrix

The output is a `topology.json` with `nodes` (num, upload/download bandwidth, country) and `edges` (source, target, latency). Go reads this JSON directly via `LoadTopology()` and ignores the `country` field.

## CLI reference

```
simctl
├── init               # Generate default run config
├── topology           # Generate network topology standalone
├── run                # Run single simulation
├── analyze            # Analyze results
├── experiment
│   ├── init           # Generate experiment config
│   └── run            # Run multiple strategies
└── remote
    ├── run            # Run on remote host
    └── experiment     # Run experiment on remote host
```

### simctl run

```bash
simctl run config.yaml
simctl run config.yaml --output-dir=results/
```

The driver (`shadow` or `simnet`) is read from `simulation.driver` in the config.

### simctl experiment run

```bash
simctl experiment run experiment.yaml --output-dir=results/
```

### simctl remote

Run simctl commands on a remote host. Syncs the local codebase via rsync, executes the command, monitors for completion, and tars the output on finish.

```bash
simctl remote run config.yaml --host=user@server
simctl remote experiment experiment.yaml --host=user@server
simctl remote experiment experiment.yaml --host=user@server --dry-run
```

## Execution modes

**Shadow mode** uses the Shadow discrete-event network simulator for realistic large-scale testing (100+ nodes). Each node runs as a separate process with simulated network I/O. The Python CLI generates a Shadow YAML config with GML network graph, builds the `simnode` binary, and invokes Shadow.

**Simnet mode** runs all nodes in-process using Go's `testing/synctest` for deterministic execution. No Shadow installation required. Reliable up to ~16 nodes.

## Strategies

RS and RLNC nodes are created via `ECStrategy`, which wraps the broadcast engine's `Scheme` interface. Gossipsub is a standalone libp2p implementation used as a baseline.

| Strategy  | Description                                   |
| --------- | --------------------------------------------- |
| RS        | Reed-Solomon erasure coding                   |
| RLNC      | Random linear network coding                  |
| gossipsub | libp2p gossipsub baseline (no erasure coding) |

## Running tests

```bash
# Unit tests (fast, in-process simnet)
GOEXPERIMENT=synctest go test ./sim/... -v -short -count=1

# Large network tests (32 nodes, realistic topologies)
GOEXPERIMENT=synctest go test ./sim/... -v -run TestLargeNetwork -timeout=30m
```

## Adding a new strategy

For erasure-coded strategies, implement a `Scheme` (see `broadcast/rs`, `broadcast/rlnc`) and wire it via `ECStrategy` in `config.go`. For non-EC strategies, implement the `Node` interface directly (see `strategy_gossipsub.go`). In both cases, add the strategy name to `StrategyConfig.UnmarshalYAML` and `strategyFunc()` in `config.go`, and to the Python config in `cli/simctl/config.py`.

## Package layout

```
sim/
├── config.go              # RunConfig, StrategyConfig, NewNodeFunc
├── scenario.go            # Scenario orchestration, RunSimnetScenario
├── node.go                # Node interface
├── host.go                # QUIC host setup
├── driver.go              # Driver interface
├── driver_simnet.go       # In-process simnet driver
├── driver_shadow.go       # Shadow driver with serialized UDP writes
├── observer.go            # Observer for tracking bytes and chunk verdicts
├── collector.go           # Metrics collection
├── trace_writer.go        # Trace event output
├── trace_observer.go      # Trace-level observation
├── strategy_gossipsub.go  # Gossipsub node implementation
├── cmd/
│   └── shadow/            # Shadow binary (--config, --node-num)
└── cli/
    ├── simctl/            # Python CLI (config, runner, topology, experiment)
    └── data/
        ├── country_weights.json     # Ethereum node geographic distribution
        └── country_latencies.json   # Country-to-country RTT matrix
```

## Known limitations

- Tests using `testing/synctest` require `GOEXPERIMENT=synctest`
- Shadow simulations require Shadow to be installed separately
- Simnet is reliable up to ~16 nodes; beyond that, tests can stall
