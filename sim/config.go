package sim

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/ethp2p/ethp2p/broadcast/rs"
	"gopkg.in/yaml.v3"
)

// RunConfig is the unified YAML configuration for a simulation run.
type RunConfig struct {
	Simulation SimulationConfig `yaml:"simulation"`
	Strategy   Strategy         `yaml:"strategy"`
	Workload   WorkloadConfig   `yaml:"workload"`
	Topology   TopologyConfig   `yaml:"topology"`
}

// TopologyConfig supports two mutually exclusive modes.
// Go only uses File; Generate is for the Python CLI.
type TopologyConfig struct {
	File     string            `yaml:"file,omitempty"`
	Generate *TopologyGenerate `yaml:"generate,omitempty"`
}

// TopologyGenerate holds parameters for Python-side topology generation.
type TopologyGenerate struct {
	NumNodes          int     `yaml:"num_nodes"`
	Degree            int     `yaml:"degree"`
	Type              string  `yaml:"type"`
	Seed              int     `yaml:"seed"`
	SuperNodeFraction float64 `yaml:"super_node_fraction"`
}

type Strategy struct {
	Name string    `yaml:"name"`
	RS   rs.Config `yaml:",inline"`
}

// SimulationConfig holds runtime/infrastructure settings.
type SimulationConfig struct {
	Driver                  string `yaml:"driver"`
	LogLevel                string `yaml:"log_level"`
	LogFile                 string `yaml:"log_file,omitempty"`
	TraceFile               string `yaml:"trace_file,omitempty"`
	BandwidthLogFrequencyMs int    `yaml:"bandwidth_log_frequency_ms"`
}

func (c *SimulationConfig) BandwidthLogFrequency() time.Duration {
	return time.Duration(c.BandwidthLogFrequencyMs) * time.Millisecond
}

// WorkloadConfig describes what to publish.
type WorkloadConfig struct {
	NumMessages        int     `yaml:"num_messages"`
	MessageSize        int     `yaml:"message_size"`
	PublishWaitSeconds float64 `yaml:"publish_wait_seconds"`
	StopTimeMinutes    float64 `yaml:"stop_time_minutes"`
}

func (c *WorkloadConfig) PublishWait() time.Duration {
	return time.Duration(c.PublishWaitSeconds * float64(time.Second))
}

func (c *WorkloadConfig) StopTime() time.Duration {
	return time.Duration(c.StopTimeMinutes * float64(time.Minute))
}

// LoadRunConfig reads and parses a YAML run config from path.
func LoadRunConfig(path string) (*RunConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	rc := RunConfig{Strategy: Strategy{RS: rs.DefaultConfig()}}
	if err := yaml.Unmarshal(data, &rc); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	switch rc.Strategy.Name {
	case "RS":
		if err := rc.Strategy.RS.Validate(); err != nil {
			return nil, fmt.Errorf("invalid RS strategy config: %w", err)
		}
	case "gossipsub":
	default:
		return nil, fmt.Errorf("unknown strategy: %s", rc.Strategy.Name)
	}
	return &rc, nil
}

// LoadTopology reads the topology JSON file referenced by Topology.File.
// Returns an error if File is empty (topology not yet generated).
func (rc *RunConfig) LoadTopology() (Topology, error) {
	if rc.Topology.File == "" {
		if rc.Topology.Generate != nil {
			return Topology{}, fmt.Errorf("topology.generate requires resolution by simctl; use topology.file for direct execution")
		}
		return Topology{}, fmt.Errorf("topology.file is required")
	}
	data, err := os.ReadFile(rc.Topology.File)
	if err != nil {
		return Topology{}, fmt.Errorf("read topology file: %w", err)
	}
	var topo Topology
	if err := json.Unmarshal(data, &topo); err != nil {
		return Topology{}, fmt.Errorf("parse topology JSON: %w", err)
	}
	return topo, nil
}

func (rc *RunConfig) BuildTraceHeaderOptions(topo Topology) (TraceHeaderOptions, error) {
	opts := TraceHeaderOptions{}
	switch rc.Strategy.Name {
	case "gossipsub":
		opts.DecoderName = "gossipsub"
		opts.PeerIDs = make([]string, len(topo.Nodes))
		for i, ns := range topo.Nodes {
			pid, err := gossipsubPeerID(ns.Num)
			if err != nil {
				return TraceHeaderOptions{}, fmt.Errorf("derive gossipsub peer id for node %d: %w", ns.Num, err)
			}
			opts.PeerIDs[i] = pid.String()
		}
	case "RS":
		opts.DecoderName = "ethp2p"
		// ethp2p peer IDs are transport identities, not node numbers, so the
		// header must carry the mapping for consumers to resolve events to nodes.
		opts.PeerIDs = make([]string, len(topo.Nodes))
		for i, ns := range topo.Nodes {
			_, peerID, err := nodeIdentity(ns.Num)
			if err != nil {
				return TraceHeaderOptions{}, fmt.Errorf("derive ethp2p peer id for node %d: %w", ns.Num, err)
			}
			opts.PeerIDs[i] = string(peerID)
		}
	default:
		return TraceHeaderOptions{}, fmt.Errorf("unknown strategy: %s", rc.Strategy.Name)
	}
	return opts, nil
}

// NewScenario builds a Scenario from this config, selecting the
// environment based on the driver ("shadow" or "simnet").
func (rc *RunConfig) NewScenario(driver string, logger *slog.Logger) (*Scenario, error) {
	if driver == "" {
		driver = rc.Simulation.Driver
	}
	if driver == "" {
		return nil, fmt.Errorf("no driver specified: set simulation.driver in config or pass driver argument")
	}

	topo, err := rc.LoadTopology()
	if err != nil {
		return nil, err
	}

	var drv Driver
	switch driver {
	case "shadow":
		drv = &ShadowDriver{Strategy: rc.Strategy}
	case "simnet":
		sd := &SimnetDriver{Strategy: rc.Strategy, Topology: topo}
		if rc.Simulation.TraceFile != "" {
			f, err := os.Create(rc.Simulation.TraceFile)
			if err != nil {
				return nil, fmt.Errorf("create trace file: %w", err)
			}
			nodes := make([]string, len(topo.Nodes))
			for i, ns := range topo.Nodes {
				nodes[i] = fmt.Sprintf("n%d", ns.Num)
			}
			cfgJSON, _ := json.Marshal(rc.Strategy)
			traceOpts, err := rc.BuildTraceHeaderOptions(topo)
			if err != nil {
				f.Close()
				return nil, fmt.Errorf("build trace header options: %w", err)
			}
			tw, err := NewTraceWriterWithOptions(f, time.Now(), nodes, topo, cfgJSON, traceOpts)
			if err != nil {
				f.Close()
				return nil, fmt.Errorf("create trace writer: %w", err)
			}
			sd.TraceWriter = tw
		}
		drv = sd
	default:
		return nil, fmt.Errorf("unknown driver: %s", driver)
	}

	if logger == nil {
		var w io.Writer = os.Stderr
		if rc.Simulation.LogFile != "" {
			f, err := os.Create(rc.Simulation.LogFile)
			if err != nil {
				return nil, fmt.Errorf("create log file: %w", err)
			}
			w = f
		}
		level := new(slog.Level)
		_ = level.UnmarshalText([]byte(rc.Simulation.LogLevel))
		logger = slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: level}))
	}

	return &Scenario{
		NumMessages:           rc.Workload.NumMessages,
		MessageSize:           rc.Workload.MessageSize,
		Driver:                drv,
		BandwidthLogFrequency: rc.Simulation.BandwidthLogFrequency(),
		Logger:                logger,
		topology:              topo,
	}, nil
}
