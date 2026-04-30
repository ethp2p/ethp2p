package sim

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/broadcast/rs"
	"gopkg.in/yaml.v3"
)

const (
	broadcastChannelID = broadcast.ChannelID("broadcast")
	autoWarmupDrain    = 10 * time.Second
)

// RunConfig is the unified YAML configuration for a simulation run.
type RunConfig struct {
	Simulation SimulationConfig `yaml:"simulation"`
	Strategy   StrategyConfig   `yaml:"strategy"`
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

// StrategyConfig uses custom UnmarshalYAML to dispatch by name into the
// correct typed config. Only the matching strategy pointer is non-nil.
type StrategyConfig struct {
	Name string
	RS   *RSStrategyConfig
	RLNC *RLNCStrategyConfig
}

// strategyRaw is used for two-pass YAML decoding: first read the name,
// then decode strategy-specific fields into the correct typed struct.
type strategyRaw struct {
	Name string `yaml:"name"`
}

func (sc *StrategyConfig) UnmarshalYAML(value *yaml.Node) error {
	var raw strategyRaw
	if err := value.Decode(&raw); err != nil {
		return fmt.Errorf("decode strategy name: %w", err)
	}
	sc.Name = raw.Name
	switch raw.Name {
	case "RS", "RS-ChunkLen":
		var cfg RSStrategyConfig
		if err := value.Decode(&cfg); err != nil {
			return fmt.Errorf("decode RS strategy config: %w", err)
		}
		sc.RS = &cfg
	case "RLNC", "RLNC-ChunkLen":
		if err := decodeRLNCStrategyConfig(sc, value); err != nil {
			return err
		}
	case "gossipsub":
		// no config
	default:
		return fmt.Errorf("unknown strategy: %s", raw.Name)
	}
	return nil
}

// RSStrategyConfig holds RS-specific parameters.
type RSStrategyConfig struct {
	DataShards        int  `yaml:"data_shards"`
	ParityShards      int  `yaml:"parity_shards"`
	ChunkLen          int  `yaml:"chunk_len"`
	ForwardMultiplier int  `yaml:"forward_multiplier"`
	EnableBitmaps     bool `yaml:"enable_bitmaps"`
	BitmapThreshold   int  `yaml:"bitmap_threshold"`
}

func (c *RSStrategyConfig) broadcastConfig() rs.Config {
	cfg := rs.Config{
		DataShards:        c.DataShards,
		ParityShards:      c.ParityShards,
		ChunkLen:          c.ChunkLen,
		ForwardMultiplier: c.ForwardMultiplier,
		DisableBitmap:     !c.EnableBitmaps,
		BitmapThreshold:   c.BitmapThreshold,
	}
	if cfg.ForwardMultiplier == 0 {
		cfg.ForwardMultiplier = 4
	}
	return cfg
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
	Warmup             string  `yaml:"warmup,omitempty"`
}

func (c *WorkloadConfig) PublishWait() time.Duration {
	return time.Duration(c.PublishWaitSeconds * float64(time.Second))
}

func (c *WorkloadConfig) StopTime() time.Duration {
	return time.Duration(c.StopTimeMinutes * float64(time.Minute))
}

func (c *WorkloadConfig) WarmupEnabled() (bool, error) {
	switch c.Warmup {
	case "", "auto":
		return true, nil
	case "off":
		return false, nil
	default:
		return false, fmt.Errorf("unknown workload.warmup: %s", c.Warmup)
	}
}

// LoadRunConfig reads and parses a YAML run config from path.
func LoadRunConfig(path string) (*RunConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var rc RunConfig
	if err := yaml.Unmarshal(data, &rc); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
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

// StrategyFunc creates a simulation node.
type StrategyFunc func(nodeNum int, conn net.PacketConn, logger *slog.Logger, obs broadcast.Observer, tw *TraceWriter) (Node, error)

// ECStrategy returns a StrategyFunc that creates broadcast nodes using
// the given erasure coding scheme. Engine, channel, and subscription setup
// are handled here; the scheme is the only varying part.
func ECStrategy[CI broadcast.ChunkIdent, R broadcast.Wire, P broadcast.Wire](scheme broadcast.Scheme[CI, R, P]) StrategyFunc {
	return func(nodeNum int, conn net.PacketConn, logger *slog.Logger, obs broadcast.Observer, tw *TraceWriter) (Node, error) {
		ready := newPeerReadyTracker(broadcastChannelID)
		if obs == nil {
			obs = broadcast.NoOpObserver{}
		}
		obs = &readinessObserver{Observer: obs, ready: ready}
		engine := broadcast.NewEngine(broadcast.EngineConfig{
			PeerID:   broadcast.PeerID(fmt.Sprintf("%d", nodeNum)),
			Observer: obs,
		})
		channel := broadcast.AttachChannel(engine, broadcastChannelID, scheme)
		recvCh := make(chan broadcast.FullMessage, 64)
		if err := channel.Subscribe(recvCh); err != nil {
			engine.Close()
			return nil, fmt.Errorf("failed to subscribe: %w", err)
		}
		return newBroadcastNode(
			engine,
			func(mid broadcast.MessageID, data []byte) error { return channel.Publish(mid, data) },
			channel.Stop,
			recvCh,
			conn, nodeNum, logger,
			ready,
		)
	}
}

// GossipsubStrategy returns a StrategyFunc that creates gossipsub nodes.
func GossipsubStrategy() StrategyFunc {
	return func(nodeNum int, conn net.PacketConn, logger *slog.Logger, obs broadcast.Observer, tw *TraceWriter) (Node, error) {
		return NewGossipsubNode(conn, nodeNum, logger, tw)
	}
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
	default:
		opts.DecoderName = "ethp2p"
	}
	return opts, nil
}

func (rc *RunConfig) strategyFunc() (StrategyFunc, error) {
	switch rc.Strategy.Name {
	case "RS", "RS-ChunkLen":
		return ECStrategy(rs.NewScheme(rc.Strategy.RS.broadcastConfig())), nil
	case "RLNC", "RLNC-ChunkLen":
		return rlncStrategyFunc(rc.Strategy.RLNC)
	case "gossipsub":
		return GossipsubStrategy(), nil
	default:
		return nil, fmt.Errorf("unknown strategy: %s", rc.Strategy.Name)
	}
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

	newNode, err := rc.strategyFunc()
	if err != nil {
		return nil, err
	}

	var drv Driver
	topo, topoErr := rc.LoadTopology()
	if topoErr != nil {
		return nil, topoErr
	}
	warmupEnabled, err := rc.Workload.WarmupEnabled()
	if err != nil {
		return nil, err
	}
	switch driver {
	case "shadow":
		drv = &ShadowDriver{Strategy: newNode}
	case "simnet":
		sd := &SimnetDriver{Strategy: newNode, Topology: topo}
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

	var warmupBytes map[int]map[int]int
	var warmupDrain time.Duration
	if warmupEnabled {
		warmupBytes = AutoWarmupBytes(topo, rc.Workload.MessageSize)
		warmupDrain = autoWarmupDrain
	}

	return &Scenario{
		NumMessages:           rc.Workload.NumMessages,
		MessageSize:           rc.Workload.MessageSize,
		Driver:                drv,
		BandwidthLogFrequency: rc.Simulation.BandwidthLogFrequency(),
		WarmupBytes:           warmupBytes,
		WarmupDrain:           warmupDrain,
		Logger:                logger,
	}, nil
}
