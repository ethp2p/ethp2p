//go:build rlnc

package sim

import (
	"fmt"

	"github.com/ethp2p/ethp2p/broadcast/rlnc"
	"gopkg.in/yaml.v3"
)

// RLNCStrategyConfig holds RLNC-specific parameters.
type RLNCStrategyConfig struct {
	NumChunks              int `yaml:"num_chunks"`
	NumChunksPerGeneration int `yaml:"num_chunks_per_generation"`
	TargetChunkSize        int `yaml:"target_chunk_size"`
	OriginRedundancy       int `yaml:"origin_redundancy"`
	ForwardMultiplier      int `yaml:"forward_multiplier"`
}

func (c *RLNCStrategyConfig) broadcastConfig() rlnc.Config {
	cfg := rlnc.Config{
		NumChunks:              c.NumChunks,
		NumChunksPerGeneration: c.NumChunksPerGeneration,
		TargetChunkSize:        c.TargetChunkSize,
		OriginRedundancy:       c.OriginRedundancy,
		ForwardMultiplier:      c.ForwardMultiplier,
	}
	if cfg.ForwardMultiplier == 0 {
		cfg.ForwardMultiplier = 4
	}
	return cfg
}

func decodeRLNCStrategyConfig(sc *StrategyConfig, value *yaml.Node) error {
	var cfg RLNCStrategyConfig
	if err := value.Decode(&cfg); err != nil {
		return fmt.Errorf("decode RLNC strategy config: %w", err)
	}
	sc.RLNC = &cfg
	return nil
}

func rlncStrategyFunc(config *RLNCStrategyConfig) (StrategyFunc, error) {
	if config == nil {
		return nil, fmt.Errorf("missing RLNC strategy config")
	}
	return ECStrategy(rlnc.NewScheme(config.broadcastConfig())), nil
}
