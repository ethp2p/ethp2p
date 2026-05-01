//go:build !rlnc

package sim

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// RLNCStrategyConfig is available in the public schema so RLNC configs can
// produce a targeted build-tag error when the private implementation is absent.
type RLNCStrategyConfig struct {
	NumChunks              int `yaml:"num_chunks"`
	NumChunksPerGeneration int `yaml:"num_chunks_per_generation"`
	TargetChunkSize        int `yaml:"target_chunk_size"`
	OriginRedundancy       int `yaml:"origin_redundancy"`
	ForwardMultiplier      int `yaml:"forward_multiplier"`
}

func decodeRLNCStrategyConfig(_ *StrategyConfig, _ *yaml.Node) error {
	return fmt.Errorf("RLNC strategy requires -tags rlnc and a linked broadcast/rlnc implementation")
}

func rlncStrategyFunc(_ *RLNCStrategyConfig) (StrategyFunc, error) {
	return nil, fmt.Errorf("RLNC strategy requires -tags rlnc and a linked broadcast/rlnc implementation")
}
