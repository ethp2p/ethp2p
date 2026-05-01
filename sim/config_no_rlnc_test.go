//go:build !rlnc

package sim

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestStrategyConfigRejectsRLNCWithoutBuildTag(t *testing.T) {
	var cfg StrategyConfig
	err := yaml.Unmarshal([]byte("name: RLNC\nnum_chunks: 16\n"), &cfg)
	require.ErrorContains(t, err, "RLNC strategy requires -tags rlnc")
}
