package sim

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestStrategyConfigRejectsRemovedChunkLenVariants(t *testing.T) {
	for _, name := range []string{"RS-ChunkLen", "RLNC-ChunkLen"} {
		t.Run(name, func(t *testing.T) {
			var cfg StrategyConfig
			err := yaml.Unmarshal([]byte("name: "+name+"\n"), &cfg)
			require.ErrorContains(t, err, "unknown strategy: "+name)
		})
	}
}
