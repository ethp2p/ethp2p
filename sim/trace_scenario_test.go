package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/ethp2p/broadcast/rs"
	"github.com/stretchr/testify/require"
)

// TestTraceScenario runs a small 6-node RS simulation with tracing enabled
// and writes the .bctrace output to $TMPDIR/test.bctrace.
func TestTraceScenario(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		topo := Topology{
			Nodes: []NodeSpec{
				{Num: 0, UploadBWMbps: 50, DownloadBWMbps: 50},
				{Num: 1, UploadBWMbps: 50, DownloadBWMbps: 50},
				{Num: 2, UploadBWMbps: 50, DownloadBWMbps: 50},
				{Num: 3, UploadBWMbps: 50, DownloadBWMbps: 50},
				{Num: 4, UploadBWMbps: 50, DownloadBWMbps: 50},
				{Num: 5, UploadBWMbps: 50, DownloadBWMbps: 50},
			},
			Edges: []EdgeSpec{
				{Source: 0, Target: 1, LatencyMs: 50},
				{Source: 0, Target: 4, LatencyMs: 50},
				{Source: 1, Target: 2, LatencyMs: 50},
				{Source: 1, Target: 4, LatencyMs: 50},
				{Source: 2, Target: 1, LatencyMs: 50},
				{Source: 2, Target: 3, LatencyMs: 50},
				{Source: 3, Target: 2, LatencyMs: 50},
				{Source: 3, Target: 4, LatencyMs: 50},
				{Source: 4, Target: 1, LatencyMs: 50},
				{Source: 4, Target: 0, LatencyMs: 50},
				{Source: 4, Target: 5, LatencyMs: 50},
				{Source: 5, Target: 4, LatencyMs: 50},
			},
		}

		nodes := make([]string, len(topo.Nodes))
		for i, ns := range topo.Nodes {
			nodes[i] = fmt.Sprintf("n%d", ns.Num)
		}
		cfgJSON, _ := json.Marshal(map[string]any{
			"strategy": "rs", "dataShards": 16, "parityShards": 16,
		})

		var buf bytes.Buffer
		tw, err := NewTraceWriter(&buf, time.Now(), nodes, topo, cfgJSON)
		require.NoError(t, err)

		drv := &SimnetDriver{
			Strategy: ECStrategy(rs.NewScheme(rs.Config{
				DataShards:   16,
				ParityShards: 16,
			})),
			Topology:    topo,
			TraceWriter: tw,
		}

		s := &Scenario{
			NumMessages: 1,
			MessageSize: 10 * 1024,
			Driver:      drv,
		}
		defer s.Close()

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
		defer cancel()

		stats, err := RunSimnetScenario(ctx, s, 5*time.Second)
		require.NoError(t, err)
		require.Len(t, stats.ReceivedMessages, len(topo.Nodes)-1)

		require.NoError(t, tw.Close())

		outPath := os.TempDir() + "/test.bctrace"
		require.NoError(t, os.WriteFile(outPath, buf.Bytes(), 0644))
		t.Logf("trace written to %s (%d bytes)", outPath, buf.Len())
	})
}
