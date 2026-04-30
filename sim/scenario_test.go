package sim

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/ethp2p/broadcast/rs"
	"github.com/stretchr/testify/require"
)

func TestNetwork(t *testing.T) {
	tests := []Topology{
		{
			Nodes: []NodeSpec{
				{Num: 0, UploadBWMbps: 50, DownloadBWMbps: 50},
				{Num: 1, UploadBWMbps: 50, DownloadBWMbps: 50},
			},
			Edges: []EdgeSpec{
				{Source: 0, Target: 1, LatencyMs: 50},
			},
		},
		{
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
		},
	}
	for i, tt := range tests {
		nets := []struct {
			Name     string
			Scenario *Scenario
		}{
			{
				Name: "RS",
				Scenario: &Scenario{
					NumMessages: 1,
					MessageSize: 10 * 1024,
					Driver: &SimnetDriver{
						Strategy: ECStrategy(rs.NewScheme(rs.Config{
							DataShards:   16,
							ParityShards: 16,
						})),
						Topology: tt,
					},
				},
			},
			{
				Name: "RS-ChunkLen",
				Scenario: &Scenario{
					NumMessages: 10,
					MessageSize: 10 * 1024,
					Driver: &SimnetDriver{
						Strategy: ECStrategy(rs.NewScheme(rs.Config{
							ChunkLen: 16 << 10,
						})),
						Topology: tt,
					},
				},
			},
			{
				Name: "RS-ChunkLen-2",
				Scenario: &Scenario{
					NumMessages: 10,
					MessageSize: 10 * 1024,
					Driver: &SimnetDriver{
						Strategy: ECStrategy(rs.NewScheme(rs.Config{
							ChunkLen: 16 << 10,
						})),
						Topology: tt,
					},
				},
			},
			{
				Name: "Gossipsub",
				Scenario: &Scenario{
					NumMessages: 30,
					MessageSize: 10 * 1024,
					Driver: &SimnetDriver{
						Strategy: GossipsubStrategy(),
						Topology: tt,
					},
				},
			},
		}
		for _, ns := range nets {
			t.Run(fmt.Sprintf("%s-%d", ns.Name, i), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					defer ns.Scenario.Close()
					ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
					defer cancel()
					stats, err := RunSimnetScenario(ctx, ns.Scenario, 5*time.Second)
					require.NoError(t, err)
					require.Len(t, stats.ReceivedMessages, len(tt.Nodes)-1)
					for nn, received := range stats.ReceivedMessages {
						require.Len(t, received, ns.Scenario.NumMessages)
						for mid := range received {
							require.Contains(t, stats.PublishedMessages, mid,
								"node %d received unknown message %s", nn, mid)
						}
					}
				})
			})
		}
	}
}
