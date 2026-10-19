package simnet

import (
	"bytes"
	"context"
	"flag"
	"testing"
	"testing/synctest"

	"github.com/ethp2p/ethp2p/sim"
)

var configFile = flag.String("config", "", "resolved simulation config YAML")

// TestScenario is the simctl entry point. It checks the complete published
// payload at every non-origin node, so a successful process exit means all
// configured messages reached every node before the simulated stop time.
func TestScenario(t *testing.T) {
	if *configFile == "" {
		t.Skip("run through simctl with --config")
	}
	rc, err := sim.LoadRunConfig(*configFile)
	if err != nil {
		t.Fatal(err)
	}
	topology, err := rc.LoadTopology()
	if err != nil {
		t.Fatal(err)
	}
	if len(topology.Nodes) < 2 {
		t.Fatalf("topology has %d nodes, want at least two", len(topology.Nodes))
	}

	synctest.Test(t, func(t *testing.T) {
		scenario, err := rc.NewScenario("simnet", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer scenario.Close()
		ctx, cancel := context.WithTimeout(t.Context(), rc.Workload.StopTime())
		defer cancel()
		stats, err := sim.RunSimnetScenario(ctx, scenario, rc.Workload.PublishWait())
		if err != nil {
			t.Fatal(err)
		}
		if got := len(stats.PublishedMessages); got != rc.Workload.NumMessages {
			t.Fatalf("published %d/%d messages", got, rc.Workload.NumMessages)
		}
		for _, node := range topology.Nodes {
			if node.Num == 0 {
				continue
			}
			received := stats.ReceivedMessages[node.Num]
			if len(received) != rc.Workload.NumMessages {
				t.Errorf("node %d received %d/%d messages", node.Num, len(received), rc.Workload.NumMessages)
				continue
			}
			for id, want := range stats.PublishedMessages {
				if !bytes.Equal(received[id], want) {
					t.Errorf("node %d received wrong payload for %s", node.Num, id)
				}
			}
		}
		if !t.Failed() {
			t.Logf("delivered %d message(s) to %d/%d peers", rc.Workload.NumMessages, len(topology.Nodes)-1, len(topology.Nodes)-1)
		}
	})
}
