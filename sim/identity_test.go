package sim

import (
	"testing"
)

// TestBuildTraceHeaderOptionsPeerIDs covers the mapping consumers use to resolve
// trace events back to nodes. ethp2p reports transport identities rather than
// node numbers, so the header must carry them or events are unresolvable.
func TestBuildTraceHeaderOptionsPeerIDs(t *testing.T) {
	topo := Topology{
		Nodes: []NodeSpec{
			{Num: 0, UploadBWMbps: 50, DownloadBWMbps: 50},
			{Num: 1, UploadBWMbps: 50, DownloadBWMbps: 50},
			{Num: 2, UploadBWMbps: 50, DownloadBWMbps: 50},
		},
		Edges: []EdgeSpec{{Source: 0, Target: 1, LatencyMs: 50}},
	}

	for _, strategy := range []string{"gossipsub", "RS"} {
		t.Run(strategy, func(t *testing.T) {
			rc := &RunConfig{Strategy: Strategy{Name: strategy}}
			opts, err := rc.BuildTraceHeaderOptions(topo)
			if err != nil {
				t.Fatal(err)
			}
			if len(opts.PeerIDs) != len(topo.Nodes) {
				t.Fatalf("got %d peer IDs for %d nodes", len(opts.PeerIDs), len(topo.Nodes))
			}
			seen := map[string]int{}
			for i, id := range opts.PeerIDs {
				if id == "" {
					t.Fatalf("peer ID %d is empty", i)
				}
				if prev, dup := seen[id]; dup {
					t.Fatalf("nodes %d and %d share peer ID %q", prev, i, id)
				}
				seen[id] = i
			}
		})
	}
}

// TestNodeIdentityIsDeterministic keeps experiments reproducible: a re-run must
// produce the same identities, or traces from two runs cannot be compared.
func TestNodeIdentityIsDeterministic(t *testing.T) {
	for _, num := range []int{0, 1, 7} {
		key1, id1, err := nodeIdentity(num)
		if err != nil {
			t.Fatal(err)
		}
		_, id2, err := nodeIdentity(num)
		if err != nil {
			t.Fatal(err)
		}
		if id1 != id2 {
			t.Fatalf("node %d identity is not deterministic", num)
		}
		if key1.Public().PeerID() != id1 {
			t.Fatalf("node %d peer ID does not match its key", num)
		}
	}
}
