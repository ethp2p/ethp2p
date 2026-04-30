package sim

import "testing"

func TestEdgesMapIsUndirectedAndDeduplicated(t *testing.T) {
	topo := Topology{
		Edges: []EdgeSpec{
			{Source: 0, Target: 1},
			{Source: 1, Target: 0},
			{Source: 1, Target: 2},
		},
	}

	peers := topo.EdgesMap()
	assertIntsEqual(t, peers[0], []int{1})
	assertIntsEqual(t, peers[1], []int{0, 2})
	assertIntsEqual(t, peers[2], []int{1})
}

func TestAutoWarmupBytesUsesTopologyBDP(t *testing.T) {
	topo := Topology{
		Nodes: []NodeSpec{
			{Num: 0, UploadBWMbps: 50, DownloadBWMbps: 100},
			{Num: 1, UploadBWMbps: 10, DownloadBWMbps: 50},
		},
		Edges: []EdgeSpec{{Source: 0, Target: 1, LatencyMs: 50}},
	}

	warmup := AutoWarmupBytes(topo, 2<<20)
	want := 1_250_000
	if warmup[0][1] != want {
		t.Fatalf("warmup[0][1]=%d, want %d", warmup[0][1], want)
	}
	if warmup[1][0] != want {
		t.Fatalf("warmup[1][0]=%d, want %d", warmup[1][0], want)
	}
}

func TestAutoWarmupBytesClampsToMessageSize(t *testing.T) {
	topo := Topology{
		Nodes: []NodeSpec{
			{Num: 0, UploadBWMbps: 1, DownloadBWMbps: 1},
			{Num: 1, UploadBWMbps: 1, DownloadBWMbps: 1},
		},
		Edges: []EdgeSpec{{Source: 0, Target: 1, LatencyMs: 1}},
	}

	warmup := AutoWarmupBytes(topo, 32<<10)
	if warmup[0][1] != 32<<10 {
		t.Fatalf("warmup[0][1]=%d, want %d", warmup[0][1], 32<<10)
	}
}

func assertIntsEqual(t *testing.T, got, want []int) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}
