package sim

import "sort"

// NodeSpec describes a node in the simulation topology.
type NodeSpec struct {
	Num            int `json:"num" yaml:"num"`
	UploadBWMbps   int `json:"upload_bw_mbps" yaml:"upload_bw_mbps"`
	DownloadBWMbps int `json:"download_bw_mbps" yaml:"download_bw_mbps"`
}

// EdgeSpec describes a directed edge in the topology.
type EdgeSpec struct {
	Source    int `json:"source" yaml:"source"`
	Target    int `json:"target" yaml:"target"`
	LatencyMs int `json:"latency_ms" yaml:"latency_ms"`
}

// Topology describes the network topology for simulation.
type Topology struct {
	Nodes []NodeSpec `json:"nodes" yaml:"nodes"`
	Edges []EdgeSpec `json:"edges" yaml:"edges"`
}

// EdgesMap returns a deduplicated undirected peer map for the topology.
func (t Topology) EdgesMap() map[int][]int {
	peers := make(map[int][]int)
	seen := make(map[int]map[int]struct{})
	for _, e := range t.Edges {
		addPeer(peers, seen, e.Source, e.Target)
		addPeer(peers, seen, e.Target, e.Source)
	}
	for nodeNum := range peers {
		sort.Ints(peers[nodeNum])
	}
	return peers
}

func addPeer(peers map[int][]int, seen map[int]map[int]struct{}, source, target int) {
	if source == target {
		return
	}
	if seen[source] == nil {
		seen[source] = make(map[int]struct{})
	}
	if _, ok := seen[source][target]; ok {
		return
	}
	seen[source][target] = struct{}{}
	peers[source] = append(peers[source], target)
}

// AutoWarmupBytes returns per-peer warmup sizes derived from topology BDP.
func AutoWarmupBytes(t Topology, messageSize int) map[int]map[int]int {
	nodeByNum := make(map[int]NodeSpec, len(t.Nodes))
	for _, n := range t.Nodes {
		nodeByNum[n.Num] = n
	}

	warmup := make(map[int]map[int]int)
	for _, e := range t.Edges {
		src, ok := nodeByNum[e.Source]
		if !ok {
			continue
		}
		dst, ok := nodeByNum[e.Target]
		if !ok {
			continue
		}

		rttSeconds := float64(e.LatencyMs*2) / 1000
		forward := bdpBytes(min(src.UploadBWMbps, dst.DownloadBWMbps), rttSeconds)
		reverse := bdpBytes(min(dst.UploadBWMbps, src.DownloadBWMbps), rttSeconds)
		bytes := clampWarmupBytes(max(forward, reverse)*2, messageSize)
		setWarmupBytes(warmup, e.Source, e.Target, bytes)
		setWarmupBytes(warmup, e.Target, e.Source, bytes)
	}
	return warmup
}

func bdpBytes(mbps int, rttSeconds float64) int {
	return int(float64(mbps) * 1_000_000 / 8 * rttSeconds)
}

func clampWarmupBytes(bytes, messageSize int) int {
	maxBytes := min(messageSize, 4<<20)
	if maxBytes <= 0 {
		return 0
	}
	minBytes := min(64<<10, maxBytes)
	if bytes < minBytes {
		return minBytes
	}
	if bytes > maxBytes {
		return maxBytes
	}
	return bytes
}

func setWarmupBytes(warmup map[int]map[int]int, source, target, bytes int) {
	if warmup[source] == nil {
		warmup[source] = make(map[int]int)
	}
	if warmup[source][target] < bytes {
		warmup[source][target] = bytes
	}
}
