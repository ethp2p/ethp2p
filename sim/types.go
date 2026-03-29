package sim

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

// EdgesMap returns a map from node number to its outgoing peer node numbers.
func (t Topology) EdgesMap() map[int][]int {
	peers := make(map[int][]int)
	for _, e := range t.Edges {
		peers[e.Source] = append(peers[e.Source], e.Target)
	}
	return peers
}
