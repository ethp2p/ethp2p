package sim

import (
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/marcopolo/simnet"
)

// SimnetDriver implements Driver for simnet-based testing.
type SimnetDriver struct {
	Strategy    StrategyFunc
	Topology    Topology
	TraceWriter *TraceWriter

	mu        sync.RWMutex
	nodeSpecs map[int]NodeSpec
	latency   map[int]map[int]time.Duration
	ipToNode  map[string]int
	nodes     map[int]Node
	observers map[int]*Observer
	simnet    *simnet.Simnet
	once      sync.Once
}

var _ Driver = &SimnetDriver{}

func (s *SimnetDriver) init() {
	s.once.Do(func() {
		s.nodeSpecs = make(map[int]NodeSpec)
		for _, ns := range s.Topology.Nodes {
			s.nodeSpecs[ns.Num] = ns
		}
		s.latency = make(map[int]map[int]time.Duration)
		for _, e := range s.Topology.Edges {
			if _, ok := s.latency[e.Source]; !ok {
				s.latency[e.Source] = make(map[int]time.Duration)
			}
			s.latency[e.Source][e.Target] = time.Duration(e.LatencyMs) * time.Millisecond
		}
		for ss, m := range s.latency {
			for e, d := range m {
				if _, ok := s.latency[e]; !ok {
					s.latency[e] = make(map[int]time.Duration)
				}
				if _, ok := s.latency[e][ss]; !ok {
					s.latency[e][ss] = d
				}
			}
		}

		s.nodes = make(map[int]Node)
		s.observers = make(map[int]*Observer)
		s.ipToNode = make(map[string]int)
		s.simnet = &simnet.Simnet{
			LatencyFunc: s.latencyFunc,
		}
	})
}

// NewNode creates a node using simnet for network simulation.
func (s *SimnetDriver) NewNode(nodeNum int, logger *slog.Logger) (Node, error) {
	s.init()
	ip := simnet.IntToPublicIPv4(nodeNum)
	ns := s.nodeSpecs[nodeNum]
	ls := simnet.NodeBiDiLinkSettings{
		Uplink:   simnet.LinkSettings{BitsPerSecond: ns.UploadBWMbps * simnet.Mibps},
		Downlink: simnet.LinkSettings{BitsPerSecond: ns.DownloadBWMbps * simnet.Mibps},
	}
	udpAddr := &net.UDPAddr{IP: ip, Port: DefaultListenPort}
	conn := s.simnet.NewEndpoint(udpAddr, ls)

	var obs broadcast.Observer
	var statsObs *Observer
	if s.TraceWriter != nil {
		to := NewTracingObserver(ns.Num, s.TraceWriter)
		statsObs = to.Observer
		obs = to
	} else {
		statsObs = NewObserver()
		obs = statsObs
	}
	nd, err := s.Strategy(ns.Num, conn, logger, obs, s.TraceWriter)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.nodes[ns.Num] = nd
	s.observers[ns.Num] = statsObs
	s.ipToNode[string(ip)] = ns.Num
	s.mu.Unlock()

	return nd, nil
}

func (s *SimnetDriver) latencyFunc(p *simnet.Packet) time.Duration {
	senderIP := p.From.(*net.UDPAddr).IP
	dstIP := p.To.(*net.UDPAddr).IP

	s.mu.RLock()
	src := s.ipToNode[string(senderIP)]
	dst := s.ipToNode[string(dstIP)]
	s.mu.RUnlock()

	if _, ok := s.latency[src]; !ok {
		panic("no edges for source")
	}
	d, ok := s.latency[src][dst]
	if !ok {
		panic("no edges for destination")
	}
	return d
}

// Start initializes the simnet simulation.
func (s *SimnetDriver) Start() {
	s.simnet.Start()
}

// Close shuts down the simnet simulation.
func (s *SimnetDriver) Close() error {
	s.simnet.Close()
	return nil
}

// BandwidthByRole returns per-node origin and relay byte counts from the
// Observers injected into each node.
func (s *SimnetDriver) BandwidthByRole() (origin, relay map[int]int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	origin = make(map[int]int, len(s.observers))
	relay = make(map[int]int, len(s.observers))
	for num, obs := range s.observers {
		o, r := obs.Stats()
		origin[num] = o
		relay[num] = r
	}
	return origin, relay
}

// TransportStats returns per-node bytes sent and received at the transport
// (QUIC) level. Must be called before nodes are closed.
func (s *SimnetDriver) TransportStats() (sent, received map[int]int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sent = make(map[int]int, len(s.nodes))
	received = make(map[int]int, len(s.nodes))
	for num, nd := range s.nodes {
		s, r := nd.BandwidthStats()
		sent[num] = s
		received[num] = r
	}
	return sent, received
}

// ChunkStatsByNode returns per-node chunk reception verdicts.
func (s *SimnetDriver) ChunkStatsByNode() map[int]ChunkStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[int]ChunkStats, len(s.observers))
	for num, obs := range s.observers {
		out[num] = obs.Chunks()
	}
	return out
}

// NodeAddr returns the network address for a node.
func (s *SimnetDriver) NodeAddr(nodeNum int) net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()

	node := s.nodes[nodeNum]
	return node.Addr()
}
