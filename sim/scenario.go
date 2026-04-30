package sim

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Scenario orchestrates simulation test scenarios.
type Scenario struct {
	NumMessages           int
	MessageSize           int
	Driver                Driver
	BandwidthLogFrequency time.Duration
	WarmupBytes           map[int]map[int]int
	WarmupDrain           time.Duration
	Logger                *slog.Logger

	mu             sync.RWMutex
	publishChans   []chan NodeEvent
	receiveChans   []chan NodeEvent
	bandwidthChans []chan BandwidthEvent
	ctx            context.Context
	cancel         context.CancelFunc
	once           sync.Once
	wg             sync.WaitGroup
}

func (s *Scenario) init() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

// PushEventsTo registers channels to receive scenario events.
// Nil channels are ignored. Each call adds new channels; multiple
// consumers each get a copy of every event.
func (s *Scenario) PushEventsTo(published, received chan NodeEvent, bandwidth chan BandwidthEvent) {
	s.once.Do(s.init)
	s.mu.Lock()
	defer s.mu.Unlock()
	if published != nil {
		s.publishChans = append(s.publishChans, published)
	}
	if received != nil {
		s.receiveChans = append(s.receiveChans, received)
	}
	if bandwidth != nil {
		s.bandwidthChans = append(s.bandwidthChans, bandwidth)
	}
}

// NewNode creates a node for the given node number.
func (s *Scenario) NewNode(ctx context.Context, nodeNum int) (Node, error) {
	s.once.Do(s.init)
	logger := s.Logger
	if logger == nil {
		logger = newNodeLogger(nodeNum)
	}
	return s.Driver.NewNode(nodeNum, logger)
}

// Start begins the simulation.
func (s *Scenario) Start() {
	s.Driver.Start()
}

// RunNode starts a node and handles publishing/receiving based on node number.
func (s *Scenario) RunNode(ctx context.Context, node Node, peers []int, publishWait time.Duration) {
	nodeNum := node.NodeNum()
	nodeCtx := context.WithoutCancel(ctx)
	node.Start(nodeCtx)
	if s.BandwidthLogFrequency != 0 {
		s.wg.Go(func() { s.reportNodeBandwidth(s.ctx, s.BandwidthLogFrequency, nodeNum, node) })
	}
	time.Sleep(10 * time.Second)

	ch := make(chan struct{}, 20)
	var wg sync.WaitGroup
	for _, p := range peers {
		if nodeNum > p {
			continue
		}
		ch <- struct{}{}
		wg.Go(func() {
			defer func() { <-ch }()
			addr := s.Driver.NodeAddr(p)
			err := node.DialPeer(nodeCtx, p, addr)
			if err != nil {
				panic(fmt.Sprintf("failed to dial: %d %s: %s", p, addr, err.Error()))
			}
		})
	}
	wg.Wait()
	readyCtx, cancelReady := context.WithTimeout(ctx, 30*time.Second)
	defer cancelReady()
	if err := node.AwaitReady(readyCtx, peers); err != nil {
		panic(fmt.Sprintf("node %d failed to become ready: %s", nodeNum, err.Error()))
	}
	if s.WarmupBytes != nil {
		s.warmupPeers(readyCtx, node, peers)
		if err := node.AwaitWarmup(readyCtx, peers); err != nil {
			panic(fmt.Sprintf("node %d failed to complete warmup: %s", nodeNum, err.Error()))
		}
		if nodeNum == 0 && s.WarmupDrain > 0 {
			if err := waitContext(ctx, s.WarmupDrain); err != nil {
				return
			}
		}
	}
	node.ResetBandwidthStats()
	if nodeNum == 0 {
		s.publishMessages(nodeCtx, nodeNum, node, publishWait)
	} else {
		s.receiveMessages(nodeCtx, nodeNum, node)
	}
}

func (s *Scenario) warmupPeers(ctx context.Context, node Node, peers []int) {
	nodeNum := node.NodeNum()
	ch := make(chan struct{}, 20)
	var wg sync.WaitGroup
	for _, p := range peers {
		if nodeNum > p {
			continue
		}
		ch <- struct{}{}
		wg.Go(func() {
			defer func() { <-ch }()
			if err := node.WarmupPeer(ctx, p, s.WarmupBytes[nodeNum][p]); err != nil {
				panic(fmt.Sprintf("failed to warm peer: %d: %s", p, err.Error()))
			}
		})
	}
	wg.Wait()
}

func (s *Scenario) publishMessages(ctx context.Context, nodeNum int, node Node, publishWait time.Duration) {
	for i := range s.NumMessages {
		messageID := fmt.Sprintf("msg-%d", i)
		data := make([]byte, s.MessageSize)
		_, err := rand.Read(data)
		if err != nil {
			return
		}

		node.Publish(messageID, data)

		now := time.Now()
		nextPublishAt := time.Now().Add(publishWait)
		ev := NodeEvent{MessageID: messageID, Data: data, NodeNum: nodeNum, At: now}
		s.mu.RLock()
		chans := s.publishChans
		s.mu.RUnlock()
		for _, ch := range chans {
			select {
			case ch <- ev:
			case <-ctx.Done():
				return
			case <-s.ctx.Done():
				return
			}
		}
		delay := time.Until(nextPublishAt)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Scenario) receiveMessages(ctx context.Context, nodeNum int, node Node) {
	for range s.NumMessages {
		messageID, data, err := node.Receive(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			panic(err)
		}
		now := time.Now()
		ev := NodeEvent{MessageID: messageID, Data: data, NodeNum: nodeNum, At: now}
		s.mu.RLock()
		chans := s.receiveChans
		s.mu.RUnlock()
		for _, ch := range chans {
			select {
			case ch <- ev:
			case <-ctx.Done():
				return
			case <-s.ctx.Done():
				return
			}
		}
	}
}

// Close shuts down the scenario.
func (s *Scenario) Close() {
	s.Driver.Close()
	s.cancel()
	s.wg.Wait()
}

func (s *Scenario) reportNodeBandwidth(ctx context.Context, d time.Duration, nodeNum int, n Node) {
	t := time.NewTicker(d)
	prevSent, prevReceived := 0, 0
	for {
		select {
		case <-t.C:
			sent, received := n.BandwidthStats()
			ds, dr := sent-prevSent, received-prevReceived
			prevSent, prevReceived = sent, received
			sbps, rbps := int(float64(ds*8)/d.Seconds()), int(float64((dr*8))/d.Seconds())
			if sbps < 10 && rbps < 10 {
				continue
			}
			ev := BandwidthEvent{
				NodeNum:            nodeNum,
				SentBps:            sbps,
				ReceivedBps:        rbps,
				SentBytesTotal:     sent,
				ReceivedBytesTotal: received,
				At:                 time.Now(),
			}
			s.mu.RLock()
			chans := s.bandwidthChans
			s.mu.RUnlock()
			for _, ch := range chans {
				select {
				case ch <- ev:
				case <-ctx.Done():
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Scenario) NewLogCollector() *LogCollector {
	if s.Logger == nil {
		panic("Scenario.Logger must be set before calling NewLogCollector")
	}
	published := make(chan NodeEvent, 100)
	received := make(chan NodeEvent, 100)
	bandwidth := make(chan BandwidthEvent, 100)
	s.PushEventsTo(published, received, bandwidth)
	return &LogCollector{
		logger:    s.Logger,
		published: published,
		received:  received,
		bandwidth: bandwidth,
	}
}

func (s *Scenario) NewStatsCollector() *StatsCollector {
	published := make(chan NodeEvent, 100)
	received := make(chan NodeEvent, 100)
	s.PushEventsTo(published, received, nil)
	return &StatsCollector{
		published: published,
		received:  received,
	}
}

// RunSimnetScenario runs a complete simnet scenario: creates nodes,
// wires them according to the topology, and returns collected stats.
func RunSimnetScenario(ctx context.Context, s *Scenario, publishWait time.Duration) (ScenarioStats, error) {
	topology := s.Driver.(*SimnetDriver).Topology
	nodes := make(map[int]Node)
	for _, ns := range topology.Nodes {
		nd, err := s.NewNode(ctx, ns.Num)
		if err != nil {
			return ScenarioStats{}, fmt.Errorf("new node failed: %w", err)
		}
		nodes[ns.Num] = nd
		defer nd.Close()
	}

	var stats ScenarioStats
	collector := s.NewStatsCollector()
	var wg sync.WaitGroup
	wg.Go(func() {
		stats = collector.Run(ctx)
	})

	peers := topology.EdgesMap()
	s.Start()
	var nodeWg sync.WaitGroup
	for _, nd := range nodes {
		nodeWg.Go(func() { s.RunNode(ctx, nd, peers[nd.NodeNum()], publishWait) })
	}
	nodeWg.Wait()

	<-ctx.Done()
	wg.Wait()

	if sh, ok := s.Driver.(*SimnetDriver); ok {
		origin, relay := sh.BandwidthByRole()
		stats.OriginBytesSent = origin
		stats.RelayBytesSent = relay
		stats.TransportBytesSent, stats.TransportBytesReceived = sh.TransportStats()
		stats.ChunksPerNode = sh.ChunkStatsByNode()
	}

	return stats, nil
}
