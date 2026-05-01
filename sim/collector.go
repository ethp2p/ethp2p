package sim

import (
	"context"
	"log/slog"
	"time"

	"github.com/ethp2p/ethp2p/broadcast"
)

// NodeEvent represents a publish or receive event from a node.
type NodeEvent struct {
	MessageID string
	NodeNum   int
	At        time.Time
}

// BandwidthEvent represents a periodic bandwidth usage sample from a node.
type BandwidthEvent struct {
	NodeNum            int
	SentBps            int
	ReceivedBps        int
	SentBytesTotal     int
	ReceivedBytesTotal int
	At                 time.Time
}

// ScenarioStats holds collected publish/receive data from a simulation run.
type ScenarioStats struct {
	PublishedMessages map[broadcast.MessageID]struct{}
	PublishedAt       map[broadcast.MessageID]time.Time

	ReceivedMessages map[int]map[broadcast.MessageID]struct{}      // nodeNum → messageID
	ReceivedAt       map[int]map[broadcast.MessageID]time.Time     // nodeNum → messageID → time
	ReceivedLatency  map[int]map[broadcast.MessageID]time.Duration // nodeNum → messageID → latency from PublishedAt

	OriginBytesSent map[int]int // nodeNum → total bytes sent as origin
	RelayBytesSent  map[int]int // nodeNum → total bytes sent as relay

	TransportBytesSent     map[int]int // nodeNum → total bytes sent (transport level)
	TransportBytesReceived map[int]int // nodeNum → total bytes received (transport level)

	ChunksPerNode map[int]ChunkStats // nodeNum → chunk reception verdicts
}

// LogCollector reads Published, Received, and Bandwidth events from a
// Scenario and logs them. Call Run in a goroutine before starting the node.
type LogCollector struct {
	logger    *slog.Logger
	published chan NodeEvent
	received  chan NodeEvent
	bandwidth chan BandwidthEvent
}

func (c *LogCollector) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-c.published:
			c.logger.Info("Published Message", "node-num", ev.NodeNum, "message-id", ev.MessageID, "at", ev.At.UnixMilli())
		case ev := <-c.received:
			c.logger.Info("Received Message", "node-num", ev.NodeNum, "message-id", ev.MessageID, "at", ev.At.UnixMilli())
		case ev := <-c.bandwidth:
			c.logger.Info("bandwidth usage", "node-num", ev.NodeNum,
				"sentbps", ev.SentBps, "receivedbps", ev.ReceivedBps,
				"sentBytesTotal", ev.SentBytesTotal, "receivedBytesTotal", ev.ReceivedBytesTotal,
				"at", ev.At.UnixMilli())
		}
	}
}

// StatsCollector collects Published and Received events from a Scenario into
// a ScenarioStats. Call Run in a goroutine; it blocks until the context is
// cancelled and then returns the collected stats.
type StatsCollector struct {
	published chan NodeEvent
	received  chan NodeEvent
}

func (c *StatsCollector) Run(ctx context.Context) ScenarioStats {
	stats := ScenarioStats{
		PublishedMessages: make(map[broadcast.MessageID]struct{}),
		PublishedAt:       make(map[broadcast.MessageID]time.Time),
		ReceivedMessages:  make(map[int]map[broadcast.MessageID]struct{}),
		ReceivedAt:        make(map[int]map[broadcast.MessageID]time.Time),
		ReceivedLatency:   make(map[int]map[broadcast.MessageID]time.Duration),
	}
	for {
		select {
		case <-ctx.Done():
			return stats
		case ev := <-c.published:
			stats.PublishedMessages[broadcast.MessageID(ev.MessageID)] = struct{}{}
			stats.PublishedAt[broadcast.MessageID(ev.MessageID)] = ev.At
		case ev := <-c.received:
			mid := broadcast.MessageID(ev.MessageID)
			if _, ok := stats.ReceivedMessages[ev.NodeNum]; !ok {
				stats.ReceivedMessages[ev.NodeNum] = make(map[broadcast.MessageID]struct{})
				stats.ReceivedAt[ev.NodeNum] = make(map[broadcast.MessageID]time.Time)
				stats.ReceivedLatency[ev.NodeNum] = make(map[broadcast.MessageID]time.Duration)
			}
			stats.ReceivedMessages[ev.NodeNum][mid] = struct{}{}
			stats.ReceivedAt[ev.NodeNum][mid] = ev.At
			if pubTime, ok := stats.PublishedAt[mid]; ok {
				stats.ReceivedLatency[ev.NodeNum][mid] = ev.At.Sub(pubTime)
			}
		}
	}
}
