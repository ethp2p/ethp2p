package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/sim"
)

var (
	configFile = flag.String("config", "", "Path to run config YAML")
	nodeNum    = flag.Int("node-num", 0, "This node's number")
)

func main() {
	flag.Parse()

	if *configFile == "" {
		log.Fatal("--config is required")
	}

	rc, err := sim.LoadRunConfig(*configFile)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	os.Setenv("LOG_LEVEL", rc.Simulation.LogLevel)

	topo, err := rc.LoadTopology()
	if err != nil {
		log.Fatalf("failed to load topology: %v", err)
	}
	peers := topo.EdgesMap()[*nodeNum]

	scenario, err := rc.NewScenario("shadow", nil)
	if err != nil {
		log.Fatalf("failed to create scenario: %v", err)
	}
	defer scenario.Close()

	// When trace_file is configured, each Shadow node writes its events
	// to a local file. Shadow sets CWD to shadow.data/hosts/nodeN/, so
	// the file lands in the per-node output directory. After simulation,
	// simctl merges all per-node event files into a single .bctrace.
	if rc.Simulation.TraceFile != "" {
		drv := scenario.Driver.(*sim.ShadowDriver)

		f, err := os.Create("events.ndjson")
		if err != nil {
			log.Fatalf("failed to create trace file: %v", err)
		}
		defer f.Close()

		nodes := make([]string, len(topo.Nodes))
		for i, ns := range topo.Nodes {
			nodes[i] = fmt.Sprintf("n%d", ns.Num)
		}
		cfgJSON, _ := json.Marshal(rc.Strategy)
		traceOpts, err := rc.BuildTraceHeaderOptions(topo)
		if err != nil {
			log.Fatalf("failed to build trace header options: %v", err)
		}
		tw, err := sim.NewTraceWriterWithOptions(f, time.Now(), nodes, topo, cfgJSON, traceOpts)
		if err != nil {
			log.Fatalf("failed to create trace writer: %v", err)
		}
		drv.TraceWriter = tw
		defer tw.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), rc.Workload.StopTime())
	defer cancel()

	node, err := scenario.NewNode(ctx, *nodeNum)
	if err != nil {
		log.Fatalf("failed to create node: %v", err)
	}
	defer node.Close()

	collector := scenario.NewLogCollector()
	go collector.Run(ctx)

	// Subscribe to publish (origin) and receive (relay) events so we
	// can snapshot-and-reset stats per message after a quiescence period.
	var eventCh chan sim.NodeEvent
	if *nodeNum == 0 {
		ch := make(chan sim.NodeEvent, 100)
		scenario.PushEventsTo(ch, nil, nil)
		eventCh = ch
	} else {
		ch := make(chan sim.NodeEvent, 100)
		scenario.PushEventsTo(nil, ch, nil)
		eventCh = ch
	}

	drv, _ := scenario.Driver.(*sim.ShadowDriver)
	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		for ev := range eventCh {
			time.Sleep(10 * time.Second)
			logMessageStats(scenario, drv, node, *nodeNum, ev.MessageID)
		}
	}()

	scenario.Start()
	scenario.RunNode(ctx, node, peers, rc.Workload.PublishWait())

	time.Sleep(10 * time.Second)
	close(eventCh)
	<-statsDone
}

func logMessageStats(scenario *sim.Scenario, drv *sim.ShadowDriver, node sim.Node, nodeNum int, messageID string) {
	sent, recv := node.ResetBandwidthStats()
	args := []any{
		"node-num", nodeNum,
		"message-id", messageID,
		"sentBytesTotal", sent,
		"receivedBytesTotal", recv,
	}
	if drv != nil {
		if obs := drv.Observer(); obs != nil {
			snap := obs.ResetMessage(broadcast.MessageID(messageID))
			args = append(args,
				"accepted", snap.Chunks.Accepted,
				"redundant", snap.Chunks.Redundant,
				"decoding", snap.Chunks.Decoding,
				"surplus", snap.Chunks.Surplus,
			)
		}
	}
	scenario.Logger.Info("message stats", args...)
}
