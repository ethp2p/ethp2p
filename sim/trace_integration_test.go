package sim

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/broadcast"
)

func TestTracingObserver_ProducesValidTrace(t *testing.T) {
	var buf bytes.Buffer
	t0 := time.Now()

	nodes := []string{"n0", "n1", "n2", "n3"}
	topo := Topology{
		Nodes: []NodeSpec{
			{Num: 0, UploadBWMbps: 50, DownloadBWMbps: 50},
			{Num: 1, UploadBWMbps: 50, DownloadBWMbps: 50},
			{Num: 2, UploadBWMbps: 50, DownloadBWMbps: 50},
			{Num: 3, UploadBWMbps: 50, DownloadBWMbps: 50},
		},
		Edges: []EdgeSpec{
			{Source: 0, Target: 1, LatencyMs: 50},
			{Source: 0, Target: 2, LatencyMs: 50},
			{Source: 1, Target: 3, LatencyMs: 50},
			{Source: 2, Target: 3, LatencyMs: 50},
		},
	}
	config := json.RawMessage(`{"strategy":"rs","dataShards":8,"parityShards":4}`)

	tw, err := NewTraceWriter(&buf, t0, nodes, topo, config)
	if err != nil {
		t.Fatal(err)
	}

	obs := make([]*TracingObserver, 4)
	for i := range obs {
		obs[i] = NewTracingObserver(i, tw)
	}

	channelID := broadcast.ChannelID("broadcast")
	msgID := broadcast.MessageID("msg-0")

	// Node 0: origin session.
	obs[0].OnSessionStarted(channelID, msgID, broadcast.SessionRoleOrigin)
	obs[0].OnChunkSent("1", channelID, msgID, 1024)
	obs[0].OnChunkSent("2", channelID, msgID, 1024)

	// Nodes 1,2: relay sessions.
	obs[1].OnPreambleOpened("0", channelID, msgID)
	obs[1].OnSessionStarted(channelID, msgID, broadcast.SessionRoleRelay)
	obs[2].OnPreambleOpened("0", channelID, msgID)
	obs[2].OnSessionStarted(channelID, msgID, broadcast.SessionRoleRelay)

	// Nodes receive chunks with progress.
	for i := 1; i <= 8; i++ {
		obs[1].OnChunkRcvd("0", channelID, msgID, broadcast.VerdictAccepted)
		obs[1].OnStrategyProgress(channelID, msgID, i, 8)
		obs[2].OnChunkRcvd("0", channelID, msgID, broadcast.VerdictAccepted)
		obs[2].OnStrategyProgress(channelID, msgID, i, 8)
	}

	// Routing updates.
	obs[1].OnRoutingUpdate("0", channelID, msgID)
	obs[2].OnRoutingUpdate("0", channelID, msgID)

	// Relays forward to node 3.
	obs[1].OnChunkSent("3", channelID, msgID, 1024)
	obs[2].OnChunkSent("3", channelID, msgID, 1024)

	obs[3].OnPreambleOpened("1", channelID, msgID)
	obs[3].OnSessionStarted(channelID, msgID, broadcast.SessionRoleRelay)
	for i := 1; i <= 8; i++ {
		obs[3].OnChunkRcvd("1", channelID, msgID, broadcast.VerdictAccepted)
		obs[3].OnStrategyProgress(channelID, msgID, i, 8)
	}

	// All receivers decode.
	obs[1].OnSessionDecoded(channelID, msgID, 50*time.Millisecond)
	obs[2].OnSessionDecoded(channelID, msgID, 55*time.Millisecond)
	obs[3].OnSessionDecoded(channelID, msgID, 80*time.Millisecond)

	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) < 3 {
		t.Fatalf("expected at least 3 lines, got %d", len(lines))
	}

	// Verify header.
	var hdr traceHeader
	if err := json.Unmarshal([]byte(lines[0]), &hdr); err != nil {
		t.Fatalf("invalid header: %v", err)
	}
	if hdr.V != 1 || len(hdr.Nodes) != 4 || len(hdr.Topology.Edges) != 4 {
		t.Fatalf("unexpected header: v=%d nodes=%d edges=%d", hdr.V, len(hdr.Nodes), len(hdr.Topology.Edges))
	}

	// Verify footer.
	var ftr traceFooter
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &ftr); err != nil {
		t.Fatalf("invalid footer: %v", err)
	}
	if !ftr.End {
		t.Fatal("footer.end should be true")
	}

	// Verify events.
	eventCodes := make(map[string]int)
	for _, line := range lines[1 : len(lines)-1] {
		var ev []any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("invalid event: %v\nline: %s", err, line)
		}
		if len(ev) < 3 {
			t.Fatalf("event too short: %v", ev)
		}
		code, ok := ev[2].(string)
		if !ok {
			t.Fatalf("event code not string: %v", ev[2])
		}
		eventCodes[code]++
	}

	// Check expected event types.
	checks := map[string]int{"ss": 4, "sd": 3, "po": 3, "ru": 2}
	for code, want := range checks {
		if got := eventCodes[code]; got != want {
			t.Errorf("event %q: got %d, want %d", code, got, want)
		}
	}
	for _, code := range []string{"cs", "cr", "sp"} {
		if eventCodes[code] == 0 {
			t.Errorf("expected %q events, got none", code)
		}
	}

	t.Logf("trace: %d event lines, codes: %v", len(lines)-2, eventCodes)
}
