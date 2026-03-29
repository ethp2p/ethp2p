package sim

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestTraceWriter_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	t0 := time.Date(2025, 3, 9, 12, 0, 0, 0, time.UTC)

	nodes := []string{"n0", "n1", "n2"}
	topo := Topology{
		Nodes: []NodeSpec{
			{Num: 0, UploadBWMbps: 50, DownloadBWMbps: 50},
			{Num: 1, UploadBWMbps: 50, DownloadBWMbps: 50},
			{Num: 2, UploadBWMbps: 50, DownloadBWMbps: 50},
		},
		Edges: []EdgeSpec{
			{Source: 0, Target: 1, LatencyMs: 50},
			{Source: 0, Target: 2, LatencyMs: 50},
			{Source: 1, Target: 2, LatencyMs: 50},
		},
	}
	config := json.RawMessage(`{"strategy":"rs"}`)

	tw, err := NewTraceWriter(&buf, t0, nodes, topo, config)
	if err != nil {
		t.Fatal(err)
	}

	// Write 4 events with increasing timestamps.
	tw.WriteEvent(t0.Add(100*time.Microsecond), 0, "ss", "broadcast", "msg-0", 0)
	tw.WriteEvent(t0.Add(200*time.Microsecond), 0, "cs", "1", "broadcast", "msg-0", 1420)
	tw.WriteEvent(t0.Add(500*time.Microsecond), 1, "cr", "0", "broadcast", "msg-0", 0)
	tw.WriteEvent(t0.Add(1000*time.Microsecond), 1, "sp", "broadcast", "msg-0", 4, 8)

	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 6 { // header + 4 events + footer
		t.Fatalf("expected 6 lines, got %d:\n%s", len(lines), buf.String())
	}

	// Verify header.
	var hdr traceHeader
	if err := json.Unmarshal([]byte(lines[0]), &hdr); err != nil {
		t.Fatalf("parse header: %v", err)
	}
	if hdr.V != 1 {
		t.Fatalf("expected version 1, got %d", hdr.V)
	}
	if len(hdr.Nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(hdr.Nodes))
	}

	// Verify first event is a JSON array with correct event code.
	var ev []any
	if err := json.Unmarshal([]byte(lines[1]), &ev); err != nil {
		t.Fatalf("parse event: %v", err)
	}
	if len(ev) < 3 {
		t.Fatalf("event too short: %v", ev)
	}
	if ev[2] != "ss" {
		t.Fatalf("expected event code 'ss', got %v", ev[2])
	}
	// ts should be 100 (microseconds)
	if ts, ok := ev[0].(float64); !ok || ts != 100 {
		t.Fatalf("expected ts=100, got %v", ev[0])
	}

	// Verify footer.
	var ftr traceFooter
	if err := json.Unmarshal([]byte(lines[5]), &ftr); err != nil {
		t.Fatalf("parse footer: %v", err)
	}
	if !ftr.End {
		t.Fatal("expected footer.end=true")
	}
	if len(ftr.Index) == 0 {
		t.Fatal("expected non-empty index")
	}
}
