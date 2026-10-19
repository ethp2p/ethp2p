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
	tw, err := NewTraceWriter(&buf, time.Now(), []string{"n0"}, Topology{}, json.RawMessage(`{"strategy":"rs"}`))
	if err != nil {
		t.Fatal(err)
	}
	obs := NewTracingObserver(0, tw)
	const channel = broadcast.ChannelID("broadcast")
	const message = broadcast.MessageID("msg-0")

	obs.OnSessionStarted(channel, message, broadcast.SessionRoleRelay)
	obs.OnPreambleOpened("peer", channel, message)
	obs.OnRoutingUpdate("peer", channel, message)
	obs.OnChunkSent("peer", channel, message, 1024)
	obs.OnChunkRcvd("peer", channel, message, broadcast.VerdictAccepted)
	obs.OnStrategyProgress(channel, message, 1, 8)
	obs.OnSessionDecoded(channel, message)
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 9 { // header + 7 events + footer
		t.Fatalf("trace lines = %d, want 9:\n%s", len(lines), buf.String())
	}
	var header traceHeader
	if err := json.Unmarshal([]byte(lines[0]), &header); err != nil || header.V != 1 {
		t.Fatalf("header = %+v, %v", header, err)
	}
	var footer traceFooter
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &footer); err != nil || !footer.End {
		t.Fatalf("footer = %+v, %v", footer, err)
	}
	want := []string{"ss", "po", "ru", "cs", "cr", "sp", "sd"}
	for i, line := range lines[1 : len(lines)-1] {
		var event []any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
		if len(event) < 3 || event[2] != want[i] {
			t.Fatalf("event %d = %v, want code %q", i, event, want[i])
		}
	}
}
