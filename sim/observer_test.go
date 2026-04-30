package sim

import (
	"testing"

	"github.com/ethp2p/ethp2p/broadcast"
)

func TestObserverResetMessageScopesChunkVerdicts(t *testing.T) {
	obs := NewObserver()
	channelID := broadcast.ChannelID("broadcast")
	msg0 := broadcast.MessageID("msg-0")
	msg1 := broadcast.MessageID("msg-1")

	obs.OnSessionStarted(channelID, msg0, broadcast.SessionRoleOrigin)
	obs.OnSessionStarted(channelID, msg1, broadcast.SessionRoleRelay)
	obs.OnChunkSent("peer-1", channelID, msg0, 100)
	obs.OnChunkSent("peer-2", channelID, msg1, 50)
	obs.OnChunkRcvd("peer-1", channelID, msg0, broadcast.VerdictAccepted)
	obs.OnChunkRcvd("peer-2", channelID, msg0, broadcast.VerdictDecoding)
	obs.OnChunkRcvd("peer-3", channelID, msg1, broadcast.VerdictRedundant)
	obs.OnChunkRcvd("peer-4", channelID, msg1, broadcast.VerdictSurplus)

	snap := obs.ResetMessage(msg0)
	if snap.OriginSent != 100 {
		t.Fatalf("OriginSent=%d, want 100", snap.OriginSent)
	}
	if snap.RelaySent != 0 {
		t.Fatalf("RelaySent=%d, want 0", snap.RelaySent)
	}
	if snap.Chunks.Accepted != 1 || snap.Chunks.Decoding != 1 {
		t.Fatalf("msg0 chunks=%+v, want accepted=1 decoding=1", snap.Chunks)
	}
	if snap.Chunks.Redundant != 0 || snap.Chunks.Surplus != 0 {
		t.Fatalf("msg0 chunks=%+v, want no msg1 verdicts", snap.Chunks)
	}

	remaining := obs.Reset()
	if remaining.OriginSent != 0 {
		t.Fatalf("remaining OriginSent=%d, want 0", remaining.OriginSent)
	}
	if remaining.RelaySent != 50 {
		t.Fatalf("remaining RelaySent=%d, want 50", remaining.RelaySent)
	}
	if remaining.Chunks.Redundant != 1 || remaining.Chunks.Surplus != 1 {
		t.Fatalf("remaining chunks=%+v, want redundant=1 surplus=1", remaining.Chunks)
	}
	if remaining.Chunks.Accepted != 0 || remaining.Chunks.Decoding != 0 {
		t.Fatalf("remaining chunks=%+v, want no msg0 verdicts", remaining.Chunks)
	}
}
