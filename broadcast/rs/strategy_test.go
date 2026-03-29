package rs

import (
	"bytes"
	"testing"

	"github.com/ethp2p/ethp2p/broadcast"
)

func ssTestConfig() *Config {
	cfg := DefaultConfig()
	cfg.DataShards = 4
	cfg.ParityShards = 4
	cfg.ChunkLen = 1024
	return &cfg
}

func ssTestPayload(_ *testing.T, n int) []byte {
	payload := make([]byte, n)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	return payload
}

func ssTestStats(peer broadcast.PeerID) *broadcast.PeerSessionStats {
	return broadcast.NewPeerSessionStats(peer)
}

func takeChunk(s *strategy, peer broadcast.PeerID, idx int, data []byte) (broadcast.Verdict, bool, error) {
	chunkID := &ChunkIdent{
		Index: idx,
	}
	return s.TakeChunk(peer, chunkID, data, nil)
}

func TestOriginPollChunkDispatch(t *testing.T) {
	payload := ssTestPayload(t, 4*1024)
	s, err := newOriginStrategy(ssTestConfig(), payload)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	s.AttachPeer("peerA", ssTestStats("peerA"))
	chunks := s.PollChunks()
	if len(chunks) == 0 {
		t.Fatal("expected chunk dispatch")
	}
	if chunks[0].Peer != "peerA" {
		t.Fatalf("peer = %q", chunks[0].Peer)
	}
	if len(chunks[0].Data) == 0 {
		t.Fatal("expected non-empty chunk data")
	}
}

func TestRelayDecode(t *testing.T) {
	payload := ssTestPayload(t, 4*1024)
	origin, err := newOriginStrategy(ssTestConfig(), payload)
	if err != nil {
		t.Fatal(err)
	}
	relay, err := newRelayStrategy(ssTestConfig(), &origin.preamble)
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()

	relay.AttachPeer("peerA", ssTestStats("peerA"))
	var complete bool
	for i := 0; i < origin.preamble.DataChunks; i++ {
		v, c, err := takeChunk(relay, "peerA", i, origin.chunks[i])
		if err != nil {
			t.Fatal(err)
		}
		if v != broadcast.VerdictAccepted {
			t.Fatalf("verdict=%d", v)
		}
		complete = c
	}

	if !complete {
		t.Fatal("expected completeness signal")
	}
	decoded, err := relay.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, payload) {
		t.Fatal("decoded payload mismatch")
	}
}

func TestRelayDuplicateIsUseless(t *testing.T) {
	payload := ssTestPayload(t, 4*1024)
	origin, err := newOriginStrategy(ssTestConfig(), payload)
	if err != nil {
		t.Fatal(err)
	}
	relay, err := newRelayStrategy(ssTestConfig(), &origin.preamble)
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()

	relay.AttachPeer("peerA", ssTestStats("peerA"))
	data := origin.chunks[0]
	if v, _, err := takeChunk(relay, "peerA", 0, data); err != nil || v != broadcast.VerdictAccepted {
		t.Fatalf("first verdict=%d err=%v", v, err)
	}
	if v, _, err := takeChunk(relay, "peerA", 0, data); err != nil || v != broadcast.VerdictRedundant {
		t.Fatalf("second verdict=%d err=%v", v, err)
	}
}

func TestRelayBadChunkInvalid(t *testing.T) {
	payload := ssTestPayload(t, 4*1024)
	origin, err := newOriginStrategy(ssTestConfig(), payload)
	if err != nil {
		t.Fatal(err)
	}
	relay, err := newRelayStrategy(ssTestConfig(), &origin.preamble)
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()

	relay.AttachPeer("peerA", ssTestStats("peerA"))
	chunkID := &ChunkIdent{Index: 0}
	v := relay.VerifyChunk("peerA", chunkID, []byte("corrupted"))
	if v != broadcast.VerdictInvalid {
		t.Fatalf("verdict=%d", v)
	}
}

func TestRoutingUpdateReturnsCancelHandles(t *testing.T) {
	payload := ssTestPayload(t, 4*1024)
	origin, err := newOriginStrategy(ssTestConfig(), payload)
	if err != nil {
		t.Fatal(err)
	}
	relay, err := newRelayStrategy(ssTestConfig(), &origin.preamble)
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()

	relay.AttachPeer("peerA", ssTestStats("peerA"))

	// Simulate in-flight send of chunk 1 to peerA.
	ps := relay.peers["peerA"]
	ps.inflight[1] = 1

	bm := NewBitmap(origin.totalChunks)
	bm.Set(1)
	handles, err := relay.RoutingUpdate("peerA", &bm)
	if err != nil {
		t.Fatal(err)
	}
	if len(handles) != 1 || handles[0] != 1 {
		t.Fatalf("expected cancel handles [1], got %v", handles)
	}
}

func TestHaveChunkAndDedupKey(t *testing.T) {
	payload := ssTestPayload(t, 4*1024)
	origin, err := newOriginStrategy(ssTestConfig(), payload)
	if err != nil {
		t.Fatal(err)
	}
	defer origin.Close()

	chunkID := &ChunkIdent{Index: 0}
	if !origin.HaveChunk(chunkID) {
		t.Fatal("origin should report having all chunks")
	}
	key := origin.DedupKey(chunkID)
	if len(key) == 0 {
		t.Fatal("expected dedup key")
	}
}
