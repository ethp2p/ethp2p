package broadcast

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
)

// fakeReceiveStream wraps a bytes.Reader to satisfy transport.ReceiveStream
// in tests. CancelRead and SetReadDeadline are no-ops.
type fakeReceiveStream struct {
	*bytes.Reader
}

func (f *fakeReceiveStream) CancelRead(uint64)               {}
func (f *fakeReceiveStream) SetReadDeadline(time.Time) error { return nil }

// newFakeChunk builds a channelChunkStream with a fakeReceiveStream backed by
// the given payload. Convenience helper for tests that need to send
// inbound chunks through the channel inbox.
func newFakeChunk(peer PeerID, msgID MessageID, payload []byte) channelChunkStream {
	return channelChunkStream{
		peerID: peer,
		frame: &bcastpb.Chunk_Header{
			MessageId:  string(msgID),
			ChunkId:    append([]byte(nil), payload...),
			DataLength: uint32(len(payload)),
		},
		stream: &fakeReceiveStream{bytes.NewReader(payload)},
	}
}

// newMockScheme returns a Scheme factory that creates mockStrategy
// instances for both origin and relay sessions. The relay coder
// decodes on first receive by default.
func newMockScheme() Scheme[*testChunk, *testRouting, *testPreamble] {
	return newMockSchemeWithHooks(nil, nil)
}

func newMockSchemeWithHooks(
	originHook func(MessageID, []byte) (Strategy[*testChunk, *testRouting], *testPreamble, error),
	relayHook func(MessageID, *testPreamble) (Strategy[*testChunk, *testRouting], error),
) Scheme[*testChunk, *testRouting, *testPreamble] {
	return Scheme[*testChunk, *testRouting, *testPreamble]{
		Name: "mock",
		NewOrigin: func(msgID MessageID, payload []byte) (Strategy[*testChunk, *testRouting], *testPreamble, error) {
			if originHook != nil {
				return originHook(msgID, payload)
			}
			ms := newMockStrategy()
			p := testPreamble("mock-preamble")
			return ms, &p, nil
		},
		NewRelay: func(msgID MessageID, preamble *testPreamble) (Strategy[*testChunk, *testRouting], error) {
			if relayHook != nil {
				return relayHook(msgID, preamble)
			}
			return newMockStrategy(), nil
		},
		NewCI: func() *testChunk { return &testChunk{} },
		NewR:  func() *testRouting { r := testRouting{}; return &r },
		NewP:  func() *testPreamble { p := testPreamble{}; return &p },
	}
}

// decodeOnTakeStrategy is a relay strategy mock that publishes a decode
// task on the first TakeChunk call, simulating single-chunk decode.
type decodeOnTakeStrategy struct {
	mockStrategy
	decoded bool
}

func newDecodeOnTakeStrategy() *decodeOnTakeStrategy {
	return &decodeOnTakeStrategy{
		mockStrategy: mockStrategy{
			takeVerdict: VerdictAccepted,
		},
	}
}

func (ds *decodeOnTakeStrategy) TakeChunk(peer PeerID, chunk *testChunk, data []byte, dedup *DedupCancel) (Verdict, bool, error) {
	if ds.decoded {
		return VerdictRedundant, false, nil
	}
	v, _, err := ds.mockStrategy.TakeChunk(peer, chunk, data, dedup)
	ds.decoded = true
	ds.mockStrategy.decodeResult = append([]byte(nil), data...)
	return v, true, err
}

// pushingStrategy is a test Strategy that returns chunk dispatches from
// Poll one at a time per peer, enabling end-to-end verification of the
// Publish -> Session dispatch -> PeerConn -> SendResult pipeline.
//
// It dispatches one chunk per peer at a time. When ChunkSent reports
// success, the next unsent chunk for that peer becomes available via
// Poll. This mirrors the slot-based backpressure in real strategies.
type pushingStrategy struct {
	messageID MessageID
	pending   []*testChunk
	peers     map[PeerID]*PeerSessionStats
	peerSent  map[PeerID]map[int]bool
	peerReady map[PeerID]bool // true = peer can receive next chunk via Poll
	closed    bool
	pushHook  func(messageID MessageID, peer PeerID, chunkIdx int)

	mu             sync.Mutex
	pushCount      map[PeerID]int
	sentTotal      map[PeerID]int
	sentOK         map[PeerID]int
	unexpectedSent map[PeerID]int
	sentOrder      map[PeerID][]int
}

func newPushingStrategy(messageID MessageID) *pushingStrategy {
	return newPushingStrategyWithPushHook(messageID, nil)
}

func newPushingStrategyWithPushHook(messageID MessageID, pushHook func(messageID MessageID, peer PeerID, chunkIdx int)) *pushingStrategy {
	return &pushingStrategy{
		messageID:      messageID,
		peers:          make(map[PeerID]*PeerSessionStats),
		peerSent:       make(map[PeerID]map[int]bool),
		peerReady:      make(map[PeerID]bool),
		pushHook:       pushHook,
		pushCount:      make(map[PeerID]int),
		sentTotal:      make(map[PeerID]int),
		sentOK:         make(map[PeerID]int),
		unexpectedSent: make(map[PeerID]int),
		sentOrder:      make(map[PeerID][]int),
	}
}

func (ps *pushingStrategy) HaveChunk(_ *testChunk) bool { return false }
func (ps *pushingStrategy) VerifyChunk(_ PeerID, _ *testChunk, _ []byte) Verdict {
	return VerdictAccepted
}
func (ps *pushingStrategy) Verified() <-chan VerifyResult[*testChunk] { return nil }
func (ps *pushingStrategy) DedupKey(_ *testChunk) []byte              { return nil }
func (ps *pushingStrategy) AttachPeer(peer PeerID, stats *PeerSessionStats) {
	ps.peers[peer] = stats
	ps.peerSent[peer] = make(map[int]bool)
	ps.peerReady[peer] = true
}

func (ps *pushingStrategy) DetachPeer(peer PeerID, _ bool) {
	delete(ps.peers, peer)
	delete(ps.peerSent, peer)
	delete(ps.peerReady, peer)
}

func (ps *pushingStrategy) TakeChunk(_ PeerID, chunkID *testChunk, data []byte, _ *DedupCancel) (Verdict, bool, error) {
	ps.pending = append(ps.pending, &testChunk{ID: chunkID.ID, Data: append([]byte(nil), data...)})
	return VerdictAccepted, false, nil
}

func (ps *pushingStrategy) Decode() ([]byte, error) { return nil, nil }

func (ps *pushingStrategy) RoutingUpdate(_ PeerID, _ *testRouting) ([]ChunkHandle, error) {
	return nil, nil
}

// PollChunks returns one chunk dispatch for the first ready peer that has
// unsent chunks.
func (ps *pushingStrategy) PollChunks() []ChunkDispatch[*testChunk] {
	if ps.closed {
		return nil
	}
	for peer := range ps.peers {
		if !ps.peerReady[peer] {
			continue
		}
		sent := ps.peerSent[peer]
		for i, chunk := range ps.pending {
			if sent[i] {
				continue
			}
			ps.peerSent[peer][i] = true
			ps.peerReady[peer] = false

			ps.mu.Lock()
			ps.pushCount[peer]++
			ps.mu.Unlock()
			if ps.pushHook != nil {
				ps.pushHook(ps.messageID, peer, i)
			}

			return []ChunkDispatch[*testChunk]{
				{Peer: peer, ChunkID: chunk, Data: chunk.Data},
			}
		}
	}
	return nil
}

func (ps *pushingStrategy) PollRouting(force bool) (*testRouting, bool) {
	return nil, false
}

func (ps *pushingStrategy) ChunkSent(peer PeerID, handle ChunkHandle, err error) {
	chunkIdx := int(handle)
	if chunkIdx < 0 || chunkIdx >= len(ps.pending) {
		ps.mu.Lock()
		ps.unexpectedSent[peer]++
		ps.mu.Unlock()
		return
	}
	ps.mu.Lock()
	ps.sentTotal[peer]++
	ps.sentOrder[peer] = append(ps.sentOrder[peer], chunkIdx)
	if err == nil {
		ps.sentOK[peer]++
	}
	ps.mu.Unlock()

	ps.peerReady[peer] = true
}

func (ps *pushingStrategy) Progress() (have, need int) { return 0, 0 }

func (ps *pushingStrategy) Work() <-chan struct{} { return nil }

func (ps *pushingStrategy) Close() error {
	ps.closed = true
	return nil
}

func (ps *pushingStrategy) snapshot() (pushCount, sentOK map[PeerID]int) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	pc := make(map[PeerID]int, len(ps.pushCount))
	so := make(map[PeerID]int, len(ps.sentOK))
	for k, v := range ps.pushCount {
		pc[k] = v
	}
	for k, v := range ps.sentOK {
		so[k] = v
	}
	return pc, so
}

var _ Strategy[*testChunk, *testRouting] = (*pushingStrategy)(nil)

// --- Observer helpers ---

type recordObserver struct {
	NoOpObserver
	mu          sync.Mutex
	chunksSent  int
	chunkErrors []ChunkProcessError
	created     []MessageID
	decoded     []MessageID
	disposed    []sessionDisposedEvent
}

type sessionDisposedEvent struct {
	channelID ChannelID
	messageID MessageID
	reason    string
}

func (o *recordObserver) OnSessionStarted(_ ChannelID, messageID MessageID, _ SessionRole) {
	o.mu.Lock()
	o.created = append(o.created, messageID)
	o.mu.Unlock()
}

func (o *recordObserver) OnSessionDecoded(_ ChannelID, messageID MessageID, _ time.Duration) {
	o.mu.Lock()
	o.decoded = append(o.decoded, messageID)
	o.mu.Unlock()
}

func (o *recordObserver) OnSessionDisposed(channelID ChannelID, messageID MessageID, reason string) {
	o.mu.Lock()
	o.disposed = append(o.disposed, sessionDisposedEvent{
		channelID: channelID,
		messageID: messageID,
		reason:    reason,
	})
	o.mu.Unlock()
}

func (o *recordObserver) OnChunkSent(_ PeerID, _ ChannelID, _ MessageID, _ int) {
	o.mu.Lock()
	o.chunksSent++
	o.mu.Unlock()
}

func (o *recordObserver) OnChunkError(err ChunkProcessError) {
	o.mu.Lock()
	o.chunkErrors = append(o.chunkErrors, err)
	o.mu.Unlock()
}

// --- Channel tests ---

func TestChannelPublish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := NewEngine(EngineConfig{})
		defer engine.Close()

		scheme := newMockScheme()
		runner := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", scheme)
		defer runner.Stop()

		err := runner.Publish("msg-1", []byte("hello"))
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}

		synctest.Wait()

		if _, ok := runner.sessions["msg-1"]; !ok {
			t.Fatal("session not created")
		}
	})
}

func TestChannelReceiveChunk(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := NewEngine(EngineConfig{})
		defer engine.Close()

		scheme := newMockScheme()
		runner := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", scheme)
		defer runner.Stop()

		runner.inbox <- channelSessionOpen{peerID: "peer1", msg: &bcastpb.Sess_Open{MessageId: "msg-1", Preamble: []byte("mock-header")}}
		synctest.Wait()

		if _, ok := runner.sessions["msg-1"]; !ok {
			t.Fatal("session not created from received chunk")
		}
	})
}

func TestChannelSendToMultiplePeers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := NewEngine(EngineConfig{})
		defer engine.Close()

		scheme := newMockScheme()
		runner := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", scheme)

		t1, _ := newTestTransportPair(context.Background())
		t2, _ := newTestTransportPair(context.Background())
		registerTestPeer(engine, "peer1", t1, ProtocolVersion(1), []ChannelID{"test-channel"})
		registerTestPeer(engine, "peer2", t2, ProtocolVersion(1), []ChannelID{"test-channel"})
		synctest.Wait()

		if len(runner.members) != 2 {
			t.Errorf("expected 2 peers, got %d", len(runner.members))
		}

		defer runner.Stop()

		if err := runner.Publish("msg-1", []byte("hello")); err != nil {
			t.Fatalf("Publish failed: %v", err)
		}

		synctest.Wait()
		if _, ok := runner.sessions["msg-1"]; !ok {
			t.Fatal("session not created")
		}
	})
}

func TestChannelMessageHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := NewEngine(EngineConfig{})
		defer engine.Close()

		// Use a relay hook that decodes on first TakeChunk.
		scheme := newMockSchemeWithHooks(nil, func(_ MessageID, _ *testPreamble) (Strategy[*testChunk, *testRouting], error) {
			return newDecodeOnTakeStrategy(), nil
		})

		runner := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", scheme)

		ch := make(chan FullMessage, 128)
		runner.Subscribe(ch)
		defer runner.Stop()

		runner.inbox <- channelSessionOpen{peerID: "peer1", msg: &bcastpb.Sess_Open{MessageId: "msg-1", Preamble: []byte("mock-header")}}
		synctest.Wait()
		runner.inbox <- newFakeChunk("peer1", "msg-1", []byte("test data"))
		synctest.Wait()

		select {
		case msg := <-ch:
			if msg.MessageID != "msg-1" {
				t.Errorf("expected message handler to receive msg-1, got %s", msg.MessageID)
			}
			if string(msg.Data) != "test data" {
				t.Errorf("expected message data 'test data', got %s", string(msg.Data))
			}
		default:
			t.Fatal("no message on subscription channel")
		}
	})
}

func TestChannel_PublishOriginEndToEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := NewEngine(EngineConfig{})
		defer engine.Close()

		numChunks := 3
		chunks := make([]*testChunk, numChunks)
		for i := range chunks {
			chunks[i] = &testChunk{
				ID:   i,
				Data: []byte(fmt.Sprintf("chunk-%d", i)),
			}
		}

		var strat *pushingStrategy

		scheme := newMockSchemeWithHooks(
			func(msgID MessageID, _ []byte) (Strategy[*testChunk, *testRouting], *testPreamble, error) {
				strat = newPushingStrategy(msgID)
				strat.pending = chunks
				p := testPreamble("test-preamble")
				return strat, &p, nil
			},
			nil,
		)

		channel := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", scheme)

		peerIDs := []PeerID{"peerA", "peerB", "peerC"}
		for _, pid := range peerIDs {
			tr, _ := newTestTransportPair(context.Background())
			registerTestPeer(engine, pid, tr, ProtocolVersion(1), []ChannelID{"test-channel"})
		}

		synctest.Wait()

		if err := channel.Publish("msg-1", []byte("hello")); err != nil {
			t.Fatalf("Publish: %v", err)
		}

		synctest.Wait()

		pushCount, sentOK := strat.snapshot()

		for _, pid := range peerIDs {
			if pushCount[pid] != numChunks {
				t.Errorf("peer %s: pushCount=%d, want %d", pid, pushCount[pid], numChunks)
			}
			if sentOK[pid] != numChunks {
				t.Errorf("peer %s: sentOK=%d, want %d", pid, sentOK[pid], numChunks)
			}
		}

		_, hasSess := channel.sessions["msg-1"]
		if !hasSess {
			t.Error("session not registered")
		}

		channel.Stop()
		engine.Close()
	})
}

func TestChannel_RelayEndToEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := NewEngine(EngineConfig{})
		defer engine.Close()

		var strat *pushingStrategy

		scheme := newMockSchemeWithHooks(nil, func(msgID MessageID, _ *testPreamble) (Strategy[*testChunk, *testRouting], error) {
			strat = newPushingStrategy(msgID)
			return strat, nil
		})

		channel := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", scheme)

		peerIDs := []PeerID{"peerB", "peerC"}
		for _, pid := range peerIDs {
			tr, _ := newTestTransportPair(context.Background())
			registerTestPeer(engine, pid, tr, ProtocolVersion(1), []ChannelID{"test-channel"})
		}
		synctest.Wait()

		channel.inbox <- channelSessionOpen{peerID: "peerA", msg: &bcastpb.Sess_Open{MessageId: "msg-1", Preamble: []byte("hdr")}}
		synctest.Wait()
		channel.inbox <- newFakeChunk("peerA", "msg-1", []byte("chunk-data"))
		synctest.Wait()

		pushCount, sentOK := strat.snapshot()
		for _, pid := range peerIDs {
			if pushCount[pid] != 1 {
				t.Errorf("peer %s: pushCount=%d, want 1", pid, pushCount[pid])
			}
			if sentOK[pid] != 1 {
				t.Errorf("peer %s: sentOK=%d, want 1", pid, sentOK[pid])
			}
		}

		_, hasSess := channel.sessions["msg-1"]
		if !hasSess {
			t.Error("session not registered for relay session")
		}

		channel.Stop()
		engine.Close()
	})
}

func TestChannel_RelayDecode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := NewEngine(EngineConfig{})
		defer engine.Close()

		scheme := newMockSchemeWithHooks(nil, func(_ MessageID, _ *testPreamble) (Strategy[*testChunk, *testRouting], error) {
			return newDecodeOnTakeStrategy(), nil
		})

		channel := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", scheme)
		ch := make(chan FullMessage, 128)
		channel.Subscribe(ch)

		channel.inbox <- channelSessionOpen{peerID: "peerA", msg: &bcastpb.Sess_Open{MessageId: "msg-1", Preamble: []byte("hdr")}}
		synctest.Wait()
		channel.inbox <- newFakeChunk("peerA", "msg-1", []byte("decoded-payload"))
		synctest.Wait()

		select {
		case msg := <-ch:
			if msg.MessageID != "msg-1" {
				t.Errorf("messageID = %q, want %q", msg.MessageID, "msg-1")
			}
			if string(msg.Data) != "decoded-payload" {
				t.Errorf("payload = %q, want %q", msg.Data, "decoded-payload")
			}
		default:
			t.Fatal("no decoded message on subscription")
		}

		channel.Stop()
		engine.Close()
	})
}

func TestChannel_RelayDecodeKeepsSessionForAssistAndDoesNotDispose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		obs := &recordObserver{}
		engine := NewEngine(EngineConfig{Observer: obs})
		defer engine.Close()

		scheme := newMockSchemeWithHooks(nil, func(_ MessageID, _ *testPreamble) (Strategy[*testChunk, *testRouting], error) {
			return newDecodeOnTakeStrategy(), nil
		})

		channel := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", scheme)
		ch := make(chan FullMessage, 128)
		channel.Subscribe(ch)

		channel.inbox <- channelSessionOpen{peerID: "peerA", msg: &bcastpb.Sess_Open{MessageId: "msg-1", Preamble: []byte("hdr")}}
		synctest.Wait()
		channel.inbox <- newFakeChunk("peerA", "msg-1", []byte("decoded-payload"))
		synctest.Wait()

		select {
		case msg := <-ch:
			if msg.MessageID != "msg-1" {
				t.Fatalf("messageID = %q, want %q", msg.MessageID, "msg-1")
			}
		default:
			t.Fatal("no decoded message on subscription")
		}

		_, hasSession := channel.sessions["msg-1"]
		if !hasSession {
			t.Fatal("relay session should remain available for assist after decode")
		}

		obs.mu.Lock()
		created := append([]MessageID(nil), obs.created...)
		decoded := append([]MessageID(nil), obs.decoded...)
		disposed := append([]sessionDisposedEvent(nil), obs.disposed...)
		obs.mu.Unlock()

		if len(created) != 1 || created[0] != "msg-1" {
			t.Fatalf("created events = %#v, want [msg-1]", created)
		}
		if len(decoded) != 1 || decoded[0] != "msg-1" {
			t.Fatalf("decoded events = %#v, want [msg-1]", decoded)
		}
		if len(disposed) != 0 {
			t.Fatalf("disposed events = %#v, want none before channel stop", disposed)
		}

		channel.Stop()
		engine.Close()
	})
}

func TestChannel_PeerChurnMidSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := NewEngine(EngineConfig{})
		defer engine.Close()

		var strat *pushingStrategy

		scheme := newMockSchemeWithHooks(nil, func(msgID MessageID, _ *testPreamble) (Strategy[*testChunk, *testRouting], error) {
			strat = newPushingStrategy(msgID)
			return strat, nil
		})

		channel := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", scheme)

		allPeers := []PeerID{"peerA", "peerB", "peerC"}
		for _, pid := range allPeers {
			tr, _ := newTestTransportPair(context.Background())
			registerTestPeer(engine, pid, tr, ProtocolVersion(1), []ChannelID{"test-channel"})
		}
		synctest.Wait()

		channel.inbox <- channelSessionOpen{peerID: "peerX", msg: &bcastpb.Sess_Open{MessageId: "msg-1", Preamble: []byte("hdr")}}
		synctest.Wait()
		channel.inbox <- newFakeChunk("peerX", "msg-1", []byte("chunk-1"))
		synctest.Wait()

		pushBefore, _ := strat.snapshot()
		for _, pid := range allPeers {
			if pushBefore[pid] == 0 {
				t.Fatalf("peer %s: no pushes in first round", pid)
			}
		}

		channel.inbox <- channelPeerChange{peerID: "peerB"}
		synctest.Wait()

		channel.inbox <- newFakeChunk("peerX", "msg-1", []byte("chunk-2"))
		synctest.Wait()

		pushAfter, _ := strat.snapshot()
		if pushAfter["peerA"] <= pushBefore["peerA"] {
			t.Errorf("peerA: pushCount did not increase (%d -> %d)", pushBefore["peerA"], pushAfter["peerA"])
		}
		if pushAfter["peerC"] <= pushBefore["peerC"] {
			t.Errorf("peerC: pushCount did not increase (%d -> %d)", pushBefore["peerC"], pushAfter["peerC"])
		}
		if pushAfter["peerB"] != pushBefore["peerB"] {
			t.Errorf("peerB: pushCount increased after unbind (%d -> %d)", pushBefore["peerB"], pushAfter["peerB"])
		}

		channel.Stop()
		engine.Close()
	})
}
