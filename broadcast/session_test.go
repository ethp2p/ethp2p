package broadcast

import (
	"bytes"
	"context"
	"testing"
	"testing/synctest"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/wire"
)

// testChunk is the canonical chunk type for all core tests.
type testChunk struct {
	ID   int
	Data []byte
}

func (c testChunk) Marshal() ([]byte, error) {
	return c.Data, nil
}

func (c *testChunk) Unmarshal(data []byte) error {
	c.Data = data
	return nil
}

func (c *testChunk) Handle() ChunkHandle {
	if c == nil {
		return 0
	}
	return ChunkHandle(c.ID)
}

// testRouting is the canonical routing type for all core tests.
type testRouting []byte

func (r testRouting) Marshal() ([]byte, error)     { return []byte(r), nil }
func (r *testRouting) Unmarshal(data []byte) error { *r = testRouting(data); return nil }

// testPreamble is the canonical preamble type for all core tests.
type testPreamble []byte

func (p testPreamble) Marshal() ([]byte, error)     { return []byte(p), nil }
func (p *testPreamble) Unmarshal(data []byte) error { *p = testPreamble(data); return nil }

type mockTakeCall struct {
	peer    identity.PeerID
	chunk   *testChunk
	verdict Verdict
}

type mockDetachCall struct {
	peer      identity.PeerID
	completed bool
}

type mockRoutingCall struct {
	peer   identity.PeerID
	update *testRouting
}

type mockSentCall struct {
	peer identity.PeerID
	ok   bool
}

type mockAttachCall struct {
	peer identity.PeerID
}

type mockStrategy struct {
	attachCalls  []mockAttachCall
	detachCalls  []mockDetachCall
	takeCalls    []mockTakeCall
	routingCalls []mockRoutingCall
	sentCalls    []mockSentCall
	pollCalled   int
	closed       bool

	takeVerdict   Verdict
	verifyVerdict Verdict // defaults to VerdictAccepted (zero value maps to it)
	takeErr       error
	pollResults   []ChunkDispatch[*testChunk]
	pollIdx       int
	verifiedCh    chan VerifyResult[*testChunk]
	haveChunk     bool
	dedupKeyFn    func(*testChunk) []byte
	decodeResult  []byte
	decodeErr     error
}

func newMockStrategy() *mockStrategy {
	return &mockStrategy{
		takeVerdict: VerdictAccepted,
	}
}

func (ms *mockStrategy) HaveChunk(_ *testChunk) bool { return ms.haveChunk }
func (ms *mockStrategy) VerifyChunk(_ identity.PeerID, _ *testChunk, _ []byte) Verdict {
	return ms.verifyVerdict
}
func (ms *mockStrategy) Verified() <-chan VerifyResult[*testChunk] {
	return ms.verifiedCh
}
func (ms *mockStrategy) DedupKey(chunkID *testChunk) []byte {
	if ms.dedupKeyFn != nil {
		return ms.dedupKeyFn(chunkID)
	}
	return nil
}
func (ms *mockStrategy) AttachPeer(peer identity.PeerID) {
	ms.attachCalls = append(ms.attachCalls, mockAttachCall{peer: peer})
}

func (ms *mockStrategy) DetachPeer(peer identity.PeerID, completed bool) {
	ms.detachCalls = append(ms.detachCalls, mockDetachCall{peer: peer, completed: completed})
}

func (ms *mockStrategy) TakeChunk(peer identity.PeerID, chunk *testChunk, data []byte, dedup *DedupCancel) (Verdict, bool, error) {
	ms.takeCalls = append(ms.takeCalls, mockTakeCall{
		peer:    peer,
		chunk:   &testChunk{ID: chunk.ID, Data: append([]byte(nil), data...)},
		verdict: ms.takeVerdict,
	})
	if ms.takeVerdict == VerdictAccepted {
		dedup.Cancel()
	}
	return ms.takeVerdict, ms.decodeResult != nil, ms.takeErr
}

func (ms *mockStrategy) Decode() ([]byte, error) {
	return ms.decodeResult, ms.decodeErr
}

func (ms *mockStrategy) RoutingUpdate(peer identity.PeerID, update *testRouting) ([]ChunkHandle, error) {
	ms.routingCalls = append(ms.routingCalls, mockRoutingCall{peer: peer, update: update})
	return nil, nil
}

func (ms *mockStrategy) PollChunks() []ChunkDispatch[*testChunk] {
	ms.pollCalled++
	if ms.pollIdx >= len(ms.pollResults) {
		return nil
	}
	chunks := ms.pollResults[ms.pollIdx:]
	ms.pollIdx = len(ms.pollResults)
	return chunks
}

func (ms *mockStrategy) PollRouting(force bool) (*testRouting, bool) {
	return nil, false
}

func (ms *mockStrategy) ChunkSent(peer identity.PeerID, _ ChunkHandle, err error) {
	ms.sentCalls = append(ms.sentCalls, mockSentCall{peer: peer, ok: err == nil})
}

func (ms *mockStrategy) Progress() (have, need int) { return 0, 0 }

func (ms *mockStrategy) Close() error {
	ms.closed = true
	return nil
}

var _ Strategy[*testChunk, *testRouting] = (*mockStrategy)(nil)

func testPeer(peerID identity.PeerID) *PeerConn {
	ctx, cancel := context.WithCancelCause(context.Background())
	_ = cancel
	bcastOut, _ := newTestBcastStreams(ctx)
	return &PeerConn{
		id:            peerID,
		handle:        newHighCapTransport(context.Background()),
		ctrlOut:       bcastOut,
		ctrlState:     newCtrlOutState(),
		unread:        newFIFO[ethp2p.ReceiveStream](),
		wakeCh:        make(chan struct{}, 1),
		ctx:           ctx,
		cancel:        cancel,
		handshakeDone: make(chan struct{}),
	}
}

func newTestChannel(inbox chan channelEvent) *Channel[*testChunk, *testRouting, *testPreamble] {
	return &Channel[*testChunk, *testRouting, *testPreamble]{
		engine: &Engine{config: EngineConfig{Observer: NoOpObserver{}}},
		id:     "test-channel",
		scheme: Scheme[*testChunk, *testRouting, *testPreamble]{
			NewCI: func() *testChunk { return &testChunk{} },
			NewR:  func() *testRouting { r := testRouting{}; return &r },
		},
		inbox: inbox,
		ctx:   context.Background(),
	}
}

func newTestSession(strat *mockStrategy, inbox chan channelEvent) *session[*testChunk, *testRouting] {
	return newTestSessionWithRole(strat, inbox, false)
}

func newTestSessionWithRole(strat *mockStrategy, inbox chan channelEvent, isOrigin bool) *session[*testChunk, *testRouting] {
	tr := newTestChannel(inbox)
	return tr.newSession("msg1", []byte("preamble"), isOrigin, strat)
}

func TestSession_ChunkIntake(t *testing.T) {
	for _, test := range []struct {
		name      string
		origin    bool
		haveChunk bool
		peers     []identity.PeerID
		wantTakes []identity.PeerID
	}{
		{name: "accepted in order", peers: []identity.PeerID{"p1", "p2", "p3"}, wantTakes: []identity.PeerID{"p1", "p2", "p3"}},
		{name: "already have chunk", haveChunk: true, peers: []identity.PeerID{"p1"}},
		{name: "origin", origin: true, haveChunk: true, peers: []identity.PeerID{"p1", "p2"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				strat := newMockStrategy()
				strat.haveChunk = test.haveChunk
				s := newTestSessionWithRole(strat, nil, test.origin)
				defer s.Close()

				for _, peer := range test.peers {
					s.handleChunkData(peer, []byte("chunk-data"), []byte("chunk-data"))
				}
				if len(strat.takeCalls) != len(test.wantTakes) {
					t.Fatalf("TakeChunk calls = %d, want %d", len(strat.takeCalls), len(test.wantTakes))
				}
				for i, call := range strat.takeCalls {
					if call.peer != test.wantTakes[i] || call.verdict != VerdictAccepted {
						t.Fatalf("take %d = {%q, %v}, want {%q, VerdictAccepted}", i, call.peer, call.verdict, test.wantTakes[i])
					}
				}
			})
		})
	}
}

func TestSession_DispatchTriggers(t *testing.T) {
	dispatch := ChunkDispatch[*testChunk]{Peer: "p1", ChunkID: &testChunk{Data: []byte("chunk-0")}, Data: []byte("chunk-0")}

	t.Run("peer attached", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			strat := newMockStrategy()
			strat.pollResults = []ChunkDispatch[*testChunk]{dispatch}
			s := newTestSession(strat, nil)
			defer s.Close()

			s.handlePeerAttached(testPeer("p1"))
			synctest.Wait()
			if strat.pollIdx != 1 || len(strat.attachCalls) != 1 || strat.attachCalls[0].peer != "p1" {
				t.Fatalf("pollIdx=%d attachCalls=%v", strat.pollIdx, strat.attachCalls)
			}
		})
	})

	t.Run("send complete", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			strat := newMockStrategy()
			strat.pollResults = []ChunkDispatch[*testChunk]{dispatch}
			s := newTestSession(strat, nil)
			defer s.Close()

			s.handlePeerAttached(testPeer("p1"))
			synctest.Wait()
			chunk := <-s.peers["p1"].chunkOutbox
			strat.pollResults = append(strat.pollResults, ChunkDispatch[*testChunk]{Peer: "p1", ChunkID: &testChunk{Data: []byte("chunk-1")}, Data: []byte("chunk-1")})
			s.handleSendComplete(chunk.peerID, chunk.handle, nil, len(chunk.payload))
			if strat.pollIdx != 2 {
				t.Fatalf("pollIdx = %d, want 2", strat.pollIdx)
			}
			if len(strat.sentCalls) != 1 || strat.sentCalls[0].peer != "p1" || !strat.sentCalls[0].ok {
				t.Fatalf("sentCalls = %+v", strat.sentCalls)
			}
		})
	})

	t.Run("routing update", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			strat := newMockStrategy()
			strat.pollResults = []ChunkDispatch[*testChunk]{dispatch}
			s := newTestSession(strat, nil)
			defer s.Close()

			s.handlePeerAttached(testPeer("p1"))
			synctest.Wait()
			<-s.peers["p1"].chunkOutbox
			pollBefore := strat.pollCalled
			s.handleRoutingUpdate("p1", []byte{0xff})
			if len(strat.routingCalls) != 1 || strat.routingCalls[0].peer != "p1" {
				t.Fatalf("routingCalls = %+v", strat.routingCalls)
			}
			if strat.pollCalled <= pollBefore {
				t.Fatalf("Poll not called after routing update: before=%d after=%d", pollBefore, strat.pollCalled)
			}
		})
	})
}

func TestSession_AutoDispose(t *testing.T) {
	for _, test := range []struct {
		name string
		want bool
		run  func(t *testing.T, s *session[*testChunk, *testRouting]) bool
	}{
		{"relay peers completed", false, func(t *testing.T, s *session[*testChunk, *testRouting]) bool {
			s.handlePeerCompleted("p1")
			s.handlePeerCompleted("p2")
			return s.idle()
		}},
		{"origin peers completed", true, func(t *testing.T, s *session[*testChunk, *testRouting]) bool {
			s.stage = stageOrigin
			s.handlePeerCompleted("p1")
			s.handlePeerCompleted("p2")
			return s.idle()
		}},
		{"reconstructed peers completed", true, func(t *testing.T, s *session[*testChunk, *testRouting]) bool {
			s.stage = stageReconstructed
			s.handlePeerCompleted("p1")
			s.handlePeerCompleted("p2")
			return s.idle()
		}},
		{"reconstruction after peers completed", true, func(t *testing.T, s *session[*testChunk, *testRouting]) bool {
			s.handlePeerCompleted("p1")
			s.handlePeerCompleted("p2")
			if s.idle() {
				t.Fatal("relay disposed before reconstruction")
			}
			s.stage = stageReconstructed
			return s.idle()
		}},
		{"mixed completed and dropped", false, func(t *testing.T, s *session[*testChunk, *testRouting]) bool {
			s.handlePeerCompleted("p1")
			s.handlePeerDropped("p2")
			return s.idle()
		}},
		{"all peers dropped", true, func(t *testing.T, s *session[*testChunk, *testRouting]) bool {
			s.handlePeerDropped("p1")
			if s.idle() {
				t.Fatal("disposed with one peer remaining")
			}
			s.handlePeerDropped("p2")
			return s.idle()
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newTestSession(newMockStrategy(), nil)
				defer s.Close()
				s.handlePeerAttached(testPeer("p1"))
				synctest.Wait()
				s.handlePeerAttached(testPeer("p2"))
				synctest.Wait()
				if got := test.run(t, s); got != test.want {
					t.Fatalf("idle = %v, want %v", got, test.want)
				}
			})
		})
	}

	t.Run("no peers", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			s := newTestSession(newMockStrategy(), nil)
			defer s.Close()
			if s.idle() {
				t.Fatal("disposed with no peers ever attached")
			}
		})
	})
}

// DetachPeer promises that in-flight sends of the peer are cancelled first,
// whether the peer completed or was dropped.
func TestSession_DroppedPeerSendsCancelledBeforeDetach(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		s := newTestSession(strat, nil)
		defer s.Close()

		s.handlePeerAttached(testPeer("p1"))
		synctest.Wait()
		ctx, cancel := context.WithCancel(context.Background())
		s.outboundCancels[outboundKey{peer: "p1", handle: 7}] = cancel

		s.handlePeerDropped("p1")

		if ctx.Err() == nil {
			t.Fatal("handlePeerDropped left an in-flight send of the dropped peer running")
		}
		if len(s.outboundCancels) != 0 {
			t.Fatalf("outboundCancels = %d entries, want 0", len(s.outboundCancels))
		}
	})
}

func TestSession_HandleInboundStream_OriginRejects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		inbox := make(chan channelEvent, 16)
		s := newTestSessionWithRole(strat, inbox, true)

		stream := &fakeReceiveStream{bytes.NewReader(wire.AppendFrame(nil, dataFrame, []byte("data")))}
		s.handleChunkStream("p1", []byte("data"), 4, stream)
		synctest.Wait()

		select {
		case <-inbox:
			t.Fatal("origin should reject inbound streams")
		default:
		}

		s.Close()
	})
}

func TestSession_HandleChunkData_DedupCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		strat.dedupKeyFn = func(id *testChunk) []byte { return id.Data }
		inbox := make(chan channelEvent, 16)
		s := newTestSession(strat, inbox)

		chunkID := []byte("data")

		payload := []byte("data")
		s.handleChunkStream("p1", chunkID, uint32(len(payload)), &fakeReceiveStream{bytes.NewReader(wire.AppendFrame(nil, dataFrame, payload))})
		synctest.Wait()

		if _, ok := s.dedupGroups[string(chunkID)]; !ok {
			t.Fatal("expected dedup group to exist after handleChunkStream")
		}

		evt := <-inbox
		cr := evt.(channelChunkData)

		s.handleChunkData(cr.peerID, cr.chunkID, cr.payload)

		if _, ok := s.dedupGroups[string(chunkID)]; ok {
			t.Fatal("dedup group should be removed after VerdictAccepted")
		}

		if len(strat.takeCalls) != 1 {
			t.Fatalf("expected 1 TakeChunk call, got %d", len(strat.takeCalls))
		}

		s.Close()
	})
}
