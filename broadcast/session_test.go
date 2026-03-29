package broadcast

import (
	"bytes"
	"context"
	"testing"
	"testing/synctest"
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

// --- mockStrategy implements Strategy[*testChunk, *testRouting] ---

type mockTakeCall struct {
	peer    PeerID
	chunk   *testChunk
	verdict Verdict
}

type mockDetachCall struct {
	peer      PeerID
	completed bool
}

type mockRoutingCall struct {
	peer   PeerID
	update *testRouting
}

type mockSentCall struct {
	peer PeerID
	ok   bool
}

type mockAttachCall struct {
	peer PeerID
}

type mockStrategy struct {
	// Recording
	attachCalls  []mockAttachCall
	detachCalls  []mockDetachCall
	takeCalls    []mockTakeCall
	routingCalls []mockRoutingCall
	sentCalls    []mockSentCall
	pollCalled   int
	closed       bool

	// Configuration
	takeVerdict   Verdict
	verifyVerdict Verdict // defaults to VerdictAccepted (zero value maps to it)
	takeErr       error
	pollResults   []ChunkDispatch[*testChunk]
	pollIdx       int
	readyCh       chan struct{}
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
func (ms *mockStrategy) VerifyChunk(_ PeerID, _ *testChunk, _ []byte) Verdict {
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
func (ms *mockStrategy) AttachPeer(peer PeerID, stats *PeerSessionStats) {
	ms.attachCalls = append(ms.attachCalls, mockAttachCall{peer: peer})
}

func (ms *mockStrategy) DetachPeer(peer PeerID, completed bool) {
	ms.detachCalls = append(ms.detachCalls, mockDetachCall{peer: peer, completed: completed})
}

func (ms *mockStrategy) TakeChunk(peer PeerID, chunk *testChunk, data []byte, dedup *DedupCancel) (Verdict, bool, error) {
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

func (ms *mockStrategy) RoutingUpdate(peer PeerID, update *testRouting) ([]ChunkHandle, error) {
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

func (ms *mockStrategy) ChunkSent(peer PeerID, _ ChunkHandle, err error) {
	ms.sentCalls = append(ms.sentCalls, mockSentCall{peer: peer, ok: err == nil})
}

func (ms *mockStrategy) Work() <-chan struct{} {
	return ms.readyCh
}

func (ms *mockStrategy) Progress() (have, need int) { return 0, 0 }

func (ms *mockStrategy) Close() error {
	ms.closed = true
	return nil
}

var _ Strategy[*testChunk, *testRouting] = (*mockStrategy)(nil)

// --- Helper ---

func testPeer(peerID PeerID) *PeerConn {
	ctx, cancel := context.WithCancel(context.Background())
	_ = cancel
	bcastOut, _ := newTestBcastStreams(ctx)
	return &PeerConn{
		id:      peerID,
		conn:    newHighCapTransport(context.Background()),
		ctrlOut: bcastOut,
		ctrlQ:   make(chan peerCtrlEvent, ctrlQCap),
		wakeCh:  make(chan struct{}, 1),
		ctx:     ctx,
		cancel:  cancel,
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

// --- Tests ---

func TestSession_EventOrdering(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		s := newTestSession(strat, nil)

		s.handleChunkData("p1", []byte("chunk-0"), []byte("chunk-0"))
		s.handleChunkData("p2", []byte("chunk-1"), []byte("chunk-1"))
		s.handleChunkData("p3", []byte("chunk-2"), []byte("chunk-2"))

		if len(strat.takeCalls) != 3 {
			t.Fatalf("expected 3 TakeChunk calls, got %d", len(strat.takeCalls))
		}

		wantPeers := []PeerID{"p1", "p2", "p3"}
		for i, call := range strat.takeCalls {
			if call.peer != wantPeers[i] {
				t.Errorf("call %d: peer=%q, want %q", i, call.peer, wantPeers[i])
			}
			if call.verdict != VerdictAccepted {
				t.Errorf("call %d: verdict=%d, want VerdictAccepted", i, call.verdict)
			}
		}

		s.Close()
	})
}

func TestSession_PollDrivenDispatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		// Pre-configure 3 Poll results that dispatch to "p1".
		strat.pollResults = make([]ChunkDispatch[*testChunk], 3)
		for i := range 3 {
			strat.pollResults[i] = ChunkDispatch[*testChunk]{
				Peer:    "p1",
				ChunkID: &testChunk{ID: i, Data: []byte("data")},
				Data:    []byte("data"),
			}
		}

		s := newTestSession(strat, nil)

		peer := testPeer("p1")
		s.handlePeerAttached(peer)
		synctest.Wait()

		// handlePeerAttached calls drainPolls, which should consume
		// all 3 pollResults and place chunks in p1's slot.
		if strat.pollCalled < 1 {
			t.Fatalf("expected at least 1 Poll call, got %d", strat.pollCalled)
		}

		if len(strat.attachCalls) != 1 {
			t.Fatalf("expected 1 AttachPeer call, got %d", len(strat.attachCalls))
		}
		if strat.attachCalls[0].peer != "p1" {
			t.Errorf("AttachPeer peer=%q, want %q", strat.attachCalls[0].peer, "p1")
		}

		s.Close()
	})
}

func TestSession_CloseCallsStrategyClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		s := newTestSession(strat, nil)

		s.handleChunkData("p1", []byte("data"), []byte("data"))
		s.Close()

		if !strat.closed {
			t.Error("Strategy.Close() was not called")
		}
	})
}

func TestSession_SendCompleteCallsChunkSent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		s := newTestSession(strat, nil)

		s.handleSendComplete("p1", 0, nil, 10)
		s.handleSendComplete("p2", 1, ErrChunkWriteFail, 0)

		if len(strat.sentCalls) != 2 {
			t.Fatalf("expected 2 ChunkSent calls, got %d", len(strat.sentCalls))
		}
		if strat.sentCalls[0].peer != "p1" || strat.sentCalls[0].ok != true {
			t.Errorf("sent[0] = %+v, want {p1, true}", strat.sentCalls[0])
		}
		if strat.sentCalls[1].peer != "p2" || strat.sentCalls[1].ok != false {
			t.Errorf("sent[1] = %+v, want {p2, false}", strat.sentCalls[1])
		}

		s.Close()
	})
}

func TestSession_OriginSkipsTakeChunk(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		strat.haveChunk = true // Origin strategies always report having all chunks.
		s := newTestSessionWithRole(strat, nil, true)
		defer s.Close()

		s.handleChunkData("p1", []byte("chunk-data"), []byte("chunk-data"))
		s.handleChunkData("p2", []byte("chunk-data"), []byte("chunk-data"))

		if len(strat.takeCalls) != 0 {
			t.Fatalf("origin session called TakeChunk %d times, want 0", len(strat.takeCalls))
		}
	})
}

func TestSession_SlotBasedDispatchResumption(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()

		// Set up 2 chunk dispatches: one will be returned on the first
		// drainPolls (from handlePeerAttached), one on the second
		// drainPolls (from handleSendComplete).
		strat.pollResults = []ChunkDispatch[*testChunk]{
			{Peer: "p1", ChunkID: &testChunk{Data: []byte("chunk-0")}, Data: []byte("chunk-0")},
		}

		s := newTestSession(strat, nil)
		defer s.Close()

		peer := testPeer("p1")
		s.handlePeerAttached(peer)
		synctest.Wait()

		// After attach, drainPolls consumed 1 dispatch.
		if strat.pollIdx != 1 {
			t.Fatalf("expected pollIdx=1 after attach, got %d", strat.pollIdx)
		}

		// Consume the chunk from the slot (simulates outbound loop).
		sp := s.peers["p1"]
		chunk := <-sp.chunkOutbox

		// Add a second poll result for the retry.
		strat.pollResults = append(strat.pollResults,
			ChunkDispatch[*testChunk]{Peer: "p1", ChunkID: &testChunk{Data: []byte("chunk-1")}, Data: []byte("chunk-1")},
		)

		// handleSendComplete triggers drainPolls again.
		s.handleSendComplete(chunk.peerID, chunk.handle, nil, len(chunk.payload))

		if strat.pollIdx != 2 {
			t.Fatalf("expected pollIdx=2 after send complete, got %d", strat.pollIdx)
		}
	})
}

func TestSession_RoutingUpdateTriggersDispatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()

		// One dispatch on attach.
		strat.pollResults = []ChunkDispatch[*testChunk]{
			{Peer: "p1", ChunkID: &testChunk{Data: []byte("chunk-0")}, Data: []byte("chunk-0")},
		}

		s := newTestSession(strat, nil)
		defer s.Close()

		peer := testPeer("p1")
		s.handlePeerAttached(peer)
		synctest.Wait()

		// Consume the initial chunk from the slot.
		sp := s.peers["p1"]
		<-sp.chunkOutbox

		pollBefore := strat.pollCalled

		// Send a routing update. This should call strategy.RoutingUpdate
		// and trigger drainPolls.
		s.handleRoutingUpdate("p1", []byte{0xff})

		if len(strat.routingCalls) != 1 {
			t.Fatalf("expected 1 RoutingUpdate call, got %d", len(strat.routingCalls))
		}
		if strat.routingCalls[0].peer != "p1" {
			t.Errorf("RoutingUpdate peer=%q, want %q", strat.routingCalls[0].peer, "p1")
		}

		// Verify Poll was called at least once more after routing update.
		if strat.pollCalled <= pollBefore {
			t.Fatalf("expected Poll to be called after routing update, pollCalled: before=%d, after=%d", pollBefore, strat.pollCalled)
		}
	})
}

func TestSession_AutoDisposeOnAllPeersCompleted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		channelInbox := make(chan channelEvent, 16)
		s := newTestSession(strat, channelInbox)
		defer s.Close()

		p1 := testPeer("p1")
		p2 := testPeer("p2")
		s.handlePeerAttached(p1)
		synctest.Wait()
		s.handlePeerAttached(p2)
		synctest.Wait()

		s.handlePeerCompleted("p1")
		s.handlePeerCompleted("p2")

		// Non-origin relay that hasn't reconstructed must not
		// auto-dispose: completed peers can still send chunks.
		select {
		case <-channelInbox:
			t.Fatal("non-reconstructed relay must not auto-dispose on peer completion")
		default:
		}
	})
}

func TestSession_AutoDisposeOnAllPeersCompleted_Origin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		channelInbox := make(chan channelEvent, 16)
		s := newTestSessionWithRole(strat, channelInbox, true)
		defer s.Close()

		p1 := testPeer("p1")
		p2 := testPeer("p2")
		s.handlePeerAttached(p1)
		synctest.Wait()
		s.handlePeerAttached(p2)
		synctest.Wait()

		s.handlePeerCompleted("p1")
		s.handlePeerCompleted("p2")

		select {
		case evt := <-channelInbox:
			if _, ok := evt.(channelSessionDisposed); !ok {
				t.Fatalf("expected channelSessionDisposed, got %T", evt)
			}
		default:
			t.Fatal("origin should auto-dispose after all peers completed")
		}
	})
}

func TestSession_AutoDisposeOnAllPeersCompleted_Reconstructed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		channelInbox := make(chan channelEvent, 16)
		s := newTestSession(strat, channelInbox)
		defer s.Close()

		p1 := testPeer("p1")
		p2 := testPeer("p2")
		s.handlePeerAttached(p1)
		synctest.Wait()
		s.handlePeerAttached(p2)
		synctest.Wait()

		s.signalReconstructed()

		s.handlePeerCompleted("p1")
		s.handlePeerCompleted("p2")

		select {
		case evt := <-channelInbox:
			if _, ok := evt.(channelSessionDisposed); !ok {
				t.Fatalf("expected channelSessionDisposed, got %T", evt)
			}
		default:
			t.Fatal("reconstructed relay should auto-dispose after all peers completed")
		}
	})
}

func TestSession_DisposeOnReconstructionWithCompletedPeers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		channelInbox := make(chan channelEvent, 16)
		s := newTestSession(strat, channelInbox)
		defer s.Close()

		p1 := testPeer("p1")
		p2 := testPeer("p2")
		s.handlePeerAttached(p1)
		synctest.Wait()
		s.handlePeerAttached(p2)
		synctest.Wait()

		// Both peers complete before the relay reconstructs.
		s.handlePeerCompleted("p1")
		s.handlePeerCompleted("p2")

		// Non-reconstructed relay must not dispose yet.
		select {
		case <-channelInbox:
			t.Fatal("non-reconstructed relay must not dispose when peers complete")
		default:
		}

		// Reconstruction should trigger disposal since all peers
		// are already completed.
		s.signalReconstructed()

		select {
		case evt := <-channelInbox:
			if _, ok := evt.(channelSessionDisposed); !ok {
				t.Fatalf("expected channelSessionDisposed, got %T", evt)
			}
		default:
			t.Fatal("should dispose on reconstruction when all peers already completed")
		}
	})
}

func TestSession_AutoDisposeOnMixedCompletedAndDropped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		channelInbox := make(chan channelEvent, 16)
		s := newTestSession(strat, channelInbox)
		defer s.Close()

		p1 := testPeer("p1")
		p2 := testPeer("p2")
		s.handlePeerAttached(p1)
		synctest.Wait()
		s.handlePeerAttached(p2)
		synctest.Wait()

		s.handlePeerCompleted("p1")
		s.handlePeerDropped("p2")

		// p1 still in map (completed but alive), relay not
		// reconstructed: must not auto-dispose.
		select {
		case <-channelInbox:
			t.Fatal("non-reconstructed relay must not auto-dispose while completed peers remain")
		default:
		}
	})
}

func TestSession_AutoDisposeOnAllPeersDropped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		channelInbox := make(chan channelEvent, 16)
		s := newTestSession(strat, channelInbox)
		defer s.Close()

		p1 := testPeer("p1")
		p2 := testPeer("p2")
		s.handlePeerAttached(p1)
		synctest.Wait()
		s.handlePeerAttached(p2)
		synctest.Wait()

		s.handlePeerDropped("p1")

		select {
		case <-channelInbox:
			t.Fatal("auto-dispose should not fire with one peer remaining")
		default:
		}

		s.handlePeerDropped("p2")

		select {
		case evt := <-channelInbox:
			if _, ok := evt.(channelSessionDisposed); !ok {
				t.Fatalf("expected channelSessionDisposed, got %T", evt)
			}
		default:
			t.Fatal("should auto-dispose after all peers dropped")
		}
	})
}

func TestSession_NoAutoDisposeWithNoPeers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		channelInbox := make(chan channelEvent, 16)
		s := newTestSession(strat, channelInbox)

		select {
		case <-channelInbox:
			t.Fatal("auto-dispose must not fire when no peers were ever attached")
		default:
		}

		s.Close()
	})
}

// --- handleChunkData and handleChunkStream tests ---

func TestSession_HandleChunkData(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		s := newTestSession(strat, nil)
		defer s.Close()

		s.handleChunkData("p1", []byte("chunk-data"), []byte("chunk-data"))

		if len(strat.takeCalls) != 1 {
			t.Fatalf("expected 1 TakeChunk call, got %d", len(strat.takeCalls))
		}
		if strat.takeCalls[0].peer != "p1" {
			t.Errorf("peer = %q, want %q", strat.takeCalls[0].peer, "p1")
		}
		if string(strat.takeCalls[0].chunk.Data) != "chunk-data" {
			t.Errorf("chunk data = %q, want %q", strat.takeCalls[0].chunk.Data, "chunk-data")
		}
	})
}

func TestSession_HandleChunkData_HaveChunkSkips(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		strat.haveChunk = true
		s := newTestSession(strat, nil)
		defer s.Close()

		s.handleChunkData("p1", []byte("chunk-data"), []byte("chunk-data"))

		if len(strat.takeCalls) != 0 {
			t.Fatalf("expected 0 TakeChunk calls when HaveChunk=true, got %d", len(strat.takeCalls))
		}
	})
}

func TestSession_HandleInboundStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		inbox := make(chan channelEvent, 16)
		s := newTestSession(strat, inbox)

		payload := []byte("chunk-data")
		stream := &fakeReceiveStream{bytes.NewReader(payload)}
		s.handleChunkStream("p1", []byte("chunk-data"), uint32(len(payload)), stream)
		synctest.Wait()

		select {
		case evt := <-inbox:
			cr, ok := evt.(channelChunkData)
			if !ok {
				t.Fatalf("expected channelChunkData, got %T", evt)
			}
			if cr.peerID != "p1" {
				t.Errorf("peerID = %q, want %q", cr.peerID, "p1")
			}
			if string(cr.payload) != "chunk-data" {
				t.Errorf("payload = %q, want %q", cr.payload, "chunk-data")
			}
			if !bytes.Equal(cr.chunkID, []byte("chunk-data")) {
				t.Errorf("chunkID = %v, want chunk-data", cr.chunkID)
			}
		default:
			t.Fatal("no channelChunkData on inbox")
		}

		s.Close()
	})
}

func TestSession_HandleInboundStream_OriginRejects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		inbox := make(chan channelEvent, 16)
		s := newTestSessionWithRole(strat, inbox, true)

		stream := &fakeReceiveStream{bytes.NewReader([]byte("data"))}
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

func TestSession_HandleInboundStream_HaveChunkRejects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		strat.haveChunk = true
		inbox := make(chan channelEvent, 16)
		s := newTestSession(strat, inbox)

		stream := &fakeReceiveStream{bytes.NewReader([]byte("data"))}
		s.handleChunkStream("p1", []byte("data"), 4, stream)
		synctest.Wait()

		select {
		case <-inbox:
			t.Fatal("should reject when HaveChunk=true")
		default:
		}

		s.Close()
	})
}

func TestSession_HandleInboundStream_SemaphoreFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strat := newMockStrategy()
		inbox := make(chan channelEvent, 64)
		s := newTestSession(strat, inbox)

		// Fill the semaphore to capacity.
		for range maxConcurrentReads {
			s.readSem <- struct{}{}
		}

		// This stream should be rejected because the semaphore is full.
		stream := &fakeReceiveStream{bytes.NewReader([]byte("data"))}
		s.handleChunkStream("p1", []byte("data"), 4, stream)
		synctest.Wait()

		select {
		case <-inbox:
			t.Fatal("should reject when semaphore is full")
		default:
		}

		// Drain semaphore so Close can proceed.
		for range maxConcurrentReads {
			<-s.readSem
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

		// Initiate an inbound stream to create the dedup group.
		payload := []byte("data")
		s.handleChunkStream("p1", chunkID, uint32(len(payload)), &fakeReceiveStream{bytes.NewReader(payload)})
		synctest.Wait()

		// Verify the dedup group was created.
		key := string(chunkID)
		if _, ok := s.dedupGroups[key]; !ok {
			t.Fatal("expected dedup group to exist after handleChunkStream")
		}

		// Drain the channelChunkData event.
		evt := <-inbox
		cr := evt.(channelChunkData)

		// Process the chunk data: VerdictAccepted should cancel the dedup group.
		s.handleChunkData(cr.peerID, cr.chunkID, cr.payload)

		if _, ok := s.dedupGroups[key]; ok {
			t.Fatal("dedup group should be removed after VerdictAccepted")
		}

		if len(strat.takeCalls) != 1 {
			t.Fatalf("expected 1 TakeChunk call, got %d", len(strat.takeCalls))
		}

		s.Close()
	})
}
