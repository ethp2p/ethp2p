package broadcast

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/wire"
)

// BenchmarkSessionDispatchFanout measures the throughput of the session
// dispatch path: drainPolls -> sendChunk -> PeerConn slot enqueue.
//
// Each iteration adds one chunk to the strategy's poll queue and calls
// drainPolls, then processes completions from every peer. The benchmark owns
// session state, matching the production channel's single-goroutine ownership.
func BenchmarkSessionDispatchFanout(b *testing.B) {
	for _, numPeers := range []int{1, 10, 50} {
		b.Run(fmt.Sprintf("peers=%d", numPeers), func(b *testing.B) {
			benchmarkSessionDispatchFanout(b, numPeers)
		})
	}
}

func benchmarkSessionDispatchFanout(b *testing.B, numPeers int) {
	strat := &dispatchStrategy{}

	channelInbox := make(chan channelEvent, 4096)

	tr := &Channel[*testChunk, *testRouting, *testPreamble]{
		engine: &Engine{config: EngineConfig{Observer: NoOpObserver{}}},
		id:     "bench-channel",
		scheme: Scheme[*testChunk, *testRouting, *testPreamble]{
			NewCI: func() *testChunk { return &testChunk{} },
			NewR:  func() *testRouting { return &testRouting{} },
		},
		inbox: channelInbox,
		ctx:   b.Context(),
	}
	s := tr.newSession("bench-msg", []byte("preamble"), false, strat)
	defer s.Close()

	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()

	peerIDs := make([]transport.PeerID, numPeers)
	for i := range numPeers {
		pid := transport.PeerID(fmt.Sprintf("p%d", i))
		peerIDs[i] = pid
		peer := testPeer(pid)
		s.handlePeerAttached(peer)
		// Drain ctrlQ and chunk slots, send completions back.
		go func(p *PeerConn) {
			for {
				select {
				case <-p.lifecycle.ready:
					ctrl, _ := p.lifecycle.pop()
					if e, ok := ctrl.(peerOpenSession); ok {
						go func() {
							for {
								select {
								case chunk := <-e.chunkOutbox:
									select {
									case chunk.resultCh <- channelChunkSent{
										messageID: chunk.messageID,
										peerID:    chunk.peerID,
										handle:    chunk.handle,
										err:       nil,
										size:      len(chunk.payload),
									}:
									case <-ctx.Done():
										return
									}
								case <-ctx.Done():
									return
								}
							}
						}()
					}
				case <-ctx.Done():
					return
				}
			}
		}(peer)
	}

	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		// Enqueue a dispatch for each peer.
		for _, pid := range peerIDs {
			strat.pollQueue = append(strat.pollQueue, ChunkDispatch[*testChunk]{
				Peer:    pid,
				ChunkID: &testChunk{ID: i, Data: []byte("bench")},
				Data:    []byte("bench"),
			})
		}
		s.drainPolls()
		for range numPeers {
			event := <-channelInbox
			completion, ok := event.(channelChunkSent)
			if !ok {
				b.Fatalf("unexpected completion event %T", event)
			}
			s.handleSendComplete(completion.peerID, completion.handle, completion.err, completion.size)
		}
	}
}

// BenchmarkOutboundLoopChunkThroughput measures the raw throughput of
// PeerConn's outbound loop processing chunk send events via chunk slots.
func BenchmarkOutboundLoopChunkThroughput(b *testing.B) {
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()

	streams := &blackholeOpener{}
	bcastOut := &blackholeStream{}
	p := &PeerConn{
		id:        "bench-peer",
		streams:   streams,
		ctrlOut:   bcastOut,
		ctrlQ:     make(chan peerCtrlEvent, ctrlQCap),
		lifecycle: newFIFO[peerCtrlEvent](),
		wakeCh:    make(chan struct{}, 1),
		ctx:       ctx,
		cancel:    cancel,
	}

	// Result sink: channel drained in background.
	var processed atomic.Int64
	resultCh := make(chan channelEvent, 1024)
	go func() {
		for range resultCh {
			processed.Add(1)
		}
	}()

	slotCh := make(chan slotUpdate, slotUpdateCap)
	done := make(chan struct{})
	go func() {
		p.runCtrlLoop(slotCh)
	}()
	go func() {
		p.runDataLoop(slotCh)
		close(done)
	}()

	// Register a session with a chunk slot so the outbound loop picks it up.
	chunkOutbox := make(chan peerSendChunk, 2)
	p.ctrlQ <- peerOpenSession{
		channelID:    "bench-channel",
		messageID:    "bench-msg",
		channelInbox: resultCh,
		chunkOutbox:  chunkOutbox,
	}
	// Wait for the control event to be processed.
	spinUntil(func() bool {
		return len(p.ctrlQ) == 0
	})

	b.ResetTimer()
	b.ReportAllocs()

	sessionDone := make(chan struct{})
	// The completion drain below is timed too; b.Loop would stop timing first.
	for i := range b.N {
		chunkOutbox <- peerSendChunk{
			peerID:      "bench-peer",
			channelID:   "bench-channel",
			messageID:   MessageID(fmt.Sprintf("msg-%d", i)),
			payload:     []byte("bench-payload"),
			resultCh:    resultCh,
			sessionDone: sessionDone,
		}
		select {
		case p.wakeCh <- struct{}{}:
		default:
		}
	}

	// Wait for all chunks to be processed.
	spinUntil(func() bool {
		return processed.Load() >= int64(b.N)
	})

	b.StopTimer()
	cancel()
	<-done
	close(resultCh)
}

// BenchmarkSubscriptionChurn measures the cost of rapidly adding and
// removing peers from the engine's topology. This exercises the event
// loop's membership tracking path.
func BenchmarkSubscriptionChurn(b *testing.B) {
	engine := NewEngine(EngineConfig{})
	defer engine.Close()

	scheme := newMockScheme()
	channel := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "bench-channel", scheme)
	defer channel.Stop()

	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		pid := transport.PeerID(fmt.Sprintf("churn-%d", i))
		conn, _ := newTestTransportPair(b.Context())
		peer := registerTestPeer(engine, pid, conn, ProtocolVersion(1), []ChannelID{"bench-channel"})
		engine.notifyPeerGone(peer)
	}
}

// dispatchStrategy returns chunks from a pre-filled benchmark poll queue.
type dispatchStrategy struct {
	pollQueue []ChunkDispatch[*testChunk]
}

func (cs *dispatchStrategy) HaveChunk(_ *testChunk) bool { return false }
func (cs *dispatchStrategy) VerifyChunk(_ transport.PeerID, _ *testChunk, _ []byte) Verdict {
	return VerdictAccepted
}
func (cs *dispatchStrategy) Verified() <-chan VerifyResult[*testChunk] { return nil }
func (cs *dispatchStrategy) DedupKey(_ *testChunk) []byte              { return nil }
func (cs *dispatchStrategy) AttachPeer(transport.PeerID)               {}
func (cs *dispatchStrategy) DetachPeer(transport.PeerID, bool)         {}

func (cs *dispatchStrategy) TakeChunk(_ transport.PeerID, _ *testChunk, _ []byte, _ *DedupCancel) (Verdict, bool, error) {
	return VerdictAccepted, false, nil
}

func (cs *dispatchStrategy) Decode() ([]byte, error) { return nil, nil }

func (cs *dispatchStrategy) RoutingUpdate(_ transport.PeerID, _ *testRouting) ([]ChunkHandle, error) {
	return nil, nil
}

func (cs *dispatchStrategy) PollChunks() []ChunkDispatch[*testChunk] {
	if len(cs.pollQueue) == 0 {
		return nil
	}
	chunks := cs.pollQueue
	cs.pollQueue = nil
	return chunks
}

func (cs *dispatchStrategy) PollRouting(force bool) (*testRouting, bool) {
	return nil, false
}

func (cs *dispatchStrategy) ChunkSent(_ transport.PeerID, _ ChunkHandle, _ error) {}

func (cs *dispatchStrategy) Progress() (have, need int) { return 0, 0 }

func (cs *dispatchStrategy) Work() <-chan struct{} { return nil }

func (cs *dispatchStrategy) Close() error { return nil }

// spinUntil busy-waits for the condition, yielding the processor between checks.
func spinUntil(cond func() bool) {
	for !cond() {
		runtime.Gosched()
	}
}

// blackholeOpener discards stream writes to benchmark outbound loop overhead.
type blackholeOpener struct{}

func (*blackholeOpener) OpenUniStream(_ context.Context, _ wire.Selector) (ethp2p.SendStream, error) {
	return &blackholeStream{}, nil
}

type blackholeStream struct{}

func (s *blackholeStream) Write(p []byte) (int, error)        { return len(p), nil }
func (s *blackholeStream) Close() error                       { return nil }
func (s *blackholeStream) CancelWrite(_ wire.Code)            {}
func (s *blackholeStream) SetWriteDeadline(_ time.Time) error { return nil }
