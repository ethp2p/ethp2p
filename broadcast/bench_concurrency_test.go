package broadcast

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/transport"
)

// BenchmarkSessionDispatchFanout measures the throughput of the session
// dispatch path: drainPolls -> sendChunk -> PeerConn slot enqueue.
//
// Each iteration adds one chunk to the strategy's poll queue and calls
// drainPolls, then waits for it to be pushed to all peer outbound
// queues. The countingStrategy counts dispatches atomically.
func BenchmarkSessionDispatchFanout(b *testing.B) {
	for _, numPeers := range []int{1, 10, 50} {
		b.Run(fmt.Sprintf("peers=%d", numPeers), func(b *testing.B) {
			benchmarkSessionDispatchFanout(b, numPeers)
		})
	}
}

func benchmarkSessionDispatchFanout(b *testing.B, numPeers int) {
	var committed atomic.Int64
	strat := &countingStrategy{
		committed: &committed,
		numPeers:  numPeers,
		peers:     make(map[PeerID]struct{}),
	}

	channelInbox := make(chan channelEvent, 4096)

	tr := &Channel[*testChunk, *testRouting, *testPreamble]{
		engine: &Engine{config: EngineConfig{Observer: NoOpObserver{}}},
		id:     "bench-channel",
		scheme: Scheme[*testChunk, *testRouting, *testPreamble]{
			NewCI: func() *testChunk { return &testChunk{} },
			NewR:  func() *testRouting { r := testRouting{}; return &r },
		},
		inbox: channelInbox,
		ctx:   context.Background(),
	}
	s := tr.newSession("bench-msg", []byte("preamble"), false, strat)
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peerIDs := make([]PeerID, numPeers)
	for i := range numPeers {
		pid := PeerID(fmt.Sprintf("p%d", i))
		peerIDs[i] = pid
		peer := testPeer(pid)
		s.handlePeerAttached(peer)
		// Drain ctrlQ and chunk slots, send completions back.
		go func(p *PeerConn) {
			for {
				select {
				case ctrl := <-p.ctrlQ:
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

	// Wait for all peers to be attached.
	spinUntil(func() bool {
		return committed.Load() >= int64(numPeers)
	})
	committed.Store(0)

	// Drain channel inbox in background, feeding send completions back to the session.
	go func() {
		for {
			select {
			case evt := <-channelInbox:
				if sc, ok := evt.(channelChunkSent); ok {
					s.handleSendComplete(sc.peerID, sc.handle, sc.err, sc.size)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		// Enqueue a dispatch for each peer.
		for _, pid := range peerIDs {
			strat.pollQueue = append(strat.pollQueue, ChunkDispatch[*testChunk]{
				Peer:    pid,
				ChunkID: &testChunk{ID: i, Data: []byte("bench")},
				Data:    []byte("bench"),
			})
		}
		s.drainPolls()
		// Wait for all peers to have this chunk committed.
		target := int64((i + 1) * numPeers)
		spinUntil(func() bool {
			return committed.Load() >= target
		})
	}
}

// BenchmarkOutboundLoopChunkThroughput measures the raw throughput of
// PeerConn's outbound loop processing chunk send events via chunk slots.
func BenchmarkOutboundLoopChunkThroughput(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn := &blackholeTransport{ctx: ctx}
	bcastOut := &blackholeStream{}
	p := &PeerConn{
		id:      "bench-peer",
		conn:    conn,
		ctrlOut: bcastOut,
		ctrlQ:   make(chan peerCtrlEvent, ctrlQCap),
		wakeCh:  make(chan struct{}, 1),
		ctx:     ctx,
		cancel:  cancel,
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

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		pid := PeerID(fmt.Sprintf("churn-%d", i))
		conn, _ := newTestTransportPair(context.Background())
		registerTestPeer(engine, pid, conn, ProtocolVersion(1), []ChannelID{"bench-channel"})
		engine.NotifyPeerGone(pid)
	}
}

// countingStrategy is a minimal Strategy for benchmarks. It returns
// chunks from a pre-filled poll queue and counts dispatches atomically
// for lock-free synchronization.
type countingStrategy struct {
	committed *atomic.Int64
	numPeers  int
	peers     map[PeerID]struct{}
	pollQueue []ChunkDispatch[*testChunk]
}

func (cs *countingStrategy) HaveChunk(_ *testChunk) bool { return false }
func (cs *countingStrategy) VerifyChunk(_ PeerID, _ *testChunk, _ []byte) Verdict {
	return VerdictAccepted
}
func (cs *countingStrategy) Verified() <-chan VerifyResult[*testChunk] { return nil }
func (cs *countingStrategy) DedupKey(_ *testChunk) []byte              { return nil }
func (cs *countingStrategy) AttachPeer(peer PeerID, _ *PeerSessionStats) {
	cs.peers[peer] = struct{}{}
	cs.committed.Add(1) // signal that peer is attached
}

func (cs *countingStrategy) DetachPeer(peer PeerID, _ bool) {
	delete(cs.peers, peer)
}

func (cs *countingStrategy) TakeChunk(_ PeerID, _ *testChunk, _ []byte, _ *DedupCancel) (Verdict, bool, error) {
	return VerdictAccepted, false, nil
}

func (cs *countingStrategy) Decode() ([]byte, error) { return nil, nil }

func (cs *countingStrategy) RoutingUpdate(_ PeerID, _ *testRouting) ([]ChunkHandle, error) {
	return nil, nil
}

func (cs *countingStrategy) PollChunks() []ChunkDispatch[*testChunk] {
	if len(cs.pollQueue) == 0 {
		return nil
	}
	chunks := cs.pollQueue
	cs.pollQueue = nil
	cs.committed.Add(int64(len(chunks)))
	return chunks
}

func (cs *countingStrategy) PollRouting(force bool) (*testRouting, bool) {
	return nil, false
}

func (cs *countingStrategy) ChunkSent(_ PeerID, _ ChunkHandle, _ error) {}

func (cs *countingStrategy) Progress() (have, need int) { return 0, 0 }

func (cs *countingStrategy) Work() <-chan struct{} { return nil }

func (cs *countingStrategy) Close() error { return nil }

// spinUntil busy-waits for the condition, yielding the processor between checks.
func spinUntil(cond func() bool) {
	for !cond() {
		runtime.Gosched()
	}
}

// blackholeTransport implements transport.Conn with streams that discard
// all writes immediately. Used in benchmarks to measure outbound loop
// throughput without transport I/O overhead.
type blackholeTransport struct {
	ctx context.Context
}

func (t *blackholeTransport) SupportsStreams() bool              { return true }
func (t *blackholeTransport) SupportsDatagrams() bool            { return false }
func (t *blackholeTransport) Close() error                       { return nil }
func (t *blackholeTransport) ConnectionStats() (uint64, uint64)  { return 0, 0 }
func (t *blackholeTransport) Direction() transport.ConnDirection { return transport.Outbound }
func (t *blackholeTransport) AuthInfo() transport.AuthInfo {
	return testAuthInfo("bench-local", "bench-remote")
}

func (t *blackholeTransport) OpenStream(_ context.Context) (transport.Stream, error) {
	return &blackholeStream{}, nil
}

func (t *blackholeTransport) AcceptStream(ctx context.Context) (transport.Stream, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (t *blackholeTransport) OpenUniStream(_ context.Context) (transport.SendStream, error) {
	return &blackholeStream{}, nil
}

func (t *blackholeTransport) AcceptUniStream(ctx context.Context) (transport.ReceiveStream, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (t *blackholeTransport) SendDatagram(_ context.Context, _ []byte) error { return nil }

func (t *blackholeTransport) RecvDatagram(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type blackholeStream struct{}

func (s *blackholeStream) Read([]byte) (int, error)           { return 0, io.EOF }
func (s *blackholeStream) Write(p []byte) (int, error)        { return len(p), nil }
func (s *blackholeStream) Close() error                       { return nil }
func (s *blackholeStream) CancelRead(_ uint64)                {}
func (s *blackholeStream) CancelWrite(_ uint64)               {}
func (s *blackholeStream) Reset() error                       { return nil }
func (s *blackholeStream) SetDeadline(_ time.Time) error      { return nil }
func (s *blackholeStream) SetReadDeadline(_ time.Time) error  { return nil }
func (s *blackholeStream) SetWriteDeadline(_ time.Time) error { return nil }
