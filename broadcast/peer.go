package broadcast

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/internal/ctxutil"
	"github.com/ethp2p/ethp2p/internal/devhook"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/wire"
)

const (
	handshakeTimeout = 10 * time.Second
	ctrlQCap         = 16
	streamQueueCap   = 64
)

type uniStreamOpener interface {
	OpenUniStream(context.Context, wire.Selector) (ethp2p.SendStream, error)
}

// PeerConn holds the broadcast state for one connected remote peer.
// The application owns the borrowed connection; PeerConn only owns streams
// handed to this broadcast binding and never closes the connection itself.
type PeerConn struct {
	engine *Engine
	// peer is the stable root connection identity. It is separate from id
	// because reconnects can reuse an authenticated peer ID.
	peer    *ethp2p.Peer
	id      transport.PeerID
	version ProtocolVersion
	streams uniStreamOpener

	bcastAccepted atomic.Bool
	bcastIn       chan ethp2p.ReceiveStream
	// The handoff queues keep a stream that arrives before the BCAST
	// handshake from blocking the engine's event actor. Dedicated loops wait for
	// handshake completion and then start the owned stream readers.
	sessionIn chan ethp2p.ReceiveStream
	chunkIn   *fifo[ethp2p.ReceiveStream]
	// handshakeDone publishes the engine's channel bindings, not just the
	// completed wire handshake. Readers must wait before looking up channels.
	handshakeDone chan struct{}
	handshakeOnce sync.Once
	handshakeOK   bool
	handshakeErr  error
	handlersMu    sync.Mutex
	ready         bool

	// We use a CoW map because channel subscriptions change infrequently,
	// and this approach skips a channel hop via the Engine and provides
	// an O(1) lookup.
	channelInboxes atomic.Pointer[map[ChannelID]*channelDelivery]

	// ctrlQ carries control events (session lifecycle, routing,
	// subscriptions) to the outbound loop. Buffered to absorb bursts.
	ctrlQ chan peerCtrlEvent
	// Lifecycle FIFO nodes per peer <= 2 × live attachments + 1: at most one
	// queued open per attachment, separated by batches of adjacent closes.
	// Elision joins neighboring batches. Batch contents also retain already-open
	// attachments awaiting retirement; total memory is O(peers × per-peer live
	// session cap), plus application-owned local publishes. No network wait occurs
	// while enqueueing or retiring an attachment.
	lifecycle                  *fifo[peerCtrlEvent]
	chunkMu                    sync.Mutex
	heldChunks                 map[*heldChunk]chunkHolding
	liveSessions               map[*sessionLease]struct{}
	parkedChunks, queuedChunks int

	// ctrlOut is our outbound BCAST stream (we opened it, we write to it).
	// bcastIn is the peer's outbound BCAST stream (they opened it, we read from it).
	// Both set during handshake, then owned by outbound loop and control reader respectively.
	ctrlOut ethp2p.SendStream
	ctrlIn  ethp2p.ReceiveStream
	// ctrlOutEnded records a write failure so shutdown does not overwrite its
	// more specific cancellation code with Unspecified.
	ctrlOutEnded atomic.Bool

	// wakeCh is a coalesce notification: sessions signal this channel
	// (buffered 1) after depositing a chunk into their per-session slot.
	// The outbound loop wakes and iterates all slots.
	wakeCh chan struct{}

	// These semaphores bound concurrent inbound readers. Chunk readers hand
	// payloads to sessions; SESS readers keep a slot for the stream's lifetime.
	// Excess SESS streams are reset because their completion needs CHUNK.
	// CHUNK header readers instead wait for a slot outside handlersMu.
	chunkSem   chan struct{}
	sessionSem chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closeOnce sync.Once
}

// newPeerConn creates a PeerConn ready for handshake.
func newPeerConn(engine *Engine, bindCtx context.Context, id transport.PeerID, streams uniStreamOpener) *PeerConn {
	ctx, cancelContext := context.WithCancel(bindCtx)
	stopEngineCancel := context.AfterFunc(engine.ctx, cancelContext)
	cancel := func() {
		stopEngineCancel()
		cancelContext()
	}
	p := &PeerConn{
		streams:       streams,
		ctrlQ:         make(chan peerCtrlEvent, ctrlQCap),
		lifecycle:     newFIFO[peerCtrlEvent](),
		wakeCh:        make(chan struct{}, 1),
		chunkSem:      make(chan struct{}, engine.config.maxInboundChunkStreams()),
		sessionSem:    make(chan struct{}, streamQueueCap),
		bcastIn:       make(chan ethp2p.ReceiveStream, 1),
		sessionIn:     make(chan ethp2p.ReceiveStream, streamQueueCap),
		chunkIn:       newFIFO[ethp2p.ReceiveStream](),
		handshakeDone: make(chan struct{}),
		engine:        engine,
		ctx:           ctx,
		cancel:        cancel,
	}
	p.id = id
	return p
}

// channelInboxFor returns the channel event channel for the given channel, or nil.
func (p *PeerConn) channelInboxFor(channelID ChannelID) *channelDelivery {
	m := p.channelInboxes.Load()
	if m == nil {
		return nil
	}
	return (*m)[channelID]
}

// BindChannel registers a channel inbox channel using atomic copy-on-write
// for lock-free reads on the dispatch hot path.
func (p *PeerConn) BindChannel(channelID ChannelID, inbox *channelDelivery) {
	newMap := make(map[ChannelID]*channelDelivery)
	if old := p.channelInboxes.Load(); old != nil {
		maps.Copy(newMap, *old)
	}
	newMap[channelID] = inbox
	p.channelInboxes.Store(&newMap)
}

// UnbindChannel removes the channel inbox for the given channel using atomic
// copy-on-write.
func (p *PeerConn) UnbindChannel(channelID ChannelID) {
	old := p.channelInboxes.Load()
	if old == nil {
		return
	}
	if _, ok := (*old)[channelID]; !ok {
		return
	}
	newMap := maps.Clone(*old)
	delete(newMap, channelID)
	p.channelInboxes.Store(&newMap)
}

// Run drives the peer through its lifecycle: handshake, active, loops.
// It blocks until the peer is closed or the context is cancelled.
// ourChannels is the engine's channel list captured at spawn time.
func (p *PeerConn) Run(ourChannels []ChannelID) error {
	controlLoopsStarted := false
	defer func() {
		p.stop()
		p.handlersMu.Lock()
		p.ready = false
		p.handlersMu.Unlock()
		p.wg.Wait()
		// Handshake may have succeeded just as Close canceled the context,
		// before the control loops could take ownership of these streams.
		if !controlLoopsStarted && p.ctrlIn != nil {
			p.ctrlIn.CancelRead(streamFailureCode(p.ctx.Err()))
		}
		if !controlLoopsStarted && p.ctrlOut != nil {
			p.ctrlOut.CancelWrite(streamFailureCode(p.ctx.Err()))
		}
		p.engine.notifyPeerGone(p)
	}()

	// These loops start before handshake so a SESS or CHUNK notification may
	// wait for BCAST without preventing the BCAST notification from arriving.
	// Close uses this same lock to prohibit new workers before joining them.
	p.handlersMu.Lock()
	if err := p.ctx.Err(); err != nil {
		p.handlersMu.Unlock()
		return err
	}
	p.wg.Go(p.runSessionAcceptLoop)
	p.wg.Go(p.runChunkAcceptLoop)
	p.handlersMu.Unlock()

	hsCtx, hsCancel := context.WithTimeout(p.ctx, handshakeTimeout)
	defer hsCancel()

	version, channels, err := p.handshake(hsCtx, ourChannels)
	hsCancel()
	if err != nil {
		p.finishHandshake(false, err)
		p.engine.onPeerHandshake(p, nil, err)
		return fmt.Errorf("handshake: %w", err)
	}

	p.version = version
	if !p.engine.config.hooks.Wait(p.ctx, devhook.Site{Point: devhook.PointHandshake, Peer: string(p.id)}) {
		p.finishHandshake(false, p.ctx.Err())
		return p.ctx.Err()
	}

	// Start ctrl and data loops before any session attachment/control
	// events can arrive via ctrlQ. The slot channel connects them:
	// ctrl notifies data when chunk slots appear or disappear.
	p.handlersMu.Lock()
	if err := p.ctx.Err(); err != nil {
		p.handlersMu.Unlock()
		return err
	}
	slotCh := make(chan slotUpdate, slotUpdateCap)
	p.wg.Go(func() { p.runCtrlLoop(slotCh) })
	p.wg.Go(func() { p.runDataLoop(slotCh) })
	p.wg.Go(p.runCtrlReader)
	p.ready = true
	controlLoopsStarted = true
	p.handlersMu.Unlock()

	p.engine.onPeerHandshake(p, channels, nil)
	<-p.ctx.Done()
	return nil
}

func (p *PeerConn) runSessionAcceptLoop() {
	for {
		select {
		case stream := <-p.sessionIn:
			if stream != nil {
				p.acceptSession(stream)
			}
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *PeerConn) runChunkAcceptLoop() {
	for {
		if p.ctx.Err() != nil {
			return
		}
		if stream, ok := p.chunkIn.pop(); ok {
			p.acceptChunk(stream)
			continue
		}
		select {
		case <-p.chunkIn.ready:
		case <-p.ctx.Done():
			return
		}
	}
}

// enqueueStream transfers an incoming stream from the root Stack adapter to
// the stream-class loop. Unread CHUNK streams retain QUIC credit, bounding
// that FIFO. SESS keeps a capacity bound because it depends on CHUNK progress.
func (p *PeerConn) enqueueStream(selector wire.Selector, stream ethp2p.ReceiveStream) {
	if stream == nil {
		return
	}
	if p.ctx.Err() != nil {
		stream.CancelRead(wire.Unspecified)
		return
	}
	switch selector {
	case BCAST:
		p.acceptBcast(stream)
	case SESS:
		// SESS completion depends on CHUNK progress. Refuse excess streams
		// so they cannot retain all credit needed to deliver those chunks.
		select {
		case p.sessionIn <- stream:
		case <-p.ctx.Done():
			stream.CancelRead(wire.Unspecified)
		default:
			stream.CancelRead(wire.Overloaded)
		}
	case CHUNK:
		if !p.chunkIn.push(stream) {
			stream.CancelRead(wire.Unspecified)
		}
	default:
		stream.CancelRead(wire.Refused)
	}
}

func (p *PeerConn) disposeQueuedStreams() {
	p.notifyCreatorDeparture()
	p.cancelHeldChunks()
	p.lifecycle.close()
	for _, stream := range p.chunkIn.close() {
		stream.CancelRead(wire.Unspecified)
	}
	// Engine calls Close on the same actor that enqueues streams. Run only
	// reports completion; draining here therefore cannot race a final enqueue.
	for _, streams := range []chan ethp2p.ReceiveStream{p.bcastIn, p.sessionIn} {
		for streams != nil {
			select {
			case stream := <-streams:
				if stream != nil {
					stream.CancelRead(wire.Unspecified)
				}
			default:
				streams = nil
			}
		}
	}
}

// handshake performs a symmetric handshake: both sides concurrently open
// their outbound BCAST stream (write BCAST preamble + Handshake) and
// accept the peer's inbound BCAST stream (read BCAST preamble + Handshake).
// Data-stream queues wait independently for this handshake to complete.
//
// Using two unidirectional streams, one per direction, is deliberate: it
// makes simultaneous open the normal case rather than a race to resolve.
// Neither side needs a role (initiator or responder), so there is no
// tie-breaking and no crossed-open state; each side writes on the stream it
// opened and reads on the one it accepted. A single bidirectional stream
// would require both sides to agree on who opens it.
func (p *PeerConn) handshake(ctx context.Context, ourChannels []ChannelID) (ProtocolVersion, []ChannelID, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	channelStrings := make([]string, len(ourChannels))
	for i, t := range ourChannels {
		channelStrings[i] = string(t)
	}

	hsMsg := &bcastpb.Bcast{
		Message: &bcastpb.Bcast_PeerHandshake{
			PeerHandshake: &bcastpb.Bcast_Handshake{
				Version:  ProtocolV1,
				Channels: channelStrings,
			},
		},
	}

	type writeResult struct {
		stream ethp2p.SendStream
		err    error
	}
	writeCh := make(chan writeResult, 1)

	// Send while reading, and join the writer on every exit. A failed half
	// cancels the other half's stream I/O without closing the connection.
	go func() {
		var result writeResult
		defer func() {
			if result.err != nil {
				cancel()
			}
			writeCh <- result
		}()
		result.stream, result.err = p.streams.OpenUniStream(ctx, BCAST)
		if result.err != nil {
			return
		}
		stop := ctxutil.OnCancel(ctx, func() { result.stream.CancelWrite(streamFailureCode(ctx.Err())) })
		result.err = WriteFrame(result.stream, hsMsg)
		stop()
		if result.err == nil {
			result.err = ctx.Err()
		}
	}()

	var incoming ethp2p.ReceiveStream
	var response bcastpb.Bcast
	var readErr error
	var readFrameSucceeded bool
	rejectionCode := wire.Unspecified
	select {
	case incoming = <-p.bcastIn:
		stop := ctxutil.OnCancel(ctx, func() { incoming.CancelRead(streamFailureCode(ctx.Err())) })
		readErr = ReadFrame(incoming, &response)
		stop()
		readFrameSucceeded = readErr == nil
		if readErr != nil {
			// No effect if the read failed, which already cancelled the stream;
			// ends it for a malformed frame.
			incoming.CancelRead(wire.Unspecified)
		}
		if readErr == nil {
			readErr = ctx.Err()
		}
	case <-ctx.Done():
		readErr = ctx.Err()
	}
	if readErr == nil && response.GetPeerHandshake() == nil {
		readErr = ErrUnexpectedMsgType
		rejectionCode = wire.Refused
	}
	if readErr != nil {
		cancel()
	}
	written := <-writeCh
	err := errors.Join(readErr, written.err)
	var peerVersion uint32
	if err == nil {
		peerVersion, err = validateProtocolVersion(response.GetPeerHandshake().Version)
		if err != nil {
			rejectionCode = wire.Refused
		}
	}
	if err != nil {
		if written.stream != nil && written.err == nil {
			code := rejectionCode
			if code == wire.Unspecified {
				code = streamFailureCode(readErr)
				if readErr == nil {
					code = streamFailureCode(written.err)
				}
			}
			written.stream.CancelWrite(code)
		}
		if incoming != nil && readFrameSucceeded {
			code := rejectionCode
			if code == wire.Unspecified {
				code = streamFailureCode(readErr)
				if readErr == nil {
					code = streamFailureCode(written.err)
				}
			}
			incoming.CancelRead(code)
		}
		return 0, nil, err
	}

	p.ctrlOut = written.stream
	p.ctrlIn = incoming

	channels := response.GetPeerHandshake().Channels
	remoteChannels := make([]ChannelID, len(channels))
	for i, t := range channels {
		remoteChannels[i] = ChannelID(t)
	}
	return ProtocolVersion(peerVersion), remoteChannels, nil
}

// Close stops the peer and waits for its broadcast goroutines.
func (p *PeerConn) Close() {
	p.closeOnce.Do(func() {
		p.stop()
		p.handlersMu.Lock()
		p.ready = false
		p.handlersMu.Unlock()
		p.wg.Wait()
		p.disposeQueuedStreams()
	})
}

func (p *PeerConn) stop() {
	p.cancel()
	p.lifecycle.close()
	p.finishHandshake(false, p.ctx.Err())
}

func (p *PeerConn) finishHandshake(ok bool, err error) {
	p.handshakeOnce.Do(func() {
		p.handshakeOK = ok
		p.handshakeErr = err
		close(p.handshakeDone)
	})
}

// ID returns the peer's ID. Safe to call after handshake completes.
func (p *PeerConn) ID() transport.PeerID {
	return p.id
}

func validateProtocolVersion(peerVersion uint32) (uint32, error) {
	v := min(peerVersion, ProtocolV1)
	if v != ProtocolV1 {
		return 0, ErrProtocolMismatch
	}
	return v, nil
}
