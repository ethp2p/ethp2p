package broadcast

import (
	"context"
	"errors"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/internal/ctxutil"
	"github.com/ethp2p/ethp2p/wire"
)

const (
	handshakeTimeout = 10 * time.Second
	streamQueueCap   = 64
)

type uniStreamOpener interface {
	OpenUniStream(context.Context, wire.Protocol) (ethp2p.SendStream, error)
}

// PeerConn holds the broadcast state for one connected remote peer.
// The application owns the borrowed connection; PeerConn only owns streams
// handed to this broadcast binding and never closes the connection itself.
type PeerConn struct {
	engine *Engine
	// announced is the channel list our Hello carries. The engine sets it
	// before Run starts and reads it when the handshake completes.
	announced []ChannelID
	id        identity.PeerID
	streams   uniStreamOpener
	// subs is the remote's channel subscriptions: set by the handshake, then
	// updated by its Subscribe and Unsubscribe. Owned by the engine actor.
	subs map[ChannelID]struct{}

	bcastAccepted bool // engine actor only
	bcastIn       chan ethp2p.ReceiveStream
	// The handoff queues keep a stream that arrives before the BCAST
	// handshake from blocking the engine's event actor. Run starts the
	// dedicated accept loops, which start the owned stream readers, only once
	// the handshake completes.
	sessionIn chan ethp2p.ReceiveStream
	chunkIn   *fifo[ethp2p.ReceiveStream]
	// The engine closes handshakeDone once it has bound this peer's
	// channels; Run starts the workers only after that.
	handshakeDone chan struct{}
	// handlersMu guards startClaimed: Run starts its workers under it, and
	// Close claims the start under it, so once Close has claimed, no worker
	// can start, and every started worker's wg.Go happens-before Close's
	// wg.Wait.
	handlersMu   sync.Mutex
	startClaimed bool

	// We use a CoW map because channel subscriptions change infrequently,
	// and this approach skips a channel hop via the Engine and provides
	// an O(1) lookup.
	channelInboxes atomic.Pointer[map[ChannelID]*channelHandle]

	// ctrlState coalesces desired outbound channel and session state until the
	// control loop emits each transition.
	ctrlState *ctrlOutState

	// ctrlOut is our outbound BCAST stream (we opened it, we write to it).
	// bcastIn is the peer's outbound BCAST stream (they opened it, we read from it).
	// Both set during handshake, then owned by outbound loop and control reader respectively.
	ctrlOut ethp2p.SendStream
	ctrlIn  ethp2p.ReceiveStream

	// wakeCh is a coalesce notification: sessions signal this channel
	// (buffered 1) after depositing a chunk into their per-session slot.
	// The outbound loop wakes and iterates all slots.
	wakeCh chan struct{}

	// These semaphores bound concurrent inbound readers. Chunk readers hand
	// payloads to sessions; SESS readers keep a slot for the stream's lifetime.
	// Excess SESS streams are reset because their completion needs CHUNK.
	// CHUNK header readers instead wait for a slot.
	chunkSem   chan struct{}
	sessionSem chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closeOnce sync.Once
}

func newPeerConn(engine *Engine, bindCtx context.Context, id identity.PeerID, streams uniStreamOpener) *PeerConn {
	ctx, cancelContext := context.WithCancel(bindCtx)
	stopEngineCancel := context.AfterFunc(engine.ctx, cancelContext)
	cancel := func() {
		stopEngineCancel()
		cancelContext()
	}
	p := &PeerConn{
		id:            id,
		streams:       streams,
		ctrlState:     newCtrlOutState(),
		wakeCh:        make(chan struct{}, 1),
		chunkSem:      make(chan struct{}, maxInboundChunkStreams),
		sessionSem:    make(chan struct{}, streamQueueCap),
		bcastIn:       make(chan ethp2p.ReceiveStream, 1),
		sessionIn:     make(chan ethp2p.ReceiveStream, streamQueueCap),
		chunkIn:       newFIFO[ethp2p.ReceiveStream](),
		handshakeDone: make(chan struct{}),
		engine:        engine,
		ctx:           ctx,
		cancel:        cancel,
	}
	return p
}

func (p *PeerConn) channelInboxFor(channelID ChannelID) *channelHandle {
	m := p.channelInboxes.Load()
	if m == nil {
		return nil
	}
	return (*m)[channelID]
}

// BindChannel registers a channel inbox channel using atomic copy-on-write
// for lock-free reads on the dispatch hot path.
func (p *PeerConn) BindChannel(channelID ChannelID, inbox *channelHandle) {
	newMap := make(map[ChannelID]*channelHandle)
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
func (p *PeerConn) Run() {
	controlLoopsStarted := false
	defer func() {
		p.stop()
		p.wg.Wait()
		// Handshake may have succeeded just as Close canceled the context,
		// before the control loops could take ownership of these streams.
		if !controlLoopsStarted && p.ctrlIn != nil {
			p.ctrlIn.CancelRead(streamFailureCode(p.ctx.Err()))
		}
		if !controlLoopsStarted && p.ctrlOut != nil {
			p.ctrlOut.CancelWrite(streamFailureCode(p.ctx.Err()))
		}
		p.engine.post(nil, func() { p.engine.handlePeerGone(p) })
	}()

	// A failed handshake only returns: the deferred stop ends the peer and
	// handlePeerGone closes the binding, disposing its queued streams.
	channels, err := p.handshake()
	if err != nil {
		return
	}
	// Workers start only after the engine has bound this peer's channels, so
	// readers can route at once and the ctrl reader's events follow the
	// handshake on the engine actor. Streams arriving earlier wait in the
	// handoff queues; outbound control state waits for the control loop.
	p.engine.post(nil, func() { p.engine.handlePeerHandshake(p, channels) })
	select {
	case <-p.handshakeDone:
	case <-p.ctx.Done():
	}

	p.handlersMu.Lock()
	if p.ctx.Err() != nil || p.startClaimed {
		p.handlersMu.Unlock()
		return
	}
	p.startClaimed = true
	// The slot channel connects ctrl and data: ctrl notifies data when chunk
	// slots appear or disappear.
	slotCh := make(chan slotUpdate, slotUpdateCap)
	p.wg.Go(func() { p.runCtrlLoop(slotCh) })
	p.wg.Go(func() { p.runDataLoop(slotCh) })
	p.wg.Go(p.runCtrlReader)
	p.wg.Go(p.runSessionAcceptLoop)
	p.wg.Go(p.runChunkAcceptLoop)
	controlLoopsStarted = true
	p.handlersMu.Unlock()

	<-p.ctx.Done()
}

func (p *PeerConn) runSessionAcceptLoop() {
	for {
		select {
		case stream := <-p.sessionIn:
			p.acceptSession(stream)
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
func (p *PeerConn) enqueueStream(protocol wire.Protocol, stream ethp2p.ReceiveStream) {
	if p.ctx.Err() != nil {
		stream.CancelRead(wire.Unspecified)
		return
	}
	switch protocol {
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
			stream.CancelRead(wire.Unspecified)
		}
	case CHUNK:
		if !p.chunkIn.push(stream) {
			stream.CancelRead(wire.Unspecified)
		}
	}
}

func (p *PeerConn) disposeQueuedStreams() {
	for _, stream := range p.chunkIn.close() {
		stream.CancelRead(wire.Unspecified)
	}
	// Engine calls Close on the same actor that enqueues streams. Run only
	// reports completion; draining here therefore cannot race a final enqueue.
	for _, streams := range []chan ethp2p.ReceiveStream{p.bcastIn, p.sessionIn} {
		for streams != nil {
			select {
			case stream := <-streams:
				stream.CancelRead(wire.Unspecified)
			default:
				streams = nil
			}
		}
	}
}

// handshake performs a symmetric handshake: each side opens its outbound
// BCAST stream and writes its Handshake while it reads the peer's Handshake
// from the inbound BCAST stream.
// Data streams wait in their handoff queues until this handshake completes.
//
// Using two unidirectional streams, one per direction, is deliberate: it
// makes simultaneous open the normal case rather than a race to resolve.
// Neither side needs a role (initiator or responder), so there is no
// tie-breaking and no crossed-open state; each side writes on the stream it
// opened and reads on the one it accepted. A single bidirectional stream
// would require both sides to agree on who opens it.
func (p *PeerConn) handshake() ([]ChannelID, error) {
	ctx, cancel := context.WithTimeout(p.ctx, handshakeTimeout)
	defer cancel()
	channelStrings := make([]string, len(p.announced))
	for i, t := range p.announced {
		channelStrings[i] = string(t)
	}
	hsMsg := &bcastpb.Bcast_Handshake{
		Version:  ProtocolV1,
		Channels: channelStrings,
	}

	// Both Handshakes may be larger than the initial stream window: only
	// maxBroadcastFrame bounds the channel list. Such a write completes only as the
	// peer reads, so the write must overlap the read; writing first and
	// reading second deadlocks when both frames are large. A failed half
	// cancels ctx to end the other, and the writer is joined on every exit.
	type written struct {
		out ethp2p.SendStream
		err error
	}
	writeCh := make(chan written, 1)
	go func() {
		out, err := p.streams.OpenUniStream(ctx, BCAST)
		if err == nil {
			stop := ctxutil.OnCancel(ctx, func() { out.CancelWrite(streamFailureCode(ctx.Err())) })
			err = writeFrame(out, handshakeFrame, hsMsg)
			stop()
			if err == nil {
				err = ctx.Err()
			}
			if err != nil {
				// Writes never cancel a side, so end it here before abandoning the stream.
				out.CancelWrite(streamFailureCode(err))
			}
		}
		if err != nil {
			cancel()
		}
		writeCh <- written{out, err}
	}()

	// An inbound stream left in bcastIn is cancelled by disposeQueuedStreams.
	var in ethp2p.ReceiveStream
	var response bcastpb.Bcast_Handshake
	var err error
	select {
	case in = <-p.bcastIn:
		stop := ctxutil.OnCancel(ctx, func() { in.CancelRead(streamFailureCode(ctx.Err())) })
		err = readFrame(in, handshakeFrame, &response)
		stop()
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			// Reads never cancel a side, so end it here with the read error's code.
			in.CancelRead(streamCancellationCode(errors.Join(err, ctx.Err())))
		}
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err != nil {
		cancel()
	}
	w := <-writeCh
	if err != nil || w.err != nil {
		// Each half ended its own stream on failure; end the one that succeeded.
		if err == nil {
			in.CancelRead(streamFailureCode(w.err))
		}
		if w.err == nil {
			w.out.CancelWrite(streamFailureCode(err))
		}
		return nil, errors.Join(err, w.err)
	}
	// ProtocolV1 is the only version: a lower one cannot be negotiated.
	if response.Version < ProtocolV1 {
		in.CancelRead(wire.Refused)
		w.out.CancelWrite(wire.Refused)
		return nil, ErrProtocolMismatch
	}

	p.ctrlOut, p.ctrlIn = w.out, in
	remoteChannels := make([]ChannelID, len(response.Channels))
	for i, t := range response.Channels {
		remoteChannels[i] = ChannelID(t)
	}
	return remoteChannels, nil
}

// Close stops the peer and waits for its broadcast goroutines.
func (p *PeerConn) Close() {
	p.closeOnce.Do(func() {
		p.stop()
		p.handlersMu.Lock()
		p.startClaimed = true
		p.handlersMu.Unlock()
		p.wg.Wait()
		p.disposeQueuedStreams()
	})
}

func (p *PeerConn) stop() {
	p.cancel()
	p.ctrlState.close()
}
