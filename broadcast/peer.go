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
	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/internal/ctxutil"
	"github.com/ethp2p/ethp2p/wire"
)

const (
	handshakeTimeout = 10 * time.Second
	streamQueueCap   = 64
	// maxOpeningReads bounds concurrent opening-message reads per peer, so a
	// peer that stalls its streams stalls only its own openers.
	maxOpeningReads = 8
	// openingTimeout bounds the read of one opening message, and of the
	// PREAMBLE that follows OPEN.
	openingTimeout = 5 * time.Second
)

// peerHandle is the stack's handle for one binding (*ethp2p.Peer).
type peerHandle interface {
	OpenUniStream(context.Context) (ethp2p.SendStream, error)
	Close(wire.Code)
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
	handle    peerHandle
	// subs is the remote's channel subscriptions: set by the handshake, then
	// updated by its Subscribe and Unsubscribe. Owned by the engine actor.
	subs map[ChannelID]struct{}
	// goneReported is set once OnPeerGone reported this binding. Engine
	// actor only: Run's exit and PeerDown both reach handlePeerGone.
	goneReported bool

	// unread holds streams whose opening message no opener has read yet.
	// Each holds QUIC stream credit, which bounds the FIFO.
	unread *fifo[ethp2p.ReceiveStream]
	// handshakeIn carries the peer's control stream and its decoded
	// HANDSHAKE. The opener that wins bcastTaken owns its one slot, so its
	// send never blocks.
	handshakeIn chan inboundHandshake
	bcastTaken  atomic.Bool
	// parkMu guards handshook and parked. Openings read before the handshake
	// completes wait in parked; each holds an open stream, so QUIC stream
	// credit bounds it. They are dispatched once handshakeDone closes.
	parkMu    sync.Mutex
	handshook bool
	parked    []opening
	// The engine closes handshakeDone once it has bound this peer's
	// channels; Run starts the control loops only after that.
	handshakeDone chan struct{}
	// handlersMu guards closing: Run starts goroutines under it only while
	// closing is false, and Close sets it, so every started goroutine's wg.Go
	// happens-before Close's wg.Wait.
	handlersMu sync.Mutex
	closing    bool

	// We use a CoW map because channel subscriptions change infrequently,
	// and this approach skips a channel hop via the Engine and provides
	// an O(1) lookup.
	channelInboxes atomic.Pointer[map[ChannelID]*channelHandle]

	// ctrlState coalesces desired outbound channel and session state until the
	// control loop emits each transition.
	ctrlState *ctrlOutState

	// ctrlOut is our outbound control stream (we opened it, we write to it).
	// ctrlIn is the peer's outbound control stream (they opened it, we read
	// from it). Both set during handshake, then owned by the control loop and
	// control reader respectively.
	ctrlOut ethp2p.SendStream
	ctrlIn  ethp2p.ReceiveStream

	// wakeCh is a coalesce notification: sessions signal this channel
	// (buffered 1) after depositing a chunk into their per-session slot.
	// The outbound loop wakes and iterates all slots.
	wakeCh chan struct{}

	// sessionSem bounds inbound session readers, each of which keeps a slot
	// for its stream's lifetime. Excess session streams are reset because
	// their completion needs chunk streams.
	sessionSem chan struct{}

	ctx context.Context
	// cancel ends the binding with a cause; the first cause wins. A nil
	// cause (PeerDown, Close, engine shutdown) reports as no error.
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup

	closeOnce sync.Once
}

// opening is an admitted stream with its decoded opening message: open for a
// session stream, header for a chunk stream.
type opening struct {
	stream ethp2p.ReceiveStream
	open   *bcastpb.Sess_Open
	header *bcastpb.Chunk_Header
}

type inboundHandshake struct {
	stream ethp2p.ReceiveStream
	msg    *bcastpb.Bcast_Handshake
}

func newPeerConn(engine *Engine, bindCtx context.Context, id identity.PeerID, handle peerHandle) *PeerConn {
	ctx, cancelContext := context.WithCancelCause(bindCtx)
	stopEngineCancel := context.AfterFunc(engine.ctx, func() { cancelContext(nil) })
	cancel := func(cause error) {
		stopEngineCancel()
		cancelContext(cause)
	}
	p := &PeerConn{
		id:            id,
		handle:        handle,
		ctrlState:     newCtrlOutState(),
		wakeCh:        make(chan struct{}, 1),
		sessionSem:    make(chan struct{}, streamQueueCap),
		unread:        newFIFO[ethp2p.ReceiveStream](),
		handshakeIn:   make(chan inboundHandshake, 1),
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

	// Openers start before the handshake: the peer's HANDSHAKE is itself an
	// opening message, and may arrive after its session and chunk streams.
	if !p.startGoroutines(func() {
		for range maxOpeningReads {
			p.wg.Go(p.runOpener)
		}
	}) {
		return
	}
	// A failed handshake ends the binding with its cause; handlePeerGone
	// reports it and closes the binding, disposing its queued streams.
	channels, err := p.handshake()
	if err != nil {
		p.cancel(fmt.Errorf("broadcast handshake: %w", err))
		return
	}
	// Control loops start only after the engine has bound this peer's
	// channels, so readers can route at once and the ctrl reader's events
	// follow the handshake on the engine actor. Openings read earlier wait in
	// parked; outbound control state waits for the control loop.
	p.engine.post(nil, func() { p.engine.handlePeerHandshake(p, channels) })
	select {
	case <-p.handshakeDone:
	case <-p.ctx.Done():
	}
	controlLoopsStarted = p.startGoroutines(func() {
		// The slot channel connects ctrl and data: ctrl notifies data when
		// chunk slots appear or disappear.
		slotCh := make(chan slotUpdate, slotUpdateCap)
		p.wg.Go(func() { p.runCtrlLoop(slotCh) })
		p.wg.Go(func() { p.runDataLoop(slotCh) })
		p.wg.Go(p.runCtrlReader)
		// Dispatching starts session readers with wg.Go, so it runs as a
		// wg member.
		p.wg.Go(p.dispatchParked)
	})
	if !controlLoopsStarted {
		return
	}
	<-p.ctx.Done()
}

// startGoroutines runs start under handlersMu unless the binding has ended or
// Close has begun, and reports whether it ran.
func (p *PeerConn) startGoroutines(start func()) bool {
	p.handlersMu.Lock()
	defer p.handlersMu.Unlock()
	if p.ctx.Err() != nil || p.closing {
		return false
	}
	start()
	return true
}

// enqueueStream hands an incoming stream to the openers. It runs on the
// engine actor and never blocks.
func (p *PeerConn) enqueueStream(stream ethp2p.ReceiveStream) {
	if p.ctx.Err() != nil || !p.unread.push(stream) {
		stream.CancelRead(wire.Unspecified)
	}
}

// disposeQueuedStreams cancels every stream the binding still holds. Close
// calls it after joining the openers, so nothing is added concurrently, and
// the engine actor that enqueues streams is the one calling Close.
func (p *PeerConn) disposeQueuedStreams() {
	for _, stream := range p.unread.close() {
		stream.CancelRead(wire.Unspecified)
	}
	p.parkMu.Lock()
	parked := p.parked
	p.parked = nil
	p.parkMu.Unlock()
	for _, o := range parked {
		o.stream.CancelRead(wire.Unspecified)
	}
	select {
	case in := <-p.handshakeIn:
		in.stream.CancelRead(wire.Unspecified)
	default:
	}
}

// handshake performs a symmetric handshake: each side opens its outbound
// control stream and writes its HANDSHAKE while an opener reads the peer's
// HANDSHAKE from the inbound control stream. Session and chunk openings wait
// in parked until this handshake completes.
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
	hsMsg := &bcastpb.Bcast_Handshake{Channels: channelStrings}

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
		out, err := p.handle.OpenUniStream(ctx)
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

	// A HANDSHAKE left in handshakeIn is cancelled by disposeQueuedStreams.
	var in inboundHandshake
	var err error
	select {
	case in = <-p.handshakeIn:
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
			in.stream.CancelRead(streamFailureCode(w.err))
		}
		if w.err == nil {
			w.out.CancelWrite(streamFailureCode(err))
		}
		return nil, errors.Join(err, w.err)
	}

	p.ctrlOut, p.ctrlIn = w.out, in.stream
	remoteChannels := make([]ChannelID, len(in.msg.Channels))
	for i, t := range in.msg.Channels {
		remoteChannels[i] = ChannelID(t)
	}
	return remoteChannels, nil
}

// Close stops the peer and waits for its broadcast goroutines.
func (p *PeerConn) Close() {
	p.closeOnce.Do(func() {
		p.stop()
		p.handlersMu.Lock()
		p.closing = true
		p.handlersMu.Unlock()
		p.wg.Wait()
		p.disposeQueuedStreams()
	})
}

func (p *PeerConn) stop() {
	p.cancel(nil)
	p.ctrlState.close()
}
