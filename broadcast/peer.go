package broadcast

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/protocol"
	"github.com/ethp2p/ethp2p/transport"
)

const (
	handshakeTimeout = 10 * time.Second
	ctrlQCap         = 16
)

// PeerConn holds the broadcast state for one connected remote peer. Stack owns
// the connection and closes it after PeerConn signals that it has stopped.
type PeerConn struct {
	engine  *Engine
	id      PeerID
	version ProtocolVersion
	conn    transport.Conn

	bcastAccepted atomic.Bool
	bcastIn       chan transport.ReceiveStream
	handshakeDone chan struct{}
	handshakeOnce sync.Once
	handshakeOK   bool
	handlersMu    sync.Mutex
	ready         bool

	// We use a CoW map because channel subscriptions change infrequently,
	// and this approach skips a channel hop via the Engine and provides
	// an O(1) lookup.
	channelInboxes atomic.Pointer[map[ChannelID]chan<- channelEvent]

	// ctrlQ carries control events (session lifecycle, routing,
	// subscriptions) to the outbound loop. Buffered to absorb bursts.
	ctrlQ chan peerCtrlEvent

	// ctrlOut is our outbound BCAST stream (we opened it, we write to it).
	// bcastIn is the peer's outbound BCAST stream (they opened it, we read from it).
	// Both set during handshake, then owned by outbound loop and control reader respectively.
	ctrlOut transport.SendStream
	ctrlIn  transport.ReceiveStream

	// wakeCh is a coalesce notification: sessions signal this channel
	// (buffered 1) after depositing a chunk into their per-session slot.
	// The outbound loop wakes and iterates all slots.
	wakeCh chan struct{}

	// chunkSem bounds concurrent inbound chunk stream processing.
	chunkSem chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	done   chan struct{}

	stopOnce  sync.Once
	closeOnce sync.Once
}

// newPeerConn creates a PeerConn ready for handshake.
func newPeerConn(engine *Engine, bindCtx context.Context, conn transport.Conn) *PeerConn {
	ctx, cancelContext := context.WithCancel(bindCtx)
	stopEngineCancel := context.AfterFunc(engine.ctx, cancelContext)
	cancel := func() {
		stopEngineCancel()
		cancelContext()
	}
	p := &PeerConn{
		conn:          conn,
		ctrlQ:         make(chan peerCtrlEvent, ctrlQCap),
		wakeCh:        make(chan struct{}, 1),
		chunkSem:      make(chan struct{}, engine.config.maxInboundChunkStreams()),
		bcastIn:       make(chan transport.ReceiveStream, 1),
		handshakeDone: make(chan struct{}),
		done:          make(chan struct{}),
		engine:        engine,
		ctx:           ctx,
		cancel:        cancel,
	}
	return p
}

// channelInboxFor returns the channel event channel for the given channel, or nil.
func (p *PeerConn) channelInboxFor(channelID ChannelID) chan<- channelEvent {
	m := p.channelInboxes.Load()
	if m == nil {
		return nil
	}
	return (*m)[channelID]
}

// BindChannel registers a channel inbox channel using atomic copy-on-write
// for lock-free reads on the dispatch hot path.
func (p *PeerConn) BindChannel(channelID ChannelID, inbox chan<- channelEvent) {
	newMap := make(map[ChannelID]chan<- channelEvent)
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
	defer p.stop()

	hsCtx, hsCancel := context.WithTimeout(p.ctx, handshakeTimeout)
	defer hsCancel()

	peerID, version, channels, err := p.handshake(hsCtx, ourChannels)
	if err != nil {
		p.finishHandshake(false)
		p.engine.onPeerHandshake(p, nil, err)
		return fmt.Errorf("handshake: %w", err)
	}

	p.id = peerID
	p.version = version

	// Start ctrl and data loops before any session attachment/control
	// events can arrive via ctrlQ. The slot channel connects them:
	// ctrl notifies data when chunk slots appear or disappear.
	slotCh := make(chan slotUpdate, slotUpdateCap)
	p.wg.Go(func() { p.runCtrlLoop(slotCh) })
	p.wg.Go(func() { p.runDataLoop(slotCh) })
	p.wg.Go(p.runCtrlReader)
	p.handlersMu.Lock()
	p.ready = true
	p.handlersMu.Unlock()
	p.finishHandshake(true)

	p.engine.onPeerHandshake(p, channels, nil)
	<-p.ctx.Done()
	p.handlersMu.Lock()
	p.ready = false
	p.handlersMu.Unlock()
	p.wg.Wait()

	// All goroutines exited; notify engine so it can unbind the peer
	// from channels and clean up.
	p.engine.NotifyPeerGone(p.id)
	return nil
}

// handshake performs a symmetric handshake: both sides concurrently open
// their outbound BCAST stream (write BCAST preamble + Handshake) and
// accept the peer's inbound BCAST stream (read BCAST preamble + Handshake).
// Non-BCAST streams that arrive during handshake are cancelled.
func (p *PeerConn) handshake(ctx context.Context, ourChannels []ChannelID) (PeerID, ProtocolVersion, []ChannelID, error) {
	auth := p.conn.AuthInfo()
	if auth.Local == "" || auth.Remote == "" {
		return "", 0, nil, fmt.Errorf("authenticated peer ID is empty")
	}
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
		stream transport.SendStream
		err    error
	}
	type readResult struct {
		hs  *bcastpb.Bcast_Handshake
		s   transport.ReceiveStream
		err error
	}

	writeCh := make(chan writeResult, 1)
	readCh := make(chan readResult, 1)

	// Writer: open bcastOut, write BCAST preamble + Handshake.
	go func() {
		s, err := p.conn.OpenUniStream(ctx)
		if err != nil {
			writeCh <- writeResult{err: err}
			return
		}
		if err := protocol.WriteSelector(s, bcastCodepoint); err != nil {
			s.CancelWrite(0)
			writeCh <- writeResult{err: fmt.Errorf("write bcast selector: %w", err)}
			return
		}
		if err := WriteFrame(s, hsMsg); err != nil {
			s.CancelWrite(0)
			writeCh <- writeResult{err: fmt.Errorf("write handshake: %w", err)}
			return
		}
		writeCh <- writeResult{stream: s}
	}()

	// Reader: wait for Stack to route the peer's BCAST stream, then read its
	// handshake.
	go func() {
		var s transport.ReceiveStream
		select {
		case s = <-p.bcastIn:
		case <-ctx.Done():
			readCh <- readResult{err: ctx.Err()}
			return
		}
		resp := &bcastpb.Bcast{}
		if err := ReadFrame(s, resp); err != nil {
			s.CancelRead(0)
			readCh <- readResult{err: fmt.Errorf("read handshake: %w", err)}
			return
		}
		hs := resp.GetPeerHandshake()
		if hs == nil {
			s.CancelRead(0)
			readCh <- readResult{err: ErrUnexpectedMsgType}
			return
		}
		readCh <- readResult{hs: hs, s: s}
	}()

	// Wait for both sides to complete.
	var wr writeResult
	var rr readResult
	for i := 0; i < 2; i++ {
		select {
		case wr = <-writeCh:
			if wr.err != nil {
				return "", 0, nil, wr.err
			}
		case rr = <-readCh:
			if rr.err != nil {
				return "", 0, nil, rr.err
			}
		case <-ctx.Done():
			return "", 0, nil, ctx.Err()
		}
	}

	peerVersion, err := validateProtocolVersion(rr.hs.Version)
	if err != nil {
		wr.stream.CancelWrite(0)
		rr.s.CancelRead(0)
		return "", 0, nil, err
	}

	p.ctrlOut = wr.stream
	p.ctrlIn = rr.s

	remoteChannels := make([]ChannelID, len(rr.hs.Channels))
	for i, t := range rr.hs.Channels {
		remoteChannels[i] = ChannelID(t)
	}
	return PeerID(auth.Remote), ProtocolVersion(peerVersion), remoteChannels, nil
}

// Close stops the peer and waits for its broadcast goroutines.
func (p *PeerConn) Close() {
	p.closeOnce.Do(func() {
		p.stop()
		p.handlersMu.Lock()
		p.ready = false
		p.handlersMu.Unlock()
		p.wg.Wait()
	})
}

func (p *PeerConn) stop() {
	p.stopOnce.Do(func() {
		p.cancel()
		p.finishHandshake(false)
		close(p.done)
	})
}

func (p *PeerConn) finishHandshake(ok bool) {
	p.handshakeOnce.Do(func() {
		p.handshakeOK = ok
		close(p.handshakeDone)
	})
}

// ID returns the peer's ID. Safe to call after handshake completes.
func (p *PeerConn) ID() PeerID {
	return p.id
}

func validateProtocolVersion(peerVersion uint32) (uint32, error) {
	v := peerVersion
	if v > ProtocolV1 {
		v = ProtocolV1
	}
	if v != ProtocolV1 {
		return 0, ErrProtocolMismatch
	}
	return v, nil
}
