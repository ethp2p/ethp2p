package broadcast

import (
	"context"
	"maps"
	"slices"
	"sync"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/wire"
)

// maxInboundChunkStreams bounds concurrent inbound CHUNK header readers per peer.
const maxInboundChunkStreams = 5

// EngineConfig holds configuration for an Engine.
type EngineConfig struct {
	Observer Observer
}

// Engine manages channels, peers, and message routing via a single-goroutine
// event loop. Its maps belong to run(); registration is synchronized with Close.
type Engine struct {
	config EngineConfig

	channels map[ChannelID]*channelHandle

	// peers contains the current ready binding for each authenticated peer ID.
	// Cleanup events carry the binding that stopped and must compare it with
	// this map: a reconnect's binding can be installed before the old one's
	// Run reports its exit.
	peers map[identity.PeerID]*PeerConn

	// bindings routes streams by the stable family peer handle. Only PeerUp
	// creates a binding; PeerDown removes it.
	bindings map[*ethp2p.Peer]*PeerConn

	eventCh chan func()

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	// family is set by Register before the event loop starts (Register
	// starts it), so the loop reads it without synchronization.
	family       *ethp2p.Family
	started      bool
	deliveryWake chan struct{}
}

// NewEngine constructs an Engine. Its event loop is not running yet:
// Register starts it, or Start does for engines that are never registered.
func NewEngine(config EngineConfig) *Engine {
	if config.Observer == nil {
		config.Observer = NoOpObserver{}
	}
	ctx, cancel := context.WithCancel(context.Background())

	e := &Engine{
		config:       config,
		channels:     make(map[ChannelID]*channelHandle),
		peers:        make(map[identity.PeerID]*PeerConn),
		bindings:     make(map[*ethp2p.Peer]*PeerConn),
		eventCh:      make(chan func(), 128),
		deliveryWake: make(chan struct{}, 1),
		ctx:          ctx,
		cancel:       cancel,
	}
	return e
}

// Start begins the event loop. It is called by Register, and directly for
// engines that are never registered. At most once; later calls are no-ops.
func (e *Engine) Start() {
	if e.started {
		return
	}
	e.started = true
	e.wg.Go(e.run)
}

// post runs fn on the engine actor unless the engine, or the sender's own
// lifetime stop (nil if none), ends first.
func (e *Engine) post(stop <-chan struct{}, fn func()) {
	select {
	case e.eventCh <- fn:
	case <-stop:
	case <-e.ctx.Done():
	}
}

// Close stops the engine and waits for its goroutines to exit. Per the
// startup choreography, Close is called at most once, after Register.
func (e *Engine) Close() error {
	e.cancel()
	e.wg.Wait()
	return nil
}

// DropChannel unregisters a channel from the engine.
func (e *Engine) DropChannel(id ChannelID) {
	e.post(nil, func() { e.removeChannel(id) })
}

// channelHandle is the producer side of one Channel actor, shared by the
// engine and every PeerConn bound to it. It is immutable after creation.
type channelHandle struct {
	inbox chan<- channelEvent
	// ctx is the channel's lifetime: producers stop once it ends, and
	// Channel.drainInbox runs only after it ends.
	ctx  context.Context
	done <-chan struct{} // actor exited
}

func (e *Engine) run() {
	for {
		select {
		case <-e.ctx.Done():
			e.shutdown()
			return
		case <-e.deliveryWake:
			if family := e.family; family != nil {
				for event, ok := family.Next(); ok; event, ok = family.Next() {
					e.handleDelivery(event)
				}
			}
		case fn := <-e.eventCh:
			fn()
		}
	}
}

func (e *Engine) addChannel(id ChannelID, h *channelHandle, cancel func()) {
	if _, ok := e.channels[id]; ok {
		cancel()
		e.config.Observer.OnChannelAttached(id, ErrChannelExists)
		return
	}
	e.channels[id] = h
	e.config.Observer.OnChannelAttached(id, nil)

	// Bind any existing peers that had already declared this channel during
	// handshake, and notify all peers of our new subscription.
	for _, p := range e.peers {
		if _, ok := p.subs[id]; ok {
			e.enrolPeerToChannel(p, id)
		}
		p.ctrlState.announce(id, true)
	}
}

func (e *Engine) removeChannel(id ChannelID) {
	if _, ok := e.channels[id]; !ok {
		return
	}
	for _, p := range e.peers {
		p.UnbindChannel(id)
		p.ctrlState.announce(id, false)
	}
	delete(e.channels, id)
	e.config.Observer.OnChannelDropped(id)
}

func (e *Engine) peerSubscribed(p *PeerConn, id ChannelID) {
	if e.peers[p.id] != p {
		return
	}
	if _, subscribed := p.subs[id]; subscribed {
		return
	}
	p.subs[id] = struct{}{}
	e.enrolPeerToChannel(p, id)
	e.config.Observer.OnPeerSubscribed(p.id, id)
}

func (e *Engine) peerUnsubscribed(p *PeerConn, id ChannelID) {
	if e.peers[p.id] != p {
		return
	}
	if _, subscribed := p.subs[id]; !subscribed {
		return
	}
	delete(p.subs, id)
	e.unenrolPeerFromChannel(p, id)
	e.config.Observer.OnPeerUnsubscribed(p.id, id)
}

func (e *Engine) bindPeer(peer *ethp2p.Peer) *PeerConn {
	if peer.Context().Err() != nil {
		return nil
	}
	p := newPeerConn(e, peer.Context(), peer.ID(), peer)
	e.bindings[peer] = p

	p.announced = slices.Collect(maps.Keys(e.channels))
	e.wg.Go(p.Run)
	return p
}

func (e *Engine) handleStreamEvent(event ethp2p.Event) {
	if e.ctx.Err() != nil {
		event.Cancel(wire.Unspecified)
		return
	}
	if _, ok := event.Stream.(ethp2p.Stream); ok {
		// Broadcast's wire protocols use unidirectional streams. A bidi
		// event is a routing mismatch; cancel both halves before dropping it.
		event.Reject()
		return
	}
	p := e.bindings[event.Peer]
	if p == nil {
		event.Reject()
		return
	}
	p.enqueueStream(event.Protocol, event.Stream)
}

func (e *Engine) handlePeerHandshake(p *PeerConn, channels []ChannelID) {
	// PeerDown closes p, so a handshake that finishes after it fails this
	// check. The stack delivers an ID's PeerDown before the next PeerUp, so
	// an earlier binding for p.id has already left e.peers.
	if p.ctx.Err() != nil {
		return
	}
	e.peers[p.id] = p
	p.ctrlState.announceInitial(p.announced)
	// Channel changes reach only peers in e.peers, and our Hello listed the
	// channels at bind time; announce what changed in between.
	for id := range e.channels {
		if !slices.Contains(p.announced, id) {
			p.ctrlState.announce(id, true)
		}
	}
	for _, id := range p.announced {
		if _, ok := e.channels[id]; !ok {
			p.ctrlState.announce(id, false)
		}
	}
	subs := make(map[ChannelID]struct{}, len(channels))
	for _, channelID := range channels {
		if _, exists := subs[channelID]; exists {
			continue
		}
		subs[channelID] = struct{}{}
		e.enrolPeerToChannel(p, channelID)
	}
	p.subs = subs
	close(p.handshakeDone)
	e.config.Observer.OnPeerHandshook(p.id, channels)
	for _, channelID := range channels {
		e.config.Observer.OnPeerSubscribed(p.id, channelID)
	}
}

func (e *Engine) handlePeerGone(p *PeerConn) {
	// A binding may finish after a reconnect has already installed a new
	// PeerConn for the same authenticated ID. Only the current binding can
	// remove the peer and notify channels.
	e.removePeer(p)
	p.Close()
}

func (e *Engine) removePeer(p *PeerConn) {
	if e.peers[p.id] != p {
		return
	}
	delete(e.peers, p.id)
	for channelID := range p.subs {
		e.unenrolPeerFromChannel(p, channelID)
	}
	e.config.Observer.OnPeerGone(p.id)
}

func (e *Engine) enrolPeerToChannel(p *PeerConn, channelID ChannelID) {
	t, ok := e.channels[channelID]
	if !ok {
		return
	}
	p.BindChannel(channelID, t)
	select {
	case t.inbox <- channelPeerChange{peerID: p.id, peerRef: p}:
	case <-t.done:
	case <-e.ctx.Done():
	}
}

func (e *Engine) unenrolPeerFromChannel(p *PeerConn, channelID ChannelID) {
	p.UnbindChannel(channelID)
	if t := e.channels[channelID]; t != nil {
		select {
		case t.inbox <- channelPeerChange{peerID: p.id}:
		case <-t.done:
		case <-e.ctx.Done():
		}
	}
}

func (e *Engine) shutdown() {
	if family := e.family; family != nil {
		for event, ok := family.Next(); ok; event, ok = family.Next() {
			event.Cancel(wire.Unspecified)
		}
	}

	// Every live PeerConn keeps its binding until PeerDown closes it, so this
	// also closes every peer with a handshake event still queued. Senders of
	// unprocessed events escape through ctx.Done().
	for _, p := range e.bindings {
		p.Close()
	}

	for _, th := range e.channels {
		<-th.done
	}
}

func (e *Engine) handleDelivery(event ethp2p.Event) {
	switch event.Kind {
	case ethp2p.PeerUp:
		e.bindPeer(event.Peer)
	case ethp2p.StreamIn:
		e.handleStreamEvent(event)
	case ethp2p.PeerDown:
		if p := e.bindings[event.Peer]; p != nil {
			delete(e.bindings, event.Peer)
			e.handlePeerGone(p)
		}
	}
}
