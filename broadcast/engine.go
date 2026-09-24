package broadcast

import (
	"context"
	"maps"
	"slices"
	"sync"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/transport"
)

const defaultMaxInboundChunkStreams = 5

// EngineConfig holds configuration for an Engine.
type EngineConfig struct {
	Observer Observer

	// MaxInboundChunkStreams bounds the number of concurrent inbound
	// chunk streams per peer. Zero uses the default (5).
	MaxInboundChunkStreams int
}

func (c *EngineConfig) maxInboundChunkStreams() int {
	if c.MaxInboundChunkStreams > 0 {
		return c.MaxInboundChunkStreams
	}
	return defaultMaxInboundChunkStreams
}

// Engine manages channels, peers, and message routing via a single-goroutine
// event loop. Its maps belong to run(); a separate lock only coordinates
// Serve admission with Close.
type Engine struct {
	config EngineConfig

	// channels contains the channels we have made the Engine aware of using AttachChannel.
	// TODO: need a DetachChannel.
	channels map[ChannelID]*channelHandle

	// peers contains the current ready binding for each authenticated peer ID.
	// A peer ID can be replaced by a reconnecting *ethp2p.Peer. Cleanup events
	// carry the binding that stopped and must compare it with this map before
	// removing anything, otherwise an old connection can tear down its
	// replacement.
	peers map[transport.PeerID]*PeerConn

	// bindings lets a stream event create the same PeerConn that a peer event
	// would create. The root stack deliberately does not promise processing
	// order between the two notification channels, so the stable *Peer pointer
	// is the connection identity even when an authenticated ID is reused. Failed
	// bindings stay here until Peer.Context ends: a delayed notification must
	// not start a second handshake on the same connection.
	bindings map[*ethp2p.Peer]*PeerConn

	// peerSubs tracks which channels each remote peer is subscribed to.
	// Entries are inserted upon handshake, and subsequently updated
	// as the peer subscribes and unsubscribes.
	peerSubs map[transport.PeerID]map[ChannelID]struct{}

	eventCh chan engineEvent

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	// serveMu coordinates Engine.Close with Serve producers. The event actor
	// drains eventCh only after every producer has stopped, so an event cannot
	// be queued after shutdown's final drain.
	serveMu sync.Mutex
	serveWG sync.WaitGroup
}

// NewEngine constructs an Engine and starts its event loop.
func NewEngine(config EngineConfig) *Engine {
	if config.Observer == nil {
		config.Observer = NoOpObserver{}
	}
	ctx, cancel := context.WithCancel(context.Background())

	e := &Engine{
		config:   config,
		channels: make(map[ChannelID]*channelHandle),
		peers:    make(map[transport.PeerID]*PeerConn),
		bindings: make(map[*ethp2p.Peer]*PeerConn),
		peerSubs: make(map[transport.PeerID]map[ChannelID]struct{}),
		eventCh:  make(chan engineEvent, 128),
		ctx:      ctx,
		cancel:   cancel,
	}
	e.wg.Go(e.run)
	return e
}

func (e *Engine) notifyPeerGone(p *PeerConn) {
	if p == nil {
		return
	}
	select {
	case e.eventCh <- engineEvent{kind: evPeerGone, peer: p}:
	case <-e.ctx.Done():
	}
}

// Close stops the engine and waits for its goroutines to exit.
func (e *Engine) Close() error {
	e.serveMu.Lock()
	e.cancel()
	e.serveMu.Unlock()
	e.wg.Wait()
	return nil
}

// Context returns the engine lifetime context.
func (e *Engine) Context() context.Context {
	return e.ctx
}

// DropChannel unregisters a channel from the engine.
func (e *Engine) DropChannel(id ChannelID) {
	select {
	case e.eventCh <- engineEvent{kind: evChannelRemoved, channelID: id}:
	case <-e.ctx.Done():
	}
}

// onPeerHandshake is invoked when handshake completes.
func (e *Engine) onPeerHandshake(p *PeerConn, channels []ChannelID, err error) {
	select {
	case e.eventCh <- engineEvent{kind: evPeerHandshake, peer: p, channels: channels, err: err}:
	case <-e.ctx.Done():
	}
}

// onPeerSubscribed is invoked when the peer subscribes to a new channel.
func (e *Engine) onPeerSubscribed(p *PeerConn, channelID ChannelID) {
	if p == nil {
		return
	}
	select {
	case e.eventCh <- engineEvent{kind: evPeerSubscribed, peer: p, channelID: channelID}:
	case <-p.ctx.Done():
	case <-e.ctx.Done():
	}
}

// onPeerUnsubscribed is invoked when the peer unsubscribes from a channel.
func (e *Engine) onPeerUnsubscribed(p *PeerConn, channelID ChannelID) {
	if p == nil {
		return
	}
	select {
	case e.eventCh <- engineEvent{kind: evPeerUnsubscribed, peer: p, channelID: channelID}:
	case <-p.ctx.Done():
	case <-e.ctx.Done():
	}
}

type channelHandle struct {
	inbox chan<- channelEvent
	done  <-chan struct{}
}

func (e *Engine) run() {
	for {
		select {
		case <-e.ctx.Done():
			e.shutdown()
			return
		case evt := <-e.eventCh:
			e.handle(evt)
		}
	}
}

func (e *Engine) handle(ev engineEvent) {
	switch ev.kind {
	case evChannelCreated:
		if _, ok := e.channels[ev.channelID]; ok {
			ev.cancel()
			e.config.Observer.OnChannelAttached(ev.channelID, ErrChannelExists)
			return
		}
		e.channels[ev.channelID] = &channelHandle{inbox: ev.inbox, done: ev.done}
		e.config.Observer.OnChannelAttached(ev.channelID, nil)

		// Bind any existing peers that had already declared this channel during
		// handshake, and notify all peers of our new subscription.
		for _, p := range e.peers {
			if subs := e.peerSubs[p.id]; subs != nil {
				if _, ok := subs[ev.channelID]; ok {
					e.enrolPeerToChannel(p, ev.channelID)
				}
			}
			select {
			case p.ctrlQ <- peerSubscribe{channelID: ev.channelID}:
			default:
			}
		}

	case evChannelRemoved:
		if _, ok := e.channels[ev.channelID]; !ok {
			return
		}
		for _, p := range e.peers {
			p.UnbindChannel(ev.channelID)
			select {
			case p.ctrlQ <- peerUnsubscribe{channelID: ev.channelID}:
			default:
			}
		}
		delete(e.channels, ev.channelID)
		e.config.Observer.OnChannelDropped(ev.channelID)

	case evPeerEvent:
		e.bindPeer(ev.appPeer)

	case evStreamEvent:
		e.handleStreamEvent(ev.stream)

	case evPeerHandshake:
		e.handlePeerHandshake(ev)

	case evPeerGone:
		e.handlePeerGone(ev)

	case evPeerSubscribed:
		p := ev.peer
		if e.peers[p.id] != p {
			return
		}
		e.peerSubs[p.id][ev.channelID] = struct{}{}
		e.enrolPeerToChannel(p, ev.channelID)
		e.config.Observer.OnPeerSubscribed(p.id, ev.channelID)

	case evPeerUnsubscribed:
		p := ev.peer
		if e.peers[p.id] != p {
			return
		}
		delete(e.peerSubs[p.id], ev.channelID)
		p.UnbindChannel(ev.channelID)
		e.config.Observer.OnPeerUnsubscribed(p.id, ev.channelID)
		if t := e.channels[ev.channelID]; t != nil && t.inbox != nil {
			select {
			case t.inbox <- channelPeerChange{peerID: p.id}:
			case <-e.ctx.Done():
			}
		}
	}
}

func (e *Engine) bindPeer(peer *ethp2p.Peer) *PeerConn {
	if !supportsBroadcast(peer) {
		return nil
	}
	if p := e.bindings[peer]; p != nil {
		return p
	}

	p := newPeerConn(e, peer.Context, peer.ID, peer)
	p.peer = peer
	e.bindings[peer] = p
	e.wg.Go(func() {
		select {
		case <-peer.Context.Done():
			e.notifyPeerGone(p)
		case <-e.ctx.Done():
		}
	})
	channels := slices.Collect(maps.Keys(e.channels))
	e.wg.Go(func() { _ = p.Run(channels) })
	return p
}

func (e *Engine) handleStreamEvent(event ethp2p.StreamEvent) {
	if event.Stream == nil {
		return
	}
	if _, ok := event.Stream.(transport.Stream); ok {
		// Broadcast's wire protocols use unidirectional streams. A bidi
		// event is a routing mismatch; reset both halves before dropping it.
		event.Reject()
		return
	}
	if !isBroadcastSelector(event.Selector) {
		event.Reject()
		return
	}

	p := e.bindPeer(event.Peer)
	if p == nil {
		event.Reject()
		return
	}
	p.enqueueStream(event.Selector, event.Stream)
}

func (e *Engine) handlePeerHandshake(ev engineEvent) {
	p := ev.peer
	if p == nil {
		return
	}
	if ev.err != nil || p.ctx.Err() != nil {
		p.Close()
		return
	}

	if p.peer != nil && e.bindings[p.peer] != p {
		p.Close()
		return
	}

	if old := e.peers[p.id]; old != nil && old != p {
		// Replace the active binding, but do not let the old binding's later
		// cleanup remove the new map entry.
		e.removePeer(old)
		e.removeBinding(old)
		old.Close()
	}
	e.peers[p.id] = p
	subs := make(map[ChannelID]struct{}, len(ev.channels))
	for _, channelID := range ev.channels {
		subs[channelID] = struct{}{}
		e.enrolPeerToChannel(p, channelID)
	}
	e.peerSubs[p.id] = subs
	p.finishHandshake(true)
	e.config.Observer.OnPeerHandshook(p.id, p.version, ev.channels)
	for _, channelID := range ev.channels {
		e.config.Observer.OnPeerSubscribed(p.id, channelID)
	}
}

func (e *Engine) handlePeerGone(ev engineEvent) {
	p := ev.peer
	if p == nil {
		return
	}

	// A binding may finish after a reconnect has already installed a new
	// PeerConn for the same authenticated ID. Only the current binding can
	// remove the peer and notify channels.
	e.removePeer(p)
	e.removeBinding(p)
	p.Close()
}

func (e *Engine) removePeer(p *PeerConn) {
	if p == nil {
		return
	}
	if e.peers[p.id] != p {
		return
	}
	delete(e.peers, p.id)
	if subs := e.peerSubs[p.id]; subs != nil {
		for channelID := range subs {
			p.UnbindChannel(channelID)
			if t := e.channels[channelID]; t != nil && t.inbox != nil {
				select {
				case t.inbox <- channelPeerChange{peerID: p.id}:
				case <-e.ctx.Done():
				}
			}
		}
	}
	delete(e.peerSubs, p.id)
	e.config.Observer.OnPeerGone(p.id)
}

func (e *Engine) removeBinding(p *PeerConn) {
	if p == nil || p.peer == nil {
		return
	}
	if p.peer.Context != nil && p.peer.Context.Err() == nil {
		return
	}
	if e.bindings[p.peer] == p {
		delete(e.bindings, p.peer)
	}
}

func (e *Engine) enrolPeerToChannel(p *PeerConn, channelID ChannelID) {
	t, ok := e.channels[channelID]
	if !ok || t.inbox == nil {
		return
	}
	p.BindChannel(channelID, t.inbox)
	select {
	case t.inbox <- channelPeerChange{peerID: p.id, peerRef: p}:
	case <-e.ctx.Done():
	}
}

func (e *Engine) shutdown() {
	// Serve may still be publishing adapter events after Engine.Close cancels
	// the lifetime. Join those producers before draining eventCh so no stream
	// arrives after the final disposal pass.
	e.serveWG.Wait()

	// Phase 1: close all peers and wait for their goroutines to exit.
	// ponytail: PeerConn.Close is idempotent; overlap needs no tracking map.
	for _, p := range e.peers {
		p.Close()
	}
	for _, p := range e.bindings {
		p.Close()
	}

	// Phase 2: drain unprocessed peer events.
	for {
		select {
		case ev := <-e.eventCh:
			switch ev.kind {
			case evStreamEvent:
				ev.stream.Reject()
			case evPeerHandshake:
				if ev.peer != nil {
					ev.peer.Close()
				}
			default:
				// Reply-bearing events: callers escape via ctx.Done().
			}
		default:
			goto waitChannels
		}
	}

waitChannels:
	// Phase 3: wait for channel goroutines to exit.
	for _, th := range e.channels {
		if th.done != nil {
			<-th.done
		}
	}
}
