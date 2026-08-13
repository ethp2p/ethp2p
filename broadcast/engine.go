package broadcast

import (
	"context"
	"sync"

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
// event loop. All map mutations happen inside run(), eliminating mutex use.
type Engine struct {
	config EngineConfig

	// channels contains the channels we have made the Engine aware of using AttachChannel.
	// TODO: need a DetachChannel.
	channels map[ChannelID]*channelHandle

	// peers are all peers we are connected to that support our version of the
	// broadcast protocol.
	peers map[PeerID]*PeerConn

	// peerSubs tracks which channels each remote peer is subscribed to.
	// Entries are inserted upon handshake, and subsequently updated
	// as the peer subscribes and unsubscribes.
	peerSubs map[PeerID]map[ChannelID]struct{}

	eventCh chan engineEvent

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
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
		peers:    make(map[PeerID]*PeerConn),
		peerSubs: make(map[PeerID]map[ChannelID]struct{}),
		eventCh:  make(chan engineEvent, 128),
		ctx:      ctx,
		cancel:   cancel,
	}
	e.wg.Go(e.run)
	return e
}

// NotifyPeerConnected is invoked by the environment to inform us when we've
// connected to a new peer who could potentially participate in broadcast.
// The conn is sent through the event loop, which spawns the peer's handshake
// and run goroutines.
//
// Ownership of this protocol-scoped connection is passed to the Engine. When
// compat shares the underlying QUIC connection with libp2p, closing this value
// closes only the borrowed ethp2p view.
func (e *Engine) NotifyPeerConnected(conn transport.Conn) {
	select {
	case e.eventCh <- engineEvent{kind: evPeerConnected, conn: conn}:
	case <-e.ctx.Done():
		conn.Close()
	}
}

// NotifyPeerGone reports that a peer is no longer available.
func (e *Engine) NotifyPeerGone(peerID PeerID) {
	select {
	case e.eventCh <- engineEvent{kind: evPeerGone, peerID: peerID}:
	case <-e.ctx.Done():
	}
}

// Close stops the engine and waits for its goroutines to exit.
func (e *Engine) Close() error {
	e.cancel()
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
func (e *Engine) onPeerSubscribed(peerID PeerID, channelID ChannelID) {
	select {
	case e.eventCh <- engineEvent{kind: evPeerSubscribed, peerID: peerID, channelID: channelID}:
	case <-e.ctx.Done():
	}
}

// onPeerUnsubscribed is invoked when the peer unsubscribes from a channel.
func (e *Engine) onPeerUnsubscribed(peerID PeerID, channelID ChannelID) {
	select {
	case e.eventCh <- engineEvent{kind: evPeerUnsubscribed, peerID: peerID, channelID: channelID}:
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

	case evPeerConnected:
		p := newPeerConn(e, ev.conn)
		channels := make([]ChannelID, 0, len(e.channels))
		for id := range e.channels {
			channels = append(channels, id)
		}
		e.wg.Go(func() { p.Run(channels) })

	case evPeerHandshake:
		if ev.err != nil {
			e.wg.Go(func() { ev.peer.Close() })
			return
		}
		p := ev.peer
		if _, ok := e.peers[p.id]; ok {
			return
		}
		e.peers[p.id] = p
		subs := make(map[ChannelID]struct{}, len(ev.channels))
		for _, channelID := range ev.channels {
			subs[channelID] = struct{}{}
			e.enrolPeerToChannel(p, channelID)
		}
		e.peerSubs[p.id] = subs
		e.config.Observer.OnPeerHandshook(p.id, p.version, ev.channels)
		for _, channelID := range ev.channels {
			e.config.Observer.OnPeerSubscribed(p.id, channelID)
		}

	case evPeerGone:
		p, ok := e.peers[ev.peerID]
		if !ok {
			return
		}
		delete(e.peers, ev.peerID)
		// Unbind peer from all channels so sessions see the peer drop.
		if subs := e.peerSubs[ev.peerID]; subs != nil {
			for channelID := range subs {
				p.UnbindChannel(channelID)
				if t := e.channels[channelID]; t != nil && t.inbox != nil {
					select {
					case t.inbox <- channelPeerChange{peerID: ev.peerID}:
					case <-e.ctx.Done():
					}
				}
			}
		}
		delete(e.peerSubs, ev.peerID)
		e.config.Observer.OnPeerGone(ev.peerID)
		e.wg.Go(func() { p.Close() })

	case evPeerSubscribed:
		if p, ok := e.peers[ev.peerID]; ok {
			if e.peerSubs[ev.peerID] == nil {
				e.peerSubs[ev.peerID] = make(map[ChannelID]struct{})
			}
			e.peerSubs[ev.peerID][ev.channelID] = struct{}{}
			e.enrolPeerToChannel(p, ev.channelID)
			e.config.Observer.OnPeerSubscribed(ev.peerID, ev.channelID)
		}

	case evPeerUnsubscribed:
		if subs := e.peerSubs[ev.peerID]; subs != nil {
			delete(subs, ev.channelID)
		}
		if p, ok := e.peers[ev.peerID]; ok {
			p.UnbindChannel(ev.channelID)
			e.config.Observer.OnPeerUnsubscribed(ev.peerID, ev.channelID)
		}
		if t := e.channels[ev.channelID]; t != nil && t.inbox != nil {
			select {
			case t.inbox <- channelPeerChange{peerID: ev.peerID}:
			case <-e.ctx.Done():
			}
		}
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
	// Phase 1: close all peers and wait for their goroutines to exit.
	for _, p := range e.peers {
		p.Close()
	}

	// Phase 2: drain unprocessed peer events.
	for {
		select {
		case ev := <-e.eventCh:
			switch ev.kind {
			case evPeerConnected:
				ev.conn.Close()
			case evPeerHandshake:
				ev.peer.Close()
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
