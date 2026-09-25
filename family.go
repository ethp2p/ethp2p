package ethp2p

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/ethp2p/ethp2p/enr"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/ethp2p/ethp2p/transport"
)

// ProtocolSpec registers one protocol of a Family.
type ProtocolSpec struct {
	Selector wire.Selector
	// MaxQueued bounds, per peer, the incoming streams classified but not yet
	// returned by Next. It is local and never advertised; it must be at least 1.
	MaxQueued int
}

// Family is a set of protocols registered together, sharing one event queue
// and wake channel. It is shared with a peer only when the peer's Hello lists
// every one of its selectors.
type Family struct {
	stack     *Stack
	protocols []ProtocolSpec // ascending by Selector; immutable after Register
	wake      chan<- struct{}
	mu        sync.Mutex
	ready     list.List // round-robin ring of *peerDelivery, at most one entry per peer
}

// Register assigns a set of protocols to one Family with its wake channel.
// Invalid registrations leave the stack unchanged. All setup must finish
// before Start.
func (s *Stack) Register(protocols []ProtocolSpec, wake chan<- struct{}) (*Family, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrStackClosed
	}
	if s.serving {
		return nil, errors.New("stack registration is closed")
	}
	if len(protocols) == 0 {
		return nil, errors.New("family needs at least one protocol")
	}
	if wake == nil || cap(wake) == 0 {
		return nil, errors.New("wake channel must have capacity at least one")
	}
	if len(s.selectors)+len(protocols) > wire.MaxSelectors {
		return nil, fmt.Errorf("too many selectors: limit %d", wire.MaxSelectors)
	}
	for i, spec := range protocols {
		if i > 0 && spec.Selector <= protocols[i-1].Selector {
			return nil, errors.New("family selectors must be strictly ascending")
		}
		if err := wire.ValidateSelector(spec.Selector); err != nil {
			return nil, err
		}
		if _, exists := s.selectors[spec.Selector]; exists {
			return nil, fmt.Errorf("selector %d already registered", spec.Selector)
		}
		if spec.MaxQueued < 1 {
			return nil, fmt.Errorf("selector %d needs MaxQueued at least 1", spec.Selector)
		}
	}
	if s.selectors == nil {
		s.selectors = make(map[wire.Selector]*Family)
	}
	family := &Family{stack: s, protocols: slices.Clone(protocols), wake: wake}
	s.families = append(s.families, family)
	for _, spec := range protocols {
		s.selectors[spec.Selector] = family
	}
	return family, nil
}

// Next returns one pending event without blocking. Peers take turns, and each
// peer delivers PeerUp, its streams in FIFO order, then PeerDown. A returned
// StreamIn stops counting against its protocol's MaxQueued. Streams still
// queued on peer closure are cancelled by the stack and are never returned.
func (f *Family) Next() (Event, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	front := f.ready.Front()
	if front == nil {
		return Event{}, false
	}
	d := front.Value.(*peerDelivery)
	f.ready.Remove(front)
	d.ready = nil
	event := Event{Peer: d.peer}
	switch {
	case !d.up:
		d.up = true
		event.Kind = PeerUp
	case len(d.streams) != 0:
		pending := d.streams[0]
		d.streams[0] = pendingStream{}
		d.streams = d.streams[1:]
		if len(d.streams) == 0 {
			d.streams = nil
		}
		d.queued[pending.selector]--
		event.Kind, event.Selector = StreamIn, pending.selector
		event.Stream = wrapReceiveStream(pending.stream)
	default:
		event.Kind, event.Code = PeerDown, d.code
		return event, true
	}
	if len(d.streams) != 0 || d.down {
		d.ready = f.ready.PushBack(d)
	}
	return event, true
}

// owns reports whether sel is one of the family's protocols.
func (f *Family) owns(sel wire.Selector) bool {
	for _, spec := range f.protocols {
		if spec.Selector == sel {
			return true
		}
	}
	return false
}

// maxQueued returns the queue limit for a protocol the family owns.
func (f *Family) maxQueued(sel wire.Selector) int {
	for _, spec := range f.protocols {
		if spec.Selector == sel {
			return spec.MaxQueued
		}
	}
	return 0
}

// sharedWith reports whether every protocol's selector is in the
// (ascending) list, so the family is never partially shared.
func (f *Family) sharedWith(selectors []wire.Selector) bool {
	next := 0
	for _, spec := range f.protocols {
		for next < len(selectors) && selectors[next] < spec.Selector {
			next++
		}
		if next >= len(selectors) || selectors[next] != spec.Selector {
			return false
		}
		next++
	}
	return true
}

// addPeer queues a PeerUp for a new handle. The caller fills in
// peer.delivery from the result.
func (f *Family) addPeer(p *Peer) *peerDelivery {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := &peerDelivery{family: f, peer: p, queued: make(map[wire.Selector]int)}
	d.schedule()
	return d
}

// signal wakes the consumer without blocking. The consumer drains until Next
// reports empty before waiting, so no wake is ever lost.
func (f *Family) signal() {
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

// EventKind identifies a family delivery.
type EventKind uint8

const (
	// PeerUp introduces a peer before any of its streams.
	PeerUp EventKind = iota + 1
	// StreamIn transfers ownership of a selected stream to the consumer.
	StreamIn
	// PeerDown ends a peer's deliveries.
	PeerDown
)

// Event is one delivery to a family.
type Event struct {
	Kind     EventKind
	Peer     *Peer
	Selector wire.Selector // StreamIn only.
	Stream   ReceiveStream // StreamIn only; bidirectional streams also implement Stream.
	Code     wire.Code     // PeerDown only.
}

// Cancel cancels every applicable side of StreamIn; other events are unchanged.
func (e Event) Cancel(code wire.Code) {
	if e.Kind != StreamIn || e.Stream == nil {
		return
	}
	e.Stream.CancelRead(code)
	if stream, ok := e.Stream.(Stream); ok {
		stream.CancelWrite(code)
	}
}

// Reject refuses a StreamIn event. Other events are unchanged.
func (e Event) Reject() { e.Cancel(wire.Refused) }

// Peer is one Family's handle for one view. A new view gets a new handle.
// Its context ends before the family can receive PeerDown.
type Peer struct {
	ctx      context.Context
	cancel   context.CancelFunc
	id       transport.PeerID
	sup      *peerSupervisor // Record source; receives Close notifications
	family   *Family
	conn     *transport.Conn
	delivery *peerDelivery
}

// ID returns the authenticated remote identity.
func (p *Peer) ID() transport.PeerID { return p.id }

// Record returns the highest-sequence record learned from Hello or Connect,
// or nil when absent. Later Connect calls may update it.
func (p *Peer) Record() *enr.Record { return p.sup.record.Load() }

// Context ends when this handle ends.
func (p *Peer) Context() context.Context { return p.ctx }

// OpenUniStream opens a unidirectional stream whose selector frame was written
// by the transport. On success, the caller owns the stream and must close or
// cancel it. It panics for a selector the family does not own.
func (p *Peer) OpenUniStream(ctx context.Context, sel wire.Selector) (SendStream, error) {
	if !p.family.owns(sel) {
		panic(fmt.Sprintf("ethp2p: selector %d not owned by family", sel))
	}
	if err := p.ctx.Err(); err != nil {
		return nil, err
	}
	stream, err := p.conn.OpenUniStream(ctx, sel)
	if err := p.contextError(ctx); err != nil {
		if stream != nil {
			stream.CancelWrite(streamFailureCode(p.ctx, err).Wire())
		}
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	return wrapSendStream(stream), nil
}

// OpenStream opens a bidirectional stream whose selector frame was written
// by the transport. On success, the caller owns the stream and must close or
// cancel each side with a code. It panics for a selector the family does not
// own.
func (p *Peer) OpenStream(ctx context.Context, sel wire.Selector) (Stream, error) {
	if !p.family.owns(sel) {
		panic(fmt.Sprintf("ethp2p: selector %d not owned by family", sel))
	}
	if err := p.ctx.Err(); err != nil {
		return nil, err
	}
	stream, err := p.conn.OpenStream(ctx, sel)
	if err := p.contextError(ctx); err != nil {
		if stream != nil {
			cancelTransportStream(stream, streamFailureCode(p.ctx, err))
		}
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	return wrapBidirectionalStream(stream), nil
}

// Close ends this handle: queued streams are reset, its context ends, and a
// local PeerDown with code is queued. Other families keep using the view; when
// no family holds it, the stack closes the view. Idempotent.
func (p *Peer) Close(code wire.Code) {
	if p.delivery.closePeer(code, wire.UnsupportedSelector) {
		p.sup.post(supervisorRequest{kind: reqHandleClosed})
	}
}

func (p *Peer) contextError(ctx context.Context) error {
	if err := p.ctx.Err(); err != nil {
		return err
	}
	return ctx.Err()
}

type pendingStream struct {
	selector wire.Selector
	stream   transport.ReceiveStream
}

// peerDelivery belongs to one family mutex. Keeping its terminal state even
// after removal lets an accidental late push reset its own stream.
type peerDelivery struct {
	family  *Family
	peer    *Peer
	up      bool
	down    bool
	code    wire.Code
	streams []pendingStream
	queued  map[wire.Selector]int
	ready   *list.Element
}

// push queues a classified stream, or resets it when the handle has ended or
// its protocol's local queue is full. Cancellation runs after releasing the
// family mutex.
func (d *peerDelivery) push(sel wire.Selector, stream transport.ReceiveStream) {
	family := d.family
	family.mu.Lock()
	if d.down {
		family.mu.Unlock()
		cancelReceived(stream, wire.UnsupportedSelector)
		return
	}
	if d.queued[sel] >= family.maxQueued(sel) {
		// Over the local bound: reset without a reason, so a peer cannot
		// probe local queue state. The stream is never delivered.
		family.mu.Unlock()
		cancelReceived(stream, wire.Unspecified)
		return
	}
	d.streams = append(d.streams, pendingStream{sel, stream})
	d.queued[sel]++
	d.schedule()
	family.mu.Unlock()
}

// closePeer ends the handle with code, resetting queued streams with
// resetCode. It cancels the handle's context before PeerDown becomes visible,
// so the consumer never observes a live handle after PeerDown. A handle whose
// PeerUp was never returned produces no events at all. It reports whether this
// call ended the handle.
func (d *peerDelivery) closePeer(code, resetCode wire.Code) bool {
	family := d.family
	family.mu.Lock()
	if d.down {
		family.mu.Unlock()
		return false
	}
	d.down, d.code = true, code
	d.peer.cancel()
	streams := d.streams
	d.streams = nil
	clear(d.queued)
	if !d.up {
		if d.ready != nil {
			family.ready.Remove(d.ready)
			d.ready = nil
		}
	} else {
		d.schedule()
	}
	family.mu.Unlock()
	for _, pending := range streams {
		cancelReceived(pending.stream, resetCode)
	}
	return true
}

func (d *peerDelivery) schedule() {
	if d.ready == nil {
		d.ready = d.family.ready.PushBack(d)
	}
	d.family.signal()
}

// cancelReceived cancels the read side and, for bidirectional streams, the
// write side, preserving the supplied code's namespace.
func cancelReceived(stream transport.ReceiveStream, code wire.Code) {
	stream.CancelRead(code.Wire())
	if bi, ok := stream.(transport.Stream); ok {
		bi.CancelWrite(code.Wire())
	}
}
