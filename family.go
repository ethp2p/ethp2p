package ethp2p

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/ethp2p/ethp2p/control"
	"github.com/ethp2p/ethp2p/enr"
	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/quic-go/quic-go"
)

// ProtocolSpec registers one protocol of a Family.
type ProtocolSpec struct {
	Protocol wire.Protocol
	// MaxQueued bounds, per peer, the incoming streams classified but not yet
	// returned by Next. It is local and never advertised; it must be at least 1.
	MaxQueued int
}

// Family is a set of protocols registered together under one family code,
// sharing one event queue and wake channel. It is shared with a peer when the
// peer's Hello lists its code.
type Family struct {
	code      wire.Family
	protocols []ProtocolSpec // ascending by Protocol; immutable after Register
	wake      chan<- struct{}
	mu        sync.Mutex
	ready     list.List // round-robin ring of *peerDelivery, at most one entry per peer
}

// Register assigns a set of protocols, strictly ascending by number, to the
// family with code, and gives it its wake channel. The code must not be the
// control family, the reserved family, or one already registered. Invalid
// registrations leave the stack unchanged. All setup must finish before Start.
func (s *Stack) Register(code wire.Family, protocols []ProtocolSpec, wake chan<- struct{}) (*Family, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return nil, ErrStackClosed
	}
	if s.helloBytes != nil {
		return nil, errors.New("stack registration is closed")
	}
	if len(protocols) == 0 {
		return nil, errors.New("family needs at least one protocol")
	}
	if wake == nil || cap(wake) == 0 {
		return nil, errors.New("wake channel must have capacity at least one")
	}
	if code == control.ControlFamily || code == wire.ReservedFamily {
		return nil, fmt.Errorf("family %d cannot be registered", code)
	}
	if len(s.families) == control.MaxFamilies {
		return nil, fmt.Errorf("too many families: limit %d", control.MaxFamilies)
	}
	for _, family := range s.families {
		if family.code == code {
			return nil, fmt.Errorf("family %d already registered", code)
		}
	}
	for i, spec := range protocols {
		if i > 0 && spec.Protocol <= protocols[i-1].Protocol {
			return nil, errors.New("protocols must be strictly ascending")
		}
		if spec.MaxQueued < 1 {
			return nil, fmt.Errorf("protocol %d needs MaxQueued at least 1", spec.Protocol)
		}
	}
	family := &Family{code: code, protocols: slices.Clone(protocols), wake: wake}
	s.families = append(s.families, family)
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
		d.queued[pending.protocol]--
		event.Kind, event.Protocol = StreamIn, pending.protocol
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

// maxQueued returns the queue limit for a protocol the family owns, and 0 for
// any other protocol: Register requires every limit to be at least 1.
func (f *Family) maxQueued(protocol wire.Protocol) int {
	for _, spec := range f.protocols {
		if spec.Protocol == protocol {
			return spec.MaxQueued
		}
	}
	return 0
}

// sharedWith reports whether the family's code is in the peer's families,
// which readHello validated as strictly ascending.
func (f *Family) sharedWith(families []wire.Family) bool {
	_, found := slices.BinarySearch(families, f.code)
	return found
}

// addPeer queues a PeerUp for a new handle. The caller fills in
// peer.delivery from the result.
func (f *Family) addPeer(p *Peer) *peerDelivery {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := &peerDelivery{family: f, peer: p, queued: make(map[wire.Protocol]int)}
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
	Protocol wire.Protocol // StreamIn only; a protocol of the receiving family.
	Stream   ReceiveStream // StreamIn only; bidirectional streams also implement Stream.
	Code     wire.Code     // PeerDown only.
}

// Cancel cancels every applicable side of StreamIn; other events are unchanged.
func (e Event) Cancel(code wire.Code) {
	if e.Kind != StreamIn {
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
	id       identity.PeerID
	sup      *peerSupervisor // Record source; receives Close notifications
	family   *Family
	conn     *transport.Conn
	delivery *peerDelivery
}

// ID returns the authenticated remote identity.
func (p *Peer) ID() identity.PeerID { return p.id }

// Record returns the highest-sequence record learned from Hello or Connect,
// or nil when absent. Later Connect calls may update it.
func (p *Peer) Record() *enr.Record { return p.sup.record.Load() }

// Context ends when this handle ends.
func (p *Peer) Context() context.Context { return p.ctx }

// OpenUniStream opens a unidirectional stream of one of the family's
// protocols and writes its stream header. On success, the caller owns the
// stream and must close or cancel it. It panics for a protocol the
// family does not own.
func (p *Peer) OpenUniStream(ctx context.Context, protocol wire.Protocol) (SendStream, error) {
	if p.family.maxQueued(protocol) == 0 {
		panic(fmt.Sprintf("ethp2p: protocol %d not owned by family %d", protocol, p.family.code))
	}
	if err := p.ctx.Err(); err != nil {
		return nil, err
	}
	// The handle's end cancels the open like the caller's context: the
	// context is checked after writing the stream header, and a failure resets
	// the stream with Timeout or Unspecified.
	ctx, cancel := context.WithCancel(ctx)
	defer context.AfterFunc(p.ctx, cancel)()
	defer cancel()
	stream, err := p.conn.OpenUniStream(ctx)
	if err != nil {
		return nil, err
	}
	// A fresh stream's first write of up to a packet is buffered without
	// blocking in quic-go, so the context is checked once it returns.
	err = wire.WriteSelector(stream, wire.Selector{Family: p.family.code, Protocol: protocol})
	if err = errors.Join(err, ctx.Err()); err != nil {
		stream.CancelWrite(streamCode(wire.TimeoutOr(err, wire.Unspecified)))
		return nil, err
	}
	// AfterFunc cancels asynchronously, so an open racing Close can succeed
	// after the handle ended. This recheck never returns such a stream; a
	// Close landing after it leaves the caller owning a live stream.
	if err := p.ctx.Err(); err != nil {
		stream.CancelWrite(streamCode(wire.Unspecified))
		return nil, err
	}
	return wrapSendStream(stream), nil
}

// OpenStream opens a bidirectional stream like OpenUniStream. On success, the
// caller owns the stream and must close or cancel each side with a code.
func (p *Peer) OpenStream(ctx context.Context, protocol wire.Protocol) (Stream, error) {
	if p.family.maxQueued(protocol) == 0 {
		panic(fmt.Sprintf("ethp2p: protocol %d not owned by family %d", protocol, p.family.code))
	}
	if err := p.ctx.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer context.AfterFunc(p.ctx, cancel)()
	defer cancel()
	stream, err := p.conn.OpenStream(ctx)
	if err != nil {
		return nil, err
	}
	err = wire.WriteSelector(stream, wire.Selector{Family: p.family.code, Protocol: protocol})
	if err = errors.Join(err, ctx.Err()); err != nil {
		cancelReceived(stream, wire.TimeoutOr(err, wire.Unspecified))
		return nil, err
	}
	if err := p.ctx.Err(); err != nil {
		cancelReceived(stream, wire.Unspecified)
		return nil, err
	}
	return wrapBidirectionalStream(stream), nil
}

// Close ends this handle: queued streams are reset, its context ends, and a
// local PeerDown with code is queued. Other families keep using the view; when
// no family holds it, the stack closes the view. Idempotent.
func (p *Peer) Close(code wire.Code) {
	if p.delivery.closePeer(code, wire.UnsupportedSelector) {
		p.sup.post(p.sup.handleClosed)
	}
}

type pendingStream struct {
	protocol wire.Protocol
	stream   receivedStream
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
	queued  map[wire.Protocol]int
	ready   *list.Element
}

// push queues a classified stream, or resets it when the handle has ended or
// its protocol's local queue is full. Cancellation runs after releasing the
// family mutex.
func (d *peerDelivery) push(protocol wire.Protocol, stream receivedStream) {
	family := d.family
	family.mu.Lock()
	if d.down {
		family.mu.Unlock()
		cancelReceived(stream, wire.UnsupportedSelector)
		return
	}
	if d.queued[protocol] >= family.maxQueued(protocol) {
		// Over the local bound: reset without a reason, so a peer cannot
		// probe local queue state. The stream is never delivered.
		family.mu.Unlock()
		cancelReceived(stream, wire.Unspecified)
		return
	}
	d.streams = append(d.streams, pendingStream{protocol, stream})
	d.queued[protocol]++
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
func cancelReceived(stream receivedStream, code wire.Code) {
	stream.CancelRead(streamCode(code))
	if bi, ok := stream.(*quic.Stream); ok {
		bi.CancelWrite(streamCode(code))
	}
}
