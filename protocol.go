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

// Protocol is one registered protocol: its number, event queue and wake
// channel. It is shared with a peer when the peer's Hello lists its number.
type Protocol struct {
	id    wire.Protocol
	wake  chan<- struct{}
	mu    sync.Mutex
	ready list.List // round-robin ring of *peerDelivery, at most one entry per peer
}

// Register assigns wake to the protocol numbered id. id must not be the
// control protocol, the reserved number, or one already registered. Invalid
// registrations leave the stack unchanged. All setup must finish before Start.
func (s *Stack) Register(id wire.Protocol, wake chan<- struct{}) (*Protocol, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return nil, ErrStackClosed
	}
	if s.helloBytes != nil {
		return nil, errors.New("stack registration is closed")
	}
	if wake == nil || cap(wake) == 0 {
		return nil, errors.New("wake channel must have capacity at least one")
	}
	if id == control.ControlProtocol || id == wire.ReservedProtocol {
		return nil, fmt.Errorf("protocol %d cannot be registered", id)
	}
	if len(s.protocols) == control.MaxProtocols {
		return nil, fmt.Errorf("too many protocols: limit %d", control.MaxProtocols)
	}
	for _, protocol := range s.protocols {
		if protocol.id == id {
			return nil, fmt.Errorf("protocol %d already registered", id)
		}
	}
	protocol := &Protocol{id: id, wake: wake}
	s.protocols = append(s.protocols, protocol)
	return protocol, nil
}

// Next returns one pending event without blocking. Peers take turns, and each
// peer delivers PeerUp, its streams in FIFO order, then PeerDown. A returned
// Streams still queued on peer closure are cancelled by the stack and are
// never returned.
func (f *Protocol) Next() (Event, bool) {
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
		stream := d.streams[0]
		d.streams[0] = nil
		d.streams = d.streams[1:]
		if len(d.streams) == 0 {
			d.streams = nil
		}
		event.Kind = StreamIn
		event.Stream = wrapReceiveStream(stream)
	default:
		event.Kind, event.Code = PeerDown, d.code
		return event, true
	}
	if len(d.streams) != 0 || d.down {
		d.ready = f.ready.PushBack(d)
	}
	return event, true
}

// sharedWith reports whether the protocol's number is in the peer's protocols,
// which readHello validated as strictly ascending.
func (f *Protocol) sharedWith(protocols []wire.Protocol) bool {
	_, found := slices.BinarySearch(protocols, f.id)
	return found
}

// addPeer queues a PeerUp for a new handle. The caller fills in
// peer.delivery from the result.
func (f *Protocol) addPeer(p *Peer) *peerDelivery {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := &peerDelivery{protocol: f, peer: p}
	d.schedule()
	return d
}

// signal wakes the consumer without blocking. The consumer drains until Next
// reports empty before waiting, so no wake is ever lost.
func (f *Protocol) signal() {
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

// EventKind identifies a protocol delivery.
type EventKind uint8

const (
	// PeerUp introduces a peer before any of its streams.
	PeerUp EventKind = iota + 1
	// StreamIn transfers ownership of a selected stream to the consumer.
	StreamIn
	// PeerDown ends a peer's deliveries.
	PeerDown
)

// Event is one delivery to a protocol.
type Event struct {
	Kind   EventKind
	Peer   *Peer
	Stream ReceiveStream // StreamIn only; bidirectional streams also implement Stream.
	Code   wire.Code     // PeerDown only.
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

// Peer is one Protocol's handle for one view. A new view gets a new handle.
// Its context ends before the protocol can receive PeerDown.
type Peer struct {
	ctx      context.Context
	cancel   context.CancelFunc
	id       identity.PeerID
	sup      *peerSupervisor // Record source; receives Close notifications
	protocol *Protocol
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

// OpenUniStream opens a unidirectional stream for the handle's protocol and
// writes its stream header. On success, the caller owns the stream and must
// close or cancel it.
func (p *Peer) OpenUniStream(ctx context.Context) (SendStream, error) {
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
	err = wire.WriteHeader(stream, p.protocol.id)
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
func (p *Peer) OpenStream(ctx context.Context) (Stream, error) {
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
	err = wire.WriteHeader(stream, p.protocol.id)
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
// local PeerDown with code is queued. Other protocols keep using the view; when
// no protocol holds it, the stack closes the view. Idempotent.
func (p *Peer) Close(code wire.Code) {
	if p.delivery.closePeer(code, wire.UnsupportedProtocol) {
		p.sup.post(p.sup.handleClosed)
	}
}

// peerDelivery belongs to one protocol mutex. Keeping its terminal state even
// after removal lets an accidental late push reset its own stream.
type peerDelivery struct {
	protocol *Protocol
	peer     *Peer
	up       bool
	down     bool
	code     wire.Code
	// streams is bounded by the view's QUIC stream limits: each holds stream
	// credit until Next returns it or closePeer resets it.
	streams []receivedStream
	ready   *list.Element
}

// push queues a routed stream, or resets it when the handle has ended.
// Cancellation runs after releasing the protocol mutex.
func (d *peerDelivery) push(stream receivedStream) {
	protocol := d.protocol
	protocol.mu.Lock()
	if d.down {
		protocol.mu.Unlock()
		cancelReceived(stream, wire.UnsupportedProtocol)
		return
	}
	d.streams = append(d.streams, stream)
	d.schedule()
	protocol.mu.Unlock()
}

// closePeer ends the handle with code, resetting queued streams with
// resetCode. It cancels the handle's context before PeerDown becomes visible,
// so the consumer never observes a live handle after PeerDown. A handle whose
// PeerUp was never returned produces no events at all. It reports whether this
// call ended the handle.
func (d *peerDelivery) closePeer(code, resetCode wire.Code) bool {
	protocol := d.protocol
	protocol.mu.Lock()
	if d.down {
		protocol.mu.Unlock()
		return false
	}
	d.down, d.code = true, code
	d.peer.cancel()
	streams := d.streams
	d.streams = nil
	if !d.up {
		if d.ready != nil {
			protocol.ready.Remove(d.ready)
			d.ready = nil
		}
	} else {
		d.schedule()
	}
	protocol.mu.Unlock()
	for _, stream := range streams {
		cancelReceived(stream, resetCode)
	}
	return true
}

func (d *peerDelivery) schedule() {
	if d.ready == nil {
		d.ready = d.protocol.ready.PushBack(d)
	}
	d.protocol.signal()
}

// cancelReceived cancels the read side and, for bidirectional streams, the
// write side, preserving the supplied code's namespace.
func cancelReceived(stream receivedStream, code wire.Code) {
	stream.CancelRead(streamCode(code))
	if bi, ok := stream.(*quic.Stream); ok {
		bi.CancelWrite(streamCode(code))
	}
}
