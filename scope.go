package ethp2p

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/ethp2p/ethp2p/enr"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/ethp2p/ethp2p/transport"
)

// ErrSelectorNotShared reports an open for a selector outside this subsystem's match.
var ErrSelectorNotShared = errors.New("protocol selector was not shared")

// Peer is one view's match with a subsystem. A reconnect gets a new handle.
// Its context ends before the subsystem can receive PeerDown.
type Peer struct {
	ctx       context.Context
	id        transport.PeerID
	record    *atomic.Pointer[enr.Record]
	selectors []wire.Selector
	conn      transport.Conn
}

// ID returns the authenticated remote identity.
func (p *Peer) ID() transport.PeerID { return p.id }

// Record returns the highest-sequence record learned from Hello or Connect,
// or nil when absent. Later Connect calls may update it.
func (p *Peer) Record() *enr.Record {
	if p.record == nil {
		return nil
	}
	return p.record.Load()
}

// Selectors returns a copy of this subsystem's ascending shared selectors.
func (p *Peer) Selectors() []wire.Selector { return slices.Clone(p.selectors) }

// Context ends when this peer goes down.
func (p *Peer) Context() context.Context { return p.ctx }

// OpenUniStream opens a unidirectional stream whose selector frame was written
// by the transport. On success, the caller owns the stream and must close or
// cancel it. Opening fails when selector was not negotiated for this peer.
func (p *Peer) OpenUniStream(ctx context.Context, selector wire.Selector) (SendStream, error) {
	if err := p.checkSelector(selector); err != nil {
		return nil, err
	}

	stream, err := p.conn.OpenUniStream(ctx, selector)
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
// cancel each side with a code. Opening fails when selector was not negotiated
// for this peer.
func (p *Peer) OpenStream(ctx context.Context, selector wire.Selector) (Stream, error) {
	if err := p.checkSelector(selector); err != nil {
		return nil, err
	}

	stream, err := p.conn.OpenStream(ctx, selector)
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

func (p *Peer) checkSelector(selector wire.Selector) error {
	if !slices.Contains(p.selectors, selector) {
		return fmt.Errorf("%w: %d", ErrSelectorNotShared, selector)
	}
	return p.ctx.Err()
}

func (p *Peer) contextError(ctx context.Context) error {
	if err := p.ctx.Err(); err != nil {
		return err
	}
	return ctx.Err()
}

// EventKind identifies a subsystem delivery.
type EventKind uint8

const (
	// PeerUp introduces a peer before any of its streams.
	PeerUp EventKind = iota + 1
	// StreamIn transfers ownership of a selected stream to the wire.
	StreamIn
	// PeerDown ends a peer's deliveries.
	PeerDown
)

// Event is one delivery to a subsystem.
type Event struct {
	Kind     EventKind
	Peer     *Peer
	Selector wire.Selector // StreamIn only.
	Stream   ReceiveStream     // StreamIn only; bidirectional streams also implement Stream.
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

// SubsystemConfig configures a subsystem at registration.
type SubsystemConfig struct {
	// Policy decides whether to serve a peer with intersecting selectors.
	// nil accepts any intersection. It must return promptly and may run concurrently.
	Policy func(*Peer) bool
}

// Subsystem owns pending events for its registered selectors. Notify provides
// optional wakeups; Next transfers ownership of one event without blocking.
// Queued streams retain QUIC stream credit, so a slow consumer makes the peer's
// opens wait. All protocols on a connection share that credit.
type Subsystem struct {
	stack  *Stack
	policy func(*Peer) bool
	wake   chan<- struct{}
	mu     sync.Mutex
	ready  list.List // round-robin ring of *peerDelivery, with at most one entry per peer
}

// Register assigns selectors exclusively to name. Invalid registrations
// leave the stack unchanged. All setup must finish before Start.
func (s *Stack) Register(name string, selectors []wire.Selector, cfg SubsystemConfig) (*Subsystem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrStackClosed
	}
	if s.serving {
		return nil, errors.New("stack registration is closed")
	}
	if name == "" || len(selectors) == 0 {
		return nil, errors.New("subsystem needs a name and selectors")
	}
	if _, exists := s.subsystems[name]; exists {
		return nil, fmt.Errorf("subsystem %q already registered", name)
	}
	if len(s.selectors)+len(selectors) > wire.MaxSelectors {
		return nil, fmt.Errorf("too many selectors: limit %d", wire.MaxSelectors)
	}
	ordered := wire.Canonical(selectors)
	if len(ordered) != len(selectors) {
		return nil, errors.New("duplicate selectors in subsystem")
	}
	for _, selector := range ordered {
		if err := wire.ValidateSelector(selector); err != nil {
			return nil, err
		}
		if _, exists := s.selectors[selector]; exists {
			return nil, fmt.Errorf("selector %d already registered", selector)
		}
	}
	if s.subsystems == nil {
		s.subsystems = make(map[string]*Subsystem)
		s.selectors = make(map[wire.Selector]*Subsystem)
	}
	subsystem := &Subsystem{stack: s, policy: cfg.Policy}
	s.subsystems[name] = subsystem
	for _, selector := range ordered {
		s.selectors[selector] = subsystem
	}
	return subsystem, nil
}

// Notify replaces the optional wake channel before Start. Its capacity must be
// at least one. Several subsystems may share it; callers must not close it while
// the stack can enqueue events. Each enqueue signals it without blocking.
// Drain every associated subsystem until Next reports empty before waiting.
// Without Notify, callers can still poll Next.
func (s *Subsystem) Notify(wake chan<- struct{}) error {
	s.stack.mu.Lock()
	defer s.stack.mu.Unlock()
	if s.stack.closed {
		return ErrStackClosed
	}
	if s.stack.serving {
		return errors.New("stack registration is closed")
	}
	if wake == nil || cap(wake) == 0 {
		return errors.New("wake channel must have capacity at least one")
	}
	s.wake = wake
	return nil
}

type pendingStream struct {
	selector wire.Selector
	stream   transport.ReceiveStream
}

// peerDelivery belongs to one subsystem mutex. Keeping its terminal state even
// after removal lets an accidental late push reset its own stream.
type peerDelivery struct {
	sub     *Subsystem
	peer    *Peer
	up      bool
	down    bool
	code    wire.Code
	streams []pendingStream
	ready   *list.Element
}

func (s *Subsystem) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (d *peerDelivery) schedule() {
	if d.ready == nil {
		d.ready = d.sub.ready.PushBack(d)
	}
	d.sub.signal()
}

func (s *Subsystem) addPeer(peer *Peer) *peerDelivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := &peerDelivery{sub: s, peer: peer}
	d.schedule()
	return d
}

func (d *peerDelivery) push(sel wire.Selector, stream transport.ReceiveStream) {
	s := d.sub
	s.mu.Lock()
	if d.down {
		s.mu.Unlock()
		cancelQueuedStream(stream)
		return
	}
	d.streams = append(d.streams, pendingStream{sel, stream})
	d.schedule()
	s.mu.Unlock()
}

func cancelQueuedStream(stream transport.ReceiveStream) {
	stream.CancelRead(wire.Closing.Wire())
	if bi, ok := stream.(transport.Stream); ok {
		bi.CancelWrite(wire.Closing.Wire())
	}
}

func (d *peerDelivery) closePeer(code wire.Code) {
	s := d.sub
	s.mu.Lock()
	if d.down {
		s.mu.Unlock()
		return
	}
	d.down, d.code = true, code
	streams := d.streams
	d.streams = nil
	if !d.up {
		if d.ready != nil {
			s.ready.Remove(d.ready)
			d.ready = nil
		}
	} else {
		d.schedule()
	}
	s.mu.Unlock()
	for _, pending := range streams {
		cancelQueuedStream(pending.stream)
	}
}

// Next returns one pending event without blocking. Peers take turns, and each
// peer delivers PeerUp, its streams in FIFO order, then PeerDown. Streams still
// queued on peer closure are cancelled by the stack and are never returned.
func (s *Subsystem) Next() (Event, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	front := s.ready.Front()
	if front == nil {
		return Event{}, false
	}
	d := front.Value.(*peerDelivery)
	s.ready.Remove(front)
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
		event.Kind, event.Selector = StreamIn, pending.selector
		event.Stream = wrapReceiveStream(pending.stream)
	default:
		event.Kind, event.Code = PeerDown, d.code
		return event, true
	}
	if len(d.streams) != 0 || d.down {
		d.ready = s.ready.PushBack(d)
	}
	return event, true
}
