package ethp2p

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/ethp2p/ethp2p/internal/ctxutil"
	"github.com/ethp2p/ethp2p/protocol"
	"github.com/ethp2p/ethp2p/transport"
)

// ErrSelectorNotShared is returned when a peer tries to open a selector that
// was not negotiated for its subsystem.
var ErrSelectorNotShared = errors.New("protocol selector was not shared")

// Peer describes one connection's match with a subsystem. Treat its exported
// fields as read-only. ID is the authenticated remote identity. Context ends
// when ServeConn returns. Selectors lists the subsystem's shared selectors in
// canonical order. The pointer identifies this connection's notification,
// including before the application has processed the peer channel. A reconnect
// gets a different Peer.
type Peer struct {
	Context   context.Context
	ID        transport.PeerID
	Selectors []protocol.Selector
	conn      transport.Conn
}

// OpenUniStream opens a unidirectional stream with selector's protocol frame
// already written. On success, the caller owns the stream and must close or
// cancel it. Opening fails when selector was not negotiated for this peer.
func (p *Peer) OpenUniStream(ctx context.Context, selector protocol.Selector) (transport.SendStream, error) {
	if err := p.checkSelector(selector); err != nil {
		return nil, err
	}

	openCtx, stopPeerContext := p.openContext(ctx)
	stream, err := p.conn.OpenUniStream(openCtx)
	stopPeerContext()
	if err := p.contextError(ctx); err != nil {
		if stream != nil {
			stream.CancelWrite(0)
		}
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	if err := p.writeSelector(ctx, stream, selector, func() { stream.CancelWrite(0) }); err != nil {
		return nil, err
	}
	return stream, nil
}

// OpenStream opens a bidirectional stream with selector's protocol frame
// already written. On success, the caller owns the stream and must close or
// reset it. Opening fails when selector was not negotiated for this peer.
func (p *Peer) OpenStream(ctx context.Context, selector protocol.Selector) (transport.Stream, error) {
	if err := p.checkSelector(selector); err != nil {
		return nil, err
	}

	openCtx, stopPeerContext := p.openContext(ctx)
	stream, err := p.conn.OpenStream(openCtx)
	stopPeerContext()
	if err := p.contextError(ctx); err != nil {
		if stream != nil {
			_ = stream.Reset()
		}
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	if err := p.writeSelector(ctx, stream, selector, func() { _ = stream.Reset() }); err != nil {
		return nil, err
	}
	return stream, nil
}

func (p *Peer) checkSelector(selector protocol.Selector) error {
	if !slices.Contains(p.Selectors, selector) {
		return fmt.Errorf("%w: %d", ErrSelectorNotShared, selector)
	}
	return p.Context.Err()
}

func (p *Peer) openContext(ctx context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(p.Context, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

func (p *Peer) contextError(ctx context.Context) error {
	if err := p.Context.Err(); err != nil {
		return err
	}
	return ctx.Err()
}

func (p *Peer) writeSelector(ctx context.Context, w io.Writer, selector protocol.Selector, cancelStream func()) error {
	var cancelOnce sync.Once
	cancel := func() { cancelOnce.Do(cancelStream) }
	stopCtx := ctxutil.OnCancel(ctx, cancel)
	stopPeer := ctxutil.OnCancel(p.Context, cancel)
	err := protocol.WriteSelector(w, selector)
	stopPeer()
	stopCtx()

	if err := p.contextError(ctx); err != nil {
		cancel()
		return err
	}
	if err != nil {
		cancel()
		return err
	}
	return nil
}

// StreamEvent transfers one selected stream to a subsystem. Every ethp2p stream
// begins with a selector frame; Stream retains all protocol data after that
// frame. Bidirectional streams also implement transport.Stream. The receiver
// owns stream cleanup after successful delivery, even if the event is still
// queued when Peer.Context ends.
type StreamEvent struct {
	Peer     *Peer
	Selector protocol.Selector
	Stream   transport.ReceiveStream
}

// Reject disposes of a delivered stream the receiver will not handle. A nil
// Stream is a no-op; bidirectional streams have both halves reset.
func (e StreamEvent) Reject() {
	if e.Stream == nil {
		return
	}
	if stream, ok := e.Stream.(transport.Stream); ok {
		_ = stream.Reset()
		return
	}
	e.Stream.CancelRead(0)
}

// Subsystem registers selectors, a peer policy, and notification destinations.
// It does not construct or run the subsystem's implementation.
type Subsystem struct {
	stack   *Stack
	peers   chan<- *Peer
	streams chan<- StreamEvent
	policy  func(*Peer) bool
}

// RegisterSubsystem assigns selectors exclusively to name. Invalid registrations
// leave the stack unchanged. All setup must finish before the first ServeConn.
func (s *Stack) RegisterSubsystem(name string, selectors ...protocol.Selector) (*Subsystem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serving {
		return nil, errors.New("stack registration is closed")
	}
	if name == "" || len(selectors) == 0 {
		return nil, errors.New("subsystem needs a name and selectors")
	}
	if _, exists := s.subsystems[name]; exists {
		return nil, fmt.Errorf("subsystem %q already registered", name)
	}
	if len(s.selectors)+len(selectors) > protocol.MaxSelectors {
		return nil, fmt.Errorf("too many selectors: limit %d", protocol.MaxSelectors)
	}
	ordered := protocol.Canonical(selectors)
	if len(ordered) != len(selectors) {
		return nil, errors.New("duplicate selectors in subsystem")
	}
	for _, selector := range ordered {
		if err := protocol.ValidateSelector(selector); err != nil {
			return nil, err
		}
		if _, exists := s.selectors[selector]; exists {
			return nil, fmt.Errorf("selector %d already registered", selector)
		}
	}
	if s.subsystems == nil {
		s.subsystems = make(map[string]*Subsystem)
		s.selectors = make(map[protocol.Selector]*Subsystem)
	}
	subsystem := &Subsystem{stack: s}
	s.subsystems[name] = subsystem
	for _, selector := range ordered {
		s.selectors[selector] = subsystem
	}
	return subsystem, nil
}

// NotifyPeers sets the destination for matching peers. Sends precede stream
// delivery, but separate channels do not guarantee the consumer processes peers
// first. The channel belongs to the application and must stay open while serving.
// A full channel applies backpressure until the serving context ends.
func (s *Subsystem) NotifyPeers(peers chan<- *Peer) error {
	s.stack.mu.Lock()
	defer s.stack.mu.Unlock()
	if s.stack.serving {
		return errors.New("stack registration is closed")
	}
	if peers == nil {
		return errors.New("nil peer channel")
	}
	s.peers = peers
	return nil
}

// NotifyStreams sets the sole destination for selected streams. The application
// must consume or dispose of queued streams and must not close the channel while
// serving. With no destination, incoming streams for this subsystem are reset.
// A full channel applies cancellable backpressure; streams are never silently lost.
func (s *Subsystem) NotifyStreams(streams chan<- StreamEvent) error {
	s.stack.mu.Lock()
	defer s.stack.mu.Unlock()
	if s.stack.serving {
		return errors.New("stack registration is closed")
	}
	if streams == nil {
		return errors.New("nil stream channel")
	}
	s.streams = streams
	return nil
}

// SetPolicy sets a predicate applied to matching peers before notification or
// stream delivery. It can require a complete selector set or reject a peer.
// Policies must return promptly and may be called concurrently for connections.
// A nil policy accepts any nonempty selector intersection.
func (s *Subsystem) SetPolicy(policy func(*Peer) bool) error {
	s.stack.mu.Lock()
	defer s.stack.mu.Unlock()
	if s.stack.serving {
		return errors.New("stack registration is closed")
	}
	s.policy = policy
	return nil
}

type route struct {
	peer    *Peer
	streams chan<- StreamEvent
}

func (s *Stack) notifyPeers(ctx context.Context, conn transport.Conn, shared []protocol.Selector) (map[protocol.Selector]route, error) {
	matched := make(map[*Subsystem][]protocol.Selector)
	for _, selector := range shared {
		subsystem := s.selectors[selector]
		matched[subsystem] = append(matched[subsystem], selector)
	}
	routes := make(map[protocol.Selector]route)
	for subsystem, selectors := range matched {
		peer := &Peer{Context: ctx, ID: conn.RemotePeerID(), Selectors: selectors, conn: conn}
		if subsystem.policy != nil && !subsystem.policy(peer) {
			continue
		}
		if subsystem.peers != nil {
			select {
			case subsystem.peers <- peer:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		for _, selector := range selectors {
			routes[selector] = route{peer: peer, streams: subsystem.streams}
		}
	}
	return routes, nil
}
