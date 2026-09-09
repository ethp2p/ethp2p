package ethp2p

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethp2p/ethp2p/protocol"
	"github.com/ethp2p/ethp2p/transport"
)

// Peer describes one connection's match with a subsystem. Treat its fields as
// read-only. Context ends when ServeConn returns; Conn is borrowed from the
// application. Selectors lists the subsystem's shared selectors in canonical order.
// The pointer identifies this connection's notification, including before the
// application has processed the peer channel. A reconnect gets a different Peer.
type Peer struct {
	Context   context.Context
	Conn      transport.Conn
	Selectors []protocol.Selector
}

// StreamEvent transfers one selected stream to a subsystem. Stream retains all
// protocol data after the selector. Bidirectional streams also implement
// transport.Stream. The receiver owns stream cleanup after successful delivery,
// even if the event is still queued when Peer.Context ends.
type StreamEvent struct {
	Peer     *Peer
	Selector protocol.Selector
	Stream   transport.ReceiveStream
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
		if selector == 0 || selector == '/' {
			return nil, fmt.Errorf("reserved selector %d", selector)
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
		peer := &Peer{Context: ctx, Conn: conn, Selectors: selectors}
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
