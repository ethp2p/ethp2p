package ethp2p

import (
	"cmp"
	"context"
	"errors"
	"net"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/ethp2p/ethp2p/enr"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/wire"
)

// ErrStackClosed reports Start or Connect after stack closure.
var ErrStackClosed = errors.New("stack closed")

// ErrDisconnected reports a Connect abandoned by Disconnect.
var ErrDisconnected = errors.New("connection attempt disconnected")

// Config configures a Stack.
type Config struct {
	// Record is the optional local Hello record, signed by the endpoint's identity.
	Record *enr.Record
}

// ConnID identifies an admitted view locally. IDs increase and are never reused.
type ConnID uint64

// ConnInfo is a snapshot of an active view.
type ConnInfo struct {
	ID       ConnID
	Peer     transport.PeerID
	Outbound bool
	Since    time.Time
}

// Stack owns ethp2p views, registration and delivery queues. Construct it with
// NewStack; its zero value is not usable. Transport owns all connection pumps:
// the stack runs one accept loop and one supervisor goroutine per peer.
type Stack struct {
	transport *transport.Ethp2pTransport
	config    Config
	ctx       context.Context // ends in Close; stops the accept loop
	cancel    context.CancelFunc
	wg        sync.WaitGroup // accept loop, supervisors, closeView goroutines; joined by Close

	mu             sync.Mutex
	serving        bool
	closed         bool
	families       []*Family
	selectors      map[wire.Selector]*Family
	peers          map[transport.PeerID]*peerSupervisor
	views          map[*transport.Conn]struct{} // committed views not yet released
	dials          map[transport.PeerID]*dialAttempt
	generations    map[transport.PeerID]uint64
	nextID         ConnID
	sent, received uint64
}

type dialAttempt struct {
	gen       uint64
	rec       *enr.Record
	done      chan struct{}
	err       error
	abandoned bool
	cancel    context.CancelFunc
}

// NewStack binds identity and configuration to an endpoint without starting it.
func NewStack(t *transport.Ethp2pTransport, cfg Config) (*Stack, error) {
	if t == nil {
		return nil, errors.New("nil ethp2p transport")
	}
	if cfg.Record != nil && cfg.Record.PeerID() != t.PeerID() {
		return nil, errors.New("local record identity mismatch")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Stack{transport: t, config: cfg, ctx: ctx, cancel: cancel,
		selectors: make(map[wire.Selector]*Family), peers: make(map[transport.PeerID]*peerSupervisor),
		views: make(map[*transport.Conn]struct{}), dials: make(map[transport.PeerID]*dialAttempt),
		generations: make(map[transport.PeerID]uint64)}, nil
}

// Start freezes registration, sends the configured Hello and starts listening.
// It may be called once. Listener errors are reported synchronously.
func (s *Stack) Start() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrStackClosed
	}
	if s.serving {
		s.mu.Unlock()
		return errors.New("stack already started")
	}
	s.serving = true
	hello := transport.Hello{Selectors: s.localSelectors()}
	if s.config.Record != nil {
		hello.Record = s.config.Record.Encode()
	}
	s.mu.Unlock()
	if err := s.transport.SetHello(hello); err != nil {
		return err
	}
	if err := s.transport.Listen(); err != nil {
		return err
	}
	s.wg.Go(s.acceptLoop)
	return nil
}

// acceptLoop hands every inbound view to its peer's supervisor. A view the
// stack no longer accepts is closed at once.
func (s *Stack) acceptLoop() {
	for {
		conn, err := s.transport.Accept(s.ctx)
		if err != nil {
			return
		}
		if !s.attach(conn, nil) {
			_ = conn.CloseWithCode(wire.Closing)
		}
	}
}

// attach routes a view to its peer's supervisor, starting one when needed.
// It reports whether the supervisor took the view.
func (s *Stack) attach(conn *transport.Conn, result chan<- error) bool {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false
	}
	id := conn.RemotePeerID()
	sup := s.peers[id]
	if sup == nil {
		sup = &peerSupervisor{stack: s, id: id, wake: make(chan struct{}, 1)}
		s.peers[id] = sup
		s.wg.Go(sup.run)
	}
	sup.inbox = append(sup.inbox, supervisorRequest{kind: reqAttach, conn: conn, result: result})
	s.mu.Unlock()
	select {
	case sup.wake <- struct{}{}:
	default:
	}
	return true
}

// sharedFamilies returns the registered Families whose selectors are all in
// selectors (ascending).
func (s *Stack) sharedFamilies(selectors []wire.Selector) []*Family {
	s.mu.Lock()
	defer s.mu.Unlock()
	var shared []*Family
	for _, family := range s.families {
		if family.sharedWith(selectors) {
			shared = append(shared, family)
		}
	}
	return shared
}

// closeView releases a view with code in a goroutine joined by Close: the
// QUIC close bounds GoAway delivery, so the supervisor never waits for it.
// done, if given, closes once the view's traffic is accounted.
func (s *Stack) closeView(conn *transport.Conn, code wire.Code, done chan<- struct{}) {
	s.wg.Go(func() {
		_ = conn.CloseWithCode(code)
		s.mu.Lock()
		if _, ok := s.views[conn]; ok {
			sent, received := conn.ConnectionStats()
			s.sent += sent
			s.received += received
			delete(s.views, conn)
		}
		s.mu.Unlock()
		if done != nil {
			close(done)
		}
	})
}

// cleanupGeneration runs under mu. A stale admission can only still be running
// while its driver's dial record exists, so retaining generations until both
// the peer's supervisor and dial record are gone is sufficient.
func (s *Stack) cleanupGeneration(id transport.PeerID) {
	if s.peers[id] == nil && s.dials[id] == nil {
		delete(s.generations, id)
	}
}

// localSelectors runs under mu. Registration is immutable after Start, so
// attach-time readers never race it.
func (s *Stack) localSelectors() []wire.Selector {
	selectors := make([]wire.Selector, 0, len(s.selectors))
	for sel := range s.selectors {
		selectors = append(selectors, sel)
	}
	slices.Sort(selectors)
	return selectors
}

// Disconnect abandons the peer's dial and releases its view, returning once
// the view is released. The generation gate keeps an outbound dial that
// completes afterwards from being accepted. An unknown peer is a no-op.
func (s *Stack) Disconnect(id transport.PeerID) error {
	s.mu.Lock()
	s.generations[id]++
	if d := s.dials[id]; d != nil {
		d.cancel()
	}
	sup := s.peers[id]
	if sup == nil {
		s.cleanupGeneration(id)
		s.mu.Unlock()
		return nil
	}
	done := make(chan struct{})
	sup.inbox = append(sup.inbox, supervisorRequest{kind: reqDisconnect, done: done})
	s.mu.Unlock()
	select {
	case sup.wake <- struct{}{}:
	default:
	}
	<-done
	return nil
}

// Close releases every view without closing the endpoint or socket, then joins
// every stack goroutine. It is idempotent: later calls return nil at once.
func (s *Stack) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	for _, d := range s.dials {
		d.cancel()
	}
	// The accept loop observes this; supervisors drain their disconnect below.
	s.cancel()
	var sups []*peerSupervisor
	var dones []chan struct{}
	for _, sup := range s.peers {
		done := make(chan struct{})
		sup.inbox = append(sup.inbox, supervisorRequest{kind: reqDisconnect, done: done})
		sups = append(sups, sup)
		dones = append(dones, done)
	}
	s.mu.Unlock()
	for _, sup := range sups {
		select {
		case sup.wake <- struct{}{}:
		default:
		}
	}
	s.transport.Close()
	for _, done := range dones {
		<-done
	}
	s.wg.Wait()
	return nil
}

// Connections returns snapshots of connected views sorted by local ConnID.
func (s *Stack) Connections() []ConnInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	var infos []ConnInfo
	for _, sup := range s.peers {
		if sup.connected {
			infos = append(infos, sup.info)
		}
	}
	slices.SortFunc(infos, func(a, b ConnInfo) int {
		return cmp.Compare(a.ID, b.ID)
	})
	return infos
}

// Traffic returns cumulative QUIC traffic across committed views, including
// released ones. Shared connections include libp2p bytes.
func (s *Stack) Traffic() (sent, received uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sent, received = s.sent, s.received
	for conn := range s.views {
		a, b := conn.ConnectionStats()
		sent += a
		received += b
	}
	return
}

// Connect authenticates and admits a peer using the record's endpoints.
// Concurrent callers share a dial driven in one caller's goroutine. If that
// caller cancels, a live waiter may take over. No dial goroutine is detached.
func (s *Stack) Connect(ctx context.Context, rec *enr.Record) error {
	if rec == nil {
		return errors.New("nil peer record")
	}
	id := rec.PeerID()
	if id == s.transport.PeerID() {
		return errors.New("cannot connect to self")
	}
	endpoints := rec.QUIC()
	if len(endpoints) == 0 {
		return errors.New("record has no QUIC endpoints")
	}
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return ErrStackClosed
		}
		if !s.serving {
			s.mu.Unlock()
			return errors.New("stack not started")
		}
		if sup := s.peers[id]; sup != nil && sup.connected {
			sup.updateRecord(rec)
			s.mu.Unlock()
			return nil
		}
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			return err
		}
		if d := s.dials[id]; d != nil {
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-d.done:
			}
			s.mu.Lock()
			if sup := s.peers[id]; sup != nil && sup.connected {
				sup.updateRecord(rec)
				s.mu.Unlock()
				return nil
			}
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return ErrStackClosed
			}
			if d.abandoned && ctx.Err() == nil {
				continue
			}
			return d.err
		}
		dialCtx, cancel := context.WithCancel(ctx)
		d := &dialAttempt{gen: s.generations[id], rec: rec, done: make(chan struct{}), cancel: cancel}
		s.dials[id] = d
		s.mu.Unlock()
		var failures []error
		for _, endpoint := range endpoints {
			if dialCtx.Err() != nil {
				failures = append(failures, dialCtx.Err())
				break
			}
			conn, err := s.transport.Dial(dialCtx, net.UDPAddrFromAddrPort(endpoint), id)
			if err != nil {
				failures = append(failures, err)
				if errors.Is(err, transport.ErrDialLegacyPeer) {
					break
				}
				continue
			}
			result := make(chan error, 1)
			if !s.attach(conn, result) {
				_ = conn.CloseWithCode(wire.Closing)
				break
			}
			if err := <-result; err != nil {
				failures = append(failures, err)
				continue
			}
			break
		}
		cancel()
		s.mu.Lock()
		switch {
		case s.closed:
			d.err = ErrStackClosed
		case s.peers[id] != nil && s.peers[id].connected:
			// The attempt's own view, or one that beat it under the duplicate
			// rule, is active: either way the peer is connected.
			s.peers[id].updateRecord(rec)
		case d.gen != s.generations[id]:
			d.err = ErrDisconnected
		default:
			d.err = errors.Join(failures...)
			if d.err == nil {
				d.err = errors.New("admitted view closed before Connect completed")
			}
			d.abandoned = ctx.Err() != nil
		}
		delete(s.dials, id)
		s.cleanupGeneration(id)
		close(d.done)
		s.mu.Unlock()
		return d.err
	}
}

// streamFailureCode maps failed I/O to Timeout for expired deadlines and to
// Unspecified for cancellation and other errors.
func streamFailureCode(ctx context.Context, err error) wire.Code {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) ||
		errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), os.ErrDeadlineExceeded) {
		return wire.Timeout
	}
	if timeout, ok := errors.AsType[net.Error](err); ok && timeout.Timeout() {
		return wire.Timeout
	}
	return wire.Unspecified
}

func cancelTransportStream(stream transport.Stream, code wire.Code) {
	raw := code.Wire()
	stream.CancelRead(raw)
	stream.CancelWrite(raw)
}
