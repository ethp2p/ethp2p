// Package ethp2p owns authenticated connection views and delivers protocol events.
package ethp2p

import (
	"context"
	"errors"
	"net"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethp2p/ethp2p/enr"
	"github.com/ethp2p/ethp2p/protocol"
	"github.com/ethp2p/ethp2p/transport"
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
// NewStack; its zero value is not usable. Transport owns all connection pumps.
type Stack struct {
	mu             sync.Mutex
	transport      *transport.Ethp2pTransport
	config         Config
	serving        bool
	closed         bool
	subsystems     map[string]*Subsystem
	selectors      map[protocol.Selector]*Subsystem
	active         map[transport.PeerID]*stackView
	unreleased     map[transport.Conn]*stackView
	dials          map[transport.PeerID]*dialAttempt
	generations    map[transport.PeerID]uint64
	nextID         ConnID
	sent, received uint64
}

type stackView struct {
	conn     transport.Conn
	info     ConnInfo
	record   atomic.Pointer[enr.Record]
	routes   map[protocol.Selector]*peerDelivery
	cancels  []context.CancelFunc
	peers    []*peerDelivery
	released chan struct{}
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
	return &Stack{transport: t, config: cfg, active: make(map[transport.PeerID]*stackView),
		unreleased: make(map[transport.Conn]*stackView), dials: make(map[transport.PeerID]*dialAttempt),
		generations: make(map[transport.PeerID]uint64)}, nil
}

// Start freezes registration, sends the configured Hello and starts listening.
// It may be called once. Listener errors are reported synchronously.
func (s *Stack) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStackClosed
	}
	if s.serving {
		return errors.New("stack already started")
	}
	s.serving = true
	hello := transport.Hello{Selectors: s.localSelectors()}
	if s.config.Record != nil {
		hello.Record = s.config.Record.Encode()
	}
	return s.transport.Bind(hello, stackSink{s})
}

func (s *Stack) localSelectors() []protocol.Selector {
	selectors := make([]protocol.Selector, 0, len(s.selectors))
	for sel := range s.selectors {
		selectors = append(selectors, sel)
	}
	slices.Sort(selectors)
	return selectors
}

func (s *Stack) retire(v *stackView, code protocol.Code) {
	delete(s.active, v.info.Peer)
	for _, cancel := range v.cancels {
		cancel()
	}
	for _, peer := range v.peers {
		peer.closePeer(code)
	}
}

// cleanupGeneration runs under mu. A stale admission can only still be running
// while its driver's dial record exists, so retaining generations until both
// the active view and dial record are gone is sufficient.
func (s *Stack) cleanupGeneration(id transport.PeerID) {
	if s.active[id] == nil && s.dials[id] == nil {
		delete(s.generations, id)
	}
}

// Disconnect abandons the peer's dial and waits for its active view's release.
// An unknown peer is a no-op.
func (s *Stack) Disconnect(id transport.PeerID) error {
	s.mu.Lock()
	s.generations[id]++
	if d := s.dials[id]; d != nil {
		d.cancel()
	}
	v := s.active[id]
	if v != nil {
		s.retire(v, protocol.Closing)
	}
	s.cleanupGeneration(id)
	s.mu.Unlock()
	if v == nil {
		return nil
	}
	err := v.conn.CloseWithCode(protocol.Closing)
	<-v.released
	return err
}

// Close releases every admitted view, without closing the endpoint or socket.
// The only goroutines the stack starts are these caller-joined close calls.
// Subsequent calls return nil immediately.
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
	var active []*stackView
	for _, v := range s.active {
		active = append(active, v)
		s.retire(v, protocol.Closing)
	}
	var releases []<-chan struct{}
	for _, v := range s.unreleased {
		releases = append(releases, v.released)
	}
	s.mu.Unlock()
	var wg sync.WaitGroup
	errs := make([]error, len(active))
	for i, v := range active {
		wg.Go(func() { errs[i] = v.conn.CloseWithCode(protocol.Closing) })
	}
	wg.Wait()
	for _, released := range releases {
		<-released
	}
	return errors.Join(errs...)
}

// Connections returns active view snapshots sorted by local ConnID.
func (s *Stack) Connections() []ConnInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	infos := make([]ConnInfo, 0, len(s.active))
	for _, v := range s.active {
		infos = append(infos, v.info)
	}
	slices.SortFunc(infos, func(a, b ConnInfo) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	return infos
}

// Traffic returns cumulative QUIC traffic across admitted views, including
// released views. Shared connections include libp2p bytes.
func (s *Stack) Traffic() (sent, received uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sent, received = s.sent, s.received
	for _, v := range s.unreleased {
		a, b := v.conn.ConnectionStats()
		sent += a
		received += b
	}
	return
}

func updateRecord(v *stackView, rec *enr.Record) {
	if rec != nil {
		old := v.record.Load()
		if old == nil || rec.Seq() > old.Seq() {
			v.record.Store(rec)
		}
	}
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
		if v := s.active[id]; v != nil {
			updateRecord(v, rec)
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
			if v := s.active[id]; v != nil {
				updateRecord(v, rec)
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
			_, err := s.transport.Dial(dialCtx, net.UDPAddrFromAddrPort(endpoint), id)
			if err == nil {
				break
			}
			failures = append(failures, err)
			if errors.Is(err, transport.ErrDialLegacyPeer) {
				break
			}
		}
		cancel()
		s.mu.Lock()
		switch {
		case s.closed:
			d.err = ErrStackClosed
		case s.active[id] != nil:
			updateRecord(s.active[id], rec)
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
func streamFailureCode(ctx context.Context, err error) protocol.Code {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) ||
		errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), os.ErrDeadlineExceeded) {
		return protocol.Timeout
	}
	if timeout, ok := errors.AsType[net.Error](err); ok && timeout.Timeout() {
		return protocol.Timeout
	}
	return protocol.Unspecified
}

func cancelTransportStream(stream transport.Stream, code protocol.Code) {
	wire := code.Wire()
	stream.CancelRead(wire)
	stream.CancelWrite(wire)
}
