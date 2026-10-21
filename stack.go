package ethp2p

import (
	"cmp"
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/ethp2p/ethp2p/control"
	"github.com/ethp2p/ethp2p/enr"
	"github.com/ethp2p/ethp2p/identity"
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
	// HelloTimeout is HELLO_TIMEOUT, counted from when the stack receives
	// a view. Zero means 5 seconds.
	HelloTimeout time.Duration
	// HeaderTimeout is CLASSIFY_TIMEOUT for reading one stream header.
	// Zero means 5 seconds.
	HeaderTimeout time.Duration
}

const defaultTimeout = 5 * time.Second

func (c Config) helloTimeout() time.Duration  { return cmp.Or(c.HelloTimeout, defaultTimeout) }
func (c Config) headerTimeout() time.Duration { return cmp.Or(c.HeaderTimeout, defaultTimeout) }

// ConnID identifies an admitted view locally. IDs increase and are never reused.
type ConnID uint64

// ConnInfo is a snapshot of an active view.
type ConnInfo struct {
	ID       ConnID
	Peer     identity.PeerID
	Outbound bool
	Since    time.Time
}

// Stack owns ethp2p views, registration, the control protocol and delivery
// queues. Construct it with NewStack; its zero value is not usable. It runs
// one accept loop, one control handshake per new view, and one supervisor
// goroutine per peer.
type Stack struct {
	transport *transport.Ethp2pTransport
	config    Config
	ctx       context.Context // ends in Close; stops the accept loop and handshakes
	cancel    context.CancelFunc
	// wg holds the accept loop, handshakes, supervisors, control writers and
	// readers, header reads and closeView goroutines; joined by Close.
	wg sync.WaitGroup
	// helloBytes is our control stream's header and HELLO, fixed by Start.
	helloBytes []byte
	// acceptDone closes when the accept loop returns; set by Start.
	acceptDone chan struct{}

	mu             sync.Mutex
	protocols      []*Protocol // appended by Register; immutable once serving
	peers          map[identity.PeerID]*peerSupervisor
	views          map[*transport.Conn]struct{} // committed views not yet released
	dials          map[identity.PeerID]*dialAttempt
	nextID         ConnID
	sent, received uint64
}

// dialState is the lifecycle of one dial attempt. err is valid only in
// dialDone (and in dialDisconnected, where it is ErrDisconnected).
type dialState uint8

const (
	dialInFlight dialState = iota
	// dialDisconnected: Disconnect landed mid-dial; the attempt's outbound
	// view is rejected and its result is ErrDisconnected.
	dialDisconnected
	// dialDriverGone: the driving caller cancelled; a live waiter may
	// take over with a fresh attempt.
	dialDriverGone
	dialDone
)

type dialAttempt struct {
	rec    *enr.Record
	done   chan struct{}
	state  dialState
	err    error
	cancel context.CancelFunc
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
		peers: make(map[identity.PeerID]*peerSupervisor),
		views: make(map[*transport.Conn]struct{}), dials: make(map[identity.PeerID]*dialAttempt)}, nil
}

// Start freezes registration, fixes the HELLO every view sends, and starts
// accepting views. It may be called once. Listening itself begins at
// Endpoint.Start, which the application calls before Start per
// the startup choreography.
func (s *Stack) Start() error {
	// mu is held until the accept loop is in wg, so a Close that takes mu
	// next either sees the loop spawned or none at all. Listen never calls
	// back into the stack, so holding mu across it cannot deadlock.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return ErrStackClosed
	}
	if s.helloBytes != nil {
		return errors.New("stack already started")
	}
	var record []byte
	if s.config.Record != nil {
		record = s.config.Record.Encode()
	}
	helloBytes, err := control.HelloBytes(s.localProtocols(), record)
	if err != nil {
		return err
	}
	s.helloBytes = helloBytes
	s.acceptDone = make(chan struct{})
	s.wg.Go(func() {
		defer close(s.acceptDone)
		s.acceptLoop()
	})
	return nil
}

// acceptLoop runs each inbound view's handshake in its own goroutine, so a
// peer slow to send HELLO never delays the next accept, then hands the view
// to its peer's supervisor.
func (s *Stack) acceptLoop() {
	for {
		conn, err := s.transport.Accept(s.ctx)
		if err != nil {
			return
		}
		s.wg.Go(func() {
			if v, err := s.handshake(s.ctx, conn); err == nil && !s.attach(v, nil) {
				v.release(wire.Closing)
			}
		})
	}
}

// handshake writes our HELLO (openControl), then waits for the peer's, both
// within HELLO_TIMEOUT of now. On failure the view is released and the cause
// returned.
func (s *Stack) handshake(ctx context.Context, conn *transport.Conn) (*pendingView, error) {
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(s.config.helloTimeout()))
	defer cancel()
	ctl, err := openControl(ctx, conn, s.helloBytes)
	if err != nil {
		return nil, err
	}
	h, err := ctl.awaitHello(ctx)
	if err != nil {
		return nil, err
	}
	return &pendingView{ctl: ctl, hello: h}, nil
}

type pendingView struct {
	ctl   *controlSession
	hello control.PeerHello
}

// release sends GOAWAY and releases the view. Transport resets any streams
// still queued for admission with Closing.
func (v *pendingView) release(code wire.Code) { v.ctl.close(code, true) }

// attach routes a view to its peer's supervisor, starting one when needed.
// It reports whether the supervisor took the view.
func (s *Stack) attach(v *pendingView, result chan<- error) bool {
	s.mu.Lock()
	if s.ctx.Err() != nil {
		s.mu.Unlock()
		return false
	}
	id := v.ctl.conn.RemotePeerID()
	sup := s.peers[id]
	if sup == nil {
		sup = &peerSupervisor{stack: s, id: id, wake: make(chan struct{}, 1)}
		s.peers[id] = sup
		s.wg.Go(sup.run)
	}
	// result (capacity 1) is Connect's report; the accept loop passes nil.
	sup.inbox = append(sup.inbox, func() {
		if err := sup.attach(v); result != nil {
			result <- err
		}
	})
	s.mu.Unlock()
	sup.signal()
	return true
}

// closeView releases a view with code in a goroutine joined by Close:
// GOAWAY_TIMEOUT bounds the GOAWAY write, so the supervisor never waits for
// it. goAway is false when the peer closed the view or the connection ended.
// done, if given, closes once the view's traffic is accounted.
func (s *Stack) closeView(ctl *controlSession, code wire.Code, goAway bool, done chan<- struct{}) {
	conn := ctl.conn
	s.wg.Go(func() {
		ctl.close(code, goAway)
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

// localProtocols runs under mu.
func (s *Stack) localProtocols() []wire.Protocol {
	protocols := make([]wire.Protocol, len(s.protocols))
	for i, protocol := range s.protocols {
		protocols[i] = protocol.id
	}
	slices.Sort(protocols)
	return protocols
}

// Disconnect abandons the peer's dial and releases its view, returning once
// the view is released. Marking the dial record disconnected keeps an outbound
// dial that completes afterwards from being accepted; a dial started later
// gets a fresh record and is unaffected. An unknown peer is a no-op.
func (s *Stack) Disconnect(id identity.PeerID) error {
	s.mu.Lock()
	if d := s.dials[id]; d != nil && d.state == dialInFlight {
		d.state = dialDisconnected
		d.cancel()
	}
	sup := s.peers[id]
	if sup == nil {
		s.mu.Unlock()
		return nil
	}
	done := make(chan struct{})
	sup.inbox = append(sup.inbox, func() { sup.dropView(wire.Closing, true, done) })
	s.mu.Unlock()
	sup.signal()
	<-done
	return nil
}

// Close releases every view without closing the endpoint or socket, then joins
// every stack goroutine. It is idempotent: later calls return nil at once.
// The phases are sequenced: cancel stops the accept loop and handshakes,
// the accept loop (the only handshake spawner) is joined, supervisors are
// signalled to drop their views, and only then is wg joined - so no
// goroutine can be added to wg while Close waits on it.
func (s *Stack) Close() error {
	s.mu.Lock()
	if s.ctx.Err() != nil {
		s.mu.Unlock()
		return nil
	}
	for _, d := range s.dials {
		d.cancel()
	}
	s.cancel()
	acceptDone := s.acceptDone
	var sups []*peerSupervisor
	for _, sup := range s.peers {
		sup.inbox = append(sup.inbox, func() { sup.dropView(wire.Closing, true, nil) })
		sups = append(sups, sup)
	}
	s.mu.Unlock()
	if acceptDone != nil {
		<-acceptDone
	}
	for _, sup := range sups {
		sup.signal()
	}
	// Supervisors and the closeView goroutines they start are all in wg, so
	// this also waits for every view's release.
	s.wg.Wait()
	return nil
}

// Connections returns snapshots of connected views sorted by local ConnID.
func (s *Stack) Connections() []ConnInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	var infos []ConnInfo
	for _, sup := range s.peers {
		if sup.info != nil {
			infos = append(infos, *sup.info)
		}
	}
	slices.SortFunc(infos, func(a, b ConnInfo) int { return cmp.Compare(a.ID, b.ID) })
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
		if s.ctx.Err() != nil {
			s.mu.Unlock()
			return ErrStackClosed
		}
		if s.helloBytes == nil {
			s.mu.Unlock()
			return errors.New("stack not started")
		}
		if sup := s.peers[id]; sup != nil && sup.info != nil {
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
			if sup := s.peers[id]; sup != nil && sup.info != nil {
				sup.updateRecord(rec)
				s.mu.Unlock()
				return nil
			}
			closed := s.ctx.Err() != nil
			s.mu.Unlock()
			if closed {
				return ErrStackClosed
			}
			if d.state == dialDriverGone && ctx.Err() == nil {
				continue
			}
			return d.err
		}
		dialCtx, cancel := context.WithCancel(ctx)
		d := &dialAttempt{rec: rec, done: make(chan struct{}), cancel: cancel}
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
					// Every endpoint reaches the same libp2p-only peer, so stop
					// and leave retry policy to the caller; no peer entry exists.
					break
				}
				continue
			}
			v, err := s.handshake(dialCtx, conn)
			if err != nil {
				failures = append(failures, err)
				continue
			}
			result := make(chan error, 1)
			if !s.attach(v, result) {
				v.release(wire.Closing)
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
		case s.ctx.Err() != nil:
			d.err = ErrStackClosed
		case s.peers[id] != nil && s.peers[id].info != nil:
			// The attempt's own view, or one that beat it under the duplicate
			// rule, is active: either way the peer is connected.
			s.peers[id].updateRecord(rec)
		case d.state == dialDisconnected:
			d.err = ErrDisconnected
		default:
			d.err = errors.Join(failures...)
			if d.err == nil {
				d.err = errors.New("admitted view closed before Connect completed")
			}
			if ctx.Err() != nil {
				d.state = dialDriverGone
			}
		}
		if d.state == dialInFlight {
			d.state = dialDone
		}
		delete(s.dials, id)
		close(d.done)
		s.mu.Unlock()
		return d.err
	}
}
