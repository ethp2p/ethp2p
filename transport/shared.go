package transport

import (
	"bytes"
	"context"
	"crypto/hkdf"
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethp2p/ethp2p/protocol"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/quic-go/quic-go"
)

const (
	// maxLibp2pStreamsPendingDelivery bounds only the libp2p stream queue.
	maxLibp2pStreamsPendingDelivery = 8
	// maxStreamsPendingClassify limits concurrent stream classification per conn.
	// This handles sustained packet loss and malicious peers.
	maxStreamsPendingClassify = 4
	// maxConnsPendingDelivery is the limit of conns pending delivery to the downstream stack, per stack.
	maxConnsPendingDelivery = 16
)

var classifyTimeout = func() *atomic.Int64 {
	d := new(atomic.Int64)
	d.Store(int64(5 * time.Second))
	return d
}()

var helloTimeout = func() *atomic.Int64 {
	d := new(atomic.Int64)
	d.Store(int64(5 * time.Second))
	return d
}()

var goAwayTimeout = time.Second

type side uint32

// Side bitmasks for SharedTransport.interest and sharedConn.closed.
const (
	sideLibp2p side = 1 << iota
	sideEthp2p
)

// SharedTransport owns a QUIC endpoint shared by libp2p and ethp2p.
// The application owns endpoint shutdown; ethp2p.Stack owns only its views.
// A closed transport cannot be restarted.
type SharedTransport struct {
	raw        *quic.Transport
	handshaker *handshaker
	profile    Profile

	// ctx is the transport lifetime.
	ctx    context.Context
	cancel context.CancelFunc

	// Each queue holds views only after its side has registered interest.
	// Unclaimed or unqueueable views are released immediately.
	libQ chan quicreuse.QUICConn
	ethQ chan Conn

	// interest records, as a bitmap of side values, the sides whose transport
	// the application requested. Views for other sides are released at once.
	interest  atomic.Uint32
	helloMu   sync.RWMutex
	hello     Hello
	hasHello  bool
	sink      Sink // protected by helloMu; immutable after Bind
	sinkBound chan struct{}
	// libListening is set by the first Libp2pTransport.Listen and never cleared.
	libListening atomic.Bool
	// libMu serializes libp2p offers with listener detachment, so no view can
	// be queued after Close drains libQ. libDone closes on detachment.
	libMu       sync.Mutex
	libDetached bool
	libDone     chan struct{}

	// Listener startup is attempted once, including on failure.
	// lnOnce synchronizes access to lnErr between concurrent attaches.
	ln     atomic.Pointer[quic.Listener]
	lnOnce sync.Once
	lnErr  error

	// lifetimeMu gates external reservations against cancellation of ctx, the
	// single source of truth for shutdown. Tracked parents may add children
	// while shutdown waits, but no external caller may revive a drained wg.
	lifetimeMu sync.Mutex
	// wg tracks listener startup, outbound Dial reservations, and all accept,
	// dispatcher, classifier, control and sink pump goroutines. Dial transfers
	// its reservation to an admitted pump; shutdown joins them after raw.Close.
	wg sync.WaitGroup
}

// NewShared creates a shared QUIC endpoint using key for authentication and
// profile for connection policy. Listening starts on the first
// [Libp2pTransport.Listen] or [Ethp2pTransport.Accept].
// The caller remains responsible for closing packetConn.
func NewShared(key *PrivKey, packetConn net.PacketConn, profile Profile) (*SharedTransport, error) {
	if packetConn == nil {
		return nil, errNilPacketConn
	}
	handshaker, err := newHandshaker(key)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	resetBytes, err := hkdf.Key(sha256.New, key.Bytes(), nil, "ethp2p stateless reset", 32)
	if err != nil {
		cancel()
		return nil, err
	}
	var resetKey quic.StatelessResetKey
	copy(resetKey[:], resetBytes)
	t := &SharedTransport{
		raw:        &quic.Transport{Conn: packetConn, StatelessResetKey: &resetKey},
		handshaker: handshaker,
		profile:    profile,
		ctx:        ctx,
		cancel:     cancel,
		libQ:       make(chan quicreuse.QUICConn, maxConnsPendingDelivery),
		ethQ:       make(chan Conn, maxConnsPendingDelivery),
		sinkBound:  make(chan struct{}),
		libDone:    make(chan struct{}),
	}

	// ConnContext installs the per-connection identity slot that the verify
	// callback fills during the accept handshake and acceptLoop reads back.
	// It is not used for dialed connections, which own their slot locally.
	t.raw.ConnContext = func(ctx context.Context, _ *quic.ClientInfo) (context.Context, error) {
		return context.WithValue(ctx, remoteIdentityContextKey{}, &remoteIdentitySlot{}), nil
	}
	return t, nil
}

// Libp2p returns the transport to lend through
// quicreuse.ConnManager.LendTransport and registers interest in libp2p views.
// Until the first call, the libp2p view of every new connection is released
// immediately, so request it before connections arrive.
func (t *SharedTransport) Libp2p() quicreuse.QUICTransport {
	t.interest.Or(uint32(sideLibp2p))
	return &Libp2pTransport{t}
}

// Ethp2p returns the ethp2p transport. SetHello registers interest in views.
func (t *SharedTransport) Ethp2p() *Ethp2pTransport {
	return &Ethp2pTransport{t}
}

// PeerID returns the local identity.
func (t *SharedTransport) PeerID() PeerID { return t.handshaker.peerID }

// PublicKey returns the local identity key.
func (t *SharedTransport) PublicKey() *PubKey { return t.handshaker.publicKey }

// Addr returns the local packet connection address.
func (t *SharedTransport) Addr() net.Addr { return t.raw.Conn.LocalAddr() }

// Close stops listening, closes all connections, and waits for transport
// goroutines to exit. It does not close the supplied packet connection.
// Repeated calls are safe. The application owns this call on shutdown.
// [Libp2pTransport.Close] is a no-op.
func (t *SharedTransport) Close() error { return t.shutdown() }

// start begins listening unless the transport is closed.
func (t *SharedTransport) start() error {
	if t.ctx.Err() != nil {
		return ErrClosed
	}
	return t.ensureListener()
}

// offerEthp2p starts a bound sink pump or queues an unbound view in ethQ.
// Unclaimed views and unbound queue overflow release only the ethp2p view;
// libp2p delivery is independent.
func (t *SharedTransport) offerEthp2p(c *ethp2pConn) {
	if t.interest.Load()&uint32(sideEthp2p) == 0 {
		_ = c.Close()
		return
	}
	if err := c.startControl(t.helloSnapshot()); err != nil {
		_ = c.Close()
		return
	}
	t.helloMu.RLock()
	sink := t.sink
	t.helloMu.RUnlock()
	if sink != nil {
		t.wg.Go(func() {
			if err := t.admit(t.ctx, c, sink); err == nil {
				t.pump(c, sink)
			}
		})
		return
	}
	select {
	case t.ethQ <- c:
	default:
		// A blocked Hello must not delay libp2p delivery or the next accept.
		t.wg.Go(func() { _ = c.CloseWithCode(protocol.Overloaded) })
	}
}

// offerLibp2p hands a libp2p view, or a legacy libp2p-only connection, to
// libQ. It releases the connection instead when libp2p was never requested,
// has detached, or libQ is full.
func (t *SharedTransport) offerLibp2p(c quicreuse.QUICConn) {
	t.libMu.Lock()
	queued, code, reason := false, appNoError, "unclaimed"
	if !t.libDetached && t.interest.Load()&uint32(sideLibp2p) != 0 {
		select {
		case t.libQ <- c:
			queued = true
		default:
			code, reason = appFailure, "delivery queue full"
		}
	}
	t.libMu.Unlock()
	if !queued {
		_ = c.CloseWithError(code, reason)
	}
}

// detachLibp2p stops delivery to libp2p and releases the views it never
// accepted. It is idempotent and leaves ethp2p running.
func (t *SharedTransport) detachLibp2p() {
	t.libMu.Lock()
	if t.libDetached {
		t.libMu.Unlock()
		return
	}
	t.libDetached = true
	close(t.libDone)
	var queued []quicreuse.QUICConn
drain:
	for {
		select {
		case c := <-t.libQ:
			queued = append(queued, c)
		default:
			break drain
		}
	}
	t.libMu.Unlock()
	for _, c := range queued {
		_ = c.CloseWithError(appNoError, "listener closed")
	}
}

// ensureListener starts the raw listener and its accept loop exactly once.
// A failed start is cached and returned on every later call. lnOnce
// synchronizes reads of lnErr across concurrent calls.
func (t *SharedTransport) ensureListener() error {
	if t.ln.Load() != nil {
		return nil
	}
	t.lnOnce.Do(func() {
		release, ok := t.reserve()
		if !ok {
			t.lnErr = ErrClosed
			return
		}
		defer release()
		ln, err := t.raw.Listen(t.handshaker.serverConfig(), t.profile.quicConfig())
		if err != nil {
			t.lnErr = err
			return
		}
		t.ln.Store(ln)
		t.wg.Go(func() { t.acceptLoop(ln) })
	})
	return t.lnErr
}

func (t *SharedTransport) acceptLoop(ln *quic.Listener) {
	for {
		raw, err := ln.Accept(t.ctx)
		if err != nil {
			return
		}

		state := raw.ConnectionState()
		switch state.TLS.NegotiatedProtocol {
		case AlpnLibp2p:
			// peer is legacy: the libp2p view is the whole connection
			t.offerLibp2p(raw)
		case AlpnEthp2p:
			sc := newSharedConn(raw, &t.wg, acceptedPeerID(raw))
			t.offerEthp2p(sc.ethp2p())
			t.offerLibp2p(sc.libp2p())
			sc.startDispatchers()
		default:
			_ = raw.CloseWithError(appFailure, "unsupported negotiated protocol")
		}
	}
}

// acceptedPeerID returns the peer identity authenticated during the accept
// handshake. An ethp2p_0 connection always has one, because the ALPN could only
// be negotiated after the verify callback accepted the peer's certificate.
func acceptedPeerID(raw *quic.Conn) PeerID {
	slot, _ := raw.Context().Value(remoteIdentityContextKey{}).(*remoteIdentitySlot)
	if slot == nil || slot.key == nil {
		return ""
	}
	return slot.key.PeerID()
}

// shutdown cancels transport waits and closes the endpoint before joining
// its goroutines. Closing the endpoint unblocks stream acceptance and
// classification on every connection, including views still queued for delivery.
func (t *SharedTransport) shutdown() error {
	t.lifetimeMu.Lock()
	t.cancel()
	t.lifetimeMu.Unlock()
	err := t.raw.Close()
	t.wg.Wait()
	return err
}

// reserve keeps a caller and any goroutines it starts owned by shutdown.
// A successful reservation must be released, or transferred to one child.
func (t *SharedTransport) reserve() (release func(), ok bool) {
	t.lifetimeMu.Lock()
	defer t.lifetimeMu.Unlock()
	if t.ctx.Err() != nil {
		return nil, false
	}
	t.wg.Add(1)
	return t.wg.Done, true
}

//
// CONNECTION
//////////////////////

type sharedConn struct {
	outbound bool
	conn     *quic.Conn
	created  time.Time // one Hello deadline, including control-stream arrival
	biSem    chan struct{}
	uniSem   chan struct{}
	// Each view's context ends when that view closes or the connection does;
	// pending operations on the view wait on it.
	libp2pCtx    context.Context
	cancelLibp2p context.CancelCauseFunc
	ethp2pCtx    context.Context
	cancelEthp2p context.CancelCauseFunc
	// wg owns this connection's control, dispatcher and sink goroutines.
	// Starts require a tracked parent: the accept loop or a Dial reservation.
	wg *sync.WaitGroup

	// delivery queues for classified incoming streams
	libp2pBi       chan *quic.Stream
	ethp2pBi       *streamQueue[selectedBi]
	ethp2pUni      *streamQueue[selectedUni]
	controlIn      chan *quic.ReceiveStream
	controlClaimed atomic.Bool
	control        controlState

	// View-closed bitfield over sideLibp2p|sideEthp2p. The raw connection
	// closes once both views are closed, so closing one view never kills
	// the other. Accessed atomically.
	closed atomic.Uint32

	// remote is the identity authenticated for the remote endpoint.
	remote PeerID
}

type selectedBi struct {
	stream   *quic.Stream
	selector protocol.Selector
}

type selectedUni struct {
	stream   *quic.ReceiveStream
	selector protocol.Selector
	frameLen int
}

// newSharedConn splits an ethp2p_0 connection into the shared state backing its
// two views. Callers initialize or release the control protocol before starting
// dispatchers, so an inbound violation cannot race outbound control creation.
//
// remote is the peer identity the handshake authenticated. An ethp2p_0
// connection only reaches this point after the verify callback accepted the
// peer's certificate, so the identity is always established by then.
func newSharedConn(raw *quic.Conn, wg *sync.WaitGroup, remote PeerID) *sharedConn {
	libp2pCtx, cancelLibp2p := context.WithCancelCause(raw.Context())
	ethp2pCtx, cancelEthp2p := context.WithCancelCause(raw.Context())
	sc := &sharedConn{
		conn:         raw,
		created:      time.Now(),
		libp2pCtx:    libp2pCtx,
		cancelLibp2p: cancelLibp2p,
		ethp2pCtx:    ethp2pCtx,
		cancelEthp2p: cancelEthp2p,
		wg:           wg,
		remote:       remote,
		biSem:        make(chan struct{}, maxStreamsPendingClassify),
		uniSem:       make(chan struct{}, maxStreamsPendingClassify),
		libp2pBi:     make(chan *quic.Stream, maxLibp2pStreamsPendingDelivery),
		ethp2pBi:     newStreamQueue[selectedBi](),
		ethp2pUni:    newStreamQueue[selectedUni](),
		controlIn:    make(chan *quic.ReceiveStream, 1),
	}
	sc.control.helloReady = make(chan struct{})
	for range maxStreamsPendingClassify {
		sc.biSem <- struct{}{}
		sc.uniSem <- struct{}{}
	}
	return sc
}

func (c *sharedConn) startDispatchers() {
	c.wg.Go(c.drainBidi)
	c.wg.Go(c.drainUni)
}

// remotePeerID returns the identity authenticated for the remote endpoint.
func (c *sharedConn) remotePeerID() PeerID { return c.remote }

// closeSide releases one view and closes the connection when both are released.
func (c *sharedConn) closeSide(want side, code quic.ApplicationErrorCode, reason string) {
	switch want {
	case sideLibp2p:
		c.cancelLibp2p(ErrViewClosed)
	case sideEthp2p:
		c.cancelEthp2p(&ViewClosedError{Code: protocol.Closing})
	}
	// Or returns the pre-OR value. RMW atomicity means no other close
	// can land between its read and write, so old|want is the post-OR value.
	// Releasing the last view closes the connection before its queued streams
	// are reset, so the resets cannot hand the peer stream credit.
	if c.closed.Or(uint32(want))|uint32(want) == uint32(sideLibp2p|sideEthp2p) {
		_ = c.conn.CloseWithError(code, reason)
	}
	switch want {
	case sideLibp2p:
		c.drainLibp2pQueue()
	case sideEthp2p:
		c.drainEthp2pQueues()
	}
}

// viewContext bounds a caller's wait by its view without losing the caller's
// deadline or cancellation. The returned cleanup stops the callback.
func viewContext(ctx, view context.Context) (context.Context, func()) {
	bound, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(view, func() { cancel(context.Cause(view)) })
	if view.Err() != nil {
		cancel(context.Cause(view))
	}
	return bound, func() { stop(); cancel(nil) }
}

func (c *sharedConn) drainLibp2pQueue() {
	for {
		select {
		case stream := <-c.libp2pBi:
			resetBi(stream, protocol.Closing)
		default:
			return
		}
	}
}

func (c *sharedConn) drainEthp2pQueues() {
	for _, s := range c.ethp2pBi.close() {
		resetBi(s.stream, protocol.Closing)
	}
	for _, s := range c.ethp2pUni.close() {
		s.stream.CancelRead(quicCode(protocol.Closing))
	}
}

// ethp2p returns the ethp2p view of the shared connection.
func (c *sharedConn) ethp2p() *ethp2pConn {
	return &ethp2pConn{sharedConn: c}
}

// libp2p returns the libp2p view of the shared connection.
func (c *sharedConn) libp2p() quicreuse.QUICConn {
	return (*libp2pConn)(c)
}

// drainBidi accepts bidirectional streams and classifies them.
// The semaphore limits the number of concurrent classifications.
func (c *sharedConn) drainBidi() {
	ctx := c.conn.Context()
	for {
		select {
		case <-c.biSem:
		case <-ctx.Done():
			return
		}
		stream, err := c.conn.AcceptStream(ctx)
		if err != nil {
			c.biSem <- struct{}{}
			if ctx.Err() == nil {
				slog.Warn("failed to accept stream", "err", err)
			}
			return
		}
		c.wg.Go(func() {
			defer func() { c.biSem <- struct{}{} }()
			isEthp2p, selector, err := classifyBi(stream)
			if err != nil {
				if ctx.Err() == nil {
					if peerReset(err) {
						stream.CancelWrite(quicCode(protocol.Unspecified))
					} else {
						resetBi(stream, classificationCode(err))
					}
				}
				return
			}
			if isEthp2p {
				if c.ethp2pCtx.Err() != nil {
					resetBi(stream, protocol.Closing)
					return
				}
				if selector == protocol.ControlSelector {
					resetBi(stream, protocol.ControlViolation)
					c.controlViolation()
					return
				}
				// The send side stays open until the protocol ends it, so a
				// queued bidi stream holds credit even after receiving FIN.
				if !c.ethp2pBi.push(selectedBi{stream, selector}) {
					resetBi(stream, protocol.Closing)
				}
				return
			}
			if c.libp2pCtx.Err() != nil {
				resetBi(stream, protocol.Closing)
				return
			}
			select {
			case c.libp2pBi <- stream:
				if c.libp2pCtx.Err() != nil {
					c.drainLibp2pQueue()
				}
			default:
				resetBi(stream, protocol.Overloaded)
			}
		})
	}
}

type peekableStream interface {
	io.Reader
	Peek([]byte) (int, error)
	SetReadDeadline(time.Time) error
}

// classifyBi consumes the selector for ethp2p, but leaves libp2p's head intact.
func classifyBi[S peekableStream](stream S) (ethp2p bool, selector protocol.Selector, err error) {
	if err := stream.SetReadDeadline(time.Now().Add(time.Duration(classifyTimeout.Load()))); err != nil {
		return false, 0, err
	}
	defer func() {
		_ = stream.SetReadDeadline(time.Time{})
	}()
	var head [2]byte
	if _, err := stream.Peek(head[:1]); err != nil {
		return false, 0, err
	}
	if head[0] >= 1 && head[0] <= 10 {
		selector, err := protocol.ReadSelector(stream)
		return true, selector, err
	}
	if _, err := stream.Peek(head[:2]); err != nil {
		return false, 0, err
	}
	if head[1] == '/' {
		return false, 0, nil
	}
	return false, 0, protocol.ErrInvalidSelectors
}

func classifyUni(stream *quic.ReceiveStream) (protocol.Selector, int, error) {
	// A truncated frame followed by FIN has no live sender side to notify.
	if err := stream.SetReadDeadline(time.Now().Add(time.Duration(classifyTimeout.Load()))); err != nil {
		return 0, 0, err
	}
	defer func() { _ = stream.SetReadDeadline(time.Time{}) }()
	var frame [11]byte
	if _, err := stream.Peek(frame[:1]); err != nil {
		return 0, 0, err
	}
	if frame[0] < 1 || frame[0] > 10 {
		return 0, 0, protocol.ErrInvalidSelectors
	}
	frameLen := 1 + int(frame[0])
	if _, err := stream.Peek(frame[:frameLen]); err != nil {
		return 0, 0, err
	}
	selector, err := protocol.ReadSelector(bytes.NewReader(frame[:frameLen]))
	return selector, frameLen, err
}

func classificationCode(err error) protocol.Code {
	if errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return protocol.Timeout
	}
	if timeout, ok := errors.AsType[net.Error](err); ok && timeout.Timeout() {
		return protocol.Timeout
	}
	return protocol.BadSelector
}

func peerReset(err error) bool {
	reset, ok := errors.AsType[*quic.StreamError](err)
	return ok && reset.Remote
}

func quicCode(code protocol.Code) quic.StreamErrorCode { return quic.StreamErrorCode(code.Wire()) }

func resetBi(stream *quic.Stream, code protocol.Code) {
	stream.CancelRead(quicCode(code))
	stream.CancelWrite(quicCode(code))
}

// drainUni classifies unidirectional streams independently of bidirectional streams.
func (c *sharedConn) drainUni() {
	ctx := c.conn.Context()
	for {
		select {
		case <-c.uniSem:
		case <-ctx.Done():
			return
		}
		stream, err := c.conn.AcceptUniStream(ctx)
		if err != nil {
			c.uniSem <- struct{}{}
			if ctx.Err() == nil {
				slog.Warn("failed to accept stream", "err", err)
			}
			return
		}
		c.wg.Go(func() {
			defer func() { c.uniSem <- struct{}{} }()
			selector, frameLen, err := classifyUni(stream)
			if err == nil && selector == protocol.ControlSelector {
				// Control bypasses delivery queues and is owned by the view.
				var frame [11]byte
				_, err = io.ReadFull(stream, frame[:frameLen])
			}
			if err != nil {
				if ctx.Err() == nil && !peerReset(err) {
					stream.CancelRead(quicCode(classificationCode(err)))
				}
				return
			}
			if c.ethp2pCtx.Err() != nil {
				stream.CancelRead(quicCode(protocol.Closing))
				return
			}
			if selector == protocol.ControlSelector {
				if !c.controlClaimed.CompareAndSwap(false, true) {
					stream.CancelRead(quicCode(protocol.ControlViolation))
					c.controlViolation()
					return
				}
				c.control.mu.Lock()
				if c.ethp2pCtx.Err() != nil {
					code := c.control.peerCancel
					c.control.mu.Unlock()
					stream.CancelRead(quicCode(code))
					return
				}
				c.control.peer = stream
				// Only the first control stream enters this one-slot channel.
				c.controlIn <- stream
				c.control.mu.Unlock()
				return
			}
			if !c.ethp2pUni.push(selectedUni{stream, selector, frameLen}) {
				stream.CancelRead(quicCode(protocol.Closing))
			}
		})
	}
}
