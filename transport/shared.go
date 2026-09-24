package transport

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/multiformats/go-varint"
	"github.com/quic-go/quic-go"
)

const (
	// maxStreamsPendingDelivery is the limit of streams pending to be delivered, per conn x per outbox.
	// Unqueueable streams are aborted.
	// This handles slow application consumers.
	maxStreamsPendingDelivery = 8
	// maxStreamsPendingClassify limits concurrent stream classification per conn.
	// This handles sustained packet loss and malicious peers.
	maxStreamsPendingClassify = 4
	// maxConnsPendingDelivery is the limit of conns pending delivery to the downstream stack, per stack.
	maxConnsPendingDelivery = 16

	classifyTimeout = 5 * time.Second
)

type side uint32

// Side bitmasks for SharedTransport.interest and sharedConn.closed.
const (
	sideLibp2p side = 1 << iota
	sideEthp2p
)

// SharedTransport owns a QUIC endpoint shared by libp2p and ethp2p.
// The application owns its shutdown; ethp2p.Stack only borrows its connections.
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
	interest atomic.Uint32
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

	// wg tracks every goroutine owned by the transport: the accept loop
	// plus, per sharedConn, the drainers and per-stream classifiers.
	// shutdown waits on it after closing the endpoint.
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
	t := &SharedTransport{
		raw:        &quic.Transport{Conn: packetConn},
		handshaker: handshaker,
		profile:    profile,
		ctx:        ctx,
		cancel:     cancel,
		libQ:       make(chan quicreuse.QUICConn, maxConnsPendingDelivery),
		ethQ:       make(chan Conn, maxConnsPendingDelivery),
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

// Ethp2p returns the ethp2p transport and registers interest in ethp2p views.
// Until the first call, the ethp2p view of every new connection is released
// immediately, so request it before connections arrive.
func (t *SharedTransport) Ethp2p() *Ethp2pTransport {
	t.interest.Or(uint32(sideEthp2p))
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

// offerEthp2p hands the ethp2p view of a new connection to ethQ. It releases
// the view instead when ethp2p was never requested or ethQ is full; the
// libp2p view of the same connection is offered independently.
func (t *SharedTransport) offerEthp2p(c *ethp2pConn) {
	if t.interest.Load()&uint32(sideEthp2p) == 0 {
		c.closeSide(sideEthp2p, appNoError, "unclaimed")
		return
	}
	select {
	case t.ethQ <- c:
	default:
		c.closeSide(sideEthp2p, appFailure, "delivery queue full")
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
	t.cancel()
	err := t.raw.Close()
	t.wg.Wait()
	return err
}

//
// CONNECTION
//////////////////////

type sharedConn struct {
	conn *quic.Conn
	sem  chan struct{}
	// Each view's context ends when that view closes or the connection does;
	// pending operations on the view wait on it.
	libp2pCtx    context.Context
	cancelLibp2p context.CancelCauseFunc
	ethp2pCtx    context.Context
	cancelEthp2p context.CancelCauseFunc
	// wg owns every goroutine started for this connection, so transport
	// shutdown waits for the dispatchers and their classifiers.
	wg *sync.WaitGroup

	// delivery queues for classified incoming streams
	libp2pBi  chan *quic.Stream
	ethp2pBi  chan *quic.Stream
	ethp2pUni chan *quic.ReceiveStream

	// View-closed bitfield over sideLibp2p|sideEthp2p. The raw connection
	// closes once both views are closed, so closing one view never kills
	// the other. Accessed atomically.
	closed atomic.Uint32

	// remote is the identity authenticated for the remote endpoint.
	remote PeerID
}

// newSharedConn splits an ethp2p_0 connection into the shared state backing its
// two views and starts the stream dispatchers on wg. Callers build the views
// with ethp2p() and libp2p().
//
// remote is the peer identity the handshake authenticated. An ethp2p_0
// connection only reaches this point after the verify callback accepted the
// peer's certificate, so the identity is always established by then.
func newSharedConn(raw *quic.Conn, wg *sync.WaitGroup, remote PeerID) *sharedConn {
	libp2pCtx, cancelLibp2p := context.WithCancelCause(raw.Context())
	ethp2pCtx, cancelEthp2p := context.WithCancelCause(raw.Context())
	sc := &sharedConn{
		conn:         raw,
		libp2pCtx:    libp2pCtx,
		cancelLibp2p: cancelLibp2p,
		ethp2pCtx:    ethp2pCtx,
		cancelEthp2p: cancelEthp2p,
		wg:           wg,
		remote:       remote,
		sem:          make(chan struct{}, maxStreamsPendingClassify),
		libp2pBi:     make(chan *quic.Stream, maxStreamsPendingDelivery),
		ethp2pBi:     make(chan *quic.Stream, maxStreamsPendingDelivery),
		ethp2pUni:    make(chan *quic.ReceiveStream, maxStreamsPendingDelivery),
	}
	for range maxStreamsPendingClassify {
		sc.sem <- struct{}{}
	}
	wg.Go(sc.drainBidi)
	wg.Go(sc.drainUni)
	return sc
}

// remotePeerID returns the identity authenticated for the remote endpoint.
func (c *sharedConn) remotePeerID() PeerID { return c.remote }

// closeSide releases one view and closes the connection when both are released.
func (c *sharedConn) closeSide(want side, code quic.ApplicationErrorCode, reason string) {
	switch want {
	case sideLibp2p:
		c.cancelLibp2p(errViewClosed)
	case sideEthp2p:
		c.cancelEthp2p(errViewClosed)
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
			stream.CancelRead(streamReset)
			stream.CancelWrite(streamReset)
		default:
			return
		}
	}
}

func (c *sharedConn) drainEthp2pQueues() {
	for {
		select {
		case stream := <-c.ethp2pBi:
			stream.CancelRead(streamReset)
			stream.CancelWrite(streamReset)
		case stream := <-c.ethp2pUni:
			stream.CancelRead(streamReset)
		default:
			return
		}
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
		case <-c.sem:
		case <-ctx.Done():
			return
		}

		stream, err := c.conn.AcceptStream(ctx)
		if err != nil {
			c.sem <- struct{}{}
			if ctx.Err() == nil {
				slog.Warn("failed to accept stream", "err", err)
			}
			return
		}

		c.wg.Go(func() {
			defer func() { c.sem <- struct{}{} }()

			isEthp2p, err := classify(ctx, stream)
			if err != nil {
				slog.Debug("failed to classify stream", "err", err)
				stream.CancelRead(streamReset)
				stream.CancelWrite(streamReset)
				return
			}

			queue := c.libp2pBi
			view := c.libp2pCtx
			drain := c.drainLibp2pQueue
			if isEthp2p {
				queue = c.ethp2pBi
				view = c.ethp2pCtx
				drain = c.drainEthp2pQueues
			}
			if view.Err() != nil {
				stream.CancelRead(streamReset)
				stream.CancelWrite(streamReset)
				return
			}

			select {
			case queue <- stream:
				if view.Err() != nil {
					drain()
				}
			case <-view.Done():
				stream.CancelRead(streamReset)
				stream.CancelWrite(streamReset)
			case <-ctx.Done():
				// connection was closed, no stream-level cleanup needed
			default:
				stream.CancelRead(streamReset)
				stream.CancelWrite(streamReset)
			}
		})
	}
}

type peekableStream interface {
	io.Reader
	Peek([]byte) (int, error)
	CancelRead(quic.StreamErrorCode)
	SetReadDeadline(time.Time) error
}

// classify identifies the destination from the first frame's length prefix and
// first payload byte without consuming bytes. '/' routes to libp2p; any other
// byte routes to ethp2p. It does not validate the full frame.
func classify[S peekableStream](ctx context.Context, stream S) (ethp2p bool, err error) {
	stop := context.AfterFunc(ctx, func() { stream.CancelRead(streamReset) })
	defer stop()

	if err := stream.SetReadDeadline(time.Now().Add(classifyTimeout)); err != nil {
		return false, err
	}
	defer func() {
		if clearErr := stream.SetReadDeadline(time.Time{}); err == nil {
			err = clearErr
		}
	}()

	peekExact := func(dst []byte) error {
		n, err := stream.Peek(dst)
		if n == len(dst) {
			return nil
		}
		if err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}

	var buf [varint.MaxLenUvarint63 + 1]byte
	for prefixLen := 1; prefixLen <= varint.MaxLenUvarint63; prefixLen++ {
		if err := peekExact(buf[:prefixLen]); err != nil {
			return false, err
		}
		if buf[prefixLen-1]&byte(0x80) != 0 {
			continue
		}

		payloadLen, _, err := varint.FromUvarint(buf[:prefixLen])
		if err != nil {
			return false, err
		}
		if payloadLen == 0 {
			return false, errEmptyFirstFrame
		}
		if err := peekExact(buf[:prefixLen+1]); err != nil {
			return false, err
		}
		return buf[prefixLen] != '/', nil
	}
	return false, errInvalidFrame
}

// drainUni routes all incoming unidirectional streams to ethp2p; libp2p uses none.
// A full queue holds at most one additional accepted stream here and stops
// acceptance, leaving QUIC to enforce stream and flow-control limits. This
// wait does not occupy bidirectional classifier slots.
func (c *sharedConn) drainUni() {
	ctx := c.conn.Context()

	for {
		stream, err := c.conn.AcceptUniStream(ctx)
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("failed to accept stream", "err", err)
			}
			return
		}
		if c.ethp2pCtx.Err() != nil {
			stream.CancelRead(streamReset)
			continue
		}
		select {
		case c.ethp2pUni <- stream:
			if c.ethp2pCtx.Err() != nil {
				c.drainEthp2pQueues()
			}
		case <-c.ethp2pCtx.Done():
			stream.CancelRead(streamReset)
		case <-ctx.Done():
			// connection was closed, no stream-level cleanup needed
			return
		}
	}
}
