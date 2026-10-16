package transport

import (
	"context"
	"crypto/hkdf"
	"crypto/sha256"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/quic-go/quic-go"
)

const (
	// maxStreamsPendingClassify is MAX_STREAMS_PENDING_CLASSIFY: the
	// bidirectional streams classified at once on one connection.
	maxStreamsPendingClassify = 4
	// maxConnsPendingDelivery bounds each side's queue of views awaiting
	// accept, against an application that stops accepting.
	maxConnsPendingDelivery = 16
)

// defaultClassifyTimeout bounds the wait for a bidirectional stream's first
// byte. It is the classify timeout every Config starts from; see
// defaultConfig.
const defaultClassifyTimeout = 5 * time.Second

type side uint32

const (
	sideLibp2p side = 1 << iota
	sideEthp2p
)

// Endpoint is a QUIC endpoint shared by libp2p and ethp2p.
// The application owns endpoint shutdown; ethp2p.Stack owns only its views.
// A closed transport cannot be restarted.
type Endpoint struct {
	raw        *quic.Transport
	handshaker *handshaker
	config     Config

	ctx    context.Context
	cancel context.CancelFunc

	libQ chan quicreuse.QUICConn
	ethQ chan *Conn

	// libMu serializes libp2p offers with listener detachment during the
	// shutdown sequence, so no view can be queued after Close drains libQ.
	// libListener is the listener handed out by the first
	// Libp2pTransport.Listen; libDone closes on detachment.
	libMu       sync.Mutex
	libListener *libp2pListener
	libDone     chan struct{}

	// wired records, as side bits, that Libp2p and Ethp2p have each been
	// handed out. Start requires both. started records that Start ran.
	wired   atomic.Uint32
	started bool
	ln      *quic.Listener

	// stopped is set by shutdown before ctx is cancelled. Under the shutdown
	// choreography no new Dial or Listen begins once shutdown does - the
	// stack and the libp2p host are already stopped - so entry points check
	// stopped; Ethp2pTransport.Dial additionally reserves in wg before
	// checking stopped, because it starts goroutines on the connection it
	// creates.
	stopped atomic.Bool

	classifyTimeout time.Duration
	// wg tracks the accept loop and all dispatcher and classifier
	// goroutines. Shutdown joins them after raw.Close.
	wg sync.WaitGroup
}

// NewEndpoint creates a shared QUIC endpoint using key for authentication and
// config for connection policy. The endpoint serves both the libp2p and
// ethp2p views of every ethp2p_0 connection; a legacy libp2p connection
// is libp2p-only.
// The caller remains responsible for closing packetConn.
func NewEndpoint(key *identity.PrivKey, packetConn net.PacketConn, config Config) (*Endpoint, error) {
	if key == nil {
		return nil, errNilKey
	}
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
	t := &Endpoint{
		raw:             &quic.Transport{Conn: packetConn, StatelessResetKey: &resetKey},
		handshaker:      handshaker,
		config:          config,
		ctx:             ctx,
		classifyTimeout: config.classifyTimeout,
		cancel:          cancel,
		libQ:            make(chan quicreuse.QUICConn, maxConnsPendingDelivery),
		ethQ:            make(chan *Conn, maxConnsPendingDelivery),
		libDone:         make(chan struct{}),
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
// quicreuse.ConnManager.LendTransport.
func (t *Endpoint) Libp2p() quicreuse.QUICTransport {
	t.wired.Or(uint32(sideLibp2p))
	return &Libp2pTransport{t}
}

// Ethp2p returns the ethp2p transport.
func (t *Endpoint) Ethp2p() *Ethp2pTransport {
	t.wired.Or(uint32(sideEthp2p))
	return &Ethp2pTransport{t}
}

// PeerID returns the local identity.
func (t *Endpoint) PeerID() identity.PeerID { return t.handshaker.peerID }

// Addr returns the local packet connection address.
func (t *Endpoint) Addr() net.Addr { return t.raw.Conn.LocalAddr() }

// Close stops listening, closes all connections, and waits for transport
// goroutines to exit. It does not close the supplied packet connection.
// Repeated calls are safe. The application owns this call on shutdown.
// [Libp2pTransport.Close] is a no-op.
func (t *Endpoint) Close() error { return t.shutdown() }

// Start begins listening, once both views have been wired by Libp2p and
// Ethp2p. It is the only place the listener starts and the only place a
// startup error is returned; per the client choreography the application
// calls it once during startup and exits on error.
func (t *Endpoint) Start() error {
	if side(t.wired.Load()) != sideLibp2p|sideEthp2p {
		return errNotWired
	}
	if t.started {
		return errAlreadyStarted
	}
	t.started = true
	if t.stopped.Load() {
		return ErrClosed
	}
	ln, err := t.raw.Listen(t.handshaker.serverConfig(), t.config.quicConfig())
	if err != nil {
		return err
	}
	t.ln = ln
	t.wg.Go(func() { t.acceptLoop(ln) })
	return nil
}

// offerEthp2p queues a new ethp2p view in ethQ. It releases the view
// instead when ethQ is full (Unspecified). libp2p delivery is independent.
func (t *Endpoint) offerEthp2p(c *Conn) {
	select {
	case t.ethQ <- c:
	default:
		c.CloseWithCode(wire.Unspecified)
	}
}

// offerLibp2p hands a libp2p view, or a legacy libp2p-only connection, to
// libQ. It releases the connection instead when libp2p has detached, or
// libQ is full.
func (t *Endpoint) offerLibp2p(c quicreuse.QUICConn) {
	t.libMu.Lock()
	queued, code, reason := false, appNoError, "unclaimed"
	if !t.libDetached() {
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
func (t *Endpoint) detachLibp2p() {
	t.libMu.Lock()
	if t.libDetached() {
		t.libMu.Unlock()
		return
	}
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
		_ = c.CloseWithError(quic.ApplicationErrorCode(wire.Closing.Wire()), "listener closed")
	}
}

// libDetached reports whether libp2p has detached: libDone, closed by
// detachLibp2p, is the state.
func (t *Endpoint) libDetached() bool {
	select {
	case <-t.libDone:
		return true
	default:
		return false
	}
}

func (t *Endpoint) acceptLoop(ln *quic.Listener) {
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
			conn := newConn(raw, &t.wg, acceptedPeerID(raw), t.ctx, t.classifyTimeout)
			// Offer libp2p first: when ethp2p delivery is detached, the
			// ethp2p release below is the last one, so the raw close carries
			// Closing and the dialer observes a ViewClosedError with that code.
			t.offerLibp2p(conn.libp2p())
			t.offerEthp2p(conn)
			conn.startDispatchers()
		default:
			_ = raw.CloseWithError(appFailure, "unsupported negotiated protocol")
		}
	}
}

// acceptedPeerID returns the peer identity authenticated during the accept
// handshake. An ethp2p_0 connection always has one, because the ALPN could only
// be negotiated after the verify callback accepted the peer's certificate.
func acceptedPeerID(raw *quic.Conn) identity.PeerID {
	return raw.Context().Value(remoteIdentityContextKey{}).(*remoteIdentitySlot).pubKey.PeerID()
}

// shutdown cancels transport waits and closes the endpoint before joining
// its goroutines. Closing the endpoint unblocks stream acceptance and
// classification on every connection, including views still queued for delivery.
func (t *Endpoint) shutdown() error {
	t.stopped.Store(true)
	t.cancel()
	err := t.raw.Close()
	t.wg.Wait()
	return err
}
