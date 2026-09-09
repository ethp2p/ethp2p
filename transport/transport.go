package transport

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/quic-go/quic-go"
)

var _ quicreuse.QUICTransport = (*TransportLib)(nil)

// quicConfig is the connection-wide policy, inherited from libp2p.
var quicConfig = &quic.Config{
	MaxIncomingStreams:               256,
	MaxIncomingUniStreams:            5,
	MaxStreamReceiveWindow:           10 << 20,
	MaxConnectionReceiveWindow:       15 << 20,
	KeepAlivePeriod:                  15 * time.Second,
	Versions:                         []quic.Version{quic.Version1},
	EnableDatagrams:                  true,
	EnableStreamResetPartialDelivery: true,
}

type side uint32

// Side bitmasks for TransportShared.attached and sharedConn.closed.
const (
	sideLibp2p side = 1 << iota
	sideEthp2p
)

// connQueueLen bounds inbound connections buffered before Accept.
// Delivery is expected to be immediate. This is just a small safety buffer.
const connQueueLen = 16

// TransportShared owns a QUIC endpoint shared by libp2p and ethp2p.
// The application owns its shutdown; ethp2p.Stack only borrows its connections.
// A closed transport cannot be restarted.
type TransportShared struct {
	raw        *quic.Transport
	handshaker *handshaker

	// ctx is the transport lifetime.
	ctx    context.Context
	cancel context.CancelFunc

	// These queues exist for the transport's lifetime, allowing either side
	// to receive connections before its listener attaches.
	libQ chan quicreuse.QUICConn
	ethQ chan Conn

	// attached records the sides that have attached.
	attached atomic.Uint32

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

// NewShared creates a shared QUIC endpoint using key for authentication.
// Listening starts on the first [TransportLib.Listen] or [TransportEth.Accept].
// The caller remains responsible for closing packetConn.
func NewShared(key *PrivKey, packetConn net.PacketConn) (*TransportShared, error) {
	if packetConn == nil {
		return nil, errNilPacketConn
	}
	handshaker, err := newHandshaker(key)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	t := &TransportShared{
		raw:        &quic.Transport{Conn: packetConn},
		handshaker: handshaker,
		ctx:        ctx,
		cancel:     cancel,
		libQ:       make(chan quicreuse.QUICConn, connQueueLen),
		ethQ:       make(chan Conn, connQueueLen),
	}
	// ConnContext attaches one handshake instance to every incoming
	// connection's context; the accept path reads the identity it memoizes.
	// It is not used for dialed connections.
	t.raw.ConnContext = func(ctx context.Context, _ *quic.ClientInfo) (context.Context, error) {
		return context.WithValue(ctx, handshakeContextKey{}, &handshake{}), nil
	}
	return t, nil
}

// Libp2p returns the view to lend through quicreuse.ConnManager.LendTransport.
func (t *TransportShared) Libp2p() *TransportLib { return &TransportLib{t} }

// Ethp2p returns the ethp2p view of the shared transport.
func (t *TransportShared) Ethp2p() *TransportEth { return &TransportEth{t} }

// Close stops listening, closes all connections, and waits for transport
// goroutines to exit. It does not close the supplied packet connection.
// Repeated calls are safe. The application owns this call on shutdown.
// Closing [TransportEth] has the same effect; [TransportLib.Close] is a no-op.
func (t *TransportShared) Close() error { return t.shutdown() }

// attach starts listening and reports whether side was newly claimed.
// A failed start leaves the side claimed.
func (t *TransportShared) attach(side side) (claimed bool, err error) {
	if t.ctx.Err() != nil {
		return false, errClosed
	}
	claimed = t.attached.Or(uint32(side))&uint32(side) == 0
	return claimed, t.ensureListener()
}

// ensureListener starts the raw listener and its accept loop exactly once.
// A failed start is cached and returned on every later call. lnOnce
// synchronizes reads of lnErr across concurrent calls.
func (t *TransportShared) ensureListener() error {
	if t.ln.Load() != nil {
		return nil
	}
	t.lnOnce.Do(func() {
		ln, err := t.raw.Listen(t.handshaker.serverConfig(), quicConfig.Clone())
		if err != nil {
			t.lnErr = err
			return
		}
		t.ln.Store(ln)
		t.wg.Go(func() { t.acceptLoop(ln) })
	})
	return t.lnErr
}

func (t *TransportShared) acceptLoop(ln *quic.Listener) {
	for {
		raw, err := ln.Accept(t.ctx)
		if err != nil {
			return
		}

		hs, ok := raw.Context().Value(handshakeContextKey{}).(*handshake)
		if !ok {
			slog.Warn("unexpectedly missing handshake context")
		}

		state := raw.ConnectionState()
		switch state.TLS.NegotiatedProtocol {
		case AlpnLibp2p:
			// peer is legacy: the libp2p view is the whole connection
			select {
			case t.libQ <- raw:
			default:
				_ = raw.CloseWithError(appFailure, errClosed.Error())
			}
		case AlpnEthp2p:
			sc := t.split(raw)
			ethp2p := sc.ethp2p()
			ethp2p.dir = ConnDirIn
			ethp2p.auth = hs.auth
			libp2p := sc.libp2p()
			// Dispose the ethp2p view first so a double drop closes the raw
			// connection with the libp2p failure code rather than a clean
			// one.
			select {
			case t.ethQ <- ethp2p:
				select {
				case t.libQ <- libp2p:
				default:
					_ = libp2p.CloseWithError(appFailure, errClosed.Error())
				}
			default:
				_ = ethp2p.Close()
				_ = libp2p.CloseWithError(appFailure, errClosed.Error())
			}
		default:
			_ = raw.CloseWithError(appFailure, "unsupported negotiated protocol")
		}
	}
}

// shutdown cancels transport waits and closes the endpoint before joining
// its goroutines. Closing the endpoint unblocks stream acceptance and
// classification on every connection, including views still queued for delivery.
func (t *TransportShared) shutdown() error {
	t.cancel()
	err := t.raw.Close()
	t.wg.Wait()
	return err
}
