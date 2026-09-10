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
	// deliveryQueueLen bounds bidirectional streams classified but not yet handed over.
	// Delivery is expected immediate, so this is just a safety buffer.
	deliveryQueueLen = 4
	// uniDeliveryQueueLen absorbs short bursts while the application consumes
	// streams. Sixteen is a burst allowance, not a limit on accepted traffic:
	// a full queue pauses acceptance until delivery or connection shutdown.
	uniDeliveryQueueLen = 16
	// maxPendingStreamsPerConn limits concurrent stream classifications.
	// Fragmentation, packet loss, or a stalled peer can delay classification;
	// the bound limits resources held by streams awaiting their first bytes.
	maxPendingStreamsPerConn = 4
	// connQueueLen bounds inbound connections buffered before Accept.
	// Delivery is expected to be immediate. This is just a small safety buffer.
	connQueueLen = 16

	classifyTimeout       = 5 * time.Second
	varintContinuationBit = byte(0x80)
)

type side uint32

// Side bitmasks for TransportShared.attached and sharedConn.closed.
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

	// ctx is the transport lifetime.
	ctx    context.Context
	cancel context.CancelFunc

	// These queues exist for the transport's lifetime, allowing either side
	// to receive connections before its listener attaches.
	libQ chan quicreuse.QUICConn
	ethQ chan Conn

	// attached records the sides that have attached as a bitmap
	// of side values.
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
// Listening starts on the first [Libp2pTransport.Listen] or [Ethp2pTransport.Accept].
// The caller remains responsible for closing packetConn.
func NewShared(key *PrivKey, packetConn net.PacketConn) (*SharedTransport, error) {
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
		ctx:        ctx,
		cancel:     cancel,
		libQ:       make(chan quicreuse.QUICConn, connQueueLen),
		ethQ:       make(chan Conn, connQueueLen),
	}

	// ConnContext attaches one handshake instance to every incoming
	// connection's context. We memoize the identity of the peer during
	// the accept handshake. Not used for outbound connections.
	t.raw.ConnContext = func(ctx context.Context, _ *quic.ClientInfo) (context.Context, error) {
		return context.WithValue(ctx, handshakeContextKey{}, &handshake{}), nil
	}
	return t, nil
}

// Libp2p returns the view to lend through quicreuse.ConnManager.LendTransport.
func (t *SharedTransport) Libp2p() quicreuse.QUICTransport { return &Libp2pTransport{t} }

// Ethp2p returns the ethp2p view of the shared transport.
func (t *SharedTransport) Ethp2p() *Ethp2pTransport { return &Ethp2pTransport{t} }

// Close stops listening, closes all connections, and waits for transport
// goroutines to exit. It does not close the supplied packet connection.
// Repeated calls are safe. The application owns this call on shutdown.
// Closing [Ethp2pTransport] has the same effect; [Libp2pTransport.Close] is a no-op.
func (t *SharedTransport) Close() error { return t.shutdown() }

// attach starts listening and reports whether side was newly claimed.
// A failed start leaves the side claimed.
func (t *SharedTransport) attach(side side) (claimed bool, err error) {
	if t.ctx.Err() != nil {
		return false, errClosed
	}
	claimed = t.attached.Or(uint32(side))&uint32(side) == 0
	return claimed, t.ensureListener()
}

// ensureListener starts the raw listener and its accept loop exactly once.
// A failed start is cached and returned on every later call. lnOnce
// synchronizes reads of lnErr across concurrent calls.
func (t *SharedTransport) ensureListener() error {
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

func (t *SharedTransport) acceptLoop(ln *quic.Listener) {
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
			sc := newSharedConn(raw)
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

	// outboxes for classified incoming streams
	libp2pBi  chan *quic.Stream
	ethp2pBi  chan *quic.Stream
	ethp2pUni chan *quic.ReceiveStream

	// View-closed bitfield over sideLibp2p|sideEthp2p. The raw connection
	// closes once both views are closed, so closing one view never kills
	// the other. Accessed atomically.
	closed atomic.Uint32
}

// newSharedConn splits an ethp2p_0 connection into the shared state backing
// its two views. Callers build the views with ethp2p() and libp2p() and stamp
// the view metadata themselves. The caller owns the goroutine lifecycle:
// everything joins t.wg, so shutdown waits for the drainers and their
// classifiers.
func newSharedConn(raw *quic.Conn) *sharedConn {
	sc := &sharedConn{
		conn:      raw,
		sem:       make(chan struct{}, maxPendingStreamsPerConn),
		libp2pBi:  make(chan *quic.Stream, deliveryQueueLen),
		ethp2pBi:  make(chan *quic.Stream, deliveryQueueLen),
		ethp2pUni: make(chan *quic.ReceiveStream, uniDeliveryQueueLen),
	}
	for range maxPendingStreamsPerConn {
		sc.sem <- struct{}{}
	}
	return sc
}

// closeSide releases one view and closes the connection when both are released.
func (s *sharedConn) closeSide(want side, code quic.ApplicationErrorCode, reason string) {
	// Or returns the pre-OR value. RMW atomicity means no other close
	// can land between its read and write, so old|want is the post-OR value.
	if s.closed.Or(uint32(want))|uint32(want) == uint32(sideLibp2p|sideEthp2p) {
		_ = s.conn.CloseWithError(code, reason)
	}
}

// ethp2p returns the ethp2p view. The caller must set its authentication and direction.
func (sc *sharedConn) ethp2p() *ethp2pConn {
	return &ethp2pConn{sharedConn: sc}
}

// libp2p returns the libp2p view of the shared connection.
func (r *sharedConn) libp2p() quicreuse.QUICConn {
	return (*libp2pConn)(r)
}

// drainBidi accepts bidirectional streams and classifies them.
// The semaphore limits the number of concurrent classifications.
func (r *sharedConn) drainBidi(wg *sync.WaitGroup) {
	ctx := r.conn.Context()

	for {
		select {
		case <-r.sem:
		case <-ctx.Done():
			return
		}

		stream, err := r.conn.AcceptStream(ctx)
		if err != nil {
			r.sem <- struct{}{}
			if ctx.Err() == nil {
				slog.Warn("failed to accept stream", "err", err)
			}
			return
		}

		wg.Go(func() {
			defer func() { r.sem <- struct{}{} }()

			isEthp2p, err := classify(ctx, stream)
			if err != nil {
				slog.Warn("failed to classify stream", "err", err)
				stream.CancelRead(streamReset)
				stream.CancelWrite(streamReset)
				return
			}

			queue := r.libp2pBi
			if isEthp2p {
				queue = r.ethp2pBi
			}

			select {
			case queue <- stream:
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
		if buf[prefixLen-1]&varintContinuationBit != 0 {
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

func (r *sharedConn) classify[S peekableStream]() {

}

// drainUni routes all incoming unidirectional streams to ethp2p; libp2p uses none.
// A full queue holds at most one additional accepted stream here and stops
// acceptance, leaving QUIC to enforce stream and flow-control limits. This
// wait does not occupy bidirectional classifier slots.
func (r *sharedConn) drainUni() {
	ctx := r.conn.Context()

	for {
		stream, err := r.conn.AcceptUniStream(ctx)
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("failed to accept stream", "err", err)
			}
			return
		}
		select {
		case r.ethp2pUni <- stream:
		case <-ctx.Done():
			// connection was closed, no stream-level cleanup needed
			return
		}
	}
}
