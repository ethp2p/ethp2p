package transport

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/ethp2p/ethp2p/protocol"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/quic-go/quic-go"
)

// TODO: try to get rid of the libp2p import here.

var (
	_ quicreuse.QUICTransport = (*Libp2pTransport)(nil)
	_ quicreuse.QUICListener  = (*libp2pListener)(nil)
	_ quicreuse.QUICConn      = (*libp2pConn)(nil)
)

//
// TRANSPORT
//////////////////////

// Libp2pTransport provides the libp2p view of a shared QUIC endpoint.
type Libp2pTransport struct{ shared *SharedTransport }

// Listen returns the libp2p listener, starting the shared listener if needed.
// It ignores the supplied configurations. Subsequent calls fail, even after
// the returned listener is closed.
func (t *Libp2pTransport) Listen(_ *tls.Config, _ *quic.Config) (quicreuse.QUICListener, error) {
	if err := t.shared.start(); err != nil {
		return nil, err
	}
	if !t.shared.libListening.CompareAndSwap(false, true) {
		return nil, errAlreadyListening
	}
	return &libp2pListener{transport: t.shared, queue: t.shared.libQ}, nil
}

// Dial opens a libp2p-only connection using tlsConf. It ignores the supplied
// QUIC configuration. libp2p authenticates the peer using tlsConf.
func (t *Libp2pTransport) Dial(ctx context.Context, addr net.Addr, tlsConf *tls.Config, _ *quic.Config) (quicreuse.QUICConn, error) {
	if t.shared.ctx.Err() != nil {
		return nil, ErrClosed
	}

	raw, err := t.shared.raw.Dial(ctx, addr, tlsConf, t.shared.profile.quicConfig())
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// ReadNonQUICPacket reads the next non-QUIC packet from the shared endpoint.
func (t *Libp2pTransport) ReadNonQUICPacket(ctx context.Context, payload []byte) (int, net.Addr, error) {
	return t.shared.raw.ReadNonQUICPacket(ctx, payload)
}

// WriteTo sends a packet to addr through the shared endpoint.
func (t *Libp2pTransport) WriteTo(payload []byte, addr net.Addr) (int, error) {
	return t.shared.raw.WriteTo(payload, addr)
}

// Close is a no-op returning nil. The application owns endpoint shutdown;
// libp2p must not close the shared endpoint while ethp2p still uses it.
// Use [SharedTransport.Close] to stop the endpoint.
func (*Libp2pTransport) Close() error { return nil }

//
// LISTENER
//////////////////////

// libp2pListener is the libp2p listener on the shared QUIC transport. There is at
// most one; closing it detaches libp2p without stopping the endpoint.
type libp2pListener struct {
	transport *SharedTransport
	queue     chan quicreuse.QUICConn
}

// Accept waits for a libp2p connection, including those dialed by Ethp2pTransport.
func (l *libp2pListener) Accept(ctx context.Context) (quicreuse.QUICConn, error) {
	t := l.transport
	select {
	case c := <-l.queue:
		// The endpoint may have closed, or Close may have detached libp2p,
		// while this view was queued.
		t.libMu.Lock()
		closed := t.libDetached || t.ctx.Err() != nil
		t.libMu.Unlock()
		if closed {
			_ = c.CloseWithError(quic.ApplicationErrorCode(protocol.Closing.Wire()), "listener closed")
			return nil, ErrClosed
		}
		return c, nil
	case <-t.libDone:
		return nil, ErrClosed
	case <-t.ctx.Done():
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Addr returns the local address of the shared endpoint.
func (l *libp2pListener) Addr() net.Addr { return l.transport.raw.Conn.LocalAddr() }

// Close detaches libp2p and releases its queued views without stopping ethp2p.
func (l *libp2pListener) Close() error {
	l.transport.detachLibp2p()
	return nil
}

//
// CONNECTION
//////////////////////

// libp2pConn is the libp2p view of a split QUIC connection. It receives only
// streams classified as libp2p while sharing outbound operations with the
// ethp2p view. Closing it stops pending and later accepts and opens, and
// resets streams routed to it; streams already handed out remain usable.
type libp2pConn sharedConn

func (c *libp2pConn) AcceptStream(ctx context.Context) (*quic.Stream, error) {
	if err := c.libp2pCtx.Err(); err != nil {
		return nil, context.Cause(c.libp2pCtx)
	}
	select {
	case stream := <-c.libp2pBi:
		if c.libp2pCtx.Err() != nil {
			resetBi(stream, protocol.Closing)
			return nil, context.Cause(c.libp2pCtx)
		}
		return stream, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.libp2pCtx.Done():
		return nil, context.Cause(c.libp2pCtx)
	}
}

func (c *libp2pConn) OpenStreamSync(ctx context.Context) (*quic.Stream, error) {
	if c.libp2pCtx.Err() != nil {
		return nil, context.Cause(c.libp2pCtx)
	}
	bound, cleanup := viewContext(ctx, c.libp2pCtx)
	defer cleanup()
	stream, err := c.conn.OpenStreamSync(bound)
	if err != nil && c.libp2pCtx.Err() != nil {
		return nil, context.Cause(c.libp2pCtx)
	}
	return stream, err
}

func (c *libp2pConn) CloseWithError(code quic.ApplicationErrorCode, reason string) error {
	// Mark only; the raw connection closes once the ethp2p view closes too.
	(*sharedConn)(c).closeSide(sideLibp2p, code, reason)
	return nil
}

func (c *libp2pConn) ConnectionState() quic.ConnectionState {
	state := c.conn.ConnectionState()
	state.TLS.NegotiatedProtocol = AlpnLibp2p
	return state
}

func (c *libp2pConn) Context() context.Context { return c.libp2pCtx }
func (c *libp2pConn) LocalAddr() net.Addr      { return c.conn.LocalAddr() }
func (c *libp2pConn) RemoteAddr() net.Addr     { return c.conn.RemoteAddr() }
