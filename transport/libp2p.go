package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"

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
type Libp2pTransport struct{ *SharedTransport }

// Listen returns the libp2p listener, starting the shared listener if needed.
// It ignores the supplied configurations. Subsequent calls fail, even after
// the returned listener is closed.
func (t *Libp2pTransport) Listen(_ *tls.Config, _ *quic.Config) (quicreuse.QUICListener, error) {
	claimed, err := t.attach(sideLibp2p)
	if err != nil {
		return nil, err
	}
	if !claimed {
		return nil, errAlreadyListening
	}
	return &libp2pListener{transport: t.SharedTransport, queue: t.libQ}, nil
}

// Dial opens a libp2p-only connection using tlsConf. It rejects any other ALPN
// and ignores the supplied QUIC configuration.
func (t *Libp2pTransport) Dial(ctx context.Context, addr net.Addr, tlsConf *tls.Config, _ *quic.Config) (quicreuse.QUICConn, error) {
	// this dial was requested by libp2p
	//
	// libp2p handles peer ID authentication inside the VerifyPeerCertificate
	// closure, so we refrain from authenticating in our end and delegate
	// that responsibility to libp2p.

	hs := t.handshaker.newHandshake("")
	theirs := tlsConf.VerifyPeerCertificate
	tlsConf.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		if err := theirs(rawCerts, verifiedChains); err != nil {
			return err
		}
		return hs.config.VerifyPeerCertificate(rawCerts, verifiedChains)
	}

	raw, err := t.raw.Dial(ctx, addr, tlsConf, quicConfig.Clone())
	if err != nil {
		return nil, err
	}

	alpn := raw.ConnectionState().TLS.NegotiatedProtocol
	if alpn == AlpnEthp2p {
		// this is a modern ethp2p peer.
		// in addition to returning the connection to libp2p,
		// we also feed it to ethp2p.
		// If its queue is full, do no block, just return the libp2p end.
		// TODO review this behaviour
		sc := newSharedConn(raw)

		select {
		case t.ethQ <- &ethp2pConn{sharedConn: sc}:
		default:
		}
		return sc.libp2p(), nil
	}

	return raw, nil
}

// ReadNonQUICPacket reads the next non-QUIC packet from the shared endpoint.
func (t *Libp2pTransport) ReadNonQUICPacket(ctx context.Context, payload []byte) (int, net.Addr, error) {
	return t.raw.ReadNonQUICPacket(ctx, payload)
}

// WriteTo sends a packet to addr through the shared endpoint.
func (t *Libp2pTransport) WriteTo(payload []byte, addr net.Addr) (int, error) {
	return t.raw.WriteTo(payload, addr)
}

// Close is a no-op returning nil. The application owns endpoint shutdown;
// libp2p must not close the shared endpoint while ethp2p still uses it.
// Use [SharedTransport.Close] to stop the endpoint.
func (*Libp2pTransport) Close() error { return nil }

//
// LISTENER
//////////////////////

// libp2pListener is the libp2p listener on the shared QUIC transport. There is at
// most one; listening stops only with the whole transport.
type libp2pListener struct {
	transport *SharedTransport
	queue     chan quicreuse.QUICConn
}

// Accept waits for a libp2p connection, including those dialed by TransportEth.
func (l *libp2pListener) Accept(ctx context.Context) (quicreuse.QUICConn, error) {
	t := l.transport
	select {
	case c := <-l.queue:
		if t.ctx.Err() != nil {
			return nil, errClosed
		}
		return c, nil
	case <-t.ctx.Done():
		return nil, errClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Addr returns the local address of the shared endpoint.
func (l *libp2pListener) Addr() net.Addr { return l.transport.raw.Conn.LocalAddr() }

// Close is a no-op. Use [SharedTransport.Close] to stop listening.
func (l *libp2pListener) Close() error {
	return fmt.Errorf("listener close rejected due to shared transport: close TransportShared instead")
}

//
// CONNECTION
//////////////////////

// libp2pConn is the libp2p view of a split QUIC connection. It receives only
// streams classified as libp2p while sharing outbound operations with the
// ethp2p view.
type libp2pConn sharedConn

func (c *libp2pConn) AcceptStream(ctx context.Context) (*quic.Stream, error) {
	select {
	case stream := <-c.libp2pBi:
		return stream, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.conn.Context().Done():
		return nil, c.conn.Context().Err()
	}
}

func (c *libp2pConn) OpenStreamSync(ctx context.Context) (*quic.Stream, error) {
	return c.conn.OpenStreamSync(ctx)
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

func (c *libp2pConn) Context() context.Context { return c.conn.Context() }
func (c *libp2pConn) LocalAddr() net.Addr      { return c.conn.LocalAddr() }
func (c *libp2pConn) RemoteAddr() net.Addr     { return c.conn.RemoteAddr() }
