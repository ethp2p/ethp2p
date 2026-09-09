package transport

import (
	"context"
	"net"

	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/quic-go/quic-go"
)

var _ quicreuse.QUICConn = (*connLib)(nil)

// connLib is the libp2p view of a split QUIC connection. It receives only
// streams classified as libp2p while sharing outbound operations with the
// ethp2p view.
type connLib sharedConn

func (c *connLib) AcceptStream(ctx context.Context) (*quic.Stream, error) {
	select {
	case stream := <-c.libp2pBi:
		return stream, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.conn.Context().Done():
		return nil, c.conn.Context().Err()
	}
}

func (c *connLib) OpenStreamSync(ctx context.Context) (*quic.Stream, error) {
	return c.conn.OpenStreamSync(ctx)
}

func (c *connLib) CloseWithError(code quic.ApplicationErrorCode, reason string) error {
	// Mark only; the raw connection closes once the ethp2p view closes too.
	(*sharedConn)(c).closeSide(sideLibp2p, code, reason)
	return nil
}

func (c *connLib) ConnectionState() quic.ConnectionState {
	state := c.conn.ConnectionState()
	state.TLS.NegotiatedProtocol = AlpnLibp2p
	return state
}

func (c *connLib) Context() context.Context { return c.conn.Context() }
func (c *connLib) LocalAddr() net.Addr      { return c.conn.LocalAddr() }
func (c *connLib) RemoteAddr() net.Addr     { return c.conn.RemoteAddr() }
