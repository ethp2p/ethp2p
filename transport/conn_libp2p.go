package transport

import (
	"context"
	"net"

	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/quic-go/quic-go"
)

var (
	_ quicreuse.QUICConn = (*connLib)(nil)
	_ quicreuse.QUICConn = (*exclusiveConnLib)(nil)
)

// connLib is the libp2p view of a split QUIC connection. It receives only
// streams classified as libp2p while sharing outbound operations with the
// ethp2p view.
type connLib sharedConn

// exclusiveConnLib is a libp2p-only QUIC connection.
type exclusiveConnLib struct{ *quic.Conn }

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

// As refuses access to the raw connection because sharedConn owns inbound
// stream acceptance.
func (c *connLib) As(any) bool { return false }

func (c *exclusiveConnLib) As(target any) bool {
	if raw, ok := target.(**quic.Conn); ok {
		*raw = c.Conn
		return true
	}
	return false
}
