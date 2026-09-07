package transport

import (
	"context"
	"net"

	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
)

var _ quicreuse.QUICListener = (*listenerLib)(nil)

// listenerLib is the libp2p listener on the shared QUIC transport. There is at
// most one; listening stops only with the whole transport.
type listenerLib struct {
	transport *TransportShared
	queue     chan quicreuse.QUICConn
}

// Accept waits for a libp2p connection, including those dialed by TransportEth.
func (l *listenerLib) Accept(ctx context.Context) (quicreuse.QUICConn, error) {
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
func (l *listenerLib) Addr() net.Addr { return l.transport.raw.Conn.LocalAddr() }

// Close is a no-op. Use [TransportShared.Close] to stop listening.
func (l *listenerLib) Close() error { return nil }
