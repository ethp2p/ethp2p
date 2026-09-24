package transport

import (
	"context"
	"net"
)

var _ Conn = (*ethp2pConn)(nil)

//
// TRANSPORT
//////////////////////

// Ethp2pTransport provides ethp2p connections on a shared endpoint.
// The application closes the owning [SharedTransport] to shut down the endpoint.
type Ethp2pTransport struct{ shared *SharedTransport }

// Accept waits for the next inbound ethp2p connection, starting the listener
// if needed. Concurrent callers share the same connection queue.
func (t *Ethp2pTransport) Accept(ctx context.Context) (Conn, error) {
	if err := t.shared.start(); err != nil {
		return nil, err
	}
	select {
	case c := <-t.shared.ethQ:
		return c, nil
	case <-t.shared.ctx.Done():
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Dial connects to addr and authenticates the peer. A nonempty expect requires
// that peer identity. It returns [ErrDialLegacyPeer] if the peer selects libp2p.
func (t *Ethp2pTransport) Dial(ctx context.Context, addr net.Addr, expect PeerID) (Conn, error) {
	if t.shared.ctx.Err() != nil {
		return nil, ErrClosed
	}

	slot := &remoteIdentitySlot{}
	tlsConfig := t.shared.handshaker.connConfig(slot, expect)
	raw, err := t.shared.raw.Dial(ctx, addr, tlsConfig, t.shared.profile.quicConfig())
	if err != nil {
		return nil, err
	}

	alpn := raw.ConnectionState().TLS.NegotiatedProtocol
	if alpn == AlpnLibp2p {
		// Hand a legacy connection to libp2p when that side is interested.
		t.shared.offerLibp2p(raw)
		return nil, ErrDialLegacyPeer
	}

	// Create the shared connection and its dispatchers, then return the ethp2p
	// side and feed the libp2p side to libp2p.
	sc := newSharedConn(raw, &t.shared.wg, slot.key.PeerID())
	ethp2p := sc.ethp2p()
	t.shared.offerLibp2p(sc.libp2p())
	return ethp2p, nil
}

//
// CONNECTION
//////////////////////

// ethp2pConn is the ethp2p view of one routed QUIC connection. It receives
// classified streams from the ethp2p queues, shares the raw connection with the
// libp2p view, and implements the Conn interface.
type ethp2pConn struct {
	*sharedConn
}

// RemotePeerID returns the identity authenticated for the remote endpoint.
func (c *ethp2pConn) RemotePeerID() PeerID { return c.remotePeerID() }

func (c *ethp2pConn) OpenStream(ctx context.Context) (Stream, error) {
	if c.ethp2pCtx.Err() != nil {
		return nil, context.Cause(c.ethp2pCtx)
	}
	bound, cleanup := viewContext(ctx, c.ethp2pCtx)
	defer cleanup()
	s, err := c.conn.OpenStreamSync(bound)
	if err != nil {
		if c.ethp2pCtx.Err() != nil {
			return nil, context.Cause(c.ethp2pCtx)
		}
		return nil, err
	}
	return stream{s}, nil
}

func (c *ethp2pConn) OpenUniStream(ctx context.Context) (SendStream, error) {
	if c.ethp2pCtx.Err() != nil {
		return nil, context.Cause(c.ethp2pCtx)
	}
	bound, cleanup := viewContext(ctx, c.ethp2pCtx)
	defer cleanup()
	s, err := c.conn.OpenUniStreamSync(bound)
	if err != nil {
		if c.ethp2pCtx.Err() != nil {
			return nil, context.Cause(c.ethp2pCtx)
		}
		return nil, err
	}
	return sendStream{s}, nil
}

func (c *ethp2pConn) AcceptBiStream(ctx context.Context) (Stream, error) {
	if c.ethp2pCtx.Err() != nil {
		return nil, context.Cause(c.ethp2pCtx)
	}
	select {
	case s := <-c.ethp2pBi:
		if c.ethp2pCtx.Err() != nil {
			s.CancelRead(streamReset)
			s.CancelWrite(streamReset)
			return nil, context.Cause(c.ethp2pCtx)
		}
		return stream{s}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ethp2pCtx.Done():
		return nil, context.Cause(c.ethp2pCtx)
	}
}

func (c *ethp2pConn) AcceptUniStream(ctx context.Context) (ReceiveStream, error) {
	if c.ethp2pCtx.Err() != nil {
		return nil, context.Cause(c.ethp2pCtx)
	}
	select {
	case s := <-c.ethp2pUni:
		if c.ethp2pCtx.Err() != nil {
			s.CancelRead(streamReset)
			return nil, context.Cause(c.ethp2pCtx)
		}
		return receiveStream{s}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ethp2pCtx.Done():
		return nil, context.Cause(c.ethp2pCtx)
	}
}

func (c *ethp2pConn) SendDatagram(_ context.Context, payload []byte) error {
	if c.ethp2pCtx.Err() != nil {
		return context.Cause(c.ethp2pCtx)
	}
	return c.conn.SendDatagram(payload)
}

func (c *ethp2pConn) RecvDatagram(ctx context.Context) ([]byte, error) {
	if c.ethp2pCtx.Err() != nil {
		return nil, context.Cause(c.ethp2pCtx)
	}
	bound, cleanup := viewContext(ctx, c.ethp2pCtx)
	defer cleanup()
	payload, err := c.conn.ReceiveDatagram(bound)
	if err != nil && c.ethp2pCtx.Err() != nil {
		return nil, context.Cause(c.ethp2pCtx)
	}
	return payload, err
}

func (c *ethp2pConn) Close() error {
	// Mark only; the raw connection closes once the libp2p view closes too.
	c.closeSide(sideEthp2p, appNoError, "closed")
	return nil
}

func (c *ethp2pConn) SupportsDatagrams() bool {
	support := c.conn.ConnectionState().SupportsDatagrams
	return support.Local && support.Remote
}

func (c *ethp2pConn) ConnectionStats() (uint64, uint64) {
	s := c.conn.ConnectionStats()
	return s.BytesSent, s.BytesReceived
}
