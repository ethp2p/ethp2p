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
// It promotes [SharedTransport.Close], so closing this view shuts down the
// entire endpoint, including libp2p connections. The application owns that call.
type Ethp2pTransport struct{ *SharedTransport }

// PeerID returns the local identity.
func (t *Ethp2pTransport) PeerID() PeerID { return t.handshaker.peerID }

// PublicKey returns the local identity key.
func (t *Ethp2pTransport) PublicKey() *PubKey { return t.handshaker.publicKey }

// Addr returns the local packet connection address.
func (t *Ethp2pTransport) Addr() net.Addr { return t.raw.Conn.LocalAddr() }

// Accept waits for the next inbound ethp2p connection, starting the listener
// if needed. Concurrent callers share the same connection queue.
func (t *Ethp2pTransport) Accept(ctx context.Context) (Conn, error) {
	if _, err := t.attach(sideEthp2p); err != nil {
		return nil, err
	}
	select {
	case c := <-t.ethQ:
		return c, nil
	case <-t.ctx.Done():
		return nil, errClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Dial connects to addr and authenticates the peer. A nonempty expect requires
// that peer identity. It returns [ErrDialLegacyPeer] if the peer selects libp2p.
func (t *Ethp2pTransport) Dial(ctx context.Context, addr net.Addr, expect PeerID) (Conn, error) {
	if t.ctx.Err() != nil {
		return nil, errClosed
	}

	slot := &identitySlot{}
	tlsConfig := t.handshaker.dialConfig(slot, expect)
	raw, err := t.raw.Dial(ctx, addr, tlsConfig, quicConfig.Clone())
	if err != nil {
		return nil, err
	}

	alpn := raw.ConnectionState().TLS.NegotiatedProtocol
	if alpn == AlpnLibp2p {
		// this is a legacy libp2p peer.
		// but since we've estalished the connection, we might as well hand
		// it over to libp2p on a best-effort basis.
		// If its queue is full, close it because we are not returning a
		// valid connection to our caller anyway.
		select {
		case t.libQ <- raw:
		default:
			_ = raw.CloseWithError(appFailure, errClosed.Error())
		}
		return nil, ErrDialLegacyPeer
	}

	// Create the shared connection and its dispatchers, then return the ethp2p
	// side and feed the libp2p side to libp2p.
	sc := newSharedConn(raw, &t.wg, slot.id)
	ethp2p := sc.ethp2p()
	select {
	case t.libQ <- sc.libp2p():
	default:
		_ = ethp2p.Close()
	}
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

// RemotePeerID returns the identity authenticated for the remote endpoint, or
// the caller-supplied identity for connections built by NewQUICConn. It reports
// the empty PeerID if the connection carries neither.
func (c *ethp2pConn) RemotePeerID() PeerID {
	id, _ := c.remoteID()
	return id
}

func (c *ethp2pConn) OpenStream(ctx context.Context) (Stream, error) {
	s, err := c.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return stream{s}, nil
}

func (c *ethp2pConn) AcceptBiStream(ctx context.Context) (Stream, error) {
	select {
	case s := <-c.ethp2pBi:
		return stream{s}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.conn.Context().Done():
		return nil, c.conn.Context().Err()
	}
}

func (c *ethp2pConn) OpenUniStream(ctx context.Context) (SendStream, error) {
	s, err := c.conn.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return sendStream{s}, nil
}

func (c *ethp2pConn) AcceptUniStream(ctx context.Context) (ReceiveStream, error) {
	select {
	case s := <-c.ethp2pUni:
		return receiveStream{s}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.conn.Context().Done():
		return nil, c.conn.Context().Err()
	}
}

func (c *ethp2pConn) SendDatagram(_ context.Context, payload []byte) error {
	return c.conn.SendDatagram(payload)
}

func (c *ethp2pConn) RecvDatagram(ctx context.Context) ([]byte, error) {
	return c.conn.ReceiveDatagram(ctx)
}

func (c *ethp2pConn) Close() error {
	// Mark only; the raw connection closes once the libp2p view closes too.
	c.closeSide(sideEthp2p, appNoError, "closed")
	return nil
}

func (c *ethp2pConn) SupportsStreams() bool { return true }

func (c *ethp2pConn) SupportsDatagrams() bool {
	support := c.conn.ConnectionState().SupportsDatagrams
	return support.Local && support.Remote
}

func (c *ethp2pConn) ConnectionStats() (uint64, uint64) {
	s := c.conn.ConnectionStats()
	return s.BytesSent, s.BytesReceived
}
