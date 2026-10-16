package transport

import (
	"context"
	"net"

	"github.com/ethp2p/ethp2p/identity"
)

// Ethp2pTransport provides ethp2p connections on a shared endpoint.
// The application closes the owning [Endpoint] to shut down the endpoint.
type Ethp2pTransport struct{ endpoint *Endpoint }

// Accept waits for the next inbound ethp2p connection. Concurrent callers
// share the same connection queue. It returns ErrClosed after
// Endpoint.Close.
func (t *Ethp2pTransport) Accept(ctx context.Context) (*Conn, error) {
	select {
	case c := <-t.endpoint.ethQ:
		return c, nil
	case <-t.endpoint.ctx.Done():
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// PeerID returns the local identity, the same one [Endpoint.PeerID] reports.
func (t *Ethp2pTransport) PeerID() identity.PeerID { return t.endpoint.PeerID() }

// Dial connects to addr and returns once TLS has authenticated the peer. A
// nonempty expect requires that peer identity. It returns [ErrDialLegacyPeer]
// if the peer selects libp2p, and ErrClosed after Endpoint.Close.
func (t *Ethp2pTransport) Dial(ctx context.Context, addr net.Addr, expect identity.PeerID) (*Conn, error) {
	// Reserve in wg before checking stopped, so a dial that passes the
	// check is counted before shutdown can wait on wg, and a dial that
	// arrives after shutdown refuses itself. The reservation is released
	// only on return - after startDispatchers has registered the new
	// connection's goroutines in wg, or after the dial failed - so Close
	// cannot return while this dial is in flight. The joined context ends
	// the handshake when shutdown cancels the endpoint.
	t.endpoint.wg.Add(1)
	defer t.endpoint.wg.Done()
	if t.endpoint.stopped.Load() {
		return nil, ErrClosed
	}
	bound, cleanup := joinContext(ctx, t.endpoint.ctx)
	defer cleanup()
	slot := &remoteIdentitySlot{}
	tlsConfig := t.endpoint.handshaker.connConfig(slot, expect)
	raw, err := t.endpoint.raw.Dial(bound, addr, tlsConfig, t.endpoint.config.quicConfig())
	if err != nil {
		if t.endpoint.ctx.Err() != nil {
			return nil, ErrClosed
		}
		return nil, ethp2pError(err)
	}

	alpn := raw.ConnectionState().TLS.NegotiatedProtocol
	if alpn == AlpnLibp2p {
		t.endpoint.offerLibp2p(raw)
		return nil, ErrDialLegacyPeer
	}

	conn := newConn(raw, &t.endpoint.wg, slot.pubKey.PeerID(), t.endpoint.ctx, t.endpoint.classifyTimeout)
	conn.outbound = true
	t.endpoint.offerLibp2p(conn.libp2p())
	conn.startDispatchers()
	return conn, nil
}
