package transport

import (
	"context"
	"net"
)

// TransportEth provides ethp2p connections on a shared endpoint.
// It promotes [TransportShared.Close] because the ethp2p stack owns shutdown
// of the entire endpoint, including libp2p connections.
type TransportEth struct{ *TransportShared }

// PeerID returns the local identity.
func (t *TransportEth) PeerID() PeerID { return t.handshaker.peerID }

// PublicKey returns the local identity key.
func (t *TransportEth) PublicKey() *PubKey { return t.handshaker.publicKey }

// Addr returns the local packet connection address.
func (t *TransportEth) Addr() net.Addr { return t.raw.Conn.LocalAddr() }

// Accept waits for the next inbound ethp2p connection, starting the listener
// if needed. Concurrent callers share the same connection queue.
func (t *TransportEth) Accept(ctx context.Context) (Conn, error) {
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
func (t *TransportEth) Dial(ctx context.Context, addr net.Addr, expect PeerID) (Conn, error) {
	if t.ctx.Err() != nil {
		return nil, errClosed
	}
	hs := t.handshaker.newHandshake(expect)
	raw, err := t.raw.Dial(ctx, addr, &hs.config, quicConfig.Clone())
	if err != nil {
		return nil, err
	}
	state := raw.ConnectionState()
	if state.TLS.NegotiatedProtocol == AlpnLibp2p {
		// Stock libp2p peer: hand the connection to the local libp2p listener.
		// If its queue is full, close the connection because no view is
		// returned to the caller.
		select {
		case t.libQ <- &exclusiveConnLib{raw}:
		default:
			_ = raw.CloseWithError(appFailure, errClosed.Error())
		}
		return nil, ErrDialLegacyPeer
	}
	sc := t.split(raw)
	ethp2p := sc.ethp2p()
	ethp2p.dir = ConnDirOut
	ethp2p.auth = hs.auth
	select {
	case t.libQ <- sc.libp2p():
	default:
		_ = ethp2p.Close()
	}
	return ethp2p, nil
}
