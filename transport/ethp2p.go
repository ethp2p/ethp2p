package transport

import (
	"context"
	"net"
)

//
// TRANSPORT
//////////////////////

// Ethp2pTransport provides ethp2p connections on a shared endpoint.
// The application closes the owning [SharedTransport] to shut down the endpoint.
type Ethp2pTransport struct{ shared *SharedTransport }

// Accept waits for the next inbound ethp2p connection, starting the listener
// if needed. It returns only views whose peer Hello has arrived and was
// validated. Concurrent callers share the same connection queue. It returns
// ErrClosed after SharedTransport.Close or Ethp2pTransport.Close.
func (t *Ethp2pTransport) Accept(ctx context.Context) (*Conn, error) {
	if err := t.shared.start(); err != nil {
		return nil, ethp2pError(err)
	}
	select {
	case c := <-t.shared.ethQ:
		return c, nil
	case <-t.shared.ethDone:
		return nil, ErrClosed
	case <-t.shared.ctx.Done():
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Dial connects to addr and authenticates the peer. A nonempty expect requires
// that peer identity. It returns [ErrDialLegacyPeer] if the peer selects libp2p.
// It returns after the peer Hello has arrived and was validated; on failure it
// closes the view and returns the error through ethp2pError. The Dial
// reservation is released when Dial returns. It returns ErrClosed after
// Ethp2pTransport.Close.
func (t *Ethp2pTransport) Dial(ctx context.Context, addr net.Addr, expect PeerID) (*Conn, error) {
	release, ok := t.shared.reserve()
	if !ok {
		return nil, ErrClosed
	}
	defer release()
	t.shared.ethMu.Lock()
	ethClosed := t.shared.ethDetached
	t.shared.ethMu.Unlock()
	if ethClosed {
		return nil, ErrClosed
	}
	t.shared.helloMu.RLock()
	configured := t.shared.hasHello
	t.shared.helloMu.RUnlock()
	if !configured {
		return nil, ErrNoHello
	}

	slot := &remoteIdentitySlot{}
	tlsConfig := t.shared.handshaker.connConfig(slot, expect)
	raw, err := t.shared.raw.Dial(ctx, addr, tlsConfig, t.shared.profile.quicConfig())
	if err != nil {
		return nil, ethp2pError(err)
	}

	alpn := raw.ConnectionState().TLS.NegotiatedProtocol
	if alpn == AlpnLibp2p {
		// Hand a legacy connection to libp2p when that side is interested.
		t.shared.offerLibp2p(raw)
		return nil, ErrDialLegacyPeer
	}

	// Create the shared connection and its dispatchers, then return the ethp2p
	// side and feed the libp2p side to libp2p.
	sc := newSharedConn(raw, &t.shared.wg, PeerIDFromKey(slot.key), t.shared.ctx)
	sc.outbound = true
	ethp2p := sc.ethp2p()
	if err := ethp2p.startControl(t.shared.helloSnapshot()); err != nil {
		_ = ethp2p.Close()
		t.shared.offerLibp2p(sc.libp2p())
		sc.startDispatchers()
		return nil, ethp2pError(err)
	}
	t.shared.offerLibp2p(sc.libp2p())
	sc.startDispatchers()
	if _, err := ethp2p.waitHello(ctx); err != nil {
		_ = ethp2p.Close()
		return nil, ethp2pError(err)
	}
	return ethp2p, nil
}

// Listen starts the shared listener and reports its error synchronously.
// Accept also starts it.
func (t *Ethp2pTransport) Listen() error {
	return t.shared.start()
}

// Close stops ethp2p view delivery, mirroring detachLibp2p. It is idempotent;
// it clears ethp2p interest so later connections release their ethp2p view at
// once, closes every view still queued in ethQ with wire.Closing, and wakes
// blocked Accept calls with ErrClosed. Views already returned by Accept or
// Dial are unaffected. libp2p is unaffected.
func (t *Ethp2pTransport) Close() {
	t.shared.ethMu.Lock()
	if t.shared.ethDetached {
		t.shared.ethMu.Unlock()
		return
	}
	t.shared.ethDetached = true
	close(t.shared.ethDone)
	var queued []*Conn
drain:
	for {
		select {
		case c := <-t.shared.ethQ:
			queued = append(queued, c)
		default:
			break drain
		}
	}
	t.shared.interest.And(^uint32(sideEthp2p))
	t.shared.ethMu.Unlock()
	for _, c := range queued {
		_ = c.Close()
	}
}
