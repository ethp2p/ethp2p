package transport

import (
	"context"
	"errors"
	"net"
	"os"

	"github.com/ethp2p/ethp2p/wire"
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
	t.shared.helloMu.RLock()
	bound, changed := t.shared.sink != nil, t.shared.sinkBound
	t.shared.helloMu.RUnlock()
	if bound {
		return nil, ErrSinkBound
	}
	if err := t.shared.start(); err != nil {
		return nil, ethp2pError(err)
	}
	select {
	case <-changed:
		return nil, ErrSinkBound
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
// When bound, it waits for Hello and calls Sink.Admit in the caller's goroutine
// before returning. A rejected admission returns a ViewClosedError.
func (t *Ethp2pTransport) Dial(ctx context.Context, addr net.Addr, expect PeerID) (Conn, error) {
	release, ok := t.shared.reserve()
	if !ok {
		return nil, ErrClosed
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
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
	sc := newSharedConn(raw, &t.shared.wg, slot.key.PeerID())
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
	t.shared.helloMu.RLock()
	sink := t.shared.sink
	t.shared.helloMu.RUnlock()
	if sink != nil {
		if err := t.shared.admit(ctx, ethp2p, sink); err != nil {
			return nil, ethp2pError(err)
		}
		// Transfer the existing reservation without an unowned gap, including
		// when shutdown overtook successful admission in the caller.
		transferred = true
		go func() {
			defer release()
			t.shared.pump(ethp2p, sink)
		}()
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

func openFailureCode(ctx context.Context, err error) wire.Code {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return wire.Timeout
	}
	if timeout, ok := errors.AsType[net.Error](err); ok && timeout.Timeout() {
		return wire.Timeout
	}
	return wire.Unspecified
}

// RemotePeerID returns the identity authenticated for the remote endpoint.
func (c *ethp2pConn) RemotePeerID() PeerID { return c.remotePeerID() }

func (c *ethp2pConn) Outbound() bool { return c.outbound }

func (c *ethp2pConn) OpenStream(ctx context.Context, selector wire.Selector) (Stream, error) {
	if c.ethp2pCtx.Err() != nil {
		return nil, ethp2pError(context.Cause(c.ethp2pCtx))
	}
	bound, cleanup := viewContext(ctx, c.ethp2pCtx)
	defer cleanup()
	s, err := c.conn.OpenStreamSync(bound)
	if err != nil {
		if c.ethp2pCtx.Err() != nil {
			return nil, ethp2pError(context.Cause(c.ethp2pCtx))
		}
		return nil, ethp2pError(err)
	}
	stop := context.AfterFunc(bound, func() { s.CancelWrite(quicCode(openFailureCode(bound, bound.Err()))) })
	err = wire.WriteSelector(s, selector)
	stop()
	if err = errors.Join(err, bound.Err()); err != nil {
		code := openFailureCode(bound, err)
		resetBi(s, code)
		return nil, ethp2pError(err)
	}
	return stream{s}, nil
}

func (c *ethp2pConn) OpenUniStream(ctx context.Context, selector wire.Selector) (SendStream, error) {
	if c.ethp2pCtx.Err() != nil {
		return nil, ethp2pError(context.Cause(c.ethp2pCtx))
	}
	bound, cleanup := viewContext(ctx, c.ethp2pCtx)
	defer cleanup()
	s, err := c.conn.OpenUniStreamSync(bound)
	if err != nil {
		if c.ethp2pCtx.Err() != nil {
			return nil, ethp2pError(context.Cause(c.ethp2pCtx))
		}
		return nil, ethp2pError(err)
	}
	stop := context.AfterFunc(bound, func() { s.CancelWrite(quicCode(openFailureCode(bound, bound.Err()))) })
	err = wire.WriteSelector(s, selector)
	stop()
	if err = errors.Join(err, bound.Err()); err != nil {
		s.CancelWrite(quicCode(openFailureCode(bound, err)))
		return nil, ethp2pError(err)
	}
	return sendStream{s}, nil
}

func (c *ethp2pConn) AcceptStream(ctx context.Context) (Stream, wire.Selector, error) {
	if _, err := c.PeerHello(ctx); err != nil {
		return nil, 0, ethp2pError(err)
	}
	s, err := c.ethp2pBi.pop(ctx, c.ethp2pCtx)
	if err != nil {
		return nil, 0, ethp2pError(err)
	}
	if c.ethp2pCtx.Err() != nil {
		resetBi(s.stream, wire.Closing)
		return nil, 0, ethp2pError(context.Cause(c.ethp2pCtx))
	}
	return stream{s.stream}, s.selector, nil
}

func (c *ethp2pConn) AcceptUniStream(ctx context.Context) (ReceiveStream, wire.Selector, error) {
	if _, err := c.PeerHello(ctx); err != nil {
		return nil, 0, ethp2pError(err)
	}
	s, err := c.ethp2pUni.pop(ctx, c.ethp2pCtx)
	if err != nil {
		return nil, 0, ethp2pError(err)
	}
	if c.ethp2pCtx.Err() != nil {
		s.stream.CancelRead(quicCode(wire.Closing))
		return nil, 0, ethp2pError(context.Cause(c.ethp2pCtx))
	}
	return &receiveStream{ReceiveStream: s.stream, skip: s.frameLen}, s.selector, nil
}

func (c *ethp2pConn) SendDatagram(_ context.Context, payload []byte) error {
	if c.ethp2pCtx.Err() != nil {
		return ethp2pError(context.Cause(c.ethp2pCtx))
	}
	return ethp2pError(c.conn.SendDatagram(payload))
}

func (c *ethp2pConn) RecvDatagram(ctx context.Context) ([]byte, error) {
	if c.ethp2pCtx.Err() != nil {
		return nil, ethp2pError(context.Cause(c.ethp2pCtx))
	}
	bound, cleanup := viewContext(ctx, c.ethp2pCtx)
	defer cleanup()
	payload, err := c.conn.ReceiveDatagram(bound)
	if err != nil && c.ethp2pCtx.Err() != nil {
		return nil, ethp2pError(context.Cause(c.ethp2pCtx))
	}
	return payload, ethp2pError(err)
}

func (c *ethp2pConn) Close() error {
	return c.CloseWithCode(wire.Closing)
}

// CloseWithCode sends GoAway and FIN before releasing the view. The first
// closure cause wins; subsequent calls are no-ops.
func (c *ethp2pConn) CloseWithCode(code wire.Code) error {
	if code.Wire()&1 != 0 {
		return errors.New("CloseWithCode requires a stack code")
	}
	c.closeControl(code, false, true, wire.Closing)
	return nil
}

// PeerHello waits for the peer's validated Hello. Its result owns its slices.
func (c *ethp2pConn) PeerHello(ctx context.Context) (Hello, error) {
	if c.ethp2pCtx.Err() != nil {
		return Hello{}, ethp2pError(context.Cause(c.ethp2pCtx))
	}
	select {
	case <-c.control.helloReady:
		if c.ethp2pCtx.Err() != nil {
			return Hello{}, ethp2pError(context.Cause(c.ethp2pCtx))
		}
		c.control.mu.Lock()
		h := cloneHello(c.control.peerHello)
		c.control.mu.Unlock()
		return h, nil
	case <-c.ethp2pCtx.Done():
		return Hello{}, ethp2pError(context.Cause(c.ethp2pCtx))
	case <-ctx.Done():
		return Hello{}, ctx.Err()
	}
}

func (c *ethp2pConn) SupportsDatagrams() bool {
	support := c.conn.ConnectionState().SupportsDatagrams
	return support.Local && support.Remote
}

func (c *ethp2pConn) ConnectionStats() (uint64, uint64) {
	s := c.conn.ConnectionStats()
	return s.BytesSent, s.BytesReceived
}
