package transport

import (
	"context"
	"errors"
	"net"
	"os"

	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/wire"
	"google.golang.org/protobuf/encoding/protowire"
)

// PeerID is the binary multihash representation used by libp2p peer.ID. A
// PeerID is not the human-readable base-encoded form of that multihash.
type PeerID string

// PeerIDFromKey returns the inline identity-multihash peer ID of key.
func PeerIDFromKey(key *identity.PubKey) PeerID {
	raw := key.Marshal()
	id := []byte{0} // identity multihash code
	id = protowire.AppendVarint(id, uint64(len(raw)))
	return PeerID(append(id, raw...))
}

// Conn is the ethp2p view of an authenticated QUIC connection. A Conn can be
// used concurrently by multiple goroutines.
//
// Accept and Dial return a Conn only after the peer's Hello arrived and was
// validated, so PeerHello is always present. Classified streams wait in one
// per-connection queue, in classification completion order across both
// directions; the consumer pulls them with NextStream, signaled by Streams.
type Conn struct{ *sharedConn }

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
func (c *Conn) RemotePeerID() PeerID { return c.remote }

func (c *Conn) Outbound() bool { return c.outbound }

func (c *Conn) OpenStream(ctx context.Context, selector wire.Selector) (Stream, error) {
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

func (c *Conn) OpenUniStream(ctx context.Context, selector wire.Selector) (SendStream, error) {
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

// Streams is the stream queue's wake channel (capacity 1). It is signaled
// when classification enqueues a stream; the consumer then drains with
// NextStream until it reports empty.
func (c *Conn) Streams() <-chan struct{} { return c.ethp2pStreams.ready }

// NextStream returns the next classified stream in classification completion
// order, across both directions. It never blocks; it reports false when the
// queue is empty or the view is closed. Bidirectional streams implement
// Stream; unidirectional streams skip their selector frame on first Read.
func (c *Conn) NextStream() (ReceiveStream, wire.Selector, bool) {
	if c.ethp2pCtx.Err() != nil {
		return nil, 0, false
	}
	s, ok := c.ethp2pStreams.tryPop()
	if !ok {
		return nil, 0, false
	}
	return s.stream, s.selector, true
}

// Done closes when the view is released. CloseCode then reports the cause.
func (c *Conn) Done() <-chan struct{} { return c.ethp2pCtx.Done() }

// CloseCode reports how the view closed. It is valid once Done is closed: a
// control-plane closure keeps its stack code; an endpoint shutdown is
// Closing; a timeout is Timeout; anything else is Unspecified.
func (c *Conn) CloseCode() wire.Code {
	err := ethp2pError(context.Cause(c.ethp2pCtx))
	if closed, ok := errors.AsType[*ViewClosedError](err); ok {
		return closed.Code
	}
	if c.endpoint.Err() != nil {
		return wire.Closing
	}
	if timeout, ok := errors.AsType[net.Error](err); ok && timeout.Timeout() {
		return wire.Timeout
	}
	return wire.Unspecified
}

func (c *Conn) SendDatagram(_ context.Context, payload []byte) error {
	if c.ethp2pCtx.Err() != nil {
		return ethp2pError(context.Cause(c.ethp2pCtx))
	}
	return ethp2pError(c.conn.SendDatagram(payload))
}

func (c *Conn) RecvDatagram(ctx context.Context) ([]byte, error) {
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

func (c *Conn) Close() error {
	return c.CloseWithCode(wire.Closing)
}

// CloseWithCode sends GoAway and FIN before releasing the view. The first
// closure cause wins; subsequent calls are no-ops.
func (c *Conn) CloseWithCode(code wire.Code) error {
	if code.Wire()&1 != 0 {
		return errors.New("CloseWithCode requires a stack code")
	}
	c.closeControl(code, false, true, wire.Closing)
	return nil
}

// PeerHello returns a copy of the peer's validated Hello. It does not block:
// Accept and Dial return only after the Hello arrived. Its result owns its
// slices.
func (c *Conn) PeerHello() Hello {
	c.control.mu.Lock()
	defer c.control.mu.Unlock()
	return cloneHello(c.control.peerHello)
}

func (c *Conn) SupportsDatagrams() bool {
	support := c.conn.ConnectionState().SupportsDatagrams
	return support.Local && support.Remote
}

func (c *Conn) ConnectionStats() (uint64, uint64) {
	s := c.conn.ConnectionStats()
	return s.BytesSent, s.BytesReceived
}
