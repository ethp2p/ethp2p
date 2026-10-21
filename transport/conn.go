package transport

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/quic-go/quic-go"
)

// Conn is the ethp2p view of an authenticated QUIC connection. A Conn can be
// used concurrently by multiple goroutines.
//
// Accept and Dial return a Conn once TLS has authenticated the peer.
type Conn struct {
	outbound bool
	conn     *quic.Conn
	biSem    chan struct{}
	// Each view's context ends when that view closes or the connection does;
	// pending operations on the view wait on it.
	libp2pCtx    context.Context
	cancelLibp2p context.CancelCauseFunc
	ethp2pCtx    context.Context
	cancelEthp2p context.CancelCauseFunc
	// wg owns this connection's dispatcher and classification goroutines.
	// Starts require a tracked parent: the accept loop or a Dial reservation.
	wg *sync.WaitGroup

	// Delivery queues for incoming streams. ethp2p streams wait unread, each
	// holding its stream credit, so the peer's stream limit bounds the queue.
	libp2pStreams *streamQueue[*quic.Stream]
	ethp2pBidi    *streamQueue[*quic.Stream]
	ethp2pUni     *streamQueue[*quic.ReceiveStream]
	// closed is a bitfield of released views. The raw connection closes once
	// both are released, so closing one view never kills the other.
	closed atomic.Uint32

	remote identity.PeerID
	// endpoint is the Endpoint's lifetime. CloseCode reports Closing
	// while it is done, distinguishing endpoint shutdown from other closures.
	endpoint context.Context

	classifyTimeout time.Duration
}

// RemotePeerID returns the identity authenticated for the remote endpoint.
func (c *Conn) RemotePeerID() identity.PeerID { return c.remote }

// Outbound reports whether this endpoint dialed the connection.
func (c *Conn) Outbound() bool { return c.outbound }

// OpenStream opens a bidirectional stream, waiting for stream credit until ctx
// or the view ends. The caller writes the stream header.
func (c *Conn) OpenStream(ctx context.Context) (*quic.Stream, error) {
	bound, cleanup := joinContext(ctx, c.ethp2pCtx)
	defer cleanup()
	s, err := c.conn.OpenStreamSync(bound)
	if err != nil {
		if c.ethp2pCtx.Err() != nil {
			return nil, ethp2pError(context.Cause(c.ethp2pCtx))
		}
		return nil, ethp2pError(err)
	}
	return s, nil
}

// OpenUniStream opens a unidirectional stream, waiting for stream credit until
// ctx or the view ends. The caller writes the stream header.
func (c *Conn) OpenUniStream(ctx context.Context) (*quic.SendStream, error) {
	bound, cleanup := joinContext(ctx, c.ethp2pCtx)
	defer cleanup()
	s, err := c.conn.OpenUniStreamSync(bound)
	if err != nil {
		if c.ethp2pCtx.Err() != nil {
			return nil, ethp2pError(context.Cause(c.ethp2pCtx))
		}
		return nil, ethp2pError(err)
	}
	return s, nil
}

// Streams is the wake channel (capacity 1) of the bidirectional stream queue.
// It is signaled when a stream is queued; the consumer then drains with
// NextStream until it reports empty.
func (c *Conn) Streams() <-chan struct{} { return c.ethp2pBidi.ready }

// NextStream returns the next incoming bidirectional ethp2p stream, unread,
// in classification completion order. It never blocks; it reports false when
// the queue is empty or the view is closed. The caller owns the stream and
// must end both sides.
func (c *Conn) NextStream() (*quic.Stream, bool) {
	if c.ethp2pCtx.Err() != nil {
		return nil, false
	}
	return c.ethp2pBidi.tryPop()
}

// UniStreams is the wake channel (capacity 1) of the unidirectional stream
// queue, signaled like Streams.
func (c *Conn) UniStreams() <-chan struct{} { return c.ethp2pUni.ready }

// NextUniStream returns the next incoming unidirectional ethp2p stream,
// unread, in stream ID order, so the first one returned is the peer's first
// unidirectional stream. It never blocks; it reports false when the queue is
// empty or the view is closed.
func (c *Conn) NextUniStream() (*quic.ReceiveStream, bool) {
	if c.ethp2pCtx.Err() != nil {
		return nil, false
	}
	return c.ethp2pUni.tryPop()
}

// Done closes when the view is released. CloseCode then reports the cause.
func (c *Conn) Done() <-chan struct{} { return c.ethp2pCtx.Done() }

// CloseCode reports how the view closed. It is valid once Done is closed: a
// CloseWithCode closure keeps its code; an endpoint shutdown is Closing; a
// timeout is Timeout; anything else is Unspecified.
func (c *Conn) CloseCode() wire.Code {
	err := ethp2pError(context.Cause(c.ethp2pCtx))
	if closed, ok := errors.AsType[*ViewClosedError](err); ok {
		return closed.Code
	}
	if c.endpoint.Err() != nil {
		return wire.Closing
	}
	return wire.TimeoutOr(err, wire.Unspecified)
}

// Close releases the view with Closing.
func (c *Conn) Close() { c.CloseWithCode(wire.Closing) }

// CloseWithCode releases the view: queued streams are reset with Closing, and
// once the libp2p view is also released the connection closes with code. It
// sends no GOAWAY. The first closure cause wins; later calls are no-ops.
func (c *Conn) CloseWithCode(code wire.Code) {
	c.closeSide(sideEthp2p, quic.ApplicationErrorCode(code.Wire()), code.String(), func() {
		c.cancelEthp2p(&ViewClosedError{Code: code})
	})
}

// ConnectionStats returns the bytes sent and received on the whole QUIC
// connection, libp2p traffic included.
func (c *Conn) ConnectionStats() (uint64, uint64) {
	s := c.conn.ConnectionStats()
	return s.BytesSent, s.BytesReceived
}

func newConn(raw *quic.Conn, wg *sync.WaitGroup, remote identity.PeerID, endpoint context.Context, classifyTimeout time.Duration) *Conn {
	libp2pCtx, cancelLibp2p := context.WithCancelCause(raw.Context())
	ethp2pCtx, cancelEthp2p := context.WithCancelCause(raw.Context())
	sc := &Conn{
		conn:            raw,
		libp2pCtx:       libp2pCtx,
		cancelLibp2p:    cancelLibp2p,
		ethp2pCtx:       ethp2pCtx,
		cancelEthp2p:    cancelEthp2p,
		wg:              wg,
		remote:          remote,
		biSem:           make(chan struct{}, maxStreamsPendingClassify),
		libp2pStreams:   newStreamQueue[*quic.Stream](),
		ethp2pBidi:      newStreamQueue[*quic.Stream](),
		ethp2pUni:       newStreamQueue[*quic.ReceiveStream](),
		endpoint:        endpoint,
		classifyTimeout: classifyTimeout,
	}
	for range maxStreamsPendingClassify {
		sc.biSem <- struct{}{}
	}
	return sc
}

// startDispatchers starts the connection's only accepters of incoming streams,
// one per direction. Both end with the connection's context.
func (c *Conn) startDispatchers() {
	c.wg.Go(c.drainBidi)
	c.wg.Go(c.drainUni)
}

// claimRelease atomically marks want's view released. first reports that
// this call made the transition (and so owns the release work); last
// reports that both views are now released. It is the single release
// mechanism for both views: a concurrent releaser observes its bit
// already set and does nothing.
func (c *Conn) claimRelease(want side) (first, last bool) {
	prev := c.closed.Or(uint32(want))
	return prev&uint32(want) == 0,
		prev|uint32(want) == uint32(sideLibp2p|sideEthp2p)
}

// closeSide releases want's view. onFirst, if set, runs for the first
// releaser before the connection is closed (CloseWithCode records the
// view's cause there).
func (c *Conn) closeSide(want side, code quic.ApplicationErrorCode, reason string, onFirst func()) {
	first, last := c.claimRelease(want)
	if !first {
		return
	}
	if onFirst != nil {
		onFirst()
	}
	if want == sideLibp2p {
		c.cancelLibp2p(ErrViewClosed)
	}
	// Releasing the last view closes the connection before its queued streams
	// are reset, so the resets cannot hand the peer stream credit.
	if last {
		_ = c.conn.CloseWithError(code, reason)
	}
	switch want {
	case sideLibp2p:
		c.drainLibp2pQueue()
	case sideEthp2p:
		c.drainEthp2pQueues()
	}
}

// joinContext bounds a caller's wait by bound - a view's context, or the
// endpoint's - without losing the caller's deadline or cancellation. The
// returned cleanup stops the callback.
func joinContext(ctx, bound context.Context) (context.Context, func()) {
	joined, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(bound, func() { cancel(context.Cause(bound)) })
	if bound.Err() != nil {
		cancel(context.Cause(bound))
	}
	return joined, func() { stop(); cancel(nil) }
}

func (c *Conn) drainLibp2pQueue() {
	for _, s := range c.libp2pStreams.close() {
		resetBi(s, wire.Closing)
	}
}

func (c *Conn) drainEthp2pQueues() {
	for _, s := range c.ethp2pBidi.close() {
		s.CancelRead(quicCode(wire.Closing))
		s.CancelWrite(quicCode(wire.Closing))
	}
	for _, s := range c.ethp2pUni.close() {
		s.CancelRead(quicCode(wire.Closing))
	}
}

func (c *Conn) libp2p() quicreuse.QUICConn {
	return (*libp2pConn)(c)
}

// drainBidi accepts bidirectional streams and classifies them as ethp2p or
// libp2p. It takes a biSem token before asking QUIC for the next stream, so
// streams beyond the bound stay in QUIC's accept queue.
func (c *Conn) drainBidi() {
	ctx := c.conn.Context()
	for {
		select {
		case <-c.biSem:
		case <-ctx.Done():
			return
		}
		accepted, err := c.conn.AcceptStream(ctx)
		if err != nil {
			c.biSem <- struct{}{}
			if ctx.Err() == nil {
				slog.Warn("failed to accept stream", "err", err)
			}
			return
		}
		c.wg.Go(func() {
			defer func() { c.biSem <- struct{}{} }()
			libp2p, err := classifyBi(accepted, c.classifyTimeout)
			if err != nil {
				if ctx.Err() == nil {
					if peerReset(err) {
						accepted.CancelWrite(quicCode(wire.Unspecified))
					} else {
						resetBi(accepted, wire.TimeoutOr(err, wire.BadHeader))
					}
				}
				return
			}
			if !libp2p {
				if c.ethp2pCtx.Err() != nil {
					resetBi(accepted, wire.Closing)
					return
				}
				// The send side stays open until the protocol ends it, so a
				// queued bidi stream holds credit even after receiving FIN.
				if !c.ethp2pBidi.push(accepted) {
					resetBi(accepted, wire.Closing)
				}
				return
			}
			if c.libp2pCtx.Err() != nil {
				resetBi(accepted, wire.Closing)
				return
			}
			if !c.libp2pStreams.push(accepted) {
				resetBi(accepted, wire.Closing)
			}
		})
	}
}

// multistreamFirstByte begins every multistream-select header: it is the
// length of "/multistream/1.0.0\n". No ethp2p stream header begins with it,
// because protocol 19 is reserved.
const multistreamFirstByte = 0x13

// classifyBi peeks a bidirectional stream's first byte within timeout,
// leaving it unread, and reports whether it begins libp2p's
// multistream-select header.
func classifyBi(stream *quic.Stream, timeout time.Duration) (libp2p bool, err error) {
	if err := stream.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return false, err
	}
	defer func() {
		_ = stream.SetReadDeadline(time.Time{})
	}()
	var first [1]byte
	if _, err := stream.Peek(first[:]); err != nil {
		return false, err
	}
	return first[0] == multistreamFirstByte, nil
}

func peerReset(err error) bool {
	reset, ok := errors.AsType[*quic.StreamError](err)
	return ok && reset.Remote
}

func quicCode(code wire.Code) quic.StreamErrorCode { return quic.StreamErrorCode(code.Wire()) }

func resetBi(stream *quic.Stream, code wire.Code) {
	stream.CancelRead(quicCode(code))
	stream.CancelWrite(quicCode(code))
}

// drainUni hands every unidirectional stream to the ethp2p uni queue unread, in
// stream ID order, the order QUIC accepts them; the stack takes the first as
// the peer's control stream. libp2p opens no unidirectional streams.
func (c *Conn) drainUni() {
	ctx := c.conn.Context()
	for {
		s, err := c.conn.AcceptUniStream(ctx)
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("failed to accept stream", "err", err)
			}
			return
		}
		if c.ethp2pCtx.Err() != nil || !c.ethp2pUni.push(s) {
			s.CancelRead(quicCode(wire.Closing))
		}
	}
}
