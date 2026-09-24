package transport

import (
	"context"
	"errors"
	"net"

	"github.com/ethp2p/ethp2p/protocol"
)

// Sink receives ethp2p views after Bind. Calls run in transport-owned
// per-connection goroutines, except dial admission, which runs in Dial's caller.
// Methods must never block on protocols. They may close other views, which
// waits at most the GoAway timeout.
type Sink interface {
	// Admit decides whether to admit a view whose peer Hello has arrived,
	// at most once per view.
	// A rejection closes it with code; Stream and Closed are never called for it.
	Admit(conn Conn, hello Hello) (code protocol.Code, ok bool)
	// Stream transfers ownership of a classified stream from an admitted view.
	// Bidirectional streams also implement Stream.
	Stream(conn Conn, sel protocol.Selector, s ReceiveStream)
	// Closed reports exactly once that an admitted view was released.
	Closed(conn Conn, code protocol.Code)
}

// PeerID returns this endpoint's authenticated identity.
func (t *Ethp2pTransport) PeerID() PeerID { return t.shared.PeerID() }

// Bind sets the Hello for new views, routes later views to sink, and starts
// listening. It may succeed once. SetHello and Accept then return ErrSinkBound.
func (t *Ethp2pTransport) Bind(h Hello, sink Sink) error {
	if sink == nil {
		return errors.New("nil ethp2p sink")
	}
	if err := validateHello(h); err != nil {
		return err
	}
	t.shared.helloMu.Lock()
	defer t.shared.helloMu.Unlock()
	if t.shared.sink != nil {
		return ErrSinkBound
	}
	if err := t.shared.start(); err != nil {
		return err
	}
	t.shared.hello, t.shared.hasHello = cloneHello(h), true
	t.shared.sink = sink
	t.shared.interest.Or(uint32(sideEthp2p))
	close(t.shared.sinkBound)
	return nil
}

func (t *SharedTransport) admit(ctx context.Context, c *ethp2pConn, sink Sink) error {
	hello, err := c.PeerHello(ctx)
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = context.Cause(c.ethp2pCtx)
	}
	if err != nil {
		_ = c.Close()
		return err
	}
	code, ok := sink.Admit(c, hello)
	if !ok {
		_ = c.CloseWithCode(code)
		return &ViewClosedError{Code: code}
	}
	return nil
}

// pump runs in a transport-owned goroutine. Its sibling bidi loop is also
// owned by wg and joined before Closed. Sink ownership starts only after Admit.
func (t *SharedTransport) pump(c *ethp2pConn, sink Sink) {
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	done := make(chan error, 1)
	t.wg.Go(func() {
		for {
			stream, sel, err := c.AcceptStream(ctx)
			if err != nil {
				cancel()
				done <- err
				return
			}
			sink.Stream(c, sel, stream)
		}
	})
	var uniErr error
	for {
		stream, sel, err := c.AcceptUniStream(ctx)
		if err != nil {
			uniErr = err
			break
		}
		sink.Stream(c, sel, stream)
	}
	cancel()
	err := errors.Join(context.Cause(c.ethp2pCtx), uniErr, <-done)
	code := protocol.Unspecified
	if closed, ok := errors.AsType[*ViewClosedError](err); ok {
		code = closed.Code
	} else if t.ctx.Err() != nil {
		code = protocol.Closing
	} else if timeout, ok := errors.AsType[net.Error](err); ok && timeout.Timeout() {
		code = protocol.Timeout
	}
	_ = c.Close()
	sink.Closed(c, code)
}
