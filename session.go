package ethp2p

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethp2p/ethp2p/control"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/quic-go/quic-go"
)

// goAwayTimeout is GOAWAY_TIMEOUT. It bounds GOAWAY and FIN, so a peer
// that stops reading cannot stall a view's release.
var goAwayTimeout = time.Second

// controlSession runs the control protocol on one view and exists only once
// our HELLO is written. The stack opens our control stream before any other
// stream on the view, and every release of the view goes through close,
// so an outgoing GOAWAY always follows HELLO and precedes the release.
type controlSession struct {
	conn *transport.Conn
	out  *quic.SendStream
	// closing is set before close cancels the peer's control stream, so a
	// reader can tell that cancellation from a reset by the peer.
	closing   atomic.Bool
	mu        sync.Mutex
	peer      *quic.ReceiveStream // mu; the peer's control stream once found
	closeOnce sync.Once
}

// openControl opens our control stream, the first stream the stack opens on
// the view, and writes its header and HELLO before returning, so a view's
// controlSession exists only once its HELLO is written and close may always
// send GOAWAY. The open and the write both end when ctx ends; the caller
// gives ctx the HELLO deadline (HELLO_TIMEOUT from the stack receiving the
// view). A HELLO is at most MAX_CONTROL_FRAME, so the write blocks only on a
// peer that grants less stream or connection credit than that.
//
// On failure it releases the view without GOAWAY, since the peer never read
// our HELLO, and returns the cause. One code, computed once, both resets a
// stream it opened and closes the view: Closing when ctx ended, Timeout at
// the deadline, Unspecified otherwise. Nothing is reset when the connection
// ended, and a peer's STOP_SENDING has already reset the stream with the
// peer's code, which quic-go keeps.
func openControl(ctx context.Context, conn *transport.Conn, helloPayload []byte) (*controlSession, error) {
	out, err := conn.OpenUniStream(ctx)
	if err != nil {
		conn.CloseWithCode(helloWriteCode(ctx, err))
		return nil, err
	}
	deadline, _ := ctx.Deadline()
	_ = out.SetWriteDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { _ = out.SetWriteDeadline(time.Now()) })
	_, err = out.Write(helloPayload)
	stop()
	if err != nil {
		code := helloWriteCode(ctx, err)
		if !transport.ConnectionClosed(err) {
			out.CancelWrite(streamCode(code))
		}
		conn.CloseWithCode(code)
		return nil, err
	}
	_ = out.SetWriteDeadline(time.Time{})
	return &controlSession{conn: conn, out: out}, nil
}

// helloWriteCode is the code a failed HELLO open or write ends with:
// caller cancellation ends with Closing, the HELLO deadline with Timeout.
func helloWriteCode(ctx context.Context, err error) wire.Code {
	if ctx.Err() != nil && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return wire.Closing
	}
	return wire.TimeoutOr(err, wire.Unspecified)
}

// awaitHello waits until ctx ends for the peer's first unidirectional
// stream, its control stream, and reads its header and HELLO. The caller
// gives ctx the same HELLO deadline as openControl. Bidirectional streams
// stay in transport's queue until the supervisor reads them after admission.
// On failure it releases the view and returns the cause; transport resets
// streams still queued with Closing.
func (s *controlSession) awaitHello(ctx context.Context) (control.PeerHello, error) {
	deadline, _ := ctx.Deadline()
	var in *quic.ReceiveStream
	for in == nil {
		stream, ok := s.conn.NextUniStream()
		if !ok {
			select {
			case <-s.conn.UniStreams():
				continue
			case <-s.conn.Done():
				return control.PeerHello{}, &transport.ViewClosedError{Code: s.conn.CloseCode()}
			case <-ctx.Done():
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					s.close(wire.ControlViolation, true)
					return control.PeerHello{}, &transport.ViewClosedError{Code: wire.ControlViolation}
				}
				s.close(wire.Closing, true)
				return control.PeerHello{}, ctx.Err()
			}
		}
		in = stream
	}
	s.mu.Lock()
	if s.closing.Load() {
		s.mu.Unlock()
		in.CancelRead(streamCode(wire.Closing))
		return control.PeerHello{}, &transport.ViewClosedError{Code: wire.Closing}
	}
	s.peer = in
	s.mu.Unlock()
	// One context covers the read, so Stack.Close or a cancelled Connect ends
	// it at once rather than at the HELLO deadline. The deadline itself is
	// left to the read: its stream deadline ends it at the same instant, and
	// readFailure classifies it ControlViolation, so the callback must not
	// win the close race with Closing.
	stop := context.AfterFunc(ctx, func() {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return
		}
		s.close(wire.Closing, true)
	})
	defer stop()
	_ = in.SetReadDeadline(deadline)
	h, err := control.ReadHello(in)
	if err != nil {
		if ctx.Err() != nil && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return control.PeerHello{}, ctx.Err()
		}
		return control.PeerHello{}, s.readFailure(err)
	}
	_ = in.SetReadDeadline(time.Time{})
	return h, nil
}

// readGoAway reads one frame from the peer's control stream after its HELLO,
// which must be GOAWAY. It reports the view's closing code and whether we
// still owe the peer a GOAWAY. If the stream ends or fails first,
// classifyReadError decides. It reports ok false when the stack closed the
// view itself or the connection ended, since then nothing is left to do.
func (s *controlSession) readGoAway() (code wire.Code, goAway, ok bool) {
	parsed, err := control.ReadGoAway(s.peer)
	if err != nil {
		return s.classifyReadError(err, false)
	}
	return parsed, false, true
}

// classifyReadError maps a failed control read to the view's closing code and
// whether we send GOAWAY. ok is false when the stack is already closing the
// view or the whole connection ended.
func (s *controlSession) classifyReadError(err error, first bool) (code wire.Code, goAway, ok bool) {
	if s.closing.Load() || transport.ConnectionClosed(err) {
		return wire.Unspecified, false, false
	}
	if reset, isReset := errors.AsType[*quic.StreamError](err); isReset {
		// Only close cancels this stream locally, and it sets closing first,
		// so this reset is the peer's.
		if reset.ErrorCode&1 != 0 {
			return wire.ControlViolation, true, true
		}
		return wire.ParseCode(uint64(reset.ErrorCode)), false, true
	}
	if errors.Is(err, io.EOF) && !first {
		return wire.Unspecified, false, true
	}
	return wire.ControlViolation, true, true
}

// readFailure closes the view for a failed HELLO read and returns the cause.
// A connection end is left to the view, which reports it through CloseCode.
func (s *controlSession) readFailure(err error) error {
	code, goAway, ok := s.classifyReadError(err, true)
	if !ok {
		return err
	}
	s.close(code, goAway)
	return &transport.ViewClosedError{Code: code, Remote: !goAway}
}

// close is the view's only release: it ends our control stream within
// GOAWAY_TIMEOUT, writing GOAWAY first when goAway is set, cancels reading
// the peer's unless the peer closed it, and releases the view with code. code
// must be a stack code.
func (s *controlSession) close(code wire.Code, goAway bool) {
	s.closeOnce.Do(func() {
		s.closing.Store(true)
		_ = s.out.SetWriteDeadline(time.Now().Add(goAwayTimeout))
		if goAway {
			if raw := control.GoAwayBytes(code); raw != nil {
				_, _ = s.out.Write(raw)
			}
		}
		_ = s.out.Close()
		cancelCode := wire.Closing
		if code == wire.ControlViolation {
			cancelCode = wire.ControlViolation
		}
		s.mu.Lock()
		if s.peer != nil && goAway {
			s.peer.CancelRead(streamCode(cancelCode))
		}
		s.mu.Unlock()
		s.conn.CloseWithCode(code)
	})
}
