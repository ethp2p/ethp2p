package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethp2p/ethp2p/wire"
	"github.com/ethp2p/ethp2p/transport/pb"
	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/proto"
)

const maxControlFrame = 16 << 10

// Hello is the first message on a view's control stream. Record is an optional
// EIP-778 encoding; the transport leaves its interpretation to the stack.
type Hello struct {
	Selectors []wire.Selector // Strictly ascending, at most wire.MaxSelectors, none zero.
	Record    []byte
}

func cloneHello(h Hello) Hello {
	return Hello{Selectors: slices.Clone(h.Selectors), Record: slices.Clone(h.Record)}
}

func helloMessage(h Hello) *pb.Control {
	selectors := make([]uint64, len(h.Selectors))
	for i, selector := range h.Selectors {
		selectors[i] = uint64(selector)
	}
	return &pb.Control{Message: &pb.Control_Hello{Hello: &pb.Hello{Selectors: selectors, Record: h.Record}}}
}

// SetHello validates and snapshots the Hello used by views created after this
// call. Its first successful call registers interest in ethp2p views.
func (t *Ethp2pTransport) SetHello(h Hello) error {
	if err := validateHello(h); err != nil {
		return err
	}
	t.shared.helloMu.Lock()
	defer t.shared.helloMu.Unlock()
	if t.shared.sink != nil {
		return ErrSinkBound
	}
	t.shared.hello = cloneHello(h)
	t.shared.hasHello = true
	t.shared.interest.Or(uint32(sideEthp2p))
	return nil
}

func validateHello(h Hello) error {
	if err := wire.ValidateSelectors(h.Selectors); err != nil {
		return err
	}
	raw, err := proto.Marshal(helloMessage(h))
	if err != nil {
		return err
	}
	if len(raw) > maxControlFrame {
		return wire.ErrFrameTooLarge
	}
	return nil
}

func (t *SharedTransport) helloSnapshot() Hello {
	t.helloMu.RLock()
	defer t.helloMu.RUnlock()
	return cloneHello(t.hello)
}

// ViewClosedError is the closure cause of an ethp2p view closed through its
// control wire. Remote means the peer ended its control stream.
type ViewClosedError struct {
	Code   wire.Code
	Remote bool
}

// Error describes the view closure.
func (e *ViewClosedError) Error() string {
	if e.Remote {
		return fmt.Sprintf("peer closed ethp2p view: %s", e.Code)
	}
	return fmt.Sprintf("closed ethp2p view: %s", e.Code)
}

// Is matches ErrViewClosed independently of the code and origin.
func (e *ViewClosedError) Is(target error) bool { return target == ErrViewClosed }

type controlState struct {
	mu           sync.Mutex // protects peer stream ownership and the received Hello
	writeMu      sync.Mutex // orders Hello before closure; never held by the reader
	out          atomic.Pointer[quic.SendStream]
	helloWritten bool
	peer         *quic.ReceiveStream
	peerCancel   wire.Code
	peerHello    Hello
	helloReady   chan struct{}
	closeOnce    sync.Once
}

// startControl reserves the first outbound unidirectional stream before the
// view becomes visible. The writer and reader both belong to the transport wg.
func (c *ethp2pConn) startControl(h Hello) error {
	if c.ethp2pCtx.Err() != nil {
		return ethp2pError(context.Cause(c.ethp2pCtx))
	}
	out, err := c.conn.OpenUniStream()
	if err != nil {
		return err
	}
	ctl := &c.control
	ctl.out.Store(out)
	// Reserve the writer's turn synchronously: an immediate Close must still
	// send Hello first. closeControl can bound this write by setting its deadline
	// without taking writeMu.
	ctl.writeMu.Lock()
	c.wg.Go(func() { c.writeHello(h) })
	c.wg.Go(func() { c.readControl(c.created) })
	return nil
}

func (c *sharedConn) writeHello(h Hello) {
	ctl := &c.control
	message, err := proto.Marshal(helloMessage(h))
	if err != nil {
		ctl.writeMu.Unlock()
		c.closeControl(wire.Unspecified, false, true, wire.Closing)
		return
	}
	raw := wire.AppendFrame(nil, []byte{0})
	raw = wire.AppendFrame(raw, message)
	n, err := ctl.out.Load().Write(raw)
	if err == nil && n != len(raw) {
		err = io.ErrShortWrite
	}
	ctl.helloWritten = err == nil
	ctl.writeMu.Unlock()
	if err != nil {
		if _, ok := errors.AsType[*quic.ApplicationError](err); ok {
			c.cancelEthp2p(ethp2pError(err))
		}
		c.closeControl(wire.Unspecified, false, true, wire.Closing)
	}
}

func (c *sharedConn) readControl(created time.Time) {
	deadline := created.Add(time.Duration(helloTimeout.Load()))
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	var in *quic.ReceiveStream
	select {
	case in = <-c.controlIn:
	case <-timer.C:
		c.controlViolation()
		return
	case <-c.ethp2pCtx.Done():
		return
	}
	ctl := &c.control
	if c.ethp2pCtx.Err() != nil {
		return
	}
	_ = in.SetReadDeadline(deadline)
	message, err := readControlFrame(in)
	if err != nil {
		c.controlReadError(err, true)
		return
	}
	hello := message.GetHello()
	if hello == nil {
		c.controlViolation()
		return
	}
	h := Hello{Selectors: make([]wire.Selector, len(hello.Selectors)), Record: slices.Clone(hello.Record)}
	for i, selector := range hello.Selectors {
		h.Selectors[i] = wire.Selector(selector)
	}
	if wire.ValidateSelectors(h.Selectors) != nil || time.Now().After(deadline) {
		c.controlViolation()
		return
	}
	_ = in.SetReadDeadline(time.Time{})
	ctl.mu.Lock()
	ctl.peerHello = h
	close(ctl.helloReady)
	ctl.mu.Unlock()
	for {
		message, err = readControlFrame(in)
		if err != nil {
			c.controlReadError(err, false)
			return
		}
		switch message.Message.(type) {
		case *pb.Control_GoAway:
			value := message.GetGoAway().Code
			code := wire.Unspecified
			if value <= math.MaxUint64>>1 {
				code = wire.ParseCode(0, value<<1)
			}
			c.closeControl(code, true, false, wire.Closing)
			return
		case *pb.Control_Hello:
			c.controlViolation()
			return
		}
	}
}

func readControlFrame(in *quic.ReceiveStream) (*pb.Control, error) {
	raw, err := wire.ReadFrame(in, maxControlFrame)
	if err != nil {
		return nil, err
	}
	var message pb.Control
	if err := proto.Unmarshal(raw, &message); err != nil {
		return nil, err
	}
	return &message, nil
}

func (c *sharedConn) controlReadError(err error, first bool) {
	// QUIC wakes blocked stream reads before cancellation has necessarily
	// propagated to raw.Context. A connection close is not malformed control
	// input, even in that interval. Preserve its cause before any local close.
	if _, ok := errors.AsType[*quic.ApplicationError](err); ok {
		c.cancelEthp2p(ethp2pError(err))
		return
	}
	if c.ethp2pCtx.Err() != nil || c.conn.Context().Err() != nil {
		return
	}
	if reset, ok := errors.AsType[*quic.StreamError](err); ok && reset.Remote {
		raw := uint64(reset.ErrorCode)
		if raw&1 != 0 {
			c.controlViolation()
		} else {
			c.closeControl(wire.ParseCode(0, raw), true, false, wire.Closing)
		}
		return
	}
	if errors.Is(err, io.EOF) && !first {
		c.closeControl(wire.Unspecified, true, false, wire.Closing)
		return
	}
	if c.conn.Context().Err() == nil {
		c.controlViolation()
	}
}

func (c *sharedConn) controlViolation() {
	c.closeControl(wire.ControlViolation, false, true, wire.ControlViolation)
}

// closeControl is the sole control-protocol release path. It bounds a pending
// Hello write and the GoAway write with one deadline, then stops the peer read
// and releases the view. A peer closure sends only FIN in reply.
func (c *sharedConn) closeControl(code wire.Code, remote, sendGoAway bool, cancelCode wire.Code) {
	c.control.closeOnce.Do(func() {
		ctl := &c.control
		out := ctl.out.Load()
		if out != nil {
			_ = out.SetWriteDeadline(time.Now().Add(goAwayTimeout))
			ctl.writeMu.Lock()
			if sendGoAway && ctl.helloWritten {
				raw, err := proto.Marshal(&pb.Control{Message: &pb.Control_GoAway{GoAway: &pb.GoAway{Code: code.Wire() >> 1}}})
				if err == nil {
					frame := wire.AppendFrame(nil, raw)
					_, _ = out.Write(frame)
				}
			}
			_ = out.Close()
			ctl.writeMu.Unlock()
		}
		ctl.mu.Lock()
		peer := ctl.peer
		ctl.peerCancel = cancelCode
		if peer != nil && !remote {
			peer.CancelRead(quicCode(cancelCode))
		}
		c.cancelEthp2p(&ViewClosedError{Code: code, Remote: remote})
		ctl.mu.Unlock()
		c.closeSide(sideEthp2p, quic.ApplicationErrorCode(code.Wire()), code.String())
	})
}
