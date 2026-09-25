package transport_test

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/wire"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/quic-go/quic-go"
)

type sinkAdmission struct {
	conn  transport.Conn
	hello transport.Hello
}
type sinkStream struct {
	conn   transport.Conn
	sel    wire.Selector
	stream transport.ReceiveStream
}
type recordingSink struct {
	admissions chan sinkAdmission
	streams    chan sinkStream
	closed     chan wire.Code
	admit      func(transport.Conn, transport.Hello) (wire.Code, bool)
	count      atomic.Int32
}

func newRecordingSink() *recordingSink {
	return &recordingSink{admissions: make(chan sinkAdmission, 4), streams: make(chan sinkStream, 8), closed: make(chan wire.Code, 4)}
}
func (s *recordingSink) Admit(c transport.Conn, h transport.Hello) (wire.Code, bool) {
	s.count.Add(1)
	s.admissions <- sinkAdmission{c, h}
	if s.admit != nil {
		return s.admit(c, h)
	}
	return wire.Unspecified, true
}
func (s *recordingSink) Stream(c transport.Conn, sel wire.Selector, in transport.ReceiveStream) {
	s.streams <- sinkStream{c, sel, in}
}
func (s *recordingSink) Closed(_ transport.Conn, c wire.Code) { s.closed <- c }
func sinkTake[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("sink callback timeout")
		var zero T
		return zero
	}
}

func TestSinkBind(t *testing.T) {
	e := transporttest.NewEndpoint(t)
	sink := newRecordingSink()
	if err := e.Eth.Bind(transport.Hello{Selectors: []wire.Selector{0}}, sink); err == nil {
		t.Fatal("invalid Hello accepted")
	}
	if err := e.Eth.Bind(transport.Hello{}, sink); err != nil {
		t.Fatal(err)
	}
	if err := e.Eth.Bind(transport.Hello{}, sink); !errors.Is(err, transport.ErrSinkBound) {
		t.Fatal(err)
	}
	if _, err := e.Eth.Accept(t.Context()); !errors.Is(err, transport.ErrSinkBound) {
		t.Fatal(err)
	}
	if err := e.Eth.SetHello(transport.Hello{}); !errors.Is(err, transport.ErrSinkBound) {
		t.Fatal(err)
	}
	closed := transporttest.NewEndpoint(t)
	_ = closed.Shared.Close()
	if err := closed.Eth.Bind(transport.Hello{}, sink); !errors.Is(err, transport.ErrClosed) {
		t.Fatal(err)
	}
}

func TestSinkCloseDuringDialAdmission(t *testing.T) {
	client, server := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	sink := newRecordingSink()
	entered, release := make(chan struct{}), make(chan struct{})
	sink.admit = func(transport.Conn, transport.Hello) (wire.Code, bool) {
		close(entered)
		<-release
		return wire.Unspecified, true
	}
	if err := client.Eth.Bind(transport.Hello{}, sink); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	accepted := make(chan error, 1)
	go func() { _, err := server.Eth.Accept(ctx); accepted <- err }()
	dialed := make(chan error, 1)
	go func() { _, err := client.Eth.Dial(ctx, server.Shared.Addr(), server.Shared.PeerID()); dialed <- err }()
	sinkTake(t, entered)
	closed := make(chan error, 1)
	go func() { closed <- client.Shared.Close() }()
	early := false
	select {
	case err := <-closed:
		if err != nil {
			t.Error(err)
		}
		early = true
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := sinkTake(t, dialed); err != nil {
		t.Errorf("accepted Dial = %v", err)
	}
	if !early {
		if err := sinkTake(t, closed); err != nil {
			t.Error(err)
		}
	}
	if err := sinkTake(t, accepted); err != nil {
		t.Error(err)
	}
	if early {
		t.Error("Close returned while outbound admission was still running")
	}
	// Once Close returns, Closed must already be queued, exactly once. No
	// waiting here: waiting could hide a callback from a late-started pump.
	select {
	case code := <-sink.closed:
		if code != wire.Closing {
			t.Errorf("Closed = %v", code)
		}
	default:
		t.Error("Close returned before Closed")
	}
	select {
	case code := <-sink.closed:
		t.Errorf("late/duplicate Closed(%v)", code)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestSinkInboundPump(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(map[bool]string{false: "local", true: "remote"}[remote], func(t *testing.T) {
			client, server := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
			// Retain libp2p to exercise GoAway while the physical connection stays open.
			if _, err := client.Shared.Libp2p().Listen(nil, nil); err != nil {
				t.Fatal(err)
			}
			// Keep this view alive to exercise control-stream closure independently.
			listener, err := server.Shared.Libp2p().Listen(nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			sink := newRecordingSink()
			gate := make(chan struct{})
			sink.admit = func(transport.Conn, transport.Hello) (wire.Code, bool) { <-gate; return wire.Unspecified, true }
			if err := server.Eth.Bind(transport.Hello{}, sink); err != nil {
				t.Fatal(err)
			}
			want := transport.Hello{Selectors: []wire.Selector{7}, Record: []byte("opaque")}
			if err := client.Eth.SetHello(want); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			out, err := client.Eth.Dial(ctx, server.Shared.Addr(), server.Shared.PeerID())
			if err != nil {
				t.Fatal(err)
			}
			lib, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer lib.CloseWithError(0, "done")
			admitted := sinkTake(t, sink.admissions)
			if admitted.conn.Outbound() || !out.Outbound() || string(admitted.hello.Record) != "opaque" || admitted.hello.Selectors[0] != 7 {
				t.Fatal("admission metadata")
			}
			send := func(bidi bool, payload byte) {
				t.Helper()
				var stream transport.SendStream
				var err error
				if bidi {
					stream, err = out.OpenStream(ctx, 7)
				} else {
					stream, err = out.OpenUniStream(ctx, 7)
				}
				if err != nil {
					t.Fatal(err)
				}
				_, err = stream.Write([]byte{payload})
				if err != nil {
					t.Fatal(err)
				}
				_ = stream.Close()
			}
			send(false, 1)
			send(true, 1)
			close(gate)
			check := func(want byte) {
				t.Helper()
				for range 2 {
					ev := sinkTake(t, sink.streams)
					data, err := io.ReadAll(ev.stream)
					if err != nil || len(data) != 1 || data[0] != want || ev.conn != admitted.conn || ev.sel != 7 {
						t.Fatalf("stream = %x %v", data, err)
					}
					if bi, ok := ev.stream.(transport.Stream); ok {
						_ = bi.Close()
					}
				}
			}
			check(1)
			send(false, 2)
			send(true, 2)
			check(2)
			code := wire.Closing
			if remote {
				code = wire.Refused
				_ = out.CloseWithCode(code)
			} else {
				_ = admitted.conn.Close()
			}
			if got := sinkTake(t, sink.closed); got != code {
				t.Fatalf("Closed = %v", got)
			}
			if sink.count.Load() != 1 {
				t.Fatal("repeated admission")
			}
			select {
			case <-sink.closed:
				t.Fatal("duplicate Closed")
			default:
			}
		})
	}
}

func TestSinkRejectedView(t *testing.T) {
	client, server := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	// Retain libp2p to exercise GoAway while the physical connection stays open.
	if _, err := client.Shared.Libp2p().Listen(nil, nil); err != nil {
		t.Fatal(err)
	}
	listener, err := server.Shared.Libp2p().Listen(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	sink := newRecordingSink()
	gate := make(chan struct{})
	sink.admit = func(transport.Conn, transport.Hello) (wire.Code, bool) { <-gate; return wire.Refused, false }
	if err := server.Eth.Bind(transport.Hello{}, sink); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	out, err := client.Eth.Dial(ctx, server.Shared.Addr(), server.Shared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	lib, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lib.CloseWithError(0, "done")
	sinkTake(t, sink.admissions)
	stream, err := out.OpenUniStream(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	close(gate)
	_, _, err = out.AcceptUniStream(ctx)
	closed, ok := errors.AsType[*transport.ViewClosedError](err)
	if !ok || closed.Code != wire.Refused || !closed.Remote {
		t.Fatalf("rejection = %v", err)
	}
	_ = stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = stream.Write(make([]byte, 16<<20))
	reset, ok := errors.AsType[*quic.StreamError](err)
	if !ok || !reset.Remote || reset.ErrorCode != 20 {
		t.Fatalf("queued reset = %v", err)
	}
	select {
	case <-sink.streams:
		t.Fatal("rejected stream delivered")
	case <-sink.closed:
		t.Fatal("rejected view reported Closed")
	default:
	}
}

func TestSinkDialAdmission(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(map[bool]string{false: "accept", true: "reject"}[reject], func(t *testing.T) {
			client, server := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
			sink := newRecordingSink()
			sink.admit = func(transport.Conn, transport.Hello) (wire.Code, bool) {
				return wire.NoSharedProtocols, !reject
			}
			if err := client.Eth.Bind(transport.Hello{}, sink); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			accepted := make(chan error, 1)
			go func() { _, err := server.Eth.Accept(ctx); accepted <- err }()
			conn, err := client.Eth.Dial(ctx, server.Shared.Addr(), server.Shared.PeerID())
			if sink.count.Load() != 1 {
				t.Fatal("Dial returned before Admit")
			}
			if reject {
				closed, ok := errors.AsType[*transport.ViewClosedError](err)
				if !ok || closed.Code != wire.NoSharedProtocols || closed.Remote {
					t.Fatalf("Dial = %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				_ = conn.Close()
				sinkTake(t, sink.closed)
			}
			if err := <-accepted; err != nil {
				t.Fatal(err)
			}
		})
	}
	client, server := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	sink := newRecordingSink()
	if err := client.Eth.Bind(transport.Hello{}, sink); err != nil {
		t.Fatal(err)
	}
	// The unstarted endpoint cannot supply Hello before this deadline.
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := client.Eth.Dial(ctx, server.Shared.Addr(), server.Shared.PeerID()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if sink.count.Load() != 0 {
		t.Fatal("cancelled Dial admitted")
	}
}
