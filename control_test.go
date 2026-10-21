package ethp2p

import (
	"bytes"
	"errors"
	"io"
	"math"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/control"
	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/proto"
)

// Control frame types and the MAX_CONTROL_FRAME payload limit, mirrored
// from package control so the raw peer below can speak the protocol by hand.
const (
	helloFrame  wire.FrameType = 0
	goAwayFrame wire.FrameType = 1
)

const maxControlFrame = 16 << 10

// rawControlPeer speaks the control protocol by hand through quic-go against a
// stack serving protocol 1. The stack's endpoint keeps a
// libp2p view, so releasing the ethp2p view does not close the QUIC connection
// before the raw peer reads GOAWAY.
type rawControlPeer struct {
	stack        *Stack
	prot         *Protocol
	wake         chan struct{}
	raw          *quic.Conn
	out          *quic.SendStream // our control stream, header written; nil without one
	stackControl *quic.ReceiveStream
	peer         *Peer // set once the stack admitted the view
}

func newRawControlPeer(t *testing.T) rawControlPeer {
	return startRawControlPeer(t, true, &quic.Config{}, true)
}

// startRawControlPeer dials a fresh stack with protocol 1 and no extra protocols.
// It reads the stack's HELLO and opens our control stream as requested.
func startRawControlPeer(t *testing.T, openControl bool, config *quic.Config, readHello bool) rawControlPeer {
	t.Helper()
	return startRawControlPeerWith(t, openControl, config, readHello, 0)
}

// startRawControlPeerWith dials a fresh stack. It reads the stack's header and
// HELLO when readHello is set, and opens our control stream with its header
// when openControl is set; it sends no HELLO. Extra protocols use codes above
// the reserved protocol 19, so a large HELLO can exceed quic-go's write buffer
// without changing the advertised receive window or making the HELLO invalid.
func startRawControlPeerWith(t *testing.T, openControl bool, config *quic.Config, readHello bool, extraProtocols int) rawControlPeer {
	t.Helper()
	ctx := transporttest.Context(t)
	server := transporttest.NewEndpoint(t)
	keepLibp2p(t, server)
	s := newTestStack(t, server)
	prot, wake := registerTestProtocol(t, s, 1)
	for i := range extraProtocols {
		registerTestProtocol(t, s, wire.Protocol(20+i))
	}
	startTestStack(t, s)
	raw, err := transporttest.DialRaw(t, ctx, server.Endpoint.Addr(), server.Endpoint.PeerID(), config)
	if err != nil {
		t.Fatal(err)
	}
	in, err := raw.AcceptUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if in.StreamID() != 3 {
		t.Fatalf("control stream ID = %d, want first server uni stream 3", in.StreamID())
	}
	if readHello {
		if protocol, err := wire.ReadHeader(in); err != nil || protocol != control.ControlProtocol {
			t.Fatalf("stack control protocol = %d, %v", protocol, err)
		}
		if message, err := readTestFrame(in, helloFrame, new(control.Hello)); err != nil {
			t.Fatalf("stack HELLO = %v, %v", message, err)
		}
	}
	var out *quic.SendStream
	if openControl {
		out, err = raw.OpenUniStream()
		if err != nil {
			t.Fatal(err)
		}
		if err := wire.WriteHeader(out, control.ControlProtocol); err != nil {
			t.Fatal(err)
		}
	}
	return rawControlPeer{stack: s, prot: prot, wake: wake, raw: raw, out: out, stackControl: in}
}

// newHelloedRawControlPeer sends a valid HELLO for protocol 1 and returns once
// the stack admitted the view.
func newHelloedRawControlPeer(t *testing.T) rawControlPeer {
	t.Helper()
	p := newRawControlPeer(t)
	p.write(t, helloFrame, helloMsg(1))
	p.peer = awaitPeer(t, p.prot, p.wake)
	return p
}

func (p rawControlPeer) write(t *testing.T, typ wire.FrameType, message proto.Message) {
	t.Helper()
	if _, err := p.out.Write(controlWire(t, typ, message)); err != nil {
		t.Fatal(err)
	}
}

func (p rawControlPeer) writeViolation(t *testing.T, raw []byte) {
	t.Helper()
	// The stack can reject the frame's length prefix before Write has
	// buffered its body. Either a completed write or that exact remote reset
	// is expected; GOAWAY and the view's end are checked separately.
	_, err := p.out.Write(raw)
	if err != nil {
		reset, ok := errors.AsType[*quic.StreamError](err)
		if !ok || !reset.Remote || uint64(reset.ErrorCode) != wire.ControlViolation.Wire() {
			t.Fatalf("invalid frame write = %v, want remote wire 22", err)
		}
	}
}

func (p rawControlPeer) assertNoEvent(t *testing.T) {
	t.Helper()
	if ev, ok := p.prot.Next(); ok {
		t.Fatalf("unadmitted view produced %+v", ev)
	}
}

func (p rawControlPeer) awaitDown(t *testing.T, code wire.Code) {
	t.Helper()
	down := awaitEvent(t, p.prot, p.wake)
	if down.Kind != PeerDown || down.Peer != p.peer || down.Code != code {
		t.Fatalf("down = %+v, want PeerDown with %s", down, code)
	}
}

func helloMsg(protocols ...uint32) *control.Hello {
	return &control.Hello{Protocols: protocols}
}

func goAwayMsg(code wire.Code) *control.GoAway {
	return &control.GoAway{Code: code.Wire() >> 1}
}

func controlWire(t *testing.T, typ wire.FrameType, message proto.Message) []byte {
	t.Helper()
	raw, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return wire.AppendFrame(nil, typ, raw)
}

func assertRawGoAway(t *testing.T, in *quic.ReceiveStream, code wire.Code) {
	t.Helper()
	_ = in.SetReadDeadline(time.Now().Add(time.Second))
	message, err := readTestFrame(in, goAwayFrame, new(control.GoAway))
	if err != nil || message.Code != code.Wire()>>1 {
		t.Fatalf("GOAWAY = %v, %v; want %s", message, err, code)
	}
	if _, _, err := wire.ReadFrame(in, maxControlFrame); !errors.Is(err, io.EOF) {
		t.Fatalf("after GOAWAY = %v, want FIN", err)
	}
}

func assertRawReadCancel(t *testing.T, out *quic.SendStream, code wire.Code) {
	t.Helper()
	assertRawReadCancelWithin(t, out, code, time.Second)
}

func assertRawReadCancelWithin(t *testing.T, out *quic.SendStream, code wire.Code, d time.Duration) {
	t.Helper()
	_ = out.SetWriteDeadline(time.Now().Add(d))
	_, err := out.Write(bytes.Repeat([]byte{0x42}, 1<<20))
	reset, ok := errors.AsType[*quic.StreamError](err)
	if !ok || !reset.Remote || uint64(reset.ErrorCode) != code.Wire() {
		t.Fatalf("control read cancellation = %v, want remote wire %d", err, code.Wire())
	}
}

func shortenHelloTimeout(t *testing.T, timeout time.Duration) {
	t.Helper()
	pendingHelloTimeout = timeout
}

func TestControlViolations(t *testing.T) {
	tooMany := make([]uint32, control.MaxProtocols+1)
	for i := range tooMany {
		tooMany[i] = uint32(i + 20)
	}
	tests := []struct {
		name    string
		helloed bool
		wire    []byte
		send    func(*testing.T, rawControlPeer)
	}{
		{name: "descending", wire: controlWire(t, helloFrame, helloMsg(2, 1))},
		{name: "control protocol", wire: controlWire(t, helloFrame, helloMsg(0))},
		{name: "reserved protocol", wire: controlWire(t, helloFrame, helloMsg(19))},
		{name: "too many", wire: controlWire(t, helloFrame, helloMsg(tooMany...))},
		{name: "oversize", wire: wire.AppendFrame(nil, helloFrame, bytes.Repeat([]byte{0}, maxControlFrame+1))},
		{name: "malformed protobuf", wire: wire.AppendFrame(nil, helloFrame, []byte{0xff})},
		{name: "unknown field", wire: wire.AppendFrame(nil, helloFrame, []byte{0x1a, 0})},
		{name: "GOAWAY first", wire: controlWire(t, goAwayFrame, goAwayMsg(wire.Refused))},
		{name: "unknown type before HELLO", wire: wire.AppendFrame(nil, 2, nil)},
		{name: "non-minimal type", wire: []byte{0x80, 0x00, 0x00}},
		{
			name:    "second HELLO",
			helloed: true,
			send: func(t *testing.T, p rawControlPeer) {
				p.write(t, helloFrame, helloMsg(1))
			},
		},
		{
			name:    "second control",
			helloed: true,
			send: func(t *testing.T, p rawControlPeer) {
				other, err := p.raw.OpenUniStream()
				if err != nil {
					t.Fatal(err)
				}
				if err := wire.WriteHeader(other, control.ControlProtocol); err != nil {
					t.Fatal(err)
				}
				assertRawReadCancel(t, other, wire.ControlViolation)
			},
		},
		{
			name:    "bidirectional control",
			helloed: true,
			send: func(t *testing.T, p rawControlPeer) {
				stream, err := p.raw.OpenStreamSync(transporttest.Context(t))
				if err != nil {
					t.Fatal(err)
				}
				if err := wire.WriteHeader(stream, control.ControlProtocol); err != nil {
					t.Fatal(err)
				}
				_ = stream.SetReadDeadline(time.Now().Add(time.Second))
				_, err = stream.Read(make([]byte, 1))
				reset, ok := errors.AsType[*quic.StreamError](err)
				if !ok || !reset.Remote || uint64(reset.ErrorCode) != wire.ControlViolation.Wire() {
					t.Fatalf("bidi reset = %v, want remote wire 22", err)
				}
				_ = stream.SetWriteDeadline(time.Now().Add(time.Second))
				_, err = stream.Write(make([]byte, 1<<20))
				reset, ok = errors.AsType[*quic.StreamError](err)
				if !ok || !reset.Remote || uint64(reset.ErrorCode) != wire.ControlViolation.Wire() {
					t.Fatalf("bidi read cancellation = %v, want remote wire 22", err)
				}
			},
		},
		{name: "unknown GOAWAY field", helloed: true, wire: wire.AppendFrame(nil, goAwayFrame, []byte{0x1a, 0})},
		{name: "unknown type after HELLO", helloed: true, wire: wire.AppendFrame(nil, 2, nil)},
		{name: "malformed GOAWAY", helloed: true, wire: wire.AppendFrame(nil, goAwayFrame, []byte{0xff})},
		{name: "oversize GOAWAY", helloed: true, wire: wire.AppendFrame(nil, goAwayFrame, make([]byte, maxControlFrame+1))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var p rawControlPeer
			if test.helloed {
				p = newHelloedRawControlPeer(t)
			} else {
				p = newRawControlPeer(t)
			}
			if test.send != nil {
				test.send(t, p)
			} else {
				p.writeViolation(t, test.wire)
			}
			assertRawGoAway(t, p.stackControl, wire.ControlViolation)
			assertRawReadCancel(t, p.out, wire.ControlViolation)
			if test.helloed {
				p.awaitDown(t, wire.ControlViolation)
			} else {
				p.assertNoEvent(t)
			}
		})
	}
}

func TestControlTimeoutAndPeerClosure(t *testing.T) {
	for _, test := range []string{"no stream", "empty stream"} {
		t.Run(test, func(t *testing.T) {
			shortenHelloTimeout(t, 100*time.Millisecond)
			p := startRawControlPeer(t, test == "empty stream", &quic.Config{}, true)
			assertRawGoAway(t, p.stackControl, wire.ControlViolation)
			if p.out != nil {
				assertRawReadCancel(t, p.out, wire.ControlViolation)
			}
			p.assertNoEvent(t)
		})
	}
	for _, test := range []struct {
		name   string
		end    func(rawControlPeer)
		want   wire.Code
		remote bool
	}{
		{"GOAWAY", func(p rawControlPeer) { p.write(t, goAwayFrame, goAwayMsg(wire.Refused)) }, wire.Refused, true},
		{"unknown GOAWAY", func(p rawControlPeer) {
			p.write(t, goAwayFrame, &control.GoAway{Code: math.MaxUint64})
		}, wire.Unspecified, true},
		{"FIN", func(p rawControlPeer) { _ = p.out.Close() }, wire.Unspecified, true},
		{"stack reset", func(p rawControlPeer) { p.out.CancelWrite(quic.StreamErrorCode(wire.Duplicate.Wire())) }, wire.Duplicate, true},
		{"protocol reset", func(p rawControlPeer) { p.out.CancelWrite(quic.StreamErrorCode(23)) }, wire.ControlViolation, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := newHelloedRawControlPeer(t)
			test.end(p)
			if test.remote {
				// A peer's closure gets FIN only, no GOAWAY.
				_ = p.stackControl.SetReadDeadline(time.Now().Add(time.Second))
				if _, _, err := wire.ReadFrame(p.stackControl, maxControlFrame); !errors.Is(err, io.EOF) {
					t.Fatalf("peer closure reply = %v, want FIN", err)
				}
			} else {
				assertRawGoAway(t, p.stackControl, wire.ControlViolation)
			}
			p.awaitDown(t, test.want)
		})
	}
}

func TestControlHelloTimeoutWhileOutboundWriteBlocked(t *testing.T) {
	shortenHelloTimeout(t, 100*time.Millisecond)
	// The peer reads nothing and sends no HELLO. The small outbound HELLO
	// can be buffered, but the close must still end within HELLO_TIMEOUT
	// plus GOAWAY_TIMEOUT.
	p := startRawControlPeer(t, true, &quic.Config{InitialStreamReceiveWindow: 1, MaxStreamReceiveWindow: 1}, false)
	assertRawReadCancelWithin(t, p.out, wire.ControlViolation, 2*time.Second)
	p.assertNoEvent(t)
}

func TestControlHelloWriteFailureClosesView(t *testing.T) {
	// A HELLO spanning several packets cannot fit in quic-go's write buffer
	// while the peer's one-byte window stays exhausted. STOP_SENDING must
	// therefore fail the pending write and release the view before its deadline.
	p := startRawControlPeerWith(t, true, &quic.Config{InitialStreamReceiveWindow: 1, MaxStreamReceiveWindow: 1}, false, 1023)
	p.stackControl.CancelRead(streamCode(wire.Refused))
	assertRawReadCancel(t, p.out, wire.Closing)
	p.assertNoEvent(t)
}

func TestControlHelloWriteTimeout(t *testing.T) {
	shortenHelloTimeout(t, 100*time.Millisecond)
	p := startRawControlPeerWith(t, true, &quic.Config{InitialStreamReceiveWindow: 1, MaxStreamReceiveWindow: 1}, false, 1023)
	// Reading only after release keeps the peer's one-byte window exhausted
	// for the whole write and prevents a read from completing the HELLO.
	assertRawReadCancelWithin(t, p.out, wire.Closing, 2*time.Second)
	_ = p.stackControl.SetReadDeadline(time.Now().Add(time.Second))
	_, err := io.ReadAll(p.stackControl)
	reset, ok := errors.AsType[*quic.StreamError](err)
	if !ok || !reset.Remote || uint64(reset.ErrorCode) != wire.Timeout.Wire() {
		t.Fatalf("HELLO write reset = %v, want remote wire %d", err, wire.Timeout.Wire())
	}
	p.assertNoEvent(t)
}

func TestControlEarlyStreamWaitsForHello(t *testing.T) {
	p := newRawControlPeer(t)
	ctx := transporttest.Context(t)
	bi, err := p.raw.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.WriteHeader(bi, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := bi.Write([]byte("early bidi")); err != nil {
		t.Fatal(err)
	}
	_ = bi.Close()
	out, err := p.raw.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.WriteHeader(out, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := out.Write([]byte("before HELLO")); err != nil {
		t.Fatal(err)
	}
	_ = out.Close()
	// The view is not admitted before HELLO arrives.
	select {
	case <-p.wake:
		ev, _ := p.prot.Next()
		t.Fatalf("event before HELLO: %+v", ev)
	case <-time.After(30 * time.Millisecond):
	}
	p.write(t, helloFrame, helloMsg(1))
	awaitPeer(t, p.prot, p.wake)
	// Both early streams are routed after admission; their relative order is
	// header-read completion order, so accept the set.
	got := make(map[bool]string)
	for range 2 {
		ev := awaitStreamEvent(t, p.prot, p.wake)
		payload, err := io.ReadAll(ev.Stream)
		if err != nil {
			t.Fatal(err)
		}
		_, bidi := ev.Stream.(Stream)
		got[bidi] = string(payload)
	}
	if got[false] != "before HELLO" || got[true] != "early bidi" {
		t.Fatalf("early streams = %v", got)
	}
}

// Disconnect sends GOAWAY with Closing and cancels reading the peer's control
// stream with Closing; only a violation cancels it with ControlViolation.
func TestDisconnectSendsGoAway(t *testing.T) {
	p := newHelloedRawControlPeer(t)
	disconnectTest(t, p.stack, p.peer.ID())
	assertRawGoAway(t, p.stackControl, wire.Closing)
	assertRawReadCancel(t, p.out, wire.Closing)
	p.awaitDown(t, wire.Closing)
}

func readTestFrame[M proto.Message](in *quic.ReceiveStream, want wire.FrameType, message M) (M, error) {
	typ, raw, err := wire.ReadFrame(in, maxControlFrame)
	if err != nil {
		return message, err
	}
	if typ != want {
		return message, errors.New("unexpected control frame type")
	}
	return message, proto.Unmarshal(raw, message)
}
