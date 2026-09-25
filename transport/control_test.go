package transport_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/pb"
	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"
	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/proto"
)

// rawControlPeer sends its control frames directly through quic-go. The server
// keeps a libp2p view, so releasing its ethp2p view does not close this QUIC
// connection before the raw peer can read GoAway.
type rawControlPeer struct {
	server *transport.Conn
	shared *transport.SharedTransport
	raw    *quic.Conn
	out    *quic.SendStream
	in     *quic.ReceiveStream
}

type rawAccept struct {
	conn *transport.Conn
	err  error
}

func newRawControlPeer(t *testing.T) rawControlPeer {
	return newRawControlPeerWithStream(t, true)
}

func newRawControlPeerWithStream(t *testing.T, openControl bool) rawControlPeer {
	return newRawControlPeerConfigured(t, openControl, transport.Hello{}, &quic.Config{EnableDatagrams: true}, true)
}

// newRawControlPeerConfigured opens the raw side without sending Hello, so the
// server view is never delivered. Tests assert its local cause through the
// libp2p sibling view with assertSiblingCause.
func newRawControlPeerConfigured(t *testing.T, openControl bool, hello transport.Hello, config *quic.Config, readHello bool) rawControlPeer {
	p, _ := startRawControlPeer(t, openControl, hello, config, readHello)
	return p
}

// newHelloedRawControlPeer sends a valid Hello during setup and returns after
// the server accepted the view.
func newHelloedRawControlPeer(t *testing.T) rawControlPeer {
	p, accepted := startRawControlPeer(t, true, transport.Hello{}, &quic.Config{EnableDatagrams: true}, true)
	p.write(t, pbHello(1))
	result := <-accepted
	if result.err != nil {
		t.Fatal(result.err)
	}
	p.server = result.conn
	return p
}

func startRawControlPeer(t *testing.T, openControl bool, hello transport.Hello, config *quic.Config, readHello bool) (rawControlPeer, <-chan rawAccept) {
	t.Helper()
	ctx := controlContext(t)
	serverShared, _, serverEth, packet := controlEndpoint(t)
	serverShared.Libp2p()
	if err := serverEth.SetHello(hello); err != nil {
		t.Fatal(err)
	}
	accepted := make(chan rawAccept, 1)
	go func() {
		conn, err := serverEth.Accept(ctx)
		accepted <- rawAccept{conn: conn, err: err}
	}()
	raw, err := dialRawControl(t, ctx, packet.LocalAddr(), serverShared.PeerID(), config)
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
		if selector, err := wire.ReadSelector(in); err != nil || selector != 0 {
			t.Fatalf("server control selector = %d, %v", selector, err)
		}
		if message, err := readControlFrame(in); err != nil || message.GetHello() == nil {
			t.Fatalf("server transport.Hello = %v, %v", message, err)
		}
	}
	var out *quic.SendStream
	if openControl {
		out, err = raw.OpenUniStream()
		if err != nil {
			t.Fatal(err)
		}
		if err := wire.WriteSelector(out, wire.ControlSelector); err != nil {
			t.Fatal(err)
		}
	}
	return rawControlPeer{shared: serverShared, raw: raw, out: out, in: in}, accepted
}

// assertSiblingCause checks the local cause of the server view, which is never
// delivered to Accept without a valid Hello, through its surviving libp2p
// sibling.
func (p rawControlPeer) assertSiblingCause(t *testing.T, code wire.Code, remote bool) {
	t.Helper()
	listener, err := p.shared.Libp2p().Listen(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	libView, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	transport.AssertEthp2pViewClosed(t, libView, code, remote)
}

func (p rawControlPeer) write(t *testing.T, message *pb.Control) {
	t.Helper()
	raw, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	p.writeBytes(t, wire.AppendFrame(nil, raw))
}

func (p rawControlPeer) writeBytes(t *testing.T, raw []byte) {
	t.Helper()
	if _, err := p.out.Write(raw); err != nil {
		t.Fatal(err)
	}
}

func (p rawControlPeer) writeViolation(t *testing.T, raw []byte) {
	t.Helper()
	// The receiver can reject the frame's length prefix before Write has
	// buffered its body. Either a completed write or that exact remote reset
	// is expected; GoAway and the view cause are checked separately.
	_, err := p.out.Write(raw)
	if err != nil {
		reset, ok := errors.AsType[*quic.StreamError](err)
		if !ok || !reset.Remote || uint64(reset.ErrorCode) != 22 {
			t.Fatalf("invalid frame write = %v, want remote wire 22", err)
		}
	}
}

func pbHello(selectors ...uint64) *pb.Control {
	return &pb.Control{Message: &pb.Control_Hello{Hello: &pb.Hello{Selectors: selectors}}}
}

func pbGoAway(code wire.Code) *pb.Control {
	return &pb.Control{Message: &pb.Control_GoAway{GoAway: &pb.GoAway{Code: code.Wire() >> 1}}}
}

func assertViewCause(t *testing.T, err error, code wire.Code, remote bool) {
	t.Helper()
	closed, ok := errors.AsType[*transport.ViewClosedError](err)
	if !ok || closed.Code != code || closed.Remote != remote || !errors.Is(err, transport.ErrViewClosed) {
		t.Fatalf("view cause = %v, want {%s %t}", err, code, remote)
	}
}

func waitViewCause(t *testing.T, conn *transport.Conn, code wire.Code, remote bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	select {
	case <-conn.Done():
	case <-ctx.Done():
		t.Fatalf("view did not close: %v", ctx.Err())
	}
	if got := conn.CloseCode(); got != code {
		t.Fatalf("CloseCode = %s, want %s", got, code)
	}
	// OpenStream preserves the closure cause, including its remote origin.
	_, err := conn.OpenStream(ctx, 1)
	assertViewCause(t, err, code, remote)
}

func assertRawGoAway(t *testing.T, in *quic.ReceiveStream, code wire.Code) {
	t.Helper()
	_ = in.SetReadDeadline(time.Now().Add(time.Second))
	message, err := readControlFrame(in)
	if err != nil || message.GetGoAway() == nil || message.GetGoAway().Code != code.Wire()>>1 {
		t.Fatalf("GoAway = %v, %v; want %s", message, err, code)
	}
	if _, err := wire.ReadFrame(in, maxControlFrame); !errors.Is(err, io.EOF) {
		t.Fatalf("after GoAway = %v, want FIN", err)
	}
}

func assertRawReadCancel(t *testing.T, out *quic.SendStream, code wire.Code) {
	t.Helper()
	_ = out.SetWriteDeadline(time.Now().Add(time.Second))
	_, err := out.Write(bytes.Repeat([]byte{0x42}, 1<<20))
	reset, ok := errors.AsType[*quic.StreamError](err)
	if !ok || !reset.Remote || uint64(reset.ErrorCode) != code.Wire() {
		t.Fatalf("control read cancellation = %v, want remote wire %d", err, code.Wire())
	}
}

func TestControlHelloExchangeAndSnapshot(t *testing.T) {
	_, _, clientEth, _ := controlEndpoint(t)
	serverShared, _, serverEth, serverPC := controlEndpoint(t)
	clientHello := transport.Hello{Selectors: []wire.Selector{1, 3}, Record: []byte{1, 2, 3}}
	serverHello := transport.Hello{Selectors: []wire.Selector{2, 4}, Record: []byte{4, 5}}
	if err := clientEth.SetHello(clientHello); err != nil {
		t.Fatal(err)
	}
	if err := serverEth.SetHello(serverHello); err != nil {
		t.Fatal(err)
	}
	clientHello.Selectors[0], clientHello.Record[0] = 99, 99
	serverHello.Selectors[0], serverHello.Record[0] = 99, 99
	ctx := controlContext(t)
	accepted := make(chan *transport.Conn, 1)
	go func() { conn, _ := serverEth.Accept(ctx); accepted <- conn }()
	client, err := clientEth.Dial(ctx, serverPC.LocalAddr(), serverShared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	server := <-accepted
	if server == nil {
		t.Fatal("server did not accept")
	}
	if err := clientEth.SetHello(transport.Hello{Selectors: []wire.Selector{9}}); err != nil {
		t.Fatal(err)
	}
	if err := serverEth.SetHello(transport.Hello{Selectors: []wire.Selector{10}}); err != nil {
		t.Fatal(err)
	}
	gotClient := client.PeerHello()
	gotServer := server.PeerHello()
	if !slices.Equal(gotClient.Selectors, []wire.Selector{2, 4}) || !bytes.Equal(gotClient.Record, []byte{4, 5}) ||
		!slices.Equal(gotServer.Selectors, []wire.Selector{1, 3}) || !bytes.Equal(gotServer.Record, []byte{1, 2, 3}) {
		t.Fatalf("exchanged transport.Hello = client %+v, server %+v", gotClient, gotServer)
	}
	gotClient.Record[0], gotClient.Selectors[0] = 99, 99
	again := client.PeerHello()
	if !bytes.Equal(again.Record, []byte{4, 5}) || !slices.Equal(again.Selectors, []wire.Selector{2, 4}) {
		t.Fatalf("PeerHello reused caller slices: %+v", again)
	}
	go func() { conn, _ := serverEth.Accept(ctx); accepted <- conn }()
	nextClient, err := clientEth.Dial(ctx, serverPC.LocalAddr(), serverShared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	nextServer := <-accepted
	if nextServer == nil {
		t.Fatal("server did not accept the next view")
	}
	nextFromServer := nextClient.PeerHello()
	nextFromClient := nextServer.PeerHello()
	if !slices.Equal(nextFromServer.Selectors, []wire.Selector{10}) || !slices.Equal(nextFromClient.Selectors, []wire.Selector{9}) {
		t.Fatalf("next view transport.Hello = client %+v, server %+v", nextFromServer, nextFromClient)
	}
	_ = nextClient.Close()
	_ = nextServer.Close()
	_ = client.Close()
	_ = server.Close()
}

func TestControlViolations(t *testing.T) {
	tooMany := make([]uint64, wire.MaxSelectors+1)
	for i := range tooMany {
		tooMany[i] = uint64(i + 1)
	}
	tests := []struct {
		name string
		wire []byte
	}{
		{"descending", controlWire(t, pbHello(2, 1))},
		{"zero", controlWire(t, pbHello(0))},
		{"too many", controlWire(t, pbHello(tooMany...))},
		{"oversize", wire.AppendFrame(nil, bytes.Repeat([]byte{0}, maxControlFrame+1))},
		{"malformed protobuf", wire.AppendFrame(nil, []byte{0xff})},
		{"first GoAway", controlWire(t, pbGoAway(wire.Refused))},
		{"first unset", controlWire(t, &pb.Control{})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := newRawControlPeer(t)
			p.writeViolation(t, test.wire)
			assertRawGoAway(t, p.in, wire.ControlViolation)
			assertRawReadCancel(t, p.out, wire.ControlViolation)
			// No valid Hello arrives, so the view is never delivered to
			// Accept; its local cause is visible through the libp2p sibling.
			p.assertSiblingCause(t, wire.ControlViolation, false)
		})
	}
	for _, test := range []string{"second transport.Hello", "second control", "bidirectional control"} {
		t.Run(test, func(t *testing.T) {
			// The setup Hello is the first frame; the test sends the violation.
			p := newHelloedRawControlPeer(t)
			switch test {
			case "second transport.Hello":
				p.write(t, pbHello(1))
			case "second control":
				second, err := p.raw.OpenUniStream()
				if err != nil {
					t.Fatal(err)
				}
				if err := wire.WriteSelector(second, 0); err != nil {
					t.Fatal(err)
				}
				_ = second.SetWriteDeadline(time.Now().Add(time.Second))
				_, err = second.Write(bytes.Repeat([]byte{0xa5}, 1<<20))
				reset, ok := errors.AsType[*quic.StreamError](err)
				if !ok || !reset.Remote || uint64(reset.ErrorCode) != wire.ControlViolation.Wire() {
					t.Fatalf("second control reset = %v, want remote wire 22", err)
				}
			case "bidirectional control":
				stream, err := p.raw.OpenStreamSync(controlContext(t))
				if err != nil {
					t.Fatal(err)
				}
				if err := wire.WriteSelector(stream, 0); err != nil {
					t.Fatal(err)
				}
				var one [1]byte
				_, err = stream.Read(one[:])
				reset, ok := errors.AsType[*quic.StreamError](err)
				if !ok || !reset.Remote || uint64(reset.ErrorCode) != wire.ControlViolation.Wire() {
					t.Fatalf("bidi reset = %v, want remote wire 22", err)
				}
				_ = stream.SetWriteDeadline(time.Now().Add(time.Second))
				_, err = stream.Write(make([]byte, 1<<20))
				reset, ok = errors.AsType[*quic.StreamError](err)
				if !ok || !reset.Remote || uint64(reset.ErrorCode) != 22 {
					t.Fatalf("bidi read cancellation = %v, want remote wire 22", err)
				}
			}
			assertRawGoAway(t, p.in, wire.ControlViolation)
			assertRawReadCancel(t, p.out, wire.ControlViolation)
			waitViewCause(t, p.server, wire.ControlViolation, false)
		})
	}
}

func controlWire(t *testing.T, message *pb.Control) []byte {
	t.Helper()
	raw, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return wire.AppendFrame(nil, raw)
}

func TestControlTimeoutAndPeerClosure(t *testing.T) {
	for _, test := range []string{"no stream", "empty stream"} {
		t.Run(test, func(t *testing.T) {
			transport.ShortenHelloTimeout(t, 100*time.Millisecond)
			p := newRawControlPeerWithStream(t, test == "empty stream")
			assertRawGoAway(t, p.in, wire.ControlViolation)
			if p.out != nil {
				assertRawReadCancel(t, p.out, wire.ControlViolation)
			}
			p.assertSiblingCause(t, wire.ControlViolation, false)
		})
	}
	for _, test := range []struct {
		name   string
		end    func(rawControlPeer)
		want   wire.Code
		remote bool
	}{
		{"GoAway", func(p rawControlPeer) { p.write(t, pbGoAway(wire.Refused)) }, wire.Refused, true},
		{"unknown GoAway", func(p rawControlPeer) {
			p.write(t, &pb.Control{Message: &pb.Control_GoAway{GoAway: &pb.GoAway{Code: math.MaxUint64}}})
		}, wire.Unspecified, true},
		{"FIN", func(p rawControlPeer) { _ = p.out.Close() }, wire.Unspecified, true},
		{"stack reset", func(p rawControlPeer) { p.out.CancelWrite(controlQuicCode(wire.Duplicate)) }, wire.Duplicate, true},
		{"protocol reset", func(p rawControlPeer) { p.out.CancelWrite(quic.StreamErrorCode(23)) }, wire.ControlViolation, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := newHelloedRawControlPeer(t)
			test.end(p)
			if test.remote {
				_ = p.in.SetReadDeadline(time.Now().Add(time.Second))
				if _, err := wire.ReadFrame(p.in, maxControlFrame); !errors.Is(err, io.EOF) {
					t.Fatalf("peer closure reply = %v, want FIN", err)
				}
			} else {
				assertRawGoAway(t, p.in, wire.ControlViolation)
			}
			waitViewCause(t, p.server, test.want, test.remote)
			if _, sel, ok := p.server.NextStream(); ok {
				t.Fatalf("NextStream succeeded after closure: %d", sel)
			}
			if _, err := p.server.OpenStream(controlContext(t), 1); err == nil {
				t.Fatal("OpenStream succeeded after closure")
			} else {
				assertViewCause(t, err, test.want, test.remote)
			}
		})
	}
}

func TestControlUnknownMessagesIgnored(t *testing.T) {
	p := newHelloedRawControlPeer(t)
	p.write(t, &pb.Control{})
	p.writeBytes(t, wire.AppendFrame(nil, []byte{0x1a, 0}))
	out, err := p.raw.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.WriteSelector(out, 1); err != nil {
		t.Fatal(err)
	}
	in, selector, err := transporttest.NextStream(controlContext(t), p.server)
	if err != nil || selector != 1 {
		t.Fatalf("stream after unknown control = %d, %v", selector, err)
	}
	in.CancelRead(0)
	// A later control frame proves the reader processed both ignored messages.
	p.write(t, pbGoAway(wire.Refused))
	waitViewCause(t, p.server, wire.Refused, true)
}

func TestControlRejectsInvalidLaterFrame(t *testing.T) {
	for _, raw := range [][]byte{wire.AppendFrame(nil, []byte{0xff}), wire.AppendFrame(nil, make([]byte, maxControlFrame+1))} {
		p := newHelloedRawControlPeer(t)
		p.writeViolation(t, raw)
		assertRawGoAway(t, p.in, wire.ControlViolation)
		assertRawReadCancel(t, p.out, wire.ControlViolation)
		waitViewCause(t, p.server, wire.ControlViolation, false)
	}
}

func TestControlImmediateCloseSendsHelloFirst(t *testing.T) {
	for range 30 {
		pair := controlViewPair(t)
		if err := pair.clientEth.CloseWithCode(wire.Duplicate); err != nil {
			t.Fatal(err)
		}
		waitViewCause(t, pair.clientEth, wire.Duplicate, false)
		waitViewCause(t, pair.serverEth, wire.Duplicate, true)
	}
}

func TestControlHelloTimeoutWhileOutboundWriteBlocked(t *testing.T) {
	transport.ShortenHelloTimeout(t, 100*time.Millisecond)
	// The peer consumes no bytes, and its receive window is smaller than Hello.
	// The inbound deadline must still run and release the view within the Hello
	// timeout plus the one-second close-write budget.
	p := newRawControlPeerConfigured(t, true,
		transport.Hello{Record: make([]byte, maxControlFrame-32)},
		&quic.Config{InitialStreamReceiveWindow: 1, MaxStreamReceiveWindow: 1}, false)
	// No valid Hello arrives, so the view is never delivered to Accept; its
	// local cause is visible through the libp2p sibling.
	p.assertSiblingCause(t, wire.ControlViolation, false)
	assertRawReadCancel(t, p.out, wire.ControlViolation)
}

func TestControlHelloWriteFailureClosesView(t *testing.T) {
	p := newRawControlPeerConfigured(t, true,
		transport.Hello{Record: make([]byte, maxControlFrame-32)},
		&quic.Config{InitialStreamReceiveWindow: 1, MaxStreamReceiveWindow: 1}, false)
	p.in.CancelRead(quic.StreamErrorCode(wire.Refused.Wire()))
	p.assertSiblingCause(t, wire.Unspecified, false)
	assertRawReadCancel(t, p.out, wire.Closing)
}

func TestControlOverloadDoesNotBlockOtherConnections(t *testing.T) {
	endpoint := transporttest.NewEndpoint(t)
	selectors := make([]wire.Selector, wire.MaxSelectors)
	for i := range selectors {
		selectors[i] = wire.Selector(1<<56) + wire.Selector(i)
	}
	if err := endpoint.Eth.SetHello(transport.Hello{Selectors: selectors}); err != nil {
		t.Fatal(err)
	}
	listener, err := endpoint.Shared.Libp2p().Listen(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	ctx := controlContext(t)
	dial := func(config *quic.Config) *quic.Conn {
		raw, err := dialRawControl(t, ctx, endpoint.Shared.Addr(), endpoint.Shared.PeerID(), config)
		if err != nil {
			t.Fatal(err)
		}
		out, err := raw.OpenUniStream()
		if err != nil {
			t.Fatal(err)
		}
		frames := wire.AppendFrame(nil, []byte{0})
		frames = append(frames, controlWire(t, pbHello(1))...)
		if _, err := out.Write(frames); err != nil {
			t.Fatal(err)
		}
		return raw
	}
	// Fill the ethp2p queue, draining libp2p and the outbound Hello so no
	// filler connection is flow-control blocked.
	for range 16 {
		raw := dial(&quic.Config{})
		in, err := raw.AcceptUniStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if selector, err := wire.ReadSelector(in); err != nil || selector != wire.ControlSelector {
			t.Fatalf("control selector = %d, %v", selector, err)
		}
		if message, err := readControlFrame(in); err != nil || message.GetHello() == nil {
			t.Fatalf("Hello = %v, %v", message, err)
		}
		if _, err := listener.Accept(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Delivery to ethQ follows Hello asynchronously, so wait for the queue to
	// fill before overloading it.
	waitPending := time.After(5 * time.Second)
	for transport.PendingEthp2p(endpoint.Shared) != 16 {
		select {
		case <-waitPending:
			t.Fatalf("queued views = %d, want 16", transport.PendingEthp2p(endpoint.Shared))
		default:
			time.Sleep(time.Millisecond)
		}
	}
	blocked := dial(&quic.Config{InitialStreamReceiveWindow: 1, MaxStreamReceiveWindow: 1})
	// The overloaded peer never reads its control stream. Both its libp2p
	// view and a new normal connection must be delivered before the one-second
	// GoAway deadline can expire.
	start := time.Now()
	normal := dial(&quic.Config{})
	delivery, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	first, err := listener.Accept(delivery)
	if err != nil {
		t.Fatal(err)
	}
	second, err := listener.Accept(delivery)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed >= 500*time.Millisecond {
		t.Fatalf("unrelated connection delayed %s by overload", elapsed)
	}
	blockedView, normalView := first, second
	if first.RemoteAddr().String() != blocked.LocalAddr().String() {
		blockedView, normalView = second, first
	}
	if blockedView.RemoteAddr().String() != blocked.LocalAddr().String() || normalView.RemoteAddr().String() != normal.LocalAddr().String() {
		t.Fatal("listener delivered unexpected peers")
	}
	transport.AssertEthp2pViewClosed(t, blockedView, wire.Unspecified, false)
	// Prove the rejected connection still carries libp2p traffic after the
	// ethp2p release, without reading its blocked outbound control stream.
	out, err := blocked.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := wire.AppendFrame(nil, []byte("/multistream/1.0.0\n"))
	if _, err := out.Write(want); err != nil {
		t.Fatal(err)
	}
	in, err := blockedView.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(in, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("surviving libp2p read = %x, %v", got, err)
	}
	if _, err := in.Write(got); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(out, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("surviving libp2p reply = %x, %v", got, err)
	}
}

func TestControlEarlyStreamWaitsForHello(t *testing.T) {
	p, accepted := startRawControlPeer(t, true, transport.Hello{}, &quic.Config{EnableDatagrams: true}, true)
	bi, err := p.raw.OpenStreamSync(controlContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.WriteSelector(bi, 2); err != nil {
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
	if err := wire.WriteSelector(out, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := out.Write([]byte("before transport.Hello")); err != nil {
		t.Fatal(err)
	}
	_ = out.Close()
	// The view is not delivered before Hello arrives.
	short, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := p.shared.Ethp2p().Accept(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pre-transport.Hello view accepted: %v", err)
	}
	p.write(t, pbHello(1))
	result := <-accepted
	if result.err != nil {
		t.Fatal(result.err)
	}
	server := result.conn
	// Both early streams were classified before Hello; their relative order
	// is completion order, so accept the set.
	got := make(map[wire.Selector]string)
	for range 2 {
		s, sel, err := transporttest.NextStream(controlContext(t), server)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := io.ReadAll(s)
		if err != nil {
			t.Fatal(err)
		}
		got[sel] = string(payload)
	}
	if got[1] != "before transport.Hello" || got[2] != "early bidi" {
		t.Fatalf("early streams = %v", got)
	}
}

func TestCloseWithCodeSendsGoAwayAndIsIdempotent(t *testing.T) {
	for _, test := range []struct {
		name  string
		code  wire.Code
		close func(*transport.Conn) error
	}{
		{"Close", wire.Closing, func(c *transport.Conn) error { return c.Close() }},
		{"CloseWithCode", wire.Duplicate, func(c *transport.Conn) error { return c.CloseWithCode(wire.Duplicate) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			pair := controlViewPair(t)
			// Accept and Dial return only after Hello, so no wait is needed here.
			if err := test.close(pair.clientEth); err != nil {
				t.Fatal(err)
			}
			waitViewCause(t, pair.clientEth, test.code, false)
			waitViewCause(t, pair.serverEth, test.code, true)
			if err := pair.clientEth.CloseWithCode(wire.Refused); err != nil {
				t.Fatal(err)
			}
			waitViewCause(t, pair.clientEth, test.code, false)
		})
	}
}

func TestRawConnectionClosureKeepsQuicCause(t *testing.T) {
	p := newHelloedRawControlPeer(t)
	if err := p.raw.CloseWithError(99, "raw close"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	select {
	case <-p.server.Done():
	case <-ctx.Done():
		t.Fatal("raw connection closure did not stop view")
	}
	_, err := p.server.OpenStream(ctx, 1)
	if app, ok := errors.AsType[*quic.ApplicationError](err); !ok || app.ErrorCode != 99 || !app.Remote {
		t.Fatalf("raw connection closure = %v, want quic ApplicationError", err)
	}
	if errors.Is(err, transport.ErrViewClosed) {
		t.Fatalf("raw connection closure mapped to view closure: %v", err)
	}
}

func TestSetHelloAndDialPreconditions(t *testing.T) {
	packet := controlPacket(t)
	shared, err := transport.NewShared(controlKey(t), packet, transport.Interop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shared.Close() })
	eth := shared.Ethp2p()
	if _, err := eth.Dial(controlContext(t), packet.LocalAddr(), ""); !errors.Is(err, transport.ErrNoHello) {
		t.Fatalf("Dial before SetHello = %v", err)
	}
	for _, selectors := range [][]wire.Selector{{2, 1}, {0}, make([]wire.Selector, wire.MaxSelectors+1)} {
		if err := eth.SetHello(transport.Hello{Selectors: selectors}); err == nil {
			t.Fatalf("SetHello accepted %d invalid selectors", len(selectors))
		}
	}
	if err := eth.SetHello(transport.Hello{Record: bytes.Repeat([]byte{0}, maxControlFrame+1)}); !errors.Is(err, wire.ErrFrameTooLarge) {
		t.Fatalf("oversized transport.Hello = %v", err)
	}
	if _, err := eth.Dial(controlContext(t), packet.LocalAddr(), ""); !errors.Is(err, transport.ErrNoHello) {
		t.Fatalf("invalid Hello registered interest: %v", err)
	}
	if err := eth.SetHello(transport.Hello{Selectors: []wire.Selector{1}}); err != nil {
		t.Fatal(err)
	}
}

func TestStatelessResetAfterEndpointRestart(t *testing.T) {
	ctx := controlContext(t)
	key := controlKey(t)
	packet, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := packet.LocalAddr()
	server, err := transport.NewShared(key, packet, transport.Interop())
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Ethp2p().SetHello(transport.Hello{}); err != nil {
		t.Fatal(err)
	}
	_, _, clientEth, _ := controlEndpoint(t)
	accepted := make(chan *transport.Conn, 1)
	go func() { conn, _ := server.Ethp2p().Accept(ctx); accepted <- conn }()
	client, err := clientEth.Dial(ctx, addr, server.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	if <-accepted == nil {
		t.Fatal("server did not accept")
	}
	// Accept and Dial return only after Hello, so no wait is needed here.
	_ = client.PeerHello()
	if err := packet.Close(); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	restartedPacket, err := net.ListenPacket("udp4", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := transport.NewShared(key, restartedPacket, transport.Interop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close(); _ = restartedPacket.Close() })
	if err := restarted.Ethp2p().SetHello(transport.Hello{}); err != nil {
		t.Fatal(err)
	}
	acceptCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _, _ = restarted.Ethp2p().Accept(acceptCtx) }()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	closed := false
	for !closed {
		select {
		case <-ticker.C:
			_ = client.SendDatagram(ctx, bytes.Repeat([]byte{0x42}, 128))
			select {
			case <-client.Done():
				closed = true
			default:
			}
		case <-deadline.C:
			t.Fatal("stateless reset did not close the connection within 3s")
		}
	}
	_, err = client.OpenStream(ctx, 1)
	if _, ok := errors.AsType[*quic.StatelessResetError](err); !ok {
		t.Fatalf("connection ended with %v, want stateless reset", err)
	}
}

const maxControlFrame = 16 << 10

func controlContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func controlEndpoint(t *testing.T) (*transport.SharedTransport, *transporttest.Endpoint, *transport.Ethp2pTransport, net.PacketConn) {
	t.Helper()
	e := transporttest.NewEndpoint(t)
	e.Shared.Libp2p()
	return e.Shared, e, e.Eth, e.Packet
}

func controlViewPair(t *testing.T) struct {
	clientEth, serverEth *transport.Conn
	serverShared         *transport.SharedTransport
} {
	t.Helper()
	client, server := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	client.Shared.Libp2p()
	server.Shared.Libp2p()
	dialed, accepted := transporttest.Connect(t, client, server)
	return struct {
		clientEth, serverEth *transport.Conn
		serverShared         *transport.SharedTransport
	}{dialed, accepted, server.Shared}
}

func controlPacket(t *testing.T) net.PacketConn {
	t.Helper()
	p, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func controlKey(t *testing.T) *identity.PrivKey {
	t.Helper()
	k, err := identity.GenPrivKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// dialRawControl uses the public libp2p TLS identity format, with ethp2p ALPN,
// so malformed control traffic exercises the real authenticated dispatcher.
func dialRawControl(t *testing.T, ctx context.Context, addr net.Addr, remote transport.PeerID, config *quic.Config) (*quic.Conn, error) {
	t.Helper()
	key, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, -1)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := libp2ptls.NewIdentity(key)
	if err != nil {
		t.Fatal(err)
	}
	tlsConfig, _ := identity.ConfigForPeer(peer.ID(remote))
	tlsConfig.NextProtos = []string{transport.AlpnEthp2p}
	raw := &quic.Transport{Conn: controlPacket(t)}
	t.Cleanup(func() { _ = raw.Close() })
	return raw.Dial(ctx, addr, tlsConfig, config)
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

func controlQuicCode(code wire.Code) quic.StreamErrorCode {
	return quic.StreamErrorCode(code.Wire())
}
