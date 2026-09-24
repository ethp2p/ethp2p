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

	"github.com/ethp2p/ethp2p/protocol"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/pb"
	"github.com/ethp2p/ethp2p/transport/transporttest"
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
	server transport.Conn
	raw    *quic.Conn
	out    *quic.SendStream
	in     *quic.ReceiveStream
}

func newRawControlPeer(t *testing.T) rawControlPeer {
	return newRawControlPeerWithStream(t, true)
}

func newRawControlPeerWithStream(t *testing.T, openControl bool) rawControlPeer {
	return newRawControlPeerConfigured(t, openControl, transport.Hello{}, &quic.Config{EnableDatagrams: true}, true)
}

func newRawControlPeerConfigured(t *testing.T, openControl bool, hello transport.Hello, config *quic.Config, readHello bool) rawControlPeer {
	t.Helper()
	ctx := controlContext(t)
	serverShared, _, serverEth, packet := controlEndpoint(t)
	serverShared.Libp2p()
	if err := serverEth.SetHello(hello); err != nil {
		t.Fatal(err)
	}
	accepted := make(chan transport.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := serverEth.Accept(ctx)
		accepted <- conn
		acceptErr <- err
	}()
	raw, err := dialRawControl(t, ctx, packet.LocalAddr(), serverShared.PeerID(), config)
	if err != nil {
		t.Fatal(err)
	}
	server := <-accepted
	if err := <-acceptErr; err != nil {
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
		if selector, err := protocol.ReadSelector(in); err != nil || selector != 0 {
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
		if err := protocol.WriteSelector(out, protocol.ControlSelector); err != nil {
			t.Fatal(err)
		}
	}
	return rawControlPeer{server: server, raw: raw, out: out, in: in}
}

func (p rawControlPeer) write(t *testing.T, message *pb.Control) {
	t.Helper()
	wire, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	p.writeBytes(t, protocol.AppendFrame(nil, wire))
}

func (p rawControlPeer) writeBytes(t *testing.T, wire []byte) {
	t.Helper()
	if _, err := p.out.Write(wire); err != nil {
		t.Fatal(err)
	}
}

func (p rawControlPeer) writeViolation(t *testing.T, wire []byte) {
	t.Helper()
	// The receiver can reject the frame's length prefix before Write has
	// buffered its body. Either a completed write or that exact remote reset
	// is expected; GoAway and the view cause are checked separately.
	_, err := p.out.Write(wire)
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

func pbGoAway(code protocol.Code) *pb.Control {
	return &pb.Control{Message: &pb.Control_GoAway{GoAway: &pb.GoAway{Code: code.Wire() >> 1}}}
}

func assertViewCause(t *testing.T, err error, code protocol.Code, remote bool) {
	t.Helper()
	closed, ok := errors.AsType[*transport.ViewClosedError](err)
	if !ok || closed.Code != code || closed.Remote != remote || !errors.Is(err, transport.ErrViewClosed) {
		t.Fatalf("view cause = %v, want {%s %t}", err, code, remote)
	}
}

func waitViewCause(t *testing.T, conn transport.Conn, code protocol.Code, remote bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for {
		_, err := conn.PeerHello(ctx)
		if err != nil {
			assertViewCause(t, err, code, remote)
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("view did not close: %v", ctx.Err())
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func assertRawGoAway(t *testing.T, in *quic.ReceiveStream, code protocol.Code) {
	t.Helper()
	_ = in.SetReadDeadline(time.Now().Add(time.Second))
	message, err := readControlFrame(in)
	if err != nil || message.GetGoAway() == nil || message.GetGoAway().Code != code.Wire()>>1 {
		t.Fatalf("GoAway = %v, %v; want %s", message, err, code)
	}
	if _, err := protocol.ReadFrame(in, maxControlFrame); !errors.Is(err, io.EOF) {
		t.Fatalf("after GoAway = %v, want FIN", err)
	}
}

func assertRawReadCancel(t *testing.T, out *quic.SendStream, code protocol.Code) {
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
	clientHello := transport.Hello{Selectors: []protocol.Selector{1, 3}, Record: []byte{1, 2, 3}}
	serverHello := transport.Hello{Selectors: []protocol.Selector{2, 4}, Record: []byte{4, 5}}
	if err := clientEth.SetHello(clientHello); err != nil {
		t.Fatal(err)
	}
	if err := serverEth.SetHello(serverHello); err != nil {
		t.Fatal(err)
	}
	clientHello.Selectors[0], clientHello.Record[0] = 99, 99
	serverHello.Selectors[0], serverHello.Record[0] = 99, 99
	ctx := controlContext(t)
	accepted := make(chan transport.Conn, 1)
	go func() { conn, _ := serverEth.Accept(ctx); accepted <- conn }()
	client, err := clientEth.Dial(ctx, serverPC.LocalAddr(), serverShared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	server := <-accepted
	if server == nil {
		t.Fatal("server did not accept")
	}
	if err := clientEth.SetHello(transport.Hello{Selectors: []protocol.Selector{9}}); err != nil {
		t.Fatal(err)
	}
	if err := serverEth.SetHello(transport.Hello{Selectors: []protocol.Selector{10}}); err != nil {
		t.Fatal(err)
	}
	gotClient, err := client.PeerHello(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gotServer, err := server.PeerHello(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(gotClient.Selectors, []protocol.Selector{2, 4}) || !bytes.Equal(gotClient.Record, []byte{4, 5}) ||
		!slices.Equal(gotServer.Selectors, []protocol.Selector{1, 3}) || !bytes.Equal(gotServer.Record, []byte{1, 2, 3}) {
		t.Fatalf("exchanged transport.Hello = client %+v, server %+v", gotClient, gotServer)
	}
	gotClient.Record[0], gotClient.Selectors[0] = 99, 99
	again, err := client.PeerHello(ctx)
	if err != nil || !bytes.Equal(again.Record, []byte{4, 5}) || !slices.Equal(again.Selectors, []protocol.Selector{2, 4}) {
		t.Fatalf("PeerHello reused caller slices: %+v, %v", again, err)
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
	nextFromServer, err := nextClient.PeerHello(ctx)
	if err != nil {
		t.Fatal(err)
	}
	nextFromClient, err := nextServer.PeerHello(ctx)
	if err != nil || !slices.Equal(nextFromServer.Selectors, []protocol.Selector{10}) || !slices.Equal(nextFromClient.Selectors, []protocol.Selector{9}) {
		t.Fatalf("next view transport.Hello = client %+v, server %+v, err %v", nextFromServer, nextFromClient, err)
	}
	_ = nextClient.Close()
	_ = nextServer.Close()
	_ = client.Close()
	_ = server.Close()
}

func TestControlViolations(t *testing.T) {
	tooMany := make([]uint64, protocol.MaxSelectors+1)
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
		{"oversize", protocol.AppendFrame(nil, bytes.Repeat([]byte{0}, maxControlFrame+1))},
		{"malformed protobuf", protocol.AppendFrame(nil, []byte{0xff})},
		{"first GoAway", controlWire(t, pbGoAway(protocol.Refused))},
		{"first unset", controlWire(t, &pb.Control{})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := newRawControlPeer(t)
			p.writeViolation(t, test.wire)
			assertRawGoAway(t, p.in, protocol.ControlViolation)
			assertRawReadCancel(t, p.out, protocol.ControlViolation)
			waitViewCause(t, p.server, protocol.ControlViolation, false)
		})
	}
	for _, test := range []string{"second transport.Hello", "second control", "bidirectional control"} {
		t.Run(test, func(t *testing.T) {
			p := newRawControlPeer(t)
			p.write(t, pbHello(1))
			if _, err := p.server.PeerHello(controlContext(t)); err != nil {
				t.Fatal(err)
			}
			switch test {
			case "second transport.Hello":
				p.write(t, pbHello(1))
			case "second control":
				second, err := p.raw.OpenUniStream()
				if err != nil {
					t.Fatal(err)
				}
				if err := protocol.WriteSelector(second, 0); err != nil {
					t.Fatal(err)
				}
				_ = second.SetWriteDeadline(time.Now().Add(time.Second))
				_, err = second.Write(bytes.Repeat([]byte{0xa5}, 1<<20))
				reset, ok := errors.AsType[*quic.StreamError](err)
				if !ok || !reset.Remote || uint64(reset.ErrorCode) != protocol.ControlViolation.Wire() {
					t.Fatalf("second control reset = %v, want remote wire 22", err)
				}
			case "bidirectional control":
				stream, err := p.raw.OpenStreamSync(controlContext(t))
				if err != nil {
					t.Fatal(err)
				}
				if err := protocol.WriteSelector(stream, 0); err != nil {
					t.Fatal(err)
				}
				var one [1]byte
				_, err = stream.Read(one[:])
				reset, ok := errors.AsType[*quic.StreamError](err)
				if !ok || !reset.Remote || uint64(reset.ErrorCode) != protocol.ControlViolation.Wire() {
					t.Fatalf("bidi reset = %v, want remote wire 22", err)
				}
				_ = stream.SetWriteDeadline(time.Now().Add(time.Second))
				_, err = stream.Write(make([]byte, 1<<20))
				reset, ok = errors.AsType[*quic.StreamError](err)
				if !ok || !reset.Remote || uint64(reset.ErrorCode) != 22 {
					t.Fatalf("bidi read cancellation = %v, want remote wire 22", err)
				}
			}
			assertRawGoAway(t, p.in, protocol.ControlViolation)
			assertRawReadCancel(t, p.out, protocol.ControlViolation)
			waitViewCause(t, p.server, protocol.ControlViolation, false)
		})
	}
}

func controlWire(t *testing.T, message *pb.Control) []byte {
	t.Helper()
	wire, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.AppendFrame(nil, wire)
}

func TestControlTimeoutAndPeerClosure(t *testing.T) {
	for _, test := range []string{"no stream", "empty stream"} {
		t.Run(test, func(t *testing.T) {
			transport.ShortenHelloTimeout(t, 100*time.Millisecond)
			p := newRawControlPeerWithStream(t, test == "empty stream")
			assertRawGoAway(t, p.in, protocol.ControlViolation)
			if p.out != nil {
				assertRawReadCancel(t, p.out, protocol.ControlViolation)
			}
			waitViewCause(t, p.server, protocol.ControlViolation, false)
		})
	}
	for _, test := range []struct {
		name   string
		end    func(rawControlPeer)
		want   protocol.Code
		remote bool
	}{
		{"GoAway", func(p rawControlPeer) { p.write(t, pbGoAway(protocol.Refused)) }, protocol.Refused, true},
		{"unknown GoAway", func(p rawControlPeer) {
			p.write(t, &pb.Control{Message: &pb.Control_GoAway{GoAway: &pb.GoAway{Code: math.MaxUint64}}})
		}, protocol.Unspecified, true},
		{"FIN", func(p rawControlPeer) { _ = p.out.Close() }, protocol.Unspecified, true},
		{"stack reset", func(p rawControlPeer) { p.out.CancelWrite(controlQuicCode(protocol.Duplicate)) }, protocol.Duplicate, true},
		{"protocol reset", func(p rawControlPeer) { p.out.CancelWrite(quic.StreamErrorCode(23)) }, protocol.ControlViolation, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := newRawControlPeer(t)
			p.write(t, pbHello(1))
			if _, err := p.server.PeerHello(controlContext(t)); err != nil {
				t.Fatal(err)
			}
			test.end(p)
			if test.remote {
				_ = p.in.SetReadDeadline(time.Now().Add(time.Second))
				if _, err := protocol.ReadFrame(p.in, maxControlFrame); !errors.Is(err, io.EOF) {
					t.Fatalf("peer closure reply = %v, want FIN", err)
				}
			} else {
				assertRawGoAway(t, p.in, protocol.ControlViolation)
			}
			waitViewCause(t, p.server, test.want, test.remote)
			if _, _, err := p.server.AcceptStream(controlContext(t)); err == nil {
				t.Fatal("AcceptStream succeeded after closure")
			} else {
				assertViewCause(t, err, test.want, test.remote)
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
	p := newRawControlPeer(t)
	p.write(t, pbHello(1))
	if _, err := p.server.PeerHello(controlContext(t)); err != nil {
		t.Fatal(err)
	}
	p.write(t, &pb.Control{})
	p.writeBytes(t, protocol.AppendFrame(nil, []byte{0x1a, 0}))
	out, err := p.raw.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.WriteSelector(out, 1); err != nil {
		t.Fatal(err)
	}
	in, selector, err := p.server.AcceptUniStream(controlContext(t))
	if err != nil || selector != 1 {
		t.Fatalf("stream after unknown control = %d, %v", selector, err)
	}
	in.CancelRead(0)
	// A later control frame proves the reader processed both ignored messages.
	p.write(t, pbGoAway(protocol.Refused))
	waitViewCause(t, p.server, protocol.Refused, true)
}

func TestControlRejectsInvalidLaterFrame(t *testing.T) {
	for _, wire := range [][]byte{protocol.AppendFrame(nil, []byte{0xff}), protocol.AppendFrame(nil, make([]byte, maxControlFrame+1))} {
		p := newRawControlPeer(t)
		p.write(t, pbHello(1))
		if _, err := p.server.PeerHello(controlContext(t)); err != nil {
			t.Fatal(err)
		}
		p.writeViolation(t, wire)
		assertRawGoAway(t, p.in, protocol.ControlViolation)
		assertRawReadCancel(t, p.out, protocol.ControlViolation)
		waitViewCause(t, p.server, protocol.ControlViolation, false)
	}
}

func TestControlImmediateCloseSendsHelloFirst(t *testing.T) {
	for range 30 {
		pair := controlViewPair(t)
		if err := pair.clientEth.CloseWithCode(protocol.Duplicate); err != nil {
			t.Fatal(err)
		}
		waitViewCause(t, pair.clientEth, protocol.Duplicate, false)
		waitViewCause(t, pair.serverEth, protocol.Duplicate, true)
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
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err := p.server.PeerHello(ctx)
	assertViewCause(t, err, protocol.ControlViolation, false)
	assertRawReadCancel(t, p.out, protocol.ControlViolation)
}

func TestControlHelloWriteFailureClosesView(t *testing.T) {
	p := newRawControlPeerConfigured(t, true,
		transport.Hello{Record: make([]byte, maxControlFrame-32)},
		&quic.Config{InitialStreamReceiveWindow: 1, MaxStreamReceiveWindow: 1}, false)
	p.in.CancelRead(quic.StreamErrorCode(protocol.Refused.Wire()))
	waitViewCause(t, p.server, protocol.Unspecified, false)
	assertRawReadCancel(t, p.out, protocol.Closing)
}

func TestControlOverloadDoesNotBlockOtherConnections(t *testing.T) {
	endpoint := transporttest.NewEndpoint(t)
	selectors := make([]protocol.Selector, protocol.MaxSelectors)
	for i := range selectors {
		selectors[i] = protocol.Selector(1<<56) + protocol.Selector(i)
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
		wire := protocol.AppendFrame(nil, []byte{0})
		wire = append(wire, controlWire(t, pbHello(1))...)
		if _, err := out.Write(wire); err != nil {
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
		if selector, err := protocol.ReadSelector(in); err != nil || selector != protocol.ControlSelector {
			t.Fatalf("control selector = %d, %v", selector, err)
		}
		if message, err := readControlFrame(in); err != nil || message.GetHello() == nil {
			t.Fatalf("Hello = %v, %v", message, err)
		}
		if _, err := listener.Accept(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := transport.PendingEthp2p(endpoint.Shared); got != 16 {
		t.Fatalf("queued views = %d, want 16", got)
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
	transport.AssertEthp2pViewClosed(t, blockedView, protocol.Overloaded, false)
	// Prove the rejected connection still carries libp2p traffic after the
	// ethp2p release, without reading its blocked outbound control stream.
	out, err := blocked.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := protocol.AppendFrame(nil, []byte("/multistream/1.0.0\n"))
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
	p := newRawControlPeer(t)
	bi, err := p.raw.OpenStreamSync(controlContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.WriteSelector(bi, 2); err != nil {
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
	if err := protocol.WriteSelector(out, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := out.Write([]byte("before transport.Hello")); err != nil {
		t.Fatal(err)
	}
	_ = out.Close()
	short, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, _, err := p.server.AcceptUniStream(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pre-transport.Hello stream accepted: %v", err)
	}
	biWait, cancelBi := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancelBi()
	if _, _, err := p.server.AcceptStream(biWait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pre-Hello bidi accepted: %v", err)
	}
	p.write(t, pbHello(1))
	in, selector, err := p.server.AcceptUniStream(controlContext(t))
	if err != nil || selector != 1 {
		t.Fatalf("post-transport.Hello stream = %d, %v", selector, err)
	}
	payload, err := io.ReadAll(in)
	if err != nil || string(payload) != "before transport.Hello" {
		t.Fatalf("payload = %q, %v", payload, err)
	}
	acceptedBi, selector, err := p.server.AcceptStream(controlContext(t))
	if err != nil || selector != 2 {
		t.Fatalf("post-Hello bidi = %d, %v", selector, err)
	}
	payload, err = io.ReadAll(acceptedBi)
	if err != nil || string(payload) != "early bidi" {
		t.Fatalf("bidi payload = %q, %v", payload, err)
	}
}

func TestCloseWithCodeSendsGoAwayAndIsIdempotent(t *testing.T) {
	for _, test := range []struct {
		name  string
		code  protocol.Code
		close func(transport.Conn) error
	}{
		{"Close", protocol.Closing, func(c transport.Conn) error { return c.Close() }},
		{"CloseWithCode", protocol.Duplicate, func(c transport.Conn) error { return c.CloseWithCode(protocol.Duplicate) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			pair := controlViewPair(t)
			if _, err := pair.serverEth.PeerHello(controlContext(t)); err != nil {
				t.Fatal(err)
			}
			if err := test.close(pair.clientEth); err != nil {
				t.Fatal(err)
			}
			waitViewCause(t, pair.clientEth, test.code, false)
			waitViewCause(t, pair.serverEth, test.code, true)
			if err := pair.clientEth.CloseWithCode(protocol.Refused); err != nil {
				t.Fatal(err)
			}
			waitViewCause(t, pair.clientEth, test.code, false)
		})
	}
}

func TestRawConnectionClosureKeepsQuicCause(t *testing.T) {
	p := newRawControlPeer(t)
	p.write(t, pbHello(1))
	if _, err := p.server.PeerHello(controlContext(t)); err != nil {
		t.Fatal(err)
	}
	if err := p.raw.CloseWithError(99, "raw close"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for {
		_, err := p.server.PeerHello(ctx)
		if err != nil {
			if app, ok := errors.AsType[*quic.ApplicationError](err); !ok || app.ErrorCode != 99 || !app.Remote {
				t.Fatalf("raw connection closure = %v, want quic ApplicationError", err)
			}
			if errors.Is(err, transport.ErrViewClosed) {
				t.Fatalf("raw connection closure mapped to view closure: %v", err)
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("raw connection closure did not stop view")
		default:
			time.Sleep(time.Millisecond)
		}
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
	for _, selectors := range [][]protocol.Selector{{2, 1}, {0}, make([]protocol.Selector, protocol.MaxSelectors+1)} {
		if err := eth.SetHello(transport.Hello{Selectors: selectors}); err == nil {
			t.Fatalf("SetHello accepted %d invalid selectors", len(selectors))
		}
	}
	if err := eth.SetHello(transport.Hello{Record: bytes.Repeat([]byte{0}, maxControlFrame+1)}); !errors.Is(err, protocol.ErrFrameTooLarge) {
		t.Fatalf("oversized transport.Hello = %v", err)
	}
	if _, err := eth.Dial(controlContext(t), packet.LocalAddr(), ""); !errors.Is(err, transport.ErrNoHello) {
		t.Fatalf("invalid Hello registered interest: %v", err)
	}
	if err := eth.SetHello(transport.Hello{Selectors: []protocol.Selector{1}}); err != nil {
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
	accepted := make(chan transport.Conn, 1)
	go func() { conn, _ := server.Ethp2p().Accept(ctx); accepted <- conn }()
	client, err := clientEth.Dial(ctx, addr, server.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	if <-accepted == nil {
		t.Fatal("server did not accept")
	}
	if _, err := client.PeerHello(ctx); err != nil {
		t.Fatal(err)
	}
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
	for {
		select {
		case <-ticker.C:
			_ = client.SendDatagram(ctx, bytes.Repeat([]byte{0x42}, 128))
			if _, err := client.PeerHello(ctx); err != nil {
				if _, ok := errors.AsType[*quic.StatelessResetError](err); !ok {
					t.Fatalf("connection ended with %v, want stateless reset", err)
				}
				return
			}
		case <-deadline.C:
			t.Fatal("stateless reset did not close the connection within 3s")
		}
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
	clientEth, serverEth transport.Conn
	serverShared         *transport.SharedTransport
} {
	t.Helper()
	client, server := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	client.Shared.Libp2p()
	server.Shared.Libp2p()
	dialed, accepted := transporttest.Connect(t, client, server)
	return struct {
		clientEth, serverEth transport.Conn
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

func controlKey(t *testing.T) *transport.PrivKey {
	t.Helper()
	k, err := transport.GenPrivKey()
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
	wire, err := protocol.ReadFrame(in, maxControlFrame)
	if err != nil {
		return nil, err
	}
	var message pb.Control
	if err := proto.Unmarshal(wire, &message); err != nil {
		return nil, err
	}
	return &message, nil
}

func controlQuicCode(code protocol.Code) quic.StreamErrorCode {
	return quic.StreamErrorCode(code.Wire())
}
