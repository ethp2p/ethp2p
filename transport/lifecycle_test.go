package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/wire"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/quic-go/quic-go"
)

type viewPair struct {
	clientEth, serverEth *Conn
	clientLib, serverLib quicreuse.QUICConn
}

func newViewPair(t *testing.T, incomingUni int64) viewPair {
	t.Helper()
	return newViewPairWithConfig(t, ConfigWithIncomingUniStreams(incomingUni))
}

func newViewPairWithConfig(t *testing.T, config Config) viewPair {
	t.Helper()
	ctx := testContext(t)
	_, clientLib, clientEth, _ := newEndpoint(t)
	_, serverLib, serverEth, serverPC := newEndpointWith(t, config)
	clientListener := listen(t, clientLib, clientEth)
	serverListener := listen(t, serverLib, serverEth)
	client, err := clientEth.Dial(ctx, serverPC.LocalAddr(), serverEth.endpoint.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	server, err := serverEth.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	clientView, err := clientListener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	serverView, err := serverListener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client.Close()
		server.Close()
		_ = clientView.CloseWithError(appNoError, "test done")
		_ = serverView.CloseWithError(appNoError, "test done")
	})
	return viewPair{client, server, clientView, serverView}
}

func waitViewError(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if !errors.Is(err, ErrViewClosed) {
			t.Fatalf("pending view operation = %v, want view closed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending view operation did not stop within a second")
	}
}

func waitEthViewError(t *testing.T, result <-chan error, code wire.Code, remote bool) {
	t.Helper()
	select {
	case err := <-result:
		assertEthViewCause(t, err, code, remote)
	case <-time.After(time.Second):
		t.Fatal("pending ethp2p operation did not stop within a second")
	}
}

func assertEthViewCause(t *testing.T, err error, code wire.Code, remote bool) {
	t.Helper()
	closed, ok := errors.AsType[*ViewClosedError](err)
	if !ok || closed.Code != code || closed.Remote != remote || !errors.Is(err, ErrViewClosed) {
		t.Fatalf("view cause = %v, want {%s %t}", err, code, remote)
	}
}

func checkLibp2pRoundTrip(t *testing.T, ctx context.Context, from, to quicreuse.QUICConn) {
	t.Helper()
	out, err := from.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := append([]byte{0x13}, "/multistream/1.0.0\n/view-lifetime"...)
	if _, err := out.Write(want); err != nil {
		t.Fatal(err)
	}
	in, err := to.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(in, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("received %x, %v; want %x", got, err, want)
	}
	if _, err := in.Write(got); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(out, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("reply %x, %v; want %x", got, err, want)
	}
}

func TestLibp2pViewCloseStopsAccept(t *testing.T) {
	ctx := testContext(t)
	pair := newViewPair(t, 32)
	pending := make(chan error, 1)
	go func() { _, err := pair.serverLib.AcceptStream(ctx); pending <- err }()
	if err := pair.serverLib.CloseWithError(appNoError, "test close"); err != nil {
		t.Fatal(err)
	}
	waitViewError(t, pending)
	select {
	case <-pair.serverLib.Context().Done():
	default:
		t.Fatal("libp2p view context is still live")
	}
	if pair.serverEth.conn.Context().Err() != nil {
		t.Fatal("physical connection closed with one view still live")
	}
	out := openStream(t, ctx, pair.clientEth, 1)
	want := []byte{1}
	if _, err := out.Write(want); err != nil {
		t.Fatal(err)
	}
	received, protocol := nextSelected(t, pair.serverEth)
	if protocol != 1 {
		t.Fatalf("protocol = %d, want 1", protocol)
	}
	in, ok := any(received).(*quic.Stream)
	if !ok {
		t.Fatal("bidirectional stream is not a *quic.Stream")
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(in, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("ethp2p received %x, %v; want %x", got, err, want)
	}
	if _, err := in.Write(got); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(out, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("ethp2p reply %x, %v; want %x", got, err, want)
	}
}

func TestEthp2pViewCloseStopsPendingOperations(t *testing.T) {
	ctx := testContext(t)
	// The first stream takes the one available outbound uni stream credit.
	pair := newViewPair(t, 1)
	if _, err := pair.clientEth.OpenUniStream(ctx); err != nil {
		t.Fatal(err)
	}
	queued := make(chan error, 1)
	go func() { _, err := waitNextUniStream(ctx, pair.clientEth); queued <- err }()
	blocked := make(chan error, 1)
	go func() { _, err := pair.clientEth.OpenUniStream(ctx); blocked <- err }()
	select {
	case err := <-blocked:
		t.Fatalf("stream credit was not exhausted: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	pair.clientEth.Close()
	waitEthViewError(t, queued, wire.Closing, false)
	waitEthViewError(t, blocked, wire.Closing, false)
	checkLibp2pRoundTrip(t, ctx, pair.clientLib, pair.serverLib)
}

func TestClosedEthp2pViewResetsQueuedAndNewStreams(t *testing.T) {
	ctx := testContext(t)
	pair := newViewPair(t, 32)
	bi := openStream(t, ctx, pair.clientEth, 1)
	uni, err := pair.clientEth.OpenUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uni.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	waitQueued(t, pair.serverEth.ethp2pBidi, 1)
	waitQueued(t, pair.serverEth.ethp2pUni, 1)
	pair.serverEth.Close()
	select {
	case <-pair.serverEth.Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	assertEthViewCause(t, context.Cause(pair.serverEth.ethp2pCtx), wire.Closing, false)
	var one [1]byte
	_, err = bi.Read(one[:])
	reset, ok := errors.AsType[*quic.StreamError](err)
	if !ok || uint64(reset.ErrorCode) != wire.Closing.Wire() {
		t.Fatalf("bidi reset = %v", err)
	}
	for _, out := range []interface {
		Write([]byte) (int, error)
		SetWriteDeadline(time.Time) error
	}{bi, uni} {
		_ = out.SetWriteDeadline(time.Now().Add(time.Second))
		_, err := out.Write(make([]byte, 1<<20))
		reset, ok := errors.AsType[*quic.StreamError](err)
		if !ok || !reset.Remote || uint64(reset.ErrorCode) != wire.Closing.Wire() {
			t.Fatalf("write reset = %v", err)
		}
	}
	// The libp2p views keep the connection open, so the client's view stays
	// open: only the stack's GOAWAY would tell it about the closure. Its late
	// stream stands for a peer that ignores closure.
	stream, err := pair.clientEth.conn.OpenUniStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte{1, 1, 0x42}); err != nil {
		reset, ok := errors.AsType[*quic.StreamError](err)
		if !ok || !reset.Remote || uint64(reset.ErrorCode) != wire.Closing.Wire() {
			t.Fatal(err)
		}
	}
	select {
	case <-stream.Context().Done():
		var reset *quic.StreamError
		if !errors.As(context.Cause(stream.Context()), &reset) || !reset.Remote || uint64(reset.ErrorCode) != wire.Closing.Wire() {
			t.Fatalf("peer stream cause = %v, want remote wire %d", context.Cause(stream.Context()), wire.Closing.Wire())
		}
	case <-time.After(time.Second):
		t.Fatal("peer did not observe unidirectional stream reset")
	}
	checkLibp2pRoundTrip(t, ctx, pair.clientLib, pair.serverLib)
}

func TestBothViewsClosePhysicalConnection(t *testing.T) {
	ctx := testContext(t)
	clientEndpoint, _, client, _ := newEndpoint(t)
	serverEndpoint, _, server, serverPC := newEndpoint(t)
	clientConn, err := client.Dial(ctx, serverPC.LocalAddr(), server.endpoint.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	serverConn, err := server.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	clientRaw := clientConn.conn
	serverRaw := serverConn.conn
	clientConn.Close()
	serverConn.Close()
	// The physical connection stays open while either libp2p view is held;
	// releasing those too must close it.
	for _, ep := range []struct {
		endpoint *Endpoint
		eth      *Ethp2pTransport
	}{{clientEndpoint, client}, {serverEndpoint, server}} {
		listener := listen(t, ep.endpoint.Libp2p(), ep.eth)
		libConn, err := listener.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := libConn.CloseWithError(0, ""); err != nil {
			t.Fatal(err)
		}
	}
	for _, raw := range []*quic.Conn{clientRaw, serverRaw} {
		select {
		case <-raw.Context().Done():
		case <-ctx.Done():
			t.Fatal("physical connection remained open")
		}
	}
}

func TestFullLibp2pQueueKeepsDialedEthp2pView(t *testing.T) {
	ctx := testContext(t)
	_, _, client, _ := newEndpoint(t)
	_, _, server, serverPC := newEndpoint(t)
	for range maxConnsPendingDelivery {
		if _, err := client.Dial(ctx, serverPC.LocalAddr(), server.endpoint.PeerID()); err != nil {
			t.Fatal(err)
		}
		serverConn, err := server.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		serverConn.Close()
	}
	conn, err := client.Dial(ctx, serverPC.LocalAddr(), server.endpoint.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	serverConn, err := server.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Check queue rejection before deliberately closing the peer's view;
	// its GoAway or connection close can immediately release our view too.
	serverConn.Close()
	conn.Close()
	select {
	case <-conn.conn.Context().Done():
	case <-ctx.Done():
		t.Fatal("physical connection remained open after full libp2p queue")
	}
}

// TestEndpointCloseWaitsForInflightDial: a dial blocked in the handshake
// holds the endpoint open: Close returns only after the dial has been
// released, the dial reports an error rather than returning a live view,
// and a dial started after Close is refused with ErrClosed.
func TestEndpointCloseWaitsForInflightDial(t *testing.T) {
	endpoint, _, ethp2p, _ := newEndpoint(t)
	// A bound but silent UDP socket: QUIC Initials are delivered and never
	// answered, so the handshake stalls until the caller's context ends.
	silent, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = silent.Close() }()

	ctx, cancel := context.WithCancel(testContext(t))
	type dialResult struct {
		conn *Conn
		err  error
	}
	dialDone := make(chan dialResult, 1)
	go func() {
		conn, err := ethp2p.Dial(ctx, silent.LocalAddr(), "")
		dialDone <- dialResult{conn, err}
	}()
	// Give the dial time to pass its entry checks and block in the handshake.
	time.Sleep(100 * time.Millisecond)

	closeDone := make(chan error, 1)
	go func() { closeDone <- endpoint.Close() }()
	cancel()
	result := <-dialDone
	if result.err == nil || result.conn != nil {
		t.Fatalf("in-flight dial = (%v, %v), want no view and an error", result.conn, result.err)
	}
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Endpoint.Close did not return after the dial was released")
	}
	if _, err := ethp2p.Dial(testContext(t), silent.LocalAddr(), ""); !errors.Is(err, ErrClosed) {
		t.Fatalf("dial after Close = %v, want ErrClosed", err)
	}
}

// TestEndpointStartErrors pins Start's three refusals: before both views
// are wired, a second Start, and Start after Close.
func TestEndpointStartErrors(t *testing.T) {
	unwired, err := NewEndpoint(testKey(t), testPacketConn(t), Interop())
	if err != nil {
		t.Fatal(err)
	}
	if err := unwired.Start(); !errors.Is(err, errNotWired) {
		t.Fatalf("Start before wired = %v, want %v", err, errNotWired)
	}

	endpoint, _, _, _ := newEndpoint(t)
	if err := endpoint.Start(); !errors.Is(err, errAlreadyStarted) {
		t.Fatalf("second Start = %v, want %v", err, errAlreadyStarted)
	}

	closed, err := NewEndpoint(testKey(t), testPacketConn(t), Interop())
	if err != nil {
		t.Fatal(err)
	}
	_ = closed.Libp2p()
	_ = closed.Ethp2p()
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closed.Start(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Start after Close = %v, want %v", err, ErrClosed)
	}
}
