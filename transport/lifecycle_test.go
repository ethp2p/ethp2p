package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/wire"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/quic-go/quic-go"
)

type viewPair struct {
	clientEth, serverEth Conn
	clientLib, serverLib quicreuse.QUICConn
}

func newViewPair(t *testing.T, incomingUni int64) viewPair {
	t.Helper()
	ctx := testContext(t)
	_, clientLib, clientEth, _ := newEndpoint(t)
	_, serverLib, serverEth, serverPC := newEndpointWith(t, testProfile(incomingUni))
	clientListener := listen(t, clientLib, clientEth)
	serverListener := listen(t, serverLib, serverEth)
	client, err := clientEth.Dial(ctx, serverPC.LocalAddr(), serverEth.shared.PeerID())
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
		_ = client.Close()
		_ = server.Close()
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
	want := frame([]byte("/multistream/1.0.0\n/view-lifetime"))
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
	if pair.serverEth.(*ethp2pConn).conn.Context().Err() != nil {
		t.Fatal("physical connection closed with one view still live")
	}
	out, err := pair.clientEth.OpenStream(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{1}
	if _, err := out.Write(want); err != nil {
		t.Fatal(err)
	}
	in, selector, err := pair.serverEth.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if selector != 1 {
		t.Fatalf("selector = %d, want 1", selector)
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
	// The control stream consumes the one available outbound uni stream.
	pair := newViewPair(t, 1)
	bi := make(chan error, 1)
	uni := make(chan error, 1)
	go func() { _, _, err := pair.clientEth.AcceptStream(ctx); bi <- err }()
	go func() { _, _, err := pair.clientEth.AcceptUniStream(ctx); uni <- err }()
	blocked := make(chan error, 1)
	go func() { _, err := pair.clientEth.OpenUniStream(ctx, 1); blocked <- err }()
	select {
	case err := <-blocked:
		t.Fatalf("stream credit was not exhausted: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := pair.clientEth.Close(); err != nil {
		t.Fatal(err)
	}
	waitEthViewError(t, bi, wire.Closing, false)
	waitEthViewError(t, uni, wire.Closing, false)
	waitEthViewError(t, blocked, wire.Closing, false)
	checkLibp2pRoundTrip(t, ctx, pair.clientLib, pair.serverLib)
}

func TestClosedEthp2pViewResetsNewUniStream(t *testing.T) {
	ctx := testContext(t)
	pair := newViewPair(t, 32)
	if err := pair.serverEth.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := pair.serverEth.PeerHello(ctx)
	assertEthViewCause(t, err, wire.Closing, false)
	client := pair.clientEth.(*ethp2pConn)
	select {
	case <-client.ethp2pCtx.Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_, err = client.PeerHello(ctx)
	assertEthViewCause(t, err, wire.Closing, true)
	// The public view refuses new streams after GoAway. Use raw QUIC to
	// exercise a peer that ignores closure and sends a late stream anyway.
	stream, err := client.conn.OpenUniStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte{1, 1, 0x42}); err != nil {
		reset, ok := errors.AsType[*quic.StreamError](err)
		if !ok || !reset.Remote || uint64(reset.ErrorCode) != 20 {
			t.Fatal(err)
		}
	}
	select {
	case <-stream.Context().Done():
		var reset *quic.StreamError
		if !errors.As(context.Cause(stream.Context()), &reset) || !reset.Remote || uint64(reset.ErrorCode) != wire.Closing.Wire() {
			t.Fatalf("peer stream cause = %v, want remote wire 20", context.Cause(stream.Context()))
		}
	case <-time.After(time.Second):
		t.Fatal("peer did not observe unidirectional stream reset")
	}
}

func TestEthp2pOnlyViewsClosePhysicalConnection(t *testing.T) {
	ctx := testContext(t)
	_, client, _ := newEthp2pEndpoint(t)
	serverShared, server, serverPC := newEthp2pEndpoint(t)
	if err := serverShared.ensureListener(); err != nil {
		t.Fatal(err)
	}
	clientConn, err := client.Dial(ctx, serverPC.LocalAddr(), server.shared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	serverConn, err := server.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	clientRaw := clientConn.(*ethp2pConn).conn
	serverRaw := serverConn.(*ethp2pConn).conn
	if err := clientConn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := serverConn.Close(); err != nil {
		t.Fatal(err)
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
	_, clientLib, client, _ := newEndpoint(t)
	serverShared, server, serverPC := newEthp2pEndpoint(t)
	if err := serverShared.ensureListener(); err != nil {
		t.Fatal(err)
	}
	_ = clientLib // Libp2p() registers interest without a consumer.
	for range maxConnsPendingDelivery {
		if _, err := client.Dial(ctx, serverPC.LocalAddr(), server.shared.PeerID()); err != nil {
			t.Fatal(err)
		}
		serverConn, err := server.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_ = serverConn.Close()
	}
	conn, err := client.Dial(ctx, serverPC.LocalAddr(), server.shared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	serverConn, err := server.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	view := conn.(*ethp2pConn)
	if view.closed.Load()&uint32(sideEthp2p) != 0 {
		t.Fatal("dial returned a closed ethp2p view")
	}
	// Check queue rejection before deliberately closing the peer's view;
	// its GoAway or connection close can immediately release our view too.
	_ = serverConn.Close()
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-view.conn.Context().Done():
	case <-ctx.Done():
		t.Fatal("physical connection remained open after full libp2p queue")
	}
}

func TestFullEthp2pQueueKeepsInboundLibp2pView(t *testing.T) {
	ctx := testContext(t)
	_, clientLib, clientEth, _ := newEndpoint(t)
	_, serverLib, serverEth, serverPC := newEndpoint(t)
	clientListener := listen(t, clientLib, clientEth)
	serverListener := listen(t, serverLib, serverEth)
	for range maxConnsPendingDelivery {
		eth, err := clientEth.Dial(ctx, serverPC.LocalAddr(), serverEth.shared.PeerID())
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
		_ = clientView.CloseWithError(appNoError, "test done")
		_ = serverView.CloseWithError(appNoError, "test done")
		_ = eth.Close()
	}
	clientEthConn, err := clientEth.Dial(ctx, serverPC.LocalAddr(), serverEth.shared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	defer clientEthConn.Close()
	clientView, err := clientListener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clientView.CloseWithError(appNoError, "test done")
	serverView, err := serverListener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer serverView.CloseWithError(appNoError, "test done")
	serverContext := (*sharedConn)(serverView.(*libp2pConn)).ethp2pCtx
	select {
	case <-serverContext.Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	assertEthViewCause(t, context.Cause(serverContext), wire.Overloaded, false)
	clientContext := clientEthConn.(*ethp2pConn).ethp2pCtx
	select {
	case <-clientContext.Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_, err = clientEthConn.PeerHello(ctx)
	assertEthViewCause(t, err, wire.Overloaded, true)
	stream, err := clientView.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := frame([]byte("/multistream/1.0.0\n/lifecycle"))
	if _, err := stream.Write(want); err != nil {
		t.Fatal(err)
	}
	gotStream, err := serverView.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(gotStream, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("libp2p stream = %x, want %x", got, want)
	}
	if _, err := gotStream.Write(got); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, len(want))
	if _, err := io.ReadFull(stream, reply); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reply, want) {
		t.Fatalf("libp2p reply = %x, want %x", reply, want)
	}
}
