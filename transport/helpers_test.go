package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/quic-go/quic-go"
)

func assertSharedALPN(t *testing.T, conn quicreuse.QUICConn) {
	t.Helper()
	shared, ok := conn.(*libp2pConn)
	if !ok {
		t.Fatalf("connection type = %T, want *libp2pConn", conn)
	}
	if got := shared.conn.ConnectionState().TLS.NegotiatedProtocol; got != AlpnEthp2p {
		t.Fatalf("raw ALPN = %q, want %q", got, AlpnEthp2p)
	}
	if got := conn.ConnectionState().TLS.NegotiatedProtocol; got != AlpnLibp2p {
		t.Fatalf("presented ALPN = %q, want %q", got, AlpnLibp2p)
	}
}

func assertRawALPN(t *testing.T, conn quicreuse.QUICConn, want string) {
	t.Helper()
	raw, ok := conn.(*quic.Conn)
	if !ok {
		t.Fatalf("connection does not expose a raw QUIC connection")
	}
	if got := raw.ConnectionState().TLS.NegotiatedProtocol; got != string(want) {
		t.Fatalf("raw ALPN = %q, want %q", got, want)
	}
}

// assertPeer checks the identity reported for the remote endpoint. The local
// identity belongs to the endpoint rather than the connection, and the remote
// peer ID is derived from the authenticated certificate, so there is no
// separate public-key assertion left to make here.
func assertPeer(t *testing.T, conn *Conn, remote identity.PeerID) {
	t.Helper()
	if got := conn.RemotePeerID(); got != remote {
		t.Fatalf("remote peer ID = %x, want %x", got, remote)
	}
}

// waitNextStream pulls the next incoming bidirectional ethp2p stream, unread,
// waiting on the view's wake channel. It returns the view closure cause or
// ctx's error when no stream arrives first. Unlike nextStream it reports
// errors, so goroutines other than the test goroutine can use it.
func waitNextStream(ctx context.Context, c *Conn) (*quic.Stream, error) {
	for {
		if s, ok := c.NextStream(); ok {
			return s, nil
		}
		select {
		case <-c.Streams():
		case <-c.Done():
			return nil, ethp2pError(context.Cause(c.ethp2pCtx))
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// nextStream pulls the next incoming bidirectional ethp2p stream, waiting on
// the view's wake channel up to 10 seconds. It fails tb on timeout or when the
// view closes.
func nextStream(tb testing.TB, c *Conn) *quic.Stream {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), 10*time.Second)
	defer cancel()
	s, err := waitNextStream(ctx, c)
	if err != nil {
		tb.Fatal(err)
	}
	return s
}

// waitNextUniStream pulls the next incoming unidirectional ethp2p stream,
// unread, waiting on the view's wake channel. It returns the view closure
// cause or ctx's error when no stream arrives first. Unlike nextUniStream it
// reports errors, so goroutines other than the test goroutine can use it.
func waitNextUniStream(ctx context.Context, c *Conn) (*quic.ReceiveStream, error) {
	for {
		if s, ok := c.NextUniStream(); ok {
			return s, nil
		}
		select {
		case <-c.UniStreams():
		case <-c.Done():
			return nil, ethp2pError(context.Cause(c.ethp2pCtx))
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// nextUniStream pulls the next incoming unidirectional ethp2p stream, waiting
// on the view's wake channel up to 10 seconds. It fails tb on timeout or when
// the view closes.
func nextUniStream(tb testing.TB, c *Conn) *quic.ReceiveStream {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), 10*time.Second)
	defer cancel()
	s, err := waitNextUniStream(ctx, c)
	if err != nil {
		tb.Fatal(err)
	}
	return s
}

// openStream opens a bidirectional stream and writes sel's stream header, as
// the stack does.
func openStream(t *testing.T, ctx context.Context, c *Conn, sel wire.Protocol) *quic.Stream {
	t.Helper()
	s, err := c.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.WriteHeader(s, sel); err != nil {
		t.Fatal(err)
	}
	return s
}

// nextSelected pulls the next incoming bidirectional ethp2p stream and reads
// its stream header, as the stack does.
func nextSelected(tb testing.TB, c *Conn) (*quic.Stream, wire.Protocol) {
	tb.Helper()
	s := nextStream(tb, c)
	sel, err := wire.ReadHeader(s)
	if err != nil {
		tb.Fatal(err)
	}
	return s, sel
}

func newEndpoint(t *testing.T) (*Endpoint, quicreuse.QUICTransport, *Ethp2pTransport, *net.UDPConn) {
	t.Helper()
	return newEndpointWith(t, Interop())
}

// newEndpointWith builds an endpoint with an explicit connection config.
func newEndpointWith(t *testing.T, config Config) (*Endpoint, quicreuse.QUICTransport, *Ethp2pTransport, *net.UDPConn) {
	t.Helper()
	packetConn := testPacketConn(t)
	endpoint, err := NewEndpoint(testKey(t), packetConn, config)
	if err != nil {
		t.Fatal(err)
	}
	libp2p, ethp2p := endpoint.Libp2p(), endpoint.Ethp2p()
	if err := endpoint.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	return endpoint, libp2p, ethp2p, packetConn
}

func listen(t *testing.T, libp2p quicreuse.QUICTransport, ethp2p *Ethp2pTransport) quicreuse.QUICListener {
	t.Helper()
	listener, err := libp2p.Listen(ethp2p.endpoint.handshaker.serverConfig(), &quic.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func testKey(t *testing.T) *identity.PrivKey {
	t.Helper()
	key, err := identity.GenPrivKey()
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func testPacketConn(t *testing.T) *net.UDPConn {
	t.Helper()
	packetConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = packetConn.Close() })
	return packetConn
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func waitQueued[T any](t *testing.T, q *streamQueue[T], n int) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		q.mu.Lock()
		count := len(q.items)
		q.mu.Unlock()
		if count == n {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("queued = %d, want %d", count, n)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
