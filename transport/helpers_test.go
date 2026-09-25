package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	varint "github.com/multiformats/go-varint"
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
func assertPeer(t *testing.T, conn *Conn, remote PeerID) {
	t.Helper()
	if got := conn.RemotePeerID(); got != remote {
		t.Fatalf("remote peer ID = %x, want %x", got, remote)
	}
}

// waitNextStream pulls the next classified stream, waiting on the view's wake
// channel. It returns the view closure cause or ctx's error when no stream
// arrives first. Unlike nextStream it reports errors, so goroutines other than
// the test goroutine can use it.
func waitNextStream(ctx context.Context, c *Conn) (ReceiveStream, wire.Selector, error) {
	for {
		if s, sel, ok := c.NextStream(); ok {
			return s, sel, nil
		}
		select {
		case <-c.Streams():
		case <-c.Done():
			return nil, 0, ethp2pError(context.Cause(c.ethp2pCtx))
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		}
	}
}

// nextStream pulls the next classified stream, waiting on the view's wake
// channel up to 10 seconds. It fails tb on timeout or when the view closes.
func nextStream(tb testing.TB, c *Conn) (ReceiveStream, wire.Selector) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), 10*time.Second)
	defer cancel()
	s, sel, err := waitNextStream(ctx, c)
	if err != nil {
		tb.Fatal(err)
	}
	return s, sel
}

func newEndpoint(t *testing.T) (*SharedTransport, quicreuse.QUICTransport, *Ethp2pTransport, *net.UDPConn) {
	t.Helper()
	return newEndpointWith(t, Interop())
}

// newEthp2pEndpoint leaves libp2p uninterested, as in an ethp2p-only process.
func newEthp2pEndpoint(t *testing.T) (*SharedTransport, *Ethp2pTransport, *net.UDPConn) {
	t.Helper()
	packetConn := testPacketConn(t)
	shared, err := NewShared(testKey(t), packetConn, Interop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shared.Close() })
	eth := shared.Ethp2p()
	if err := eth.SetHello(Hello{}); err != nil {
		t.Fatal(err)
	}
	return shared, eth, packetConn
}

// newEndpointWith builds an endpoint with an explicit connection profile.
func newEndpointWith(t *testing.T, profile Profile) (*SharedTransport, quicreuse.QUICTransport, *Ethp2pTransport, *net.UDPConn) {
	t.Helper()
	packetConn := testPacketConn(t)
	shared, err := NewShared(testKey(t), packetConn, profile)
	if err != nil {
		t.Fatal(err)
	}
	libp2p, ethp2p := shared.Libp2p(), shared.Ethp2p()
	if err := ethp2p.SetHello(Hello{}); err != nil {
		t.Fatal(err)
	}
	if shared.PublicKey() == nil || PeerIDFromKey(shared.PublicKey()) != shared.PeerID() {
		t.Fatal("transport public key does not match its peer ID")
	}
	t.Cleanup(func() { _ = shared.Close() })
	return shared, libp2p, ethp2p, packetConn
}

func listen(t *testing.T, libp2p quicreuse.QUICTransport, ethp2p *Ethp2pTransport) quicreuse.QUICListener {
	t.Helper()
	listener, err := libp2p.Listen(ethp2p.shared.handshaker.serverConfig(), &quic.Config{})
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

func frame(payload []byte) []byte {
	return append(varint.ToUvarint(uint64(len(payload))), payload...)
}
