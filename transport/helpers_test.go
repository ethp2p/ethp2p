package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	varint "github.com/multiformats/go-varint"
	"github.com/quic-go/quic-go"
)

func assertSharedALPN(t *testing.T, conn quicreuse.QUICConn) {
	t.Helper()
	shared, ok := conn.(*connLib)
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

func assertAuth(t *testing.T, conn Conn, local, remote PeerID, direction ConnDir) {
	t.Helper()
	got := conn.AuthInfo()
	if got.Local != local || got.Remote != remote {
		t.Fatalf("auth = %+v, want local %q remote %q", got, local, remote)
	}
	if got := conn.Direction(); got != direction {
		t.Fatalf("direction = %d, want %d", got, direction)
	}
	if got.RemoteKey == nil || got.RemoteKey.PeerID() != remote {
		t.Fatalf("remote public key does not match peer %x", remote)
	}
}

func newEndpoint(t *testing.T) (*TransportShared, *TransportLib, *TransportEth, *net.UDPConn) {
	t.Helper()
	packetConn := testPacketConn(t)
	shared, err := NewShared(testKey(t), packetConn)
	if err != nil {
		t.Fatal(err)
	}
	libp2p, ethp2p := shared.Libp2p(), shared.Ethp2p()
	if ethp2p.PublicKey() == nil || ethp2p.PublicKey().PeerID() != ethp2p.PeerID() {
		t.Fatal("transport public key does not match its peer ID")
	}
	t.Cleanup(func() { _ = shared.Close() })
	return shared, libp2p, ethp2p, packetConn
}

func listen(t *testing.T, libp2p *TransportLib, ethp2p *TransportEth) quicreuse.QUICListener {
	t.Helper()
	listener, err := libp2p.Listen(ethp2p.handshaker.serverConfig(), &quic.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func testKey(t *testing.T) *PrivKey {
	t.Helper()
	key, err := GenPrivKey()
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
