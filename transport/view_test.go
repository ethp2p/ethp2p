package transport_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/quic-go/quic-go"
)

// A full ethp2p queue releases the new view with Unspecified at once: its
// libp2p view and an unrelated connection are still delivered promptly.
func TestFullEthp2pQueueDoesNotBlockOtherConnections(t *testing.T) {
	endpoint := transporttest.NewEndpoint(t)
	listener, err := endpoint.Endpoint.Libp2p().Listen(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	ctx := transporttest.Context(t)
	dial := func() *quic.Conn {
		raw, err := transporttest.DialRaw(t, ctx, endpoint.Endpoint.Addr(), endpoint.Endpoint.PeerID(), &quic.Config{})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	// Fill the ethp2p queue, draining libp2p so no filler waits on it.
	for range 16 {
		dial()
		if _, err := listener.Accept(ctx); err != nil {
			t.Fatal(err)
		}
	}
	waitPending := time.After(5 * time.Second)
	for transport.PendingEthp2p(endpoint.Endpoint) != 16 {
		select {
		case <-waitPending:
			t.Fatalf("queued views = %d, want 16", transport.PendingEthp2p(endpoint.Endpoint))
		default:
			time.Sleep(time.Millisecond)
		}
	}
	overflow := dial()
	start := time.Now()
	normal := dial()
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
	overflowView, normalView := first, second
	if first.RemoteAddr().String() != overflow.LocalAddr().String() {
		overflowView, normalView = second, first
	}
	if overflowView.RemoteAddr().String() != overflow.LocalAddr().String() || normalView.RemoteAddr().String() != normal.LocalAddr().String() {
		t.Fatal("listener delivered unexpected peers")
	}
	transport.AssertEthp2pViewClosed(t, overflowView, wire.Unspecified, false)
	// The rejected connection still carries libp2p traffic after the ethp2p
	// release.
	out, err := overflow.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := append([]byte{0x13}, "/multistream/1.0.0\n"...)
	if _, err := out.Write(want); err != nil {
		t.Fatal(err)
	}
	in, err := overflowView.AcceptStream(ctx)
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

func TestRawConnectionClosureKeepsQuicCause(t *testing.T) {
	endpoint := transporttest.NewEndpoint(t)
	ctx := transporttest.Context(t)
	accepted := make(chan *transport.Conn, 1)
	go func() { conn, _ := endpoint.Eth.Accept(ctx); accepted <- conn }()
	raw, err := transporttest.DialRaw(t, ctx, endpoint.Endpoint.Addr(), endpoint.Endpoint.PeerID(), &quic.Config{})
	if err != nil {
		t.Fatal(err)
	}
	server := <-accepted
	if server == nil {
		t.Fatal("server did not accept")
	}
	if err := raw.CloseWithError(99, "raw close"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-server.Done():
	case <-ctx.Done():
		t.Fatal("raw connection closure did not stop view")
	}
	_, err = server.OpenStream(ctx)
	if app, ok := errors.AsType[*quic.ApplicationError](err); !ok || app.ErrorCode != 99 || !app.Remote {
		t.Fatalf("raw connection closure = %v, want quic ApplicationError", err)
	}
	if errors.Is(err, transport.ErrViewClosed) {
		t.Fatalf("raw connection closure mapped to view closure: %v", err)
	}
}

func TestStatelessResetAfterEndpointRestart(t *testing.T) {
	ctx := transporttest.Context(t)
	key := testKey(t)
	packet, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := packet.LocalAddr()
	server, err := transport.NewEndpoint(key, packet, transport.Interop())
	if err != nil {
		t.Fatal(err)
	}
	_ = server.Libp2p()
	_ = server.Ethp2p()
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	client := transporttest.NewEndpoint(t)
	accepted := make(chan *transport.Conn, 1)
	go func() { conn, _ := server.Ethp2p().Accept(ctx); accepted <- conn }()
	conn, err := client.Eth.Dial(ctx, addr, server.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	if <-accepted == nil {
		t.Fatal("server did not accept")
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
	restarted, err := transport.NewEndpoint(key, restartedPacket, transport.Interop())
	if err != nil {
		t.Fatal(err)
	}
	_ = restarted.Libp2p()
	_ = restarted.Ethp2p()
	if err := restarted.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close(); _ = restartedPacket.Close() })
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
			// quic-go answers only packets longer than 42 bytes with a stateless
			// reset, so each tick sends a 128-byte payload.
			if s, err := conn.OpenUniStream(ctx); err == nil {
				_, _ = s.Write(bytes.Repeat([]byte{0x42}, 128))
			}
			select {
			case <-conn.Done():
				closed = true
			default:
			}
		case <-deadline.C:
			t.Fatal("stateless reset did not close the connection within 3s")
		}
	}
	_, err = conn.OpenStream(ctx)
	if _, ok := errors.AsType[*quic.StatelessResetError](err); !ok {
		t.Fatalf("connection ended with %v, want stateless reset", err)
	}
}

func testPacket(t *testing.T) net.PacketConn {
	t.Helper()
	p, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func testKey(t *testing.T) *identity.PrivKey {
	t.Helper()
	k, err := identity.GenPrivKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}
