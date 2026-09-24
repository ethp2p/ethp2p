package transport_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/transport"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
	libp2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/quic-go/quic-go"
)

const echoProtocol = "/ethp2p/interop/echo/1.0.0"

type node struct {
	host.Host
	shared *transport.SharedTransport
}

type ethPair struct {
	dialed, accepted transport.Conn
}

// The harness owns its nodes. Shared sockets bind the wildcard address for
// libp2p reuse; test dials target loopback.
type harness struct{ t *testing.T }

func newHarness(t *testing.T) *harness { return &harness{t: t} }

func (h *harness) context() (context.Context, context.CancelFunc) {
	return context.WithTimeout(h.t.Context(), 10*time.Second)
}

func (h *harness) listenUDP(ip net.IP) *net.UDPConn {
	h.t.Helper()
	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: ip})
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = udp.Close() })
	return udp
}

func (h *harness) quicAddr(addr net.Addr) ma.Multiaddr {
	h.t.Helper()
	base, err := manet.FromNetAddr(addr)
	if err != nil {
		h.t.Fatal(err)
	}
	return base.Encapsulate(ma.StringCast("/quic-v1"))
}

func (h *harness) ethp2pNode(options ...libp2p.Option) *node {
	h.t.Helper()
	key, err := transport.GenPrivKey()
	if err != nil {
		h.t.Fatal(err)
	}
	udp := h.listenUDP(net.IPv4zero)
	shared, err := transport.NewShared(key, udp, transport.Interop())
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = shared.Close() })
	identity, err := crypto.UnmarshalSecp256k1PrivateKey(key.Bytes())
	if err != nil {
		h.t.Fatal(err)
	}
	manager, err := quicreuse.NewConnManager(quic.StatelessResetKey{}, quic.TokenGeneratorKey{})
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = manager.Close() })
	if _, err := manager.LendTransport("udp4", shared.Libp2p(), udp); err != nil {
		h.t.Fatal(err)
	}
	// Register ethp2p interest as a production node would, so assertNoEthp2p is meaningful.
	if err := shared.Ethp2p().SetHello(transport.Hello{}); err != nil {
		h.t.Fatal(err)
	}
	opts := []libp2p.Option{
		libp2p.Identity(identity),
		libp2p.NoTransports,
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.QUICReuse(func() *quicreuse.ConnManager { return manager }),
		libp2p.ListenAddrs(h.quicAddr(udp.LocalAddr())),
	}
	lib, err := libp2p.New(append(opts, options...)...)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = lib.Close() })
	return &node{Host: lib, shared: shared}
}

func (h *harness) libp2pNode(options ...libp2p.Option) *node {
	h.t.Helper()
	key, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, -1)
	if err != nil {
		h.t.Fatal(err)
	}
	opts := []libp2p.Option{
		libp2p.Identity(key),
		libp2p.NoTransports,
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.ListenAddrs(h.quicAddr(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})),
	}
	lib, err := libp2p.New(append(opts, options...)...)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = lib.Close() })
	return &node{Host: lib}
}

func (h *harness) loopbackQUICAddr(n *node) ma.Multiaddr {
	h.t.Helper()
	for _, addr := range n.Addrs() {
		if _, err := addr.ValueForProtocol(ma.P_QUIC_V1); err != nil {
			continue
		}
		ip, err := manet.ToIP(addr)
		if err == nil && ip.Equal(net.IPv4(127, 0, 0, 1)) {
			return addr
		}
	}
	h.t.Fatalf("host %s has no loopback QUIC address", n.ID())
	return nil
}

func (h *harness) tryConnectLibp2p(ctx context.Context, from, to *node) error {
	return from.Connect(ctx, peer.AddrInfo{ID: to.ID(), Addrs: []ma.Multiaddr{h.loopbackQUICAddr(to)}})
}

func (h *harness) connectLibp2p(from, to *node) {
	h.t.Helper()
	ctx, cancel := h.context()
	defer cancel()
	if err := h.tryConnectLibp2p(ctx, from, to); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) ethp2pAddr(n *node) *net.UDPAddr {
	addr := *n.shared.Addr().(*net.UDPAddr)
	addr.IP = net.IPv4(127, 0, 0, 1)
	return &addr
}

// Accept runs alongside Dial so nodes without a libp2p listener start their
// ethp2p listener before the dial completes. The accept worker is always joined.
func (h *harness) connectEthp2p(from, to *node) ethPair {
	h.t.Helper()
	ctx, cancel := h.context()
	defer cancel()
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	type accepted struct {
		conn transport.Conn
		err  error
	}
	result := make(chan accepted, 1)
	go func() {
		conn, err := to.shared.Ethp2p().Accept(ctx)
		result <- accepted{conn, err}
	}()
	dialed, dialErr := from.shared.Ethp2p().Dial(ctx, h.ethp2pAddr(to), to.shared.PeerID())
	if dialErr != nil {
		stop()
	}
	got := <-result
	if err := errors.Join(dialErr, got.err); err != nil {
		if dialed != nil {
			_ = dialed.Close()
		}
		if got.conn != nil {
			_ = got.conn.Close()
		}
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() {
		_ = dialed.Close()
		_ = got.conn.Close()
	})
	return ethPair{dialed: dialed, accepted: got.conn}
}

func (h *harness) serveEcho(n *node, protocols ...string) {
	h.t.Helper()
	for _, name := range protocols {
		n.SetStreamHandler(protocol.ID(name), func(stream network.Stream) {
			defer stream.Close()
			payload, err := io.ReadAll(stream)
			if err != nil {
				_ = stream.Reset()
				return
			}
			if _, err := stream.Write(payload); err != nil {
				_ = stream.Reset()
				return
			}
			_ = stream.CloseWrite()
		})
	}
}

func (h *harness) exchange(ctx context.Context, from, to *node, name string, payload []byte) error {
	stream, err := from.NewStream(ctx, to.ID(), protocol.ID(name))
	if err != nil {
		return err
	}
	defer stream.Close()
	if _, err := stream.Write(payload); err != nil {
		return err
	}
	if err := stream.CloseWrite(); err != nil {
		return err
	}
	reply, err := io.ReadAll(stream)
	if err != nil {
		return err
	}
	if !bytes.Equal(reply, payload) {
		return fmt.Errorf("reply = %q, want %q", reply, payload)
	}
	return nil
}

func (h *harness) echo(from, to *node, name string, payload []byte) {
	h.t.Helper()
	ctx, cancel := h.context()
	defer cancel()
	if err := h.exchange(ctx, from, to, name, payload); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) exchangeEth(ctx context.Context, pair ethPair, payload []byte) error {
	out, err := pair.dialed.OpenStream(ctx, 1)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := out.Write(payload); err != nil {
		return err
	}
	in, selector, err := pair.accepted.AcceptStream(ctx)
	if err != nil {
		return err
	}
	if selector != 1 {
		return fmt.Errorf("selector = %d, want 1", selector)
	}
	want := payload
	got := make([]byte, len(want))
	if _, err := io.ReadFull(in, got); err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("ethp2p payload = %q, want %q", got, want)
	}
	return nil
}

func (h *harness) echoEth(pair ethPair, payload []byte) {
	h.t.Helper()
	ctx, cancel := h.context()
	defer cancel()
	if err := h.exchangeEth(ctx, pair, payload); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) waitConnected(a, b *node) {
	h.t.Helper()
	ctx, cancel := h.context()
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if a.Network().Connectedness(b.ID()) == network.Connected && b.Network().Connectedness(a.ID()) == network.Connected {
			return
		}
		select {
		case <-ctx.Done():
			h.t.Fatalf("hosts %s and %s did not connect: %v", a.ID(), b.ID(), ctx.Err())
		case <-ticker.C:
		}
	}
}

func (h *harness) newPeerID() peer.ID {
	h.t.Helper()
	key, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, -1)
	if err != nil {
		h.t.Fatal(err)
	}
	id, err := peer.IDFromPrivateKey(key)
	if err != nil {
		h.t.Fatal(err)
	}
	return id
}

func (h *harness) rawQUICServer(alpn ...string) (peer.ID, ma.Multiaddr) {
	h.t.Helper()
	udp := h.listenUDP(net.IPv4(127, 0, 0, 1))
	key, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, -1)
	if err != nil {
		h.t.Fatal(err)
	}
	id, err := peer.IDFromPrivateKey(key)
	if err != nil {
		h.t.Fatal(err)
	}
	identity, err := libp2ptls.NewIdentity(key)
	if err != nil {
		h.t.Fatal(err)
	}
	serverTLS, _ := identity.ConfigForPeer("")
	serverTLS.NextProtos = alpn
	server := &quic.Transport{Conn: udp}
	h.t.Cleanup(func() { _ = server.Close() })
	listener, err := server.Listen(serverTLS, &quic.Config{})
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = listener.Close() })
	return id, h.quicAddr(udp.LocalAddr())
}

func (h *harness) assertNoEthp2p(n *node) {
	h.t.Helper()
	if got := transport.PendingEthp2p(n.shared); got != 0 {
		h.t.Fatalf("unexpected %d queued ethp2p connections", got)
	}
}

func TestInteropRealLibp2pExchange(t *testing.T) {
	for _, tc := range []struct {
		name        string
		libp2pDials bool
	}{{name: "ethp2p host dials"}, {name: "libp2p host dials", libp2pDials: true}} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			eth, lib := h.ethp2pNode(), h.libp2pNode()
			longProtocol := "/" + strings.Repeat("selector", 20)
			h.serveEcho(eth, echoProtocol, longProtocol)
			h.serveEcho(lib, echoProtocol, longProtocol)
			from, to := eth, lib
			if tc.libp2pDials {
				from, to = lib, eth
			}
			h.connectLibp2p(from, to)
			h.echo(eth, lib, echoProtocol, []byte("shared to stock"))
			h.echo(lib, eth, echoProtocol, []byte("stock to shared"))
			h.echo(eth, lib, longProtocol, []byte("multibyte selector"))
			h.assertNoEthp2p(eth)
		})
	}
}

func TestInteropWrongPeerID(t *testing.T) {
	for _, tc := range []struct {
		name        string
		libp2pDials bool
	}{{name: "ethp2p host dials"}, {name: "libp2p host dials", libp2pDials: true}} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			eth, lib := h.ethp2pNode(), h.libp2pNode()
			from, to := eth, lib
			if tc.libp2pDials {
				from, to = lib, eth
			}
			ctx, cancel := h.context()
			defer cancel()
			err := from.Connect(ctx, peer.AddrInfo{ID: h.newPeerID(), Addrs: []ma.Multiaddr{h.loopbackQUICAddr(to)}})
			if err == nil {
				t.Fatal("connection with the wrong peer ID succeeded")
			}
			if got := len(eth.Network().Conns()); got != 0 {
				t.Fatalf("shared host retained %d connections after rejection", got)
			}
		})
	}
}

func TestInteropParallelLibp2pStreams(t *testing.T) {
	h := newHarness(t)
	client, server := h.ethp2pNode(), h.libp2pNode()
	h.serveEcho(server, echoProtocol)
	h.connectLibp2p(client, server)
	const streams = 16
	errs := make(chan error, streams)
	for i := range streams {
		go func() {
			ctx, cancel := h.context()
			defer cancel()
			errs <- h.exchange(ctx, client, server, echoProtocol, []byte(fmt.Sprintf("stream %d", i)))
		}()
	}
	for range streams {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
}

func TestInteropLibp2pDialStaysLibp2p(t *testing.T) {
	h := newHarness(t)
	left, right := h.ethp2pNode(libp2p.NoListenAddrs), h.ethp2pNode()
	h.serveEcho(right, echoProtocol)
	h.connectLibp2p(left, right)
	h.echo(left, right, echoProtocol, []byte("libp2p on shared QUIC"))
	h.assertNoEthp2p(left)
	h.assertNoEthp2p(right)
}

func TestInteropConnectionWindowGrows(t *testing.T) {
	h := newHarness(t)
	client, server := h.ethp2pNode(libp2p.NoListenAddrs), h.ethp2pNode()
	h.serveEcho(server, echoProtocol)
	h.connectLibp2p(client, server)
	h.echo(client, server, echoProtocol, make([]byte, 4<<20))
}

func TestInteropMixedTrafficOnEthp2pDial(t *testing.T) {
	h := newHarness(t)
	left, right := h.ethp2pNode(), h.ethp2pNode()
	h.serveEcho(right, echoProtocol)
	pair := h.connectEthp2p(left, right)
	h.waitConnected(left, right)
	ctx, cancel := h.context()
	defer cancel()
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := h.exchange(ctx, left, right, echoProtocol, []byte("libp2p after ethp2p dial")); err != nil {
			t.Error(err)
		}
	})
	wg.Go(func() {
		if err := h.exchangeEth(ctx, pair, []byte("ethp2p initiated")); err != nil {
			t.Error(err)
		}
	})
	wg.Wait()
}

func TestInteropEthp2pWithoutLibp2pListener(t *testing.T) {
	h := newHarness(t)
	left, right := h.ethp2pNode(libp2p.NoListenAddrs), h.ethp2pNode(libp2p.NoListenAddrs)
	h.echoEth(h.connectEthp2p(left, right), []byte("no libp2p listener"))
}

func TestInteropMalformedSelectorsRecover(t *testing.T) {
	tests := []struct {
		name string
		wire []byte
	}{
		{name: "empty frame", wire: []byte{0}},
		{name: "unterminated length", wire: []byte{0x80}},
		{name: "missing selector", wire: []byte{1}},
		{name: "overlong length", wire: []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newHarness(t)
			client, server := h.ethp2pNode(libp2p.NoListenAddrs), h.ethp2pNode(libp2p.NoListenAddrs)
			pair := h.connectEthp2p(client, server)
			ctx, cancel := h.context()
			defer cancel()
			stream, err := transport.RawOpenStream(ctx, pair.dialed)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Write(test.wire); err != nil {
				t.Fatal(err)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			h.echoEth(pair, []byte("recovered"))
		})
	}
}

func TestInteropStalledSelectorRecovers(t *testing.T) {
	h := newHarness(t)
	client, server := h.ethp2pNode(libp2p.NoListenAddrs), h.ethp2pNode(libp2p.NoListenAddrs)
	pair := h.connectEthp2p(client, server)
	ctx, cancel := h.context()
	defer cancel()
	stream, err := transport.RawOpenStream(ctx, pair.dialed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte{0x80}); err != nil {
		t.Fatal(err)
	}
	if err := stream.SetReadDeadline(time.Now().Add(7 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Read(make([]byte, 1)); err == nil {
		t.Fatal("stalled selector was not reset")
	} else if reset, ok := errors.AsType[*quic.StreamError](err); !ok || !reset.Remote || uint64(reset.ErrorCode) != 6 {
		t.Fatalf("stalled selector reset = %v, want remote wire 6", err)
	}
	h.echoEth(pair, []byte("after timeout"))
}

func TestInteropPeerClosureWhileClassifying(t *testing.T) {
	h := newHarness(t)
	client, server := h.ethp2pNode(libp2p.NoListenAddrs), h.ethp2pNode(libp2p.NoListenAddrs)
	first := h.connectEthp2p(client, server)
	ctx, cancel := h.context()
	defer cancel()
	stream, err := transport.RawOpenStream(ctx, first.dialed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte{0x80}); err != nil {
		t.Fatal(err)
	}
	if err := first.dialed.Close(); err != nil {
		t.Fatal(err)
	}
	h.echoEth(h.connectEthp2p(client, server), []byte("new connection"))
}

func TestInteropNoCommonALPN(t *testing.T) {
	t.Run("ethp2p host dials", func(t *testing.T) {
		h := newHarness(t)
		id, addr := h.rawQUICServer("h3")
		client := h.ethp2pNode()
		ctx, cancel := h.context()
		defer cancel()
		if err := client.Connect(ctx, peer.AddrInfo{ID: id, Addrs: []ma.Multiaddr{addr}}); err == nil {
			t.Fatal("dial without a common ALPN succeeded")
		}
	})
	t.Run("raw peer dials ethp2p host", func(t *testing.T) {
		h := newHarness(t)
		server := h.ethp2pNode()
		clientTLS := &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true, NextProtos: []string{"h3"}}
		ctx, cancel := h.context()
		defer cancel()
		addr, _, err := quicreuse.FromQuicMultiaddr(h.loopbackQUICAddr(server))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := quic.DialAddr(ctx, addr.String(), clientTLS, &quic.Config{}); err == nil {
			t.Fatal("inbound dial without a common ALPN succeeded")
		}
	})
}

func TestInteropDialTimeout(t *testing.T) {
	h := newHarness(t)
	client := h.ethp2pNode()
	id := h.newPeerID()
	addr := h.quicAddr(h.listenUDP(net.IPv4(127, 0, 0, 1)).LocalAddr())
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := client.Connect(ctx, peer.AddrInfo{ID: id, Addrs: []ma.Multiaddr{addr}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed-out dial error = %v, want context deadline", err)
	}
}

func TestInteropUnconsumedEthp2pKeepsLibp2p(t *testing.T) {
	h := newHarness(t)
	server := h.ethp2pNode()
	h.serveEcho(server, echoProtocol)
	const clients = 20
	var dialed []transport.Conn
	t.Cleanup(func() {
		for _, conn := range dialed {
			_ = conn.Close()
		}
	})
	for i := range clients {
		client := h.ethp2pNode()
		ctx, cancel := h.context()
		conn, err := client.shared.Ethp2p().Dial(ctx, h.ethp2pAddr(server), server.shared.PeerID())
		cancel()
		if err != nil {
			t.Fatalf("client %d ethp2p dial: %v", i, err)
		}
		dialed = append(dialed, conn)
		h.waitConnected(client, server)
		h.echo(client, server, echoProtocol, []byte(fmt.Sprintf("client %d", i)))
	}
	if got := transport.PendingEthp2p(server.shared); got != 16 {
		t.Fatalf("pending ethp2p connections = %d, want 16", got)
	}
}

func TestInteropParallelDials(t *testing.T) {
	h := newHarness(t)
	server := h.ethp2pNode()
	h.serveEcho(server, echoProtocol)
	const clients = 8
	dialers := make([]*node, clients)
	for i := range dialers {
		dialers[i] = h.libp2pNode()
	}
	errs := make(chan error, clients)
	for i, client := range dialers {
		go func() {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			if err := h.tryConnectLibp2p(ctx, client, server); err != nil {
				errs <- fmt.Errorf("client %d connect: %w", i, err)
				return
			}
			errs <- h.exchange(ctx, client, server, echoProtocol, []byte(fmt.Sprintf("parallel client %d", i)))
		}()
	}
	for range clients {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
	for _, client := range dialers {
		if len(server.Network().ConnsToPeer(client.ID())) == 0 {
			t.Fatalf("server did not retain a connection from %s", client.ID())
		}
	}
}
