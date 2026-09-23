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

	transport "github.com/ethp2p/ethp2p/transport"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	libp2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	varint "github.com/multiformats/go-varint"
	"github.com/quic-go/quic-go"
)

func TestInteropRealLibp2pExchange(t *testing.T) {
	for _, family := range []ipFamily{ipv4, ipv6} {
		t.Run(string(family), func(t *testing.T) {
			if family == ipv6 {
				probe, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
				if err != nil {
					t.Skipf("IPv6 loopback unavailable: %v", err)
				}
				_ = probe.Close()
			}
			for _, direction := range []dialDirection{sharedDialsStock, stockDialsShared} {
				t.Run(string(direction), func(t *testing.T) {
					shared := newSharedHostFamily(t, family, withListener)
					stock := newQUICHostFamily(t, family)
					longProtocol := interopProtocol("/" + strings.Repeat("selector", 20))
					serveEcho(shared.host, echoProtocol)
					serveEcho(stock, echoProtocol)
					serveEcho(shared.host, longProtocol)
					serveEcho(stock, longProtocol)

					switch direction {
					case sharedDialsStock:
						connectHosts(t, shared.host, stock)
					case stockDialsShared:
						connectHosts(t, stock, shared.host)
					}

					exchange(t, shared.host, stock, echoProtocol, interopPayload("shared to stock"))
					exchange(t, stock, shared.host, echoProtocol, interopPayload("stock to shared"))
					exchange(t, shared.host, stock, longProtocol, interopPayload("multibyte selector"))
					assertNoEthp2p(t, shared.eth)
				})
			}
		})
	}
}

func TestInteropWrongPeerID(t *testing.T) {
	for _, direction := range []dialDirection{sharedDialsStock, stockDialsShared} {
		t.Run(string(direction), func(t *testing.T) {
			shared := newSharedHost(t)
			stock := newQUICHost(t)
			wrongID := newPeerID(t)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			var err error
			switch direction {
			case sharedDialsStock:
				err = connectHostsContext(ctx, shared.host, wrongID, []ma.Multiaddr{loopbackQUICAddr(t, stock)})
			case stockDialsShared:
				err = connectHostsContext(ctx, stock, wrongID, []ma.Multiaddr{loopbackQUICAddr(t, shared.host)})
			}
			if err == nil {
				t.Fatal("connection with the wrong peer ID succeeded")
			}
			if got := len(shared.host.Network().Conns()); got != 0 {
				t.Fatalf("shared host retained %d connections after rejection", got)
			}
		})
	}
}

func TestInteropParallelLibp2pStreams(t *testing.T) {
	shared := newSharedHost(t)
	stock := newQUICHost(t)
	serveEcho(stock, echoProtocol)
	connectHosts(t, shared.host, stock)

	const streams = 16
	errs := make(chan error, streams)
	for i := range streams {
		go func() {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			errs <- exchangeContext(ctx, shared.host, stock.ID(), echoProtocol, interopPayload(fmt.Sprintf("stream %d", i)))
		}()
	}
	for range streams {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func TestInteropLibp2pDialStaysLibp2p(t *testing.T) {
	left := newSharedHostMode(t, withoutListener)
	right := newSharedHost(t)
	serveEcho(right.host, echoProtocol)
	connectHosts(t, left.host, right.host)

	// A libp2p-initiated connection serves the libp2p stack only: neither
	// side's ethp2p queue receives a view.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := exchangeContext(ctx, left.host, right.host.ID(), echoProtocol, interopPayload("libp2p on shared QUIC")); err != nil {
		t.Fatal(err)
	}
	assertNoEthp2p(t, left.eth)
	assertNoEthp2p(t, right.eth)
}

func TestInteropConnectionWindowGrows(t *testing.T) {
	client := newSharedHostMode(t, withoutListener)
	server := newSharedHost(t)
	serveEcho(server.host, echoProtocol)
	connectHosts(t, client.host, server.host)

	const payloadSize = 4 << 20
	exchange(t, client.host, server.host, echoProtocol, make(interopPayload, payloadSize))
}

func TestInteropMixedTrafficOnEthp2pDial(t *testing.T) {
	left := newSharedHost(t)
	right := newSharedHost(t)
	serveEcho(right.host, echoProtocol)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	pair := connectEth(t, left.eth, right.eth, right.udp.LocalAddr())
	waitConnected(t, left.host, right.host)

	// Drive both views of the shared connection at the same time.
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := exchangeContext(ctx, left.host, right.host.ID(), echoProtocol, interopPayload("libp2p after ethp2p dial")); err != nil {
			t.Error(err)
		}
	})
	wg.Go(func() {
		if err := exchangeEth(ctx, pair.dialed, pair.accepted, ethPayload("ethp2p initiated")); err != nil {
			t.Error(err)
		}
	})
	wg.Wait()
}

func TestInteropEthp2pWithoutLibp2pListener(t *testing.T) {
	left := newSharedHostMode(t, withoutListener)
	right := newSharedHostMode(t, withoutListener)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	pair := connectEth(t, left.eth, right.eth, right.udp.LocalAddr())
	if err := exchangeEth(ctx, pair.dialed, pair.accepted, ethPayload("no libp2p listener")); err != nil {
		t.Fatal(err)
	}
}

func TestInteropMalformedSelectorsRecover(t *testing.T) {
	tests := []struct {
		name string
		wire selectorWire
	}{
		{name: "empty frame", wire: selectorWire{0}},
		{name: "unterminated length", wire: selectorWire{0x80}},
		{name: "missing selector", wire: selectorWire{1}},
		{name: "overlong length", wire: selectorWire{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newSharedHostMode(t, withoutListener)
			server := newSharedHostMode(t, withoutListener)
			pair := connectEth(t, client.eth, server.eth, server.udp.LocalAddr())

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			stream, err := pair.dialed.OpenStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Write(test.wire); err != nil {
				t.Fatal(err)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if err := exchangeEth(ctx, pair.dialed, pair.accepted, ethPayload("recovered")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInteropStalledSelectorRecovers(t *testing.T) {
	client := newSharedHostMode(t, withoutListener)
	server := newSharedHostMode(t, withoutListener)
	pair := connectEth(t, client.eth, server.eth, server.udp.LocalAddr())
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	stream, err := pair.dialed.OpenStream(ctx)
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
	}
	if err := exchangeEth(ctx, pair.dialed, pair.accepted, ethPayload("after timeout")); err != nil {
		t.Fatal(err)
	}
}

func TestInteropPeerClosureWhileClassifying(t *testing.T) {
	client := newSharedHostMode(t, withoutListener)
	server := newSharedHostMode(t, withoutListener)
	first := connectEth(t, client.eth, server.eth, server.udp.LocalAddr())
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	stream, err := first.dialed.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte{0x80}); err != nil {
		t.Fatal(err)
	}
	if err := first.dialed.Close(); err != nil {
		t.Fatal(err)
	}
	second := connectEth(t, client.eth, server.eth, server.udp.LocalAddr())
	if err := exchangeEth(ctx, second.dialed, second.accepted, ethPayload("new connection")); err != nil {
		t.Fatal(err)
	}
}

func TestInteropNoCommonALPN(t *testing.T) {
	t.Run("shared host dials", func(t *testing.T) {
		serverUDP := listenInteropUDP(t, ipv4)
		key, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, -1)
		if err != nil {
			t.Fatal(err)
		}
		id, err := peer.IDFromPrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		identity, err := libp2ptls.NewIdentity(key)
		if err != nil {
			t.Fatal(err)
		}
		serverTLS, _ := identity.ConfigForPeer("")
		serverTLS.NextProtos = []string{"h3"}
		transport := &quic.Transport{Conn: serverUDP}
		t.Cleanup(func() { _ = transport.Close() })
		listener, err := transport.Listen(serverTLS, &quic.Config{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = listener.Close() })

		shared := newSharedHost(t)
		addr := ma.StringCast(fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic-v1", serverUDP.LocalAddr().(*net.UDPAddr).Port))
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		err = connectHostsContext(ctx, shared.host, id, []ma.Multiaddr{addr})
		if err == nil {
			t.Fatal("dial without a common ALPN succeeded")
		}
	})

	t.Run("raw peer dials shared host", func(t *testing.T) {
		shared := newSharedHost(t)
		clientTLS := &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true, NextProtos: []string{"h3"}}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		_, err := quic.DialAddr(ctx, hostUDPAddr(t, shared.host).String(), clientTLS, &quic.Config{})
		if err == nil {
			t.Fatal("inbound dial without a common ALPN succeeded")
		}
	})
}

func TestInteropDialTimeout(t *testing.T) {
	shared := newSharedHost(t)
	id := newPeerID(t)
	blackhole := listenInteropUDP(t, ipv4)
	addr := ma.StringCast(fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic-v1", blackhole.LocalAddr().(*net.UDPAddr).Port))

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := connectHostsContext(ctx, shared.host, id, []ma.Multiaddr{addr})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed-out dial error = %v, want context deadline", err)
	}
}

func TestInteropLibp2pWithUnconsumedEthp2p(t *testing.T) {
	server := newSharedHost(t)
	serveEcho(server.host, echoProtocol)
	const clients = 20
	for i := range clients {
		client := newSharedHost(t)
		connectHosts(t, client.host, server.host)
		exchange(t, client.host, server.host, echoProtocol, interopPayload(fmt.Sprintf("client %d", i)))
	}
}

func TestInteropParallelDials(t *testing.T) {
	server := newSharedHost(t)
	serveEcho(server.host, echoProtocol)
	const clients = 8
	stocks := make([]host.Host, clients)
	for i := range stocks {
		stocks[i] = newQUICHost(t)
	}
	errs := make(chan error, clients)
	for i, stock := range stocks {
		go func() {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			if err := connectHostsContext(ctx, stock, server.host.ID(), []ma.Multiaddr{loopbackQUICAddr(t, server.host)}); err != nil {
				errs <- fmt.Errorf("client %d connect: %w", i, err)
				return
			}
			errs <- exchangeContext(ctx, stock, server.host.ID(), echoProtocol, interopPayload(fmt.Sprintf("parallel client %d", i)))
		}()
	}
	for range clients {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	for _, stock := range stocks {
		if len(server.host.Network().ConnsToPeer(stock.ID())) == 0 {
			t.Fatalf("server did not retain a connection from %s", stock.ID())
		}
	}
}

type interopProtocol string

const echoProtocol interopProtocol = "/ethp2p/interop/echo/1.0.0"

type interopPayload []byte

type ethPayload []byte

type selectorWire []byte

type dialDirection string

const (
	sharedDialsStock dialDirection = "shared dials stock"
	stockDialsShared dialDirection = "stock dials shared"
)

type listenMode bool

const (
	withoutListener listenMode = false
	withListener    listenMode = true
)

type ipFamily string

const (
	ipv4 ipFamily = "udp4"
	ipv6 ipFamily = "udp6"
)

type sharedHost struct {
	host     host.Host
	shared   *transport.SharedTransport
	lib      quicreuse.QUICTransport
	eth      *transport.Ethp2pTransport
	udp      *net.UDPConn
	manager  *quicreuse.ConnManager
	identity crypto.PrivKey
	family   ipFamily
}

type ethPair struct {
	dialed   transport.Conn
	accepted transport.Conn
}

func newSharedHost(t *testing.T) *sharedHost {
	return newSharedHostFamily(t, ipv4, withListener)
}

func newSharedHostMode(t *testing.T, mode listenMode) *sharedHost {
	return newSharedHostFamily(t, ipv4, mode)
}

func newSharedHostFamily(t *testing.T, family ipFamily, mode listenMode) *sharedHost {
	t.Helper()
	key := testKey(t)
	udp := listenInteropUDP(t, family)
	shared, err := transport.NewShared(key, udp, transport.Interop())
	if err != nil {
		t.Fatal(err)
	}
	identity, err := crypto.UnmarshalSecp256k1PrivateKey(key.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	peer := &sharedHost{shared: shared, lib: shared.Libp2p(), eth: shared.Ethp2p(), udp: udp, identity: identity, family: family}
	peer.startLibp2p(t, mode)
	// Teardown mirrors the application contract: shared transport first
	// (ends listening and kills the endpoint), then libp2p.
	t.Cleanup(func() {
		_ = peer.shared.Close()
		_ = peer.host.Close()
		_ = peer.manager.Close()
	})
	return peer
}

func (p *sharedHost) startLibp2p(t *testing.T, mode listenMode) {
	t.Helper()
	manager, err := quicreuse.NewConnManager(quic.StatelessResetKey{}, quic.TokenGeneratorKey{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.LendTransport(string(p.family), p.lib, p.udp); err != nil {
		t.Fatal(err)
	}
	port := p.udp.LocalAddr().(*net.UDPAddr).Port
	listenAddr := fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", port)
	if p.family == ipv6 {
		listenAddr = fmt.Sprintf("/ip6/::/udp/%d/quic-v1", port)
	}
	options := []libp2p.Option{
		libp2p.Identity(p.identity),
		libp2p.NoTransports,
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.QUICReuse(func() *quicreuse.ConnManager { return manager }),
	}
	if mode == withListener {
		options = append(options, libp2p.ListenAddrStrings(listenAddr))
	} else {
		options = append(options, libp2p.NoListenAddrs)
	}
	h, err := libp2p.New(options...)
	if err != nil {
		t.Fatal(err)
	}
	p.host = h
	p.manager = manager
}

func newQUICHost(t *testing.T) host.Host {
	return newQUICHostFamily(t, ipv4)
}

func newQUICHostFamily(t *testing.T, family ipFamily) host.Host {
	t.Helper()
	key, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, -1)
	if err != nil {
		t.Fatal(err)
	}
	listenAddr := "/ip4/127.0.0.1/udp/0/quic-v1"
	if family == ipv6 {
		listenAddr = "/ip6/::1/udp/0/quic-v1"
	}
	h, err := libp2p.New(
		libp2p.Identity(key),
		libp2p.NoTransports,
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.ListenAddrStrings(listenAddr),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func listenInteropUDP(t *testing.T, family ipFamily) *net.UDPConn {
	t.Helper()
	addr := &net.UDPAddr{IP: net.IPv4zero}
	if family == ipv6 {
		addr = &net.UDPAddr{IP: net.IPv6zero}
	}
	udp, err := net.ListenUDP(string(family), addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = udp.Close() })
	return udp
}

func connectHosts(t *testing.T, dialer, listener host.Host) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := connectHostsContext(ctx, dialer, listener.ID(), []ma.Multiaddr{loopbackQUICAddr(t, listener)}); err != nil {
		t.Fatal(err)
	}
}

func connectHostsContext(ctx context.Context, dialer host.Host, id peer.ID, addrs []ma.Multiaddr) error {
	return dialer.Connect(ctx, peer.AddrInfo{ID: id, Addrs: addrs})
}

func serveEcho(h host.Host, name interopProtocol) {
	h.SetStreamHandler(protocol.ID(name), func(stream network.Stream) {
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

func exchange(t *testing.T, dialer, listener host.Host, name interopProtocol, payload interopPayload) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := exchangeContext(ctx, dialer, listener.ID(), name, payload); err != nil {
		t.Fatal(err)
	}
}

func exchangeContext(ctx context.Context, dialer host.Host, listener peer.ID, name interopProtocol, payload interopPayload) error {
	stream, err := dialer.NewStream(ctx, listener, protocol.ID(name))
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

func connectEth(t *testing.T, dialer *transport.Ethp2pTransport, listener *transport.Ethp2pTransport, addr net.Addr) ethPair {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	type acceptResult struct {
		conn transport.Conn
		err  error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		conn, err := listener.Accept(ctx)
		accepted <- acceptResult{conn: conn, err: err}
	}()
	dialed, err := dialer.Dial(ctx, addr, listener.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	result := <-accepted
	if result.err != nil {
		t.Fatal(result.err)
	}
	return ethPair{dialed: dialed, accepted: result.conn}
}

func exchangeEth(ctx context.Context, sender, receiver transport.Conn, payload ethPayload) error {
	wire := append(frame([]byte{1}), payload...)
	out, err := sender.OpenStream(ctx)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := out.Write(wire); err != nil {
		return err
	}
	in, err := receiver.AcceptBiStream(ctx)
	if err != nil {
		return err
	}
	got := make([]byte, len(wire))
	if _, err := io.ReadFull(in, got); err != nil {
		return err
	}
	if !bytes.Equal(got, wire) {
		return fmt.Errorf("ethp2p payload = %q, want %q", got, wire)
	}
	return nil
}

func waitConnected(t *testing.T, left, right host.Host) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if left.Network().Connectedness(right.ID()) == network.Connected && right.Network().Connectedness(left.ID()) == network.Connected {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("hosts %s and %s did not connect: %v", left.ID(), right.ID(), ctx.Err())
		case <-ticker.C:
		}
	}
}

func loopbackQUICAddr(t *testing.T, h host.Host) ma.Multiaddr {
	t.Helper()
	var fallback ma.Multiaddr
	for _, addr := range h.Addrs() {
		if _, err := addr.ValueForProtocol(ma.P_QUIC_V1); err != nil {
			continue
		}
		if fallback == nil {
			fallback = addr
		}
		ip, err := manet.ToIP(addr)
		if err == nil && net.IP(ip).IsLoopback() {
			return addr
		}
	}
	if fallback != nil {
		return fallback
	}
	t.Fatal("host has no QUIC address")
	return nil
}

func hostUDPAddr(t *testing.T, h host.Host) *net.UDPAddr {
	t.Helper()
	udp, _, err := quicreuse.FromQuicMultiaddr(loopbackQUICAddr(t, h))
	if err != nil {
		t.Fatal(err)
	}
	return udp
}

func testKey(t *testing.T) *transport.PrivKey {
	t.Helper()
	key, err := transport.GenPrivKey()
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func newPeerID(t *testing.T) peer.ID {
	t.Helper()
	return peer.ID(testKey(t).Public().PeerID())
}

func frame(payload []byte) []byte {
	return append(varint.ToUvarint(uint64(len(payload))), payload...)
}

func assertNoEthp2p(t *testing.T, eth *transport.Ethp2pTransport) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if conn, err := eth.Accept(ctx); err == nil {
		_ = conn.Close()
		t.Fatal("unexpected ethp2p connection")
	}
}
