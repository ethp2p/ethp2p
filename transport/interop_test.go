package transport_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/ethp2p/ethp2p/wire"
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
	endpoint *transport.Endpoint
}

type ethPair struct {
	dialed, accepted *transport.Conn
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
	key, err := identity.GenPrivKey()
	if err != nil {
		h.t.Fatal(err)
	}
	udp := h.listenUDP(net.IPv4zero)
	endpoint, err := transport.NewEndpoint(key, udp, transport.Interop())
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = endpoint.Close() })
	libp2pKey, err := crypto.UnmarshalSecp256k1PrivateKey(key.Bytes())
	if err != nil {
		h.t.Fatal(err)
	}
	manager, err := quicreuse.NewConnManager(quic.StatelessResetKey{}, quic.TokenGeneratorKey{})
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = manager.Close() })
	if _, err := manager.LendTransport("udp4", endpoint.Libp2p(), udp); err != nil {
		h.t.Fatal(err)
	}
	// Register ethp2p interest as a production node would, so assertNoEthp2p is meaningful.
	endpoint.Ethp2p()
	opts := []libp2p.Option{
		libp2p.Identity(libp2pKey),
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
	if err := endpoint.Start(); err != nil {
		h.t.Fatal(err)
	}
	return &node{Host: lib, endpoint: endpoint}
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
	addr := *n.endpoint.Addr().(*net.UDPAddr)
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
		conn *transport.Conn
		err  error
	}
	result := make(chan accepted, 1)
	go func() {
		conn, err := to.endpoint.Ethp2p().Accept(ctx)
		result <- accepted{conn, err}
	}()
	dialed, dialErr := from.endpoint.Ethp2p().Dial(ctx, h.ethp2pAddr(to), to.endpoint.PeerID())
	if dialErr != nil {
		stop()
	}
	got := <-result
	if err := errors.Join(dialErr, got.err); err != nil {
		if dialed != nil {
			dialed.Close()
		}
		if got.conn != nil {
			got.conn.Close()
		}
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() {
		dialed.Close()
		got.conn.Close()
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
	out, err := transporttest.OpenStream(ctx, pair.dialed, 1)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := out.Write(payload); err != nil {
		return err
	}
	in, protocol, err := transporttest.NextStream(ctx, pair.accepted)
	if err != nil {
		return err
	}
	if protocol != 1 {
		return fmt.Errorf("protocol = %v, want 1", protocol)
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
	if got := transport.PendingEthp2p(n.endpoint); got != 0 {
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
			h.echo(eth, lib, echoProtocol, make([]byte, 4<<20))
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
	first.dialed.Close()
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

func TestInteropUnconsumedEthp2pKeepsLibp2p(t *testing.T) {
	h := newHarness(t)
	server := h.ethp2pNode()
	h.serveEcho(server, echoProtocol)
	const clients = 17
	var dialed []*transport.Conn
	t.Cleanup(func() {
		for _, conn := range dialed {
			conn.Close()
		}
	})
	for i := range clients {
		client := h.ethp2pNode()
		ctx, cancel := h.context()
		conn, err := client.endpoint.Ethp2p().Dial(ctx, h.ethp2pAddr(server), server.endpoint.PeerID())
		cancel()
		if err != nil {
			if closed, ok := errors.AsType[*transport.ViewClosedError](err); !ok || !closed.Remote || closed.Code != wire.Unspecified {
				t.Fatalf("client %d ethp2p dial: %v", i, err)
			}
		} else {
			dialed = append(dialed, conn)
		}
		h.waitConnected(client, server)
		// Check the first and last queued views plus the overflow view; the
		// remaining clients only establish the queue-pressure state.
		if i == 0 || i == clients-2 || i == clients-1 {
			h.echo(client, server, echoProtocol, []byte(fmt.Sprintf("client %d", i)))
		}
	}
	waitPending := time.After(5 * time.Second)
	for transport.PendingEthp2p(server.endpoint) != 16 {
		select {
		case <-waitPending:
			t.Fatalf("pending ethp2p connections = %d, want 16", transport.PendingEthp2p(server.endpoint))
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// A shared connection's libp2p view queues streams the application has not
// accepted without a count of its own: each is an open QUIC stream, so the
// peer's stream limit bounds the queue. More than eight must arrive intact.
func TestInteropLibp2pStreamsQueueUntilAccepted(t *testing.T) {
	const streams = 12
	server := transporttest.NewEndpoint(t)
	listener, err := server.Endpoint.Libp2p().Listen(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	ctx := transporttest.Context(t)
	raw, err := transporttest.DialRaw(t, ctx, server.Endpoint.Addr(), server.Endpoint.PeerID(), &quic.Config{})
	if err != nil {
		t.Fatal(err)
	}
	view, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// A multistream-select header routes each stream to the libp2p view.
	head := append([]byte{19}, "/multistream/1.0.0\n"...)
	opened := make([]*quic.Stream, streams)
	for i := range opened {
		stream, err := raw.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Write(append(slices.Clone(head), byte(i))); err != nil {
			t.Fatal(err)
		}
		opened[i] = stream
	}
	// Accept nothing until every stream is classified, so the queue must hold
	// all of them at once.
	deadline := time.Now().Add(5 * time.Second)
	for transport.QueuedLibp2pStreams(view) != streams {
		if time.Now().After(deadline) {
			t.Fatalf("queued libp2p streams = %d, want %d", transport.QueuedLibp2pStreams(view), streams)
		}
		time.Sleep(time.Millisecond)
	}
	for range streams {
		stream, err := view.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(head)+1)
		if _, err := io.ReadFull(stream, got); err != nil || !bytes.Equal(got[:len(head)], head) {
			t.Fatalf("accepted stream head = %q, %v", got, err)
		}
		if _, err := stream.Write(got[len(head):]); err != nil {
			t.Fatal(err)
		}
	}
	// Each reply proves its stream survived queueing on both sides.
	for i, stream := range opened {
		_ = stream.SetReadDeadline(time.Now().Add(5 * time.Second))
		reply := make([]byte, 1)
		if _, err := io.ReadFull(stream, reply); err != nil || reply[0] != byte(i) {
			t.Fatalf("stream %d reply = %v, %v", i, reply, err)
		}
	}
}

// badCertificate returns a self-signed P-256 certificate carrying ext, if any,
// under the libp2p identity-binding extension ID.
func badCertificate(t *testing.T, ext func(certKey *ecdsa.PrivateKey) []byte) tls.Certificate {
	t.Helper()
	certKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	if ext != nil {
		template.ExtraExtensions = []pkix.Extension{{Id: asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 53594, 1, 1}, Value: ext(certKey)}}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, certKey.Public(), certKey)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: certKey}
}

// Certificate verification is the trust boundary for inbound handshakes. A
// dialer whose certificate does not bind a libp2p identity is refused with a
// TLS bad_certificate alert and never reaches a view.
func TestInteropInboundBadCertificateRejected(t *testing.T) {
	type signedKey struct{ PubKey, Signature []byte }
	for _, test := range []struct {
		name string
		ext  func(certKey *ecdsa.PrivateKey) []byte
	}{
		{"no identity extension", nil},
		{"unparseable identity extension", func(*ecdsa.PrivateKey) []byte { return []byte{0xde, 0xad} }},
		{"signature over another key", func(*ecdsa.PrivateKey) []byte {
			// The identity key signs a different certificate key.
			key := testKey(t)
			other, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			otherPub, err := x509.MarshalPKIXPublicKey(other.Public())
			if err != nil {
				t.Fatal(err)
			}
			value, err := asn1.Marshal(signedKey{
				PubKey:    key.Public().Marshal(),
				Signature: key.Sign(append([]byte("libp2p-tls-handshake:"), otherPub...)),
			})
			if err != nil {
				t.Fatal(err)
			}
			return value
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := transporttest.NewEndpoint(t)
			listener, err := server.Endpoint.Libp2p().Listen(nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			ctx := transporttest.Context(t)
			bad := &quic.Transport{Conn: testPacket(t)}
			t.Cleanup(func() { _ = bad.Close() })
			config := &tls.Config{
				MinVersion:         tls.VersionTLS13,
				InsecureSkipVerify: true,
				Certificates:       []tls.Certificate{badCertificate(t, test.ext)},
				NextProtos:         []string{transport.AlpnEthp2p, transport.AlpnLibp2p},
			}
			// TLS 1.3 lets the client finish before the server checks its
			// certificate, so the rejection may arrive as a close after Dial.
			conn, err := bad.Dial(ctx, server.Endpoint.Addr(), config, &quic.Config{})
			if err == nil {
				select {
				case <-conn.Context().Done():
					err = context.Cause(conn.Context())
				case <-ctx.Done():
					t.Fatal("dial with a bad certificate stayed open")
				}
			}
			const badCertificateAlert = 0x100 + 42
			if closed, ok := errors.AsType[*quic.TransportError](err); !ok || !closed.Remote || closed.ErrorCode != badCertificateAlert {
				t.Fatalf("bad certificate dial = %v, want remote bad_certificate alert", err)
			}
			// The listener delivers views in handshake order, so a good dial
			// made afterwards must be the first view it yields.
			good, err := transporttest.DialRaw(t, ctx, server.Endpoint.Addr(), server.Endpoint.PeerID(), &quic.Config{})
			if err != nil {
				t.Fatal(err)
			}
			view, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if view.RemoteAddr().String() != good.LocalAddr().String() {
				t.Fatalf("first view from %s, want the good dialer %s", view.RemoteAddr(), good.LocalAddr())
			}
		})
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
