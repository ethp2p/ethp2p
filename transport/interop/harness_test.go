package interop

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	transport "github.com/ethp2p/ethp2p/transport"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	varint "github.com/multiformats/go-varint"
	"github.com/quic-go/quic-go"
)

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
