// Package transporttest provides loopback endpoints for tests outside transport.
package transporttest

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/ethp2p/ethp2p/enr"
	"github.com/ethp2p/ethp2p/transport"
)

// Endpoint is a shared transport on a loopback UDP socket, closed at test cleanup.
type Endpoint struct {
	Shared *transport.SharedTransport
	Eth    *transport.Ethp2pTransport
	Packet net.PacketConn
	Key    *transport.PrivKey
}

// Record signs this endpoint's identity and bound QUIC address at seq.
func (e *Endpoint) Record(tb testing.TB, seq uint64) *enr.Record {
	tb.Helper()
	addr, err := netip.ParseAddrPort(e.Packet.LocalAddr().String())
	if err != nil {
		tb.Fatal(err)
	}
	ip := addr.Addr().Unmap()
	var entries []enr.Pair
	if ip.Is4() {
		entries = []enr.Pair{enr.IP.Set(ip), enr.QUIC.Set(addr.Port())}
	} else {
		entries = []enr.Pair{enr.IP6.Set(ip), enr.QUIC6.Set(addr.Port())}
	}
	rec, err := enr.Sign(secp256k1.PrivKeyFromBytes(e.Key.Bytes()), seq, entries...)
	if err != nil {
		tb.Fatal(err)
	}
	return rec
}

// NewEndpoint builds an endpoint with a fresh identity and the Interop profile.
// It registers ethp2p interest; tests that also need libp2p call Shared.Libp2p().
func NewEndpoint(tb testing.TB) *Endpoint {
	tb.Helper()
	key, err := transport.GenPrivKey()
	if err != nil {
		tb.Fatal(err)
	}
	packet, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	shared, err := transport.NewShared(key, packet, transport.Interop())
	if err != nil {
		_ = packet.Close()
		tb.Fatal(err)
	}
	endpoint := &Endpoint{Shared: shared, Eth: shared.Ethp2p(), Packet: packet, Key: key}
	if err := endpoint.Eth.SetHello(transport.Hello{}); err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		_ = shared.Close()
		_ = packet.Close()
	})
	return endpoint
}

// Connect dials server from client while server accepts, and returns both
// ethp2p views. It fails tb on error and joins the accept goroutine.
func Connect(tb testing.TB, client, server *Endpoint) (dialed, accepted transport.Conn) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), 10*time.Second)
	defer cancel()
	type acceptResult struct {
		conn transport.Conn
		err  error
	}
	result := make(chan acceptResult, 1)
	go func() {
		conn, err := server.Eth.Accept(ctx)
		result <- acceptResult{conn: conn, err: err}
	}()

	dialed, err := client.Eth.Dial(ctx, server.Shared.Addr(), server.Shared.PeerID())
	if err != nil {
		cancel()
		<-result
		tb.Fatalf("dial: %v", err)
	}
	accept := <-result
	if accept.err != nil {
		_ = dialed.Close()
		tb.Fatalf("accept: %v", accept.err)
	}
	return dialed, accept.conn
}
