// Package transporttest provides loopback endpoints for tests outside transport.
package transporttest

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/control"
	"github.com/ethp2p/ethp2p/enr"
	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"
	"github.com/quic-go/quic-go"
)

// Endpoint is a transport.Endpoint on a loopback UDP socket, closed at test cleanup.
type Endpoint struct {
	Endpoint *transport.Endpoint
	Eth      *transport.Ethp2pTransport
	Packet   net.PacketConn
	Key      *identity.PrivKey
}

// Record signs this endpoint's identity and bound QUIC address at seq.
func (e *Endpoint) Record(tb testing.TB, seq uint64) *enr.Record {
	tb.Helper()
	addr, err := netip.ParseAddrPort(e.Packet.LocalAddr().String())
	if err != nil {
		tb.Fatal(err)
	}
	rec, err := enr.Sign(e.Key, seq, enr.EndpointEntries(addr)...)
	if err != nil {
		tb.Fatal(err)
	}
	return rec
}

// NewEndpoint builds an endpoint with a fresh identity and the Interop config.
func NewEndpoint(tb testing.TB) *Endpoint {
	tb.Helper()
	key, err := identity.GenPrivKey()
	if err != nil {
		tb.Fatal(err)
	}
	packet, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	ep, err := transport.NewEndpoint(key, packet, transport.Interop())
	if err != nil {
		_ = packet.Close()
		tb.Fatal(err)
	}
	_ = ep.Libp2p()
	_ = ep.Ethp2p()
	if err := ep.Start(); err != nil {
		_ = packet.Close()
		tb.Fatal(err)
	}
	endpoint := &Endpoint{Endpoint: ep, Eth: ep.Ethp2p(), Packet: packet, Key: key}
	tb.Cleanup(func() {
		_ = ep.Close()
		_ = packet.Close()
	})
	return endpoint
}

// Context returns a context that ends after 5 seconds or at test cleanup.
func Context(tb testing.TB) context.Context {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), 5*time.Second)
	tb.Cleanup(cancel)
	return ctx
}

// DialRaw dials remote at addr through quic-go directly, from a fresh
// identity and loopback socket closed at test cleanup. It uses the libp2p TLS
// identity format with the ethp2p ALPN, so raw traffic exercises the real
// authenticated dispatcher.
func DialRaw(tb testing.TB, ctx context.Context, addr net.Addr, remote identity.PeerID, config *quic.Config) (*quic.Conn, error) {
	tb.Helper()
	key, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, -1)
	if err != nil {
		tb.Fatal(err)
	}
	identity, err := libp2ptls.NewIdentity(key)
	if err != nil {
		tb.Fatal(err)
	}
	tlsConfig, _ := identity.ConfigForPeer(peer.ID(remote))
	tlsConfig.NextProtos = []string{transport.AlpnEthp2p}
	packet, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	raw := &quic.Transport{Conn: packet}
	tb.Cleanup(func() { _ = raw.Close(); _ = packet.Close() })
	return raw.Dial(ctx, addr, tlsConfig, config)
}

// NextRawStream pulls the next incoming bidirectional ethp2p stream, unread,
// waiting on the view's wake channel. A stream already queued is returned even
// if ctx has ended. Otherwise it returns ctx.Err() or a
// transport.ErrViewClosed-matching error; if ctx has ended and the view has
// closed, which one is unspecified.
func NextRawStream(ctx context.Context, c *transport.Conn) (*quic.Stream, error) {
	for {
		if s, ok := c.NextStream(); ok {
			return s, nil
		}
		select {
		case <-c.Streams():
		case <-c.Done():
			return nil, &transport.ViewClosedError{Code: c.CloseCode()}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// NextRawUniStream pulls the next incoming unidirectional ethp2p stream like
// NextRawStream.
func NextRawUniStream(ctx context.Context, c *transport.Conn) (*quic.ReceiveStream, error) {
	for {
		if s, ok := c.NextUniStream(); ok {
			return s, nil
		}
		select {
		case <-c.UniStreams():
		case <-c.Done():
			return nil, &transport.ViewClosedError{Code: c.CloseCode()}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// NextStream pulls the next incoming bidirectional ethp2p stream like
// NextRawStream and reads its stream header.
func NextStream(ctx context.Context, c *transport.Conn) (*quic.Stream, wire.Selector, error) {
	s, err := NextRawStream(ctx, c)
	if err != nil {
		return nil, wire.Selector{}, err
	}
	sel, err := wire.ReadSelector(s)
	if err != nil {
		return nil, wire.Selector{}, err
	}
	return s, sel, nil
}

// NextUniStream pulls the next incoming unidirectional ethp2p stream like
// NextRawUniStream and reads its stream header.
func NextUniStream(ctx context.Context, c *transport.Conn) (*quic.ReceiveStream, wire.Selector, error) {
	s, err := NextRawUniStream(ctx, c)
	if err != nil {
		return nil, wire.Selector{}, err
	}
	sel, err := wire.ReadSelector(s)
	if err != nil {
		return nil, wire.Selector{}, err
	}
	return s, sel, nil
}

// OpenUniStream opens a unidirectional stream and writes its stream header,
// as an ethp2p peer would.
func OpenUniStream(ctx context.Context, c *transport.Conn, sel wire.Selector) (*quic.SendStream, error) {
	s, err := c.OpenUniStream(ctx)
	if err != nil {
		return nil, err
	}
	if err := wire.WriteSelector(s, sel); err != nil {
		s.CancelWrite(quic.StreamErrorCode(wire.Unspecified.Wire()))
		return nil, err
	}
	return s, nil
}

// OpenStream opens a bidirectional stream and writes its stream header, as
// an ethp2p peer would.
func OpenStream(ctx context.Context, c *transport.Conn, sel wire.Selector) (*quic.Stream, error) {
	s, err := c.OpenStream(ctx)
	if err != nil {
		return nil, err
	}
	if err := wire.WriteSelector(s, sel); err != nil {
		s.CancelRead(quic.StreamErrorCode(wire.Unspecified.Wire()))
		s.CancelWrite(quic.StreamErrorCode(wire.Unspecified.Wire()))
		return nil, err
	}
	return s, nil
}

// Hello is the content of a raw peer's HELLO. SendHello writes it as given,
// without validation, so a test can send an invalid family list or record.
type Hello struct {
	Families []wire.Family
	Record   []byte
}

// RawPeer is one end of a view that speaks the control protocol by hand, for
// tests against a Stack.
type RawPeer struct {
	Conn *transport.Conn
	// PeerControl is the other side's control stream, read up to its HELLO.
	PeerControl *quic.ReceiveStream
}

// DialHello dials server from client, sends h on a control stream opened
// first, and returns once the server's HELLO has arrived, as a stack does
// before admission.
func DialHello(ctx context.Context, client, server *Endpoint, h Hello) (*RawPeer, error) {
	conn, err := client.Eth.Dial(ctx, server.Endpoint.Addr(), server.Endpoint.PeerID())
	if err != nil {
		return nil, err
	}
	p, err := SendHello(ctx, conn, h)
	if err != nil {
		conn.Close()
		return nil, err
	}
	// The server's control stream is the first unidirectional stream it
	// opens; ReadHello checks its header and reads its HELLO frame.
	in, err := NextRawUniStream(ctx, conn)
	if err == nil {
		_, err = control.ReadHello(in)
	}
	if err != nil {
		conn.Close()
		return nil, err
	}
	p.PeerControl = in
	return p, nil
}

// SendHello opens conn's control stream and writes its header and h.
func SendHello(ctx context.Context, conn *transport.Conn, h Hello) (*RawPeer, error) {
	out, err := conn.OpenUniStream(ctx)
	if err != nil {
		return nil, err
	}
	frame, err := control.HelloBytes(h.Families, h.Record)
	if err != nil {
		return nil, err
	}
	if _, err := out.Write(frame); err != nil {
		return nil, err
	}
	return &RawPeer{Conn: conn}, nil
}

// Connect dials server from client while server accepts, and returns both
// ethp2p views. It fails tb on error and joins the accept goroutine.
func Connect(tb testing.TB, client, server *Endpoint) (dialed, accepted *transport.Conn) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), 10*time.Second)
	defer cancel()
	type acceptResult struct {
		conn *transport.Conn
		err  error
	}
	result := make(chan acceptResult, 1)
	go func() {
		conn, err := server.Eth.Accept(ctx)
		result <- acceptResult{conn: conn, err: err}
	}()

	dialed, err := client.Eth.Dial(ctx, server.Endpoint.Addr(), server.Endpoint.PeerID())
	if err != nil {
		cancel()
		<-result
		tb.Fatalf("dial: %v", err)
	}
	accept := <-result
	if accept.err != nil {
		dialed.Close()
		tb.Fatalf("accept: %v", accept.err)
	}
	return dialed, accept.conn
}

// ReadGoAway reads the other side's next control frame, which must be GOAWAY,
// and returns its stack code. It fails on any other frame, or when the stream
// ends first.
func (p *RawPeer) ReadGoAway() (wire.Code, error) {
	return control.ReadGoAway(p.PeerControl)
}
