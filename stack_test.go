package ethp2p

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/control"
	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
)

// Test families each register testProtocol; the raw endpoint in
// testPair.connect announces all three.
const (
	familyAlpha  wire.Family   = 3
	familyCommon wire.Family   = 7
	familyGamma  wire.Family   = 11
	testProtocol wire.Protocol = 1
	testTimeout                = 10 * time.Second
)

var (
	selectorAlpha = wire.Selector{Family: familyAlpha, Protocol: testProtocol}
	selectorGamma = wire.Selector{Family: familyGamma, Protocol: testProtocol}
)

type testPair struct {
	client, server *transporttest.Endpoint
	// raw and clientConn are set when connect runs without a client stack.
	raw                  *transporttest.RawPeer
	clientConn           *transport.Conn
	clientLib, serverLib quicreuse.QUICConn
}

func newTestPair(t *testing.T) *testPair {
	return &testPair{client: transporttest.NewEndpoint(t), server: transporttest.NewEndpoint(t)}
}

// pendingHelloTimeout and pendingHeaderTimeout are consumed by the next
// newTestStack call; shortenHelloTimeout and shortenClassifyTimeout set
// them. Tests here run sequentially, so a consumed-once hand-off is safe.
var pendingHelloTimeout, pendingHeaderTimeout time.Duration

func newTestStack(t *testing.T, endpoint *transporttest.Endpoint) *Stack {
	t.Helper()
	cfg := Config{HelloTimeout: pendingHelloTimeout, HeaderTimeout: pendingHeaderTimeout}
	pendingHelloTimeout, pendingHeaderTimeout = 0, 0
	s, err := NewStack(endpoint.Eth, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func waitFor(t *testing.T, ready func() bool) {
	t.Helper()
	end := time.Now().Add(testTimeout)
	for time.Now().Before(end) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func (p *testPair) connect(t *testing.T, client, server *Stack) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	// Retain libp2p to exercise GoAway while the physical connection stays open.
	cl, err := p.client.Endpoint.Libp2p().Listen(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	sl, err := p.server.Endpoint.Libp2p().Listen(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	if client == nil {
		// A raw client speaks control by hand and announces every test family.
		raw, err := transporttest.DialHello(ctx, p.client, p.server, transporttest.Hello{Families: []wire.Family{familyAlpha, familyCommon, familyGamma}})
		if err != nil {
			t.Fatal(err)
		}
		p.raw, p.clientConn = raw, raw.Conn
	} else {
		if err := client.Start(); err != nil {
			t.Fatal(err)
		}
		conn, err := p.client.Eth.Dial(ctx, p.server.Endpoint.Addr(), p.server.Endpoint.PeerID())
		if err != nil {
			t.Fatal(err)
		}
		// A dialed view reaches the stack through handshake and attach, as in
		// Connect.
		v, err := client.handshake(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		result := make(chan error, 1)
		if !client.attach(v, result) {
			t.Fatal("attach refused on an open stack")
		}
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	}
	// The stack owns its view; wait for admission instead of reaching in.
	waitFor(t, func() bool { return len(server.Connections()) == 1 })
	p.clientLib, err = cl.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p.serverLib, err = sl.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func disconnectTest(t *testing.T, s *Stack, id identity.PeerID) {
	t.Helper()
	if err := s.Disconnect(id); err != nil {
		t.Fatal(err)
	}
}

// registerTestFamily registers protocols under code with a fresh wake channel.
func registerTestFamily(t *testing.T, s *Stack, code wire.Family, protocols ...ProtocolSpec) (*Family, chan struct{}) {
	t.Helper()
	wake := make(chan struct{}, 1)
	family, err := s.Register(code, protocols, wake)
	if err != nil {
		t.Fatal(err)
	}
	return family, wake
}

func awaitEvent(t *testing.T, f *Family, wake <-chan struct{}) Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	for {
		if event, ok := f.Next(); ok {
			return event
		}
		select {
		case <-ctx.Done():
			t.Fatal("event did not arrive")
			return Event{}
		case <-wake:
		}
	}
}

func awaitPeer(t *testing.T, f *Family, wake <-chan struct{}) *Peer {
	t.Helper()
	event := awaitEvent(t, f, wake)
	if event.Kind != PeerUp {
		t.Fatalf("event = %v, want PeerUp", event.Kind)
	}
	return event.Peer
}

func awaitStreamEvent(t *testing.T, f *Family, wake <-chan struct{}) Event {
	t.Helper()
	event := awaitEvent(t, f, wake)
	if event.Kind != StreamIn {
		t.Fatalf("event = %v, want StreamIn", event.Kind)
	}
	return event
}

func mustPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	fn()
}

func writeSelectorPayload(t *testing.T, conn *transport.Conn, selector wire.Selector, payload []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	stream, err := transporttest.OpenUniStream(ctx, conn, selector)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStackRegistrationAndLifecycle(t *testing.T) {
	p := newTestPair(t)
	if _, err := NewStack(nil, Config{}); err == nil {
		t.Fatal("nil transport accepted")
	}
	if _, err := NewStack(p.client.Eth, Config{Record: p.server.Record(t, 1)}); err == nil {
		t.Fatal("wrong local identity accepted")
	}
	s := newTestStack(t, p.server)
	one := []ProtocolSpec{{Protocol: 1, MaxQueued: 1}}
	for _, reg := range []struct {
		code      wire.Family
		protocols []ProtocolSpec
	}{
		{familyAlpha, nil},
		{familyAlpha, []ProtocolSpec{}},
		{familyAlpha, []ProtocolSpec{{Protocol: 2, MaxQueued: 1}, {Protocol: 1, MaxQueued: 1}}},
		{familyAlpha, []ProtocolSpec{{Protocol: 1, MaxQueued: 1}, {Protocol: 1, MaxQueued: 1}}},
		{familyAlpha, []ProtocolSpec{{Protocol: 1, MaxQueued: 0}}},
		{control.ControlFamily, one},
		{wire.ReservedFamily, one},
	} {
		if _, err := s.Register(reg.code, reg.protocols, make(chan struct{}, 1)); err == nil {
			t.Fatalf("invalid registration accepted: family %d, %+v", reg.code, reg.protocols)
		}
	}
	for _, wake := range []chan struct{}{nil, make(chan struct{})} {
		if _, err := s.Register(familyAlpha, one, wake); err == nil {
			t.Fatal("invalid wake accepted")
		}
	}
	// Failed registrations leave the stack unchanged: the same family
	// registers cleanly afterwards.
	family, _ := registerTestFamily(t, s, familyAlpha, ProtocolSpec{Protocol: testProtocol, MaxQueued: 1})
	if _, err := s.Register(familyAlpha, []ProtocolSpec{{Protocol: 2, MaxQueued: 1}}, make(chan struct{}, 1)); err == nil {
		t.Fatal("collision accepted")
	}
	registerTestFamily(t, s, familyGamma, ProtocolSpec{Protocol: testProtocol, MaxQueued: 1})
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err == nil {
		t.Fatal("second Start succeeded")
	}
	if _, err := s.Register(5, one, make(chan struct{}, 1)); err == nil {
		t.Fatal("late Register succeeded")
	}
	_ = family
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); !errors.Is(err, ErrStackClosed) {
		t.Fatalf("Start = %v", err)
	}
	if err := s.Connect(t.Context(), p.client.Record(t, 1)); !errors.Is(err, ErrStackClosed) {
		t.Fatalf("Connect = %v", err)
	}
}

func TestPeerStreamsAndFamilyIntersection(t *testing.T) {
	pair := newTestPair(t)
	client, server := newTestStack(t, pair.client), newTestStack(t, pair.server)
	// The server shares only alpha; the client's gamma family is never shared.
	cAlpha, cAlphaWake := registerTestFamily(t, client, familyAlpha, ProtocolSpec{Protocol: testProtocol, MaxQueued: 8})
	cGamma, _ := registerTestFamily(t, client, familyGamma, ProtocolSpec{Protocol: testProtocol, MaxQueued: 8})
	sAlpha, sAlphaWake := registerTestFamily(t, server, familyAlpha, ProtocolSpec{Protocol: testProtocol, MaxQueued: 8})
	pair.connect(t, client, server)
	cp, sp := awaitPeer(t, cAlpha, cAlphaWake), awaitPeer(t, sAlpha, sAlphaWake)
	if cp.ID() != pair.server.Endpoint.PeerID() || sp.ID() != pair.client.Endpoint.PeerID() {
		t.Fatal("wrong identity")
	}
	if cp.Record() != nil {
		t.Fatal("unexpected peer record")
	}
	if _, ok := cGamma.Next(); ok {
		t.Fatal("unshared family produced an event")
	}
	for _, protocol := range []wire.Protocol{0, testProtocol + 1} {
		mustPanic(t, func() { _, _ = cp.OpenUniStream(t.Context(), protocol) })
		mustPanic(t, func() { _, _ = cp.OpenStream(t.Context(), protocol) })
	}
	uni, err := cp.OpenUniStream(t.Context(), testProtocol)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = uni.Write([]byte("uni"))
	_ = uni.Close()
	e := awaitStreamEvent(t, sAlpha, sAlphaWake)
	got, err := io.ReadAll(e.Stream)
	if err != nil || !bytes.Equal(got, []byte("uni")) {
		t.Fatalf("%q %v", got, err)
	}
	bi, err := cp.OpenStream(t.Context(), testProtocol)
	if err != nil {
		t.Fatal(err)
	}
	// Header-only bidi delivery must not await payload or FIN.
	e = awaitStreamEvent(t, sAlpha, sAlphaWake)
	incoming, ok := e.Stream.(Stream)
	if !ok {
		t.Fatal("lost bidi write side")
	}
	_, _ = incoming.Write([]byte("/response"))
	_ = incoming.Close()
	got, err = io.ReadAll(bi)
	if err != nil || string(got) != "/response" {
		t.Fatalf("%q %v", got, err)
	}
	_ = bi.Close()
	disconnectTest(t, client, pair.server.Endpoint.PeerID())
	if _, err := cp.OpenStream(t.Context(), testProtocol); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
