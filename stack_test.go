package ethp2p

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/wire"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
)

const (
	selectorAlpha  wire.Selector = 3
	selectorCommon wire.Selector = 7
	selectorGamma  wire.Selector = 11
	testTimeout                      = 10 * time.Second
)

type testPair struct {
	client, server       *transporttest.Endpoint
	clientConn           *transport.Conn
	clientLib, serverLib quicreuse.QUICConn
}

func newTestPair(t *testing.T) *testPair {
	return &testPair{client: transporttest.NewEndpoint(t), server: transporttest.NewEndpoint(t)}
}

func newTestStack(t *testing.T, endpoint *transporttest.Endpoint) *Stack {
	t.Helper()
	s, err := NewStack(endpoint.Eth, Config{})
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
	cl, err := p.client.Shared.Libp2p().Listen(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	sl, err := p.server.Shared.Libp2p().Listen(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range []struct {
		stack    *Stack
		endpoint *transporttest.Endpoint
	}{{client, p.client}, {server, p.server}} {
		if x.stack != nil {
			err = x.stack.Start()
		} else {
			err = x.endpoint.Eth.SetHello(transport.Hello{Selectors: []wire.Selector{selectorAlpha, selectorCommon, selectorGamma}})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	p.clientConn, err = p.client.Eth.Dial(ctx, p.server.Shared.Addr(), p.server.Shared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	if client != nil {
		// A dialed view reaches the stack only through attach, as in Connect.
		result := make(chan error, 1)
		if !client.attach(p.clientConn, result) {
			t.Fatal("attach refused on an open stack")
		}
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	}
	if server != nil {
		// The stack owns its view; wait for admission instead of reaching in.
		waitFor(t, func() bool { return len(server.Connections()) == 1 })
	} else {
		if _, err = p.server.Eth.Accept(ctx); err != nil {
			t.Fatal(err)
		}
	}
	p.clientLib, err = cl.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p.serverLib, err = sl.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func disconnectTest(t *testing.T, s *Stack, id transport.PeerID) {
	t.Helper()
	if err := s.Disconnect(id); err != nil {
		t.Fatal(err)
	}
}

// registerTestFamily registers protocols with a fresh wake channel.
func registerTestFamily(t *testing.T, s *Stack, protocols ...ProtocolSpec) (*Family, chan struct{}) {
	t.Helper()
	wake := make(chan struct{}, 1)
	family, err := s.Register(protocols, wake)
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
	stream, err := conn.OpenUniStream(ctx, selector)
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
	for _, protocols := range [][]ProtocolSpec{
		nil,
		{},
		{{Selector: 2, MaxQueued: 1}, {Selector: 1, MaxQueued: 1}},
		{{Selector: 0, MaxQueued: 1}},
		{{Selector: 1, MaxQueued: 1}, {Selector: 1, MaxQueued: 1}},
		{{Selector: 1, MaxQueued: 0}},
	} {
		if _, err := s.Register(protocols, make(chan struct{}, 1)); err == nil {
			t.Fatalf("invalid registration accepted: %+v", protocols)
		}
	}
	for _, wake := range []chan struct{}{nil, make(chan struct{})} {
		if _, err := s.Register([]ProtocolSpec{{Selector: selectorAlpha, MaxQueued: 1}}, wake); err == nil {
			t.Fatal("invalid wake accepted")
		}
	}
	// Failed registrations leave the stack unchanged: the same selectors
	// register cleanly afterwards.
	family, _ := registerTestFamily(t, s, ProtocolSpec{Selector: selectorAlpha, MaxQueued: 1})
	if _, err := s.Register([]ProtocolSpec{{Selector: selectorAlpha, MaxQueued: 1}}, make(chan struct{}, 1)); err == nil {
		t.Fatal("collision accepted")
	}
	registerTestFamily(t, s, ProtocolSpec{Selector: selectorGamma, MaxQueued: 1})
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err == nil {
		t.Fatal("second Start succeeded")
	}
	if _, err := s.Register([]ProtocolSpec{{Selector: 5, MaxQueued: 1}}, make(chan struct{}, 1)); err == nil {
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

func TestPeerStreamsAndSelectorIntersection(t *testing.T) {
	pair := newTestPair(t)
	client, server := newTestStack(t, pair.client), newTestStack(t, pair.server)
	// The server shares only alpha; the client's gamma family is never shared.
	cAlpha, cAlphaWake := registerTestFamily(t, client, ProtocolSpec{Selector: selectorAlpha, MaxQueued: 8})
	cGamma, _ := registerTestFamily(t, client, ProtocolSpec{Selector: selectorGamma, MaxQueued: 8})
	sAlpha, sAlphaWake := registerTestFamily(t, server, ProtocolSpec{Selector: selectorAlpha, MaxQueued: 8})
	pair.connect(t, client, server)
	cp, sp := awaitPeer(t, cAlpha, cAlphaWake), awaitPeer(t, sAlpha, sAlphaWake)
	if cp.ID() != pair.server.Shared.PeerID() || sp.ID() != pair.client.Shared.PeerID() {
		t.Fatal("wrong identity")
	}
	if cp.Record() != nil {
		t.Fatal("unexpected peer record")
	}
	if _, ok := cGamma.Next(); ok {
		t.Fatal("unshared family produced an event")
	}
	for _, sel := range []wire.Selector{selectorGamma, selectorCommon} {
		mustPanic(t, func() { _, _ = cp.OpenUniStream(t.Context(), sel) })
		mustPanic(t, func() { _, _ = cp.OpenStream(t.Context(), sel) })
	}
	uni, err := cp.OpenUniStream(t.Context(), selectorAlpha)
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
	bi, err := cp.OpenStream(t.Context(), selectorAlpha)
	if err != nil {
		t.Fatal(err)
	}
	// Selector-only bidi delivery must not await payload or FIN.
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
	disconnectTest(t, client, pair.server.Shared.PeerID())
	if _, err := cp.OpenStream(t.Context(), selectorAlpha); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
