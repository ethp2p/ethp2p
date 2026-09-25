package ethp2p

import (
	"bytes"
	"context"
	"errors"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"io"
	"testing"
	"time"
)

const (
	selectorAlpha  wire.Selector = 3
	selectorCommon wire.Selector = 7
	selectorGamma  wire.Selector = 11
	testTimeout                      = 10 * time.Second
)

type testPair struct {
	client, server         *transporttest.Endpoint
	clientConn, serverConn transport.Conn
	clientLib, serverLib   quicreuse.QUICConn
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
func activeConn(t *testing.T, s *Stack, id transport.PeerID) transport.Conn {
	t.Helper()
	var conn transport.Conn
	waitFor(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		if v := s.active[id]; v != nil {
			conn = v.conn
		}
		return conn != nil
	})
	return conn
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
	if server != nil {
		p.serverConn = activeConn(t, server, p.client.Shared.PeerID())
	} else {
		p.serverConn, err = p.server.Eth.Accept(ctx)
		if err != nil {
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
func awaitEvent(t *testing.T, sub *Subsystem) Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	for {
		if event, ok := sub.Next(); ok {
			return event
		}
		select {
		case <-ctx.Done():
			t.Fatal("event did not arrive")
			return Event{}
		case <-time.After(time.Millisecond):
		}
	}
}
func awaitPeer(t *testing.T, sub *Subsystem) *Peer {
	t.Helper()
	event := awaitEvent(t, sub)
	if event.Kind != PeerUp {
		t.Fatalf("event = %v, want PeerUp", event.Kind)
	}
	return event.Peer
}

func writeSelectorPayload(t *testing.T, conn transport.Conn, selector wire.Selector, payload []byte) {
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

func awaitStreamEvent(t *testing.T, sub *Subsystem) Event {
	t.Helper()
	event := awaitEvent(t, sub)
	if event.Kind != StreamIn {
		t.Fatalf("event = %v, want StreamIn", event.Kind)
	}
	return event
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
	for _, sels := range [][]wire.Selector{nil, {0}, {1, 1}} {
		if _, err := s.Register("bad", sels, SubsystemConfig{}); err == nil {
			t.Fatal("invalid registration accepted")
		}
	}
	sub := registerTestSub(t, s, "ok", selectorAlpha)
	if _, err := s.Register("collision", []wire.Selector{selectorAlpha}, SubsystemConfig{}); err == nil {
		t.Fatal("collision accepted")
	}
	if _, err := s.Register("ok", []wire.Selector{selectorGamma}, SubsystemConfig{}); err == nil {
		t.Fatal("duplicate name accepted")
	}
	registerTestSub(t, s, "gamma", selectorGamma)
	for _, wake := range []chan struct{}{nil, make(chan struct{})} {
		if err := sub.Notify(wake); err == nil {
			t.Fatal("invalid wake accepted")
		}
	}
	if err := sub.Notify(make(chan struct{}, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err == nil {
		t.Fatal("second Start succeeded")
	}
	if _, err := s.Register("late", []wire.Selector{5}, SubsystemConfig{}); err == nil {
		t.Fatal("late Register succeeded")
	}
	if err := sub.Notify(make(chan struct{}, 1)); err == nil {
		t.Fatal("late Notify succeeded")
	}
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
	csub := registerTestSub(t, client, "client", selectorAlpha, selectorCommon)
	ssub := registerTestSub(t, server, "server", selectorAlpha, selectorGamma)
	pair.connect(t, client, server)
	cp, sp := awaitPeer(t, csub), awaitPeer(t, ssub)
	if cp.ID() != pair.server.Shared.PeerID() || sp.ID() != pair.client.Shared.PeerID() {
		t.Fatal("wrong identity")
	}
	sels := cp.Selectors()
	if len(sels) != 1 || sels[0] != selectorAlpha {
		t.Fatal(sels)
	}
	sels[0] = 0
	if cp.Selectors()[0] != selectorAlpha || cp.Record() != nil {
		t.Fatal("peer metadata")
	}
	if _, err := cp.OpenUniStream(t.Context(), selectorCommon); !errors.Is(err, ErrSelectorNotShared) {
		t.Fatal(err)
	}
	if _, err := cp.OpenStream(t.Context(), selectorCommon); !errors.Is(err, ErrSelectorNotShared) {
		t.Fatal(err)
	}
	uni, err := cp.OpenUniStream(t.Context(), selectorAlpha)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = uni.Write([]byte("uni"))
	_ = uni.Close()
	e := awaitStreamEvent(t, ssub)
	got, err := io.ReadAll(e.Stream)
	if err != nil || !bytes.Equal(got, []byte("uni")) {
		t.Fatalf("%q %v", got, err)
	}
	bi, err := cp.OpenStream(t.Context(), selectorAlpha)
	if err != nil {
		t.Fatal(err)
	}
	// Selector-only bidi delivery must not await payload or FIN.
	e = awaitStreamEvent(t, ssub)
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
