package ethp2p

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/ethp2p/ethp2p/enr"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/quic-go/quic-go"
)

func startTestStack(t *testing.T, s *Stack) {
	t.Helper()
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
}
func connectTest(t *testing.T, s *Stack, e *transporttest.Endpoint) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	if err := s.Connect(ctx, e.Record(t, 1)); err != nil {
		t.Fatal(err)
	}
}
func keepLibp2p(t *testing.T, e *transporttest.Endpoint) {
	t.Helper()
	if _, err := e.Shared.Libp2p().Listen(nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestSinkRoutingRefusedAndUnsupported(t *testing.T) {
	pair := newTestPair(t)
	s := newTestStack(t, pair.server)
	accepted := registerTestSub(t, s, "accepted", selectorCommon)
	refused, err := s.Register("refused", []wire.Selector{selectorAlpha}, SubsystemConfig{Policy: func(*Peer) bool { return false }})
	if err != nil {
		t.Fatal(err)
	}
	pair.connect(t, nil, s)
	awaitPeer(t, accepted)
	for _, test := range []struct {
		selector wire.Selector
		wire     uint64
	}{{selectorAlpha, 2}, {selectorGamma, 18}} {
		stream, err := pair.clientConn.OpenStream(t.Context(), test.selector)
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.SetReadDeadline(time.Now().Add(testTimeout))
		_, err = stream.Read(make([]byte, 1))
		reset, ok := errors.AsType[*transport.StreamResetError](err)
		if !ok || reset.Code != test.wire {
			t.Fatalf("read reset = %v, want %d", err, test.wire)
		}
		_ = stream.SetWriteDeadline(time.Now().Add(testTimeout))
		_, err = stream.Write(make([]byte, 2<<20))
		writeReset, ok := errors.AsType[*quic.StreamError](err)
		if !ok || !writeReset.Remote || uint64(writeReset.ErrorCode) != test.wire {
			t.Fatalf("write reset = %v, want %d", err, test.wire)
		}
	}
	if ev, ok := refused.Next(); ok {
		t.Fatalf("refused policy got %+v", ev)
	}
}

func TestConnectInspectionRecordsAndClose(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	// Retain libp2p to exercise GoAway while the physical connection stays open.
	keepLibp2p(t, a)
	keepLibp2p(t, b)
	left, err := NewStack(a.Eth, Config{Record: a.Record(t, 1)})
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewStack(b.Eth, Config{Record: b.Record(t, 2)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
	ls, rs := registerTestSub(t, left, "left", 1), registerTestSub(t, right, "right", 1)
	startTestStack(t, left)
	startTestStack(t, right)
	before := time.Now()
	if err := left.Connect(t.Context(), b.Record(t, 3)); err != nil {
		t.Fatal(err)
	}
	lp, rp := awaitPeer(t, ls), awaitPeer(t, rs)
	if lp.Record().Seq() != 3 || rp.Record().Seq() != 1 {
		t.Fatal("wrong record sequence")
	}
	if err := left.Connect(t.Context(), b.Record(t, 5)); err != nil {
		t.Fatal(err)
	}
	if lp.Record().Seq() != 5 {
		t.Fatal("connected record not updated")
	}
	for _, x := range []struct {
		s   *Stack
		id  transport.PeerID
		out bool
	}{{left, b.Eth.PeerID(), true}, {right, a.Eth.PeerID(), false}} {
		infos := x.s.Connections()
		if len(infos) != 1 || infos[0].ID != 1 || infos[0].Peer != x.id || infos[0].Outbound != x.out || infos[0].Since.Before(before) {
			t.Fatalf("connections = %+v", infos)
		}
		infos[0].Peer = "mutated"
		if x.s.Connections()[0].Peer != x.id {
			t.Fatal("snapshot aliases table")
		}
	}
	out, err := lp.OpenUniStream(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = out.Write([]byte("traffic"))
	_ = out.Close()
	awaitStreamEvent(t, rs).Reject()
	sent, received := left.Traffic()
	if sent == 0 || received == 0 {
		t.Fatalf("traffic %d %d", sent, received)
	}
	if err := left.Close(); err != nil {
		t.Fatal(err)
	}
	for _, sub := range []*Subsystem{ls, rs} {
		e := awaitEvent(t, sub)
		if e.Kind != PeerDown || e.Code != wire.Closing || e.Peer.Context().Err() == nil {
			t.Fatalf("down = %+v", e)
		}
	}
	left.mu.Lock()
	remaining := len(left.unreleased)
	left.mu.Unlock()
	if remaining != 0 || len(left.Connections()) != 0 {
		t.Fatal("Close returned before release")
	}
	afterSent, afterReceived := left.Traffic()
	if afterSent < sent || afterReceived < received {
		t.Fatal("traffic decreased")
	}
	if err := left.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConnectValidationAndRejection(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	// Retain libp2p to exercise GoAway while the physical connection stays open.
	keepLibp2p(t, a)
	keepLibp2p(t, b)
	left, right := newTestStack(t, a), newTestStack(t, b)
	registerTestSub(t, left, "left", 1)
	registerTestSub(t, right, "right", 2)
	startTestStack(t, left)
	startTestStack(t, right)
	rec, err := enr.Sign(secp256k1.PrivKeyFromBytes(b.Key.Bytes()), 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []*enr.Record{nil, a.Record(t, 1), rec} {
		if err := left.Connect(t.Context(), record); err == nil {
			t.Fatal("invalid record accepted")
		}
	}
	err = left.Connect(t.Context(), b.Record(t, 1))
	closed, ok := errors.AsType[*transport.ViewClosedError](err)
	if !ok || closed.Code != wire.NoSharedProtocols {
		t.Fatalf("no shared = %v", err)
	}
	c := transporttest.NewEndpoint(t)
	// Sign B's endpoint with C's identity.
	copyEndpoint := *b
	copyEndpoint.Key = c.Key
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := left.Connect(ctx, copyEndpoint.Record(t, 1)); err == nil {
		t.Fatal("wrong endpoint identity accepted")
	}
}

func TestConnectCoalesces(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	left, right := newTestStack(t, a), newTestStack(t, b)
	registerTestSub(t, left, "left", 1)
	registerTestSub(t, right, "right", 1)
	startTestStack(t, left)
	startTestStack(t, right)
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for range 10 {
		wg.Go(func() { errs <- left.Connect(t.Context(), b.Record(t, 1)) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return len(right.Connections()) == 1 })
	if len(left.Connections()) != 1 || left.Connections()[0].ID != 1 || right.Connections()[0].ID != 1 {
		t.Fatal("dials did not coalesce")
	}
}

func TestConnectWaiterTakesOver(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	left, right := newTestStack(t, a), newTestStack(t, b)
	registerTestSub(t, left, "left", 1)
	registerTestSub(t, right, "right", 1)
	startTestStack(t, left)
	driverCtx, cancel := context.WithCancel(t.Context())
	driver := make(chan error, 1)
	go func() { driver <- left.Connect(driverCtx, b.Record(t, 1)) }()
	waitFor(t, func() bool { left.mu.Lock(); defer left.mu.Unlock(); return left.dials[b.Eth.PeerID()] != nil })
	ctx, stop := context.WithTimeout(t.Context(), testTimeout)
	defer stop()
	waiter := make(chan error, 1)
	go func() { waiter <- left.Connect(ctx, b.Record(t, 1)) }()
	cancel()
	if err := <-driver; !errors.Is(err, context.Canceled) {
		t.Fatalf("driver = %v", err)
	}
	startTestStack(t, right)
	if err := <-waiter; err != nil {
		t.Fatalf("waiter = %v", err)
	}
}

func TestSimultaneousConnectConverges(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	// Retain libp2p to exercise GoAway while the physical connection stays open.
	keepLibp2p(t, a)
	keepLibp2p(t, b)
	left, right := newTestStack(t, a), newTestStack(t, b)
	gate := make(chan struct{})
	entered := make(chan struct{}, 4)
	policy := func(*Peer) bool { entered <- struct{}{}; <-gate; return true }
	for _, s := range []*Stack{left, right} {
		if _, err := s.Register("one", []wire.Selector{1}, SubsystemConfig{Policy: policy}); err != nil {
			t.Fatal(err)
		}
		startTestStack(t, s)
	}
	errs := make(chan error, 2)
	go func() { errs <- left.Connect(t.Context(), b.Record(t, 1)) }()
	go func() { errs <- right.Connect(t.Context(), a.Record(t, 1)) }()
	for range 4 {
		select {
		case <-entered:
		case <-time.After(testTimeout):
			t.Fatal("simultaneous admissions stalled")
		}
	}
	close(gate)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool {
		l, r := left.Connections(), right.Connections()
		return len(l) == 1 && len(r) == 1 && l[0].Outbound == (a.Eth.PeerID() < b.Eth.PeerID()) && r[0].Outbound == (b.Eth.PeerID() < a.Eth.PeerID())
	})
}

func TestDisconnectDuringAdmission(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	left, right := newTestStack(t, a), newTestStack(t, b)
	entered, release := make(chan struct{}), make(chan struct{})
	_, err := left.Register("blocked", []wire.Selector{1}, SubsystemConfig{Policy: func(*Peer) bool { _ = left.Connections(); close(entered); <-release; return true }})
	if err != nil {
		t.Fatal(err)
	}
	registerTestSub(t, right, "right", 1)
	startTestStack(t, left)
	startTestStack(t, right)
	done := make(chan error, 1)
	go func() { done <- left.Connect(t.Context(), b.Record(t, 1)) }()
	select {
	case <-entered:
	case <-time.After(testTimeout):
		t.Fatal("policy not entered")
	}
	if err := left.Disconnect(b.Eth.PeerID()); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; !errors.Is(err, ErrDisconnected) {
		t.Fatalf("Connect = %v", err)
	}
	if len(left.Connections()) != 0 {
		t.Fatal("stale admission committed")
	}
	if err := left.Disconnect("unknown"); err != nil {
		t.Fatal(err)
	}
	left.mu.Lock()
	n := len(left.generations)
	left.mu.Unlock()
	if n != 0 {
		t.Fatal("generation entries leaked")
	}
}

func TestRestartReplacesView(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	left, right := newTestStack(t, a), newTestStack(t, b)
	sub := registerTestSub(t, left, "left", 1)
	registerTestSub(t, right, "right", 1)
	startTestStack(t, left)
	startTestStack(t, right)
	connectTest(t, right, a)
	old := awaitPeer(t, sub)
	packet, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	shared, err := transport.NewShared(b.Key, packet, transport.Interop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shared.Close(); _ = packet.Close() })
	restarted := &transporttest.Endpoint{Shared: shared, Eth: shared.Ethp2p(), Packet: packet, Key: b.Key}
	next := newTestStack(t, restarted)
	registerTestSub(t, next, "restart", 1)
	startTestStack(t, next)
	connectTest(t, next, a)
	down, up := awaitEvent(t, sub), awaitEvent(t, sub)
	if down.Kind != PeerDown || down.Code != wire.Duplicate || down.Peer != old || up.Kind != PeerUp || up.Peer == old {
		t.Fatalf("replacement = %+v then %+v", down, up)
	}
}

func TestHelloRecordMismatchRejected(t *testing.T) {
	client, server := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	// Retain libp2p to exercise GoAway while the physical connection stays open.
	keepLibp2p(t, client)
	keepLibp2p(t, server)
	s := newTestStack(t, server)
	sub := registerTestSub(t, s, "one", 1)
	startTestStack(t, s)
	if err := client.Eth.SetHello(transport.Hello{Selectors: []wire.Selector{1}, Record: server.Record(t, 1).Encode()}); err != nil {
		t.Fatal(err)
	}
	c, err := client.Eth.Dial(t.Context(), server.Shared.Addr(), server.Shared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	_, _, err = c.AcceptUniStream(ctx)
	closed, ok := errors.AsType[*transport.ViewClosedError](err)
	if !ok || closed.Code != wire.ControlViolation || !closed.Remote {
		t.Fatalf("record rejection = %v", err)
	}
	if _, ok := sub.Next(); ok {
		t.Fatal("invalid record produced event")
	}
}
