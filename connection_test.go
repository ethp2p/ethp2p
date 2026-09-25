package ethp2p

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

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

func TestRoutingUnsupported(t *testing.T) {
	pair := newTestPair(t)
	s := newTestStack(t, pair.server)
	accepted, acceptedWake := registerTestFamily(t, s, ProtocolSpec{Selector: selectorCommon, MaxQueued: 8})
	// Partial holds alpha, but the peer never lists selector 15, so the family
	// is never shared and alpha is never routed.
	partial, _ := registerTestFamily(t, s,
		ProtocolSpec{Selector: selectorAlpha, MaxQueued: 8},
		ProtocolSpec{Selector: 15, MaxQueued: 8})
	pair.connect(t, nil, s)
	awaitPeer(t, accepted, acceptedWake)
	for _, selector := range []wire.Selector{selectorAlpha, selectorGamma} {
		stream, err := pair.clientConn.OpenStream(t.Context(), selector)
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.SetReadDeadline(time.Now().Add(testTimeout))
		_, err = stream.Read(make([]byte, 1))
		reset, ok := errors.AsType[*transport.StreamResetError](err)
		if !ok || reset.Code != wire.UnsupportedSelector.Wire() {
			t.Fatalf("read reset = %v, want wire %d", err, wire.UnsupportedSelector.Wire())
		}
		_ = stream.SetWriteDeadline(time.Now().Add(testTimeout))
		_, err = stream.Write(make([]byte, 2<<20))
		writeReset, ok := errors.AsType[*quic.StreamError](err)
		if !ok || !writeReset.Remote || uint64(writeReset.ErrorCode) != wire.UnsupportedSelector.Wire() {
			t.Fatalf("write reset = %v, want wire %d", err, wire.UnsupportedSelector.Wire())
		}
	}
	if ev, ok := partial.Next(); ok {
		t.Fatalf("unshared family got %+v", ev)
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
	ls, lsWake := registerTestFamily(t, left, ProtocolSpec{Selector: 1, MaxQueued: 8})
	rs, rsWake := registerTestFamily(t, right, ProtocolSpec{Selector: 1, MaxQueued: 8})
	startTestStack(t, left)
	startTestStack(t, right)
	before := time.Now()
	if err := left.Connect(t.Context(), b.Record(t, 3)); err != nil {
		t.Fatal(err)
	}
	lp, rp := awaitPeer(t, ls, lsWake), awaitPeer(t, rs, rsWake)
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
	}{{left, b.Shared.PeerID(), true}, {right, a.Shared.PeerID(), false}} {
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
	awaitStreamEvent(t, rs, rsWake).Reject()
	sent, received := left.Traffic()
	if sent == 0 || received == 0 {
		t.Fatalf("traffic %d %d", sent, received)
	}
	if err := left.Close(); err != nil {
		t.Fatal(err)
	}
	for _, sub := range []struct {
		fam  *Family
		wake chan struct{}
	}{{ls, lsWake}, {rs, rsWake}} {
		e := awaitEvent(t, sub.fam, sub.wake)
		if e.Kind != PeerDown || e.Code != wire.Closing || e.Peer.Context().Err() == nil {
			t.Fatalf("down = %+v", e)
		}
	}
	left.mu.Lock()
	remaining := len(left.views)
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
	registerTestFamily(t, left, ProtocolSpec{Selector: 1, MaxQueued: 8})
	registerTestFamily(t, right, ProtocolSpec{Selector: 2, MaxQueued: 8})
	startTestStack(t, left)
	startTestStack(t, right)
	rec, err := enr.Sign(b.Key, 1)
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
	registerTestFamily(t, left, ProtocolSpec{Selector: 1, MaxQueued: 8})
	registerTestFamily(t, right, ProtocolSpec{Selector: 1, MaxQueued: 8})
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
	registerTestFamily(t, left, ProtocolSpec{Selector: 1, MaxQueued: 8})
	registerTestFamily(t, right, ProtocolSpec{Selector: 1, MaxQueued: 8})
	startTestStack(t, left)
	startTestStack(t, right)
	// A record for the same peer whose address never answers, so the driver's
	// dial stays in flight until it is cancelled.
	blackhole, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer blackhole.Close()
	holeAddr, err := netip.ParseAddrPort(blackhole.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	holeIP := holeAddr.Addr().Unmap()
	holeRec, err := enr.Sign(b.Key, 1, enr.IP.Set(holeIP), enr.QUIC.Set(holeAddr.Port()))
	if err != nil {
		t.Fatal(err)
	}
	driverCtx, cancel := context.WithCancel(t.Context())
	driver := make(chan error, 1)
	go func() { driver <- left.Connect(driverCtx, holeRec) }()
	waitFor(t, func() bool {
		left.mu.Lock()
		defer left.mu.Unlock()
		return left.dials[b.Shared.PeerID()] != nil
	})
	ctx, stop := context.WithTimeout(t.Context(), testTimeout)
	defer stop()
	waiter := make(chan error, 1)
	go func() { waiter <- left.Connect(ctx, b.Record(t, 1)) }()
	cancel()
	if err := <-driver; !errors.Is(err, context.Canceled) {
		t.Fatalf("driver = %v", err)
	}
	if err := <-waiter; err != nil {
		t.Fatalf("waiter = %v", err)
	}
}

func TestSimultaneousConnectConverges(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	left, right := newTestStack(t, a), newTestStack(t, b)
	registerTestFamily(t, left, ProtocolSpec{Selector: 1, MaxQueued: 8})
	registerTestFamily(t, right, ProtocolSpec{Selector: 1, MaxQueued: 8})
	startTestStack(t, left)
	startTestStack(t, right)
	errs := make(chan error, 2)
	go func() { errs <- left.Connect(t.Context(), b.Record(t, 1)) }()
	go func() { errs <- right.Connect(t.Context(), a.Record(t, 1)) }()
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	// Opposite directions keep the view dialed by the lower peer on both sides.
	lowerOutbound := a.Shared.PeerID() < b.Shared.PeerID()
	waitFor(t, func() bool {
		l, r := left.Connections(), right.Connections()
		return len(l) == 1 && len(r) == 1 && (l[0].Outbound == lowerOutbound) && (r[0].Outbound == !lowerOutbound)
	})
}

func TestDisconnectBeforeAttach(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	left, right := newTestStack(t, a), newTestStack(t, b)
	registerTestFamily(t, left, ProtocolSpec{Selector: 1, MaxQueued: 8})
	registerTestFamily(t, right, ProtocolSpec{Selector: 1, MaxQueued: 8})
	startTestStack(t, left)
	startTestStack(t, right)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	conn, err := left.transport.Dial(ctx, b.Shared.Addr(), b.Shared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	id := b.Shared.PeerID()
	// Plant the dial record a Connect would hold while dialing, so the
	// generation gate rejects the late attach.
	left.mu.Lock()
	left.dials[id] = &dialAttempt{gen: left.generations[id], done: make(chan struct{}), cancel: func() {}}
	left.mu.Unlock()
	if err := left.Disconnect(id); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	if !left.attach(conn, result) {
		t.Fatal("attach refused on an open stack")
	}
	if err := <-result; !errors.Is(err, ErrDisconnected) {
		t.Fatalf("attach = %v", err)
	}
	if infos := left.Connections(); len(infos) != 0 {
		t.Fatalf("stale attach committed: %+v", infos)
	}
	select {
	case <-conn.Done():
	case <-time.After(testTimeout):
		t.Fatal("rejected view remained open")
	}
	left.mu.Lock()
	delete(left.dials, id)
	left.cleanupGeneration(id)
	n := len(left.generations)
	left.mu.Unlock()
	if n != 0 {
		t.Fatal("generation entries leaked")
	}
	if err := left.Disconnect("unknown"); err != nil {
		t.Fatal(err)
	}
}

func TestRestartReplacesView(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	left, right := newTestStack(t, a), newTestStack(t, b)
	lfam, lWake := registerTestFamily(t, left, ProtocolSpec{Selector: 1, MaxQueued: 8})
	registerTestFamily(t, right, ProtocolSpec{Selector: 1, MaxQueued: 8})
	startTestStack(t, left)
	startTestStack(t, right)
	connectTest(t, right, a)
	old := awaitPeer(t, lfam, lWake)
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
	registerTestFamily(t, next, ProtocolSpec{Selector: 1, MaxQueued: 8})
	startTestStack(t, next)
	connectTest(t, next, a)
	down, up := awaitEvent(t, lfam, lWake), awaitEvent(t, lfam, lWake)
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
	fam, _ := registerTestFamily(t, s, ProtocolSpec{Selector: 1, MaxQueued: 8})
	startTestStack(t, s)
	if err := client.Eth.SetHello(transport.Hello{Selectors: []wire.Selector{1}, Record: server.Record(t, 1).Encode()}); err != nil {
		t.Fatal(err)
	}
	c, err := client.Eth.Dial(t.Context(), server.Shared.Addr(), server.Shared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.Done():
	case <-time.After(testTimeout):
		t.Fatal("rejected view remained open")
	}
	if code := c.CloseCode(); code != wire.ControlViolation {
		t.Fatalf("record rejection code = %s", code)
	}
	if _, ok := fam.Next(); ok {
		t.Fatal("invalid record produced event")
	}
}
