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
	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/ethp2p/ethp2p/wire"
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
	if _, err := e.Endpoint.Libp2p().Listen(nil, nil); err != nil {
		t.Fatal(err)
	}
}

// releaseLibp2pView accepts and closes e's oldest queued libp2p view. An
// ethp2p_0 connection has both views, so its physical close
// waits for its libp2p view to be released too.
func releaseLibp2pView(t *testing.T, e *transporttest.Endpoint) {
	t.Helper()
	listener, err := e.Endpoint.Libp2p().Listen(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	conn, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.CloseWithError(0, ""); err != nil {
		t.Fatal(err)
	}
}

func TestRoutingUnsupported(t *testing.T) {
	pair := newTestPair(t)
	s := newTestStack(t, pair.server)
	accepted, acceptedWake := registerTestProtocol(t, s, protocolCommon)
	// The peer never lists protocol 15, so it is never shared.
	unlisted, _ := registerTestProtocol(t, s, 15)
	pair.connect(t, nil, s)
	awaitPeer(t, accepted, acceptedWake)
	// Unregistered protocols and an unshared protocol are all unsupported.
	for _, protocol := range []wire.Protocol{protocolAlpha, protocolGamma, 15} {
		stream, err := transporttest.OpenStream(t.Context(), pair.clientConn, protocol)
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.SetReadDeadline(time.Now().Add(testTimeout))
		_, err = stream.Read(make([]byte, 1))
		reset, ok := errors.AsType[*quic.StreamError](err)
		if !ok || uint64(reset.ErrorCode) != wire.UnsupportedProtocol.Wire() {
			t.Fatalf("read reset = %v, want wire %d", err, wire.UnsupportedProtocol.Wire())
		}
		_ = stream.SetWriteDeadline(time.Now().Add(testTimeout))
		_, err = stream.Write(make([]byte, 2<<20))
		writeReset, ok := errors.AsType[*quic.StreamError](err)
		if !ok || !writeReset.Remote || uint64(writeReset.ErrorCode) != wire.UnsupportedProtocol.Wire() {
			t.Fatalf("write reset = %v, want wire %d", err, wire.UnsupportedProtocol.Wire())
		}
	}
	if ev, ok := unlisted.Next(); ok {
		t.Fatalf("unshared protocol got %+v", ev)
	}
}

func TestConnectRecordsAndSnapshot(t *testing.T) {
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
	ls, lsWake := registerTestProtocol(t, left, 1)
	rs, rsWake := registerTestProtocol(t, right, 1)
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
		id  identity.PeerID
		out bool
	}{{left, b.Endpoint.PeerID(), true}, {right, a.Endpoint.PeerID(), false}} {
		infos := x.s.Connections()
		if len(infos) != 1 || infos[0].ID != 1 || infos[0].Peer != x.id || infos[0].Outbound != x.out || infos[0].Since.Before(before) {
			t.Fatalf("connections = %+v", infos)
		}
		infos[0].Peer = "mutated"
		if x.s.Connections()[0].Peer != x.id {
			t.Fatal("snapshot aliases table")
		}
	}
	out, err := lp.OpenUniStream(t.Context())
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
}

func TestStackCloseReleasesViews(t *testing.T) {
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
	ls, lsWake := registerTestProtocol(t, left, 1)
	rs, rsWake := registerTestProtocol(t, right, 1)
	startTestStack(t, left)
	startTestStack(t, right)
	if err := left.Connect(t.Context(), b.Record(t, 1)); err != nil {
		t.Fatal(err)
	}
	awaitPeer(t, ls, lsWake)
	awaitPeer(t, rs, rsWake)
	if err := left.Close(); err != nil {
		t.Fatal(err)
	}
	for _, sub := range []struct {
		prot *Protocol
		wake chan struct{}
	}{{ls, lsWake}, {rs, rsWake}} {
		e := awaitEvent(t, sub.prot, sub.wake)
		if e.Kind != PeerDown || e.Code != wire.Closing || e.Peer.Context().Err() == nil {
			t.Fatalf("down = %+v", e)
		}
	}
	if len(left.Connections()) != 0 {
		t.Fatal("Close returned before release")
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
	registerTestProtocol(t, left, 1)
	registerTestProtocol(t, right, 2)
	startTestStack(t, left)
	startTestStack(t, right)
	rec, err := enr.Sign(b.Key, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []*enr.Record{a.Record(t, 1), rec} {
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
	registerTestProtocol(t, left, 1)
	registerTestProtocol(t, right, 1)
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
	registerTestProtocol(t, left, 1)
	registerTestProtocol(t, right, 1)
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
	holeRec, err := enr.Sign(b.Key, 1, enr.EntryIP(holeIP), enr.EntryQUIC(holeAddr.Port()))
	if err != nil {
		t.Fatal(err)
	}
	driverCtx, cancel := context.WithCancel(t.Context())
	driver := make(chan error, 1)
	go func() { driver <- left.Connect(driverCtx, holeRec) }()
	waitFor(t, func() bool {
		left.mu.Lock()
		defer left.mu.Unlock()
		return left.dials[b.Endpoint.PeerID()] != nil
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
	registerTestProtocol(t, left, 1)
	registerTestProtocol(t, right, 1)
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
	lowerOutbound := a.Endpoint.PeerID() < b.Endpoint.PeerID()
	waitFor(t, func() bool {
		l, r := left.Connections(), right.Connections()
		return len(l) == 1 && len(r) == 1 && (l[0].Outbound == lowerOutbound) && (r[0].Outbound == !lowerOutbound)
	})
}

func TestDisconnectBeforeAttach(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	left, right := newTestStack(t, a), newTestStack(t, b)
	registerTestProtocol(t, left, 1)
	registerTestProtocol(t, right, 1)
	startTestStack(t, left)
	startTestStack(t, right)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	conn, err := left.transport.Dial(ctx, b.Endpoint.Addr(), b.Endpoint.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	v, err := left.handshake(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	id := b.Endpoint.PeerID()
	// Plant the dial record a Connect would hold while dialing, so Disconnect
	// marks it and the late attach is rejected.
	left.mu.Lock()
	left.dials[id] = &dialAttempt{done: make(chan struct{}), cancel: func() {}}
	left.mu.Unlock()
	if err := left.Disconnect(id); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	if !left.attach(v, result) {
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
	left.mu.Unlock()
	if err := left.Disconnect("unknown"); err != nil {
		t.Fatal(err)
	}
}

func TestRestartReplacesView(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	left, right := newTestStack(t, a), newTestStack(t, b)
	lfam, lWake := registerTestProtocol(t, left, 1)
	registerTestProtocol(t, right, 1)
	startTestStack(t, left)
	startTestStack(t, right)
	connectTest(t, right, a)
	old := awaitPeer(t, lfam, lWake)
	packet, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := transport.NewEndpoint(b.Key, packet, transport.Interop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close(); _ = packet.Close() })
	restarted := &transporttest.Endpoint{Endpoint: endpoint, Eth: endpoint.Ethp2p(), Packet: packet, Key: b.Key}
	next := newTestStack(t, restarted)
	registerTestProtocol(t, next, 1)
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
	prot, _ := registerTestProtocol(t, s, 1)
	startTestStack(t, s)
	// The client presents the server's record as its own.
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	raw, err := transporttest.DialHello(ctx, client, server, transporttest.Hello{Protocols: []wire.Protocol{1}, Record: server.Record(t, 1).Encode()})
	if err != nil {
		t.Fatal(err)
	}
	code, err := raw.ReadGoAway()
	if err != nil {
		t.Fatal(err)
	}
	if code != wire.ControlViolation {
		t.Fatalf("record rejection code = %s", code)
	}
	if _, ok := prot.Next(); ok {
		t.Fatal("invalid record produced event")
	}
}
