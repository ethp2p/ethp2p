package ethp2p

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/quic-go/quic-go"
)

func TestFamilySharedOnlyWhenComplete(t *testing.T) {
	server := transporttest.NewEndpoint(t)
	s := newTestStack(t, server)
	fam, wake := registerTestFamily(t, s,
		ProtocolSpec{Selector: 1, MaxQueued: 8},
		ProtocolSpec{Selector: 2, MaxQueued: 8})
	startTestStack(t, s)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	// A peer missing one selector shares nothing: the view closes and no event
	// is queued.
	partial := transporttest.NewEndpoint(t)
	if err := partial.Eth.SetHello(transport.Hello{Selectors: []wire.Selector{1}}); err != nil {
		t.Fatal(err)
	}
	partialConn, err := partial.Eth.Dial(ctx, server.Shared.Addr(), server.Shared.PeerID())
	if err != nil {
		// The rejection can arrive before Dial returns.
		if closed, ok := errors.AsType[*transport.ViewClosedError](err); !ok || !closed.Remote || closed.Code != wire.NoSharedProtocols {
			t.Fatal(err)
		}
	} else {
		select {
		case <-partialConn.Done():
		case <-time.After(testTimeout):
			t.Fatal("unshared view remained open")
		}
		if code := partialConn.CloseCode(); code != wire.NoSharedProtocols {
			t.Fatalf("unshared close code = %s", code)
		}
	}
	if ev, ok := fam.Next(); ok {
		t.Fatalf("unshared peer produced %+v", ev)
	}

	// A peer listing every selector, plus extras, shares the family.
	full := transporttest.NewEndpoint(t)
	if err := full.Eth.SetHello(transport.Hello{Selectors: []wire.Selector{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	if _, err := full.Eth.Dial(ctx, server.Shared.Addr(), server.Shared.PeerID()); err != nil {
		t.Fatal(err)
	}
	peer := awaitPeer(t, fam, wake)
	if peer.ID() != full.Shared.PeerID() {
		t.Fatal("wrong identity")
	}
}

func TestPeerUpHandleCanCloseImmediately(t *testing.T) {
	f := &Family{wake: make(chan struct{}, 1)}
	s := &Stack{peers: make(map[transport.PeerID]*peerSupervisor)}
	sup := &peerSupervisor{stack: s, id: "remote", wake: make(chan struct{}, 1)}
	s.peers[sup.id] = sup
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f.addPeer(&Peer{ctx: ctx, cancel: cancel, sup: sup, family: f})

	up, ok := f.Next()
	if !ok || up.Kind != PeerUp {
		t.Fatalf("first event = %+v, %v; want PeerUp", up, ok)
	}
	up.Peer.Close(wire.Refused)
	if err := up.Peer.Context().Err(); err != context.Canceled {
		t.Fatalf("peer context after Close = %v", err)
	}
	down, ok := f.Next()
	if !ok || down.Kind != PeerDown || down.Code != wire.Refused {
		t.Fatalf("second event = %+v, %v; want PeerDown Refused", down, ok)
	}
}

func TestPeerCloseKeepsOtherFamily(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	left, right := newTestStack(t, a), newTestStack(t, b)
	leftA, leftAWake := registerTestFamily(t, left, ProtocolSpec{Selector: 1, MaxQueued: 8})
	leftB, leftBWake := registerTestFamily(t, left, ProtocolSpec{Selector: 2, MaxQueued: 8})
	rightA, rightAWake := registerTestFamily(t, right, ProtocolSpec{Selector: 1, MaxQueued: 8})
	rightB, rightBWake := registerTestFamily(t, right, ProtocolSpec{Selector: 2, MaxQueued: 8})
	startTestStack(t, left)
	startTestStack(t, right)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	if err := left.Connect(ctx, b.Record(t, 1)); err != nil {
		t.Fatal(err)
	}
	leftPeer := awaitPeer(t, leftA, leftAWake)
	awaitPeer(t, leftB, leftBWake)
	rightPeerA := awaitPeer(t, rightA, rightAWake)
	rightPeerB := awaitPeer(t, rightB, rightBWake)

	leftPeer.Close(wire.Refused)
	down := awaitEvent(t, leftA, leftAWake)
	if down.Kind != PeerDown || down.Code != wire.Refused || down.Peer != leftPeer {
		t.Fatalf("close down = %+v", down)
	}
	if ev, ok := leftA.Next(); ok {
		t.Fatalf("extra event after PeerDown: %+v", ev)
	}

	// The other family still delivers streams from the same view.
	out, err := rightPeerB.OpenUniStream(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = out.Write([]byte("still here"))
	_ = out.Close()
	in := awaitStreamEvent(t, leftB, leftBWake)
	got, err := io.ReadAll(in.Stream)
	if err != nil || !bytes.Equal(got, []byte("still here")) {
		t.Fatalf("family B stream = %q, %v", got, err)
	}

	// The closed handle's selectors are reset, not routed.
	closed, err := rightPeerA.OpenStream(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	_ = closed.SetReadDeadline(time.Now().Add(testTimeout))
	_, err = closed.Read(make([]byte, 1))
	reset, ok := errors.AsType[*transport.StreamResetError](err)
	if !ok || reset.Code != wire.UnsupportedSelector.Wire() {
		t.Fatalf("closed handle read = %v, want wire %d", err, wire.UnsupportedSelector.Wire())
	}

	// The remote handle does not end: no stack message announces local closure.
	if err := rightPeerA.Context().Err(); err != nil {
		t.Fatalf("remote handle ended: %v", err)
	}
	if ev, ok := rightA.Next(); ok {
		t.Fatalf("remote handle ended with %+v", ev)
	}
}

func TestPeerCloseCancelsBlockedOpenWithoutClosingOtherFamily(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	left, right := newTestStack(t, a), newTestStack(t, b)
	leftA, leftAWake := registerTestFamily(t, left, ProtocolSpec{Selector: 1, MaxQueued: 300})
	leftB, leftBWake := registerTestFamily(t, left, ProtocolSpec{Selector: 2, MaxQueued: 8})
	rightA, rightAWake := registerTestFamily(t, right, ProtocolSpec{Selector: 1, MaxQueued: 300})
	rightB, rightBWake := registerTestFamily(t, right, ProtocolSpec{Selector: 2, MaxQueued: 8})
	startTestStack(t, left)
	startTestStack(t, right)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	if err := left.Connect(ctx, b.Record(t, 1)); err != nil {
		t.Fatal(err)
	}
	leftPeerA := awaitPeer(t, leftA, leftAWake)
	awaitPeer(t, leftB, leftBWake)
	rightPeerA := awaitPeer(t, rightA, rightAWake)
	rightPeerB := awaitPeer(t, rightB, rightBWake)

	// Interop allows 256 incoming uni streams; the control stream uses one.
	// Keep the streams open so the receiver cannot return their credit.
	var held []SendStream
	for range 255 {
		stream, err := rightPeerA.OpenUniStream(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, stream)
	}
	defer func() {
		for _, stream := range held {
			stream.CancelWrite(wire.Unspecified)
		}
	}()
	probeCtx, stopProbe := context.WithTimeout(ctx, 50*time.Millisecond)
	_, probeErr := rightPeerA.OpenUniStream(probeCtx, 1)
	stopProbe()
	if !errors.Is(probeErr, context.DeadlineExceeded) {
		t.Fatalf("stream credit was not exhausted: %v", probeErr)
	}
	blockedCtx, blockedCancel := context.WithTimeout(ctx, 2*time.Second)
	defer blockedCancel()
	closer := time.AfterFunc(100*time.Millisecond, func() { rightPeerA.Close(wire.Refused) })
	defer closer.Stop()
	stream, err := rightPeerA.OpenUniStream(blockedCtx, 1)
	if err == nil {
		stream.CancelWrite(wire.Unspecified)
		t.Fatal("open succeeded without stream credit")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked open after Peer.Close = %v, want context.Canceled", err)
	}
	if err := blockedCtx.Err(); err != nil {
		t.Fatalf("open waited for caller deadline after Peer.Close: %v", err)
	}

	// Releasing the queued streams restores credit for the other family.
	leftPeerA.Close(wire.Refused)
	for _, stream := range held {
		stream.CancelWrite(wire.Unspecified)
	}
	other, err := rightPeerB.OpenUniStream(ctx, 2)
	if err != nil {
		t.Fatalf("other family cannot open a stream: %v", err)
	}
	_ = other.Close()
	_ = awaitStreamEvent(t, leftB, leftBWake)
}

func TestPeerCloseLastFamilyClosesView(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	left, right := newTestStack(t, a), newTestStack(t, b)
	leftFam, leftWake := registerTestFamily(t, left, ProtocolSpec{Selector: 1, MaxQueued: 8})
	rightFam, rightWake := registerTestFamily(t, right, ProtocolSpec{Selector: 1, MaxQueued: 8})
	startTestStack(t, left)
	startTestStack(t, right)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	if err := left.Connect(ctx, b.Record(t, 1)); err != nil {
		t.Fatal(err)
	}
	leftPeer := awaitPeer(t, leftFam, leftWake)
	awaitPeer(t, rightFam, rightWake)

	leftPeer.Close(wire.Refused)
	// The local handle ends with its own code.
	down := awaitEvent(t, leftFam, leftWake)
	if down.Kind != PeerDown || down.Code != wire.Refused {
		t.Fatalf("local down = %+v", down)
	}
	// With no family holding the view, it closes; the peer learns the reason.
	remote := awaitEvent(t, rightFam, rightWake)
	if remote.Kind != PeerDown || remote.Code != wire.NoSharedProtocols {
		t.Fatalf("remote down = %+v", remote)
	}
	waitFor(t, func() bool { return len(left.Connections()) == 0 })
}

func TestMaxQueuedResetsWithUnspecified(t *testing.T) {
	server := transporttest.NewEndpoint(t)
	s := newTestStack(t, server)
	fam, wake := registerTestFamily(t, s, ProtocolSpec{Selector: 1, MaxQueued: 1})
	startTestStack(t, s)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	client := transporttest.NewEndpoint(t)
	if err := client.Eth.SetHello(transport.Hello{Selectors: []wire.Selector{1}}); err != nil {
		t.Fatal(err)
	}
	clientConn, err := client.Eth.Dial(ctx, server.Shared.Addr(), server.Shared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	first, err := clientConn.OpenUniStream(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	// The first stream is queued before the second exists, so the second is
	// deterministically the one over the bound.
	waitPending(t, fam, 1)
	second, err := clientConn.OpenUniStream(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Write([]byte("second")); err != nil {
		t.Fatal(err)
	}
	_ = second.SetWriteDeadline(time.Now().Add(testTimeout))
	_, err = second.Write(make([]byte, 2<<20))
	reset, ok := errors.AsType[*quic.StreamError](err)
	if !ok || !reset.Remote || uint64(reset.ErrorCode) != wire.Unspecified.Wire() {
		t.Fatalf("over-bound write = %v, want remote wire %d", err, wire.Unspecified.Wire())
	}

	peer := awaitPeer(t, fam, wake)
	event := awaitStreamEvent(t, fam, wake)
	if event.Peer != peer {
		t.Fatal("stream changed peer handle")
	}
	got, err := io.ReadAll(event.Stream)
	if err != nil || !bytes.Equal(got, []byte("first")) {
		t.Fatalf("queued stream = %q, %v", got, err)
	}
	if ev, ok := fam.Next(); ok {
		t.Fatalf("over-bound stream delivered: %+v", ev)
	}
}

func TestPeerDownAfterDuplicate(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	left, right := newTestStack(t, a), newTestStack(t, b)
	fam, wake := registerTestFamily(t, left, ProtocolSpec{Selector: 1, MaxQueued: 8})
	registerTestFamily(t, right, ProtocolSpec{Selector: 1, MaxQueued: 8})
	startTestStack(t, left)
	startTestStack(t, right)
	connectTest(t, right, a)
	old := awaitPeer(t, fam, wake)

	// A restarted peer redials from the same identity; same-direction views
	// keep the newer one.
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

	// The replaced handle's PeerDown precedes the new handle's PeerUp on the
	// same family.
	down, up := awaitEvent(t, fam, wake), awaitEvent(t, fam, wake)
	if down.Kind != PeerDown || down.Code != wire.Duplicate || down.Peer != old {
		t.Fatalf("replaced down = %+v", down)
	}
	if up.Kind != PeerUp || up.Peer == old {
		t.Fatalf("replacement up = %+v", up)
	}
}
