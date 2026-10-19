package ethp2p

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/quic-go/quic-go"
)

func TestFamiliesSharedByCode(t *testing.T) {
	server := transporttest.NewEndpoint(t)
	s := newTestStack(t, server)
	shared, sharedWake := registerTestFamily(t, s, 1,
		ProtocolSpec{Protocol: 1, MaxQueued: 8},
		ProtocolSpec{Protocol: 2, MaxQueued: 8})
	unshared, _ := registerTestFamily(t, s, 7, ProtocolSpec{Protocol: 1, MaxQueued: 8})
	startTestStack(t, s)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	// A peer whose families are disjoint from ours shares nothing: the view
	// closes and no event is queued.
	// The stack rejects the view only after our HELLO arrives, so neither Dial
	// nor the HELLO write races the rejection. Once both endpoints also
	// release the connection's libp2p view, the rejection closes the
	// connection with its code.
	disjoint := transporttest.NewEndpoint(t)
	disjointConn, err := disjoint.Eth.Dial(ctx, server.Endpoint.Addr(), server.Endpoint.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transporttest.SendHello(ctx, disjointConn, transporttest.Hello{Families: []wire.Family{9}}); err != nil {
		t.Fatal(err)
	}
	releaseLibp2pView(t, disjoint)
	releaseLibp2pView(t, server)
	select {
	case <-disjointConn.Done():
	case <-time.After(testTimeout):
		t.Fatal("unshared view remained open")
	}
	if code := disjointConn.CloseCode(); code != wire.NoSharedProtocols {
		t.Fatalf("unshared close code = %s", code)
	}
	if ev, ok := shared.Next(); ok {
		t.Fatalf("unshared peer produced %+v", ev)
	}

	// A peer listing family 1 and others we do not serve shares family 1 only,
	// whatever protocols it implements within it.
	peer := transporttest.NewEndpoint(t)
	raw, err := transporttest.DialHello(ctx, peer, server, transporttest.Hello{Families: []wire.Family{1, 9}})
	if err != nil {
		t.Fatal(err)
	}
	conn := raw.Conn
	if awaitPeer(t, shared, sharedWake).ID() != peer.Endpoint.PeerID() {
		t.Fatal("wrong identity")
	}
	if ev, ok := unshared.Next(); ok {
		t.Fatalf("family 7 produced %+v for a peer that does not list it", ev)
	}
	// A protocol the shared family does not register, and a family not
	// shared, are both unsupported.
	for _, sel := range []wire.Selector{{Family: 1, Protocol: 3}, {Family: 7, Protocol: 1}} {
		stream, err := transporttest.OpenUniStream(ctx, conn, sel)
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.SetWriteDeadline(time.Now().Add(testTimeout))
		_, err = stream.Write(bytes.Repeat([]byte{0xa5}, 1<<20))
		reset, ok := errors.AsType[*quic.StreamError](err)
		if !ok || !reset.Remote || uint64(reset.ErrorCode) != wire.UnsupportedSelector.Wire() {
			t.Fatalf("%v write = %v, want remote wire %d", sel, err, wire.UnsupportedSelector.Wire())
		}
	}
	writeSelectorPayload(t, conn, wire.Selector{Family: 1, Protocol: 2}, []byte("two"))
	ev := awaitStreamEvent(t, shared, sharedWake)
	if ev.Protocol != 2 {
		t.Fatalf("delivered protocol = %d, want 2", ev.Protocol)
	}
	if got, err := io.ReadAll(ev.Stream); err != nil || string(got) != "two" {
		t.Fatalf("payload = %q, %v", got, err)
	}
}

func TestPeerCloseKeepsOtherFamily(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	left, right := newTestStack(t, a), newTestStack(t, b)
	leftA, leftAWake := registerTestFamily(t, left, 1, ProtocolSpec{Protocol: testProtocol, MaxQueued: 8})
	leftB, leftBWake := registerTestFamily(t, left, 2, ProtocolSpec{Protocol: testProtocol, MaxQueued: 8})
	rightA, rightAWake := registerTestFamily(t, right, 1, ProtocolSpec{Protocol: testProtocol, MaxQueued: 8})
	rightB, rightBWake := registerTestFamily(t, right, 2, ProtocolSpec{Protocol: testProtocol, MaxQueued: 8})
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
	out, err := rightPeerB.OpenUniStream(ctx, testProtocol)
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

	// The closed handle's streams are reset, not routed.
	closed, err := rightPeerA.OpenStream(ctx, testProtocol)
	if err != nil {
		t.Fatal(err)
	}
	_ = closed.SetReadDeadline(time.Now().Add(testTimeout))
	_, err = closed.Read(make([]byte, 1))
	reset, ok := errors.AsType[*quic.StreamError](err)
	if !ok || uint64(reset.ErrorCode) != wire.UnsupportedSelector.Wire() {
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

func TestPeerCloseLastFamilyClosesView(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	left, right := newTestStack(t, a), newTestStack(t, b)
	leftFam, leftWake := registerTestFamily(t, left, 1, ProtocolSpec{Protocol: testProtocol, MaxQueued: 8})
	rightFam, rightWake := registerTestFamily(t, right, 1, ProtocolSpec{Protocol: testProtocol, MaxQueued: 8})
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
	fam, wake := registerTestFamily(t, s, 1, ProtocolSpec{Protocol: testProtocol, MaxQueued: 1})
	startTestStack(t, s)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	client := transporttest.NewEndpoint(t)
	raw, err := transporttest.DialHello(ctx, client, server, transporttest.Hello{Families: []wire.Family{1}})
	if err != nil {
		t.Fatal(err)
	}
	clientConn := raw.Conn
	first, err := transporttest.OpenUniStream(ctx, clientConn, wire.Selector{Family: 1, Protocol: testProtocol})
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
	second, err := transporttest.OpenUniStream(ctx, clientConn, wire.Selector{Family: 1, Protocol: testProtocol})
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
