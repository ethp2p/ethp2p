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

func TestProtocolsSharedByCode(t *testing.T) {
	server := transporttest.NewEndpoint(t)
	s := newTestStack(t, server)
	shared, sharedWake := registerTestProtocol(t, s, 1)
	shared2, shared2Wake := registerTestProtocol(t, s, 2)
	unshared, _ := registerTestProtocol(t, s, 7)
	startTestStack(t, s)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	// A peer whose protocols are disjoint from ours shares nothing: the view
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
	if _, err := transporttest.SendHello(ctx, disjointConn, transporttest.Hello{Protocols: []wire.Protocol{9}}); err != nil {
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

	// A peer listing protocols 1 and 2 and others we do not serve shares
	// protocols 1 and 2 only.
	peer := transporttest.NewEndpoint(t)
	raw, err := transporttest.DialHello(ctx, peer, server, transporttest.Hello{Protocols: []wire.Protocol{1, 2, 9}})
	if err != nil {
		t.Fatal(err)
	}
	conn := raw.Conn
	if awaitPeer(t, shared, sharedWake).ID() != peer.Endpoint.PeerID() {
		t.Fatal("wrong identity")
	}
	awaitPeer(t, shared2, shared2Wake)
	if ev, ok := unshared.Next(); ok {
		t.Fatalf("protocol 7 produced %+v for a peer that does not list it", ev)
	}
	// A protocol not shared with this peer, and a protocol we do not
	// register, are both unsupported.
	for _, sel := range []wire.Protocol{7, 9} {
		stream, err := transporttest.OpenUniStream(ctx, conn, sel)
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.SetWriteDeadline(time.Now().Add(testTimeout))
		_, err = stream.Write(bytes.Repeat([]byte{0xa5}, 1<<20))
		reset, ok := errors.AsType[*quic.StreamError](err)
		if !ok || !reset.Remote || uint64(reset.ErrorCode) != wire.UnsupportedProtocol.Wire() {
			t.Fatalf("%v write = %v, want remote wire %d", sel, err, wire.UnsupportedProtocol.Wire())
		}
	}
	writeProtocolPayload(t, conn, 2, []byte("two"))
	ev := awaitStreamEvent(t, shared2, shared2Wake)
	if got, err := io.ReadAll(ev.Stream); err != nil || string(got) != "two" {
		t.Fatalf("payload = %q, %v", got, err)
	}
}

func TestPeerCloseKeepsOtherProtocol(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	left, right := newTestStack(t, a), newTestStack(t, b)
	leftA, leftAWake := registerTestProtocol(t, left, 1)
	leftB, leftBWake := registerTestProtocol(t, left, 2)
	rightA, rightAWake := registerTestProtocol(t, right, 1)
	rightB, rightBWake := registerTestProtocol(t, right, 2)
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

	// The other protocol still delivers streams from the same view.
	out, err := rightPeerB.OpenUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = out.Write([]byte("still here"))
	_ = out.Close()
	in := awaitStreamEvent(t, leftB, leftBWake)
	got, err := io.ReadAll(in.Stream)
	if err != nil || !bytes.Equal(got, []byte("still here")) {
		t.Fatalf("protocol B stream = %q, %v", got, err)
	}

	// The closed handle's streams are reset, not routed.
	closed, err := rightPeerA.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = closed.SetReadDeadline(time.Now().Add(testTimeout))
	_, err = closed.Read(make([]byte, 1))
	reset, ok := errors.AsType[*quic.StreamError](err)
	if !ok || uint64(reset.ErrorCode) != wire.UnsupportedProtocol.Wire() {
		t.Fatalf("closed handle read = %v, want wire %d", err, wire.UnsupportedProtocol.Wire())
	}

	// The remote handle does not end: no stack message announces local closure.
	if err := rightPeerA.Context().Err(); err != nil {
		t.Fatalf("remote handle ended: %v", err)
	}
	if ev, ok := rightA.Next(); ok {
		t.Fatalf("remote handle ended with %+v", ev)
	}
}

func TestPeerCloseLastProtocolClosesView(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	left, right := newTestStack(t, a), newTestStack(t, b)
	leftProt, leftWake := registerTestProtocol(t, left, 1)
	rightProt, rightWake := registerTestProtocol(t, right, 1)
	startTestStack(t, left)
	startTestStack(t, right)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	if err := left.Connect(ctx, b.Record(t, 1)); err != nil {
		t.Fatal(err)
	}
	leftPeer := awaitPeer(t, leftProt, leftWake)
	awaitPeer(t, rightProt, rightWake)

	leftPeer.Close(wire.Refused)
	// The local handle ends with its own code.
	down := awaitEvent(t, leftProt, leftWake)
	if down.Kind != PeerDown || down.Code != wire.Refused {
		t.Fatalf("local down = %+v", down)
	}
	// With no protocol holding the view, it closes; the peer learns the reason.
	remote := awaitEvent(t, rightProt, rightWake)
	if remote.Kind != PeerDown || remote.Code != wire.NoSharedProtocols {
		t.Fatalf("remote down = %+v", remote)
	}
	waitFor(t, func() bool { return len(left.Connections()) == 0 })
}
