package nettest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/internal/devhook"
	"github.com/ethp2p/ethp2p/internal/trace"
	"github.com/ethp2p/ethp2p/transport"
)

func expectValuePanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		got := recover()
		if got == nil || !strings.Contains(fmt.Sprint(got), "trace events must be values") {
			t.Fatalf("panic = %v, want value-event diagnostic", got)
		}
	}()
	fn()
}

func TestMatchWildcardsAndOrder(t *testing.T) {
	Run(t, func(t *testing.T, n *Net) {
		r := n.Trace()
		a := n.Node("a")
		n.Node("b")
		r.emit("a", trace.SessionStarted{Channel: "blocks", Message: "m1", Role: "origin"})
		r.emit("b", trace.SessionStarted{Channel: "blocks", Message: "m2", Role: "relay"})
		r.emit("a", trace.SessionDecoded{Channel: "blocks", Message: "m1"})
		if !Match(a, trace.SessionStarted{Message: "m1"}).Match(r.Records()[0]) {
			t.Fatal("zero-valued fields must be wildcards")
		}
		if Match(a, trace.SessionStarted{Message: "m2"}).Match(r.Records()[1]) {
			t.Fatal("node restriction ignored")
		}
		r.Require(t, Match(a, trace.SessionStarted{Message: "m1"}), Match(a, trace.SessionDecoded{Message: "m1"}))
		r.RequireNone(t, Match(a, trace.SessionDecoded{Message: "m2"}))
		if got := r.Count(Where[trace.SessionStarted](nil, func(e trace.SessionStarted) bool { return e.Channel == "blocks" })); got != 2 {
			t.Fatalf("count=%d", got)
		}
	})
}

func TestRunAndAwait(t *testing.T) {
	Run(t, func(t *testing.T, n *Net) {
		start := time.Now()
		if _, ok := n.await(Match(nil, trace.SessionDecoded{Message: "never"}), 5*time.Millisecond); ok {
			t.Fatal("unexpected record")
		}
		if elapsed := time.Since(start); elapsed != 5*time.Millisecond {
			t.Fatalf("virtual timeout elapsed %s", elapsed)
		}
		a, b := n.Node("a"), n.Node("b")
		ca, cb := a.Channel("blocks"), b.Channel("blocks")
		n.Connect(a, b)
		n.Await(t, Match(a, trace.PeerSubscribed{Channel: "blocks"}), time.Second)
		ca.Publish(t, "m1", []byte("hello"))
		n.Await(t, Match(b, trace.SessionDecoded{Channel: "blocks", Message: "m1"}), time.Second)
		n.Settle()
		cb.RequireReceived(t, "m1", []byte("hello"))
	})
}

func TestRecorderEventSemantics(t *testing.T) {
	Run(t, func(t *testing.T, n *Net) {
		a, b := n.Node("a"), n.Node("b")
		want := errors.New("sentinel")
		n.recorder.emit("a", trace.ChunkError{Peer: string(b.ID), Err: fmt.Errorf("wrapped: %w", want)})
		if !Match(a, trace.ChunkError{Err: want}).Match(n.Trace().Records()[0]) {
			t.Fatal("wrapped sentinel did not match with errors.Is")
		}
		if Match(a, trace.ChunkError{Err: errors.New("sentinel")}).Match(n.Trace().Records()[0]) {
			t.Fatal("distinct errors matched by message")
		}
		if got := (trace.PeerSubscribed{Peer: string(b.ID)}).String(); !strings.Contains(got, fmt.Sprintf("peer=%x", []byte(b.ID))[:13]) {
			t.Fatalf("peer ID was not hex encoded: %q", got)
		}
		if got := (trace.ChunkError{Err: errors.New("bad\nchunk")}).String(); strings.Contains(got, "\n") {
			t.Fatalf("event is not one line: %q", got)
		}
		if got := n.Trace().timeline(); !strings.Contains(got, "peer=b") {
			t.Fatalf("timeline lacks peer name: %q", got)
		}
		n.recorder.emit("a", trace.PeerHandshook{Channels: []string{string(b.ID)}})
		if got := n.Trace().timeline(); !strings.Contains(got, `channels=["b"]`) {
			t.Fatalf("timeline lacks peer-list name: %q", got)
		}
		expectValuePanic(t, func() { Match(nil, &trace.SessionStarted{}) })
		expectValuePanic(t, func() { Where[*trace.SessionStarted](nil, func(*trace.SessionStarted) bool { return true }) })
		expectValuePanic(t, func() { n.recorder.emit("a", &trace.SessionStarted{}) })
	})
}

func TestUnexpectedCloseRetainsJoinedErrors(t *testing.T) {
	Run(t, func(t *testing.T, _ *Net) {
		want := errors.New("real close failure")
		got := unexpectedClose(errors.Join(transport.ErrClosed, want))
		if !errors.Is(got, want) {
			t.Fatalf("unexpectedClose lost %v: %v", want, got)
		}
	})
}

func TestCurrentPeersAndContinuousCollection(t *testing.T) {
	Run(t, func(t *testing.T, n *Net) {
		a, b := n.Node("a"), n.Node("b")
		c := a.Channel("collector")
		n.recorder.emit("a", trace.PeerSubscribed{Peer: string(b.ID), Channel: "collector"})
		n.AwaitPeers(t, a, "collector", b)
		n.recorder.emit("a", trace.PeerUnsubscribed{Peer: string(b.ID), Channel: "collector"})
		n.AwaitPeers(t, a, "collector")
		for i := range 200 {
			c.recv <- broadcast.FullMessage{ChannelID: "collector", MessageID: broadcast.MessageID(fmt.Sprint(i)), Data: []byte{byte(i)}}
		}
		n.Settle()
		if got := len(c.Received()); got != 200 {
			t.Fatalf("collector retained %d/200 messages", got)
		}
		c.AwaitReceived(t, "199", []byte{199})
	})
}

func TestReceivedSettlesQueuedDelivery(t *testing.T) {
	Run(t, func(t *testing.T, n *Net) {
		c := n.Node("a").Channel("queued")
		c.recv <- broadcast.FullMessage{ChannelID: "queued", MessageID: "queued-message", Data: []byte("ready")}
		// RequireNotReceived uses the same settled snapshot as Received. A
		// pre-collector snapshot would miss this already queued delivery.
		if got := c.Received(); len(got) != 1 || got[0].MessageID != "queued-message" {
			t.Fatalf("settled deliveries = %+v, want queued-message", got)
		}
	})
}

func TestCloseRemovesCurrentLink(t *testing.T) {
	Run(t, func(t *testing.T, n *Net) {
		a, b := n.Node("a"), n.Node("b")
		n.Connect(a, b)
		b.Close()
		defer func() {
			if got := recover(); got != "nodes are not connected" {
				t.Fatalf("Disconnect panic = %v, want nodes are not connected", got)
			}
		}()
		n.Disconnect(a, b)
	})
}

func TestGateTracksCrossingsAndRunReleasesIt(t *testing.T) {
	Run(t, func(t *testing.T, n *Net) {
		a, b := n.Node("a"), n.Node("b")
		gate := b.Hold(devhook.Site{Point: devhook.PointHandshake, Peer: string(a.ID)})
		n.Connect(a, b)
		n.Await(t, Match(b, trace.GateHeld{Point: devhook.PointHandshake.String(), Peer: string(a.ID)}), DefaultTimeout)
		if got := gate.Held(); got != 1 {
			t.Fatalf("held crossings = %d, want 1", got)
		}
		gate.Release()
		gate.Release()
		n.Await(t, Match(b, trace.GateReleased{Point: devhook.PointHandshake.String(), Peer: string(a.ID)}), DefaultTimeout)
		if got := gate.Held(); got != 0 {
			t.Fatalf("held crossings after release = %d, want 0", got)
		}
	})
	Run(t, func(t *testing.T, n *Net) {
		a, b := n.Node("a"), n.Node("b")
		b.Hold(devhook.Site{Point: devhook.PointHandshake, Peer: string(a.ID)})
		n.Connect(a, b)
		n.Await(t, Match(b, trace.GateHeld{Point: devhook.PointHandshake.String()}), DefaultTimeout)
		// Run must release this gate before node shutdown.
	})
}

func TestGateAccountsOnTheHoldThatBlocks(t *testing.T) {
	Run(t, func(t *testing.T, n *Net) {
		a := n.Node("a")
		site := devhook.Site{Point: devhook.PointChunkRead, Peer: "p"}
		first := a.Hold(site)
		first.Release()
		second := a.Hold(site)
		done := make(chan bool, 1)
		go func() { done <- a.wait(t.Context(), site) }()
		n.Await(t, Match(a, trace.GateHeld{Point: site.Point.String(), Peer: "p"}), DefaultTimeout)
		if got, want := [2]int{first.Held(), second.Held()}, [2]int{0, 1}; got != want {
			t.Fatalf("held (first, second) = %v, want %v", got, want)
		}
		second.Release()
		if !<-done {
			t.Fatal("released crossing reported cancellation")
		}
		if got := second.Held(); got != 0 {
			t.Fatalf("held after release = %d, want 0", got)
		}
	})
}

func TestGateEmitsOnlyForBlockedCrossings(t *testing.T) {
	Run(t, func(t *testing.T, n *Net) {
		a := n.Node("a")
		released := devhook.Site{Point: devhook.PointChunkRead, Peer: "released"}
		a.Hold(released).Release()
		if !a.wait(t.Context(), released) {
			t.Fatal("released gate reported cancellation")
		}
		canceled := devhook.Site{Point: devhook.PointChunkRead, Peer: "canceled"}
		gate := a.Hold(canceled)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if a.wait(ctx, canceled) {
			t.Fatal("canceled crossing passed an active gate")
		}
		if got := gate.Held(); got != 0 {
			t.Fatalf("held after canceled crossing = %d, want 0", got)
		}
		n.Trace().RequireNone(t, Match(a, trace.GateHeld{}))
		n.Trace().RequireNone(t, Match(a, trace.GateReleased{}))
	})
}
