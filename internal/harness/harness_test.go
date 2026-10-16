package harness

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/internal/trace"
)

func TestMatchWildcardsAndOrder(t *testing.T) {
	Run(t, func(t *testing.T, n *Net) {
		r := n.Trace()
		a := n.Node("a")
		b := n.Node("b")
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
		if got := r.Count(Match(a, trace.SessionDecoded{Message: "m2"})); got != 0 {
			t.Fatalf("count=%d", got)
		}
		if got := r.Count(Where[trace.SessionStarted](a, func(e trace.SessionStarted) bool { return e.Channel == "blocks" })); got != 1 {
			t.Fatalf("count=%d", got)
		}
		want := errors.New("sentinel")
		r.emit("a", trace.ChunkError{Peer: string(b.ID), Err: fmt.Errorf("wrapped: %w", want)})
		if !Match(a, trace.ChunkError{Err: want}).Match(r.Records()[3]) {
			t.Fatal("wrapped sentinel did not match with errors.Is")
		}
		if Match(a, trace.ChunkError{Err: errors.New("sentinel")}).Match(r.Records()[3]) {
			t.Fatal("distinct errors matched by message")
		}
	})
}

func TestRunAndAwait(t *testing.T) {
	Run(t, func(t *testing.T, n *Net) {
		start := time.Now()
		if _, ok := n.await(eventMatcher{event: trace.SessionDecoded{Message: "never"}}, 5*time.Millisecond); ok {
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
