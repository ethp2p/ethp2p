package e2e

import (
	"testing"

	"github.com/ethp2p/ethp2p/internal/harness"
	"github.com/ethp2p/ethp2p/internal/trace"
)

func TestRelayForwardsChunksToTwoPeers(t *testing.T) {
	harness.Run(t, func(t *testing.T, n *harness.Net) {
		origin, relay, left, right := n.Node("origin"), n.Node("relay"), n.Node("left"), n.Node("right")
		co := fixedRS(origin, "relay-fanout", 1, 0)
		fixedRS(relay, "relay-fanout", 1, 0)
		cl, cr := fixedRS(left, "relay-fanout", 1, 0), fixedRS(right, "relay-fanout", 1, 0)
		n.Connect(origin, relay)
		n.Connect(relay, left)
		n.Connect(relay, right)
		n.AwaitPeers(t, relay, "relay-fanout", origin, left, right)
		data := payload(4096)
		co.Publish(t, "relay-fanout-message", data)
		cl.AwaitReceived(t, "relay-fanout-message", data)
		cr.AwaitReceived(t, "relay-fanout-message", data)
		for _, peer := range []*harness.Node{left, right} {
			if got := n.Trace().Count(harness.Match(relay, trace.ChunkSent{Peer: string(peer.ID), Message: "relay-fanout-message"})); got != 1 {
				t.Fatalf("relay sent %d chunks to %s, want exactly one", got, peer.Name)
			}
		}
	})
}
