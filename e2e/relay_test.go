package e2e

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/internal/devhook"
	"github.com/ethp2p/ethp2p/internal/nettest"
	"github.com/ethp2p/ethp2p/internal/trace"
)

func TestRelayForwardsChunksToTwoPeers(t *testing.T) {
	nettest.Run(t, func(t *testing.T, n *nettest.Net) {
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
		for _, peer := range []*nettest.Node{left, right} {
			if got := n.Trace().Count(nettest.Match(relay, trace.ChunkSent{Peer: string(peer.ID), Message: "relay-fanout-message"})); got != 1 {
				t.Fatalf("relay sent %d chunks to %s, want exactly one", got, peer.Name)
			}
		}
	})
}

func TestPeerChurnKeepsRemainingChunkDispatch(t *testing.T) {
	var firstWrite atomic.Bool
	nettest.Run(t, func(t *testing.T, n *nettest.Net) {
		gates := make(chan *nettest.Gate, 1)
		var origin *nettest.Node
		origin = n.Node("origin", nettest.ChunkFaults(func(peer *nettest.Node, _ broadcast.ChannelID, _ broadcast.MessageID, _ []byte) devhook.ChunkFault {
			if firstWrite.CompareAndSwap(false, true) {
				gates <- origin.Hold(devhook.Site{Point: devhook.PointChunkWrite, Peer: string(peer.ID), Channel: "churn-dispatch", Message: "churn-dispatch-message"})
			}
			return devhook.ChunkFault{}
		}))
		relay, left, right := n.Node("relay"), n.Node("left"), n.Node("right")
		co := smallRS(origin, "churn-dispatch")
		smallRS(relay, "churn-dispatch")
		cl := smallRS(left, "churn-dispatch")
		smallRS(right, "churn-dispatch")
		n.Connect(origin, relay)
		n.Connect(relay, left)
		n.Connect(relay, right)
		n.AwaitPeers(t, relay, "churn-dispatch", origin, left, right)
		data := payload(4096)
		co.Publish(t, "churn-dispatch-message", data)
		n.Await(t, nettest.Match(origin, trace.GateHeld{Point: devhook.PointChunkWrite.String(), Peer: string(relay.ID), Message: "churn-dispatch-message"}), nettest.DefaultTimeout)
		n.Await(t, nettest.Match(relay, trace.ChunkSent{Peer: string(left.ID), Message: "churn-dispatch-message"}), nettest.DefaultTimeout)
		n.Await(t, nettest.Match(relay, trace.ChunkSent{Peer: string(right.ID), Message: "churn-dispatch-message"}), nettest.DefaultTimeout)
		n.Settle()
		firstLeft := n.Trace().Count(nettest.Match(relay, trace.ChunkSent{Peer: string(left.ID), Message: "churn-dispatch-message"}))
		firstRight := n.Trace().Count(nettest.Match(relay, trace.ChunkSent{Peer: string(right.ID), Message: "churn-dispatch-message"}))
		n.Disconnect(relay, right)
		n.AwaitPeers(t, relay, "churn-dispatch", origin, left)
		(<-gates).Release()
		cl.AwaitReceived(t, "churn-dispatch-message", data)
		if after := n.Trace().Count(nettest.Match(relay, trace.ChunkSent{Peer: string(left.ID), Message: "churn-dispatch-message"})); after <= firstLeft {
			t.Fatalf("remaining peer received %d later chunk sends, want positive", after-firstLeft)
		}
		if after := n.Trace().Count(nettest.Match(relay, trace.ChunkSent{Peer: string(right.ID), Message: "churn-dispatch-message"})); after != firstRight {
			t.Fatalf("disconnected peer received %d later chunk sends", after-firstRight)
		}
	})
}

func TestChainRelay(t *testing.T) {
	nettest.Run(t, func(t *testing.T, n *nettest.Net) {
		a, b, c := n.Node("a"), n.Node("b"), n.Node("c")
		ca, cb, cc := a.Channel("chain-channel"), b.Channel("chain-channel"), c.Channel("chain-channel")
		n.Connect(a, b)
		n.Connect(b, c)
		n.AwaitPeers(t, a, "chain-channel", b)
		n.AwaitPeers(t, b, "chain-channel", a, c)
		n.AwaitPeers(t, c, "chain-channel", b)
		p := payload(4096)
		ca.Publish(t, "chain-msg", p)
		n.AwaitDecoded(t, b, "chain-channel", "chain-msg")
		n.AwaitDecoded(t, c, "chain-channel", "chain-msg")
		cb.AwaitReceived(t, "chain-msg", p)
		cc.AwaitReceived(t, "chain-msg", p)
	})
}

func TestStarRelay(t *testing.T) {
	nettest.Run(t, func(t *testing.T, n *nettest.Net) {
		center := n.Node("center")
		source := center.Channel("star-channel")
		var leaves []*nettest.Node
		var channels []*nettest.Channel
		for i := range 3 {
			leaf := n.Node(fmt.Sprintf("leaf-%d", i))
			leaves = append(leaves, leaf)
			channels = append(channels, leaf.Channel("star-channel"))
		}
		for _, leaf := range leaves {
			n.Connect(center, leaf)
		}
		n.AwaitPeers(t, center, "star-channel", leaves...)
		for _, leaf := range leaves {
			n.AwaitPeers(t, leaf, "star-channel", center)
		}
		p := payload(4096)
		source.Publish(t, "star-msg", p)
		for i, leaf := range leaves {
			n.AwaitDecoded(t, leaf, "star-channel", "star-msg")
			channels[i].AwaitReceived(t, "star-msg", p)
		}
	})
}
