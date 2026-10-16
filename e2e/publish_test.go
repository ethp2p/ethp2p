package e2e

import (
	"fmt"
	"testing"

	"github.com/ethp2p/ethp2p/internal/harness"
)

func TestMultiPeerFanOut(t *testing.T) {
	harness.Run(t, func(t *testing.T, n *harness.Net) {
		origin := n.Node("a")
		co := origin.Channel("fanout-channel")
		var leaves []*harness.Node
		var channels []*harness.Channel
		for i := range 3 {
			leaf := n.Node(fmt.Sprintf("leaf-%d", i))
			leaves = append(leaves, leaf)
			channels = append(channels, leaf.Channel("fanout-channel"))
		}
		for _, leaf := range leaves {
			n.Connect(origin, leaf)
		}
		n.AwaitPeers(t, origin, "fanout-channel", leaves...)
		for _, leaf := range leaves {
			n.AwaitPeers(t, leaf, "fanout-channel", origin)
		}
		p := payload(4096)
		co.Publish(t, "fanout-msg", p)
		for i, leaf := range leaves {
			n.AwaitDecoded(t, leaf, "fanout-channel", "fanout-msg")
			channels[i].AwaitReceived(t, "fanout-msg", p)
		}
	})
}

func TestBidirectionalExchange(t *testing.T) {
	harness.Run(t, func(t *testing.T, n *harness.Net) {
		a, b := n.Node("a"), n.Node("b")
		ca, cb := a.Channel("bidi-channel"), b.Channel("bidi-channel")
		n.Connect(a, b)
		n.AwaitPeers(t, a, "bidi-channel", b)
		n.AwaitPeers(t, b, "bidi-channel", a)
		p := payload(2048)
		ca.Publish(t, "msg-a-to-b", p)
		n.AwaitDecoded(t, b, "bidi-channel", "msg-a-to-b")
		cb.AwaitReceived(t, "msg-a-to-b", p)
		cb.Publish(t, "msg-b-to-a", p)
		n.AwaitDecoded(t, a, "bidi-channel", "msg-b-to-a")
		ca.AwaitReceived(t, "msg-b-to-a", p)
	})
}
