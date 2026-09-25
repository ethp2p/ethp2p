package e2e

import (
	"errors"
	"testing"

	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/broadcast/rs"
	"github.com/ethp2p/ethp2p/internal/nettest"
	"github.com/ethp2p/ethp2p/internal/trace"
)

func TestDuplicateChannelReportsError(t *testing.T) {
	nettest.Run(t, func(t *testing.T, n *nettest.Net) {
		a := n.Node("a")
		first := a.Channel("duplicate")
		second := broadcast.AttachChannel(a.Engine, "duplicate", rs.NewScheme(rs.DefaultConfig()))
		n.Await(t, nettest.Match(a, trace.ChannelAttached{Channel: "duplicate", Err: broadcast.ErrChannelExists}), nettest.DefaultTimeout)
		n.Trace().Require(t,
			nettest.Where[trace.ChannelAttached](a, func(event trace.ChannelAttached) bool { return event.Channel == "duplicate" && event.Err == nil }),
			nettest.Match(a, trace.ChannelAttached{Channel: "duplicate", Err: broadcast.ErrChannelExists}),
		)
		if got := n.Trace().Count(nettest.Match(a, trace.ChannelAttached{Channel: "duplicate"})); got != 2 {
			t.Fatalf("channel attachment callbacks = %d, want two", got)
		}
		first.Close()
		second.Stop()
	})
}

func TestDropChannelTwiceIsSafe(t *testing.T) {
	nettest.Run(t, func(t *testing.T, n *nettest.Net) {
		a := n.Node("a")
		channel := a.Channel("drop-twice")
		channel.Close()
		a.Engine.DropChannel("drop-twice")
		n.Await(t, nettest.Match(a, trace.ChannelDropped{Channel: "drop-twice"}), nettest.DefaultTimeout)
	})
}

func TestStoppingChannelClosesSubscription(t *testing.T) {
	nettest.Run(t, func(t *testing.T, n *nettest.Net) {
		a := n.Node("a")
		channel := broadcast.AttachChannel(a.Engine, "stop-subscription", rs.NewScheme(rs.DefaultConfig()))
		deliveries := make(chan broadcast.FullMessage, 1)
		if err := channel.Subscribe(deliveries); err != nil {
			t.Fatal(err)
		}
		channel.Stop()
		if _, open := <-deliveries; open {
			t.Fatal("subscription stayed open after Stop")
		}
	})
}

func TestChannelAllowsOnlyOneSubscriber(t *testing.T) {
	nettest.Run(t, func(t *testing.T, n *nettest.Net) {
		a := n.Node("a")
		channel := broadcast.AttachChannel(a.Engine, "single-subscription", rs.NewScheme(rs.DefaultConfig()))
		defer channel.Stop()
		if err := channel.Subscribe(make(chan broadcast.FullMessage, 1)); err != nil {
			t.Fatal(err)
		}
		if err := channel.Subscribe(make(chan broadcast.FullMessage, 1)); !errors.Is(err, broadcast.ErrAlreadySubscribed) {
			t.Fatalf("second Subscribe = %v, want ErrAlreadySubscribed", err)
		}
	})
}

func TestSubscribeBeforeConnect(t *testing.T) {
	nettest.Run(t, func(t *testing.T, n *nettest.Net) {
		a, b := n.Node("a"), n.Node("b")
		ca, cb := a.Channel("pre-connect-channel"), b.Channel("pre-connect-channel")
		n.Connect(a, b)
		n.AwaitPeers(t, a, "pre-connect-channel", b)
		n.AwaitPeers(t, b, "pre-connect-channel", a)
		n.Trace().Require(t, nettest.Match(a, trace.PeerHandshook{Peer: string(b.ID)}), nettest.Match(a, trace.PeerSubscribed{Channel: "pre-connect-channel"}))
		p := payload(2048)
		ca.Publish(t, "handshake-msg", p)
		n.AwaitDecoded(t, b, "pre-connect-channel", "handshake-msg")
		cb.AwaitReceived(t, "handshake-msg", p)
	})
}

func TestSubscribeAfterConnect(t *testing.T) {
	nettest.Run(t, func(t *testing.T, n *nettest.Net) {
		a, b := n.Node("a"), n.Node("b")
		n.Connect(a, b)
		n.Await(t, nettest.Where[trace.PeerHandshook](a, func(e trace.PeerHandshook) bool {
			return e.Peer == string(b.ID) && len(e.Channels) == 0
		}), nettest.DefaultTimeout)
		n.Await(t, nettest.Where[trace.PeerHandshook](b, func(e trace.PeerHandshook) bool {
			return e.Peer == string(a.ID) && len(e.Channels) == 0
		}), nettest.DefaultTimeout)
		ca, cb := a.Channel("post-connect-channel"), b.Channel("post-connect-channel")
		n.AwaitPeers(t, a, "post-connect-channel", b)
		n.AwaitPeers(t, b, "post-connect-channel", a)
		p := payload(2048)
		ca.Publish(t, "bctrl-msg", p)
		n.AwaitDecoded(t, b, "post-connect-channel", "bctrl-msg")
		cb.AwaitReceived(t, "bctrl-msg", p)
	})
}

func TestSubscribeBothPaths(t *testing.T) {
	nettest.Run(t, func(t *testing.T, n *nettest.Net) {
		a, b := n.Node("a"), n.Node("b")
		preA, preB := a.Channel("pre-channel"), b.Channel("pre-channel")
		n.Connect(a, b)
		n.AwaitPeers(t, a, "pre-channel", b)
		n.AwaitPeers(t, b, "pre-channel", a)
		postA, postB := a.Channel("post-channel"), b.Channel("post-channel")
		n.AwaitPeers(t, a, "post-channel", b)
		n.AwaitPeers(t, b, "post-channel", a)
		p := payload(2048)
		preA.Publish(t, "pre-msg", p)
		n.AwaitDecoded(t, b, "pre-channel", "pre-msg")
		preB.AwaitReceived(t, "pre-msg", p)
		postA.Publish(t, "post-msg", p)
		n.AwaitDecoded(t, b, "post-channel", "post-msg")
		postB.AwaitReceived(t, "post-msg", p)
	})
}
