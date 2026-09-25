package e2e

import (
	"context"
	"testing"

	"github.com/ethp2p/ethp2p/internal/nettest"
	"github.com/ethp2p/ethp2p/internal/trace"
)

func TestPeerDisconnectMidSession(t *testing.T) {
	nettest.Run(t, func(t *testing.T, n *nettest.Net) {
		a, b, c := n.Node("a"), n.Node("b"), n.Node("c")
		ca, cb, cc := a.Channel("churn-channel"), b.Channel("churn-channel"), c.Channel("churn-channel")
		n.Connect(a, b)
		n.Connect(a, c)
		n.AwaitPeers(t, a, "churn-channel", b, c)
		n.AwaitPeers(t, b, "churn-channel", a)
		n.AwaitPeers(t, c, "churn-channel", a)
		cb.Close()
		b.Close()
		n.Await(t, nettest.Match(a, trace.PeerGone{Peer: string(b.ID)}), nettest.DefaultTimeout)
		p := payload(4096)
		ca.Publish(t, "churn-msg", p)
		n.AwaitDecoded(t, c, "churn-channel", "churn-msg")
		cc.AwaitReceived(t, "churn-msg", p)
	})
}

func TestDisconnectReconnectPublish(t *testing.T) {
	nettest.Run(t, func(t *testing.T, n *nettest.Net) {
		a, b := n.Node("a"), n.Node("b")
		la, lb := a.Libp2pListener(t), b.Libp2pListener(t)
		ca, cb := a.Channel("reconnect-channel"), b.Channel("reconnect-channel")
		n.Connect(a, b)
		n.AwaitPeers(t, a, "reconnect-channel", b)
		n.AwaitPeers(t, b, "reconnect-channel", a)
		first := n.ConnInfo(t, a, b)
		ctx, cancel := context.WithTimeout(t.Context(), nettest.DefaultTimeout)
		defer cancel()
		viewA, err := la.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		viewB, err := lb.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		n.Disconnect(a, b)
		n.Within(t, nettest.DefaultTimeout, func(ctx context.Context) error {
			select {
			case <-viewA.Context().Done():
			case <-ctx.Done():
				return ctx.Err()
			}
			select {
			case <-viewB.Context().Done():
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		n.AwaitPeers(t, a, "reconnect-channel")
		n.AwaitPeers(t, b, "reconnect-channel")
		n.Connect(a, b)
		if current := n.ConnInfo(t, a, b); current.ID == first.ID || current.Peer != b.ID {
			t.Fatal("ConnInfo returned a stale entry after reconnect")
		}
		n.AwaitPeers(t, a, "reconnect-channel", b)
		n.AwaitPeers(t, b, "reconnect-channel", a)
		data := payload(2048)
		ca.Publish(t, "reconnected-message", data)
		n.AwaitDecoded(t, b, "reconnect-channel", "reconnected-message")
		cb.AwaitReceived(t, "reconnected-message", data)
	})
}

func TestLateJoinPeer(t *testing.T) {
	nettest.Run(t, func(t *testing.T, n *nettest.Net) {
		a, b := n.Node("a"), n.Node("b")
		ca, cb := a.Channel("late-channel"), b.Channel("late-channel")
		n.Connect(a, b)
		n.AwaitPeers(t, a, "late-channel", b)
		p := payload(2048)
		ca.Publish(t, "msg-1", p)
		n.AwaitDecoded(t, b, "late-channel", "msg-1")
		cb.AwaitReceived(t, "msg-1", p)
		c := n.Node("c")
		cc := c.Channel("late-channel")
		n.Connect(a, c)
		n.AwaitPeers(t, c, "late-channel", a)
		cc.RequireNotReceived(t, "msg-1")
		ca.Publish(t, "msg-2", p)
		n.AwaitDecoded(t, c, "late-channel", "msg-2")
		n.AwaitDecoded(t, b, "late-channel", "msg-2")
		cc.AwaitReceived(t, "msg-2", p)
		cb.AwaitReceived(t, "msg-2", p)
	})
}
