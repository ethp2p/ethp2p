package e2e

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/internal/harness"
	"github.com/ethp2p/ethp2p/internal/trace"
)

func TestSubscribeBeforeConnect(t *testing.T) {
	harness.Run(t, func(t *testing.T, n *harness.Net) {
		a, b := n.Node("a"), n.Node("b")
		ca, cb := a.Channel("pre-connect-channel"), b.Channel("pre-connect-channel")
		n.Connect(a, b)
		n.AwaitPeers(t, a, "pre-connect-channel", b)
		n.AwaitPeers(t, b, "pre-connect-channel", a)
		n.Trace().Require(t, harness.Match(a, trace.PeerHandshook{Peer: string(b.ID)}), harness.Match(a, trace.PeerSubscribed{Channel: "pre-connect-channel"}))
		p := payload(2048)
		ca.Publish(t, "handshake-msg", p)
		n.AwaitDecoded(t, b, "pre-connect-channel", "handshake-msg")
		cb.AwaitReceived(t, "handshake-msg", p)
		n.Trace().Require(t, harness.Match(b, trace.SessionStarted{Channel: "pre-connect-channel", Message: "handshake-msg", Role: "relay"}), harness.Match(b, trace.SessionDecoded{Channel: "pre-connect-channel", Message: "handshake-msg"}))
	})
}

// Each Hello here is about 700 KiB, more than QUIC's 512 KiB initial stream
// window, so neither side's write can finish until the other reads. The
// handshake completes only if each side reads while it writes.
func TestHandshakeCompletesWhenBothHellosExceedStreamWindow(t *testing.T) {
	harness.Run(t, func(t *testing.T, n *harness.Net) {
		a, b := n.Node("a"), n.Node("b")
		pad := strings.Repeat("x", 1020)
		var id broadcast.ChannelID
		var ca, cb *harness.Channel
		for i := range 700 {
			id = broadcast.ChannelID(fmt.Sprintf("%04d", i) + pad)
			ca, cb = a.Channel(id), b.Channel(id)
		}
		n.Connect(a, b)
		n.AwaitPeers(t, a, id, b)
		n.AwaitPeers(t, b, id, a)
		p := payload(2048)
		ca.Publish(t, "large-hello-msg", p)
		cb.AwaitReceived(t, "large-hello-msg", p)
	})
}

func TestSubscribeAfterConnect(t *testing.T) {
	harness.Run(t, func(t *testing.T, n *harness.Net) {
		a, b := n.Node("a"), n.Node("b")
		n.Connect(a, b)
		n.Await(t, harness.Where[trace.PeerHandshook](a, func(e trace.PeerHandshook) bool {
			return e.Peer == string(b.ID) && len(e.Channels) == 0
		}), harness.DefaultTimeout)
		n.Await(t, harness.Where[trace.PeerHandshook](b, func(e trace.PeerHandshook) bool {
			return e.Peer == string(a.ID) && len(e.Channels) == 0
		}), harness.DefaultTimeout)
		ca, cb := a.Channel("post-connect-channel"), b.Channel("post-connect-channel")
		n.AwaitPeers(t, a, "post-connect-channel", b)
		n.AwaitPeers(t, b, "post-connect-channel", a)
		p := payload(2048)
		ca.Publish(t, "bctrl-msg", p)
		n.AwaitDecoded(t, b, "post-connect-channel", "bctrl-msg")
		cb.AwaitReceived(t, "bctrl-msg", p)
	})
}
