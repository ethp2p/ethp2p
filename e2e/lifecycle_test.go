package e2e

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/internal/nettest"
	"github.com/ethp2p/ethp2p/internal/trace"
	"github.com/ethp2p/ethp2p/wire"
)

func TestSessionDone(t *testing.T) {
	nettest.Run(t, func(t *testing.T, n *nettest.Net) {
		a, b := n.Node("a"), n.Node("b")
		ca, cb := a.Channel("lifecycle-channel"), b.Channel("lifecycle-channel")
		n.Connect(a, b)
		n.AwaitPeers(t, a, "lifecycle-channel", b)
		n.AwaitPeers(t, b, "lifecycle-channel", a)
		ca.Publish(t, "sess-done-msg", payload(2048))
		n.Await(t, nettest.Match(a, trace.SessionStarted{Channel: "lifecycle-channel", Message: "sess-done-msg"}), nettest.DefaultTimeout)
		ca.Close()
		n.Await(t, nettest.Match(a, trace.ChannelDropped{Channel: "lifecycle-channel"}), nettest.DefaultTimeout)
		cb.Close()
	})
}

func TestEngineCloseCleanup(t *testing.T) {
	nettest.Run(t, func(t *testing.T, n *nettest.Net) {
		const probeSelector wire.Selector = 17
		var probeA, probeB *ethp2p.Subsystem
		wakeA, wakeB := make(chan struct{}, 1), make(chan struct{}, 1)
		a := n.Node("a", nettest.BeforeStart(func(s *ethp2p.Stack) {
			var err error
			probeA, err = s.Register("probe", []wire.Selector{probeSelector}, ethp2p.SubsystemConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if err := probeA.Notify(wakeA); err != nil {
				t.Fatal(err)
			}
		}))
		b := n.Node("b", nettest.BeforeStart(func(s *ethp2p.Stack) {
			var err error
			probeB, err = s.Register("probe", []wire.Selector{probeSelector}, ethp2p.SubsystemConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if err := probeB.Notify(wakeB); err != nil {
				t.Fatal(err)
			}
		}))
		ca, cb := a.Channel("close-channel"), b.Channel("close-channel")
		n.Connect(a, b)
		n.AwaitPeers(t, a, "close-channel", b)
		ca.Publish(t, "close-msg", payload(2048))
		n.AwaitDecoded(t, b, "close-channel", "close-msg")
		ca.Close()
		cb.Close()
		ctx, cancel := context.WithTimeout(t.Context(), nettest.DefaultTimeout)
		defer cancel()
		if err := a.Engine.Close(); err != nil {
			t.Fatal(err)
		}
		// The engine borrows its connection. The probe subsystem still routes
		// a bidirectional stream through the same Stack after engine shutdown.
		var peer *ethp2p.Peer
		for peer == nil {
			if ev, ok := probeA.Next(); ok {
				if ev.Kind != ethp2p.PeerUp {
					t.Fatalf("probe event=%v, want PeerUp", ev.Kind)
				}
				peer = ev.Peer
				break
			}
			select {
			case <-wakeA:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
		out, err := peer.OpenStream(ctx, probeSelector)
		if err != nil {
			t.Fatal(err)
		}
		defer out.Close()
		deadline := time.Now().Add(nettest.DefaultTimeout)
		if err := out.SetDeadline(deadline); err != nil {
			t.Fatal(err)
		}
		if _, err := out.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		if err := out.Close(); err != nil {
			t.Fatal(err)
		}
		var ev ethp2p.Event
		// PeerUp precedes StreamIn; drain each wake fully before waiting again.
		for ev.Kind != ethp2p.StreamIn {
			if next, ok := probeB.Next(); ok {
				ev = next
				continue
			}
			select {
			case <-wakeB:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
		if ev.Selector != probeSelector {
			t.Fatalf("selector=%d", ev.Selector)
		}
		stream, ok := ev.Stream.(ethp2p.Stream)
		if !ok {
			t.Fatalf("stream type %T", ev.Stream)
		}
		defer stream.Close()
		if err := stream.SetDeadline(deadline); err != nil {
			t.Fatal(err)
		}
		request := make([]byte, 4)
		if _, err := io.ReadFull(stream, request); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(request, []byte("ping")) {
			t.Fatalf("request=%q", request)
		}
		if _, err := stream.Write([]byte("pong")); err != nil {
			t.Fatal(err)
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		response := make([]byte, 4)
		if _, err := io.ReadFull(out, response); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(response, []byte("pong")) {
			t.Fatalf("response=%q", response)
		}
	})
}

func TestObserverSessionStarted(t *testing.T) {
	nettest.Run(t, func(t *testing.T, n *nettest.Net) {
		a, b := n.Node("a"), n.Node("b")
		ca, cb := a.Channel("observer-channel"), b.Channel("observer-channel")
		n.Connect(a, b)
		n.AwaitPeers(t, a, "observer-channel", b)
		p := payload(2048)
		ca.Publish(t, "observer-msg", p)
		n.AwaitDecoded(t, b, "observer-channel", "observer-msg")
		cb.AwaitReceived(t, "observer-msg", p)
		n.Trace().Require(t, nettest.Match(b, trace.SessionStarted{Channel: "observer-channel", Message: "observer-msg", Role: "relay"}), nettest.Match(b, trace.SessionDecoded{Channel: "observer-channel", Message: "observer-msg"}))
	})
}
