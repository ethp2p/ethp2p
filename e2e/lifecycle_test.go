package e2e

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/internal/harness"
	"github.com/ethp2p/ethp2p/wire"
)

func TestEngineCloseCleanup(t *testing.T) {
	harness.Run(t, func(t *testing.T, n *harness.Net) {
		const (
			probeFamily   wire.Family   = 17
			probeProtocol wire.Protocol = 1
		)
		var probeA, probeB *ethp2p.Family
		wakeA, wakeB := make(chan struct{}, 1), make(chan struct{}, 1)
		a := n.Node("a", harness.BeforeStart(func(s *ethp2p.Stack) {
			var err error
			probeA, err = s.Register(probeFamily, []ethp2p.ProtocolSpec{{Protocol: probeProtocol, MaxQueued: 16}}, wakeA)
			if err != nil {
				t.Fatal(err)
			}
		}))
		b := n.Node("b", harness.BeforeStart(func(s *ethp2p.Stack) {
			var err error
			probeB, err = s.Register(probeFamily, []ethp2p.ProtocolSpec{{Protocol: probeProtocol, MaxQueued: 16}}, wakeB)
			if err != nil {
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
		ctx, cancel := context.WithTimeout(t.Context(), harness.DefaultTimeout)
		defer cancel()
		if err := a.Engine.Close(); err != nil {
			t.Fatal(err)
		}
		// The engine borrows its connection. The probe family still routes
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
		out, err := peer.OpenStream(ctx, probeProtocol)
		if err != nil {
			t.Fatal(err)
		}
		defer out.Close()
		deadline := time.Now().Add(harness.DefaultTimeout)
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
		if ev.Protocol != probeProtocol {
			t.Fatalf("protocol=%d", ev.Protocol)
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
