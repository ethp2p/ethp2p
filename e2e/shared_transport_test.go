package e2e

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/ethp2p/ethp2p/internal/nettest"
)

func TestSharedTransportBroadcastRoundTrip(t *testing.T) {
	nettest.Run(t, func(t *testing.T, n *nettest.Net) {
		a, b := n.Node("a"), n.Node("b")
		la, lb := a.Libp2pListener(t), b.Libp2pListener(t)
		ca, cb := a.Channel("shared-transport-channel"), b.Channel("shared-transport-channel")
		n.Connect(a, b)
		if got := n.ConnInfo(t, a, b).Peer; got != b.ID {
			t.Fatalf("dialed ID=%s, want %s", got, b.ID)
		}
		if got := n.ConnInfo(t, b, a).Peer; got != a.ID {
			t.Fatalf("accepted ID=%s, want %s", got, a.ID)
		}
		ctx, cancel := context.WithTimeout(t.Context(), nettest.DefaultTimeout)
		defer cancel()
		viewA, err := la.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer viewA.CloseWithError(0, "done")
		viewB, err := lb.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer viewB.CloseWithError(0, "done")
		// A multistream frame routes to the libp2p view while broadcast's
		// ethp2p view remains live on the same physical connection.
		out, err := viewA.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer out.Close()
		// A multistream-select header: varint length 19, then the protocol line.
		// Its first byte is outside the ethp2p selector range and its second is '/',
		// so the endpoint routes the stream to the libp2p view.
		frame := append([]byte{19}, "/multistream/1.0.0\n"...)
		if _, err := out.Write(frame); err != nil {
			t.Fatal(err)
		}
		in, err := viewB.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer in.Close()
		buf := make([]byte, len(frame))
		if _, err := io.ReadFull(in, buf); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf, frame) {
			t.Fatalf("libp2p frame=%q", buf)
		}
		n.AwaitPeers(t, a, "shared-transport-channel", b)
		n.AwaitPeers(t, b, "shared-transport-channel", a)
		p := payload(4096)
		ca.Publish(t, "shared-transport-message", p)
		n.AwaitDecoded(t, b, "shared-transport-channel", "shared-transport-message")
		cb.AwaitReceived(t, "shared-transport-message", p)
	})
}
