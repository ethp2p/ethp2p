//go:build integration

package tests

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/broadcast"
)

// TestSharedTransportBroadcastRoundTrip exercises the broadcast stack over
// the production shared transport, including authenticated identities and
// the production unidirectional stream profile.
func TestSharedTransportBroadcastRoundTrip(t *testing.T) {
	for _, ss := range strategies {
		t.Run(ss.name, func(t *testing.T) {
			const timeout = 15 * time.Second

			ctx, cancel := context.WithTimeout(t.Context(), timeout)
			defer cancel()

			a, b := newTestNode(t), newTestNode(t)
			channelID := broadcast.ChannelID("shared-transport-channel")
			thA := ss.createChannel(t, a.engine, channelID)
			thB := ss.createChannel(t, b.engine, channelID)

			connectNodes(t, []*testNode{a, b}, chainEdges(2))
			waitForPeers(t, a.obs, channelID, 1, timeout)
			waitForPeers(t, b.obs, channelID, 1, timeout)
			for _, pair := range [][2]*testNode{{a, b}, {b, a}} {
				infos := pair[0].stack.Connections()
				if len(infos) != 1 || infos[0].Peer != pair[1].endpoint.Eth.PeerID() {
					t.Fatalf("connections = %+v", infos)
				}
			}

			payload := testPayload(4096)
			messageID := broadcast.MessageID("shared-transport-message")
			if err := thA.publish(messageID, payload); err != nil {
				t.Fatal(err)
			}
			b.obs.waitDecoded(t, channelID, messageID, timeout)

			select {
			case msg := <-thB.msgCh:
				if !bytes.Equal(msg.Data, payload) {
					t.Fatal("received payload differs from published payload")
				}
			case <-ctx.Done():
				t.Fatalf("waiting for published payload: %v", ctx.Err())
			}

			thA.stop()
			thB.stop()
		})
	}
}
