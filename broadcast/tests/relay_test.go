//go:build integration

package tests

import (
	"testing"

	"github.com/ethp2p/ethp2p/broadcast"
)

// TestChainRelay verifies multi-hop relay: A -> B -> C.
// B receives from A, relays chunks to C. C should decode.
func TestChainRelay(t *testing.T) {
	for _, ss := range strategies {
		t.Run(ss.name, func(t *testing.T) {
			nodes := make([]*testNode, 3)
			nodes[0] = newTestNode(t, "origin")
			nodes[1] = newTestNode(t, "relay")
			nodes[2] = newTestNode(t, "receiver")

			channelID := broadcast.ChannelID("chain-channel")
			handles := make([]channelHandle, 3)
			for i, n := range nodes {
				th := ss.createChannel(t, n.engine, channelID)
				defer th.stop()
				handles[i] = th
			}

			// Chain: 0 -> 1 -> 2
			connectNodes(t, nodes, chainEdges(3))
			waitForPeers(t, nodes[0].obs, channelID, 1, defaultTimeout)
			waitForPeers(t, nodes[1].obs, channelID, 2, defaultTimeout)
			waitForPeers(t, nodes[2].obs, channelID, 1, defaultTimeout)

			payload := testPayload(4096)
			msgID := broadcast.MessageID("chain-msg")

			if err := handles[0].publish(msgID, payload); err != nil {
				t.Fatalf("publish: %v", err)
			}

			// Relay node should decode.
			nodes[1].obs.waitDecoded(t, channelID, msgID, defaultTimeout)

			// End node should decode via relay.
			nodes[2].obs.waitDecoded(t, channelID, msgID, defaultTimeout)

			// Verify payload on subscription channel at end node.
			select {
			case msg := <-handles[2].msgCh:
				if len(msg.Data) != len(payload) {
					t.Errorf("payload length: got %d, want %d", len(msg.Data), len(payload))
				}
			default:
				t.Fatal("expected message on receiver subscription")
			}
		})
	}
}

// TestStarRelay verifies relay in a star topology: center node
// publishes, all leaf nodes receive.
func TestStarRelay(t *testing.T) {
	for _, ss := range strategies {
		t.Run(ss.name, func(t *testing.T) {
			const numLeaves = 3
			channelID := broadcast.ChannelID("star-channel")

			nodes := make([]*testNode, numLeaves+1)
			nodes[0] = newTestNode(t, "center")
			for i := 1; i <= numLeaves; i++ {
				nodes[i] = newTestNode(t, broadcast.PeerID("leaf-"+string(rune('0'+i))))
			}

			handles := make([]channelHandle, len(nodes))
			for i, n := range nodes {
				th := ss.createChannel(t, n.engine, channelID)
				defer th.stop()
				handles[i] = th
			}

			connectNodes(t, nodes, starEdges(len(nodes)))
			for i := 1; i <= numLeaves; i++ {
				waitForPeers(t, nodes[i].obs, channelID, 1, defaultTimeout)
			}

			payload := testPayload(4096)
			msgID := broadcast.MessageID("star-msg")

			if err := handles[0].publish(msgID, payload); err != nil {
				t.Fatalf("publish: %v", err)
			}

			for i := 1; i <= numLeaves; i++ {
				nodes[i].obs.waitDecoded(t, channelID, msgID, defaultTimeout)
			}
		})
	}
}
