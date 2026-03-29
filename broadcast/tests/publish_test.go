//go:build integration

package tests

import (
	"testing"

	"github.com/ethp2p/ethp2p/broadcast"
)

// TestPairPublishDecode verifies that a message published by node A is
// decoded and delivered to node B's subscription channel.
func TestPairPublishDecode(t *testing.T) {
	for _, ss := range strategies {
		t.Run(ss.name, func(t *testing.T) {
			a := newTestNode(t, "node-a")
			b := newTestNode(t, "node-b")

			channelID := broadcast.ChannelID("test-channel")

			// Create channels before connecting (handshake path).
			thA := ss.createChannel(t, a.engine, channelID)
			defer thA.stop()
			thB := ss.createChannel(t, b.engine, channelID)
			defer thB.stop()

			connectNodes(t, []*testNode{a, b}, chainEdges(2))
			waitForPeers(t, a.obs, channelID, 1, defaultTimeout)
			waitForPeers(t, b.obs, channelID, 1, defaultTimeout)

			payload := testPayload(4096)
			msgID := broadcast.MessageID("msg-1")

			if err := thA.publish(msgID, payload); err != nil {
				t.Fatalf("publish: %v", err)
			}

			b.obs.waitDecoded(t, channelID, msgID, defaultTimeout)

			select {
			case msg := <-thB.msgCh:
				if msg.MessageID != msgID {
					t.Errorf("message ID: got %s, want %s", msg.MessageID, msgID)
				}
				if len(msg.Data) != len(payload) {
					t.Errorf("payload length: got %d, want %d", len(msg.Data), len(payload))
				}
			default:
				t.Fatal("expected message on subscription channel after decode")
			}
		})
	}
}

// TestMultiPeerFanOut verifies that a published message reaches all
// connected peers in a star topology.
func TestMultiPeerFanOut(t *testing.T) {
	for _, ss := range strategies {
		t.Run(ss.name, func(t *testing.T) {
			const numReceivers = 3
			channelID := broadcast.ChannelID("fanout-channel")

			nodes := make([]*testNode, numReceivers+1)
			nodes[0] = newTestNode(t, "publisher")
			for i := 1; i <= numReceivers; i++ {
				nodes[i] = newTestNode(t, broadcast.PeerID("receiver-"+string(rune('0'+i))))
			}

			handles := make([]channelHandle, len(nodes))
			for i, n := range nodes {
				th := ss.createChannel(t, n.engine, channelID)
				defer th.stop()
				handles[i] = th
			}

			connectNodes(t, nodes, starEdges(len(nodes)))
			for i := 1; i <= numReceivers; i++ {
				waitForPeers(t, nodes[i].obs, channelID, 1, defaultTimeout)
			}

			payload := testPayload(4096)
			msgID := broadcast.MessageID("fanout-msg")

			if err := handles[0].publish(msgID, payload); err != nil {
				t.Fatalf("publish: %v", err)
			}

			for i := 1; i <= numReceivers; i++ {
				nodes[i].obs.waitDecoded(t, channelID, msgID, defaultTimeout)
			}
		})
	}
}

// TestBidirectionalExchange verifies that both peers can publish and
// receive from each other.
func TestBidirectionalExchange(t *testing.T) {
	for _, ss := range strategies {
		t.Run(ss.name, func(t *testing.T) {
			a := newTestNode(t, "node-a")
			b := newTestNode(t, "node-b")
			channelID := broadcast.ChannelID("bidi-channel")

			thA := ss.createChannel(t, a.engine, channelID)
			defer thA.stop()
			thB := ss.createChannel(t, b.engine, channelID)
			defer thB.stop()

			connectNodes(t, []*testNode{a, b}, chainEdges(2))
			waitForPeers(t, a.obs, channelID, 1, defaultTimeout)
			waitForPeers(t, b.obs, channelID, 1, defaultTimeout)

			payload := testPayload(2048)

			// A -> B
			msgAB := broadcast.MessageID("msg-a-to-b")
			if err := thA.publish(msgAB, payload); err != nil {
				t.Fatalf("publish A->B: %v", err)
			}
			b.obs.waitDecoded(t, channelID, msgAB, defaultTimeout)

			// B -> A
			msgBA := broadcast.MessageID("msg-b-to-a")
			if err := thB.publish(msgBA, payload); err != nil {
				t.Fatalf("publish B->A: %v", err)
			}
			a.obs.waitDecoded(t, channelID, msgBA, defaultTimeout)
		})
	}
}
