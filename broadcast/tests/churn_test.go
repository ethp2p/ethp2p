//go:build integration

package tests

import (
	"testing"

	"github.com/ethp2p/ethp2p/broadcast"
)

// TestPeerDisconnectMidSession verifies that the system handles a peer
// disconnecting while sessions are active. The remaining peers should
// still receive the message.
func TestPeerDisconnectMidSession(t *testing.T) {
	for _, ss := range strategies {
		t.Run(ss.name, func(t *testing.T) {
			a := newTestNode(t, "node-a")
			b := newTestNode(t, "node-b")
			c := newTestNode(t, "node-c")
			channelID := broadcast.ChannelID("churn-channel")

			thA := ss.createChannel(t, a.engine, channelID)
			defer thA.stop()
			thB := ss.createChannel(t, b.engine, channelID)
			defer thB.stop()
			thC := ss.createChannel(t, c.engine, channelID)
			defer thC.stop()

			nodes := []*testNode{a, b, c}
			// Star: A -> B, A -> C
			connectNodes(t, nodes, starEdges(3))
			waitForPeers(t, a.obs, channelID, 2, defaultTimeout)
			waitForPeers(t, b.obs, channelID, 1, defaultTimeout)
			waitForPeers(t, c.obs, channelID, 1, defaultTimeout)

			// Disconnect B by closing its engine. The deferred thB.stop()
			// handles channel cleanup after context cancellation.
			b.engine.Close()

			// A publishes; C should still receive.
			payload := testPayload(4096)
			msgID := broadcast.MessageID("churn-msg")
			if err := thA.publish(msgID, payload); err != nil {
				t.Fatalf("publish: %v", err)
			}

			c.obs.waitDecoded(t, channelID, msgID, defaultTimeout)
		})
	}
}

// TestLateJoinPeer verifies that a peer joining after a message was
// published can receive subsequent messages.
func TestLateJoinPeer(t *testing.T) {
	for _, ss := range strategies {
		t.Run(ss.name, func(t *testing.T) {
			a := newTestNode(t, "node-a")
			b := newTestNode(t, "node-b")
			channelID := broadcast.ChannelID("late-channel")

			thA := ss.createChannel(t, a.engine, channelID)
			defer thA.stop()
			thB := ss.createChannel(t, b.engine, channelID)
			defer thB.stop()

			connectNodes(t, []*testNode{a, b}, chainEdges(2))
			waitForPeers(t, a.obs, channelID, 1, defaultTimeout)

			// A publishes first message.
			payload := testPayload(2048)
			if err := thA.publish("msg-1", payload); err != nil {
				t.Fatalf("publish msg-1: %v", err)
			}
			b.obs.waitDecoded(t, channelID, "msg-1", defaultTimeout)

			// Late joiner C connects to A.
			c := newTestNode(t, "node-c")
			thC := ss.createChannel(t, c.engine, channelID)
			defer thC.stop()

			connectNodes(t, []*testNode{a, c}, chainEdges(2))
			waitForPeers(t, c.obs, channelID, 1, defaultTimeout)

			// A publishes second message; C should decode it.
			if err := thA.publish("msg-2", payload); err != nil {
				t.Fatalf("publish msg-2: %v", err)
			}
			c.obs.waitDecoded(t, channelID, "msg-2", defaultTimeout)
			b.obs.waitDecoded(t, channelID, "msg-2", defaultTimeout)
		})
	}
}
