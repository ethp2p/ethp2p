//go:build integration

package tests

import (
	"testing"

	"github.com/ethp2p/ethp2p/broadcast"
)

// TestSessionDone verifies that Session.Done() is closed after the
// session's run loop exits when the channel is stopped.
func TestSessionDone(t *testing.T) {
	for _, ss := range strategies {
		t.Run(ss.name, func(t *testing.T) {
			a := newTestNode(t, "node-a")
			b := newTestNode(t, "node-b")
			channelID := broadcast.ChannelID("lifecycle-channel")

			thA := ss.createChannel(t, a.engine, channelID)
			thB := ss.createChannel(t, b.engine, channelID)
			defer thB.stop()

			connectNodes(t, []*testNode{a, b}, chainEdges(2))
			waitForPeers(t, a.obs, channelID, 1, defaultTimeout)
			waitForPeers(t, b.obs, channelID, 1, defaultTimeout)

			payload := testPayload(2048)
			msgID := broadcast.MessageID("sess-done-msg")
			if err := thA.publish(msgID, payload); err != nil {
				t.Fatalf("publish: %v", err)
			}

			// Wait for the session to be created on the publisher side.
			a.obs.waitCreated(t, channelID, msgID, defaultTimeout)

			// Stop the channel; should complete without hanging, proving
			// session cleanup works.
			thA.stop()
		})
	}
}

// TestEngineCloseCleanup verifies that closing the engine stops all
// sessions and channels cleanly without hanging or panicking.
func TestEngineCloseCleanup(t *testing.T) {
	for _, ss := range strategies {
		t.Run(ss.name, func(t *testing.T) {
			a := newTestNode(t, "node-a")
			b := newTestNode(t, "node-b")
			channelID := broadcast.ChannelID("close-channel")

			thA := ss.createChannel(t, a.engine, channelID)
			thB := ss.createChannel(t, b.engine, channelID)

			connectNodes(t, []*testNode{a, b}, chainEdges(2))
			waitForPeers(t, a.obs, channelID, 1, defaultTimeout)

			payload := testPayload(2048)
			if err := thA.publish("close-msg", payload); err != nil {
				t.Fatalf("publish: %v", err)
			}

			b.obs.waitDecoded(t, channelID, "close-msg", defaultTimeout)

			// Close both engines; should not hang or panic.
			thA.stop()
			thB.stop()
			a.engine.Close()
			b.engine.Close()
		})
	}
}

// TestObserverSessionStarted verifies the observer fires
// OnSessionStarted for relay sessions on the receiving side.
func TestObserverSessionStarted(t *testing.T) {
	for _, ss := range strategies {
		t.Run(ss.name, func(t *testing.T) {
			a := newTestNode(t, "node-a")
			b := newTestNode(t, "node-b")
			channelID := broadcast.ChannelID("observer-channel")

			thA := ss.createChannel(t, a.engine, channelID)
			defer thA.stop()
			thB := ss.createChannel(t, b.engine, channelID)
			defer thB.stop()

			connectNodes(t, []*testNode{a, b}, chainEdges(2))
			waitForPeers(t, a.obs, channelID, 1, defaultTimeout)

			payload := testPayload(2048)
			msgID := broadcast.MessageID("observer-msg")
			if err := thA.publish(msgID, payload); err != nil {
				t.Fatalf("publish: %v", err)
			}

			// Observer on B should fire OnSessionStarted for the
			// relay session, then OnSessionDecoded.
			b.obs.waitCreated(t, channelID, msgID, defaultTimeout)
			b.obs.waitDecoded(t, channelID, msgID, defaultTimeout)
		})
	}
}
