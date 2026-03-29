//go:build integration

package tests

import (
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/broadcast"
)

// TestSubscribeBeforeConnect verifies the handshake path: channels exist
// before peers connect, so channel lists are exchanged during handshake.
func TestSubscribeBeforeConnect(t *testing.T) {
	for _, ss := range strategies {
		t.Run(ss.name, func(t *testing.T) {
			a := newTestNode(t, "node-a")
			b := newTestNode(t, "node-b")
			channelID := broadcast.ChannelID("pre-connect-channel")

			thA := ss.createChannel(t, a.engine, channelID)
			defer thA.stop()
			thB := ss.createChannel(t, b.engine, channelID)
			defer thB.stop()

			// Channels exist before connect; handshake exchanges them.
			connectNodes(t, []*testNode{a, b}, chainEdges(2))
			waitForPeers(t, a.obs, channelID, 1, defaultTimeout)
			waitForPeers(t, b.obs, channelID, 1, defaultTimeout)

			payload := testPayload(2048)
			msgID := broadcast.MessageID("handshake-msg")
			if err := thA.publish(msgID, payload); err != nil {
				t.Fatalf("publish: %v", err)
			}

			b.obs.waitDecoded(t, channelID, msgID, defaultTimeout)
		})
	}
}

// TestSubscribeAfterConnect verifies the bctrl path: peers connect
// first (empty channel lists), then channels are created, triggering bctrl
// Subscribe messages.
func TestSubscribeAfterConnect(t *testing.T) {
	for _, ss := range strategies {
		t.Run(ss.name, func(t *testing.T) {
			a := newTestNode(t, "node-a")
			b := newTestNode(t, "node-b")
			channelID := broadcast.ChannelID("post-connect-channel")

			// Connect first, no channels yet.
			connectNodes(t, []*testNode{a, b}, chainEdges(2))
			time.Sleep(handshakeSettleTime)

			// Create channels after connecting; triggers bctrl Subscribe.
			thA := ss.createChannel(t, a.engine, channelID)
			defer thA.stop()
			thB := ss.createChannel(t, b.engine, channelID)
			defer thB.stop()

			waitForPeers(t, a.obs, channelID, 1, defaultTimeout)
			waitForPeers(t, b.obs, channelID, 1, defaultTimeout)

			payload := testPayload(2048)
			msgID := broadcast.MessageID("bctrl-msg")
			if err := thA.publish(msgID, payload); err != nil {
				t.Fatalf("publish: %v", err)
			}

			b.obs.waitDecoded(t, channelID, msgID, defaultTimeout)
		})
	}
}

// TestSubscribeBothPaths verifies both paths in a single test: one
// channel exists before connect (handshake), another is created after
// connect (bctrl).
func TestSubscribeBothPaths(t *testing.T) {
	for _, ss := range strategies {
		t.Run(ss.name, func(t *testing.T) {
			a := newTestNode(t, "node-a")
			b := newTestNode(t, "node-b")

			preChannelID := broadcast.ChannelID("pre-channel")
			postChannelID := broadcast.ChannelID("post-channel")

			// Pre-connect channel.
			preA := ss.createChannel(t, a.engine, preChannelID)
			defer preA.stop()
			preB := ss.createChannel(t, b.engine, preChannelID)
			defer preB.stop()

			connectNodes(t, []*testNode{a, b}, chainEdges(2))
			waitForPeers(t, a.obs, preChannelID, 1, defaultTimeout)
			waitForPeers(t, b.obs, preChannelID, 1, defaultTimeout)

			// Post-connect channel.
			postA := ss.createChannel(t, a.engine, postChannelID)
			defer postA.stop()
			postB := ss.createChannel(t, b.engine, postChannelID)
			defer postB.stop()

			waitForPeers(t, a.obs, postChannelID, 1, defaultTimeout)
			waitForPeers(t, b.obs, postChannelID, 1, defaultTimeout)

			payload := testPayload(2048)

			// Publish on pre-connect channel.
			preMsg := broadcast.MessageID("pre-msg")
			if err := preA.publish(preMsg, payload); err != nil {
				t.Fatalf("publish pre: %v", err)
			}
			b.obs.waitDecoded(t, preChannelID, preMsg, defaultTimeout)

			// Publish on post-connect channel.
			postMsg := broadcast.MessageID("post-msg")
			if err := postA.publish(postMsg, payload); err != nil {
				t.Fatalf("publish post: %v", err)
			}
			b.obs.waitDecoded(t, postChannelID, postMsg, defaultTimeout)
		})
	}
}
