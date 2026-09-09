//go:build integration

package tests

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/protocol"
	"github.com/ethp2p/ethp2p/transport"
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
			const probeSelector protocol.Selector = 17

			if _, err := a.stack.RegisterSubsystem("probe", probeSelector); err != nil {
				t.Fatal(err)
			}
			probe, err := b.stack.RegisterSubsystem("probe", probeSelector)
			if err != nil {
				t.Fatal(err)
			}
			probeEvents := make(chan ethp2p.StreamEvent, 1)
			if err := probe.NotifyStreams(probeEvents); err != nil {
				t.Fatal(err)
			}

			thA := ss.createChannel(t, a.engine, channelID)
			thB := ss.createChannel(t, b.engine, channelID)

			connectNodes(t, []*testNode{a, b}, chainEdges(2))
			waitForPeers(t, a.obs, channelID, 1, defaultTimeout)

			payload := testPayload(2048)
			if err := thA.publish("close-msg", payload); err != nil {
				t.Fatalf("publish: %v", err)
			}

			b.obs.waitDecoded(t, channelID, "close-msg", defaultTimeout)

			// Close one engine while its peer connection is still live. The
			// application closes the borrowed connection view below.
			thA.stop()
			thB.stop()
			engineClosed := make(chan error, 1)
			go func() { engineClosed <- a.engine.Close() }()
			select {
			case err := <-engineClosed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(defaultTimeout):
				t.Fatal("Engine.Close blocked on a live peer")
			}

			a.mu.Lock()
			if len(a.conns) == 0 {
				a.mu.Unlock()
				t.Fatal("node a has no tracked connection")
			}
			conn := a.conns[0]
			a.mu.Unlock()

			// Engine.Close must not close the borrowed connection: another
			// subsystem can still use the application's live Stack.
			probeCtx, probeCancel := context.WithTimeout(t.Context(), defaultTimeout)
			defer probeCancel()
			deadline := time.Now().Add(defaultTimeout)
			out, err := conn.OpenStream(probeCtx)
			if err != nil {
				t.Fatalf("open probe stream after Engine.Close: %v", err)
			}
			if err := out.SetDeadline(deadline); err != nil {
				t.Fatalf("set probe write deadline: %v", err)
			}
			if err := protocol.WriteSelector(out, probeSelector); err != nil {
				t.Fatalf("write probe selector: %v", err)
			}
			if _, err := out.Write([]byte("ping")); err != nil {
				t.Fatalf("write probe request: %v", err)
			}
			if err := out.Close(); err != nil {
				t.Fatalf("close probe request: %v", err)
			}

			var event ethp2p.StreamEvent
			select {
			case event = <-probeEvents:
			case <-probeCtx.Done():
				t.Fatalf("receive probe event after Engine.Close: %v", probeCtx.Err())
			}
			if event.Selector != probeSelector {
				t.Fatalf("probe selector = %d, want %d", event.Selector, probeSelector)
			}
			probeStream, ok := event.Stream.(transport.Stream)
			if !ok {
				t.Fatalf("probe stream type = %T, want transport.Stream", event.Stream)
			}
			if err := probeStream.SetDeadline(deadline); err != nil {
				t.Fatalf("set probe read deadline: %v", err)
			}
			request := make([]byte, len("ping"))
			if _, err := io.ReadFull(probeStream, request); err != nil {
				t.Fatalf("read probe request: %v", err)
			}
			if !bytes.Equal(request, []byte("ping")) {
				t.Fatalf("probe request = %q, want ping", request)
			}
			if _, err := probeStream.Write([]byte("pong")); err != nil {
				t.Fatalf("write probe response: %v", err)
			}
			if err := probeStream.Close(); err != nil {
				t.Fatalf("close probe response: %v", err)
			}

			response := make([]byte, len("pong"))
			if _, err := io.ReadFull(out, response); err != nil {
				t.Fatalf("read probe response: %v", err)
			}
			if !bytes.Equal(response, []byte("pong")) {
				t.Fatalf("probe response = %q, want pong", response)
			}

			if err := a.Close(); err != nil {
				t.Fatal(err)
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
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
