//go:build integration

package tests

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
)

// TestSharedTransportBroadcastRoundTrip exercises the broadcast stack over
// the production shared transport. The regular integration helpers use a raw
// QUIC host, so this test specifically covers authenticated Ethp2pTransport
// connections and the production unidirectional stream profile.
func TestSharedTransportBroadcastRoundTrip(t *testing.T) {
	for _, ss := range strategies {
		t.Run(ss.name, func(t *testing.T) {
			const timeout = 15 * time.Second

			ctx, cancel := context.WithTimeout(t.Context(), timeout)
			defer cancel()

			type sharedNode struct {
				endpoint *transporttest.Endpoint
				stack    *ethp2p.Stack
				engine   *broadcast.Engine
				obs      *testObserver
				peers    chan *ethp2p.Peer
				streams  chan ethp2p.StreamEvent
				conn     transport.Conn
				wg       sync.WaitGroup
			}

			newNode := func(receivePeers bool) *sharedNode {
				endpoint := transporttest.NewEndpoint(t)

				stack := new(ethp2p.Stack)
				subsystem, err := stack.RegisterSubsystem(
					"broadcast",
					broadcast.BCAST,
					broadcast.SESS,
					broadcast.CHUNK,
				)
				if err != nil {
					t.Fatal(err)
				}
				peers := make(chan *ethp2p.Peer, 64)
				streams := make(chan ethp2p.StreamEvent, 1024)
				if err := subsystem.NotifyPeers(peers); err != nil {
					t.Fatal(err)
				}
				if err := subsystem.NotifyStreams(streams); err != nil {
					t.Fatal(err)
				}

				obs := newTestObserver()
				engine := broadcast.NewEngine(broadcast.EngineConfig{Observer: obs})
				node := &sharedNode{
					endpoint: endpoint,
					stack:    stack,
					engine:   engine,
					obs:      obs,
					peers:    peers,
					streams:  streams,
				}
				peerInput := peers
				if !receivePeers {
					peerInput = nil
				}
				node.wg.Go(func() { _ = engine.Serve(ctx, peerInput, streams) })
				return node
			}

			a := newNode(true)
			// B must bind from its first incoming BCAST event: its peer
			// notification stays queued until both handshakes complete.
			b := newNode(false)
			channelID := broadcast.ChannelID("shared-transport-channel")
			thA := ss.createChannel(t, a.engine, channelID)
			thB := ss.createChannel(t, b.engine, channelID)

			shutdownOnce := sync.OnceFunc(func() {
				cancel()

				for _, node := range []*sharedNode{a, b} {
					if node.conn != nil {
						_ = node.conn.Close()
					}
					_ = node.endpoint.Shared.Close()
					_ = node.endpoint.Packet.Close()
				}

				for _, node := range []*sharedNode{a, b} {
					node.wg.Wait()
					drainQueuedStreams(node.streams)
					drainQueuedPeers(node.peers)
				}

				thA.stop()
				thB.stop()
				_ = a.engine.Close()
				_ = b.engine.Close()
			})
			t.Cleanup(shutdownOnce)

			a.conn, b.conn = transporttest.Connect(t, a.endpoint, b.endpoint)

			// The local identity belongs to the endpoint, so only the remote
			// identity is asserted on the connection.
			if got := a.conn.RemotePeerID(); got != b.endpoint.Shared.PeerID() {
				t.Fatalf("dialed remote peer ID = %x, want %x", got, b.endpoint.Shared.PeerID())
			}
			if got := b.conn.RemotePeerID(); got != a.endpoint.Shared.PeerID() {
				t.Fatalf("accepted remote peer ID = %x, want %x", got, a.endpoint.Shared.PeerID())
			}

			for _, node := range []*sharedNode{a, b} {
				node.wg.Go(func() {
					_ = node.stack.ServeConn(ctx, node.conn)
				})
			}

			waitForPeers(t, a.obs, channelID, 1, timeout)
			waitForPeers(t, b.obs, channelID, 1, timeout)
			// The delayed peer notification carries the same *Peer as B's
			// stream events and must not create a second broadcast binding.
			b.wg.Go(func() { _ = b.engine.Serve(ctx, b.peers, nil) })

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

			shutdownOnce()
		})
	}
}
