//go:build integration

package tests

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/transport"
)

// TestSharedTransportBroadcastRoundTrip exercises the broadcast stack over
// the production shared transport. The regular integration helpers use a raw
// QUIC host, so this test specifically covers authenticated TransportEth
// connections and the production five-stream unidirectional limit.
func TestSharedTransportBroadcastRoundTrip(t *testing.T) {
	for _, ss := range strategies {
		t.Run(ss.name, func(t *testing.T) {
			const timeout = 15 * time.Second

			ctx, cancel := context.WithTimeout(t.Context(), timeout)
			defer cancel()

			type sharedNode struct {
				shared  *transport.TransportShared
				packet  *net.UDPConn
				eth     *transport.TransportEth
				stack   *ethp2p.Stack
				engine  *broadcast.Engine
				obs     *testObserver
				peers   chan *ethp2p.Peer
				streams chan ethp2p.StreamEvent
				conn    transport.Conn
				wg      sync.WaitGroup
			}

			newNode := func(receivePeers bool) *sharedNode {
				key, err := transport.GenPrivKey()
				if err != nil {
					t.Fatal(err)
				}
				packet, err := net.ListenUDP("udp4", &net.UDPAddr{
					IP:   net.IPv4(127, 0, 0, 1),
					Port: 0,
				})
				if err != nil {
					t.Fatal(err)
				}
				shared, err := transport.NewShared(key, packet)
				if err != nil {
					_ = packet.Close()
					t.Fatal(err)
				}
				t.Cleanup(func() {
					_ = shared.Close()
					_ = packet.Close()
				})

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
					shared:  shared,
					packet:  packet,
					eth:     shared.Ethp2p(),
					stack:   stack,
					engine:  engine,
					obs:     obs,
					peers:   peers,
					streams: streams,
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
					_ = node.shared.Close()
					_ = node.packet.Close()
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

			dialResult := make(chan struct {
				conn transport.Conn
				err  error
			}, 1)
			go func() {
				conn, err := a.eth.Dial(ctx, b.eth.Addr(), b.eth.PeerID())
				dialResult <- struct {
					conn transport.Conn
					err  error
				}{conn: conn, err: err}
			}()

			bConn, err := b.eth.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			dialed := <-dialResult
			if dialed.err != nil {
				t.Fatal(dialed.err)
			}
			a.conn = dialed.conn
			b.conn = bConn

			if auth := a.conn.AuthInfo(); auth.Local != transport.PeerID(a.eth.PeerID()) ||
				auth.Remote != transport.PeerID(b.eth.PeerID()) || auth.RemoteKey == nil ||
				a.conn.Direction() != transport.ConnDirOut {
				t.Fatalf("dialed auth = %+v", auth)
			}
			if auth := b.conn.AuthInfo(); auth.Local != transport.PeerID(b.eth.PeerID()) ||
				auth.Remote != transport.PeerID(a.eth.PeerID()) || auth.RemoteKey == nil ||
				b.conn.Direction() != transport.ConnDirIn {
				t.Fatalf("accepted auth = %+v", auth)
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
