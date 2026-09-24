//go:build integration

package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/broadcast/rs"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
)

// --- Strategy parameterization ---

// subscribable erases Channel's type parameters so tests can create
// channels for any strategy through a uniform API.
type subscribable interface {
	createChannel(t *testing.T, e *broadcast.Engine, id broadcast.ChannelID) channelHandle
}

// channelHandle wraps a generic Channel with type-erased accessors.
type channelHandle struct {
	msgCh   chan broadcast.FullMessage
	publish func(broadcast.MessageID, []byte) error
	stop    func()
}

type rsSetup struct{}

func (rsSetup) createChannel(t *testing.T, e *broadcast.Engine, id broadcast.ChannelID) channelHandle {
	t.Helper()
	channel := broadcast.AttachChannel(e, id, rs.NewScheme(rs.DefaultConfig()))
	ch := make(chan broadcast.FullMessage, 128)
	channel.Subscribe(ch)
	return channelHandle{
		msgCh:   ch,
		publish: channel.Publish,
		stop:    channel.Stop,
	}
}

var strategies = []struct {
	name string
	subscribable
}{
	{"rs", rsSetup{}},
}

// --- Test node ---

type testNode struct {
	endpoint *transporttest.Endpoint
	stack    *ethp2p.Stack
	bound    bool
	engine   *broadcast.Engine
	obs      *testObserver

	closeErr error
	once     sync.Once
}

func newTestNode(t *testing.T) *testNode {
	t.Helper()
	endpoint := transporttest.NewEndpoint(t)
	obs := newTestObserver()
	cfg := broadcast.EngineConfig{Observer: obs}
	stack, err := ethp2p.NewStack(endpoint.Eth, ethp2p.Config{Record: endpoint.Record(t, 1)})
	if err != nil {
		t.Fatal(err)
	}
	engine := broadcast.NewEngine(cfg)
	if err := engine.Register(stack); err != nil {
		t.Fatal(err)
	}
	n := &testNode{
		endpoint: endpoint,
		stack:    stack,
		engine:   engine,
		obs:      obs,
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func (n *testNode) Close() error {
	n.once.Do(func() {
		n.closeErr = errors.Join(n.stack.Close(), n.engine.Close(), n.endpoint.Shared.Close(), n.endpoint.Packet.Close())
	})
	return n.closeErr
}

// --- Observer ---

type observerKey struct {
	channelID broadcast.ChannelID
	messageID broadcast.MessageID
}

type testObserver struct {
	broadcast.NoOpObserver

	mu       sync.Mutex
	decoded  map[observerKey]chan struct{}
	disposed map[observerKey]chan struct{}
	created  map[observerKey]chan struct{}
	received map[observerKey]int
	errors   []broadcast.ChunkProcessError

	// peerSubs tracks per-channel peer sets from OnPeerSubscribed/OnPeerUnsubscribed/OnPeerGone.
	peerSubs map[broadcast.ChannelID]map[transport.PeerID]struct{}
}

func newTestObserver() *testObserver {
	return &testObserver{
		decoded:  make(map[observerKey]chan struct{}),
		disposed: make(map[observerKey]chan struct{}),
		created:  make(map[observerKey]chan struct{}),
		received: make(map[observerKey]int),
		peerSubs: make(map[broadcast.ChannelID]map[transport.PeerID]struct{}),
	}
}

func (o *testObserver) OnSessionStarted(channelID broadcast.ChannelID, messageID broadcast.MessageID, _ broadcast.SessionRole) {
	o.mu.Lock()
	defer o.mu.Unlock()
	key := observerKey{channelID, messageID}
	ch, ok := o.created[key]
	if !ok {
		ch = make(chan struct{})
		o.created[key] = ch
	}
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func (o *testObserver) OnSessionDecoded(channelID broadcast.ChannelID, messageID broadcast.MessageID, latency time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	key := observerKey{channelID, messageID}
	ch, ok := o.decoded[key]
	if !ok {
		ch = make(chan struct{})
		o.decoded[key] = ch
	}
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func (o *testObserver) OnSessionDisposed(channelID broadcast.ChannelID, messageID broadcast.MessageID, reason string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	key := observerKey{channelID, messageID}
	ch, ok := o.disposed[key]
	if !ok {
		ch = make(chan struct{})
		o.disposed[key] = ch
	}
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func (o *testObserver) OnPeerSubscribed(peerID transport.PeerID, channelID broadcast.ChannelID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.peerSubs[channelID] == nil {
		o.peerSubs[channelID] = make(map[transport.PeerID]struct{})
	}
	o.peerSubs[channelID][peerID] = struct{}{}
}

func (o *testObserver) OnChunkRcvd(_ transport.PeerID, channelID broadcast.ChannelID, messageID broadcast.MessageID, _ broadcast.Verdict) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.received[observerKey{channelID, messageID}]++
}

func (o *testObserver) OnChunkError(err broadcast.ChunkProcessError) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.errors = append(o.errors, err)
}

func (o *testObserver) OnPeerUnsubscribed(peerID transport.PeerID, channelID broadcast.ChannelID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if subs := o.peerSubs[channelID]; subs != nil {
		delete(subs, peerID)
	}
}

func (o *testObserver) OnPeerGone(peerID transport.PeerID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, subs := range o.peerSubs {
		delete(subs, peerID)
	}
}

func (o *testObserver) peerCount(channelID broadcast.ChannelID) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.peerSubs[channelID])
}

func (o *testObserver) waitDecoded(t *testing.T, channelID broadcast.ChannelID, messageID broadcast.MessageID, timeout time.Duration) {
	t.Helper()
	o.mu.Lock()
	key := observerKey{channelID, messageID}
	ch, ok := o.decoded[key]
	if !ok {
		ch = make(chan struct{})
		o.decoded[key] = ch
	}
	o.mu.Unlock()

	select {
	case <-ch:
	case <-time.After(timeout):
		o.mu.Lock()
		defer o.mu.Unlock()
		started := false
		select {
		case <-o.created[key]:
			started = true
		default:
		}
		t.Fatalf("timeout waiting for decode of %s/%s: started=%t received=%d errors=%v", channelID, messageID, started, o.received[key], o.errors)
	}
}

func (o *testObserver) waitCreated(t *testing.T, channelID broadcast.ChannelID, messageID broadcast.MessageID, timeout time.Duration) {
	t.Helper()
	o.mu.Lock()
	key := observerKey{channelID, messageID}
	ch, ok := o.created[key]
	if !ok {
		ch = make(chan struct{})
		o.created[key] = ch
	}
	o.mu.Unlock()

	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for session creation of %s/%s", channelID, messageID)
	}
}

// --- Topology ---

// TODO: migrate to a generic topology generator shared between
// sim and broadcast/tests (see sim/types.go Topology/EdgeSpec).

type edge struct {
	from, to int
}

func chainEdges(n int) []edge {
	edges := make([]edge, n-1)
	for i := range edges {
		edges[i] = edge{from: i, to: i + 1}
	}
	return edges
}

func starEdges(n int) []edge {
	edges := make([]edge, n-1)
	for i := range edges {
		edges[i] = edge{from: 0, to: i + 1}
	}
	return edges
}

// connectNodes connects nodes according to the edge list and serves the
// resulting connections through their stacks. Blocks until all connections are
// established but does NOT wait for handshakes to complete.
func connectNodes(t *testing.T, nodes []*testNode, edges []edge) {
	t.Helper()
	for _, node := range nodes {
		if !node.bound {
			if err := node.stack.Start(); err != nil {
				t.Fatal(err)
			}
			node.bound = true
		}
	}
	for _, e := range edges {
		from := nodes[e.from]
		to := nodes[e.to]
		ctx, cancel := context.WithTimeout(t.Context(), defaultTimeout)
		err := from.stack.Connect(ctx, to.endpoint.Record(t, 1))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
}

// waitForPeers polls the observer's peer subscription count for the given
// channel until the expected count is reached.
func waitForPeers(t *testing.T, obs *testObserver, channelID broadcast.ChannelID, expected int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if obs.peerCount(channelID) >= expected {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout: expected %d peers for channel %s, got %d", expected, channelID, obs.peerCount(channelID))
}

// testPayload generates a deterministic test payload of the given size.
func testPayload(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return data
}

// handshakeSettleTime is the time to wait after connectNodes for
// handshakes to complete over loopback.
const handshakeSettleTime = 200 * time.Millisecond

// defaultTimeout is the default timeout for waiting on async events.
const defaultTimeout = 10 * time.Second
