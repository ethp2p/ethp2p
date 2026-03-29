//go:build integration

package tests

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/broadcast/rs"
	"github.com/ethp2p/ethp2p/transport"
	quicpkg "github.com/ethp2p/ethp2p/transport/quic"
	"github.com/quic-go/quic-go"
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

// --- QUIC host ---

type quicHost struct {
	tr       *quic.Transport
	listener *quic.Listener
	addr     net.Addr
	tlsConf  *tls.Config
	quicConf *quic.Config
}

func newQUICHost(t *testing.T) *quicHost {
	t.Helper()

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("listen UDP: %v", err)
	}

	tlsConf, err := generateTestTLSConfig()
	if err != nil {
		conn.Close()
		t.Fatalf("generate TLS config: %v", err)
	}

	quicConf := &quic.Config{MaxIdleTimeout: 30 * time.Second}

	tr := &quic.Transport{Conn: conn}
	ln, err := tr.Listen(tlsConf, quicConf)
	if err != nil {
		conn.Close()
		t.Fatalf("QUIC listen: %v", err)
	}

	h := &quicHost{
		tr:       tr,
		listener: ln,
		addr:     conn.LocalAddr(),
		tlsConf:  tlsConf,
		quicConf: quicConf,
	}
	t.Cleanup(func() { h.close() })
	return h
}

func (h *quicHost) dial(ctx context.Context, addr net.Addr) (transport.Conn, error) {
	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"eth-ec-broadcast-test"},
	}
	conn, err := h.tr.Dial(ctx, addr, tlsConf, h.quicConf)
	if err != nil {
		return nil, err
	}
	return quicpkg.NewTransport(conn, transport.Outbound), nil
}

func (h *quicHost) accept(ctx context.Context) (transport.Conn, error) {
	conn, err := h.listener.Accept(ctx)
	if err != nil {
		return nil, err
	}
	return quicpkg.NewTransport(conn, transport.Inbound), nil
}

func (h *quicHost) close() error {
	return errors.Join(
		h.listener.Close(),
		h.tr.Close(),
		h.tr.Conn.Close(),
	)
}

func generateTestTLSConfig() (*tls.Config, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, priv.Public(), priv)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{certDER},
			PrivateKey:  priv,
		}},
		NextProtos:         []string{"eth-ec-broadcast-test"},
		InsecureSkipVerify: true,
	}, nil
}

// --- Test node ---

type testNode struct {
	host   *quicHost
	engine *broadcast.Engine
	obs    *testObserver
	peerID broadcast.PeerID
}

func newTestNode(t *testing.T, peerID broadcast.PeerID) *testNode {
	t.Helper()
	host := newQUICHost(t)
	obs := newTestObserver()
	cfg := broadcast.EngineConfig{
		PeerID:   peerID,
		Observer: obs,
	}
	engine := broadcast.NewEngine(cfg)
	t.Cleanup(func() { engine.Close() })
	return &testNode{
		host:   host,
		engine: engine,
		obs:    obs,
		peerID: peerID,
	}
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

	// peerSubs tracks per-channel peer sets from OnPeerSubscribed/OnPeerUnsubscribed/OnPeerGone.
	peerSubs map[broadcast.ChannelID]map[broadcast.PeerID]struct{}
}

func newTestObserver() *testObserver {
	return &testObserver{
		decoded:  make(map[observerKey]chan struct{}),
		disposed: make(map[observerKey]chan struct{}),
		created:  make(map[observerKey]chan struct{}),
		peerSubs: make(map[broadcast.ChannelID]map[broadcast.PeerID]struct{}),
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

func (o *testObserver) OnPeerSubscribed(peerID broadcast.PeerID, channelID broadcast.ChannelID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.peerSubs[channelID] == nil {
		o.peerSubs[channelID] = make(map[broadcast.PeerID]struct{})
	}
	o.peerSubs[channelID][peerID] = struct{}{}
}

func (o *testObserver) OnPeerUnsubscribed(peerID broadcast.PeerID, channelID broadcast.ChannelID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if subs := o.peerSubs[channelID]; subs != nil {
		delete(subs, peerID)
	}
}

func (o *testObserver) OnPeerGone(peerID broadcast.PeerID) {
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
		t.Fatalf("timeout waiting for decode of %s/%s", channelID, messageID)
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

// connectNodes establishes QUIC connections between nodes according to
// the edge list and notifies their engines. Blocks until all connections
// are established but does NOT wait for handshakes to complete.
func connectNodes(t *testing.T, nodes []*testNode, edges []edge) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, e := range edges {
		from := nodes[e.from]
		to := nodes[e.to]

		var dialConn, acceptConn transport.Conn
		var dialErr, acceptErr error
		var wg sync.WaitGroup

		wg.Add(2)
		go func() {
			defer wg.Done()
			dialConn, dialErr = from.host.dial(ctx, to.host.addr)
		}()
		go func() {
			defer wg.Done()
			acceptConn, acceptErr = to.host.accept(ctx)
		}()
		wg.Wait()

		if dialErr != nil {
			t.Fatalf("dial %d->%d: %v", e.from, e.to, dialErr)
		}
		if acceptErr != nil {
			t.Fatalf("accept %d->%d: %v", e.from, e.to, acceptErr)
		}

		from.engine.NotifyPeerConnected(dialConn)
		to.engine.NotifyPeerConnected(acceptConn)
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
