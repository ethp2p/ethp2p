package nettest

import (
	"bytes"
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/broadcast/rs"
	"github.com/ethp2p/ethp2p/enr"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
)

// Node owns an authenticated endpoint, stack, engine, and channel collectors.
type Node struct {
	// Name identifies the node in failure timelines.
	Name string
	// ID is the deterministic authenticated transport peer ID.
	ID transport.PeerID
	// Record is the signed dial target for this endpoint.
	Record *enr.Record
	// Stack routes negotiated protocol streams.
	Stack *ethp2p.Stack
	// Engine runs the broadcast subsystem.
	Engine       *broadcast.Engine
	net          *Net
	packet       net.PacketConn
	shared       *transport.SharedTransport
	libListener  quicreuse.QUICListener
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	gates        []*Gate
	libViews     []libView
	closeErr     error
	wg           sync.WaitGroup
	once         sync.Once
	channels     map[broadcast.ChannelID]*Channel
	channelOrder []broadcast.ChannelID
}

type libView struct {
	remote string
	conn   quicreuse.QUICConn
}

func unexpectedClose(err error) error {
	if err == nil {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var unexpected []error
		for _, part := range joined.Unwrap() {
			if remaining := unexpectedClose(part); remaining != nil {
				unexpected = append(unexpected, remaining)
			}
		}
		return errors.Join(unexpected...)
	}
	if errors.Is(err, transport.ErrClosed) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func (nd *Node) recordClose(err error) {
	err = unexpectedClose(err)
	if err == nil {
		return
	}
	nd.mu.Lock()
	nd.closeErr = errors.Join(nd.closeErr, err)
	nd.mu.Unlock()
}

func (nd *Node) closeError() error {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	return nd.closeErr
}

func (nd *Node) trackLibView(conn quicreuse.QUICConn) {
	remote := conn.RemoteAddr().(*net.UDPAddr).IP.String()
	nd.mu.Lock()
	nd.libViews = append(nd.libViews, libView{remote: remote, conn: conn})
	nd.mu.Unlock()
}

func (nd *Node) closeLibViewsFor(other *Node) {
	remote := ""
	if other != nil {
		remote = other.packet.LocalAddr().(*net.UDPAddr).IP.String()
	}
	nd.mu.Lock()
	var closing []quicreuse.QUICConn
	kept := nd.libViews[:0]
	for _, view := range nd.libViews {
		if remote == "" || view.remote == remote {
			closing = append(closing, view.conn)
		} else {
			kept = append(kept, view)
		}
	}
	nd.libViews = kept
	nd.mu.Unlock()
	for _, view := range closing {
		nd.recordClose(view.CloseWithError(0, "link closed"))
	}
}

// Close stops the node once. Run reports any unexpected close errors.
func (nd *Node) Close() {
	nd.once.Do(func() {
		nd.cancel()
		nd.net.removeNodeLinks(nd)
		nd.closeLibViewsFor(nil)
		if nd.libListener != nil {
			nd.recordClose(nd.libListener.Close())
		}
		// Stop stack delivery before channels and engine; the shared endpoint
		// remains alive until every protocol owner has released its work.
		nd.recordClose(nd.Stack.Close())
		for i := len(nd.channelOrder) - 1; i >= 0; i-- {
			nd.channels[nd.channelOrder[i]].Close()
		}
		nd.recordClose(nd.Engine.Close())
		nd.recordClose(nd.shared.Close())
		nd.recordClose(nd.packet.Close())
		nd.wg.Wait()
	})
}

// Libp2pListener registers and receives the libp2p view of a shared QUIC
// connection. Call before Connect. The harness closes accepted views on
// Disconnect or Node.Close; callers may close them sooner.
func (nd *Node) Libp2pListener(t *testing.T) quicreuse.QUICListener {
	t.Helper()
	if nd.libListener != nil {
		panic("nettest: Libp2pListener called twice")
	}
	listener, err := nd.shared.Libp2p().Listen(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	nd.libListener = &trackingListener{owner: nd, base: listener}
	return nd.libListener
}

type trackingListener struct {
	owner *Node
	base  quicreuse.QUICListener
}

func (l *trackingListener) Accept(ctx context.Context) (quicreuse.QUICConn, error) {
	conn, err := l.base.Accept(ctx)
	if err == nil {
		l.owner.trackLibView(conn)
	}
	return conn, err
}
func (l *trackingListener) Close() error   { return l.base.Close() }
func (l *trackingListener) Addr() net.Addr { return l.base.Addr() }

// Channel is a subscribed broadcast channel with a lossless collector.
type Channel struct {
	id            broadcast.ChannelID
	net           *Net
	publish       func(broadcast.MessageID, []byte) error
	stop          func()
	recv          chan broadcast.FullMessage
	mu            sync.Mutex
	received      []broadcast.FullMessage
	changed       chan struct{}
	collectorDone chan struct{}
	once          sync.Once
}

// Attach attaches scheme as channel id on nd and subscribes a collector.
func Attach[CI broadcast.ChunkIdent, R broadcast.Wire, P broadcast.Wire](nd *Node, id broadcast.ChannelID, scheme broadcast.Scheme[CI, R, P]) *Channel {
	if _, exists := nd.channels[id]; exists {
		panic("duplicate channel: " + string(id))
	}
	channel := broadcast.AttachChannel(nd.Engine, id, scheme)
	recv := make(chan broadcast.FullMessage, 128)
	if err := channel.Subscribe(recv); err != nil {
		panic(err)
	}
	c := &Channel{id: id, net: nd.net, publish: channel.Publish, stop: channel.Stop, recv: recv, changed: make(chan struct{}), collectorDone: make(chan struct{})}
	go c.collect()
	nd.channels[id] = c
	nd.channelOrder = append(nd.channelOrder, id)
	return c
}

// Channel attaches the default RS scheme to id.
func (nd *Node) Channel(id broadcast.ChannelID) *Channel {
	return Attach(nd, id, rs.NewScheme(rs.DefaultConfig()))
}

func (c *Channel) collect() {
	defer close(c.collectorDone)
	for msg := range c.recv {
		c.mu.Lock()
		c.received = append(c.received, msg)
		close(c.changed)
		c.changed = make(chan struct{})
		c.mu.Unlock()
	}
}

// Publish sends a message and fails t if the scheme rejects it.
func (c *Channel) Publish(t *testing.T, id broadcast.MessageID, payload []byte) {
	t.Helper()
	if err := c.publish(id, payload); err != nil {
		t.Fatal(err)
	}
}

// Received settles the bubble, then snapshots every subscription delivery.
func (c *Channel) Received() []broadcast.FullMessage {
	c.net.Settle()
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.received)
}

// RequireReceived settles the bubble and checks exact delivery and bytes.
func (c *Channel) RequireReceived(t *testing.T, id broadcast.MessageID, payload []byte) {
	t.Helper()
	count := 0
	for _, msg := range c.Received() {
		if msg.MessageID != id {
			continue
		}
		count++
		if msg.ChannelID != c.id {
			t.Fatalf("message %s delivered on channel %s, want %s", id, msg.ChannelID, c.id)
		}
		if !bytes.Equal(msg.Data, payload) {
			t.Fatalf("message %s payload differs", id)
		}
	}
	if count != 1 {
		t.Fatalf("message %s received %d times, want exactly once", id, count)
	}
}

// AwaitReceived waits for id in virtual time, settles, then checks exact delivery.
func (c *Channel) AwaitReceived(t *testing.T, id broadcast.MessageID, payload []byte) {
	t.Helper()
	timer := time.NewTimer(DefaultTimeout)
	defer timer.Stop()
	for {
		c.mu.Lock()
		found := slices.ContainsFunc(c.received, func(m broadcast.FullMessage) bool { return m.MessageID == id })
		changed := c.changed
		c.mu.Unlock()
		if found {
			c.net.Settle()
			c.RequireReceived(t, id, payload)
			return
		}
		select {
		case <-changed:
		case <-timer.C:
			t.Fatalf("timeout awaiting delivery of %s\n%s", id, c.net.recorder.timeline())
		}
	}
}

// RequireNotReceived settles the bubble before checking for id.
func (c *Channel) RequireNotReceived(t *testing.T, id broadcast.MessageID) {
	t.Helper()
	for _, msg := range c.Received() {
		if msg.MessageID == id {
			t.Fatalf("unexpected message %s", id)
		}
	}
}

// Close stops the broadcast channel and joins its delivery collector.
func (c *Channel) Close() {
	c.once.Do(func() { c.stop(); <-c.collectorDone })
}
