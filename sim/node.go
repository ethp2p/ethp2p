package sim

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"slices"
	"sync"

	"github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/transport"
)

// Node defines the interface for network simulation nodes.
type Node interface {
	Start(ctx context.Context)
	Publish(messageID string, data []byte)
	Receive(ctx context.Context) (messageID string, data []byte, err error)
	DialPeer(ctx context.Context, nodeNum int, addr net.Addr) error
	BandwidthStats() (sent, received int)
	ResetBandwidthStats() (sent, received int)
	Addr() net.Addr
	NodeNum() int
	Close() error
}

// BroadcastNode wraps broadcast.Engine and a generic Channel to implement the
// Node interface. The generic Channel boundary is captured at construction
// via closures (publishFn, stopFn) and a receive channel.
//
// The node owns a shared QUIC endpoint and its ethp2p view. Peer identity comes
// from the transport's authenticated handshake rather than from the topology.
type BroadcastNode struct {
	num     int
	eth     *transport.Ethp2pTransport
	shared  *transport.SharedTransport
	packet  net.PacketConn
	peerID  transport.PeerID
	stack   *ethp2p.Stack
	engine  *broadcast.Engine
	peers   chan *ethp2p.Peer
	streams chan ethp2p.StreamEvent
	appCtx  context.Context
	cancel  context.CancelFunc

	publishFn func(broadcast.MessageID, []byte) error
	stopFn    func()
	recvCh    chan broadcast.FullMessage

	logger *slog.Logger

	mu        sync.Mutex
	conns     []transport.Conn
	wg        sync.WaitGroup
	closed    bool
	closeOnce sync.Once
	closeErr  error
	baseSent  int // cumulative baseline for ResetBandwidthStats
	baseRecv  int
}

func (n *BroadcastNode) NodeNum() int {
	return n.num
}

func (n *BroadcastNode) Addr() net.Addr {
	return n.packet.LocalAddr()
}

// PeerID returns the node's authenticated transport identity.
func (n *BroadcastNode) PeerID() transport.PeerID {
	return n.peerID
}

func (n *BroadcastNode) Start(ctx context.Context) {
	acceptCtx, cancel := context.WithCancel(n.appCtx)
	stop := context.AfterFunc(ctx, cancel)
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		stop()
		cancel()
		return
	}
	n.wg.Go(func() {
		defer stop()
		defer cancel()
		n.processIncomingConnections(acceptCtx)
	})
	n.mu.Unlock()
}

func (n *BroadcastNode) Close() error {
	n.closeOnce.Do(func() {
		n.mu.Lock()
		n.closed = true
		cancel := n.cancel
		conns := slices.Clone(n.conns)
		n.mu.Unlock()

		// The application owns the context, connection views, endpoint, and
		// serving goroutines. Cancel first, then close every connection view and
		// the endpoint so all pending Stack and QUIC I/O can return.
		cancel()
		var closeErr error
		for _, conn := range conns {
			closeErr = errors.Join(closeErr, conn.Close())
		}
		// The shared transport does not own the packet connection.
		endpointErr := n.shared.Close()
		packetErr := n.packet.Close()
		n.wg.Wait()

		// Stack does not own delivered streams. The serving goroutines have stopped,
		// so no producer can enqueue another event; dispose of the events that were
		// already queued before stopping the worker and its internal event loop.
		drainStreamEvents(n.streams)
		drainPeers(n.peers)
		n.stopFn()
		engineErr := n.engine.Close()
		closeErr = errors.Join(closeErr, endpointErr, packetErr, engineErr)

		n.closeErr = closeErr
	})
	return n.closeErr
}

func (n *BroadcastNode) Publish(messageID string, data []byte) {
	n.publishFn(broadcast.MessageID(messageID), data)
}

func (n *BroadcastNode) Receive(ctx context.Context) (string, []byte, error) {
	select {
	case msg, ok := <-n.recvCh:
		if !ok {
			return "", nil, errors.New("subscription closed")
		}
		return string(msg.MessageID), msg.Data, nil
	case <-ctx.Done():
		return "", nil, ctx.Err()
	}
}

func (n *BroadcastNode) processIncomingConnections(ctx context.Context) {
	for {
		conn, err := n.eth.Accept(ctx)
		if err != nil {
			if !(errors.Is(err, ctx.Err()) || errors.Is(err, transport.ErrClosed)) {
				n.logger.Error("failed to accept connection", "err", err)
			}
			return
		}
		// The transport authenticated the peer during the handshake.
		n.serveConn(conn)
	}
}

func (n *BroadcastNode) DialPeer(ctx context.Context, p int, addr net.Addr) error {
	// The expected identity is not pinned: a Shadow node runs in its own process
	// and does not know its peers' keys before connecting. The handshake still
	// authenticates whichever ethp2p peer answers.
	conn, err := n.eth.Dial(ctx, addr, "")
	if err != nil {
		return err
	}

	n.serveConn(conn)
	return nil
}

func (n *BroadcastNode) serveConn(ethConn transport.Conn) {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		_ = ethConn.Close()
		return
	}
	n.conns = append(n.conns, ethConn)
	appCtx := n.appCtx
	n.wg.Go(func() {
		err := n.stack.ServeConn(appCtx, ethConn)
		_ = ethConn.Close()
		n.mu.Lock()
		closed := n.closed
		n.mu.Unlock()
		if err != nil && !closed {
			n.logger.Error("ethp2p connection ended", "err", err)
		}
	})
	n.mu.Unlock()
}

func (n *BroadcastNode) BandwidthStats() (bytesSent, bytesReceived int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rawBandwidth()
}

func (n *BroadcastNode) rawBandwidth() (sent, received int) {
	for _, c := range n.conns {
		tx, rx := c.ConnectionStats()
		sent += int(tx)
		received += int(rx)
	}
	return sent, received
}

// ResetBandwidthStats returns bytes sent/received since the last reset
// and advances the baseline.
func (n *BroadcastNode) ResetBandwidthStats() (bytesSent, bytesReceived int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	sent, recv := n.rawBandwidth()
	ds, dr := sent-n.baseSent, recv-n.baseRecv
	n.baseSent, n.baseRecv = sent, recv
	return ds, dr
}

// newBroadcastNode assembles a BroadcastNode from its registered stack,
// broadcast worker, channel closures, and raw PacketConn. Used by ECStrategy.
func newBroadcastNode(
	stack *ethp2p.Stack,
	engine *broadcast.Engine,
	peers chan *ethp2p.Peer,
	streams chan ethp2p.StreamEvent,
	publishFn func(broadcast.MessageID, []byte) error,
	stopFn func(),
	recvCh chan broadcast.FullMessage,
	conn net.PacketConn,
	nodeNum int,
	logger *slog.Logger,
) (*BroadcastNode, error) {
	eth, shared, peerID, err := newNodeEndpoint(nodeNum, conn)
	if err != nil {
		stopFn()
		_ = engine.Close()
		return nil, err
	}
	appCtx, cancel := context.WithCancel(context.Background())

	n := &BroadcastNode{
		num:       nodeNum,
		eth:       eth,
		shared:    shared,
		packet:    conn,
		peerID:    peerID,
		stack:     stack,
		engine:    engine,
		peers:     peers,
		streams:   streams,
		appCtx:    appCtx,
		cancel:    cancel,
		publishFn: publishFn,
		stopFn:    stopFn,
		recvCh:    recvCh,
		logger:    logger,
	}
	n.wg.Go(func() {
		if err := engine.Serve(appCtx, peers, streams); err != nil && !errors.Is(err, context.Canceled) {
			n.logger.Error("broadcast engine ended", "err", err)
		}
	})
	return n, nil
}

func drainStreamEvents(streams <-chan ethp2p.StreamEvent) {
	for {
		select {
		case event := <-streams:
			event.Reject()
		default:
			return
		}
	}
}

func drainPeers(peers <-chan *ethp2p.Peer) {
	for {
		select {
		case <-peers:
		default:
			return
		}
	}
}
