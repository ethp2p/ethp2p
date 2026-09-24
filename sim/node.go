package sim

import (
	"context"
	"errors"
	"log/slog"
	"net"
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
	num    int
	shared *transport.SharedTransport
	packet net.PacketConn
	peerID transport.PeerID
	stack  *ethp2p.Stack
	engine *broadcast.Engine

	publishFn func(broadcast.MessageID, []byte) error
	stopFn    func()
	recvCh    chan broadcast.FullMessage

	logger *slog.Logger

	mu        sync.Mutex
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

// Start satisfies Node; construction already starts the stack and reports errors.
func (n *BroadcastNode) Start(context.Context) {}

func (n *BroadcastNode) Close() error {
	n.closeOnce.Do(func() {
		stackErr := n.stack.Close()
		n.stopFn()
		engineErr := n.engine.Close()
		endpointErr := n.shared.Close()
		packetErr := n.packet.Close()
		n.closeErr = errors.Join(stackErr, engineErr, endpointErr, packetErr)
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

func (n *BroadcastNode) DialPeer(ctx context.Context, p int, addr net.Addr) error {
	rec, err := nodeRecord(p, addr)
	if err != nil {
		return err
	}
	return n.stack.Connect(ctx, rec)
}

func (n *BroadcastNode) BandwidthStats() (bytesSent, bytesReceived int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rawBandwidth()
}

func (n *BroadcastNode) rawBandwidth() (sent, received int) {
	tx, rx := n.stack.Traffic()
	return int(tx), int(rx)
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

// newBroadcastNode creates and starts the node's stack using its identity and
// bound address, then attaches the broadcast worker. Used by ECStrategy.
func newBroadcastNode(
	engine *broadcast.Engine,
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
	record, err := nodeRecord(nodeNum, conn.LocalAddr())
	if err == nil {
		var stack *ethp2p.Stack
		stack, err = ethp2p.NewStack(eth, ethp2p.Config{Record: record})
		if err == nil {
			err = engine.Register(stack)
		}
		if err == nil {
			err = stack.Start()
		}
		if err == nil {
			return &BroadcastNode{num: nodeNum, shared: shared, packet: conn, peerID: peerID,
				stack: stack, engine: engine, publishFn: publishFn, stopFn: stopFn, recvCh: recvCh, logger: logger}, nil
		}
	}
	stopFn()
	_ = engine.Close()
	_ = shared.Close()
	return nil, err
}
