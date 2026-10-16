package sim

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/broadcast/rs"
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

// BroadcastNode owns the shared QUIC endpoint, ethp2p stack, RS engine, channel,
// and message subscription, assembled in the stack's documented setup order.
//
// Peer identity comes from the transport's authenticated handshake rather than
// from the topology.
type BroadcastNode struct {
	num      int
	endpoint *transport.Endpoint
	packet   net.PacketConn
	stack    *ethp2p.Stack
	engine   *broadcast.Engine

	channel *broadcast.Channel[*rs.ChunkIdent, *rs.Bitmap, *rs.Preamble]
	recvCh  chan broadcast.FullMessage

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

// Start satisfies Node; construction already starts the stack and reports errors.
func (n *BroadcastNode) Start(context.Context) {}

func (n *BroadcastNode) Close() error {
	n.closeOnce.Do(func() {
		stackErr := n.stack.Close()
		n.channel.Stop()
		engineErr := n.engine.Close()
		endpointErr := n.endpoint.Close()
		packetErr := n.packet.Close()
		n.closeErr = errors.Join(stackErr, engineErr, endpointErr, packetErr)
	})
	return n.closeErr
}

func (n *BroadcastNode) Publish(messageID string, data []byte) {
	_ = n.channel.Publish(broadcast.MessageID(messageID), data)
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

func newParticipant(s Strategy, nodeNum int, conn net.PacketConn, logger *slog.Logger, obs broadcast.Observer, tw *TraceWriter) (Node, error) {
	switch s.Name {
	case "RS":
		return newBroadcastNode(s.RS, conn, nodeNum, obs)
	case "gossipsub":
		return NewGossipsubNode(conn, nodeNum, logger, tw)
	default:
		return nil, fmt.Errorf("unknown strategy: %s", s.Name)
	}
}

// newBroadcastNode builds the endpoint and stack, registers the RS engine, and
// subscribes the channel before starting the endpoint and stack.
// The returned node owns their shutdown and conn; on failure, the caller owns
// conn. Transport owns the connection pumps.
func newBroadcastNode(
	cfg rs.Config,
	conn net.PacketConn,
	nodeNum int,
	obs broadcast.Observer,
) (*BroadcastNode, error) {
	endpoint, err := newNodeEndpoint(nodeNum, conn)
	if err != nil {
		return nil, err
	}
	record, err := nodeRecord(nodeNum, conn.LocalAddr())
	if err != nil {
		_ = endpoint.Close()
		return nil, err
	}
	stack, err := ethp2p.NewStack(endpoint.Ethp2p(), ethp2p.Config{Record: record})
	if err != nil {
		_ = endpoint.Close()
		return nil, err
	}
	engine := broadcast.NewEngine(broadcast.EngineConfig{Observer: obs})
	if err := engine.Register(stack); err != nil {
		_ = stack.Close()
		_ = engine.Close()
		_ = endpoint.Close()
		return nil, err
	}
	channel := broadcast.AttachChannel(engine, "broadcast", rs.NewScheme(cfg))
	recvCh := make(chan broadcast.FullMessage, 64)
	if err := channel.Subscribe(recvCh); err != nil {
		_ = stack.Close()
		channel.Stop()
		_ = engine.Close()
		_ = endpoint.Close()
		return nil, fmt.Errorf("failed to subscribe: %w", err)
	}
	// Start requires both views wired, even though RS nodes run no libp2p host.
	_ = endpoint.Libp2p()
	if err := endpoint.Start(); err != nil {
		_ = stack.Close()
		channel.Stop()
		_ = engine.Close()
		_ = endpoint.Close()
		return nil, err
	}
	if err := stack.Start(); err != nil {
		_ = stack.Close()
		channel.Stop()
		_ = engine.Close()
		_ = endpoint.Close()
		return nil, err
	}
	return &BroadcastNode{num: nodeNum, endpoint: endpoint, packet: conn,
		stack: stack, engine: engine, channel: channel, recvCh: recvCh}, nil
}
