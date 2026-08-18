package sim

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/transport"
	quicTransport "github.com/ethp2p/ethp2p/transport/quic"
	"github.com/quic-go/quic-go"
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
type BroadcastNode struct {
	num      int
	quicHost QUICHost
	engine   *broadcast.Engine

	publishFn func(broadcast.MessageID, []byte) error
	stopFn    func()
	recvCh    chan broadcast.FullMessage

	logger *slog.Logger

	mu         sync.Mutex
	conns      []*quic.Conn
	peerByAddr map[string]int
	wg         sync.WaitGroup
	closed     bool
	baseSent   int // cumulative baseline for ResetBandwidthStats
	baseRecv   int
}

func (n *BroadcastNode) NodeNum() int {
	return n.num
}

func (n *BroadcastNode) Addr() net.Addr {
	return n.quicHost.UDPAddr
}

func (n *BroadcastNode) Start(ctx context.Context) {
	go n.processIncomingConnections(ctx)
}

func (n *BroadcastNode) Close() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	n.mu.Unlock()

	n.stopFn()
	err := n.engine.Close()
	err2 := n.quicHost.Close()
	n.wg.Wait()
	return errors.Join(err, err2)
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
		c, err := n.quicHost.Accept(ctx)
		if err != nil {
			if !(errors.Is(err, ctx.Err()) || strings.Contains(err.Error(), "transport closed")) {
				n.logger.Error("failed to accept connection", "err", err)
			}
			return
		}
		n.mu.Lock()
		n.conns = append(n.conns, c)
		n.mu.Unlock()
		n.mu.Lock()
		remote, ok := n.peerByAddr[c.RemoteAddr().String()]
		n.mu.Unlock()
		if !ok {
			_ = c.CloseWithError(0, "unknown simulation peer")
			n.logger.Error("failed to authenticate simulation peer", "addr", c.RemoteAddr())
			continue
		}
		n.engine.NotifyPeerConnected(quicTransport.NewTransport(
			c,
			transport.Inbound,
			simAuthInfo(n.num, remote),
		))
	}
}

// setPeerAddresses installs the topology-derived address-to-node mapping used
// to authenticate inbound simulation connections. Scenario calls this before
// Start, so a Shadow process does not depend on state from other processes.
func (n *BroadcastNode) setPeerAddresses(peers map[int]net.Addr) {
	byAddr := make(map[string]int, len(peers))
	for nodeNum, addr := range peers {
		if addr != nil {
			byAddr[addr.String()] = nodeNum
		}
	}
	n.mu.Lock()
	n.peerByAddr = byAddr
	n.mu.Unlock()
}

func (n *BroadcastNode) DialPeer(ctx context.Context, p int, addr net.Addr) error {
	cu, err := n.quicHost.Dial(ctx, addr, nil)
	if err != nil {
		return err
	}

	n.mu.Lock()
	n.conns = append(n.conns, cu)
	n.mu.Unlock()

	n.engine.NotifyPeerConnected(quicTransport.NewTransport(
		cu,
		transport.Outbound,
		simAuthInfo(n.num, p),
	))
	return nil
}

func (n *BroadcastNode) BandwidthStats() (bytesSent, bytesReceived int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rawBandwidth()
}

func (n *BroadcastNode) rawBandwidth() (sent, received int) {
	for _, c := range n.conns {
		s := c.ConnectionStats()
		sent += int(s.BytesSent)
		received += int(s.BytesReceived)
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

// newBroadcastNode assembles a BroadcastNode from an already-configured
// engine, channel closures, and a raw PacketConn. Used by NewECNodeFunc.
func newBroadcastNode(
	engine *broadcast.Engine,
	publishFn func(broadcast.MessageID, []byte) error,
	stopFn func(),
	recvCh chan broadcast.FullMessage,
	conn net.PacketConn,
	nodeNum int,
	logger *slog.Logger,
) (*BroadcastNode, error) {
	qh, err := NewQUICHost(conn)
	if err != nil {
		engine.Close()
		return nil, fmt.Errorf("failed to create quic host: %w", err)
	}

	return &BroadcastNode{
		num:        nodeNum,
		quicHost:   qh,
		engine:     engine,
		publishFn:  publishFn,
		stopFn:     stopFn,
		recvCh:     recvCh,
		logger:     logger,
		peerByAddr: make(map[string]int),
	}, nil
}

func simAuthInfo(local, remote int) transport.AuthInfo {
	localID := strconv.Itoa(local)
	remoteID := strconv.Itoa(remote)
	return transport.AuthInfo{
		Local:  transport.PeerID(localID),
		Remote: transport.PeerID(remoteID),
	}
}
