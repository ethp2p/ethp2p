package sim

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	mrand "math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/quic-go/quic-go"
	"go.uber.org/fx"
)

const warmupBytesLen = 1 << 10
const gossipsubChannelID = "broadcast-test"

var warmupBytes = make([]byte, warmupBytesLen)

func getNodePrivateKey(nodeNum int) (crypto.PrivKey, error) {
	var seed [32]byte
	binary.BigEndian.PutUint64(seed[:], uint64(nodeNum))
	rng := mrand.NewChaCha8(seed)
	privkey, _, err := crypto.GenerateEd25519Key(rng)
	if err != nil {
		return nil, err
	}
	return privkey, err
}

func handshakeStream(s io.ReadWriteCloser, bytes []byte, nodeNum int) (int, error) {
	var p int64
	err := binary.Write(s, binary.BigEndian, int64(nodeNum))
	if err != nil {
		return 0, err
	}
	err = binary.Read(s, binary.BigEndian, &p)
	if err != nil {
		return 0, err
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		io.CopyN(io.Discard, s, int64(len(bytes)))
	})
	_, err = s.Write(bytes)
	wg.Wait()
	if err != nil {
		return 0, err
	}
	return int(p), nil
}

func multiaddrFromNetAddr(a net.Addr) ma.Multiaddr {
	if ua, ok := a.(*net.UDPAddr); ok {
		return ma.StringCast(fmt.Sprintf("/ip4/%s/udp/%d/quic-v1", ua.IP, ua.Port))
	}
	if ta, ok := a.(*net.TCPAddr); ok {
		return ma.StringCast(fmt.Sprintf("/ip4/%s/tcp/%d/", ta.IP, ta.Port))
	}
	panic(fmt.Sprintf("invalid net addr: %s", a))
}

func netAddrFromMultiaddr(a ma.Multiaddr) net.Addr {
	for _, c := range a {
		if c.Protocol().Code == ma.P_QUIC_V1 {
			a, err := manet.ToNetAddr(a[:len(a)-1])
			if err != nil {
				panic(fmt.Sprintf("invalid addr: %s", a))
			}
			return a
		} else if c.Protocol().Code == ma.P_TCP {
			a, err := manet.ToNetAddr(a)
			if err != nil {
				panic(fmt.Sprintf("invalid addr: %s", a))
			}
			return a
		}
	}
	panic(fmt.Sprintf("invalid addr: %s", a))
}

// GossipsubNode implements the Node interface using libp2p's gossipsub protocol.
type GossipsubNode struct {
	num      int
	ctx      context.Context
	cancel   context.CancelFunc
	ps       *pubsub.PubSub
	host     host.Host
	channel  *pubsub.Topic
	sub      *pubsub.Subscription
	logger   *slog.Logger
	trace    *gossipsubTrace
	baseSent int
	baseRecv int
}

var _ Node = (*GossipsubNode)(nil)

type gossipsubTrace struct {
	node int
	tw   *TraceWriter
}

var _ pubsub.RawTracer = (*gossipsubTrace)(nil)
var _ pubsub.EventTracer = (*gossipsubTrace)(nil)

func newGossipsubTrace(node int, tw *TraceWriter) *gossipsubTrace {
	if tw == nil {
		return nil
	}
	return &gossipsubTrace{node: node, tw: tw}
}

func (t *gossipsubTrace) writeEvent(code string, args ...any) {
	if t == nil || t.tw == nil {
		return
	}
	t.tw.WriteEvent(time.Now(), t.node, code, args...)
}

func (t *gossipsubTrace) AddPeer(p peer.ID, proto protocol.ID) {}
func (t *gossipsubTrace) RemovePeer(p peer.ID)                 {}
func (t *gossipsubTrace) Join(topic string)                    {}
func (t *gossipsubTrace) Leave(topic string)                   {}
func (t *gossipsubTrace) Graft(p peer.ID, topic string)        {}
func (t *gossipsubTrace) Prune(p peer.ID, topic string)        {}

func (t *gossipsubTrace) ValidateMessage(msg *pubsub.Message) {
	t.writeMessageEvent("va", msg.ReceivedFrom, msg.GetTopic(), gossipsubMessageID(msg.Data))
}

func (t *gossipsubTrace) DeliverMessage(msg *pubsub.Message)               {}
func (t *gossipsubTrace) RejectMessage(msg *pubsub.Message, reason string) {}
func (t *gossipsubTrace) DuplicateMessage(msg *pubsub.Message)             {}

func (t *gossipsubTrace) ThrottlePeer(p peer.ID) {}

func (t *gossipsubTrace) RecvRPC(rpc *pubsub.RPC) {
}

func (t *gossipsubTrace) SendRPC(rpc *pubsub.RPC, p peer.ID) {
	if t == nil || rpc == nil {
		return
	}
	peerID := p.String()
	for _, msg := range rpc.GetPublish() {
		channel := msg.GetTopic()
		if channel == "" {
			channel = gossipsubChannelID
		}
		msgID := gossipsubMessageID(msg.GetData())
		t.writeEvent("ms", peerID, channel, msgID, len(msg.GetData()))
	}
	ctl := rpc.GetControl()
	if ctl == nil {
		return
	}
	for _, ihave := range ctl.GetIhave() {
		channel := ihave.GetTopicID()
		if channel == "" {
			channel = gossipsubChannelID
		}
		msgIDs := ihave.GetMessageIDs()
		for _, msgID := range msgIDs {
			t.writeEvent("hs", peerID, channel, msgID, len(msgIDs))
		}
	}
	for _, iwant := range ctl.GetIwant() {
		msgIDs := iwant.GetMessageIDs()
		for _, msgID := range msgIDs {
			t.writeEvent("ws", peerID, gossipsubChannelID, msgID, len(msgIDs))
		}
	}
	for _, idontwant := range ctl.GetIdontwant() {
		msgIDs := idontwant.GetMessageIDs()
		for _, msgID := range msgIDs {
			t.writeEvent("ns", peerID, gossipsubChannelID, msgID, len(msgIDs))
		}
	}
}

func (t *gossipsubTrace) DropRPC(rpc *pubsub.RPC, p peer.ID) {}

func (t *gossipsubTrace) Trace(evt *pubsubpb.TraceEvent) {
	if t == nil || evt == nil {
		return
	}
	switch evt.GetType() {
	case pubsubpb.TraceEvent_REJECT_MESSAGE:
		reject := evt.GetRejectMessage()
		t.writeEvent("rj", string(reject.GetReceivedFrom()), reject.GetTopic(), string(reject.GetMessageID()), reject.GetReason())
	case pubsubpb.TraceEvent_DUPLICATE_MESSAGE:
		dup := evt.GetDuplicateMessage()
		t.writeEvent("du", string(dup.GetReceivedFrom()), dup.GetTopic(), string(dup.GetMessageID()))
	case pubsubpb.TraceEvent_DELIVER_MESSAGE:
		deliver := evt.GetDeliverMessage()
		t.writeEvent("dl", string(deliver.GetReceivedFrom()), deliver.GetTopic(), string(deliver.GetMessageID()))
	case pubsubpb.TraceEvent_ADD_PEER:
		addPeer := evt.GetAddPeer()
		t.writeEvent("pa", string(addPeer.GetPeerID()), addPeer.GetProto())
	case pubsubpb.TraceEvent_REMOVE_PEER:
		removePeer := evt.GetRemovePeer()
		t.writeEvent("pg", string(removePeer.GetPeerID()))
	case pubsubpb.TraceEvent_RECV_RPC:
		recv := evt.GetRecvRPC()
		t.writeRPCMeta("hr", "wr", "nr", string(recv.GetReceivedFrom()), recv.GetMeta())
	case pubsubpb.TraceEvent_JOIN:
		t.writeEvent("tj", evt.GetJoin().GetTopic())
	case pubsubpb.TraceEvent_LEAVE:
		t.writeEvent("tl", evt.GetLeave().GetTopic())
	case pubsubpb.TraceEvent_GRAFT:
		graft := evt.GetGraft()
		t.writeEvent("mg", string(graft.GetPeerID()), graft.GetTopic())
	case pubsubpb.TraceEvent_PRUNE:
		prune := evt.GetPrune()
		t.writeEvent("mp", string(prune.GetPeerID()), prune.GetTopic())
	}
}

func (t *gossipsubTrace) UndeliverableMessage(msg *pubsub.Message) {
	t.writeEvent("ud", msg.GetTopic(), gossipsubMessageID(msg.Data))
}

func (t *gossipsubTrace) writeMessageEvent(code string, from peer.ID, channelID, messageID string, args ...any) {
	if messageID == "" {
		return
	}
	row := []any{from.String(), channelID, messageID}
	row = append(row, args...)
	t.writeEvent(code, row...)
}

func (t *gossipsubTrace) writeRPCMeta(ihaveCode, iwantCode, idontwantCode, peerID string, meta *pubsubpb.TraceEvent_RPCMeta) {
	if meta == nil {
		return
	}
	control := meta.GetControl()
	if control == nil {
		return
	}
	for _, ihave := range control.GetIhave() {
		channel := ihave.GetTopic()
		if channel == "" {
			channel = gossipsubChannelID
		}
		msgIDs := ihave.GetMessageIDs()
		for _, msgID := range msgIDs {
			t.writeEvent(ihaveCode, peerID, channel, string(msgID), len(msgIDs))
		}
	}
	for _, iwant := range control.GetIwant() {
		msgIDs := iwant.GetMessageIDs()
		for _, msgID := range msgIDs {
			t.writeEvent(iwantCode, peerID, gossipsubChannelID, string(msgID), len(msgIDs))
		}
	}
	for _, idontwant := range control.GetIdontwant() {
		msgIDs := idontwant.GetMessageIDs()
		for _, msgID := range msgIDs {
			t.writeEvent(idontwantCode, peerID, gossipsubChannelID, string(msgID), len(msgIDs))
		}
	}
}

func encodeGossipsubMessage(messageID string, data []byte) []byte {
	msg := make([]byte, len(messageID)+1+len(data))
	n := copy(msg, messageID)
	msg[n] = '|'
	copy(msg[n+1:], data)
	return msg
}

func decodeGossipsubMessage(data []byte) (string, []byte, error) {
	messageIDBytes, payload, found := bytes.Cut(data, []byte{'|'})
	if !found {
		return "", nil, errors.New("invalid message: no message id")
	}
	return string(messageIDBytes), payload, nil
}

func gossipsubMessageID(data []byte) string {
	messageID, _, err := decodeGossipsubMessage(data)
	if err != nil {
		return ""
	}
	return messageID
}

func gossipsubMessageIDFromPB(msg *pubsubpb.Message) string {
	if msg == nil {
		return ""
	}
	return gossipsubMessageID(msg.GetData())
}

// NewGossipsubNode creates a new GossipsubNode with QUIC transport.
func NewGossipsubNode(conn net.PacketConn, nodeNum int, logger *slog.Logger, tw *TraceWriter) (Node, error) {
	maddr := multiaddrFromNetAddr(conn.LocalAddr())
	privKey, err := getNodePrivateKey(nodeNum)
	if err != nil {
		return nil, fmt.Errorf("failed to get private key: %w", err)
	}

	h, err := libp2p.New(
		libp2pQUICWithPacketConn(conn),
		libp2p.Identity(privKey),
		libp2p.ListenAddrs(maddr),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.ResourceManager(&network.NullResourceManager{}),
		libp2p.ConnectionManager(&connmgr.NullConnMgr{}),
		libp2p.ForceReachabilityPublic(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	return newGossipsubNode(nodeNum, h, logger, newGossipsubTrace(nodeNum, tw))
}

func newGossipsubNode(nodeNum int, h host.Host, logger *slog.Logger, trace *gossipsubTrace) (*GossipsubNode, error) {
	h.SetStreamHandler("/warmup", func(s network.Stream) {
		handshakeStream(s, warmupBytes, 1)
		s.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	ps, err := newPubSub(ctx, h, trace)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create pubsub: %w", err)
	}

	t, err := ps.Join(gossipsubChannelID)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to join channel: %w", err)
	}

	sub, err := t.Subscribe(pubsub.WithBufferSize(512))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to subscribe: %w", err)
	}

	return &GossipsubNode{
		num:     nodeNum,
		ctx:     ctx,
		cancel:  cancel,
		ps:      ps,
		host:    h,
		channel: t,
		sub:     sub,
		logger:  logger,
		trace:   trace,
	}, nil
}

func newPubSub(ctx context.Context, h host.Host, trace *gossipsubTrace) (*pubsub.PubSub, error) {
	// Matches Prysm beacon-chain/p2p/pubsub.go gossipsub parameters.
	params := pubsub.DefaultGossipSubParams()
	params.D = 8
	params.Dlo = 6
	params.Dhi = 12
	params.Dlazy = 6
	params.HeartbeatInterval = 700 * time.Millisecond
	params.FanoutTTL = 60 * time.Second
	params.HistoryLength = 6
	params.HistoryGossip = 3

	opts := []pubsub.Option{
		pubsub.WithGossipSubParams(params),
		pubsub.WithMaxMessageSize(1 << 30),
		pubsub.WithMessageIdFn(gossipsubMessageIDFromPB),
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithNoAuthor(),
		pubsub.WithPeerOutboundQueueSize(600),
		pubsub.WithValidateQueueSize(600),
	}
	if trace != nil {
		opts = append(opts, pubsub.WithRawTracer(trace))
		opts = append(opts, pubsub.WithEventTracer(trace))
	}
	return pubsub.NewGossipSub(ctx, h, opts...)
}

func (g *GossipsubNode) NodeNum() int {
	return g.num
}

func (g *GossipsubNode) Addr() net.Addr {
	maddrs := g.host.Addrs()
	return netAddrFromMultiaddr(maddrs[0])
}

func (g *GossipsubNode) Start(ctx context.Context) {
	// go-libp2p handles incoming connections automatically
}

func (g *GossipsubNode) Close() error {
	g.cancel()
	g.sub.Cancel()
	g.channel.Close()
	return g.host.Close()
}

func (g *GossipsubNode) Publish(messageID string, data []byte) {
	msg := encodeGossipsubMessage(messageID, data)
	if err := g.channel.Publish(context.Background(), msg); err != nil {
		g.logger.Error("failed to publish message", "err", err)
		return
	}
	g.trace.writeEvent("pb", gossipsubChannelID, messageID, len(msg))
}

func (g *GossipsubNode) Receive(ctx context.Context) (string, []byte, error) {
	msg, err := g.sub.Next(ctx)
	if err != nil {
		return "", nil, err
	}
	return decodeGossipsubMessage(msg.Data)
}

func (g *GossipsubNode) DialPeer(ctx context.Context, nodeNum int, addr net.Addr) error {
	peerID, err := gossipsubPeerID(nodeNum)
	if err != nil {
		return fmt.Errorf("failed to get peer ID: %w", err)
	}
	maddr := multiaddrFromNetAddr(addr)
	if err := g.host.Connect(ctx, peer.AddrInfo{ID: peerID, Addrs: []ma.Multiaddr{maddr}}); err != nil {
		return fmt.Errorf("failed to connect to peer: %w", err)
	}
	s, err := g.host.NewStream(ctx, peerID, "/warmup")
	if err != nil {
		return fmt.Errorf("failed to create warmup stream: %w", err)
	}
	handshakeStream(s, warmupBytes, 1)
	s.Close()
	return nil
}

func (g *GossipsubNode) rawBandwidth() (sent, received int) {
	var qc *quic.Conn
	for _, c := range g.host.Network().Conns() {
		if ok := c.As(&qc); ok {
			s := qc.ConnectionStats()
			sent += int(s.BytesSent)
			received += int(s.BytesReceived)
		}
	}
	return sent, received
}

func (g *GossipsubNode) BandwidthStats() (bytesSent, bytesReceived int) {
	return g.rawBandwidth()
}

func (g *GossipsubNode) ResetBandwidthStats() (bytesSent, bytesReceived int) {
	sent, recv := g.rawBandwidth()
	ds, dr := sent-g.baseSent, recv-g.baseRecv
	g.baseSent, g.baseRecv = sent, recv
	return ds, dr
}

func gossipsubPeerID(nodeNum int) (peer.ID, error) {
	privKey, err := getNodePrivateKey(nodeNum)
	if err != nil {
		return "", err
	}
	return peer.IDFromPrivateKey(privKey)
}

type mockSourceIPSelector struct {
	ip atomic.Pointer[net.IP]
}

func (m *mockSourceIPSelector) PreferredSourceIPForDestination(_ *net.UDPAddr) (net.IP, error) {
	return *m.ip.Load(), nil
}

func libp2pQUICWithPacketConn(conn net.PacketConn) libp2p.Option {
	ca := conn.LocalAddr().(*net.UDPAddr)
	m := &mockSourceIPSelector{}
	m.ip.Store(&ca.IP)
	quicReuseOpts := []quicreuse.Option{
		quicreuse.OverrideSourceIPSelector(func() (quicreuse.SourceIPSelector, error) {
			return m, nil
		}),
		quicreuse.OverrideListenUDP(func(l string, address *net.UDPAddr) (net.PacketConn, error) {
			if ca.IP.Equal(address.IP) && ca.Port == address.Port {
				return conn, nil
			}
			return nil, fmt.Errorf("invalid listen address: %s, wanted: %s", address, ca)
		}),
	}
	return libp2p.QUICReuse(
		func(l fx.Lifecycle, statelessResetKey quic.StatelessResetKey, tokenKey quic.TokenGeneratorKey, opts ...quicreuse.Option) (*quicreuse.ConnManager, error) {
			cm, err := quicreuse.NewConnManager(statelessResetKey, tokenKey, opts...)
			if err != nil {
				return nil, err
			}
			l.Append(fx.StopHook(func() error {
				return cm.Close()
			}))
			return cm, nil
		}, quicReuseOpts...)
}
