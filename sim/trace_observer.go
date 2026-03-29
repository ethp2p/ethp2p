package sim

import (
	"time"

	"github.com/ethp2p/ethp2p/broadcast"
)

// TracingObserver implements broadcast.Observer by writing compact
// event tuples to a shared TraceWriter while also tracking stats
// via an embedded Observer.
type TracingObserver struct {
	*Observer
	node int
	tw   *TraceWriter
}

func NewTracingObserver(nodeIdx int, tw *TraceWriter) *TracingObserver {
	return &TracingObserver{Observer: NewObserver(), node: nodeIdx, tw: tw}
}

func (o *TracingObserver) OnPeerHandshook(peer broadcast.PeerID, version broadcast.ProtocolVersion, channels []broadcast.ChannelID) {
	o.tw.WriteEvent(time.Now(), o.node, "ph", peer, version, channels)
}

func (o *TracingObserver) OnPeerSubscribed(peer broadcast.PeerID, channelID broadcast.ChannelID) {
	o.tw.WriteEvent(time.Now(), o.node, "ps", peer, channelID)
}

func (o *TracingObserver) OnPeerUnsubscribed(peer broadcast.PeerID, channelID broadcast.ChannelID) {
	o.tw.WriteEvent(time.Now(), o.node, "pu", peer, channelID)
}

func (o *TracingObserver) OnPeerGone(peer broadcast.PeerID) {
	o.tw.WriteEvent(time.Now(), o.node, "pg", peer)
}

func (o *TracingObserver) OnSessionStarted(channelID broadcast.ChannelID, messageID broadcast.MessageID, role broadcast.SessionRole) {
	o.Observer.OnSessionStarted(channelID, messageID, role)
	o.tw.WriteEvent(time.Now(), o.node, "ss", channelID, messageID, int(role))
}

func (o *TracingObserver) OnSessionDecoded(channelID broadcast.ChannelID, messageID broadcast.MessageID, latency time.Duration) {
	o.tw.WriteEvent(time.Now(), o.node, "sd", channelID, messageID, latency.Microseconds())
}

func (o *TracingObserver) OnSessionDisposed(channelID broadcast.ChannelID, messageID broadcast.MessageID, reason string) {
	o.tw.WriteEvent(time.Now(), o.node, "sx", channelID, messageID, reason)
}

func (o *TracingObserver) OnChunkSent(peer broadcast.PeerID, channelID broadcast.ChannelID, messageID broadcast.MessageID, bytesSent int) {
	o.Observer.OnChunkSent(peer, channelID, messageID, bytesSent)
	o.tw.WriteEvent(time.Now(), o.node, "cs", peer, channelID, messageID, bytesSent)
}

func (o *TracingObserver) OnChunkRcvd(peer broadcast.PeerID, channelID broadcast.ChannelID, messageID broadcast.MessageID, verdict broadcast.Verdict) {
	o.Observer.OnChunkRcvd(peer, channelID, messageID, verdict)
	o.tw.WriteEvent(time.Now(), o.node, "cr", peer, channelID, messageID, int(verdict))
}

func (o *TracingObserver) OnChunkError(err broadcast.ChunkProcessError) {
	o.tw.WriteEvent(time.Now(), o.node, "ce", err.ChannelID, err.MessageID, err.Err.Error())
}

func (o *TracingObserver) OnRoutingUpdate(peer broadcast.PeerID, channelID broadcast.ChannelID, messageID broadcast.MessageID) {
	o.tw.WriteEvent(time.Now(), o.node, "ru", peer, channelID, messageID)
}

func (o *TracingObserver) OnPreambleOpened(peer broadcast.PeerID, channelID broadcast.ChannelID, messageID broadcast.MessageID) {
	o.tw.WriteEvent(time.Now(), o.node, "po", peer, channelID, messageID)
}

func (o *TracingObserver) OnStrategyProgress(channelID broadcast.ChannelID, messageID broadcast.MessageID, chunksHave, chunksNeed int) {
	o.tw.WriteEvent(time.Now(), o.node, "sp", channelID, messageID, chunksHave, chunksNeed)
}
