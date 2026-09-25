package nettest

import (
	"fmt"
	"slices"
	"time"

	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/internal/trace"
	"github.com/ethp2p/ethp2p/transport"
)

type observer struct {
	name     string
	recorder *Recorder
}

func (o observer) emit(ev trace.Event) { o.recorder.emit(o.name, ev) }
func (o observer) OnChannelAttached(c broadcast.ChannelID, err error) {
	o.emit(trace.ChannelAttached{Channel: string(c), Err: err})
}
func (o observer) OnChannelDropped(c broadcast.ChannelID) {
	o.emit(trace.ChannelDropped{Channel: string(c)})
}
func (o observer) OnPeerHandshook(p transport.PeerID, v broadcast.ProtocolVersion, cs []broadcast.ChannelID) {
	channels := make([]string, len(cs))
	for i, c := range cs {
		channels[i] = string(c)
	}
	slices.Sort(channels)
	o.emit(trace.PeerHandshook{Peer: string(p), Version: uint(v), Channels: channels})
}
func (o observer) OnPeerSubscribed(p transport.PeerID, c broadcast.ChannelID) {
	o.emit(trace.PeerSubscribed{Peer: string(p), Channel: string(c)})
}
func (o observer) OnPeerUnsubscribed(p transport.PeerID, c broadcast.ChannelID) {
	o.emit(trace.PeerUnsubscribed{Peer: string(p), Channel: string(c)})
}
func (o observer) OnPeerGone(p transport.PeerID) { o.emit(trace.PeerGone{Peer: string(p)}) }
func (o observer) OnSessionStarted(c broadcast.ChannelID, m broadcast.MessageID, role broadcast.SessionRole) {
	r := "origin"
	if role == broadcast.SessionRoleRelay {
		r = "relay"
	}
	o.emit(trace.SessionStarted{Channel: string(c), Message: string(m), Role: r})
}
func (o observer) OnSessionDecoded(c broadcast.ChannelID, m broadcast.MessageID, d time.Duration) {
	o.emit(trace.SessionDecoded{Channel: string(c), Message: string(m), Latency: d})
}
func (o observer) OnSessionDisposed(c broadcast.ChannelID, m broadcast.MessageID, reason string) {
	o.emit(trace.SessionDisposed{Channel: string(c), Message: string(m), Reason: reason})
}
func (o observer) OnChunkSent(p transport.PeerID, c broadcast.ChannelID, m broadcast.MessageID, bytes int) {
	o.emit(trace.ChunkSent{Peer: string(p), Channel: string(c), Message: string(m), BytesSent: bytes})
}
func (o observer) OnChunkRcvd(p transport.PeerID, c broadcast.ChannelID, m broadcast.MessageID, v broadcast.Verdict) {
	verdict := [...]string{"accepted", "redundant", "decoding", "surplus", "invalid", "pending"}
	s := fmt.Sprintf("%d", v)
	if int(v) < len(verdict) {
		s = verdict[v]
	}
	o.emit(trace.ChunkRcvd{Peer: string(p), Channel: string(c), Message: string(m), Verdict: s})
}
func (o observer) OnChunkError(e broadcast.ChunkProcessError) {
	o.emit(trace.ChunkError{Peer: string(e.Peer), Channel: string(e.ChannelID), Message: string(e.MessageID), Err: e.Err})
}
func (o observer) OnRoutingUpdate(p transport.PeerID, c broadcast.ChannelID, m broadcast.MessageID) {
	o.emit(trace.RoutingUpdate{Peer: string(p), Channel: string(c), Message: string(m)})
}
func (o observer) OnPreambleOpened(p transport.PeerID, c broadcast.ChannelID, m broadcast.MessageID) {
	o.emit(trace.PreambleOpened{Peer: string(p), Channel: string(c), Message: string(m)})
}
func (o observer) OnStrategyProgress(c broadcast.ChannelID, m broadcast.MessageID, have, need int) {
	o.emit(trace.StrategyProgress{Channel: string(c), Message: string(m), ChunksHave: have, ChunksNeed: need})
}
