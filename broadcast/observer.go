package broadcast

import (
	"github.com/ethp2p/ethp2p/identity"
)

// SessionRole distinguishes origin sessions (the publisher) from relay
// sessions (nodes forwarding chunks they received from others).
type SessionRole uint8

const (
	SessionRoleOrigin SessionRole = iota
	SessionRoleRelay
)

// Observer is the observer interface for broadcast system events.
// Strategy-specific typed observations (e.g. chunk indices, generations)
// are handled by strategy-internal observers.
// Callbacks run inline on engine, channel or peer goroutines. They must return
// promptly and must not wait for work on those goroutines (including reentrant
// synchronous broadcast calls), or they can prevent inboxes from draining.
type Observer interface {
	OnChannelAttached(channelID ChannelID, err error)
	OnChannelDropped(channelID ChannelID)

	OnPeerHandshook(peerID identity.PeerID, channels []ChannelID)
	OnPeerSubscribed(peerID identity.PeerID, channelID ChannelID)
	OnPeerUnsubscribed(peerID identity.PeerID, channelID ChannelID)
	OnPeerGone(peerID identity.PeerID)

	OnSessionStarted(channelID ChannelID, messageID MessageID, role SessionRole)
	OnSessionDecoded(channelID ChannelID, messageID MessageID)
	OnSessionDisposed(channelID ChannelID, messageID MessageID, reason string)

	OnChunkSent(peer identity.PeerID, channelID ChannelID, messageID MessageID, bytesSent int)
	OnChunkRcvd(peer identity.PeerID, channelID ChannelID, messageID MessageID, verdict Verdict)
	OnChunkError(err ChunkProcessError)

	OnRoutingUpdate(peer identity.PeerID, channelID ChannelID, messageID MessageID)
	OnPreambleOpened(peer identity.PeerID, channelID ChannelID, messageID MessageID)
	OnStrategyProgress(channelID ChannelID, messageID MessageID, chunksHave, chunksNeed int)
}

// NoOpObserver is a no-op implementation of Observer.
type NoOpObserver struct{}

func (NoOpObserver) OnChannelAttached(ChannelID, error)                         {}
func (NoOpObserver) OnChannelDropped(ChannelID)                                 {}
func (NoOpObserver) OnPeerHandshook(identity.PeerID, []ChannelID)               {}
func (NoOpObserver) OnPeerSubscribed(identity.PeerID, ChannelID)                {}
func (NoOpObserver) OnPeerUnsubscribed(identity.PeerID, ChannelID)              {}
func (NoOpObserver) OnPeerGone(identity.PeerID)                                 {}
func (NoOpObserver) OnSessionStarted(ChannelID, MessageID, SessionRole)         {}
func (NoOpObserver) OnSessionDecoded(ChannelID, MessageID)                      {}
func (NoOpObserver) OnSessionDisposed(ChannelID, MessageID, string)             {}
func (NoOpObserver) OnChunkSent(identity.PeerID, ChannelID, MessageID, int)     {}
func (NoOpObserver) OnChunkRcvd(identity.PeerID, ChannelID, MessageID, Verdict) {}
func (NoOpObserver) OnChunkError(ChunkProcessError)                             {}
func (NoOpObserver) OnRoutingUpdate(identity.PeerID, ChannelID, MessageID)      {}
func (NoOpObserver) OnPreambleOpened(identity.PeerID, ChannelID, MessageID)     {}
func (NoOpObserver) OnStrategyProgress(ChannelID, MessageID, int, int)          {}
