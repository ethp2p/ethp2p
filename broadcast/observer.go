package broadcast

import (
	"time"

	"github.com/ethp2p/ethp2p/transport"
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
	// Channel lifecycle
	OnChannelAttached(channelID ChannelID, err error)
	OnChannelDropped(channelID ChannelID)

	// Peer lifecycle
	OnPeerHandshook(peerID transport.PeerID, version ProtocolVersion, channels []ChannelID)
	OnPeerSubscribed(peerID transport.PeerID, channelID ChannelID)
	OnPeerUnsubscribed(peerID transport.PeerID, channelID ChannelID)
	OnPeerGone(peerID transport.PeerID)

	// Session lifecycle
	OnSessionStarted(channelID ChannelID, messageID MessageID, role SessionRole)
	OnSessionDecoded(channelID ChannelID, messageID MessageID, latency time.Duration)
	OnSessionDisposed(channelID ChannelID, messageID MessageID, reason string)

	// Chunk events
	OnChunkSent(peer transport.PeerID, channelID ChannelID, messageID MessageID, bytesSent int)
	OnChunkRcvd(peer transport.PeerID, channelID ChannelID, messageID MessageID, verdict Verdict)
	OnChunkError(err ChunkProcessError)

	// Routing and strategy progress events
	OnRoutingUpdate(peer transport.PeerID, channelID ChannelID, messageID MessageID)
	OnPreambleOpened(peer transport.PeerID, channelID ChannelID, messageID MessageID)
	OnStrategyProgress(channelID ChannelID, messageID MessageID, chunksHave, chunksNeed int)
}

// NoOpObserver is a no-op implementation of Observer.
type NoOpObserver struct{}

func (NoOpObserver) OnChannelAttached(ChannelID, error)                             {}
func (NoOpObserver) OnChannelDropped(ChannelID)                                     {}
func (NoOpObserver) OnPeerHandshook(transport.PeerID, ProtocolVersion, []ChannelID) {}
func (NoOpObserver) OnPeerSubscribed(transport.PeerID, ChannelID)                   {}
func (NoOpObserver) OnPeerUnsubscribed(transport.PeerID, ChannelID)                 {}
func (NoOpObserver) OnPeerGone(transport.PeerID)                                    {}
func (NoOpObserver) OnSessionStarted(ChannelID, MessageID, SessionRole)             {}
func (NoOpObserver) OnSessionDecoded(ChannelID, MessageID, time.Duration)           {}
func (NoOpObserver) OnSessionDisposed(ChannelID, MessageID, string)                 {}
func (NoOpObserver) OnChunkSent(transport.PeerID, ChannelID, MessageID, int)        {}
func (NoOpObserver) OnChunkRcvd(transport.PeerID, ChannelID, MessageID, Verdict)    {}
func (NoOpObserver) OnChunkError(ChunkProcessError)                                 {}
func (NoOpObserver) OnRoutingUpdate(transport.PeerID, ChannelID, MessageID)         {}
func (NoOpObserver) OnPreambleOpened(transport.PeerID, ChannelID, MessageID)        {}
func (NoOpObserver) OnStrategyProgress(ChannelID, MessageID, int, int)              {}
