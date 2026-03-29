package broadcast

import "time"

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
type Observer interface {
	// Channel lifecycle
	OnChannelAttached(channelID ChannelID, err error)
	OnChannelDropped(channelID ChannelID)

	// Peer lifecycle
	OnPeerHandshook(peerID PeerID, version ProtocolVersion, channels []ChannelID)
	OnPeerSubscribed(peerID PeerID, channelID ChannelID)
	OnPeerUnsubscribed(peerID PeerID, channelID ChannelID)
	OnPeerGone(peerID PeerID)

	// Session lifecycle
	OnSessionStarted(channelID ChannelID, messageID MessageID, role SessionRole)
	OnSessionDecoded(channelID ChannelID, messageID MessageID, latency time.Duration)
	OnSessionDisposed(channelID ChannelID, messageID MessageID, reason string)

	// Chunk events
	OnChunkSent(peer PeerID, channelID ChannelID, messageID MessageID, bytesSent int)
	OnChunkRcvd(peer PeerID, channelID ChannelID, messageID MessageID, verdict Verdict)
	OnChunkError(err ChunkProcessError)

	// Routing and strategy progress events
	OnRoutingUpdate(peer PeerID, channelID ChannelID, messageID MessageID)
	OnPreambleOpened(peer PeerID, channelID ChannelID, messageID MessageID)
	OnStrategyProgress(channelID ChannelID, messageID MessageID, chunksHave, chunksNeed int)
}

// NoOpObserver is a no-op implementation of Observer.
type NoOpObserver struct{}

func (NoOpObserver) OnChannelAttached(ChannelID, error)                   {}
func (NoOpObserver) OnChannelDropped(ChannelID)                           {}
func (NoOpObserver) OnPeerHandshook(PeerID, ProtocolVersion, []ChannelID) {}
func (NoOpObserver) OnPeerSubscribed(PeerID, ChannelID)                   {}
func (NoOpObserver) OnPeerUnsubscribed(PeerID, ChannelID)                 {}
func (NoOpObserver) OnPeerGone(PeerID)                                    {}
func (NoOpObserver) OnSessionStarted(ChannelID, MessageID, SessionRole)   {}
func (NoOpObserver) OnSessionDecoded(ChannelID, MessageID, time.Duration) {}
func (NoOpObserver) OnSessionDisposed(ChannelID, MessageID, string)       {}
func (NoOpObserver) OnChunkSent(PeerID, ChannelID, MessageID, int)        {}
func (NoOpObserver) OnChunkRcvd(PeerID, ChannelID, MessageID, Verdict)    {}
func (NoOpObserver) OnChunkError(ChunkProcessError)                       {}
func (NoOpObserver) OnRoutingUpdate(PeerID, ChannelID, MessageID)         {}
func (NoOpObserver) OnPreambleOpened(PeerID, ChannelID, MessageID)        {}
func (NoOpObserver) OnStrategyProgress(ChannelID, MessageID, int, int)    {}
