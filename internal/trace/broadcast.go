package trace

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// ChannelAttached records the result of registering a broadcast channel.
type ChannelAttached struct {
	Channel string
	Err     error
}

// ChannelDropped records removal of a broadcast channel.
type ChannelDropped struct{ Channel string }

// PeerHandshook records the authenticated peer and its advertised channels.
type PeerHandshook struct {
	Peer string
	// Channels is the channel list advertised during the handshake.
	Channels []string
}

// PeerSubscribed records a peer's current subscription to a channel.
type PeerSubscribed struct{ Peer, Channel string }

// PeerUnsubscribed records removal of a peer's channel subscription.
type PeerUnsubscribed struct{ Peer, Channel string }

// PeerGone records the end of a peer's broadcast binding. Err is its cause:
// nil for an ordinary end, else a handshake failure or a protocol violation.
type PeerGone struct {
	Peer string
	Err  error
}

// SessionStarted records creation of an origin or relay session.
type SessionStarted struct{ Channel, Message, Role string }

// SessionDecoded records a reconstructed message.
type SessionDecoded struct{ Channel, Message string }

// SessionDisposed records the reason a session ended.
type SessionDisposed struct{ Channel, Message, Reason string }

// ChunkSent records bytes sent to a peer for a message.
type ChunkSent struct {
	Peer, Channel, Message string
	BytesSent              int
}

// ChunkRcvd records the verdict for a peer's chunk.
type ChunkRcvd struct{ Peer, Channel, Message, Verdict string }

// ChunkError records a failure while processing a peer's chunk.
type ChunkError struct {
	Peer, Channel, Message string
	Err                    error
}

// RoutingUpdate records a peer's routing update for a message.
type RoutingUpdate struct{ Peer, Channel, Message string }

// PreambleOpened records opening a peer's message preamble.
type PreambleOpened struct{ Peer, Channel, Message string }

// StrategyProgress records how many chunks a strategy has and needs.
type StrategyProgress struct {
	Channel, Message       string
	ChunksHave, ChunksNeed int
}

// peerLabel makes raw peer IDs safe for a one-line trace. The recorder may
// replace this short label with a harness node name in its timeline.
func peerLabel(peer string) string {
	encoded := hex.EncodeToString([]byte(peer))
	if len(encoded) > 8 {
		return encoded[:8]
	}
	return encoded
}

func oneLine(format string, args ...any) string {
	return strings.NewReplacer("\n", `\n`, "\r", `\r`, "\t", `\t`).Replace(fmt.Sprintf(format, args...))
}

func (ChannelAttached) traceEvent()  {}
func (ChannelDropped) traceEvent()   {}
func (PeerHandshook) traceEvent()    {}
func (PeerSubscribed) traceEvent()   {}
func (PeerUnsubscribed) traceEvent() {}
func (PeerGone) traceEvent()         {}
func (SessionStarted) traceEvent()   {}
func (SessionDecoded) traceEvent()   {}
func (SessionDisposed) traceEvent()  {}
func (ChunkSent) traceEvent()        {}
func (ChunkRcvd) traceEvent()        {}
func (ChunkError) traceEvent()       {}
func (RoutingUpdate) traceEvent()    {}
func (PreambleOpened) traceEvent()   {}
func (StrategyProgress) traceEvent() {}

// String formats the event on one line for failure timelines.
func (e ChannelAttached) String() string {
	return oneLine("ChannelAttached ch=%s err=%v", e.Channel, e.Err)
}

// String formats the event on one line for failure timelines.
func (e ChannelDropped) String() string { return oneLine("ChannelDropped ch=%s", e.Channel) }

// String formats the event on one line for failure timelines.
func (e PeerHandshook) String() string {
	return oneLine("PeerHandshook peer=%s channels=%q", peerLabel(e.Peer), e.Channels)
}

// String formats the event on one line for failure timelines.
func (e PeerSubscribed) String() string {
	return oneLine("PeerSubscribed peer=%s ch=%s", peerLabel(e.Peer), e.Channel)
}

// String formats the event on one line for failure timelines.
func (e PeerUnsubscribed) String() string {
	return oneLine("PeerUnsubscribed peer=%s ch=%s", peerLabel(e.Peer), e.Channel)
}

// String formats the event on one line for failure timelines.
func (e PeerGone) String() string {
	if e.Err != nil {
		return oneLine("PeerGone peer=%s err=%v", peerLabel(e.Peer), e.Err)
	}
	return oneLine("PeerGone peer=%s", peerLabel(e.Peer))
}

// String formats the event on one line for failure timelines.
func (e SessionStarted) String() string {
	return oneLine("SessionStarted ch=%s msg=%s role=%s", e.Channel, e.Message, e.Role)
}

// String formats the event on one line for failure timelines.
func (e SessionDecoded) String() string {
	return oneLine("SessionDecoded ch=%s msg=%s", e.Channel, e.Message)
}

// String formats the event on one line for failure timelines.
func (e SessionDisposed) String() string {
	return oneLine("SessionDisposed ch=%s msg=%s reason=%s", e.Channel, e.Message, e.Reason)
}

// String formats the event on one line for failure timelines.
func (e ChunkSent) String() string {
	return oneLine("ChunkSent peer=%s ch=%s msg=%s bytes=%d", peerLabel(e.Peer), e.Channel, e.Message, e.BytesSent)
}

// String formats the event on one line for failure timelines.
func (e ChunkRcvd) String() string {
	return oneLine("ChunkRcvd peer=%s ch=%s msg=%s verdict=%s", peerLabel(e.Peer), e.Channel, e.Message, e.Verdict)
}

// String formats the event on one line for failure timelines.
func (e ChunkError) String() string {
	return oneLine("ChunkError peer=%s ch=%s msg=%s err=%v", peerLabel(e.Peer), e.Channel, e.Message, e.Err)
}

// String formats the event on one line for failure timelines.
func (e RoutingUpdate) String() string {
	return oneLine("RoutingUpdate peer=%s ch=%s msg=%s", peerLabel(e.Peer), e.Channel, e.Message)
}

// String formats the event on one line for failure timelines.
func (e PreambleOpened) String() string {
	return oneLine("PreambleOpened peer=%s ch=%s msg=%s", peerLabel(e.Peer), e.Channel, e.Message)
}

// String formats the event on one line for failure timelines.
func (e StrategyProgress) String() string {
	return oneLine("StrategyProgress ch=%s msg=%s have=%d need=%d", e.Channel, e.Message, e.ChunksHave, e.ChunksNeed)
}
