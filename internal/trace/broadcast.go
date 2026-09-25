package trace

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"
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
	// Version is the broadcast protocol version negotiated with the peer.
	Version uint
	// Channels is the channel list advertised during the handshake.
	Channels []string
}

// PeerSubscribed records a peer's current subscription to a channel.
type PeerSubscribed struct{ Peer, Channel string }

// PeerUnsubscribed records removal of a peer's channel subscription.
type PeerUnsubscribed struct{ Peer, Channel string }

// PeerGone records loss of an authenticated peer.
type PeerGone struct{ Peer string }

// SessionStarted records creation of an origin or relay session.
type SessionStarted struct{ Channel, Message, Role string }

// SessionDecoded records a reconstructed message and decode latency.
type SessionDecoded struct {
	Channel, Message string
	Latency          time.Duration
}

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
	return oneLine("PeerHandshook peer=%s version=%d channels=%q", peerLabel(e.Peer), e.Version, e.Channels)
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
func (e PeerGone) String() string { return oneLine("PeerGone peer=%s", peerLabel(e.Peer)) }

// String formats the event on one line for failure timelines.
func (e SessionStarted) String() string {
	return oneLine("SessionStarted ch=%s msg=%s role=%s", e.Channel, e.Message, e.Role)
}

// String formats the event on one line for failure timelines.
func (e SessionDecoded) String() string {
	return oneLine("SessionDecoded ch=%s msg=%s latency=%s", e.Channel, e.Message, e.Latency)
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

// Reasons shared by broadcast emitters and E2E assertions. Rejections occur
// before a chunk's data is read; cancellations stop a read or its context.
const (
	// RejectOrigin means the stream came from the original message source.
	RejectOrigin = "origin"
	// RejectHaveChunk means this peer already supplied the same chunk.
	RejectHaveChunk = "have-chunk"
	// RejectComplete means the session no longer accepts chunks.
	RejectComplete = "complete"
	// CancelDedup means another stream completed the same chunk first.
	CancelDedup = "dedup"
	// CancelSessionClosed means the session ended during a read.
	CancelSessionClosed = "session-closed"
	// CancelBindClosed means the peer binding closed during a read.
	CancelBindClosed = "bind-closed"
)

// DispatchSlot records a chunk dispatch acquiring or releasing a peer slot.
type DispatchSlot struct {
	Peer, Channel, Message string
	Acquired               bool // true on acquisition, false on release
}

// SessionStage records a session lifecycle transition named by a Stage constant.
type SessionStage struct{ Peer, Channel, Message, Stage string }

// Session lifecycle transitions reported by SessionStage.
const (
	// StageDecoding means the relay has enough chunks and started decoding.
	StageDecoding = "decoding"
	// StageReconstructed means decoding succeeded.
	StageReconstructed = "reconstructed"
	// StagePeerCompleted means a peer reported it reconstructed the message.
	StagePeerCompleted = "peer-completed"
)

// StrategyClosed records the completion of a strategy's Close call.
type StrategyClosed struct{ Channel, Message string }

// ChunkSendResult records the outcome reported to Strategy.ChunkSent.
type ChunkSendResult struct {
	Peer, Channel, Message string
	Err                    error // outcome passed to Strategy.ChunkSent; nil on success
}

// ChunkHandled records processing of one chunk after its data has been read.
// Chunk identifies the chunk within the message.
type ChunkHandled struct{ Peer, Channel, Message, Chunk string }

// InboundStreamRejected records a CHUNK stream declined before reading data.
// Reason is one of the Reject constants above.
type InboundStreamRejected struct{ Peer, Channel, Message, Reason string }

// ChunkReadCancelled records cancellation of a chunk read context or stream.
// Reason is one of the Cancel constants above.
type ChunkReadCancelled struct{ Peer, Channel, Message, Reason string }

// ChunkParked records a CHUNK stream received before its session opened.
type ChunkParked struct{ Peer, Channel, Message string }

// ParkedChunksDropped records cleanup of parked CHUNK streams.
type ParkedChunksDropped struct {
	Channel, Message string
	Count            int // number of parked streams closed
}

// GateHeld records a component blocked at a development gate. Point is the
// string form of a devhook.Point; the other fields identify the crossing.
type GateHeld struct{ Point, Peer, Channel, Message string }

// GateReleased records a component leaving a development gate.
type GateReleased struct{ Point, Peer, Channel, Message string }

func (DispatchSlot) traceEvent()          {}
func (SessionStage) traceEvent()          {}
func (StrategyClosed) traceEvent()        {}
func (ChunkSendResult) traceEvent()       {}
func (ChunkHandled) traceEvent()          {}
func (InboundStreamRejected) traceEvent() {}
func (ChunkReadCancelled) traceEvent()    {}
func (ChunkParked) traceEvent()           {}
func (ParkedChunksDropped) traceEvent()   {}
func (GateHeld) traceEvent()              {}
func (GateReleased) traceEvent()          {}

// String formats the event on one line for failure timelines.
func (e DispatchSlot) String() string {
	return oneLine("DispatchSlot peer=%s ch=%s msg=%s acquired=%t", peerLabel(e.Peer), e.Channel, e.Message, e.Acquired)
}

// String formats the event on one line for failure timelines.
func (e SessionStage) String() string {
	return oneLine("SessionStage peer=%s ch=%s msg=%s stage=%s", peerLabel(e.Peer), e.Channel, e.Message, e.Stage)
}

// String formats the event on one line for failure timelines.
func (e StrategyClosed) String() string {
	return oneLine("StrategyClosed ch=%s msg=%s", e.Channel, e.Message)
}

// String formats the event on one line for failure timelines.
func (e ChunkSendResult) String() string {
	return oneLine("ChunkSendResult peer=%s ch=%s msg=%s err=%v", peerLabel(e.Peer), e.Channel, e.Message, e.Err)
}

// String formats the event on one line for failure timelines.
func (e ChunkHandled) String() string {
	return oneLine("ChunkHandled peer=%s ch=%s msg=%s chunk=%s", peerLabel(e.Peer), e.Channel, e.Message, e.Chunk)
}

// String formats the event on one line for failure timelines.
func (e InboundStreamRejected) String() string {
	return oneLine("InboundStreamRejected peer=%s ch=%s msg=%s reason=%s", peerLabel(e.Peer), e.Channel, e.Message, e.Reason)
}

// String formats the event on one line for failure timelines.
func (e ChunkReadCancelled) String() string {
	return oneLine("ChunkReadCancelled peer=%s ch=%s msg=%s reason=%s", peerLabel(e.Peer), e.Channel, e.Message, e.Reason)
}

// String formats the event on one line for failure timelines.
func (e ChunkParked) String() string {
	return oneLine("ChunkParked peer=%s ch=%s msg=%s", peerLabel(e.Peer), e.Channel, e.Message)
}

// String formats the event on one line for failure timelines.
func (e ParkedChunksDropped) String() string {
	return oneLine("ParkedChunksDropped ch=%s msg=%s count=%d", e.Channel, e.Message, e.Count)
}

// String formats the event on one line for failure timelines.
func (e GateHeld) String() string {
	return oneLine("GateHeld point=%s peer=%s ch=%s msg=%s", e.Point, peerLabel(e.Peer), e.Channel, e.Message)
}

// String formats the event on one line for failure timelines.
func (e GateReleased) String() string {
	return oneLine("GateReleased point=%s peer=%s ch=%s msg=%s", e.Point, peerLabel(e.Peer), e.Channel, e.Message)
}
