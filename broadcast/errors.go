package broadcast

import "errors"

var (
	ErrEngineClosed    = errors.New("engine closed")
	ErrChannelExists   = errors.New("channel already exists")
	ErrChannelNotFound = errors.New("channel not found")
	ErrPeerExists      = errors.New("peer already exists")
	ErrPeerNotFound    = errors.New("peer not found")
	ErrInvalidMessage  = errors.New("invalid message")

	ErrProtocolMismatch  = errors.New("protocol version mismatch")
	ErrUnexpectedMsgType = errors.New("unexpected message type")

	ErrAlreadySubscribed      = errors.New("channel already has a subscriber")
	ErrUnbufferedSubscription = errors.New("subscriber channel must be buffered")

	// ChunkSent errors reported by the session to the strategy.
	ErrChunkMarshal   = errors.New("chunk ID marshal failed")
	ErrChunkPeerGone  = errors.New("peer not attached or data length mismatch")
	ErrChunkSlotFull  = errors.New("outbound chunk slot full")
	ErrChunkWriteFail = errors.New("chunk write to peer failed")
	ErrChunkCancelled = errors.New("chunk send cancelled")
	ErrSessionClosing = errors.New("session closing")
)

type ChunkProcessError struct {
	Peer      PeerID
	ChannelID ChannelID
	MessageID MessageID
	Err       error
}
