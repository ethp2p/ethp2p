package broadcast

import (
	"errors"

	"github.com/ethp2p/ethp2p/identity"
)

var (
	ErrEngineClosed  = errors.New("engine closed")
	ErrChannelExists = errors.New("channel already exists")

	ErrProtocolMismatch = errors.New("protocol version mismatch")

	ErrAlreadySubscribed      = errors.New("channel already has a subscriber")
	ErrUnbufferedSubscription = errors.New("subscriber channel must be buffered")

	// ChunkSent errors reported by the session to the strategy.
	ErrChunkMarshal   = errors.New("chunk ID marshal failed")
	ErrChunkPeerGone  = errors.New("peer not attached or data length mismatch")
	ErrChunkSlotFull  = errors.New("outbound chunk slot full")
	ErrChunkWriteFail = errors.New("chunk write to peer failed")
	ErrChunkCancelled = errors.New("chunk send cancelled")
)

type ChunkProcessError struct {
	Peer      identity.PeerID
	ChannelID ChannelID
	MessageID MessageID
	Err       error
}
