package broadcast

import (
	"context"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"

	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/identity"
)

// channelEvent is the marker interface for events sent to a Channel inbox.
type channelEvent interface {
	channelEvent()
}

// parkedGroup is one message's parked chunk streams and the first chunk's
// arrival time, which bounds the group's TTL.
type parkedGroup struct {
	at      time.Time
	streams []channelChunkStream
}

// channelChunkStream represents an inbound chunk stream. The frame carries
// the wire metadata; the stream holds the unread payload.

type channelChunkStream struct {
	peerID identity.PeerID
	frame  *bcastpb.Chunk_Header
	stream ethp2p.ReceiveStream
}

// channelChunkData is posted by a read goroutine (spawned by session) after
// reading chunk data from a stream. Routes back through the channel inbox.
type channelChunkData struct {
	messageID MessageID
	peerID    identity.PeerID
	chunkID   []byte
	payload   []byte
}

// channelReadDone wakes the actor after a payload reader frees its slot.
// A late completion may wake a replacement session; drainReads is idempotent.
type channelReadDone struct {
	messageID MessageID
}

func (channelReadDone) channelEvent() {}

// channelExpiry is posted by a session's or parked group's deadline timer.
type channelExpiry struct{}

func (channelExpiry) channelEvent() {}

type channelSessionOpen struct {
	peerID   identity.PeerID
	msg      *bcastpb.Sess_Open
	preamble []byte
}

// channelPeerChange notifies the channel of a topology mutation.
// peerRef != nil means joined; nil means left.
type channelPeerChange struct {
	peerID  identity.PeerID
	peerRef *PeerConn // non-nil = joined, nil = left
}

// channelPeerReconstructed is sent by PeerConn when a remote peer signals
// session close (nil bitmap routing update).
type channelPeerReconstructed struct {
	messageID MessageID
	peerID    identity.PeerID
}

// channelPeerSessionEnded is sent by PeerConn when a peer's inbound session
// stream for a message ends for any reason other than Reconstructed (clean
// close, other reset, or read error) while the connection is still live. The
// peer leaves that session only. peer identifies the connection that owned the
// stream. The peer is not re-attached if it later opens the message again;
// its initial_update is still applied.
type channelPeerSessionEnded struct {
	messageID MessageID
	peerID    identity.PeerID
	peer      *PeerConn
}

type channelPublish[CI ChunkIdent, R Wire] struct {
	messageID    MessageID
	strategy     Strategy[CI, R]
	preambleData []byte // pre-marshalled preamble
	errCh        chan error
}

type channelChunkSent struct {
	messageID MessageID
	peerID    identity.PeerID
	handle    ChunkHandle
	err       error
	size      int
}

type channelRoutingUpdate struct {
	peerID    identity.PeerID
	messageID MessageID
	msg       *bcastpb.Sess_Update
}

type channelVerifyResult struct {
	messageID MessageID
	peerID    identity.PeerID
	chunkID   []byte
	payload   []byte
	verdict   Verdict
}

type channelDecoded struct {
	messageID MessageID
	payload   []byte
	err       error
}

func (channelChunkStream) channelEvent()       {}
func (channelChunkData) channelEvent()         {}
func (channelSessionOpen) channelEvent()       {}
func (channelPeerChange) channelEvent()        {}
func (channelChunkSent) channelEvent()         {}
func (channelRoutingUpdate) channelEvent()     {}
func (channelPeerReconstructed) channelEvent() {}
func (channelPeerSessionEnded) channelEvent()  {}
func (channelVerifyResult) channelEvent()      {}
func (channelDecoded) channelEvent()           {}
func (channelPublish[CI, R]) channelEvent()    {}

// peerCtrlEvent is a control event from Session to PeerConn's outbound loop.
type peerCtrlEvent interface {
	peerCtrlEvent()
}

type peerOpenSession struct {
	channelID      ChannelID
	messageID      MessageID
	preamble       []byte
	initialRouting []byte // embedded in SessionOpen proto message
	chunkOutbox    <-chan peerSendChunk
}

type peerSubscribe struct {
	channelID ChannelID
}

type peerUnsubscribe struct {
	channelID ChannelID
}

type peerSendRouting struct {
	channelID ChannelID
	messageID MessageID
	update    []byte
}

type peerCloseSession struct {
	channelID ChannelID
	messageID MessageID
}

// peerCloseStream tells PeerConn to close the outbound session
// stream (signaling we no longer need chunks) but keep the chunk slot
// alive so we can continue serving chunks to this peer.
type peerCloseStream struct {
	channelID ChannelID
	messageID MessageID
}

func (peerOpenSession) peerCtrlEvent()  {}
func (peerSendRouting) peerCtrlEvent()  {}
func (peerSubscribe) peerCtrlEvent()    {}
func (peerUnsubscribe) peerCtrlEvent()  {}
func (peerCloseSession) peerCtrlEvent() {}
func (peerCloseStream) peerCtrlEvent()  {}

// peerSendChunk is a chunk send request from Session to PeerConn.
// Each event carries its return address (resultCh) so PeerConn does not
// maintain a session-to-inbox map. handle is threaded back in
// channelChunkSent for correlation; chunkID carries the wire bytes.
type peerSendChunk struct {
	peerID      identity.PeerID
	channelID   ChannelID
	messageID   MessageID
	handle      ChunkHandle
	chunkID     []byte
	payload     []byte
	ctx         context.Context
	resultCh    chan<- channelEvent
	sessionDone <-chan struct{}
}
