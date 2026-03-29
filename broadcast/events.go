package broadcast

import (
	"context"

	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/transport"
)

// ---------------------------------------------------------------------------
// Engine events
// ---------------------------------------------------------------------------

type engineEventKind uint8

const (
	evChannelCreated engineEventKind = iota + 1
	evChannelRemoved
	evPeerConnected
	evPeerHandshake
	evPeerGone
	evPeerSubscribed
	evPeerUnsubscribed
)

type engineEvent struct {
	kind      engineEventKind
	channelID ChannelID
	peerID    PeerID
	conn      transport.Conn      // evPeerConnected
	peer      *PeerConn           // evPeerHandshake
	channels  []ChannelID         // evPeerHandshake
	err       error               // evPeerHandshake
	inbox     chan<- channelEvent // evChannelCreated
	done      <-chan struct{}     // evChannelCreated
	cancel    func()              // evChannelCreated: called on rejection
}

// ---------------------------------------------------------------------------
// Channel events
// ---------------------------------------------------------------------------

// channelEvent is the marker interface for events sent to a Channel inbox.
type channelEvent interface {
	channelEvent()
}

// channelChunkStream represents an inbound chunk stream. The frame carries
// the wire metadata; the stream holds the unread payload.
type channelChunkStream struct {
	peerID PeerID
	frame  *bcastpb.Chunk_Header
	stream transport.ReceiveStream
}

// channelChunkData is posted by a read goroutine (spawned by session) after
// reading chunk data from a stream. Routes back through the channel inbox.
type channelChunkData struct {
	messageID MessageID
	peerID    PeerID
	chunkID   []byte
	payload   []byte
}

// channelSessionOpen notifies Channel that a remote peer opened a session
// control stream.
type channelSessionOpen struct {
	peerID PeerID
	msg    *bcastpb.Sess_Open
}

// channelSessionDisposed is sent by a session when it has no remaining work.
type channelSessionDisposed struct {
	messageID MessageID
}

// channelPeerChange notifies the channel of a topology mutation.
// peerRef != nil means joined; nil means left.
type channelPeerChange struct {
	peerID  PeerID
	peerRef *PeerConn // non-nil = joined, nil = left
}

// channelPeerReconstructed is sent by PeerConn when a remote peer signals
// session close (nil bitmap routing update).
type channelPeerReconstructed struct {
	messageID MessageID
	peerID    PeerID
}

type channelPublish struct {
	messageID    MessageID
	strategy     any    // Strategy[CI, R] (type-erased; handlePublish asserts)
	preambleData []byte // pre-marshalled preamble
	errCh        chan error
}

// channelChunkSent is sent by PeerConn after a chunk write completes.
type channelChunkSent struct {
	messageID MessageID
	peerID    PeerID
	handle    ChunkHandle
	err       error
	size      int
}

// channelRoutingUpdate is sent when a routing update arrives on a SESS stream.
type channelRoutingUpdate struct {
	peerID    PeerID
	messageID MessageID
	msg       *bcastpb.Sess_Update
}

// channelWork is sent by a watchWork goroutine when a
// strategy's Work() channel fires (async production available).
type channelWork struct {
	messageID MessageID
}

// channelVerifyResult is sent by a watchVerified goroutine when an async
// chunk verification completes.
type channelVerifyResult struct {
	messageID MessageID
	peerID    PeerID
	chunkID   []byte
	payload   []byte
	verdict   Verdict
}

// channelDecoded is sent by a session-owned decode worker when an async
// decode task completes.
type channelDecoded struct {
	messageID MessageID
	payload   []byte
	err       error
}

func (channelChunkStream) channelEvent()       {}
func (channelChunkData) channelEvent()         {}
func (channelSessionOpen) channelEvent()       {}
func (channelPeerChange) channelEvent()        {}
func (channelPublish) channelEvent()           {}
func (channelSessionDisposed) channelEvent()   {}
func (channelChunkSent) channelEvent()         {}
func (channelRoutingUpdate) channelEvent()     {}
func (channelPeerReconstructed) channelEvent() {}
func (channelWork) channelEvent()              {}
func (channelVerifyResult) channelEvent()      {}
func (channelDecoded) channelEvent()           {}

// ---------------------------------------------------------------------------
// Peer control events (Session -> PeerConn outbound loop)
// ---------------------------------------------------------------------------

// peerCtrlEvent is a control event from Session to PeerConn's outbound loop.
type peerCtrlEvent interface {
	peerCtrlEvent()
}

type peerOpenSession struct {
	channelID      ChannelID
	messageID      MessageID
	preamble       []byte
	initialRouting []byte // embedded in SessionOpen proto message
	channelInbox   chan<- channelEvent
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

// peerCloseStream tells PeerConn to close the outbound SESS
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
	peerID      PeerID
	channelID   ChannelID
	messageID   MessageID
	handle      ChunkHandle
	chunkID     []byte
	payload     []byte
	ctx         context.Context
	resultCh    chan<- channelEvent
	sessionDone <-chan struct{}
}
