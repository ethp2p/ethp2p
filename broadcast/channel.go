package broadcast

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/wire"
)

const (
	channelInboxCap = 1024
	maxParkedChunks = 32

	activeSessionTTL = 5 * time.Minute
)

// Channel manages the event loop for a single channel with a given scheme.
// All mutable state (peers, sessions, pending chunks) is owned exclusively
// by the run() goroutine. External callers interact via channel sends.
// Must be created via AttachChannel.
type Channel[CI ChunkIdent, R Wire, P Wire] struct {
	engine *Engine

	id     ChannelID
	scheme Scheme[CI, R, P]
	subCh  chan FullMessage

	// Owned exclusively by run(), lock-free
	// members are peers we are connected to that subscribe to this channel
	// the channel simply tracks interest, and feeds it to the router to decide how to use
	members  map[identity.PeerID]*PeerConn
	sessions map[MessageID]*session[CI, R]
	// parked buffers inbound chunk streams that arrived before their
	// session exists, grouped per message with the first chunk's arrival
	// time bounding the group's TTL. Streams are unread; QUIC flow control
	// applies backpressure to the sender.
	parked map[MessageID]*parkedGroup

	// Producers wait only while ctx is live. Once it ends the actor drains
	// the inbox (drainInbox). The actor never waits on engine or peer progress.
	inbox chan channelEvent

	watchWg sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// AttachChannel creates a new Channel for the given channel ID and scheme.
// The channel event loop starts immediately. If the engine rejects the channel
// (e.g. duplicate ID), it cancels the channel's context and notifies the
// observer via OnChannelAttached with an error.
//
// This is a package-level generic function because Go does not allow
// generic methods on non-generic types.
func AttachChannel[CI ChunkIdent, R Wire, P Wire](e *Engine, id ChannelID, scheme Scheme[CI, R, P]) *Channel[CI, R, P] {
	ctx, cancel := context.WithCancel(e.ctx)

	tr := &Channel[CI, R, P]{
		engine:   e,
		id:       id,
		scheme:   scheme,
		members:  make(map[identity.PeerID]*PeerConn),
		sessions: make(map[MessageID]*session[CI, R]),
		parked:   make(map[MessageID]*parkedGroup),
		inbox:    make(chan channelEvent, channelInboxCap),
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
	}

	go tr.run()

	h := &channelHandle{inbox: tr.inbox, ctx: ctx, done: tr.done}
	e.post(nil, func() { e.addChannel(id, h, cancel) })

	return tr
}

// Subscribe sets the delivery channel for decoded messages. The caller
// creates the channel; Channel takes ownership and closes it on Stop.
// The channel must be buffered; deliverMessage drops messages non-blockingly
// when the channel is full. Subscribe must be called before peers connect.
func (tr *Channel[CI, R, P]) Subscribe(ch chan FullMessage) error {
	if tr.subCh != nil {
		return ErrAlreadySubscribed
	}
	if cap(ch) == 0 {
		return ErrUnbufferedSubscription
	}
	tr.subCh = ch
	return nil
}

func (tr *Channel[CI, R, P]) newSession(
	messageID MessageID,
	preamble []byte,
	isOrigin bool,
	strategy Strategy[CI, R],
) *session[CI, R] {
	sessCtx, sessCancel := context.WithCancelCause(tr.ctx)
	stage := stageConsuming
	if isOrigin {
		stage = stageOrigin
	}
	sess := &session[CI, R]{
		channelID:       tr.id,
		messageID:       messageID,
		preamble:        preamble,
		stage:           stage,
		strategy:        strategy,
		newCI:           tr.scheme.NewCI,
		newR:            tr.scheme.NewR,
		createdAt:       time.Now(),
		peers:           make(map[identity.PeerID]*sessionPeer),
		channelInbox:    tr.inbox,
		observer:        tr.engine.config.Observer,
		outboundCancels: make(map[outboundKey]context.CancelFunc),
		readSem:         make(chan struct{}, maxConcurrentReads),
		dedupGroups:     make(map[string]*dedupGroup),
		sessCtx:         sessCtx,
		sessCancel:      sessCancel,
	}
	tr.scheduleExpiry(sess.createdAt.Add(activeSessionTTL))
	return sess
}

// scheduleExpiry posts one expiry event at deadline. Sessions and parked
// groups each schedule their own, so expiry needs no periodic scan; a
// stale event is harmless because expireDue rechecks ages.
func (tr *Channel[CI, R, P]) scheduleExpiry(deadline time.Time) {
	ctx, inbox := tr.ctx, tr.inbox
	time.AfterFunc(time.Until(deadline), func() {
		select {
		case inbox <- channelExpiry{}:
		case <-ctx.Done():
		}
	})
}

// Stop stops the channel event loop and unregisters from the engine.
func (tr *Channel[CI, R, P]) Stop() {
	tr.cancel()
	<-tr.done
	tr.engine.DropChannel(tr.id)
}

// Publish publishes a message to the channel. Encoding (strategy creation,
// preamble marshalling) runs on the caller's goroutine to keep the event
// loop lean. The event loop only does session registration and peer attachment.
func (tr *Channel[CI, R, P]) Publish(messageID MessageID, payload []byte) error {
	strat, preamble, err := tr.scheme.NewOrigin(messageID, payload)
	if err != nil {
		return err
	}
	preambleBytes, err := preamble.Marshal()
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)
	select {
	case tr.inbox <- channelPublish[CI, R]{messageID: messageID, strategy: strat, preambleData: preambleBytes, errCh: errCh}:
		select {
		case err := <-errCh:
			return err
		case <-tr.ctx.Done():
			return ErrEngineClosed
		}
	case <-tr.ctx.Done():
		return ErrEngineClosed
	}
}

func (tr *Channel[CI, R, P]) deliverMessage(messageID MessageID, payload []byte) {
	if ch := tr.subCh; ch != nil {
		msg := FullMessage{
			ChannelID: tr.id,
			MessageID: messageID,
			Data:      payload,
		}
		select {
		case ch <- msg:
		default:
			slog.Warn("dropped decoded message: subscriber channel full",
				"channel", tr.id,
				"message_id", messageID,
			)
		}
	}

	tr.engine.config.Observer.OnSessionDecoded(tr.id, messageID)
}

// run is the Channel actor's event loop. All mutable state lives here.
func (tr *Channel[CI, R, P]) run() {
	defer close(tr.done)
	for {
		select {
		case <-tr.ctx.Done():
			tr.shutdown()
			return
		case evt := <-tr.inbox:
			tr.handle(evt)
		}
	}
}

func (tr *Channel[CI, R, P]) handle(evt channelEvent) {
	switch e := evt.(type) {
	case channelExpiry:
		tr.expireDue()
	case channelReadDone:
		if sess := tr.sessions[e.messageID]; sess != nil {
			sess.drainReads()
		}
	case channelChunkStream:
		tr.handleChunk(e)
	case channelChunkData:
		if sess := tr.sessions[e.messageID]; sess != nil {
			sess.handleChunkData(e.peerID, e.chunkID, e.payload)
		}
	case channelSessionOpen:
		tr.handleSessionOpen(e)
	case channelPeerChange:
		if e.peerRef != nil {
			tr.handlePeerBound(e)
		} else {
			tr.handlePeerUnbound(e)
		}
	case channelPublish[CI, R]:
		tr.handlePublish(e)
	case channelChunkSent:
		if sess := tr.sessions[e.messageID]; sess != nil {
			sess.handleSendComplete(e.peerID, e.handle, e.err, e.size)
		}
	case channelRoutingUpdate:
		if sess := tr.sessions[e.messageID]; sess != nil {
			sess.handleRoutingUpdate(e.peerID, e.msg.Data)
		}
	case channelPeerReconstructed:
		if sess := tr.sessions[e.messageID]; sess != nil {
			sess.handlePeerCompleted(e.peerID)
			tr.disposeIfIdle(sess)
		}
	case channelPeerSessionEnded:
		// A stale event from a replaced connection must not detach the peer's
		// current one.
		if tr.members[e.peerID] != e.peer {
			return
		}
		if sess := tr.sessions[e.messageID]; sess != nil {
			sess.handlePeerDropped(e.peerID)
			tr.disposeIfIdle(sess)
		}
	case channelVerifyResult:
		if sess := tr.sessions[e.messageID]; sess != nil {
			sess.handleVerifyResult(e.peerID, e.chunkID, e.payload, e.verdict)
		}
	case channelDecoded:
		if sess := tr.sessions[e.messageID]; sess != nil && sess.stage < stageReconstructed {
			if e.err != nil {
				sess.observer.OnChunkError(ChunkProcessError{
					ChannelID: sess.channelID,
					MessageID: sess.messageID,
					Err:       fmt.Errorf("decode: %w", e.err),
				})
			} else {
				tr.deliverMessage(sess.messageID, e.payload)
				sess.stage = stageReconstructed
				tr.disposeIfIdle(sess)
			}
		}
	}
}

func (tr *Channel[CI, R, P]) handleChunk(e channelChunkStream) {
	messageID := MessageID(e.frame.ObjectId)
	sess, ok := tr.sessions[messageID]
	if ok {
		sess.drainReads()
		sess.handleChunkStream(e.peerID, e.frame.ChunkId, e.frame.DataLength, e.stream)
		return
	}

	// A parked chunk stream depends on a missing session stream. Bound it per message rather
	// than holding all credit that the session stream itself needs; the peer's QUIC
	// stream limit is the only bound across messages.
	group := tr.parked[messageID]
	if group != nil && len(group.streams) >= maxParkedChunks {
		e.stream.CancelRead(wire.Unspecified)
		return
	}
	if group == nil {
		now := time.Now()
		group = &parkedGroup{at: now}
		tr.parked[messageID] = group
		tr.scheduleExpiry(now.Add(activeSessionTTL))
	}
	group.streams = append(group.streams, e)
}

func (tr *Channel[CI, R, P]) handleSessionOpen(e channelSessionOpen) {
	messageID := MessageID(e.msg.ObjectId)
	if sess, exists := tr.sessions[messageID]; exists {
		// Every open carries its sender's routing state, including opens for
		// sessions we already have (e.g. the sender connected after receiving
		// shards). The peer is already attached via the channel's members, so
		// only the routing needs applying.
		if len(e.msg.InitialUpdate) > 0 {
			sess.handleRoutingUpdate(e.peerID, e.msg.InitialUpdate)
		}
		return
	}
	if _, err := tr.createRelaySession(messageID, e.preamble, e.peerID, e.msg.InitialUpdate); err != nil {
		tr.engine.config.Observer.OnChunkError(ChunkProcessError{Peer: e.peerID, ChannelID: tr.id, MessageID: messageID, Err: err})
	}
}

func (tr *Channel[CI, R, P]) handlePeerBound(e channelPeerChange) {
	if tr.members[e.peerID] == e.peerRef {
		return
	}
	tr.members[e.peerID] = e.peerRef
	for _, sess := range tr.sessions {
		sess.handlePeerAttached(e.peerRef)
	}
}

func (tr *Channel[CI, R, P]) handlePeerUnbound(e channelPeerChange) {
	delete(tr.members, e.peerID)
	for id, group := range tr.parked {
		kept := group.streams[:0]
		for _, chunk := range group.streams {
			if chunk.peerID == e.peerID {
				chunk.stream.CancelRead(wire.Unspecified)
			} else {
				kept = append(kept, chunk)
			}
		}
		clear(group.streams[len(kept):])
		if len(kept) == 0 {
			delete(tr.parked, id)
		} else {
			group.streams = kept
		}
	}
	for _, sess := range tr.sessions {
		sess.handlePeerDropped(e.peerID)
		tr.disposeIfIdle(sess)
	}
}

func (tr *Channel[CI, R, P]) disposeIfIdle(sess *session[CI, R]) {
	if sess.idle() {
		tr.disposeSession(sess.messageID, "all_peers_done")
	}
}

func (tr *Channel[CI, R, P]) handlePublish(e channelPublish[CI, R]) {
	// Replacing a relay with a local publication must retire its attachment
	// lifecycle before overwriting the session map entry.
	tr.disposeSession(e.messageID, "replaced")
	sess := tr.newSession(e.messageID, e.preambleData, true, e.strategy)

	tr.sessions[e.messageID] = sess
	tr.maybeWatchVerified(sess)

	for _, peer := range tr.members {
		sess.handlePeerAttached(peer)
	}

	// Origin strategy already has chunks; drain polls to dispatch.
	sess.drainPolls()

	tr.engine.config.Observer.OnSessionStarted(tr.id, e.messageID, SessionRoleOrigin)
	e.errCh <- nil
}

// createRelaySession builds a relay session from the given preamble bytes,
// registers it, attaches current peers, applies initial routing from the
// opening peer, and flushes any pending chunks. The ordering matters:
// routing must be applied before pending chunks so that the strategy
// knows the opener's inventory before deciding what to emit.
// Called from within run(), so no locking needed.
func (tr *Channel[CI, R, P]) createRelaySession(messageID MessageID, preambleBytes []byte, initialPeer identity.PeerID, initialRouting []byte) (*session[CI, R], error) {
	p := tr.scheme.NewP()
	if err := p.Unmarshal(preambleBytes); err != nil {
		return nil, err
	}
	strat, err := tr.scheme.NewRelay(messageID, p)
	if err != nil {
		return nil, err
	}

	sess := tr.newSession(messageID, preambleBytes, false, strat)

	tr.sessions[messageID] = sess
	tr.maybeWatchVerified(sess)

	for _, peer := range tr.members {
		sess.handlePeerAttached(peer)
	}

	if len(initialRouting) > 0 {
		sess.handleRoutingUpdate(initialPeer, initialRouting)
	}

	if group := tr.parked[messageID]; group != nil {
		for _, c := range group.streams {
			sess.handleChunkStream(c.peerID, c.frame.ChunkId, c.frame.DataLength, c.stream)
		}
		delete(tr.parked, messageID)
	}

	tr.engine.config.Observer.OnSessionStarted(tr.id, messageID, SessionRoleRelay)
	return sess, nil
}

// maybeWatchVerified spawns a goroutine to forward the strategy's
// Verified() results into the channel inbox as channelVerifyResult events.
// If Verified() returns nil (no async verification), this is a no-op.
func (tr *Channel[CI, R, P]) maybeWatchVerified(sess *session[CI, R]) {
	ch := sess.strategy.Verified()
	if ch == nil {
		return
	}
	msgID := sess.messageID
	verifyFn := func() {
		for {
			select {
			case result, ok := <-ch:
				if !ok {
					return
				}
				chunkIDBytes, err := result.ChunkID.Marshal()
				if err != nil {
					continue
				}
				select {
				case tr.inbox <- channelVerifyResult{
					messageID: msgID,
					peerID:    result.Peer,
					chunkID:   chunkIDBytes,
					payload:   result.Data,
					verdict:   result.Verdict,
				}:
				case <-tr.ctx.Done():
					return
				}
			case <-tr.ctx.Done():
				return
			}
		}
	}
	tr.watchWg.Go(verifyFn)
}

func (tr *Channel[CI, R, P]) disposeSession(messageID MessageID, reason string) {
	sess, ok := tr.sessions[messageID]
	if !ok {
		return
	}
	delete(tr.sessions, messageID)
	code := wire.Unspecified
	if sess.stage >= stageDecoding {
		code = Redundant
	} else if reason == "ttl_expired" {
		code = wire.Refused
	}
	if group := tr.parked[messageID]; group != nil {
		for _, chunk := range group.streams {
			chunk.stream.CancelRead(code)
		}
	}
	delete(tr.parked, messageID)
	sess.closeWithCode(code)

	tr.engine.config.Observer.OnSessionDisposed(tr.id, messageID, reason)
}

func (tr *Channel[CI, R, P]) expireDue() {
	now := time.Now()
	for mid, sess := range tr.sessions {
		if now.Sub(sess.createdAt) >= activeSessionTTL {
			tr.disposeSession(mid, "ttl_expired")
		}
	}
	for mid, group := range tr.parked {
		if now.Sub(group.at) >= activeSessionTTL {
			for _, c := range group.streams {
				c.stream.CancelRead(wire.Refused)
			}
			delete(tr.parked, mid)
		}
	}
}

// drainInbox cancels chunk streams still buffered once tr.ctx has ended. It
// runs after every session and watcher has stopped, so only peers and the
// engine can still send. A producer whose send lands after the final poll
// sees ch.ctx ended and cancels its own stream (see processChunk).
func (tr *Channel[CI, R, P]) drainInbox() {
	for {
		select {
		case evt := <-tr.inbox:
			if c, ok := evt.(channelChunkStream); ok {
				c.stream.CancelRead(wire.Unspecified)
			}
		default:
			return
		}
	}
}

func (tr *Channel[CI, R, P]) shutdown() {
	for _, sess := range tr.sessions {
		sess.Close()
	}
	for _, group := range tr.parked {
		for _, c := range group.streams {
			c.stream.CancelRead(wire.Unspecified)
		}
	}
	tr.watchWg.Wait()
	tr.drainInbox()
	if tr.subCh != nil {
		close(tr.subCh)
	}
}
