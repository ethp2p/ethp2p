package broadcast

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ethp2p/ethp2p/wire"
	"github.com/ethp2p/ethp2p/transport"
)

const (
	channelInboxCap = 1024
	maxParkedChunks = 32

	activeSessionTTL = 5 * time.Minute
	pendingChunkTTL  = 10 * time.Second
	cleanupInterval  = 30 * time.Second
	// TODO: make adaptable.
	routingTickInterval = 25 * time.Millisecond
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
	members map[transport.PeerID]*PeerConn
	// sessions are active sessions on this channel
	sessions map[MessageID]*session[CI, R]
	// parked buffers inbound chunk streams that arrived before their
	// session exists. Streams are unread; QUIC flow control applies
	// backpressure to the sender.
	parked map[MessageID][]channelChunkStream

	// Producers wait only while the actor is running: delivery.done releases
	// every send when it stops. The actor never waits on engine or peer progress.
	inbox    chan channelEvent
	delivery *channelDelivery

	// watchWg tracks watchWork/watchVerified goroutines.
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
		members:  make(map[transport.PeerID]*PeerConn),
		sessions: make(map[MessageID]*session[CI, R]),
		parked:   make(map[MessageID][]channelChunkStream),
		inbox:    make(chan channelEvent, channelInboxCap),
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
	}

	tr.delivery = newChannelDelivery(tr.inbox)
	go tr.run()

	select {
	case e.eventCh <- engineEvent{kind: evChannelCreated, channelID: id, inbox: tr.inbox, done: tr.done, cancel: cancel, delivery: tr.delivery}:
	case <-e.ctx.Done():
	}

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

// newSession creates a session, wiring in channel-level context that the
// session needs (channel ID, context, factories, observer, inbox).
func (tr *Channel[CI, R, P]) newSession(
	messageID MessageID,
	preamble []byte,
	isOrigin bool,
	strategy Strategy[CI, R],
) *session[CI, R] {
	sessCtx, sessCancel := context.WithCancelCause(tr.ctx)
	channelDone := tr.ctx.Done()
	if tr.delivery != nil {
		channelDone = tr.delivery.done
	}
	stage := stageConsuming
	if isOrigin {
		stage = stageOrigin
	}
	return &session[CI, R]{
		channelID:       tr.id,
		messageID:       messageID,
		preamble:        preamble,
		stage:           stage,
		strategy:        strategy,
		newCI:           tr.scheme.NewCI,
		newR:            tr.scheme.NewR,
		createdAt:       time.Now(),
		peers:           make(map[transport.PeerID]*sessionPeer),
		channelInbox:    tr.inbox,
		channelDone:     channelDone,
		observer:        tr.engine.config.Observer,
		outboundCancels: make(map[outboundKey]context.CancelFunc),
		readSem:         make(chan struct{}, maxConcurrentReads),
		dedupGroups:     make(map[string]*dedupGroup),
		pendingVerify:   make(map[string]context.CancelFunc),
		sessCtx:         sessCtx,
		sessCancel:      sessCancel,
		ctx:             tr.ctx,
		done:            make(chan struct{}),
	}
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
	case tr.inbox <- channelPublish{messageID: messageID, strategy: strat, preambleData: preambleBytes, errCh: errCh}:
		select {
		case err := <-errCh:
			return err
		case <-tr.delivery.done:
			return ErrEngineClosed
		case <-tr.ctx.Done():
			return ErrEngineClosed
		}
	case <-tr.ctx.Done():
		return ErrEngineClosed
	case <-tr.delivery.done:
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

	tr.engine.config.Observer.OnSessionDecoded(tr.id, messageID, 0)
}

// run is the Channel actor's event loop. All mutable state lives here.
func (tr *Channel[CI, R, P]) run() {
	defer close(tr.done)
	cleanupTicker := time.NewTicker(cleanupInterval)
	defer cleanupTicker.Stop()
	routingTicker := time.NewTicker(routingTickInterval)
	defer routingTicker.Stop()
	for {
		select {
		case <-tr.ctx.Done():
			tr.shutdown()
			return
		case evt := <-tr.inbox:
			tr.handle(evt)
		case <-cleanupTicker.C:
			tr.cleanup()
		case <-routingTicker.C:
			tr.tickRouting()
		}
	}
}

func (tr *Channel[CI, R, P]) tickRouting() {
	for _, sess := range tr.sessions {
		sess.flushRouting()
	}
}

func (tr *Channel[CI, R, P]) handle(evt channelEvent) {
	switch e := evt.(type) {
	case channelCreatorDeparted:
		for mid, sess := range tr.sessions {
			if sess.lease != nil && sess.lease.peer == e.peer {
				tr.disposeSession(mid, "creator_disconnected")
			}
		}
	case channelReadDone:
		if sess := tr.sessions[e.messageID]; sess != nil && e.session == sess {
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
	case channelPublish:
		tr.handlePublish(e)
	case channelSessionDisposed:
		tr.disposeSession(e.messageID, "all_peers_done")
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
		}
	case channelWork:
		if sess := tr.sessions[e.messageID]; sess != nil {
			sess.drainPolls()
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
				sess.signalReconstructed()
			}
		}
	}
}

func (tr *Channel[CI, R, P]) handleChunk(e channelChunkStream) {
	messageID := MessageID(e.frame.MessageId)
	sess, ok := tr.sessions[messageID]
	if ok {
		sess.drainReads()
		sess.handleChunkStream(e.peerID, e.frame.ChunkId, e.frame.DataLength, e.stream)
		return
	}

	// A parked CHUNK depends on a missing SESS. Bound it rather than holding
	// all credit that the SESS itself needs, both per message and per peer.
	buf := tr.parked[messageID]
	if len(buf) >= maxParkedChunks {
		e.stream.CancelRead(wire.Overloaded)
		return
	}
	if held, ok := e.stream.(*heldChunk); ok && !held.move(chunkParked) {
		e.stream.CancelRead(wire.Overloaded)
		return
	}
	tr.parked[messageID] = append(buf, e)
}

func (tr *Channel[CI, R, P]) handleSessionOpen(e channelSessionOpen) {
	messageID := MessageID(e.msg.MessageId)
	if _, exists := tr.sessions[messageID]; exists {
		return
	}
	var lease *sessionLease
	if e.peer != nil {
		var code wire.Code
		lease, code = e.peer.reserveSession(tr.delivery)
		if lease == nil {
			e.stream.CancelRead(code)
			return
		}
	}
	sess, err := tr.createRelaySession(messageID, e.msg.Preamble, e.peerID, e.msg.InitialUpdate)
	if err != nil {
		lease.release()
		tr.engine.config.Observer.OnChunkError(ChunkProcessError{Peer: e.peerID, ChannelID: tr.id, MessageID: messageID, Err: err})
		return
	}
	sess.lease = lease
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
	for id, chunks := range tr.parked {
		kept := chunks[:0]
		for _, chunk := range chunks {
			if chunk.peerID == e.peerID {
				chunk.stream.CancelRead(wire.Unspecified)
			} else {
				kept = append(kept, chunk)
			}
		}
		clear(chunks[len(kept):])
		if len(kept) == 0 {
			delete(tr.parked, id)
		} else {
			tr.parked[id] = kept
		}
	}
	for _, sess := range tr.sessions {
		sess.handlePeerDropped(e.peerID)
	}
}

func (tr *Channel[CI, R, P]) handlePublish(e channelPublish) {
	strat, ok := e.strategy.(Strategy[CI, R])
	if !ok {
		e.errCh <- fmt.Errorf("publish: strategy type mismatch")
		return
	}

	// Replacing a relay with a local publication must retire its attachment
	// lifecycle and creator lease before overwriting the session map entry.
	tr.disposeSession(e.messageID, "replaced")
	sess := tr.newSession(e.messageID, e.preambleData, true, strat)

	tr.sessions[e.messageID] = sess
	tr.maybeWatchWork(sess)
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
func (tr *Channel[CI, R, P]) createRelaySession(messageID MessageID, preambleBytes []byte, initialPeer transport.PeerID, initialRouting []byte) (*session[CI, R], error) {
	if existing, ok := tr.sessions[messageID]; ok {
		return existing, nil
	}

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
	tr.maybeWatchWork(sess)
	tr.maybeWatchVerified(sess)

	for _, peer := range tr.members {
		sess.handlePeerAttached(peer)
	}

	if len(initialRouting) > 0 {
		sess.handleRoutingUpdate(initialPeer, initialRouting)
	}

	// Flush pending chunk streams that arrived before the session existed.
	if chunks := tr.parked[messageID]; len(chunks) > 0 {
		for _, c := range chunks {
			if held, ok := c.stream.(*heldChunk); ok && !held.move(chunkActive) {
				continue
			}
			sess.handleChunkStream(c.peerID, c.frame.ChunkId, c.frame.DataLength, c.stream)
		}
		delete(tr.parked, messageID)
	}

	tr.engine.config.Observer.OnSessionStarted(tr.id, messageID, SessionRoleRelay)
	return sess, nil
}

// maybeWatchWork spawns a goroutine to forward the strategy's Work()
// signals into the channel inbox. If Work() returns nil (no async
// production), this is a no-op.
func (tr *Channel[CI, R, P]) maybeWatchWork(sess *session[CI, R]) {
	ch := sess.strategy.Work()
	if ch == nil {
		return
	}
	msgID := sess.messageID
	watchFn := func() {
		for {
			select {
			case _, ok := <-ch:
				if !ok {
					return
				}
				select {
				case tr.inbox <- channelWork{messageID: msgID}:
				case <-tr.delivery.done:
					return
				case <-tr.ctx.Done():
					return
				}
			case <-tr.ctx.Done():
				return
			}
		}
	}
	tr.watchWg.Go(watchFn)
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
				case <-tr.delivery.done:
					return
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
	for _, chunk := range tr.parked[messageID] {
		chunk.stream.CancelRead(code)
	}
	delete(tr.parked, messageID)
	sess.closeWithCode(code)

	tr.engine.config.Observer.OnSessionDisposed(tr.id, messageID, reason)
}

func (tr *Channel[CI, R, P]) cleanup() {
	now := time.Now()
	for mid, sess := range tr.sessions {
		if now.Sub(sess.createdAt) > activeSessionTTL {
			tr.disposeSession(mid, "ttl_expired")
		}
	}
	for mid, chunks := range tr.parked {
		if len(chunks) > 0 {
			// TODO: add a creation timestamp or use a timer to expire
			// pending stream groups. For now, cancel all streams and
			// drop the group unconditionally on GC tick.
			for _, c := range chunks {
				c.stream.CancelRead(wire.Refused)
			}
			delete(tr.parked, mid)
		}
	}
}

func (tr *Channel[CI, R, P]) shutdown() {
	tr.delivery.close()
	for _, sess := range tr.sessions {
		sess.Close()
	}
	// Cancel all parked chunk streams.
	for _, chunks := range tr.parked {
		for _, c := range chunks {
			c.stream.CancelRead(wire.Unspecified)
		}
	}
	tr.watchWg.Wait()
	if tr.subCh != nil {
		close(tr.subCh)
	}
}
