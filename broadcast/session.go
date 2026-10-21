package broadcast

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/internal/ctxutil"
	"github.com/ethp2p/ethp2p/wire"
)

const maxConcurrentReads = 64

type pendingChunkRead struct {
	peer    identity.PeerID
	chunkID []byte
	dataLen uint32
	stream  ethp2p.ReceiveStream
}

// outboundKey identifies an in-flight outbound chunk send to a specific peer.
// handle is the strategy-assigned opaque token from ChunkID.Handle().
type outboundKey struct {
	peer   identity.PeerID
	handle ChunkHandle
}

// dedupGroup tracks a shared cancellation context for concurrent reads
// of chunks with the same dedup key. When one read succeeds with
// VerdictAccepted, the group is cancelled, aborting the others.
type dedupGroup struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
}

type sessionPeer struct {
	conn        *PeerConn
	chunkOutbox chan peerSendChunk
	completed   bool
}

// session is the per-message state struct, generic on chunk identifier CI and
// routing type R. All Strategy mutations happen inside the owning
// Channel's run() goroutine.
// sessionStage tracks the lifecycle of a session as a linear progression.
// Integer comparison is intentional: s.stage >= stageDecoding means
// "complete or beyond, including origin".
type sessionStage uint8

const (
	stageConsuming     sessionStage = iota // relay, accepting chunks
	stageDecoding                          // relay, decode goroutine running
	stageReconstructed                     // relay, decode succeeded
	stageOrigin                            // origin, has the payload from the start
)

type session[CI ChunkIdent, R Wire] struct {
	channelID ChannelID
	messageID MessageID
	preamble  []byte // wire bytes, sent to new peers
	stage     sessionStage

	strategy Strategy[CI, R]
	newCI    func() CI
	newR     func() R

	createdAt time.Time

	// everHadPeers is set on the first handlePeerAttached call.
	// idle uses it to distinguish "no peers yet" (wait for
	// peer attachment) from "all peers gone" (dispose).
	everHadPeers bool
	peers        map[identity.PeerID]*sessionPeer
	channelInbox chan<- channelEvent
	observer     Observer

	// outboundCancels tracks cancel functions for in-flight chunk sends.
	// When a routing update reveals the peer already has the chunk, the
	// send is cancelled via this map.
	outboundCancels map[outboundKey]context.CancelFunc

	// readSem bounds the number of concurrent inbound data reads.
	readSem   chan struct{}
	readQueue []pendingChunkRead

	// dedupGroups maps dedup keys to cancel functions. All concurrent
	// reads with the same non-nil key share a context; when one
	// completes with VerdictAccepted, the cancel cancels the others.
	dedupGroups map[string]*dedupGroup

	wg sync.WaitGroup

	// sessCtx is a child of the channel context. Cancelling it cancels this
	// session's inbound reads and drops its queued outbound chunk sends. It
	// does not cancel a chunk write already in progress: doSendChunk bounds
	// that write by the peer context and chunkWriteTimeout only.
	sessCtx    context.Context
	sessCancel context.CancelCauseFunc
}

// Close performs synchronous cleanup. Called from the channel goroutine only.
// Drops queued outbound sends, cancels inbound reads, waits for background
// goroutines to finish, closes strategy and notifies peers.
func (s *session[CI, R]) Close() {
	s.closeWithCode(wire.Unspecified)
}

// closeWithCode propagates the reason for session disposal to active stream
// readers before waiting for their cancellation to complete.
func (s *session[CI, R]) closeWithCode(code wire.Code) {
	s.notifyPeersSessionDone()
	cause := error(context.Canceled)
	switch code {
	case Redundant:
		cause = errChunkRedundant
	case wire.Refused:
		cause = errStreamRefused
	}
	s.sessCancel(cause)
	for _, pending := range s.readQueue {
		pending.stream.CancelRead(code)
	}
	s.readQueue = nil
	s.wg.Wait()
	s.strategy.Close()
}

// notifyPeersComplete marks every peer's outbound session stream for closure,
// telling it we no longer need chunks. Called as soon as the strategy signals
// completeness (before decode), since decode failure is non-recoverable.
func (s *session[CI, R]) notifyPeersComplete() {
	for _, sp := range s.peers {
		sp.conn.ctrlState.closeSessionStream(sessionKey{channelID: s.channelID, messageID: s.messageID})
	}
}

// notifyPeersSessionDone marks the peer session closed so the outbound loop
// closes its session stream and drops the chunk slot. It covers completed peers
// too: notifyPeersComplete ended only their session stream, and their chunk slot
// stays registered until this close.
func (s *session[CI, R]) notifyPeersSessionDone() {
	for _, sp := range s.peers {
		sp.conn.ctrlState.closeSession(sessionKey{channelID: s.channelID, messageID: s.messageID})
	}
}

// handleChunkStream decides whether to read data from an inbound chunk
// stream. If the chunk is needed, it acquires a semaphore slot, resolves
// a dedup group, and spawns a goroutine to read the data. The goroutine
// posts a channelChunkData back to the channel inbox on completion.
func (s *session[CI, R]) handleChunkStream(peer identity.PeerID, chunkID []byte, dataLen uint32, stream ethp2p.ReceiveStream) {
	chunk := s.newCI()
	if err := chunk.Unmarshal(chunkID); err != nil {
		s.observer.OnChunkError(ChunkProcessError{
			Peer:      peer,
			ChannelID: s.channelID,
			MessageID: s.messageID,
			Err:       fmt.Errorf("unmarshal chunk id: %w", err),
		})
		stream.CancelRead(wire.Unspecified)
		return
	}
	if dataLen > maxChunkDataSize {
		s.observer.OnChunkError(ChunkProcessError{
			Peer:      peer,
			ChannelID: s.channelID,
			MessageID: s.messageID,
			Err:       fmt.Errorf("chunk data length %d exceeds max %d", dataLen, maxChunkDataSize),
		})
		stream.CancelRead(wire.Unspecified)
		return
	}
	// The strategy is frozen for Decode once the session leaves stageConsuming,
	// so HaveChunk is consulted only before that.
	haveChunk := s.stage < stageDecoding && s.strategy.HaveChunk(chunk)
	if s.stage >= stageDecoding || haveChunk {
		v := VerdictRedundant
		if s.stage >= stageReconstructed {
			v = VerdictSurplus
		} else if s.stage == stageDecoding {
			v = VerdictDecoding
		}
		s.observer.OnChunkRcvd(peer, s.channelID, s.messageID, v)
		stream.CancelRead(Redundant)
		return
	}

	// The actor never waits for a reader. Queue independent payload reads and
	// let a completion wake the actor when a slot becomes available.
	select {
	case s.readSem <- struct{}{}:
	default:
		s.readQueue = append(s.readQueue, pendingChunkRead{peer, chunkID, dataLen, stream})
		return
	}

	readCtx := s.sessCtx
	dedupKey := s.strategy.DedupKey(chunk)
	if dedupKey != nil {
		key := string(dedupKey)
		grp, ok := s.dedupGroups[key]
		if !ok {
			ctx, cancel := context.WithCancelCause(s.sessCtx)
			grp = &dedupGroup{ctx: ctx, cancel: cancel}
			s.dedupGroups[key] = grp
		}
		readCtx = grp.ctx
	}

	msgID := s.messageID
	inbox := s.channelInbox
	s.wg.Go(func() {
		defer func() {
			<-s.readSem
			select {
			case inbox <- channelReadDone{messageID: msgID}:
			case <-s.sessCtx.Done():
			}
		}()

		// Once readCtx ends, the callback owns cancelling the stream; stop
		// joins it, so no path below races it.
		stop := ctxutil.OnCancel(readCtx, func() { stream.CancelRead(streamCancellationCode(context.Cause(readCtx))) })
		defer stop()
		_ = stream.SetReadDeadline(time.Now().Add(chunkReadTimeout))
		defer func() { _ = stream.SetReadDeadline(time.Time{}) }()

		buf, err := readChunkData(stream, dataLen)
		if err != nil {
			stop()
			// Reads never cancel a side, so end it here with the read error's
			// code: a DATA frame out of place or of the wrong length is
			// Refused, and one truncated by FIN is Unspecified.
			stream.CancelRead(streamCancellationCode(errors.Join(err, context.Cause(readCtx))))
			return
		}

		select {
		case inbox <- channelChunkData{
			messageID: msgID,
			peerID:    peer,
			chunkID:   chunkID,
			payload:   buf,
		}:
		case <-readCtx.Done():
		}
	})
}

func (s *session[CI, R]) drainReads() {
	for len(s.readQueue) > 0 && len(s.readSem) < cap(s.readSem) {
		pending := s.readQueue[0]
		s.readQueue[0] = pendingChunkRead{}
		s.readQueue = s.readQueue[1:]
		s.handleChunkStream(pending.peer, pending.chunkID, pending.dataLen, pending.stream)
	}
}

// handleChunkData processes fully-read chunk data. Called on the channel
// goroutine via channelChunkData events.
func (s *session[CI, R]) handleChunkData(peer identity.PeerID, chunkID []byte, payload []byte) {
	chunk := s.newCI()
	if err := chunk.Unmarshal(chunkID); err != nil {
		s.observer.OnChunkError(ChunkProcessError{
			Peer:      peer,
			ChannelID: s.channelID,
			MessageID: s.messageID,
			Err:       fmt.Errorf("unmarshal chunk id: %w", err),
		})
		return
	}
	if s.strategy.HaveChunk(chunk) {
		v := VerdictRedundant
		if s.stage >= stageReconstructed {
			v = VerdictSurplus
		} else if s.stage == stageDecoding {
			v = VerdictDecoding
		}
		s.observer.OnChunkRcvd(peer, s.channelID, s.messageID, v)
		return
	}

	verdict := s.strategy.VerifyChunk(peer, chunk, payload)
	switch verdict {
	case VerdictAccepted:
		s.acceptChunk(peer, chunk, payload)
	case VerdictPending:
	default:
		s.observer.OnChunkRcvd(peer, s.channelID, s.messageID, verdict)
	}
}

// handleVerifyResult processes the outcome of an async chunk
// verification. Called on the channel goroutine via channelVerifyResult.
func (s *session[CI, R]) handleVerifyResult(peer identity.PeerID, chunkID []byte, payload []byte, verdict Verdict) {
	chunk := s.newCI()
	if err := chunk.Unmarshal(chunkID); err != nil {
		return
	}

	switch verdict {
	case VerdictAccepted:
		s.acceptChunk(peer, chunk, payload)
	default:
		s.observer.OnChunkRcvd(peer, s.channelID, s.messageID, verdict)
	}
}

// acceptChunk delivers a verified chunk to the strategy. The strategy
// decides when to cancel the dedup group by calling dedup.Cancel().
// Called from handleChunkData (sync verification) and
// handleVerifyResult (async verification).
func (s *session[CI, R]) acceptChunk(peer identity.PeerID, chunk CI, payload []byte) {
	dedup := s.buildDedupCancel(chunk)
	verdict, complete, err := s.strategy.TakeChunk(peer, chunk, payload, dedup)
	if err != nil {
		s.observer.OnChunkError(ChunkProcessError{Peer: peer, ChannelID: s.channelID, MessageID: s.messageID, Err: err})
	}
	s.observer.OnChunkRcvd(peer, s.channelID, s.messageID, verdict)

	if verdict == VerdictAccepted {
		have, need := s.strategy.Progress()
		s.observer.OnStrategyProgress(s.channelID, s.messageID, have, need)
		if complete && s.stage == stageConsuming {
			// Tell peers we no longer need chunks immediately,
			// before the decode goroutine starts. Since decode
			// failure is non-recoverable, there is no reason to
			// keep accepting inbound traffic.
			s.stage = stageDecoding
			s.notifyPeersComplete()
			strategy := s.strategy
			msgID := s.messageID
			inbox := s.channelInbox
			s.wg.Go(func() {
				payload, err := strategy.Decode()
				select {
				case inbox <- channelDecoded{messageID: msgID, payload: payload, err: err}:
				case <-s.sessCtx.Done():
				}
			})
		}
	}
	s.drainPolls()
}

// buildDedupCancel looks up the dedup group for chunk and returns a
// cancel handle that, when called, tears down the group context and
// cleans up session bookkeeping (dedupGroups).
func (s *session[CI, R]) buildDedupCancel(chunk CI) *DedupCancel {
	dedupKey := s.strategy.DedupKey(chunk)
	if dedupKey == nil {
		return nil
	}
	key := string(dedupKey)
	grp, ok := s.dedupGroups[key]
	if !ok {
		return nil
	}
	return &DedupCancel{CancelFunc: func() {
		grp.cancel(errChunkRedundant)
		delete(s.dedupGroups, key)
	}}
}

func (s *session[CI, R]) handleRoutingUpdate(peer identity.PeerID, data []byte) {
	update := s.newR()
	if err := update.Unmarshal(data); err != nil {
		s.observer.OnChunkError(ChunkProcessError{Peer: peer, ChannelID: s.channelID, MessageID: s.messageID, Err: err})
		return
	}
	cancelHandles, err := s.strategy.RoutingUpdate(peer, update)
	if err != nil {
		s.observer.OnChunkError(ChunkProcessError{Peer: peer, ChannelID: s.channelID, MessageID: s.messageID, Err: err})
	}
	s.observer.OnRoutingUpdate(peer, s.channelID, s.messageID)
	for _, handle := range cancelHandles {
		key := outboundKey{peer: peer, handle: handle}
		if cancel, ok := s.outboundCancels[key]; ok {
			cancel()
			delete(s.outboundCancels, key)
		}
	}
	s.drainPolls()
}

func (s *session[CI, R]) handleSendComplete(peer identity.PeerID, handle ChunkHandle, err error, size int) {
	key := outboundKey{peer: peer, handle: handle}
	if cancel, exists := s.outboundCancels[key]; exists {
		cancel()
		delete(s.outboundCancels, key)
	}
	s.strategy.ChunkSent(peer, handle, err)
	if err == nil {
		s.observer.OnChunkSent(peer, s.channelID, s.messageID, size)
	}
	s.drainPolls()
}

func (s *session[CI, R]) handlePeerAttached(p *PeerConn) {
	// The channel unbinds a peer ID before it binds that ID's next PeerConn,
	// so an attached ID here is p itself and attaching is idempotent.
	if s.peers[p.id] != nil {
		return
	}
	s.everHadPeers = true
	chunkOutbox := make(chan peerSendChunk, 16)
	sp := &sessionPeer{
		conn:        p,
		chunkOutbox: chunkOutbox,
	}
	s.peers[p.id] = sp
	s.strategy.AttachPeer(p.id)

	// Bundle initial routing with SessionOpen so the peer has our
	// inventory before any chunk traffic.
	// TODO: this leaks origin identity; a peer receiving "I have everything"
	// as the first routing update can infer it is directly connected to the
	// publisher. Consider delaying or fuzzing the initial routing update for
	// origin sessions.
	var initialRouting []byte
	if routing, ok := s.strategy.PollRouting(true); ok {
		initialRouting, _ = routing.Marshal()
		s.observer.OnRoutingUpdate(p.id, s.channelID, s.messageID)
	}
	p.ctrlState.openSession(peerOpenSession{
		channelID:      s.channelID,
		messageID:      s.messageID,
		preamble:       s.preamble,
		initialRouting: initialRouting,
		chunkOutbox:    chunkOutbox,
	})
	s.drainPolls()
}

func (s *session[CI, R]) handlePeerDropped(peer identity.PeerID) {
	if sp := s.peers[peer]; sp != nil {
		sp.conn.ctrlState.closeSession(sessionKey{channelID: s.channelID, messageID: s.messageID})
	}
	kept := s.readQueue[:0]
	for _, pending := range s.readQueue {
		if pending.peer == peer {
			pending.stream.CancelRead(wire.Unspecified)
		} else {
			kept = append(kept, pending)
		}
	}
	clear(s.readQueue[len(kept):])
	s.readQueue = kept
	delete(s.peers, peer)
	s.cancelOutbound(peer)
	s.strategy.DetachPeer(peer, false)
}

// cancelOutbound cancels every in-flight chunk send to peer. DetachPeer's
// contract requires it before the strategy forgets the peer.
func (s *session[CI, R]) cancelOutbound(peer identity.PeerID) {
	for key, cancel := range s.outboundCancels {
		if key.peer == peer {
			cancel()
			delete(s.outboundCancels, key)
		}
	}
}

func (s *session[CI, R]) handlePeerCompleted(peer identity.PeerID) {
	sp, ok := s.peers[peer]
	if !ok || sp.completed {
		return
	}
	sp.completed = true
	s.cancelOutbound(peer)
	s.strategy.DetachPeer(peer, true)
}

// idle reports that the session has no remaining work.
//
// A session is either "decoded" (origin, or relay that has reconstructed)
// or "not decoded" (relay still receiving chunks):
//
//   - Decoded: dispose when every peer has completed (decoded) or been
//     dropped. A non-completed peer still needs chunks from us.
//   - Not decoded: dispose when all peers have been dropped (removed from
//     map). Completed peers stay in the map because they may still send
//     us chunks on separate streams; only a drop (disconnection, or the
//     end of their session stream other than Reconstructed) removes them. If
//     all peers are gone, the session can't make progress.
//
// The channel checks it after a peer drops or completes and after decode,
// and disposes the session in the same event. Sessions with no peers ever
// attached (e.g. relay decoded from buffered chunks before any peer joined)
// are left for TTL cleanup.
func (s *session[CI, R]) idle() bool {
	if !s.everHadPeers {
		return false
	}
	if s.stage < stageReconstructed {
		return len(s.peers) == 0
	}
	for _, sp := range s.peers {
		if !sp.completed {
			return false
		}
	}
	return true
}

// drainPolls emits routing first so peers learn our state before
// receiving chunks, then dispatches all pending chunk sends.
func (s *session[CI, R]) drainPolls() {
	if routing, ok := s.strategy.PollRouting(false); ok {
		s.broadcastRouting(routing)
	}
	chunks := s.strategy.PollChunks()
	for _, c := range chunks {
		s.sendChunk(c.Peer, c.ChunkID, c.Data)
	}
}

// sendChunk marshals and pushes a chunk to the target peer's slot.
// If the slot is full, reports a failed send back to the strategy.
func (s *session[CI, R]) sendChunk(peer identity.PeerID, chunkID CI, data []byte) {
	handle := chunkID.Handle()
	chunkIDBytes, err := chunkID.Marshal()
	if err != nil {
		s.strategy.ChunkSent(peer, handle, ErrChunkMarshal)
		return
	}
	sp := s.peers[peer]
	if sp == nil {
		s.strategy.ChunkSent(peer, handle, ErrChunkPeerGone)
		return
	}
	ctx, cancel := context.WithCancel(s.sessCtx)
	key := outboundKey{peer: peer, handle: handle}
	chunk := peerSendChunk{
		peerID:      peer,
		channelID:   s.channelID,
		messageID:   s.messageID,
		handle:      handle,
		chunkID:     chunkIDBytes,
		payload:     data,
		ctx:         ctx,
		resultCh:    s.channelInbox,
		sessionDone: s.sessCtx.Done(),
	}
	select {
	case sp.chunkOutbox <- chunk:
		s.outboundCancels[key] = cancel
		select {
		case sp.conn.wakeCh <- struct{}{}:
		default:
		}
	default:
		cancel()
		s.strategy.ChunkSent(peer, handle, ErrChunkSlotFull)
	}
}

func (s *session[CI, R]) broadcastRouting(update R) {
	data, err := update.Marshal()
	if err != nil {
		return
	}
	for pid, sp := range s.peers {
		if sp.completed {
			continue
		}
		if sp.conn.ctrlState.setRouting(peerSendRouting{
			channelID: s.channelID,
			messageID: s.messageID,
			update:    data,
		}) {
			s.observer.OnRoutingUpdate(pid, s.channelID, s.messageID)
		}
	}
}
