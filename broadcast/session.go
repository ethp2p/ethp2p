package broadcast

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ethp2p/ethp2p/transport"
)

const maxConcurrentReads = 64

// outboundKey identifies an in-flight outbound chunk send to a specific peer.
// handle is the strategy-assigned opaque token from ChunkID.Handle().
type outboundKey struct {
	peer   PeerID
	handle ChunkHandle
}

// dedupGroup tracks a shared cancellation context for concurrent reads
// of chunks with the same dedup key. When one read succeeds with
// VerdictAccepted, the group is cancelled, aborting the others.
type dedupGroup struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// sessionPeer is Session's per-peer state.
type sessionPeer struct {
	conn        *PeerConn
	stats       *PeerSessionStats
	chunkOutbox chan peerSendChunk
	completed   bool
}

// session is the per-message state struct, generic on chunk identifier CI and
// routing type R. All Strategy mutations happen inside the owning
// Channel's run() goroutine. Implements the exported Session interface.
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
	// maybeDispose uses it to distinguish "no peers yet" (wait for
	// peer attachment) from "all peers gone" (dispose).
	everHadPeers bool
	peers        map[PeerID]*sessionPeer
	channelInbox chan<- channelEvent
	observer     Observer

	// outboundCancels tracks cancel functions for in-flight chunk sends.
	// When a routing update reveals the peer already has the chunk, the
	// send is cancelled via this map.
	outboundCancels map[outboundKey]context.CancelFunc

	// readSem bounds the number of concurrent inbound data reads.
	readSem chan struct{}

	// dedupGroups maps dedup keys to cancel functions. All concurrent
	// reads with the same non-nil key share a context; when one
	// completes with VerdictAccepted, the cancel cancels the others.
	dedupGroups map[string]*dedupGroup

	// pendingVerify tracks chunks awaiting async verification
	// (VerdictPending from VerifyChunk). Keyed by dedup group key
	// (or a synthetic key when dedup is nil). When a dedup group
	// resolves, pending verifications in that group are cancelled.
	pendingVerify map[string]context.CancelFunc

	// wg tracks in-flight background goroutines (chunk reads, decode)
	// for graceful shutdown.
	wg sync.WaitGroup

	// sessCtx is a child of the channel context. Cancelling it cancels all
	// outstanding outbound chunk sends AND inbound reads for this session.
	sessCtx    context.Context
	sessCancel context.CancelFunc

	// ctx is the channel's context, used as a fallback in select statements
	// for channel sends (peerOpenSession, chunkOutbox).
	ctx  context.Context
	done chan struct{}
}

// Close performs synchronous cleanup. Called from the channel goroutine only.
// Cancels all outbound sends and inbound reads, waits for background
// goroutines to finish, closes strategy, notifies peers, closes the done channel.
func (s *session[CI, R]) Close() error {
	s.notifyPeersSessionDone()
	s.sessCancel()
	s.wg.Wait()
	s.strategy.Close()
	close(s.done)
	return nil
}

// Done returns a channel that is closed when the session is disposed.
func (s *session[CI, R]) Done() <-chan struct{} {
	return s.done
}

// MessageID returns the session's message ID.
func (s *session[CI, R]) MessageID() MessageID {
	return s.messageID
}

// notifyPeersComplete sends peerCloseStream to all peers,
// telling them we no longer need chunks. Called as soon as the
// strategy signals completeness (before decode), since decode failure
// is non-recoverable.
func (s *session[CI, R]) notifyPeersComplete() {
	for _, sp := range s.peers {
		select {
		case sp.conn.ctrlQ <- peerCloseStream{channelID: s.channelID, messageID: s.messageID}:
		case <-sp.conn.ctx.Done():
		default:
		}
	}
}

// signalReconstructed marks the session as reconstructed and attempts
// disposal. Called after a successful decode.
func (s *session[CI, R]) signalReconstructed() {
	s.stage = stageReconstructed
	s.maybeDispose()
}

// notifyPeersSessionDone sends peerCloseSession to each peer's ctrlQ
// so the outbound loop can evict the peerSessionState entry. Sends to
// all peers including completed ones, because completed peers may still
// have outbound loop entries (signalReconstructed only closed the SESS
// stream, it didn't remove the chunk slot).
func (s *session[CI, R]) notifyPeersSessionDone() {
	for _, sp := range s.peers {
		select {
		case sp.conn.ctrlQ <- peerCloseSession{channelID: s.channelID, messageID: s.messageID}:
		case <-sp.conn.ctx.Done():
		}
	}
}

// handleChunkStream decides whether to read data from an inbound chunk
// stream. If the chunk is needed, it acquires a semaphore slot, resolves
// a dedup group, and spawns a goroutine to read the data. The goroutine
// posts a channelChunkData back to the channel inbox on completion.
func (s *session[CI, R]) handleChunkStream(peer PeerID, chunkID []byte, dataLen uint32, stream transport.ReceiveStream) {
	chunk := s.newCI()
	if err := chunk.Unmarshal(chunkID); err != nil {
		s.observer.OnChunkError(ChunkProcessError{
			Peer:      peer,
			ChannelID: s.channelID,
			MessageID: s.messageID,
			Err:       fmt.Errorf("unmarshal chunk id: %w", err),
		})
		stream.CancelRead(0)
		return
	}
	if dataLen > maxChunkDataSize {
		s.observer.OnChunkError(ChunkProcessError{
			Peer:      peer,
			ChannelID: s.channelID,
			MessageID: s.messageID,
			Err:       fmt.Errorf("chunk data length %d exceeds max %d", dataLen, maxChunkDataSize),
		})
		stream.CancelRead(0)
		return
	}
	if s.stage >= stageDecoding || s.strategy.HaveChunk(chunk) {
		v := VerdictRedundant
		if s.stage >= stageReconstructed {
			v = VerdictSurplus
		} else if s.stage == stageDecoding {
			v = VerdictDecoding
		}
		s.observer.OnChunkRcvd(peer, s.channelID, s.messageID, v)
		stream.CancelRead(0)
		return
	}

	// Acquire semaphore (non-blocking).
	select {
	case s.readSem <- struct{}{}:
	default:
		stream.CancelRead(0)
		return
	}

	// Determine read context via dedup group.
	readCtx := s.sessCtx
	dedupKey := s.strategy.DedupKey(chunk)
	if dedupKey != nil {
		key := string(dedupKey)
		grp, ok := s.dedupGroups[key]
		if !ok {
			ctx, cancel := context.WithCancel(s.sessCtx)
			grp = &dedupGroup{ctx: ctx, cancel: cancel}
			s.dedupGroups[key] = grp
		}
		readCtx = grp.ctx
	}

	msgID := s.messageID
	inbox := s.channelInbox
	s.wg.Go(func() {
		defer func() { <-s.readSem }()

		// Watcher: cancel stream read when context is done.
		done := make(chan struct{})
		go func() {
			select {
			case <-readCtx.Done():
				stream.CancelRead(0)
			case <-done:
			}
		}()
		defer close(done)

		_ = stream.SetReadDeadline(time.Now().Add(chunkReadTimeout))
		defer func() { _ = stream.SetReadDeadline(time.Time{}) }()

		buf := make([]byte, dataLen)
		if _, err := io.ReadFull(stream, buf); err != nil {
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

// handleChunkData processes fully-read chunk data. Called on the channel
// goroutine via channelChunkData events.
func (s *session[CI, R]) handleChunkData(peer PeerID, chunkID []byte, payload []byte) {
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

	// Verify before accepting.
	verdict := s.strategy.VerifyChunk(peer, chunk, payload)
	switch verdict {
	case VerdictAccepted:
		s.acceptChunk(peer, chunk, payload)
	case VerdictPending:
		// Store a cancellable context keyed by dedup group so that
		// dedup resolution can cancel stale pending verifications.
		dedupKey := s.strategy.DedupKey(chunk)
		if dedupKey != nil {
			key := string(dedupKey)
			_, cancel := context.WithCancel(s.sessCtx)
			s.pendingVerify[key] = cancel
		}
	default:
		s.observer.OnChunkRcvd(peer, s.channelID, s.messageID, verdict)
	}
}

// handleVerifyResult processes the outcome of an async chunk
// verification. Called on the channel goroutine via channelVerifyResult.
func (s *session[CI, R]) handleVerifyResult(peer PeerID, chunkID []byte, payload []byte, verdict Verdict) {
	chunk := s.newCI()
	if err := chunk.Unmarshal(chunkID); err != nil {
		return
	}

	// Remove pending verification tracking.
	dedupKey := s.strategy.DedupKey(chunk)
	if dedupKey != nil {
		key := string(dedupKey)
		if cancel, ok := s.pendingVerify[key]; ok {
			cancel()
			delete(s.pendingVerify, key)
		}
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
func (s *session[CI, R]) acceptChunk(peer PeerID, chunk CI, payload []byte) {
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
// cleans up session bookkeeping (dedupGroups, pendingVerify).
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
		grp.cancel()
		delete(s.dedupGroups, key)
		if cancel, ok := s.pendingVerify[key]; ok {
			cancel()
			delete(s.pendingVerify, key)
		}
	}}
}

func (s *session[CI, R]) handleRoutingUpdate(peer PeerID, data []byte) {
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

func (s *session[CI, R]) handleSendComplete(peer PeerID, handle ChunkHandle, err error, size int) {
	if sp := s.peers[peer]; sp != nil {
		sp.stats.inflight--
	}
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
	s.everHadPeers = true
	chunkOutbox := make(chan peerSendChunk, 16)
	sp := &sessionPeer{
		conn:        p,
		stats:       NewPeerSessionStats(p.id),
		chunkOutbox: chunkOutbox,
	}
	s.peers[p.id] = sp
	s.strategy.AttachPeer(p.id, sp.stats)

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
	select {
	case p.ctrlQ <- peerOpenSession{
		channelID:      s.channelID,
		messageID:      s.messageID,
		preamble:       s.preamble,
		initialRouting: initialRouting,
		channelInbox:   s.channelInbox,
		chunkOutbox:    chunkOutbox,
	}:
	case <-s.ctx.Done():
		return
	}
	s.drainPolls()
}

func (s *session[CI, R]) handlePeerDropped(peer PeerID) {
	delete(s.peers, peer)
	s.strategy.DetachPeer(peer, false)
	s.maybeDispose()
}

func (s *session[CI, R]) handlePeerCompleted(peer PeerID) {
	sp, ok := s.peers[peer]
	if !ok || sp.completed {
		return
	}
	sp.completed = true
	for key, cancel := range s.outboundCancels {
		if key.peer == peer {
			cancel()
			delete(s.outboundCancels, key)
		}
	}
	s.strategy.DetachPeer(peer, true)
	s.maybeDispose()
}

// maybeDispose signals channelSessionDisposed when the session has no
// remaining work.
//
// A session is either "decoded" (origin, or relay that has reconstructed)
// or "not decoded" (relay still receiving chunks):
//
//   - Decoded: dispose when every peer has completed (decoded) or been
//     dropped. A non-completed peer still needs chunks from us.
//   - Not decoded: dispose when all peers have been dropped (removed from
//     map). Completed peers stay in the map because they may still send
//     us chunks on separate streams; only disconnection (drop) removes
//     them. If all peers are gone, the session can't make progress.
//
// Called from handlePeerDropped, handlePeerCompleted, and
// signalReconstructed. The channel's single-goroutine event loop guarantees
// these cannot fire during session creation or the peer-attach loop.
// Sessions with no peers ever attached (e.g. relay decoded from buffered
// chunks before any peer joined) are left for TTL cleanup.
func (s *session[CI, R]) maybeDispose() {
	if !s.everHadPeers {
		return
	}
	decoded := s.stage >= stageReconstructed
	if !decoded {
		if len(s.peers) > 0 {
			return
		}
	} else {
		for _, sp := range s.peers {
			if !sp.completed {
				return
			}
		}
	}
	select {
	case s.channelInbox <- channelSessionDisposed{messageID: s.messageID}:
	default:
	}
}

// drainPolls emits routing first so peers learn our state before
// receiving chunks, then dispatches all pending chunk sends.
func (s *session[CI, R]) drainPolls() {
	// Routing before chunks: peers update their view of our inventory
	// before the chunk dispatches arrive, reducing cross-sends.
	if routing, ok := s.strategy.PollRouting(false); ok {
		s.broadcastRouting(routing)
	}
	chunks := s.strategy.PollChunks()
	for _, c := range chunks {
		s.sendChunk(c.Peer, c.ChunkID, c.Data)
	}
}

// flushRouting broadcasts routing state if it changed since the last poll.
func (s *session[CI, R]) flushRouting() {
	if routing, ok := s.strategy.PollRouting(false); ok {
		s.broadcastRouting(routing)
	}
}

// sendChunk marshals and pushes a chunk to the target peer's slot.
// If the slot is full, reports a failed send back to the strategy.
func (s *session[CI, R]) sendChunk(peer PeerID, chunkID CI, data []byte) {
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
		sessionDone: s.done,
	}
	select {
	case sp.chunkOutbox <- chunk:
		s.outboundCancels[key] = cancel
		sp.stats.inflight++
		select {
		case sp.conn.wakeCh <- struct{}{}:
		default:
		}
	default:
		cancel()
		s.strategy.ChunkSent(peer, handle, ErrChunkSlotFull)
	}
}

// broadcastRouting marshals a routing update and sends it to all
// non-completed peers via their control queues.
func (s *session[CI, R]) broadcastRouting(update R) {
	data, err := update.Marshal()
	if err != nil {
		return
	}
	for pid, sp := range s.peers {
		if sp.completed {
			continue
		}
		select {
		case sp.conn.ctrlQ <- peerSendRouting{
			channelID: s.channelID,
			messageID: s.messageID,
			update:    data,
		}:
			s.observer.OnRoutingUpdate(pid, s.channelID, s.messageID)
		case <-sp.conn.ctx.Done():
		default:
			// Non-blocking: if the peer's control queue is full,
			// skip this routing update to avoid circular deadlock
			// between session goroutines and peer outbound loops.
		}
	}
}
