package broadcast

import (
	"context"
	"io"
	"time"
)

// DedupCancel is a handle the session passes into TakeChunk so the
// strategy can cancel the dedup group when it decides the group is
// satisfied. RS cancels on every accept (a shard is unique). RLNC
// cancels when the generation reaches full rank. Nil-safe: calling
// Cancel on a nil receiver is a no-op.
type DedupCancel struct {
	context.CancelFunc
}

func (d *DedupCancel) Cancel() {
	if d != nil {
		d.CancelFunc()
	}
}

// Wire is the constraint for wire-level types that know how to round-trip
// themselves to/from bytes. Types satisfy this via pointer receivers; generic
// code uses pointer type params so that the method set
// includes both Marshal and Unmarshal.
//
// Generic code cannot construct new instances from a type parameter alone (the
// zero value of pointer types is nil), so Scheme provides factory functions
// (NewCI, NewR, NewP) for allocation in generic contexts.
type Wire interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

// ChunkIdent is the strategy-specific chunk identifier carried on the wire
// before chunk data. Handle returns the opaque correlation token the
// session threads through the send/complete path (outbound tracking,
// ChunkSent, RoutingUpdate cancel handles).
type ChunkIdent interface {
	Wire
	Handle() ChunkHandle
}

type (
	// TODO formally define PeerID in ethp2p
	PeerID string
	// TODO MessageID could be a structured / nested token, so the app can dispose all sessions related to, e.g. slot N
	MessageID string
	// TODO ChannelID will be abbreviated after handshake.
	ChannelID string
	// TODO assuming a simple ordinal
	ProtocolVersion uint
	// ChunkHandle is an opaque strategy-assigned token threaded through
	// the send/complete path for correlation.
	ChunkHandle uint64
)

const ProtocolV1 uint32 = 1

// Verdict classifies the result of processing an inbound chunk.
type Verdict uint8

const (
	// VerdictAccepted indicates that the chunk was useful in advancing decoding.
	VerdictAccepted Verdict = iota
	// VerdictRedundant indicates that the chunk carries no new information:
	// we already have it, the generation/shard is full, or we have enough
	// data to decode.
	VerdictRedundant
	// VerdictDecoding indicates that the chunk arrived after completeness
	// was signalled to peers but before decode finished. These chunks are
	// in-flight leftovers from the network.
	VerdictDecoding
	// VerdictSurplus indicates that the chunk arrived after the session
	// has fully reconstructed the message.
	VerdictSurplus
	// VerdictInvalid indicates that this chunk was malformed.
	VerdictInvalid
	// VerdictPending indicates that verification has been submitted to an
	// async worker pool. The result will arrive on the Verified() channel.
	VerdictPending
)

// PeerSessionStats tracks per-peer per-session statistics. The session
// owns and mutates these (same goroutine as strategy calls). Strategy
// implementations read via getter methods; unexported fields prevent
// external writes.
type PeerSessionStats struct {
	peerID   PeerID
	sent     int
	recv     int
	inflight int
	latency  time.Duration
}

// NewPeerSessionStats creates stats for the given peer.
func NewPeerSessionStats(peer PeerID) *PeerSessionStats {
	return &PeerSessionStats{peerID: peer}
}

func (s *PeerSessionStats) PeerID() PeerID         { return s.peerID }
func (s *PeerSessionStats) Sent() int              { return s.sent }
func (s *PeerSessionStats) Recv() int              { return s.recv }
func (s *PeerSessionStats) Inflight() int          { return s.inflight }
func (s *PeerSessionStats) Latency() time.Duration { return s.latency }

// VerifyResult carries the outcome of an async chunk verification
// submitted via VerifyChunk. Posted to the Verified() channel and
// processed on the channel goroutine.
type VerifyResult[CI ChunkIdent] struct {
	Peer    PeerID
	ChunkID CI
	Data    []byte
	Verdict Verdict
}

// FullMessage represents a fully decoded message.
type FullMessage struct {
	ChannelID ChannelID
	MessageID MessageID
	Data      []byte
}

// Session is the public non-generic interface for session observation.
// The concrete session[C, R] implements this.
type Session interface {
	io.Closer
	Done() <-chan struct{}
	MessageID() MessageID
}

// ---------------------------------------------------------------------------
// Strategy types
// ---------------------------------------------------------------------------

// Strategy is the per-session interface that unifies coding, routing,
// and dispatch. One instance per (channel, message, direction). Created
// by Scheme factory functions.
//
// # Threading model
//
// All methods are called from a single goroutine (the owning Channel's
// run loop), with two exceptions:
//
//   - Work() and Verified() return channels; the channels themselves
//     are read by the channel goroutine, but the strategy may write to
//     them from internal goroutines (e.g. an async encoder or
//     verification worker pool).
//   - VerifyChunk() reads only immutable state set at construction
//     (preamble hashes, commitment parameters). It may submit work to
//     an internal worker pool whose results arrive on Verified().
//   - Decode() is called from a background goroutine after TakeChunk
//     signals completeness. Safe because the strategy rejects further
//     chunks after completeness, freezing the state for concurrent reads.
//   - Close() is called after all in-flight reads have drained, so no
//     concurrent calls are in progress.
//
// Because of single-goroutine access, implementations do not need
// internal locking for session state. The exception is strategies that
// produce chunks asynchronously (e.g. RLNC origin encoding), which
// must synchronize the producer goroutine with PollChunks.
//
// # Lifecycle
//
// Factory (Scheme.NewOrigin / Scheme.NewRelay)
//
//	→ AttachPeer (one or more)
//	→ [VerifyChunk | TakeChunk | RoutingUpdate | PollChunks | PollRouting | ChunkSent]*
//	→ TakeChunk returns complete=true (relay only)
//	→ Decode (background goroutine)
//	→ DetachPeer (for each attached peer)
//	→ Close
//
// The session calls drainPolls (PollRouting then PollChunks) after
// every state-mutating event: AttachPeer, VerifyChunk, TakeChunk,
// RoutingUpdate, and ChunkSent. Strategies make new work available by returning it
// from the next PollChunks call rather than pushing it.
//
// # Invariants the strategy must maintain
//
//   - Per-peer tracking: the strategy must track which peers are
//     attached and must not return dispatches targeting detached peers.
//   - In-flight accounting: for each dispatch returned by PollChunks,
//     the session will call ChunkSent exactly once with the dispatch's
//     Handle. The strategy must track its own in-flight state keyed
//     by handle to correlate the callback.
//   - Failure handling: when ChunkSent reports ok=false (slot full,
//     peer gone, or cancelled), the strategy decides whether to retry
//     (re-enqueue for the next PollChunks) or drop the chunk for that
//     peer. The session does not retry automatically.
//   - Routing consistency: RoutingUpdate must return the handles of
//     in-flight sends that are no longer needed after the update. The
//     session cancels those outbound sends directly.
type Strategy[CI ChunkIdent, R Wire] interface {
	io.Closer

	// HaveChunk reports whether this chunk has already been received
	// and processed. Used as a fast-path gate before TakeChunk: if
	// true, the session skips the read entirely.
	//
	// False positives are forbidden (they would drop needed chunks).
	// False negatives are harmless (TakeChunk handles duplicates).
	HaveChunk(chunkID CI) bool

	// DedupKey returns a grouping key for concurrent inbound reads.
	// All in-flight reads sharing a non-nil key share a cancellation
	// context; the strategy cancels the group via TakeChunk's dedup
	// handle when it decides the group is satisfied. Returns nil to
	// opt out of dedup grouping for this chunk.
	DedupKey(chunkID CI) []byte

	// AttachPeer registers a peer in this session. Called before
	// PollChunks can target this peer. The strategy should initialize
	// per-peer state (send tracking, routing, etc.).
	//
	// stats is owned by the session. Mutations happen only between
	// strategy calls; it is safe to retain the pointer.
	AttachPeer(peer PeerID, stats *PeerSessionStats)

	// DetachPeer removes a peer. completed=true means the peer
	// signaled successful reconstruction (SESS stream reset with
	// code 0x01); false means disconnection or unsubscribe. The
	// session cancels all in-flight sends for this peer before
	// calling DetachPeer, so any subsequent ChunkSent callbacks for
	// this peer will arrive with ok=false.
	DetachPeer(peer PeerID, completed bool)

	// VerifyChunk verifies an inbound chunk before acceptance. Returns
	// VerdictAccepted or VerdictInvalid for synchronous verification
	// (e.g. SHA-256). Returns VerdictPending when verification is
	// submitted to an async worker pool (e.g. BLS signatures, KZG
	// proofs); the result arrives on the Verified() channel.
	//
	// This is a documented exception to the single-goroutine rule:
	// it reads only immutable state set at construction (preamble
	// hashes, commitment parameters). Results are posted to
	// Verified() and processed on the channel goroutine.
	VerifyChunk(peer PeerID, chunkID CI, data []byte) Verdict

	// Verified returns a channel that delivers results of async
	// verifications submitted via VerifyChunk (VerdictPending).
	// Returns nil if the strategy never uses async verification.
	Verified() <-chan VerifyResult[CI]

	// TakeChunk delivers a pre-verified inbound chunk to the strategy.
	//
	// The session owns dedup group creation and lookup; it passes the
	// resulting cancel handle into TakeChunk via dedup. The strategy
	// decides when the group is satisfied and calls dedup.Cancel() at
	// that point. For RS, every accept satisfies the group (a shard
	// index is unique). For RLNC, the group is satisfied when the
	// generation reaches full rank, since multiple linearly independent
	// chunks per generation are all useful until then.
	//
	// dedup is nil when no dedup group exists for this chunk. Calling
	// Cancel on a nil *DedupCancel is safe.
	//
	// Never called for origin sessions (the session gates this).
	// HaveChunk is checked first, so duplicates that slip through
	// should still be handled gracefully.
	//
	// The bool return signals completeness: true means the strategy
	// has enough data to decode. The session calls Decode on a
	// background goroutine. After returning true, the strategy must
	// reject further chunks so the state remains frozen for the
	// concurrent Decode call.
	TakeChunk(peer PeerID, chunkID CI, data []byte, dedup *DedupCancel) (Verdict, bool, error)

	// RoutingUpdate delivers a peer's routing state (e.g. a bitmap
	// of which chunks they have). The strategy updates its internal
	// view of the peer's inventory so future PollChunks avoids
	// sending chunks the peer already has. Returns handles of
	// in-flight sends to this peer that are now redundant (the peer
	// acquired the data through another path).
	RoutingUpdate(peer PeerID, update R) (cancel []ChunkHandle, err error)

	// PollChunks returns pending chunk dispatches. Each dispatch
	// targets a specific peer and carries a ChunkID whose Marshal()
	// output will be passed back in ChunkSent. The session calls
	// PollChunks after every state-mutating event; returning nil
	// signals no work available.
	//
	// Must not return dispatches for detached or completed peers.
	PollChunks() []ChunkDispatch[CI]

	// PollRouting returns the current routing state for broadcast to
	// peers. force=false returns only if state changed since the last
	// poll; force=true returns unconditionally (used for the initial
	// routing update bundled with SessionOpen). The session calls
	// PollRouting before PollChunks so peers see updated routing
	// before chunks arrive.
	PollRouting(force bool) (R, bool)

	// ChunkSent reports the outcome of a chunk send. Called exactly
	// once per dispatch returned by PollChunks. handle is extracted
	// from ChunkID.Handle(). err=nil means the chunk was written to
	// the wire; non-nil carries the failure reason (ErrChunkSlotFull,
	// ErrChunkWriteFail, ErrChunkCancelled, etc.).
	ChunkSent(peer PeerID, handle ChunkHandle, err error)

	// Progress returns decode progress: chunks received and chunks needed.
	Progress() (have, need int)

	// Decode reconstructs the original message from the strategy's
	// complete state. Called from a background goroutine after
	// TakeChunk signals completeness. Safe for concurrent access
	// because the strategy rejects further chunks after completeness,
	// freezing the state.
	//
	// Decode failure is non-recoverable: neither RS nor RLNC can
	// evict a previously accepted chunk. Decode failure after
	// completeness therefore indicates a bug.
	Decode() ([]byte, error)

	// Work returns a channel signaled when new dispatches are
	// available from an async producer (e.g. background encoder).
	// Returns nil if the strategy never produces asynchronously.
	Work() <-chan struct{}
}

// Scheme is a per-channel factory for creating Strategy instances.
type Scheme[CI ChunkIdent, R Wire, P Wire] struct {
	Name      string
	NewOrigin func(MessageID, []byte) (Strategy[CI, R], P, error)
	NewRelay  func(MessageID, P) (Strategy[CI, R], error)
	NewCI     func() CI
	NewR      func() R
	NewP      func() P
}

// ChunkDispatch directs a chunk to a specific peer. The session
// extracts the correlation handle via ChunkID.Handle().
type ChunkDispatch[CI ChunkIdent] struct {
	Peer    PeerID
	ChunkID CI
	Data    []byte
}
