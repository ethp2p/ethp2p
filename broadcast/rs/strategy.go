package rs

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"

	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/klauspost/reedsolomon"
)

// Compile-time interface check.
var _ broadcast.Strategy[*ChunkIdent, *Bitmap] = (*strategy)(nil)

// strategy is the unified per-session RS implementation of
// broadcast.Strategy[*ChunkIdent, *Bitmap].
type strategy struct {
	config   *Config
	preamble Preamble
	isOrigin bool

	totalChunks  int
	chunks       [][]byte // indexed by chunk; nil = not received
	emitPlanner  *emitPlanner
	peers        map[broadcast.PeerID]*peerState
	routingDirty bool

	// Per-relay random seed for deterministic priority ordering:
	// different relays prioritize different shards when emit counts
	// are equal, reducing duplicate transmissions across the network.
	seed uint64

	// Bitmap representing which chunks we have locally.
	have     Bitmap
	complete bool // cached completeness; set once in TakeChunk
}

// peerState tracks per-peer dispatch state within a session.
type peerState struct {
	bitmap    Bitmap
	stats     *broadcast.PeerSessionStats
	inflight  []int // per-chunk in-flight send count
	completed bool
}

func newOriginStrategy(config *Config, payload []byte) (*strategy, error) {
	msgLen := len(payload)
	preamble, chunkLen := initPreamble(config, msgLen)

	enc, err := reedsolomon.New(preamble.DataChunks, preamble.ParityChunks)
	if err != nil {
		return nil, fmt.Errorf("RS: failed to create encoder: %w", err)
	}

	totalLen := chunkLen * preamble.TotalChunks()
	parityLen := totalLen - msgLen

	msg := slices.Clone(payload)
	msg = slices.Grow(msg, parityLen)
	msg = msg[:totalLen]

	shards := slices.Collect(slices.Chunk(msg, chunkLen))
	if err := enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("RS encode failed: %w", err)
	}

	chunks := make([][]byte, len(shards))
	planner := newEmitPlanner()
	chunkHashes := make([][]byte, len(shards))

	for i, data := range shards {
		chunks[i] = data
		planner.Insert(emitEntry{Idx: i})
		h := sha256.Sum256(data)
		chunkHashes[i] = h[:]
	}

	msgHash := sha256.Sum256(payload)
	preamble.ChunkHashes = chunkHashes
	preamble.MessageHash = msgHash[:]

	total := preamble.TotalChunks()

	return &strategy{
		config:       config,
		preamble:     preamble,
		isOrigin:     true,
		totalChunks:  total,
		chunks:       chunks,
		emitPlanner:  planner,
		peers:        make(map[broadcast.PeerID]*peerState),
		have:         AllOnesBitmap(total),
		routingDirty: true,
	}, nil
}

func newRelayStrategy(config *Config, preamble *Preamble) (*strategy, error) {
	if preamble == nil {
		return nil, fmt.Errorf("nil preamble")
	}
	cfg := *config
	if cfg.ForwardMultiplier <= 0 {
		cfg.ForwardMultiplier = 4
	}

	total := preamble.TotalChunks()

	return &strategy{
		config:      &cfg,
		preamble:    *preamble,
		isOrigin:    false,
		totalChunks: total,
		chunks:      make([][]byte, total),
		emitPlanner: newEmitPlanner(),
		peers:       make(map[broadcast.PeerID]*peerState),
		seed:        rand.Uint64(),
		have:        NewBitmap(total),
	}, nil
}

func (s *strategy) HaveChunk(chunkID *ChunkIdent) bool {
	idx := chunkID.Index
	if idx < 0 || idx >= s.totalChunks {
		return true
	}
	return s.have.Get(idx)
}

func (s *strategy) DedupKey(chunkID *ChunkIdent) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(chunkID.Index))
	return buf[:]
}

func (s *strategy) AttachPeer(peer broadcast.PeerID, stats *broadcast.PeerSessionStats) {
	ps := &peerState{
		bitmap:   NewBitmap(s.totalChunks),
		stats:    stats,
		inflight: make([]int, s.totalChunks),
	}
	s.peers[peer] = ps
}

func (s *strategy) DetachPeer(peer broadcast.PeerID, completed bool) {
	ps, ok := s.peers[peer]
	if !ok {
		return
	}
	if completed {
		ps.completed = true
		return
	}
	delete(s.peers, peer)
}

func (s *strategy) VerifyChunk(_ broadcast.PeerID, chunkID *ChunkIdent, data []byte) broadcast.Verdict {
	if chunkID.Index < 0 || chunkID.Index >= s.totalChunks {
		return broadcast.VerdictInvalid
	}
	return s.verifyChunk(chunkID.Index, data)
}

func (s *strategy) Verified() <-chan broadcast.VerifyResult[*ChunkIdent] {
	return nil
}

func (s *strategy) TakeChunk(peer broadcast.PeerID, chunkID *ChunkIdent, data []byte, dedup *broadcast.DedupCancel) (broadcast.Verdict, bool, error) {
	if chunkID.Index < 0 || chunkID.Index >= s.totalChunks {
		return broadcast.VerdictInvalid, false, nil
	}

	// Once complete, the decode goroutine may be reading s.chunks
	// concurrently; freeze further writes (see tryDecode).
	if s.complete {
		return broadcast.VerdictRedundant, false, nil
	}

	idx := chunkID.Index

	if s.chunks[idx] != nil {
		return broadcast.VerdictRedundant, false, nil
	}

	s.chunks[idx] = data
	s.have.Set(idx)
	s.routingDirty = true
	s.emitPlanner.Insert(emitEntry{Idx: idx, Times: 0, Priority: s.chunkPriority(idx)})
	dedup.Cancel()

	if ps, ok := s.peers[peer]; ok {
		ps.bitmap.Set(idx)
	}

	if !s.complete && s.have.OnesCount() >= s.preamble.DataChunks {
		s.complete = true
	}
	return broadcast.VerdictAccepted, s.complete, nil
}

func (s *strategy) Decode() ([]byte, error) {
	return s.tryDecode()
}

func (s *strategy) RoutingUpdate(peer broadcast.PeerID, update *Bitmap) ([]broadcast.ChunkHandle, error) {
	ps, ok := s.peers[peer]
	if !ok {
		return nil, nil
	}
	if update != nil {
		ps.bitmap.Or(*update)
	}
	var cancel []broadcast.ChunkHandle
	for idx, count := range ps.inflight {
		if count > 0 && ps.bitmap.Get(idx) {
			cancel = append(cancel, broadcast.ChunkHandle(idx))
		}
	}
	return cancel, nil
}

func (s *strategy) PollChunks() []broadcast.ChunkDispatch[*ChunkIdent] {
	var chunks []broadcast.ChunkDispatch[*ChunkIdent]
	for peerID, ps := range s.peers {
		if ps.completed {
			continue
		}
		chunkID, data, ok := s.allocate(peerID, ps)
		if !ok {
			continue
		}
		chunks = append(chunks, broadcast.ChunkDispatch[*ChunkIdent]{
			Peer:    peerID,
			ChunkID: chunkID,
			Data:    data,
		})
	}
	return chunks
}

func (s *strategy) PollRouting(force bool) (*Bitmap, bool) {
	if (s.routingDirty || force) && s.shouldEmitRouting() {
		s.routingDirty = false
		bm := s.have.Clone()
		return &bm, true
	}
	return nil, false
}

// allocate selects the next chunk for the given peer and marks it
// in-flight immediately.
func (s *strategy) allocate(peer broadcast.PeerID, ps *peerState) (*ChunkIdent, []byte, bool) {
	var skipped []emitEntry

	var foundIdx int
	var foundData []byte
	foundOK := false
	for s.emitPlanner.Len() > 0 {
		ec, _ := s.emitPlanner.Top()
		if ps.bitmap.Get(ec.Idx) || ps.inflight[ec.Idx] > 0 ||
			(!s.isOrigin && ec.Sent >= s.config.ForwardMultiplier) {
			skipped = append(skipped, s.emitPlanner.PopFront())
			continue
		}
		foundIdx = ec.Idx
		foundData = s.chunks[ec.Idx]
		foundOK = true
		break
	}

	for _, ec := range skipped {
		s.emitPlanner.Insert(ec)
	}

	if !foundOK {
		return nil, nil, false
	}

	chunkID := &ChunkIdent{
		Index: foundIdx,
	}

	ps.inflight[foundIdx]++
	s.emitPlanner.Increment(foundIdx)

	return chunkID, foundData, true
}

func (s *strategy) ChunkSent(peer broadcast.PeerID, handle broadcast.ChunkHandle, err error) {
	ps := s.peers[peer]
	if ps == nil {
		return
	}

	idx := int(handle)
	if idx < 0 || idx >= s.totalChunks {
		return
	}

	if ps.inflight[idx] <= 0 {
		return
	}
	ps.inflight[idx]--

	if err == nil {
		ps.bitmap.Set(idx)
		if !s.isOrigin {
			s.emitPlanner.AddSent(idx, 1)
		}
	} else {
		ps.bitmap.Unset(idx)
		// Refund: if the chunk's send budget was exhausted, grant
		// one more attempt. The failed send didn't reach the peer,
		// so it shouldn't count against ForwardMultiplier.
		if !s.isOrigin {
			if sent, ok := s.emitPlanner.GetSent(idx); ok && sent >= s.config.ForwardMultiplier {
				s.emitPlanner.AddSent(idx, -1)
			}
		}
	}
}

func (s *strategy) Progress() (have, need int) {
	return s.have.OnesCount(), s.preamble.DataChunks
}

func (s *strategy) Work() <-chan struct{} { return nil }

func (s *strategy) Close() error { return nil }

// --- Internal helpers ---

func (s *strategy) verifyChunk(idx int, data []byte) broadcast.Verdict {
	if idx < 0 || idx >= len(s.preamble.ChunkHashes) {
		return broadcast.VerdictInvalid
	}
	hash := sha256.Sum256(data)
	if bytes.Equal(hash[:], s.preamble.ChunkHashes[idx]) {
		return broadcast.VerdictAccepted
	}
	return broadcast.VerdictInvalid
}

// tryDecode reconstructs the original message. Safe to call from a
// background goroutine: after completeness, TakeChunk rejects further
// writes, so s.chunks is frozen. The clone is needed because
// ReconstructData mutates the slice.
func (s *strategy) tryDecode() ([]byte, error) {
	shards := slices.Clone(s.chunks)

	enc, err := reedsolomon.New(s.preamble.DataChunks, s.preamble.ParityChunks)
	if err != nil {
		return nil, fmt.Errorf("create RS encoder: %w", err)
	}

	if err := enc.ReconstructData(shards); err != nil {
		return nil, fmt.Errorf("RS reconstruct failed: %w", err)
	}

	dataLen := s.preamble.DataChunks * len(shards[0])
	data := make([]byte, 0, dataLen)
	for i := range s.preamble.DataChunks {
		data = append(data, shards[i]...)
	}

	if s.preamble.MessageLength <= 0 || s.preamble.MessageLength > len(data) {
		return nil, fmt.Errorf("invalid message length: %d (data: %d)", s.preamble.MessageLength, len(data))
	}
	data = data[:s.preamble.MessageLength]

	hash := sha256.Sum256(data)
	if !bytes.Equal(hash[:], s.preamble.MessageHash) {
		return nil, fmt.Errorf("message hash mismatch")
	}

	return data, nil
}

// chunkPriority computes a deterministic priority for a shard index
// using the relay's seed. Different seeds produce different orderings.
func (s *strategy) chunkPriority(idx int) uint32 {
	// Fibonacci hashing with golden ratio constant for good dispersion.
	return uint32((s.seed ^ uint64(idx)*0x9E3779B97F4A7C15) >> 32)
}

// shouldEmitRouting gates whether the strategy should broadcast its
// bitmap. Called from PollRouting, which the session invokes after
// every state-mutating event and on a 25ms routing ticker.
func (s *strategy) shouldEmitRouting() bool {
	if s.config.DisableBitmap {
		return false
	}
	if s.config.BitmapThreshold <= 0 {
		return true
	}
	pct := s.have.OnesCount() * 100 / s.totalChunks
	return pct >= s.config.BitmapThreshold
}
