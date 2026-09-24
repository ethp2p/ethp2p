package broadcast

import (
	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/protocol"
)

type chunkHolding uint8

const (
	chunkActive chunkHolding = iota
	chunkParked
	chunkQueued
)

// Two message-sized parked groups leave credit for SESS and useful CHUNK
// traffic even at the Interop limit, alongside the 64+64 SESS admission bound.
const maxPeerParkedChunks = 2 * maxParkedChunks

// heldChunk tracks post-header ownership until cancellation or data completion.
// Its peer owns the registry, so disconnect cancels even streams already handed
// to a channel that has not processed its peer-down event yet.
type heldChunk struct {
	ethp2p.ReceiveStream
	peer     *PeerConn
	delivery *channelDelivery
}

func (p *PeerConn) holdChunk(stream ethp2p.ReceiveStream, delivery *channelDelivery) *heldChunk {
	h := &heldChunk{ReceiveStream: stream, peer: p, delivery: delivery}
	p.chunkMu.Lock()
	if p.heldChunks == nil {
		p.heldChunks = make(map[*heldChunk]chunkHolding)
	}
	p.heldChunks[h] = chunkActive
	closed := p.ctx.Err() != nil
	p.chunkMu.Unlock()
	if closed {
		h.CancelRead(protocol.Unspecified)
	}
	return h
}

func (h *heldChunk) move(to chunkHolding) bool {
	p := h.peer
	p.chunkMu.Lock()
	defer p.chunkMu.Unlock()
	from, live := p.heldChunks[h]
	if !live {
		return false
	}
	if from == to {
		return true
	}
	if to == chunkParked && p.parkedChunks >= maxPeerParkedChunks {
		return false
	}
	if to == chunkQueued && p.queuedChunks >= p.engine.config.maxQueuedChunkStreams() {
		return false
	}
	if from == chunkParked {
		p.parkedChunks--
	}
	if from == chunkQueued {
		p.queuedChunks--
	}
	if to == chunkParked {
		p.parkedChunks++
	}
	if to == chunkQueued {
		p.queuedChunks++
	}
	p.heldChunks[h] = to
	return true
}

func (h *heldChunk) release() {
	p := h.peer
	p.chunkMu.Lock()
	if from, live := p.heldChunks[h]; live {
		if from == chunkParked {
			p.parkedChunks--
		}
		if from == chunkQueued {
			p.queuedChunks--
		}
		delete(p.heldChunks, h)
	}
	p.chunkMu.Unlock()
	if h.delivery != nil {
		h.delivery.release(h)
	}
}

func (h *heldChunk) CancelRead(code protocol.Code) {
	h.ReceiveStream.CancelRead(code)
	h.release()
}

func (p *PeerConn) cancelHeldChunks() {
	p.chunkMu.Lock()
	held := make([]*heldChunk, 0, len(p.heldChunks))
	for h := range p.heldChunks {
		held = append(held, h)
	}
	p.chunkMu.Unlock()
	for _, h := range held {
		h.CancelRead(protocol.Unspecified)
	}
}
