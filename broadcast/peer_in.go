package broadcast

import (
	"context"
	"errors"
	"io"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/internal/ctxutil"
	"github.com/ethp2p/ethp2p/internal/devhook"
	"github.com/ethp2p/ethp2p/internal/trace"
	"github.com/ethp2p/ethp2p/wire"
)

const (
	chunkReadTimeout = 5 * time.Second
	maxChunkDataSize = 1 << 20 // 1 MiB
)

func (p *PeerConn) acceptBcast(stream ethp2p.ReceiveStream) {
	if stream == nil {
		return
	}
	if p.ctx != nil && p.ctx.Err() != nil {
		stream.CancelRead(wire.Unspecified)
		return
	}
	// Each side opens exactly one outbound BCAST stream per connection, so a
	// second inbound one is a protocol violation, not a simultaneous open.
	if !p.bcastAccepted.CompareAndSwap(false, true) {
		stream.CancelRead(wire.Refused)
		return
	}
	// The sole successful admission owns the one-element handoff slot, so
	// this cannot block the engine actor. BCAST is long-lived: keep its
	// one-stream admission bound instead of retaining a FIFO of duplicates.
	select {
	case p.bcastIn <- stream:
	case <-p.ctx.Done():
		stream.CancelRead(wire.Unspecified)
	}
}

func (p *PeerConn) acceptSession(stream ethp2p.ReceiveStream) {
	if !p.awaitHandshake(stream) {
		return
	}
	p.handlersMu.Lock()
	defer p.handlersMu.Unlock()
	if !p.ready {
		if p.ctx.Err() != nil {
			stream.CancelRead(wire.Unspecified)
		} else {
			stream.CancelRead(wire.Refused)
		}
		return
	}
	select {
	case p.sessionSem <- struct{}{}:
		p.wg.Go(func() {
			defer func() { <-p.sessionSem }()
			p.runInboundSession(stream)
		})
	case <-p.ctx.Done():
		stream.CancelRead(wire.Unspecified)
	default:
		// SESS needs other streams to finish; waiting here could exhaust the
		// uni credit needed by CHUNK, so session capacity remains a refusal.
		stream.CancelRead(wire.Overloaded)
	}
}

func (p *PeerConn) acceptChunk(stream ethp2p.ReceiveStream) {
	if !p.awaitHandshake(stream) {
		return
	}
	// Wait outside handlersMu: shutdown must be able to stop new handlers.
	select {
	case p.chunkSem <- struct{}{}:
	case <-p.ctx.Done():
		if hooks := p.engine.config.hooks; hooks.Tracing() {
			hooks.Trace.Emit(trace.ChunkReadCancelled{Peer: string(p.id), Reason: trace.CancelBindClosed})
		}
		stream.CancelRead(wire.Unspecified)
		return
	}
	p.handlersMu.Lock()
	defer p.handlersMu.Unlock()
	if !p.ready {
		<-p.chunkSem
		if p.ctx.Err() != nil {
			stream.CancelRead(wire.Unspecified)
		} else {
			stream.CancelRead(wire.Refused)
		}
		return
	}
	p.wg.Go(func() {
		defer func() { <-p.chunkSem }()
		p.processChunk(stream)
	})
}

func (p *PeerConn) awaitHandshake(stream ethp2p.ReceiveStream) bool {
	select {
	case <-p.handshakeDone:
		if p.handshakeOK {
			return true
		}
	case <-p.ctx.Done():
	}
	if p.ctx.Err() != nil {
		stream.CancelRead(wire.Unspecified)
	} else {
		stream.CancelRead(wire.Refused)
	}
	return false
}

// runInboundSession reads frames from an inbound SESS stream.
// The first frame must be SessionOpen (which may carry initial routing);
// subsequent frames are RoutingUpdate. EOF signals the peer has
// reconstructed (completed).
func (p *PeerConn) runInboundSession(s ethp2p.ReceiveStream) {
	stop := ctxutil.OnCancel(p.ctx, func() { s.CancelRead(streamFailureCode(p.ctx.Err())) })
	defer stop()

	// Read the first frame: must be SessionOpen.
	var frame bcastpb.Sess
	if err := ReadFrame(s, &frame); err != nil {
		stop()
		// Reads never cancel a side, so end it here with the read error's code.
		s.CancelRead(streamCancellationCode(errors.Join(err, context.Cause(p.ctx))))
		return
	}
	so := frame.GetSessionOpen()
	if so == nil {
		s.CancelRead(wire.Refused)
		return
	}

	channelID := ChannelID(so.Channel)
	messageID := MessageID(so.MessageId)

	p.engine.config.Observer.OnPreambleOpened(p.id, channelID, messageID)

	ch := p.channelInboxFor(channelID)
	if ch == nil {
		s.CancelRead(wire.Refused)
		return
	}
	if !p.engine.config.hooks.Wait(p.ctx, devhook.Site{Point: devhook.PointSessionOpen, Peer: string(p.id), Channel: string(channelID), Message: string(messageID)}) {
		return
	}
	if !ch.add(s) {
		s.CancelRead(wire.Unspecified)
		return
	}
	defer ch.release(s)
	stopChannel := ctxutil.OnCancel(ch.ctx, func() { s.CancelRead(wire.Unspecified) })
	defer stopChannel()

	// Deliver SessionOpen to channel (with initial routing if present).
	select {
	case ch.inbox <- channelSessionOpen{
		peerID: p.id,
		msg:    so,
		peer:   p,
		stream: s,
	}:
	case <-ch.done:
		s.CancelRead(wire.Unspecified)
		return
	case <-p.ctx.Done():
		s.CancelRead(streamFailureCode(p.ctx.Err()))
		return
	}

	// Read loop: subsequent frames are RoutingUpdate.
	for {
		frame.Reset()
		if err := ReadFrame(s, &frame); err != nil {
			stop()
			if errors.Is(err, io.EOF) {
				return
			}
			if re, ok := errors.AsType[*ethp2p.ResetError](err); ok {
				if re.Code == Reconstructed {
					select {
					case ch.inbox <- channelPeerReconstructed{messageID: messageID, peerID: p.id}:
					case <-ch.done:
					case <-p.ctx.Done():
					}
				}
				return
			}
			s.CancelRead(streamCancellationCode(errors.Join(err, context.Cause(p.ctx))))
			return
		}
		ru := frame.GetRoutingUpdate()
		if ru == nil {
			s.CancelRead(wire.Refused)
			return
		}
		if !p.engine.config.hooks.Wait(p.ctx, devhook.Site{Point: devhook.PointRoutingUpdate, Peer: string(p.id), Channel: string(channelID), Message: string(messageID)}) {
			return
		}
		select {
		case ch.inbox <- channelRoutingUpdate{peerID: p.id, messageID: messageID, msg: ru}:
		case <-ch.done:
			s.CancelRead(wire.Unspecified)
			return
		case <-p.ctx.Done():
			s.CancelRead(streamFailureCode(p.ctx.Err()))
			return
		}
	}
}

func (p *PeerConn) processChunk(s ethp2p.ReceiveStream) {
	s.SetReadDeadline(time.Now().Add(chunkReadTimeout))

	var frame bcastpb.Chunk_Header
	stop := ctxutil.OnCancel(p.ctx, func() { s.CancelRead(streamFailureCode(p.ctx.Err())) })
	err := ReadFrame(s, &frame)
	stop()
	if err != nil {
		// Reads never cancel a side, so end it here with the read error's code.
		s.CancelRead(streamCancellationCode(errors.Join(err, context.Cause(p.ctx))))
		return
	}
	if p.ctx.Err() != nil {
		s.CancelRead(streamFailureCode(p.ctx.Err()))
		return
	}

	// Clear the frame read deadline so it doesn't leak into the
	// session's data read goroutine (which sets its own deadline).
	s.SetReadDeadline(time.Time{})

	ch := p.channelInboxFor(ChannelID(frame.Channel))
	if ch == nil {
		s.CancelRead(wire.Refused)
		return
	}

	held := p.holdChunk(s, ch)
	if !ch.add(held) {
		held.CancelRead(wire.Unspecified)
		return
	}
	if p.ctx.Err() != nil {
		held.CancelRead(wire.Unspecified)
		return
	}
	chnk := channelChunkStream{
		peerID: p.id,
		frame:  &frame,
		stream: held,
	}
	select {
	case ch.inbox <- chnk:
	case <-ch.done:
		chnk.stream.CancelRead(wire.Unspecified)
	case <-p.ctx.Done():
		chnk.stream.CancelRead(streamFailureCode(p.ctx.Err()))
	}
}
