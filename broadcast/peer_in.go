package broadcast

import (
	"errors"
	"io"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/internal/ctxutil"
	"github.com/ethp2p/ethp2p/wire"
)

const (
	chunkReadTimeout = 5 * time.Second
	maxChunkDataSize = 1 << 20 // 1 MiB
)

// acceptBcast runs on the engine actor, which owns bcastAccepted.
func (p *PeerConn) acceptBcast(stream ethp2p.ReceiveStream) {
	// Each side opens exactly one outbound BCAST stream per connection, so a
	// second inbound one is a protocol violation, not a simultaneous open.
	if p.bcastAccepted {
		stream.CancelRead(wire.Refused)
		return
	}
	p.bcastAccepted = true
	// The sole admission owns the one-element handoff slot, so this send
	// never blocks the engine actor. BCAST is long-lived: keep its
	// one-stream admission bound instead of retaining a FIFO of duplicates.
	p.bcastIn <- stream
}

func (p *PeerConn) acceptSession(stream ethp2p.ReceiveStream) {
	// The accept loops are p.wg members, so this Go never races Close's Wait.
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
		// uni credit needed by CHUNK, so excess SESS is reset at once.
		stream.CancelRead(wire.Unspecified)
	}
}

func (p *PeerConn) acceptChunk(stream ethp2p.ReceiveStream) {
	select {
	case p.chunkSem <- struct{}{}:
	case <-p.ctx.Done():
		stream.CancelRead(wire.Unspecified)
		return
	}
	p.wg.Go(func() {
		defer func() { <-p.chunkSem }()
		p.processChunk(stream)
	})
}

// runInboundSession reads an inbound SESS stream: an OPEN frame (which may
// carry initial routing), then UPDATE frames. FIN means the peer disposed its
// session; a reset with Reconstructed means it reconstructed the object.
func (p *PeerConn) runInboundSession(s ethp2p.ReceiveStream) {
	stop := ctxutil.OnCancel(p.ctx, func() { s.CancelRead(streamFailureCode(p.ctx.Err())) })
	defer stop()

	so := new(bcastpb.Sess_Open)
	if err := readFrame(s, openFrame, so); err != nil {
		stop()
		// Reads never cancel a side, so end it here with the read error's code.
		s.CancelRead(streamCancellationCode(errors.Join(err, p.ctx.Err())))
		return
	}

	channelID := ChannelID(so.Channel)
	messageID := MessageID(so.ObjectId)

	p.engine.config.Observer.OnPreambleOpened(p.id, channelID, messageID)

	ch := p.channelInboxFor(channelID)
	if ch == nil {
		s.CancelRead(wire.Refused)
		return
	}
	stopChannel := ctxutil.OnCancel(ch.ctx, func() { s.CancelRead(wire.Unspecified) })
	defer stopChannel()

	select {
	case ch.inbox <- channelSessionOpen{
		peerID: p.id,
		msg:    so,
	}:
	case <-ch.ctx.Done():
		s.CancelRead(wire.Unspecified)
		return
	case <-p.ctx.Done():
		s.CancelRead(streamFailureCode(p.ctx.Err()))
		return
	}

	// Whatever ends the stream except Reconstructed removes this peer from the
	// session. If the connection itself is going away, handlePeerUnbound
	// covers it, so the event is skipped once p.ctx is done.
	reconstructed := false
	defer func() {
		if reconstructed {
			return
		}
		select {
		case ch.inbox <- channelPeerSessionEnded{messageID: messageID, peerID: p.id, peer: p}:
		case <-ch.ctx.Done():
		case <-p.ctx.Done():
		}
	}()

	for {
		ru := new(bcastpb.Sess_Update)
		if err := readFrame(s, updateFrame, ru); err != nil {
			stop()
			if errors.Is(err, io.EOF) {
				return
			}
			if re, ok := errors.AsType[*ethp2p.ResetError](err); ok {
				if re.Code == Reconstructed {
					reconstructed = true
					select {
					case ch.inbox <- channelPeerReconstructed{messageID: messageID, peerID: p.id}:
					case <-ch.ctx.Done():
					case <-p.ctx.Done():
					}
				}
				return
			}
			s.CancelRead(streamCancellationCode(errors.Join(err, p.ctx.Err())))
			return
		}
		select {
		case ch.inbox <- channelRoutingUpdate{peerID: p.id, messageID: messageID, msg: ru}:
		case <-ch.ctx.Done():
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
	err := readFrame(s, headerFrame, &frame)
	stop()
	if err != nil {
		// Reads never cancel a side, so end it here with the read error's code.
		s.CancelRead(streamCancellationCode(errors.Join(err, p.ctx.Err())))
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

	select {
	case ch.inbox <- channelChunkStream{peerID: p.id, frame: &frame, stream: s}:
		// Channel.drainInbox starts only after ch.ctx ends, so a send it
		// missed is visible here.
		if ch.ctx.Err() != nil {
			s.CancelRead(wire.Unspecified)
		}
	case <-ch.ctx.Done():
		s.CancelRead(wire.Unspecified)
	case <-p.ctx.Done():
		s.CancelRead(streamFailureCode(p.ctx.Err()))
	}
}
