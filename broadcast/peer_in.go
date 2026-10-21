package broadcast

import (
	"errors"
	"fmt"
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

// runOpener reads opening messages until the binding ends.
func (p *PeerConn) runOpener() {
	for {
		if p.ctx.Err() != nil {
			return
		}
		if stream, ok := p.unread.pop(); ok {
			p.open(stream)
			continue
		}
		select {
		case <-p.unread.ready:
		case <-p.ctx.Done():
			return
		}
	}
}

// open reads s's opening message and routes the stream by it. A malformed,
// oversized, unknown or misplaced opening message is a peer violation; a
// timeout, a reset or the binding's end is not.
func (p *PeerConn) open(s ethp2p.ReceiveStream) {
	_ = s.SetReadDeadline(time.Now().Add(openingTimeout))
	stop := ctxutil.OnCancel(p.ctx, func() { s.CancelRead(streamFailureCode(p.ctx.Err())) })
	typ, data, err := wire.ReadFrameLimit(s, openingLimit)
	stop()
	// Clear the deadline so it doesn't leak into the stream's later reads,
	// which set their own.
	_ = s.SetReadDeadline(time.Time{})
	if p.ctx.Err() != nil {
		s.CancelRead(streamFailureCode(p.ctx.Err()))
		return
	}
	if err != nil {
		if isPeerFault(err) {
			p.violate(s, fmt.Errorf("opening message: %w", err))
			return
		}
		// Reads never cancel a side, so end it here with the read error's code.
		s.CancelRead(streamCancellationCode(err))
		return
	}
	switch typ {
	case handshakeFrame:
		msg := new(bcastpb.Bcast_Handshake)
		if err := unmarshalFrame(data, msg); err != nil {
			p.violate(s, err)
			return
		}
		// Each side opens exactly one control stream per binding, so a
		// second HANDSHAKE is a violation, not a simultaneous open.
		if !p.bcastTaken.CompareAndSwap(false, true) {
			p.violate(s, errors.New("second HANDSHAKE"))
			return
		}
		p.handshakeIn <- inboundHandshake{stream: s, msg: msg}
	case openFrame:
		msg := new(bcastpb.Sess_Open)
		if err := unmarshalFrame(data, msg); err != nil {
			p.violate(s, err)
			return
		}
		p.admit(opening{stream: s, open: msg})
	case headerFrame:
		msg := new(bcastpb.Chunk_Header)
		if err := unmarshalFrame(data, msg); err != nil {
			p.violate(s, err)
			return
		}
		p.admit(opening{stream: s, header: msg})
	default:
		p.violate(s, fmt.Errorf("frame type %d cannot open a stream", typ))
	}
}

// admit holds an opening until the handshake completes, then dispatches it.
func (p *PeerConn) admit(o opening) {
	p.parkMu.Lock()
	if !p.handshook {
		p.parked = append(p.parked, o)
		p.parkMu.Unlock()
		return
	}
	p.parkMu.Unlock()
	p.dispatch(o)
}

// dispatchParked marks the handshake complete and dispatches the openings
// held until then. Run starts it once the engine has bound the peer.
func (p *PeerConn) dispatchParked() {
	p.parkMu.Lock()
	p.handshook = true
	parked := p.parked
	p.parked = nil
	p.parkMu.Unlock()
	for _, o := range parked {
		p.dispatch(o)
	}
}

// dispatch runs on a p.wg member, so the session readers it starts never race
// Close's wg.Wait.
func (p *PeerConn) dispatch(o opening) {
	if o.open != nil {
		p.acceptSession(o.stream, o.open)
		return
	}
	p.routeChunk(o.stream, o.header)
}

// violate refuses s and ends the binding with a violation cause. Run's exit
// reports it through OnPeerGone and closes the stack handle.
func (p *PeerConn) violate(s ethp2p.ReceiveStream, err error) {
	s.CancelRead(wire.Refused)
	p.cancel(peerViolation(err))
}

func peerViolation(err error) error { return fmt.Errorf("%w: %w", ErrPeerViolation, err) }

func (p *PeerConn) acceptSession(stream ethp2p.ReceiveStream, open *bcastpb.Sess_Open) {
	select {
	case p.sessionSem <- struct{}{}:
		p.wg.Go(func() {
			defer func() { <-p.sessionSem }()
			p.runInboundSession(stream, open)
		})
	case <-p.ctx.Done():
		stream.CancelRead(wire.Unspecified)
	default:
		// A session needs chunk streams to finish; waiting here could exhaust
		// the stream credit those need, so excess sessions are reset at once.
		stream.CancelRead(wire.Unspecified)
	}
}

// runInboundSession reads a session stream after its OPEN: a PREAMBLE frame,
// then UPDATE frames. FIN means the peer disposed its session; a reset with
// Reconstructed means it reconstructed the object.
func (p *PeerConn) runInboundSession(s ethp2p.ReceiveStream, so *bcastpb.Sess_Open) {
	stop := ctxutil.OnCancel(p.ctx, func() { s.CancelRead(streamFailureCode(p.ctx.Err())) })
	defer stop()

	preamble := new(bcastpb.Sess_Preamble)
	_ = s.SetReadDeadline(time.Now().Add(openingTimeout))
	err := readFrame(s, preambleFrame, preamble)
	_ = s.SetReadDeadline(time.Time{})
	if err != nil {
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
		peerID:   p.id,
		msg:      so,
		preamble: preamble.Data,
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

// routeChunk hands a chunk stream, positioned after its HEADER, to its channel.
func (p *PeerConn) routeChunk(s ethp2p.ReceiveStream, header *bcastpb.Chunk_Header) {
	ch := p.channelInboxFor(ChannelID(header.Channel))
	if ch == nil {
		s.CancelRead(wire.Refused)
		return
	}

	select {
	case ch.inbox <- channelChunkStream{peerID: p.id, frame: header, stream: s}:
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
