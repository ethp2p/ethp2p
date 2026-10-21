package broadcast

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/internal/ctxutil"
	"github.com/ethp2p/ethp2p/wire"
	"google.golang.org/protobuf/proto"
)

const chunkWriteTimeout = 5 * time.Second

type sessionKey struct {
	channelID ChannelID
	messageID MessageID
}

type slotUpdate struct {
	key  sessionKey
	slot <-chan peerSendChunk // nil = slot removed
}

// runCtrlReader reads and dispatches control messages from the peer's inbound
// control stream. When the stream ends, it stops this broadcast binding, with
// a violation cause if the peer sent a bad frame. The borrowed connection
// remains owned by the application; stream cancellation is limited to streams
// owned by this binding.
func (p *PeerConn) runCtrlReader() {
	var cause error
	defer func() { p.cancel(cause) }()
	stop := ctxutil.OnCancel(p.ctx, func() { p.ctrlIn.CancelRead(streamFailureCode(p.ctx.Err())) })
	defer stop()
	for {
		typ, data, err := wire.ReadFrame(p.ctrlIn, maxBroadcastFrame)
		if err != nil {
			stop()
			// FIN between frames ends the binding without a violation.
			if !errors.Is(err, io.EOF) && isPeerFault(err) {
				cause = peerViolation(fmt.Errorf("control stream: %w", err))
			}
			// Reads never cancel a side, so end it here with the read error's code.
			p.ctrlIn.CancelRead(streamCancellationCode(errors.Join(err, p.ctx.Err())))
			return
		}
		var channel string
		var handle func(*PeerConn, ChannelID)
		switch typ {
		case subscribeFrame:
			var msg bcastpb.Bcast_Subscribe
			err = unmarshalFrame(data, &msg)
			channel, handle = msg.Channel, p.engine.peerSubscribed
		case unsubscribeFrame:
			var msg bcastpb.Bcast_Unsubscribe
			err = unmarshalFrame(data, &msg)
			channel, handle = msg.Channel, p.engine.peerUnsubscribed
		default:
			// HANDSHAKE comes only first, and the handshake consumed it.
			err = fmt.Errorf("frame type %d: %w", typ, errUnexpectedFrame)
		}
		if err != nil {
			stop()
			cause = peerViolation(fmt.Errorf("control stream: %w", err))
			p.ctrlIn.CancelRead(wire.Refused)
			return
		}
		channelID := ChannelID(channel)
		p.engine.post(p.ctx.Done(), func() { handle(p, channelID) })
	}
}

const slotUpdateCap = 32

// runCtrlLoop emits outbound control state in phase order. It owns the sessions
// map and session stream writes.
func (p *PeerConn) runCtrlLoop(slotCh chan<- slotUpdate) {
	// sessions holds each open outbound session stream until it is closed,
	// ended with Reconstructed, or fails a write.
	sessions := make(map[sessionKey]ethp2p.SendStream)
	defer func() {
		p.ctrlOut.CancelWrite(wire.Unspecified)
		for _, s := range sessions {
			s.CancelWrite(wire.Unspecified)
		}
	}()
	// ctrlState consumes before handleCtrl writes. Peer-fatal write failures
	// cancel p.ctx, checked before the next transition; session-local failures
	// retire their stream. Nothing is retried, and PeerConn teardown clears the
	// per-peer state.
	for {
		for {
			if p.ctx.Err() != nil {
				return
			}
			ctrl, ok := p.ctrlState.next()
			if !ok {
				break
			}
			if p.ctx.Err() != nil {
				return
			}
			p.handleCtrl(ctrl, sessions, slotCh)
		}
		select {
		case <-p.ctrlState.ready:
		case <-p.ctx.Done():
			return
		}
	}
}

// runDataLoop sends chunks to the peer. It receives chunk slot
// registrations from the ctrl loop and round-robins across active slots.
func (p *PeerConn) runDataLoop(slotCh <-chan slotUpdate) {
	slots := make(map[sessionKey]<-chan peerSendChunk)
	for {
		for draining := true; draining; {
			select {
			case upd := <-slotCh:
				applySlot(slots, upd)
			default:
				draining = false
			}
		}

		// Round-robin: send at most one chunk, then loop back to
		// drain slot updates before sending another.
		sent := false
		for key, slot := range slots {
			select {
			case chunk, ok := <-slot:
				if !ok {
					delete(slots, key)
					continue
				}
				p.handleSendChunk(chunk)
				sent = true
			default:
			}
			if sent {
				break
			}
		}
		if sent {
			continue
		}

		select {
		case upd := <-slotCh:
			applySlot(slots, upd)
		case <-p.wakeCh:
		case <-p.ctx.Done():
			return
		}
	}
}

func applySlot(slots map[sessionKey]<-chan peerSendChunk, upd slotUpdate) {
	if upd.slot != nil {
		slots[upd.key] = upd.slot
	} else {
		delete(slots, upd.key)
	}
}

func (p *PeerConn) handleCtrl(evt peerCtrlEvent, sessions map[sessionKey]ethp2p.SendStream, slotCh chan<- slotUpdate) {
	switch e := evt.(type) {
	case peerOpenSession:
		p.handleSessionOpen(e, sessions, slotCh)
	case peerSendRouting:
		p.handleSendRoutingUpdate(e, sessions)
	case peerSubscribe:
		p.writeCtrl(subscribeFrame, &bcastpb.Bcast_Subscribe{Channel: string(e.channelID)})
	case peerUnsubscribe:
		p.writeCtrl(unsubscribeFrame, &bcastpb.Bcast_Unsubscribe{Channel: string(e.channelID)})
	case peerCloseSession:
		p.handleSessionDone(e, sessions, slotCh)
	case peerCloseStream:
		key := sessionKey(e)
		if s := sessions[key]; s != nil {
			s.CancelWrite(Reconstructed)
			delete(sessions, key)
		}
	}
}

func (p *PeerConn) handleSessionOpen(e peerOpenSession, sessions map[sessionKey]ethp2p.SendStream, slotCh chan<- slotUpdate) {
	key := sessionKey{e.channelID, e.messageID}
	if previous := sessions[key]; previous != nil {
		if err := previous.Close(); err != nil {
			previous.CancelWrite(streamFailureCode(err))
		}
		delete(sessions, key)
		select {
		case slotCh <- slotUpdate{key: key}:
		case <-p.ctx.Done():
			return
		}
	}

	s, err := p.handle.OpenUniStream(p.ctx)
	if err != nil {
		p.cancel(nil)
		return
	}
	stop := ctxutil.OnCancel(p.ctx, func() { s.CancelWrite(streamFailureCode(p.ctx.Err())) })
	defer stop()
	// OPEN and PREAMBLE go in one write.
	frames, err := appendFrame(nil, openFrame, &bcastpb.Sess_Open{
		Channel:       string(e.channelID),
		ObjectId:      string(e.messageID),
		InitialUpdate: e.initialRouting,
	})
	if err == nil {
		frames, err = appendFrame(frames, preambleFrame, &bcastpb.Sess_Preamble{Data: e.preamble})
	}
	if err == nil {
		_, err = s.Write(frames)
	}
	if err != nil {
		stop()
		// Writes never cancel a side, so end it here before abandoning the stream.
		s.CancelWrite(streamFailureCode(err))
		p.cancel(nil)
		return
	}

	sessions[key] = s

	select {
	case slotCh <- slotUpdate{key: key, slot: e.chunkOutbox}:
	case <-p.ctx.Done():
	}
}

// writeCtrl writes a frame of type typ to the outbound control stream. On
// failure, cancels the peer context so all goroutines shut down.
func (p *PeerConn) writeCtrl(typ wire.FrameType, msg proto.Message) {
	stop := ctxutil.OnCancel(p.ctx, func() { p.ctrlOut.CancelWrite(streamFailureCode(p.ctx.Err())) })
	defer stop()
	if err := writeFrame(p.ctrlOut, typ, msg); err != nil {
		// Writes never cancel a side; the first cancellation code wins, so
		// runCtrlLoop's later Unspecified does not replace this one.
		p.ctrlOut.CancelWrite(streamFailureCode(err))
		stop()
		p.cancel(nil)
	}
}

func (p *PeerConn) handleSendRoutingUpdate(e peerSendRouting, sessions map[sessionKey]ethp2p.SendStream) {
	key := sessionKey{e.channelID, e.messageID}
	stream := sessions[key]
	if stream == nil {
		return
	}
	stop := ctxutil.OnCancel(p.ctx, func() { stream.CancelWrite(streamFailureCode(p.ctx.Err())) })
	defer stop()
	frame := &bcastpb.Sess_Update{Data: e.update}
	if err := writeFrame(stream, updateFrame, frame); err != nil {
		// Writes never cancel a side, so end it here before dropping the stream.
		stream.CancelWrite(streamFailureCode(err))
		delete(sessions, key)
	}
}

func (p *PeerConn) handleSessionDone(e peerCloseSession, sessions map[sessionKey]ethp2p.SendStream, slotCh chan<- slotUpdate) {
	key := sessionKey(e)
	if s := sessions[key]; s != nil {
		if err := s.Close(); err != nil {
			s.CancelWrite(streamFailureCode(err))
		}
	}
	delete(sessions, key)

	select {
	case slotCh <- slotUpdate{key: key}:
	case <-p.ctx.Done():
	}
}

func (p *PeerConn) handleSendChunk(e peerSendChunk) {
	size, err := p.doSendChunk(e)
	select {
	case e.resultCh <- channelChunkSent{
		messageID: e.messageID, peerID: e.peerID, handle: e.handle, err: err, size: size,
	}:
	case <-e.sessionDone:
	case <-p.ctx.Done():
	}
}

func (p *PeerConn) doSendChunk(e peerSendChunk) (int, error) {
	// This check is load-bearing, not a shortcut: e.ctx is a per-chunk
	// context derived from the session ctx, cancelled individually via the
	// session's outboundCancels. It is not derived from p.ctx, and nothing
	// below consults it, so without this check a chunk cancelled on its own
	// would still be sent.
	if e.ctx.Err() != nil {
		return 0, ErrChunkCancelled
	}

	deadline := time.Now().Add(chunkWriteTimeout)
	ctx, cancel := context.WithDeadline(p.ctx, deadline)
	defer cancel()
	s, err := p.handle.OpenUniStream(ctx)
	if err != nil {
		return 0, ErrChunkWriteFail
	}

	s.SetWriteDeadline(deadline)
	stop := ctxutil.OnCancel(ctx, func() { s.CancelWrite(streamFailureCode(errors.Join(ctx.Err(), p.ctx.Err()))) })
	defer stop()

	header, err := proto.Marshal(&bcastpb.Chunk_Header{
		Channel:    string(e.channelID),
		ObjectId:   string(e.messageID),
		ChunkId:    e.chunkID,
		DataLength: uint32(len(e.payload)),
	})
	if err != nil {
		s.CancelWrite(wire.Unspecified)
		return 0, ErrChunkWriteFail
	}
	// HEADER and the DATA frame's type and length go in one write; the
	// payload follows uncopied.
	head := wire.AppendFrame(nil, headerFrame, header)
	head = wire.AppendFrameHead(head, dataFrame, len(e.payload))
	if _, err := s.Write(head); err != nil {
		// Writes never cancel a side, so end it here before abandoning the stream.
		s.CancelWrite(streamFailureCode(err))
		return 0, ErrChunkWriteFail
	}
	if _, err := s.Write(e.payload); err != nil {
		s.CancelWrite(streamFailureCode(err))
		return 0, ErrChunkWriteFail
	}
	if err := s.Close(); err != nil {
		s.CancelWrite(streamFailureCode(err))
		return 0, ErrChunkWriteFail
	}
	return len(e.payload), nil
}
