package broadcast

import (
	"context"
	"errors"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/internal/ctxutil"
	"github.com/ethp2p/ethp2p/internal/devhook"
	"github.com/ethp2p/ethp2p/wire"
)

const chunkWriteTimeout = 5 * time.Second

type sessionKey struct {
	channelID ChannelID
	messageID MessageID
}

type peerSessionState struct {
	channelInbox chan<- channelEvent
	messageID    MessageID
	sessOut      ethp2p.SendStream
}

// slotUpdate notifies the data loop about chunk slot changes.
type slotUpdate struct {
	key  sessionKey
	slot <-chan peerSendChunk // nil = slot removed
}

// runCtrlReader reads and dispatches control messages from the peer's inbound
// BCAST stream. On read error, it stops this broadcast binding. The borrowed
// connection remains owned by the application; stream cancellation is limited
// to streams owned by this binding.
func (p *PeerConn) runCtrlReader() {
	defer p.cancel()
	if !p.awaitHandshake(p.ctrlIn) {
		return
	}
	stop := ctxutil.OnCancel(p.ctx, func() { p.ctrlIn.CancelRead(streamFailureCode(p.ctx.Err())) })
	defer stop()
	var msg bcastpb.Bcast
	for {
		msg.Reset()
		if err := ReadFrame(p.ctrlIn, &msg); err != nil {
			stop()
			// Reads never cancel a side, so end it here with the read error's code.
			p.ctrlIn.CancelRead(streamCancellationCode(errors.Join(err, context.Cause(p.ctx))))
			return
		}
		switch {
		case msg.GetChannelSubscribe() != nil:
			channelID := ChannelID(msg.GetChannelSubscribe().Channel)
			p.engine.onPeerSubscribed(p, channelID)

		case msg.GetChannelUnsubscribe() != nil:
			channelID := ChannelID(msg.GetChannelUnsubscribe().Channel)
			p.engine.onPeerUnsubscribed(p, channelID)

		default:
			p.ctrlIn.CancelRead(wire.Refused)
			return
		}
	}
}

const slotUpdateCap = 32

// runCtrlLoop processes control events: session lifecycle, routing,
// subscribe/unsubscribe. It owns the sessions map and SESS stream writes.
func (p *PeerConn) runCtrlLoop(slotCh chan<- slotUpdate) {
	sessions := make(map[sessionKey]*peerSessionState)
	defer func() {
		if !p.ctrlOutEnded.Swap(true) {
			p.ctrlOut.CancelWrite(wire.Unspecified)
		}
		for _, session := range sessions {
			if session.sessOut != nil {
				session.sessOut.CancelWrite(wire.Unspecified)
			}
		}
	}()
	for {
		if ctrl, ok := p.lifecycle.pop(); ok {
			if p.ctx.Err() != nil {
				return
			}
			p.handleCtrl(ctrl, sessions, slotCh)
			continue
		}
		select {
		case <-p.lifecycle.ready:
		case ctrl := <-p.ctrlQ:
			p.handleCtrl(ctrl, sessions, slotCh)
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
		// Drain pending slot updates.
		draining := true
		for draining {
			select {
			case upd := <-slotCh:
				if upd.slot != nil {
					slots[upd.key] = upd.slot
				} else {
					delete(slots, upd.key)
				}
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

		// Nothing ready; wait for a slot update, chunk deposit, or shutdown.
		select {
		case upd := <-slotCh:
			if upd.slot != nil {
				slots[upd.key] = upd.slot
			} else {
				delete(slots, upd.key)
			}
		case <-p.wakeCh:
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *PeerConn) handleCtrl(evt peerCtrlEvent, sessions map[sessionKey]*peerSessionState, slotCh chan<- slotUpdate) {
	switch e := evt.(type) {
	case *peerClosures:
		for _, close := range e.events {
			p.handleCtrl(close, sessions, slotCh)
		}
	case peerOpenSession:
		p.handleSessionOpen(e, sessions, slotCh)
	case peerSendRouting:
		p.handleSendRoutingUpdate(e, sessions)
	case peerSubscribe:
		p.handleSubscribe(e)
	case peerUnsubscribe:
		p.handleUnsubscribe(e)
	case peerCloseSession:
		p.handleSessionDone(e, sessions, slotCh)
	case peerCloseStream:
		key := sessionKey(e)
		if ss := sessions[key]; ss != nil && ss.sessOut != nil {
			ss.sessOut.CancelWrite(Reconstructed)
			ss.sessOut = nil
		}
	}
}

func (p *PeerConn) handleSessionOpen(e peerOpenSession, sessions map[sessionKey]*peerSessionState, slotCh chan<- slotUpdate) {
	key := sessionKey{e.channelID, e.messageID}

	s, err := p.streams.OpenUniStream(p.ctx, SESS)
	if err != nil {
		p.cancel()
		return
	}
	stop := ctxutil.OnCancel(p.ctx, func() { s.CancelWrite(streamFailureCode(p.ctx.Err())) })
	defer stop()
	frame := &bcastpb.Sess{Frame: &bcastpb.Sess_SessionOpen{SessionOpen: &bcastpb.Sess_Open{
		Channel:       string(e.channelID),
		MessageId:     string(e.messageID),
		Preamble:      e.preamble,
		InitialUpdate: e.initialRouting,
	}}}
	if err := WriteFrame(s, frame); err != nil {
		stop()
		// Writes never cancel a side, so end it here before abandoning the stream.
		s.CancelWrite(streamFailureCode(err))
		p.cancel()
		return
	}

	ss := sessions[key]
	if ss == nil {
		ss = &peerSessionState{}
		sessions[key] = ss
	}
	ss.channelInbox = e.channelInbox
	ss.messageID = e.messageID
	// A duplicate open is an internal lifecycle bug. Never abandon its stream
	// even if a future caller violates the idempotent attachment contract.
	if ss.sessOut != nil {
		if err := ss.sessOut.Close(); err != nil {
			ss.sessOut.CancelWrite(streamFailureCode(err))
		}
	}
	ss.sessOut = s

	select {
	case slotCh <- slotUpdate{key: key, slot: e.chunkOutbox}:
	case <-p.ctx.Done():
		s.CancelWrite(wire.Unspecified)
		ss.sessOut = nil
	}
}

// writeCtrl writes a control frame to the outbound BCAST stream. On
// failure, cancels the peer context so all goroutines shut down.
func (p *PeerConn) writeCtrl(msg *bcastpb.Bcast) error {
	stop := ctxutil.OnCancel(p.ctx, func() { p.ctrlOut.CancelWrite(streamFailureCode(p.ctx.Err())) })
	defer stop()
	if err := WriteFrame(p.ctrlOut, msg); err != nil {
		p.ctrlOutEnded.Store(true)
		// Writes never cancel a side; record the specific code here so the
		// runCtrlLoop shutdown does not overwrite it with Unspecified.
		p.ctrlOut.CancelWrite(streamFailureCode(err))
		stop()
		p.cancel()
		return err
	}
	return nil
}

func (p *PeerConn) handleSendRoutingUpdate(e peerSendRouting, sessions map[sessionKey]*peerSessionState) {
	key := sessionKey{e.channelID, e.messageID}
	ss := sessions[key]
	if ss == nil || ss.sessOut == nil {
		return
	}
	stream := ss.sessOut
	stop := ctxutil.OnCancel(p.ctx, func() { stream.CancelWrite(streamFailureCode(p.ctx.Err())) })
	defer stop()
	frame := &bcastpb.Sess{Frame: &bcastpb.Sess_RoutingUpdate{RoutingUpdate: &bcastpb.Sess_Update{
		Data: e.update,
	}}}
	if err := WriteFrame(ss.sessOut, frame); err != nil {
		// Writes never cancel a side, so end it here before dropping the stream.
		stream.CancelWrite(streamFailureCode(err))
		ss.sessOut = nil
	}
}

func (p *PeerConn) handleSubscribe(e peerSubscribe) {
	msg := &bcastpb.Bcast{
		Message: &bcastpb.Bcast_ChannelSubscribe{
			ChannelSubscribe: &bcastpb.Bcast_Subscribe{Channel: string(e.channelID)},
		},
	}
	p.writeCtrl(msg)
}

func (p *PeerConn) handleUnsubscribe(e peerUnsubscribe) {
	msg := &bcastpb.Bcast{
		Message: &bcastpb.Bcast_ChannelUnsubscribe{
			ChannelUnsubscribe: &bcastpb.Bcast_Unsubscribe{Channel: string(e.channelID)},
		},
	}
	p.writeCtrl(msg)
}

func (p *PeerConn) handleSessionDone(e peerCloseSession, sessions map[sessionKey]*peerSessionState, slotCh chan<- slotUpdate) {
	key := sessionKey(e)
	if ss := sessions[key]; ss != nil && ss.sessOut != nil {
		if err := ss.sessOut.Close(); err != nil {
			ss.sessOut.CancelWrite(streamFailureCode(err))
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
	case <-e.channelDone:
	case <-p.ctx.Done():
	}
}

func (p *PeerConn) doSendChunk(e peerSendChunk) (int, error) {
	if e.ctx != nil && e.ctx.Err() != nil {
		return 0, ErrChunkCancelled
	}

	deadline := time.Now().Add(chunkWriteTimeout)
	ctx, cancel := context.WithDeadline(p.ctx, deadline)
	defer cancel()
	if !p.engine.config.hooks.Wait(ctx, devhook.Site{Point: devhook.PointChunkWrite, Peer: string(p.id), Channel: string(e.channelID), Message: string(e.messageID)}) {
		return 0, ErrChunkCancelled
	}
	payload := e.payload
	copies := 1
	fault := p.engine.config.hooks.FaultChunk(string(p.id), string(e.channelID), string(e.messageID), e.payload)
	if fault.Err != nil {
		return 0, fault.Err
	}
	if fault.Data != nil {
		payload = fault.Data
	}
	if fault.Duplicate {
		copies = 2
	}
	for copyIndex := range copies {
		if copyIndex != 0 && !p.engine.config.hooks.Wait(ctx, devhook.Site{Point: devhook.PointChunkWrite, Peer: string(p.id), Channel: string(e.channelID), Message: string(e.messageID)}) {
			return 0, ErrChunkCancelled
		}
		s, err := p.streams.OpenUniStream(ctx, CHUNK)
		if err != nil {
			return 0, ErrChunkWriteFail
		}

		s.SetWriteDeadline(deadline)
		stop := ctxutil.OnCancel(ctx, func() { s.CancelWrite(streamFailureCode(errors.Join(ctx.Err(), p.ctx.Err()))) })
		defer stop()

		frame := &bcastpb.Chunk_Header{
			Channel:    string(e.channelID),
			MessageId:  string(e.messageID),
			ChunkId:    e.chunkID,
			DataLength: uint32(len(payload)),
		}
		if err := WriteFrame(s, frame); err != nil {
			// Writes never cancel a side, so end it here before abandoning the stream.
			s.CancelWrite(streamFailureCode(err))
			return 0, ErrChunkWriteFail
		}
		if _, err := s.Write(payload); err != nil {
			s.CancelWrite(streamFailureCode(err))
			return 0, ErrChunkWriteFail
		}
		if err := s.Close(); err != nil {
			s.CancelWrite(streamFailureCode(err))
			return 0, ErrChunkWriteFail
		}
	}
	return len(payload), nil
}
