package broadcast

import (
	"time"

	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/protocol"
	protopb "github.com/ethp2p/ethp2p/protocol/pb"
	"github.com/ethp2p/ethp2p/transport"
)

const chunkWriteTimeout = 5 * time.Second

type sessionKey struct {
	channelID ChannelID
	messageID MessageID
}

type peerSessionState struct {
	channelInbox chan<- channelEvent
	messageID    MessageID
	sessOut      transport.SendStream
}

// slotUpdate notifies the data loop about chunk slot changes.
type slotUpdate struct {
	key  sessionKey
	slot <-chan peerSendChunk // nil = slot removed
}

// runCtrlReader reads all control messages from the peer's inbound
// BCAST stream (bcastIn) and dispatches them. PeerConn.Close() closes
// the transport, which unblocks the blocking read. On read error, cancels
// the peer context to cascade shutdown to all goroutines.
func (p *PeerConn) runCtrlReader() {
	defer p.cancel()
	var msg bcastpb.Bcast
	for {
		msg.Reset()
		if err := ReadFrame(p.ctrlIn, &msg); err != nil {
			return
		}
		switch {
		case msg.GetChannelSubscribe() != nil:
			channelID := ChannelID(msg.GetChannelSubscribe().Channel)
			p.engine.onPeerSubscribed(p.id, channelID)

		case msg.GetChannelUnsubscribe() != nil:
			channelID := ChannelID(msg.GetChannelUnsubscribe().Channel)
			p.engine.onPeerUnsubscribed(p.id, channelID)

		default:
			// Unknown message type; skip.
		}
	}
}

const slotUpdateCap = 32

// runCtrlLoop processes control events: session lifecycle, routing,
// subscribe/unsubscribe. It owns the sessions map and SESS stream writes.
func (p *PeerConn) runCtrlLoop(slotCh chan<- slotUpdate) {
	sessions := make(map[sessionKey]*peerSessionState)
	for {
		select {
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
			ss.sessOut.CancelWrite(sessCodeReconstructed)
			ss.sessOut = nil
		}
	}
}

func (p *PeerConn) handleSessionOpen(e peerOpenSession, sessions map[sessionKey]*peerSessionState, slotCh chan<- slotUpdate) {
	key := sessionKey{e.channelID, e.messageID}

	s, err := p.conn.OpenUniStream(p.ctx)
	if err != nil {
		p.cancel()
		return
	}
	if err := protocol.WriteSelector(s, protopb.Protocol_PROTOCOL_SESS); err != nil {
		s.CancelWrite(0)
		p.cancel()
		return
	}
	frame := &bcastpb.Sess{Frame: &bcastpb.Sess_SessionOpen{SessionOpen: &bcastpb.Sess_Open{
		Channel:       string(e.channelID),
		MessageId:     string(e.messageID),
		Preamble:      e.preamble,
		InitialUpdate: e.initialRouting,
	}}}
	if err := WriteFrame(s, frame); err != nil {
		s.CancelWrite(0)
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
	ss.sessOut = s

	select {
	case slotCh <- slotUpdate{key: key, slot: e.chunkOutbox}:
	case <-p.ctx.Done():
	}
}

// writeCtrl writes a control frame to the outbound BCAST stream. On
// failure, cancels the peer context so all goroutines shut down.
func (p *PeerConn) writeCtrl(msg *bcastpb.Bcast) error {
	if err := WriteFrame(p.ctrlOut, msg); err != nil {
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
	frame := &bcastpb.Sess{Frame: &bcastpb.Sess_RoutingUpdate{RoutingUpdate: &bcastpb.Sess_Update{
		Data: e.update,
	}}}
	if err := WriteFrame(ss.sessOut, frame); err != nil {
		ss.sessOut.CancelWrite(0)
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
		ss.sessOut.Close()
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
	if e.ctx != nil && e.ctx.Err() != nil {
		return 0, ErrChunkCancelled
	}

	s, err := p.conn.OpenUniStream(p.ctx)
	if err != nil {
		return 0, ErrChunkWriteFail
	}

	s.SetWriteDeadline(time.Now().Add(chunkWriteTimeout))

	if err := protocol.WriteSelector(s, protopb.Protocol_PROTOCOL_CHUNK); err != nil {
		s.CancelWrite(0)
		return 0, ErrChunkWriteFail
	}
	frame := &bcastpb.Chunk_Header{
		Channel:    string(e.channelID),
		MessageId:  string(e.messageID),
		ChunkId:    e.chunkID,
		DataLength: uint32(len(e.payload)),
	}
	if err := WriteFrame(s, frame); err != nil {
		s.CancelWrite(0)
		return 0, ErrChunkWriteFail
	}
	if _, err := s.Write(e.payload); err != nil {
		s.CancelWrite(0)
		return 0, ErrChunkWriteFail
	}
	s.Close()
	return len(e.payload), nil
}
