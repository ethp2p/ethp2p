package broadcast

import (
	"errors"
	"time"

	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/transport"
)

const (
	chunkReadTimeout = 5 * time.Second
	maxChunkDataSize = 1 << 20 // 1 MiB

	// sessCodeReconstructed is the QUIC application error code used to
	// reset a SESS stream when the sender has reconstructed the message.
	sessCodeReconstructed uint64 = 0x01
)

func (p *PeerConn) acceptBcast(stream transport.ReceiveStream) {
	if !p.bcastAccepted.CompareAndSwap(false, true) {
		stream.CancelRead(0)
		return
	}
	select {
	case p.bcastIn <- stream:
	case <-p.ctx.Done():
		stream.CancelRead(0)
	}
}

func (p *PeerConn) acceptSession(stream transport.ReceiveStream) {
	if !p.awaitHandshake(stream) {
		return
	}
	p.handlersMu.Lock()
	defer p.handlersMu.Unlock()
	if !p.ready {
		stream.CancelRead(0)
		return
	}
	p.wg.Go(func() { p.runInboundSession(stream) })
}

func (p *PeerConn) acceptChunk(stream transport.ReceiveStream) {
	if !p.awaitHandshake(stream) {
		return
	}
	p.handlersMu.Lock()
	defer p.handlersMu.Unlock()
	if !p.ready {
		stream.CancelRead(0)
		return
	}
	select {
	case p.chunkSem <- struct{}{}:
		p.wg.Go(func() {
			defer func() { <-p.chunkSem }()
			p.processChunk(stream)
		})
	case <-p.ctx.Done():
		stream.CancelRead(0)
	}
}

func (p *PeerConn) awaitHandshake(stream transport.ReceiveStream) bool {
	if !p.bcastAccepted.Load() {
		stream.CancelRead(0)
		return false
	}
	select {
	case <-p.handshakeDone:
		if p.handshakeOK {
			return true
		}
	case <-p.ctx.Done():
	}
	stream.CancelRead(0)
	return false
}

// runInboundSession reads frames from an inbound SESS stream.
// The first frame must be SessionOpen (which may carry initial routing);
// subsequent frames are RoutingUpdate. EOF signals the peer has
// reconstructed (completed).
func (p *PeerConn) runInboundSession(s transport.ReceiveStream) {
	// Read the first frame: must be SessionOpen.
	var frame bcastpb.Sess
	if err := ReadFrame(s, &frame); err != nil {
		s.CancelRead(0)
		return
	}
	so := frame.GetSessionOpen()
	if so == nil {
		s.CancelRead(0)
		return
	}

	channelID := ChannelID(so.Channel)
	messageID := MessageID(so.MessageId)

	p.engine.config.Observer.OnPreambleOpened(p.id, channelID, messageID)

	ch := p.channelInboxFor(channelID)
	if ch == nil {
		s.CancelRead(0)
		return
	}

	// Deliver SessionOpen to channel (with initial routing if present).
	select {
	case ch <- channelSessionOpen{
		peerID: p.id,
		msg:    so,
	}:
	case <-p.ctx.Done():
		return
	}

	// Read loop: subsequent frames are RoutingUpdate.
	for {
		frame.Reset()
		if err := ReadFrame(s, &frame); err != nil {
			var re *transport.StreamResetError
			if errors.As(err, &re) && re.Code == sessCodeReconstructed {
				select {
				case ch <- channelPeerReconstructed{messageID: messageID, peerID: p.id}:
				case <-p.ctx.Done():
				}
			}
			return
		}
		ru := frame.GetRoutingUpdate()
		if ru == nil {
			continue
		}
		select {
		case ch <- channelRoutingUpdate{peerID: p.id, messageID: messageID, msg: ru}:
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *PeerConn) processChunk(s transport.ReceiveStream) {
	// TODO(raulk): s.Reset() on failure and timeout?
	//
	s.SetReadDeadline(time.Now().Add(chunkReadTimeout))

	var frame bcastpb.Chunk_Header
	if err := ReadFrame(s, &frame); err != nil {
		s.CancelRead(0)
		return
	}

	// Clear the frame read deadline so it doesn't leak into the
	// session's data read goroutine (which sets its own deadline).
	s.SetReadDeadline(time.Time{})

	ch := p.channelInboxFor(ChannelID(frame.Channel))
	if ch == nil {
		s.CancelRead(0)
		return
	}

	chnk := channelChunkStream{
		peerID: p.id,
		frame:  &frame,
		stream: s,
	}
	select {
	case ch <- chnk:
	case <-p.ctx.Done():
		s.CancelRead(0)
	}
}
