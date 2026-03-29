package broadcast

import (
	"errors"
	"time"

	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/protocol"
	protopb "github.com/ethp2p/ethp2p/protocol/pb"
	"github.com/ethp2p/ethp2p/transport"
)

const (
	chunkReadTimeout = 5 * time.Second
	maxChunkDataSize = 1 << 20 // 1 MiB

	// sessCodeReconstructed is the QUIC application error code used to
	// reset a SESS stream when the sender has reconstructed the message.
	sessCodeReconstructed uint64 = 0x01
)

// runAcceptLoop accepts inbound unidirectional streams and dispatches them
// by protocol type. Both SESS and CHUNK streams are handled in per-stream
// goroutines. CHUNK streams are bounded by chunkSem.
func (p *PeerConn) runAcceptLoop() {
	defer p.cancel()
	for {
		s, err := p.conn.AcceptUniStream(p.ctx)
		if err != nil {
			return
		}
		prot, err := protocol.ReadSelector(s)
		if err != nil {
			s.CancelRead(0)
			continue
		}
		switch prot {
		case protopb.Protocol_PROTOCOL_SESS:
			p.wg.Go(func() { p.runInboundSession(s) })
		case protopb.Protocol_PROTOCOL_CHUNK:
			select {
			case p.chunkSem <- struct{}{}:
				p.wg.Go(func() {
					defer func() { <-p.chunkSem }()
					p.processChunk(s)
				})
			default:
				s.CancelRead(0)
			}
		default:
			s.CancelRead(0)
		}
	}
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
