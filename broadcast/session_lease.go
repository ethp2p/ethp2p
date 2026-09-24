package broadcast

import "github.com/ethp2p/ethp2p/protocol"

// sessionLease charges the first opener until disposal, independently of SESS
// reader lifetime. Only session disposal releases the charge. The target is
// retained even after unsubscribe, so binding departure reaches every creator.
type sessionLease struct {
	peer   *PeerConn
	target *channelDelivery
}

func (p *PeerConn) reserveSession(target *channelDelivery) (*sessionLease, protocol.Code) {
	p.chunkMu.Lock()
	defer p.chunkMu.Unlock()
	if p.ctx.Err() != nil {
		return nil, protocol.Unspecified
	}
	if len(p.liveSessions) >= p.engine.config.maxLiveSessionsPerPeer() {
		return nil, protocol.Overloaded
	}
	if p.liveSessions == nil {
		p.liveSessions = make(map[*sessionLease]struct{})
	}
	l := &sessionLease{peer: p, target: target}
	p.liveSessions[l] = struct{}{}
	return l, protocol.Unspecified
}

func (p *PeerConn) notifyCreatorDeparture() {
	// Close cancels before this snapshot. Reservation checks cancellation under
	// this same mutex: each lease is either included here or refused. Keep the
	// charges until the channel actually closes its sessions.
	p.chunkMu.Lock()
	targets := make(map[*channelDelivery]struct{})
	for lease := range p.liveSessions {
		targets[lease.target] = struct{}{}
	}
	p.chunkMu.Unlock()
	for target := range targets {
		select {
		case target.inbox <- channelCreatorDeparted{peer: p}:
		case <-target.done:
		case <-p.engine.ctx.Done():
		}
	}
}

func (l *sessionLease) release() {
	if l == nil {
		return
	}
	p := l.peer
	p.chunkMu.Lock()
	delete(p.liveSessions, l)
	p.chunkMu.Unlock()
}
