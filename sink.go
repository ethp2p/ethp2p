package ethp2p

import (
	"context"
	"time"

	"github.com/ethp2p/ethp2p/enr"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/ethp2p/ethp2p/transport"
)

type stackSink struct{ stack *Stack }

func (sink stackSink) Admit(conn transport.Conn, hello transport.Hello) (wire.Code, bool) {
	s := sink.stack
	var record *enr.Record
	if len(hello.Record) != 0 {
		var err error
		record, err = enr.Decode(hello.Record)
		if err != nil || record.PeerID() != conn.RemotePeerID() {
			return wire.ControlViolation, false
		}
	}
	// Registration is immutable after Start. Policies may inspect the stack,
	// so no stack lock is held while constructing candidates or calling them.
	matched := make(map[*Subsystem][]wire.Selector)
	for _, sel := range wire.Intersect(s.localSelectors(), hello.Selectors) {
		sub := s.selectors[sel]
		matched[sub] = append(matched[sub], sel)
	}
	v := &stackView{conn: conn, routes: make(map[wire.Selector]*peerDelivery), released: make(chan struct{})}
	v.record.Store(record)
	type candidate struct {
		sub  *Subsystem
		peer *Peer
	}
	var accepted []candidate
	for sub, selectors := range matched {
		ctx, cancel := context.WithCancel(context.Background())
		p := &Peer{ctx: ctx, id: conn.RemotePeerID(), record: &v.record, selectors: selectors, conn: conn}
		for _, sel := range selectors {
			v.routes[sel] = nil
		}
		if sub.policy == nil || sub.policy(p) {
			v.cancels = append(v.cancels, cancel)
			accepted = append(accepted, candidate{sub, p})
		} else {
			cancel()
		}
	}
	reject := func(code wire.Code) (wire.Code, bool) {
		for _, cancel := range v.cancels {
			cancel()
		}
		return code, false
	}
	if len(accepted) == 0 {
		return reject(wire.NoSharedProtocols)
	}
	id := conn.RemotePeerID()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return reject(wire.Closing)
	}
	d := s.dials[id]
	if conn.Outbound() && d != nil && d.gen != s.generations[id] {
		s.mu.Unlock()
		return reject(wire.Closing)
	}
	old := s.active[id]
	if old != nil {
		if old.info.Outbound != conn.Outbound() {
			wantOutbound := s.transport.PeerID() < id
			if conn.Outbound() != wantOutbound {
				s.mu.Unlock()
				return reject(wire.Duplicate)
			}
		}
		s.retire(old, wire.Duplicate)
	}
	s.nextID++
	v.info = ConnInfo{ID: s.nextID, Peer: id, Outbound: conn.Outbound(), Since: time.Now()}
	if d != nil {
		updateRecord(v, d.rec)
	}
	s.active[id] = v
	s.unreleased[conn] = v
	for _, a := range accepted {
		delivery := a.sub.addPeer(a.peer)
		v.peers = append(v.peers, delivery)
		for _, sel := range a.peer.selectors {
			v.routes[sel] = delivery
		}
	}
	s.mu.Unlock()
	if old != nil {
		_ = old.conn.CloseWithCode(wire.Duplicate)
	}
	return wire.Unspecified, true
}

func (sink stackSink) Stream(conn transport.Conn, sel wire.Selector, stream transport.ReceiveStream) {
	s := sink.stack
	s.mu.Lock()
	v := s.active[conn.RemotePeerID()]
	if v == nil || v.conn != conn {
		s.mu.Unlock()
		cancelQueuedStream(stream)
		return
	}
	route, known := v.routes[sel]
	s.mu.Unlock()
	if !known {
		cancelSelectedStream(stream, wire.UnsupportedSelector)
		return
	}
	if route == nil {
		cancelSelectedStream(stream, wire.Refused)
		return
	}
	route.push(sel, stream)
}

func cancelSelectedStream(stream transport.ReceiveStream, code wire.Code) {
	stream.CancelRead(code.Wire())
	if bi, ok := stream.(transport.Stream); ok {
		bi.CancelWrite(code.Wire())
	}
}

func (sink stackSink) Closed(conn transport.Conn, code wire.Code) {
	s := sink.stack
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.unreleased[conn]
	if v == nil {
		return
	}
	if s.active[v.info.Peer] == v {
		s.retire(v, code)
	}
	sent, received := conn.ConnectionStats()
	s.sent += sent
	s.received += received
	delete(s.unreleased, conn)
	s.cleanupGeneration(v.info.Peer)
	close(v.released)
}
