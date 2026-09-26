package ethp2p

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/ethp2p/ethp2p/enr"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/wire"
)

// peerSupervisor owns every view of one remote peer. Its goroutine (run) is
// the only code that reads or writes conn, routes and handles. Fields marked
// "stack.mu" are shared with other goroutines.
type peerSupervisor struct {
	stack *Stack
	id    transport.PeerID
	// record is the highest Seq seen from Hello or Connect; read by Peer.Record.
	record atomic.Pointer[enr.Record]

	wake      chan struct{}       // capacity 1; signalled by post
	inbox     []supervisorRequest // stack.mu
	info      ConnInfo            // stack.mu; valid while connected
	connected bool                // stack.mu

	conn    *transport.Conn
	routes  map[wire.Selector]*peerDelivery
	handles []*peerDelivery
}

type requestKind uint8

const (
	// reqAttach is a new view for this peer (accepted or dialed).
	reqAttach requestKind = iota + 1
	// reqHandleClosed is a Peer.Close ending one handle.
	reqHandleClosed
	// reqDisconnect is Disconnect or Stack.Close.
	reqDisconnect
)

type supervisorRequest struct {
	kind requestKind
	// conn is reqAttach's view.
	conn *transport.Conn
	// result is reqAttach's Connect report (capacity 1); nil from the accept loop.
	result chan<- error
	// done is reqDisconnect's release signal: closed once the view is released
	// (immediately if none).
	done chan<- struct{}
}

// post appends under stack.mu and signals wake. Posting to a supervisor that
// already exited (a later supervisor for the same peer may exist) is harmless:
// a handle close needs nothing more, and a disconnect wait is released at once.
func (sup *peerSupervisor) post(req supervisorRequest) {
	s := sup.stack
	s.mu.Lock()
	if s.peers[sup.id] != sup {
		s.mu.Unlock()
		if req.done != nil {
			close(req.done)
		}
		return
	}
	sup.inbox = append(sup.inbox, req)
	s.mu.Unlock()
	select {
	case sup.wake <- struct{}{}:
	default:
	}
}

// run is the only code that changes this peer's view, routes and handles. It
// exits once the peer has no view and no pending work.
func (sup *peerSupervisor) run() {
	for {
		var streams <-chan struct{}
		var done <-chan struct{}
		if sup.conn != nil {
			streams = sup.conn.Streams()
			done = sup.conn.Done()
		}
		select {
		case <-sup.wake:
			sup.drainInbox()
		case <-streams:
			sup.routeStreams()
		case <-done:
			sup.dropView(sup.conn.CloseCode(), nil)
		}
		if sup.conn == nil && sup.tryExit() {
			return
		}
	}
}

// drainInbox handles every queued request in order.
func (sup *peerSupervisor) drainInbox() {
	sup.stack.mu.Lock()
	reqs := sup.inbox
	sup.inbox = nil
	sup.stack.mu.Unlock()
	for _, req := range reqs {
		switch req.kind {
		case reqAttach:
			err := sup.attach(req.conn)
			if req.result != nil {
				req.result <- err
			}
		case reqHandleClosed:
			sup.handleClosed()
		case reqDisconnect:
			if sup.conn == nil {
				if req.done != nil {
					close(req.done)
				}
			} else {
				sup.dropView(wire.Closing, req.done)
			}
		}
	}
}

// tryExit removes the supervisor once its peer has no view and no pending
// work. It reports whether the caller should return.
func (sup *peerSupervisor) tryExit() bool {
	s := sup.stack
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(sup.inbox) != 0 {
		return false
	}
	delete(s.peers, sup.id)
	s.cleanupGeneration(sup.id)
	return true
}

// attach runs the section 7 checks and returns the Connect result. The record
// and family checks run before taking the table mutex, so a view that shares
// no family never displaces a working view; they call no consumer code.
func (sup *peerSupervisor) attach(conn *transport.Conn) error {
	id := sup.id
	hello := conn.PeerHello()
	var rec *enr.Record
	if len(hello.Record) != 0 {
		var err error
		rec, err = enr.Decode(hello.Record)
		if err != nil || rec.PeerID() != id {
			sup.stack.closeView(conn, wire.ControlViolation, nil)
			return &transport.ViewClosedError{Code: wire.ControlViolation}
		}
	}
	families := sup.stack.sharedFamilies(hello.Selectors)
	if len(families) == 0 {
		sup.stack.closeView(conn, wire.NoSharedProtocols, nil)
		return &transport.ViewClosedError{Code: wire.NoSharedProtocols}
	}
	s := sup.stack
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.closeView(conn, wire.Closing, nil)
		return ErrStackClosed
	}
	d := s.dials[id]
	if conn.Outbound() && d != nil && d.gen != s.generations[id] {
		s.mu.Unlock()
		s.closeView(conn, wire.Closing, nil)
		return ErrDisconnected
	}
	old := sup.conn
	if old != nil {
		if sup.info.Outbound != conn.Outbound() {
			// Opposite directions: keep the view dialed by the lower peer.
			wantOutbound := s.transport.PeerID() < id
			if conn.Outbound() != wantOutbound {
				s.mu.Unlock()
				s.closeView(conn, wire.Duplicate, nil)
				return nil
			}
		}
		// Same direction, or the new view won: the newer view replaces the old.
	}
	s.nextID++
	sup.info = ConnInfo{ID: s.nextID, Peer: id, Outbound: conn.Outbound(), Since: time.Now()}
	sup.connected = true
	if s.views == nil {
		s.views = make(map[*transport.Conn]struct{})
	}
	s.views[conn] = struct{}{}
	if d != nil {
		sup.updateRecord(d.rec)
	}
	s.mu.Unlock()
	if old != nil {
		// The replaced view's PeerDowns precede the new view's PeerUps.
		sup.dropView(wire.Duplicate, nil)
	}
	sup.conn = conn
	sup.updateRecord(rec)
	routes := make(map[wire.Selector]*peerDelivery)
	var handles []*peerDelivery
	for _, family := range families {
		ctx, cancel := context.WithCancel(context.Background())
		peer := &Peer{ctx: ctx, cancel: cancel, id: id, sup: sup, family: family, conn: conn}
		delivery := family.addPeer(peer)
		handles = append(handles, delivery)
		for _, spec := range family.protocols {
			routes[spec.Selector] = delivery
		}
	}
	sup.routes = routes
	sup.handles = handles
	// dropView above cleared the flag for the replaced view; the new view holds it.
	s.mu.Lock()
	sup.connected = true
	s.mu.Unlock()
	return nil
}

// routeStreams drains the view's classified streams, resetting anything
// without a live route. Streams of a handle that ended locally are reset by
// the delivery itself.
func (sup *peerSupervisor) routeStreams() {
	conn := sup.conn
	if conn == nil {
		return
	}
	for {
		stream, sel, ok := conn.NextStream()
		if !ok {
			return
		}
		if route := sup.routes[sel]; route == nil {
			cancelReceived(stream, wire.UnsupportedSelector)
		} else {
			route.push(sel, stream)
		}
	}
}

// dropView ends the current view: routes and handles are cleared, every handle
// gets PeerDown with code, and the view is released with code. done, if given,
// closes once the view is released.
func (sup *peerSupervisor) dropView(code wire.Code, done chan<- struct{}) {
	conn := sup.conn
	if conn == nil {
		if done != nil {
			close(done)
		}
		return
	}
	sup.conn = nil
	sup.routes = nil
	handles := sup.handles
	sup.handles = nil
	for _, h := range handles {
		h.closePeer(code, wire.Closing)
	}
	s := sup.stack
	s.mu.Lock()
	sup.connected = false
	s.mu.Unlock()
	s.closeView(conn, code, done)
}

// handleClosed ends the view once no family holds it anymore.
func (sup *peerSupervisor) handleClosed() {
	if sup.conn == nil {
		return
	}
	for _, h := range sup.handles {
		h.family.mu.Lock()
		down := h.down
		h.family.mu.Unlock()
		if !down {
			return
		}
	}
	sup.dropView(wire.NoSharedProtocols, nil)
}

// updateRecord keeps the higher Seq across concurrent admission and Connect.
// Nil is ignored.
func (sup *peerSupervisor) updateRecord(rec *enr.Record) {
	if rec == nil {
		return
	}
	for {
		old := sup.record.Load()
		if old != nil && rec.Seq() <= old.Seq() {
			return
		}
		if sup.record.CompareAndSwap(old, rec) {
			return
		}
	}
}
