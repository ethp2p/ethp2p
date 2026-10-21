package ethp2p

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/ethp2p/ethp2p/control"
	"github.com/ethp2p/ethp2p/enr"
	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/quic-go/quic-go"
)

// peerSupervisor owns every view of one remote peer. Its goroutine (run) is
// the only code that reads or writes conn, routes and handles. Fields marked
// "stack.mu" are shared with other goroutines.
//
// run must never block on a consumer or the network, or routing and requests
// for this peer stall: it runs no consumer code and takes no consumer lock
// (a consumer's Peer.Close re-enters through the inbox), it releases views in
// goroutines (closeView), and it does not hold stack.mu while ending handles,
// which takes protocol mutexes and resets streams.
type peerSupervisor struct {
	stack *Stack
	id    identity.PeerID
	// record is the highest Seq seen from Hello or Connect; written under
	// stack.mu, read without it by Peer.Record.
	record atomic.Pointer[enr.Record]

	wake chan struct{} // capacity 1; signalled after every inbox append
	// inbox is run's work in arrival order: a new view, a Peer.Close,
	// Disconnect or Stack.Close. stack.mu.
	inbox []func()
	info  *ConnInfo // stack.mu; nil unless a view is admitted

	conn    *transport.Conn
	ctl     *controlSession
	routes  map[wire.Protocol]*peerDelivery
	handles []*peerDelivery
	// viewCtx ends when the view is dropped, which cancels header reads in
	// progress with Closing.
	viewCtx    context.Context
	viewCancel context.CancelFunc
	// reading counts header reads in progress per direction (uniDir, bidiDir).
	// Streams waiting for a slot remain in transport's queues.
	reading [2]int
}

// maxHeaderReads is MAX_STREAMS_PENDING_CLASSIFY: the stream headers read at
// once per direction on one view.
const maxHeaderReads = 4

const (
	uniDir  = 0
	bidiDir = 1
)

// post appends a request under stack.mu and signals wake. Posting to a
// supervisor that already exited (a later supervisor for the same peer may
// exist) is dropped and reports false: the exited supervisor released its
// view, so a request about that view needs nothing more.
func (sup *peerSupervisor) post(req func()) bool {
	s := sup.stack
	s.mu.Lock()
	if s.peers[sup.id] != sup {
		s.mu.Unlock()
		return false
	}
	sup.inbox = append(sup.inbox, req)
	s.mu.Unlock()
	sup.signal()
	return true
}

// signal wakes run after an inbox append. The send never blocks: a pending
// wake already makes run drain the whole inbox.
func (sup *peerSupervisor) signal() {
	select {
	case sup.wake <- struct{}{}:
	default:
	}
}

// run exits once the peer has no view and no pending work.
func (sup *peerSupervisor) run() {
	for {
		var streams, uniStreams <-chan struct{}
		var done <-chan struct{}
		if sup.conn != nil {
			streams = sup.conn.Streams()
			uniStreams = sup.conn.UniStreams()
			done = sup.conn.Done()
		}
		select {
		case <-sup.wake:
			sup.drainInbox()
		case <-streams:
			sup.readHeaders()
		case <-uniStreams:
			sup.readHeaders()
		case <-done:
			sup.dropView(sup.conn.CloseCode(), false, nil)
		}
		if sup.conn == nil && sup.tryExit() {
			return
		}
	}
}

func (sup *peerSupervisor) drainInbox() {
	sup.stack.mu.Lock()
	reqs := sup.inbox
	sup.inbox = nil
	sup.stack.mu.Unlock()
	for _, req := range reqs {
		req()
	}
}

// tryExit removes the supervisor unless work is pending, and reports whether
// it did.
func (sup *peerSupervisor) tryExit() bool {
	s := sup.stack
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(sup.inbox) != 0 {
		return false
	}
	delete(s.peers, sup.id)
	return true
}

// attach runs the admission checks and returns the Connect result. The record
// and protocol checks run before taking the table mutex, so a view that shares
// no protocol never displaces a working view; they call no consumer code.
func (sup *peerSupervisor) attach(v *pendingView) error {
	id := sup.id
	conn := v.ctl.conn
	var rec *enr.Record
	if len(v.hello.Record) != 0 {
		var err error
		rec, err = enr.Decode(v.hello.Record)
		if err != nil || rec.PeerID() != id {
			sup.reject(v, wire.ControlViolation)
			return &transport.ViewClosedError{Code: wire.ControlViolation}
		}
	}
	// Registration is immutable after Start, so protocols needs no lock.
	var protocols []*Protocol
	for _, protocol := range sup.stack.protocols {
		if protocol.sharedWith(v.hello.Protocols) {
			protocols = append(protocols, protocol)
		}
	}
	if len(protocols) == 0 {
		sup.reject(v, wire.NoSharedProtocols)
		return &transport.ViewClosedError{Code: wire.NoSharedProtocols}
	}
	s := sup.stack
	s.mu.Lock()
	if s.ctx.Err() != nil {
		s.mu.Unlock()
		sup.reject(v, wire.Closing)
		return ErrStackClosed
	}
	d := s.dials[id]
	if conn.Outbound() && d != nil && d.state == dialDisconnected {
		s.mu.Unlock()
		sup.reject(v, wire.Closing)
		return ErrDisconnected
	}
	old := sup.conn
	if old != nil {
		if sup.info.Outbound != conn.Outbound() {
			// Opposite directions: keep the view dialed by the lower peer.
			wantOutbound := s.transport.PeerID() < id
			if conn.Outbound() != wantOutbound {
				s.mu.Unlock()
				sup.reject(v, wire.Duplicate)
				return nil
			}
		}
		// Same direction, or the new view won: the newer view replaces the old.
	}
	s.nextID++
	info := &ConnInfo{ID: s.nextID, Peer: id, Outbound: conn.Outbound(), Since: time.Now()}
	sup.info = info
	s.views[conn] = struct{}{}
	if d != nil {
		sup.updateRecord(d.rec)
	}
	sup.updateRecord(rec)
	s.mu.Unlock()
	if old != nil {
		// The replaced view's PeerDowns precede the new view's PeerUps.
		sup.dropView(wire.Duplicate, true, nil)
	}
	sup.conn, sup.ctl = conn, v.ctl
	sup.viewCtx, sup.viewCancel = context.WithCancel(context.Background())
	routes := make(map[wire.Protocol]*peerDelivery)
	var handles []*peerDelivery
	for _, protocol := range protocols {
		ctx, cancel := context.WithCancel(context.Background())
		peer := &Peer{ctx: ctx, cancel: cancel, id: id, sup: sup, protocol: protocol, conn: conn}
		delivery := protocol.addPeer(peer)
		peer.delivery = delivery
		handles = append(handles, delivery)
		routes[protocol.id] = delivery
	}
	sup.routes = routes
	sup.handles = handles
	// dropView above cleared info for the replaced view; restore the new one.
	s.mu.Lock()
	sup.info = info
	s.mu.Unlock()
	ctl := v.ctl
	s.wg.Go(func() {
		code, goAway, ok := ctl.readGoAway()
		if ok {
			// A dropped post means the supervisor exited, and with it the view.
			sup.post(func() {
				if sup.conn == conn {
					sup.dropView(code, goAway, nil)
				}
			})
		}
	})
	sup.readHeaders()
	return nil
}

// reject releases a view that admission did not take, in a goroutine joined
// by Close, so GOAWAY_TIMEOUT never blocks run. The view never reached
// s.views, so it has no traffic to account.
func (sup *peerSupervisor) reject(v *pendingView, code wire.Code) {
	sup.stack.wg.Go(func() { v.release(code) })
}

// readHeaders starts header reads from each transport queue up to that
// direction's slot limit. Streams beyond the slots stay queued in transport;
// headerRead calls this again when a slot frees, so no wake is lost.
// Each read runs in its own goroutine and reports back through the inbox.
func (sup *peerSupervisor) readHeaders() {
	conn, ctx := sup.conn, sup.viewCtx
	for _, d := range [...]int{uniDir, bidiDir} {
		for sup.reading[d] < maxHeaderReads {
			var s receivedStream
			var ok bool
			if d == uniDir {
				s, ok = conn.NextUniStream()
			} else {
				s, ok = conn.NextStream()
			}
			if !ok {
				break
			}
			sup.reading[d]++
			sup.stack.wg.Go(func() {
				hctx, cancel := context.WithTimeout(ctx, sup.stack.config.headerTimeout())
				defer cancel()
				sel, err := readHeader(hctx, s)
				if !sup.post(func() { sup.headerRead(conn, d, s, sel, err) }) && err == nil {
					cancelReceived(s, wire.Closing)
				}
			})
		}
	}
}

// readHeader reads a stream header within the deadline ctx carries
// (CLASSIFY_TIMEOUT). On failure it cancels the stream with Timeout if the
// read deadline passed, else BadHeader; a peer's reset of a bidirectional
// stream is answered by resetting its write side with Unspecified, and a
// connection end needs no reset. Cancellation of ctx cancels a read in
// progress with Closing and reports ctx's error.
func readHeader(ctx context.Context, s receivedStream) (wire.Protocol, error) {
	stop := context.AfterFunc(ctx, func() {
		// The read deadline ends a timed-out read itself; only a caller
		// cancellation needs the interrupt, with Closing.
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			cancelReceived(s, wire.Closing)
		}
	})
	deadline, _ := ctx.Deadline()
	_ = s.SetReadDeadline(deadline)
	sel, err := wire.ReadHeader(s)
	_ = s.SetReadDeadline(time.Time{})
	if !stop() && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return 0, ctx.Err()
	}
	if err != nil {
		_, reset := errors.AsType[*quic.StreamError](err)
		switch {
		case transport.ConnectionClosed(err):
		case reset:
			if bi, ok := s.(*quic.Stream); ok {
				bi.CancelWrite(streamCode(wire.Unspecified))
			}
		default:
			cancelReceived(s, wire.TimeoutOr(err, wire.BadHeader))
		}
	}
	return sel, err
}

// headerRead routes a stream whose header read ended. Protocol 0 here is a
// control violation: the view's one control stream was taken before
// admission.
func (sup *peerSupervisor) headerRead(conn *transport.Conn, d int, s receivedStream, sel wire.Protocol, err error) {
	if sup.conn != conn {
		if err == nil {
			cancelReceived(s, wire.Closing)
		}
		return
	}
	sup.reading[d]--
	if err == nil {
		route := sup.routes[sel]
		switch {
		case sel == control.ControlProtocol:
			cancelReceived(s, wire.ControlViolation)
			sup.dropView(wire.ControlViolation, true, nil)
			return
		case route == nil:
			cancelReceived(s, wire.UnsupportedProtocol)
		default:
			route.push(s)
		}
	}
	sup.readHeaders()
}

// dropView ends the current view: every handle gets PeerDown with code, and
// the view is released with code, sending GOAWAY when goAway is set. done, if
// given, closes once the view is released. Transport resets streams still
// queued with Closing on release.
func (sup *peerSupervisor) dropView(code wire.Code, goAway bool, done chan<- struct{}) {
	conn, ctl := sup.conn, sup.ctl
	if conn == nil {
		if done != nil {
			close(done)
		}
		return
	}
	sup.viewCancel()
	sup.reading = [2]int{}
	sup.conn, sup.ctl = nil, nil
	sup.routes = nil
	handles := sup.handles
	sup.handles = nil
	for _, h := range handles {
		h.closePeer(code, wire.Closing)
	}
	s := sup.stack
	s.mu.Lock()
	sup.info = nil
	s.mu.Unlock()
	s.closeView(ctl, code, goAway, done)
}

// handleClosed ends the view once no protocol holds it anymore.
func (sup *peerSupervisor) handleClosed() {
	if sup.conn == nil {
		return
	}
	for _, h := range sup.handles {
		h.protocol.mu.Lock()
		down := h.down
		h.protocol.mu.Unlock()
		if !down {
			return
		}
	}
	sup.dropView(wire.NoSharedProtocols, true, nil)
}

// updateRecord keeps the higher Seq; nil is ignored. It runs under stack.mu:
// the load and store are separate, so two unlocked callers could store the
// lower Seq last.
func (sup *peerSupervisor) updateRecord(rec *enr.Record) {
	if rec == nil {
		return
	}
	if old := sup.record.Load(); old == nil || rec.Seq() > old.Seq() {
		sup.record.Store(rec)
	}
}
