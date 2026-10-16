package broadcast

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/wire"
)

func nextCtrlEvent(t *testing.T, p *PeerConn) peerCtrlEvent {
	t.Helper()
	event, ok := p.ctrlState.next()
	if !ok {
		t.Fatal("control state has no next event")
	}
	return event
}

func ctrlStateCounts(state *ctrlOutState) (channels, sessions int) {
	state.mu.Lock()
	defer state.mu.Unlock()
	return len(state.channels), len(state.sessions)
}

func TestCtrlOutPhaseOrder(t *testing.T) {
	p := testPeer("peer")
	defer p.Close()
	p.ctrlState.announceInitial([]ChannelID{"channel"})

	p.ctrlState.openSession(peerOpenSession{channelID: "channel", messageID: "stream"})
	if _, ok := nextCtrlEvent(t, p).(peerOpenSession); !ok {
		t.Fatal("missing stream open")
	}
	p.ctrlState.openSession(peerOpenSession{channelID: "channel", messageID: "closing"})
	if _, ok := nextCtrlEvent(t, p).(peerOpenSession); !ok {
		t.Fatal("missing closing-session open")
	}

	p.ctrlState.closeSession(sessionKey{channelID: "channel", messageID: "closing"})
	p.ctrlState.announce("channel", false)
	p.ctrlState.announce("channel", true)
	p.ctrlState.openSession(peerOpenSession{channelID: "channel", messageID: "opening"})
	p.ctrlState.closeSessionStream(sessionKey{channelID: "channel", messageID: "stream"})
	if !p.ctrlState.setRouting(peerSendRouting{channelID: "channel", messageID: "stream", update: []byte{1}}) {
		t.Fatal("routing update ignored for open session")
	}

	if event := nextCtrlEvent(t, p); event != (peerCloseSession{channelID: "channel", messageID: "closing"}) {
		t.Fatalf("phase 1 = %#v, want session close", event)
	}
	if event := nextCtrlEvent(t, p); event != (peerUnsubscribe{channelID: "channel"}) {
		t.Fatalf("phase 2 = %#v, want channel unsubscribe", event)
	}
	if event := nextCtrlEvent(t, p); event != (peerSubscribe{channelID: "channel"}) {
		t.Fatalf("phase 3 = %#v, want channel subscribe", event)
	}
	if event := nextCtrlEvent(t, p); event.(peerOpenSession).messageID != "opening" {
		t.Fatalf("phase 4 = %#v, want session open", event)
	}
	if event := nextCtrlEvent(t, p); event != (peerCloseStream{channelID: "channel", messageID: "stream"}) {
		t.Fatalf("phase 5 = %#v, want close-stream", event)
	}
	if event := nextCtrlEvent(t, p); event.(peerSendRouting).update[0] != 1 {
		t.Fatalf("phase 6 = %#v, want routing update", event)
	}
	if _, ok := p.ctrlState.next(); ok {
		t.Fatal("unexpected control event after all phases")
	}
}

func TestHandleSessionOpenReplacesExistingStream(t *testing.T) {
	p := testPeer("peer")
	defer p.Close()
	p.streams = newHighCapTransport(p.ctx)
	slotCh := make(chan slotUpdate, 3)
	sessions := make(map[sessionKey]ethp2p.SendStream)
	key := sessionKey{channelID: "channel", messageID: "message"}
	firstOutbox := make(chan peerSendChunk)
	secondOutbox := make(chan peerSendChunk)

	p.handleSessionOpen(peerOpenSession{channelID: key.channelID, messageID: key.messageID, chunkOutbox: firstOutbox}, sessions, slotCh)
	first, ok := sessions[key].(*testStream)
	if !ok {
		t.Fatalf("first stream = %T, want *testStream", sessions[key])
	}
	if update := <-slotCh; update.key != key || update.slot != firstOutbox {
		t.Fatalf("first slot update = %#v", update)
	}

	p.handleSessionOpen(peerOpenSession{channelID: key.channelID, messageID: key.messageID, chunkOutbox: secondOutbox}, sessions, slotCh)
	second, ok := sessions[key].(*testStream)
	if !ok || second == first {
		t.Fatalf("replacement stream = %T, want a fresh *testStream", sessions[key])
	}
	if !first.closed {
		t.Fatal("replaced SESS stream was left open")
	}
	if update := <-slotCh; update.key != key || update.slot != nil {
		t.Fatalf("old slot removal = %#v", update)
	}
	if update := <-slotCh; update.key != key || update.slot != secondOutbox {
		t.Fatalf("replacement slot update = %#v", update)
	}
	second.CancelWrite(wire.Unspecified)
}

func TestLateSessionOpenAppliesInitialRouting(t *testing.T) {
	strategy := newMockStrategy()
	channel := newTestChannel(make(chan channelEvent, 8))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	channel.ctx = ctx
	channel.id = "channel"
	sess := channel.newSession("message", nil, false, strategy)
	defer sess.Close()
	channel.sessions = map[MessageID]*session[*testChunk, *testRouting]{"message": sess}
	bitmap := []byte{0b0000_0101} // The late peer advertises chunks 0 and 2.

	channel.handleSessionOpen(channelSessionOpen{
		peerID: "late",
		msg: &bcastpb.Sess_Open{
			Channel:       "channel",
			ObjectId:      "message",
			InitialUpdate: bitmap,
		},
	})

	if len(strategy.routingCalls) != 1 {
		t.Fatalf("strategy received %d routing updates, want one", len(strategy.routingCalls))
	}
	got := strategy.routingCalls[0]
	if got.peer != "late" || string(*got.update) != string(bitmap) {
		t.Fatalf("routing update = peer %q, data %v; want late/%v", got.peer, *got.update, bitmap)
	}
}

func TestPeerSessionEndDisposesIdleRelayWithoutTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		observer := &sessionDisposedObserver{disposed: make(chan sessionDisposedEvent, 1)}
		channel := newTestChannel(make(chan channelEvent, 8))
		channel.ctx = ctx
		channel.engine.config.Observer = observer
		channel.members = make(map[identity.PeerID]*PeerConn)
		channel.sessions = make(map[MessageID]*session[*testChunk, *testRouting])
		peer := testPeer("peer")
		defer peer.Close()

		sess := channel.newSession("message", nil, false, newMockStrategy())
		channel.sessions[sess.messageID] = sess
		channel.handlePeerBound(channelPeerChange{peerID: peer.id, peerRef: peer})
		createdAt := sess.createdAt
		channel.handle(channelPeerSessionEnded{messageID: sess.messageID, peerID: peer.id, peer: peer})

		if time.Since(createdAt) >= activeSessionTTL {
			t.Fatal("test advanced to the session TTL; stream end must be the disposal trigger")
		}
		select {
		case event := <-observer.disposed:
			if event.channelID != channel.id || event.messageID != "message" || event.reason != "all_peers_done" {
				t.Fatalf("session disposal = %+v, want test-channel/message/all_peers_done", event)
			}
		default:
			t.Fatal("last peer's stream end did not dispose the relay session")
		}
		if _, exists := channel.sessions[sess.messageID]; exists {
			t.Fatal("disposed session remains registered")
		}
	})
}

func TestLifecycleRetirementOrderAndBound(t *testing.T) {
	p := testPeer("peer")
	defer p.Close()
	tr := newTestChannel(make(chan channelEvent, 8))
	sessions := make([]*session[*testChunk, *testRouting], 100)
	for i := range sessions {
		s := tr.newSession(MessageID(fmt.Sprint(i)), nil, false, newMockStrategy())
		sessions[i] = s
		s.handlePeerAttached(p)
		if _, ok := nextCtrlEvent(t, p).(peerOpenSession); !ok {
			t.Fatal("missing open")
		}
	}
	// Live outbound state remains one entry per attached session, regardless
	// of whether its close transition has reached the control loop yet.
	for _, s := range sessions {
		s.handlePeerDropped(p.id)
	}
	_, retained := ctrlStateCounts(p.ctrlState)
	if retained > len(sessions) {
		t.Fatalf("retained %d session states for %d attachments", retained, len(sessions))
	}
	var closes []peerCtrlEvent
	for event, ok := p.ctrlState.next(); ok; event, ok = p.ctrlState.next() {
		closes = append(closes, event)
	}
	if len(closes) != len(sessions) {
		t.Fatalf("retirement events = %d, want %d", len(closes), len(sessions))
	}
	for i, event := range closes {
		closeEvent, ok := event.(peerCloseSession)
		if !ok || closeEvent.messageID != MessageID(fmt.Sprint(i)) {
			t.Fatalf("retirement %d = %#v, want close for %d", i, event, i)
		}
	}

	s := sessions[0]
	s.handlePeerAttached(p)
	first := nextCtrlEvent(t, p)
	s.handlePeerDropped(p.id)
	s.handlePeerAttached(p)
	second := nextCtrlEvent(t, p)
	if first.(peerOpenSession).messageID != s.messageID || second.(peerOpenSession).messageID != s.messageID {
		t.Fatal("reopen did not replace the closed generation")
	}
	if _, ok := p.ctrlState.next(); ok {
		t.Fatal("reopen retained the superseded close")
	}
	for _, s := range sessions {
		s.Close()
	}
	p.Close()
	_, retained = ctrlStateCounts(p.ctrlState)
	if retained != 0 {
		t.Fatalf("peer disposal retained %d session states", retained)
	}
}

func TestLifecycleDuplicateHandshakeAndBound(t *testing.T) {
	p := testPeer("peer")
	defer p.Close()
	inbox := make(chan channelEvent, 4)
	tr := newTestChannel(inbox)
	tr.members = make(map[identity.PeerID]*PeerConn)
	s := tr.newSession("message", nil, false, newMockStrategy())
	tr.sessions = map[MessageID]*session[*testChunk, *testRouting]{s.messageID: s}
	defer s.Close()
	e := &Engine{ctx: context.Background(), config: EngineConfig{Observer: NoOpObserver{}}, peers: make(map[identity.PeerID]*PeerConn), channels: map[ChannelID]*channelHandle{tr.id: {inbox: inbox, ctx: tr.ctx}}}
	p.announced = []ChannelID{tr.id} // as bindPeer records the Hello's channels
	e.handlePeerHandshake(p, []ChannelID{tr.id, tr.id, tr.id})
	if len(inbox) != 1 {
		t.Fatalf("duplicate handshake enrolled %d times", len(inbox))
	}
	tr.handle(<-inbox)
	for range 100 {
		tr.handlePeerBound(channelPeerChange{peerID: p.id, peerRef: p})
		s.handlePeerAttached(p)
	}
	_, retained := ctrlStateCounts(p.ctrlState)
	if retained != 1 {
		t.Fatalf("duplicate attachment retained %d session states, want 1", retained)
	}
	if _, ok := nextCtrlEvent(t, p).(peerOpenSession); !ok {
		t.Fatal("missing single open")
	}
	if _, ok := p.ctrlState.next(); ok {
		t.Fatal("duplicate attachment emitted another open")
	}
}

// With the control writer stalled (no control loop drains outbound state),
// local channel churn keeps one state entry per channel while preserving the
// final subscription view and any required retire/re-enrol pair.
func TestLifecycleAnnouncementBound(t *testing.T) {
	p := testPeer("peer")
	defer p.Close()
	ids := []ChannelID{"a", "b", "c"}
	e := &Engine{ctx: context.Background(), config: EngineConfig{Observer: NoOpObserver{}}, peers: make(map[identity.PeerID]*PeerConn), channels: make(map[ChannelID]*channelHandle)}
	create := func(id ChannelID) {
		e.addChannel(id, &channelHandle{inbox: make(chan channelEvent), ctx: context.Background(), done: make(chan struct{})}, nil)
	}
	remove := e.removeChannel
	// Our Hello listed a and b; b is dropped and c attached before the handshake.
	p.announced = []ChannelID{"a", "b"}
	create("a")
	create("c")
	e.handlePeerHandshake(p, nil)
	remote := map[ChannelID]bool{"a": true, "b": true}
	apply := func(event peerCtrlEvent) {
		switch event := event.(type) {
		case peerSubscribe:
			remote[event.channelID] = true
		case peerUnsubscribe:
			delete(remote, event.channelID)
		default:
			t.Fatalf("unexpected control event %T", event)
		}
	}
	for i := range 3000 {
		id := ids[i%len(ids)]
		if _, ok := e.channels[id]; ok {
			remove(id)
		} else {
			create(id)
		}
		channels, _ := ctrlStateCounts(p.ctrlState)
		if channels > len(ids) {
			t.Fatalf("step %d: %d channel states for %d IDs", i, channels, len(ids))
		}
	}
	for event, ok := p.ctrlState.next(); ok; event, ok = p.ctrlState.next() {
		apply(event)
	}
	for _, id := range ids {
		if _, local := e.channels[id]; local != remote[id] {
			t.Fatalf("channel %s: local=%v remote=%v after drain", id, local, remote[id])
		}
	}
	// The remote has now seen a subscribe for every live channel. Replacing one
	// must reach it as unsubscribe then subscribe, not as nothing.
	remove("a")
	create("a")
	first := nextCtrlEvent(t, p)
	second := nextCtrlEvent(t, p)
	if first != (peerUnsubscribe{channelID: "a"}) || second != (peerSubscribe{channelID: "a"}) {
		t.Fatalf("replacement emitted %v, %v", first, second)
	}
}

func TestLifecycleTopologyBound(t *testing.T) {
	for _, churn := range []bool{false, true} {
		t.Run(map[bool]string{false: "duplicate_subscribe", true: "unsubscribe_resubscribe"}[churn], func(t *testing.T) {
			p := testPeer("peer")
			defer p.Close()
			inbox := make(chan channelEvent, 4)
			tr := newTestChannel(inbox)
			tr.members = make(map[identity.PeerID]*PeerConn)
			tr.sessions = make(map[MessageID]*session[*testChunk, *testRouting])
			s := tr.newSession("one", nil, false, newMockStrategy())
			tr.sessions["one"] = s
			// Keep a second peer attached so unsubscribe doesn't dispose the session.
			other := testPeer("other")
			defer other.Close()
			s.handlePeerAttached(other)
			e := &Engine{ctx: context.Background(), config: EngineConfig{Observer: NoOpObserver{}}, peers: map[identity.PeerID]*PeerConn{p.id: p}, channels: map[ChannelID]*channelHandle{tr.id: {inbox: inbox, ctx: tr.ctx}}}
			p.subs = map[ChannelID]struct{}{}
			for range 500 {
				e.peerSubscribed(p, tr.id)
				if len(inbox) > 0 {
					tr.handle(<-inbox)
				}
				if churn {
					e.peerUnsubscribed(p, tr.id)
					if len(inbox) > 0 {
						tr.handle(<-inbox)
					}
				}
				live := 0
				if s.peers[p.id] != nil {
					live = 1
				}
				_, retained := ctrlStateCounts(p.ctrlState)
				if retained > live+1 {
					t.Fatal("outbound session state bound exceeded")
				}
			}
			_, retained := ctrlStateCounts(p.ctrlState)
			t.Logf("live sessions=%d attached peers=%d outbound session states=%d", len(tr.sessions), len(s.peers), retained)
			if retained > 1 {
				t.Errorf("one live session retained %d outbound states", retained)
			}
			s.Close()
			for event, ok := p.ctrlState.next(); ok; event, ok = p.ctrlState.next() {
				if _, ok := event.(peerCloseSession); ok {
					t.Fatal("unopened session emitted a close event")
				}
			}
			_, after := ctrlStateCounts(p.ctrlState)
			if after != 0 {
				t.Errorf("session disposal retained %d outbound states", after)
			}
		})
	}
}
