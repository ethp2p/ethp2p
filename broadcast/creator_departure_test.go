package broadcast

import (
	"context"
	"errors"
	"fmt"
	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/transport"
	"testing"
)

func TestCreatorDepartureReviewUnsubscribedCreatorDeparture(t *testing.T) {
	inbox := make(chan channelEvent, 32)
	tr := newTestChannel(inbox)
	tr.sessions = make(map[MessageID]*session[*testChunk, *testRouting])
	other := testPeer("other")
	defer other.Close()
	tr.members = map[transport.PeerID]*PeerConn{other.id: other}
	tr.scheme.NewP = func() *testPreamble { return new(testPreamble) }
	tr.scheme.NewRelay = func(MessageID, *testPreamble) (Strategy[*testChunk, *testRouting], error) {
		return newMockStrategy(), nil
	}
	e := &Engine{ctx: context.Background(), config: EngineConfig{Observer: NoOpObserver{}, MaxLiveSessionsPerPeer: 1}, peers: make(map[transport.PeerID]*PeerConn), peerSubs: make(map[transport.PeerID]map[ChannelID]struct{}), channels: map[ChannelID]*channelHandle{tr.id: {inbox: inbox, delivery: tr.delivery}}}
	drain := func() {
		for len(inbox) > 0 {
			tr.handle(<-inbox)
		}
	}
	for i := 0; i < 20; i++ {
		p := newPeerConn(e, context.Background(), "same-peer", nil)
		e.handlePeerHandshake(engineEvent{peer: p, channels: []ChannelID{tr.id}})
		drain()
		tr.handleSessionOpen(channelSessionOpen{peer: p, peerID: p.id, msg: &bcastpb.Sess_Open{MessageId: fmt.Sprint(i)}})
		if len(p.liveSessions) != 1 {
			t.Fatal("setup did not charge creator")
		}
		e.handle(engineEvent{kind: evPeerUnsubscribed, peer: p, channelID: tr.id})
		drain()
		e.handlePeerGone(engineEvent{peer: p})
		if len(p.liveSessions) != 1 {
			t.Fatal("departure cleared accounting before disposal")
		}
		drain()
		if len(p.liveSessions) != 0 || len(tr.sessions) != 0 {
			t.Fatal("departed binding retained creator state")
		}
	}
	retained := len(tr.sessions)
	tr.shutdown()
	if retained != 0 {
		t.Fatalf("20 departed bindings of one peer retained %d sessions despite per-peer cap 1", retained)
	}
}

func TestCreatorDepartureReviewRetirementChurn(t *testing.T) {
	p := testPeer("peer")
	defer p.Close()
	tr := newTestChannel(make(chan channelEvent, 2048))
	var opened []*session[*testChunk, *testRouting]
	for i := 0; i < 100; i++ {
		s := tr.newSession(MessageID(fmt.Sprint(i)), nil, false, newMockStrategy())
		s.handlePeerAttached(p)
		p.lifecycle.pop() // control owned the open before it stalled
		opened = append(opened, s)
	}
	for _, s := range opened {
		s.notifyPeersComplete()
		s.handlePeerDropped(p.id)
		s.Close()
	}
	count := func() int {
		n := 0
		for e := p.lifecycle.items.Front(); e != nil; e = e.Next() {
			if b, ok := e.Value.(*peerClosures); ok {
				n += len(b.events)
			} else {
				n++
			}
		}
		return n
	}
	before := count()
	for i := 0; i < 1000; i++ {
		s := tr.newSession(MessageID(fmt.Sprint(i%100)), nil, false, newMockStrategy())
		s.handlePeerAttached(p)
		s.notifyPeersComplete()
		s.handlePeerDropped(p.id)
		s.Close()
		if p.lifecycle.items.Len() != 1 || count() != before {
			t.Fatal("churn grew retirement state")
		}
	}
	if before != 200 {
		t.Fatalf("retirement count=%d", before)
	}
}

func TestCreatorDepartureReviewFactoryFailureReleasesLease(t *testing.T) {
	tr := newTestChannel(make(chan channelEvent, 2))
	tr.sessions = make(map[MessageID]*session[*testChunk, *testRouting])
	tr.scheme.NewP = func() *testPreamble { return new(testPreamble) }
	tr.scheme.NewRelay = func(MessageID, *testPreamble) (Strategy[*testChunk, *testRouting], error) {
		return nil, errors.New("factory failed")
	}
	p := newPeerConn(&Engine{ctx: context.Background(), config: EngineConfig{MaxLiveSessionsPerPeer: 1}}, context.Background(), "peer", nil)
	defer p.Close()
	for range 100 {
		tr.handleSessionOpen(channelSessionOpen{peer: p, peerID: p.id, msg: &bcastpb.Sess_Open{MessageId: "m"}})
	}
	if len(p.liveSessions) != 0 || len(tr.sessions) != 0 {
		t.Fatal("factory failure retained lease")
	}
}

func TestCreatorDepartureReviewCreatorCapAcrossChannels(t *testing.T) {
	e := &Engine{ctx: context.Background(), config: EngineConfig{Observer: NoOpObserver{}, MaxLiveSessionsPerPeer: 1}, peers: make(map[transport.PeerID]*PeerConn), peerSubs: make(map[transport.PeerID]map[ChannelID]struct{}), channels: make(map[ChannelID]*channelHandle)}
	other := testPeer("other")
	defer other.Close()
	var channels []*Channel[*testChunk, *testRouting, *testPreamble]
	for i := 0; i < 20; i++ {
		tr := newTestChannel(make(chan channelEvent, 32))
		tr.id = ChannelID(fmt.Sprint(i))
		tr.sessions = make(map[MessageID]*session[*testChunk, *testRouting])
		tr.members = map[transport.PeerID]*PeerConn{other.id: other}
		tr.scheme.NewP = func() *testPreamble { return new(testPreamble) }
		tr.scheme.NewRelay = func(MessageID, *testPreamble) (Strategy[*testChunk, *testRouting], error) {
			return newMockStrategy(), nil
		}
		e.channels[tr.id] = &channelHandle{inbox: tr.inbox, delivery: tr.delivery}
		channels = append(channels, tr)
	}
	for _, tr := range channels {
		drain := func() {
			for len(tr.inbox) > 0 {
				tr.handle(<-tr.inbox)
			}
		}
		p := newPeerConn(e, context.Background(), "same-peer", nil)
		e.handlePeerHandshake(engineEvent{peer: p, channels: []ChannelID{tr.id}})
		drain()
		tr.handleSessionOpen(channelSessionOpen{peer: p, peerID: p.id, msg: &bcastpb.Sess_Open{MessageId: "m"}})
		e.handle(engineEvent{kind: evPeerUnsubscribed, peer: p, channelID: tr.id})
		drain()
		e.handlePeerGone(engineEvent{peer: p})
		if len(p.liveSessions) != 1 {
			t.Fatal("departure cleared accounting before disposal")
		}
		drain()
		if len(p.liveSessions) != 0 || len(tr.sessions) != 0 {
			t.Fatal("departed binding retained creator state")
		}
	}
	total := 0
	for _, tr := range channels {
		total += len(tr.sessions)
		tr.shutdown()
	}
	if total != 0 {
		t.Fatalf("one authenticated peer, cap 1 across 20 pre-existing channels: %d uncharged sessions after departure", total)
	}
}

func TestCreatorDepartureReviewReplacementDepartureOrdering(t *testing.T) {
	tr := newTestChannel(make(chan channelEvent, 8))
	tr.sessions = make(map[MessageID]*session[*testChunk, *testRouting])
	tr.scheme.NewP = func() *testPreamble { return new(testPreamble) }
	tr.scheme.NewRelay = func(MessageID, *testPreamble) (Strategy[*testChunk, *testRouting], error) {
		return newMockStrategy(), nil
	}
	other := testPeer("other")
	defer other.Close()
	tr.members = map[transport.PeerID]*PeerConn{other.id: other}
	e := &Engine{ctx: context.Background(), config: EngineConfig{Observer: NoOpObserver{}, MaxLiveSessionsPerPeer: 1}, peers: make(map[transport.PeerID]*PeerConn), peerSubs: make(map[transport.PeerID]map[ChannelID]struct{}), channels: map[ChannelID]*channelHandle{tr.id: {inbox: tr.inbox, delivery: tr.delivery}}}
	p := newPeerConn(e, context.Background(), "peer", nil)
	e.handlePeerHandshake(engineEvent{peer: p, channels: []ChannelID{tr.id}})
	tr.handle(<-tr.inbox)
	tr.handleSessionOpen(channelSessionOpen{peer: p, peerID: p.id, msg: &bcastpb.Sess_Open{MessageId: "m"}})
	// The channel actor may run between removePeer(old) and old.Close()
	// in handlePeerHandshake's live-binding replacement branch.
	e.removePeer(p)
	for len(tr.inbox) > 0 {
		tr.handle(<-tr.inbox)
	}
	p.Close()
	if len(p.liveSessions) != 1 {
		t.Fatal("close released lease before session disposal")
	}
	departure := <-tr.inbox
	// A delayed departure must not dispose a newer binding with the same ID.
	replacement := newPeerConn(e, context.Background(), p.id, nil)
	e.handlePeerHandshake(engineEvent{peer: replacement, channels: []ChannelID{tr.id}})
	for len(tr.inbox) > 0 {
		tr.handle(<-tr.inbox)
	}
	tr.handleSessionOpen(channelSessionOpen{peer: replacement, peerID: replacement.id, msg: &bcastpb.Sess_Open{MessageId: "new"}})
	tr.handle(departure)
	if len(p.liveSessions) != 0 || len(replacement.liveSessions) != 1 || tr.sessions["m"] != nil || tr.sessions["new"] == nil {
		t.Fatal("departure was not binding scoped")
	}
	e.handlePeerGone(engineEvent{peer: replacement})
	for len(tr.inbox) > 0 {
		tr.handle(<-tr.inbox)
	}
	retained := len(tr.sessions)
	tr.shutdown()
	if retained != 0 {
		t.Fatalf("replacement notification before cancellation retained %d creator sessions", retained)
	}
}

func TestCreatorDepartureCancelledReservation(t *testing.T) {
	inbox := make(chan channelEvent, 8)
	tr := newTestChannel(inbox)
	tr.members = make(map[transport.PeerID]*PeerConn)
	tr.sessions = make(map[MessageID]*session[*testChunk, *testRouting])
	e := &Engine{ctx: t.Context(), config: EngineConfig{Observer: NoOpObserver{}, MaxLiveSessionsPerPeer: 1}, peers: make(map[transport.PeerID]*PeerConn), peerSubs: make(map[transport.PeerID]map[ChannelID]struct{}), channels: map[ChannelID]*channelHandle{tr.id: {inbox: inbox, delivery: tr.delivery}}}
	p := newPeerConn(e, t.Context(), "peer", nil)
	e.handlePeerHandshake(engineEvent{peer: p, channels: []ChannelID{tr.id}})
	for len(inbox) > 0 {
		tr.handle(<-inbox)
	}
	f := newOutcomeFixture(t)
	raw, stream := f.incoming(t, SESS, nil)
	// Model an already-buffered open processed after shutdown wins the race.
	inbox <- channelSessionOpen{peer: p, peerID: p.id, stream: stream, msg: &bcastpb.Sess_Open{MessageId: "late"}}
	e.handlePeerGone(engineEvent{peer: p})
	for len(inbox) > 0 {
		tr.handle(<-inbox)
	}
	requireWireCancelCode(t, raw, 0)
	if len(p.liveSessions) != 0 || len(tr.sessions) != 0 {
		t.Fatal("cancelled reservation created a session or lease")
	}
	tr.shutdown()
}
