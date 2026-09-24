package broadcast

import (
	"context"
	"fmt"
	"github.com/ethp2p/ethp2p/transport"
	"testing"
)

func TestLifecycleRetirementOrderAndNodeBound(t *testing.T) {
	p := testPeer("peer")
	defer p.Close()
	tr := newTestChannel(make(chan channelEvent, 8))
	sessions := make([]*session[*testChunk, *testRouting], 100)
	for i := range sessions {
		s := tr.newSession(MessageID(fmt.Sprint(i)), nil, false, newMockStrategy())
		sessions[i] = s
		s.handlePeerAttached(p)
		// The control loop has taken the open; subsequent closes must be sent.
		if _, ok := p.lifecycle.pop(); !ok {
			t.Fatal("missing open")
		}
	}
	for i, s := range sessions {
		s.handlePeerDropped(p.id)
		if p.lifecycle.items.Len() > 2*(len(sessions)-i-1)+1 {
			t.Fatal("retirement node bound exceeded")
		}
	}
	batch, ok := p.lifecycle.pop()
	if !ok {
		t.Fatal("missing retirements")
	}
	closes := batch.(*peerClosures).events
	if len(closes) != len(sessions) {
		t.Fatal("retirement lost")
	}
	for i, ev := range closes {
		if ev.(peerCloseSession).messageID != MessageID(fmt.Sprint(i)) {
			t.Fatal("retirements reordered")
		}
	}
	s := sessions[0]
	s.handlePeerAttached(p)
	first, _ := p.lifecycle.pop()
	s.handlePeerDropped(p.id)
	s.handlePeerAttached(p)
	second, _ := p.lifecycle.pop()
	third, _ := p.lifecycle.pop()
	if first.(peerOpenSession).messageID != s.messageID || second.(*peerClosures).events[0].(peerCloseSession).messageID != s.messageID || third.(peerOpenSession).messageID != s.messageID {
		t.Fatal("open, close, open order lost")
	}
	for _, s := range sessions {
		_ = s.Close()
	}
	p.Close()
	if p.lifecycle.items.Len() != 0 {
		t.Fatal("peer disposal retained entries")
	}
}

func TestLifecycleDuplicateHandshakeAndBound(t *testing.T) {
	p := testPeer("peer")
	defer p.Close()
	inbox := make(chan channelEvent, 4)
	tr := newTestChannel(inbox)
	tr.members = make(map[transport.PeerID]*PeerConn)
	s := tr.newSession("message", nil, false, newMockStrategy())
	tr.sessions = map[MessageID]*session[*testChunk, *testRouting]{s.messageID: s}
	defer s.Close()
	e := &Engine{ctx: context.Background(), config: EngineConfig{Observer: NoOpObserver{}}, peers: make(map[transport.PeerID]*PeerConn), peerSubs: make(map[transport.PeerID]map[ChannelID]struct{}), channels: map[ChannelID]*channelHandle{tr.id: {inbox: inbox, delivery: tr.delivery}}}
	e.handlePeerHandshake(engineEvent{peer: p, channels: []ChannelID{tr.id, tr.id, tr.id}})
	if len(inbox) != 1 {
		t.Fatalf("duplicate handshake enrolled %d times", len(inbox))
	}
	tr.handle(<-inbox)
	for range 100 {
		tr.handlePeerBound(channelPeerChange{peerID: p.id, peerRef: p})
		s.handlePeerAttached(p)
	}
	if p.lifecycle.items.Len() != 1 {
		t.Fatal("duplicate attachment queued another open")
	}
}

func TestCapacityReviewLifecycleTopologyBound(t *testing.T) {
	for _, churn := range []bool{false, true} {
		t.Run(map[bool]string{false: "duplicate_subscribe", true: "unsubscribe_resubscribe"}[churn], func(t *testing.T) {
			p := testPeer("peer")
			defer p.Close()
			inbox := make(chan channelEvent, 4)
			tr := newTestChannel(inbox)
			tr.members = make(map[transport.PeerID]*PeerConn)
			tr.sessions = make(map[MessageID]*session[*testChunk, *testRouting])
			s := tr.newSession("one", nil, false, newMockStrategy())
			tr.sessions["one"] = s
			// Keep a second peer attached so unsubscribe doesn't dispose the session.
			other := testPeer("other")
			defer other.Close()
			s.handlePeerAttached(other)
			e := &Engine{ctx: context.Background(), config: EngineConfig{Observer: NoOpObserver{}}, peers: map[transport.PeerID]*PeerConn{p.id: p}, peerSubs: map[transport.PeerID]map[ChannelID]struct{}{p.id: {}}, channels: map[ChannelID]*channelHandle{tr.id: {inbox: inbox, delivery: tr.delivery}}}
			for range 500 {
				e.handle(engineEvent{kind: evPeerSubscribed, peer: p, channelID: tr.id})
				if len(inbox) > 0 {
					tr.handle(<-inbox)
				}
				if churn {
					e.handle(engineEvent{kind: evPeerUnsubscribed, peer: p, channelID: tr.id})
					if len(inbox) > 0 {
						tr.handle(<-inbox)
					}
				}
				live := 0
				if s.peers[p.id] != nil {
					live = 1
				}
				if p.lifecycle.items.Len() > 2*live+1 {
					t.Fatal("lifecycle node bound exceeded")
				}
			}
			p.lifecycle.mu.Lock()
			queued := p.lifecycle.items.Len()
			p.lifecycle.mu.Unlock()
			t.Logf("live sessions=%d attached peers=%d lifecycle entries=%d", len(tr.sessions), len(s.peers), queued)
			if queued > 1 {
				t.Errorf("one live session retained %d lifecycle entries", queued)
			}
			_ = s.Close()
			p.lifecycle.mu.Lock()
			after := p.lifecycle.items.Len()
			p.lifecycle.mu.Unlock()
			if after != 0 {
				t.Errorf("session disposal left %d lifecycle entries", after)
			}
		})
	}
}
