package broadcast

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/wire"
)

// Stopping a channel cancels every chunk stream a peer routed to it, including
// one whose send to the inbox lands after the channel's final drain (the late
// send in routeChunk), without closing the peer. The race can reach that
// branch but does not force it.
func TestStoppedChannelReleasesInbox(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p := newPeerConn(&Engine{ctx: t.Context(), config: EngineConfig{}}, t.Context(), "peer", nil)
	defer p.Close()
	p.handshook = true
	inbox := make(chan channelEvent, 4)
	tr := newTestChannel(inbox)
	tr.ctx = ctx
	p.BindChannel("channel", &channelHandle{inbox: inbox, ctx: ctx})
	f := newOutcomeFixture(t)
	for range 3 {
		var payload bytes.Buffer
		_ = writeFrame(&payload, headerFrame, &bcastpb.Chunk_Header{Channel: "channel", ObjectId: "m", DataLength: 1})
		raw, in := f.incoming(t, &payload)
		_ = raw.out.Close()
		p.open(in)
	}
	// Race unread payload producers with termination while earlier completed
	// headers remain buffered. The peer and QUIC connection stay alive.
	var producers sync.WaitGroup
	start := make(chan struct{})
	var probes []*wireReceiveProbe
	for range 32 {
		var payload bytes.Buffer
		_ = writeFrame(&payload, headerFrame, &bcastpb.Chunk_Header{Channel: "channel", ObjectId: "m", DataLength: 1})
		payload.Write(wire.AppendFrame(nil, dataFrame, []byte{7}))
		raw, in := f.incoming(t, &payload)
		probes = append(probes, raw)
		producers.Go(func() { <-start; p.open(in) })
	}
	close(start)
	cancel()
	tr.shutdown()
	producers.Wait()
	for _, raw := range probes {
		requireCode(t, raw.CancelReadCodes(), 0)
	}
	if p.ctx.Err() != nil {
		t.Fatal("channel stop closed peer")
	}
}

func TestTopologyEventsSkipStoppedChannel(t *testing.T) {
	for _, which := range []string{"subscribe", "unsubscribe", "peer_down"} {
		t.Run(which, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			p := testPeer("peer")
			defer p.Close()
			inbox := make(chan channelEvent, 1)
			inbox <- channelReadDone{}
			stopped := make(chan struct{})
			close(stopped)
			id := ChannelID("channel")
			e := &Engine{ctx: ctx, config: EngineConfig{Observer: NoOpObserver{}}, peers: map[identity.PeerID]*PeerConn{p.id: p}, channels: map[ChannelID]*channelHandle{id: {inbox: inbox, done: stopped}}}
			p.subs = map[ChannelID]struct{}{id: {}}
			if which == "subscribe" {
				delete(p.subs, id)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				switch which {
				case "subscribe":
					e.peerSubscribed(p, id)
				case "unsubscribe":
					e.peerUnsubscribed(p, id)
				case "peer_down":
					e.removePeer(p)
				}
			}()
			select {
			case <-done:
			case <-time.After(50 * time.Millisecond):
				t.Error("engine stalled on stopped channel inbox")
				cancel()
				<-done
			}
		})
	}
}
