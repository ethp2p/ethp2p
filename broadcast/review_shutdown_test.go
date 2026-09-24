package broadcast

import (
	"bytes"
	"context"
	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/transport"
	"sync"
	"testing"
	"time"
)

func TestCapacityReviewStoppedChannelReleasesInbox(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p := newPeerConn(&Engine{ctx: t.Context(), config: EngineConfig{}}, t.Context(), "peer", nil)
	defer p.Close()
	inbox := make(chan channelEvent, 4)
	tr := newTestChannel(inbox)
	tr.ctx = ctx
	p.BindChannel("channel", tr.delivery)
	f := newOutcomeFixture(t)
	for range 3 {
		var payload bytes.Buffer
		_ = WriteFrame(&payload, &bcastpb.Chunk_Header{Channel: "channel", MessageId: "m", DataLength: 1})
		raw, in := f.incoming(t, CHUNK, &payload)
		_ = raw.out.Close()
		p.processChunk(in)
	}
	// Race unread payload producers with termination while earlier completed
	// headers remain buffered. The peer and QUIC connection stay alive.
	var producers sync.WaitGroup
	start := make(chan struct{})
	var probes []*wireReceiveProbe
	for range 32 {
		var payload bytes.Buffer
		_ = WriteFrame(&payload, &bcastpb.Chunk_Header{Channel: "channel", MessageId: "m", DataLength: 1})
		payload.WriteByte(7)
		raw, in := f.incoming(t, CHUNK, &payload)
		probes = append(probes, raw)
		producers.Go(func() { <-start; p.processChunk(in) })
	}
	close(start)
	cancel()
	tr.shutdown()
	producers.Wait()
	for _, raw := range probes {
		requireWireCancelCode(t, raw, 0)
	}
	p.chunkMu.Lock()
	held := len(p.heldChunks)
	p.chunkMu.Unlock()
	if held != 0 || p.parkedChunks != 0 || p.queuedChunks != 0 || len(tr.delivery.streams) != 0 {
		t.Errorf("stopped channel retained %d post-header streams in peer registry", held)
	}
	if p.ctx.Err() != nil {
		t.Fatal("channel stop closed peer")
	}
}
func TestCapacityReviewTopologyStoppedChannel(t *testing.T) {
	for _, which := range []string{"subscribe", "unsubscribe", "peer_down"} {
		t.Run(which, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			p := testPeer("peer")
			defer p.Close()
			inbox := make(chan channelEvent, 1)
			inbox <- channelWork{}
			stopped := make(chan struct{})
			close(stopped)
			id := ChannelID("channel")
			e := &Engine{ctx: ctx, config: EngineConfig{Observer: NoOpObserver{}}, peers: map[transport.PeerID]*PeerConn{p.id: p}, peerSubs: map[transport.PeerID]map[ChannelID]struct{}{p.id: {id: {}}}, channels: map[ChannelID]*channelHandle{id: {inbox: inbox, done: stopped}}}
			if which == "subscribe" {
				delete(e.peerSubs[p.id], id)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				switch which {
				case "subscribe":
					e.handle(engineEvent{kind: evPeerSubscribed, peer: p, channelID: id})
				case "unsubscribe":
					e.handle(engineEvent{kind: evPeerUnsubscribed, peer: p, channelID: id})
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
