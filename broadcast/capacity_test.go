package broadcast

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/wire"
)

func capacityTake[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("capacity progress timeout")
		var zero T
		return zero
	}
}

func TestLifecycleElisionAndCloseStreamRetention(t *testing.T) {
	p := testPeer("peer")
	defer p.cancel(nil)
	p.ctrlState.openSession(peerOpenSession{messageID: "discard"})
	p.ctrlState.closeSession(sessionKey{messageID: "discard"})
	p.ctrlState.openSession(peerOpenSession{messageID: "keep"})
	p.ctrlState.closeSessionStream(sessionKey{messageID: "keep"})
	first, ok := p.ctrlState.next()
	if !ok || first.(peerOpenSession).messageID != "keep" {
		t.Fatalf("first = %#v", first)
	}
	second, ok := p.ctrlState.next()
	if !ok || second.(peerCloseStream).messageID != "keep" {
		t.Fatalf("second = %#v", second)
	}
	p.ctrlState.closeSession(sessionKey{messageID: "keep"})
	p.Close()
	p.ctrlState.openSession(peerOpenSession{messageID: "late"})
	if _, ok := p.ctrlState.next(); ok {
		t.Fatal("closed peer retained outbound control state")
	}
}

type stalledSessionOpener struct {
	entered chan struct{}
	once    sync.Once
}

func (*stalledSessionOpener) Close(wire.Code) {}

func (s *stalledSessionOpener) OpenUniStream(ctx context.Context) (ethp2p.SendStream, error) {
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	return nil, ctx.Err()
}

// A session open stalled in the control loop must not stop the channel actor
// routing an independent chunk for an existing session.
func TestChannelRoutesChunkWhileSessionOpenStalled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p := testPeer("peer")
	defer p.cancel(nil)
	opener := &stalledSessionOpener{entered: make(chan struct{})}
	p.handle = opener
	ctrlDone := make(chan struct{})
	go func() { defer close(ctrlDone); p.runCtrlLoop(make(chan slotUpdate, 1)) }()
	inbox := make(chan channelEvent, 64)
	tr := newTestChannel(inbox)
	tr.ctx = ctx
	tr.members = make(map[identity.PeerID]*PeerConn)
	tr.sessions = make(map[MessageID]*session[*testChunk, *testRouting])
	for i := range streamQueueCap + 3 {
		id := MessageID(fmt.Sprintf("message-%d", i))
		tr.sessions[id] = tr.newSession(id, nil, false, newMockStrategy())
	}
	readInbox := make(chan channelEvent, 4)
	tr.sessions["message-0"].channelInbox = readInbox
	actorDone := make(chan struct{})
	go func() {
		defer close(actorDone)
		tr.handlePeerBound(channelPeerChange{peerID: p.id, peerRef: p})
		for {
			select {
			case event := <-inbox:
				tr.handle(event)
			case <-ctx.Done():
				return
			}
		}
	}()
	capacityTake(t, opener.entered)
	f := newOutcomeFixture(t)
	raw, stream := f.incoming(t, bytes.NewReader(wire.AppendFrame(nil, dataFrame, []byte("data"))))
	// The reader waits for FIN after DATA, since DATA must be the last frame.
	_ = raw.out.Close()
	// Observe the routing result through the read-completion event before
	// passing it back to the actor, without inspecting concurrent actor state.
	// This session is selected before the actor starts handling the chunk.
	// Other sessions never start readers in this test.
	inbox <- channelChunkStream{peerID: p.id, frame: &bcastpb.Chunk_Header{ObjectId: "message-0", ChunkId: []byte("id"), DataLength: 4}, stream: stream}
	event := capacityTake(t, readInbox)
	data, ok := event.(channelChunkData)
	if !ok || string(data.payload) != "data" {
		t.Fatalf("chunk = %#v", event)
	}
	cancel()
	p.cancel(nil)
	capacityTake(t, actorDone)
	capacityTake(t, ctrlDone)
	for _, s := range tr.sessions {
		s.Close()
	}
}

func TestPeerDataLoopContinuesAfterOtherPeerDrops(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	inbox := make(chan channelEvent, 8)
	tr := newTestChannel(inbox)
	tr.ctx = ctx
	sess := tr.newSession("message", nil, false, newMockStrategy())
	defer sess.Close()

	stalled := testPeer("stalled")
	stalled.engine = &Engine{config: EngineConfig{Observer: NoOpObserver{}}}
	stalledOpener := &stalledSessionOpener{entered: make(chan struct{})}
	stalled.handle = stalledOpener
	live := testPeer("live")
	live.engine = &Engine{config: EngineConfig{Observer: NoOpObserver{}}}
	liveTransport := newHighCapTransport(live.ctx)
	live.handle = liveTransport
	sess.handlePeerAttached(stalled)
	sess.handlePeerAttached(live)

	stalledSlots := make(chan slotUpdate, 2)
	liveSlots := make(chan slotUpdate, 2)
	stalledCtrlDone := make(chan struct{})
	stalledDataDone := make(chan struct{})
	liveCtrlDone := make(chan struct{})
	liveDataDone := make(chan struct{})
	go func() { defer close(stalledCtrlDone); stalled.runCtrlLoop(stalledSlots) }()
	go func() { defer close(stalledDataDone); stalled.runDataLoop(stalledSlots) }()
	go func() { defer close(liveCtrlDone); live.runCtrlLoop(liveSlots) }()
	go func() { defer close(liveDataDone); live.runDataLoop(liveSlots) }()
	t.Cleanup(func() {
		stalled.Close()
		live.Close()
		<-stalledCtrlDone
		<-stalledDataDone
		<-liveCtrlDone
		<-liveDataDone
	})

	capacityTake(t, stalledOpener.entered)
	// The live control loop has opened its session stream and registered the
	// chunk slot before the stalled peer is dropped.
	capacityTake(t, liveTransport.streamSend)
	capacityTake(t, liveTransport.streamSend)

	stalled.Close()
	sess.handlePeerDropped(stalled.id)
	capacityTake(t, stalledCtrlDone)
	capacityTake(t, stalledDataDone)

	sess.sendChunk(live.id, &testChunk{ID: 7, Data: []byte("chunk-id")}, []byte("payload"))
	event := capacityTake(t, inbox)
	sent, ok := event.(channelChunkSent)
	if !ok || sent.peerID != live.id || sent.err != nil {
		t.Fatalf("live peer chunk result = %#v, want successful send", event)
	}
}

// Chunk streams waiting for an opener have no count cap: QUIC stream credit
// bounds them, so every one of them is routed once the openers run.
func TestUnreadStreamsHaveNoCountCap(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	engine := &Engine{ctx: ctx}
	p := newPeerConn(engine, ctx, "peer", nil)
	p.handshook = true
	inbox := make(chan channelEvent, 100)
	p.BindChannel("channel", &channelHandle{inbox: inbox, ctx: ctx})
	f := newOutcomeFixture(t)
	const count = 80
	for i := range count {
		var payload bytes.Buffer
		if err := writeFrame(&payload, headerFrame, &bcastpb.Chunk_Header{Channel: "channel", ObjectId: "m", DataLength: 1}); err != nil {
			t.Fatal(err)
		}
		payload.Write(wire.AppendFrame(nil, dataFrame, []byte{byte(i)}))
		_, stream := f.incoming(t, &payload)
		p.enqueueStream(stream)
	}
	for range maxOpeningReads {
		p.wg.Go(p.runOpener)
	}
	seen := make(map[byte]bool, count)
	for range count {
		e := capacityTake(t, inbox).(channelChunkStream)
		typ, data, err := wire.ReadFrame(e.stream, 1)
		if err != nil || typ != dataFrame || len(data) != 1 || seen[data[0]] {
			t.Fatalf("payload = %d %x, %v", typ, data, err)
		}
		seen[data[0]] = true
		e.stream.CancelRead(wire.Unspecified)
	}
	p.Close()
}

// Post-header chunk streams parked or queued for a peer are cancelled once the
// channel processes that peer's unbind; the QUIC connection may stay up.
func TestChunkStreamsCancelledOnPeerUnbind(t *testing.T) {
	inbox := make(chan channelEvent, 8)
	tr := newTestChannel(inbox)
	tr.parked = make(map[MessageID]*parkedGroup)
	tr.sessions = make(map[MessageID]*session[*testChunk, *testRouting])
	sess := tr.newSession("queued", nil, false, newMockStrategy())
	sess.readSem = make(chan struct{}, 1)
	sess.readSem <- struct{}{}
	tr.sessions["queued"] = sess
	defer sess.Close()
	f := newOutcomeFixture(t)
	var probes []*wireReceiveProbe
	for _, id := range []MessageID{"parked", "queued"} {
		payload := bytes.NewReader(wire.AppendFrame(nil, dataFrame, []byte{7}))
		raw, in := f.incoming(t, payload)
		probes = append(probes, raw)
		tr.handleChunk(channelChunkStream{peerID: "peer", frame: &bcastpb.Chunk_Header{ObjectId: string(id), DataLength: 1}, stream: in})
	}
	if len(tr.parked["parked"].streams) != 1 || len(sess.readQueue) != 1 {
		t.Fatalf("parked=%d queued=%d", len(tr.parked["parked"].streams), len(sess.readQueue))
	}
	tr.handlePeerUnbound(channelPeerChange{peerID: "peer"})
	for _, raw := range probes {
		requireCode(t, raw.CancelReadCodes(), 0)
	}
	if len(tr.parked) != 0 || len(sess.readQueue) != 0 {
		t.Fatal("unbind retained post-header streams")
	}
}

func TestSessionReadQueueDeliversBurst(t *testing.T) {
	f := newOutcomeFixture(t)
	inbox := make(chan channelEvent, 256)
	s := newTestSession(newMockStrategy(), inbox)
	defer s.Close()
	s.readSem = make(chan struct{}, 1)
	s.readSem <- struct{}{}
	const count = 80
	for i := range count {
		raw, in := f.incoming(t, bytes.NewReader(wire.AppendFrame(nil, dataFrame, []byte{byte(i)})))
		// The reader waits for FIN after DATA, since DATA must be the last frame.
		_ = raw.out.Close()
		s.handleChunkStream("peer", []byte{byte(i)}, 1, in)
	}
	if len(s.readQueue) != count || len(inbox) != 0 {
		t.Fatal("a read started while the payload slot was held")
	}
	<-s.readSem
	s.drainReads()
	for received := 0; received < count; {
		switch event := capacityTake(t, inbox).(type) {
		case channelChunkData:
			if len(event.payload) != 1 || event.payload[0] != byte(received) {
				t.Fatalf("payload %d = %x", received, event.payload)
			}
			received++
		case channelReadDone:
			s.drainReads()
		default:
			t.Fatalf("unexpected event %T", event)
		}
	}
}

func TestChannelRejectsFullParkedChunkQueueAsUnspecified(t *testing.T) {
	const messageID MessageID = "msg-full"
	channel := &Channel[*testChunk, *testRouting, *testPreamble]{
		parked: map[MessageID]*parkedGroup{
			messageID: {streams: make([]channelChunkStream, maxParkedChunks)},
		},
	}
	raw, stream := newOutcomeFixture(t).incoming(t, nil)
	chunk := newFakeChunk("peer1", messageID, nil)
	chunk.stream = stream

	channel.handleChunk(chunk)

	requireCode(t, raw.CancelReadCodes(), 0)
}

// Routing updates are snapshots: enqueuing one drops its queued predecessors
// for the same session, and an unsubscribe purges the channel's queued
// updates while the retire/re-enrol pair and the post-subscribe snapshot are
// retained.
func TestLifecycleRoutingCoalescing(t *testing.T) {
	p := testPeer("peer")
	defer p.cancel(nil)
	if p.ctrlState.setRouting(peerSendRouting{channelID: "ch", messageID: "missing", update: []byte{0}}) {
		t.Fatal("routing update created a missing session")
	}
	p.ctrlState.announceInitial([]ChannelID{"ch"})
	p.ctrlState.openSession(peerOpenSession{channelID: "ch", messageID: "m"})
	p.ctrlState.openSession(peerOpenSession{channelID: "ch", messageID: "n"})
	if _, ok := nextCtrlEvent(t, p).(peerOpenSession); !ok {
		t.Fatal("missing first session open")
	}
	if _, ok := nextCtrlEvent(t, p).(peerOpenSession); !ok {
		t.Fatal("missing second session open")
	}
	for _, data := range []byte{1, 2, 3} {
		if !p.ctrlState.setRouting(peerSendRouting{channelID: "ch", messageID: "m", update: []byte{data}}) {
			t.Fatal("routing update ignored for open session")
		}
	}
	p.ctrlState.setRouting(peerSendRouting{channelID: "ch", messageID: "n", update: []byte{8}})
	p.ctrlState.announce("ch", false)
	p.ctrlState.announce("ch", true)
	if !p.ctrlState.setRouting(peerSendRouting{channelID: "ch", messageID: "m", update: []byte{4}}) {
		t.Fatal("routing update ignored after re-enrolment")
	}

	if ev := nextCtrlEvent(t, p); ev != (peerUnsubscribe{channelID: "ch"}) {
		t.Fatalf("first = %T, want peerUnsubscribe", ev)
	}
	if ev := nextCtrlEvent(t, p); ev != (peerSubscribe{channelID: "ch"}) {
		t.Fatalf("second = %T, want peerSubscribe", ev)
	}
	ev := nextCtrlEvent(t, p)
	if routing, ok := ev.(peerSendRouting); !ok || len(routing.update) != 1 || routing.update[0] != 4 {
		t.Fatalf("third = %#v, want routing update 4", ev)
	}
	if _, ok := p.ctrlState.next(); ok {
		t.Fatal("state retained superseded or unsubscribed routing updates")
	}

	// Latest-only is per session: a second message keeps its own snapshot.
	p.ctrlState.setRouting(peerSendRouting{channelID: "ch", messageID: "m", update: []byte{5}})
	p.ctrlState.setRouting(peerSendRouting{channelID: "ch", messageID: "m", update: []byte{6}})
	p.ctrlState.setRouting(peerSendRouting{channelID: "ch", messageID: "n", update: []byte{7}})
	for _, want := range []byte{6, 7} {
		ev := nextCtrlEvent(t, p)
		if routing, ok := ev.(peerSendRouting); !ok || len(routing.update) != 1 || routing.update[0] != want {
			t.Fatalf("next = %#v, want routing update %d", ev, want)
		}
	}
}
