package broadcast

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/ethp2p/ethp2p/transport"
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

func TestLifecycleFIFO(t *testing.T) {
	p := testPeer("peer")
	defer p.cancel()
	p.enqueueLifecycle(peerOpenSession{messageID: "discard"})
	p.enqueueLifecycle(peerCloseSession{messageID: "discard"})
	p.enqueueLifecycle(peerOpenSession{messageID: "keep"})
	p.enqueueLifecycle(peerCloseStream{messageID: "keep"})
	first, ok := p.lifecycle.pop()
	if !ok || first.(peerOpenSession).messageID != "keep" {
		t.Fatalf("first = %#v", first)
	}
	second, ok := p.lifecycle.pop()
	if !ok || second.(*peerClosures).events[0].(peerCloseStream).messageID != "keep" {
		t.Fatalf("second = %#v", second)
	}
	p.enqueueLifecycle(peerCloseSession{messageID: "keep"})
	p.Close()
	p.enqueueLifecycle(peerOpenSession{messageID: "late"})
	if _, ok := p.lifecycle.pop(); ok {
		t.Fatal("closed peer retained lifecycle work")
	}
}

type stalledSessionOpener struct {
	entered chan struct{}
	once    sync.Once
}

func (s *stalledSessionOpener) OpenUniStream(ctx context.Context, _ wire.Selector) (ethp2p.SendStream, error) {
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	return nil, ctx.Err()
}

// A full old ctrlQ stalls attachment on the channel actor; that same actor
// must remain able to route an independent CHUNK for an existing session.
func TestChannelRoutesChunkWhileSessionOpenStalled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p := testPeer("peer")
	defer p.cancel()
	opener := &stalledSessionOpener{entered: make(chan struct{})}
	p.streams = opener
	ctrlDone := make(chan struct{})
	go func() { defer close(ctrlDone); p.runCtrlLoop(make(chan slotUpdate, 1)) }()
	inbox := make(chan channelEvent, 64)
	tr := newTestChannel(inbox)
	tr.ctx = ctx
	tr.members = make(map[transport.PeerID]*PeerConn)
	tr.sessions = make(map[MessageID]*session[*testChunk, *testRouting])
	for i := range ctrlQCap + 3 {
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
	_, stream := f.incoming(t, CHUNK, bytes.NewReader([]byte("data")))
	// Observe the routing result through the read-completion event before
	// passing it back to the actor, without inspecting concurrent actor state.
	// This session is selected before the actor starts handling the chunk.
	// Other sessions never start readers in this test.
	inbox <- channelChunkStream{peerID: p.id, frame: &bcastpb.Chunk_Header{MessageId: "message-0", ChunkId: []byte("id"), DataLength: 4}, stream: stream}
	event := capacityTake(t, readInbox)
	data, ok := event.(channelChunkData)
	if !ok || string(data.payload) != "data" {
		t.Fatalf("chunk = %#v", event)
	}
	cancel()
	p.cancel()
	capacityTake(t, actorDone)
	capacityTake(t, ctrlDone)
	for _, s := range tr.sessions {
		_ = s.Close()
	}
}

func TestChunkFIFOAndHeaderBudgetWait(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		t.Run(fmt.Sprint(blocked), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			engine := &Engine{ctx: ctx, config: EngineConfig{MaxInboundChunkStreams: 1}}
			p := newPeerConn(engine, ctx, "peer", nil)
			p.ready = true
			p.finishHandshake(true, nil)
			inbox := make(chan channelEvent, 100)
			p.BindChannel("channel", newChannelDelivery(inbox))
			if blocked {
				p.chunkSem <- struct{}{}
			}
			f := newOutcomeFixture(t)
			const count = 80 // exceeds the old streamQueueCap of 64
			for i := range count {
				var payload bytes.Buffer
				if err := WriteFrame(&payload, &bcastpb.Chunk_Header{Channel: "channel", MessageId: "m", DataLength: 1}); err != nil {
					t.Fatal(err)
				}
				payload.WriteByte(byte(i))
				_, stream := f.incoming(t, CHUNK, &payload)
				p.enqueueStream(CHUNK, stream)
			}
			p.wg.Go(p.runChunkAcceptLoop)
			if blocked {
				select {
				case <-inbox:
					t.Fatal("read without header slot")
				case <-time.After(20 * time.Millisecond):
				}
				<-p.chunkSem
			}
			for i := range count {
				e := capacityTake(t, inbox).(channelChunkStream)
				data := make([]byte, 1)
				if _, err := io.ReadFull(e.stream, data); err != nil || data[0] != byte(i) {
					t.Fatalf("payload %d = %x, %v", i, data, err)
				}
				e.stream.CancelRead(wire.Unspecified)
			}
			p.Close()
		})
	}
}

func TestParkedChunksBoundedAcrossMessagesAndDisposed(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p := newPeerConn(&Engine{ctx: ctx, config: EngineConfig{}}, ctx, "peer", nil)
	t.Cleanup(p.Close)
	inbox := make(chan channelEvent, 1)
	p.BindChannel("channel", newChannelDelivery(inbox))
	tr := newTestChannel(inbox)
	tr.parked = make(map[MessageID][]channelChunkStream)
	f := newOutcomeFixture(t)
	for i := range maxPeerParkedChunks + 1 {
		var payload bytes.Buffer
		_ = WriteFrame(&payload, &bcastpb.Chunk_Header{Channel: "channel", MessageId: fmt.Sprint(i), DataLength: 1})
		payload.WriteByte(7)
		raw, in := f.incoming(t, CHUNK, &payload)
		p.processChunk(in)
		tr.handleChunk(capacityTake(t, inbox).(channelChunkStream))
		if i == maxPeerParkedChunks {
			requireWireCancelCode(t, raw, 4)
		}
	}
	p.chunkMu.Lock()
	count := p.parkedChunks
	p.chunkMu.Unlock()
	if count != maxPeerParkedChunks {
		t.Fatalf("parked = %d", count)
	}
	tr.handlePeerUnbound(channelPeerChange{peerID: p.id})
	if len(tr.parked) != 0 || p.parkedChunks != 0 || len(p.heldChunks) != 0 {
		t.Fatal("peer departure retained parked streams")
	}
}

func TestSessionQueueBoundAndDisposal(t *testing.T) {
	for _, end := range []string{"peer", "session", "engine"} {
		t.Run(end, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			engine := &Engine{ctx: ctx, config: EngineConfig{MaxQueuedChunkStreams: 2}}
			p := newPeerConn(engine, ctx, "peer", nil)
			t.Cleanup(p.Close)
			inbox := make(chan channelEvent, 8)
			p.BindChannel("channel", newChannelDelivery(inbox))
			s := newTestSession(newMockStrategy(), inbox)
			s.readSem = make(chan struct{}, 1)
			s.readSem <- struct{}{}
			f := newOutcomeFixture(t)
			for i := range 3 {
				var payload bytes.Buffer
				_ = WriteFrame(&payload, &bcastpb.Chunk_Header{Channel: "channel", MessageId: "m", DataLength: 1})
				payload.WriteByte(7)
				raw, in := f.incoming(t, CHUNK, &payload)
				p.processChunk(in)
				e := capacityTake(t, inbox).(channelChunkStream)
				s.handleChunkStream(p.id, e.frame.ChunkId, e.frame.DataLength, e.stream)
				if i == 2 {
					requireWireCancelCode(t, raw, 0)
				}
			}
			if p.queuedChunks != 2 {
				t.Fatalf("queued = %d", p.queuedChunks)
			}
			switch end {
			case "peer":
				p.Close()
			case "session":
				_ = s.Close()
			case "engine":
				cancel()
				engine.peers = map[transport.PeerID]*PeerConn{p.id: p}
				engine.shutdown()
			}
			if p.queuedChunks != 0 || len(p.heldChunks) != 0 {
				t.Fatal("held count leaked")
			}
			if end != "session" {
				_ = s.Close()
			}
		})
	}
}

func TestSessionQueueRejectsCompletedHeaderFIN(t *testing.T) {
	ctx := t.Context()
	engine := &Engine{ctx: ctx, config: EngineConfig{MaxQueuedChunkStreams: 2}}
	p := newPeerConn(engine, ctx, "peer", nil)
	t.Cleanup(p.Close)
	inbox := make(chan channelEvent, 8)
	p.BindChannel("channel", newChannelDelivery(inbox))
	s := newTestSession(newMockStrategy(), inbox)
	defer func() {
		select {
		case <-s.Done():
		default:
			_ = s.Close()
		}
	}()
	s.readSem = make(chan struct{}, 1)
	s.readSem <- struct{}{}
	f := newOutcomeFixture(t)
	for i := range 3 {
		var payload bytes.Buffer
		_ = WriteFrame(&payload, &bcastpb.Chunk_Header{Channel: "channel", MessageId: "m", DataLength: 1})
		raw, in := f.incoming(t, CHUNK, &payload)
		if err := raw.out.Close(); err != nil {
			t.Fatal(err)
		}
		p.processChunk(in)
		e := capacityTake(t, inbox).(channelChunkStream)
		// Observe EOF before admission: this stream has already returned credit.
		var b [1]byte
		if n, err := e.stream.Read(b[:]); n != 0 || err != io.EOF {
			t.Fatalf("completed header: read = %d, %v", n, err)
		}
		s.handleChunkStream(p.id, e.frame.ChunkId, e.frame.DataLength, e.stream)
		want := min(i+1, 2)
		if len(s.readQueue) != want || p.queuedChunks != want || len(p.heldChunks) != want {
			t.Fatalf("stream %d: queue=%d count=%d held=%d", i, len(s.readQueue), p.queuedChunks, len(p.heldChunks))
		}
		if i == 2 {
			for _, queued := range s.readQueue {
				if queued.stream == e.stream {
					t.Fatal("excess completed stream was queued")
				}
			}
			if _, held := p.heldChunks[e.stream.(*heldChunk)]; held {
				t.Fatal("excess completed stream retained ownership")
			}
		}
	}
	_ = s.Close()
	if p.queuedChunks != 0 || len(p.heldChunks) != 0 {
		t.Fatal("session close retained completed streams")
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
		_, in := f.incoming(t, CHUNK, bytes.NewReader([]byte{byte(i)}))
		s.handleChunkStream("peer", []byte{byte(i)}, 1, in)
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
