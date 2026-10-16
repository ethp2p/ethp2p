package broadcast

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/wire"
)

// fakeReceiveStream wraps a bytes.Reader to satisfy ethp2p.ReceiveStream
// in tests. CancelRead and SetReadDeadline are no-ops.
type fakeReceiveStream struct {
	*bytes.Reader
}

func (f *fakeReceiveStream) CancelRead(wire.Code)            {}
func (f *fakeReceiveStream) SetReadDeadline(time.Time) error { return nil }

var _ ethp2p.ReceiveStream = (*fakeReceiveStream)(nil)

type sessionDisposedEvent struct {
	channelID ChannelID
	messageID MessageID
	reason    string
}

type sessionDisposedObserver struct {
	NoOpObserver
	disposed chan sessionDisposedEvent
}

func (o *sessionDisposedObserver) OnSessionDisposed(channelID ChannelID, messageID MessageID, reason string) {
	o.disposed <- sessionDisposedEvent{channelID: channelID, messageID: messageID, reason: reason}
}

type recordingObserver struct {
	NoOpObserver
	attached chan error
}

func (o *recordingObserver) OnChannelAttached(_ ChannelID, err error) {
	o.attached <- err
}

// newFakeChunk builds a channelChunkStream with a fakeReceiveStream backed by
// the given payload. Convenience helper for tests that need to send
// inbound chunks through the channel inbox.
func newFakeChunk(peer identity.PeerID, msgID MessageID, payload []byte) channelChunkStream {
	return channelChunkStream{
		peerID: peer,
		frame: &bcastpb.Chunk_Header{
			ObjectId:   string(msgID),
			ChunkId:    append([]byte(nil), payload...),
			DataLength: uint32(len(payload)),
		},
		stream: &fakeReceiveStream{bytes.NewReader(wire.AppendFrame(nil, dataFrame, payload))},
	}
}

// newMockScheme returns a Scheme factory that creates mockStrategy
// instances for both origin and relay sessions. The relay coder
// decodes on first receive by default.
func newMockScheme() Scheme[*testChunk, *testRouting, *testPreamble] {
	return newMockSchemeWithFactories(nil, nil)
}

func newMockSchemeWithFactories(
	originHook func(MessageID, []byte) (Strategy[*testChunk, *testRouting], *testPreamble, error),
	relayHook func(MessageID, *testPreamble) (Strategy[*testChunk, *testRouting], error),
) Scheme[*testChunk, *testRouting, *testPreamble] {
	return Scheme[*testChunk, *testRouting, *testPreamble]{
		NewOrigin: func(msgID MessageID, payload []byte) (Strategy[*testChunk, *testRouting], *testPreamble, error) {
			if originHook != nil {
				return originHook(msgID, payload)
			}
			ms := newMockStrategy()
			p := testPreamble("mock-preamble")
			return ms, &p, nil
		},
		NewRelay: func(msgID MessageID, preamble *testPreamble) (Strategy[*testChunk, *testRouting], error) {
			if relayHook != nil {
				return relayHook(msgID, preamble)
			}
			return newMockStrategy(), nil
		},
		NewCI: func() *testChunk { return &testChunk{} },
		NewR:  func() *testRouting { r := testRouting{}; return &r },
		NewP:  func() *testPreamble { p := testPreamble{}; return &p },
	}
}

// decodeOnTakeStrategy is a relay strategy mock that publishes a decode
// task on the first TakeChunk call, simulating single-chunk decode.
type decodeOnTakeStrategy struct {
	mockStrategy
	decoded bool
}

func newDecodeOnTakeStrategy() *decodeOnTakeStrategy {
	return &decodeOnTakeStrategy{
		mockStrategy: mockStrategy{
			takeVerdict: VerdictAccepted,
		},
	}
}

func (ds *decodeOnTakeStrategy) TakeChunk(peer identity.PeerID, chunk *testChunk, data []byte, dedup *DedupCancel) (Verdict, bool, error) {
	if ds.decoded {
		return VerdictRedundant, false, nil
	}
	v, _, err := ds.mockStrategy.TakeChunk(peer, chunk, data, dedup)
	ds.decoded = true
	ds.mockStrategy.decodeResult = append([]byte(nil), data...)
	return v, true, err
}

func TestChannelReceive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := NewEngine(EngineConfig{})
		engine.Start()
		defer engine.Close()

		scheme := newMockSchemeWithFactories(nil, func(_ MessageID, _ *testPreamble) (Strategy[*testChunk, *testRouting], error) {
			return newDecodeOnTakeStrategy(), nil
		})
		channel := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", scheme)
		defer channel.Stop()

		ch := make(chan FullMessage, 128)
		if err := channel.Subscribe(ch); err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}
		if err := channel.Subscribe(make(chan FullMessage, 1)); !errors.Is(err, ErrAlreadySubscribed) {
			t.Fatalf("second Subscribe = %v, want ErrAlreadySubscribed", err)
		}

		channel.inbox <- channelSessionOpen{peerID: "peer1", msg: &bcastpb.Sess_Open{ObjectId: "msg1", Preamble: []byte("mock-header")}}
		synctest.Wait()
		channel.inbox <- newFakeChunk("peer1", "msg1", []byte("hello"))
		synctest.Wait()

		select {
		case msg := <-ch:
			if msg.ChannelID != "test-channel" {
				t.Errorf("got channelID %q, want %q", msg.ChannelID, "test-channel")
			}
			if msg.MessageID != "msg1" {
				t.Errorf("got msgID %q, want %q", msg.MessageID, "msg1")
			}
			if string(msg.Data) != "hello" {
				t.Errorf("got data %q, want %q", string(msg.Data), "hello")
			}
		default:
			t.Fatal("no message on subscription channel")
		}

		channel.Stop()
		if _, open := <-ch; open {
			t.Fatal("subscription stayed open after Stop")
		}
	})
}

func TestDuplicateChannelReportsError(t *testing.T) {
	observer := &recordingObserver{attached: make(chan error, 2)}
	engine := NewEngine(EngineConfig{Observer: observer})
	engine.Start()
	defer engine.Close()

	first := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "duplicate", newMockScheme())
	defer first.Stop()
	second := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "duplicate", newMockScheme())
	defer second.Stop()

	got := make([]error, 0, 2)
	for range 2 {
		select {
		case err := <-observer.attached:
			got = append(got, err)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for channel attachment callbacks")
		}
	}
	if got[0] != nil || got[1] != ErrChannelExists {
		t.Fatalf("channel attachment errors = %v, want [nil, %v]", got, ErrChannelExists)
	}
}

func TestChannelExpireDue(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(*Channel[*testChunk, *testRouting, *testPreamble]) *recordingReceiveStream
		verify func(*testing.T, *Channel[*testChunk, *testRouting, *testPreamble], *recordingReceiveStream)
	}{
		{
			name: "backdated session is disposed",
			setup: func(channel *Channel[*testChunk, *testRouting, *testPreamble]) *recordingReceiveStream {
				sess := channel.newSession("msg1", []byte("mock-header"), false, newMockStrategy())
				sess.createdAt = time.Now().Add(-(activeSessionTTL + time.Second))
				channel.sessions["msg1"] = sess
				return nil
			},
			verify: func(t *testing.T, channel *Channel[*testChunk, *testRouting, *testPreamble], _ *recordingReceiveStream) {
				if _, ok := channel.sessions["msg1"]; ok {
					t.Fatal("stale session should have been disposed")
				}
			},
		},
		{
			name: "backdated parked group is dropped and streams cancelled",
			setup: func(channel *Channel[*testChunk, *testRouting, *testPreamble]) *recordingReceiveStream {
				stream := &recordingReceiveStream{}
				channel.parked["msg2"] = &parkedGroup{at: time.Now().Add(-(activeSessionTTL + time.Second)), streams: []channelChunkStream{{peerID: "peer1", stream: stream}}}
				return stream
			},
			verify: func(t *testing.T, channel *Channel[*testChunk, *testRouting, *testPreamble], stream *recordingReceiveStream) {
				if _, ok := channel.parked["msg2"]; ok {
					t.Fatal("parked chunk group should have been removed")
				}
				requireCode(t, stream.CancelReadCodes(), wire.Refused.Wire())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			channel := newTestChannel(make(chan channelEvent, 1))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			channel.ctx = ctx
			channel.sessions = make(map[MessageID]*session[*testChunk, *testRouting])
			channel.parked = make(map[MessageID]*parkedGroup)
			stream := test.setup(channel)
			channel.expireDue()
			test.verify(t, channel, stream)
		})
	}
}

func TestChannel_SessionExpiryTimerDisposesSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		observer := &sessionDisposedObserver{disposed: make(chan sessionDisposedEvent, 1)}
		engine := NewEngine(EngineConfig{Observer: observer})
		engine.Start()
		defer engine.Close()

		channel := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", newMockScheme())
		defer channel.Stop()

		channel.inbox <- channelSessionOpen{peerID: "peer1", msg: &bcastpb.Sess_Open{ObjectId: "msg1", Preamble: []byte("mock-header")}}
		synctest.Wait()

		time.Sleep(activeSessionTTL + time.Second)
		synctest.Wait()

		select {
		case event := <-observer.disposed:
			if event.channelID != "test-channel" || event.messageID != "msg1" || event.reason != "ttl_expired" {
				t.Fatalf("session disposed = %+v, want test-channel/msg1/ttl_expired", event)
			}
		default:
			t.Fatal("session expiry timer did not dispose the session")
		}
	})
}

// A relay session that goes idle while a Publish of the same message ID is
// already queued must not take the origin session that replaces it down with
// it: disposal belongs to the relay, not to whatever session holds the ID
// when the channel next looks.
func TestChannel_RelayGoingIdleBeforePublishKeepsOriginSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inbox := make(chan channelEvent, 16)
		channel := newTestChannel(inbox)
		channel.members = map[identity.PeerID]*PeerConn{}
		channel.sessions = map[MessageID]*session[*testChunk, *testRouting]{}
		channel.parked = map[MessageID]*parkedGroup{}

		peer := testPeer("p1")
		channel.handle(channelPeerChange{peerID: "p1", peerRef: peer})
		relay := channel.newSession("msg1", nil, false, newMockStrategy())
		channel.sessions["msg1"] = relay
		relay.handlePeerAttached(peer)

		errCh := make(chan error, 1)
		inbox <- channelPublish[*testChunk, *testRouting]{messageID: "msg1", strategy: newMockStrategy(), errCh: errCh}
		// The relay's only peer leaves while the publish is queued.
		channel.handle(channelPeerChange{peerID: "p1"})
		for len(inbox) > 0 {
			channel.handle(<-inbox)
		}
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}

		if sess := channel.sessions["msg1"]; sess == nil || sess.stage != stageOrigin {
			t.Fatal("the relay's disposal removed the origin session that replaced it")
		}
	})
}

func TestPeerHandshakeAnnouncesChannelChangesSinceHello(t *testing.T) {
	e := NewEngine(EngineConfig{})
	e.addChannel("gone", &channelHandle{}, func() {})

	p := &PeerConn{
		id:            identity.PeerID("peer"),
		ctx:           context.Background(),
		ctrlState:     newCtrlOutState(),
		handshakeDone: make(chan struct{}),
		announced:     []ChannelID{"gone"},
	}

	// The channel set changes after Hello's snapshot but before binding.
	e.removeChannel("gone")
	e.addChannel("late", &channelHandle{}, func() {})
	e.handlePeerHandshake(p, nil)

	var got []peerCtrlEvent
	for {
		event, ok := p.ctrlState.next()
		if !ok {
			break
		}
		got = append(got, event)
	}
	if len(got) != 2 {
		t.Fatalf("got %d control events, want unsubscribe and subscribe: %#v", len(got), got)
	}
	unsub, ok := got[0].(peerUnsubscribe)
	if !ok || unsub.channelID != "gone" {
		t.Fatalf("first event = %#v, want unsubscribe gone", got[0])
	}
	sub, ok := got[1].(peerSubscribe)
	if !ok || sub.channelID != "late" {
		t.Fatalf("second event = %#v, want subscribe late", got[1])
	}
}
