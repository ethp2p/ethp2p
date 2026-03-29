package broadcast

import (
	"testing"
	"testing/synctest"
	"time"

	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
)

// funcObserver delegates to function fields, falling back to NoOpObserver.
type funcObserver struct {
	NoOpObserver
	onChannelAttached func(ChannelID, error)
}

func (o *funcObserver) OnChannelAttached(id ChannelID, err error) {
	if o.onChannelAttached != nil {
		o.onChannelAttached(id, err)
	}
}

func TestEngineCreateDuplicateChannel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attached []error
		obs := &funcObserver{
			onChannelAttached: func(_ ChannelID, err error) {
				attached = append(attached, err)
			},
		}
		engine := NewEngine(EngineConfig{Observer: obs})
		defer engine.Close()

		scheme := newMockScheme()
		AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", scheme)
		synctest.Wait()

		AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", scheme)
		synctest.Wait()

		if len(attached) != 2 {
			t.Fatalf("expected 2 OnChannelAttached calls, got %d", len(attached))
		}
		if attached[0] != nil {
			t.Errorf("first attach: got %v, want nil", attached[0])
		}
		if attached[1] != ErrChannelExists {
			t.Errorf("second attach: got %v, want ErrChannelExists", attached[1])
		}
	})
}

func TestEngineDeleteChannel(t *testing.T) {
	engine := NewEngine(EngineConfig{})
	defer engine.Close()

	scheme := newMockScheme()
	AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", scheme)

	engine.DropChannel("test-channel")

	engine.DropChannel("test-channel")
}

func TestChannelReceive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := NewEngine(EngineConfig{})
		defer engine.Close()

		scheme := newMockSchemeWithHooks(nil, func(_ MessageID, _ *testPreamble) (Strategy[*testChunk, *testRouting], error) {
			return newDecodeOnTakeStrategy(), nil
		})
		channel := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", scheme)
		defer channel.Stop()

		ch := make(chan FullMessage, 128)
		if err := channel.Subscribe(ch); err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		channel.inbox <- channelSessionOpen{peerID: "peer1", msg: &bcastpb.Sess_Open{MessageId: "msg1", Preamble: []byte("mock-header")}}
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
	})
}

func TestSubscribeClosedOnStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := NewEngine(EngineConfig{})
		defer engine.Close()

		scheme := newMockScheme()
		channel := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", scheme)

		ch := make(chan FullMessage, 128)
		if err := channel.Subscribe(ch); err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		channel.Stop()

		select {
		case _, ok := <-ch:
			if ok {
				t.Fatal("expected channel to be closed")
			}
		default:
			t.Fatal("channel should be closed after Stop")
		}
	})
}

func TestSubscribeOnlyOnce(t *testing.T) {
	engine := NewEngine(EngineConfig{})
	defer engine.Close()

	scheme := newMockScheme()
	channel := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", scheme)
	defer channel.Stop()

	ch := make(chan FullMessage, 1)
	if err := channel.Subscribe(ch); err != nil {
		t.Fatalf("first Subscribe failed: %v", err)
	}

	ch2 := make(chan FullMessage, 1)
	if err := channel.Subscribe(ch2); err != ErrAlreadySubscribed {
		t.Fatalf("second Subscribe: got %v, want ErrAlreadySubscribed", err)
	}
}

func TestChannel_StaleSessionDisposed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := NewEngine(EngineConfig{})
		defer engine.Close()

		scheme := newMockScheme()
		channel := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", scheme)
		defer channel.Stop()

		// Create a relay session.
		channel.inbox <- channelSessionOpen{peerID: "peer1", msg: &bcastpb.Sess_Open{MessageId: "msg1", Preamble: []byte("mock-header")}}
		synctest.Wait()

		sess, ok := channel.sessions["msg1"]
		if !ok {
			t.Fatal("session msg1 should exist")
		}

		// Backdate the session's createdAt past the TTL.
		sess.createdAt = time.Now().Add(-(activeSessionTTL + time.Second))

		// Run cleanup directly.
		channel.cleanup()

		if _, ok := channel.sessions["msg1"]; ok {
			t.Fatal("stale session should have been disposed")
		}
	})
}

func TestChannel_ParkedChunksDroppedOnCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := NewEngine(EngineConfig{})
		defer engine.Close()

		scheme := newMockScheme()
		channel := AttachChannel[*testChunk, *testRouting, *testPreamble](engine, "test-channel", scheme)
		defer channel.Stop()

		// Park a chunk stream (no session exists for "msg2").
		channel.inbox <- newFakeChunk("peer1", "msg2", []byte("data"))
		synctest.Wait()

		if len(channel.parked["msg2"]) != 1 {
			t.Fatalf("expected 1 parked chunk, got %d", len(channel.parked["msg2"]))
		}

		// Run cleanup; parked streams are cancelled and dropped.
		channel.cleanup()

		if chunks, ok := channel.parked["msg2"]; ok && len(chunks) > 0 {
			t.Fatal("parked chunks should have been dropped after cleanup")
		}
	})
}
