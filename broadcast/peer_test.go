package broadcast

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/wire"
)

type blockingReceiveStream struct {
	*fakeReceiveStream
	blocked chan struct{}
	once    sync.Once
}

func (s *blockingReceiveStream) Read(p []byte) (int, error) {
	if s.Reader.Len() == 0 {
		<-s.blocked
		return 0, io.EOF
	}
	return s.fakeReceiveStream.Read(p)
}

func (s *blockingReceiveStream) CancelRead(wire.Code) {
	s.once.Do(func() { close(s.blocked) })
}

func TestBindContextUnblocksChunkBackpressure(t *testing.T) {
	engine := NewEngine(EngineConfig{})
	engine.Start()
	t.Cleanup(func() { _ = engine.Close() })
	bindCtx, cancelBind := context.WithCancel(t.Context())
	peer := newPeerConn(engine, bindCtx, "", nil)
	peer.chunkSem = make(chan struct{}, 1)
	peer.bcastAccepted = true
	peer.chunkSem <- struct{}{}

	stream := newGateStream()
	returned := make(chan struct{})
	go func() {
		peer.acceptChunk(stream)
		close(returned)
	}()
	cancelBind()

	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("chunk handler ignored the bind context")
	}
	select {
	case <-stream.canceled:
	default:
		t.Fatal("blocked chunk stream was not canceled")
	}
	peer.Close()
}

func TestAcceptSessionRejectsFullHandlerBudgetAsUnspecified(t *testing.T) {
	peer := &PeerConn{
		ctx:        context.Background(),
		sessionSem: make(chan struct{}, 1),
	}
	peer.sessionSem <- struct{}{}

	raw, stream := newOutcomeFixture(t).incoming(t, SESS, nil)
	peer.acceptSession(stream)
	requireCode(t, raw.CancelReadCodes(), 0)
}

func TestInboundStreamsWaitForHandshake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := NewEngine(EngineConfig{})
		inbox := make(chan channelEvent, 8)
		channelDone := make(chan struct{})
		engine.channels["early"] = &channelHandle{inbox: inbox, ctx: context.Background(), done: channelDone}
		engine.Start()

		peer := testPeer("peer")
		peer.engine = engine
		peer.announced = []ChannelID{"early"}
		peer.bcastIn = make(chan ethp2p.ReceiveStream, 1)
		peer.sessionIn = make(chan ethp2p.ReceiveStream, 1)
		peer.sessionSem = make(chan struct{}, streamQueueCap)

		var sessFrame bytes.Buffer
		if err := writeFrame(&sessFrame, openFrame, &bcastpb.Sess_Open{Channel: "early", ObjectId: "msg"}); err != nil {
			t.Fatal(err)
		}
		peer.enqueueStream(SESS, &fakeReceiveStream{bytes.NewReader(sessFrame.Bytes())})

		runDone := make(chan struct{})
		go func() {
			peer.Run()
			close(runDone)
		}()
		t.Cleanup(func() {
			peer.Close()
			<-runDone
			close(channelDone)
			if err := engine.Close(); err != nil {
				t.Error(err)
			}
		})
		synctest.Wait()
		if len(inbox) != 0 {
			t.Fatalf("unbound peer routed %d channel events before handshake", len(inbox))
		}
		if got := len(peer.sessionIn); got != 1 {
			t.Fatalf("pending SESS streams = %d before binding, want 1", got)
		}

		var handshake bytes.Buffer
		if err := writeFrame(&handshake, handshakeFrame, &bcastpb.Bcast_Handshake{Version: ProtocolV1, Channels: []string{"early"}}); err != nil {
			t.Fatal(err)
		}
		peer.bcastIn <- &blockingReceiveStream{
			fakeReceiveStream: &fakeReceiveStream{bytes.NewReader(handshake.Bytes())},
			blocked:           make(chan struct{}),
		}
		select {
		case <-time.After(time.Second):
			t.Fatal("SESS open was not routed after handshake bound the peer")
		case event := <-inbox:
			if _, ok := event.(channelPeerChange); !ok {
				t.Fatalf("first channel event = %T, want peer binding", event)
			}
		}
		select {
		case <-time.After(time.Second):
			t.Fatal("SESS open was not routed after handshake bound the peer")
		case event := <-inbox:
			opened, ok := event.(channelSessionOpen)
			if !ok || opened.msg.ObjectId != "msg" {
				t.Fatalf("second channel event = %#v, want session open msg", event)
			}
		}

	})
}

type gateStream struct {
	readStarted chan struct{}
	canceled    chan struct{}
	readOnce    sync.Once
	cancelOnce  sync.Once
}

func newGateStream() *gateStream {
	return &gateStream{
		readStarted: make(chan struct{}),
		canceled:    make(chan struct{}),
	}
}

func (s *gateStream) Read([]byte) (int, error) {
	s.readOnce.Do(func() { close(s.readStarted) })
	return 0, io.EOF
}

func (s *gateStream) CancelRead(wire.Code) {
	s.cancelOnce.Do(func() { close(s.canceled) })
}

func (*gateStream) SetReadDeadline(time.Time) error {
	return nil
}

var _ ethp2p.ReceiveStream = (*gateStream)(nil)
