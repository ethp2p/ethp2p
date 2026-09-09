package broadcast

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/protocol"
	"github.com/ethp2p/ethp2p/transport"
)

type uniHandshakeTransport struct {
	*testTransport
}

func (t *uniHandshakeTransport) AcceptBiStream(ctx context.Context) (transport.Stream, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestHandshakeUsesAuthenticatedPeerIDs(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	leftRaw, rightRaw := newTestTransportPair(ctx)
	leftRaw.auth = testAuthInfo("authenticated-left", "authenticated-right")
	rightRaw.auth = testAuthInfo("authenticated-right", "authenticated-left")
	left := newPeerConn(
		&Engine{ctx: ctx, config: EngineConfig{Observer: NoOpObserver{}}},
		ctx,
		&uniHandshakeTransport{testTransport: leftRaw},
	)
	right := newPeerConn(
		&Engine{ctx: ctx, config: EngineConfig{Observer: NoOpObserver{}}},
		ctx,
		&uniHandshakeTransport{testTransport: rightRaw},
	)

	type result struct {
		peer PeerID
		err  error
	}
	leftResult := make(chan result, 1)
	rightResult := make(chan result, 1)
	go func() {
		_, _, err := left.handshake(ctx, nil)
		leftResult <- result{peer: left.ID(), err: err}
	}()
	go func() {
		_, _, err := right.handshake(ctx, nil)
		rightResult <- result{peer: right.ID(), err: err}
	}()
	routeErr := make(chan error, 2)
	routeBcast := func(peer *PeerConn, conn *testTransport) {
		stream, err := conn.AcceptUniStream(ctx)
		if err != nil {
			routeErr <- err
			return
		}
		reader := bufio.NewReader(stream)
		codepoint, err := protocol.ReadSelector(reader)
		if err != nil {
			routeErr <- err
			return
		}
		if codepoint != BCAST {
			routeErr <- fmt.Errorf("selector = %d, want %d", codepoint, BCAST)
			return
		}
		peer.acceptBcast(bufferedReceiveStream{ReceiveStream: stream, reader: reader})
		routeErr <- nil
	}
	go routeBcast(left, leftRaw)
	go routeBcast(right, rightRaw)

	if got := <-leftResult; got.err != nil || got.peer != "authenticated-right" {
		t.Fatalf("left handshake = (%q, %v)", got.peer, got.err)
	}
	if got := <-rightResult; got.err != nil || got.peer != "authenticated-left" {
		t.Fatalf("right handshake = (%q, %v)", got.peer, got.err)
	}
	for range 2 {
		if err := <-routeErr; err != nil {
			t.Fatal(err)
		}
	}
}

type bufferedReceiveStream struct {
	transport.ReceiveStream
	reader *bufio.Reader
}

func (s bufferedReceiveStream) Read(p []byte) (int, error) { return s.reader.Read(p) }

func TestHandshakeRequiresAuthenticatedPeerIDs(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	raw, _ := newTestTransportPair(ctx)
	raw.auth = transport.AuthInfo{}
	peer := newPeerConn(
		&Engine{ctx: ctx, config: EngineConfig{Observer: NoOpObserver{}}},
		ctx,
		&uniHandshakeTransport{testTransport: raw},
	)
	if _, _, err := peer.handshake(ctx, nil); err == nil {
		t.Fatal("handshake accepted empty authenticated identity")
	}
}

func TestInboundStreamsWaitForHandshake(t *testing.T) {
	tests := []struct {
		name   string
		accept func(*PeerConn, transport.ReceiveStream)
	}{
		{name: "session", accept: (*PeerConn).acceptSession},
		{name: "chunk", accept: (*PeerConn).acceptChunk},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			engine := NewEngine(EngineConfig{})
			t.Cleanup(func() { _ = engine.Close() })
			peer := newPeerConn(engine, t.Context(), nil)
			peer.bcastAccepted.Store(true)
			peer.handlersMu.Lock()
			peer.ready = true
			peer.handlersMu.Unlock()

			stream := newGateStream()
			accepted := make(chan struct{})
			go func() {
				close(accepted)
				test.accept(peer, stream)
			}()
			<-accepted
			select {
			case <-stream.readStarted:
				t.Fatal("stream read before handshake completed")
			case <-time.After(20 * time.Millisecond):
			}

			peer.finishHandshake(true)
			select {
			case <-stream.readStarted:
			case <-time.After(time.Second):
				t.Fatal("stream was not read after handshake completed")
			}
			peer.Close()
		})
	}
}

func TestBindContextUnblocksChunkBackpressure(t *testing.T) {
	engine := NewEngine(EngineConfig{MaxInboundChunkStreams: 1})
	t.Cleanup(func() { _ = engine.Close() })
	bindCtx, cancelBind := context.WithCancel(t.Context())
	peer := newPeerConn(engine, bindCtx, nil)
	peer.bcastAccepted.Store(true)
	peer.handlersMu.Lock()
	peer.ready = true
	peer.handlersMu.Unlock()
	peer.finishHandshake(true)
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

func (s *gateStream) CancelRead(uint64) {
	s.cancelOnce.Do(func() { close(s.canceled) })
}

func (*gateStream) SetReadDeadline(time.Time) error {
	return nil
}

var _ transport.ReceiveStream = (*gateStream)(nil)
