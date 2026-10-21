package broadcast

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/quic-go/quic-go"
)

// openingObserver records the binding events the opening-message tests wait on.
type openingObserver struct {
	NoOpObserver
	handshook chan []ChannelID
	gone      chan error
	preamble  chan MessageID
	chunk     chan Verdict
}

func newOpeningObserver() *openingObserver {
	return &openingObserver{
		handshook: make(chan []ChannelID, 4),
		gone:      make(chan error, 4),
		preamble:  make(chan MessageID, 4),
		chunk:     make(chan Verdict, 4),
	}
}

func (o *openingObserver) OnPeerHandshook(_ identity.PeerID, channels []ChannelID) {
	o.handshook <- channels
}

func (o *openingObserver) OnPeerGone(_ identity.PeerID, err error) { o.gone <- err }

func (o *openingObserver) OnPreambleOpened(_ identity.PeerID, _ ChannelID, id MessageID) {
	o.preamble <- id
}

func (o *openingObserver) OnChunkRcvd(_ identity.PeerID, _ ChannelID, _ MessageID, v Verdict) {
	o.chunk <- v
}

// rawBroadcastPeer is a remote peer that speaks broadcast by hand to a real
// Stack serving a real Engine, so a test controls every byte of every stream.
type rawBroadcastPeer struct {
	ctx    context.Context
	engine *Engine
	stack  *ethp2p.Stack
	raw    *transporttest.RawPeer
	obs    *openingObserver
}

func newRawBroadcastPeer(t *testing.T, attach func(*Engine)) *rawBroadcastPeer {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	client, server := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	stack, err := ethp2p.NewStack(server.Eth, ethp2p.Config{})
	if err != nil {
		t.Fatal(err)
	}
	obs := newOpeningObserver()
	engine := NewEngine(EngineConfig{Observer: obs})
	if err := engine.Register(stack); err != nil {
		t.Fatal(err)
	}
	if attach != nil {
		attach(engine)
	}
	if err := stack.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = stack.Close()
		_ = engine.Close()
	})
	raw, err := transporttest.DialHello(ctx, client, server, transporttest.Hello{Protocols: []wire.Protocol{Protocol}})
	if err != nil {
		t.Fatal(err)
	}
	return &rawBroadcastPeer{ctx: ctx, engine: engine, stack: stack, raw: raw, obs: obs}
}

// open opens a broadcast stream and writes frames on it, leaving it open.
func (r *rawBroadcastPeer) open(t *testing.T, frames ...[]byte) *quic.SendStream {
	t.Helper()
	out, err := transporttest.OpenUniStream(r.ctx, r.raw.Conn, Protocol)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := out.Write(bytes.Join(frames, nil)); err != nil {
		t.Fatal(err)
	}
	return out
}

func (r *rawBroadcastPeer) handshake(t *testing.T, channels ...string) {
	t.Helper()
	r.open(t, testFrame(t, testHandshakeType, &bcastpb.Bcast_Handshake{Channels: channels}))
	select {
	case <-r.obs.handshook:
	case err := <-r.obs.gone:
		t.Fatalf("binding ended before the handshake: %v", err)
	case <-r.ctx.Done():
		t.Fatal("handshake did not complete")
	}
}

func (r *rawBroadcastPeer) awaitGone(t *testing.T) error {
	t.Helper()
	select {
	case err := <-r.obs.gone:
		return err
	case <-r.ctx.Done():
		t.Fatal("binding did not end")
		return nil
	}
}

// awaitRelease waits for the server to release the view: with broadcast the
// only protocol, a closed handle leaves no protocol holding it.
func (r *rawBroadcastPeer) awaitRelease(t *testing.T) {
	t.Helper()
	_ = r.raw.PeerControl.SetReadDeadline(time.Now().Add(5 * time.Second))
	code, err := r.raw.ReadGoAway()
	if err != nil || code != wire.NoSharedProtocols {
		t.Fatalf("GOAWAY = %v, %v; want NoSharedProtocols", code, err)
	}
}

func requireViolation(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrPeerViolation) {
		t.Fatalf("OnPeerGone cause = %v, want ErrPeerViolation", err)
	}
}

// Frame types from the broadcast spec, written out here so the tests check
// the wire numbers rather than the package's constants.
const (
	testHandshakeType   wire.FrameType = 0
	testSubscribeType   wire.FrameType = 1
	testUnsubscribeType wire.FrameType = 2
	testOpenType        wire.FrameType = 3
	testPreambleType    wire.FrameType = 4
	testUpdateType      wire.FrameType = 5
	testHeaderType      wire.FrameType = 6
	testDataType        wire.FrameType = 7
)

func TestOpeningViolationsEndBinding(t *testing.T) {
	for _, test := range []struct {
		name  string
		first bool // handshake before the bad stream
		frame func(*testing.T) []byte
	}{
		{name: "unknown frame type", frame: func(*testing.T) []byte { return wire.AppendFrame(nil, 9, nil) }},
		{name: "non-opening frame type", frame: func(t *testing.T) []byte {
			return testFrame(t, testSubscribeType, &bcastpb.Bcast_Subscribe{Channel: "c"})
		}},
		{name: "malformed OPEN", frame: func(*testing.T) []byte { return wire.AppendFrame(nil, testOpenType, []byte{0x0f}) }},
		{name: "second HANDSHAKE", first: true, frame: func(t *testing.T) []byte {
			return testFrame(t, testHandshakeType, &bcastpb.Bcast_Handshake{})
		}},
		{name: "OPEN over 16 KiB", first: true, frame: func(t *testing.T) []byte {
			return testFrame(t, testOpenType, &bcastpb.Sess_Open{Channel: "c", ObjectId: "m", InitialUpdate: make([]byte, 16<<10)})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := newRawBroadcastPeer(t, nil)
			if test.first {
				r.handshake(t)
			}
			r.open(t, test.frame(t))
			requireViolation(t, r.awaitGone(t))
			r.awaitRelease(t)
		})
	}
}

func TestHandshakeOver16KiBIsAccepted(t *testing.T) {
	r := newRawBroadcastPeer(t, nil)
	channels := make([]string, 300)
	for i := range channels {
		channels[i] = strings.Repeat("c", 60) + string(rune('A'+i%26)) + string(rune('A'+i/26))
	}
	r.handshake(t, channels...)
}

// OPEN and HEADER streams that arrive before the peer's HANDSHAKE are held,
// then processed once the handshake binds the peer to the channel. Before the
// binding, the channel lookup would refuse them.
func TestOpeningsBeforeHandshakeAreProcessedAfterIt(t *testing.T) {
	relay := make(chan []byte, 1)
	r := newRawBroadcastPeer(t, func(e *Engine) {
		AttachChannel(e, "c", Scheme[*testChunk, *testRouting, *testPreamble]{
			NewRelay: func(_ MessageID, p *testPreamble) (Strategy[*testChunk, *testRouting], error) {
				relay <- []byte(*p)
				return newMockStrategy(), nil
			},
			NewCI: func() *testChunk { return &testChunk{} },
			NewR:  func() *testRouting { return &testRouting{} },
			NewP:  func() *testPreamble { return &testPreamble{} },
		})
	})
	chunkID, err := testChunk{Data: []byte("id")}.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	r.open(t,
		testFrame(t, testOpenType, &bcastpb.Sess_Open{Channel: "c", ObjectId: "m"}),
		testFrame(t, testPreambleType, &bcastpb.Sess_Preamble{Data: []byte("preamble")}))
	chunk := r.open(t,
		testFrame(t, testHeaderType, &bcastpb.Chunk_Header{Channel: "c", ObjectId: "m", ChunkId: chunkID, DataLength: 4}),
		wire.AppendFrame(nil, testDataType, []byte("data")))
	if err := chunk.Close(); err != nil {
		t.Fatal(err)
	}
	awaitHeld(t, r, 2)
	r.handshake(t, "c")
	select {
	case got := <-relay:
		if string(got) != "preamble" {
			t.Fatalf("relay preamble = %q", got)
		}
	case <-r.ctx.Done():
		t.Fatal("held OPEN was not processed")
	}
	select {
	case v := <-r.obs.chunk:
		if v != VerdictAccepted {
			t.Fatalf("chunk verdict = %v", v)
		}
	case <-r.ctx.Done():
		t.Fatal("held HEADER was not processed")
	}
}

// awaitHeld waits until the sole binding holds n openings read before its
// handshake, so the test's HANDSHAKE stream is the last to be read.
func awaitHeld(t *testing.T, r *rawBroadcastPeer, n int) {
	t.Helper()
	for {
		held := make(chan int, 1)
		r.engine.post(nil, func() {
			count := 0
			for _, p := range r.engine.bindings {
				p.parkMu.Lock()
				count += len(p.parked)
				p.parkMu.Unlock()
			}
			held <- count
		})
		select {
		case got := <-held:
			if got == n {
				return
			}
		case <-r.ctx.Done():
			t.Fatalf("binding did not hold %d openings", n)
		}
		select {
		case <-time.After(time.Millisecond):
		case <-r.ctx.Done():
			t.Fatalf("binding did not hold %d openings", n)
		}
	}
}

func TestEngineCloseReleasesViews(t *testing.T) {
	r := newRawBroadcastPeer(t, nil)
	r.handshake(t)
	if err := r.engine.Close(); err != nil {
		t.Fatal(err)
	}
	r.awaitRelease(t)
}

// An opening read that times out resets only its stream: a slow peer is not a
// violation. open arms the real openingTimeout, so this runs on the synctest
// clock.
func TestOpeningReadTimesOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := &Engine{ctx: t.Context()}
		p := newPeerConn(engine, t.Context(), "peer", nil)
		defer p.Close()
		s := &codeStream{}
		p.open(s)
		if len(s.codes) != 1 || s.codes[0] != wire.Timeout {
			t.Fatalf("CancelRead codes = %v, want [Timeout]", s.codes)
		}
		if err := context.Cause(p.ctx); err != nil {
			t.Fatalf("binding ended: %v", err)
		}
	})
}
