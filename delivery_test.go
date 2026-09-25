package ethp2p

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/wire"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/quic-go/quic-go"
)

// Observe queue admission without transferring stream ownership to the test.
// This distinguishes streams pending in the stack from those still in QUIC.
func waitPending(t *testing.T, f *Family, count int) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		n := 0
		for e := f.ready.Front(); e != nil; e = e.Next() {
			n += len(e.Value.(*peerDelivery).streams)
		}
		f.mu.Unlock()
		if n == count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("stack did not queue %d streams", count)
}

func TestDeliveryOrderingAndManyQueued(t *testing.T) {
	pair := newTestPair(t)
	stack := newTestStack(t, pair.server)
	fam, wake := registerTestFamily(t, stack, ProtocolSpec{Selector: selectorAlpha, MaxQueued: 128})
	pair.connect(t, nil, stack)
	ctx, stop := context.WithTimeout(t.Context(), testTimeout)
	defer stop()

	// Sequential admission establishes completion order even though transport
	// classifiers run concurrently. Leave PeerUp and every stream undrained.
	for i := range 100 {
		writeSelectorPayload(t, pair.clientConn, selectorAlpha, []byte{byte(i)})
		waitPending(t, fam, i+1)
	}
	for i := range 16 {
		stream, err := pair.clientConn.OpenStream(ctx, selectorAlpha)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Write([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		waitPending(t, fam, 101+i)
	}
	peer := awaitPeer(t, fam, wake)
	for i := range 116 {
		event := awaitStreamEvent(t, fam, wake)
		if event.Peer != peer {
			t.Fatal("stream changed peer handle")
		}
		payload, err := io.ReadAll(event.Stream)
		want := byte(i)
		if i >= 100 {
			want = byte(i - 100)
		}
		if err != nil || !bytes.Equal(payload, []byte{want}) {
			t.Fatalf("stream %d payload = %x, err = %v", i, payload, err)
		}
		if bi, ok := event.Stream.(Stream); ok {
			if i < 100 {
				t.Fatal("uni stream became bidirectional")
			}
			_ = bi.Close()
		} else if i >= 100 {
			t.Fatal("bidirectional stream lost write side")
		}
	}
	if event, ok := fam.Next(); ok {
		t.Fatalf("extra event: %+v", event)
	}
	disconnectTest(t, stack, pair.client.Shared.PeerID())
	down := awaitEvent(t, fam, wake)
	if down.Kind != PeerDown || down.Peer != peer || down.Code != wire.Closing || peer.Context().Err() == nil {
		t.Fatalf("PeerDown = %+v, context = %v", down, peer.Context().Err())
	}
	if _, ok := fam.Next(); ok {
		t.Fatal("event after PeerDown")
	}
}

func TestDeliveryQueuedStreamsResetOnDown(t *testing.T) {
	for _, test := range []struct {
		name               string
		takeUp, closeStack bool
	}{{"disconnect before up", false, false}, {"disconnect after up", true, false}, {"close before up", false, true}, {"close after up", true, true}} {
		t.Run(test.name, func(t *testing.T) {
			pair := newTestPair(t)
			stack := newTestStack(t, pair.server)
			fam, wake := registerTestFamily(t, stack, ProtocolSpec{Selector: selectorAlpha, MaxQueued: 8})
			pair.connect(t, nil, stack)
			if test.takeUp {
				awaitPeer(t, fam, wake)
			}
			ctx, stop := context.WithTimeout(t.Context(), testTimeout)
			defer stop()
			uni, err := pair.clientConn.OpenUniStream(ctx, selectorAlpha)
			if err != nil {
				t.Fatal(err)
			}
			bi, err := pair.clientConn.OpenStream(ctx, selectorAlpha)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = uni.Write([]byte{1})
			_, _ = bi.Write([]byte{2})
			waitPending(t, fam, 2)
			if test.closeStack {
				if err := stack.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				disconnectTest(t, stack, pair.client.Shared.PeerID())
			}
			if test.takeUp {
				down := awaitEvent(t, fam, wake)
				if down.Kind != PeerDown || down.Code != wire.Closing || down.Peer.Context().Err() == nil {
					t.Fatalf("down = %+v", down)
				}
			}
			if event, ok := fam.Next(); ok {
				t.Fatalf("unexpected event after close: %+v", event)
			}
			for _, out := range []transport.SendStream{uni, bi} {
				_ = out.SetWriteDeadline(time.Now().Add(testTimeout))
				_, err := out.Write(make([]byte, 2<<20))
				reset, ok := errors.AsType[*quic.StreamError](err)
				if !ok || !reset.Remote || uint64(reset.ErrorCode) != 20 {
					t.Fatalf("queued stream write = %v, want remote wire 20", err)
				}
			}
			_ = bi.SetReadDeadline(time.Now().Add(testTimeout))
			_, err = bi.Read(make([]byte, 1))
			reset, ok := errors.AsType[*transport.StreamResetError](err)
			if !ok || reset.Code != 20 {
				t.Fatalf("bidi read = %v, want wire 20", err)
			}
			select {
			case <-pair.clientConn.Done():
			case <-time.After(testTimeout):
				t.Fatal("disconnected view remained open")
			}
			lib, err := pair.clientLib.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			payload := wire.AppendFrame(nil, []byte("/still-live"))
			_, _ = lib.Write(payload)
			_ = lib.Close()
			in, err := pair.serverLib.AcceptStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(in)
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("libp2p = %x, %v", got, err)
			}
		})
	}
}

func TestDeliveryRoundRobin(t *testing.T) {
	pair := newTestPair(t)
	stack := newTestStack(t, pair.server)
	fam, wake := registerTestFamily(t, stack, ProtocolSpec{Selector: selectorAlpha, MaxQueued: 8})
	pair.connect(t, nil, stack)
	defer disconnectTest(t, stack, pair.client.Shared.PeerID())
	a := awaitPeer(t, fam, wake)
	clientB := transporttest.NewEndpoint(t)
	if err := clientB.Eth.SetHello(transport.Hello{Selectors: []wire.Selector{selectorAlpha}}); err != nil {
		t.Fatal(err)
	}
	outB, err := clientB.Eth.Dial(t.Context(), pair.server.Shared.Addr(), pair.server.Shared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	defer disconnectTest(t, stack, clientB.Shared.PeerID())
	b := awaitPeer(t, fam, wake)
	for i := range 4 {
		writeSelectorPayload(t, pair.clientConn, selectorAlpha, []byte{byte(i)})
	}
	writeSelectorPayload(t, outB, selectorAlpha, []byte{9})
	waitPending(t, fam, 5)
	first, second := awaitStreamEvent(t, fam, wake), awaitStreamEvent(t, fam, wake)
	if first.Peer == second.Peer || (first.Peer != a && first.Peer != b) || (second.Peer != a && second.Peer != b) {
		t.Fatal("one peer monopolized the first two events")
	}
	first.Reject()
	second.Reject()
}

func TestDeliverySharedWake(t *testing.T) {
	pair := newTestPair(t)
	stack := newTestStack(t, pair.server)
	wake := make(chan struct{}, 1)
	a, err := stack.Register([]ProtocolSpec{{Selector: selectorAlpha, MaxQueued: 8}}, wake)
	if err != nil {
		t.Fatal(err)
	}
	b, err := stack.Register([]ProtocolSpec{{Selector: selectorCommon, MaxQueued: 8}}, wake)
	if err != nil {
		t.Fatal(err)
	}
	pair.connect(t, nil, stack)
	defer disconnectTest(t, stack, pair.client.Shared.PeerID())
	awaitPeer(t, a, wake)
	awaitPeer(t, b, wake)
	for _, item := range []struct {
		fam *Family
		sel wire.Selector
	}{{a, selectorAlpha}, {b, selectorCommon}} {
		if _, ok := item.fam.Next(); ok {
			t.Fatal("queue not empty")
		}
		select {
		case <-wake:
		default:
		}
		writeSelectorPayload(t, pair.clientConn, item.sel, []byte("wake"))
		select {
		case <-wake:
		case <-time.After(testTimeout):
			t.Fatal("missing wake")
		}
		event, ok := item.fam.Next()
		if !ok || event.Kind != StreamIn {
			t.Fatalf("woken event = %+v, %v", event, ok)
		}
		event.Reject()
	}
}

func TestEventMethods(t *testing.T) {
	for _, kind := range []EventKind{PeerUp, PeerDown, StreamIn} {
		raw := new(transporttest.RawReceiveStream)
		event := Event{Kind: kind, Stream: wrapReceiveStream(raw)}
		event.Cancel(wire.Overloaded)
		codes := raw.CancelReadCodes()
		if kind == StreamIn {
			if len(codes) != 1 || codes[0] != 4 {
				t.Fatalf("stream cancellation = %v", codes)
			}
		} else if len(codes) != 0 {
			t.Fatalf("kind %d cancelled stream: %v", kind, codes)
		}
		raw = new(transporttest.RawReceiveStream)
		event.Stream = wrapReceiveStream(raw)
		event.Reject()
		codes = raw.CancelReadCodes()
		if kind == StreamIn {
			if len(codes) != 1 || codes[0] != 2 {
				t.Fatalf("rejection = %v", codes)
			}
		} else if len(codes) != 0 {
			t.Fatalf("kind %d rejected stream: %v", kind, codes)
		}
	}
}

func TestDeliveryViewClosureCode(t *testing.T) {
	for _, takeUp := range []bool{false, true} {
		t.Run(map[bool]string{false: "unobserved", true: "observed"}[takeUp], func(t *testing.T) {
			pair := newTestPair(t)
			stack := newTestStack(t, pair.server)
			fam, wake := registerTestFamily(t, stack, ProtocolSpec{Selector: selectorAlpha, MaxQueued: 8})
			pair.connect(t, nil, stack)
			if takeUp {
				awaitPeer(t, fam, wake)
			}
			writeSelectorPayload(t, pair.clientConn, selectorAlpha, []byte("queued"))
			waitPending(t, fam, 1)
			if err := pair.clientConn.CloseWithCode(wire.Duplicate); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { return len(stack.Connections()) == 0 })
			select {
			case <-pair.clientConn.Done():
			case <-time.After(testTimeout):
				t.Fatal("closed view remained open")
			}
			if code := pair.clientConn.CloseCode(); code != wire.Duplicate {
				t.Fatalf("view close code = %s", code)
			}
			if takeUp {
				down := awaitEvent(t, fam, wake)
				if down.Kind != PeerDown || down.Code != wire.Duplicate || down.Peer.Context().Err() == nil {
					t.Fatalf("view PeerDown = %+v", down)
				}
			}
			if event, ok := fam.Next(); ok {
				t.Fatalf("closed peer left event: %+v", event)
			}
		})
	}
}

func TestDeliverySelectorOnlyStream(t *testing.T) {
	pair := newTestPair(t)
	stack := newTestStack(t, pair.server)
	fam, wake := registerTestFamily(t, stack, ProtocolSpec{Selector: selectorAlpha, MaxQueued: 8})
	pair.connect(t, nil, stack)
	defer disconnectTest(t, stack, pair.client.Shared.PeerID())
	awaitPeer(t, fam, wake)
	writeSelectorPayload(t, pair.clientConn, selectorAlpha, nil)
	event := awaitStreamEvent(t, fam, wake)
	var one [1]byte
	n, err := event.Stream.Read(one[:])
	if n != 0 || err != io.EOF {
		t.Fatalf("first Read = %d, %v", n, err)
	}
}
