package transport_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/ethp2p/ethp2p/wire"
)

func connContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestAcceptReturnsAfterHello(t *testing.T) {
	client, server := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	clientHello := transport.Hello{Selectors: []wire.Selector{1, 2}}
	serverHello := transport.Hello{Selectors: []wire.Selector{3, 4}}
	if err := client.Eth.SetHello(clientHello); err != nil {
		t.Fatal(err)
	}
	if err := server.Eth.SetHello(serverHello); err != nil {
		t.Fatal(err)
	}
	dialed, accepted := transporttest.Connect(t, client, server)
	// Accept and Dial return only after the peer Hello arrived, so PeerHello
	// does not block and reports the dialer's selectors on the accepted view
	// and vice versa.
	if got := accepted.PeerHello(); !slices.Equal(got.Selectors, []wire.Selector{1, 2}) {
		t.Fatalf("accepted PeerHello = %+v, want selectors [1 2]", got)
	}
	if got := dialed.PeerHello(); !slices.Equal(got.Selectors, []wire.Selector{3, 4}) {
		t.Fatalf("dialed PeerHello = %+v, want selectors [3 4]", got)
	}
}

func TestNextStreamMergesDirections(t *testing.T) {
	client, server := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	dialed, accepted := transporttest.Connect(t, client, server)
	ctx := connContext(t)
	uni, err := dialed.OpenUniStream(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uni.Write([]byte{0xaa}); err != nil {
		t.Fatal(err)
	}
	_ = uni.Close()
	// The first stream is delivered before the second is opened, fixing the
	// classification order across directions.
	first, sel, err := transporttest.NextStream(ctx, accepted)
	if err != nil || sel != 5 {
		t.Fatalf("first stream = %d, %v; want 5", sel, err)
	}
	if _, ok := first.(transport.Stream); ok {
		t.Fatal("unidirectional stream implements transport.Stream")
	}
	payload, err := io.ReadAll(first)
	if err != nil || len(payload) != 1 || payload[0] != 0xaa {
		t.Fatalf("uni payload = %x, %v", payload, err)
	}
	bidi, err := dialed.OpenStream(ctx, 6)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bidi.Write([]byte{0xbb}); err != nil {
		t.Fatal(err)
	}
	_ = bidi.Close()
	second, sel, err := transporttest.NextStream(ctx, accepted)
	if err != nil || sel != 6 {
		t.Fatalf("second stream = %d, %v; want 6", sel, err)
	}
	secondBi, ok := second.(transport.Stream)
	if !ok {
		t.Fatal("bidirectional stream does not implement transport.Stream")
	}
	payload, err = io.ReadAll(secondBi)
	if err != nil || len(payload) != 1 || payload[0] != 0xbb {
		t.Fatalf("bidi payload = %x, %v", payload, err)
	}
}

func TestCloseCodeFromGoAway(t *testing.T) {
	client, server := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	dialed, accepted := transporttest.Connect(t, client, server)
	if err := dialed.CloseWithCode(wire.Duplicate); err != nil {
		t.Fatal(err)
	}
	ctx := connContext(t)
	select {
	case <-accepted.Done():
	case <-ctx.Done():
		t.Fatalf("acceptor Done did not close: %v", ctx.Err())
	}
	if got := accepted.CloseCode(); got != wire.Duplicate {
		t.Fatalf("CloseCode = %s, want %s", got, wire.Duplicate)
	}
}

func TestEthp2pTransportCloseStopsAccept(t *testing.T) {
	client, server := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	ctx := connContext(t)
	pending := make(chan error, 1)
	go func() {
		_, err := server.Eth.Accept(ctx)
		pending <- err
	}()
	server.Eth.Close()
	server.Eth.Close()
	if err := <-pending; !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("pending Accept = %v, want %v", err, transport.ErrClosed)
	}
	if _, err := server.Eth.Accept(ctx); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("Accept after Close = %v, want %v", err, transport.ErrClosed)
	}
	// The server releases the ethp2p view at once and the raw close carries
	// Closing, so the dialer observes a view closure with that code.
	_, err := client.Eth.Dial(ctx, server.Shared.Addr(), server.Shared.PeerID())
	closed, ok := errors.AsType[*transport.ViewClosedError](err)
	if !ok || closed.Code != wire.Closing {
		t.Fatalf("Dial after peer Close = %v, want ViewClosedError with Closing", err)
	}
}
