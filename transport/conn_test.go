package transport_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/quic-go/quic-go"
)

func connContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestNextStreamPerDirection(t *testing.T) {
	client, server := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	dialed, accepted := transporttest.Connect(t, client, server)
	ctx := connContext(t)
	uni, err := transporttest.OpenUniStream(ctx, dialed, testSelector(5))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uni.Write([]byte{0xaa}); err != nil {
		t.Fatal(err)
	}
	_ = uni.Close()
	// Wait for the uni stream to be queued before checking the bidi queue,
	// so an empty result proves direction separation rather than arrival timing.
	select {
	case <-accepted.UniStreams():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if s, ok := accepted.NextStream(); ok {
		t.Fatalf("bidi queue yielded %T before a bidi stream was opened", s)
	}
	first, sel, err := transporttest.NextUniStream(ctx, accepted)
	if err != nil || sel != testSelector(5) {
		t.Fatalf("first stream = %d, %v; want 5", sel, err)
	}
	if _, ok := any(first).(*quic.Stream); ok {
		t.Fatal("unidirectional stream is a *quic.Stream")
	}
	payload, err := io.ReadAll(first)
	if err != nil || len(payload) != 1 || payload[0] != 0xaa {
		t.Fatalf("uni payload = %x, %v", payload, err)
	}
	bidi, err := transporttest.OpenStream(ctx, dialed, testSelector(6))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bidi.Write([]byte{0xbb}); err != nil {
		t.Fatal(err)
	}
	_ = bidi.Close()
	second, sel, err := transporttest.NextStream(ctx, accepted)
	if err != nil || sel != testSelector(6) {
		t.Fatalf("second stream = %d, %v; want 6", sel, err)
	}
	defer second.Close()
	payload, err = io.ReadAll(second)
	if err != nil || len(payload) != 1 || payload[0] != 0xbb {
		t.Fatalf("bidi payload = %x, %v", payload, err)
	}
}

func TestCloseCodeFromConnectionClose(t *testing.T) {
	client, server := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	dialed, accepted := transporttest.Connect(t, client, server)
	ctx := connContext(t)
	// Release both libp2p views too: an ethp2p_0 connection has both views,
	// and the physical connection closes only once every view is released.
	for _, ep := range []*transporttest.Endpoint{client, server} {
		listener, err := ep.Endpoint.Libp2p().Listen(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		libConn, err := listener.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := libConn.CloseWithError(quic.ApplicationErrorCode(wire.Duplicate.Wire()), ""); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = listener.Close() })
	}
	dialed.CloseWithCode(wire.Duplicate)
	select {
	case <-accepted.Done():
	case <-ctx.Done():
		t.Fatalf("acceptor Done did not close: %v", ctx.Err())
	}
	if got := accepted.CloseCode(); got != wire.Duplicate {
		t.Fatalf("CloseCode = %s, want %s", got, wire.Duplicate)
	}
}
