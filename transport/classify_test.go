package transport

import (
	"io"
	"testing"

	"github.com/ethp2p/ethp2p/wire"
	"github.com/quic-go/quic-go"
)

func TestClassifiedStreamsWaitWithoutOverload(t *testing.T) {
	ctx := testContext(t)
	pair := newViewPair(t, 64)
	server := pair.serverEth
	const count = 20
	for i := range count {
		bi, err := pair.clientEth.OpenStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// A selector keeps payload 0x13 from being mistaken for libp2p's
		// multistream-select prefix during bidirectional classification.
		head := wire.AppendSelector(nil, testSelector(wire.Protocol(i+1)))
		if _, err := bi.Write(append(head, byte(i))); err != nil {
			t.Fatal(err)
		}
		if err := bi.Close(); err != nil {
			t.Fatal(err)
		}
		// Wait for each queue to retain the stream before opening the next
		// pair, so the reads below exercise transport's queued backpressure.
		waitQueued(t, server.ethp2pBidi, i+1)
		uni, err := pair.clientEth.OpenUniStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		head = wire.AppendSelector(nil, testSelector(wire.Protocol(i+1)))
		if _, err := uni.Write(append(head, byte(i))); err != nil {
			t.Fatal(err)
		}
		if err := uni.Close(); err != nil {
			t.Fatal(err)
		}
		waitQueued(t, server.ethp2pUni, i+1)
	}
	checkLibp2pRoundTrip(t, ctx, pair.clientLib, pair.serverLib)
	for i := range count {
		received := nextStream(t, pair.serverEth)
		bi, ok := any(received).(*quic.Stream)
		if !ok {
			t.Fatal("bidirectional stream is not a *quic.Stream")
		}
		sel, err := wire.ReadSelector(bi)
		if err != nil || sel != testSelector(wire.Protocol(i+1)) {
			t.Fatalf("bidi selector = %v, %v; want %v", sel, err, testSelector(wire.Protocol(i+1)))
		}
		data, err := io.ReadAll(bi)
		if err != nil || len(data) != 1 || data[0] != byte(i) {
			t.Fatalf("bidi payload = %x, %v", data, err)
		}
		_ = bi.Close()
		uni := nextUniStream(t, pair.serverEth)
		if _, ok := any(uni).(*quic.Stream); ok {
			t.Fatal("unidirectional stream is a *quic.Stream")
		}
		sel, err = wire.ReadSelector(uni)
		if err != nil || sel != testSelector(wire.Protocol(i+1)) {
			t.Fatalf("uni selector = %v, %v; want %v", sel, err, testSelector(wire.Protocol(i+1)))
		}
		data, err = io.ReadAll(uni)
		if err != nil || len(data) != 1 || data[0] != byte(i) {
			t.Fatalf("uni payload = %x, %v", data, err)
		}
	}
}
