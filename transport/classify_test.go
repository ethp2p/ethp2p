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
	// Protocol values skip 19: as a first byte it is libp2p's
	// multistream-select prefix, not an ethp2p header.
	protocolAt := func(i int) wire.Protocol {
		p := wire.Protocol(i + 1)
		if p >= 19 {
			p++
		}
		return p
	}
	for i := range count {
		bi, err := pair.clientEth.OpenStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// A protocol header keeps payload 0x13 from being mistaken for libp2p's
		// multistream-select prefix during bidirectional classification.
		head := wire.AppendHeader(nil, protocolAt(i))
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
		head = wire.AppendHeader(nil, protocolAt(i))
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
		sel, err := wire.ReadHeader(bi)
		if err != nil || sel != protocolAt(i) {
			t.Fatalf("bidi protocol = %v, %v; want %v", sel, err, protocolAt(i))
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
		sel, err = wire.ReadHeader(uni)
		if err != nil || sel != protocolAt(i) {
			t.Fatalf("uni protocol = %v, %v; want %v", sel, err, protocolAt(i))
		}
		data, err = io.ReadAll(uni)
		if err != nil || len(data) != 1 || data[0] != byte(i) {
			t.Fatalf("uni payload = %x, %v", data, err)
		}
	}
}
