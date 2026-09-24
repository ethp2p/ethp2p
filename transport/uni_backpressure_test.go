package transport

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/protocol"
	"github.com/quic-go/quic-go"
)

// testProfile returns Interop with the incoming unidirectional stream limit set
// explicitly to exercise stream-credit backpressure.
func testProfile(incomingUni int64) Profile {
	profile := Interop()
	profile.maxIncomingUniStreams = incomingUni
	return profile
}

func waitQueued[T any](t *testing.T, q *streamQueue[T], n int) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		q.mu.Lock()
		count := len(q.items)
		q.mu.Unlock()
		if count == n {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("queued = %d, want %d", count, n)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestClassifiedStreamsWaitWithoutOverload(t *testing.T) {
	ctx := testContext(t)
	pair := newViewPair(t, 64)
	server := pair.serverEth.(*ethp2pConn)
	const count = 20
	for i := range count {
		bi, err := pair.clientEth.OpenStream(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := bi.Write([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		if err := bi.Close(); err != nil {
			t.Fatal(err)
		}
		uni, err := pair.clientEth.OpenUniStream(ctx, 2)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := uni.Write([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		if err := uni.Close(); err != nil {
			t.Fatal(err)
		}
		// Finish one classification before opening the next: this checks FIFO
		// delivery independently of the classifier's intentional completion ordering.
		waitQueued(t, server.ethp2pBi, i+1)
		waitQueued(t, server.ethp2pUni, i+1)
	}
	checkLibp2pRoundTrip(t, ctx, pair.clientLib, pair.serverLib)
	for i := range count {
		bi, sel, err := pair.serverEth.AcceptStream(ctx)
		if err != nil || sel != 1 {
			t.Fatalf("bidi = %d, %v", sel, err)
		}
		data, err := io.ReadAll(bi)
		if err != nil || len(data) != 1 || data[0] != byte(i) {
			t.Fatalf("bidi payload = %x, %v", data, err)
		}
		_ = bi.Close()
		uni, sel, err := pair.serverEth.AcceptUniStream(ctx)
		if err != nil || sel != 2 {
			t.Fatalf("uni = %d, %v", sel, err)
		}
		data, err = io.ReadAll(uni)
		if err != nil || len(data) != 1 || data[0] != byte(i) {
			t.Fatalf("uni payload = %x, %v", data, err)
		}
	}
}

func TestQueuedStreamsResetOnViewClose(t *testing.T) {
	ctx := testContext(t)
	pair := newViewPair(t, 32)
	bi, err := pair.clientEth.OpenStream(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	uni, err := pair.clientEth.OpenUniStream(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	waitQueued(t, pair.serverEth.(*ethp2pConn).ethp2pBi, 1)
	waitQueued(t, pair.serverEth.(*ethp2pConn).ethp2pUni, 1)
	if err := pair.serverEth.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = pair.serverEth.PeerHello(ctx)
	assertEthViewCause(t, err, protocol.Closing, false)
	var one [1]byte
	_, err = bi.Read(one[:])
	reset, ok := errors.AsType[*StreamResetError](err)
	if !ok || reset.Code != 20 {
		t.Fatalf("bidi reset = %v", err)
	}
	for _, out := range []SendStream{bi, uni} {
		_ = out.SetWriteDeadline(time.Now().Add(time.Second))
		_, err := out.Write(make([]byte, 1<<20))
		reset, ok := errors.AsType[*quic.StreamError](err)
		if !ok || !reset.Remote || uint64(reset.ErrorCode) != 20 {
			t.Fatalf("write reset = %v", err)
		}
	}
	checkLibp2pRoundTrip(t, ctx, pair.clientLib, pair.serverLib)
}
