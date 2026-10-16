package ethp2p

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/ethp2p/ethp2p/wire"
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

func TestDeliveryQueuedStreamsResetOnDown(t *testing.T) {
	for _, test := range []struct {
		name               string
		takeUp, closeStack bool
	}{{"disconnect before up", false, false}, {"disconnect after up", true, false}, {"close before up", false, true}, {"close after up", true, true}} {
		t.Run(test.name, func(t *testing.T) {
			pair := newTestPair(t)
			stack := newTestStack(t, pair.server)
			fam, wake := registerTestFamily(t, stack, familyAlpha, ProtocolSpec{Protocol: testProtocol, MaxQueued: 8})
			pair.connect(t, nil, stack)
			if test.takeUp {
				awaitPeer(t, fam, wake)
			}
			ctx, stop := context.WithTimeout(t.Context(), testTimeout)
			defer stop()
			uni, err := transporttest.OpenUniStream(ctx, pair.clientConn, selectorAlpha)
			if err != nil {
				t.Fatal(err)
			}
			bi, err := transporttest.OpenStream(ctx, pair.clientConn, selectorAlpha)
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
				disconnectTest(t, stack, pair.client.Endpoint.PeerID())
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
			for _, out := range []interface {
				Write([]byte) (int, error)
				SetWriteDeadline(time.Time) error
			}{uni, bi} {
				_ = out.SetWriteDeadline(time.Now().Add(testTimeout))
				_, err := out.Write(make([]byte, 2<<20))
				reset, ok := errors.AsType[*quic.StreamError](err)
				if !ok || !reset.Remote || uint64(reset.ErrorCode) != 20 {
					t.Fatalf("queued stream write = %v, want remote wire 20", err)
				}
			}
			_ = bi.SetReadDeadline(time.Now().Add(testTimeout))
			_, err = bi.Read(make([]byte, 1))
			reset, ok := errors.AsType[*quic.StreamError](err)
			if !ok || uint64(reset.ErrorCode) != 20 {
				t.Fatalf("bidi read = %v, want wire 20", err)
			}
			// libp2p keeps the connection open, so GOAWAY is what tells the
			// raw client.
			_ = pair.raw.PeerControl.SetReadDeadline(time.Now().Add(testTimeout))
			if code, err := pair.raw.ReadGoAway(); err != nil || code != wire.Closing {
				t.Fatalf("GOAWAY = %s, %v; want %s", code, err, wire.Closing)
			}
			lib, err := pair.clientLib.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			payload := append([]byte{0x13}, "/still-live"...)
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
