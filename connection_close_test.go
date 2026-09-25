package ethp2p

import (
	"errors"
	"testing"

	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/ethp2p/ethp2p/transport/transporttest"
)

func TestPureEthp2pCloseCode(t *testing.T) {
	for _, disconnect := range []bool{false, true} {
		t.Run(map[bool]string{false: "Close", true: "Disconnect"}[disconnect], func(t *testing.T) {
			a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
			left, right := newTestStack(t, a), newTestStack(t, b)
			ls, lsWake := registerTestFamily(t, left, ProtocolSpec{Selector: 1, MaxQueued: 8})
			rs, rsWake := registerTestFamily(t, right, ProtocolSpec{Selector: 1, MaxQueued: 8})
			startTestStack(t, left)
			startTestStack(t, right)
			if err := left.Connect(t.Context(), b.Record(t, 1)); err != nil {
				t.Fatal(err)
			}
			awaitPeer(t, ls, lsWake)
			awaitPeer(t, rs, rsWake)
			var err error
			if disconnect {
				err = left.Disconnect(b.Shared.PeerID())
			} else {
				err = left.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, sub := range []struct {
				fam  *Family
				wake chan struct{}
			}{{ls, lsWake}, {rs, rsWake}} {
				e := awaitEvent(t, sub.fam, sub.wake)
				if e.Kind != PeerDown || e.Code != wire.Closing {
					t.Fatalf("down = %+v", e)
				}
			}
		})
	}
}

func TestPureEthp2pRejectionCode(t *testing.T) {
	a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	left, right := newTestStack(t, a), newTestStack(t, b)
	registerTestFamily(t, left, ProtocolSpec{Selector: 1, MaxQueued: 8})
	registerTestFamily(t, right, ProtocolSpec{Selector: 2, MaxQueued: 8})
	startTestStack(t, left)
	startTestStack(t, right)
	err := left.Connect(t.Context(), b.Record(t, 1))
	closed, ok := errors.AsType[*transport.ViewClosedError](err)
	if !ok || closed.Code != wire.NoSharedProtocols {
		t.Fatalf("rejection = %v", err)
	}
}
