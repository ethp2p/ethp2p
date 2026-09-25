package ethp2p

import (
	"errors"
	"testing"

	"github.com/ethp2p/ethp2p/wire"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
)

func TestPureEthp2pCloseCode(t *testing.T) {
	for _, disconnect := range []bool{false, true} {
		t.Run(map[bool]string{false: "Close", true: "Disconnect"}[disconnect], func(t *testing.T) {
			a, b := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
			left, right := newTestStack(t, a), newTestStack(t, b)
			ls, rs := registerTestSub(t, left, "left", 1), registerTestSub(t, right, "right", 1)
			startTestStack(t, left)
			startTestStack(t, right)
			if err := left.Connect(t.Context(), b.Record(t, 1)); err != nil {
				t.Fatal(err)
			}
			awaitPeer(t, ls)
			awaitPeer(t, rs)
			var err error
			if disconnect {
				err = left.Disconnect(b.Eth.PeerID())
			} else {
				err = left.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, sub := range []*Subsystem{ls, rs} {
				e := awaitEvent(t, sub)
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
	registerTestSub(t, left, "left", 1)
	registerTestSub(t, right, "right", 2)
	startTestStack(t, left)
	startTestStack(t, right)
	err := left.Connect(t.Context(), b.Record(t, 1))
	closed, ok := errors.AsType[*transport.ViewClosedError](err)
	if !ok || closed.Code != wire.NoSharedProtocols {
		t.Fatalf("rejection = %v", err)
	}
}
