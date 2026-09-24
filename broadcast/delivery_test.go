package broadcast

import (
	"context"
	"testing"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/protocol"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
)

func TestRegisteredDeliveryClosesBindingAndRefusesUnboundStream(t *testing.T) {
	// The test owns the event loop so it can inspect bindings without a
	// production inspection event or racing run(). Delivery still comes from
	// a real Stack and QUIC endpoints.
	ctx, cancel := context.WithCancel(t.Context())
	e := &Engine{
		ctx: ctx, cancel: cancel, config: EngineConfig{Observer: NoOpObserver{}},
		eventCh: make(chan engineEvent, 128), deliveryWake: make(chan struct{}, 1),
		bindings: make(map[*ethp2p.Peer]*PeerConn),
		peers:    make(map[transport.PeerID]*PeerConn),
	}
	t.Cleanup(func() { cancel(); e.shutdown(); e.wg.Wait() })
	endpoint := transporttest.NewEndpoint(t)
	stack, err := ethp2p.NewStack(endpoint.Eth, ethp2p.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stack.Close() })
	if err := e.Register(stack); err != nil {
		t.Fatal(err)
	}
	if err := e.Register(stack); err == nil {
		t.Fatal("second Register succeeded")
	}
	if err := stack.Start(); err != nil {
		t.Fatal(err)
	}
	client := transporttest.NewEndpoint(t)
	if err := client.Eth.SetHello(transport.Hello{Selectors: []protocol.Selector{BCAST, SESS, CHUNK}}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Eth.Dial(t.Context(), endpoint.Shared.Addr(), endpoint.Shared.PeerID()); err != nil {
		t.Fatal(err)
	}
	sub := e.subsystem.Load()
	up := nextOutcomeEvent(t, sub, e.deliveryWake)
	if up.Kind != ethp2p.PeerUp {
		t.Fatalf("first event = %v", up.Kind)
	}
	e.handleDelivery(up)
	binding := e.bindings[up.Peer]
	if binding == nil {
		t.Fatal("PeerUp did not bind peer")
	}
	if err := stack.Disconnect(client.Shared.PeerID()); err != nil {
		t.Fatal(err)
	}
	down := nextOutcomeEvent(t, sub, e.deliveryWake)
	if down.Kind != ethp2p.PeerDown || down.Code != protocol.Closing {
		t.Fatalf("down = %+v", down)
	}
	e.handleDelivery(down)
	if e.bindings[up.Peer] != nil || binding.ctx.Err() == nil {
		t.Fatal("PeerDown left binding live")
	}

	f := newOutcomeFixture(t)
	raw, stream := f.incoming(t, SESS, nil)
	e.handleDelivery(ethp2p.Event{Kind: ethp2p.StreamIn, Peer: f.peer, Selector: SESS, Stream: stream})
	assertTransportCodes(t, raw.CancelReadCodes(), []uint64{2})
	if e.bindings[f.peer] != nil {
		t.Fatal("stream created a binding without PeerUp")
	}
}
