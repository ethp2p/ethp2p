package broadcast

import (
	"context"
	"testing"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/ethp2p/ethp2p/wire"
)

func TestRegisteredDeliveryClosesBindingAndRefusesUnboundStream(t *testing.T) {
	// The test owns the event loop so it can inspect bindings without a
	// production inspection event or racing run(). Delivery still comes from
	// a real Stack and QUIC endpoints.
	ctx, cancel := context.WithCancel(t.Context())
	e := &Engine{
		ctx: ctx, cancel: cancel, config: EngineConfig{Observer: NoOpObserver{}},
		eventCh: make(chan func(), 128), deliveryWake: make(chan struct{}, 1),
		bindings: make(map[peerHandle]*PeerConn),
		peers:    make(map[identity.PeerID]*PeerConn),
	}
	// The test owns the event loop: claim Start so Register does not start it.
	e.started = true
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
	if _, err := transporttest.DialHello(t.Context(), client, endpoint, transporttest.Hello{Protocols: []wire.Protocol{Protocol}}); err != nil {
		t.Fatal(err)
	}
	sub := e.proto
	up := nextOutcomeEvent(t, sub, e.deliveryWake)
	if up.Kind != ethp2p.PeerUp {
		t.Fatalf("first event = %v", up.Kind)
	}
	e.handleDelivery(up)
	binding := e.bindings[up.Peer]
	if binding == nil {
		t.Fatal("PeerUp did not bind peer")
	}
	if err := stack.Disconnect(client.Endpoint.PeerID()); err != nil {
		t.Fatal(err)
	}
	down := nextOutcomeEvent(t, sub, e.deliveryWake)
	if down.Kind != ethp2p.PeerDown || down.Code != wire.Closing {
		t.Fatalf("down = %+v", down)
	}
	e.handleDelivery(down)
	if e.bindings[up.Peer] != nil || binding.ctx.Err() == nil {
		t.Fatal("PeerDown left binding live")
	}

	f := newOutcomeFixture(t)
	raw, stream := f.incoming(t, nil)
	e.handleDelivery(ethp2p.Event{Kind: ethp2p.StreamIn, Peer: f.peer, Stream: stream})
	requireCode(t, raw.CancelReadCodes(), 2)
	if e.bindings[f.peer] != nil {
		t.Fatal("stream created a binding without PeerUp")
	}
}
