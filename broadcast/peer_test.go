package broadcast

import (
	"context"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/transport"
)

type uniHandshakeTransport struct {
	*testTransport
}

func (t *uniHandshakeTransport) AcceptStream(ctx context.Context) (transport.Stream, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestHandshakeUsesAuthenticatedPeerIDs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	leftRaw, rightRaw := newTestTransportPair(ctx)
	leftRaw.auth = testAuthInfo("authenticated-left", "authenticated-right")
	rightRaw.auth = testAuthInfo("authenticated-right", "authenticated-left")
	left := newPeerConn(
		&Engine{ctx: ctx, config: EngineConfig{Observer: NoOpObserver{}}},
		&uniHandshakeTransport{testTransport: leftRaw},
	)
	right := newPeerConn(
		&Engine{ctx: ctx, config: EngineConfig{Observer: NoOpObserver{}}},
		&uniHandshakeTransport{testTransport: rightRaw},
	)

	type result struct {
		peer PeerID
		err  error
	}
	leftResult := make(chan result, 1)
	rightResult := make(chan result, 1)
	go func() {
		got, _, _, err := left.handshake(ctx, nil)
		leftResult <- result{peer: got, err: err}
	}()
	go func() {
		got, _, _, err := right.handshake(ctx, nil)
		rightResult <- result{peer: got, err: err}
	}()

	if got := <-leftResult; got.err != nil || got.peer != "authenticated-right" {
		t.Fatalf("left handshake = (%q, %v)", got.peer, got.err)
	}
	if got := <-rightResult; got.err != nil || got.peer != "authenticated-left" {
		t.Fatalf("right handshake = (%q, %v)", got.peer, got.err)
	}
}

func TestHandshakeRequiresAuthenticatedPeerIDs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	raw, _ := newTestTransportPair(ctx)
	raw.auth = transport.AuthInfo{}
	peer := newPeerConn(
		&Engine{ctx: ctx, config: EngineConfig{Observer: NoOpObserver{}}},
		&uniHandshakeTransport{testTransport: raw},
	)
	if _, _, _, err := peer.handshake(ctx, nil); err == nil {
		t.Fatal("handshake accepted empty authenticated identity")
	}
}
