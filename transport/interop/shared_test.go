package interop

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestInteropLibp2pDialStaysLibp2p(t *testing.T) {
	left := newSharedHostMode(t, withoutListener)
	right := newSharedHost(t)
	serveEcho(right.host, echoProtocol)
	connectHosts(t, left.host, right.host)

	// A libp2p-initiated connection serves the libp2p stack only: neither
	// side's ethp2p queue receives a view.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := exchangeContext(ctx, left.host, right.host.ID(), echoProtocol, interopPayload("libp2p on shared QUIC")); err != nil {
		t.Fatal(err)
	}
	assertNoEthp2p(t, left.eth)
	assertNoEthp2p(t, right.eth)
}

func TestInteropConnectionWindowGrows(t *testing.T) {
	client := newSharedHostMode(t, withoutListener)
	server := newSharedHost(t)
	serveEcho(server.host, echoProtocol)
	connectHosts(t, client.host, server.host)

	const payloadSize = 4 << 20
	exchange(t, client.host, server.host, echoProtocol, make(interopPayload, payloadSize))
}

func TestInteropMixedTrafficOnEthp2pDial(t *testing.T) {
	left := newSharedHost(t)
	right := newSharedHost(t)
	serveEcho(right.host, echoProtocol)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	pair := connectEth(t, left.eth, right.eth, right.udp.LocalAddr())
	waitConnected(t, left.host, right.host)

	// Drive both views of the shared connection at the same time.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := exchangeContext(ctx, left.host, right.host.ID(), echoProtocol, interopPayload("libp2p after ethp2p dial")); err != nil {
			t.Error(err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := exchangeEth(ctx, pair.dialed, pair.accepted, ethPayload("ethp2p initiated")); err != nil {
			t.Error(err)
		}
	}()
	wg.Wait()
}
