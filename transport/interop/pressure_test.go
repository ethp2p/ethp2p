package interop

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	ma "github.com/multiformats/go-multiaddr"
)

func TestInteropLibp2pWithUnconsumedEthp2p(t *testing.T) {
	server := newSharedHost(t)
	serveEcho(server.host, echoProtocol)
	const clients = 20
	for i := range clients {
		client := newSharedHost(t)
		connectHosts(t, client.host, server.host)
		exchange(t, client.host, server.host, echoProtocol, interopPayload(fmt.Sprintf("client %d", i)))
	}
}

func TestInteropParallelDials(t *testing.T) {
	server := newSharedHost(t)
	serveEcho(server.host, echoProtocol)
	const clients = 8
	stocks := make([]host.Host, clients)
	for i := range stocks {
		stocks[i] = newQUICHost(t)
	}
	errs := make(chan error, clients)
	for i, stock := range stocks {
		go func() {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			if err := connectHostsContext(ctx, stock, server.host.ID(), []ma.Multiaddr{loopbackQUICAddr(t, server.host)}); err != nil {
				errs <- fmt.Errorf("client %d connect: %w", i, err)
				return
			}
			errs <- exchangeContext(ctx, stock, server.host.ID(), echoProtocol, interopPayload(fmt.Sprintf("parallel client %d", i)))
		}()
	}
	for range clients {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	for _, stock := range stocks {
		if len(server.host.Network().ConnsToPeer(stock.ID())) == 0 {
			t.Fatalf("server did not retain a connection from %s", stock.ID())
		}
	}
}
