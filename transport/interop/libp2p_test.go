package interop

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	ma "github.com/multiformats/go-multiaddr"
)

func TestInteropRealLibp2pExchange(t *testing.T) {
	for _, family := range []ipFamily{ipv4, ipv6} {
		t.Run(string(family), func(t *testing.T) {
			if family == ipv6 {
				probe, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
				if err != nil {
					t.Skipf("IPv6 loopback unavailable: %v", err)
				}
				_ = probe.Close()
			}
			for _, direction := range []dialDirection{sharedDialsStock, stockDialsShared} {
				t.Run(string(direction), func(t *testing.T) {
					shared := newSharedHostFamily(t, family, withListener)
					stock := newQUICHostFamily(t, family)
					longProtocol := interopProtocol("/" + strings.Repeat("selector", 20))
					serveEcho(shared.host, echoProtocol)
					serveEcho(stock, echoProtocol)
					serveEcho(shared.host, longProtocol)
					serveEcho(stock, longProtocol)

					switch direction {
					case sharedDialsStock:
						connectHosts(t, shared.host, stock)
					case stockDialsShared:
						connectHosts(t, stock, shared.host)
					}

					exchange(t, shared.host, stock, echoProtocol, interopPayload("shared to stock"))
					exchange(t, stock, shared.host, echoProtocol, interopPayload("stock to shared"))
					exchange(t, shared.host, stock, longProtocol, interopPayload("multibyte selector"))
					assertNoEthp2p(t, shared.eth)
				})
			}
		})
	}
}

func TestInteropWrongPeerID(t *testing.T) {
	for _, direction := range []dialDirection{sharedDialsStock, stockDialsShared} {
		t.Run(string(direction), func(t *testing.T) {
			shared := newSharedHost(t)
			stock := newQUICHost(t)
			wrongID := newPeerID(t)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			var err error
			switch direction {
			case sharedDialsStock:
				err = connectHostsContext(ctx, shared.host, wrongID, []ma.Multiaddr{loopbackQUICAddr(t, stock)})
			case stockDialsShared:
				err = connectHostsContext(ctx, stock, wrongID, []ma.Multiaddr{loopbackQUICAddr(t, shared.host)})
			}
			if err == nil {
				t.Fatal("connection with the wrong peer ID succeeded")
			}
			if got := len(shared.host.Network().Conns()); got != 0 {
				t.Fatalf("shared host retained %d connections after rejection", got)
			}
		})
	}
}

func TestInteropParallelLibp2pStreams(t *testing.T) {
	shared := newSharedHost(t)
	stock := newQUICHost(t)
	serveEcho(stock, echoProtocol)
	connectHosts(t, shared.host, stock)

	const streams = 16
	errs := make(chan error, streams)
	for i := range streams {
		go func() {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			errs <- exchangeContext(ctx, shared.host, stock.ID(), echoProtocol, interopPayload(fmt.Sprintf("stream %d", i)))
		}()
	}
	for range streams {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}
