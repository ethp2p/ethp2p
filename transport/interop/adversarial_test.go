package interop

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/quic-go/quic-go"
)

func TestInteropMalformedSelectorsRecover(t *testing.T) {
	tests := []struct {
		name string
		wire selectorWire
	}{
		{name: "empty frame", wire: selectorWire{0}},
		{name: "unterminated length", wire: selectorWire{0x80}},
		{name: "missing selector", wire: selectorWire{1}},
		{name: "overlong length", wire: selectorWire{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newSharedHostMode(t, withoutListener)
			server := newSharedHostMode(t, withoutListener)
			pair := connectEth(t, client.eth, server.eth, server.udp.LocalAddr())

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			stream, err := pair.dialed.OpenStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Write(test.wire); err != nil {
				t.Fatal(err)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if err := exchangeEth(ctx, pair.dialed, pair.accepted, ethPayload("recovered")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInteropStalledSelectorRecovers(t *testing.T) {
	client := newSharedHostMode(t, withoutListener)
	server := newSharedHostMode(t, withoutListener)
	pair := connectEth(t, client.eth, server.eth, server.udp.LocalAddr())
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	stream, err := pair.dialed.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte{0x80}); err != nil {
		t.Fatal(err)
	}
	if err := stream.SetReadDeadline(time.Now().Add(7 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Read(make([]byte, 1)); err == nil {
		t.Fatal("stalled selector was not reset")
	}
	if err := exchangeEth(ctx, pair.dialed, pair.accepted, ethPayload("after timeout")); err != nil {
		t.Fatal(err)
	}
}

func TestInteropPeerClosureWhileClassifying(t *testing.T) {
	client := newSharedHostMode(t, withoutListener)
	server := newSharedHostMode(t, withoutListener)
	first := connectEth(t, client.eth, server.eth, server.udp.LocalAddr())
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	stream, err := first.dialed.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte{0x80}); err != nil {
		t.Fatal(err)
	}
	if err := first.dialed.Close(); err != nil {
		t.Fatal(err)
	}
	second := connectEth(t, client.eth, server.eth, server.udp.LocalAddr())
	if err := exchangeEth(ctx, second.dialed, second.accepted, ethPayload("new connection")); err != nil {
		t.Fatal(err)
	}
}

func TestInteropNoCommonALPN(t *testing.T) {
	t.Run("shared host dials", func(t *testing.T) {
		serverUDP := listenInteropUDP(t, ipv4)
		key, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, -1)
		if err != nil {
			t.Fatal(err)
		}
		id, err := peer.IDFromPrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		identity, err := libp2ptls.NewIdentity(key)
		if err != nil {
			t.Fatal(err)
		}
		serverTLS, _ := identity.ConfigForPeer("")
		serverTLS.NextProtos = []string{"h3"}
		transport := &quic.Transport{Conn: serverUDP}
		t.Cleanup(func() { _ = transport.Close() })
		listener, err := transport.Listen(serverTLS, &quic.Config{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = listener.Close() })

		shared := newSharedHost(t)
		addr := ma.StringCast(fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic-v1", serverUDP.LocalAddr().(*net.UDPAddr).Port))
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		err = connectHostsContext(ctx, shared.host, id, []ma.Multiaddr{addr})
		if err == nil {
			t.Fatal("dial without a common ALPN succeeded")
		}
	})

	t.Run("raw peer dials shared host", func(t *testing.T) {
		shared := newSharedHost(t)
		clientTLS := &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true, NextProtos: []string{"h3"}}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		_, err := quic.DialAddr(ctx, hostUDPAddr(t, shared.host).String(), clientTLS, &quic.Config{})
		if err == nil {
			t.Fatal("inbound dial without a common ALPN succeeded")
		}
	})
}

func TestInteropDialTimeout(t *testing.T) {
	shared := newSharedHost(t)
	id := newPeerID(t)
	blackhole := listenInteropUDP(t, ipv4)
	addr := ma.StringCast(fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic-v1", blackhole.LocalAddr().(*net.UDPAddr).Port))

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := connectHostsContext(ctx, shared.host, id, []ma.Multiaddr{addr})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed-out dial error = %v, want context deadline", err)
	}
}
