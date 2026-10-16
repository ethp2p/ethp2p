package transport_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
)

func TestUnidirectionalCreditBackpressure(t *testing.T) {
	for _, payload := range [][]byte{{0xaa}, nil} {
		name := "payload"
		if payload == nil {
			name = "fin-only"
		}
		t.Run(name, func(t *testing.T) { testUniCredit(t, payload) })
	}
}

func testUniCredit(t *testing.T, want []byte) {
	client := transporttest.NewEndpoint(t)
	key, err := identity.GenPrivKey()
	if err != nil {
		t.Fatal(err)
	}
	packet, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer packet.Close()
	// Transport opens no stream of its own, so all four credits are the
	// peer's.
	endpoint, err := transport.NewEndpoint(key, packet, transport.ConfigWithIncomingUniStreams(4))
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	_ = endpoint.Libp2p()
	_ = endpoint.Ethp2p()
	if err := endpoint.Start(); err != nil {
		t.Fatal(err)
	}
	server := &transporttest.Endpoint{Endpoint: endpoint, Eth: endpoint.Ethp2p(), Packet: packet, Key: key}
	outConn, inConn := transporttest.Connect(t, client, server)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for range 4 {
		out, err := outConn.OpenUniStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := out.Write(want); err != nil {
			t.Fatal(err)
		}
		if err := out.Close(); err != nil {
			t.Fatal(err)
		}
	}
	short, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stop()
	if _, err := outConn.OpenUniStream(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("open without credit = %v", err)
	}
	readOne := func() {
		t.Helper()
		in, err := transporttest.NextRawUniStream(ctx, inConn)
		if err != nil {
			t.Fatal(err)
		}
		if len(want) == 0 {
			var one [1]byte
			n, err := in.Read(one[:])
			if n != 0 || err != io.EOF {
				t.Fatalf("first Read = %d, %v", n, err)
			}
			return
		}
		payload, err := io.ReadAll(in)
		if err != nil || !bytes.Equal(payload, want) {
			t.Fatalf("queued stream = %x, %v", payload, err)
		}
	}
	readOne()
	out, err := outConn.OpenUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = out.Close()
	for range 3 {
		readOne()
	}
}
