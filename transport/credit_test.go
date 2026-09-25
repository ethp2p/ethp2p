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
			name = "selector-only"
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
	// Four application streams plus the lifetime control stream.
	shared, err := transport.NewShared(key, packet, transport.ProfileWithIncomingUniStreams(5))
	if err != nil {
		t.Fatal(err)
	}
	defer shared.Close()
	server := &transporttest.Endpoint{Shared: shared, Eth: shared.Ethp2p(), Packet: packet, Key: key}
	if err := server.Eth.SetHello(transport.Hello{}); err != nil {
		t.Fatal(err)
	}
	outConn, inConn := transporttest.Connect(t, client, server)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for range 4 {
		out, err := outConn.OpenUniStream(ctx, 1)
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
	if _, err := outConn.OpenUniStream(short, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("open without credit = %v", err)
	}
	readOne := func() {
		t.Helper()
		in, _, err := transporttest.NextStream(ctx, inConn)
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
	out, err := outConn.OpenUniStream(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	_ = out.Close()
	for range 3 {
		readOne()
	}
}

func TestUniSelectorSkip(t *testing.T) {
	for _, reset := range []bool{false, true} {
		name := "one-byte payload reads"
		if reset {
			name = "reset before first read"
		}
		t.Run(name, func(t *testing.T) {
			client, server := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
			outConn, inConn := transporttest.Connect(t, client, server)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			// Exercise a selector whose encoding spans several bytes.
			const selector = 16384
			out, err := outConn.OpenUniStream(ctx, selector)
			if err != nil {
				t.Fatal(err)
			}
			in, gotSelector, err := transporttest.NextStream(ctx, inConn)
			if err != nil || gotSelector != selector {
				t.Fatalf("accept = %d, %v", gotSelector, err)
			}
			_ = in.SetReadDeadline(time.Now().Add(5 * time.Second))
			if reset {
				out.CancelWrite(77)
				var one [1]byte
				n, err := in.Read(one[:])
				r, ok := errors.AsType[*transport.StreamResetError](err)
				if n != 0 || !ok || r.Code != 77 {
					t.Fatalf("first Read = %d, %v; want reset 77", n, err)
				}
				return
			}
			want := []byte("payload after selector")
			if _, err := out.Write(want); err != nil {
				t.Fatal(err)
			}
			if err := out.Close(); err != nil {
				t.Fatal(err)
			}
			var got []byte
			for {
				var one [1]byte
				n, err := in.Read(one[:])
				got = append(got, one[:n]...)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("payload = %q, want %q", got, want)
			}
		})
	}
}
