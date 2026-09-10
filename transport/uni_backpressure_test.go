package transport

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

func TestUnidirectionalBackpressure(t *testing.T) {
	for _, resume := range []bool{true, false} {
		name := "close"
		if resume {
			name = "resume"
		}
		t.Run(name, func(t *testing.T) {
			ctx := testContext(t)
			// Exercise the public adapter with a caller-supplied QUIC limit
			// larger than the delivery buffer. Production's five-stream limit
			// cannot fill that buffer; changing it is not part of this test.
			const streams = 32
			client, server := newQUICPair(t, streams)
			for i := range streams {
				stream, err := client.OpenUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := stream.Write([]byte{byte(i)}); err != nil {
					t.Fatal(err)
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
			}

			// Without a consumer, credit must stay exhausted. Resetting a
			// queued stream would return credit and let this open succeed.
			waitCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			_, err := client.OpenUniStream(waitCtx)
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("open beyond unread stream limit = %v, want deadline exceeded", err)
			}

			if !resume {
				opened := make(chan error, 1)
				go func() {
					_, err := client.OpenUniStream(ctx)
					opened <- err
				}()
				if err := server.Close(); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-opened:
					if err == nil || errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("blocked open after connection close = %v, want connection error", err)
					}
				case <-ctx.Done():
					t.Fatal("connection close did not unblock sender")
				}
				return
			}

			for i := range streams {
				stream, err := server.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := stream.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				payload, err := io.ReadAll(stream)
				if err != nil {
					t.Fatalf("read stream %d: %v", i, err)
				}
				if len(payload) != 1 || payload[0] != byte(i) {
					t.Fatalf("stream %d payload = %x", i, payload)
				}
			}
			stream, err := client.OpenUniStream(ctx)
			if err != nil {
				t.Fatalf("consumption did not restore stream credit: %v", err)
			}
			stream.CancelWrite(0)
		})
	}
}

func newQUICPair(t *testing.T, incomingUni int64) (Conn, Conn) {
	t.Helper()
	ctx := testContext(t)
	clientIdentity, err := newHandshaker(testKey(t))
	if err != nil {
		t.Fatal(err)
	}
	serverIdentity, err := newHandshaker(testKey(t))
	if err != nil {
		t.Fatal(err)
	}
	clientTLS := clientIdentity.dialConfig(nil, serverIdentity.peerID)
	serverTLS := serverIdentity.dialConfig(nil, clientIdentity.peerID)
	clientTransport := &quic.Transport{Conn: testPacketConn(t)}
	serverTransport := &quic.Transport{Conn: testPacketConn(t)}
	t.Cleanup(func() { _ = clientTransport.Close() })
	t.Cleanup(func() { _ = serverTransport.Close() })
	listener, err := serverTransport.Listen(serverTLS, &quic.Config{MaxIncomingUniStreams: incomingUni})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	clientRaw, err := clientTransport.Dial(ctx, serverTransport.Conn.LocalAddr(), clientTLS, &quic.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clientRaw.CloseWithError(0, "test done") })
	serverRaw, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = serverRaw.CloseWithError(0, "test done") })
	return NewQUICConn(clientRaw, serverIdentity.peerID), NewQUICConn(serverRaw, clientIdentity.peerID)
}
