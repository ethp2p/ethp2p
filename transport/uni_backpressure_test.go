package transport

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
)

// testProfile returns Interop with the incoming unidirectional stream limit set
// explicitly. These tests drive the sender past the point where QUIC credit is
// exhausted, which needs a limit the test controls: the delivery buffer between
// acceptance and hand-over holds maxStreamsPendingDelivery (8) streams, so
// the limit must exceed that for the senders to fill it and block.
func testProfile(incomingUni int64) Profile {
	profile := Interop()
	profile.maxIncomingUniStreams = incomingUni
	return profile
}

// sharedPair is one ethp2p connection between two shared endpoints.
//
// The server keeps both its views because a shared connection closes only when
// both views are released, and only that close is sent to the peer. A
// libp2pConn.CloseWithError plus an ethp2pConn.Close is what puts a
// CONNECTION_CLOSE on the wire; closing the endpoint does not, because quic-go
// destroys its connections locally without notifying the peer.
type sharedPair struct {
	client    Conn
	server    Conn
	serverLib quicreuse.QUICConn
}

// closeServer releases both server views, which closes the physical connection
// and notifies the peer.
func (p sharedPair) closeServer(reason string) error {
	// Release the libp2p view first. Closing the ethp2p view while libp2p
	// still holds the connection resets its queued streams, which returns
	// stream credit to the client; as the last release it closes the
	// connection before that reset.
	return errors.Join(
		p.serverLib.CloseWithError(0, reason),
		p.server.Close(),
	)
}

func newSharedPair(t *testing.T, incomingUni int64) sharedPair {
	t.Helper()
	ctx := testContext(t)
	_, _, clientEth, _ := newEndpointWith(t, Interop())
	_, serverLib, serverEth, serverPC := newEndpointWith(t, testProfile(incomingUni))
	listener := listen(t, serverLib, serverEth)

	type acceptResult struct {
		conn Conn
		err  error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		conn, err := serverEth.Accept(ctx)
		accepted <- acceptResult{conn: conn, err: err}
	}()

	client, err := clientEth.Dial(ctx, serverPC.LocalAddr(), serverEth.shared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	result := <-accepted
	if result.err != nil {
		t.Fatal(result.err)
	}
	serverLibConn, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return sharedPair{client: client, server: result.conn, serverLib: serverLibConn}
}

func TestUnidirectionalBackpressure(t *testing.T) {
	for _, resume := range []bool{true, false} {
		name := "close"
		if resume {
			name = "resume"
		}
		t.Run(name, func(t *testing.T) {
			ctx := testContext(t)
			// The incoming limit must exceed the delivery buffer so the test can
			// reach the point where QUIC credit is exhausted.
			const streams = 32
			pair := newSharedPair(t, streams)

			for i := range streams {
				stream, err := pair.client.OpenUniStream(ctx)
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
			_, err := pair.client.OpenUniStream(waitCtx)
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("open beyond unread stream limit = %v, want deadline exceeded", err)
			}

			if !resume {
				opened := make(chan error, 1)
				go func() {
					_, err := pair.client.OpenUniStream(ctx)
					opened <- err
				}()
				// Closing the connection must unblock a sender waiting on stream
				// credit. Both views are released so the close reaches the peer.
				if err := pair.closeServer("test done"); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-opened:
					if err == nil || errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("blocked open after endpoint close = %v, want connection error", err)
					}
				case <-ctx.Done():
					t.Fatal("endpoint close did not unblock sender")
				}
				return
			}

			for i := range streams {
				stream, err := pair.server.AcceptUniStream(ctx)
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
			stream, err := pair.client.OpenUniStream(ctx)
			if err != nil {
				t.Fatalf("consumption did not restore stream credit: %v", err)
			}
			stream.CancelWrite(0)
		})
	}
}
