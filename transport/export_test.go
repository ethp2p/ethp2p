package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/wire"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/quic-go/quic-go"
)

// ConfigWithIncomingUniStreams returns the Interop config with its incoming
// unidirectional stream limit set to limit.
func ConfigWithIncomingUniStreams(limit int64) Config {
	p := Interop()
	p.maxIncomingUniStreams = limit
	return p
}

// RawOpenStream bypasses the ethp2p view to exercise malformed stream heads.
func RawOpenStream(ctx context.Context, conn *Conn) (*quic.Stream, error) {
	return conn.conn.OpenStreamSync(ctx)
}

// PendingEthp2p reports queued ethp2p connection views for external tests.
func PendingEthp2p(t *Endpoint) int { return len(t.ethQ) }

// QueuedLibp2pStreams reports the classified streams a shared connection's
// libp2p view holds for AcceptStream.
func QueuedLibp2pStreams(conn quicreuse.QUICConn) int {
	q := (*Conn)(conn.(*libp2pConn)).libp2pStreams
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// AssertEthp2pViewClosed checks the released ethp2p sibling of a libp2p view.
// An overloaded view is never delivered through Ethp2pTransport.Accept, so
// external tests inspect its cause through the surviving sibling here.
func AssertEthp2pViewClosed(t *testing.T, conn quicreuse.QUICConn, code wire.Code, remote bool) {
	t.Helper()
	ctx := (*Conn)(conn.(*libp2pConn)).ethp2pCtx
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("ethp2p sibling did not close")
	}
	err := context.Cause(ctx)
	closed, ok := errors.AsType[*ViewClosedError](err)
	if !ok || closed.Code != code || closed.Remote != remote || !errors.Is(err, ErrViewClosed) {
		t.Fatalf("ethp2p sibling cause = %v, want {%s %t}", err, code, remote)
	}
}
