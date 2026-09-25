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

// ProfileWithIncomingUniStreams sets the total uni limit, including control.
func ProfileWithIncomingUniStreams(limit int64) Profile {
	p := Interop()
	p.maxIncomingUniStreams = limit
	return p
}

// RawOpenStream bypasses the ethp2p view to exercise malformed stream heads.
func RawOpenStream(ctx context.Context, conn Conn) (*quic.Stream, error) {
	return conn.(*ethp2pConn).conn.OpenStreamSync(ctx)
}

// RawOpenUniStream bypasses the ethp2p view to exercise malformed stream heads.
func RawOpenUniStream(ctx context.Context, conn Conn) (*quic.SendStream, error) {
	return conn.(*ethp2pConn).conn.OpenUniStreamSync(ctx)
}

// ShortenClassifyTimeout applies a test-only timeout until cleanup.
func ShortenClassifyTimeout(t *testing.T, timeout time.Duration) {
	t.Helper()
	previous := classifyTimeout.Swap(int64(timeout))
	t.Cleanup(func() { classifyTimeout.Store(previous) })
}

// ShortenHelloTimeout applies a test-only Hello deadline until cleanup.
func ShortenHelloTimeout(t *testing.T, timeout time.Duration) {
	t.Helper()
	previous := helloTimeout.Swap(int64(timeout))
	t.Cleanup(func() { helloTimeout.Store(previous) })
}

// PendingEthp2p reports queued ethp2p connection views for external tests.
func PendingEthp2p(t *SharedTransport) int { return len(t.ethQ) }

// PendingLibp2p reports queued libp2p connection views for external tests.
func PendingLibp2p(t *SharedTransport) int { return len(t.libQ) }

// AssertEthp2pViewClosed checks the released ethp2p sibling of a libp2p view.
// An overloaded view is never delivered through Ethp2pTransport.Accept, so
// external tests inspect its cause through the surviving sibling here.
func AssertEthp2pViewClosed(t *testing.T, conn quicreuse.QUICConn, code wire.Code, remote bool) {
	t.Helper()
	ctx := (*sharedConn)(conn.(*libp2pConn)).ethp2pCtx
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
