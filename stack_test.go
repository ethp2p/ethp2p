package ethp2p

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/protocol"
	"github.com/ethp2p/ethp2p/transport"
)

func TestStackInitializesBroadcast(t *testing.T) {
	stack := Stack{
		PeerID: "local",
		Key:    testKey{},
	}
	if err := stack.Init(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stack.Close() })
	if stack.BroadcastEngine() == nil {
		t.Fatal("broadcast engine is nil")
	}
}

func TestStackCloseZeroValue(t *testing.T) {
	var stack Stack
	if err := stack.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stack.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestProtocolTerminationClosesConnection(t *testing.T) {
	stack := Stack{PeerID: "local", Key: testKey{}}
	if err := stack.Init(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stack.Close() })

	conn := newBlockingConn()
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- stack.ServeConn(t.Context(), conn)
	}()

	select {
	case <-conn.closed:
	case <-time.After(time.Second):
		t.Fatal("protocol termination did not close the connection")
	}
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("ServeConn did not return after protocol termination")
	}
}

func TestRouteStream(t *testing.T) {
	const codepoint protocol.Codepoint = 300
	const payload = "\x00/protocol payload"
	checkPayload := func(stream io.Reader) {
		t.Helper()
		got, err := io.ReadAll(stream)
		if err != nil || string(got) != payload {
			t.Fatalf("handler payload = %q, error = %v", got, err)
		}
	}
	var acceptedBi, acceptedUni bool
	routes := testRoutes(t, protocol.Handlers{
		AcceptBi: func(stream transport.Stream) {
			checkPayload(stream)
			stream.CancelRead(0)
			acceptedBi = true
		},
		AcceptUni: func(stream transport.ReceiveStream) {
			checkPayload(stream)
			stream.CancelRead(0)
			acceptedUni = true
		},
	})

	bi := newRouteStream(codepoint, payload)
	routeBi(bi, routes)
	if !acceptedBi || bi.reset || !bi.canceled {
		t.Fatal("known bidirectional stream was not accepted")
	}

	uni := newRouteStream(codepoint, payload)
	routeUni(uni, routes)
	if !acceptedUni || !uni.canceled {
		t.Fatal("known unidirectional stream was not accepted")
	}
}

func TestRouteStreamRejectsInvalidRoutes(t *testing.T) {
	const codepoint protocol.Codepoint = 300
	uniOnly := testRoutes(t, protocol.Handlers{
		AcceptUni: func(transport.ReceiveStream) {},
	})

	wrongKind := newRouteStream(codepoint)
	routeBi(wrongKind, uniOnly)
	if !wrongKind.reset {
		t.Fatal("bidirectional stream for a unidirectional protocol was not reset")
	}

	unknown := newRouteStream(codepoint + 1)
	routeUni(unknown, uniOnly)
	if !unknown.canceled {
		t.Fatal("unknown protocol stream was not canceled")
	}

	malformed := &routeStream{Reader: bytes.NewReader([]byte{0x80})}
	routeUni(malformed, uniOnly)
	if !malformed.canceled {
		t.Fatal("malformed protocol stream was not canceled")
	}
}

func testRoutes(t *testing.T, handlers protocol.Handlers) *protocol.Routes {
	t.Helper()
	const codepoint protocol.Codepoint = 300
	var registry protocol.Registry
	err := registry.Register(protocol.Set{
		Descriptors: []protocol.Descriptor{{Codepoint: codepoint, Name: "test"}},
		Bind: func(context.Context, transport.Conn) (protocol.ConnectionHandlers, error) {
			return protocol.ConnectionHandlers{
				ByCodepoint: map[protocol.Codepoint]protocol.Handlers{codepoint: handlers},
			}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	routes, err := registry.Bind(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(routes.Close)
	return routes
}

type routeStream struct {
	*bytes.Reader
	canceled bool
	reset    bool
}

func newRouteStream(codepoint protocol.Codepoint, payload ...string) *routeStream {
	selector := binary.AppendUvarint(nil, uint64(codepoint))
	for _, data := range payload {
		selector = append(selector, data...)
	}
	return &routeStream{Reader: bytes.NewReader(selector)}
}

func (s *routeStream) Write(p []byte) (int, error)      { return len(p), nil }
func (s *routeStream) Close() error                     { return nil }
func (s *routeStream) CancelRead(uint64)                { s.canceled = true }
func (s *routeStream) CancelWrite(uint64)               {}
func (s *routeStream) Reset() error                     { s.reset = true; return nil }
func (s *routeStream) SetDeadline(time.Time) error      { return nil }
func (s *routeStream) SetReadDeadline(time.Time) error  { return nil }
func (s *routeStream) SetWriteDeadline(time.Time) error { return nil }

type testKey struct{}

func (testKey) Sign([]byte) ([]byte, error) {
	return nil, nil
}

var _ PrivKey = testKey{}

type blockingConn struct {
	closed    chan struct{}
	closeOnce sync.Once
}

func newBlockingConn() *blockingConn {
	return &blockingConn{closed: make(chan struct{})}
}

func (c *blockingConn) OpenStream(context.Context) (transport.Stream, error) {
	return nil, errors.New("not supported")
}

func (c *blockingConn) AcceptBiStream(ctx context.Context) (transport.Stream, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, errors.New("connection closed")
	}
}

func (c *blockingConn) OpenUniStream(context.Context) (transport.SendStream, error) {
	return nil, errors.New("not supported")
}

func (c *blockingConn) AcceptUniStream(ctx context.Context) (transport.ReceiveStream, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, errors.New("connection closed")
	}
}

func (c *blockingConn) SendDatagram(context.Context, []byte) error {
	return errors.ErrUnsupported
}

func (c *blockingConn) RecvDatagram(context.Context) ([]byte, error) {
	return nil, errors.ErrUnsupported
}

func (c *blockingConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (*blockingConn) SupportsStreams() bool {
	return true
}

func (*blockingConn) SupportsDatagrams() bool {
	return false
}

func (*blockingConn) ConnectionStats() (uint64, uint64) {
	return 0, 0
}

func (*blockingConn) Direction() transport.ConnDir {
	return transport.ConnDirIn
}

func (*blockingConn) AuthInfo() transport.AuthInfo {
	return transport.AuthInfo{}
}

func (*blockingConn) RemoteKey() *transport.PubKey { return nil }

var _ transport.Conn = (*blockingConn)(nil)
