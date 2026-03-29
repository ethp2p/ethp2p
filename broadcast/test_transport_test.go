package broadcast

import (
	"context"
	"io"
	"time"

	"github.com/ethp2p/ethp2p/transport"
)

// testTransport implements transport.Conn for in-process testing.
type testTransport struct {
	streamSend chan []byte
	streamRecv chan []byte
	dataSend   chan []byte
	dataRecv   chan []byte
	ctx        context.Context
}

func newTestTransportPair(ctx context.Context) (*testTransport, *testTransport) {
	// 256 slots: enough for high-throughput tests where all testStreams
	// share one channel per direction and the receive side is not drained.
	stream1to2 := make(chan []byte, 256)
	stream2to1 := make(chan []byte, 256)
	data1to2 := make(chan []byte, 256)
	data2to1 := make(chan []byte, 256)

	t1 := &testTransport{
		streamSend: stream1to2,
		streamRecv: stream2to1,
		dataSend:   data1to2,
		dataRecv:   data2to1,
		ctx:        ctx,
	}
	t2 := &testTransport{
		streamSend: stream2to1,
		streamRecv: stream1to2,
		dataSend:   data2to1,
		dataRecv:   data1to2,
		ctx:        ctx,
	}
	return t1, t2
}

func newHighCapTransport(ctx context.Context) *testTransport {
	return &testTransport{
		streamSend: make(chan []byte, 4096),
		streamRecv: make(chan []byte, 4096),
		dataSend:   make(chan []byte, 4096),
		dataRecv:   make(chan []byte, 4096),
		ctx:        ctx,
	}
}

func (t *testTransport) SupportsStreams() bool              { return true }
func (t *testTransport) SupportsDatagrams() bool            { return true }
func (t *testTransport) Close() error                       { return nil }
func (t *testTransport) ConnectionStats() (uint64, uint64)  { return 0, 0 }
func (t *testTransport) Direction() transport.ConnDirection { return transport.Outbound }

func (t *testTransport) OpenStream(ctx context.Context) (transport.Stream, error) {
	return &testStream{send: t.streamSend, recv: t.streamRecv, ctx: t.ctx}, nil
}

func (t *testTransport) AcceptStream(ctx context.Context) (transport.Stream, error) {
	return &testStream{send: t.streamSend, recv: t.streamRecv, ctx: t.ctx}, nil
}

func (t *testTransport) OpenUniStream(ctx context.Context) (transport.SendStream, error) {
	return &testStream{send: t.streamSend, recv: t.streamRecv, ctx: t.ctx}, nil
}

func (t *testTransport) AcceptUniStream(ctx context.Context) (transport.ReceiveStream, error) {
	return &testStream{send: t.streamSend, recv: t.streamRecv, ctx: t.ctx}, nil
}

func (t *testTransport) SendDatagram(ctx context.Context, data []byte) error {
	select {
	case t.dataSend <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-t.ctx.Done():
		return t.ctx.Err()
	}
}

func (t *testTransport) RecvDatagram(ctx context.Context) ([]byte, error) {
	select {
	case data := <-t.dataRecv:
		return data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.ctx.Done():
		return nil, t.ctx.Err()
	}
}

var _ transport.Conn = (*testTransport)(nil)

// testStream implements transport.Stream for in-process testing.
type testStream struct {
	send   chan []byte
	recv   chan []byte
	ctx    context.Context
	buf    []byte
	closed bool
}

func (s *testStream) Read(p []byte) (int, error) {
	if s.closed {
		return 0, io.EOF
	}
	if len(s.buf) > 0 {
		n := copy(p, s.buf)
		s.buf = s.buf[n:]
		return n, nil
	}
	select {
	case data := <-s.recv:
		n := copy(p, data)
		if n < len(data) {
			s.buf = data[n:]
		}
		return n, nil
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	}
}

func (s *testStream) Write(p []byte) (int, error) {
	if s.closed {
		return 0, io.ErrClosedPipe
	}
	cp := make([]byte, len(p))
	copy(cp, p)
	select {
	case s.send <- cp:
		return len(p), nil
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	}
}

func (s *testStream) Close() error {
	s.closed = true
	return nil
}

func (s *testStream) CancelRead(code uint64)             {}
func (s *testStream) CancelWrite(code uint64)            {}
func (s *testStream) Reset() error                       { return nil }
func (s *testStream) SetDeadline(t time.Time) error      { return nil }
func (s *testStream) SetReadDeadline(t time.Time) error  { return nil }
func (s *testStream) SetWriteDeadline(t time.Time) error { return nil }

var _ transport.Stream = (*testStream)(nil)

// newTestBcastStreams returns a pair of streams suitable for use as bcastOut/bcastIn
// in tests where no real control I/O is needed.
func newTestBcastStreams(ctx context.Context) (transport.SendStream, transport.ReceiveStream) {
	ch := make(chan []byte, 256)
	out := &testStream{send: ch, recv: ch, ctx: ctx}
	in := &testStream{send: ch, recv: ch, ctx: ctx}
	return out, in
}

// registerTestPeer constructs a PeerConn and injects it into the Engine via
// onPeerHandshake, which is the real event loop pathway.
func registerTestPeer(e *Engine, id PeerID, conn transport.Conn, version ProtocolVersion, channels []ChannelID) {
	bcastOut, bcastIn := newTestBcastStreams(e.ctx)
	p := &PeerConn{
		id:      id,
		version: version,
		conn:    conn,
		ctrlOut: bcastOut,
		ctrlIn:  bcastIn,
		ctrlQ:   make(chan peerCtrlEvent, ctrlQCap),
		wakeCh:  make(chan struct{}, 1),
		engine:  e,
	}
	p.ctx, p.cancel = context.WithCancel(e.ctx)
	slotCh := make(chan slotUpdate, slotUpdateCap)
	go p.runCtrlLoop(slotCh)
	go p.runDataLoop(slotCh)
	e.onPeerHandshake(p, channels, nil)
}
