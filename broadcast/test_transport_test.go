package broadcast

import (
	"bytes"
	"context"
	"io"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/protocol"
	"github.com/ethp2p/ethp2p/transport"
)

// testTransport provides in-process unidirectional streams for peer tests.
type testTransport struct {
	streamSend chan []byte
	streamRecv chan []byte
	ctx        context.Context
}

func newTestTransportPair(ctx context.Context) (*testTransport, *testTransport) {
	// 256 slots: enough for high-throughput tests where all testStreams
	// share one channel per direction and the receive side is not drained.
	stream1to2 := make(chan []byte, 256)
	stream2to1 := make(chan []byte, 256)
	t1 := &testTransport{
		streamSend: stream1to2,
		streamRecv: stream2to1,
		ctx:        ctx,
	}
	t2 := &testTransport{
		streamSend: stream2to1,
		streamRecv: stream1to2,
		ctx:        ctx,
	}
	return t1, t2
}

func newHighCapTransport(ctx context.Context) *testTransport {
	return &testTransport{
		streamSend: make(chan []byte, 4096),
		streamRecv: make(chan []byte, 4096),
		ctx:        ctx,
	}
}

func (t *testTransport) OpenUniStream(ctx context.Context, selector protocol.Selector) (ethp2p.SendStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stream := &testStream{send: t.streamSend, recv: t.streamRecv, ctx: t.ctx}
	if err := protocol.WriteSelector(stream, selector); err != nil {
		stream.CancelWrite(protocol.Unspecified)
		return nil, err
	}
	return stream, nil
}

func (t *testTransport) AcceptUniStream(context.Context) (ethp2p.ReceiveStream, error) {
	return &testStream{send: t.streamSend, recv: t.streamRecv, ctx: t.ctx}, nil
}

// testStream implements ethp2p.Stream for in-process broadcast testing.
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
	cp := bytes.Clone(p)
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

func (s *testStream) CancelRead(protocol.Code)           {}
func (s *testStream) CancelWrite(protocol.Code)          {}
func (s *testStream) SetDeadline(t time.Time) error      { return nil }
func (s *testStream) SetReadDeadline(t time.Time) error  { return nil }
func (s *testStream) SetWriteDeadline(t time.Time) error { return nil }

var _ ethp2p.Stream = (*testStream)(nil)

// newTestBcastStreams returns a pair of streams suitable for use as bcastOut/bcastIn
// in tests where no real control I/O is needed.
func newTestBcastStreams(ctx context.Context) (ethp2p.SendStream, ethp2p.ReceiveStream) {
	ch := make(chan []byte, 256)
	out := &testStream{send: ch, recv: ch, ctx: ctx}
	in := &testStream{send: ch, recv: ch, ctx: ctx}
	return out, in
}

// registerTestPeer constructs a PeerConn and injects it into the Engine via
// onPeerHandshake, which is the real event loop pathway. The returned binding
// lets callers identify the same peer in a subsequent cleanup event.
func registerTestPeer(e *Engine, id transport.PeerID, streams uniStreamOpener, version ProtocolVersion, channels []ChannelID) *PeerConn {
	bcastOut, bcastIn := newTestBcastStreams(e.ctx)
	p := &PeerConn{
		id:            id,
		version:       version,
		streams:       streams,
		ctrlOut:       bcastOut,
		ctrlIn:        bcastIn,
		ctrlQ:         make(chan peerCtrlEvent, ctrlQCap),
		wakeCh:        make(chan struct{}, 1),
		engine:        e,
		handshakeDone: make(chan struct{}),
	}
	p.ctx, p.cancel = context.WithCancel(e.ctx)
	slotCh := make(chan slotUpdate, slotUpdateCap)
	go p.runCtrlLoop(slotCh)
	go p.runDataLoop(slotCh)
	e.onPeerHandshake(p, channels, nil)
	return p
}
