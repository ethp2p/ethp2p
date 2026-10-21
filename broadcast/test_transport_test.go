package broadcast

import (
	"bytes"
	"context"
	"io"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/wire"
)

// testTransport provides in-process unidirectional streams for peer tests.
type testTransport struct {
	streamSend chan []byte
	streamRecv chan []byte
	ctx        context.Context
}

func newHighCapTransport(ctx context.Context) *testTransport {
	return &testTransport{
		streamSend: make(chan []byte, 4096),
		streamRecv: make(chan []byte, 4096),
		ctx:        ctx,
	}
}

func (t *testTransport) OpenUniStream(ctx context.Context) (ethp2p.SendStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stream := &testStream{send: t.streamSend, recv: t.streamRecv, ctx: t.ctx}
	if err := wire.WriteHeader(stream, Protocol); err != nil {
		stream.CancelWrite(wire.Unspecified)
		return nil, err
	}
	return stream, nil
}

func (*testTransport) Close(wire.Code) {}

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

func (s *testStream) CancelRead(wire.Code)               {}
func (s *testStream) CancelWrite(wire.Code)              {}
func (s *testStream) SetDeadline(t time.Time) error      { return nil }
func (s *testStream) SetReadDeadline(t time.Time) error  { return nil }
func (s *testStream) SetWriteDeadline(t time.Time) error { return nil }

var _ ethp2p.Stream = (*testStream)(nil)

// newTestBcastStreams returns a pair of streams suitable for use as ctrlOut/ctrlIn
// in tests where no real control I/O is needed.
func newTestBcastStreams(ctx context.Context) (ethp2p.SendStream, ethp2p.ReceiveStream) {
	ch := make(chan []byte, 256)
	out := &testStream{send: ch, recv: ch, ctx: ctx}
	in := &testStream{send: ch, recv: ch, ctx: ctx}
	return out, in
}
