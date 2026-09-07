package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

func TestClassifyPreservesStream(t *testing.T) {
	tests := []struct {
		name       string
		wire       []byte
		wantLibp2p bool
	}{
		{
			name:       "libp2p",
			wire:       frame([]byte("/multistream/1.0.0\n")),
			wantLibp2p: true,
		},
		{
			name: "ethp2p",
			wire: append(frame([]byte{0xac, 0x02}), 'x'),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stream := &memoryStream{data: test.wire}
			gotEthp2p, err := classify(t.Context(), stream)
			if err != nil {
				t.Fatal(err)
			}
			wantEthp2p := !test.wantLibp2p
			if gotEthp2p != wantEthp2p {
				t.Fatalf("ethp2p route = %t, want %t", gotEthp2p, wantEthp2p)
			}
			unread, err := io.ReadAll(stream)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(unread, test.wire) {
				t.Fatalf("unread bytes = %x, want original %x", unread, test.wire)
			}
		})
	}
}

func TestClassifyRejectsMalformedFrames(t *testing.T) {
	for _, wire := range [][]byte{
		{0},
		{0x80, 0},
		{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80},
	} {
		stream := &memoryStream{data: wire}
		if _, err := classify(t.Context(), stream); err == nil {
			t.Fatalf("malformed frame %x was accepted", wire)
		}
	}
}

func TestClassifyCancellationUnblocksPeek(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	stream := newBlockingStream()
	done := make(chan error, 1)
	go func() {
		_, err := classify(ctx, stream)
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("route returned no cancellation error")
		}
	case <-time.After(time.Second):
		t.Fatal("route remained blocked after cancellation")
	}
}

type memoryStream struct {
	data []byte
	off  int
}

func (s *memoryStream) Peek(dst []byte) (int, error) {
	n := copy(dst, s.data[s.off:])
	if n < len(dst) {
		return n, io.EOF
	}
	return n, nil
}

func (s *memoryStream) Read(dst []byte) (int, error) {
	if s.off == len(s.data) {
		return 0, io.EOF
	}
	n := copy(dst, s.data[s.off:])
	s.off += n
	return n, nil
}

func (*memoryStream) CancelRead(quic.StreamErrorCode) {}

func (*memoryStream) SetReadDeadline(time.Time) error { return nil }

type blockingStream struct {
	canceled chan struct{}
}

func newBlockingStream() *blockingStream {
	return &blockingStream{canceled: make(chan struct{})}
}

func (s *blockingStream) Peek([]byte) (int, error) {
	<-s.canceled
	return 0, context.Canceled
}

func (*blockingStream) Read([]byte) (int, error) { return 0, errors.New("unexpected read") }

func (s *blockingStream) CancelRead(quic.StreamErrorCode) {
	select {
	case <-s.canceled:
	default:
		close(s.canceled)
	}
}

func (*blockingStream) SetReadDeadline(time.Time) error { return nil }
