package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/wire"
	"github.com/quic-go/quic-go"
)

func rawBi(t *testing.T, ctx context.Context, conn *Conn, head []byte) *quic.Stream {
	t.Helper()
	s, err := conn.conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(head); err != nil {
		t.Fatal(err)
	}
	return s
}

func assertBiReset(t *testing.T, s *quic.Stream, want uint64) {
	t.Helper()
	if err := s.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err := s.Read(make([]byte, 1))
	reset, ok := errors.AsType[*quic.StreamError](err)
	if !ok || !reset.Remote || uint64(reset.ErrorCode) != want {
		t.Fatalf("peer reset = %v, want remote wire %d", err, want)
	}
}

// A bidirectional stream goes to libp2p when its first byte is 0x13 and to
// ethp2p otherwise, with nothing consumed either way: each owner reads its own
// header.
func TestBidirectionalClassification(t *testing.T) {
	for _, head := range [][]byte{{0x13, '/'}, {0x13, 'x'}, {0x13}} {
		t.Run(fmt.Sprintf("libp2p-%x", head), func(t *testing.T) {
			ctx := testContext(t)
			pair := newViewPair(t, 32)
			out := rawBi(t, ctx, pair.clientEth, head)
			in, err := pair.serverLib.AcceptStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(head))
			if _, err := io.ReadFull(in, got); err != nil || !bytes.Equal(got, head) {
				t.Fatalf("libp2p head = %x, %v", got, err)
			}
			out.CancelRead(0)
		})
	}
	// Any other first byte is ethp2p, even an invalid stream header: the stack
	// reads and validates it.
	for _, head := range [][]byte{{0x01, 0x01, 0xaa}, {0x80, 0x00, 0x01}, {0x14}} {
		t.Run(fmt.Sprintf("ethp2p-%x", head), func(t *testing.T) {
			ctx := testContext(t)
			pair := newViewPair(t, 32)
			out := rawBi(t, ctx, pair.clientEth, head)
			in := nextStream(t, pair.serverEth)
			if _, ok := any(in).(*quic.Stream); !ok {
				t.Fatalf("stream %T is not bidirectional", in)
			}
			got := make([]byte, len(head))
			if _, err := io.ReadFull(in, got); err != nil || !bytes.Equal(got, head) {
				t.Fatalf("ethp2p head = %x, %v", got, err)
			}
			out.CancelRead(0)
		})
	}
}

func TestBidirectionalClassificationFailures(t *testing.T) {
	t.Run("no first byte", func(t *testing.T) {
		ctx := testContext(t)
		config := ConfigWithIncomingUniStreams(32)
		config.classifyTimeout = 50 * time.Millisecond
		pair := newViewPairWithConfig(t, config)
		// quic-go sends nothing for an unwritten stream, but data on a later
		// stream opens every earlier one (RFC 9000, section 2.1).
		empty, err := pair.clientEth.conn.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rawBi(t, ctx, pair.clientEth, []byte{0x01, 0x01})
		assertBiReset(t, empty, wire.Timeout.Wire())
	})
	t.Run("FIN before any byte", func(t *testing.T) {
		ctx := testContext(t)
		pair := newViewPair(t, 32)
		out, err := pair.clientEth.conn.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_ = out.Close()
		assertBiReset(t, out, wire.BadHeader.Wire())
	})
	t.Run("peer reset", func(t *testing.T) {
		ctx := testContext(t)
		pair := newViewPair(t, 32)
		out, err := pair.clientEth.conn.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		out.CancelWrite(quic.StreamErrorCode(42))
		assertBiReset(t, out, wire.Unspecified.Wire())
	})
}

// Unidirectional streams reach the queue unread and in stream ID order, even
// when a later stream's bytes arrive first: the stack takes the first one as
// the peer's control stream.
func TestUnidirectionalStreamsInIDOrder(t *testing.T) {
	ctx := testContext(t)
	pair := newViewPair(t, 32)
	first, err := pair.clientEth.conn.OpenUniStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := pair.clientEth.conn.OpenUniStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Write([]byte{2}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	for want := byte(1); want <= 2; want++ {
		in := nextUniStream(t, pair.serverEth)
		var b [1]byte
		if _, err := io.ReadFull(in, b[:]); err != nil || b[0] != want {
			t.Fatalf("stream %d first byte = %x, %v", want, b, err)
		}
	}
}
