package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/wire"
	"github.com/quic-go/quic-go"
)

func rawBi(t *testing.T, ctx context.Context, conn Conn, head []byte) *quic.Stream {
	t.Helper()
	s, err := conn.(*ethp2pConn).conn.OpenStreamSync(ctx)
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

func TestDispatcherClassification(t *testing.T) {
	for n := 1; n <= 10; n++ {
		sel := wire.Selector(uint64(1) << (7 * (n - 1)))
		t.Run("selector-length-"+string(rune('0'+n)), func(t *testing.T) {
			ctx := testContext(t)
			pair := newViewPair(t, 32)
			head := wire.AppendFrame(nil, binary.AppendUvarint(nil, uint64(sel)))
			out := rawBi(t, ctx, pair.clientEth, append(head, 0xaa))
			in, got, err := pair.serverEth.AcceptStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if got != sel {
				t.Fatalf("selector = %d, want %d", got, sel)
			}
			var payload [1]byte
			if _, err := io.ReadFull(in, payload[:]); err != nil || payload[0] != 0xaa {
				t.Fatalf("payload = %x, %v", payload, err)
			}
			out.CancelRead(0)
		})
	}
	t.Run("selector-slash", func(t *testing.T) {
		ctx := testContext(t)
		pair := newViewPair(t, 32)
		out := rawBi(t, ctx, pair.clientEth, []byte{1, 0x2f, 0xaa})
		in, sel, err := pair.serverEth.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if sel != 0x2f {
			t.Fatalf("selector = %d, want 47", sel)
		}
		var b [1]byte
		if _, err := io.ReadFull(in, b[:]); err != nil || b[0] != 0xaa {
			t.Fatalf("payload = %x, %v", b, err)
		}
		out.CancelRead(0)
	})
	t.Run("multistream-head-preserved", func(t *testing.T) {
		ctx := testContext(t)
		pair := newViewPair(t, 32)
		out := rawBi(t, ctx, pair.clientEth, []byte{0x13, '/'})
		in, err := pair.serverLib.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var head [2]byte
		if _, err := io.ReadFull(in, head[:]); err != nil || !bytes.Equal(head[:], []byte{0x13, '/'}) {
			t.Fatalf("libp2p head = %x, %v", head, err)
		}
		out.CancelRead(0)
	})
	for _, tc := range []struct {
		name string
		head []byte
	}{
		{"zero", []byte{0, 'x'}},
		{"eleven", []byte{11, 'x'}},
		{"multistream-prefix-without-slash", []byte{0x13, 'x'}},
		{"nonminimal-selector", []byte{2, 0x81, 0}},
		{"overlong-selector-frame", []byte{11, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testContext(t)
			pair := newViewPair(t, 32)
			out := rawBi(t, ctx, pair.clientEth, tc.head)
			assertBiReset(t, out, 16)
		})
	}
}

func TestDispatcherSlowloris(t *testing.T) {
	ShortenClassifyTimeout(t, 50*time.Millisecond)
	ctx := testContext(t)
	pair := newViewPair(t, 32)
	out := rawBi(t, ctx, pair.clientEth, []byte{2})
	assertBiReset(t, out, 6)
}

func TestDispatcherAnswersPeerResetOnBidirectionalStream(t *testing.T) {
	ctx := testContext(t)
	pair := newViewPair(t, 32)
	out := rawBi(t, ctx, pair.clientEth, []byte{2})
	out.CancelWrite(quic.StreamErrorCode(42))
	assertBiReset(t, out, 0)
}

func assertUniReset(t *testing.T, s *quic.SendStream, want uint64) {
	t.Helper()
	select {
	case <-s.Context().Done():
		reset, ok := errors.AsType[*quic.StreamError](context.Cause(s.Context()))
		if !ok || !reset.Remote || uint64(reset.ErrorCode) != want {
			t.Fatalf("peer reset = %v, want remote wire %d", context.Cause(s.Context()), want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("peer did not reset uni stream with wire %d", want)
	}
}

func TestUnidirectionalSelectorErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		head    []byte
		want    uint64
		timeout time.Duration
	}{
		{"bad-frame", []byte{2, 0x81, 0}, 16, 0},
		{"nonminimal-length", []byte{0x81, 0x00, 1}, 16, 0},
		{"slowloris", []byte{2}, 6, 50 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.timeout != 0 {
				ShortenClassifyTimeout(t, tc.timeout)
			}
			ctx := testContext(t)
			pair := newViewPair(t, 32)
			out, err := pair.clientEth.(*ethp2pConn).conn.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := out.Write(tc.head); err != nil {
				t.Fatal(err)
			}
			assertUniReset(t, out, tc.want)
		})
	}
}

func TestLaterCompletedUnidirectionalStreamCanBeDeliveredFirst(t *testing.T) {
	ctx := testContext(t)
	pair := newViewPair(t, 32)
	first, err := pair.clientEth.(*ethp2pConn).conn.OpenUniStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	second, err := pair.clientEth.OpenUniStream(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Write([]byte{0xaa}); err != nil {
		t.Fatal(err)
	}
	_, sel, err := pair.serverEth.AcceptUniStream(ctx)
	if err != nil || sel != 1 {
		t.Fatalf("completed selector = %d, %v; want 1", sel, err)
	}
	if _, err := first.Write([]byte{2}); err != nil {
		t.Fatal(err)
	}
	_, sel, err = pair.serverEth.AcceptUniStream(ctx)
	if err != nil || sel != 2 {
		t.Fatalf("first selector = %d, %v; want 2", sel, err)
	}
}

func waitClassifierFull(t *testing.T, sem chan struct{}) {
	t.Helper()
	deadline := time.After(time.Second)
	for len(sem) != 0 {
		select {
		case <-deadline:
			t.Fatal("classifier slots did not fill")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestDispatcherDirectionsAreIndependent(t *testing.T) {
	for _, stalled := range []string{"bidirectional", "unidirectional"} {
		t.Run(stalled, func(t *testing.T) {
			ShortenClassifyTimeout(t, 2*time.Second)
			ctx := testContext(t)
			pair := newViewPair(t, 32)
			server := pair.serverEth.(*ethp2pConn)
			if stalled == "bidirectional" {
				for range maxStreamsPendingClassify {
					rawBi(t, ctx, pair.clientEth, []byte{2})
				}
				waitClassifierFull(t, server.biSem)
				out, err := pair.clientEth.OpenUniStream(ctx, 1)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := out.Write([]byte{0xaa}); err != nil {
					t.Fatal(err)
				}
				acceptCtx, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()
				in, sel, err := pair.serverEth.AcceptUniStream(acceptCtx)
				if err != nil || sel != 1 {
					t.Fatalf("uni acceptance = %d, %v", sel, err)
				}
				var b [1]byte
				if _, err := io.ReadFull(in, b[:]); err != nil || b[0] != 0xaa {
					t.Fatalf("uni payload = %x, %v", b, err)
				}
			} else {
				for range maxStreamsPendingClassify {
					s, err := pair.clientEth.(*ethp2pConn).conn.OpenUniStreamSync(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := s.Write([]byte{2}); err != nil {
						t.Fatal(err)
					}
				}
				waitClassifierFull(t, server.uniSem)
				out, err := pair.clientEth.OpenStream(ctx, 1)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := out.Write([]byte{0xaa}); err != nil {
					t.Fatal(err)
				}
				acceptCtx, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()
				in, sel, err := pair.serverEth.AcceptStream(acceptCtx)
				if err != nil || sel != 1 {
					t.Fatalf("bi acceptance = %d, %v", sel, err)
				}
				var b [1]byte
				if _, err := io.ReadFull(in, b[:]); err != nil || b[0] != 0xaa {
					t.Fatalf("bi payload = %x, %v", b, err)
				}
			}
		})
	}
}
