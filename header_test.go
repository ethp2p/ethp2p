package ethp2p

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/quic-go/quic-go"
)

func shortenClassifyTimeout(t *testing.T, timeout time.Duration) {
	t.Helper()
	pendingHeaderTimeout = timeout
}

// headerReads reports the header reads in progress in direction d of id's
// view, read on its supervisor's goroutine.
func headerReads(t *testing.T, s *Stack, id identity.PeerID, d int) int {
	t.Helper()
	s.mu.Lock()
	sup := s.peers[id]
	s.mu.Unlock()
	got := make(chan int, 1)
	if sup == nil || !sup.post(func() { got <- sup.reading[d] }) {
		t.Fatal("no supervisor for peer")
	}
	return <-got
}

func waitHeaderReads(t *testing.T, s *Stack, id identity.PeerID, d, n int) {
	t.Helper()
	waitFor(t, func() bool { return headerReads(t, s, id, d) == n })
}

func (p rawControlPeer) rawBi(t *testing.T, head []byte) *quic.Stream {
	t.Helper()
	s, err := p.raw.OpenStreamSync(transporttest.Context(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(head); err != nil {
		t.Fatal(err)
	}
	return s
}

func (p rawControlPeer) rawUni(t *testing.T, head []byte) *quic.SendStream {
	t.Helper()
	s, err := p.raw.OpenUniStreamSync(transporttest.Context(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(head); err != nil {
		t.Fatal(err)
	}
	return s
}

func assertBiReset(t *testing.T, s *quic.Stream, want wire.Code) {
	t.Helper()
	if err := s.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err := s.Read(make([]byte, 1))
	reset, ok := errors.AsType[*quic.StreamError](err)
	if !ok || !reset.Remote || uint64(reset.ErrorCode) != want.Wire() {
		t.Fatalf("peer reset = %v, want remote wire %d", err, want.Wire())
	}
}

func assertUniReset(t *testing.T, s *quic.SendStream, want wire.Code) {
	t.Helper()
	select {
	case <-s.Context().Done():
		reset, ok := errors.AsType[*quic.StreamError](context.Cause(s.Context()))
		if !ok || !reset.Remote || uint64(reset.ErrorCode) != want.Wire() {
			t.Fatalf("peer reset = %v, want remote wire %d", context.Cause(s.Context()), want.Wire())
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("peer did not reset uni stream with wire %d", want.Wire())
	}
}

// The header read consumes exactly the header: the payload after it reaches
// the protocol intact, in both directions.
func TestHeaderReadLeavesPayload(t *testing.T) {
	p := newHelloedRawControlPeer(t)
	head := wire.AppendHeader(nil, 1)
	_ = p.rawUni(t, append(slices.Clone(head), "uni payload"...)).Close()
	_ = p.rawBi(t, append(slices.Clone(head), "bi payload"...)).Close()
	got := make(map[bool]string)
	for range 2 {
		ev := awaitStreamEvent(t, p.prot, p.wake)
		payload, err := io.ReadAll(ev.Stream)
		if err != nil {
			t.Fatal(err)
		}
		_, bidi := ev.Stream.(Stream)
		got[bidi] = string(payload)
	}
	if got[false] != "uni payload" || got[true] != "bi payload" {
		t.Fatalf("payloads = %v", got)
	}
}

func TestHeaderErrors(t *testing.T) {
	for _, tc := range []struct {
		name       string
		head       []byte
		fin        bool
		noUniReset bool
		recover    bool
		want       wire.Code
		timeout    time.Duration
	}{
		{"nonminimal-protocol", []byte{0x81, 0x00}, false, false, false, wire.BadHeader, 0},
		{"protocol-above-32-bits", []byte{0xff, 0xff, 0xff, 0xff, 0x1f}, false, false, false, wire.BadHeader, 0},
		{"truncated-by-fin", []byte{0x80}, true, true, true, wire.BadHeader, 0},
		{"slowloris", []byte{0x80}, false, false, true, wire.Timeout, 50 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.timeout != 0 {
				shortenClassifyTimeout(t, tc.timeout)
			}
			p := newHelloedRawControlPeer(t)
			uni := p.rawUni(t, tc.head)
			bi := p.rawBi(t, tc.head)
			if tc.fin {
				_ = uni.Close()
				_ = bi.Close()
			}
			// Closing the uni sender cancels its context locally, and once the
			// receiver reads FIN, quic-go sends no STOP_SENDING for it.
			if !tc.noUniReset {
				assertUniReset(t, uni, tc.want)
			}
			assertBiReset(t, bi, tc.want)
			if ev, ok := p.prot.Next(); ok {
				t.Fatalf("invalid header delivered protocol event: %+v", ev)
			}
			if !tc.recover {
				return
			}

			// Header rejection must leave this admitted view usable in both
			// directions, including after a timeout or a truncated FIN.
			head := wire.AppendHeader(nil, 1)
			if err := p.rawUni(t, append(slices.Clone(head), "uni recovery payload"...)).Close(); err != nil {
				t.Fatal(err)
			}
			if err := p.rawBi(t, append(slices.Clone(head), "bi recovery payload"...)).Close(); err != nil {
				t.Fatal(err)
			}
			got := make(map[bool]string)
			for range 2 {
				ev := awaitStreamEvent(t, p.prot, p.wake)
				if ev.Peer != p.peer {
					t.Fatalf("recovery event = %+v, want original peer", ev)
				}
				payload, err := io.ReadAll(ev.Stream)
				if err != nil {
					t.Fatal(err)
				}
				_, bidi := ev.Stream.(Stream)
				got[bidi] = string(payload)
			}
			if got[false] != "uni recovery payload" || got[true] != "bi recovery payload" {
				t.Fatalf("recovery payloads = %v", got)
			}
			if ev, ok := p.prot.Next(); ok {
				t.Fatalf("unexpected protocol event after recovery: %+v", ev)
			}
		})
	}
	// libp2p opens no unidirectional streams, so protocol 19 there is just an
	// invalid header.
	t.Run("multistream-head", func(t *testing.T) {
		p := newHelloedRawControlPeer(t)
		assertUniReset(t, p.rawUni(t, []byte{0x13, '/'}), wire.BadHeader)
	})
}

// A peer that resets a bidirectional stream before its header gets the write
// side reset with Unspecified.
func TestHeaderPeerResetOnBidirectionalStream(t *testing.T) {
	p := newHelloedRawControlPeer(t)
	out := p.rawBi(t, []byte{0x80})
	out.CancelWrite(quic.StreamErrorCode(42))
	assertBiReset(t, out, wire.Unspecified)
}

func TestLaterCompletedHeaderCanBeDeliveredFirst(t *testing.T) {
	p := newHelloedRawControlPeer(t)
	first := p.rawUni(t, nil)
	second := p.rawUni(t, append(wire.AppendHeader(nil, 1), "second"...))
	_ = second.Close()
	ev := awaitStreamEvent(t, p.prot, p.wake)
	if payload, err := io.ReadAll(ev.Stream); err != nil || string(payload) != "second" {
		t.Fatalf("completed payload = %q, %v; want second", payload, err)
	}
	if _, err := first.Write(append(wire.AppendHeader(nil, 1), "first"...)); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()
	ev = awaitStreamEvent(t, p.prot, p.wake)
	if payload, err := io.ReadAll(ev.Stream); err != nil || string(payload) != "first" {
		t.Fatalf("first payload = %q, %v; want first", payload, err)
	}
}

// Stalled header reads in one direction do not hold up the other: each
// direction has its own MAX_STREAMS_PENDING_CLASSIFY reads.
func TestHeaderDirectionsAreIndependent(t *testing.T) {
	for _, stalled := range []int{uniDir, bidiDir} {
		name := map[int]string{uniDir: "unidirectional", bidiDir: "bidirectional"}[stalled]
		t.Run(name, func(t *testing.T) {
			shortenClassifyTimeout(t, 2*time.Second)
			p := newHelloedRawControlPeer(t)
			id := p.peer.ID()
			for range maxHeaderReads {
				if stalled == uniDir {
					p.rawUni(t, []byte{0x80})
				} else {
					p.rawBi(t, []byte{0x80})
				}
			}
			waitHeaderReads(t, p.stack, id, stalled, maxHeaderReads)
			head := append(wire.AppendHeader(nil, 1), 0xaa)
			if stalled == uniDir {
				_ = p.rawBi(t, head).Close()
			} else {
				_ = p.rawUni(t, head).Close()
			}
			ev := awaitStreamEvent(t, p.prot, p.wake)
			_, bidi := ev.Stream.(Stream)
			if bidi != (stalled == uniDir) {
				t.Fatalf("delivered bidirectional = %t, want %t", bidi, stalled == uniDir)
			}
			if payload, err := io.ReadAll(ev.Stream); err != nil || !bytes.Equal(payload, []byte{0xaa}) {
				t.Fatalf("payload = %x, %v", payload, err)
			}
		})
	}
}
