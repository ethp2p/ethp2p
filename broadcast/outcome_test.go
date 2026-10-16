package broadcast

import (
	"bytes"
	"context"
	"errors"
	"github.com/quic-go/quic-go"
	"io"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
	"github.com/ethp2p/ethp2p/wire"
)

func TestStreamEndPolicyUsesWireCodes(t *testing.T) {
	tests := []struct {
		name string
		want uint64
		run  func(*testing.T) []uint64
	}{
		{
			name: "malformed input",
			want: 0,
			run: func(t *testing.T) []uint64 {
				payload := wire.AppendFrame(nil, headerFrame, []byte{0x0f})
				stream := &recordingReceiveStream{Reader: bytes.NewReader(payload)}
				(&PeerConn{ctx: context.Background()}).processChunk(stream)
				return stream.CancelReadCodes()
			},
		},
		{
			name: "bounded stream queue is full",
			want: 0,
			run: func(t *testing.T) []uint64 {
				stream := &recordingReceiveStream{}
				peer := &PeerConn{ctx: context.Background(), sessionIn: make(chan ethp2p.ReceiveStream, 1)}
				peer.sessionIn <- nil
				peer.enqueueStream(SESS, stream)
				return stream.CancelReadCodes()
			},
		},
		{
			name: "duplicate BCAST is refused",
			want: 2,
			run: func(t *testing.T) []uint64 {
				stream := &recordingReceiveStream{}
				peer := &PeerConn{ctx: context.Background()}
				peer.bcastAccepted = true
				peer.acceptBcast(stream)
				return stream.CancelReadCodes()
			},
		},
		{
			name: "local shutdown",
			want: 0,
			run: func(t *testing.T) []uint64 {
				f := newOutcomeFixture(t)
				raw := f.openIncoming(t, SESS, nil)
				select {
				case <-f.wake:
				case <-time.After(time.Second):
					t.Fatal("stream not queued")
				}
				engine := &Engine{eventCh: make(chan func(), 1)}
				engine.family = f.family
				engine.shutdown()
				return raw.CancelReadCodes()
			},
		},
		{
			name: "sender reconstructed the message",
			want: 3,
			run: func(t *testing.T) []uint64 {
				f := newOutcomeFixture(t)
				stream, err := f.peer.OpenUniStream(f.ctx, SESS)
				if err != nil {
					t.Fatal(err)
				}
				in, _, err := transporttest.NextUniStream(f.ctx, f.conn)
				if err != nil {
					t.Fatal(err)
				}
				raw := &wireSendProbe{in: in}
				key := sessionKey{channelID: "c", messageID: "m"}
				(&PeerConn{}).handleCtrl(peerCloseStream(key), map[sessionKey]ethp2p.SendStream{
					key: stream,
				}, make(chan slotUpdate, 1))
				return raw.CancelWriteCodes()
			},
		},
		{
			name: "receiver no longer needs the chunk",
			want: 5, // Redundant is protocol value 2, wire code 0x05.
			run: func(t *testing.T) []uint64 {
				strat := newMockStrategy()
				strat.haveChunk = true
				inbox := make(chan channelEvent, 1)
				sess := newTestSession(strat, inbox)
				defer sess.Close()
				stream := &recordingReceiveStream{}
				sess.handleChunkStream("p1", []byte("chunk-id"), 0, stream)
				if len(inbox) != 0 {
					t.Fatal("rejected chunk reached the channel")
				}
				return stream.CancelReadCodes()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requireCode(t, test.run(t), test.want)
		})
	}
}

// processChunk arms the real chunkReadTimeout, so this case runs on the
// synctest clock instead of waiting it out.
func TestChunkHeaderReadTimesOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &codeStream{}
		(&PeerConn{ctx: context.Background()}).processChunk(s)
		if len(s.codes) != 1 || s.codes[0] != wire.Timeout {
			t.Fatalf("CancelRead codes = %v, want [Timeout]", s.codes)
		}
	})
}

// codeStream is a receive stream whose peer never sends: Read blocks until
// the read deadline, or forever when none is set.
type codeStream struct {
	deadline time.Time
	codes    []wire.Code
}

func (s *codeStream) Read([]byte) (int, error) {
	if s.deadline.IsZero() {
		select {}
	}
	time.Sleep(time.Until(s.deadline))
	return 0, os.ErrDeadlineExceeded
}

func (s *codeStream) CancelRead(code wire.Code) { s.codes = append(s.codes, code) }

func (s *codeStream) SetReadDeadline(t time.Time) error {
	s.deadline = t
	return nil
}

type recordingReceiveStream struct {
	*bytes.Reader
	codes []wire.Code
}

func (s *recordingReceiveStream) CancelRead(code wire.Code) { s.codes = append(s.codes, code) }

func (*recordingReceiveStream) SetReadDeadline(time.Time) error { return nil }

func (s *recordingReceiveStream) CancelReadCodes() []uint64 {
	codes := make([]uint64, len(s.codes))
	for i, code := range s.codes {
		codes[i] = code.Wire()
	}
	return codes
}

type outcomeFixture struct {
	ctx    context.Context
	conn   *transport.Conn
	peer   *ethp2p.Peer
	family *ethp2p.Family
	wake   chan struct{}
}

func newOutcomeFixture(t *testing.T) *outcomeFixture {
	t.Helper()
	client, server := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	stack, err := ethp2p.NewStack(server.Eth, ethp2p.Config{})
	if err != nil {
		t.Fatal(err)
	}
	wake := make(chan struct{}, 1)
	family, err := stack.Register(Family, []ethp2p.ProtocolSpec{{Protocol: BCAST, MaxQueued: 1}, {Protocol: SESS, MaxQueued: 64}, {Protocol: CHUNK, MaxQueued: 128}}, wake)
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stack.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	raw, err := transporttest.DialHello(ctx, client, server, transporttest.Hello{Families: []wire.Family{Family}})
	if err != nil {
		t.Fatal(err)
	}
	conn := raw.Conn
	up := nextOutcomeEvent(t, family, wake)
	if up.Kind != ethp2p.PeerUp {
		t.Fatal("missing peer")
	}
	select {
	case <-wake:
	default:
	}
	return &outcomeFixture{ctx: ctx, conn: conn, peer: up.Peer, family: family, wake: wake}
}

func (f *outcomeFixture) openIncoming(t *testing.T, protocol wire.Protocol, payload []byte) *wireReceiveProbe {
	t.Helper()
	out, err := transporttest.OpenUniStream(f.ctx, f.conn, wire.Selector{Family: Family, Protocol: protocol})
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) != 0 {
		if _, err := out.Write(payload); err != nil {
			t.Fatal(err)
		}
	}
	return &wireReceiveProbe{out: out}
}
func (f *outcomeFixture) incoming(t *testing.T, protocol wire.Protocol, reader io.Reader) (*wireReceiveProbe, ethp2p.ReceiveStream) {
	t.Helper()
	var payload []byte
	if reader != nil {
		var err error
		payload, err = io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
	}
	raw := f.openIncoming(t, protocol, payload)
	event := nextOutcomeEvent(t, f.family, f.wake)
	if event.Kind != ethp2p.StreamIn || event.Protocol != protocol {
		t.Fatalf("event = %+v", event)
	}
	return raw, event.Stream
}

// These probes observe actual numeric reset codes on the remote QUIC endpoint.
type wireReceiveProbe struct {
	out   *quic.SendStream
	once  sync.Once
	codes []uint64
}

func (p *wireReceiveProbe) CancelReadCodes() []uint64 {
	p.once.Do(func() {
		_ = p.out.SetWriteDeadline(time.Now().Add(5 * time.Second))
		chunk := make([]byte, 64<<10)
		for range 512 {
			_, err := p.out.Write(chunk)
			if err != nil {
				if reset, ok := errors.AsType[*quic.StreamError](err); ok && reset.Remote {
					p.codes = []uint64{uint64(reset.ErrorCode)}
				}
				return
			}
		}
	})
	return p.codes
}

type wireSendProbe struct{ in *quic.ReceiveStream }

func (p *wireSendProbe) CancelWriteCodes() []uint64 {
	_ = p.in.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err := p.in.Read(make([]byte, 1))
	if reset, ok := errors.AsType[*quic.StreamError](err); ok {
		return []uint64{uint64(reset.ErrorCode)}
	}
	return nil
}

func nextOutcomeEvent(t *testing.T, family *ethp2p.Family, wake <-chan struct{}) ethp2p.Event {
	t.Helper()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	for {
		if event, ok := family.Next(); ok {
			return event
		}
		select {
		case <-wake:
		case <-timeout.C:
			t.Fatal("stack did not deliver event")
			return ethp2p.Event{}
		}
	}
}

// requireCode fails unless the transport saw exactly one cancellation, with want.
func requireCode(t *testing.T, got []uint64, want uint64) {
	t.Helper()
	if len(got) != 1 || got[0] != want {
		t.Fatalf("transport cancellation codes = %v, want exactly [%d]", got, want)
	}
}
