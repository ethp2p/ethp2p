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
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/wire"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
)

func TestStreamEndPolicyUsesWireCodes(t *testing.T) {
	tests := []struct {
		name     string
		want     uint64
		wantRead bool
		run      func(*testing.T, *outcomeFixture) (*wireReceiveProbe, *wireSendProbe)
	}{
		{
			name:     "deadline expires while reading",
			want:     6,
			wantRead: true,
			run: func(t *testing.T, f *outcomeFixture) (*wireReceiveProbe, *wireSendProbe) {
				raw, stream := f.incoming(t, CHUNK, failReader{os.ErrDeadlineExceeded})
				(&PeerConn{ctx: context.Background()}).processChunk(stream)
				return raw, nil
			},
		},
		{
			name:     "malformed input",
			want:     0,
			wantRead: true,
			run: func(t *testing.T, f *outcomeFixture) (*wireReceiveProbe, *wireSendProbe) {
				payload := wire.AppendFrame(nil, []byte{0x0f})
				raw, stream := f.incoming(t, CHUNK, bytes.NewReader(payload))
				(&PeerConn{ctx: context.Background()}).processChunk(stream)
				return raw, nil
			},
		},
		{
			name:     "bounded stream queue is full",
			want:     4,
			wantRead: true,
			run: func(t *testing.T, f *outcomeFixture) (*wireReceiveProbe, *wireSendProbe) {
				raw, stream := f.incoming(t, SESS, nil)
				peer := &PeerConn{ctx: context.Background(), sessionIn: make(chan ethp2p.ReceiveStream, 1)}
				peer.sessionIn <- nil
				peer.enqueueStream(SESS, stream)
				return raw, nil
			},
		},
		{
			name:     "duplicate BCAST is refused",
			want:     2,
			wantRead: true,
			run: func(t *testing.T, f *outcomeFixture) (*wireReceiveProbe, *wireSendProbe) {
				raw, stream := f.incoming(t, BCAST, nil)
				peer := &PeerConn{ctx: context.Background()}
				peer.bcastAccepted.Store(true)
				peer.acceptBcast(stream)
				return raw, nil
			},
		},
		{
			name:     "local shutdown",
			want:     0,
			wantRead: true,
			run: func(t *testing.T, f *outcomeFixture) (*wireReceiveProbe, *wireSendProbe) {
				raw := f.openIncoming(t, SESS, nil)
				select {
				case <-f.wake:
				case <-time.After(time.Second):
					t.Fatal("stream not queued")
				}
				engine := &Engine{eventCh: make(chan engineEvent, 1)}
				engine.subsystem.Store(f.sub)
				engine.shutdown()
				return raw, nil
			},
		},
		{
			name: "sender reconstructed the message",
			want: 3,
			run: func(t *testing.T, f *outcomeFixture) (*wireReceiveProbe, *wireSendProbe) {
				stream, err := f.peer.OpenUniStream(f.ctx, SESS)
				if err != nil {
					t.Fatal(err)
				}
				in, _, err := f.conn.AcceptUniStream(f.ctx)
				if err != nil {
					t.Fatal(err)
				}
				raw := &wireSendProbe{in: in}
				key := sessionKey{channelID: "c", messageID: "m"}
				(&PeerConn{}).handleCtrl(peerCloseStream{channelID: key.channelID, messageID: key.messageID}, map[sessionKey]*peerSessionState{
					key: {sessOut: stream},
				}, make(chan slotUpdate, 1))
				return nil, raw
			},
		},
		{
			name:     "receiver no longer needs the chunk",
			want:     3,
			wantRead: true,
			run: func(t *testing.T, f *outcomeFixture) (*wireReceiveProbe, *wireSendProbe) {
				strat := newMockStrategy()
				strat.haveChunk = true
				sess := newTestSession(strat, make(chan channelEvent, 1))
				defer sess.Close()
				raw, stream := f.incoming(t, CHUNK, nil)
				sess.handleChunkStream("p1", []byte("chunk-id"), 0, stream)
				return raw, nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newOutcomeFixture(t)
			rawRead, rawWrite := test.run(t, f)
			if test.wantRead {
				assertTransportCodes(t, rawRead.CancelReadCodes(), []uint64{test.want})
				return
			}
			assertTransportCodes(t, rawWrite.CancelWriteCodes(), []uint64{test.want})
		})
	}
}

type outcomeFixture struct {
	ctx  context.Context
	conn transport.Conn
	peer *ethp2p.Peer
	sub  *ethp2p.Subsystem
	wake chan struct{}
}

func newOutcomeFixture(t *testing.T) *outcomeFixture {
	t.Helper()
	client, server := transporttest.NewEndpoint(t), transporttest.NewEndpoint(t)
	stack, err := ethp2p.NewStack(server.Eth, ethp2p.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := stack.Register("broadcast-test", []wire.Selector{BCAST, SESS, CHUNK}, ethp2p.SubsystemConfig{})
	if err != nil {
		t.Fatal(err)
	}
	wake := make(chan struct{}, 1)
	if err := sub.Notify(wake); err != nil {
		t.Fatal(err)
	}
	if err := stack.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stack.Close() })
	if err := client.Eth.SetHello(transport.Hello{Selectors: []wire.Selector{BCAST, SESS, CHUNK}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	conn, err := client.Eth.Dial(ctx, server.Shared.Addr(), server.Shared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	up := nextOutcomeEvent(t, sub, wake)
	if up.Kind != ethp2p.PeerUp {
		t.Fatal("missing peer")
	}
	select {
	case <-wake:
	default:
	}
	return &outcomeFixture{ctx: ctx, conn: conn, peer: up.Peer, sub: sub, wake: wake}
}

func (f *outcomeFixture) openIncoming(t *testing.T, sel wire.Selector, payload []byte) *wireReceiveProbe {
	t.Helper()
	out, err := f.conn.OpenUniStream(f.ctx, sel)
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
func (f *outcomeFixture) incoming(t *testing.T, sel wire.Selector, reader io.Reader) (*wireReceiveProbe, ethp2p.ReceiveStream) {
	t.Helper()
	var payload []byte
	_, deadline := reader.(failReader)
	if reader != nil && !deadline {
		var err error
		payload, err = io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
	}
	raw := f.openIncoming(t, sel, payload)
	event := nextOutcomeEvent(t, f.sub, f.wake)
	if event.Kind != ethp2p.StreamIn || event.Selector != sel {
		t.Fatalf("event = %+v", event)
	}
	if deadline {
		_ = event.Stream.SetReadDeadline(time.Now().Add(-time.Second))
	}
	return raw, event.Stream
}

// These probes observe actual numeric reset codes on the remote QUIC endpoint.
type wireReceiveProbe struct {
	out   transport.SendStream
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

type wireSendProbe struct{ in transport.ReceiveStream }

func (p *wireSendProbe) CancelWriteCodes() []uint64 {
	_ = p.in.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err := p.in.Read(make([]byte, 1))
	if reset, ok := errors.AsType[*transport.StreamResetError](err); ok {
		return []uint64{reset.Code}
	}
	return nil
}

func nextOutcomeEvent(t *testing.T, sub *ethp2p.Subsystem, wake <-chan struct{}) ethp2p.Event {
	t.Helper()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	for {
		if event, ok := sub.Next(); ok {
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

func newWrappedRecordingReceiveStream(t *testing.T, selector wire.Selector, payload []byte) (*wireReceiveProbe, ethp2p.ReceiveStream) {
	t.Helper()
	return newOutcomeFixture(t).incoming(t, selector, bytes.NewReader(payload))
}

type failReader struct{ err error }

func (r failReader) Read([]byte) (int, error) { return 0, r.err }

func assertTransportCodes(t *testing.T, got, want []uint64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("transport cancellation codes = %v, want exactly %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("transport cancellation codes = %v, want %v", got, want)
		}
	}
}

func requireWireCancelCode(t *testing.T, raw *wireReceiveProbe, want uint64) {
	t.Helper()
	got := raw.CancelReadCodes()
	if len(got) != 1 {
		t.Fatalf("transport received %d read cancellations %v, want exactly one", len(got), got)
	}
	if got[0] != want {
		t.Fatalf("first transport read cancellation = %d, want %d", got[0], want)
	}
}
