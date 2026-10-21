package broadcast

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/wire"
	"google.golang.org/protobuf/proto"
)

func testFrame(t *testing.T, typ wire.FrameType, msg proto.Message) []byte {
	t.Helper()
	raw, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return wire.AppendFrame(nil, typ, raw)
}

// Frames out of place after a stream's opening message refuse the stream. On
// the control stream they also end the binding as a violation; opening
// messages themselves are covered by TestOpeningViolationsEndBinding.
func TestFramesAfterOpeningMessage(t *testing.T) {
	ctrlReader := func(t *testing.T, in ethp2p.ReceiveStream) {
		ctx, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)
		(&PeerConn{ctx: ctx, cancel: cancel, ctrlIn: in}).runCtrlReader()
		if err := context.Cause(ctx); !errors.Is(err, ErrPeerViolation) {
			t.Fatalf("binding cause = %v, want ErrPeerViolation", err)
		}
	}
	session := func(t *testing.T, in ethp2p.ReceiveStream) {
		(&PeerConn{ctx: t.Context()}).runInboundSession(in, &bcastpb.Sess_Open{Channel: "c", ObjectId: "m"})
	}
	tests := []struct {
		name   string
		frame  []byte
		invoke func(*testing.T, ethp2p.ReceiveStream)
	}{
		{
			name:   "control/HANDSHAKE",
			frame:  testFrame(t, testHandshakeType, &bcastpb.Bcast_Handshake{}),
			invoke: ctrlReader,
		},
		{
			name:   "control/unknown",
			frame:  wire.AppendFrame(nil, 9, nil),
			invoke: ctrlReader,
		},
		{
			name:   "session/UPDATE before PREAMBLE",
			frame:  testFrame(t, testUpdateType, &bcastpb.Sess_Update{Data: []byte{5}}),
			invoke: session,
		},
		{
			name:   "session/unknown",
			frame:  wire.AppendFrame(nil, 9, nil),
			invoke: session,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, in := newOutcomeFixture(t).incoming(t, bytes.NewReader(test.frame))
			test.invoke(t, in)
			requireCode(t, raw.CancelReadCodes(), wire.Refused.Wire())
		})
	}
}

// TestChunkDataFrame feeds the bytes after a HEADER announcing 3 bytes. A
// stream the sender finished with FIN shows no reset code on the wire: once a
// read has returned EOF, quic-go sends no STOP_SENDING. Those cases check only
// that nothing is delivered.
func TestChunkDataFrame(t *testing.T) {
	data := wire.AppendFrame(nil, testDataType, []byte("abc"))
	for _, test := range []struct {
		name    string
		frames  [][]byte
		fin     bool
		deliver bool
	}{
		{name: "one DATA of data_length", frames: [][]byte{data}, fin: true, deliver: true},
		{name: "no DATA", fin: true},
		{name: "truncated DATA", frames: [][]byte{data[:3]}, fin: true},
		{name: "wrong type", frames: [][]byte{wire.AppendFrame(nil, testHeaderType, []byte("abc"))}},
		{name: "shorter than data_length", frames: [][]byte{wire.AppendFrame(nil, testDataType, []byte("ab"))}},
		{name: "longer than data_length", frames: [][]byte{wire.AppendFrame(nil, testDataType, []byte("abcd"))}},
		{name: "second DATA", frames: [][]byte{data, data}},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, in := newOutcomeFixture(t).incoming(t, bytes.NewReader(bytes.Join(test.frames, nil)))
			if test.fin {
				if err := raw.out.Close(); err != nil {
					t.Fatal(err)
				}
			}
			inbox := make(chan channelEvent, 4)
			sess := newTestSession(newMockStrategy(), inbox)
			defer sess.Close()
			sess.handleChunkStream("p1", []byte("chunk-id"), 3, in)
			var delivered []byte
			for done := false; !done; {
				select {
				case ev := <-inbox:
					switch ev := ev.(type) {
					case channelChunkData:
						delivered = ev.payload
					case channelReadDone:
						done = true
					}
				case <-time.After(5 * time.Second):
					t.Fatal("chunk read did not finish")
				}
			}
			if test.deliver {
				if string(delivered) != "abc" {
					t.Fatalf("payload = %q, want abc", delivered)
				}
				return
			}
			if delivered != nil {
				t.Fatalf("delivered %q from an invalid DATA frame", delivered)
			}
			if !test.fin {
				requireCode(t, raw.CancelReadCodes(), wire.Refused.Wire())
			}
		})
	}
}
