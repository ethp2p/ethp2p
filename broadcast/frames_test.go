package broadcast

import (
	"bytes"
	"context"
	"testing"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/wire"
	"google.golang.org/protobuf/proto"
)

// Frame types from the BCAST, SESS and CHUNK specs, written out here so the
// tests check the wire numbers rather than the package's constants.
const (
	testHandshakeType wire.FrameType = 0
	testSubscribeType wire.FrameType = 1
	testUpdateType    wire.FrameType = 1
	testHeaderType    wire.FrameType = 0
	testDataType      wire.FrameType = 1
)

func testFrame(t *testing.T, typ wire.FrameType, msg proto.Message) []byte {
	t.Helper()
	raw, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return wire.AppendFrame(nil, typ, raw)
}

func TestProtocolFirstFrameGates(t *testing.T) {
	tests := []struct {
		name     string
		protocol wire.Protocol
		frame    []byte
		invoke   func(*testing.T, ethp2p.ReceiveStream)
	}{
		{
			name:     "BCAST/first/SUBSCRIBE",
			protocol: BCAST,
			frame:    testFrame(t, testSubscribeType, &bcastpb.Bcast_Subscribe{Channel: "c"}),
			invoke: func(t *testing.T, in ethp2p.ReceiveStream) {
				p := &PeerConn{ctx: t.Context(), streams: &blackholeOpener{}, bcastIn: make(chan ethp2p.ReceiveStream, 1)}
				p.bcastIn <- in
				if _, err := p.handshake(); err == nil {
					t.Fatal("handshake accepted a non-HANDSHAKE first frame")
				}
			},
		},
		{
			name:     "BCAST/first/unknown",
			protocol: BCAST,
			frame:    wire.AppendFrame(nil, 7, nil),
			invoke: func(t *testing.T, in ethp2p.ReceiveStream) {
				p := &PeerConn{ctx: t.Context(), streams: &blackholeOpener{}, bcastIn: make(chan ethp2p.ReceiveStream, 1)}
				p.bcastIn <- in
				if _, err := p.handshake(); err == nil {
					t.Fatal("handshake accepted an unknown first frame")
				}
			},
		},
		{
			name:     "BCAST/second/HANDSHAKE",
			protocol: BCAST,
			frame:    testFrame(t, testHandshakeType, &bcastpb.Bcast_Handshake{Version: ProtocolV1}),
			invoke: func(t *testing.T, in ethp2p.ReceiveStream) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				(&PeerConn{ctx: ctx, cancel: cancel, ctrlIn: in}).runCtrlReader()
			},
		},
		{
			name:     "BCAST/second/unknown",
			protocol: BCAST,
			frame:    wire.AppendFrame(nil, 7, nil),
			invoke: func(t *testing.T, in ethp2p.ReceiveStream) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				(&PeerConn{ctx: ctx, cancel: cancel, ctrlIn: in}).runCtrlReader()
			},
		},
		{
			name:     "SESS/first/UPDATE",
			protocol: SESS,
			frame:    testFrame(t, testUpdateType, &bcastpb.Sess_Update{Data: []byte{5}}),
			invoke: func(t *testing.T, in ethp2p.ReceiveStream) {
				(&PeerConn{ctx: t.Context()}).runInboundSession(in)
			},
		},
		{
			name:     "SESS/first/unknown",
			protocol: SESS,
			frame:    wire.AppendFrame(nil, 7, nil),
			invoke: func(t *testing.T, in ethp2p.ReceiveStream) {
				(&PeerConn{ctx: t.Context()}).runInboundSession(in)
			},
		},
		{
			name:     "CHUNK/first/DATA",
			protocol: CHUNK,
			frame:    wire.AppendFrame(nil, testDataType, []byte("abc")),
			invoke: func(t *testing.T, in ethp2p.ReceiveStream) {
				(&PeerConn{ctx: t.Context()}).processChunk(in)
			},
		},
		{
			name:     "CHUNK/first/unknown",
			protocol: CHUNK,
			frame:    wire.AppendFrame(nil, 7, nil),
			invoke: func(t *testing.T, in ethp2p.ReceiveStream) {
				(&PeerConn{ctx: t.Context()}).processChunk(in)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, in := newOutcomeFixture(t).incoming(t, test.protocol, bytes.NewReader(test.frame))
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
			raw, in := newOutcomeFixture(t).incoming(t, CHUNK, bytes.NewReader(bytes.Join(test.frames, nil)))
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
