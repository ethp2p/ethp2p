package ethp2p

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/wire"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
)

type rawSendStream = transporttest.RawSendStream
type rawReceiveStream = transporttest.RawReceiveStream

func TestStreamWrappersSendSuppliedCode(t *testing.T) {
	for _, test := range []struct {
		name string
		code wire.Code
		want uint64
	}{
		{name: "protocol namespace", code: wire.ProtocolCode(1), want: 3},
		{name: "shared stack code", code: wire.Refused, want: 2},
		{name: "stack-only code not rewritten", code: wire.BadSelector, want: 16},
		{name: "closing not rewritten", code: wire.Closing, want: 20},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := new(rawSendStream)
			wrapSendStream(raw).CancelWrite(test.code)
			assertCancelCodes(t, raw.CancelWriteCodes(), []uint64{test.want})
		})
	}

	rawReceive := new(rawReceiveStream)
	receive := wrapReceiveStream(rawReceive)
	receive.CancelRead(wire.Refused)
	assertCancelCodes(t, rawReceive.CancelReadCodes(), []uint64{2})
	receive.CancelRead(wire.Timeout)
	assertCancelCodes(t, rawReceive.CancelReadCodes(), []uint64{2})
}

func TestStreamErrorsDoNotCancel(t *testing.T) {
	// A read that times out on a deadline leaves the side open: extending the
	// deadline lets the stream be used again for data the peer writes after.
	raw := &rawReceiveStream{ReadErr: context.DeadlineExceeded}
	stream := wrapReceiveStream(raw)
	if _, err := stream.Read(make([]byte, 8)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Read error = %v, want deadline exceeded", err)
	}
	assertCancelCodes(t, raw.CancelReadCodes(), nil)
	if err := stream.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	raw.ReadErr = nil
	raw.Reader = bytes.NewReader([]byte("payload"))
	got, err := io.ReadAll(stream)
	if err != nil || string(got) != "payload" {
		t.Fatalf("Read after deadline extension = %q, %v, want payload", got, err)
	}
	assertCancelCodes(t, raw.CancelReadCodes(), nil)

	rawSend := &rawSendStream{WriteErr: context.DeadlineExceeded}
	send := wrapSendStream(rawSend)
	if _, err := send.Write([]byte("payload")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Write error = %v, want deadline exceeded", err)
	}
	assertCancelCodes(t, rawSend.CancelWriteCodes(), nil)

	rawClose := &rawSendStream{CloseErr: errors.New("close failed")}
	if err := wrapSendStream(rawClose).Close(); err == nil {
		t.Fatal("Close unexpectedly succeeded")
	}
	assertCancelCodes(t, rawClose.CancelWriteCodes(), nil)
}

func TestCancelSendsSuppliedCode(t *testing.T) {
	raw := new(rawSendStream)
	send := wrapSendStream(raw)
	send.CancelWrite(wire.Closing)
	assertCancelCodes(t, raw.CancelWriteCodes(), []uint64{wire.Closing.Wire()})

	// The peer decodes the reset with the supplied code.
	peer := wrapReceiveStream(&rawReceiveStream{ReadErr: &transport.StreamResetError{Code: wire.Closing.Wire()}})
	_, err := peer.Read(nil)
	reset, ok := errors.AsType[*ResetError](err)
	if !ok || reset.Code != wire.Closing {
		t.Fatalf("peer reset = (%T, %v), want *ResetError code %s", err, err, wire.Closing)
	}

	send.CancelWrite(wire.Refused)
	assertCancelCodes(t, raw.CancelWriteCodes(), []uint64{wire.Closing.Wire()})
}

func TestResetErrorDecodesProtocolAndStackCodes(t *testing.T) {
	transportErr := &transport.StreamResetError{Code: 3}
	receive := wrapReceiveStream(&rawReceiveStream{ReadErr: transportErr})
	_, err := receive.Read(nil)
	reset, ok := errors.AsType[*ResetError](err)
	if !ok {
		t.Fatalf("Read error = %T, want *ResetError", err)
	}
	if reset.Code != wire.ProtocolCode(1) {
		t.Fatalf("reset code = %s, want %s", reset.Code, wire.ProtocolCode(1))
	}
	if !errors.Is(err, transportErr) {
		t.Fatalf("ResetError does not unwrap to transport error: %v", err)
	}

	for _, test := range []struct {
		name string
		wire uint64
		want wire.Code
	}{
		{name: "stack-only preserved", wire: 16, want: wire.BadSelector},
		{name: "unknown stack normalized", wire: 28, want: wire.Unspecified},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := wrapReceiveStream(&rawReceiveStream{
				ReadErr: &transport.StreamResetError{Code: test.wire},
			}).Read(nil)
			reset, ok := errors.AsType[*ResetError](err)
			if !ok || reset.Code != test.want {
				t.Fatalf("Read reset = (%T, %v), want *ResetError code %s", err, err, test.want)
			}
		})
	}
}

func TestReceiveWrapperDoesNotEchoPeerReset(t *testing.T) {
	raw := &rawReceiveStream{ReadErr: &transport.StreamResetError{Code: 3}}
	_, err := wrapReceiveStream(raw).Read(nil)
	reset, ok := errors.AsType[*ResetError](err)
	if !ok {
		t.Fatalf("Read error = %T, want *ResetError", err)
	}
	if reset.Code != wire.ProtocolCode(1) {
		t.Fatalf("reset code = %s, want %s", reset.Code, wire.ProtocolCode(1))
	}
	assertCancelCodes(t, raw.CancelReadCodes(), nil)
}

func TestReceiveWrapperPreservesOrdinaryReadErrors(t *testing.T) {
	want := io.EOF
	_, err := wrapReceiveStream(&rawReceiveStream{ReadErr: want}).Read(nil)
	if !errors.Is(err, want) {
		t.Fatalf("Read error = %v, want %v", err, want)
	}
}

func assertCancelCodes(t *testing.T, got, want []uint64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("cancellation codes = %v, want exactly %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("cancellation codes = %v, want %v", got, want)
		}
	}
}
