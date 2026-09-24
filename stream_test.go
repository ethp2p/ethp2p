package ethp2p

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/ethp2p/ethp2p/protocol"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/ethp2p/ethp2p/transport/transporttest"
)

type rawSendStream = transporttest.RawSendStream
type rawReceiveStream = transporttest.RawReceiveStream

func TestStreamWrappersEncodeCodesAtTransportBoundary(t *testing.T) {
	selector := protocol.Selector(2)
	for _, test := range []struct {
		name string
		code protocol.Code
		want uint64
	}{
		{name: "protocol namespace", code: selector.Code(1), want: 3},
		{name: "shared stack code", code: protocol.Overloaded, want: 4},
		{name: "stack-only code maps to unspecified", code: protocol.BadSelector, want: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := new(rawSendStream)
			wrapSendStream(selector, raw).CancelWrite(test.code)
			assertCancelCodes(t, raw.CancelWriteCodes(), []uint64{test.want})
		})
	}

	badSend := new(rawSendStream)
	send := wrapSendStream(selector, badSend)
	assertPanics(t, func() { send.CancelWrite(protocol.Selector(3).Code(1)) })

	rawReceive := new(rawReceiveStream)
	receive := wrapReceiveStream(selector, rawReceive)
	receive.CancelRead(protocol.Overloaded)
	assertCancelCodes(t, rawReceive.CancelReadCodes(), []uint64{4})
	assertPanics(t, func() { receive.CancelRead(protocol.Selector(3).Code(1)) })
}

func TestStreamWrappersCancelFailedIO(t *testing.T) {
	selector := protocol.Selector(2)
	rawSend := &rawSendStream{WriteErr: context.DeadlineExceeded}
	send := wrapSendStream(selector, rawSend)
	if _, err := send.Write([]byte("payload")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Write error = %v, want deadline exceeded", err)
	}
	assertCancelCodes(t, rawSend.CancelWriteCodes(), []uint64{6})
	send.CancelWrite(protocol.Refused)
	assertCancelCodes(t, rawSend.CancelWriteCodes(), []uint64{6})

	rawClose := &rawSendStream{CloseErr: errors.New("close failed")}
	if err := wrapSendStream(selector, rawClose).Close(); err == nil {
		t.Fatal("Close unexpectedly succeeded")
	}
	assertCancelCodes(t, rawClose.CancelWriteCodes(), []uint64{0})

	rawReceive := &rawReceiveStream{ReadErr: context.DeadlineExceeded}
	receive := wrapReceiveStream(selector, rawReceive)
	if _, err := receive.Read(nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Read error = %v, want deadline exceeded", err)
	}
	assertCancelCodes(t, rawReceive.CancelReadCodes(), []uint64{6})
}

func TestResetErrorDecodesProtocolAndStackCodes(t *testing.T) {
	selector := protocol.Selector(2)
	transportErr := &transport.StreamResetError{Code: 3}
	receive := wrapReceiveStream(selector, &rawReceiveStream{ReadErr: transportErr})
	_, err := receive.Read(nil)
	reset, ok := errors.AsType[*ResetError](err)
	if !ok {
		t.Fatalf("Read error = %T, want *ResetError", err)
	}
	if reset.Code != selector.Code(1) {
		t.Fatalf("reset code = %s, want %s", reset.Code, selector.Code(1))
	}
	if !errors.Is(err, transportErr) {
		t.Fatalf("ResetError does not unwrap to transport error: %v", err)
	}

	for _, test := range []struct {
		name string
		wire uint64
		want protocol.Code
	}{
		{name: "stack-only preserved", wire: 16, want: protocol.BadSelector},
		{name: "unknown stack normalized", wire: 28, want: protocol.Unspecified},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := wrapReceiveStream(selector, &rawReceiveStream{
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
	selector := protocol.Selector(2)
	raw := &rawReceiveStream{ReadErr: &transport.StreamResetError{Code: 3}}
	_, err := wrapReceiveStream(selector, raw).Read(nil)
	reset, ok := errors.AsType[*ResetError](err)
	if !ok {
		t.Fatalf("Read error = %T, want *ResetError", err)
	}
	if reset.Code != selector.Code(1) {
		t.Fatalf("reset code = %s, want %s", reset.Code, selector.Code(1))
	}
	assertCancelCodes(t, raw.CancelReadCodes(), nil)
}

func TestReceiveWrapperPreservesOrdinaryReadErrors(t *testing.T) {
	want := io.EOF
	_, err := wrapReceiveStream(protocol.Selector(2), &rawReceiveStream{ReadErr: want}).Read(nil)
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

func assertPanics(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("operation did not panic")
		}
	}()
	fn()
}
