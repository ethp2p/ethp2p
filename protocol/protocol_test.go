package protocol

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/transport"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	testSmallCodepoint Codepoint = 17
	testLargeCodepoint Codepoint = 300
)

func TestWriteReadSelectorFraming(t *testing.T) {
	tests := []struct {
		name      string
		codepoint Codepoint
		wire      []byte
	}{
		{name: "one-byte codepoint", codepoint: testSmallCodepoint, wire: []byte{17}},
		{name: "two-byte codepoint", codepoint: testLargeCodepoint, wire: []byte{0xac, 0x02}},
		{
			name:      "ten-byte codepoint",
			codepoint: math.MaxUint64,
			wire:      []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteSelector(&buf, test.codepoint); err != nil {
				t.Fatalf("WriteSelector(%d): %v", test.codepoint, err)
			}
			wire := buf.Bytes()
			if !bytes.Equal(wire, test.wire) {
				t.Fatalf("selector frame = %x, want %x", wire, test.wire)
			}

			// Selection must also finish when no protocol data follows it.
			got, err := ReadSelector(bytes.NewReader(wire))
			if err != nil || got != test.codepoint {
				t.Fatalf("selector alone = (%d, %v), want %d", got, err, test.codepoint)
			}

			const payload = "\x00/payload"
			reader := bytes.NewReader(append(bytes.Clone(wire), payload...))
			got, err = ReadSelector(reader)
			if err != nil {
				t.Fatalf("ReadSelector: %v", err)
			}
			if got != test.codepoint {
				t.Fatalf("codepoint = %d, want %d", got, test.codepoint)
			}
			remaining, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			if string(remaining) != payload {
				t.Fatalf("remaining payload = %q, want %q", remaining, payload)
			}
		})
	}
}

func TestRegistry(t *testing.T) {
	var registry Registry
	descriptor := Descriptor{Codepoint: 300, Name: "test"}
	if err := registry.Register(testProtocolSet(descriptor)); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(testProtocolSet(descriptor)); !errors.Is(err, ErrCodepointInUse) {
		t.Fatalf("duplicate Register error = %v, want %v", err, ErrCodepointInUse)
	}
	tests := []struct {
		name       string
		descriptor Descriptor
		want       error
	}{
		{name: "empty name", descriptor: Descriptor{Codepoint: 301}, want: ErrInvalidDescriptor},
		{name: "zero", descriptor: Descriptor{Codepoint: 0, Name: "zero"}, want: ErrReservedCodepoint},
		{name: "libp2p marker", descriptor: Descriptor{Codepoint: '/', Name: "slash"}, want: ErrReservedCodepoint},
		{name: "duplicate codepoint", descriptor: Descriptor{Codepoint: 300, Name: "other"}, want: ErrCodepointInUse},
		{name: "duplicate name", descriptor: Descriptor{Codepoint: 301, Name: "test"}, want: ErrNameInUse},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := registry.Register(testProtocolSet(test.descriptor)); !errors.Is(err, test.want) {
				t.Fatalf("Register(%+v) error = %v, want %v", test.descriptor, err, test.want)
			}
		})
	}

	if err := registry.Register(Set{}); !errors.Is(err, ErrInvalidDescriptor) {
		t.Fatalf("empty set error = %v, want %v", err, ErrInvalidDescriptor)
	}
	if err := registry.Register(Set{
		Descriptors: []Descriptor{{Codepoint: 301, Name: "nil-bind"}},
	}); !errors.Is(err, ErrInvalidHandler) {
		t.Fatalf("nil bind error = %v, want %v", err, ErrInvalidHandler)
	}
}

func TestRegistryBindsHandlers(t *testing.T) {
	var registry Registry
	descriptor := Descriptor{Codepoint: 300, Name: "test"}
	closed := false
	done := make(chan struct{})
	set := testProtocolSet(descriptor)
	set.Bind = func(context.Context, transport.Conn) (ConnectionHandlers, error) {
		return ConnectionHandlers{
			ByCodepoint: map[Codepoint]Handlers{
				descriptor.Codepoint: {AcceptUni: func(transport.ReceiveStream) {}},
			},
			Done:  done,
			Close: func() { closed = true },
		}, nil
	}
	if err := registry.Register(set); err != nil {
		t.Fatal(err)
	}
	routes, err := registry.Bind(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	handler, ok := routes.Lookup(descriptor.Codepoint)
	if !ok || handler.AcceptUni == nil {
		t.Fatal("bound handler is missing")
	}
	close(done)
	select {
	case <-routes.Done():
	case <-time.After(time.Second):
		t.Fatal("routes did not report protocol termination")
	}
	routes.Close()
	routes.Close()
	if !closed {
		t.Fatal("bound handler was not closed")
	}
}

func TestRoutesCloseSignalsDone(t *testing.T) {
	var registry Registry
	if err := registry.Register(testProtocolSet(Descriptor{Codepoint: 300, Name: "test"})); err != nil {
		t.Fatal(err)
	}
	routes, err := registry.Bind(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	routes.Close()
	select {
	case <-routes.Done():
	default:
		t.Fatal("closed routes did not report termination")
	}
}

func TestRegistryBindRejectsInvalidHandlers(t *testing.T) {
	descriptor := Descriptor{Codepoint: 300, Name: "test"}
	tests := []struct {
		name     string
		handlers map[Codepoint]Handlers
	}{
		{name: "missing"},
		{name: "empty", handlers: map[Codepoint]Handlers{300: {}}},
		{
			name: "undeclared",
			handlers: map[Codepoint]Handlers{
				300: {AcceptUni: func(transport.ReceiveStream) {}},
				301: {AcceptUni: func(transport.ReceiveStream) {}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var registry Registry
			err := registry.Register(Set{
				Descriptors: []Descriptor{descriptor},
				Bind: func(context.Context, transport.Conn) (ConnectionHandlers, error) {
					return ConnectionHandlers{ByCodepoint: test.handlers}, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := registry.Bind(t.Context(), nil); !errors.Is(err, ErrInvalidHandler) {
				t.Fatalf("Bind error = %v, want %v", err, ErrInvalidHandler)
			}
		})
	}
}

func TestRegistryBindClosesEarlierSetsOnError(t *testing.T) {
	var registry Registry
	closed := false
	first := testProtocolSet(Descriptor{Codepoint: 300, Name: "first"})
	first.Bind = func(context.Context, transport.Conn) (ConnectionHandlers, error) {
		return ConnectionHandlers{
			ByCodepoint: map[Codepoint]Handlers{
				300: {AcceptUni: func(transport.ReceiveStream) {}},
			},
			Close: func() { closed = true },
		}, nil
	}
	if err := registry.Register(first); err != nil {
		t.Fatal(err)
	}
	second := testProtocolSet(Descriptor{Codepoint: 301, Name: "second"})
	second.Bind = func(context.Context, transport.Conn) (ConnectionHandlers, error) {
		return ConnectionHandlers{}, errors.New("bind failed")
	}
	if err := registry.Register(second); err != nil {
		t.Fatal(err)
	}

	if _, err := registry.Bind(t.Context(), nil); err == nil {
		t.Fatal("Bind succeeded")
	}
	if !closed {
		t.Fatal("earlier set was not closed")
	}
}

func testProtocolSet(descriptors ...Descriptor) Set {
	return Set{
		Descriptors: descriptors,
		Bind: func(context.Context, transport.Conn) (ConnectionHandlers, error) {
			handlers := make(map[Codepoint]Handlers, len(descriptors))
			for _, descriptor := range descriptors {
				handlers[descriptor.Codepoint] = Handlers{
					AcceptUni: func(transport.ReceiveStream) {},
				}
			}
			return ConnectionHandlers{ByCodepoint: handlers}, nil
		},
	}
}

func TestWriteSelectorRejectsReservedCodepoints(t *testing.T) {
	for _, codepoint := range []Codepoint{0, '/'} {
		var buf bytes.Buffer
		if err := WriteSelector(&buf, codepoint); !errors.Is(err, ErrReservedCodepoint) {
			t.Fatalf("WriteSelector(%d) error = %v, want %v", codepoint, err, ErrReservedCodepoint)
		}
	}
}

func TestReadSelectorDecodesReservedCodepoints(t *testing.T) {
	for _, codepoint := range []Codepoint{0, '/'} {
		selector := protowire.AppendVarint(nil, uint64(codepoint))
		reader := bytes.NewReader(append(selector, []byte("payload")...))
		got, err := ReadSelector(reader)
		if err != nil || got != codepoint {
			t.Fatalf("ReadSelector(%d) = (%d, %v)", codepoint, got, err)
		}
		remaining, err := io.ReadAll(reader)
		if err != nil || string(remaining) != "payload" {
			t.Fatalf("remaining payload = %q, error = %v", remaining, err)
		}
	}
}

func TestReadSelectorRejectsMalformedVarint(t *testing.T) {
	tests := []struct {
		name string
		wire []byte
	}{
		{name: "empty stream"},
		{name: "truncated selector", wire: []byte{0xac}},
		{name: "unterminated selector", wire: bytes.Repeat([]byte{0x80}, 10)},
		{name: "overflowing selector", wire: append(bytes.Repeat([]byte{0xff}, 9), 0x02)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ReadSelector(bytes.NewReader(test.wire)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestWriteSelectorReportsShortWrite(t *testing.T) {
	if err := WriteSelector(shortWriter{}, testSmallCodepoint); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("WriteSelector error = %v, want %v", err, io.ErrShortWrite)
	}
}

type shortWriter struct{}

func (shortWriter) Write([]byte) (int, error) {
	return 0, nil
}
