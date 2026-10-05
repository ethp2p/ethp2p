package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"testing"
)

func TestProtocolCodeWire(t *testing.T) {
	if got := ProtocolCode(0).Wire(); got != 1 {
		t.Fatalf("ProtocolCode(0).Wire() = %d, want 1", got)
	}
	if got := ProtocolCode(1).Wire(); got != 3 {
		t.Fatalf("ProtocolCode(1).Wire() = %d, want 3", got)
	}
	if got, want := ProtocolCode(^uint16(0)).Wire(), uint64(^uint16(0))<<1|1; got != want {
		t.Fatalf("ProtocolCode(max).Wire() = %d, want %d", got, want)
	}
	for _, test := range []struct {
		name string
		code Code
		want uint64
	}{
		{name: "Unspecified", code: Unspecified, want: 0},
		{name: "Refused", code: Refused, want: 2},
		{name: "Timeout", code: Timeout, want: 4},
		{name: "BadSelector", code: BadSelector, want: 16},
		{name: "UnsupportedSelector", code: UnsupportedSelector, want: 18},
		{name: "Closing", code: Closing, want: 20},
		{name: "ControlViolation", code: ControlViolation, want: 22},
		{name: "NoSharedProtocols", code: NoSharedProtocols, want: 24},
		{name: "Duplicate", code: Duplicate, want: 26},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.code.Wire(); got != test.want {
				t.Fatalf("Wire(%s) = %d, want %d", test.code, got, test.want)
			}
		})
	}
	if got, want := ProtocolCode(1).String(), "protocol value 1"; got != want {
		t.Fatalf("ProtocolCode(1).String() = %q, want %q", got, want)
	}
}

func TestParseCode(t *testing.T) {
	for _, test := range []struct {
		name string
		wire uint64
		want Code
	}{
		{name: "Unspecified", wire: 0, want: Unspecified},
		{name: "Refused", wire: 2, want: Refused},
		{name: "Timeout", wire: 4, want: Timeout},
		{name: "gap 3", wire: 6, want: Unspecified},
		{name: "BadSelector", wire: 16, want: BadSelector},
		{name: "UnsupportedSelector", wire: 18, want: UnsupportedSelector},
		{name: "Closing", wire: 20, want: Closing},
		{name: "ControlViolation", wire: 22, want: ControlViolation},
		{name: "NoSharedProtocols", wire: 24, want: NoSharedProtocols},
		{name: "Duplicate", wire: 26, want: Duplicate},
		{name: "gap 4", wire: 8, want: Unspecified},
		{name: "gap 5", wire: 10, want: Unspecified},
		{name: "gap 6", wire: 12, want: Unspecified},
		{name: "gap 7", wire: 14, want: Unspecified},
		{name: "gap 14", wire: 28, want: Unspecified},
		{name: "large unknown stack value", wire: 1 << 20, want: Unspecified},
		{name: "protocol value zero", wire: 1, want: ProtocolCode(0)},
		{name: "protocol value one", wire: 3, want: ProtocolCode(1)},
		{name: "unknown protocol value preserved", wire: 199, want: ProtocolCode(99)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := ParseCode(test.wire); got != test.want {
				t.Fatalf("ParseCode(%d) = %s, want %s", test.wire, got, test.want)
			}
		})
	}
}

func TestFrameRoundTrips(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload []byte
		maxLen  int
	}{
		{name: "empty", maxLen: 0},
		{name: "one byte", payload: []byte{0x2a}, maxLen: 1},
		{name: "127 bytes", payload: bytes.Repeat([]byte{0xa5}, 127), maxLen: 127},
		{name: "128 bytes", payload: bytes.Repeat([]byte{0x5a}, 128), maxLen: 128},
		{name: "maxLen boundary", payload: []byte{1, 2, 3}, maxLen: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			wire := AppendFrame(nil, test.payload)
			got, err := ReadFrame(bytes.NewReader(wire), test.maxLen)
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			if !bytes.Equal(got, test.payload) {
				t.Fatalf("frame payload length = %d, want %d", len(got), len(test.payload))
			}
		})
	}
}

func TestReadFrameDoesNotReadPastFrame(t *testing.T) {
	const following = "next frame"
	wire := AppendFrame(nil, []byte("one frame"))
	wire = append(wire, following...)
	reader := bytes.NewReader(wire)
	if _, err := ReadFrame(reader, 32); err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	got := make([]byte, len(following))
	if _, err := io.ReadFull(reader, got); err != nil {
		t.Fatalf("read following data: %v", err)
	}
	if string(got) != following {
		t.Fatalf("following data = %q, want %q", got, following)
	}
}

func TestReadFrameRejectsOversizeBeforeAllocation(t *testing.T) {
	oversizeReader := countingReader{data: []byte{2, 0xaa, 0xbb}}
	oversizeAllocations := testing.AllocsPerRun(100, func() {
		oversizeReader.offset = 0
		_, err := ReadFrame(&oversizeReader, 1)
		if err != ErrFrameTooLarge {
			panic("ReadFrame did not return ErrFrameTooLarge")
		}
	})
	validReader := countingReader{data: []byte{1, 0xaa}}
	validAllocations := testing.AllocsPerRun(100, func() {
		validReader.offset = 0
		if _, err := ReadFrame(&validReader, 1); err != nil {
			panic("ReadFrame rejected a frame at maxLen")
		}
	})
	if oversizeAllocations >= validAllocations {
		t.Fatalf("oversize rejection allocations = %v, valid frame allocations = %v; payload allocation was not avoided", oversizeAllocations, validAllocations)
	}
	if oversizeReader.offset != 1 {
		t.Fatalf("oversize read consumed %d bytes, want only the length prefix", oversizeReader.offset)
	}
}

func TestReadFrameRejectsTruncatedAndInvalidLengths(t *testing.T) {
	for _, test := range []struct {
		name string
		wire []byte
		want error
	}{
		{name: "truncated prefix", wire: []byte{0x80}, want: io.ErrUnexpectedEOF},
		{name: "empty truncated payload", wire: []byte{2}, want: io.ErrUnexpectedEOF},
		{name: "partial truncated payload", wire: []byte{2, 0xaa}, want: io.ErrUnexpectedEOF},
		// encoding/binary reports the overflow with an unexported error.
		{name: "overflowing prefix", wire: bytes.Repeat([]byte{0x80}, binary.MaxVarintLen64)},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ReadFrame(bytes.NewReader(test.wire), 32)
			if err == nil || test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("ReadFrame error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestSelectorFramesRoundTrip(t *testing.T) {
	for _, test := range []struct {
		name     string
		selector Selector
	}{
		{name: "one", selector: 1},
		{name: "127", selector: 127},
		{name: "128", selector: 128},
		{name: "one shifted 63", selector: 1 << 63},
		{name: "maximum uint64", selector: math.MaxUint64},
	} {
		t.Run(test.name, func(t *testing.T) {
			selector := test.selector
			writer := new(recordingWriter)
			if err := WriteSelector(writer, selector); err != nil {
				t.Fatalf("WriteSelector(%d): %v", selector, err)
			}
			if writer.calls != 1 {
				t.Fatalf("WriteSelector made %d writes, want 1", writer.calls)
			}
			want := selectorFrame(selector)
			if !bytes.Equal(writer.Bytes(), want) {
				t.Fatalf("selector wire = %x, want %x", writer.Bytes(), want)
			}

			reader := bytes.NewReader(append(bytes.Clone(want), 0xaa))
			got, err := ReadSelector(reader)
			if err != nil || got != selector {
				t.Fatalf("ReadSelector = (%d, %v), want (%d, nil)", got, err, selector)
			}
			if b, err := reader.ReadByte(); err != nil || b != 0xaa {
				t.Fatalf("byte after selector = (%x, %v), want 0xaa", b, err)
			}
		})
	}
}

func TestSelectorZeroAndSlashAreValidFrames(t *testing.T) {
	for _, selector := range []Selector{ControlSelector, Selector('/')} {
		writer := new(recordingWriter)
		if err := WriteSelector(writer, selector); err != nil {
			t.Fatalf("WriteSelector(%d): %v", selector, err)
		}
		if !bytes.Equal(writer.Bytes(), selectorFrame(selector)) {
			t.Fatalf("selector frame = %x", writer.Bytes())
		}

		got, err := ReadSelector(bytes.NewReader(selectorFrame(selector)))
		if err != nil || got != selector {
			t.Fatalf("ReadSelector(%d) = (%d, %v)", selector, got, err)
		}
	}
}

func TestReadSelectorRejectsInvalidFrames(t *testing.T) {
	for _, test := range []struct {
		name string
		wire []byte
		want error
	}{
		{name: "zero length", wire: []byte{0}, want: ErrInvalidSelectors},
		{name: "more than ten bytes", wire: []byte{11}, want: ErrFrameTooLarge},
		{name: "encoding shorter than frame", wire: []byte{2, 17, 18}, want: ErrInvalidSelectors},
		{name: "unterminated encoding", wire: []byte{1, 0x80}, want: ErrInvalidSelectors},
		{name: "non-minimal encoding", wire: []byte{2, 0x81, 0}, want: ErrInvalidSelectors},
		{name: "truncated prefix", wire: []byte{0x80}, want: io.ErrUnexpectedEOF},
		{name: "truncated frame", wire: []byte{2, 17}, want: io.ErrUnexpectedEOF},
		{name: "overflowing encoding", wire: append([]byte{10}, append(bytes.Repeat([]byte{0xff}, 9), 0x02)...), want: ErrInvalidSelectors},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ReadSelector(bytes.NewReader(test.wire)); !errors.Is(err, test.want) {
				t.Fatalf("ReadSelector error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestValidateSelectors(t *testing.T) {
	if err := ValidateSelectors([]Selector{1, 2, '/', 300, math.MaxUint64}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSelectors(nil); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		entries []Selector
		want    error
	}{
		{name: "non-ascending", entries: []Selector{2, 1}, want: ErrInvalidSelectors},
		{name: "duplicate", entries: []Selector{3, 3}, want: ErrInvalidSelectors},
		{name: "control selector reserved", entries: []Selector{ControlSelector}, want: ErrInvalidSelectors},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateSelectors(test.entries); !errors.Is(err, test.want) {
				t.Fatalf("ValidateSelectors error = %v, want %v", err, test.want)
			}
		})
	}
	selectors := make([]Selector, MaxSelectors)
	for i := range selectors {
		selectors[i] = Selector(i + 1000)
	}
	if err := ValidateSelectors(selectors); err != nil {
		t.Fatalf("ValidateSelectors(%d selectors): %v", MaxSelectors, err)
	}
	if err := ValidateSelectors(append(selectors, 2024)); !errors.Is(err, ErrInvalidSelectors) {
		t.Fatalf("ValidateSelectors too many = %v", err)
	}
}

func selectorFrame(selector Selector) []byte {
	payload := binary.AppendUvarint(nil, uint64(selector))
	return AppendFrame(nil, payload)
}

type recordingWriter struct {
	bytes.Buffer
	calls int
}

func (w *recordingWriter) Write(p []byte) (int, error) {
	w.calls++
	return w.Buffer.Write(p)
}

type countingReader struct {
	data   []byte
	offset int
}

func (r *countingReader) Read(p []byte) (int, error) {
	if r.offset == len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}
