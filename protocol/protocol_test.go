package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"slices"
	"testing"
)

func TestSelectorCodeIsScopedAndControlSelectorPanics(t *testing.T) {
	sess := Selector(2)
	chunk := Selector(3)
	if sess.Code(1) != sess.Code(1) {
		t.Fatal("same selector code values differ")
	}
	if sess.Code(1) == chunk.Code(1) {
		t.Fatal("equal values from different selectors compare equal")
	}
	if sess.Code(0) == sess.Code(1) {
		t.Fatal("different values for one selector compare equal")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("Selector(0).Code did not panic")
		}
	}()
	Selector(0).Code(1)
}

func TestCodeWireFor(t *testing.T) {
	selector := Selector(2)
	other := Selector(3)
	for _, test := range []struct {
		name string
		code Code
		want uint64
	}{
		{name: "Unspecified", code: Unspecified, want: 0},
		{name: "Refused", code: Refused, want: 2},
		{name: "Overloaded", code: Overloaded, want: 4},
		{name: "Timeout", code: Timeout, want: 6},
		{name: "protocol value zero", code: selector.Code(0), want: 1},
		{name: "protocol value one", code: selector.Code(1), want: 3},
		{name: "protocol max uint16", code: selector.Code(^uint16(0)), want: uint64(^uint16(0))<<1 | 1},
		{name: "BadSelector sent as Unspecified", code: BadSelector, want: 0},
		{name: "UnsupportedSelector sent as Unspecified", code: UnsupportedSelector, want: 0},
		{name: "Closing sent as Unspecified", code: Closing, want: 0},
		{name: "ControlViolation sent as Unspecified", code: ControlViolation, want: 0},
		{name: "NoSharedProtocols sent as Unspecified", code: NoSharedProtocols, want: 0},
		{name: "Duplicate sent as Unspecified", code: Duplicate, want: 0},
		{name: "unassigned stack value 4", code: Code{value: 4}, want: 0},
		{name: "unassigned stack value 5", code: Code{value: 5}, want: 0},
		{name: "unassigned stack value 6", code: Code{value: 6}, want: 0},
		{name: "unassigned stack value 7", code: Code{value: 7}, want: 0},
		{name: "unassigned stack value 14", code: Code{value: 14}, want: 0},
		{name: "unassigned large stack value", code: Code{value: 1 << 20}, want: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.code.WireFor(selector); got != test.want {
				t.Fatalf("WireFor(%s) = %d, want %d", test.code, got, test.want)
			}
		})
	}
	assertCodePanics(t, func() { _ = selector.Code(1).WireFor(other) })
}

func TestParseCode(t *testing.T) {
	selector := Selector(7)
	for _, test := range []struct {
		name string
		wire uint64
		want Code
	}{
		{name: "Unspecified", wire: 0, want: Unspecified},
		{name: "Refused", wire: 2, want: Refused},
		{name: "Overloaded", wire: 4, want: Overloaded},
		{name: "Timeout", wire: 6, want: Timeout},
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
		{name: "protocol value zero", wire: 1, want: selector.Code(0)},
		{name: "protocol value one", wire: 3, want: selector.Code(1)},
		{name: "unknown protocol value preserved", wire: 199, want: selector.Code(99)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := ParseCode(selector, test.wire); got != test.want {
				t.Fatalf("ParseCode(%d) = %s, want %s", test.wire, got, test.want)
			}
		})
	}
}

func assertCodePanics(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("operation did not panic")
		}
	}()
	fn()
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
		{name: "overflowing prefix", wire: bytes.Repeat([]byte{0x80}, binary.MaxVarintLen64), want: ErrInvalidFrame},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ReadFrame(bytes.NewReader(test.wire), 32)
			if !errors.Is(err, test.want) {
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
	}{
		{name: "zero length", wire: []byte{0}},
		{name: "more than ten bytes", wire: []byte{11}},
		{name: "encoding shorter than frame", wire: []byte{2, 17, 18}},
		{name: "unterminated encoding", wire: []byte{1, 0x80}},
		{name: "non-minimal encoding", wire: []byte{2, 0x81, 0}},
		{name: "truncated prefix", wire: []byte{0x80}},
		{name: "truncated frame", wire: []byte{2, 17}},
		{name: "overflowing encoding", wire: append([]byte{10}, append(bytes.Repeat([]byte{0xff}, 9), 0x02)...)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ReadSelector(bytes.NewReader(test.wire)); !errors.Is(err, ErrInvalidSelectors) {
				t.Fatalf("ReadSelector error = %v, want ErrInvalidSelectors", err)
			}
		})
	}
}

func TestReadSelectorAcceptsReservedSelectorFrames(t *testing.T) {
	for _, selector := range []Selector{ControlSelector} {
		got, err := ReadSelector(bytes.NewReader(selectorFrame(selector)))
		if err != nil || got != selector {
			t.Fatalf("ReadSelector(%d) = (%d, %v)", selector, got, err)
		}
	}
}

func TestWriteSelectorReportsShortWrite(t *testing.T) {
	if err := WriteSelector(shortWriter{}, 17); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("WriteSelector error = %v, want io.ErrShortWrite", err)
	}
}

func TestValidateSelector(t *testing.T) {
	for _, selector := range []Selector{ControlSelector} {
		if err := ValidateSelector(selector); !errors.Is(err, ErrReservedSelector) {
			t.Fatalf("ValidateSelector(%d) = %v, want ErrReservedSelector", selector, err)
		}
	}
	if err := ValidateSelector(1); err != nil {
		t.Fatalf("ValidateSelector(1) = %v", err)
	}
	if err := ValidateSelector(Selector('/')); err != nil {
		t.Fatalf("ValidateSelector('/'): %v", err)
	}
}

func TestCanonicalSortsDeduplicatesAndDoesNotAlias(t *testing.T) {
	input := []Selector{9, 1, 9, 2, 1}
	got := Canonical(input)
	if want := []Selector{1, 2, 9}; !slices.Equal(got, want) {
		t.Fatalf("Canonical = %v, want %v", got, want)
	}

	got[0] = 99
	if want := []Selector{9, 1, 9, 2, 1}; !slices.Equal(input, want) {
		t.Fatalf("Canonical modified input: got input %v, want %v", input, want)
	}
}

func TestIntersect(t *testing.T) {
	a := []Selector{1, 3, 5, 9, 12}
	b := []Selector{0, 3, 4, 9, 10, 12}
	got := Intersect(a, b)
	if want := []Selector{3, 9, 12}; !slices.Equal(got, want) {
		t.Fatalf("Intersect = %v, want %v", got, want)
	}

	got[0] = 99
	if a[1] != 3 || b[1] != 3 {
		t.Fatalf("Intersect result aliases an input: a=%v b=%v", a, b)
	}
}

func TestValidateSelectors(t *testing.T) {
	if err := ValidateSelectors([]Selector{1, 2, 300, math.MaxUint64}); err != nil {
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
		{name: "control selector reserved", entries: []Selector{ControlSelector}, want: ErrReservedSelector},
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
	if err := ValidateSelectors(append(selectors, 2024)); !errors.Is(err, ErrTooManySelectors) {
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
	short bool
}

func (w *recordingWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.short {
		return len(p) - 1, nil
	}
	return w.Buffer.Write(p)
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

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
