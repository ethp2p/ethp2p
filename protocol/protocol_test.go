package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"slices"
	"testing"
)

func TestWriteReadSelectorWire(t *testing.T) {
	tests := []struct {
		name     string
		selector Selector
		wire     []byte
	}{
		{name: "one byte", selector: 17, wire: []byte{0x11}},
		{name: "two bytes", selector: 300, wire: []byte{0xac, 0x02}},
		{
			name:     "ten bytes",
			selector: math.MaxUint64,
			wire:     []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteSelector(&buf, test.selector); err != nil {
				t.Fatalf("WriteSelector(%d): %v", test.selector, err)
			}
			if !bytes.Equal(buf.Bytes(), test.wire) {
				t.Fatalf("selector wire = %x, want %x", buf.Bytes(), test.wire)
			}

			got, err := ReadSelector(bytes.NewReader(test.wire))
			if err != nil || got != test.selector {
				t.Fatalf("ReadSelector = (%d, %v), want (%d, nil)", got, err, test.selector)
			}
		})
	}
}

func TestReadSelectorLeavesBufferedPayloadUntouched(t *testing.T) {
	const payload = "\x00/payload"
	wire := binary.AppendUvarint(nil, uint64(300))
	reader := bufio.NewReader(bytes.NewReader(append(wire, payload...)))

	selector, err := ReadSelector(reader)
	if err != nil {
		t.Fatalf("ReadSelector: %v", err)
	}
	if selector != 300 {
		t.Fatalf("selector = %d, want 300", selector)
	}
	remaining, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if string(remaining) != payload {
		t.Fatalf("remaining payload = %q, want %q", remaining, payload)
	}
}

func TestReadSelectorAcceptsReservedValues(t *testing.T) {
	for _, selector := range []Selector{0, '/'} {
		t.Run(string(rune(selector)), func(t *testing.T) {
			wire := binary.AppendUvarint(nil, uint64(selector))
			got, err := ReadSelector(bytes.NewReader(append(wire, 'p')))
			if err != nil || got != selector {
				t.Fatalf("ReadSelector(%d) = (%d, %v)", selector, got, err)
			}
		})
	}
}

func TestWriteSelectorRejectsReservedValues(t *testing.T) {
	for _, selector := range []Selector{0, '/'} {
		var buf bytes.Buffer
		if err := WriteSelector(&buf, selector); !errors.Is(err, ErrReservedSelector) {
			t.Fatalf("WriteSelector(%d) error = %v, want %v", selector, err, ErrReservedSelector)
		}
		if buf.Len() != 0 {
			t.Fatalf("reserved selector wrote %d bytes", buf.Len())
		}
	}
}

func TestReadSelectorRejectsMalformedValues(t *testing.T) {
	tests := []struct {
		name string
		wire []byte
	}{
		{name: "empty stream"},
		{name: "truncated selector", wire: []byte{0xac}},
		{name: "unterminated selector", wire: bytes.Repeat([]byte{0x80}, binary.MaxVarintLen64)},
		{name: "overflowing selector", wire: append(bytes.Repeat([]byte{0xff}, binary.MaxVarintLen64-1), 0x02)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ReadSelector(bytes.NewReader(test.wire)); err == nil {
				t.Fatal("ReadSelector succeeded for malformed value")
			}
		})
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

func TestWriteReadSelectorsCanonicalRoundTrip(t *testing.T) {
	selectors := []Selector{1, 2, 300, math.MaxUint64}
	var buf bytes.Buffer
	if err := WriteSelectors(&buf, selectors); err != nil {
		t.Fatalf("WriteSelectors: %v", err)
	}

	wantWire := make([]byte, 0)
	for _, selector := range selectors {
		wantWire = binary.AppendUvarint(wantWire, uint64(selector))
	}
	if !bytes.Equal(buf.Bytes(), wantWire) {
		t.Fatalf("selector list wire = %x, want %x", buf.Bytes(), wantWire)
	}

	got, err := ReadSelectors(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("ReadSelectors: %v", err)
	}
	if !slices.Equal(got, selectors) {
		t.Fatalf("ReadSelectors = %v, want %v", got, selectors)
	}
}

func TestReadSelectorsAllowsEmptyFIN(t *testing.T) {
	got, err := ReadSelectors(bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("ReadSelectors(empty): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty advertisement = %v", got)
	}
}

func TestReadSelectorsRejectsMalformedAndNonCanonicalValues(t *testing.T) {
	validPrefix := binary.AppendUvarint(nil, 1)
	tests := []struct {
		name string
		wire []byte
	}{
		{name: "truncated first selector", wire: []byte{0xac}},
		{name: "truncated after prefix", wire: append(slices.Clone(validPrefix), 0xac)},
		{
			name: "unterminated selector",
			wire: bytes.Repeat([]byte{0x80}, binary.MaxVarintLen64),
		},
		{
			name: "overflowing selector",
			wire: append(bytes.Repeat([]byte{0xff}, binary.MaxVarintLen64-1), 0x02),
		},
		{name: "duplicate", wire: []byte{1, 1}},
		{name: "descending", wire: []byte{2, 1}},
		{name: "zero is reserved", wire: []byte{0}},
		{name: "slash is reserved", wire: []byte{'/'}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ReadSelectors(bytes.NewReader(test.wire)); err == nil {
				t.Fatal("ReadSelectors succeeded for malformed value")
			}
		})
	}
}

func TestReadSelectorsMaxSelectorsBoundary(t *testing.T) {
	wire := make([]byte, 0, MaxSelectors)
	for i := range MaxSelectors {
		wire = binary.AppendUvarint(wire, uint64(i+1000))
	}
	got, err := ReadSelectors(bytes.NewReader(wire))
	if err != nil {
		t.Fatalf("ReadSelectors(%d selectors): %v", MaxSelectors, err)
	}
	if len(got) != MaxSelectors {
		t.Fatalf("selector count = %d, want %d", len(got), MaxSelectors)
	}

	tooMany := binary.AppendUvarint(slices.Clone(wire), uint64(MaxSelectors+1000))
	if _, err := ReadSelectors(bytes.NewReader(tooMany)); !errors.Is(err, ErrTooManySelectors) {
		t.Fatalf("ReadSelectors(%d selectors) error = %v, want %v", MaxSelectors+1, err, ErrTooManySelectors)
	}
}

func TestWriteSelectorsValidatesBeforeWriting(t *testing.T) {
	tests := []struct {
		name      string
		selectors []Selector
		want      error
	}{
		{name: "duplicate", selectors: []Selector{1, 1}, want: ErrInvalidSelectors},
		{name: "descending", selectors: []Selector{2, 1}, want: ErrInvalidSelectors},
		{name: "zero reserved", selectors: []Selector{0}, want: ErrReservedSelector},
		{name: "slash reserved", selectors: []Selector{'/'}, want: ErrReservedSelector},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			buf.WriteByte(0xff)
			before := bytes.Clone(buf.Bytes())
			if err := WriteSelectors(&buf, test.selectors); !errors.Is(err, test.want) {
				t.Fatalf("WriteSelectors(%v) error = %v, want %v", test.selectors, err, test.want)
			}
			if !bytes.Equal(buf.Bytes(), before) {
				t.Fatalf("invalid list wrote bytes: got %x, want %x", buf.Bytes(), before)
			}
		})
	}

	tooMany := make([]Selector, MaxSelectors+1)
	for i := range tooMany {
		tooMany[i] = Selector(i + 1000)
	}
	if err := WriteSelectors(io.Discard, tooMany); !errors.Is(err, ErrTooManySelectors) {
		t.Fatalf("WriteSelectors(%d selectors) error = %v, want %v", len(tooMany), err, ErrTooManySelectors)
	}
}

func TestWriteSelectorsReportsShortWrite(t *testing.T) {
	if err := WriteSelectors(shortWriter{}, []Selector{17}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("WriteSelectors error = %v, want %v", err, io.ErrShortWrite)
	}
}

type shortWriter struct{}

func (shortWriter) Write([]byte) (int, error) {
	return 0, nil
}
