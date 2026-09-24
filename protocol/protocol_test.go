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

func TestSelectorReservedValuesAreRejectedOnWriteAndAcceptedOnRead(t *testing.T) {
	for _, selector := range []Selector{AdvertisementSelector, Selector('/')} {
		writer := new(recordingWriter)
		if err := WriteSelector(writer, selector); !errors.Is(err, ErrReservedSelector) {
			t.Fatalf("WriteSelector(%d) error = %v, want ErrReservedSelector", selector, err)
		}
		if writer.calls != 0 || writer.Len() != 0 {
			t.Fatalf("reserved selector wrote %d times and %d bytes", writer.calls, writer.Len())
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
	for _, selector := range []Selector{AdvertisementSelector, Selector('/')} {
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
	for _, selector := range []Selector{AdvertisementSelector, Selector('/')} {
		if err := ValidateSelector(selector); !errors.Is(err, ErrReservedSelector) {
			t.Fatalf("ValidateSelector(%d) = %v, want ErrReservedSelector", selector, err)
		}
	}
	if err := ValidateSelector(1); err != nil {
		t.Fatalf("ValidateSelector(1) = %v", err)
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

func TestWriteReadSelectorsAdvertisement(t *testing.T) {
	selectors := []Selector{1, 2, 300, math.MaxUint64}
	writer := new(recordingWriter)
	if err := WriteSelectors(writer, selectors); err != nil {
		t.Fatalf("WriteSelectors: %v", err)
	}
	if writer.calls != 1 {
		t.Fatalf("WriteSelectors made %d writes, want 1", writer.calls)
	}
	if want := advertisementWire(selectors...); !bytes.Equal(writer.Bytes(), want) {
		t.Fatalf("advertisement wire = %x, want %x", writer.Bytes(), want)
	}

	got, err := ReadSelectors(bytes.NewReader(writer.Bytes()))
	if err != nil {
		t.Fatalf("ReadSelectors: %v", err)
	}
	if !slices.Equal(got, selectors) {
		t.Fatalf("ReadSelectors = %v, want %v", got, selectors)
	}
}

func TestWriteReadEmptyAdvertisement(t *testing.T) {
	var writer bytes.Buffer
	if err := WriteSelectors(&writer, nil); err != nil {
		t.Fatalf("WriteSelectors(empty): %v", err)
	}
	if want := selectorFrame(AdvertisementSelector); !bytes.Equal(writer.Bytes(), want) {
		t.Fatalf("empty advertisement = %x, want header frame %x", writer.Bytes(), want)
	}
	got, err := ReadSelectors(bytes.NewReader(writer.Bytes()))
	if err != nil || len(got) != 0 {
		t.Fatalf("ReadSelectors(empty): selectors=%v err=%v", got, err)
	}
}

func TestReadSelectorsRejectsMissingOrWrongHeader(t *testing.T) {
	for _, test := range []struct {
		name string
		wire []byte
	}{
		{name: "missing header"},
		{name: "selector instead of advertisement header", wire: selectorFrame(1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ReadSelectors(bytes.NewReader(test.wire)); !errors.Is(err, ErrInvalidSelectors) {
				t.Fatalf("ReadSelectors error = %v, want ErrInvalidSelectors", err)
			}
		})
	}
}

func TestReadSelectorsRejectsEOFInsideFrame(t *testing.T) {
	wire := selectorFrame(AdvertisementSelector)
	wire = append(wire, 2, 1)
	if _, err := ReadSelectors(bytes.NewReader(wire)); !errors.Is(err, ErrInvalidSelectors) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadSelectors error = %v, want ErrInvalidSelectors and io.ErrUnexpectedEOF", err)
	}
}

func TestReadSelectorsRejectsInvalidEntries(t *testing.T) {
	for _, test := range []struct {
		name    string
		entries []Selector
		want    error
	}{
		{name: "non-ascending", entries: []Selector{2, 1}, want: ErrInvalidSelectors},
		{name: "duplicate", entries: []Selector{3, 3}, want: ErrInvalidSelectors},
		{name: "advertisement selector reserved as entry", entries: []Selector{AdvertisementSelector}, want: ErrReservedSelector},
		{name: "slash reserved as entry", entries: []Selector{Selector('/')}, want: ErrReservedSelector},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ReadSelectors(bytes.NewReader(advertisementWire(test.entries...))); !errors.Is(err, test.want) {
				t.Fatalf("ReadSelectors error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestReadSelectorsMaxSelectorsBoundary(t *testing.T) {
	selectors := make([]Selector, MaxSelectors)
	for i := range selectors {
		selectors[i] = Selector(i + 1000)
	}
	got, err := ReadSelectors(bytes.NewReader(advertisementWire(selectors...)))
	if err != nil {
		t.Fatalf("ReadSelectors(%d selectors): %v", MaxSelectors, err)
	}
	if len(got) != MaxSelectors {
		t.Fatalf("selector count = %d, want %d", len(got), MaxSelectors)
	}

	tooMany := make([]Selector, MaxSelectors+1)
	for i := range tooMany {
		tooMany[i] = Selector(i + 1000)
	}
	if _, err := ReadSelectors(bytes.NewReader(advertisementWire(tooMany...))); !errors.Is(err, ErrTooManySelectors) {
		t.Fatalf("ReadSelectors(%d selectors) error = %v, want ErrTooManySelectors", len(tooMany), err)
	}
}

func TestWriteSelectorsValidatesBeforeWriting(t *testing.T) {
	for _, test := range []struct {
		name      string
		selectors []Selector
		want      error
	}{
		{name: "duplicate", selectors: []Selector{1, 1}, want: ErrInvalidSelectors},
		{name: "descending", selectors: []Selector{2, 1}, want: ErrInvalidSelectors},
		{name: "advertisement selector reserved", selectors: []Selector{AdvertisementSelector}, want: ErrReservedSelector},
		{name: "slash reserved", selectors: []Selector{Selector('/')}, want: ErrReservedSelector},
	} {
		t.Run(test.name, func(t *testing.T) {
			writer := new(recordingWriter)
			if err := WriteSelectors(writer, test.selectors); !errors.Is(err, test.want) {
				t.Fatalf("WriteSelectors error = %v, want %v", err, test.want)
			}
			if writer.calls != 0 || writer.Len() != 0 {
				t.Fatalf("invalid advertisement wrote %d times and %d bytes", writer.calls, writer.Len())
			}
		})
	}

	tooMany := make([]Selector, MaxSelectors+1)
	for i := range tooMany {
		tooMany[i] = Selector(i + 1000)
	}
	if err := WriteSelectors(io.Discard, tooMany); !errors.Is(err, ErrTooManySelectors) {
		t.Fatalf("WriteSelectors(%d selectors) error = %v, want ErrTooManySelectors", len(tooMany), err)
	}
}

func TestWriteSelectorsReportsShortWrite(t *testing.T) {
	if err := WriteSelectors(shortWriter{}, []Selector{17}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("WriteSelectors error = %v, want io.ErrShortWrite", err)
	}
}

func selectorFrame(selector Selector) []byte {
	payload := binary.AppendUvarint(nil, uint64(selector))
	return AppendFrame(nil, payload)
}

func advertisementWire(selectors ...Selector) []byte {
	wire := selectorFrame(AdvertisementSelector)
	for _, selector := range selectors {
		wire = append(wire, selectorFrame(selector)...)
	}
	return wire
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
