package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"testing"
)

func TestCodeWireMapping(t *testing.T) {
	for _, test := range []struct {
		name   string
		code   Code
		wire   uint64
		parsed Code
	}{
		{name: "Unspecified", code: Unspecified, wire: 0, parsed: Unspecified},
		{name: "Refused", code: Refused, wire: 2, parsed: Refused},
		{name: "Timeout", code: Timeout, wire: 4, parsed: Timeout},
		{name: "BadHeader", code: BadHeader, wire: 16, parsed: BadHeader},
		{name: "UnsupportedProtocol", code: UnsupportedProtocol, wire: 18, parsed: UnsupportedProtocol},
		{name: "Closing", code: Closing, wire: 20, parsed: Closing},
		{name: "ControlViolation", code: ControlViolation, wire: 22, parsed: ControlViolation},
		{name: "NoSharedProtocols", code: NoSharedProtocols, wire: 24, parsed: NoSharedProtocols},
		{name: "Duplicate", code: Duplicate, wire: 26, parsed: Duplicate},
		{name: "gap 3", code: Code{value: 3}, wire: 6, parsed: Unspecified},
		{name: "gap 4", code: Code{value: 4}, wire: 8, parsed: Unspecified},
		{name: "gap 5", code: Code{value: 5}, wire: 10, parsed: Unspecified},
		{name: "gap 6", code: Code{value: 6}, wire: 12, parsed: Unspecified},
		{name: "gap 7", code: Code{value: 7}, wire: 14, parsed: Unspecified},
		{name: "gap 14", code: Code{value: 14}, wire: 28, parsed: Unspecified},
		{name: "large unknown stack value", code: Code{value: 1 << 19}, wire: 1 << 20, parsed: Unspecified},
		{name: "protocol value zero", code: ProtocolCode(0), wire: 1, parsed: ProtocolCode(0)},
		{name: "protocol value one", code: ProtocolCode(1), wire: 3, parsed: ProtocolCode(1)},
		{name: "unknown protocol value preserved", code: ProtocolCode(99), wire: 199, parsed: ProtocolCode(99)},
		{name: "maximum protocol value", code: ProtocolCode(^uint16(0)), wire: uint64(^uint16(0))<<1 | 1, parsed: ProtocolCode(^uint16(0))},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.code.Wire(); got != test.wire {
				t.Fatalf("Wire(%s) = %d, want %d", test.code, got, test.wire)
			}
			if got := ParseCode(test.wire); got != test.parsed {
				t.Fatalf("ParseCode(%d) = %s, want %s", test.wire, got, test.parsed)
			}
		})
	}
}

func TestFrameRoundTrips(t *testing.T) {
	for _, test := range []struct {
		name    string
		typ     FrameType
		payload []byte
		maxLen  int
	}{
		{name: "empty", maxLen: 0},
		{name: "one byte", typ: 1, payload: []byte{0x2a}, maxLen: 1},
		{name: "127 bytes", typ: 2, payload: bytes.Repeat([]byte{0xa5}, 127), maxLen: 127},
		{name: "128 bytes", payload: bytes.Repeat([]byte{0x5a}, 128), maxLen: 128},
		{name: "maxLen boundary", typ: 3, payload: []byte{1, 2, 3}, maxLen: 3},
		{name: "two-byte type", typ: 300, payload: []byte{7}, maxLen: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			wire := AppendFrame(nil, test.typ, test.payload)
			typ, got, err := ReadFrame(bytes.NewReader(wire), test.maxLen)
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			if typ != test.typ || !bytes.Equal(got, test.payload) {
				t.Fatalf("frame = (%d, %d bytes), want (%d, %d bytes)", typ, len(got), test.typ, len(test.payload))
			}
		})
	}
}

func TestAppendFrameLayout(t *testing.T) {
	// Type 1, then the payload prefixed by its length.
	if got, want := AppendFrame(nil, 1, []byte{0xaa, 0xbb, 0xcc}), []byte{1, 3, 0xaa, 0xbb, 0xcc}; !bytes.Equal(got, want) {
		t.Fatalf("AppendFrame = %x, want %x", got, want)
	}
}

func TestReadFrameDoesNotReadPastFrame(t *testing.T) {
	const following = "next frame"
	wire := AppendFrame(nil, 4, []byte("one frame"))
	wire = append(wire, following...)
	reader := bytes.NewReader(wire)
	if _, _, err := ReadFrame(reader, 32); err != nil {
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
	oversizeReader := countingReader{data: []byte{0, 2, 0xaa, 0xbb}}
	oversizeAllocations := testing.AllocsPerRun(100, func() {
		oversizeReader.offset = 0
		_, _, err := ReadFrame(&oversizeReader, 1)
		if err != ErrFrameTooLarge {
			panic("ReadFrame did not return ErrFrameTooLarge")
		}
	})
	validReader := countingReader{data: []byte{0, 1, 0xaa}}
	validAllocations := testing.AllocsPerRun(100, func() {
		validReader.offset = 0
		if _, _, err := ReadFrame(&validReader, 1); err != nil {
			panic("ReadFrame rejected a frame at maxLen")
		}
	})
	if oversizeAllocations >= validAllocations {
		t.Fatalf("oversize rejection allocations = %v, valid frame allocations = %v; payload allocation was not avoided", oversizeAllocations, validAllocations)
	}
	if oversizeReader.offset != 2 {
		t.Fatalf("oversize read consumed %d bytes, want only the type and length", oversizeReader.offset)
	}
}

func TestReadFrameRejectsTruncatedAndInvalidFrames(t *testing.T) {
	for _, test := range []struct {
		name string
		wire []byte
		want error
	}{
		{name: "empty", wire: nil, want: io.EOF},
		{name: "truncated type", wire: []byte{0x80}, want: io.ErrUnexpectedEOF},
		{name: "missing length", wire: []byte{1}, want: io.ErrUnexpectedEOF},
		{name: "truncated length", wire: []byte{1, 0x80}, want: io.ErrUnexpectedEOF},
		{name: "empty truncated payload", wire: []byte{1, 2}, want: io.ErrUnexpectedEOF},
		{name: "partial truncated payload", wire: []byte{1, 2, 0xaa}, want: io.ErrUnexpectedEOF},
		{name: "non-minimal type", wire: []byte{0x81, 0x00, 0}, want: ErrFrameEncoding},
		{name: "non-minimal zero length", wire: []byte{1, 0x80, 0x00}, want: ErrFrameEncoding},
		{name: "non-minimal length", wire: []byte{1, 0x81, 0x00, 0xaa}, want: ErrFrameEncoding},
		{name: "overflowing type", wire: bytes.Repeat([]byte{0x80}, binary.MaxVarintLen64), want: ErrFrameEncoding},
		{name: "overflowing length", wire: append([]byte{1}, bytes.Repeat([]byte{0x80}, binary.MaxVarintLen64)...), want: ErrFrameEncoding},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := ReadFrame(bytes.NewReader(test.wire), 32)
			if err == nil || test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("ReadFrame error = %v, want %v", err, test.want)
			}
			if test.want == io.EOF && err != io.EOF {
				t.Fatalf("ReadFrame error = %v, want io.EOF itself", err)
			}
		})
	}
}

func TestStreamHeadersRoundTrip(t *testing.T) {
	for _, test := range []struct {
		name     string
		protocol Protocol
		wire     []byte
	}{
		{name: "control", protocol: 0, wire: []byte{0x00}},
		{name: "one", protocol: 1, wire: []byte{0x01}},
		{name: "slash", protocol: '/', wire: []byte{0x2f}},
		{name: "two-byte field", protocol: 300, wire: []byte{0xac, 0x02}},
		{name: "protocol 18", protocol: 18, wire: []byte{0x12}},
		{name: "protocol 20", protocol: 20, wire: []byte{0x14}},
		{name: "maximum", protocol: math.MaxUint32,
			wire: []byte{0xff, 0xff, 0xff, 0xff, 0x0f}},
	} {
		t.Run(test.name, func(t *testing.T) {
			writer := new(recordingWriter)
			if err := WriteHeader(writer, test.protocol); err != nil {
				t.Fatalf("WriteHeader(%v): %v", test.protocol, err)
			}
			if writer.calls != 1 {
				t.Fatalf("WriteHeader made %d writes, want 1", writer.calls)
			}
			if !bytes.Equal(writer.Bytes(), test.wire) {
				t.Fatalf("stream header = %x, want %x", writer.Bytes(), test.wire)
			}

			reader := bytes.NewReader(append(bytes.Clone(test.wire), 0xaa))
			got, err := ReadHeader(reader)
			if err != nil || got != test.protocol {
				t.Fatalf("ReadHeader = (%v, %v), want (%v, nil)", got, err, test.protocol)
			}
			if b, err := reader.ReadByte(); err != nil || b != 0xaa {
				t.Fatalf("byte after header = (%x, %v), want 0xaa", b, err)
			}

			sel, n, err := ParseHeader(append(bytes.Clone(test.wire), 0xaa))
			if err != nil || sel != test.protocol || n != len(test.wire) {
				t.Fatalf("ParseHeader = (%v, %d, %v), want (%v, %d, nil)", sel, n, err, test.protocol, len(test.wire))
			}
			for i := range len(test.wire) {
				if sel, n, err := ParseHeader(test.wire[:i]); err != nil || n != 0 {
					t.Fatalf("ParseHeader(%x) = (%v, %d, %v), want incomplete", test.wire[:i], sel, n, err)
				}
			}
		})
	}
	if len(AppendHeader(nil, math.MaxUint32)) != MaxHeaderLen {
		t.Fatalf("largest header is not MaxHeaderLen bytes")
	}
}

func TestReadHeaderRejectsInvalidHeaders(t *testing.T) {
	for _, test := range []struct {
		name string
		wire []byte
		want error
	}{
		{name: "protocol 19", wire: []byte{0x13}, want: errInvalidHeader},
		{name: "multistream-select header", wire: []byte{0x13, '/', 'm'}, want: errInvalidHeader},
		{name: "non-minimal protocol", wire: []byte{0x80, 0x00}, want: errInvalidHeader},
		{name: "non-minimal protocol 19", wire: []byte{0x93, 0x00}, want: errInvalidHeader},
		{name: "protocol above 32 bits", wire: []byte{0x80, 0x80, 0x80, 0x80, 0x10}, want: errInvalidHeader},
		{name: "protocol longer than five bytes", wire: []byte{0x80, 0x80, 0x80, 0x80, 0x80}, want: errInvalidHeader},
		{name: "empty", wire: nil, want: io.EOF},
		{name: "truncated protocol", wire: []byte{0x80}, want: io.ErrUnexpectedEOF},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ReadHeader(bytes.NewReader(test.wire)); !errors.Is(err, test.want) {
				t.Fatalf("ReadHeader error = %v, want %v", err, test.want)
			}
			_, n, err := ParseHeader(test.wire)
			if test.want == errInvalidHeader {
				if !errors.Is(err, errInvalidHeader) {
					t.Fatalf("ParseHeader error = %v, want %v", err, errInvalidHeader)
				}
			} else if err != nil || n != 0 {
				t.Fatalf("ParseHeader = (%d, %v), want incomplete", n, err)
			}
		})
	}
}

func TestParseHeaderRejectsProtocol19FromFirstByte(t *testing.T) {
	// A classifier peeking one byte at a time must see the rejection at once.
	if _, _, err := ParseHeader([]byte{0x13}); !errors.Is(err, errInvalidHeader) {
		t.Fatalf("ParseHeader(13) error = %v, want %v", err, errInvalidHeader)
	}
}

func TestReadHeaderDoesNotReadPastHeader(t *testing.T) {
	reader := &countingReader{data: []byte{0x01, 0x02, 0xaa, 0xbb}}
	if _, err := ReadHeader(reader); err != nil {
		t.Fatal(err)
	}
	if reader.offset != 1 {
		t.Fatalf("ReadHeader consumed %d bytes, want 1", reader.offset)
	}
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
