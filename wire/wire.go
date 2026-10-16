package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
)

// ReservedFamily is never assigned: its stream header would begin with 0x13,
// the first byte of every multistream-select header, which is how a receiver
// tells libp2p streams apart.
const ReservedFamily Family = 19

// MaxHeaderLen is the longest stream header: two uvarints of at most 32 bits,
// five bytes each.
const MaxHeaderLen = 10

const maxFieldLen = 5

var (
	errInvalidHeader = errors.New("invalid stream header")
	// ErrFrameTooLarge indicates that the frame payload exceeds the caller's limit.
	ErrFrameTooLarge = errors.New("frame exceeds maximum length")
	errFrameLength   = errors.New("frame type or length is not a minimal uvarint")
)

// Family numbers a set of protocols that nodes serve together.
type Family uint32

// Protocol numbers a protocol within its family.
type Protocol uint32

// Selector names an ethp2p stream protocol on the wire: its family and its
// number within that family.
type Selector struct {
	Family   Family
	Protocol Protocol
}

// String formats the selector as family/protocol.
func (s Selector) String() string {
	return fmt.Sprintf("%d/%d", s.Family, s.Protocol)
}

// FrameType numbers a frame type within its protocol.
type FrameType uint64

// Code is a stream outcome, stack or protocol. It is comparable; its
// zero value is [Unspecified].
type Code struct {
	value    uint64
	protocol bool
}

// Shared stack codes may be sent by either endpoint on any stream.
var (
	// Unspecified means no specific reason, including ordinary cancellation.
	Unspecified = Code{}
	// Refused means the receiver will not process this stream.
	Refused = Code{value: 1}
	// Timeout means a deadline expired.
	Timeout = Code{value: 2}

	// BadSelector means the stream header was invalid.
	BadSelector = Code{value: 8}
	// UnsupportedSelector means the stream's family is not shared on the view,
	// or its protocol is not one the family serves.
	UnsupportedSelector = Code{value: 9}
	// Closing means the stack view is closing.
	Closing = Code{value: 10}
	// ControlViolation means the control stream protocol was violated.
	ControlViolation = Code{value: 11}
	// NoSharedProtocols means no family is shared on the view, or no local
	// family still holds it.
	NoSharedProtocols = Code{value: 12}
	// Duplicate means another connection to the same peer was kept.
	Duplicate = Code{value: 13}
)

var stackCodeNames = map[uint64]string{
	0: "Unspecified", 1: "Refused", 2: "Timeout",
	8: "BadSelector", 9: "UnsupportedSelector", 10: "Closing",
	11: "ControlViolation", 12: "NoSharedProtocols", 13: "Duplicate",
}

// ProtocolCode returns a protocol-namespace outcome code for value. It
// carries no selector binding; the stream it is sent on determines where it
// applies.
func ProtocolCode(value uint16) Code {
	return Code{value: uint64(value), protocol: true}
}

// Wire returns the QUIC application error code for c, value<<1 | namespace.
// It is the only encoding.
func (c Code) Wire() uint64 {
	if c.protocol {
		return c.value<<1 | 1
	}
	return c.value << 1
}

// TimeoutOr returns Timeout when err is or wraps a net.Error reporting a
// timeout, and other otherwise. context.DeadlineExceeded,
// os.ErrDeadlineExceeded and QUIC idle and stream deadline errors all are.
func TimeoutOr(err error, other Code) Code {
	if t, ok := errors.AsType[net.Error](err); ok && t.Timeout() {
		return Timeout
	}
	return other
}

// ParseCode decodes a received QUIC application error code.
// Protocol-namespace values are preserved. Unknown stack values decode as
// [Unspecified].
func ParseCode(raw uint64) Code {
	value := raw >> 1
	if raw&1 != 0 {
		return Code{value: value, protocol: true}
	}
	if _, ok := stackCodeNames[value]; !ok {
		return Unspecified
	}
	return Code{value: value}
}

// String formats the outcome for errors and logs.
func (c Code) String() string {
	if c.protocol {
		return fmt.Sprintf("protocol value %d", c.value)
	}
	// Codes are built only from the named values or by ParseCode, which maps
	// unknown stack values to Unspecified, so every stack Code has a name.
	return stackCodeNames[c.value]
}

// AppendFrame appends one frame of type typ containing payload to dst.
func AppendFrame(dst []byte, typ FrameType, payload []byte) []byte {
	return append(AppendFrameHead(dst, typ, len(payload)), payload...)
}

// AppendFrameHead appends the type and length of a frame whose payload of
// length bytes the caller writes next, so a large payload need not be copied.
func AppendFrameHead(dst []byte, typ FrameType, length int) []byte {
	dst = binary.AppendUvarint(dst, uint64(typ))
	return binary.AppendUvarint(dst, uint64(length))
}

// ReadFrame reads one frame without consuming bytes after it. maxLen bounds the
// payload allocation and must not be negative. An EOF before the first byte is
// returned as io.EOF; a truncated type, length or payload returns
// io.ErrUnexpectedEOF; a type or length that is not a minimal uvarint returns
// errFrameLength. The caller decides which types its protocol accepts.
func ReadFrame(r io.Reader, maxLen int) (FrameType, []byte, error) {
	typ, err := readMinimalUvarint(r)
	if err != nil {
		return 0, nil, err
	}
	length, err := readMinimalUvarint(r)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}
	if length > uint64(maxLen) {
		return 0, nil, ErrFrameTooLarge
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}
	return FrameType(typ), payload, nil
}

// readMinimalUvarint reads one uvarint a byte at a time, so it never consumes
// past it, and rejects overlong encodings such as 80 00 for 0, which
// ReadUvarint accepts.
func readMinimalUvarint(r io.Reader) (uint64, error) {
	br := &byteReader{Reader: r}
	value, err := binary.ReadUvarint(br)
	if err != nil {
		return 0, err
	}
	if br.n != len(binary.AppendUvarint(nil, value)) {
		return 0, errFrameLength
	}
	return value, nil
}

type byteReader struct {
	io.Reader
	n int
}

func (r *byteReader) ReadByte() (byte, error) {
	var b [1]byte
	_, err := io.ReadFull(r.Reader, b[:])
	if err == nil {
		r.n++
	}
	return b[0], err
}

// AppendSelector appends the stream header for selector to dst.
func AppendSelector(dst []byte, selector Selector) []byte {
	dst = binary.AppendUvarint(dst, uint64(selector.Family))
	return binary.AppendUvarint(dst, uint64(selector.Protocol))
}

// WriteSelector writes one stream header in a single write. The selected
// protocol applies to the rest of the stream; this function does not close w.
func WriteSelector(w io.Writer, selector Selector) error {
	// io.Writer requires a non-nil error on a short write.
	_, err := w.Write(AppendSelector(nil, selector))
	return err
}

// ParseSelector parses the stream header at the start of b. It returns the
// header's length, or 0 and a nil error when b holds only part of a valid
// header, so a caller can peek one byte more and retry. It rejects a field that
// is not a minimal uvarint or exceeds 32 bits, and family 19 as soon as its one
// byte is seen. Whether the selector is served is the caller's policy.
func ParseSelector(b []byte) (Selector, int, error) {
	family, n, err := parseField(b)
	if n == 0 || err != nil {
		return Selector{}, 0, err
	}
	if Family(family) == ReservedFamily {
		return Selector{}, 0, fmt.Errorf("%w: family %d is reserved", errInvalidHeader, ReservedFamily)
	}
	protocol, m, err := parseField(b[n:])
	if m == 0 || err != nil {
		return Selector{}, 0, err
	}
	return Selector{Family: Family(family), Protocol: Protocol(protocol)}, n + m, nil
}

// parseField parses one header field: a minimal uvarint of at most 32 bits.
// It returns 0 and a nil error when b ends inside a field that is valid so far.
func parseField(b []byte) (uint32, int, error) {
	var value uint64
	for i := range min(len(b), maxFieldLen) {
		value |= uint64(b[i]&0x7f) << (7 * i)
		if b[i] < 0x80 {
			if i > 0 && b[i] == 0 {
				return 0, 0, fmt.Errorf("%w: field is not a minimal uvarint", errInvalidHeader)
			}
			if value > math.MaxUint32 {
				return 0, 0, fmt.Errorf("%w: field exceeds 32 bits", errInvalidHeader)
			}
			return uint32(value), i + 1, nil
		}
	}
	if len(b) >= maxFieldLen {
		return 0, 0, fmt.Errorf("%w: field exceeds 32 bits", errInvalidHeader)
	}
	return 0, 0, nil
}

// ReadSelector reads one stream header a byte at a time, so it never consumes
// past it. An EOF before the first byte is returned as io.EOF and a truncated
// header as io.ErrUnexpectedEOF.
func ReadSelector(r io.Reader) (Selector, error) {
	var head [MaxHeaderLen]byte
	for i := range head {
		if _, err := io.ReadFull(r, head[i:i+1]); err != nil {
			if err == io.EOF && i > 0 {
				err = io.ErrUnexpectedEOF
			}
			return Selector{}, err
		}
		selector, n, err := ParseSelector(head[:i+1])
		if err != nil || n != 0 {
			return selector, err
		}
	}
	// Two complete fields fit in MaxHeaderLen bytes, so ParseSelector has
	// returned a header or an error before the loop ends.
	panic("unreachable")
}
