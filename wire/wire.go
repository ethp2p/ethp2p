package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

// ControlSelector identifies the view's control stream.
const ControlSelector Selector = 0

// MaxSelectors bounds the number of selectors in one Hello.
const MaxSelectors = 1024

var (
	// ErrInvalidSelectors indicates a malformed selector frame or list.
	ErrInvalidSelectors = errors.New("invalid protocol selector list")
	// ErrFrameTooLarge indicates that the frame payload exceeds the caller's limit.
	ErrFrameTooLarge = errors.New("frame exceeds maximum length")
)

// Selector identifies an ethp2p stream protocol on the wire.
type Selector uint64

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

	// BadSelector means the stream head was not a valid selector frame.
	BadSelector = Code{value: 8}
	// UnsupportedSelector means the selector is not shared on this connection.
	UnsupportedSelector = Code{value: 9}
	// Closing means the stack view is closing.
	Closing = Code{value: 10}
	// ControlViolation means the control stream protocol was violated.
	ControlViolation = Code{value: 11}
	// NoSharedProtocols means no local protocol accepted the peer.
	NoSharedProtocols = Code{value: 12}
	// Duplicate means another connection to the same peer was kept.
	Duplicate = Code{value: 13}
)

// stackCodeNames names the stack code values defined above.
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

// AppendFrame appends one frame containing payload to dst.
func AppendFrame(dst, payload []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(payload)))
	return append(dst, payload...)
}

// ReadFrame reads one frame without consuming bytes after it. maxLen bounds the
// payload allocation and must not be negative. An EOF before any length-prefix
// byte is returned as io.EOF; a truncated prefix or payload returns
// io.ErrUnexpectedEOF.
func ReadFrame(r io.Reader, maxLen int) ([]byte, error) {
	length, err := binary.ReadUvarint(byteReader{r})
	if err != nil {
		return nil, err
	}
	if length > uint64(maxLen) {
		return nil, ErrFrameTooLarge
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return payload, nil
}

// byteReader reads one byte per call, so ReadUvarint never consumes past the
// length prefix.
type byteReader struct{ io.Reader }

func (r byteReader) ReadByte() (byte, error) {
	var b [1]byte
	_, err := io.ReadFull(r.Reader, b[:])
	return b[0], err
}

// WriteSelector writes one selector frame in a single write. The selected
// protocol applies to the rest of the stream; this function does not close w.
func WriteSelector(w io.Writer, selector Selector) error {
	encoded := binary.AppendUvarint(nil, uint64(selector))
	// io.Writer requires a non-nil error on a short write.
	_, err := w.Write(AppendFrame(nil, encoded))
	return err
}

// ReadSelector reads one selector frame. Selector policy belongs to the caller,
// so this codec operation deliberately accepts reserved values.
func ReadSelector(r io.Reader) (Selector, error) {
	payload, err := ReadFrame(r, binary.MaxVarintLen64)
	if err != nil {
		return 0, err
	}
	// Uvarint accepts overlong encodings; re-encoding keeps only the minimal one.
	value, n := binary.Uvarint(payload)
	if n <= 0 || n != len(payload) || len(binary.AppendUvarint(nil, value)) != n {
		return 0, fmt.Errorf("%w: selector frame %x is not one minimal uvarint", ErrInvalidSelectors, payload)
	}
	return Selector(value), nil
}

// ValidateSelectors requires a strictly ascending list of at most MaxSelectors
// nonzero selectors.
func ValidateSelectors(selectors []Selector) error {
	if len(selectors) > MaxSelectors {
		return fmt.Errorf("%w: maximum is %d", ErrInvalidSelectors, MaxSelectors)
	}
	for i, selector := range selectors {
		if selector == ControlSelector {
			return fmt.Errorf("%w: control selector %d is reserved", ErrInvalidSelectors, ControlSelector)
		}
		if i > 0 && selector <= selectors[i-1] {
			return fmt.Errorf("%w: selectors must be strictly ascending", ErrInvalidSelectors)
		}
	}
	return nil
}
