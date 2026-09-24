package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
)

// ControlSelector identifies the view's control stream.
const ControlSelector Selector = 0

// MaxSelectors bounds the number of selectors in one Hello.
const MaxSelectors = 1024

var (
	ErrReservedSelector = errors.New("reserved protocol selector")
	ErrInvalidSelectors = errors.New("invalid protocol selector list")
	ErrTooManySelectors = errors.New("too many protocol selectors")
	// ErrInvalidFrame indicates that a frame length prefix is not a valid uvarint.
	ErrInvalidFrame = errors.New("invalid frame length prefix")
	// ErrFrameTooLarge indicates that the frame payload exceeds the caller's limit.
	ErrFrameTooLarge = errors.New("frame exceeds maximum length")
)

// Selector identifies an ethp2p stream protocol on the wire.
type Selector uint64

// Code is a stream outcome, scoped to a selector when it is protocol-defined.
// It is comparable; its zero value is [Unspecified].
type Code struct {
	selector Selector
	value    uint64
	protocol bool
}

// Shared stack codes may be sent by either endpoint on any stream.
var (
	// Unspecified means no specific reason, including ordinary cancellation.
	Unspecified = Code{}
	// Refused means the receiver will not process this stream.
	Refused = Code{value: 1}
	// Overloaded means a bounded queue or budget is full.
	Overloaded = Code{value: 2}
	// Timeout means a deadline expired.
	Timeout = Code{value: 3}

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

// Code constructs a protocol-namespace outcome for s. Selector zero cannot
// have protocol-specific outcomes.
func (s Selector) Code(value uint16) Code {
	if s == 0 {
		panic("protocol: selector zero has no protocol outcome codes")
	}
	return Code{selector: s, value: uint64(value), protocol: true}
}

// WireFor returns the QUIC application error code for c on a stream with
// selector sel. A protocol code for another selector panics. Stack-only codes
// are encoded as [Unspecified]; only stack-owned paths may send them.
func (c Code) WireFor(sel Selector) uint64 {
	if c.protocol && c.selector != sel {
		panic(fmt.Sprintf("protocol: code for selector %d sent on selector %d", c.selector, sel))
	}
	if !c.protocol && c.value > 3 {
		return Unspecified.Wire()
	}
	return c.Wire()
}

// Wire returns the QUIC application error code for c, value<<1 | namespace,
// without the checks WireFor applies. It is for the stack's own send paths,
// which may send stack-only codes. Protocols never hold streams that take a
// raw code, so they cannot use it to bypass WireFor.
func (c Code) Wire() uint64 {
	if c.protocol {
		return c.value<<1 | 1
	}
	return c.value << 1
}

// ParseCode decodes a received QUIC application error code on a stream with
// selector sel. Protocol-namespace values remain bound to sel. Unknown stack
// values decode as [Unspecified].
func ParseCode(sel Selector, wire uint64) Code {
	value := wire >> 1
	if wire&1 != 0 {
		return Code{selector: sel, value: value, protocol: true}
	}
	switch value {
	case 0, 1, 2, 3, 8, 9, 10, 11, 12, 13:
		return Code{value: value}
	default:
		return Unspecified
	}
}

// String formats the outcome for errors and logs.
func (c Code) String() string {
	if c.protocol {
		return fmt.Sprintf("protocol selector %d value %d", c.selector, c.value)
	}
	switch c.value {
	case 0:
		return "Unspecified"
	case 1:
		return "Refused"
	case 2:
		return "Overloaded"
	case 3:
		return "Timeout"
	case 8:
		return "BadSelector"
	case 9:
		return "UnsupportedSelector"
	case 10:
		return "Closing"
	case 11:
		return "ControlViolation"
	case 12:
		return "NoSharedProtocols"
	case 13:
		return "Duplicate"
	default:
		return "Unspecified"
	}
}

// AppendFrame appends one frame containing payload to dst.
func AppendFrame(dst, payload []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(payload)))
	return append(dst, payload...)
}

// ReadFrame reads one frame without consuming bytes after it. maxLen bounds the
// payload allocation. An EOF before any length-prefix byte is returned as io.EOF;
// a truncated prefix or payload returns io.ErrUnexpectedEOF.
func ReadFrame(r io.Reader, maxLen int) ([]byte, error) {
	var prefix [binary.MaxVarintLen64]byte
	for i := range prefix {
		if _, err := io.ReadFull(r, prefix[i:i+1]); err != nil {
			if i == 0 && errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, io.EOF
			}
			if i > 0 && errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, fmt.Errorf("read frame length: %w", err)
		}
		if prefix[i]&0x80 != 0 {
			continue
		}

		length, n := binary.Uvarint(prefix[:i+1])
		if n <= 0 {
			return nil, ErrInvalidFrame
		}
		if maxLen < 0 || length > uint64(maxLen) {
			return nil, ErrFrameTooLarge
		}
		payload := make([]byte, int(length))
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, fmt.Errorf("read frame payload: %w", err)
		}
		return payload, nil
	}
	return nil, ErrInvalidFrame
}

// WriteSelector writes one selector frame in a single write. The selected
// protocol applies to the rest of the stream; this function does not close w.
func WriteSelector(w io.Writer, selector Selector) error {
	encoded := binary.AppendUvarint(nil, uint64(selector))
	frame := AppendFrame(nil, encoded)
	n, err := w.Write(frame)
	if err == nil && n != len(frame) {
		return io.ErrShortWrite
	}
	return err
}

// ReadSelector reads one selector frame. Selector policy belongs to the caller,
// so this codec operation deliberately accepts reserved values.
func ReadSelector(r io.Reader) (Selector, error) {
	selector, err := readSelectorFrame(r)
	if err != nil {
		return 0, invalidSelectors(err)
	}
	return selector, nil
}

// Canonical returns selectors in ascending order without duplicates. The
// returned slice does not share backing storage with selectors.
func Canonical(selectors []Selector) []Selector {
	canonical := slices.Clone(selectors)
	slices.Sort(canonical)
	return slices.Compact(canonical)
}

// Intersect returns the common selectors in ascending order. a and b must
// already be canonical (ascending and duplicate-free).
func Intersect(a, b []Selector) []Selector {
	intersection := make([]Selector, 0, min(len(a), len(b)))
	for i, j := 0, 0; i < len(a) && j < len(b); {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			intersection = append(intersection, a[i])
			i++
			j++
		}
	}
	return intersection
}

// ValidateSelector rejects the reserved control selector as a protocol selector.
func ValidateSelector(selector Selector) error {
	switch selector {
	case ControlSelector:
		return fmt.Errorf("%w: control selector %d", ErrReservedSelector, ControlSelector)
	default:
		return nil
	}
}

// ValidateSelectors requires a strictly ascending list of at most MaxSelectors
// nonzero selectors.
func ValidateSelectors(selectors []Selector) error {
	if len(selectors) > MaxSelectors {
		return fmt.Errorf("%w: maximum is %d", ErrTooManySelectors, MaxSelectors)
	}
	for i, selector := range selectors {
		if err := ValidateSelector(selector); err != nil {
			return err
		}
		if i > 0 && selector <= selectors[i-1] {
			return fmt.Errorf("%w: selectors must be strictly ascending", ErrInvalidSelectors)
		}
	}
	return nil
}

func readSelectorFrame(r io.Reader) (Selector, error) {
	payload, err := ReadFrame(r, binary.MaxVarintLen64)
	if err != nil {
		return 0, err
	}
	if len(payload) == 0 {
		return 0, fmt.Errorf("%w: empty selector frame", ErrInvalidSelectors)
	}
	value, n := binary.Uvarint(payload)
	if n <= 0 || n != len(payload) {
		return 0, fmt.Errorf("%w: selector encoding length %d, frame length %d", ErrInvalidSelectors, n, len(payload))
	}
	var canonical [binary.MaxVarintLen64]byte
	if encodedLen := binary.PutUvarint(canonical[:], value); encodedLen != len(payload) {
		return 0, fmt.Errorf("%w: selector encoding is not minimal", ErrInvalidSelectors)
	}
	return Selector(value), nil
}

func invalidSelectors(err error) error {
	if errors.Is(err, ErrInvalidSelectors) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrInvalidSelectors, err)
}
