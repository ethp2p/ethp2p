package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
)

// AdvertisementSelector identifies the selector advertisement stream.
const AdvertisementSelector Selector = 0

// MaxSelectors bounds the number of selectors in one connection advertisement.
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
	if err := ValidateSelector(selector); err != nil {
		return err
	}
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

// WriteSelectors writes an advertisement stream in one write: the reserved
// advertisement selector frame followed by the strictly ascending selector
// frames. It validates the complete list before writing anything and never
// closes w.
func WriteSelectors(w io.Writer, selectors []Selector) error {
	if err := validateSelectors(selectors); err != nil {
		return err
	}

	encoded := AppendFrame(nil, binary.AppendUvarint(nil, uint64(AdvertisementSelector)))
	for _, selector := range selectors {
		encoded = AppendFrame(encoded, binary.AppendUvarint(nil, uint64(selector)))
	}
	n, err := w.Write(encoded)
	if err == nil && n != len(encoded) {
		return io.ErrShortWrite
	}
	return err
}

// ReadSelectors reads an advertisement stream. The first frame must contain
// AdvertisementSelector; subsequent selector frames end at a clean EOF (FIN).
// Entries must be strictly ascending, within MaxSelectors, and valid for stream
// use. EOF inside a frame is an error.
func ReadSelectors(r io.Reader) ([]Selector, error) {
	header, err := readSelectorFrame(r)
	if err != nil {
		return nil, invalidSelectors(err)
	}
	if header != AdvertisementSelector {
		return nil, fmt.Errorf("%w: advertisement starts with selector %d, want %d", ErrInvalidSelectors, header, AdvertisementSelector)
	}

	selectors := make([]Selector, 0)
	for {
		selector, err := readSelectorFrame(r)
		if err == io.EOF {
			return selectors, nil
		}
		if err != nil {
			return nil, invalidSelectors(err)
		}
		if len(selectors) >= MaxSelectors {
			return nil, fmt.Errorf("%w: maximum is %d", ErrTooManySelectors, MaxSelectors)
		}
		if err := ValidateSelector(selector); err != nil {
			return nil, err
		}
		if len(selectors) > 0 && selector <= selectors[len(selectors)-1] {
			return nil, fmt.Errorf("%w: selectors must be strictly ascending", ErrInvalidSelectors)
		}
		selectors = append(selectors, selector)
	}
}

// ValidateSelector rejects the reserved advertisement selector and '/' as
// stream selectors. '/' is the first byte of libp2p multistream-select frames.
func ValidateSelector(selector Selector) error {
	switch selector {
	case AdvertisementSelector:
		return fmt.Errorf("%w: advertisement selector %d", ErrReservedSelector, AdvertisementSelector)
	case Selector('/'):
		return fmt.Errorf("%w: %d conflicts with libp2p", ErrReservedSelector, selector)
	default:
		return nil
	}
}

func validateSelectors(selectors []Selector) error {
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
