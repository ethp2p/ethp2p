package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
)

// MaxSelectors bounds the number of selectors in one connection advertisement.
const MaxSelectors = 1024

var (
	ErrReservedSelector = errors.New("reserved protocol selector")
	ErrInvalidSelectors = errors.New("invalid protocol selector list")
	ErrTooManySelectors = errors.New("too many protocol selectors")
)

// Selector identifies an ethp2p stream protocol on the wire.
type Selector uint64

// WriteSelector writes one unsigned-varint selector. The selected protocol
// applies to the rest of the stream; this function does not add a length
// prefix or close w.
func WriteSelector(w io.Writer, selector Selector) error {
	if err := validateSelector(selector); err != nil {
		return err
	}

	encoded := binary.AppendUvarint(nil, uint64(selector))
	n, err := w.Write(encoded)
	if err == nil && n != len(encoded) {
		return io.ErrShortWrite
	}
	return err
}

// ReadSelector reads one unsigned-varint selector. Selector policy is resolved
// by the caller, so this codec operation deliberately accepts reserved values.
func ReadSelector(r io.ByteReader) (Selector, error) {
	value, err := binary.ReadUvarint(r)
	return Selector(value), err
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

// WriteSelectors writes a strictly ascending, duplicate-free selector list as
// consecutive unsigned varints. It validates the complete list before writing
// anything, and never closes w.
func WriteSelectors(w io.Writer, selectors []Selector) error {
	if err := validateSelectors(selectors); err != nil {
		return err
	}
	if len(selectors) == 0 {
		return nil
	}

	encoded := make([]byte, 0, len(selectors)*binary.MaxVarintLen64)
	for _, selector := range selectors {
		encoded = binary.AppendUvarint(encoded, uint64(selector))
	}
	n, err := w.Write(encoded)
	if err == nil && n != len(encoded) {
		return io.ErrShortWrite
	}
	return err
}

// ReadSelectors reads consecutive unsigned-varint selectors until a clean EOF
// marks the end of the advertisement. Advertisements must be strictly
// ascending, duplicate-free, within MaxSelectors, and must not contain values
// reserved by the stream protocol.
func ReadSelectors(r io.ByteReader) ([]Selector, error) {
	selectors := make([]Selector, 0)
	for {
		value, err := binary.ReadUvarint(r)
		// Only an exact EOF denotes a clean end; ReadUvarint may propagate
		// a wrapped EOF after consuming part of an unterminated varint.
		if err == io.EOF {
			return selectors, nil
		}
		if err != nil {
			return nil, err
		}
		if len(selectors) >= MaxSelectors {
			return nil, fmt.Errorf("%w: maximum is %d", ErrTooManySelectors, MaxSelectors)
		}

		selector := Selector(value)
		if err := validateSelector(selector); err != nil {
			return nil, err
		}
		if len(selectors) > 0 && selector <= selectors[len(selectors)-1] {
			return nil, fmt.Errorf("%w: selectors must be strictly ascending", ErrInvalidSelectors)
		}
		selectors = append(selectors, selector)
	}
}

func validateSelector(selector Selector) error {
	switch selector {
	case 0:
		return fmt.Errorf("%w: 0", ErrReservedSelector)
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
		if err := validateSelector(selector); err != nil {
			return err
		}
		if i > 0 && selector <= selectors[i-1] {
			return fmt.Errorf("%w: selectors must be strictly ascending", ErrInvalidSelectors)
		}
	}
	return nil
}
