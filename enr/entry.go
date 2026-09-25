package enr

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
)

// Entry describes a record entry: its key and the codec for its value, which
// has type T. Get decodes a value only when requested.
type Entry[T any] struct {
	key    string
	encode func(T) ([]byte, error)
	decode func([]byte) (T, error)
}

// Pair is a key/value pair created with Entry.Set, for Sign and Update.
type Pair struct {
	key   string
	value []byte
	err   error
}

// BytesEntry describes an entry under key whose value is an RLP byte string.
// Get returns a copy, so callers cannot mutate a record through it.
func BytesEntry(key string) Entry[[]byte] {
	return Entry[[]byte]{
		key: key,
		encode: func(value []byte) ([]byte, error) {
			return encodeRLPString(value), nil
		},
		decode: func(raw []byte) ([]byte, error) {
			item, err := decodeSingleRLP(raw)
			if err != nil {
				return nil, err
			}
			if item.kind != rlpString {
				return nil, errors.New("value is not an RLP byte string")
			}
			return append([]byte(nil), item.payload...), nil
		},
	}
}

// UintEntry describes an entry under key whose value is an unsigned RLP
// integer.
func UintEntry(key string) Entry[uint64] {
	return Entry[uint64]{
		key: key,
		encode: func(value uint64) ([]byte, error) {
			return encodeUint64(value), nil
		},
		decode: func(raw []byte) (uint64, error) {
			item, err := decodeSingleRLP(raw)
			if err != nil {
				return 0, err
			}
			return decodeUint(item)
		},
	}
}

// IP is the "ip" entry, encoded as exactly four IPv4 bytes.
var IP = Entry[netip.Addr]{
	key: "ip",
	encode: func(addr netip.Addr) ([]byte, error) {
		if !addr.Is4() {
			return nil, errors.New("ip must be an IPv4 address")
		}
		value := addr.As4()
		return encodeRLPString(value[:]), nil
	},
	decode: func(raw []byte) (netip.Addr, error) {
		item, err := decodeSingleRLP(raw)
		if err != nil {
			return netip.Addr{}, err
		}
		if item.kind != rlpString || len(item.payload) != 4 {
			return netip.Addr{}, errors.New("ip must contain exactly four bytes")
		}
		var value [4]byte
		copy(value[:], item.payload)
		return netip.AddrFrom4(value), nil
	},
}

// IP6 is the "ip6" entry, encoded as exactly sixteen IPv6 bytes. IPv4-mapped
// addresses are rejected.
var IP6 = Entry[netip.Addr]{
	key: "ip6",
	encode: func(addr netip.Addr) ([]byte, error) {
		if !addr.Is6() || addr.Is4In6() {
			return nil, errors.New("ip6 must be a non-mapped IPv6 address")
		}
		value := addr.As16()
		return encodeRLPString(value[:]), nil
	},
	decode: func(raw []byte) (netip.Addr, error) {
		item, err := decodeSingleRLP(raw)
		if err != nil {
			return netip.Addr{}, err
		}
		if item.kind != rlpString || len(item.payload) != 16 {
			return netip.Addr{}, errors.New("ip6 must contain exactly sixteen bytes")
		}
		var value [16]byte
		copy(value[:], item.payload)
		addr := netip.AddrFrom16(value)
		if addr.Is4In6() {
			return netip.Addr{}, errors.New("ip6 must not be IPv4-mapped")
		}
		return addr, nil
	},
}

// UDP is the "udp" entry, encoded as a uint16 port.
var UDP = uint16Entry("udp")

// UDP6 is the "udp6" entry, encoded as a uint16 port.
var UDP6 = uint16Entry("udp6")

// TCP is the "tcp" entry, encoded as a uint16 port.
var TCP = uint16Entry("tcp")

// TCP6 is the "tcp6" entry, encoded as a uint16 port.
var TCP6 = uint16Entry("tcp6")

// QUIC is the "quic" entry, encoded as a uint16 port.
var QUIC = uint16Entry("quic")

// QUIC6 is the "quic6" entry, encoded as a uint16 port.
var QUIC6 = uint16Entry("quic6")

// Get returns the value of entry in r. ok is false when the key is absent;
// err is non-nil when it is present but its value is malformed.
func (r *Record) Get[T any](entry Entry[T]) (value T, ok bool, err error) {
	raw, ok := r.values[entry.key]
	if !ok {
		return value, false, nil
	}
	value, err = entry.decode(raw)
	if err != nil {
		return value, true, fmt.Errorf("decode ENR key %q: %w", entry.key, err)
	}
	return value, true, nil
}

// Set pairs the entry's key with value, for Sign and Update. Encoding errors,
// such as an invalid address, are returned by Sign and Update.
func (e Entry[T]) Set(value T) Pair {
	encoded, err := e.encode(value)
	return Pair{key: e.key, value: encoded, err: err}
}

func uint16Entry(key string) Entry[uint16] {
	return Entry[uint16]{
		key: key,
		encode: func(value uint16) ([]byte, error) {
			return encodeUint64(uint64(value)), nil
		},
		decode: func(raw []byte) (uint16, error) {
			item, err := decodeSingleRLP(raw)
			if err != nil {
				return 0, err
			}
			value, err := decodeUint(item)
			if err != nil {
				return 0, err
			}
			if value > math.MaxUint16 {
				return 0, fmt.Errorf("port %d exceeds uint16", value)
			}
			return uint16(value), nil
		},
	}
}
