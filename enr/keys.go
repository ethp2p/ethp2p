package enr

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
)

// Key is a typed ENR key. Get decodes its value only when requested.
type Key[T any] struct {
	name   string
	encode func(T) ([]byte, error)
	decode func([]byte) (T, error)
}

// Entry is a typed key/value pair created with Set for a signed record.
type Entry struct {
	name   string
	encode func() ([]byte, error)
	err    error
}

// BytesKey creates a key whose value is an RLP byte string. Returned values
// are copied so callers cannot mutate a record through Get.
func BytesKey(name string) Key[[]byte] {
	return Key[[]byte]{
		name: name,
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

// UintKey creates a key whose value is an unsigned RLP integer.
func UintKey(name string) Key[uint64] {
	return Key[uint64]{
		name: name,
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

// IP is the "ip" ENR key, encoded as exactly four IPv4 bytes.
var IP = Key[netip.Addr]{
	name: "ip",
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

// IP6 is the "ip6" ENR key, encoded as exactly sixteen IPv6 bytes. IPv4-mapped
// addresses are rejected.
var IP6 = Key[netip.Addr]{
	name: "ip6",
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

// UDP is the "udp" ENR key, encoded as a uint16 port.
var UDP = uint16Key("udp")

// UDP6 is the "udp6" ENR key, encoded as a uint16 port.
var UDP6 = uint16Key("udp6")

// TCP is the "tcp" ENR key, encoded as a uint16 port.
var TCP = uint16Key("tcp")

// TCP6 is the "tcp6" ENR key, encoded as a uint16 port.
var TCP6 = uint16Key("tcp6")

// QUIC is the "quic" ENR key, encoded as a uint16 port.
var QUIC = uint16Key("quic")

// QUIC6 is the "quic6" ENR key, encoded as a uint16 port.
var QUIC6 = uint16Key("quic6")

// Get returns the value for k. ok is false when the key is absent; err is
// non-nil when the key is present but malformed for k's type.
func Get[T any](record *Record, key Key[T]) (value T, ok bool, err error) {
	if record == nil {
		return value, false, errors.New("nil ENR")
	}
	if key.decode == nil {
		return value, false, errors.New("key has no decoder")
	}
	raw, ok := record.values[key.name]
	if !ok {
		return value, false, nil
	}
	value, err = key.decode(raw)
	if err != nil {
		return value, true, fmt.Errorf("decode ENR key %q: %w", key.name, err)
	}
	return value, true, nil
}

// Set creates an entry for Sign. Encoding errors, such as an invalid address,
// are returned by Sign.
func Set[T any](key Key[T], value T) Entry {
	entry := Entry{name: key.name}
	if key.encode == nil {
		entry.err = errors.New("key has no encoder")
		return entry
	}
	entry.encode = func() ([]byte, error) {
		return key.encode(value)
	}
	return entry
}

func uint16Key(name string) Key[uint16] {
	return Key[uint16]{
		name: name,
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
