package enr

import (
	"errors"
	"fmt"
	"net/netip"

	"github.com/ethp2p/ethp2p/identity"
)

// Entry is a record entry: a key and its value. Each entry type has one key
// and owns the encoding of its value, rejecting invalid values in both
// directions. To be read with [Record.Get], an entry type's pointer must also
// have an UnmarshalValue(raw []byte) error method, which decodes one RLP item.
type Entry interface {
	// Key returns the entry's key. It does not depend on the value.
	Key() string
	// MarshalValue encodes the value as one canonical RLP item.
	MarshalValue() ([]byte, error)
}

// entryPtr is a pointer to an entry type that decodes its value in place.
type entryPtr[E any] interface {
	*E
	Entry
	UnmarshalValue(raw []byte) error
}

// entry is an entry already encoded: a key and its value as raw RLP. Sign
// encodes every entry into this form before sorting and signing.
type entry struct {
	key   string
	value []byte
}

// The entries the stack reads. EIP-778 defines ip and ip6; libp2p nodes
// publish quic and quic6. Unspecified and multicast addresses and port 0 are
// invalid, since a record's addresses and ports say where its node can be
// reached.
type (
	// EntryIP is the node's IPv4 address under key ip, 4 bytes. It accepts an
	// IPv4-mapped IPv6 address and stores it unmapped.
	EntryIP netip.Addr
	// EntryIP6 is the node's IPv6 address under key ip6, 16 bytes.
	// IPv4-mapped addresses are invalid; they belong in [EntryIP].
	EntryIP6 netip.Addr
	// EntryQUIC is the node's QUIC port on its [EntryIP] address, under key
	// quic.
	EntryQUIC uint16
	// EntryQUIC6 is the node's QUIC port on its [EntryIP6] address, under key
	// quic6.
	EntryQUIC6 uint16
)

func (EntryIP) Key() string                        { return "ip" }
func (e EntryIP) MarshalValue() ([]byte, error)    { return marshalIP(netip.Addr(e).Unmap(), 4) }
func (e *EntryIP) UnmarshalValue(raw []byte) error { return unmarshalIP(raw, 4, (*netip.Addr)(e)) }

func (EntryIP6) Key() string                        { return "ip6" }
func (e EntryIP6) MarshalValue() ([]byte, error)    { return marshalIP(netip.Addr(e), 16) }
func (e *EntryIP6) UnmarshalValue(raw []byte) error { return unmarshalIP(raw, 16, (*netip.Addr)(e)) }

func (EntryQUIC) Key() string                        { return "quic" }
func (e EntryQUIC) MarshalValue() ([]byte, error)    { return marshalPort(uint16(e)) }
func (e *EntryQUIC) UnmarshalValue(raw []byte) error { return unmarshalPort(raw, (*uint16)(e)) }

func (EntryQUIC6) Key() string                        { return "quic6" }
func (e EntryQUIC6) MarshalValue() ([]byte, error)    { return marshalPort(uint16(e)) }
func (e *EntryQUIC6) UnmarshalValue(raw []byte) error { return unmarshalPort(raw, (*uint16)(e)) }

// EntryID is the record's identity scheme under key id. Sign writes v4, and
// Decode accepts only v4.
type EntryID string

func (EntryID) Key() string                     { return "id" }
func (e EntryID) MarshalValue() ([]byte, error) { return encodeBytes(e), nil }
func (e *EntryID) UnmarshalValue(raw []byte) error {
	id, err := decodeBytes(raw)
	if err != nil {
		return err
	}
	*e = EntryID(id)
	return nil
}

// EntrySecp256k1 is the record's identity key under key secp256k1, a 33-byte
// compressed public key. Sign writes it from the signing key, and Decode
// verifies the signature with it.
type EntrySecp256k1 struct{ *identity.PubKey }

func (EntrySecp256k1) Key() string                     { return "secp256k1" }
func (e EntrySecp256k1) MarshalValue() ([]byte, error) { return encodeBytes(e.Bytes()), nil }
func (e *EntrySecp256k1) UnmarshalValue(raw []byte) error {
	compressed, err := decodeBytes(raw)
	if err != nil {
		return err
	}
	e.PubKey, err = identity.ParsePubKey(compressed)
	return err
}

func marshalIP(addr netip.Addr, size int) ([]byte, error) {
	if err := checkIP(addr, size); err != nil {
		return nil, err
	}
	return encodeBytes(addr.AsSlice()), nil
}

func unmarshalIP(raw []byte, size int, dst *netip.Addr) error {
	payload, err := decodeBytes(raw)
	if err != nil {
		return err
	}
	if len(payload) != size {
		return fmt.Errorf("address must be %d bytes", size)
	}
	addr, _ := netip.AddrFromSlice(payload)
	if err := checkIP(addr, size); err != nil {
		return err
	}
	*dst = addr
	return nil
}

func checkIP(addr netip.Addr, size int) error {
	switch {
	case !addr.IsValid():
		return errors.New("invalid address")
	case size == 4 && !addr.Is4(), size == 16 && (!addr.Is6() || addr.Is4In6()):
		return fmt.Errorf("address %v is not %d bytes", addr, size)
	case addr.IsUnspecified() || addr.IsMulticast():
		return fmt.Errorf("address %v is unspecified or multicast", addr)
	}
	return nil
}

func marshalPort(port uint16) ([]byte, error) {
	if port == 0 {
		return nil, errors.New("port 0")
	}
	return encodeUint(port), nil
}

func unmarshalPort(raw []byte, dst *uint16) error {
	item, err := decodeSingleRLP(raw)
	if err != nil {
		return err
	}
	port, err := decodeUint[uint16](item)
	if err != nil {
		return err
	}
	if port == 0 {
		return errors.New("port 0")
	}
	*dst = port
	return nil
}
