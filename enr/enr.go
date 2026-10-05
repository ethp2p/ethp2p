// Package enr parses, verifies, and signs Ethereum Node Records (EIP-778) using
// the v4 identity scheme. A record keeps every entry's value as raw canonical
// RLP, and each [Entry] type owns the encoding of its value, so applications
// read and sign their own entries by declaring entry types, without changes
// here.
//
// This is not a general ENR library. It implements only what the stack's
// network view needs from a record, the peer's identity and its QUIC
// endpoints, so it has one identity scheme, no record mutation, and entry
// types only for the keys the stack reads.
//
// # Differences from go-ethereum
//
// The records are the same on the wire as those of go-ethereum's p2p/enr and
// p2p/enode; the API differs where those packages leave a caller room for a
// mistake or copy what this one shares.
//
//   - [Decode] verifies the signature, so every [Record] is authentic.
//     go-ethereum decodes and verifies in separate steps, and a caller that
//     skips the second holds an unverified record.
//   - A Record is immutable, and [Sign] is the only way to build one.
//     go-ethereum's Set changes a record in place and drops its signature until
//     SetSig signs it again, so a record can sit unsigned.
//   - Entry types check what a value means as well as its size, in both
//     directions: [EntryIP] and [EntryIP6] reject unspecified and multicast
//     addresses, and [EntryQUIC] and [EntryQUIC6] reject port 0. Sign therefore
//     cannot publish such an endpoint, and [Record.Get] never returns one.
//     go-ethereum's entry types check only the size and leave the rest to the
//     code that dials.
//   - [EntryIP6] rejects an IPv4-mapped address, which go-ethereum's enode
//     package uses as an IPv4 endpoint. Accepting it would let one record carry
//     two IPv4 endpoints, one under ip and one under ip6.
//   - Decode keeps the caller's buffer instead of copying it: entry values and
//     [Record.Encode] point into it, so the caller must not modify it.
//     go-ethereum's decoder reads from an io.Reader and copies every value it
//     returns.
//   - [Record.Get] returns the decoded entry instead of filling one passed by
//     pointer, and there is no reflection-based catch-all like go-ethereum's
//     WithEntry: an application declares a typed entry.
package enr

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"net/netip"
	"slices"

	"github.com/ethp2p/ethp2p/identity"
	"golang.org/x/crypto/sha3"
)

// maxRecordSize is checked before parsing, so RLP nesting depth and memory are
// bounded. Sign checks it too, since it decodes the record it signs.
const maxRecordSize = 300

// ErrMissing reports that a record has no entry under the requested key.
var ErrMissing = errors.New("ENR key is missing")

// Record is an immutable, signature-verified Ethereum Node Record.
type Record struct {
	raw     []byte
	seq     uint64
	pub     *identity.PubKey
	entries map[string][]byte
}

// Decode parses canonical RLP bytes and verifies the v4 signature. The record
// keeps raw, so the caller must not modify it afterwards.
func Decode(raw []byte) (*Record, error) {
	if len(raw) > maxRecordSize {
		return nil, fmt.Errorf("ENR exceeds %d-byte limit", maxRecordSize)
	}
	root, rest, err := decodeRLP(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid RLP: %w", err)
	}
	if len(rest) != 0 {
		return nil, errors.New("trailing bytes after ENR")
	}
	if root.kind != rlpList {
		return nil, errors.New("ENR must be an RLP list")
	}
	if len(root.children) < 2 || (len(root.children)-2)%2 != 0 {
		return nil, errors.New("ENR must contain a signature, sequence, and entries")
	}
	signature := root.children[0]
	if signature.kind != rlpString || len(signature.payload) != 64 {
		return nil, errors.New("ENR signature must contain 64 bytes")
	}
	seq, err := decodeUint[uint64](root.children[1])
	if err != nil {
		return nil, fmt.Errorf("invalid ENR sequence: %w", err)
	}

	entries := make(map[string][]byte, (len(root.children)-2)/2)
	var previous []byte
	for i := 2; i < len(root.children); i += 2 {
		key, value := root.children[i], root.children[i+1]
		if key.kind != rlpString {
			return nil, errors.New("ENR key must be an RLP byte string")
		}
		if i > 2 && bytes.Compare(previous, key.payload) >= 0 {
			return nil, errors.New("ENR keys must be strictly ascending")
		}
		previous = key.payload
		entries[string(key.payload)] = value.raw
	}

	r := &Record{raw: raw, seq: seq, entries: entries}
	id, err := r.Get[EntryID]()
	if err != nil {
		return nil, err
	}
	if id != "v4" {
		return nil, fmt.Errorf("ENR identity scheme %q is not v4", string(id))
	}
	publicKey, err := r.Get[EntrySecp256k1]()
	if err != nil {
		return nil, err
	}
	r.pub = publicKey.PubKey

	content := make([]byte, 0, len(raw)-len(signature.raw))
	for _, field := range root.children[1:] {
		content = append(content, field.raw...)
	}
	hash := keccak256(encodeRLPList(content))
	if !publicKey.VerifyHash(hash, signature.payload) {
		return nil, errors.New("invalid ENR signature")
	}
	return r, nil
}

// Encode returns the record's canonical RLP bytes, exactly as decoded or
// signed; re-encoding from parsed entries could change unknown entries and
// break the signature. The caller must not modify the result.
func (r *Record) Encode() []byte { return r.raw }

// Seq returns the record sequence number.
func (r *Record) Seq() uint64 {
	return r.seq
}

// PeerID derives the transport peer ID from the record's identity key.
func (r *Record) PeerID() identity.PeerID { return identity.PeerIDFromKey(r.pub) }

// QUIC returns the record's QUIC endpoints, IPv4 ([EntryIP] and [EntryQUIC])
// before IPv6 ([EntryIP6] and [EntryQUIC6]). A family whose address or port is
// missing or invalid is skipped. Callers should try each returned endpoint.
func (r *Record) QUIC() []netip.AddrPort {
	var endpoints []netip.AddrPort
	ip, ipErr := r.Get[EntryIP]()
	port, portErr := r.Get[EntryQUIC]()
	if ipErr == nil && portErr == nil {
		endpoints = append(endpoints, netip.AddrPortFrom(netip.Addr(ip), uint16(port)))
	}
	ip6, ipErr := r.Get[EntryIP6]()
	port6, portErr := r.Get[EntryQUIC6]()
	if ipErr == nil && portErr == nil {
		endpoints = append(endpoints, netip.AddrPortFrom(netip.Addr(ip6), uint16(port6)))
	}
	return endpoints
}

// Get decodes the record's entry of type E. It reports [ErrMissing] when the
// record has no entry under E's key, and E's decoding error when the value is
// invalid.
func (r *Record) Get[E any, P entryPtr[E]]() (E, error) {
	var e E
	key := P(&e).Key()
	raw, ok := r.entries[key]
	if !ok {
		return e, fmt.Errorf("%w: %q", ErrMissing, key)
	}
	if err := P(&e).UnmarshalValue(raw); err != nil {
		var zero E
		return zero, fmt.Errorf("ENR key %q: %w", key, err)
	}
	return e, nil
}

// Sign creates and verifies a v4 record holding entries plus the id and
// secp256k1 entries derived from key. It reports the first entry whose value
// failed to encode. Decoding the result rejects a key that appears twice, id
// and secp256k1 included.
func Sign(key *identity.PrivKey, seq uint64, entries ...Entry) (*Record, error) {
	if key == nil {
		return nil, errors.New("nil ENR signing key")
	}
	entries = append([]Entry{EntryID("v4"), EntrySecp256k1{key.Public()}}, entries...)
	signed := make([]entry, 0, len(entries))
	for _, e := range entries {
		value, err := e.MarshalValue()
		if err != nil {
			return nil, fmt.Errorf("ENR key %q: %w", e.Key(), err)
		}
		signed = append(signed, entry{key: e.Key(), value: value})
	}
	return Decode(signEntries(key, seq, signed))
}

// signEntries sorts entries by key in place, signs them as they are, adding
// neither id nor secp256k1, and returns the encoded record.
func signEntries(key *identity.PrivKey, seq uint64, entries []entry) []byte {
	slices.SortFunc(entries, func(a, b entry) int { return cmp.Compare(a.key, b.key) })
	content := encodeSignedContent(seq, entries)
	signature := key.SignHash(keccak256(encodeRLPList(content)))
	return encodeRLPList(append(encodeBytes(signature), content...))
}

func encodeSignedContent(seq uint64, entries []entry) []byte {
	content := encodeUint(seq)
	for _, entry := range entries {
		content = append(content, encodeBytes(entry.key)...)
		content = append(content, entry.value...)
	}
	return content
}

func keccak256(message []byte) []byte {
	hash := sha3.NewLegacyKeccak256()
	_, _ = hash.Write(message)
	return hash.Sum(nil)
}
